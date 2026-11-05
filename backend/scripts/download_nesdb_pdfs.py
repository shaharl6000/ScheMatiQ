"""Download the PDF of every paper cited in NESdb that is not in the folder yet.

Scrapes the NESdb (LRNes) protein pages for their PubMed IDs, like
schematiq-lib/schematiq/scripts/download_nes_papers.py does, skips papers that
already have a '<name>_<pmid>_full.pdf' in the output folder (or in any
--also-check folder), and fetches the rest from PMC Open Access, the publisher,
Unpaywall and EuropePMC. Each new PDF is checked against the paper's PubMed
title; PDFs that don't match go to <out_dir>/_unverified/ instead.

Papers that can't be fetched are written to <out_dir>/failed.json (the format
download_pdfs_browser.py reads) and <out_dir>/missing.csv (PMC links for
downloading by hand or with Claude in Chrome). Older copies of these two files
are kept as *_before_nesdb.*.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\download_nesdb_pdfs.py <out_dir>
        [--also-check DIR ...] [--include-doubt] [--email you@x] [--delay SECONDS]
"""

import argparse
import asyncio
import csv
import json
import random
import re
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import pymupdf
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_dataset_pdfs import download_from_pmc_s3, nes  # noqa: E402

PAGES = {"good": "namesGood.php", "doubt": "namesDoubt.php"}
PDF_PMID_RE = re.compile(r"_(\d{5,9})_full\.pdf$")


def scrape_nesdb(include_doubt: bool) -> dict[str, str]:
    """Return {pmid: protein name} for every paper linked from NESdb's protein pages."""
    papers: dict[str, str] = {}
    for kind in ["good", "doubt"] if include_doubt else ["good"]:
        soup = BeautifulSoup(requests.get(urljoin(nes.BASE_URL, PAGES[kind]), timeout=60).text, "html.parser")
        proteins = {}
        for a in soup.select("td:nth-of-type(1) a"):
            if a.get("href") and a.text.strip():
                proteins.setdefault(urljoin(nes.BASE_URL, a["href"]), a.text.strip())
        print(f"NESdb {kind}: {len(proteins)} proteins")
        for n, (url, name) in enumerate(proteins.items(), 1):
            html = requests.get(url, headers=nes.HEADERS, timeout=60).text
            for pmid in re.findall(r"list_uids=(\d+)", html):
                papers.setdefault(pmid, re.sub(r'[\\/:*?"<>|\s]+', "_", name))
            if n % 50 == 0:
                print(f"  {n}/{len(proteins)} proteins, {len(papers)} papers")
            time.sleep(0.2)
    return papers


def fetch_titles(pmids: list[str]) -> dict[str, str]:
    titles = {}
    for i in range(0, len(pmids), 200):
        r = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
                         params={"db": "pubmed", "id": ",".join(pmids[i:i + 200]), "retmode": "json"}, timeout=60)
        result = r.json().get("result", {})
        titles.update({p: result[p].get("title", "") for p in result.get("uids", [])})
        time.sleep(0.4)
    return titles


def matches_title(pdf: Path, title: str) -> bool:
    """True if most of the title's words appear in the PDF's first pages (or the PDF has no text layer)."""
    words = {w for w in re.findall(r"[a-z0-9]+", title.lower()) if len(w) > 3}
    try:
        doc = pymupdf.open(pdf)
        text = " ".join(doc[i].get_text() for i in range(min(len(doc), 3))).lower()
    except Exception:
        return False
    text = set(re.findall(r"[a-z0-9]+", text))
    if not words or len(text) < 50:  # scanned PDF: can't check
        return True
    return len(words & text) / len(words) >= 0.6


def backup(path: Path) -> None:
    old = path.with_name(f"{path.stem}_before_nesdb{path.suffix}")
    if path.exists() and not old.exists():
        shutil.copy(path, old)


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out_dir")
    parser.add_argument("--also-check", action="append", default=[], metavar="DIR",
                        help="Folder whose '<name>_<pmid>_full.pdf' files also count as downloaded")
    parser.add_argument("--include-doubt", action="store_true", help="Also the 'NESs in doubt' proteins")
    parser.add_argument("--email", default="", help="Email for the Unpaywall API (enables that strategy)")
    parser.add_argument("--delay", type=float, default=10,
                        help="Average seconds to wait between papers fetched from publisher sites (default: %(default)s)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    nes._setup_logging(str(out_dir))

    index_path = out_dir / "nesdb_index.json"
    if index_path.exists():
        papers = json.loads(index_path.read_text(encoding="utf-8"))
        print(f"Using cached NESdb index ({len(papers)} papers); delete {index_path.name} to re-scrape")
    else:
        papers = scrape_nesdb(args.include_doubt)
        index_path.write_text(json.dumps(papers, indent=1), encoding="utf-8")

    have = set()
    for d in [out_dir, *map(Path, args.also_check)]:
        have |= {m.group(1) for f in d.glob("*.pdf") if (m := PDF_PMID_RE.search(f.name))}
    todo = [p for p in papers if p not in have]
    print(f"{len(papers)} NESdb papers: {len(papers) - len(todo)} already have a PDF, {len(todo)} to download")

    titles = fetch_titles(todo)
    unverified_dir = out_dir / "_unverified"
    download_log = {"successful": {}, "failed": {}}
    meta_all, unverified = {}, []
    for start in range(0, len(todo), 50):
        batch = todo[start:start + 50]
        metadata = nes.fetch_pubmed_metadata(batch)
        meta_all.update(metadata)
        for pmid in batch:
            meta = metadata.get(pmid, {})
            doi = meta.get("doi")
            dest = out_dir / f"{papers[pmid]}_{pmid}_full.pdf"
            if not (meta.get("pmc_id") and download_from_pmc_s3(meta["pmc_id"], dest)):
                await nes.download_paper(
                    pubmed_id=pmid,
                    protein_name=papers[pmid],
                    abstract=meta.get("abstract"),
                    doi=doi,
                    full_text_url=f"https://doi.org/{doi}" if doi else None,
                    page=None,
                    download_log=download_log,
                    output_dir=str(out_dir),
                    api_email=args.email,
                    skip_playwright=True,
                    pmc_id=meta.get("pmc_id"),
                )
                # Publisher sites block fast, regular request patterns
                time.sleep(random.uniform(0.5, 1.5) * args.delay)
            if dest.exists() and not matches_title(dest, titles.get(pmid, "")):
                unverified_dir.mkdir(exist_ok=True)
                shutil.move(str(dest), str(unverified_dir / dest.name))
                unverified.append(pmid)
            if not dest.exists() and pmid not in download_log["failed"]:
                download_log["failed"][pmid] = {
                    "protein": papers[pmid], "doi": doi,
                    "full_text_url": f"https://doi.org/{doi}" if doi else None,
                    "publisher": nes.get_publisher_from_doi(doi) if doi else "unknown",
                    "reason": "PDF did not match the PubMed title" if pmid in unverified else "all strategies failed",
                }
            download_log["failed"].get(pmid, {})["title"] = titles.get(pmid, "")
        done = min(start + 50, len(todo))
        print(f"  {done}/{len(todo)}  failed={len(download_log['failed'])} unverified={len(unverified)}")

    failed = download_log["failed"]
    for name in ("failed.json", "missing.csv"):
        backup(out_dir / name)
    (out_dir / "failed.json").write_text(json.dumps({"failed": failed}, indent=2), encoding="utf-8")
    with open(out_dir / "missing.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pmid", "save_as", "doi", "pmc_id", "pmc_pdf_page", "publisher"])
        for pmid, info in failed.items():
            pmc = meta_all.get(pmid, {}).get("pmc_id") or ""
            pmc = pmc if not pmc or pmc.startswith("PMC") else f"PMC{pmc}"
            w.writerow([pmid, f"{papers[pmid]}_{pmid}_full.pdf", info.get("doi") or "", pmc,
                        f"https://pmc.ncbi.nlm.nih.gov/articles/{pmc}/pdf/" if pmc else "", info.get("publisher")])

    print(f"Done: {len(todo) - len(failed)} new PDFs, {len(failed)} still missing "
          f"({len(unverified)} of them downloaded but didn't match the title, see {unverified_dir.name}/). "
          f"Lists: {out_dir / 'missing.csv'}, {out_dir / 'failed.json'}")


if __name__ == "__main__":
    asyncio.run(main())
