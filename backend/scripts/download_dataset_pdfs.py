"""Re-download the original PDFs for a folder of NES .txt papers.

The Supabase datasets only hold extracted text. This takes the PubMed ID from
each '<name>_<pmid>_full.txt' filename and fetches the PDF from the open web
(EuropePMC, publisher URL, Unpaywall, PMC) using the download strategies in
schematiq-lib/schematiq/scripts/download_nes_papers.py. Files without a PubMed
ID in their name, and paywalled papers, cannot be fetched and are listed in
failed.json in the output folder.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\download_dataset_pdfs.py <txt_dir> <pdf_out_dir> [--email you@x]
"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "schematiq-lib" / "schematiq" / "scripts"))
import download_nes_papers as nes  # noqa: E402

# Keep only the PDF; the .txt versions already exist.
nes.convert_pdf_to_text = lambda pdf_path: None

PMID_RE = re.compile(r"^(.*)_(\d{5,9})_full\.txt$")
PMC_S3 = "https://pmc-oa-opendata.s3.amazonaws.com/{id}.{v}/{id}.{v}.pdf"


def download_from_pmc_s3(pmc_id: str, dest: Path) -> bool:
    """PMC Open Access subset PDFs on AWS open data (EuropePMC/PMC sites block scripts)."""
    pmc_id = pmc_id if pmc_id.startswith("PMC") else f"PMC{pmc_id}"
    for v in (1, 2, 3):
        try:
            r = requests.get(PMC_S3.format(id=pmc_id, v=v), timeout=60)
        except requests.RequestException:
            continue
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            dest.write_bytes(r.content)
            return True
    return False


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("txt_dir")
    parser.add_argument("out_dir")
    parser.add_argument("--email", default="", help="Email for the Unpaywall API (enables that strategy)")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    nes._setup_logging(str(out_dir))

    papers, no_pmid = {}, []
    for f in sorted(Path(args.txt_dir).glob("*.txt")):
        m = PMID_RE.match(f.name)
        if m:
            papers[m.group(2)] = m.group(1)
        else:
            no_pmid.append(f.name)

    download_log = {"successful": {}, "failed": {}}
    ids = list(papers)
    print(f"{len(ids)} papers with PubMed IDs, {len(no_pmid)} without (skipped)")

    for start in range(0, len(ids), 50):
        batch = ids[start:start + 50]
        metadata = nes.fetch_pubmed_metadata(batch)
        for pmid in batch:
            if (out_dir / f"{papers[pmid]}_{pmid}_full.pdf").exists():
                download_log["successful"][pmid] = "existing"
                continue
            meta = metadata.get(pmid, {})
            doi = meta.get("doi")
            if meta.get("pmc_id") and download_from_pmc_s3(meta["pmc_id"], out_dir / f"{papers[pmid]}_{pmid}_full.pdf"):
                download_log["successful"][pmid] = "pmc_s3"
                continue
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
            time.sleep(0.3)
        print(f"  {min(start + 50, len(ids))}/{len(ids)}  ok={len(download_log['successful'])} failed={len(download_log['failed'])}")

    report = {"no_pubmed_id": no_pmid, "failed": download_log["failed"]}
    (out_dir / "failed.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Done: {len(download_log['successful'])} PDFs, {len(download_log['failed'])} failed, "
          f"{len(no_pmid)} without PubMed ID. Details: {out_dir / 'failed.json'}")


if __name__ == "__main__":
    asyncio.run(main())
