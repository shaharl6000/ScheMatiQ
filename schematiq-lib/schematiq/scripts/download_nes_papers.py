#!/usr/bin/env python3
"""
Download papers from the LRNes protein database.

Scrapes protein pages from LRNes, follows PubMed links, and downloads
full-text papers using a multi-strategy pipeline:
  1. Publisher-specific PDF URL construction (sync, requests)
  2. Unpaywall open access lookup (sync, requires DOI)
  3. PubMed Central / EuropePMC (sync, requires DOI)
  4. Playwright browser automation (async, last resort)

Supports resume via JSON download log, retry of failed downloads,
CAPTCHA detection, cookie dismissal, and PDF-to-text conversion.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, quote

import aiofiles
import difflib
import requests
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "http://prodata.swmed.edu/LRNes/IndexFiles/"
MAIN_PAGE = "http://prodata.swmed.edu/LRNes/IndexFiles/namesGood.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DEFAULT_OUTPUT_DIR_ABS = "research/data/new_nes/abstracts"
DEFAULT_OUTPUT_DIR_FULL = "research/data/new_nes/full_text"

# CAPTCHA / human-verification indicators — skip pages with these
CAPTCHA_INDICATORS = [
    "verify you are human",
    "verifying you are human",
    "are you a robot",
    "captcha",
    "human verification",
    "please verify",
    "security check",
    "checking your browser",
    "just a moment",
    "enable javascript and cookies",
    "enable javascript",
    "ray id",
    "cf-browser-verification",
    "challenge-running",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("nes_downloader")


def _setup_logging(output_dir: str) -> None:
    """Configure file + stderr logging."""
    log_path = os.path.join(output_dir, "download.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ],
    )


# ---------------------------------------------------------------------------
# Download log (resume / retry support)
# ---------------------------------------------------------------------------


def _log_path(output_dir: str) -> str:
    return os.path.join(output_dir, "download_log.json")


def load_download_log(output_dir: str) -> dict:
    path = _log_path(output_dir)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"successful": {}, "failed": {}}


def save_download_log(log: dict, output_dir: str) -> None:
    with open(_log_path(output_dir), "w") as f:
        json.dump(log, f, indent=2)


# ---------------------------------------------------------------------------
# Session for connection pooling
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)


def _headers_with_referer(url: str) -> dict:
    parsed = urlparse(url)
    h = HEADERS.copy()
    h["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    return h


# ---------------------------------------------------------------------------
# DOI helpers
# ---------------------------------------------------------------------------


def extract_doi(url: str) -> Optional[str]:
    """Extract DOI (10.xxxx/…) from any string/URL."""
    m = re.search(r"10\.\d{4,}/[^\s<>\"]+", url)
    return m.group(0).rstrip(".") if m else None


def extract_doi_from_pubmed(html: str) -> Optional[str]:
    """Parse a PubMed page for the article DOI."""
    soup = BeautifulSoup(html, "html.parser")
    # <span class="identifier doi"> or <a> with doi.org href
    doi_span = soup.find("span", class_="identifier doi")
    if doi_span:
        a_tag = doi_span.find("a")
        if a_tag:
            return extract_doi(a_tag.get("href", "") or a_tag.get_text())
        return extract_doi(doi_span.get_text())
    # Fallback: any link containing doi.org
    for a in soup.find_all("a", href=True):
        if "doi.org" in a["href"]:
            return extract_doi(a["href"])
    # Regex fallback over raw HTML
    return extract_doi(html)


# ---------------------------------------------------------------------------
# NCBI E-utilities (programmatic PubMed access — avoids 403 from HTML scraping)
# ---------------------------------------------------------------------------


def fetch_pubmed_metadata(pubmed_ids: list[str]) -> dict[str, dict]:
    """Batch-fetch metadata for PubMed IDs via NCBI E-utilities.

    Returns dict keyed by pubmed_id with keys: doi, abstract, pmc_id.
    """
    if not pubmed_ids:
        return {}

    results: dict[str, dict] = {}
    # E-utilities supports up to ~200 IDs per request
    batch_size = 100
    for i in range(0, len(pubmed_ids), batch_size):
        batch = pubmed_ids[i : i + batch_size]
        ids_str = ",".join(batch)
        url = (
            f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
            f"?db=pubmed&id={ids_str}&rettype=xml"
        )
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "xml")
            for article in soup.find_all("PubmedArticle"):
                pmid_el = article.find("PMID")
                if not pmid_el:
                    continue
                pmid = pmid_el.text

                # DOI — first from ELocationID, then from ArticleIdList
                doi = None
                doi_el = article.find("ELocationID", EIdType="doi")
                if doi_el:
                    doi = doi_el.text
                if not doi:
                    for aid in article.find_all("ArticleId", IdType="doi"):
                        doi = aid.text
                        break

                # Abstract
                abstract_parts = article.find_all("AbstractText")
                abstract = "\n".join(p.text for p in abstract_parts) if abstract_parts else None

                # PMC ID (gives us a free full-text source)
                pmc_id = None
                for aid in article.find_all("ArticleId", IdType="pmc"):
                    pmc_id = aid.text
                    break

                results[pmid] = {
                    "doi": doi,
                    "abstract": abstract,
                    "pmc_id": pmc_id,
                }

            # Rate-limit: NCBI asks for ≤3 requests/sec without API key
            time.sleep(0.4)
        except Exception as exc:
            logger.error(f"E-utilities fetch failed for batch: {exc}")

    return results


# ---------------------------------------------------------------------------
# Publisher identification
# ---------------------------------------------------------------------------


def get_publisher_from_url(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    publisher_map = {
        "rupress.org": "jcb",
        "sciencedirect.com": "elsevier",
        "linkinghub.elsevier.com": "elsevier",
        "nature.com": "nature",
        "cell.com": "cell",
        "science.org": "science",
        "sciencemag.org": "science",
        "pnas.org": "pnas",
        "plos.org": "plos",
        "journals.plos.org": "plos",
        "elifesciences.org": "elife",
        "frontiersin.org": "frontiers",
        "europepmc.org": "pmc",
        "ncbi.nlm.nih.gov": "pmc",
        "pubmed.ncbi.nlm.nih.gov": "pubmed",
        "academic.oup.com": "oxford",
        "oup.com": "oxford",
        "springer.com": "springer",
        "link.springer.com": "springer",
        "wiley.com": "wiley",
        "onlinelibrary.wiley.com": "wiley",
        "febs.onlinelibrary.wiley.com": "wiley",
        "embopress.org": "embo",
        "portlandpress.com": "portland",
        "biochemsoctrans.org": "portland",
        "biochemj.org": "portland",
        "mcb.asm.org": "asm",
        "asm.org": "asm",
        "journals.asm.org": "asm",
        "molbiolcell.org": "ascb",
        "jbc.org": "jbc",
        "genesdev.org": "cshl",
        "cshlp.org": "cshl",
        "biorxiv.org": "biorxiv",
        "medrxiv.org": "medrxiv",
        "biologists.com": "jcs",
        "journals.biologists.com": "jcs",
        "pubs.rsc.org": "rsc",
        "rsc.org": "rsc",
        "tandfonline.com": "taylor",
        "landesbioscience.com": "landes",
    }
    for key, pub in publisher_map.items():
        if key in domain:
            return pub
    if "doi.org" in domain:
        path = urlparse(url).path.lower()
        if "10.4161" in path:
            return "landes"
        if "10.1080" in path:
            return "taylor"
    return "unknown"


def get_publisher_from_doi(doi: str) -> str:
    """Identify publisher from DOI prefix."""
    if not doi:
        return "unknown"
    _DOI_PREFIX_MAP = {
        "10.1038": "nature",
        "10.1016": "elsevier",
        "10.1006": "elsevier",
        "10.1074": "jbc",
        "10.1083": "jcb",
        "10.1073": "pnas",
        "10.1126": "science",
        "10.1242": "jcs",
        "10.1091": "ascb",
        "10.1002": "wiley",
        "10.1111": "wiley",
        "10.1007": "springer",
        "10.1093": "oxford",
        "10.1371": "plos",
        "10.7554": "elife",
        "10.3389": "frontiers",
        "10.1186": "springer",
        "10.1101": "biorxiv",
        "10.1128": "asm",
        "10.1080": "taylor",
        "10.4161": "landes",
        "10.1039": "rsc",
        "10.15252": "embo",
        "10.1261": "cshl",
        "10.1101/gad": "cshl",
        "10.1042": "portland",
    }
    for prefix, pub in _DOI_PREFIX_MAP.items():
        if doi.startswith(prefix):
            return pub
    return "unknown"


def _pdf_url_from_doi(doi: str, publisher: str) -> Optional[str]:
    """Construct a direct PDF URL from a DOI for known publishers."""
    if publisher == "jbc":
        return f"https://www.jbc.org/doi/pdf/{doi}"
    if publisher == "jcb":
        return f"https://rupress.org/jcb/article-pdf/doi/{doi}"
    if publisher == "nature":
        # nature.com/articles/<suffix>.pdf
        suffix = doi.replace("10.1038/", "")
        return f"https://www.nature.com/articles/{suffix}.pdf"
    if publisher == "pnas":
        return f"https://www.pnas.org/doi/pdf/{doi}"
    if publisher == "science":
        return f"https://www.science.org/doi/pdf/{doi}"
    if publisher == "wiley":
        return f"https://onlinelibrary.wiley.com/doi/pdf/{doi}"
    if publisher == "springer":
        return f"https://link.springer.com/content/pdf/{doi}.pdf"
    if publisher == "oxford":
        return None  # Oxford URLs are too varied to construct from DOI
    if publisher == "ascb":
        return f"https://www.molbiolcell.org/doi/pdf/{doi}"
    if publisher == "asm":
        return f"https://journals.asm.org/doi/pdf/{doi}"
    if publisher == "taylor":
        return f"https://www.tandfonline.com/doi/pdf/{doi}"
    if publisher == "landes":
        return f"https://www.tandfonline.com/doi/pdf/{doi}"
    if publisher == "cshl":
        return None  # CSHL has multiple journal domains
    if publisher == "embo":
        return None
    if publisher == "elife":
        return None
    if publisher == "plos":
        return None
    if publisher == "frontiers":
        return None
    if publisher == "biorxiv":
        return f"https://www.biorxiv.org/content/{doi}.full.pdf"
    if publisher == "rsc":
        return None
    if publisher == "elsevier":
        return None  # Elsevier needs PII, not DOI
    if publisher == "jcs":
        return None  # JCS URLs are complex
    return None


# ---------------------------------------------------------------------------
# Publisher-specific PDF URL construction
# ---------------------------------------------------------------------------


def _pdf_url_nature(url: str) -> Optional[str]:
    if "/articles/" in url:
        return url.rstrip("/") + ".pdf"
    return None


def _pdf_url_cell(url: str) -> Optional[str]:
    if "/fulltext/" in url:
        return url.replace("/fulltext/", "/pdf/") + ".pdf"
    if "/abstract/" in url:
        return url.replace("/abstract/", "/pdf/") + ".pdf"
    return None


def _pdf_url_elsevier(url: str) -> Optional[str]:
    if "pii/" in url:
        m = re.search(r"pii/([A-Z0-9]+)", url)
        if m:
            return f"https://www.sciencedirect.com/science/article/pii/{m.group(1)}/pdfft"
    return None


def _pdf_url_wiley(url: str) -> Optional[str]:
    if "/doi/" in url and "/pdf/" not in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


def _pdf_url_springer(url: str) -> Optional[str]:
    if "/article/" in url:
        doi_part = url.split("/article/")[-1]
        return f"https://link.springer.com/content/pdf/{doi_part}.pdf"
    return None


def _pdf_url_pnas(url: str) -> Optional[str]:
    if "/content/" in url:
        return url.rstrip("/") + ".full.pdf"
    if "/doi/" in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


def _pdf_url_plos(url: str) -> Optional[str]:
    if "id=" in url:
        m = re.search(r"id=([^&]+)", url)
        if m:
            return f"https://journals.plos.org/plosone/article/file?id={m.group(1)}&type=printable"
    return None


def _pdf_url_elife(url: str) -> Optional[str]:
    m = re.search(r"/articles/(\d+)", url)
    if m:
        return f"https://elifesciences.org/download/{m.group(1)}/pdf"
    m = re.search(r"/reviewed-preprints/(\d+)", url)
    if m:
        return f"https://elifesciences.org/reviewed-preprints/{m.group(1)}/downloads/pdf"
    return None


def _pdf_url_oxford(url: str) -> Optional[str]:
    if "/article/" in url:
        try:
            r = session.get(url, timeout=30)
            m = re.search(r'href="([^"]+\.pdf[^"]*)"', r.text)
            if m:
                return urljoin(url, m.group(1))
        except Exception:
            pass
    return None


def _pdf_url_frontiers(url: str) -> Optional[str]:
    if "/full" in url:
        return url.replace("/full", "/pdf")
    if "/articles/" in url:
        return url.rstrip("/") + "/pdf"
    return None


def _pdf_url_embo(url: str) -> Optional[str]:
    if "/content/" in url:
        return url.rstrip("/") + ".full.pdf"
    return None


def _pdf_url_jbc(url: str) -> Optional[str]:
    if "/fulltext" in url:
        return url.replace("/fulltext", ".pdf")
    if "/article/" in url:
        return url.rstrip("/") + ".pdf"
    return None


def _pdf_url_cshl(url: str) -> Optional[str]:
    if "/content/" in url:
        return url.rstrip("/") + ".full.pdf"
    return None


def _pdf_url_asm(url: str) -> Optional[str]:
    if "/doi/" in url and "/pdf/" not in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


def _pdf_url_ascb(url: str) -> Optional[str]:
    if "/doi/" in url and "/pdf/" not in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


def _pdf_url_portland(url: str) -> Optional[str]:
    if "/article/" in url:
        return url.rstrip("/") + ".pdf"
    return None


def _pdf_url_biorxiv(url: str) -> Optional[str]:
    if "/content/" in url:
        base = url.rstrip("/")
        return base if base.endswith(".pdf") else base + ".full.pdf"
    return None


def _pdf_url_jcb(url: str) -> Optional[str]:
    if "/content/" in url:
        return url.rstrip("/") + ".full.pdf"
    if "/article/" in url:
        try:
            r = session.get(url, timeout=30, headers=_headers_with_referer(url))
            m = re.search(r'href="([^"]+/article-pdf/[^"]+)"', r.text)
            if m:
                return urljoin(url, m.group(1))
        except Exception:
            pass
    return None


def _pdf_url_jcs(url: str) -> Optional[str]:
    if "/article/" in url:
        try:
            r = session.get(url, timeout=30)
            m = re.search(r'href="([^"]+article-pdf[^"]+\.pdf)"', r.text)
            if m:
                return urljoin(url, m.group(1))
        except Exception:
            pass
    if "/content/" in url:
        return url.rstrip("/") + ".full.pdf"
    return None


def _pdf_url_rsc(url: str) -> Optional[str]:
    if "/articlelanding/" in url:
        return url.replace("/articlelanding/", "/articlepdf/")
    if "/articlehtml/" in url:
        return url.replace("/articlehtml/", "/articlepdf/")
    return None


def _pdf_url_landes(url: str) -> Optional[str]:
    if "10.4161" in url:
        m = re.search(r"(10\.4161/[^\s?#]+)", url)
        if m:
            return f"https://www.tandfonline.com/doi/pdf/{m.group(1)}"
    return None


def _pdf_url_taylor(url: str) -> Optional[str]:
    if "/doi/full/" in url:
        return url.replace("/doi/full/", "/doi/pdf/")
    if "/doi/abs/" in url:
        return url.replace("/doi/abs/", "/doi/pdf/")
    if "/doi/" in url and "/pdf/" not in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


def _pdf_url_science(url: str) -> Optional[str]:
    if "/doi/" in url and "/pdf/" not in url:
        return url.replace("/doi/", "/doi/pdf/")
    return None


_PUBLISHER_PDF_HANDLERS = {
    "nature": _pdf_url_nature,
    "cell": _pdf_url_cell,
    "elsevier": _pdf_url_elsevier,
    "wiley": _pdf_url_wiley,
    "springer": _pdf_url_springer,
    "pnas": _pdf_url_pnas,
    "plos": _pdf_url_plos,
    "elife": _pdf_url_elife,
    "oxford": _pdf_url_oxford,
    "frontiers": _pdf_url_frontiers,
    "embo": _pdf_url_embo,
    "jbc": _pdf_url_jbc,
    "cshl": _pdf_url_cshl,
    "asm": _pdf_url_asm,
    "ascb": _pdf_url_ascb,
    "portland": _pdf_url_portland,
    "biorxiv": _pdf_url_biorxiv,
    "medrxiv": _pdf_url_biorxiv,
    "jcb": _pdf_url_jcb,
    "jcs": _pdf_url_jcs,
    "rsc": _pdf_url_rsc,
    "landes": _pdf_url_landes,
    "taylor": _pdf_url_taylor,
    "science": _pdf_url_science,
}


def get_publisher_pdf_url(html_url: str, publisher: str) -> Optional[str]:
    handler = _PUBLISHER_PDF_HANDLERS.get(publisher)
    return handler(html_url) if handler else None


# ---------------------------------------------------------------------------
# Unpaywall & PMC open-access lookups
# ---------------------------------------------------------------------------


def check_unpaywall(doi: str, api_email: str) -> Optional[str]:
    """Check Unpaywall for an open-access PDF URL."""
    try:
        r = session.get(
            f"https://api.unpaywall.org/v2/{doi}?email={api_email}",
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        oa = data.get("best_oa_location")
        if oa:
            pdf = oa.get("url_for_pdf")
            if pdf:
                logger.info(f"Unpaywall PDF: {pdf}")
                return pdf
            landing = oa.get("url")
            if landing:
                logger.info(f"Unpaywall landing: {landing}")
                return landing
        for loc in data.get("oa_locations", []):
            pdf = loc.get("url_for_pdf")
            if pdf:
                logger.info(f"Unpaywall alt PDF: {pdf}")
                return pdf
    except Exception as exc:
        logger.debug(f"Unpaywall failed for {doi}: {exc}")
    return None


def check_pmc(doi: str) -> Optional[str]:
    """Check EuropePMC for a free PDF."""
    try:
        r = session.get(
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{doi}&format=json",
            timeout=15,
        )
        if r.status_code == 200:
            results = r.json().get("resultList", {}).get("result", [])
            if results:
                pmcid = results[0].get("pmcid")
                if pmcid:
                    pdf_url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmcid}&blobtype=pdf"
                    logger.info(f"EuropePMC: {pmcid}")
                    return pdf_url
    except Exception as exc:
        logger.debug(f"PMC check failed for {doi}: {exc}")
    return None


# ---------------------------------------------------------------------------
# PDF download & validation
# ---------------------------------------------------------------------------


def download_pdf(pdf_url: str, output_path: str, referer_url: str = None) -> bool:
    """Download a PDF, validate magic bytes + minimum size."""
    try:
        headers = _headers_with_referer(referer_url or pdf_url)
        r = session.get(pdf_url, timeout=60, stream=True, headers=headers)
        r.raise_for_status()

        ct = r.headers.get("content-type", "").lower()
        if "html" in ct and "pdf" not in ct:
            logger.warning(f"Got HTML instead of PDF: {pdf_url}")
            return False

        content = r.content
        if not content.startswith(b"%PDF"):
            if b"<html" in content[:1000].lower():
                logger.warning(f"HTML page instead of PDF: {pdf_url}")
                return False
            logger.warning(f"Content doesn't look like PDF (saving anyway): {pdf_url}")

        with open(output_path, "wb") as f:
            f.write(content)

        if os.path.getsize(output_path) < 1000:
            logger.warning(f"PDF too small ({os.path.getsize(output_path)} bytes)")
            os.unlink(output_path)
            return False

        return True
    except requests.RequestException as exc:
        logger.error(f"Download failed {pdf_url}: {exc}")
        return False


# ---------------------------------------------------------------------------
# PDF → text conversion
# ---------------------------------------------------------------------------


def convert_pdf_to_text(pdf_path: str) -> Optional[str]:
    """Convert PDF to markdown text via pymupdf4llm. Returns None on failure.

    Validates that the extracted text contains meaningful content (not just
    replacement characters from scanned/image-based PDFs).
    """
    try:
        import pymupdf4llm

        text = pymupdf4llm.to_markdown(pdf_path)

        # Quality check: if <10% of alphabetic chars are ASCII, it's likely
        # a scanned PDF producing garbage replacement characters
        alpha_chars = [c for c in text if c.isalpha()]
        if alpha_chars:
            ascii_ratio = sum(1 for c in alpha_chars if c.isascii()) / len(alpha_chars)
            if ascii_ratio < 0.3:
                logger.warning(
                    f"PDF text extraction too low quality (ASCII ratio {ascii_ratio:.0%}), "
                    f"likely scanned: {pdf_path}"
                )
                return None

        if len(text.strip()) < 100:
            logger.warning(f"PDF text extraction too short: {pdf_path}")
            return None

        txt_path = Path(pdf_path).with_suffix(".txt")
        txt_path.write_text(text, encoding="utf-8")
        logger.info(f"Converted PDF→text: {txt_path}")
        return str(txt_path)
    except ImportError:
        logger.debug("pymupdf4llm not installed — skipping PDF-to-text")
    except Exception as exc:
        logger.warning(f"PDF-to-text failed for {pdf_path}: {exc}")
    return None


# ---------------------------------------------------------------------------
# Playwright helpers (cookie dismissal, CAPTCHA, full-text extraction)
# ---------------------------------------------------------------------------


async def dismiss_cookie_popup(page) -> bool:
    """Try to dismiss cookie-consent banners."""
    selectors = [
        'button:has-text("Accept")',
        'button:has-text("Accept all")',
        'button:has-text("Accept cookies")',
        'button:has-text("I agree")',
        'button:has-text("OK")',
        'button:has-text("Got it")',
        'button:has-text("Allow all")',
        'button:has-text("Agree")',
        'button:has-text("Continue")',
        'button[id*="accept"]',
        'button[id*="cookie"]',
        'a:has-text("Accept")',
        'a:has-text("Accept all")',
        ".cc-accept",
        "#onetrust-accept-btn-handler",
        '[data-testid="cookie-accept"]',
        '[data-testid="accept-button"]',
        ".cookie-accept",
        ".accept-cookies",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        ".js-accept-cookies",
        '[aria-label="Accept cookies"]',
        'button[data-cc-action="accept"]',
        "button.cc-btn.cc-dismiss",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=800):
                logger.info(f"Dismissing cookie popup: {sel}")
                await btn.click()
                await page.wait_for_timeout(400)
                return True
        except Exception:
            continue
    return False


def _is_captcha(html_lower: str) -> bool:
    return any(ind in html_lower for ind in CAPTCHA_INDICATORS)


def extract_journal_text(html: str, abstract_text: str = None) -> Optional[str]:
    """Extract body text from a journal HTML page, trimming before abstract
    and after references."""
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(separator="\n", strip=True)
    if not full_text:
        return None

    def clean_abstract(text):
        if not text:
            return ""
        text = re.sub(r"^(abstract[\s:\-]*)", "", text.strip(), flags=re.IGNORECASE)
        return " ".join(text.split()[:10])

    clean_abs = clean_abstract(abstract_text)

    def fuzzy_find(text, pattern, threshold=0.8):
        plen = len(pattern)
        best_ratio, best_start = 0, -1
        for i in range(0, len(text) - plen):
            window = text[i : i + plen + 10]
            ratio = difflib.SequenceMatcher(None, pattern.lower(), window.lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_start = i
        return best_start

    start_idx = fuzzy_find(full_text, clean_abs) if clean_abs else -1
    if start_idx == -1:
        logger.debug("Could not locate abstract in full text — starting from top")
        start_idx = 0

    full_text = full_text[start_idx:]
    end_idx = None
    for marker in ["References", "REFERENCES", "Bibliography"]:
        idx = full_text.find(marker)
        if idx != -1:
            end_idx = idx
            break
    text = full_text[:end_idx].strip() if end_idx else full_text.strip()
    return text if text else None


# ---------------------------------------------------------------------------
# Playwright-based full-text download (last-resort strategy)
# ---------------------------------------------------------------------------


async def try_save_full_text_playwright(
    url: str,
    protein_name: str,
    pubmed_id: str,
    abstract: Optional[str],
    page,
    output_dir: str,
) -> bool:
    """Use Playwright to grab full text (PDF redirect or HTML scrape)."""
    try:
        logger.info(f"Playwright: navigating to {url}")
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(5000)
        await dismiss_cookie_popup(page)
        await page.wait_for_timeout(3000)

        html = await page.content()
        final_url = page.url

        if _is_captcha(html.lower()):
            logger.warning("CAPTCHA detected — skipping Playwright path")
            return False

        # If redirected to a PDF, download it via requests
        if ".pdf" in final_url.lower():
            filepath = os.path.join(output_dir, f"{protein_name}_{pubmed_id}_full.pdf")
            r = requests.get(final_url, headers=HEADERS, timeout=15)
            if r.ok and r.content.startswith(b"%PDF"):
                with open(filepath, "wb") as f:
                    f.write(r.content)
                logger.info(f"Saved PDF (Playwright redirect): {filepath}")
                convert_pdf_to_text(filepath)
                return True

        # Otherwise extract HTML text
        text = extract_journal_text(html, abstract)
        if text:
            filepath = os.path.join(output_dir, f"{protein_name}_{pubmed_id}_full.txt")
            async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
                await f.write(text)
            logger.info(f"Saved HTML text: {filepath}")
            return True

        return False
    except Exception as exc:
        logger.error(f"Playwright failed for {url}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Multi-strategy download orchestrator
# ---------------------------------------------------------------------------


async def download_paper(
    pubmed_id: str,
    protein_name: str,
    abstract: Optional[str],
    doi: Optional[str],
    full_text_url: Optional[str],
    page,  # Playwright page (may be None if --skip-playwright)
    download_log: dict,
    output_dir: str,
    api_email: str,
    skip_playwright: bool = False,
    pmc_id: Optional[str] = None,
) -> bool:
    """Try all strategies to download a paper. Returns True on success."""

    # Already downloaded?
    if pubmed_id in download_log["successful"]:
        logger.info(f"Skipping {pubmed_id} — already downloaded")
        return True

    pdf_urls_to_try: list[tuple[str, str]] = []  # (source_label, url)

    # Resolve publisher from full-text URL or DOI prefix
    publisher = get_publisher_from_url(full_text_url) if full_text_url else "unknown"
    if publisher == "unknown" and doi:
        publisher = get_publisher_from_doi(doi)

    # Strategy 1: PMC direct (if we already have PMC ID from E-utilities)
    if pmc_id:
        pmc_pdf = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmc_id}&blobtype=pdf"
        pdf_urls_to_try.append(("pmc_direct", pmc_pdf))

    # Strategy 2: publisher-specific PDF URL from full-text URL
    if full_text_url and publisher != "unknown":
        pdf_url = get_publisher_pdf_url(full_text_url, publisher)
        if pdf_url:
            pdf_urls_to_try.append(("publisher", pdf_url))

    # Strategy 3: construct PDF URL directly from DOI + publisher
    if doi and publisher != "unknown":
        doi_pdf = _pdf_url_from_doi(doi, publisher)
        if doi_pdf:
            # Avoid duplicates
            existing_urls = {u for _, u in pdf_urls_to_try}
            if doi_pdf not in existing_urls:
                pdf_urls_to_try.append(("doi_direct", doi_pdf))

    # Strategy 4: Unpaywall
    if doi and api_email:
        oa_url = check_unpaywall(doi, api_email)
        if oa_url:
            pdf_urls_to_try.append(("unpaywall", oa_url))

    # Strategy 5: PMC / EuropePMC via DOI lookup (if no pmc_id already)
    if doi and not pmc_id:
        pmc_url = check_pmc(doi)
        if pmc_url:
            pdf_urls_to_try.append(("pmc", pmc_url))

    # Construct full_text_url for Playwright fallback if we only have DOI
    if not full_text_url and doi:
        full_text_url = f"https://doi.org/{doi}"

    # Try sync downloads
    filepath_pdf = os.path.join(output_dir, f"{protein_name}_{pubmed_id}_full.pdf")
    for source, try_url in pdf_urls_to_try:
        logger.info(f"Trying {source}: {try_url}")
        if download_pdf(try_url, filepath_pdf, referer_url=full_text_url):
            logger.info(f"Downloaded ({source}): {filepath_pdf}")
            download_log["successful"][pubmed_id] = filepath_pdf
            convert_pdf_to_text(filepath_pdf)
            return True
        time.sleep(0.5)

    # Strategy 4: Playwright browser (last resort)
    if not skip_playwright and full_text_url and page is not None:
        if await try_save_full_text_playwright(
            full_text_url, protein_name, pubmed_id, abstract, page, output_dir
        ):
            # Determine which file was saved
            for ext in (".pdf", ".txt"):
                fp = os.path.join(output_dir, f"{protein_name}_{pubmed_id}_full{ext}")
                if os.path.exists(fp):
                    download_log["successful"][pubmed_id] = fp
                    break
            else:
                download_log["successful"][pubmed_id] = f"{protein_name}_{pubmed_id}"
            return True

    # All strategies failed
    tried = [s for s, _ in pdf_urls_to_try]
    if not skip_playwright and full_text_url:
        tried.append("playwright")
    download_log["failed"][pubmed_id] = {
        "protein": protein_name,
        "doi": doi,
        "full_text_url": full_text_url,
        "publisher": publisher,
        "strategies_tried": tried,
        "reason": "all strategies failed",
    }
    return False


# ---------------------------------------------------------------------------
# Main scraping loop
# ---------------------------------------------------------------------------


async def main():
    parser = argparse.ArgumentParser(
        description="Download papers from the LRNes protein database"
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR_FULL,
        help="Directory for full-text downloads (default: %(default)s)",
    )
    parser.add_argument(
        "--abstracts-dir",
        default=DEFAULT_OUTPUT_DIR_ABS,
        help="Directory for abstracts (default: %(default)s)",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="Retry previously failed downloads",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Run Playwright browser in headless mode",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run Playwright with visible browser (default)",
    )
    parser.add_argument(
        "--skip-playwright",
        action="store_true",
        help="Only use sync download strategies (no browser)",
    )
    parser.add_argument(
        "--api-email",
        default=None,
        help="Email for Unpaywall/CrossRef APIs (required for Unpaywall)",
    )
    args = parser.parse_args()

    headless = args.headless and not args.no_headless

    output_dir = args.output_dir
    abstracts_dir = args.abstracts_dir
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(abstracts_dir, exist_ok=True)

    _setup_logging(output_dir)

    download_log = load_download_log(output_dir)

    # --retry mode: only re-attempt failed IDs
    retry_ids: set[str] | None = None
    if args.retry:
        retry_ids = set(download_log.get("failed", {}).keys())
        if not retry_ids:
            logger.info("No failed downloads to retry.")
            return
        logger.info(f"Retry mode: {len(retry_ids)} failed papers to retry")
        # Clear them from failed so they get re-attempted
        for pid in retry_ids:
            download_log["failed"].pop(pid, None)

    # Set up Playwright (unless skipped)
    page = None
    browser = None
    pw = None
    if not args.skip_playwright:
        from playwright.async_api import async_playwright

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="en-US",
            extra_http_headers=HEADERS,
        )
        page = await context.new_page()

    try:
        # Fetch protein list from LRNes
        soup = BeautifulSoup(requests.get(MAIN_PAGE).text, "html.parser")
        protein_links = [
            (a.text.strip().replace("/", "_"), urljoin(BASE_URL, a.get("href")))
            for a in soup.select("td:nth-of-type(1) a")
            if a.get("href") and a.text.strip()
        ]
        logger.info(f"Found {len(protein_links)} proteins on LRNes")

        successful = 0
        failed = 0
        skipped = 0

        for protein_name, protein_url in tqdm_asyncio(
            protein_links, desc="Processing proteins"
        ):
            try:
                protein_html = requests.get(protein_url, headers=HEADERS).text
                soup = BeautifulSoup(protein_html, "html.parser")
                pubmed_links = [
                    a.get("href")
                    for a in soup.find_all("a")
                    if "PubMed" in a.text and "ncbi.nlm.nih.gov" in a.get("href", "")
                ]

                # Extract PubMed IDs from the old-style URLs
                pubmed_ids = []
                for pubmed_url in pubmed_links:
                    pid = pubmed_url.strip("/").split("=")[-1]
                    pubmed_ids.append(pid)

                # Filter based on retry mode and already-successful
                ids_to_process = []
                for pid in pubmed_ids:
                    if retry_ids is not None and pid not in retry_ids:
                        continue
                    if pid in download_log["successful"]:
                        skipped += 1
                        continue
                    ids_to_process.append(pid)

                if not ids_to_process:
                    continue

                # Batch-fetch metadata from NCBI E-utilities
                metadata = fetch_pubmed_metadata(ids_to_process)

                for pubmed_id in ids_to_process:
                    meta = metadata.get(pubmed_id, {})
                    doi = meta.get("doi")
                    abstract = meta.get("abstract")
                    pmc_id = meta.get("pmc_id")

                    # Save abstract
                    if abstract:
                        abs_path = os.path.join(
                            abstracts_dir, f"{protein_name}_{pubmed_id}_abstract.txt"
                        )
                        async with aiofiles.open(abs_path, "w", encoding="utf-8") as f:
                            await f.write(abstract)

                    # Construct full-text URL from DOI when available
                    full_text_url = f"https://doi.org/{doi}" if doi else None

                    ok = await download_paper(
                        pubmed_id=pubmed_id,
                        protein_name=protein_name,
                        abstract=abstract,
                        doi=doi,
                        full_text_url=full_text_url,
                        page=page,
                        download_log=download_log,
                        output_dir=output_dir,
                        api_email=args.api_email or "",
                        skip_playwright=args.skip_playwright,
                        pmc_id=pmc_id,
                    )
                    if ok:
                        successful += 1
                    else:
                        failed += 1

                    # Rate-limit
                    time.sleep(0.5)

                # Save log after each protein batch
                save_download_log(download_log, output_dir)

            except Exception as exc:
                logger.error(f"Error processing protein {protein_url}: {exc}")

        # Final save
        save_download_log(download_log, output_dir)

        # Summary
        total_ok = len(download_log["successful"])
        total_fail = len(download_log["failed"])
        print(f"\n{'=' * 50}")
        print("Download Summary:")
        print(f"  Successful this run: {successful}")
        print(f"  Failed this run:     {failed}")
        print(f"  Skipped (already):   {skipped}")
        print(f"  Total successful:    {total_ok}")
        print(f"  Total failed:        {total_fail}")
        print(f"{'=' * 50}")
        print(f"Log: {_log_path(output_dir)}")
        if failed > 0:
            print(f"\nTo retry failed downloads:  python {sys.argv[0]} --retry")
            print(f"To skip browser fallback:   python {sys.argv[0]} --skip-playwright")

    finally:
        if browser:
            await browser.close()
        if pw:
            await pw.stop()


if __name__ == "__main__":
    asyncio.run(main())
