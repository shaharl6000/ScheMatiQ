"""
Script to download PDFs from paper links in a Google Sheet.
Supports OpenReview, ArXiv, and other sources.

Usage:
    python download_cot_pdfs.py
"""

import os
import re
import time
import requests
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import hashlib

# Google Sheet URL (exported as CSV)
SHEET_ID = "1zCxzKUg9BrbNfqJY1BEmNgCN0d38jaF8ads4Dyt5nbE"
GID = "1117968829"
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

# Output directory
OUTPUT_DIR = Path(__file__).parent / "cot_pdf"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def get_sheet_data() -> pd.DataFrame:
    """Fetch the Google Sheet as a CSV and return as DataFrame."""
    print(f"Fetching Google Sheet...")
    response = requests.get(SHEET_CSV_URL)
    response.raise_for_status()

    from io import StringIO
    df = pd.read_csv(StringIO(response.text))
    print(f"Found {len(df)} rows in the sheet")
    return df


def get_unique_paper_links(df: pd.DataFrame) -> list[str]:
    """Extract unique paper links from the first column."""
    first_col = df.iloc[:, 0]
    unique_links = first_col.dropna().astype(str).unique().tolist()
    # Filter out non-URL values
    unique_links = [link for link in unique_links if link.startswith('http')]
    print(f"Found {len(unique_links)} unique paper links")
    return unique_links


def get_pdf_url_and_filename(url: str) -> tuple[str, str] | None:
    """
    Convert a paper URL to its PDF download URL and a safe filename.
    Returns (pdf_url, filename) or None if unsupported.
    """
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # OpenReview
    if "openreview.net" in domain:
        query_params = parse_qs(parsed.query)
        if "id" in query_params:
            paper_id = query_params["id"][0]
            pdf_url = f"https://openreview.net/pdf?id={paper_id}"
            return pdf_url, f"openreview_{paper_id}.pdf"

    # ArXiv
    if "arxiv.org" in domain:
        # Handle various arxiv URL formats
        # https://arxiv.org/abs/2301.12345
        # https://arxiv.org/pdf/2301.12345
        # https://arxiv.org/abs/2301.12345v1
        path = parsed.path
        arxiv_id_match = re.search(r'(\d{4}\.\d{4,5}(?:v\d+)?)', path)
        if arxiv_id_match:
            arxiv_id = arxiv_id_match.group(1)
            pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
            safe_id = arxiv_id.replace('.', '_')
            return pdf_url, f"arxiv_{safe_id}.pdf"

    # ACL Anthology
    if "aclanthology.org" in domain:
        path = parsed.path.strip('/')
        if path:
            # If it's already a PDF link, use it directly
            if path.endswith('.pdf'):
                paper_id = path.replace('.pdf', '').split('/')[-1]
                safe_id = paper_id.replace('.', '_').replace('-', '_')
                return url, f"acl_{safe_id}.pdf"
            # Otherwise construct PDF URL
            paper_id = path.split('/')[-1]
            pdf_url = f"https://aclanthology.org/{paper_id}.pdf"
            safe_id = paper_id.replace('.', '_').replace('-', '_')
            return pdf_url, f"acl_{safe_id}.pdf"

    # NeurIPS proceedings
    if "proceedings.neurips.cc" in domain or "papers.nips.cc" in domain:
        # Try to extract paper hash/id
        path = parsed.path
        if "/paper" in path:
            # Extract the paper identifier
            parts = path.split('/')
            for i, part in enumerate(parts):
                if part == "paper" and i + 1 < len(parts):
                    paper_id = parts[-1].replace('.pdf', '')
                    pdf_url = url if url.endswith('.pdf') else f"{url.rstrip('/')}/paper.pdf"
                    safe_id = paper_id[:50]  # Truncate long IDs
                    return pdf_url, f"neurips_{safe_id}.pdf"

    # PMLR (proceedings.mlr.press)
    if "proceedings.mlr.press" in domain:
        path = parsed.path
        # https://proceedings.mlr.press/v162/paper123.html
        match = re.search(r'/v(\d+)/([^/]+)', path)
        if match:
            vol, paper = match.groups()
            paper_id = paper.replace('.html', '')
            pdf_url = f"https://proceedings.mlr.press/v{vol}/{paper_id}/{paper_id}.pdf"
            return pdf_url, f"pmlr_v{vol}_{paper_id}.pdf"

    # Direct PDF link
    if url.endswith('.pdf'):
        # Create filename from URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return url, f"direct_{url_hash}.pdf"

    # Nature
    if "nature.com" in domain:
        # https://www.nature.com/articles/s41586-023-12345-6
        match = re.search(r'/articles/([^/]+)', parsed.path)
        if match:
            article_id = match.group(1)
            pdf_url = f"https://www.nature.com/articles/{article_id}.pdf"
            safe_id = article_id.replace('.', '_').replace('-', '_')
            return pdf_url, f"nature_{safe_id}.pdf"

    # IEEE
    if "ieee.org" in domain or "ieeexplore" in domain:
        match = re.search(r'/document/(\d+)', parsed.path)
        if match:
            doc_id = match.group(1)
            # IEEE requires authentication usually, but try
            pdf_url = f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={doc_id}"
            return pdf_url, f"ieee_{doc_id}.pdf"

    # Semantic Scholar
    if "semanticscholar.org" in domain:
        match = re.search(r'/paper/[^/]+/([a-f0-9]+)', parsed.path)
        if match:
            paper_hash = match.group(1)
            # Semantic Scholar doesn't host PDFs directly, return None
            return None

    # Generic fallback - try adding .pdf
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    return url, f"unknown_{url_hash}.pdf"


def download_pdf(pdf_url: str, filename: str, output_dir: Path) -> bool:
    """Download a PDF from the given URL."""
    output_path = output_dir / filename

    # Skip if already downloaded
    if output_path.exists() and output_path.stat().st_size > 1000:
        print(f"  Already exists: {filename}")
        return True

    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=30, allow_redirects=True)
        response.raise_for_status()

        # Check if we got a PDF
        content_type = response.headers.get("Content-Type", "")
        is_pdf = "pdf" in content_type.lower() or response.content[:5] == b"%PDF-"

        if not is_pdf:
            print(f"  Warning: {filename} - not a PDF (got {content_type[:50]})")
            return False

        # Save the PDF
        output_path.write_bytes(response.content)
        print(f"  Downloaded: {filename} ({len(response.content) / 1024:.1f} KB)")
        return True

    except requests.exceptions.RequestException as e:
        print(f"  Error: {filename} - {e}")
        return False


def main():
    """Main function to orchestrate the download."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")

    # Fetch sheet data
    df = get_sheet_data()

    # Get unique paper links
    paper_links = get_unique_paper_links(df)

    if not paper_links:
        print("No paper links found!")
        return

    # Process and download PDFs
    print(f"\nProcessing {len(paper_links)} unique links...")
    successful = 0
    failed = 0
    skipped = 0

    for i, url in enumerate(paper_links, 1):
        print(f"[{i}/{len(paper_links)}] {url[:80]}...")

        result = get_pdf_url_and_filename(url)
        if result is None:
            print(f"  Skipped: unsupported source")
            skipped += 1
            continue

        pdf_url, filename = result

        if download_pdf(pdf_url, filename, OUTPUT_DIR):
            successful += 1
        else:
            failed += 1

        # Rate limiting
        if i < len(paper_links):
            time.sleep(0.5)

    print(f"\nDone!")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print(f"PDFs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
