"""Browser fallback for the papers listed in <pdf_dir>/failed.json.

Opens each paper's DOI page in a real Chromium window (Playwright), dismisses
cookie banners, finds the PDF link (citation_pdf_url meta tag, the publisher
URL pattern from download_nes_papers.py, or a PDF link on the page) and fetches
it with the browser's cookies. Only real PDFs are saved; pages behind a CAPTCHA
or paywall are left in failed.json.

Usage (from the repo root):
    .venv\\Scripts\\python.exe backend\\scripts\\download_pdfs_browser.py <pdf_dir> [--headless]
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "schematiq-lib" / "schematiq" / "scripts"))
import download_nes_papers as nes  # noqa: E402

PDF_LINK_SELECTORS = [
    'a[href$=".pdf"]',
    'a[href*="/pdf/"]',
    'a[href*="pdfft"]',
    'a[href*="article-pdf"]',
    'a:has-text("Download PDF")',
    'a:has-text("PDF")',
]


async def candidate_pdf_urls(page, publisher: str) -> list[str]:
    urls = []
    meta = await page.locator('meta[name="citation_pdf_url"]').first.get_attribute("content", timeout=2000) \
        if await page.locator('meta[name="citation_pdf_url"]').count() else None
    if meta:
        urls.append(meta)
    pub_url = nes.get_publisher_pdf_url(page.url, publisher) if publisher != "unknown" else None
    if pub_url:
        urls.append(pub_url)
    for sel in PDF_LINK_SELECTORS:
        loc = page.locator(sel)
        for i in range(min(await loc.count(), 3)):
            href = await loc.nth(i).get_attribute("href")
            if href and not href.startswith(("javascript:", "#", "mailto:")):
                urls.append(urljoin(page.url, href))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


async def try_paper(page, info: dict, dest: Path) -> str:
    doi = info.get("doi")
    url = info.get("full_text_url") or (f"https://doi.org/{doi}" if doi else None)
    if not url:
        return "no DOI"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except Exception as e:
        return f"navigation failed: {e.__class__.__name__}"
    await page.wait_for_timeout(4000)
    await nes.dismiss_cookie_popup(page)
    if nes._is_captcha((await page.content()).lower()):
        return "captcha"

    publisher = nes.get_publisher_from_url(page.url)
    if publisher == "unknown" and doi:
        publisher = nes.get_publisher_from_doi(doi)

    for pdf_url in await candidate_pdf_urls(page, publisher):
        try:
            resp = await page.context.request.get(pdf_url, headers={"Referer": page.url}, timeout=60000)
            body = await resp.body()
        except Exception:
            continue
        if resp.ok and body[:4] == b"%PDF" and len(body) > 1000:
            dest.write_bytes(body)
            return "ok"
    return "no accessible PDF (paywall?)"


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pdf_dir")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    pdf_dir = Path(args.pdf_dir)
    report_path = pdf_dir / "failed.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    failed = report["failed"]
    print(f"{len(failed)} papers to try in the browser")

    still_failed = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=args.headless)
        context = await browser.new_context(user_agent=nes.HEADERS["User-Agent"], locale="en-US", accept_downloads=True)
        page = await context.new_page()
        for n, (pmid, info) in enumerate(failed.items(), 1):
            dest = pdf_dir / f"{info['protein']}_{pmid}_full.pdf"
            result = "ok" if dest.exists() else await try_paper(page, info, dest)
            if result != "ok":
                still_failed[pmid] = {**info, "browser_result": result}
            print(f"  {n}/{len(failed)} {pmid}: {result}")
            await page.wait_for_timeout(2000)
        await browser.close()

    report["failed"] = still_failed
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Done: {len(failed) - len(still_failed)} PDFs via browser, {len(still_failed)} still missing")


if __name__ == "__main__":
    asyncio.run(main())
