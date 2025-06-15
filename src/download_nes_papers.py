import os
import re
import asyncio
import aiofiles
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import async_playwright
import difflib


BASE_URL = "http://prodata.swmed.edu/LRNes/IndexFiles/"
MAIN_PAGE = "http://prodata.swmed.edu/LRNes/IndexFiles/namesGood.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
OUTPUT_DIR_ABS = "abstracts"
OUTPUT_DIR_FULL = "full_text"
os.makedirs(OUTPUT_DIR_ABS, exist_ok=True)
os.makedirs(OUTPUT_DIR_FULL, exist_ok=True)


async def get_html_with_playwright(url, page):
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(10000)
    html = await page.content()
    final_url = page.url
    return html, final_url


def extract_journal_text(html, abstract_text=None):
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
        """Return start index if a close-enough match of `pattern` in `text` is found."""
        pattern_len = len(pattern)
        best_ratio = 0
        best_start = -1
        for i in range(0, len(text) - pattern_len):
            window = text[i:i + pattern_len + 10]  # allow a bit more room
            ratio = difflib.SequenceMatcher(None, pattern.lower(), window.lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_start = i
        return best_start

    start_idx = fuzzy_find(full_text, clean_abs)
    if start_idx == -1:
        print("⚠️ Could not locate abstract in full text — starting from top")
        start_idx = 0

    full_text = full_text[start_idx:]
    end_idx = None
    for marker in ["References", "REFERENCES", "Bibliography"]:
        idx = full_text.find(marker)
        if idx != -1:
            end_idx = idx
            break
    text = full_text[:end_idx].strip() if end_idx else full_text.strip()

    return text


async def try_save_full_text(url, protein_name, pubmed_id, abstract, page):
    try:
        print(f"Trying to access full text: {url}")
        html, final_url = await get_html_with_playwright(url, page)

        if any(term in html.lower() for term in ["verifying you are human", "enable javascript"]):
            print("⚠️ Cloudflare challenge detected")
            return False

        if ".pdf" in final_url.lower():
            r = requests.get(final_url, headers=HEADERS, timeout=15)
            if r.ok:
                filepath = os.path.join(OUTPUT_DIR_FULL, f"{protein_name}_{pubmed_id}_full.pdf")
                with open(filepath, "wb") as f:
                    f.write(r.content)
                print(f"✔️ Saved PDF: {filepath}")
                return True

        text = extract_journal_text(html, abstract)
        if text:
            filepath = os.path.join(OUTPUT_DIR_FULL, f"{protein_name}_{pubmed_id}_full.txt")
            async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
                await f.write(text)
            print(f"✔️ Saved HTML: {filepath}")
            return True
        return False

    except Exception as e:
        print(f"❌ Failed to download full text from {url}: {e}")
        return False


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Headless -> False to avoid detection
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
            extra_http_headers=HEADERS
        )
        page = await context.new_page()

        soup = BeautifulSoup(requests.get(MAIN_PAGE).text, "html.parser")
        protein_links = [(a.text.strip().replace("/", "_"), urljoin(BASE_URL, a.get("href")))
                         for a in soup.select("td:nth-of-type(1) a") if a.get("href") and a.text.strip()]

        for protein_name, protein_url in tqdm_asyncio(protein_links, desc="Processing proteins"):
            try:
                protein_html = requests.get(protein_url, headers=HEADERS).text
                soup = BeautifulSoup(protein_html, "html.parser")
                pubmed_links = [a.get("href") for a in soup.find_all("a") if "PubMed" in a.text and "ncbi.nlm.nih.gov" in a.get("href", "")]

                for pubmed_url in pubmed_links:
                    pubmed_id = pubmed_url.strip("/").split("=")[-1]
                    abs_html = requests.get(pubmed_url, headers=HEADERS).text
                    abstract_soup = BeautifulSoup(abs_html, "html.parser")
                    abs_div = abstract_soup.find("div", class_="abstract") or abstract_soup.find("div", id="abstract")
                    abstract = abs_div.get_text("\n", strip=True) if abs_div else None

                    if abstract:
                        abs_path = os.path.join(OUTPUT_DIR_ABS, f"{protein_name}_{pubmed_id}_abstract.txt")
                        async with aiofiles.open(abs_path, "w", encoding="utf-8") as f:
                            await f.write(abstract)

                    soup = BeautifulSoup(abs_html, "html.parser")
                    full_text_url = None
                    link_div = soup.find("div", class_="full-text-links-list")
                    if link_div:
                        link = link_div.find("a", href=True)
                        full_text_url = link["href"] if link else None

                    if full_text_url:
                        await try_save_full_text(full_text_url, protein_name, pubmed_id, abstract, page)

            except Exception as e:
                print(f"❌ Error processing protein: {protein_url}: {e}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
