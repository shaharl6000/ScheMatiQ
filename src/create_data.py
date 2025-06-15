import PyPDF2
import json
import os
import re
import wget
from tqdm import tqdm
from datasets import load_dataset
import arxiv
import io
import requests
from PyPDF2 import PdfReader
from collections import Counter
import io


MIN_COLUMNS_THRESH = 4


def arxivDIGESTables_data():
    tables = load_dataset("blnewman/arxivDIGESTables", split="validation",
                          trust_remote_code=True)
    arxiv_ids = tables["arxiv_id"]
    tables_txt = tables["table"]
    tabids = tables["tabid"]
    captions = tables["caption"]

    lengths = []
    for raw in tables_txt:
        try:
            obj = json.loads(raw)  # parse the JSON string
            lengths.append(len(obj))  # number of top‑level keys
        except (json.JSONDecodeError, TypeError):
            # handle bad or non‑JSON items gracefully
            continue

    hist = Counter(lengths)  # build the histogram

    print("Histogram of top‑level key counts:")
    for n_keys, freq in sorted(hist.items()):
        print(f"{n_keys:>2} keys : ({freq})")

    out_path = f"arxiv_tables_filtered_{MIN_COLUMNS_THRESH}_columns.jsonl"
    client = arxiv.Client()

    count_skipped = 0

    with open(out_path, "w", encoding="utf-8") as fout:
        for arxiv_id, tabid, table, caption in tqdm(zip(arxiv_ids, tabids, tables_txt, captions),
                                    total=len(arxiv_ids)):
            try:
                if len(json.loads(table)) < MIN_COLUMNS_THRESH:
                    count_skipped += 1
                    if count_skipped % 10 == 0:
                        print(f"skipped {count_skipped} tables")
                    continue


                # 1) look up the record and get the PDF URL
                record = next(client.results(arxiv.Search(id_list=[arxiv_id])))
                pdf_url = record.pdf_url

                # 2) stream the PDF into memory
                pdf_bytes = requests.get(pdf_url, timeout=30).content
                pdf_buffer = io.BytesIO(pdf_bytes)

                # 3) extract plain text
                reader = PdfReader(pdf_buffer)
                pages_text = [page.extract_text() or "" for page in reader.pages]
                paper_text = "\n".join(pages_text)

                # 4) write a single JSON object per line
                fout.write(json.dumps(
                    {
                        "id": arxiv_id,
                        "paper_content": paper_text,
                        "tabid" : tabid,
                        "table": table,
                        "caption": caption
                    },
                    ensure_ascii=False  # keep Unicode chars readable
                ) + "\n")

            except Exception as e:  # network error, bad PDF, ...
                print(f"[{arxiv_id}] skipped ({e})")
                continue

    print(f"✓ Saved {out_path}")


def get_queries_from_arxivDIGESTables(path_json):
    Prompt = "    You are given the content of a scientific paper and one of its tables. Your task is to infer the specific research question or motivation that this table was designed to answer. The question should reflect the purpose behind including this table in the paper." \
             "\nImportant: \n " \
             "Do not simply restate or rephrase the column headers. Instead, infer the underlying question or hypothesis the authors were investigating through this table. " \
             "\nWhat did they want to understand, compare, or demonstrate? " \
             "\n Return only the inferred question in a clear and concise way."

    questions = ["How do recent NLG systems turn restaurant meaning representations into natural‑language utterances, and how much stylistic variation do those outputs exhibit?",
                 "Which pre‑trained CNN architecture provides the most suitable trade‑off between model size (trainable parameters) and feature dimensionality for effective anomaly detection in surveillance videos?"]



    # client = arxiv.Client()
    #
    # search = arxiv.Search(
    #   id_list=original_papers_arxiv_id_val[:3]
    # )
    # results = client.results(search)
    #
    # all_results = list(results)
    # print([r.title for r in all_results])



# # read jsonl
# data = []
# with open(r'C:\Users\shaharl\Desktop\shahar\Uni\Information_Extraction\dev_10_1.jsonl', 'r', encoding='utf-8') as f:
#     for line in f:
#         cur = json.loads(line)
#         data.append(cur)
#
# # Now `data` is a list of Python dictionaries
# print(data)

def text_from_url_s2orc(url):
    # modify these
    API_KEY = "..." #TODO: waiting for approval or the api key
    DATASET_NAME = "s2orc"
    LOCAL_PATH = "/my/local/path/for/s2orc/"
    os.makedirs(LOCAL_PATH, exist_ok=True)

    # get latest release's ID
    response = requests.get(url).json()
    RELEASE_ID = response["release_id"]
    print(f"Latest release ID: {RELEASE_ID}")

    # get the download links for the s2orc dataset; needs to pass API key through `x-api-key` header
    # download via wget. this can take a while...
    response = requests.get(f"https://api.semanticscholar.org/datasets/v1/release/{RELEASE_ID}/dataset/{DATASET_NAME}/", headers={"x-api-key": API_KEY}).json()
    for url in tqdm(response["files"]):
        match = re.match(r"https://ai2-s2ag.s3.amazonaws.com/staging/(.*)/s2orc/(.*).gz(.*)", url)
        assert match.group(1) == RELEASE_ID
        SHARD_ID = match.group(2)
        wget.download(url, out=os.path.join(LOCAL_PATH, f"{SHARD_ID}.gz"))
    print("Downloaded all shards.")


def text_from_pdf(pdf_path, output_txt_path=""):
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)

        # Number of pages
        num_pages = len(pdf_reader.pages)

        text = ""
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()  # Extract text from each page

    # Save text to a .txt file
    with open(output_txt_path, "w", encoding="utf-8") as txt_file:
        txt_file.write(text)

    print(f"Text extracted and saved to {output_txt_path}")
    return text


def pdf_to_json(pdf_path, output_txt_path):

    caption = ("A sample page from NESdb that shows 7 of the 14 illustrated features of the NES "
               "from snurportin 1 (SNUPN). The features not shown include full name, alternative names, "
               "organism, three-dimensional structures, comments, references, and a user input form.")

    with open('../data/NESTable.jsonl', 'r', encoding='utf-8') as f:
        table = f.read().strip()

    paper_text = text_from_pdf(pdf_path)

    # Save text to a .txt file
    with open(output_txt_path, "w", encoding="utf-8") as fout:
        fout.write(json.dumps(
            {
                "id": "NES",
                "paper_content": paper_text,
                "table": table,
                "caption": caption
            },
            ensure_ascii=False  # keep Unicode chars readable
        ) + "\n")


    print(f"Text extracted and saved to {output_txt_path}")


if __name__ == "__main__":
    print("start")

    pdf_path = r"C:\Users\shaharl\Desktop\shahar\Uni\Information_Extraction\QueryDiscovery\data\NES_DATA" \
               r"\pdf\Structural_prerequisites_for_CRM1.pdf"
    output_txt_path = r"C:\Users\shaharl\Desktop\shahar\Uni\Information_Extraction\QueryDiscovery\data\NES_DATA" \
                      r"\text\Structural_prerequisites_for_CRM1.txt"
    text_from_pdf(pdf_path, output_txt_path)

    # arxivDIGESTables_data()

    # pdf_to_json(r"../data/NES_paper.pdf", "NESdb.txt")

