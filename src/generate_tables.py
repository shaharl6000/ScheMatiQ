from __future__ import annotations

import argparse
from pathlib import Path
import tiktoken

import ast
import json
import re
from pathlib import Path
from typing import Dict, List
from datasets import load_dataset
from tqdm import tqdm
import time
from utils import get_generator

# system instruction from ArxivDigest paper
SYSTEM_INSTRUCTION = (
    "You are an intelligent and precise assistant that can understand the contents of research papers. "
    "You are knowledgable on different fields and domains of science, in particular computer science. "
    "You are able to interpret research papers, create questions and answers, and compare multiple papers."
)

# prompt template from ArxivDigest paper with changes of receiving query
PROMPT_TEMPLATE_BASELINE = """[System]
Imagine the following scenario: A user is making a table for a scholarly paper that aims to answer the research question:

    "{query}"

To compare and contrast relevant works, the user provides the title and content of each paper.
  
Your task is the following: **Given the list of papers *and* the query**, find aspects that are shared by the given research papers and are pertinent to answering the query.  
Within each aspect, identify attributes that can be used to compare the given papers **with respect to the query**.

First, return the list of similar aspects as a Python list, for example:  
    ["<similar aspect that all given papers shared>", ...]  

Then, think of each aspect as a topic for the Related Work section of the user's paper.  
Finally, list attributes that compare the given papers for that aspect.  
Return a JSON object in the following format:

```json
{{
  "<aspect 1>": ["<comparable attribute within aspect 1>",
                 "<comparable attribute within aspect 1>",
                 ...],
  ...
}}
"""

PROMPT_TEMPLATE_CAPTION =  """ [System] 
Imagine the following scenario: A user is making a table for a scholarly paper that aims to answer the research question:

    "{query}"
    
To compare and contrast the papers, the user provides the title and content of each paper. 
To help you build the table, the user provides a caption of this table, which is referred to in the paper as additional information.
Your task is the following: **Given the list of papers *and* the query**, find aspects that are shared by the given research papers and are pertinent to answering the query.  
Within each aspect, identify attributes that can be used to compare the given papers **with respect to the query**.

[Caption] {caption} 

Your task is the following: **Given the list of papers *and* the query**, find aspects that are shared by the given research papers and are pertinent to answering the query.  
Within each aspect, identify attributes that can be used to compare the given papers **with respect to the query**.

First, return the list of similar aspects as a Python list, for example:  
    ["<similar aspect that all given papers shared>", ...]  

Then, think of each aspect as a topic for the Related Work section of the user's paper.  
Finally, list attributes that compare the given papers for that aspect.  
Return a JSON object in the following format:

```json
{{
  "<aspect 1>": ["<comparable attribute within aspect 1>",
                 "<comparable attribute within aspect 1>",
                 ...],
  ...
}}
"""


prompt_template = PROMPT_TEMPLATE_CAPTION

#--------------------  Generation parameters -------------------------------
MAX_NEW_TOKENS = 512
TEMPERATURE = 0.3
STOP_SEQUENCE = None # keep None unless you need a custom stop token
NUM_ATTRIBUTES = 6 # default attributes per aspect

ENC = tiktoken.encoding_for_model("gpt-3.5-turbo")  # close enough

MAX_CTX_TOKENS = 8192
SAFETY_MARGIN   = 32
DEFAULT_SENTENCE_LEVELS = (11, 9, 7, 5, 3, 1)

def fit_prompt(
    messages: list[dict[str, str]],
    max_new: int = MAX_NEW_TOKENS,
    sentence_levels: tuple[int, ...] = DEFAULT_SENTENCE_LEVELS,
) -> list[dict[str, str]]:
    """
    Ensure (prompt_tokens + max_new) <= MAX_CTX_TOKENS – SAFETY_MARGIN.
    * If already true, returns unchanged (no trimming).
    * Otherwise, progressively shorten abstracts to the specified
      sentence caps, then drop full papers if still too long.
    """
    def n_tokens(s: str) -> int:
        return len(ENC.encode(s))

    def shorten(block: str, cap: int) -> str:
        title, abstract = block.split("Abstract:", 1)
        sentences = re.split(r"(?<=[.!?])\s+", abstract.strip())
        short_abs = " ".join(sentences[:cap])
        return f"{title}Abstract: {short_abs}"

    user_msg = messages[1]["content"]
    header, _, papers_blob = user_msg.partition("\n\nPapers:\n\n")
    blocks = papers_blob.split("\n\n") if papers_blob else []

    full_len = n_tokens(user_msg) + max_new
    if full_len <= MAX_CTX_TOKENS - SAFETY_MARGIN:
        # fits as-is – done ✅
        return messages

    # 1) shorten abstracts ---------------------------------------------------
    for cap in sentence_levels:
        new_blocks = [shorten(b, cap) for b in blocks]
        short_prompt = header + "\n\nPapers:\n\n" + "\n\n".join(new_blocks)
        if n_tokens(short_prompt) + max_new <= MAX_CTX_TOKENS - SAFETY_MARGIN:
            print(f"✂️  Trimmed abstracts to ≤ {cap} sentences each.")
            messages[1]["content"] = short_prompt
            return messages

    # 2) drop papers from the end -------------------------------------------
    while blocks:
        blocks.pop()  # remove last paper
        short_prompt = header + "\n\nPapers:\n\n" + "\n\n".join(blocks)
        if n_tokens(short_prompt) + max_new <= MAX_CTX_TOKENS - SAFETY_MARGIN:
            print(f"📄  Dropped papers – {len(blocks)} remain.")
            messages[1]["content"] = short_prompt
            return messages

    # 3) Fallback: all papers gone; keep header only -------------------------
    print("⚠️  All papers trimmed; only the header remains.")
    messages[1]["content"] = header
    return messages

# -------------------- Message builder -------------------------------------



def build_messages(
    query: str,
    papers: List[Dict[str, str]],
    caption: str,
    num_attributes: int = NUM_ATTRIBUTES,
) -> List[Dict[str, str]]:
    """
    Convert (query, papers, caption) ➜ chat-completion messages.
    Each paper dict must contain at least 'title' and 'abstract'.
    """
    # --- build the paper list once ------------------------------------------
    paper_blocks = [
        (
            f"[Paper {i}]\n"
            f"Title: {p.get('title', '').strip()}\n"
            f"Abstract: {p.get('abstract', '').strip()}"
        )
        for i, p in enumerate(papers, start=1)
    ]
    papers_section = "\n\n".join(paper_blocks)

    # --- render the prompt ---------------------------------------------------
    user_prompt = prompt_template.format(
        query=query,
        caption=caption,          # <- now supplied
        num_attributes=num_attributes,
    ) + "\n\nPapers:\n\n" + papers_section

    return [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {"role": "user",   "content": user_prompt},
    ]

# -------------------- Main processing loop --------------------------------
def process_query_file(
    inp: Path,
    out: Path,
    generate,
    num_attributes: int = NUM_ATTRIBUTES,
    resume: bool = False,            # ← new flag
) -> None:
    """
    Read JSONL with {"tabid", "query", "caption"}, build schemas with an LLM,
    and append results to JSONL out.  If resume=True and out already exists,
    tabids that are already present are skipped.
    """
    # -- Load the queries ----------------------------------------------------
    with inp.open("r", encoding="utf-8") as f_in:
        records = [json.loads(l) for l in f_in if l.strip()]
    if not records:
        raise ValueError("Input JSONL is empty!")

    # -- If resuming, read existing tabids -----------------------------------
    done_tabids: set[str] = set()
    if resume and out.exists():
        with out.open("r", encoding="utf-8") as f_out:
            done_tabids = {json.loads(l)["tabid"] for l in f_out if l.strip()}
        print(f"🔄  Resume enabled – {len(done_tabids)} tabids already processed.")

    # -- Load the validation split once --------------------------------------
    ds = load_dataset("blnewman/arxivDIGESTables",
                      split="validation",
                      trust_remote_code=True)
    tabid_to_row = {tid: ds[i] for i, tid in enumerate(ds["tabid"])}

    # -- Iterate and generate -------------------------------------------------
    # open in *append* mode so we keep previous runs intact
    mode = "a" if resume else "w"
    with out.open(mode, encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Inferring aspects"):
            tabid, query, caption = rec["tabid"], rec["query"], rec["caption"]

            if tabid in done_tabids:
                continue  # already done ✔︎

            if tabid not in tabid_to_row:
                print(f"⚠️  tabid {tabid} not found in dataset; skipping.")
                continue

            papers = tabid_to_row[tabid]["row_bib_map"]

            messages = build_messages(query, papers, caption, num_attributes)

            time.sleep(5.1)
            trimmed_messages = fit_prompt(messages, MAX_NEW_TOKENS)

            answer = generate(
                trimmed_messages,
                max_tokens=MAX_NEW_TOKENS,
                temperature=TEMPERATURE,
                stop=[STOP_SEQUENCE] if STOP_SEQUENCE else None,
            ).strip()

            # -- Parse model output -------------------------------------------
            try:
                aspects_match = re.search(r"\[.*?\]", answer, re.DOTALL)
                json_match = re.search(r"\{.*}", answer, re.DOTALL)

                aspects = (
                    ast.literal_eval(aspects_match.group(0))
                    if aspects_match else []
                )
                schema = (
                    json.loads(json_match.group(0))
                    if json_match else {}
                )
            except Exception as exc:
                print(f"⚠️  Could not parse output for tabid {tabid}: {exc}")
                continue

            # -- Write one JSONL line -----------------------------------------
            f_out.write(
                json.dumps(
                    {
                        "tabid": tabid,
                        "caption": caption,
                        "query": query,
                        "aspects": aspects,
                        "schema": schema,
                        "raw_answer": answer,  # keep for reproducibility
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Infer table from research‑questions and papers."
    )
    parser.add_argument(
        "-o", "--output_jsonl",
        type=Path,
        default=Path("queries.jsonl"),
        help="output JSONL (default: queries.jsonl)",
    )

    parser.add_argument(
        "-i", "--input_jsonl",
        type=Path,
        required=True,
        help="path to input tabid and queries",
    )

    parser.add_argument(
        "--backend",
        choices=["hf", "together"],
        default="together",
        help="Which backend to use: 'hf' (local) or 'together' (hosted).",
    )
    parser.add_argument(
        "--resume", dest="resume_out", action="store_true",
        help="If set, append to the existing output JSONL and skip tabids "
             "that have already been processed."
    )

    args = parser.parse_args()

    generate = get_generator(args.backend)
    process_query_file(
        args.input_jsonl,
        args.output_jsonl,
        generate,
        resume=args.resume_out,
    )
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")

if __name__ == "__main__":
    main()