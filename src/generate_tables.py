from __future__ import annotations

import argparse
from pathlib import Path

import ast
import json
import re
from pathlib import Path
from typing import Dict, List

from datasets import load_dataset
from tqdm import tqdm
from utils import get_generator

# system instruction from ArxivDigest paper
SYSTEM_INSTRUCTION = (
    "You are an intelligent and precise assistant that can understand the contents of research papers. "
    "You are knowledgable on different fields and domains of science, in particular computer science. "
    "You are able to interpret research papers, create questions and answers, and compare multiple papers."
)

# prompt template from ArxivDigest paper with changes of receiving query
PROMPT_TEMPLATE = """[System]
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

#--------------------  Generation parameters -------------------------------
MAX_NEW_TOKENS = 512
TEMPERATURE = 0.3
STOP_SEQUENCE = None # keep None unless you need a custom stop token
NUM_ATTRIBUTES = 3 # default attributes per aspect


# -------------------- Message builder -------------------------------------
def build_messages(
    query: str,
    papers: List[Dict[str, str]],
    num_attributes: int = NUM_ATTRIBUTES,
    ) -> List[Dict[str, str]]:
    """
    Convert (query, papers) ➜ chat-completion message list.
    Each paper dict must contain 'title' and 'abstract' keys.
    """
    paper_blocks = []
    for i, p in enumerate(papers, 1):
        title = p.get("title", "").strip()
    abstract = p.get("abstract", "").strip()
    paper_blocks.append(
    f"[Paper {i}]\n"
    f"Title: {title}\n"
    f"Abstract: {abstract}"
    )
    input_info = "Papers:\n\n" + "\n\n".join(paper_blocks)

    user_prompt = PROMPT_TEMPLATE.format(
        query=query,
        num_attributes=num_attributes,
        input_info=input_info,
    )

    return [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {"role": "user", "content": user_prompt},
    ]

# -------------------- Main processing loop --------------------------------
def process_query_file(
    inp: Path,
    out: Path,
    generate,
    num_attributes: int = NUM_ATTRIBUTES,
    ) -> None:
    """
    Read JSONL with {"tabid", "query"}, build query-centric schemas
    with an LLM, and write results to JSONL out.
    """
    # -- Load the queries ------------------------------------------------------
    with inp.open("r", encoding="utf-8") as f_in:
        records = [json.loads(line) for line in f_in if line.strip()]
    if not records:
        raise ValueError("Input JSONL is empty!")
    # -- Load the arxivDIGESTables validation split once ----------------------
    ds = load_dataset(
        "blnewman/arxivDIGESTables",
        split="validation",
        trust_remote_code=True,
    )
    tabid_to_row = {tid: ds[i] for i, tid in enumerate(ds["tabid"])}

    # -- Iterate and generate --------------------------------------------------
    with out.open("w", encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Inferring aspects"):
            tabid: str = rec["tabid"]
            query: str = rec["query"]

            if tabid not in tabid_to_row:
                print(f"⚠️  tabid {tabid} not found in dataset; skipping.")
                continue

            row = tabid_to_row[tabid]
            papers = row["row_bib_map"]  # list[dict]

            messages = build_messages(query, papers, num_attributes)

            answer = generate(
                messages,
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
    args = parser.parse_args()

    generate = get_generator(args.backend)
    process_query_file(args.input_jsonl, args.output_jsonl, generate)
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")

if __name__ == "__main__":
    main()