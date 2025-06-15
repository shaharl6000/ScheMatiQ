#!/usr/bin/env python
# -*- coding: utf‑8 -*-
"""
---------------------------------------------------------
Infer the research‑question each table in a paper answers
with **Meta‑Llama‑3‑70B‑Instruct** via either
  • a local 4‑bit‑quantised HF model,  *or*
  • Together AI’s hosted endpoint.

INPUT  : JSONL  {id, paper_content, table}
OUTPUT : JSONL  {id, processed_paper_content, table, query}

USAGE
-----
python infer_questions.py  papers.jsonl  -o queries.jsonl        # default HF
python infer_questions.py  papers.jsonl  --backend together      # Together AI
export TOGETHER_API_KEY=... ; pip install together               # for Together
export HF_TOKEN=... ; pip install "transformers>=4.40" accelerate bitsandbytes
---------------------------------------------------------
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterator, List
from tqdm import tqdm

from utils import get_generator

# ─────────────────────────────  CONSTANTS  ──────────────────────────────
MAX_NEW_TOKENS    = 200
TEMPERATURE       = 0.9
STOP_SEQUENCE     = "###"             # custom delimiter
MAX_PAPER_CHARS   = 15_000
MIN_COLUMNS_THRESH = 0

TASK_INSTRUCTIONS = (
    "You are given the content of a scientific paper, the caption of one of its tables, and the table itself. "
    "Your task is to infer the specific research question or motivation that this table was designed to answer. "
    "The question should reflect the purpose behind including this table in the paper.\n\n"
    "Important:\n"
    "• Do NOT simply restate or re‑phrase the column headers — you CANNOT use the headers at all in the question.\n"
    "• Instead, infer the underlying question or hypothesis the authors were investigating through this table. "
    "What did they want to understand, compare, or demonstrate?\n"
    "• If the table is descriptive (e.g., about datasets or models), think carefully about why these specific columns "
    "were included. What are they aiming to highlight or explain by presenting this set of attributes?\n\n"
    "Output format:\n"
    "• If you succeed, return *only* the inferred question (one line).\n"
    "• If you cannot find any valid research question that clearly fits and was induced by the paper and table, "
    "output: \"NO_QUERY\" (exactly, without quotes or extra text)."
)

# ─────────────────────────── REGEX HELPERS ─────────────────────────────
REF_CUE_RE       = re.compile(r"(?im)^(references|bibliography|acknowledg(e)?ments?)\b")
RESULT_LIKE_RE   = re.compile(r"(?im)^(results?|experiments?|evaluation|findings|discussion|conclusions?)\b")
INTRO_RE         = re.compile(r"(?im)^(1[.)]?\s+)?introduction\b")

# ───────────────────────────  UTILS  ──────────────────────────────────
def preprocess_paper_extra(text: str) -> str:
    """Clean the paper and truncate."""
    # 1️⃣ strip references
    ref = REF_CUE_RE.search(text)
    body = text[: ref.start()].strip() if ref else text.strip()

    # 2️⃣ grab abstract
    abs_m = re.search(r"(?is)^abstract\b(.+?)(?=^\s*\w)", body, re.M)
    abstract = abs_m.group(0).strip() if abs_m else ""

    # 3️⃣ pick a main informative section
    main_section = ""
    for pat in (RESULT_LIKE_RE, INTRO_RE):
        m = pat.search(body)
        if m:
            main_section = body[m.start():]
            break
    if not main_section:
        main_section = body  # fallback

    cleaned = f"{abstract}\n\n{main_section}".strip()
    return cleaned[:MAX_PAPER_CHARS]


def iter_jsonl(path: Path) -> Iterator[Dict[str, str]]:
    with path.open(encoding="utf-8") as fh:
        for raw in fh:
            if raw.strip():
                yield json.loads(raw)


def build_messages(example: Dict[str, str], current: Dict[str, str]) -> List[Dict[str, str]]:
    """Common chat‑messages format expected by both backends."""
    return [
        {"role": "system", "content": TASK_INSTRUCTIONS},
        {
            "role": "user",
            "content":
                f"### Task\n"
                f"Paper Content:\n{current['processed_paper_content']}\n\n"
                f"Caption:\n{current['caption']}\n\n"
                f"Table:\n{current['table']}\n\n"
                "Answer:"
        },
    ]


# ───────────────────────────  DRIVER  ────────────────────────────────
def process_file(inp: Path, out: Path, generate) -> None:
    records = list(iter_jsonl(inp))
    if not records:
        raise ValueError("Input JSONL is empty!")

    for rec in records:
        rec["processed_paper_content"] = preprocess_paper_extra(rec["paper_content"])

    example = records[0]  # kept for expansion if you want few‑shot

    with out.open("w", encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Inferring questions"):
            messages = build_messages(example, rec)

            # Ask the model
            answer = generate(
                messages,
                max_tokens=MAX_NEW_TOKENS,
                temperature=TEMPERATURE,
                stop=[STOP_SEQUENCE],
            ).strip()

            # If the model signals failure, just log it and keep going
            if "NO_QUERY" in answer:
                print(f"NO_QUERY for id {rec['id']}")
                continue  # ⇢ skip writing this record to the JSONL

            # Otherwise keep the normal path
            rec["query"] = answer
            print(rec["query"])

            f_out.write(
                json.dumps(
                    {
                        "id": rec["id"],
                        "processed_paper_content": rec["processed_paper_content"],
                        "table": rec["table"],
                        "query": rec["query"],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Infer table research‑questions with Llama‑3‑70B‑Instruct."
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
        help="path to input JSONL",
    )

    parser.add_argument(
        "--backend",
        choices=["hf", "together"],
        default="together",
        help="Which backend to use: 'hf' (local) or 'together' (hosted).",
    )
    args = parser.parse_args()

    generate = get_generator(args.backend)
    process_file(args.input_jsonl, args.output_jsonl, generate)
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")


def read_queries():
    pass


if __name__ == "__main__":
    main()
