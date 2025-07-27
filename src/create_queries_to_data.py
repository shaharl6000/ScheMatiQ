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
from typing import Dict, Iterator, List, Any, Callable
from tqdm import tqdm

from utils import get_generator

# ─────────────────────────────  CONSTANTS  ──────────────────────────────
MAX_NEW_TOKENS    = 200
TEMPERATURE       = 0.9
STOP_SEQUENCE     = "###"             # custom delimiter
MAX_PAPER_CHARS   = 15_000
MIN_COLUMNS_THRESH = 0

EXTRACT_QUERY_INSTRUCTION = (
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

RETRIEVAL_QUERY_INSTRUCTION = """
You are QueryRewriteLLM. Your job is to convert a natural‑language query into retrieval-optimized keyword strings.
TASK
Given ORIGINAL_QUERY, produce 3–5 alternative rewrites that are:
Short (≤12 tokens each), noun-heavy, minimal or no verbs
Focused on key entities, concepts, attributes
Include common synonyms/aliases and section cues (e.g., “method”, “dataset”, “table 1”) when helpful
No punctuation except commas or semicolons, no stopwords (“the”, “of”, “to”, “for”, etc.)
OUTPUT FORMAT (only JSON)
{
  "rewrites": [
    "…",
    "…"
  ]
}
CONSTRAINTS
Do not invent entities not implied by the original query.
Prefer nouns/adjectives; if a verb is unavoidable, keep 1–2 max, infinitive form.
Avoid questions, full sentences, and filler words.

EXAMPLE
ORIGINAL_QUERY: "Compare how having many short documents vs. few long ones affects RAG accuracy with fixed context length"
OUTPUT:
{
  "rewrites": [
    "RAG breadth depth tradeoff context length fixed accuracy",
    "many short documents vs few long documents retrieval performance",
    "context budget allocation documents count passage length RAG metrics",
    "retrieval augmented generation document granularity evaluation",
    "fixed token budget document segmentation impact LLM accuracy"
  ]
}
"""

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

JSON_FENCE = re.compile(r"```json(.*?)```", re.S | re.I)
FIRST_OBJ = re.compile(r"\{.*\}", re.S)

def safe_parse_json(text: str) -> Dict[str, Any]:
    """
    Try to robustly extract the first JSON object from a model response.
    """
    m = JSON_FENCE.search(text)
    if m:
        candidate = m.group(1).strip()
    else:
        m = FIRST_OBJ.search(text)
        candidate = m.group(0).strip() if m else ""

    try:
        return json.loads(candidate)
    except Exception:
        # Last resort: try to repair common mistakes
        candidate = candidate.replace("\n", " ").strip()
        return json.loads(candidate)


def build_messages_extract_query(example: Dict[str, str], current: Dict[str, str]) -> List[Dict[str, str]]:
    """Common chat‑messages format expected by both backends."""
    return [
        {"role": "system", "content": EXTRACT_QUERY_INSTRUCTION},
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

def build_messages_retrieval_query(rec: Dict[str, Any]) -> List[Dict[str, str]]:
    """Chat-completion style messages."""
    original_query = rec["query"]
    return [
        {"role": "system", "content": RETRIEVAL_QUERY_INSTRUCTION},
        {
            "role": "user",
            "content": f"ORIGINAL_QUERY: \"\"\"{original_query}\"\"\"\nReturn only JSON."
        },
    ]


# ───────────────────────────  DRIVER  ────────────────────────────────
def process_file_query_extraction(inp: Path, out: Path, generate) -> None:
    records = list(iter_jsonl(inp))
    if not records:
        raise ValueError("Input JSONL is empty!")

    for rec in records:
        rec["processed_paper_content"] = preprocess_paper_extra(rec["paper_content"])

    example = records[0]  # kept for expansion if you want few‑shot

    with out.open("w", encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Inferring questions"):
            messages = build_messages_extract_query(example, rec)

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
                        "tabid": rec["tabid"],
                        "caption": rec["caption"]
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def process_file_retrieval_query(
    inp: Path,
    out: Path,
    generate,
    max_new_tokens: int = 256,
    temperature: float = 0.3,
    stop: List[str] | None = None,
    resume: bool = True,
) -> None:
    records = list(iter_jsonl(inp))
    if not records:
        raise ValueError("Input JSONL is empty!")

    # ---- 1. Collect already processed tabids (if resume & file exists) ----
    processed_tabids: set[str] = set()
    if resume and out.exists():
        for line in iter_jsonl(out):
            tid = line.get("tabid")
            if tid is not None:
                processed_tabids.add(tid)

    # ---- 2. Open file (append if resuming, else overwrite) -----------------
    mode = "a" if (resume and out.exists()) else "w"
    with out.open(mode, encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Rewriting queries"):
            tid = rec.get("tabid")
            if resume and tid in processed_tabids:
                # Skip work, already done
                continue

            messages = build_messages_retrieval_query(rec)

            raw = generate(
                messages=messages,
                max_tokens=max_new_tokens,
                temperature=temperature,
                stop=stop or [],
            ).strip()

            try:
                parsed = safe_parse_json(raw)
                rewrites = parsed.get("rewrites", [])
                if not isinstance(rewrites, list) or not rewrites:
                    raise ValueError("Missing/empty 'rewrites' list.")
            except Exception as e:
                raise ValueError(
                    f"Failed to parse model output for tabid={tid}: {raw}"
                ) from e

            rec["retrieval_queries"] = rewrites

            f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f_out.flush()  # optional: safer on long runs


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
    # process_file_query_extraction(args.input_jsonl, args.output_jsonl, generate)
    process_file_retrieval_query(args.input_jsonl, args.output_jsonl, generate)
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")


def read_queries():
    pass


if __name__ == "__main__":
    main()
