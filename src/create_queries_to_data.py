"""
---------------------------------------------------------
Infer the research‑question each table in a paper answers
using **Meta‑Llama‑3‑70B‑Instruct** loaded locally with
4‑bit quantisation (bitsandbytes).

INPUT  : JSONL –{id, paper_content, table}
OUTPUT : JSONL –{id, processed_paper_content, table, query}

Dependencies
------------
pip install "transformers>=4.40.0" accelerate bitsandbytes tqdm

(If you have several GPUs, set `CUDA_VISIBLE_DEVICES`,
otherwise the model will automatically shard.)

Author : you 😉
---------------------------------------------------------
"""

from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterator, List
import os
HF_TOKEN = os.environ.get("HF_TOKEN")
import torch
from tqdm import tqdm
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    BitsAndBytesConfig,
)

# ─────────────────────────────  CONFIG  ──────────────────────────────
MODEL_NAME = "meta-llama/Llama-3.3-70B-Instruct"   # HF hub name
MAX_NEW_TOKENS  = 200
TEMPERATURE     = 0.9
STOP_SEQUENCE   = "###"                              # we append this at end

# 4‑bit nf4 quantisation –fits in ~34GB VRAM
bnb_cfg = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_use_double_quant=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16,
)

print("⏳  Loading tokenizer & model (this can take several minutes)…")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, use_fast=True, token=HF_TOKEN)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    quantization_config=bnb_cfg,
    device_map="auto",
    torch_dtype=torch.float16,
    trust_remote_code=True,   # Llama‑3 uses ChatML template in tokenizer
    token=HF_TOKEN
)
model.eval()

# ──────────────────────  PROMPT COMPONENTS  ──────────────────────────
TASK_INSTRUCTIONS = (
    "You are given the content of a scientific paper and one of its tables. "
    "Your task is to infer the specific research question or motivation that "
    "this table was designed to answer. The question should reflect the "
    "purpose behind including this table in the paper.\n"
    "Important:\n"
    "• Do NOT simply restate or rephrase the column headers.\n"
    "• Instead, infer the underlying question or hypothesis the authors were "
    "investigating through this table.\n"
    "What did they want to understand, compare, or demonstrate?\n"
    "Return only the inferred question, in a clear and concise way."
)

ONE_SHOT_ANSWER = (
    "How do recent NLG systems turn restaurant meaning representations into "
    "natural‑language utterances, and how much stylistic variation do those "
    "outputs exhibit?"
)

# ─────────────────────────  REGEX HELPERS  ───────────────────────────
REF_CUE_RE = re.compile(r"(?im)^(references|bibliography|acknowledg(e)?ments?)\b")


def preprocess_paper(text: str) -> str:
    """Strip trailing references / bibliography / acknowledgments."""
    m = REF_CUE_RE.search(text)
    return text[: m.start()].strip() if m else text.strip()


def iter_jsonl(path: Path) -> Iterator[Dict[str, str]]:
    with path.open(encoding="utf-8") as fh:
        for raw in fh:
            if raw.strip():
                yield json.loads(raw)


def build_prompt(example: Dict[str, str], current: Dict[str, str]) -> str:
    """
    Llama‑3 expects ChatML. We’ll emit a single system + user turn:
    <|begin_of_text|><|start_header_id|>system\n…<|end_header_id|>\n… etc.

    The tokenizer can build this with `apply_chat_template`.
    """
    messages = [
        {"role": "system", "content": TASK_INSTRUCTIONS},
        {
            "role": "user",
            "content": (
                "### Example\n"
                f"Paper Content:\n{example['processed_paper_content']}\n\n"
                f"Table:\n{example['table']}\n\n"
                f"Answer:\n{ONE_SHOT_ANSWER}\n\n"
                "### Task\n"
                f"Paper Content:\n{current['processed_paper_content']}\n\n"
                f"Table:\n{current['table']}\n\n"
                "Answer:"
            ),
        },
    ]
    return tokenizer.apply_chat_template(messages, add_generation_prompt=True)


@torch.inference_mode()
def generate_query(prompt: str) -> str:
    input_ids = tokenizer(prompt, return_tensors="pt").to(model.device)
    output_ids = model.generate(
        **input_ids,
        max_new_tokens=MAX_NEW_TOKENS,
        temperature=TEMPERATURE,
        pad_token_id=tokenizer.eos_token_id,
        eos_token_id=tokenizer.convert_tokens_to_ids(STOP_SEQUENCE),
    )
    generated = tokenizer.decode(output_ids[0][input_ids["input_ids"].shape[-1] :])
    return generated.strip().split(STOP_SEQUENCE)[0].strip()


# ───────────────────────────  MAIN FLOW  ─────────────────────────────
def process_file(inp: Path, out: Path) -> None:
    records: List[Dict[str, str]] = list(iter_jsonl(inp))
    if not records:
        raise ValueError("Input JSONL is empty!")

    # preprocess once
    for rec in records:
        rec["processed_paper_content"] = preprocess_paper(rec["paper_content"])

    example = records[0]  # use first line as one‑shot demonstration

    with out.open("w", encoding="utf-8") as f_out:
        for rec in tqdm(records, desc="Inferring questions"):
            prompt = build_prompt(example, rec)
            rec["query"] = generate_query(prompt)
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


def cli() -> None:
    ap = argparse.ArgumentParser(
        description="Infer table research‑questions with a quantised Llama‑3.3‑70B."
    )
    ap.add_argument("input_jsonl", type=Path, help="input JSONL")
    ap.add_argument(
        "-o",
        "--output_jsonl",
        type=Path,
        default=Path("queries.jsonl"),
        help="output JSONL (default: queries.jsonl)",
    )
    args = ap.parse_args()
    process_file(args.input_jsonl, args.output_jsonl)
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")


if __name__ == "__main__":
    cli()