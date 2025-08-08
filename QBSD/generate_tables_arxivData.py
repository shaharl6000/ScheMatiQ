from __future__ import annotations

import arxiv
import argparse
import random
import ast
import json
import re
from pathlib import Path
from typing import Dict, List, Any
from datasets import load_dataset
from huggingface_hub import snapshot_download
from tqdm import tqdm
import time
import utils
import QBSD

# system instruction from ArxivDigest paper
SYSTEM_INSTRUCTION = (
    "You are an intelligent and precise assistant that can understand the contents of research papers. "
    "You are knowledgable on different fields and domains of science, in particular computer science. "
    "You are able to interpret research papers, create questions and answers, and compare multiple papers."
)

# ---------------------------------------------------------------------------
# 1) Prompt templates (generic, with an optional {query_block} placeholder)
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE_BASELINE = """[System]
Imagine the following scenario: A user is making a table for a scholarly paper{query_block}

To compare and contrast relevant works, the user provides the title and content of each paper.

Your task is the following: **Given the list of papers{query_suffix}**, find aspects that are shared by the given research papers{pertinent}.
Within each aspect, identify attributes that can be used to compare the given papers{respect}.

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

PROMPT_TEMPLATE_CAPTION = """[System]
Imagine the following scenario: A user is making a table for a scholarly paper{query_block}

To compare and contrast the papers, the user provides the title and content of each paper.  
To help you build the table, the user also provides a caption referring to it as additional information.

[Caption] {caption}

Your task is the following: **Given the list of papers{query_suffix}**, find aspects that are shared by the given research papers{pertinent}.
Within each aspect, identify attributes that can be used to compare the given papers{respect}.

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

PROMPT_TEMPLATE_ICL = """[System]
Imagine the following scenario: A user is making a table for a scholarly paper{query_block}

To compare and contrast the papers, the user provides the title and content of each paper.  
To help you build the table, the user provides similar tables that you can refer to as follows: 

{icl_tables_block}

Your task is the following: **Given the list of papers{query_suffix}**, find aspects that are shared by the given research papers{pertinent}.
Within each aspect, identify attributes that can be used to compare the given papers{respect}.

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

# ---------------------------------------------------------------------------
# 2) Build all four prompt variants in one place
# ---------------------------------------------------------------------------

PROMPT_VARIANTS = [
    # name                         template                 use_caption  include_query  use_icl     use_retrieval
    ("baseline_query",              PROMPT_TEMPLATE_BASELINE, False,       True,          False,          False),
    ("baseline_query_retrieval",    PROMPT_TEMPLATE_BASELINE, False,       True,          False,          True),
    ("baseline_noquery",            PROMPT_TEMPLATE_BASELINE, False,       False,         False,          False),
    ("caption_query",               PROMPT_TEMPLATE_CAPTION,  True,        True,          False,          False),
    ("caption_query_retrieval",     PROMPT_TEMPLATE_CAPTION,  True,        True,          False,          True),
    ("caption_noquery",             PROMPT_TEMPLATE_CAPTION,  True,        False,         False,          False),
    ("icl_query",                   PROMPT_TEMPLATE_ICL,      False,       True,          True ,          False),
    ("icl_query_retrieval",         PROMPT_TEMPLATE_ICL,      False,       True,          True ,          True),
    ("icl_noquery",                 PROMPT_TEMPLATE_ICL,      False,       False,         True ,          False),
    ("QBSD",                        "",                       False,       True,          False,          True),
]


#--------------------  Generation parameters -------------------------------
MAX_NEW_TOKENS = 512
TEMPERATURE = 0.3
STOP_SEQUENCE = None # keep None unless you need a custom stop token
NUM_ATTRIBUTES = 6 # default attributes per aspect


JSON_FENCE = re.compile(r"```json(.*?)```", re.S)  # fenced block
FIRST_JS = re.compile(r"\{[^{}]*\}(?=\s*$)", re.S)  # { … } at end of text
LIST_RE = re.compile(r"\[[^\[\]]*\]", re.S)

def parse_llm_output(text: str) -> tuple[list, dict]:
    """
    Extract (<aspect-list>, <schema-dict>) from the raw model answer.
    Raises ValueError if neither structure is found.
    """
    # ---------- aspect list -------------------------------------------------
    m = LIST_RE.search(text)
    aspects = ast.literal_eval(m.group(0)) if m else []

    # ---------- JSON object -------------------------------------------------
    # 1) prefer a ```json code fence
    m = JSON_FENCE.search(text)
    if m:
        candidate = m.group(1).strip()
    else:
        # 2) otherwise grab the *last* balanced {...} block
        m = FIRST_JS.search(text)
        if not m:
            raise ValueError("No JSON object found.")
        candidate = m.group(0)

    try:
        schema = json.loads(candidate)
    except json.JSONDecodeError as exc:
        # final fallback: try to load until the first '}\n'
        candidate = candidate.split("}\n", 1)[0] + "}"
        schema = json.loads(candidate)   # may still fail
    return aspects, schema

def pick_example_tables(
    this_tabid: str,
    tabid_to_row: Dict[str, Dict[str, Any]],
    k: int = 5,
) -> List[str]:
    """
    Collect up to *k* Markdown tables from other rows in the dataset.

    Searches several commonly-used field names; returns an empty list if none
    are found.  Does **not** raise.
    """
    pool = [
        row for tid, row in tabid_to_row.items()
        if tid != this_tabid
    ]
    if not pool:
        return []

    tables: List[str] = []
    # oversample a bit to improve the chance of finding non-empty tables
    for row in random.sample(pool, k=min(k * 3, len(pool))):
        tbl = (
            row.get("markdown_table")
            or row.get("table_markdown")
            or row.get("table_md")
            or row.get("table")
            or ""
        ).strip()
        if tbl:
            tables.append(tbl)
        if len(tables) >= k:
            break

    return tables[:k]

# -------------------- Message builder -------------------------------------

def build_messages(
    query: str,
    papers: List[Dict[str, str]],
    caption: str,
    *,
    template: str,
    include_query: bool,
    retriever = None,
    example_tables: List[str] | None = None,
    num_attributes: int = NUM_ATTRIBUTES,
) -> List[Dict[str, str]]:

    # -- query-dependent wording -------------------------------------------
    if include_query:
        query_block  = (
            " that aims to answer the research question:\n\n"
            f'    "{query}"\n'
        )
        query_suffix = " *and* the query"
        pertinent    = " and are pertinent to answering the query"
        respect      = " **with respect to the query**"
    else:
        query_block = query_suffix = pertinent = respect = ""

    # -- ICL block (may be empty) ------------------------------------------
    if example_tables:
        icl_tables_block = "\n\n".join(
            f"[Table {i}]\n{tbl.strip()}" for i, tbl in enumerate(example_tables, 1)
        )
    else:
        icl_tables_block = ""

    # -- render template ----------------------------------------------------
    user_prompt = template.format(
        query_block=query_block,
        query_suffix=query_suffix,
        pertinent=pertinent,
        respect=respect,
        caption=caption,
        icl_tables_block=icl_tables_block,
        num_attributes=num_attributes,
    )

    # -- append the current paper list -------------------------------------
    if retriever:
        fallbacks = 0
        paper_num = 0
        paper_blocks = []
        for i, p in enumerate(papers, 1):
            paper_num += 1
            text = p.get("text", "")
            if text == "":
                content = p.get('abstract','').strip()
                fallbacks += 1
            else:
                try:
                    passages = retriever.query([text], question=query)
                    content = "".join(p.strip() for p in passages)
                except Exception as exc:
                    fallbacks += 1
                    content = p.get('abstract', '').strip()
                    print(f"Failed to retrieve {text}: {exc}")

            paper_blocks.append(
                f"[Paper {i}]\n"
                f"Title: {p.get('title', '').strip()}\n"
                f"Paper content: {content}"
            )
        if fallbacks > 0:
            print(f"fallback to abstract in {fallbacks}/{paper_num} papers.")
    else:
        paper_blocks = [
            f"[Paper {i}]\nTitle: {p.get('title','').strip()}\n"
            f"Paper content:: {p.get('abstract','').strip()}"
            for i, p in enumerate(papers, 1)
        ]
    user_prompt += "\n\nPapers:\n\n" + "\n\n".join(paper_blocks)

    return [
        {"role": "system", "content": SYSTEM_INSTRUCTION},
        {"role": "user",   "content": user_prompt},
    ]

# -------------------- Main processing loop --------------------------------
def process_query_file(
    inp: Path,
    out: Path,                     # e.g. results.jsonl  (suffix gives us ".jsonl")
    llm_for_generation,
    retriever_k: int = 10,
    retriever = None,
    prompts=None,
    num_attributes: int = NUM_ATTRIBUTES,
    resume: bool = False,
) -> None:

    # -- load task records once ---------------------------------------------
    with inp.open(encoding="utf-8") as f_in:
        records = [json.loads(l) for l in f_in if l.strip()]
    if not records:
        raise ValueError("Input JSONL is empty!")

    # TODO: debugging
    records = records[:100]

    # -- load dataset once ---------------------------------------------------
    ds = load_dataset("blnewman/arxivDIGESTables",
                                 split="validation", trust_remote_code=True)
    tabid_to_row = {tid: ds[i] for i, tid in enumerate(ds["tabid"])}

    # -----------------------------------------------------------------------
    # helper to make "base_stem_variant.jsonl"
    # -----------------------------------------------------------------------
    def variant_path(base: Path, variant_name: str) -> Path:
        return base.with_name(f"{base.stem}_{variant_name}{base.suffix}")

    # -----------------------------------------------------------------------
    # iterate over prompt variants
    # -----------------------------------------------------------------------
    prompt_variants = PROMPT_VARIANTS
    if prompts:
        prompt_variants = []
        for i in prompts:
            for j in PROMPT_VARIANTS:
                if j[0] == i:
                    prompt_variants.append(j)
    print(f"-------------------prompt_variants: {prompt_variants}")
    for var_name, tmpl, use_cap, inc_q, use_icl, use_retrieval in prompt_variants:
        client = arxiv.Client() if use_retrieval else None
        if use_retrieval and retriever is None:
            retriever = utils.build_retriever({"k": retriever_k})

        path = variant_path(out, var_name)
        mode = "a" if resume and path.exists() else "w"

        done = set()
        if resume and path.exists():
            with path.open(encoding="utf-8") as f_prev:
                done = {json.loads(l)["tabid"] for l in f_prev if l.strip()}
            print(f"🔄  {var_name}: skipping {len(done)} completed tabids.")

        with path.open(mode, encoding="utf-8") as f_out, \
                tqdm(records, desc=f"{var_name:>18}") as pbar:
            if "retrieval_queries" in records[0]:
                print("------- retrieved query is used")
            for rec in pbar:
                tabid, query, caption = rec["tabid"], rec["query"], rec["caption"]
                if "retrieval_queries" in rec:
                    query = rec["retrieval_queries"][0] #todo, for now take the first
                if tabid in done:
                    continue

                papers = tabid_to_row.get(tabid, {}).get("row_bib_map", [])

                if use_retrieval:
                    for paper in papers:
                        paper["text"] = utils.get_paper_from_title(paper["title"], client) or ""

                if not papers:
                    print(f"⚠️  tabid {tabid} missing papers; skip.")
                    continue

                example_tables = (
                    pick_example_tables(tabid, tabid_to_row, k=5) if use_icl else None
                )
                if use_icl and not example_tables:
                    print(f"⚠️  No example tables found for {tabid}.")

                # ---------------------------------------------------------------
                if "QBSD" in var_name:
                    full_schema = QBSD.discover_schema(query=query, documents=papers, batch_size=2,
                                                  max_keys_schema=NUM_ATTRIBUTES,
                                                  llm=llm_for_generation, retriever=retriever)
                    aspects = []
                    schema = {}
                    for c in full_schema.columns:
                        aspects.append(c.name)
                        schema[c.name] = c.definition
                    answer = ""

                else:
                    messages = build_messages(
                        query,
                        papers,
                        caption if use_cap else "",
                        template=tmpl,
                        include_query=inc_q,
                        retriever=retriever,
                        example_tables=example_tables,
                        num_attributes=num_attributes,
                    )

                    if not use_retrieval:
                        time.sleep(5.1)
                    trimmed = utils.fit_prompt(messages, MAX_NEW_TOKENS)

                    answer = llm_for_generation.generate(
                        trimmed,
                        max_tokens=MAX_NEW_TOKENS,
                        temperature=TEMPERATURE,
                        stop=[STOP_SEQUENCE] if STOP_SEQUENCE else None,
                    ).strip()

                    # --- parse model output (same as before) --------------------
                    try:
                        aspects, schema = parse_llm_output(answer)
                    except Exception as exc:
                        print(f"⚠️  parse failure for {tabid} in {var_name}: {exc}")
                        continue
                try:
                    # --- write result ------------------------------------------
                    f_out.write(
                        json.dumps(
                            {
                                "tabid": tabid,
                                "caption": caption,
                                "query": query,
                                "aspects": aspects,
                                "schema": schema,
                                "raw_answer": answer,
                            },
                            ensure_ascii=False,
                        ) + "\n"
                    )
                except Exception as exc:
                    print(f"⚠️ Json write failure for {tabid} in {var_name}: {exc}")
                    continue

        print(f"✅  Finished {var_name} ➜ {path.resolve()}")


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
        choices=["hf", "together", "openai"],
        default="together",
        help="Which backend to use: 'hf' (local) or 'together' (hosted).",
    )
    parser.add_argument(
        "--resume", dest="resume_out", action="store_true",
        help="If set, append to the existing output JSONL and skip tabids "
             "that have already been processed."
    )

    parser.add_argument(
        "--api_key", dest="api_key", default=None,
        help=""
    )

    parser.add_argument(
        "--prompts", dest="prompts", nargs="+", default=None,
        help=""
    )

    parser.add_argument(
        "--retriever_name", dest="retriever_name", default=None,
        help=""
    )

    parser.add_argument(
        "--retriever_type", dest="retriever_type", default="embedding",
        help=""
    )

    parser.add_argument(
        "--retriever_k", dest="retriever_k", type=int, default=10,
        help=""
    )

    args = parser.parse_args()

    llm_for_generation = utils.build_llm({"provider": args.backend, "api_key": args.api_key})

    retriever_cfg = {"model_name": args.retriever_name,
                                       "k": args.retriever_k,
                                       "type": args.retriever_type}

    retriever = utils.build_retriever(retriever_cfg, llm_for_prompting=llm_for_generation) \
                                    if (args.retriever_name or args.retriever_type != "embedding") else None

    process_query_file(
        args.input_jsonl,
        args.output_jsonl,
        llm_for_generation,
        retriever_k=args.retriever_k,
        retriever=retriever,
        prompts=args.prompts,
        resume=args.resume_out,
    )
    print(f"✅  Done – results in {args.output_jsonl.resolve()}")

if __name__ == "__main__":
    main()
