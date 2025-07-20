from pathlib import Path
from dataclasses import asdict
import pandas as pd         #  only std dep; install if missing
from schema import Schema, Column
from llm_backends import LLMInterface
from retrievers import Retriever
from typing import List, Dict, Any
import json, logging
import utils

SYSTEM_PROMPT_VAL = """
You are *ValueLLM*, a meticulous data curator.

### Task
1. You receive **a scientific paper**, and one
   or more *column specifications* (name + definition).
2. For each requested column, extract the answer **strictly from the paper
   text**. If the paper does not contain the information, return {{}}.
3. Return the answer(s) together with up to 5 **exact** supporting excerpts
   (total ≤800 words). Do not invent facts or paraphrase the excerpts.

### Output spec (single or multi‑column)
{
  "<column_name>": {
    "answer":   "<concise but complete answer>",
    "excerpts": ["<exact span 1>", "..."]
  },
  ...
}

Only output JSON – no extra text, no markdown.
""".strip()


def build_val_messages(query: str,
                       paper_title: str,
                       paper_text: str,
                       columns: List[Column],
                       mode: str = "all") -> List[Dict[str, str]]:
    """
    mode: "all"  – ask for all columns at once
          "one"  – assume `columns` has length 1 and ask for that column only
    """
    # Turn column specs into a prompt block
    if mode == "one":
        col_block = f"""
        <REQUESTED_COLUMN>
        name: {columns[0]['column']}
        definition: {columns[0]['definition']}
        </REQUESTED_COLUMN>
        """.strip()
    else:
        col_specs = "\n".join(
            f"- **{c['column']}**: {c['definition']}" for c in columns
        )
        col_block = f"""
        <REQUESTED_COLUMNS>
        {col_specs}
        </REQUESTED_COLUMNS>
        """.strip()

    user_prompt = f"""
        <QUESTION>
        {query}
        </QUESTION>
        
        {col_block}
        
        <PAPER_TITLE>
        {paper_title}
        </PAPER_TITLE>
        
        <PAPER_TEXT>
        {paper_text}
        </PAPER_TEXT>
        """.strip()

    return [
        {"role": "system", "content": SYSTEM_PROMPT_VAL},
        {"role": "user",   "content": user_prompt},
    ]


###############################################################################
def extract_values_for_paper(paper_title: str,
                             paper_text: str,
                             schema: Schema,
                             llm: LLMInterface,
                             max_new_tokens: int,
                             mode: str = "all") -> Dict[str, Any]:
    """
    Returns a dict {column_name -> {"answer": str, "excerpts": [...]}}
    """
    if mode == "all":
        cols = [c for c in schema.columns]
        msgs = build_val_messages(schema.query, paper_title, paper_text, cols, mode="all")
        trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens)
        raw = llm.generate(trimmed)
        return json.loads(raw)

    # mode == "one" : loop over columns
    row: Dict[str, Any] = {}
    for col in schema:
        msgs = build_val_messages(schema.query, paper_title, paper_text,
                                  [asdict(col)], mode="one")
        raw = llm.generate(msgs)
        # Expect {"<col_name>": {...}}
        answer_obj = json.loads(raw).get(col.name, {})
        row[col.name] = answer_obj
    return row


def build_table(schema_path: Path,
                llm: LLMInterface,
                max_new_tokens: int,
                mode: str = "all") -> pd.DataFrame:
    """
    • input_path: JSON produced by QBSD.py (contains schema + docs_path)
    • mode: "all" (faster, bigger prompt) or "one" (smaller prompt, more calls)
    Returns a pandas DataFrame with papers as rows and schema columns as cols.
    """
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    schema = Schema(query=data["query"],
                    columns=data["schema"],
                    max_keys=len(data["schema"]))
    docs_path = Path(data["docs_path"])
    docs = list(docs_path.glob("*"))         # expects one file per paper
    if not docs:
        raise RuntimeError(f"No docs found under {docs_path}")

    table_rows = []
    for doc_path in docs:
        paper_title = doc_path.stem
        paper_text  = doc_path.read_text(encoding="utf-8", errors="ignore")
        logging.info("Extracting values for %s …", paper_title)
        row_dict = extract_values_for_paper(paper_title, paper_text, schema, llm, max_new_tokens, mode)
        row_dict["_paper"] = paper_title
        table_rows.append(row_dict)

    df = pd.DataFrame(table_rows).set_index("_paper")
    return df


def main(cfg_path: Path) -> None:
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    logging.info("Loaded config from %s", cfg_path)

    schema_path = Path(cfg["schema_path"])
    backend_cfg = cfg.get("backend_cfg", {})
    mode = cfg.get("mode", {})
    output_path = Path(cfg.get("output_path", "table_output.json"))

    llm = utils.build_llm(backend_cfg)

    df = build_table(schema_path, llm, mode=mode, max_new_tokens=backend_cfg.get("max_tokens", 512))
    df.to_csv(output_path, index=True)
    logging.info("Saved table to %s", output_path)
    print(df.head())

if __name__ == "__main__":
    main(r"configurations/valueExtractionConfig.json")
