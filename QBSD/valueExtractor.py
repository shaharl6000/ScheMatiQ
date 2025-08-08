from pathlib import Path
from schema import Schema, Column
from llm_backends import LLMInterface
import re
from typing import List, Dict, Any
import json, time
from tqdm import tqdm
import utils

# ─── regexes reused / adapted from earlier helper ──────────────────────────
JSON_FENCE = re.compile(r"```json(.*?)```", re.S)   # fenced ```json … ```
LAST_JS    = re.compile(r"\{[\s\S]*\}\s*$", re.S)       # last {...} before EOF

def _extract_json_str(text: str) -> str:
    """
    Pull the *text* of the JSON object out of the LLM output.
    """
    m = JSON_FENCE.search(text)
    candidate = m.group(1).strip() if m else None

    if candidate is None:
        m = LAST_JS.search(text)
        if not m:
            raise ValueError("No JSON object found in output.")
        candidate = m.group(0)

    return candidate


def parse_llm_output(text: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse ValueLLM output into a normalised dict::

        {
          "<column>": { "answer": <str>, "excerpts": <list[str]> },
          ...
        }

    • Missing "answer" / "excerpts" keys are filled with "" / []
    • If the model returned just a string for a column, it is wrapped
      into the {"answer": "..."} structure for consistency.
    """
    raw_json = _extract_json_str(text)

    # First pass – try as‑is
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError:
        # Common failure: trailing tokens after the last } – truncate
        raw_json = raw_json.split("}\n", 1)[0] + "}"
        data = json.loads(raw_json)

    if not isinstance(data, dict):
        raise ValueError("Top‑level JSON is not an object.")

    # Normalise each column entry
    norm: Dict[str, Dict[str, Any]] = {}
    for col, val in data.items():
        # Case 1: already the expected dict but possibly missing keys
        if isinstance(val, dict):
            answer   = val.get("answer", "")
            excerpts = val.get("excerpts", [])
            # Guarantee correct types
            if not isinstance(answer, str):
                answer = str(answer)
            if not isinstance(excerpts, list):
                excerpts = [str(excerpts)]
            norm[col] = {"answer": answer, "excerpts": excerpts}

        # Case 2: the model emitted a bare string / number
        else:
            norm[col] = {"answer": str(val), "excerpts": []}

    return norm

SYSTEM_PROMPT_VAL = """
You are *ValueLLM*, a meticulous data curator.

### Task
1. You receive **a scientific paper**, and one
   or more *column specifications* (name + definition).
2. For each requested column, extract the answer **strictly from the paper
   text**. If the paper does not contain the information, return {{}}.
3. If there is no answer in the paper, return an empty dictionary, i.e., '{}'.

### Output spec (single or multi‑column)
{
  "<column_name>": {
    "answer":   "<concise but complete answer>",
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


def _build_retrieval_query(schema: Schema, columns: List[Column] = None) -> str:
    """
    Build an effective retrieval query combining the main schema query 
    with specific column information for targeted passage retrieval.
    """
    base_query = schema.query
    
    if columns:
        # Add column-specific context for more targeted retrieval
        column_context = " ".join([
            f"Information about {col.name}: {col.definition}" 
            for col in columns
        ])
        return f"{base_query} {column_context}"
    
    return base_query


###############################################################################
def extract_values_for_paper(
    paper_title: str,
    paper_text: str,
    schema: Schema,
    llm: LLMInterface,
    max_new_tokens: int,
    mode: str = "all",
    retriever = None,
    retrieval_k: int = 8,
) -> Dict[str, Any]:
    """
    Returns a dict {column_name -> {...}} for one paper.
    Uses retriever when available to find relevant passages, otherwise falls back to text truncation.
    """
    # Determine effective text to use for value extraction
    effective_text = paper_text
    
    if retriever is not None:
        # Use retriever to find relevant passages instead of full text
        if mode == "all":
            retrieval_query = _build_retrieval_query(schema, list(schema.columns))
        else:
            retrieval_query = _build_retrieval_query(schema)
            
        try:
            relevant_passages = retriever.query([paper_text], retrieval_query, k=retrieval_k)
            if relevant_passages:
                # Combine retrieved passages with clear separators
                effective_text = "\n\n--- RELEVANT PASSAGE ---\n\n".join(relevant_passages)
                print(f"📖 Retrieved {len(relevant_passages)} relevant passages for {paper_title}")
            else:
                print(f"⚠️  No relevant passages found for {paper_title}, using full text")
        except Exception as e:
            print(f"⚠️  Retrieval failed for {paper_title}: {e}, using full text")
    
    if mode == "all":
        msgs = build_val_messages(
            schema.query,
            paper_title,
            effective_text,
            [c.to_dict() for c in schema.columns],
            mode="all",
        )
        trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, safety_margins=512)
        raw = llm.generate(trimmed)
        try:
            return parse_llm_output(raw)
        except Exception as e:
            print(f"⚠️  parse failure for {paper_title}: {e}")
            return {}

    # mode == "one"
    row: Dict[str, Any] = {}
    for col in schema.columns:
        msgs = build_val_messages(
            schema.query, paper_title, effective_text, [col.to_dict()], mode="one"
        )
        raw = llm.generate(msgs)
        try:
            parsed = json.loads(raw)
            row[col.name] = parsed.get(col.name, {})
        except json.JSONDecodeError as e:
            print(f"⚠️  JSON parse failure for column {col.name} in {paper_title}: {e}")
            row[col.name] = {}
    return row


def build_table_jsonl(
    schema_path: Path,
    docs_directory: Path,
    output_path: Path,
    llm: LLMInterface,
    retriever = None,
    *,
    max_new_tokens: int = 512,
    resume: bool = False,
    mode: str = "all",
    retrieval_k: int = 8,
) -> None:
    """
    Stream each paper's extracted values to *output_path* (JSONL).
    Nothing is kept in memory except the current row.
    """
    # ----- load schema ------------------------------------------------------
    data   = json.loads(schema_path.read_text(encoding="utf-8"))
    
    # Convert dictionary columns to Column objects
    columns = []
    for col_dict in data["schema"]:
        if isinstance(col_dict, dict):
            # Handle both possible formats: {"column": name, "definition": def} or {"name": name, "definition": def}
            name = col_dict.get("column") or col_dict.get("name")
            definition = col_dict.get("definition", "")
            rationale = col_dict.get("explanation", "") or col_dict.get("rationale", "")
            columns.append(Column(name=name, definition=definition, rationale=rationale))
        else:
            columns.append(col_dict)  # Already a Column object
    
    schema = Schema(query=data["query"],
                    columns=columns,
                    max_keys=len(data["schema"]))

    docs = sorted(docs_directory.glob("*"))
    if not docs:
        raise RuntimeError(f"No docs found under {docs_directory.resolve()}")

    # ----- resume logic -----------------------------------------------------
    done: set[str] = set()
    mode_flag      = "a" if resume and output_path.exists() else "w"

    if resume and output_path.exists():
        with output_path.open(encoding="utf-8") as f_prev:
            done = {json.loads(l)["_paper"] for l in f_prev if l.strip()}
        print(f"🔄  Resuming – {len(done)} papers already done; skipping.")

    # ----- extraction loop --------------------------------------------------
    written = 0

    with output_path.open(mode_flag, encoding="utf-8") as f_out, \
            tqdm(docs, desc="extract") as pbar:

        for doc_path in pbar:
            paper_title = doc_path.stem
            if paper_title in done:
                continue

            paper_text = doc_path.read_text(encoding="utf-8", errors="ignore")
            print(f"Extracting values for {paper_title} …")

            row_dict = extract_values_for_paper(
                paper_title, paper_text, schema, llm, max_new_tokens, mode, retriever, retrieval_k
            )
            row_dict["_paper"] = paper_title

            f_out.write(json.dumps(row_dict, ensure_ascii=False) + "\n")
            written += 1
            time.sleep(0.1)        # gentle on the API

    print(f"✅  Added {written} new papers ➜ {output_path.resolve()}")


def main(cfg_path: Path) -> None:
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    print(f"Loaded config from {cfg_path}")

    schema_path = Path(cfg["schema_path"])
    docs_dir    = Path(cfg["docs_directory"])
    output_path = Path(cfg["output_path"])
    backend_cfg = cfg.get("backend_cfg", {})
    max_new     = backend_cfg.get("max_tokens", 512)
    mode        = cfg.get("mode", "all")
    resume      = cfg.get("resume", False)
    retriever_cfg = cfg.get("retriever", None)
    retrieval_k = cfg.get("retrieval_k", 8)  # Default to 8 passages

    llm = utils.build_llm(backend_cfg)
    retriever = utils.build_retriever(retriever_cfg) if retriever_cfg is not None else None

    build_table_jsonl(
        schema_path,
        docs_dir,
        output_path,
        llm,
        retriever,
        max_new_tokens=max_new,
        resume=resume,
        mode=mode,
        retrieval_k=retrieval_k,
    )

if __name__ == "__main__":
    main(Path("configurations/valueExtractionConfig.json"))
