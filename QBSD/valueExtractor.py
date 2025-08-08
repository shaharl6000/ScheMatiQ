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
Given a scientific paper and one or more requested columns (name + definition),
extract answers **strictly from the paper**.

### Rules (MUST follow)
- If a column’s answer is **not in the paper**, **omit that column** from the JSON.
  Do **not** invent placeholders like "not provided", "unknown", "N/A", etc.
- Output **only JSON**, no prose, no markdown fences.
- Include a column **only** when the answer is supported by the provided text.

### Output (single or multi-column)
{
  "<column_name>": {
    "answer": "<concise answer>",
    "excerpts": ["<short supporting quote or span from the paper>", ...]
  },
  ...
}

### Examples
# Missing
{}

# Present
{
  "dataset_name": {
    "answer": "CIFAR-10",
    "excerpts": ["We evaluate on CIFAR-10 and ImageNet..."]
  }
}
""".strip()


SYSTEM_PROMPT_VAL_STRICT = """
You are *ValueLLM*, extracting values **only if directly supported by the text**.

### Strict Rules (ENFORCED)
- Include a column **only if** you can provide at least one supporting excerpt (verbatim or near-verbatim).
- If you cannot find a supported answer, **omit the column** entirely (return `{}` for single-column).
- Do **not** use placeholders like "not provided", "unknown", "N/A", "cannot be determined".

### Output
JSON only (no markdown). Same schema as before.
""".strip()

def build_val_messages(query: str,
                       paper_title: str,
                       paper_text: str,
                       columns: List[Column],
                       mode: str = "all",
                       *,
                       strict: bool = False) -> List[Dict[str, str]]:
    """
    mode:
      - "all"         – ask for all columns at once
      - "one"         – (deprecated) alias of "one_by_one"
      - "one_by_one"  – single-column prompt, called per column by the caller
    """
    if mode in {"one", "one_by_one"}:
        col = columns[0]
        col_block = f"""
        <REQUESTED_COLUMN>
        name: {col['column']}
        definition: {col['definition']}
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

    system = SYSTEM_PROMPT_VAL_STRICT if strict else SYSTEM_PROMPT_VAL
    return [
        {"role": "system", "content": system},
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

# ─── placeholder detection ─────────────────────────────────────────────────────
_PLACEHOLDER_RE = re.compile(
    r"\b(not\s+provided|not\s+specified|not\s+mentioned|no\s+(information|data)|"
    r"cannot\s+be\s+determined|unknown|n/?a|no\s+answer|insufficient\s+information)\b",
    re.I,
)

def _is_placeholder(answer: str, excerpts: List[str]) -> bool:
    if not isinstance(answer, str):
        return False
    if excerpts:  # if they gave evidence, allow it (paper may literally say "unknown")
        return False
    return bool(_PLACEHOLDER_RE.search(answer.strip()))


def _postprocess(parsed: Dict[str, Dict[str, Any]],
                 requested_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Ensure missing columns are omitted or set to {} and clean placeholders."""
    out: Dict[str, Dict[str, Any]] = {}
    for col in requested_cols:
        entry = parsed.get(col)
        if not entry:
            continue  # omit missing column
        ans = entry.get("answer", "")
        exs = entry.get("excerpts", [])
        if _is_placeholder(ans, exs) or (isinstance(ans, str) and not ans.strip()):
            # treat as missing by omitting the column (conforms to prompt spec)
            continue
        # normalize types
        if not isinstance(ans, str):
            ans = str(ans)
        if not isinstance(exs, list):
            exs = [str(exs)]
        out[col] = {"answer": ans, "excerpts": exs}
    return out


# ─── retrieval & snippet helpers ───────────────────────────────────────────────
def _expand_k(k: int) -> int:
    return min(max(2 * k, 8), 24)

def _heuristic_snippets(text: str, keywords: List[str], max_snippets: int = 8) -> str:
    """Pick top paragraphs by keyword hits as a poor-man's retrieval."""
    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if not paras:
        return text
    keys = {w.lower() for w in keywords if len(w) > 2}
    def score(p: str) -> int:
        tokens = re.findall(r"[A-Za-z0-9_]+", p.lower())
        return sum(t in keys for t in tokens)
    ranked = sorted(paras, key=score, reverse=True)[:max_snippets]
    return "\n\n--- RELEVANT PASSAGE ---\n\n".join(ranked)

def _keywords_for(col: Column) -> List[str]:
    base = f"{col.name} {col.definition}"
    return re.findall(r"[A-Za-z0-9_]+", base)


def extract_row_name_from_filename(filename: str) -> str:
    """
    Extract row name from filename by taking the part before the first underscore.
    
    Examples:
        abc-gamma_348734_full.txt -> abc-gamma
        4E-T_32520643_full.txt -> 4E-T
        simple_file.txt -> simple
    """
    # Remove file extension and get the base name
    base_name = Path(filename).stem
    # Split by underscore and take the first part
    row_name = base_name.split('_')[0]
    return row_name


def merge_row_data(existing_row: Dict[str, Any], new_row: Dict[str, Any], new_paper_title: str) -> Dict[str, Any]:
    """
    Intelligently merge data from two rows with the same row name.
    
    Args:
        existing_row: The existing row data from the JSONL file
        new_row: New extracted data for the same row name
        new_paper_title: Title of the new paper being processed
    
    Returns:
        Merged row data combining both sources
    """
    merged = existing_row.copy()
    
    # Track source papers - update the _papers list
    existing_papers = merged.get('_papers', [])
    if new_paper_title not in existing_papers:
        existing_papers.append(new_paper_title)
    merged['_papers'] = existing_papers
    
    # For each column in the new data
    for col_name, new_col_data in new_row.items():
        if col_name.startswith('_'):  # Skip metadata fields
            continue
            
        if not isinstance(new_col_data, dict):
            continue
            
        new_answer = new_col_data.get('answer', '').strip()
        new_excerpts = new_col_data.get('excerpts', [])
        
        # If no existing data for this column, use new data
        if col_name not in merged:
            if new_answer:  # Only add if there's actually an answer
                merged[col_name] = new_col_data.copy()
            continue
            
        existing_col_data = merged[col_name]
        if not isinstance(existing_col_data, dict):
            continue
            
        existing_answer = existing_col_data.get('answer', '').strip()
        existing_excerpts = existing_col_data.get('excerpts', [])
        
        # Merge logic: prefer more complete/detailed answer
        merged_answer = existing_answer
        if new_answer and (not existing_answer or len(new_answer) > len(existing_answer)):
            merged_answer = new_answer
        elif new_answer and existing_answer and new_answer != existing_answer:
            # If both have answers but different, combine them
            merged_answer = f"{existing_answer}; {new_answer}"
        
        # Merge excerpts, removing duplicates while preserving order
        all_excerpts = existing_excerpts + new_excerpts
        unique_excerpts = []
        seen = set()
        for excerpt in all_excerpts:
            excerpt_clean = excerpt.strip().lower()
            if excerpt_clean and excerpt_clean not in seen:
                unique_excerpts.append(excerpt)
                seen.add(excerpt_clean)
        
        merged[col_name] = {
            'answer': merged_answer,
            'excerpts': unique_excerpts
        }
    
    return merged


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
    if mode == "one":
        mode = "one_by_one"

    def _retrieve_effective_text(columns_for_query=None, k=None) -> str:
        if retriever is None:
            return paper_text
        try:
            retrieval_query = _build_retrieval_query(schema, columns_for_query)
            passages = retriever.query([paper_text], retrieval_query, k=(k or retrieval_k))
            if passages:
                print(f"📖 Retrieved {len(passages)} relevant passages for {paper_title}")
                return "\n\n--- RELEVANT PASSAGE ---\n\n".join(passages)
            else:
                print(f"⚠️  No relevant passages found for {paper_title}, using full text")
                return paper_text
        except Exception as e:
            print(f"⚠️  Retrieval failed for {paper_title}: {e}, using full text")
            return paper_text

    def _single_column_attempt(col: Column,
                               strict: bool,
                               k_override: int | None,
                               use_snippets: bool = False) -> Dict[str, Any]:
        # prepare text
        if retriever is not None:
            eff = _retrieve_effective_text([col], k=k_override)
        else:
            if use_snippets:
                eff = _heuristic_snippets(paper_text, _keywords_for(col))
            else:
                eff = paper_text

        msgs = build_val_messages(
            schema.query, paper_title, eff, [col.to_dict()],
            mode="one_by_one", strict=strict
        )
        trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, safety_margins=256)
        raw = llm.generate(trimmed)
        try:
            parsed = parse_llm_output(raw)
            cleaned = _postprocess(parsed, [col.name])
            return cleaned.get(col.name, {})
        except Exception as e:
            print(f"⚠️  parse failure for column {col.name} in {paper_title}: {e}")
            return {}

    if mode == "all":
        # joint retrieval + one call
        eff = _retrieve_effective_text(list(schema.columns))
        msgs = build_val_messages(
            schema.query, paper_title, eff, [c.to_dict() for c in schema.columns],
            mode="all", strict=False
        )
        trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, safety_margins=512)
        raw = llm.generate(trimmed)
        try:
            parsed = parse_llm_output(raw)
        except Exception as e:
            print(f"⚠️  parse failure for {paper_title}: {e}")
            parsed = {}

        requested = [c.name for c in schema.columns]
        cleaned = _postprocess(parsed, requested)

        # Fallback for missing columns: retry per-column, stricter + expanded retrieval
        missing = [c for c in schema.columns if c.name not in cleaned]
        if missing:
            print(f"↻ Fallback per-column for {len(missing)} missing: {[c.name for c in missing]}")
        for col in missing:
            # First fallback: expanded k + strict prompt
            col_res = _single_column_attempt(col, strict=True, k_override=_expand_k(retrieval_k), use_snippets=False)
            if not col_res:
                # Second fallback: heuristic snippets (no retriever) or even stricter evidence demand
                col_res = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
            if col_res:
                cleaned[col.name] = col_res

        return cleaned

    # mode == "one_by_one"
    row: Dict[str, Any] = {}
    for col in schema.columns:
        # Attempt 1: normal rules, per-column retrieval
        first = _single_column_attempt(col, strict=False, k_override=None, use_snippets=False)

        if first:
            row[col.name] = first
            continue

        # Attempt 2: expanded retrieval + strict prompt
        second = _single_column_attempt(col, strict=True, k_override=_expand_k(retrieval_k), use_snippets=False)
        if second:
            row[col.name] = second
            continue

        # Attempt 3: heuristic snippets + strict prompt (works even without a retriever)
        third = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
        row[col.name] = third if third else {}

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
    Extract values from papers and write to JSONL, grouping by row names and merging intelligently.
    Each row represents a unique row name with data from potentially multiple papers.
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

    # ----- Group papers by row name ----------------------------------------
    print("📋 Grouping papers by row name...")
    papers_by_row: Dict[str, List[Path]] = {}
    for doc_path in docs:
        row_name = extract_row_name_from_filename(doc_path.name)
        if row_name not in papers_by_row:
            papers_by_row[row_name] = []
        papers_by_row[row_name].append(doc_path)
    
    print(f"📊 Found {len(papers_by_row)} unique row names from {len(docs)} papers")
    for row_name, papers in papers_by_row.items():
        print(f"  • {row_name}: {len(papers)} papers")

    # ----- Resume logic - load existing rows by row name -------------------
    existing_rows: Dict[str, Dict[str, Any]] = {}
    processed_papers: set[str] = set()
    
    if resume and output_path.exists():
        print("🔄 Loading existing data for resume...")
        with output_path.open(encoding="utf-8") as f_prev:
            for line in f_prev:
                if line.strip():
                    try:
                        row_data = json.loads(line)
                        row_name = row_data.get("_row_name")
                        if row_name:
                            existing_rows[row_name] = row_data
                            # Track all papers that contributed to this row
                            papers = row_data.get("_papers", [])
                            processed_papers.update(papers)
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Skipping invalid JSON line: {e}")
        
        print(f"🔄 Loaded {len(existing_rows)} existing rows, {len(processed_papers)} papers already processed")

    # ----- Row processing loop ----------------------------------------------
    rows_written = 0
    papers_processed = 0
    
    # Use a temporary file to write all new content, then replace the original
    temp_output = output_path.with_suffix('.tmp')
    
    with temp_output.open('w', encoding="utf-8") as f_out, \
         tqdm(papers_by_row.items(), desc="processing rows") as pbar:
        
        for row_name, papers_for_row in pbar:
            pbar.set_description(f"processing {row_name}")
            
            # Start with existing row data if resuming
            current_row = existing_rows.get(row_name, {
                "_row_name": row_name,
                "_papers": []
            })
            
            # Process each paper for this row
            for doc_path in papers_for_row:
                paper_title = doc_path.stem
                
                # Skip if this paper was already processed for this row
                if paper_title in processed_papers:
                    continue
                
                try:
                    paper_text = doc_path.read_text(encoding="utf-8", errors="ignore")
                    print(f"🔍 Extracting values for {paper_title} (row: {row_name})...")

                    # Extract values for this paper
                    paper_data = extract_values_for_paper(
                        paper_title, paper_text, schema, llm, max_new_tokens, mode, retriever, retrieval_k
                    )
                    
                    # If this is the first paper for this row, initialize the row
                    if not current_row.get("_papers"):
                        current_row.update(paper_data)
                        current_row["_papers"] = [paper_title]
                    else:
                        # Merge with existing row data
                        current_row = merge_row_data(current_row, paper_data, paper_title)
                    
                    papers_processed += 1
                    time.sleep(0.1)  # gentle on the API
                    
                except Exception as e:
                    print(f"⚠️  Error processing {paper_title}: {e}")
                    continue
            
            # Write the completed row (even if no new papers were processed)
            f_out.write(json.dumps(current_row, ensure_ascii=False) + "\n")
            rows_written += 1
    
    # Replace the original file with the temporary file
    if output_path.exists():
        output_path.unlink()
    temp_output.rename(output_path)
    
    print(f"✅ Processed {papers_processed} papers into {rows_written} rows ➜ {output_path.resolve()}")


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
