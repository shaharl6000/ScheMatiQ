from pathlib import Path
from schema import Schema, Column
from llm_backends import LLMInterface
import re
from typing import List, Dict, Any
import json, time
from tqdm import tqdm
import utils
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import lru_cache
import hashlib

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

# ─── caching and optimization ──────────────────────────────────────────────────
# Thread-safe cache for LLM responses
_llm_cache = {}
_cache_lock = threading.Lock()

def _get_text_hash(text: str) -> str:
    """Generate a hash for text content for caching purposes."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()[:16]

def _get_cache_key(paper_text: str, column_name: str, mode: str, strict: bool) -> str:
    """Generate cache key for LLM responses."""
    text_hash = _get_text_hash(paper_text)
    return f"{text_hash}:{column_name}:{mode}:{strict}"

def _get_cached_response(cache_key: str) -> Dict[str, Any] | None:
    """Thread-safe cache retrieval."""
    with _cache_lock:
        return _llm_cache.get(cache_key)

def _cache_response(cache_key: str, response: Dict[str, Any]) -> None:
    """Thread-safe cache storage with size limit."""
    with _cache_lock:
        if len(_llm_cache) > 1000:  # Limit cache size
            # Remove oldest entries (simple FIFO)
            oldest_keys = list(_llm_cache.keys())[:100]
            for key in oldest_keys:
                del _llm_cache[key]
        _llm_cache[cache_key] = response

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


def validate_row_completion(row_data: Dict[str, Any], expected_papers: set[str]) -> bool:
    """
    Check if a row is complete by verifying all expected papers have been processed.
    
    Args:
        row_data: The row data from JSONL
        expected_papers: Set of paper titles that should be in this row
    
    Returns:
        True if the row is complete and doesn't need further processing
    """
    if not row_data or not isinstance(row_data, dict):
        return False
    
    # Get papers that contributed to this row
    row_papers = set(row_data.get("_papers", []))
    
    # Row is complete if it contains all expected papers
    return expected_papers.issubset(row_papers)


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
        if col_name.startswith('_'):  # Skip metadata fields (they're preserved from existing_row)
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

        # Check cache first
        cache_key = _get_cache_key(eff, col.name, "one_by_one", strict)
        cached_result = _get_cached_response(cache_key)
        if cached_result is not None:
            return cached_result

        msgs = build_val_messages(
            schema.query, paper_title, eff, [col.to_dict()],
            mode="one_by_one", strict=strict
        )
        trimmed = utils.fit_prompt(msgs, truncate=True, max_new=max_new_tokens, safety_margins=256)
        raw = llm.generate(trimmed)
        try:
            parsed = parse_llm_output(raw)
            cleaned = _postprocess(parsed, [col.name])
            result = cleaned.get(col.name, {})
            
            # Cache the result
            _cache_response(cache_key, result)
            return result
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

    # mode == "one_by_one" with optimized fallback logic
    row: Dict[str, Any] = {}
    for col in schema.columns:
        # Attempt 1: normal rules, per-column retrieval
        first = _single_column_attempt(col, strict=False, k_override=None, use_snippets=False)

        if first and first.get('answer', '').strip():
            row[col.name] = first
            continue

        # Attempt 2: Only try expanded retrieval if we have a retriever and got empty result
        if retriever is not None:
            second = _single_column_attempt(col, strict=True, k_override=_expand_k(retrieval_k), use_snippets=False)
            if second and second.get('answer', '').strip():
                row[col.name] = second
                continue

        # Attempt 3: Only if previous attempts truly failed and we have substantial text
        if len(paper_text) > 1000:  # Only for reasonably sized documents
            third = _single_column_attempt(col, strict=True, k_override=None, use_snippets=True)
            if third and third.get('answer', '').strip():
                row[col.name] = third

    return row


def process_single_paper(
    doc_path: Path,
    schema: Schema,
    llm: LLMInterface,
    max_new_tokens: int,
    mode: str,
    retriever,
    retrieval_k: int,
    processed_papers: set
) -> tuple[str, str, Dict[str, Any] | None]:
    """
    Process a single paper and return (row_name, paper_title, extracted_data).
    Returns None for extracted_data if paper should be skipped or fails.
    """
    paper_title = doc_path.stem
    row_name = extract_row_name_from_filename(doc_path.name)
    
    # Skip if already processed
    if paper_title in processed_papers:
        return row_name, paper_title, None
    
    try:
        paper_text = doc_path.read_text(encoding="utf-8", errors="ignore")
        print(f"🔍 Extracting values for {paper_title} (row: {row_name})...")

        # Extract values for this paper
        paper_data = extract_values_for_paper(
            paper_title, paper_text, schema, llm, max_new_tokens, mode, retriever, retrieval_k
        )
        
        return row_name, paper_title, paper_data
        
    except Exception as e:
        print(f"⚠️  Error processing {paper_title}: {e}")
        return row_name, paper_title, None


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
    max_workers: int = 3,
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

    # ----- Resume logic - load existing rows and track completion -----------
    existing_rows: Dict[str, Dict[str, Any]] = {}
    processed_papers: set[str] = set()
    completed_rows: set[str] = set()  # Track rows that are fully processed
    
    if resume and output_path.exists():
        print("🔄 Loading existing data for resume...")
        try:
            with output_path.open(encoding="utf-8") as f_prev:
                for line_num, line in enumerate(f_prev, 1):
                    if line.strip():
                        try:
                            row_data = json.loads(line)
                            row_name = row_data.get("_row_name")
                            if row_name:
                                existing_rows[row_name] = row_data
                                # Track all papers that contributed to this row
                                papers = row_data.get("_papers", [])
                                processed_papers.update(papers)
                                
                                # Check if this row is complete (has data for all expected papers)
                                expected_papers_for_row = {p.stem for p in papers_by_row.get(row_name, [])}
                                if validate_row_completion(row_data, expected_papers_for_row):
                                    completed_rows.add(row_name)
                        except json.JSONDecodeError as e:
                            print(f"⚠️  Skipping invalid JSON line {line_num}: {e}")
        except Exception as e:
            print(f"⚠️  Error reading existing file: {e}")
            # If we can't read the existing file safely, don't use resume mode
            existing_rows.clear()
            processed_papers.clear()
            completed_rows.clear()
        
        print(f"🔄 Loaded {len(existing_rows)} existing rows, {len(processed_papers)} papers already processed")
        print(f"🔄 {len(completed_rows)} rows are complete, {len(existing_rows) - len(completed_rows)} rows may need updates")

    # ----- Determine what work needs to be done -----------------------------
    papers_processed = 0
    
    # Collect all papers that need processing (not already processed)
    papers_to_process = []
    for row_name, papers_for_row in papers_by_row.items():
        # Skip rows that are already complete
        if row_name in completed_rows:
            continue
            
        for doc_path in papers_for_row:
            paper_title = doc_path.stem
            if paper_title not in processed_papers:
                papers_to_process.append(doc_path)
    
    if not papers_to_process:
        print("✅ All papers already processed. Nothing to do.")
        return
    
    # Track which rows have been written to avoid duplicates
    written_rows = set()
    
    # Helper function to write a completed row incrementally
    def write_row_if_complete(row_name: str, new_paper_results: dict):
        """Write a row to output file if all its papers are processed."""
        if row_name in written_rows or row_name in completed_rows:
            return
            
        # Check if all papers for this row have been processed
        expected_papers = {p.stem for p in papers_by_row.get(row_name, [])}
        processed_papers_for_row = {title for title, (r_name, _) in new_paper_results.items() if r_name == row_name}
        existing_papers = set(existing_rows.get(row_name, {}).get("_papers", []))
        all_papers_for_row = processed_papers_for_row | existing_papers
        
        if not expected_papers.issubset(all_papers_for_row):
            return  # Row not complete yet
            
        # Row is complete - assemble and write it
        if row_name in existing_rows:
            current_row = existing_rows[row_name].copy()
        else:
            current_row = {
                "_row_name": row_name,
                "_papers": [],
                "_metadata": {
                    "query": schema.query,
                    "retriever": {
                        "type": retriever.__class__.__name__ if retriever else None,
                        "model": getattr(retriever, 'model_name', None) if retriever else None
                    },
                    "backend": {
                        "type": llm.__class__.__name__,
                        "model": getattr(llm, 'model', None),
                        "temperature": getattr(llm, 'temperature', None),
                        "max_tokens": getattr(llm, 'max_tokens', None)
                    }
                }
            }
        
        # Merge new paper results for this row
        for paper_title, (paper_row_name, paper_data) in new_paper_results.items():
            if paper_row_name == row_name:
                existing_papers = current_row.get("_papers", [])
                if paper_title in existing_papers:
                    continue  # Skip duplicates
                    
                if not existing_papers:
                    current_row.update(paper_data)
                    current_row["_papers"] = [paper_title]
                    current_row["_row_name"] = row_name
                else:
                    current_row = merge_row_data(current_row, paper_data, paper_title)
        
        # Add GT_NES column
        gt_nes_value = "in_doubt" if "namesDoubt" in str(docs_directory) else "yes"
        current_row["GT_NES"] = {
            "answer": gt_nes_value,
            "excerpts": [f"Based on source directory: {docs_directory}"]
        }
        
        # Write row if it has actual data
        if any(key for key in current_row.keys() if not key.startswith('_')):
            try:
                with output_path.open('a', encoding="utf-8") as f_out:
                    f_out.write(json.dumps(current_row, ensure_ascii=False) + "\n")
                written_rows.add(row_name)
                print(f"✅ Completed and wrote row: {row_name}")
            except Exception as e:
                print(f"❌ Failed to write row {row_name}: {e}")

    # Initialize output file (truncate if not resuming, or backup existing)
    if not resume or not output_path.exists():
        # Start fresh
        with output_path.open('w', encoding="utf-8") as f:
            pass  # Create empty file
    else:
        # Create backup and start fresh for incremental writing
        backup_path = output_path.with_suffix(f".backup.{int(time.time())}")
        import shutil
        try:
            shutil.copy2(output_path, backup_path)
            print(f"🔒 Created backup: {backup_path}")
        except Exception as e:
            print(f"⚠️  Could not create backup: {e}")
        
        # Write existing complete rows first
        with output_path.open('w', encoding="utf-8") as f_out:
            for row_name, row_data in existing_rows.items():
                if row_name in completed_rows:
                    # Add GT_NES to existing complete rows
                    gt_nes_value = "in_doubt" if "namesDoubt" in str(docs_directory) else "yes"
                    row_data["GT_NES"] = {
                        "answer": gt_nes_value,
                        "excerpts": [f"Based on source directory: {docs_directory}"]
                    }
                    f_out.write(json.dumps(row_data, ensure_ascii=False) + "\n")
                    written_rows.add(row_name)

    # Process papers with incremental row writing
    paper_results = {}  # Still needed for tracking what's been processed
    
    if max_workers == 0:
        # Sequential processing
        print(f"🔄 Processing {len(papers_to_process)} papers sequentially with incremental writing...")
        for doc_path in tqdm(papers_to_process, desc="processing papers"):
            try:
                row_name, paper_title, extracted_data = process_single_paper(
                    doc_path, schema, llm, max_new_tokens, mode, retriever, retrieval_k, processed_papers
                )
                if extracted_data is not None:
                    paper_results[paper_title] = (row_name, extracted_data)
                    papers_processed += 1
                    
                    # Try to write row if complete
                    write_row_if_complete(row_name, paper_results)
                    time.sleep(0.5)  # Gentle on API
                    
            except Exception as e:
                print(f"⚠️  Error processing {doc_path.stem}: {e}")
    else:
        # Parallel processing with incremental writing
        print(f"🚀 Processing {len(papers_to_process)} papers using {max_workers} parallel workers with incremental writing...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_paper = {
                executor.submit(
                    process_single_paper, 
                    doc_path, schema, llm, max_new_tokens, mode, retriever, retrieval_k, processed_papers
                ): doc_path
                for doc_path in papers_to_process
            }
            
            for future in tqdm(as_completed(future_to_paper), total=len(papers_to_process), desc="processing papers"):
                doc_path = future_to_paper[future]
                try:
                    row_name, paper_title, extracted_data = future.result()
                    if extracted_data is not None:
                        paper_results[paper_title] = (row_name, extracted_data)
                        papers_processed += 1
                        
                        # Try to write row if complete
                        write_row_if_complete(row_name, paper_results)
                        
                except Exception as e:
                    print(f"⚠️  Error in parallel processing for {doc_path.stem}: {e}")
    
    # Final check: write any remaining incomplete rows
    print("📝 Writing any remaining incomplete rows...")
    remaining_rows = 0
    for row_name in papers_by_row.keys():
        if row_name not in written_rows and row_name not in completed_rows:
            write_row_if_complete(row_name, paper_results)
            if row_name in written_rows:
                remaining_rows += 1
    
    total_rows = len(written_rows) + len(completed_rows)
    print(f"✅ Incremental processing complete: {papers_processed} papers processed into {total_rows} total rows ➜ {output_path.resolve()}")
    if remaining_rows > 0:
        print(f"📄 Wrote {remaining_rows} additional incomplete rows at the end")


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
    max_workers = cfg.get("max_workers", 3)  # Default to 3 parallel workers

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
        max_workers=max_workers,
    )

if __name__ == "__main__":
    main(Path("configurations/valueExtractionConfig.json"))
