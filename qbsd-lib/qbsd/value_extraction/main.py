"""Main entry point for value extraction with refactored structure."""

import json
import time
from pathlib import Path
from typing import Callable, Any, Optional

from qbsd.core import utils
from .core.table_builder import TableBuilder
from .core.paper_processor import OnValueExtractedCallback
from .config.constants import DEFAULT_MAX_NEW_TOKENS, DEFAULT_MAX_WORKERS


def build_table_jsonl(
    schema_path: Path,
    docs_directories: list[Path],
    output_path: Path,
    llm,
    retriever=None,
    *,
    max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
    resume: bool = False,
    mode: str = "all",
    retrieval_k: int = 8,
    max_workers: int = DEFAULT_MAX_WORKERS,
    on_value_extracted: Optional[OnValueExtractedCallback] = None,
) -> None:
    """
    Extract values from papers and write to JSONL, grouping by row names and merging intelligently.
    Each row represents a unique row name with data from potentially multiple papers.

    Args:
        on_value_extracted: Optional callback called when each column value is extracted.
            Signature: (row_name: str, column_name: str, value: Any) -> None
            Used for real-time streaming of values to UI.

    This is the main entry point that maintains backward compatibility with the original API.
    """
    table_builder = TableBuilder(llm, retriever, on_value_extracted=on_value_extracted)
    table_builder.build_table_jsonl_multi_dirs(
        schema_path,
        docs_directories,
        output_path,
        max_new_tokens=max_new_tokens,
        resume=resume,
        mode=mode,
        retrieval_k=retrieval_k,
        max_workers=max_workers,
    )


def main(cfg_path: Path) -> None:
    """Main function that loads config and runs value extraction."""
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    print(f"Loaded config from {cfg_path}")

    schema_path = Path(cfg["schema_path"])
    
    # Support both single directory (backward compatibility) and multiple directories
    if "docs_directories" in cfg:
        docs_directories = [Path(d) for d in cfg["docs_directories"]]
    elif "docs_directory" in cfg:
        docs_directories = [Path(cfg["docs_directory"])]
    else:
        raise ValueError("Configuration must specify either 'docs_directory' or 'docs_directories'")
    
    output_path = Path(cfg["output_path"])
    backend_cfg = cfg.get("backend_cfg", {})
    max_new = backend_cfg.get("max_tokens", DEFAULT_MAX_NEW_TOKENS)
    mode = cfg.get("mode", "all")
    resume = cfg.get("resume", False)
    retriever_cfg = cfg.get("retriever", None)
    retrieval_k = cfg.get("retrieval_k", 8)
    max_workers = cfg.get("max_workers", DEFAULT_MAX_WORKERS)

    llm = utils.build_llm(backend_cfg)
    retriever = utils.build_retriever(retriever_cfg) if retriever_cfg is not None else None

    # Count documents across all directories
    total_docs_count = 0
    for docs_dir in docs_directories:
        docs = sorted(docs_dir.glob("*"))
        docs_count = len(docs)
        total_docs_count += docs_count
        print(f"📁 Found {docs_count} documents in {docs_dir}")
    
    # Run value extraction with timing
    print(f"\nStarting value extraction for {total_docs_count} documents across {len(docs_directories)} directories...")
    start_time = time.time()
    build_table_jsonl(
        schema_path,
        docs_directories,
        output_path,
        llm,
        retriever,
        max_new_tokens=max_new,
        resume=resume,
        mode=mode,
        retrieval_k=retrieval_k,
        max_workers=max_workers,
    )
    elapsed_time = time.time() - start_time
    
    print(f"\nValue extraction completed for {total_docs_count} documents in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")