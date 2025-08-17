"""Main entry point for value extraction with refactored structure."""

import json
from pathlib import Path

import utils
from .core.table_builder import TableBuilder
from .config.constants import DEFAULT_MAX_NEW_TOKENS, DEFAULT_MAX_WORKERS


def build_table_jsonl(
    schema_path: Path,
    docs_directory: Path,
    output_path: Path,
    llm,
    retriever=None,
    *,
    max_new_tokens: int = DEFAULT_MAX_NEW_TOKENS,
    resume: bool = False,
    mode: str = "all",
    retrieval_k: int = 8,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """
    Extract values from papers and write to JSONL, grouping by row names and merging intelligently.
    Each row represents a unique row name with data from potentially multiple papers.
    
    This is the main entry point that maintains backward compatibility with the original API.
    """
    table_builder = TableBuilder(llm, retriever)
    table_builder.build_table_jsonl(
        schema_path,
        docs_directory,
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
    docs_dir = Path(cfg["docs_directory"])
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