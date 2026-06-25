"""Regression tests for incremental-extraction document selection."""

from pathlib import Path

import pytest

from app.services.continue_discovery_service import (
    _extract_papers,
    _plan_incremental_extraction_documents,
)


def _write_docs(docs_dir: Path, files: dict[str, str]) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (docs_dir / name).write_text(content, encoding="utf-8")


def test_extract_papers_handles_variants():
    row = {
        "papers": {"answer": ["judges.txt"]},
    }
    assert _extract_papers(row) == ["judges.txt"]

    row = {"_papers": "judges.txt"}
    assert _extract_papers(row) == ["judges.txt"]

    row = {"data": {"papers": ["alpha", "beta"]}}
    assert _extract_papers(row) == ["alpha", "beta"]


def test_papers_mode_selects_multi_unit_source_document(tmp_path):
    """One source file, many observation-unit rows — papers field drives selection."""
    docs_dir = tmp_path / "documents"
    _write_docs(docs_dir, {"judges.txt": "federal judges roster"})

    rows = [
        {"_row_name": "Leo T. Sorokin", "_papers": ["judges.txt"]},
        {"_row_name": "David J. Barron", "_papers": ["judges.txt"]},
        {"_row_name": "Julie Rikelman", "papers": ["judges"]},
    ]

    selected, mode, warnings = _plan_incremental_extraction_documents(docs_dir, rows)

    assert mode == "papers"
    assert warnings == []
    assert [p.name for p in selected] == ["judges.txt"]


def test_papers_mode_honors_row_scope(tmp_path):
    docs_dir = tmp_path / "documents"
    _write_docs(
        docs_dir,
        {
            "judges.txt": "judges",
            "other.txt": "other",
        },
    )

    rows = [
        {"_row_name": "Row A", "_papers": ["judges.txt"]},
        {"_row_name": "Row B", "_papers": ["other.txt"]},
    ]

    selected, mode, warnings = _plan_incremental_extraction_documents(
        docs_dir, rows, rows_in_scope={"Row A"}
    )

    assert mode == "papers"
    assert warnings == []
    assert [p.name for p in selected] == ["judges.txt"]


def test_prefix_fallback_when_no_papers(tmp_path):
    """Legacy single-unit-per-file sessions without papers keep filename-prefix matching."""
    docs_dir = tmp_path / "documents"
    _write_docs(
        docs_dir,
        {
            "abc-gamma_348734_full.txt": "protein abc-gamma",
            "unrelated.txt": "skip me",
        },
    )

    rows = [
        {"_row_name": "abc-gamma"},
        {"_row_name": "other-row"},
    ]

    selected, mode, warnings = _plan_incremental_extraction_documents(docs_dir, rows)

    assert mode == "prefix"
    assert warnings == []
    assert [p.name for p in selected] == ["abc-gamma_348734_full.txt"]


def test_missing_paper_emits_warning(tmp_path):
    docs_dir = tmp_path / "documents"
    _write_docs(docs_dir, {"present.txt": "ok"})

    rows = [{"_row_name": "Row A", "_papers": ["missing.pdf"]}]

    selected, mode, warnings = _plan_incremental_extraction_documents(docs_dir, rows)

    assert mode == "papers"
    assert selected == []
    assert len(warnings) == 1
    assert "missing.pdf" in warnings[0]


def test_non_readable_resolved_file_emits_warning(tmp_path):
    docs_dir = tmp_path / "documents"
    docs_dir.mkdir(parents=True)
    (docs_dir / "judges.pdf").write_bytes(b"%PDF-1.4")

    rows = [{"_row_name": "Judge A", "_papers": ["judges.pdf"]}]

    selected, mode, warnings = _plan_incremental_extraction_documents(docs_dir, rows)

    assert mode == "papers"
    assert selected == []
    assert len(warnings) == 1
    assert "cannot be read by the extractor" in warnings[0]
