"""Tests for per-run figure gating in TableBuilder and build_table_jsonl."""

import json
from unittest.mock import MagicMock

from schematiq.core.results import ExtractionResult
from schematiq.value_extraction.main import build_table_jsonl


def test_build_table_jsonl_respects_enable_figures_flag(tmp_path, monkeypatch):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "paper1.txt").write_text("Some legal ruling text.", encoding="utf-8")
    fig_dir = docs_dir / "figures" / "paper1"
    fig_dir.mkdir(parents=True)
    (fig_dir / "manifest.json").write_text(json.dumps({"figures": []}), encoding="utf-8")

    schema_path = tmp_path / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "query": "What did the court rule?",
                "schema": [{"name": "outcome", "definition": "Case outcome"}],
                "observation_unit": {"name": "Case", "definition": "One court decision"},
            }
        ),
        encoding="utf-8",
    )

    seen_figures_dirs = []

    def fake_extract_with_units(self, **kwargs):
        seen_figures_dirs.append(kwargs.get("figures_dir"))
        return ExtractionResult(rows=[{"_unit_name": "Case 1", "outcome": "affirmed"}])

    monkeypatch.setattr(
        "schematiq.value_extraction.core.paper_processor.PaperProcessor.extract_values_for_paper_with_units",
        fake_extract_with_units,
    )

    out_disabled = tmp_path / "out_disabled.jsonl"
    build_table_jsonl(
        schema_path=schema_path,
        docs_directories=[docs_dir],
        output_path=out_disabled,
        llm=MagicMock(),
        enable_figures=False,
    )

    out_enabled = tmp_path / "out_enabled.jsonl"
    build_table_jsonl(
        schema_path=schema_path,
        docs_directories=[docs_dir],
        output_path=out_enabled,
        llm=MagicMock(),
    )

    assert seen_figures_dirs == [None, fig_dir]

