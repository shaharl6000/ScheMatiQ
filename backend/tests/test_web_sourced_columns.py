"""Focused tests for opt-in, grounded web column enrichment."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from app.models.session import ColumnInfo
import app.services.web_enrichment_service as web_module
from app.services.reextraction_service import ReextractionService
from app.services.schema_baseline import (
    build_schema_baseline,
    calculate_column_checksum,
)
from app.services.web_enrichment_service import WebEnrichmentService
from schematiq.core.llm_backends import GeminiLLM


def test_orchestration_wires_web_enrichment_to_reextraction():
    from app.services import orchestration

    assert (
        orchestration.reextraction_service._web_enrichment_service
        is orchestration.web_enrichment_service
    )


def test_column_strategy_is_backward_compatible_and_changes_checksum():
    document = ColumnInfo(name="party", definition="Appointing party")
    web = ColumnInfo(
        name="party",
        definition="Appointing party",
        extraction_strategy="web",
    )

    legacy = hashlib.md5("Appointing party".encode()).hexdigest()
    assert document.extraction_strategy == "document"
    assert calculate_column_checksum(document) == legacy
    assert calculate_column_checksum(web) != legacy

    baseline = build_schema_baseline(
        [document, ColumnInfo(name="party_excerpt", definition="Source excerpt")]
    )
    assert list(baseline.columns) == ["party"]
    assert baseline.columns["party"].checksum == legacy


def test_grounding_source_extraction_deduplicates_urls():
    chunks = [
        SimpleNamespace(web=SimpleNamespace(uri="https://example.com/a", title="A")),
        SimpleNamespace(web=SimpleNamespace(uri="https://example.com/a", title="Again")),
        SimpleNamespace(web=SimpleNamespace(uri="https://example.com/b", title="B")),
    ]
    candidate = SimpleNamespace(
        grounding_metadata=SimpleNamespace(grounding_chunks=chunks)
    )

    assert GeminiLLM._extract_grounding_sources(candidate) == [
        {"url": "https://example.com/a", "title": "A"},
        {"url": "https://example.com/b", "title": "B"},
    ]


@pytest.mark.asyncio
async def test_web_enrichment_caches_entity_and_writes_url_provenance(
    tmp_path, monkeypatch
):
    data_file = tmp_path / "data.jsonl"
    rows = [
        {
            "row_name": "Jane Doe",
            "court": "Example Circuit",
            "case": "Case One",
            "data": {"party": None},
        },
        {
            "row_name": "Jane Doe",
            "court": "Example Circuit",
            "case": "Case Two",
            "data": {"party": None},
        },
    ]
    data_file.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    async def resolve_files(_session_id):
        return [data_file]

    async def persist(_session_id, _path):
        return None

    monkeypatch.setattr(web_module, "resolve_session_data_files", resolve_files)
    monkeypatch.setattr(web_module, "persist_session_data_file", persist)

    class FakeLLM:
        def __init__(self):
            self.calls = 0

        def generate_grounded(self, _prompt, **_kwargs):
            self.calls += 1
            return (
                '{"value": "democratic"}',
                [{"url": "https://example.com/source", "title": "Example"}],
            )

    llm = FakeLLM()
    service = WebEnrichmentService(session_manager=SimpleNamespace())
    column = ColumnInfo(
        name="party",
        definition="Appointing party",
        allowed_values=["Democratic", "Republican"],
        extraction_strategy="web",
    )

    stats = await service.enrich_columns("session", [column], llm)

    saved = [json.loads(line) for line in data_file.read_text().splitlines()]
    assert llm.calls == 1
    assert stats == {"lookups": 1, "cache_hits": 1, "updated_cells": 2}
    assert [row["data"]["party"]["answer"] for row in saved] == [
        "Democratic",
        "Democratic",
    ]
    assert all(row["_cell_status"]["party"] == "external_source" for row in saved)
    assert "https://example.com/source" in saved[0]["data"]["party"]["excerpts"][0]["text"]
