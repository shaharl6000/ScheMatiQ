"""Tests for restoring a runnable schematiq_config.json on import.

A complete-export project.json already carries the query, both LLM backends
(under ``llm_configuration``), and ``documents_batch_size``. On import these must
be reassembled into ``<data_dir>/<session_id>/schematiq_config.json`` — the exact
file and key shape the re-extraction path reads
(``ReextractionService._get_llm_from_session``) — so a re-imported project is
extraction-capable with its ORIGINAL backends instead of RELEASE_CONFIG defaults
synthesized at rediscover time.

These drive the public ``FileParser.parse_file`` so they exercise the real import
path end to end.
"""

from __future__ import annotations

import json

import pytest

from app.services.file_parser import FileParser


def _write_export(session_dir, payload):
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "project.json").write_text(json.dumps(payload))


def _complete_export():
    return {
        "query": "extract drug interactions",
        "schema": {
            "columns": [
                {"name": "drug", "definition": "the drug", "rationale": ""},
            ]
        },
        "llm_configuration": {
            "schema_creation_backend": {
                "provider": "openai",
                "model": "gpt-4o",
                "temperature": 0,
            },
            "value_extraction_backend": {
                "provider": "gemini",
                "model": "gemini-3-flash",
                "temperature": 0,
            },
        },
        "metadata": {"documents_batch_size": 4, "total_documents": 3},
        "data": [
            {"row_name": "r1", "papers": ["a.pdf"], "data": {"drug": "aspirin"}},
        ],
    }


@pytest.mark.asyncio
async def test_complete_export_reassembles_runnable_config(tmp_path):
    fp = FileParser(data_dir=str(tmp_path))
    sid = "sess-complete"
    _write_export(tmp_path / sid, _complete_export())

    await fp.parse_file(sid)

    cfg = json.loads((tmp_path / sid / "schematiq_config.json").read_text())
    assert cfg["documents_batch_size"] == 4
    assert cfg["query"] == "extract drug interactions"
    assert cfg["schema_creation_backend"]["provider"] == "openai"
    assert cfg["value_extraction_backend"]["model"] == "gemini-3-flash"


@pytest.mark.asyncio
async def test_old_export_without_backends_stays_backward_compatible(tmp_path):
    # An export that predates llm_configuration: only documents_batch_size is
    # persisted, exactly as before. No backends invented.
    fp = FileParser(data_dir=str(tmp_path))
    sid = "sess-old"
    payload = _complete_export()
    payload.pop("llm_configuration")
    _write_export(tmp_path / sid, payload)

    await fp.parse_file(sid)

    cfg = json.loads((tmp_path / sid / "schematiq_config.json").read_text())
    assert cfg["documents_batch_size"] == 4
    assert "schema_creation_backend" not in cfg
    assert "value_extraction_backend" not in cfg


@pytest.mark.asyncio
async def test_config_merge_preserves_unrelated_existing_keys(tmp_path):
    # A pre-existing config on disk keeps its unrelated fields; the export's
    # values win for the fields it carries (it is authoritative for the import).
    fp = FileParser(data_dir=str(tmp_path))
    sid = "sess-merge"
    session_dir = tmp_path / sid
    _write_export(session_dir, _complete_export())
    (session_dir / "schematiq_config.json").write_text(
        json.dumps({"max_keys_schema": 100, "documents_batch_size": 99})
    )

    await fp.parse_file(sid)

    cfg = json.loads((session_dir / "schematiq_config.json").read_text())
    assert cfg["max_keys_schema"] == 100          # unrelated key preserved
    assert cfg["documents_batch_size"] == 4        # export value wins
    assert cfg["schema_creation_backend"]["provider"] == "openai"
