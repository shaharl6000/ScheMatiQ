"""Tests for known_units construction during re-extraction."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.models.session import (
    ColumnInfo,
    DataStatistics,
    SessionMetadata,
    SessionStatus,
    SessionType,
    SkippedDocumentInfo,
    VisualizationSession,
)
from app.services.reextraction_service import ReextractionService


def _make_service() -> ReextractionService:
    return ReextractionService(
        websocket_manager=MagicMock(),
        session_manager=MagicMock(),
    )


def _session_with_skipped(skipped_names: list[str]) -> VisualizationSession:
    return VisualizationSession(
        id="sess-skipped",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", created=datetime.now()),
        columns=[ColumnInfo(name="Title", definition="Title")],
        statistics=DataStatistics(
            total_rows=1,
            total_columns=1,
            total_documents=2,
            completeness=100.0,
            column_stats=[],
            skipped_documents=[
                SkippedDocumentInfo(document=name, reason="No units found")
                for name in skipped_names
            ],
        ),
    )


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    return work_dir, data_dir


def test_known_units_includes_skipped_documents_as_empty_lists(isolated_dirs):
    """Skipped papers must map to [] so the lib skips LLM unit discovery."""
    work_dir, _ = isolated_dirs
    session_id = "sess-skipped"
    extracted = work_dir / session_id / "extracted_data.jsonl"
    extracted.parent.mkdir(parents=True)
    extracted.write_text(
        json.dumps(
            {
                "_row_name": "Unit A",
                "_papers": ["paper_with_units.txt"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    session = _session_with_skipped(["Amica2025-07-06DDC"])
    service = _make_service()

    known_units = service._build_known_units_for_reextraction(
        session_id, session, rediscover_observation_units=False
    )

    assert known_units["paper_with_units"] == ["Unit A"]
    assert known_units["Amica2025-07-06DDC"] == []


def test_known_units_skipped_document_stem_matches_row_paper_keying(isolated_dirs):
    """Skipped document names with extensions are keyed by stem like row papers."""
    work_dir, _ = isolated_dirs
    session_id = "sess-stem"
    extracted = work_dir / session_id / "extracted_data.jsonl"
    extracted.parent.mkdir(parents=True)
    extracted.write_text(
        json.dumps({"_row_name": "U1", "_papers": ["doc_a.pdf"]}) + "\n",
        encoding="utf-8",
    )

    session = _session_with_skipped(["skipped_doc.pdf"])
    service = _make_service()

    known_units = service._build_known_units_for_reextraction(
        session_id, session, rediscover_observation_units=False
    )

    assert "doc_a" in known_units
    assert known_units["skipped_doc"] == []


def test_known_units_does_not_overwrite_existing_units_with_empty_list(isolated_dirs):
    """A skipped entry must not clobber units already read from extracted rows."""
    work_dir, _ = isolated_dirs
    session_id = "sess-overlap"
    extracted = work_dir / session_id / "extracted_data.jsonl"
    extracted.parent.mkdir(parents=True)
    extracted.write_text(
        json.dumps({"_row_name": "Existing Unit", "_papers": ["overlap_doc"]})
        + "\n",
        encoding="utf-8",
    )

    session = _session_with_skipped(["overlap_doc"])
    service = _make_service()

    known_units = service._build_known_units_for_reextraction(
        session_id, session, rediscover_observation_units=False
    )

    assert known_units["overlap_doc"] == ["Existing Unit"]


def test_known_units_rediscover_mode_ignores_skipped_documents():
    """Observation-unit rediscovery must not seed known_units from skipped docs."""
    session = _session_with_skipped(["Amica2025-07-06DDC"])
    service = _make_service()

    known_units = service._build_known_units_for_reextraction(
        "sess-rediscover", session, rediscover_observation_units=True
    )

    assert known_units == {}


def test_known_units_total_units_count_excludes_empty_list_papers(isolated_dirs):
    """Empty-list skipped papers contribute 0 to observation-unit totals."""
    work_dir, _ = isolated_dirs
    session_id = "sess-total"
    extracted = work_dir / session_id / "extracted_data.jsonl"
    extracted.parent.mkdir(parents=True)
    extracted.write_text(
        json.dumps({"_row_name": "U1", "_papers": ["doc1"]})
        + "\n"
        + json.dumps({"_row_name": "U2", "_papers": ["doc1"]})
        + "\n",
        encoding="utf-8",
    )

    session = _session_with_skipped(["skipped_only"])
    service = _make_service()

    known_units = service._build_known_units_for_reextraction(
        session_id, session, rediscover_observation_units=False
    )

    total_units = sum(len(units) for units in known_units.values())
    assert total_units == 2
    assert known_units["skipped_only"] == []
