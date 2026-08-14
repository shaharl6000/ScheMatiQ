"""Tests for document-scoped availability gating in re-extraction.

A whole-project availability precheck must not block a run that only targets a
specific document (e.g. ``extract_cells`` on a single previously-skipped file).
When a document scope is requested, only the scoped documents' availability
should gate the run.
"""

from unittest.mock import MagicMock

from app.services.reextraction_service import ReextractionService


def _make_service() -> ReextractionService:
    return ReextractionService(
        websocket_manager=MagicMock(),
        session_manager=MagicMock(),
    )


def _availability(local=None, cloud=None, missing=None) -> dict:
    """Shape a precheck_document_availability-style result for tests."""
    return {
        "local_documents": [{"name": n} for n in (local or [])],
        "cloud_documents": [{"name": n} for n in (cloud or [])],
        "missing_documents": [{"name": n} for n in (missing or [])],
        "can_proceed": bool(local or cloud),
    }


def test_scoped_doc_on_disk_is_available_despite_missing_project(tmp_path, monkeypatch):
    """A re-attached skipped file on disk counts as available even when the
    rest of the project is unreachable (the CASA re-attach case)."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-scope"
    # Simulate a "Show source document" re-attach landing in pending_documents/.
    pending = tmp_path / "data" / session_id / "pending_documents"
    pending.mkdir(parents=True)
    (pending / "CASA2025-06-27SCt.txt").write_text("opinion text", encoding="utf-8")

    service = _make_service()
    # Whole project: everything else is missing (mirrors the 102-missing error).
    availability = _availability(missing=[f"doc_{i}" for i in range(101)])

    result = service._scoped_documents_availability(
        session_id, ["CASA2025-06-27SCt"], availability
    )

    assert result["available"] == ["CASA2025-06-27SCt"]
    assert result["missing"] == []


def test_scoped_doc_available_from_cloud_precheck(tmp_path, monkeypatch):
    """A scoped stem the whole-project precheck already classified cloud/local
    counts as available even if it is not on the local filesystem."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-cloud"
    (tmp_path / "data" / session_id).mkdir(parents=True)

    service = _make_service()
    availability = _availability(cloud=["CASA2025-06-27SCt.txt"], missing=["other"])

    result = service._scoped_documents_availability(
        session_id, ["CASA2025-06-27SCt"], availability
    )

    assert result["available"] == ["CASA2025-06-27SCt"]
    assert result["missing"] == []


def test_scoped_doc_truly_missing_is_reported(tmp_path, monkeypatch):
    """A scoped document that is neither on disk nor in the precheck is missing."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-missing"
    (tmp_path / "data" / session_id).mkdir(parents=True)

    service = _make_service()
    availability = _availability(local=["something_else"])

    result = service._scoped_documents_availability(
        session_id, ["CASA2025-06-27SCt"], availability
    )

    assert result["missing"] == ["CASA2025-06-27SCt"]
    assert result["available"] == []
