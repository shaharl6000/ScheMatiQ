"""Tests for document-scoped availability gating in re-extraction.

A whole-project availability precheck must not block a run that only targets a
specific document (e.g. ``extract_cells`` on a single previously-skipped file).
When a document scope is requested, only the scoped documents' availability
should gate the run.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

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


# --------------------------------------------------------------------------- #
# only_empty narrowing must preserve previously-skipped docs (end-to-end).
#
# extract_cells defaults only_empty=True. A previously-skipped document has no
# rows, so the empty-cell scan finds nothing for it; without special handling
# the run would raise "No empty cells to fill" before reaching the availability
# gate, so re-discovering a skipped document (the CASA case) would be blocked.
# --------------------------------------------------------------------------- #

def _session_with_skipped(skipped_names: list[str]) -> VisualizationSession:
    return VisualizationSession(
        id="sess-skip",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", created=datetime.now()),
        columns=[ColumnInfo(name="ruling_type", definition="Ruling")],
        statistics=DataStatistics(
            total_rows=0,
            total_columns=1,
            total_documents=1,
            completeness=0.0,
            column_stats=[],
            skipped_documents=[
                SkippedDocumentInfo(document=n, reason="Broader legal ruling")
                for n in skipped_names
            ],
        ),
    )


def _wire(service: ReextractionService, availability: dict) -> None:
    service.capture_and_save_baseline = AsyncMock()
    service.resolve_reextraction_columns = AsyncMock(return_value=["ruling_type"])
    service.discover_papers = AsyncMock(return_value={
        "total_rows": 0, "rows_with_papers": 0, "available_papers": [],
        "missing_papers": [], "paper_to_rows": {}, "cloud_papers": {},
        "local_papers": [], "session_document_count": 0,
    })
    service.precheck_document_availability = AsyncMock(return_value=availability)
    service.start_reextraction = AsyncMock(return_value={"status": "started"})


async def test_only_empty_scoped_skipped_doc_proceeds():
    """extract_cells (only_empty=True) on a previously-skipped document must NOT
    raise 'No empty cells' — it has no rows and is re-discovered from scratch."""
    session = _session_with_skipped(["CASA2025-06-27SCt"])
    sm = MagicMock()
    sm.get_session.return_value = session
    service = ReextractionService(MagicMock(), sm)
    _wire(service, {
        "local_documents": [{"name": "CASA2025-06-27SCt"}],
        "cloud_documents": [], "missing_documents": [], "can_proceed": True,
    })
    service._project_document_stems = MagicMock(return_value={"CASA2025-06-27SCt"})
    from app.services.reextraction_service import _OnlyEmptyScan
    service._scan_only_empty_scope = MagicMock(
        return_value=_OnlyEmptyScan(True, set(), set(), False, False, {})
    )

    result = await service.start_gated_reextraction(
        "sess-skip", columns=None, scope="all",
        documents=["CASA2025-06-27SCt"], only_empty=True,
    )

    assert result == {"status": "started"}
    service.start_reextraction.assert_awaited_once()
    assert service.start_reextraction.await_args.kwargs["documents"] == ["CASA2025-06-27SCt"]


async def test_only_empty_scoped_nonskipped_doc_with_no_gaps_still_raises():
    """A non-skipped scoped doc with no empty cells keeps the original guard."""
    session = _session_with_skipped([])  # nothing skipped
    sm = MagicMock()
    sm.get_session.return_value = session
    service = ReextractionService(MagicMock(), sm)
    _wire(service, {
        "local_documents": [{"name": "SomeDoc"}],
        "cloud_documents": [], "missing_documents": [], "can_proceed": True,
    })
    service._project_document_stems = MagicMock(return_value={"SomeDoc"})
    from app.services.reextraction_service import _OnlyEmptyScan
    service._scan_only_empty_scope = MagicMock(
        return_value=_OnlyEmptyScan(True, set(), set(), False, False, {})
    )

    raised = False
    try:
        await service.start_gated_reextraction(
            "sess-skip", columns=None, scope="all",
            documents=["SomeDoc"], only_empty=True,
        )
    except ValueError as e:
        raised = "No empty cells" in str(e)
    assert raised
    service.start_reextraction.assert_not_awaited()
