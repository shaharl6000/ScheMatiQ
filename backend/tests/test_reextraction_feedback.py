"""``feedback`` threading through the gated re-extraction entry point.

start_gated_reextraction(feedback=...) must forward the note into the
operation it hands to start_reextraction; omitting it (every caller other
than the "Wrong, try again" menu item) must leave it None.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.session import (
    ColumnInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.reextraction_service import ReextractionOperation, ReextractionService


def _session() -> VisualizationSession:
    return VisualizationSession(
        id="sess-1",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", created=datetime.now()),
        columns=[ColumnInfo(name="col", definition="")],
    )


def _service_with_mocks() -> ReextractionService:
    session_manager = MagicMock()
    session_manager.get_session.return_value = _session()

    service = ReextractionService(MagicMock(), session_manager)
    service.capture_and_save_baseline = AsyncMock()
    discovery = {
        "total_rows": 0,
        "rows_with_papers": 0,
        "available_papers": [],
        "missing_papers": [],
        "paper_to_rows": {},
        "cloud_papers": {},
        "local_papers": [],
        "session_document_count": 0,
    }
    service.discover_papers = AsyncMock(return_value=discovery)
    service.precheck_document_availability = AsyncMock(
        return_value={"can_proceed": True}
    )
    service.start_reextraction = AsyncMock(return_value={"status": "started"})
    return service


@pytest.mark.asyncio
async def test_feedback_forwarded_to_start_reextraction():
    service = _service_with_mocks()

    await service.start_gated_reextraction(
        "sess-1", columns=["col"], rows=["row1"], only_empty=False,
        feedback="wrong, try again",
    )

    assert service.start_reextraction.await_args.kwargs["feedback"] == "wrong, try again"


@pytest.mark.asyncio
async def test_no_feedback_is_unchanged():
    service = _service_with_mocks()

    await service.start_gated_reextraction("sess-1", columns=["col"])

    assert service.start_reextraction.await_args.kwargs["feedback"] is None


def test_operation_defaults_feedback_to_none():
    operation = ReextractionOperation(
        operation_id="op-1", session_id="sess-1", columns=["col"],
    )
    assert operation.feedback is None


def test_operation_stores_feedback():
    operation = ReextractionOperation(
        operation_id="op-1", session_id="sess-1", columns=["col"],
        feedback="wrong, try again",
    )
    assert operation.feedback == "wrong, try again"
