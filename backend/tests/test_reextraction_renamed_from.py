"""Tests that renamed_from is captured before baseline recapture."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.modification import ModificationAction
from app.models.session import (
    ColumnBaseline,
    ColumnInfo,
    SchemaBaseline,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.reextraction_service import ReextractionService


def _session_with_rename() -> VisualizationSession:
    return VisualizationSession(
        id="sess-1",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test", created=datetime.now()),
        columns=[ColumnInfo(name="judge_name", definition="Name")],
        schema_baseline=SchemaBaseline(
            columns={
                "judge_name": ColumnBaseline(
                    name="appointing_president",
                    definition="",
                    rationale="",
                    checksum="abc",
                )
            },
            captured_at=datetime.now(),
        ),
        modification_history=[
            ModificationAction(
                action_type="column_edited",
                column_name="judge_name",
                details={
                    "original_name": "appointing_president",
                    "new_name": "judge_name",
                },
            )
        ],
    )


@pytest.mark.asyncio
async def test_gated_reextraction_collects_renamed_from_before_baseline_recapture():
    session = _session_with_rename()
    session_manager = MagicMock()
    session_manager.get_session.return_value = session

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

    await service.start_gated_reextraction("sess-1", columns=["judge_name"])

    service.start_reextraction.assert_awaited_once_with(
        "sess-1",
        ["judge_name"],
        renamed_from={"judge_name": "appointing_president"},
        paper_discovery=discovery,
        documents=None,
        rows=None,
        only_empty=False,
        only_empty_targets=None,
    )
    service.capture_and_save_baseline.assert_awaited_once_with("sess-1")
    assert service.start_reextraction.await_args.kwargs["renamed_from"] == {
        "judge_name": "appointing_president"
    }
