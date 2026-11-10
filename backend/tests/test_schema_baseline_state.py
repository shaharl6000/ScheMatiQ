"""Tests for SchemaBaseline rename tracking, reconciliation, and change detection."""

from unittest.mock import AsyncMock

from app.models.session import (
    ColumnInfo,
    SchemaBaseline,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.reextraction_service import ReextractionService


def test_schema_baseline_chained_rename_and_change_detection(session_manager_fixture):
    ws = AsyncMock()
    service = ReextractionService(ws, session_manager_fixture)

    session = VisualizationSession(
        id="baseline-rename-test",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[
            ColumnInfo(
                name="judge_name",
                definition="Name of the presiding judge",
                rationale="Identify decision maker",
                data_type="object",
                non_null_count=5,
            )
        ],
    )
    session.schema_baseline = SchemaBaseline.from_columns(session.columns)
    session_manager_fixture.create_session(session)

    # First rename: judge_name -> presiding_judge
    assert session.schema_baseline.record_column_rename("judge_name", "presiding_judge") is True
    session.columns[0].name = "presiding_judge"

    # Chained rename before re-extraction: presiding_judge -> court_judge
    assert session.schema_baseline.record_column_rename("presiding_judge", "court_judge") is True
    session.columns[0].name = "court_judge"

    changes = service.detect_schema_changes(session)
    assert changes["has_changes"] is True
    assert changes["changed_columns"] == ["court_judge"]
    detail = changes["column_changes"]["court_judge"]
    assert detail["change_type"] == "renamed"
    assert detail["old_name"] == "judge_name"
    assert detail["old_value"] == "judge_name"
    assert detail["new_value"] == "court_judge"


def test_schema_baseline_reconciles_extracted_columns_without_reporting_new(
    session_manager_fixture,
):
    ws = AsyncMock()
    service = ReextractionService(ws, session_manager_fixture)

    col_existing = ColumnInfo(
        name="outcome",
        definition="Case outcome",
        rationale="Track result",
        data_type="object",
        non_null_count=10,
    )
    baseline = SchemaBaseline.from_columns([col_existing])

    # Add two columns not in baseline:
    # 1. `discovered_col` already has non_null_count > 0 (e.g. from continue-discovery)
    # 2. `user_added_col` has non_null_count == 0 (genuinely new unextracted column)
    col_discovered = ColumnInfo(
        name="discovered_col",
        definition="Discovered in continue discovery",
        rationale="Extra info",
        data_type="object",
        non_null_count=4,
    )
    col_user_new = ColumnInfo(
        name="user_added_col",
        definition="User added column",
        rationale="Needs extraction",
        data_type="object",
        non_null_count=0,
    )

    session = VisualizationSession(
        id="baseline-reconcile-test",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=[col_existing, col_discovered, col_user_new],
        schema_baseline=baseline,
    )
    session_manager_fixture.create_session(session)

    changes = service.detect_schema_changes(session)
    assert "discovered_col" in session.schema_baseline.columns
    assert changes["new_columns"] == ["user_added_col"]
    assert "discovered_col" not in changes["column_changes"]
    assert changes["has_changes"] is True
