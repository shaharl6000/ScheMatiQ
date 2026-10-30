"""Old session JSON still loads, and extraction no longer grows allowed_values."""

import pytest

from app.models.session import (
    ColumnInfo,
    SessionMetadata,
    SessionType,
    VisualizationSession,
)
from app.services.pipeline.value_extraction import process_suggested_values


def _old_session_payload() -> dict:
    return {
        "id": "old-session",
        "type": "schematiq",
        "metadata": {"source": "test"},
        "columns": [
            {
                "name": "court",
                "definition": "Which court decided the case",
                "allowed_values": ["supreme court", "district court"],
                "auto_expand_threshold": 3,
                "pending_values": [
                    {
                        "value": "appellate court",
                        "document_count": 1,
                        "documents": ["opinion.pdf"],
                    }
                ],
            }
        ],
        "schema_suggestions": [
            {
                "column_name": "court",
                "suggested_values": ["appellate court"],
                "value_details": {
                    "appellate court": {
                        "value": "appellate court",
                        "document_count": 1,
                        "documents": ["opinion.pdf"],
                    }
                },
            }
        ],
    }


def test_old_session_json_keeps_pending_values_and_threshold():
    session = VisualizationSession.model_validate(_old_session_payload())
    dumped = session.model_dump(mode="json")

    column = dumped["columns"][0]
    assert column["auto_expand_threshold"] == 3
    assert column["pending_values"][0]["value"] == "appellate court"
    assert dumped["schema_suggestions"][0]["column_name"] == "court"
    assert dumped["schema_suggestions"][0]["suggested_values"] == ["appellate court"]

    again = VisualizationSession.model_validate(dumped)
    assert again.columns[0].auto_expand_threshold == 3
    assert again.columns[0].pending_values[0].value == "appellate court"
    assert again.schema_suggestions[0].column_name == "court"


@pytest.mark.asyncio
async def test_process_suggested_values_does_not_grow_the_list():
    session = VisualizationSession(
        id="old-session",
        type=SessionType.SCHEMATIQ,
        metadata=SessionMetadata(source="test"),
        columns=[
            ColumnInfo(
                name="court",
                allowed_values=["supreme court", "district court"],
                auto_expand_threshold=1,
                pending_values=None,
            )
        ],
    )
    await process_suggested_values(
        session,
        None,
        {"court": {"appellate court": {"count": 5, "documents": ["a.pdf"]}}},
        None,
    )
    column = session.columns[0]
    assert column.allowed_values == ["supreme court", "district court"]
    assert column.pending_values is None
    assert column.auto_expand_threshold == 1
