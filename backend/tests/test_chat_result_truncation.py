"""Regression tests for chat tool-result truncation.

A large get_schema result must never be collapsed into an opaque preview.
Doing so silently reported "0 columns" to the UI and fed the model a JSON
string cut mid-object, which made the agent mishandle column edits (e.g.
claiming rationale was unsupported, or editing every column at once).

truncate_result must keep the high-signal summary keys (column_count,
column_names) and preserve as many full schema entries as the budget allows,
so both the UI label and the model still see an accurate schema.
"""

from __future__ import annotations

import json

import pytest

from app.models.session import (
    ColumnInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.chat.agent_service import ChatAgentService
from app.services.chat.deps import truncate_result
from app.services.chat.tool_executor import ToolExecutor

_LONG_DEF = (
    "A deliberately long column definition sentence used to inflate the "
    "serialized schema payload well beyond the truncation budget so the "
    "truncation branch is actually exercised by this test."
)
_LONG_RATIONALE = (
    "An equally verbose rationale describing why this column matters for the "
    "research query and how its values should be extracted from documents."
)


def _big_schema_result(n: int = 40) -> dict:
    cols = [
        {
            "name": f"column_{i}",
            "definition": _LONG_DEF,
            "rationale": _LONG_RATIONALE,
        }
        for i in range(n)
    ]
    return {
        "query": "some research query",
        "column_count": n,
        "column_names": [c["name"] for c in cols],
        "schema": cols,
        "observation_unit": None,
    }


def test_small_result_is_returned_unchanged():
    payload = {
        "query": "q",
        "column_count": 1,
        "column_names": ["a"],
        "schema": [{"name": "a", "definition": "short"}],
    }
    assert truncate_result(payload) is payload


def test_large_schema_preserves_count_and_names():
    payload = _big_schema_result(40)
    # Sanity check: the payload genuinely exceeds the budget, so we are testing
    # the truncation path and not the pass-through path.
    assert len(json.dumps(payload, ensure_ascii=False)) > 8000

    out = truncate_result(payload)

    assert out["truncated"] is True
    # The two keys the UI label and the agent depend on must survive intact.
    assert out["column_count"] == 40
    assert out["column_names"] == payload["column_names"]
    # The schema list is kept partially (never dropped wholesale), and the
    # number of omitted entries is reported.
    assert isinstance(out.get("schema"), list)
    assert len(out["schema"]) >= 1
    if len(out["schema"]) < 40:
        assert out["schema_omitted"] == 40 - len(out["schema"])


def test_large_list_keeps_partial_items_not_preview():
    big = [{"i": i, "pad": "x" * 200} for i in range(200)]
    out = truncate_result(big)
    assert out["truncated"] is True
    assert "preview" not in out
    assert len(out["items"]) < 200
    assert out["omitted"] == 200 - len(out["items"])


def test_label_reports_real_count_from_truncated_result():
    agent = ChatAgentService()
    truncated = truncate_result(_big_schema_result(40))
    label = agent._tool_done_message("get_schema", truncated)
    assert "40 columns" in label
    assert "(0 columns" not in label


@pytest.fixture
def large_session(session_manager_fixture):
    cols = [
        ColumnInfo(
            name=f"col_{i}",
            definition=_LONG_DEF,
            rationale=_LONG_RATIONALE,
        )
        for i in range(40)
    ]
    session = VisualizationSession(
        id="chat-truncation-session",
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="test"),
        columns=cols,
        schema_query="big query",
    )
    session_manager_fixture.create_session(session)
    return session


@pytest.mark.asyncio
async def test_get_schema_survives_truncation_end_to_end(large_session):
    """The original failure: get_schema on a many-column session reported 0."""
    executor = ToolExecutor()
    result = await executor.execute("get_schema", large_session.id, "schematiq", {})

    assert result["column_count"] == 40
    assert len(result["column_names"]) == 40

    label = ChatAgentService()._tool_done_message("get_schema", result)
    assert "40 columns" in label
    assert "(0 columns" not in label
