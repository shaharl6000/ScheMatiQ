"""Tests for the chat agent loop and tool executor scoping.

The focus here is the safety contract that the workspace depends on:

* expensive tools must pause the loop and wait for an explicit confirmation
  rather than running inline;
* a cancelled action must be cleared server-side so a later /confirm cannot run
  it;
* re-extraction scope must never silently widen to the whole table.

These run with stubbed ML deps and a fake Gemini chat (see conftest), so no real
LLM or network call happens.
"""

from __future__ import annotations

import types

import pytest

from app.services.chat.agent_service import ChatAgentService
from app.services.chat.session_store import (
    ChatSessionState,
    PendingToolCall,
    chat_session_store,
)


# --- fakes -----------------------------------------------------------------


class FakeFunctionCall:
    def __init__(self, name: str, args: dict) -> None:
        self.name = name
        self.args = args


class FakeResponse:
    """Mimics a google-genai response: either function_calls or text."""

    def __init__(self, function_calls=None, text: str = "") -> None:
        self.function_calls = function_calls or []
        self.text = text


class FakeChat:
    """Returns scripted responses for each send_message call."""

    def __init__(self, scripted: list[FakeResponse]) -> None:
        self._scripted = list(scripted)
        self.sent: list = []

    async def send_message(self, content):
        self.sent.append(content)
        if self._scripted:
            return self._scripted.pop(0)
        return FakeResponse(text="done")


def _make_state(chat: FakeChat, session_id: str = "sess-1") -> ChatSessionState:
    state = ChatSessionState(
        client=object(),
        chat=chat,
        workspace_session_id=session_id,
        session_mode="schematiq",
    )
    chat_id = chat_session_store.create(state)
    state.chat_id = chat_id
    return state


class RecordingExecutor:
    """Stand-in executor that records calls instead of touching the backend."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, dict]] = []

    async def estimate_cost(self, tool_name, session_id, args) -> str:
        return "Estimated cost: $0.10, 5 API calls."

    async def execute(self, tool_name, session_id, session_mode, args) -> dict:
        self.executed.append((tool_name, args))
        return {"status": "started", "message": f"{tool_name} ran"}


@pytest.fixture
def agent_with_fake_executor(monkeypatch):
    agent = ChatAgentService()
    executor = RecordingExecutor()
    agent._executor = executor
    return agent, executor


# --- the cost gate ---------------------------------------------------------


@pytest.mark.asyncio
async def test_expensive_tool_pauses_and_does_not_execute(agent_with_fake_executor):
    agent, executor = agent_with_fake_executor
    chat = FakeChat([])
    state = _make_state(chat)

    outbound: list[dict] = []
    first = FakeResponse(function_calls=[FakeFunctionCall("reextract", {"columns": ["Age"]})])
    result = await agent._continue_after_tool(state, first, outbound)

    assert result["status"] == "pending_confirmation"
    assert result["pending_action"]["tool_name"] == "reextract"
    # The crucial safety property: the expensive tool was NOT executed.
    assert executor.executed == []
    # ...and it was stashed as pending for an explicit confirm.
    assert state.pending is not None
    assert state.pending.tool_name == "reextract"


@pytest.mark.asyncio
async def test_cheap_tool_runs_inline_without_gate(agent_with_fake_executor):
    agent, executor = agent_with_fake_executor
    # First response asks for a cheap read; second response is the final text.
    chat = FakeChat([FakeResponse(text="Here is the schema.")])
    state = _make_state(chat)

    outbound: list[dict] = []
    first = FakeResponse(function_calls=[FakeFunctionCall("get_schema", {})])
    result = await agent._continue_after_tool(state, first, outbound)

    assert result["status"] == "complete"
    assert ("get_schema", {}) in executor.executed
    assert state.pending is None


# --- confirm / cancel ------------------------------------------------------


@pytest.mark.asyncio
async def test_confirm_runs_the_pending_action(agent_with_fake_executor):
    agent, executor = agent_with_fake_executor
    # After the tool result is sent back, the model replies with final text.
    chat = FakeChat([FakeResponse(text="Re-extraction started.")])
    state = _make_state(chat)
    state.pending = PendingToolCall(
        tool_name="reextract",
        args={"columns": ["Age"]},
        function_call_part=FakeFunctionCall("reextract", {"columns": ["Age"]}),
    )

    result = await agent.confirm_pending(state.workspace_session_id, state.chat_id)

    assert result["status"] == "complete"
    assert ("reextract", {"columns": ["Age"]}) in executor.executed
    # Pending is cleared once confirmed so it cannot run twice.
    assert state.pending is None


@pytest.mark.asyncio
async def test_confirm_without_pending_raises(agent_with_fake_executor):
    agent, _ = agent_with_fake_executor
    chat = FakeChat([])
    state = _make_state(chat)
    state.pending = None

    with pytest.raises(ValueError):
        await agent.confirm_pending(state.workspace_session_id, state.chat_id)


@pytest.mark.asyncio
async def test_cancel_clears_pending_so_confirm_cannot_run_it(agent_with_fake_executor):
    agent, executor = agent_with_fake_executor
    chat = FakeChat([FakeResponse(text="Understood.")])
    state = _make_state(chat)
    state.pending = PendingToolCall(
        tool_name="run_schematiq",
        args={},
        function_call_part=FakeFunctionCall("run_schematiq", {}),
    )

    cancel_result = await agent.cancel_pending(state.workspace_session_id, state.chat_id)
    assert cancel_result["status"] == "complete"
    assert state.pending is None
    assert executor.executed == []
    # _abort_pending must unblock Gemini by sending the cancelled function response.
    assert len(chat.sent) == 1

    # A stale /confirm after a cancel must now error instead of executing.
    with pytest.raises(ValueError):
        await agent.confirm_pending(state.workspace_session_id, state.chat_id)
    assert executor.executed == []


@pytest.mark.asyncio
async def test_abort_pending_drains_gemini_turn_without_executing_tool(
    agent_with_fake_executor,
):
    """Held state.pending is cleared and Gemini is notified; the tool never runs."""
    agent, executor = agent_with_fake_executor
    chat = FakeChat([FakeResponse(text="Okay, I will not re-extract.")])
    state = _make_state(chat)
    state.pending = PendingToolCall(
        tool_name="reextract",
        args={"columns": ["Age"]},
        function_call_part=FakeFunctionCall("reextract", {"columns": ["Age"]}),
    )

    messages, new_pending = await agent._abort_pending(
        state,
        reason="User declined confirmation.",
    )

    assert state.pending is None
    assert new_pending is None
    assert executor.executed == []
    assert len(chat.sent) == 1
    assert any(msg.get("role") == "assistant" for msg in messages)


@pytest.mark.asyncio
async def test_send_message_aborts_held_pending_before_new_turn(
    agent_with_fake_executor,
    monkeypatch,
):
    """A new user message must drop a held confirmation so /confirm cannot run later."""
    agent, executor = agent_with_fake_executor
    chat = FakeChat([FakeResponse(text="Noted.")])
    state = _make_state(chat)
    state.pending = PendingToolCall(
        tool_name="reextract",
        args={"columns": ["Age"]},
        function_call_part=FakeFunctionCall("reextract", {"columns": ["Age"]}),
    )

    run_loop_calls: list[str] = []

    async def fake_run_loop(st, user_text, outbound_messages):
        run_loop_calls.append(user_text)
        outbound_messages.append(agent._text_message("loop done"))
        return {"status": "complete"}

    monkeypatch.setattr(agent, "_run_loop", fake_run_loop)

    result = await agent.send_message(
        session_id=state.workspace_session_id,
        message="never mind",
        session_mode="schematiq",
        chat_id=state.chat_id,
    )

    assert result["status"] == "complete"
    assert state.pending is None
    assert run_loop_calls == ["never mind"]
    assert executor.executed == []

    with pytest.raises(ValueError):
        await agent.confirm_pending(state.workspace_session_id, state.chat_id)


@pytest.mark.asyncio
async def test_cancel_is_idempotent_when_nothing_pending(agent_with_fake_executor):
    agent, _ = agent_with_fake_executor
    chat = FakeChat([])
    state = _make_state(chat)
    state.pending = None

    result = await agent.cancel_pending(state.workspace_session_id, state.chat_id)
    assert result["status"] == "complete"
    assert result["messages"] == []


@pytest.mark.asyncio
async def test_cancel_wrong_session_raises(agent_with_fake_executor):
    agent, _ = agent_with_fake_executor
    chat = FakeChat([])
    state = _make_state(chat, session_id="sess-A")

    with pytest.raises(ValueError):
        await agent.cancel_pending("sess-B", state.chat_id)


# --- affected-column scoping (chat edit mirrors a manual edit) -------------


def test_affected_columns_for_schema_edits():
    fn = ChatAgentService._affected_columns
    assert fn("add_column", {"name": "Age"}) == ["Age"]
    assert fn("edit_column", {"old_name": "A", "new_name": "B"}) == ["B"]
    assert fn("edit_column", {"old_name": "A"}) == ["A"]
    assert fn("merge_columns", {"target_name": "T", "column_a": "X"}) == ["T"]
    # A cell edit is a leaf: it touches no schema column, so no follow-up scope.
    assert fn("update_cell", {"row": 1, "column": "Age", "value": "30"}) == []
    assert fn("delete_column", {"name": "Age"}) == []


# --- re-extraction scope: the "no silent widening" guarantee ---------------


def _reextraction_service_with_columns(monkeypatch, column_names, changed=None, new=None):
    from app.services.reextraction_service import ReextractionService

    service = ReextractionService(
        websocket_manager=types.SimpleNamespace(),
        session_manager=types.SimpleNamespace(),
    )
    columns = [types.SimpleNamespace(name=name) for name in column_names]
    session = types.SimpleNamespace(columns=columns)
    service.session_manager.get_session = lambda _sid: session  # type: ignore[attr-defined]
    monkeypatch.setattr(
        service,
        "detect_schema_changes",
        lambda _session: {
            "has_changes": bool(changed or new),
            "changed_columns": list(changed or []),
            "new_columns": list(new or []),
        },
    )
    return service


@pytest.mark.asyncio
async def test_explicit_columns_are_scoped_and_validated(monkeypatch):
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Name", "City"])
    resolved = await service.resolve_reextraction_columns("s1", columns=["Age"])
    assert resolved == ["Age"]


@pytest.mark.asyncio
async def test_unknown_columns_raise_rather_than_widen(monkeypatch):
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Name"])
    with pytest.raises(ValueError):
        await service.resolve_reextraction_columns("s1", columns=["Nonexistent"])


@pytest.mark.asyncio
async def test_edited_only_with_no_changes_raises_not_widens(monkeypatch):
    # The whole point of §6c.1: edited_only with nothing edited must NOT fall
    # back to re-extracting the entire table.
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Name"], changed=[], new=[])
    with pytest.raises(ValueError):
        await service.resolve_reextraction_columns("s1", scope="edited_only")


@pytest.mark.asyncio
async def test_edited_only_targets_just_the_changed_columns(monkeypatch):
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Name", "City"], changed=["Name"])
    resolved = await service.resolve_reextraction_columns("s1", scope="edited_only")
    assert resolved == ["Name"]


@pytest.mark.asyncio
async def test_scope_all_targets_every_column(monkeypatch):
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Name", "City"])
    resolved = await service.resolve_reextraction_columns("s1", scope="all")
    assert set(resolved) == {"Age", "Name", "City"}


@pytest.mark.asyncio
async def test_excerpt_columns_excluded_from_targets(monkeypatch):
    service = _reextraction_service_with_columns(monkeypatch, ["Age", "Age_excerpt"])
    resolved = await service.resolve_reextraction_columns("s1", scope="all")
    assert resolved == ["Age"]


# --- batched tool calls ----------------------------------------------------


@pytest.mark.asyncio
async def test_batched_cheap_calls_all_execute(agent_with_fake_executor):
    """When the model emits several cheap calls in one turn, all run (not just the
    first). Regression: previously only function_calls[:1] ran, so filling N cells
    via a batch of update_cell left N-1 cells silently unwritten."""
    agent, executor = agent_with_fake_executor
    chat = FakeChat([FakeResponse(text="Filled all rows.")])
    state = _make_state(chat)

    rows = [
        ("John C. Coughenour", "Other Republican"),
        ("Judge Canby", "Democratic"),
        ("Judge Forrest", "Trump"),
        ("Judge M. Smith", "Other Republican"),
        ("Clay D. Land", "Other Republican"),
    ]
    calls = [
        FakeFunctionCall("update_cell", {"row": r, "column": "appointee", "value": v})
        for r, v in rows
    ]
    outbound: list[dict] = []
    result = await agent._continue_after_tool(state, FakeResponse(function_calls=calls), outbound)

    assert result["status"] == "complete"
    executed_rows = [args["row"] for name, args in executor.executed if name == "update_cell"]
    assert executed_rows == [r for r, _ in rows]  # all five, in order
    # A single combined function-response message carries one part per call.
    assert len(chat.sent) == 1
    assert isinstance(chat.sent[0], list) and len(chat.sent[0]) == 5


@pytest.mark.asyncio
async def test_batch_with_expensive_still_pauses(agent_with_fake_executor):
    """A batch containing an expensive tool must still gate on confirmation and not
    execute anything inline."""
    agent, executor = agent_with_fake_executor
    chat = FakeChat([])
    state = _make_state(chat)

    calls = [
        FakeFunctionCall("reextract", {"columns": ["Age"]}),
        FakeFunctionCall("update_cell", {"row": "x", "column": "c", "value": "v"}),
    ]
    outbound: list[dict] = []
    result = await agent._continue_after_tool(state, FakeResponse(function_calls=calls), outbound)

    assert result["status"] == "pending_confirmation"
    assert result["pending_action"]["tool_name"] == "reextract"
    assert executor.executed == []
    assert state.pending is not None and state.pending.tool_name == "reextract"
