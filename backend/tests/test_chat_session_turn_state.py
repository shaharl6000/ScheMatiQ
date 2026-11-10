"""Unit tests for ChatSessionState turn and pending-confirmation lifecycle."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.chat.agent_service import ChatAgentService
from app.services.chat.session_store import ChatSessionState, chat_session_store
from schematiq.core.llm_call_tracker import QuotaExceededError


def _make_state(session_id: str = "sess-chat") -> ChatSessionState:
    chat = MagicMock()
    chat.send_message = AsyncMock(
        return_value=SimpleNamespace(text="Done", function_calls=[])
    )
    chat.get_history = MagicMock(return_value=[])
    state = ChatSessionState(
        client=MagicMock(),
        chat=chat,
        workspace_session_id=session_id,
        session_mode="schematiq",
        model="gemini-3.8-flash",
    )
    chat_id = chat_session_store.create(state)
    state.chat_id = chat_id
    return state


def test_chat_session_state_turn_and_confirmation_helpers():
    state = _make_state("sess-helpers")
    try:
        state.request_stop()
        assert state.stop_requested is True
        state.begin_turn()
        assert state.stop_requested is False

        pending = state.set_pending_confirmation("reextract", {"columns": ["a"]}, "fc")
        assert state.pending is pending
        assert state.consume_pending_confirmation() is pending
        assert state.pending is None

        state.record_llm_call()
        state.record_llm_call()
        assert state.take_pending_llm_calls() == 2
        assert state.pending_llm_calls == 0
    finally:
        if state.chat_id:
            chat_session_store.delete(state.chat_id)


@pytest.mark.asyncio
async def test_send_message_resets_stop_requested_before_aborting_pending(monkeypatch):
    svc = ChatAgentService()
    monkeypatch.setattr(svc, "_message_cap_reached", lambda _sid: False)
    monkeypatch.setattr(svc, "_record_message_used", AsyncMock())
    monkeypatch.setattr(svc, "_ensure_quota_available", AsyncMock())
    monkeypatch.setattr(svc, "_persist_history", AsyncMock())
    monkeypatch.setattr(svc, "_flush_llm_usage", AsyncMock())
    monkeypatch.setattr(
        "app.services.chat.agent_service. types_part",
        None,
        raising=False,
    )
    monkeypatch.setattr(
        svc,
        "_function_response_part",
        lambda tool_name, res: {"tool": tool_name, "result": res},
    )

    state = _make_state("sess-abort-stop")
    try:
        state.set_pending_confirmation("reextract", {"columns": ["col1"]}, "fc_part")
        # Stale stop_requested left over from a stop clicked while pending confirmation was shown.
        state.request_stop()

        observed_stop_during_abort = []

        async def fake_send_chat_message(st, content, outbound):
            observed_stop_during_abort.append(st.stop_requested)
            return SimpleNamespace(text="Acknowledged", function_calls=[])

        monkeypatch.setattr(svc, "_send_chat_message", fake_send_chat_message)

        result = await svc.send_message(
            session_id="sess-abort-stop",
            message="Actually do something else",
            session_mode="schematiq",
            chat_id=state.chat_id,
        )

        # Both the _abort_pending call and the new user message saw stop_requested == False.
        assert observed_stop_during_abort == [False, False]
        assert result["status"] == "complete"
        assert state.pending is None
    finally:
        if state.chat_id:
            chat_session_store.delete(state.chat_id)


@pytest.mark.asyncio
async def test_confirm_pending_discards_chat_when_tool_raises_quota_exceeded(monkeypatch):
    svc = ChatAgentService()
    monkeypatch.setattr(svc, "_ensure_quota_available", AsyncMock())
    monkeypatch.setattr(svc, "_flush_llm_usage", AsyncMock())

    state = _make_state("sess-quota-confirm")
    chat_id = state.chat_id
    state.set_pending_confirmation("reextract", {"columns": ["col1"]}, "fc_part")

    monkeypatch.setattr(
        svc._executor,
        "execute",
        AsyncMock(side_effect=QuotaExceededError(used=1000, limit=1000)),
    )

    result = await svc.confirm_pending("sess-quota-confirm", chat_id)
    assert result["status"] == "complete"
    # Because pending was consumed without sending a function_response, the live
    # chat must be discarded from chat_session_store so no dangling function_call remains.
    assert chat_session_store.get(chat_id) is None


@pytest.mark.asyncio
async def test_send_message_honors_stop_pressed_during_abort_pending(monkeypatch):
    svc = ChatAgentService()
    monkeypatch.setattr(svc, "_message_cap_reached", lambda _sid: False)
    monkeypatch.setattr(svc, "_record_message_used", AsyncMock())
    monkeypatch.setattr(svc, "_ensure_quota_available", AsyncMock())
    monkeypatch.setattr(svc, "_persist_history", AsyncMock())
    flush_mock = AsyncMock()
    monkeypatch.setattr(svc, "_flush_llm_usage", flush_mock)
    monkeypatch.setattr(
        svc,
        "_function_response_part",
        lambda tool_name, res: {"tool": tool_name, "result": res},
    )

    state = _make_state("sess-stop-during-abort")
    chat_id = state.chat_id
    state.set_pending_confirmation("reextract", {"columns": ["col1"]}, "fc_part")

    calls = []

    async def fake_send_chat_message(st, content, outbound):
        calls.append(content)
        # Simulate user pressing Stop while the abort_pending response is in flight
        st.request_stop()
        return SimpleNamespace(text="Aborted", function_calls=[])

    monkeypatch.setattr(svc, "_send_chat_message", fake_send_chat_message)

    result = await svc.send_message(
        session_id="sess-stop-during-abort",
        message="Do a new task",
        session_mode="schematiq",
        chat_id=chat_id,
    )

    # Stop during _abort_pending must halt the whole turn and never send the new user message.
    assert len(calls) == 1
    assert result["status"] == "stopped"
    assert chat_session_store.get(chat_id) is None
    flush_mock.assert_awaited_once_with(state)
