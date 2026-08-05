"""Tests for the per-workspace-session chat message cap.

The contract: once a workspace session has used its allowance, send_message
returns a plain refusal and performs no Gemini work at all — and the allowance
cannot be reset by starting a new conversation.

Uses a fake Gemini chat, so no real LLM or network call happens.
"""

from __future__ import annotations

import pytest

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import (
    MESSAGE_CAP_CHAT_MESSAGE,
    ChatAgentService,
)
from app.services.chat.session_store import (
    ChatSessionState,
    SessionMessageCounter,
    chat_session_store,
)


class _FakeResponse:
    function_calls: list = []
    text = "done"


class _FakeChat:
    """Records every turn sent to Gemini so tests can assert on call count."""

    def __init__(self) -> None:
        self.sent: list = []

    async def send_message(self, content):
        self.sent.append(content)
        return _FakeResponse()


@pytest.fixture
def service(monkeypatch):
    """A ChatAgentService whose sessions use a fake chat and no quota check."""
    svc = ChatAgentService()
    # Every send_message without a chat_id creates a fresh chat object, so the
    # fakes must be accumulated rather than kept per session id — otherwise a
    # later call silently replaces the one holding the turns we want to count.
    chats: dict[str, list[_FakeChat]] = {}

    def _fake_create(session_id, session_mode, model=None):
        chat = _FakeChat()
        chats.setdefault(session_id, []).append(chat)
        return object(), chat

    async def _no_quota():
        return None

    async def _no_flush(state):
        return None

    monkeypatch.setattr(svc, "_create_gemini_chat", _fake_create, raising=True)
    monkeypatch.setattr(svc, "_ensure_quota_available", _no_quota, raising=True)
    monkeypatch.setattr(svc, "_flush_llm_usage", _no_flush, raising=True)
    svc._test_chats = chats  # type: ignore[attr-defined]
    svc._test_turns = lambda sid: sum(  # type: ignore[attr-defined]
        len(c.sent) for c in chats.get(sid, [])
    )
    return svc


@pytest.fixture
def fresh_counter(monkeypatch):
    """Isolate the module-level counter so tests don't leak into each other."""
    counter = SessionMessageCounter()
    monkeypatch.setattr(agent_module, "session_message_counter", counter, raising=True)
    return counter


def _set_cap(monkeypatch, value: int) -> None:
    """The cap is read from the module namespace, so patch it there."""
    monkeypatch.setattr(
        agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", value, raising=True
    )
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", False, raising=True)


# --- counter semantics ----------------------------------------------------


def test_counter_is_per_workspace_session():
    counter = SessionMessageCounter()
    counter.increment("sess-a")
    counter.increment("sess-a")
    counter.increment("sess-b")
    assert counter.count("sess-a") == 2
    assert counter.count("sess-b") == 1
    assert counter.count("sess-never-seen") == 0


def test_counter_increments_are_thread_safe():
    import threading

    counter = SessionMessageCounter()

    def bump():
        for _ in range(500):
            counter.increment("sess")

    threads = [threading.Thread(target=bump) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert counter.count("sess") == 4000


# --- enforcement ----------------------------------------------------------


@pytest.mark.asyncio
async def test_messages_under_the_cap_reach_gemini(service, fresh_counter, monkeypatch):
    _set_cap(monkeypatch, 2)
    result = await service.send_message("sess-1", "add a rationale", "schematiq")
    assert result["status"] == "complete"
    assert service._test_turns("sess-1") == 1, "turn should have reached Gemini"
    assert fresh_counter.count("sess-1") == 1


@pytest.mark.asyncio
async def test_capped_session_refuses_and_makes_no_gemini_call(
    service, fresh_counter, monkeypatch
):
    _set_cap(monkeypatch, 2)
    await service.send_message("sess-1", "one", "schematiq")
    await service.send_message("sess-1", "two", "schematiq")
    turns_before = service._test_turns("sess-1")
    assert turns_before == 2

    result = await service.send_message("sess-1", "three", "schematiq")

    assert result["status"] == "complete"
    assert [m["content"] for m in result["messages"]] == [MESSAGE_CAP_CHAT_MESSAGE]
    assert service._test_turns("sess-1") == turns_before, (
        "a capped turn must not reach Gemini"
    )
    # The refused turn must not push the counter further up.
    assert fresh_counter.count("sess-1") == 2


@pytest.mark.asyncio
async def test_cap_is_not_reset_by_starting_a_new_conversation(
    service, fresh_counter, monkeypatch
):
    _set_cap(monkeypatch, 1)
    first = await service.send_message("sess-1", "one", "schematiq")
    # Drop the chat session the way stale-chat recovery does, then come back
    # with no chat_id at all — a brand new conversation for the same project.
    chat_session_store.delete(first["chat_id"])

    result = await service.send_message("sess-1", "two", "schematiq", chat_id=None)

    assert [m["content"] for m in result["messages"]] == [MESSAGE_CAP_CHAT_MESSAGE]


@pytest.mark.asyncio
async def test_other_sessions_are_unaffected(service, fresh_counter, monkeypatch):
    _set_cap(monkeypatch, 1)
    await service.send_message("sess-1", "one", "schematiq")
    capped = await service.send_message("sess-1", "two", "schematiq")
    assert [m["content"] for m in capped["messages"]] == [MESSAGE_CAP_CHAT_MESSAGE]

    other = await service.send_message("sess-2", "one", "schematiq")
    assert other["messages"] == [] or MESSAGE_CAP_CHAT_MESSAGE not in [
        m["content"] for m in other["messages"]
    ]
    assert service._test_turns("sess-2") == 1, "a different project keeps working"


# --- escape hatches -------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_of_zero_disables_enforcement(service, fresh_counter, monkeypatch):
    _set_cap(monkeypatch, 0)
    for _ in range(5):
        result = await service.send_message("sess-1", "hi", "schematiq")
    assert MESSAGE_CAP_CHAT_MESSAGE not in [m["content"] for m in result["messages"]]


@pytest.mark.asyncio
async def test_developer_mode_bypasses_the_cap(service, fresh_counter, monkeypatch):
    monkeypatch.setattr(agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", 1, raising=True)
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", True, raising=True)
    await service.send_message("sess-1", "one", "schematiq")
    result = await service.send_message("sess-1", "two", "schematiq")
    assert MESSAGE_CAP_CHAT_MESSAGE not in [m["content"] for m in result["messages"]]
