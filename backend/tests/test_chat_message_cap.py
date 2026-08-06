"""Tests for the per-workspace-session chat message cap.

The contract: once a workspace session has used its allowance, send_message
returns a plain refusal and performs no Gemini work at all. The allowance is
held on the persisted session record, so it survives both a new conversation
and a process restart.

Uses a fake Gemini chat, so no real LLM or network call happens.
"""

from __future__ import annotations

import pytest

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import (
    MESSAGE_CAP_CHAT_MESSAGE,
    ChatAgentService,
)
from app.services.chat.session_store import chat_session_store


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


class _FakeSession:
    """Stand-in for VisualizationSession, with the persisted counter field."""

    def __init__(self, session_id: str) -> None:
        self.id = session_id
        self.chat_messages_used = 0


@pytest.fixture
def store(monkeypatch):
    """An in-test stand-in for the persisted session store.

    Records every update_session call so tests can prove the count is written
    through to storage rather than only held in memory.
    """
    sessions: dict[str, _FakeSession] = {}
    writes: list[str] = []

    def _get_session(session_id: str):
        return sessions.setdefault(session_id, _FakeSession(session_id))

    def _update_session(session):
        writes.append(session.id)

    monkeypatch.setattr(
        agent_module.session_manager, "get_session", _get_session, raising=True
    )
    monkeypatch.setattr(
        agent_module.session_manager, "update_session", _update_session, raising=True
    )
    return type("Store", (), {"sessions": sessions, "writes": writes})()


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
    svc._test_turns = lambda sid: sum(  # type: ignore[attr-defined]
        len(c.sent) for c in chats.get(sid, [])
    )
    return svc


def _set_cap(monkeypatch, value: int) -> None:
    """The cap is read from the module namespace, so patch it there."""
    monkeypatch.setattr(
        agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", value, raising=True
    )
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", False, raising=True)


def _texts(result) -> list[str]:
    return [m["content"] for m in result["messages"]]


# --- enforcement ----------------------------------------------------------


@pytest.mark.asyncio
async def test_messages_under_the_cap_reach_gemini(service, store, monkeypatch):
    _set_cap(monkeypatch, 2)
    result = await service.send_message("sess-1", "add a rationale", "schematiq")
    assert result["status"] == "complete"
    assert service._test_turns("sess-1") == 1, "turn should have reached Gemini"
    assert store.sessions["sess-1"].chat_messages_used == 1


@pytest.mark.asyncio
async def test_the_count_is_written_through_to_storage(service, store, monkeypatch):
    """The whole point of moving off an in-memory counter."""
    _set_cap(monkeypatch, 5)
    await service.send_message("sess-1", "one", "schematiq")
    await service.send_message("sess-1", "two", "schematiq")
    assert store.writes == ["sess-1", "sess-1"]


@pytest.mark.asyncio
async def test_capped_session_refuses_and_makes_no_gemini_call(
    service, store, monkeypatch
):
    _set_cap(monkeypatch, 2)
    await service.send_message("sess-1", "one", "schematiq")
    await service.send_message("sess-1", "two", "schematiq")
    turns_before = service._test_turns("sess-1")
    assert turns_before == 2

    result = await service.send_message("sess-1", "three", "schematiq")

    assert result["status"] == "complete"
    assert _texts(result) == [MESSAGE_CAP_CHAT_MESSAGE]
    assert service._test_turns("sess-1") == turns_before, (
        "a capped turn must not reach Gemini"
    )
    # The refused turn must not push the count further up.
    assert store.sessions["sess-1"].chat_messages_used == 2


@pytest.mark.asyncio
async def test_cap_survives_a_process_restart(service, store, monkeypatch):
    """A fresh service instance still sees the count: it lives on the session."""
    _set_cap(monkeypatch, 1)
    await service.send_message("sess-1", "one", "schematiq")

    restarted = ChatAgentService()
    monkeypatch.setattr(
        restarted,
        "_create_gemini_chat",
        lambda sid, mode, model=None: (object(), _FakeChat()),
        raising=True,
    )
    result = await restarted.send_message("sess-1", "two", "schematiq")

    assert _texts(result) == [MESSAGE_CAP_CHAT_MESSAGE]


@pytest.mark.asyncio
async def test_cap_is_not_reset_by_starting_a_new_conversation(
    service, store, monkeypatch
):
    _set_cap(monkeypatch, 1)
    first = await service.send_message("sess-1", "one", "schematiq")
    # Drop the chat session the way stale-chat recovery does, then come back
    # with no chat_id at all — a brand new conversation for the same project.
    chat_session_store.delete(first["chat_id"])

    result = await service.send_message("sess-1", "two", "schematiq", chat_id=None)

    assert _texts(result) == [MESSAGE_CAP_CHAT_MESSAGE]


@pytest.mark.asyncio
async def test_other_sessions_are_unaffected(service, store, monkeypatch):
    _set_cap(monkeypatch, 1)
    await service.send_message("sess-1", "one", "schematiq")
    capped = await service.send_message("sess-1", "two", "schematiq")
    assert _texts(capped) == [MESSAGE_CAP_CHAT_MESSAGE]

    await service.send_message("sess-2", "one", "schematiq")
    assert service._test_turns("sess-2") == 1, "a different project keeps working"


@pytest.mark.asyncio
async def test_unknown_session_is_not_capped(service, monkeypatch):
    """The route layer 404s these first; the service must not crash on them."""
    _set_cap(monkeypatch, 1)
    monkeypatch.setattr(
        agent_module.session_manager, "get_session", lambda sid: None, raising=True
    )
    result = await service.send_message("ghost", "hi", "schematiq")
    assert _texts(result) != [MESSAGE_CAP_CHAT_MESSAGE]


# --- escape hatches -------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_of_zero_disables_enforcement(service, store, monkeypatch):
    _set_cap(monkeypatch, 0)
    for _ in range(5):
        result = await service.send_message("sess-1", "hi", "schematiq")
    assert MESSAGE_CAP_CHAT_MESSAGE not in _texts(result)
    assert store.writes == [], "disabled cap should not write to storage"


@pytest.mark.asyncio
async def test_developer_mode_bypasses_the_cap(service, store, monkeypatch):
    monkeypatch.setattr(agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", 1, raising=True)
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", True, raising=True)
    await service.send_message("sess-1", "one", "schematiq")
    result = await service.send_message("sess-1", "two", "schematiq")
    assert MESSAGE_CAP_CHAT_MESSAGE not in _texts(result)
