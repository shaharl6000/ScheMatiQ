"""Tests for model switching versus reattach on the workspace chat.

A Gemini chat is bound to its model at creation, and the UI switches models by
nulling chat_id. The reattach path (added for page-refresh resume) must tell
these apart: the same model on a null chat_id is a refresh and reuses the live
chat, while a different model is a switch and rebuilds a fresh chat on the new
model — carrying the conversation forward via restore, and keeping one chat per
project. Without this, a model switch is silently ignored until the in-memory
chat is dropped.

Uses a fake Gemini chat and fake storage, so no real LLM or network call happens.
"""

from __future__ import annotations

import json

import pytest
from google.genai import types

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import ChatAgentService
from app.services.chat.session_store import chat_session_store


class _FakeResponse:
    function_calls: list = []
    text = "done"


class _FakeChat:
    def __init__(self, history=None) -> None:
        self._history: list = list(history or [])

    async def send_message(self, content):
        self._history.append(
            types.Content(role="user", parts=[types.Part(text="u")])
        )
        self._history.append(
            types.Content(role="model", parts=[types.Part(text="done")])
        )
        return _FakeResponse()

    def get_history(self):
        return list(self._history)


class _FakeSession:
    def __init__(self, session_id: str, opt_out: bool = False) -> None:
        self.id = session_id
        self.chat_messages_used = 0
        self.opt_out_data_collection = opt_out


class _FakeStorage:
    def __init__(self) -> None:
        self.blobs: dict[tuple[str, str], dict] = {}

    async def upload_json(self, bucket, path, data):
        self.blobs[(bucket, path)] = json.loads(json.dumps(data, default=str))
        return path

    async def download_json(self, bucket, path):
        return self.blobs.get((bucket, path))


@pytest.fixture(autouse=True)
def _isolate_chat_store():
    chat_session_store._sessions.clear()
    yield
    chat_session_store._sessions.clear()


@pytest.fixture
def sessions(monkeypatch):
    registry: dict[str, _FakeSession] = {}

    def _get_session(session_id: str):
        return registry.setdefault(session_id, _FakeSession(session_id))

    monkeypatch.setattr(
        agent_module.session_manager, "get_session", _get_session, raising=True
    )
    monkeypatch.setattr(agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", 0, raising=True)
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", False, raising=True)
    return registry


@pytest.fixture
def service(monkeypatch):
    svc = ChatAgentService()
    creates: list[dict] = []

    def _fake_create(session_id, session_mode, model=None, history=None):
        creates.append({"model": model, "history": history})
        return object(), _FakeChat(history=history)

    async def _no_quota():
        return None

    async def _no_flush(state):
        return None

    monkeypatch.setattr(svc, "_create_gemini_chat", _fake_create, raising=True)
    monkeypatch.setattr(svc, "_ensure_quota_available", _no_quota, raising=True)
    monkeypatch.setattr(svc, "_flush_llm_usage", _no_flush, raising=True)
    svc._creates = creates  # type: ignore[attr-defined]
    return svc


def _states_for(session_id: str) -> list:
    return [
        s
        for s in chat_session_store._sessions.values()
        if s.workspace_session_id == session_id
    ]


# --- refresh vs switch ----------------------------------------------------


@pytest.mark.asyncio
async def test_same_model_without_chat_id_reattaches(service, sessions):
    """A page refresh (null chat_id, unchanged model) reuses the live chat."""
    sessions["s1"] = _FakeSession("s1", opt_out=True)  # opt-out: no storage needed

    first = await service.send_message("s1", "one", "schematiq", model="gemini-a")
    second = await service.send_message(
        "s1", "two", "schematiq", chat_id=None, model="gemini-a"
    )

    assert len(service._creates) == 1, "same model must not build a second chat"
    assert first["chat_id"] == second["chat_id"]


@pytest.mark.asyncio
async def test_switching_model_rebuilds_on_the_new_model(service, sessions):
    """A model switch (null chat_id, different model) rebuilds on the new model
    and drops the old chat, preserving one chat per project."""
    sessions["s1"] = _FakeSession("s1", opt_out=True)

    await service.send_message("s1", "one", "schematiq", model="gemini-a")
    await service.send_message("s1", "two", "schematiq", chat_id=None, model="gemini-b")

    assert len(service._creates) == 2, "a switch must rebuild the chat"
    assert service._creates[-1]["model"] == "gemini-b"
    remaining = _states_for("s1")
    assert len(remaining) == 1, "the old-model chat must be dropped"
    assert remaining[0].model == "gemini-b"


@pytest.mark.asyncio
async def test_refresh_after_switch_reattaches_to_new_model(service, sessions):
    sessions["s1"] = _FakeSession("s1", opt_out=True)

    await service.send_message("s1", "one", "schematiq", model="gemini-a")
    await service.send_message("s1", "two", "schematiq", chat_id=None, model="gemini-b")
    await service.send_message("s1", "three", "schematiq", chat_id=None, model="gemini-b")

    assert len(service._creates) == 2, "refresh on the new model must not rebuild again"


@pytest.mark.asyncio
async def test_valid_chat_id_reuses_regardless_of_model(service, sessions):
    """The explicit chat_id fast-path keeps the same conversation; the UI nulls
    chat_id to switch, so this path never carries a switch."""
    sessions["s1"] = _FakeSession("s1", opt_out=True)

    first = await service.send_message("s1", "one", "schematiq", model="gemini-a")
    await service.send_message(
        "s1", "two", "schematiq", chat_id=first["chat_id"], model="gemini-b"
    )

    assert len(service._creates) == 1


@pytest.mark.asyncio
async def test_default_model_and_none_are_the_same(service, sessions):
    """An unspecified model resolves to the default, so alternating None and the
    explicit default must not be seen as a switch."""
    from app.services.chat.deps import CHAT_MODEL

    sessions["s1"] = _FakeSession("s1", opt_out=True)

    await service.send_message("s1", "one", "schematiq", model=None)
    await service.send_message("s1", "two", "schematiq", chat_id=None, model=CHAT_MODEL)

    assert len(service._creates) == 1, "None and the default model are equivalent"


# --- history carries across a switch --------------------------------------


@pytest.mark.asyncio
async def test_switch_carries_conversation_forward(service, sessions, monkeypatch):
    """Switching models rebuilds the chat with the restored transcript, so the
    new model continues with full context instead of starting from zero."""
    sessions["s1"] = _FakeSession("s1", opt_out=False)
    storage = _FakeStorage()
    monkeypatch.setattr(service, "_get_storage", lambda: storage, raising=True)

    await service.send_message("s1", "hi", "schematiq", model="gemini-a")
    # The first turn persisted a transcript; switching must feed it back in.
    await service.send_message("s1", "again", "schematiq", chat_id=None, model="gemini-b")

    last = service._creates[-1]
    assert last["model"] == "gemini-b"
    assert last["history"], "the rebuilt chat must be seeded with the restored history"
