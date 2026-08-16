"""Tests for workspace chat-history persistence and restore (PR1).

The contract: a conversation is durably backed by a per-session transcript so it
survives a backend restart/redeploy. The Gemini chat object is a transient cache
rebuilt from that transcript. Persistence is best-effort (never breaks a turn),
gated on ``opt_out_data_collection``, and a missing/unreadable transcript falls
back to an empty chat — restore can only ever add context, never remove function.

Uses a fake Gemini chat and a fake storage backend, so no real LLM, network, or
disk is touched.
"""

from __future__ import annotations

import json

import pytest
from google.genai import types

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import (
    CHAT_HISTORY_BUCKET,
    ChatAgentService,
    _chat_history_path,
)
from app.services.chat.session_store import chat_session_store


class _FakeResponse:
    function_calls: list = []
    text = "done"


class _FakeChat:
    """Stand-in for the SDK async chat.

    Seeds itself from a restored ``history`` (list of ``types.Content``) and grows
    it by one user + one model turn per ``send_message``, so ``get_history`` returns
    a realistic transcript the service can serialize and persist.
    """

    def __init__(self, history=None) -> None:
        self._history: list = list(history or [])
        self.seeded = list(history or [])

    async def send_message(self, content):
        text = content if isinstance(content, str) else "tool-response"
        self._history.append(
            types.Content(role="user", parts=[types.Part(text=str(text))])
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
    """Minimal StorageInterface stand-in backed by an in-memory dict."""

    def __init__(self, fail_write: bool = False) -> None:
        self.blobs: dict[tuple[str, str], dict] = {}
        self.writes: list[tuple[str, str]] = []
        self.fail_write = fail_write

    async def upload_json(self, bucket: str, path: str, data: dict) -> str:
        if self.fail_write:
            raise IOError("simulated storage failure")
        # Round-trip through JSON exactly like the real helper, to catch any
        # non-serializable turn slipping through.
        self.blobs[(bucket, path)] = json.loads(json.dumps(data, default=str))
        self.writes.append((bucket, path))
        return path

    async def download_json(self, bucket: str, path: str):
        return self.blobs.get((bucket, path))


@pytest.fixture(autouse=True)
def _clean_chat_store():
    """The chat session store is a process-wide singleton; isolate each test so a
    leftover in-memory chat can't skew reattach/create assertions."""
    chat_session_store._sessions.clear()
    yield
    chat_session_store._sessions.clear()


@pytest.fixture
def sessions(monkeypatch):
    """Patch the session lookup and disable the message cap for these tests."""
    registry: dict[str, _FakeSession] = {}

    def _get_session(session_id: str):
        return registry.setdefault(session_id, _FakeSession(session_id))

    monkeypatch.setattr(
        agent_module.session_manager, "get_session", _get_session, raising=True
    )
    # Cap disabled so _record_message_used / _message_cap_reached stay out of the
    # way; this suite is about persistence, not the cap.
    monkeypatch.setattr(
        agent_module, "CHAT_MAX_MESSAGES_PER_SESSION", 0, raising=True
    )
    monkeypatch.setattr(agent_module, "DEVELOPER_MODE", False, raising=True)
    return registry


def _make_service(monkeypatch, storage: _FakeStorage) -> ChatAgentService:
    """A service whose chats are fakes, quota is a no-op, and storage is faked."""
    svc = ChatAgentService()

    def _fake_create(session_id, session_mode, model=None, history=None):
        return object(), _FakeChat(history=history)

    async def _no_quota():
        return None

    async def _no_flush(state):
        return None

    monkeypatch.setattr(svc, "_create_gemini_chat", _fake_create, raising=True)
    monkeypatch.setattr(svc, "_ensure_quota_available", _no_quota, raising=True)
    monkeypatch.setattr(svc, "_flush_llm_usage", _no_flush, raising=True)
    monkeypatch.setattr(svc, "_get_storage", lambda: storage, raising=True)
    return svc


# --- serialization --------------------------------------------------------


def test_serialize_deserialize_round_trip():
    """User text, a function call, its response, and model text all survive a
    JSON round trip and rebuild into equivalent Content turns."""
    history = [
        types.Content(role="user", parts=[types.Part(text="add column X")]),
        types.Content(
            role="model",
            parts=[
                types.Part(
                    function_call=types.FunctionCall(
                        name="add_column", args={"name": "X"}
                    )
                )
            ],
        ),
        types.Content(
            role="user",
            parts=[
                types.Part.from_function_response(
                    name="add_column", response={"result": {"message": "ok"}}
                )
            ],
        ),
        types.Content(role="model", parts=[types.Part(text="Added column X.")]),
    ]

    dumped = ChatAgentService._serialize_history(history)
    dumped = json.loads(json.dumps(dumped))  # storage does the same
    restored = ChatAgentService._deserialize_history(dumped)

    assert [c.role for c in restored] == ["user", "model", "user", "model"]
    assert restored[1].parts[0].function_call.name == "add_column"
    assert restored[2].parts[0].function_response.name == "add_column"
    assert restored[0].parts[0].text == "add column X"


def test_deserialize_skips_malformed_turns():
    """One bad turn is dropped, not fatal to the whole restore."""
    good = types.Content(role="user", parts=[types.Part(text="hi")]).model_dump(
        mode="json", exclude_none=True
    )
    restored = ChatAgentService._deserialize_history([good, {"role": 123}, "junk"])
    assert len(restored) == 1
    assert restored[0].parts[0].text == "hi"


# --- persistence ----------------------------------------------------------


@pytest.mark.asyncio
async def test_completed_turn_persists_history(sessions, monkeypatch):
    storage = _FakeStorage()
    svc = _make_service(monkeypatch, storage)

    await svc.send_message("sess-1", "hello", "schematiq")

    path = _chat_history_path("sess-1")
    assert (CHAT_HISTORY_BUCKET, path) == ("data", "sess-1/chat_history.json")
    assert storage.writes == [(CHAT_HISTORY_BUCKET, path)]
    payload = storage.blobs[(CHAT_HISTORY_BUCKET, path)]
    assert payload["version"] == 1
    assert len(payload["turns"]) >= 2  # at least the user + model turn


@pytest.mark.asyncio
async def test_opt_out_skips_writing(sessions, monkeypatch):
    sessions["sess-opt"] = _FakeSession("sess-opt", opt_out=True)
    storage = _FakeStorage()
    svc = _make_service(monkeypatch, storage)

    result = await svc.send_message("sess-opt", "hello", "schematiq")

    assert result["status"] == "complete"  # chat still works within the run
    assert storage.writes == []  # but nothing is retained


@pytest.mark.asyncio
async def test_write_failure_does_not_raise(sessions, monkeypatch):
    storage = _FakeStorage(fail_write=True)
    svc = _make_service(monkeypatch, storage)

    result = await svc.send_message("sess-1", "hello", "schematiq")

    # The turn still completes even though persistence blew up.
    assert result["status"] == "complete"
    assert any(m.get("kind") == "text" for m in result["messages"])


# --- restore --------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_after_restart_rehydrates_the_model(sessions, monkeypatch):
    """A fresh service (simulating a redeploy that emptied the in-memory store)
    restores the persisted transcript and seeds the new chat with it."""
    storage = _FakeStorage()

    first = _make_service(monkeypatch, storage)
    await first.send_message("sess-1", "remember apples", "schematiq")
    persisted_turns = storage.blobs[
        (CHAT_HISTORY_BUCKET, _chat_history_path("sess-1"))
    ]["turns"]
    assert persisted_turns

    # Wipe the in-memory store the way a process restart would.
    for state in list(chat_session_store._sessions.values()):
        if state.workspace_session_id == "sess-1":
            chat_session_store.delete(state.chat_id)

    captured: dict = {}

    def _capturing_create(session_id, session_mode, model=None, history=None):
        chat = _FakeChat(history=history)
        captured["seeded"] = chat.seeded
        return object(), chat

    restarted = _make_service(monkeypatch, storage)
    monkeypatch.setattr(restarted, "_create_gemini_chat", _capturing_create)

    await restarted.send_message("sess-1", "and pears", "schematiq", chat_id=None)

    # The rebuilt chat was seeded from the persisted transcript, so the model
    # continues with context instead of starting from zero.
    assert captured["seeded"], "restarted chat should be seeded with restored history"
    assert len(captured["seeded"]) == len(persisted_turns)


@pytest.mark.asyncio
async def test_missing_file_restores_empty(sessions, monkeypatch):
    """No persisted transcript → a fresh empty chat, identical to today."""
    storage = _FakeStorage()  # nothing stored
    svc = _make_service(monkeypatch, storage)

    captured: dict = {}

    def _capturing_create(session_id, session_mode, model=None, history=None):
        captured["history"] = history
        return object(), _FakeChat(history=history)

    monkeypatch.setattr(svc, "_create_gemini_chat", _capturing_create)

    await svc.send_message("ghost", "hello", "schematiq")

    assert not captured["history"]  # None or empty — no history to restore


@pytest.mark.asyncio
async def test_read_failure_restores_empty(sessions, monkeypatch):
    class _BoomStorage(_FakeStorage):
        async def download_json(self, bucket, path):
            raise IOError("simulated read failure")

    svc = _make_service(monkeypatch, _BoomStorage())

    captured: dict = {}

    def _capturing_create(session_id, session_mode, model=None, history=None):
        captured["history"] = history
        return object(), _FakeChat(history=history)

    monkeypatch.setattr(svc, "_create_gemini_chat", _capturing_create)

    result = await svc.send_message("sess-1", "hello", "schematiq")

    assert result["status"] == "complete"
    assert not captured["history"]


# --- one chat per project -------------------------------------------------


@pytest.mark.asyncio
async def test_second_message_without_chat_id_reattaches(sessions, monkeypatch):
    """A client that lost its chat_id (page refresh) reattaches to the one live
    chat for the session rather than starting a second conversation."""
    storage = _FakeStorage()
    svc = _make_service(monkeypatch, storage)

    creates = {"n": 0}
    original_create = svc._create_gemini_chat

    def _counting_create(session_id, session_mode, model=None, history=None):
        creates["n"] += 1
        return original_create(session_id, session_mode, model, history)

    monkeypatch.setattr(svc, "_create_gemini_chat", _counting_create)

    first = await svc.send_message("sess-1", "one", "schematiq")
    second = await svc.send_message("sess-1", "two", "schematiq", chat_id=None)

    assert creates["n"] == 1, "the second turn must reuse the existing chat"
    assert first["chat_id"] == second["chat_id"]
