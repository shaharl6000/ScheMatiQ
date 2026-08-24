"""Focused tests for Gemini text streaming in the workspace chat."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.chat import agent_service as agent_module
from app.services.chat.agent_service import ChatAgentService
from app.services.chat.session_store import ChatSessionState


class _Chunk:
    def __init__(self, text: str = "", function_calls=None) -> None:
        self.text = text
        self.function_calls = function_calls or []


class _StreamingChat:
    def __init__(self, chunks: list[_Chunk], after_first=None) -> None:
        self._chunks = chunks
        self._after_first = after_first

    async def send_message_stream(self, content):
        async def _iterate():
            for index, chunk in enumerate(self._chunks):
                yield chunk
                if index == 0 and self._after_first:
                    self._after_first()

        return _iterate()


def _state(chat) -> ChatSessionState:
    return ChatSessionState(
        client=object(),
        chat=chat,
        workspace_session_id="stream-session",
        session_mode="schematiq",
    )


@pytest.fixture
def recorded_events(monkeypatch):
    events: list[dict] = []

    async def _record(session_id: str, message: dict) -> None:
        assert session_id == "stream-session"
        events.append(message)

    monkeypatch.setattr(
        agent_module.websocket_manager,
        "broadcast_to_session",
        _record,
        raising=True,
    )
    return events


@pytest.mark.asyncio
async def test_text_chunks_stream_and_finish_with_authoritative_message(recorded_events):
    service = ChatAgentService()
    state = _state(_StreamingChat([_Chunk("Hello"), _Chunk(" world")]))
    outbound: list[dict] = []

    response = await service._send_chat_message(state, "hi", outbound)

    assert response.text == "Hello world"
    assert response.message_emitted is True
    assert [event["type"] for event in recorded_events] == [
        "chat_message_delta",
        "chat_message_delta",
        "chat_message",
    ]
    message_id = recorded_events[0]["data"]["id"]
    assert recorded_events[1]["data"]["id"] == message_id
    assert recorded_events[2]["data"]["id"] == message_id
    assert outbound == [recorded_events[2]["data"]]
    assert outbound[0]["content"] == "Hello world"


@pytest.mark.asyncio
async def test_late_tool_call_discards_provisional_text(recorded_events):
    service = ChatAgentService()
    function_call = SimpleNamespace(name="get_schema", args={})
    state = _state(
        _StreamingChat([_Chunk("Let me check."), _Chunk(function_calls=[function_call])])
    )
    outbound: list[dict] = []

    response = await service._send_chat_message(state, "show schema", outbound)

    assert response.function_calls == [function_call]
    assert response.message_emitted is False
    assert outbound == []
    assert [event["type"] for event in recorded_events] == [
        "chat_message_delta",
        "chat_message_discard",
    ]
    assert recorded_events[0]["data"]["id"] == recorded_events[1]["data"]["id"]


@pytest.mark.asyncio
async def test_stop_discards_partial_text(recorded_events):
    service = ChatAgentService()
    state = _state(None)
    state.chat = _StreamingChat(
        [_Chunk("partial"), _Chunk(" ignored")],
        after_first=lambda: setattr(state, "stop_requested", True),
    )
    outbound: list[dict] = []

    response = await service._send_chat_message(state, "start", outbound)

    assert response.stopped is True
    assert outbound == []
    assert [event["type"] for event in recorded_events] == [
        "chat_message_delta",
        "chat_message_discard",
    ]
