"""Tests for GET /{session_id}/messages and transcript reconstruction (PR2).

The endpoint is read-only: it reconstructs a display transcript from the
persisted model history so a reloaded client can repaint the conversation, and
returns the chat_id to resume with when one is live. Like the other chat routes
it 404s an unknown session before touching the agent service, and it never runs
Gemini.

Route handlers are called directly with the agent service faked, matching
test_chat_session_validation.py.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from google.genai import types

from app.api.routes import chat as chat_routes
from app.models.chat import ChatMessagesResponse, ChatTurnMessage
from app.services.chat.agent_service import ChatAgentService


class _Session:
    id = "real-session"


@pytest.fixture
def known_session(monkeypatch):
    """Only 'real-session' resolves; everything else is unknown."""

    def _get_session(session_id: str):
        return _Session() if session_id == "real-session" else None

    monkeypatch.setattr(
        chat_routes.session_manager, "get_session", _get_session, raising=True
    )


# --- the route: guard + shape --------------------------------------------


@pytest.mark.asyncio
async def test_unknown_session_is_rejected_without_calling_agent(
    known_session, monkeypatch
):
    async def _boom(*args, **kwargs):
        raise AssertionError("agent service must not be reached for a bad session")

    monkeypatch.setattr(
        chat_routes.chat_agent_service, "get_transcript", _boom, raising=True
    )

    with pytest.raises(HTTPException) as exc:
        await chat_routes.get_chat_messages("does-not-exist")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_empty_when_no_history(known_session, monkeypatch):
    async def _empty(session_id):
        return {"chat_id": None, "messages": []}

    monkeypatch.setattr(
        chat_routes.chat_agent_service, "get_transcript", _empty, raising=True
    )

    result = await chat_routes.get_chat_messages("real-session")
    assert isinstance(result, ChatMessagesResponse)
    assert result.chat_id is None
    assert result.messages == []


@pytest.mark.asyncio
async def test_returns_turns_and_chat_id_when_history_exists(known_session, monkeypatch):
    async def _transcript(session_id):
        return {
            "chat_id": "chat-abc",
            "messages": [
                {"id": "1", "role": "user", "kind": "text", "content": "hi"},
                {
                    "id": "2",
                    "role": "tool",
                    "kind": "tool_log",
                    "tool_name": "add_column",
                    "tool_status": "done",
                    "content": "...adding column",
                },
                {"id": "3", "role": "assistant", "kind": "text", "content": "done"},
            ],
        }

    monkeypatch.setattr(
        chat_routes.chat_agent_service, "get_transcript", _transcript, raising=True
    )

    result = await chat_routes.get_chat_messages("real-session")
    assert result.chat_id == "chat-abc"
    assert [m.role for m in result.messages] == ["user", "tool", "assistant"]
    assert result.messages[1].tool_name == "add_column"
    assert result.messages[1].tool_status == "done"


# --- reconstruction from model history ------------------------------------


def _history():
    """A realistic persisted conversation: user text, a tool call, its response,
    and the model's reply."""
    return [
        types.Content(role="user", parts=[types.Part(text="add a column X")]),
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


def test_reconstruct_maps_bubbles_and_hides_function_response():
    messages = ChatAgentService._reconstruct_transcript(_history())

    kinds = [(m["role"], m.get("kind")) for m in messages]
    assert kinds == [
        ("user", "text"),
        ("tool", "tool_log"),
        ("assistant", "text"),
    ], "function_response turn must be hidden; text turns become bubbles"

    user_bubble, tool_log, assistant_bubble = messages
    assert user_bubble["content"] == "add a column X"
    assert tool_log["tool_name"] == "add_column"
    assert tool_log["tool_status"] == "done"
    assert assistant_bubble["content"] == "Added column X."


def test_reconstruct_ids_are_unique():
    messages = ChatAgentService._reconstruct_transcript(_history())
    assert len({m["id"] for m in messages}) == len(messages)


def test_reconstructed_messages_validate_as_chat_turn_messages():
    """What the reconstruction emits must satisfy the response model unchanged."""
    for msg in ChatAgentService._reconstruct_transcript(_history()):
        ChatTurnMessage(**msg)  # raises if a field is missing or mistyped


def test_reconstruct_empty_history_is_empty():
    assert ChatAgentService._reconstruct_transcript([]) == []


def test_reconstruct_unknown_tool_falls_back_to_generic_label():
    history = [
        types.Content(
            role="model",
            parts=[
                types.Part(
                    function_call=types.FunctionCall(name="mystery_tool", args={})
                )
            ],
        )
    ]
    (tool_log,) = ChatAgentService._reconstruct_transcript(history)
    assert tool_log["tool_name"] == "mystery_tool"
    # tool_running_label has a generic fallback, so the content is non-empty.
    assert tool_log["content"].strip(". ")
