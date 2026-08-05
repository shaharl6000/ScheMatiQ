"""Tests for the session guard on the chat routes.

The chat routes are unauthenticated and the agent service will happily build a
Gemini chat for any ``session_id`` string. The contract these tests protect is
that an unknown session is rejected *before* any LLM work happens, so the
endpoint cannot be used to spend the server's quota with no project.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.api.routes import chat as chat_routes
from app.models.chat import ChatConfirmRequest, ChatMessageRequest


class _Session:
    """Stand-in for a persisted VisualizationSession."""

    id = "real-session"


@pytest.fixture
def known_session(monkeypatch):
    """Make only 'real-session' resolvable through the session manager."""

    def _get_session(session_id: str):
        return _Session() if session_id == "real-session" else None

    monkeypatch.setattr(
        chat_routes.session_manager, "get_session", _get_session, raising=True
    )


@pytest.fixture
def exploding_agent(monkeypatch):
    """Fail loudly if any chat route reaches the agent service."""

    async def _boom(*args, **kwargs):
        raise AssertionError("agent service must not be reached for a bad session")

    for method in ("send_message", "confirm_pending", "cancel_pending", "list_tools"):
        monkeypatch.setattr(
            chat_routes.chat_agent_service, method, _boom, raising=True
        )


# --- the guard itself ------------------------------------------------------


def test_unknown_session_is_rejected_with_404(known_session):
    with pytest.raises(HTTPException) as exc:
        chat_routes._require_session("does-not-exist")
    assert exc.value.status_code == 404


def test_known_session_passes(known_session):
    assert chat_routes._require_session("real-session") is None


# --- routes stop before the agent service ---------------------------------


@pytest.mark.asyncio
async def test_send_message_rejects_unknown_session_without_calling_agent(
    known_session, exploding_agent
):
    with pytest.raises(HTTPException) as exc:
        await chat_routes.send_chat_message(
            "does-not-exist", ChatMessageRequest(message="what is the integral of x^2?")
        )
    # 404, not 500: the guard must run before the handler's bare `except
    # Exception`, which would otherwise swallow it into a 500.
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_confirm_rejects_unknown_session_without_calling_agent(
    known_session, exploding_agent
):
    with pytest.raises(HTTPException) as exc:
        await chat_routes.confirm_chat_action(
            "does-not-exist", ChatConfirmRequest(chat_id="chat-1")
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_cancel_rejects_unknown_session_without_calling_agent(
    known_session, exploding_agent
):
    with pytest.raises(HTTPException) as exc:
        await chat_routes.cancel_chat_action(
            "does-not-exist", ChatConfirmRequest(chat_id="chat-1")
        )
    assert exc.value.status_code == 404


# --- /tools keeps working with no session --------------------------------


@pytest.mark.asyncio
async def test_tools_without_session_is_not_gated(known_session, monkeypatch):
    """The registry serves project-creation tools with no session; keep that."""
    captured = {}

    async def _list_tools(session_id, session_mode):
        captured["session_id"] = session_id
        return []

    monkeypatch.setattr(
        chat_routes.chat_agent_service, "list_tools", _list_tools, raising=True
    )
    result = await chat_routes.list_chat_tools(session_id=None, session_mode="schematiq")
    assert result.tools == []
    assert captured["session_id"] is None


@pytest.mark.asyncio
async def test_tools_with_unknown_session_is_gated(known_session, exploding_agent):
    with pytest.raises(HTTPException) as exc:
        await chat_routes.list_chat_tools(
            session_id="does-not-exist", session_mode="schematiq"
        )
    assert exc.value.status_code == 404
