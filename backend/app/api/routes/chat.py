"""Chat agent API endpoints for workspace tool-calling."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.chat import (
    ChatConfirmRequest,
    ChatMessageRequest,
    ChatMessageResponse,
    ChatMessagesResponse,
    ChatToolsResponse,
    ChatToolInfo,
    ChatTurnMessage,
    PendingChatAction,
)
from app.services import session_manager
from app.services.chat.agent_service import chat_agent_service

router = APIRouter(tags=["chat"])


def _require_session(session_id: str) -> None:
    """Reject chat requests for workspace sessions that do not exist.

    These routes are unauthenticated, and the agent service builds a Gemini
    chat for whatever ``session_id`` string it is handed — it never checks that
    the session is real. Without this guard, any caller can spend the server's
    LLM quota by POSTing to an arbitrary ``/api/chat/<anything>/message`` path,
    with no project and no documents.

    Sessions are persisted through the storage backend and fetched on first
    access (``SessionManager.get_session``), so a legitimate session_id keeps
    working across restarts and redeploys.

    Must be called *before* the route's ``try`` block: the handlers catch bare
    ``Exception`` and would otherwise convert this 404 into a 500.
    """
    if session_manager.get_session(session_id) is None:
        raise HTTPException(status_code=404, detail="Session not found.")


@router.get("/tools", response_model=ChatToolsResponse)
async def list_chat_tools(
    session_id: Optional[str] = Query(None),
    session_mode: str = Query("schematiq"),
) -> ChatToolsResponse:
    # session_id is optional here: with no session the registry returns the
    # project-creation tools, which is how the UI populates /tools before a
    # project exists. Only validate when the caller claims a session.
    if session_id:
        _require_session(session_id)
    tools = await chat_agent_service.list_tools(session_id, session_mode)
    return ChatToolsResponse(tools=[ChatToolInfo(**tool) for tool in tools])


@router.get("/{session_id}/messages", response_model=ChatMessagesResponse)
async def get_chat_messages(session_id: str) -> ChatMessagesResponse:
    """Return the reconstructed transcript so a reloaded client can repaint it.

    Read-only: no Gemini work happens here. Guarded like the other routes so an
    unknown session is a 404 before the agent service is touched. When there is
    no persisted history (or the session opted out), ``messages`` is empty and
    the client keeps its local seed.
    """
    _require_session(session_id)
    try:
        result = await chat_agent_service.get_transcript(session_id)
        return ChatMessagesResponse(
            chat_id=result["chat_id"],
            messages=[ChatTurnMessage(**msg) for msg in result["messages"]],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{session_id}/message", response_model=ChatMessageResponse)
async def send_chat_message(
    session_id: str,
    request: ChatMessageRequest,
) -> ChatMessageResponse:
    _require_session(session_id)
    try:
        result = await chat_agent_service.send_message(
            session_id=session_id,
            message=request.message,
            session_mode=request.session_mode,
            chat_id=request.chat_id,
            pinned_tool=request.pinned_tool,
            model=request.model,
        )
        return ChatMessageResponse(
            chat_id=result["chat_id"],
            status=result["status"],
            messages=[ChatTurnMessage(**msg) for msg in result["messages"]],
            pending_action=(
                PendingChatAction(**result["pending_action"])
                if result.get("pending_action")
                else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{session_id}/confirm", response_model=ChatMessageResponse)
async def confirm_chat_action(
    session_id: str,
    request: ChatConfirmRequest,
) -> ChatMessageResponse:
    _require_session(session_id)
    try:
        result = await chat_agent_service.confirm_pending(session_id, request.chat_id)
        return ChatMessageResponse(
            chat_id=result["chat_id"],
            status=result["status"],
            messages=[ChatTurnMessage(**msg) for msg in result["messages"]],
            pending_action=(
                PendingChatAction(**result["pending_action"])
                if result.get("pending_action")
                else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{session_id}/cancel", response_model=ChatMessageResponse)
async def cancel_chat_action(
    session_id: str,
    request: ChatConfirmRequest,
) -> ChatMessageResponse:
    _require_session(session_id)
    try:
        result = await chat_agent_service.cancel_pending(session_id, request.chat_id)
        return ChatMessageResponse(
            chat_id=result["chat_id"],
            status=result["status"],
            messages=[ChatTurnMessage(**msg) for msg in result["messages"]],
            pending_action=(
                PendingChatAction(**result["pending_action"])
                if result.get("pending_action")
                else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{session_id}/stop")
async def stop_chat_turn(session_id: str) -> dict[str, bool]:
    """Halt the in-flight chat turn for a session.

    Fire-and-forget companion to the client aborting its request: the run loop
    checks a cooperative flag at its next step boundary, then discards the turn
    without persisting it. Returns ``stopped`` = whether a live turn existed.
    """
    _require_session(session_id)
    stopped = chat_agent_service.request_stop(session_id)
    return {"stopped": stopped}
