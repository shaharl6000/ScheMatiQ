"""Chat agent API endpoints for workspace tool-calling."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.chat import (
    ChatConfirmRequest,
    ChatMessageRequest,
    ChatMessageResponse,
    ChatToolsResponse,
    ChatToolInfo,
    ChatTurnMessage,
    PendingChatAction,
)
from app.services.chat.agent_service import chat_agent_service

router = APIRouter(tags=["chat"])


@router.get("/tools", response_model=ChatToolsResponse)
async def list_chat_tools(
    session_id: Optional[str] = Query(None),
    session_mode: str = Query("schematiq"),
) -> ChatToolsResponse:
    tools = await chat_agent_service.list_tools(session_id, session_mode)
    return ChatToolsResponse(tools=[ChatToolInfo(**tool) for tool in tools])


@router.post("/{session_id}/message", response_model=ChatMessageResponse)
async def send_chat_message(
    session_id: str,
    request: ChatMessageRequest,
) -> ChatMessageResponse:
    try:
        result = await chat_agent_service.send_message(
            session_id=session_id,
            message=request.message,
            session_mode=request.session_mode,
            chat_id=request.chat_id,
            pinned_tool=request.pinned_tool,
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
