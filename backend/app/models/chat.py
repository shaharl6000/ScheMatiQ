"""Pydantic models for chat agent API."""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ChatMessageRequest(BaseModel):
    message: str = Field(..., min_length=1)
    chat_id: Optional[str] = None
    session_mode: Literal["schematiq", "load"] = "schematiq"
    pinned_tool: Optional[str] = None


class ChatConfirmRequest(BaseModel):
    chat_id: str


class ChatTurnMessage(BaseModel):
    id: str
    role: Literal["assistant", "user", "tool"]
    content: str
    kind: Optional[Literal["text", "tool_log"]] = None
    tool_name: Optional[str] = None
    tool_status: Optional[Literal["running", "done", "error"]] = None


class PendingChatAction(BaseModel):
    tool_name: str
    label: str
    description: str
    args: dict[str, Any] = Field(default_factory=dict)


class ChatMessageResponse(BaseModel):
    chat_id: str
    status: Literal["complete", "pending_confirmation"]
    messages: list[ChatTurnMessage]
    pending_action: Optional[PendingChatAction] = None


class ChatToolInfo(BaseModel):
    name: str
    description: str
    cost_class: str
    available: bool
    parameters: dict[str, Any]


class ChatToolsResponse(BaseModel):
    tools: list[ChatToolInfo]
