"""In-memory store for Gemini chat sessions and pending confirmations."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class PendingToolCall:
    tool_name: str
    args: dict[str, Any]
    function_call_part: Any


@dataclass
class ChatSessionState:
    client: Any
    chat: Any
    workspace_session_id: str
    session_mode: str
    chat_id: Optional[str] = None
    pending: Optional[PendingToolCall] = None
    pinned_tool: Optional[str] = None


class ChatSessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, ChatSessionState] = {}

    def create(self, state: ChatSessionState) -> str:
        chat_id = str(uuid.uuid4())
        self._sessions[chat_id] = state
        return chat_id

    def get(self, chat_id: str) -> Optional[ChatSessionState]:
        return self._sessions.get(chat_id)

    def delete(self, chat_id: str) -> None:
        self._sessions.pop(chat_id, None)


chat_session_store = ChatSessionStore()
