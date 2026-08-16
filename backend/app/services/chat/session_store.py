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
    # The resolved model the SDK chat was built with. A Gemini chat is bound to
    # its model at creation, so this lets a reattach tell a plain refresh (same
    # model -> reuse) from a model switch (different model -> rebuild).
    model: Optional[str] = None
    pending: Optional[PendingToolCall] = None
    # Gemini calls made since the last quota flush (chat bypasses the
    # schematiq LLM backends, so calls are counted here explicitly).
    pending_llm_calls: int = 0


class ChatSessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, ChatSessionState] = {}

    def create(self, state: ChatSessionState) -> str:
        chat_id = str(uuid.uuid4())
        self._sessions[chat_id] = state
        return chat_id

    def get(self, chat_id: str) -> Optional[ChatSessionState]:
        return self._sessions.get(chat_id)

    def get_by_workspace_session(
        self, workspace_session_id: str
    ) -> Optional[ChatSessionState]:
        """Return the live chat for a workspace session, if one is in memory.

        One chat per project: a client that lost its ``chat_id`` (page refresh)
        or holds a stale id (after a redeploy repopulated the store) reattaches
        to the existing conversation instead of starting a new empty one. At
        most one state exists per workspace session under the current flow, so
        the first match is authoritative.
        """
        for state in self._sessions.values():
            if state.workspace_session_id == workspace_session_id:
                return state
        return None

    def delete(self, chat_id: str) -> None:
        self._sessions.pop(chat_id, None)


chat_session_store = ChatSessionStore()
