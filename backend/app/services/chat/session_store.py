"""In-memory store for Gemini chat sessions and pending confirmations."""

from __future__ import annotations

import threading
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

    def delete(self, chat_id: str) -> None:
        self._sessions.pop(chat_id, None)


class SessionMessageCounter:
    """Counts user chat messages per workspace session.

    Keyed on the *workspace* session id rather than the chat id. A chat id is
    minted per conversation and ``ChatSessionStore.delete`` drops it on stale
    chat recovery, so counting there would let a caller reset the cap simply by
    starting a new conversation.

    In-memory only: the counts reset when the process restarts, the same
    tradeoff as the local ``global_llm_usage.json`` runtime state. This is a
    soft cap meant to bound a single project's quota consumption, not a durable
    entitlement. Making it survive redeploys means persisting it to the storage
    backend, which is deliberately out of scope here.
    """

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def count(self, session_id: str) -> int:
        with self._lock:
            return self._counts.get(session_id, 0)

    def increment(self, session_id: str) -> int:
        with self._lock:
            total = self._counts.get(session_id, 0) + 1
            self._counts[session_id] = total
            return total


chat_session_store = ChatSessionStore()
session_message_counter = SessionMessageCounter()
