"""Persistence for chat summaries that never pass through the chat agent.

Some conversation messages are produced by background operations rather than by
the chat agent's own turns: the re-extraction recap and the reference-fill recap
are computed inside their services and pushed to the client over the WebSocket.
They are never part of the Gemini SDK history, so the transcript reconstruction
in ``agent_service`` (which reads only the persisted model history) cannot see
them, and they vanish on reload.

This module gives those messages a home. Each summary is written as its own
small file keyed by the originating operation id -- the same id that already
appears, at the correct chronological position, inside the ``function_response``
of the tool call that started the operation in the persisted model history. On
reload, ``get_transcript`` loads this map once and splices each summary in right
after its tool call, so the restored order matches what the user saw live.

Design notes:
- One file per summary (``{session_id}/chat_summaries/{op_id}.json``) so two
  concurrent operations can never race on a shared file (no read-modify-write).
- Gated by the same ``opt_out_data_collection`` flag as the model-history
  persistence, and strictly best-effort: a storage failure logs and continues,
  never breaking an operation or a transcript load.
- No dependency on ``agent_service``; the services and the agent both import
  this module, keeping the dependency graph acyclic.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Reuse the bucket the model-history persistence already writes to, so a
# session's chat artifacts live together and are cleaned up together.
CHAT_SUMMARY_BUCKET = "data"
CHAT_SUMMARY_VERSION = 1


def _chat_summaries_prefix(session_id: str) -> str:
    return f"{session_id}/chat_summaries/"


def _chat_summary_path(session_id: str, op_id: str) -> str:
    return f"{_chat_summaries_prefix(session_id)}{op_id}.json"


def _get_storage() -> Any:
    from app.storage import get_storage

    return get_storage()


def _opted_out(session_id: str) -> bool:
    """Mirror ``agent_service._history_opted_out`` without importing it.

    Persisting a summary is the same kind of retention as persisting the model
    history, so it is governed by the same flag. If opt-out cannot be read, we
    treat the session as opted out -- the privacy-conservative default, matching
    the model-history path.
    """
    try:
        from app.services.chat.deps import session_manager

        session = session_manager.get_session(session_id)
        return bool(session and session.opt_out_data_collection)
    except Exception:
        logger.debug(
            "could not read opt-out for session %s; skipping summary persistence",
            session_id,
            exc_info=True,
        )
        return True


async def save_summary(session_id: str, op_id: str, content: str) -> None:
    """Persist one WS-only summary message, keyed by its operation id.

    Best-effort and opt-out gated. ``op_id`` is the re-extraction ``operation_id``
    or the reference-fill ``fill_id`` -- the value that also appears in the tool
    call's persisted ``function_response``, which is how the summary is later
    anchored back into the transcript at the right position.
    """
    if not op_id or not content:
        return
    try:
        if _opted_out(session_id):
            return
        payload = {
            "version": CHAT_SUMMARY_VERSION,
            "op_id": op_id,
            "content": content,
        }
        await _get_storage().upload_json(
            CHAT_SUMMARY_BUCKET, _chat_summary_path(session_id, op_id), payload
        )
    except Exception as exc:
        logger.warning(
            "could not persist chat summary for session %s op %s: %s",
            session_id,
            op_id,
            exc,
        )


async def load_summaries(session_id: str) -> Dict[str, str]:
    """Return ``{op_id: content}`` for a session, or ``{}`` when absent/opted-out.

    Best-effort: any listing or read failure yields an empty map so a transcript
    load degrades to model-history-only (today's behavior) rather than failing.
    """
    try:
        if _opted_out(session_id):
            return {}
        storage = _get_storage()
        prefix = _chat_summaries_prefix(session_id)
        paths = await storage.list_files(CHAT_SUMMARY_BUCKET, prefix)
        summaries: Dict[str, str] = {}
        for path in paths or []:
            payload: Optional[Dict[str, Any]] = await storage.download_json(
                CHAT_SUMMARY_BUCKET, path
            )
            if not isinstance(payload, dict):
                continue
            op_id = payload.get("op_id")
            content = payload.get("content")
            if op_id and content:
                summaries[str(op_id)] = str(content)
        return summaries
    except Exception as exc:
        logger.warning(
            "could not load chat summaries for session %s: %s", session_id, exc
        )
        return {}
