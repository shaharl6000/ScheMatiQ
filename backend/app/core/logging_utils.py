"""Session-aware logging utilities.

Uses Python's contextvars to automatically inject session IDs into all
logger.* calls via a custom logging.Filter. Set the session context once
at the entry point (route handler or service method), and it propagates
automatically through BackgroundTasks, run_in_executor threads, and
asyncio.create_task calls.
"""

import contextvars
import logging

# Context variable holding the current session ID.
# Empty string means no session context (e.g., startup, system-level logs).
current_session_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    'current_session_id', default=''
)


class SessionFilter(logging.Filter):
    """Injects session_id into every log record from the contextvar."""

    def filter(self, record: logging.LogRecord) -> bool:
        sid = current_session_id.get('')
        record.session_id = sid[:8] if sid else 'no-session'
        return True


def set_session_context(session_id: str) -> contextvars.Token:
    """Set the session_id for the current async/thread context.

    Returns a token that can be used to reset the context via
    current_session_id.reset(token).
    """
    return current_session_id.set(session_id)
