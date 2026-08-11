"""Session-aware logging utilities.

Uses Python's contextvars to automatically inject session IDs into all
logger.* calls via a custom logging.Filter. Set the session context once
at the entry point (route handler or service method).

Propagation is not uniform, which is worth knowing before adding a call:
``asyncio.create_task`` and ``asyncio.to_thread`` copy the current context,
so the session id survives. Plain ``loop.run_in_executor`` does NOT: the
function runs in a bare thread with a fresh context and every record from it
logs as ``no-session``. Use ``ContextPropagatingThreadPoolExecutor`` below
for executors, or ``asyncio.to_thread`` instead of the default executor.
"""

import contextvars
import functools
import logging
from concurrent.futures import ThreadPoolExecutor

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


class ContextPropagatingThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor that carries the caller's context into the worker.

    ``loop.run_in_executor(pool, fn)`` calls ``pool.submit(fn)``, and a plain
    pool runs ``fn`` in a thread with a fresh context, so ``current_session_id``
    is lost and the whole offloaded operation logs as ``no-session``. Copying
    the context at submit time fixes every call site through this pool without
    touching any of them.
    """

    def submit(self, fn, /, *args, **kwargs):
        ctx = contextvars.copy_context()
        return super().submit(ctx.run, functools.partial(fn, *args, **kwargs))


def set_session_context(session_id: str) -> contextvars.Token:
    """Set the session_id for the current async/thread context.

    Returns a token that can be used to reset the context via
    current_session_id.reset(token).
    """
    return current_session_id.set(session_id)
