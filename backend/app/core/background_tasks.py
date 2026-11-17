"""Fire-and-forget asyncio tasks that are kept alive and whose errors are logged.

The event loop holds only a weak reference to a task, so a task created with a
bare ``asyncio.create_task(...)`` and not stored anywhere can be garbage
collected before it finishes, and an exception it raises is reported only as
"Task exception was never retrieved" (if at all). ``spawn_background`` keeps a
strong reference until the task is done and logs any failure.
"""

import asyncio
import logging
from typing import Any, Coroutine, Optional, Set

logger = logging.getLogger(__name__)

_background_tasks: Set[asyncio.Task] = set()


def _on_done(task: asyncio.Task) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Background task %s failed: %s", task.get_name(), exc, exc_info=exc
        )


def spawn_background(
    coro: Coroutine[Any, Any, Any], *, name: Optional[str] = None
) -> asyncio.Task:
    """Schedule *coro* on the running loop and keep it alive until it finishes."""
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    task.add_done_callback(_on_done)
    return task
