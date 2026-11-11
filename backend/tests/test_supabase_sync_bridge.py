"""The shared sync bridge in supabase_backend (#634)."""

import asyncio
import threading

import pytest

from app.storage import supabase_backend as sb


def test_sync_call_inside_a_running_loop_runs_on_the_bridge_thread():
    async def inner():
        return threading.current_thread().name

    async def outer():
        return sb._run_coro_sync(inner())

    assert asyncio.run(outer()) == "supabase-sync-bridge"


def test_sync_call_from_the_bridge_loop_raises_instead_of_deadlocking():
    async def inner():
        return 1

    async def outer():
        with pytest.raises(RuntimeError, match="sync bridge loop"):
            sb._run_coro_sync(inner())
        return True

    loop = sb._get_bg_event_loop()
    assert asyncio.run_coroutine_threadsafe(outer(), loop).result(timeout=5)
