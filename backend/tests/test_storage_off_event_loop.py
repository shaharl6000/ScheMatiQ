"""Supabase calls run off the event loop, keep per-object write order, and
session saves on the server loop do not wait for the upload."""

import asyncio
import threading
import time
from types import SimpleNamespace

from app.storage.supabase_backend import SupabaseStorageBackend, _ObjectWriteOrder


class _FakeBucket:
    def __init__(self, store, delays, name):
        self._store, self._delays, self._name = store, delays, name

    def upload(self, path, data, options):
        time.sleep(self._delays.pop(0) if self._delays else 0)
        self._store[(self._name, path)] = data

    def remove(self, paths):
        for p in paths:
            self._store.pop((self._name, p), None)

    def download(self, path):
        return self._store[(self._name, path)]


def _backend(delays=None):
    store: dict = {}
    delays = list(delays or [])
    backend = SupabaseStorageBackend.__new__(SupabaseStorageBackend)
    backend.client = SimpleNamespace(
        storage=SimpleNamespace(from_=lambda name: _FakeBucket(store, delays, name))
    )
    backend._writes = _ObjectWriteOrder()
    backend._get_storage_path = lambda bucket, path: path
    return backend, store


def test_older_write_is_skipped_once_a_newer_one_landed():
    order = _ObjectWriteOrder()
    old, new = order.ticket(), order.ticket()
    ran = []
    assert order.run([("b", "k")], new, lambda: ran.append("new")) is True
    assert order.run([("b", "k")], old, lambda: ran.append("old")) is False
    assert ran == ["new"]


async def test_upload_does_not_block_the_event_loop():
    backend, store = _backend(delays=[0.3])
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    t = asyncio.create_task(ticker())
    await backend.upload_file("data", "s/data.jsonl", b"x")
    t.cancel()
    assert store[("data", "s/data.jsonl")] == b"x"
    assert ticks >= 10


async def test_newest_upload_wins_when_an_older_one_is_slower():
    backend, store = _backend(delays=[0.2, 0.0])
    await asyncio.gather(
        backend.upload_file("data", "s/data.jsonl", b"old"),
        backend.upload_file("data", "s/data.jsonl", b"new"),
    )
    assert store[("data", "s/data.jsonl")] == b"new"


async def test_session_delete_is_not_undone_by_an_earlier_slow_save():
    backend, store = _backend(delays=[0.2])
    await asyncio.gather(
        backend.save_session("s1", {"id": "s1"}),
        backend.delete_session("s1"),
    )
    assert ("sessions", "s1.json") not in store


class _SlowNetworkStorage:
    saves_over_network = True

    def __init__(self):
        self.saved: list = []
        self.sync_calls = 0
        self.release = asyncio.Event()

    async def save_session(self, session_id, data):
        await self.release.wait()
        self.saved.append(data["marker"])
        return True

    def save_session_sync(self, session_id, data):
        self.sync_calls += 1
        self.saved.append(data["marker"])
        return True


class _Session:
    def __init__(self, marker):
        self.id = "s1"
        self.marker = marker

    def model_dump(self):
        return {"marker": self.marker}


async def test_save_on_the_server_loop_runs_in_the_background():
    from app.services.session_manager import SessionManager

    storage = _SlowNetworkStorage()
    mgr = SessionManager(storage=storage)
    mgr.bind_server_loop(asyncio.get_running_loop())

    mgr._save_session(_Session("a"))  # returns without waiting for the upload
    assert storage.saved == [] and storage.sync_calls == 0

    storage.release.set()
    await mgr.flush_pending_saves()
    assert storage.saved == ["a"]
    assert not mgr._pending_saves


def test_save_off_the_server_loop_stays_synchronous():
    from app.services.session_manager import SessionManager

    storage = _SlowNetworkStorage()
    mgr = SessionManager(storage=storage)
    server_loop = asyncio.new_event_loop()
    try:
        mgr.bind_server_loop(server_loop)
        worker = threading.Thread(target=mgr._save_session, args=(_Session("t"),))
        worker.start()
        worker.join(5)
    finally:
        server_loop.close()
    assert storage.sync_calls == 1 and storage.saved == ["t"]
