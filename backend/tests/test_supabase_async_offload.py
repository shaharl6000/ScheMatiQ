"""Verify SupabaseStorageBackend offloads blocking SDK calls off the event loop."""

import asyncio
import json
import threading
import time

import pytest

from app.storage.supabase_backend import SupabaseStorageBackend


def _make_fake_backend(upload_delay_s: float = 0.0):
    recorded_threads: list[tuple[str, int]] = []
    buckets: dict[str, dict[str, bytes]] = {
        "sessions": {"s1.json": json.dumps({"id": "s1"}).encode("utf-8")},
        "data": {"s1/sub/part.jsonl": b'{"a":1}\n', "s1/root.jsonl": b'{"b":2}\n'},
        "datasets": {"ds1/doc1.txt": b"hello"},
    }

    class _FakeBucket:
        def __init__(self, name: str):
            self.name = name

        def upload(self, path: str, data: bytes, options=None):
            recorded_threads.append(("upload", threading.get_ident()))
            if upload_delay_s:
                time.sleep(upload_delay_s)
            buckets.setdefault(self.name, {})[path] = data

        def download(self, path: str) -> bytes:
            recorded_threads.append(("download", threading.get_ident()))
            store = buckets.get(self.name, {})
            if path not in store:
                raise RuntimeError("Object not found")
            return store[path]

        def remove(self, paths: list[str]):
            recorded_threads.append(("remove", threading.get_ident()))
            store = buckets.get(self.name, {})
            for p in paths:
                store.pop(p, None)

        def list(self, path: str = "", options=None):
            recorded_threads.append(("list", threading.get_ident()))
            store = buckets.get(self.name, {})
            prefix = f"{path.rstrip('/')}/" if path else ""
            entries: dict[str, dict] = {}
            for key in store:
                if prefix and not key.startswith(prefix):
                    continue
                rest = key[len(prefix):] if prefix else key
                if "/" in rest:
                    folder = rest.split("/", 1)[0]
                    entries.setdefault(folder, {"name": folder, "id": None})
                elif rest:
                    entries[rest] = {"name": rest, "id": key, "metadata": {"size": len(store[key])}}
            return list(entries.values())

    class _FakeStorage:
        def from_(self, bucket: str):
            return _FakeBucket(bucket)

    class _FakeClient:
        storage = _FakeStorage()

    backend = SupabaseStorageBackend.__new__(SupabaseStorageBackend)
    backend.client = _FakeClient()
    return backend, recorded_threads, buckets


@pytest.mark.asyncio
async def test_supabase_async_methods_execute_off_event_loop():
    backend, recorded_threads, buckets = _make_fake_backend()
    loop_tid = threading.get_ident()

    assert await backend.save_session("s2", {"id": "s2"}) is True
    assert await backend.get_session("s1") == {"id": "s1"}
    await backend.upload_file("data", "s2/file.jsonl", b"{}\n")
    assert await backend.download_file("data", "s2/file.jsonl") == b"{}\n"
    folder_files = await backend.list_folder_files("data", "s1")
    assert "root.jsonl" in folder_files
    all_files = await backend.list_files("data", "s1")
    assert sorted(all_files) == ["s1/root.jsonl", "s1/sub/part.jsonl"]
    assert await backend.delete_directory("data", "s1") is True
    assert "s1/root.jsonl" not in buckets["data"]
    datasets = await backend.list_datasets()
    assert [d.name for d in datasets] == ["ds1"]

    assert recorded_threads
    assert all(tid != loop_tid for _, tid in recorded_threads)


@pytest.mark.asyncio
async def test_upload_file_does_not_block_event_loop():
    backend, _, _ = _make_fake_backend(upload_delay_s=0.08)

    async def fast_ticker() -> float:
        t0 = time.monotonic()
        await asyncio.sleep(0.01)
        return time.monotonic() - t0

    upload_task = asyncio.create_task(backend.upload_file("data", "s1/slow.jsonl", b"{}\n"))
    await asyncio.sleep(0.005)
    ticker_elapsed = await fast_ticker()
    await upload_task

    assert ticker_elapsed < 0.05

