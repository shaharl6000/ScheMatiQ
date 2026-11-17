"""Template parsing and dataset file writes in SupabaseStorageBackend run off
the event loop, with the same results as before."""

import threading
from types import SimpleNamespace

from app.storage import supabase_backend as sb

CSV = b"# comment\nrow_name,papers,a,a_excerpt,b\nr1,p,1,x,2\nr2,p,3,y,4\n"
JSONL = b'{"data": {"a": 1, "b": 2, "c": 3}}\n{"data": {"a": 4, "b": 5, "c": 6}}\n'


class _Bucket:
    def __init__(self, files):
        self._files = files

    def list(self, *args):
        return [{"id": n, "name": n} for n in self._files]

    def download(self, name):
        return self._files[name]


def _backend(buckets):
    backend = sb.SupabaseStorageBackend.__new__(sb.SupabaseStorageBackend)
    backend.client = SimpleNamespace(storage=SimpleNamespace(from_=lambda b: _Bucket(buckets[b])))
    return backend


async def test_list_templates_parses_off_the_loop_with_same_counts(monkeypatch):
    loop_thread = threading.get_ident()
    parse_threads = []
    real = sb._template_counts

    def recording(name, ext, content):
        parse_threads.append(threading.get_ident())
        return real(name, ext, content)

    monkeypatch.setattr(sb, "_template_counts", recording)
    backend = _backend({"templates": {"b.csv": CSV, "a.jsonl": JSONL, "notes.txt": b"x", "bad.json": b"{"}})

    templates = await backend.list_templates()

    assert [t.name for t in templates] == ["a", "b", "bad"]
    a, b, bad = templates
    assert (a.row_count, a.column_count) == (2, 3)
    assert (b.row_count, b.column_count) == (2, 2)  # excerpt and metadata columns excluded
    assert (bad.row_count, bad.column_count) == (None, None)
    assert parse_threads and loop_thread not in parse_threads


async def test_download_dataset_to_local_writes_files(tmp_path, monkeypatch):
    backend = _backend({})

    async def list_files(name):
        return [SimpleNamespace(name="d1.txt"), SimpleNamespace(name="d2.txt")]

    async def download(name, file_name):
        return file_name.encode()

    monkeypatch.setattr(backend, "list_dataset_files", list_files, raising=False)
    monkeypatch.setattr(backend, "download_dataset_file", download, raising=False)

    created = await backend.download_dataset_to_local("ds", str(tmp_path / "out"))

    assert sorted(created) == [str(tmp_path / "out" / "d1.txt"), str(tmp_path / "out" / "d2.txt")]
    assert (tmp_path / "out" / "d2.txt").read_bytes() == b"d2.txt"
