"""Continue discovery's incremental extraction reads rows from, and merges new
column values into, every session data file. A ScheMatiQ-run session keeps its
rows in the work dir's extracted_data.jsonl, which the old data/<sid>/data.jsonl
only code never saw."""

import importlib
import json
from pathlib import Path

cds = importlib.import_module("app.services.continue_discovery_service")


def _write(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _read(path: Path):
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def _service(tmp_path, monkeypatch, sid):
    svc = cds.ContinueDiscoveryService.__new__(cds.ContinueDiscoveryService)
    svc._get_data_dir = lambda: tmp_path / "data"
    svc._get_schematiq_work_dir = lambda: tmp_path / "work"
    svc.session_manager = type("SM", (), {"get_session": lambda self, s: None})()
    persisted = []

    async def files(session_id, *args, **kwargs):
        candidates = [
            tmp_path / "work" / session_id / "extracted_data.jsonl",
            tmp_path / "data" / session_id / "data.jsonl",
        ]
        return [p for p in candidates if p.exists()]

    async def persist(session_id, local_path, storage=None):
        persisted.append(Path(local_path))

    monkeypatch.setattr(cds, "resolve_session_data_files", files)
    from app.services import session_data_store
    monkeypatch.setattr(session_data_store, "persist_session_data_file", persist)
    return svc, persisted


async def test_rows_in_data_dir_get_new_values(tmp_path, monkeypatch):
    sid = "s-data"
    svc, persisted = _service(tmp_path, monkeypatch, sid)
    data_file = tmp_path / "data" / sid / "data.jsonl"
    _write(data_file, [{"_row_name": "r1", "_source_document": "doc1", "data": {"a": 1}}])
    extraction = tmp_path / "out.jsonl"
    _write(extraction, [{"_row_name": "r1", "_source_document": "doc1", "new_col": "v1"}])

    await svc._merge_incremental_data(sid, ["new_col"], extraction)

    assert _read(data_file)[0]["data"]["new_col"] == "v1"
    assert persisted == [data_file]


async def test_rows_in_work_dir_extracted_data_get_new_values(tmp_path, monkeypatch):
    sid = "s-work"
    svc, persisted = _service(tmp_path, monkeypatch, sid)
    work_file = tmp_path / "work" / sid / "extracted_data.jsonl"
    _write(work_file, [
        {"_row_name": "r1", "_source_document": "doc1", "a": 1},
        {"_row_name": "r2", "_source_document": "doc2", "a": 2},
    ])
    extraction = tmp_path / "out.jsonl"
    _write(extraction, [{"_row_name": "r1", "_source_document": "doc1", "new_col": "v1"}])

    await svc._merge_incremental_data(sid, ["new_col"], extraction)

    r1, r2 = _read(work_file)
    assert r1["new_col"] == "v1" and r1["a"] == 1
    assert "new_col" in r2 and r2["new_col"] is None
    assert persisted == [work_file]


async def test_rows_in_both_files_are_all_updated(tmp_path, monkeypatch):
    sid = "s-both"
    svc, persisted = _service(tmp_path, monkeypatch, sid)
    work_file = tmp_path / "work" / sid / "extracted_data.jsonl"
    data_file = tmp_path / "data" / sid / "data.jsonl"
    _write(work_file, [{"_row_name": "r1", "_source_document": "doc1"}])
    _write(data_file, [{"_row_name": "r2", "_source_document": "doc2"}])
    extraction = tmp_path / "out.jsonl"
    _write(extraction, [
        {"_row_name": "r1", "_source_document": "doc1", "new_col": "v1"},
        {"_row_name": "r2", "_source_document": "doc2", "new_col": "v2"},
    ])

    await svc._merge_incremental_data(sid, ["new_col"], extraction)

    assert _read(work_file)[0]["new_col"] == "v1"
    assert _read(data_file)[0]["new_col"] == "v2"
    assert sorted(persisted) == sorted([work_file, data_file])


def test_extraction_scope_loads_rows_from_all_files_once(tmp_path):
    work_file = tmp_path / "extracted_data.jsonl"
    data_file = tmp_path / "data.jsonl"
    _write(work_file, [
        {"_row_name": "r1", "_source_document": "doc1", "papers": ["doc1.pdf"]},
    ])
    _write(data_file, [
        {"_row_name": "r1", "_source_document": "doc1", "papers": ["doc1.pdf"]},
        {"_row_name": "r2", "_source_document": "doc2", "papers": ["doc2.pdf"]},
    ])

    rows = cds._load_session_rows([work_file, data_file])

    assert [r["_row_name"] for r in rows] == ["r1", "r2"]
