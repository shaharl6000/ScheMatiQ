"""Tests for bundle source-document collection.

Focus on the additive behavior introduced so a re-imported bundle carries the
files needed to become extraction-capable: skipped documents (which have no
rows) and on-disk-only files (uploaded but not yet extracted). These exercise
the pure-filesystem helpers directly, so they need no LLM, no storage backend,
and no FastAPI import chain.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.services import document_files


def _make_session(skipped=None, cloud_dataset=None):
    stats = SimpleNamespace(
        skipped_documents=[
            SimpleNamespace(document=name) for name in (skipped or [])
        ]
    )
    metadata = SimpleNamespace(cloud_dataset=cloud_dataset)
    return SimpleNamespace(statistics=stats, metadata=metadata)


def _seed_docs(root: Path, session_id: str, sub: str, filenames):
    doc_dir = root / "data" / session_id / sub
    doc_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        (doc_dir / name).write_bytes(f"bytes::{name}".encode())
    return doc_dir


def test_iter_local_documents_covers_both_dirs_and_dedupes(tmp_path, monkeypatch):
    session_id = "s1"
    _seed_docs(tmp_path, session_id, "documents", ["a.pdf", "b.pdf"])
    _seed_docs(tmp_path, session_id, "pending_documents", ["c.pdf", "a.pdf"])
    # A hidden file must be ignored.
    (tmp_path / "data" / session_id / "documents" / ".DS_Store").write_bytes(b"x")

    monkeypatch.setattr(
        document_files, "candidate_data_dirs", lambda: [tmp_path / "data"]
    )

    found = sorted(p.name for p in document_files._iter_local_documents(session_id))
    # a.pdf appears in both dirs but is yielded once; hidden file excluded.
    assert found == ["a.pdf", "b.pdf", "c.pdf"]


def test_skipped_document_names_from_models_and_dicts():
    session = _make_session(skipped=["/tmp/x/skipped_one.pdf", "skipped_two.txt"])
    assert document_files._skipped_document_names(session) == [
        "skipped_one.pdf",
        "skipped_two.txt",
    ]
    # dict-shaped skipped entries are handled too, and a None session is safe.
    dict_session = SimpleNamespace(
        statistics=SimpleNamespace(skipped_documents=[{"document": "d.pdf"}])
    )
    assert document_files._skipped_document_names(dict_session) == ["d.pdf"]
    assert document_files._skipped_document_names(None) == []


async def test_gather_bundles_skipped_and_on_disk_docs(tmp_path, monkeypatch):
    session_id = "s1"
    # rowed.pdf is referenced by rows; skipped.pdf was skipped (no rows);
    # orphan.pdf is on disk but referenced by neither.
    _seed_docs(
        tmp_path, session_id, "documents", ["rowed.pdf", "skipped.pdf", "orphan.pdf"]
    )
    monkeypatch.setattr(
        document_files, "candidate_data_dirs", lambda: [tmp_path / "data"]
    )
    # Only rowed.pdf is row-referenced.
    monkeypatch.setattr(
        document_files.unit_view_service,
        "get_source_documents",
        lambda sid: [{"name": "rowed.pdf", "row_count": 2}],
    )

    session = _make_session(skipped=["skipped.pdf"])
    out = await document_files.gather_source_documents(session, session_id)

    names = sorted(name for name, _ in out)
    assert names == ["orphan.pdf", "rowed.pdf", "skipped.pdf"]
    # Bytes are the originals, one entry per file (no duplicates).
    assert len(out) == 3
    assert dict(out)["skipped.pdf"] == b"bytes::skipped.pdf"


async def test_gather_is_superset_never_drops_rowed_docs(tmp_path, monkeypatch):
    # Regression guard: adding skipped/on-disk collection must not remove the
    # row-referenced documents that were always bundled.
    session_id = "s2"
    _seed_docs(tmp_path, session_id, "documents", ["only_rowed.pdf"])
    monkeypatch.setattr(
        document_files, "candidate_data_dirs", lambda: [tmp_path / "data"]
    )
    monkeypatch.setattr(
        document_files.unit_view_service,
        "get_source_documents",
        lambda sid: [{"name": "only_rowed.pdf", "row_count": 1}],
    )
    session = _make_session()  # no skipped docs

    out = await document_files.gather_source_documents(session, session_id)
    assert [name for name, _ in out] == ["only_rowed.pdf"]
