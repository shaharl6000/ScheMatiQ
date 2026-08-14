"""Tests for gather_source_documents (project bundle export).

The bundle must round-trip every source file that physically exists for a
session — not just documents referenced by table rows. In particular a
previously-skipped document (no rows) and a file re-attached via "Show source
document" (lands in pending_documents/) must be included so a re-imported
project can preview them and re-run discovery against them.
"""

from types import SimpleNamespace

import pytest

from app.services import document_files


@pytest.fixture
def patched_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(document_files, "candidate_data_dirs", lambda: [tmp_path])
    # No cloud, and only one row-backed document reported by the unit view.
    monkeypatch.setattr(
        document_files.unit_view_service,
        "get_source_documents",
        lambda session_id: [{"name": "OtherDoc.txt", "row_count": 3}],
    )

    def _no_storage():
        raise RuntimeError("cloud not configured in test")

    monkeypatch.setattr(document_files, "get_storage", _no_storage)
    return tmp_path


def _session_with_skipped(*skipped_names):
    return SimpleNamespace(
        statistics=SimpleNamespace(
            skipped_documents=[
                SimpleNamespace(document=n) for n in skipped_names
            ]
        ),
        metadata=SimpleNamespace(cloud_dataset=None),
    )


async def test_bundle_includes_skipped_and_on_disk_documents(patched_dirs):
    session_id = "sess-bundle"
    docs = patched_dirs / session_id / "documents"
    pending = patched_dirs / session_id / "pending_documents"
    docs.mkdir(parents=True)
    pending.mkdir(parents=True)

    (docs / "OtherDoc.txt").write_bytes(b"row-backed")
    # Skipped document re-attached via "Show source document" (no rows).
    (pending / "CASA2025-06-27SCt.txt").write_bytes(b"skipped re-attached")
    # A file on disk that is neither row-backed nor recorded as skipped.
    (docs / "RandomExtra.txt").write_bytes(b"orphan on disk")

    session = _session_with_skipped("CASA2025-06-27SCt")
    result = await document_files.gather_source_documents(session, session_id)
    names = sorted(name for name, _ in result)

    assert "OtherDoc.txt" in names          # row-backed (existing behavior)
    assert "CASA2025-06-27SCt.txt" in names  # skipped, re-attached (new)
    assert "RandomExtra.txt" in names        # on-disk orphan (new)
    # Deduped: the row-backed doc is included exactly once.
    assert names.count("OtherDoc.txt") == 1


async def test_bundle_dedups_skipped_name_against_row_backed(patched_dirs):
    """A skipped name that also resolves on disk is not double-added."""
    session_id = "sess-dedup"
    docs = patched_dirs / session_id / "documents"
    docs.mkdir(parents=True)
    (docs / "OtherDoc.txt").write_bytes(b"row-backed")

    # Same stem listed as skipped too — must not appear twice.
    session = _session_with_skipped("OtherDoc")
    result = await document_files.gather_source_documents(session, session_id)
    names = [name for name, _ in result]

    assert names.count("OtherDoc.txt") == 1
