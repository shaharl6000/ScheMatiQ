"""Tests for re-extraction merge with storage-backed session data."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services.reextraction_service import ReextractionService


class InMemoryStorage:
    """Minimal storage mock for data-bucket JSONL hydration tests."""

    def __init__(self, files: dict[str, bytes] | None = None):
        self.files = dict(files or {})

    async def download_file(self, bucket: str, path: str) -> bytes | None:
        return self.files.get(f"{bucket}/{path}")

    async def file_exists(self, bucket: str, path: str) -> bool:
        return f"{bucket}/{path}" in self.files

    async def upload_file(self, bucket: str, path: str, content: bytes, **kwargs) -> None:
        self.files[f"{bucket}/{path}"] = content


def _make_service() -> ReextractionService:
    return ReextractionService(
        websocket_manager=MagicMock(),
        session_manager=MagicMock(),
    )


def _jsonl_lines(rows: list[dict]) -> bytes:
    return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode()


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    """Isolate storage under tmp_path with CWD set like dev.sh (runtime = tmp_path)."""
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    return work_dir, data_dir


@pytest.mark.asyncio
async def test_merge_hydrates_from_storage_and_preserves_other_columns(
    isolated_dirs, monkeypatch
):
    """Scoped re-extraction must not wipe rows/columns when data lives only in storage."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-merge-1"
    storage_key = f"data/{session_id}/extracted_data.jsonl"

    existing_rows = [
        {
            "_row_name": "Row A",
            "_source_document": "doc1",
            "Title": {"answer": "Original A"},
            "Year": {"answer": "2020"},
        },
        {
            "_row_name": "Row B",
            "_source_document": "doc2",
            "Title": {"answer": "Original B"},
            "Year": {"answer": "2021"},
        },
    ]
    storage = InMemoryStorage({storage_key: _jsonl_lines(existing_rows)})

    monkeypatch.setattr(
        "app.storage.get_storage",
        lambda: storage,
    )
    async def _noop_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _noop_persist,
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.parent.mkdir(parents=True)
    extraction_rows = [
        {
            "_row_name": "Row A",
            "_source_document": "doc1",
            "Title": {"answer": "Updated A"},
        },
        {
            "_row_name": "Row B",
            "_source_document": "doc2",
            "Title": {"answer": "Updated B"},
        },
    ]
    extraction_file.write_bytes(_jsonl_lines(extraction_rows))

    service = _make_service()
    await service._merge_reextracted_data(
        session_id,
        columns=["Title"],
        extraction_file=extraction_file,
    )

    merged_path = work_dir / session_id / "extracted_data.jsonl"
    assert merged_path.exists()
    merged = _read_jsonl(merged_path)
    assert len(merged) == 2

    by_name = {r["_row_name"]: r for r in merged}
    assert by_name["Row A"]["Title"]["answer"] == "Updated A"
    assert by_name["Row B"]["Title"]["answer"] == "Updated B"
    assert by_name["Row A"]["Year"]["answer"] == "2020"
    assert by_name["Row B"]["Year"]["answer"] == "2021"


@pytest.mark.asyncio
async def test_merge_refuses_wipe_when_storage_has_data_but_hydration_fails(
    isolated_dirs, monkeypatch
):
    """Empty local data_files + data in storage must abort, not overwrite."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-guard-1"
    storage_key = f"data/{session_id}/extracted_data.jsonl"

    storage = InMemoryStorage(
        {
            storage_key: _jsonl_lines(
                [{"_row_name": "Only Row", "Col": {"answer": "keep me"}}]
            )
        }
    )

    async def _hydration_fails(*_args, **_kwargs):
        return False

    monkeypatch.setattr(
        "app.storage.get_storage",
        lambda: storage,
    )
    monkeypatch.setattr(
        "app.services.data_utils.ensure_session_data_file_local",
        _hydration_fails,
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.parent.mkdir(parents=True)
    extraction_file.write_bytes(
        _jsonl_lines([{"_row_name": "Only Row", "Col": {"answer": "wiped"}}])
    )

    service = _make_service()
    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        await service._merge_reextracted_data(
            session_id,
            columns=["Col"],
            extraction_file=extraction_file,
        )

    assert not (work_dir / session_id / "extracted_data.jsonl").exists()


@pytest.mark.asyncio
async def test_merge_first_extraction_still_creates_file_when_no_storage_data(
    isolated_dirs, monkeypatch
):
    """Genuine first extraction (no local or remote data) still writes extracted rows."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-first-1"
    storage = InMemoryStorage()

    monkeypatch.setattr(
        "app.storage.get_storage",
        lambda: storage,
    )
    persisted: list[Path] = []

    async def _capture_persist(sid, path, storage=None):
        persisted.append(path)

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _capture_persist,
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.parent.mkdir(parents=True)
    extraction_file.write_bytes(
        _jsonl_lines([{"_row_name": "New Row", "Col": {"answer": "fresh"}}])
    )

    service = _make_service()
    service._update_session_stats_after_merge = MagicMock()

    await service._merge_reextracted_data(
        session_id,
        columns=["Col"],
        extraction_file=extraction_file,
    )

    out = work_dir / session_id / "extracted_data.jsonl"
    assert out.exists()
    rows = _read_jsonl(out)
    assert len(rows) == 1
    assert rows[0]["Col"]["answer"] == "fresh"
    assert persisted == [out]


@pytest.mark.asyncio
async def test_merge_space_named_column_from_sanitized_extraction_key(
    isolated_dirs, monkeypatch
):
    """Re-extract output may use sanitized keys; merge must write schema column names."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-space-col"
    storage = InMemoryStorage()

    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    async def _noop_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _noop_persist,
    )

    extracted_path = work_dir / session_id / "extracted_data.jsonl"
    extracted_path.parent.mkdir(parents=True)
    extracted_path.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Row A",
                    "_source_document": "doc1",
                    "judge_name": {"answer": "Original"},
                }
            ]
        )
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Row A",
                    "_source_document": "doc1",
                    "judge_full_name": {"answer": "Jane Smith"},
                }
            ]
        )
    )

    service = _make_service()
    await service._merge_reextracted_data(
        session_id,
        columns=["judge full name"],
        extraction_file=extraction_file,
    )

    merged = _read_jsonl(extracted_path)[0]
    assert merged["judge full name"]["answer"] == "Jane Smith"
    assert "judge_full_name" not in merged
    assert merged["judge_name"]["answer"] == "Original"


@pytest.mark.asyncio
async def test_merge_hyphen_named_column_from_sanitized_extraction_key(
    isolated_dirs, monkeypatch
):
    """Hyphens sanitize the same way as spaces (IssueCourt-1 -> IssueCourt_1)."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-hyphen-col"
    storage = InMemoryStorage()

    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    async def _noop_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _noop_persist,
    )

    col_name = "IssueCourt-1"
    extracted_path = work_dir / session_id / "extracted_data.jsonl"
    extracted_path.parent.mkdir(parents=True)
    extracted_path.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Row A",
                    "_source_document": "doc1",
                    "other_col": {"answer": "keep"},
                }
            ]
        )
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Row A",
                    "_source_document": "doc1",
                    "IssueCourt_1": {"answer": "Ninth Circuit"},
                }
            ]
        )
    )

    service = _make_service()
    await service._merge_reextracted_data(
        session_id,
        columns=[col_name],
        extraction_file=extraction_file,
    )

    merged = _read_jsonl(extracted_path)[0]
    assert merged[col_name]["answer"] == "Ninth Circuit"
    assert "IssueCourt_1" not in merged
    assert merged["other_col"]["answer"] == "keep"


@pytest.mark.asyncio
async def test_merge_loose_row_name_match_after_observation_unit_rediscovery(
    isolated_dirs, monkeypatch
):
    """OU rediscovery may shorten unit names; merge must still update existing rows."""
    work_dir, _data_dir = isolated_dirs
    session_id = "sess-loose-name"
    storage = InMemoryStorage()

    monkeypatch.setattr("app.storage.get_storage", lambda: storage)

    async def _noop_persist(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        "app.services.data_utils.persist_session_data_file",
        _noop_persist,
    )

    extracted_path = work_dir / session_id / "extracted_data.jsonl"
    extracted_path.parent.mkdir(parents=True)
    extracted_path.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "David J. Barron",
                    "_source_document": "doc2",
                    "_papers": ["doc2.txt"],
                    "judge_name": {"answer": "Barron"},
                },
            ]
        )
    )

    extraction_file = work_dir / session_id / "reextract_output.jsonl"
    extraction_file.write_bytes(
        _jsonl_lines(
            [
                {
                    "_row_name": "Barron",
                    "_source_document": "doc2",
                    "_papers": ["doc2.txt"],
                    "judge_full_name": {"answer": "David J. Barron"},
                },
            ]
        )
    )

    service = _make_service()
    await service._merge_reextracted_data(
        session_id,
        columns=["judge full name"],
        extraction_file=extraction_file,
    )

    merged = _read_jsonl(extracted_path)
    assert len(merged) == 1
    assert merged[0]["judge full name"]["answer"] == "David J. Barron"
    assert merged[0]["judge_name"]["answer"] == "Barron"
