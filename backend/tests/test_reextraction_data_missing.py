"""Tests for paper discovery and re-extraction resilience when extracted data is missing."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.models.session import (
    ColumnInfo,
    ObservationUnitInfo,
    SessionMetadata,
    SessionStatus,
    SessionType,
    VisualizationSession,
)
from app.services.reextraction_service import ReextractionOperation, ReextractionService


class MockDatasetStorage:
    """Minimal storage stub supporting list_folder_files and download_file."""

    def __init__(self, dataset_files: dict[str, set[str]] | None = None):
        self.dataset_files = dict(dataset_files or {})

    async def list_folder_files(self, bucket: str, folder: str) -> set[str]:
        if bucket != "datasets":
            return set()
        return set(self.dataset_files.get(folder, set()))

    async def download_file(self, bucket: str, path: str) -> bytes | None:
        return None

    async def file_exists(self, bucket: str, path: str) -> bool:
        return False


def _make_session(
    session_id: str,
    *,
    cloud_dataset: str | None = None,
    uploaded_documents: list[str] | None = None,
    document_metadata: dict[str, dict] | None = None,
) -> VisualizationSession:
    return VisualizationSession(
        id=session_id,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="schematiq",
            cloud_dataset=cloud_dataset,
            uploaded_documents=list(uploaded_documents or []),
            document_metadata=dict(document_metadata or {}),
        ),
        columns=[
            ColumnInfo(name="col1", definition="Column 1"),
            ColumnInfo(name="col2", definition="Column 2"),
        ],
        schema_query="Test query",
        observation_unit=ObservationUnitInfo(
            name="Study",
            definition="One empirical study",
        ),
    )


def _make_service(session: VisualizationSession) -> ReextractionService:
    session_manager = MagicMock()
    session_manager.get_session.return_value = session
    return ReextractionService(
        websocket_manager=MagicMock(),
        session_manager=session_manager,
    )


@pytest.fixture
def isolated_dirs(tmp_path, monkeypatch):
    """Isolate storage under tmp_path with CWD set to tmp_path."""
    monkeypatch.chdir(tmp_path)
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"
    work_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    return work_dir, data_dir


@pytest.mark.asyncio
async def test_precheck_discovers_cloud_dataset_when_data_files_missing(
    isolated_dirs, monkeypatch
):
    """When data_files == [] and local disk is empty, recorded cloud docs are discovered."""
    _work_dir, _data_dir = isolated_dirs
    session_id = "sess-cloud-missing-data"
    session = _make_session(
        session_id,
        cloud_dataset="my_dataset",
        uploaded_documents=["doc1.txt", "doc2.txt"],
    )

    storage = MockDatasetStorage({"my_dataset": {"doc1.txt", "doc2.txt"}})
    monkeypatch.setattr(
        "app.services.reextraction_service.get_storage",
        lambda: storage,
    )

    service = _make_service(session)
    availability = await service.precheck_document_availability(session_id)

    assert availability["can_proceed"] is True
    assert availability["total_documents"] == 2
    assert availability["local_documents"] == []
    assert availability["missing_documents"] == []
    assert sorted(d["name"] for d in availability["cloud_documents"]) == [
        "doc1.txt",
        "doc2.txt",
    ]
    assert sorted(d["cloud_path"] for d in availability["cloud_documents"]) == [
        "my_dataset/doc1.txt",
        "my_dataset/doc2.txt",
    ]


def _big_dataset_storage() -> MockDatasetStorage:
    files = {f"other_{i}.txt" for i in range(50)} | {"picked_a.pdf", "picked_b.txt"}
    return MockDatasetStorage({"big_dataset": files})


@pytest.mark.asyncio
async def test_discover_papers_uses_only_picked_cloud_docs_after_redeploy(
    isolated_dirs, monkeypatch
):
    """A cloud subset session re-runs on its own docs, not the whole dataset folder."""
    session_id = "sess-cloud-subset"
    session = _make_session(
        session_id,
        cloud_dataset="big_dataset",
        # picked_a.pdf was recorded under its converted display name.
        uploaded_documents=["picked_a.txt", "picked_b.txt"],
    )
    storage = _big_dataset_storage()
    monkeypatch.setattr("app.services.reextraction_service.get_storage", lambda: storage)

    discovery = await _make_service(session).discover_papers(session_id)

    assert discovery["cloud_papers"] == {
        "picked_a.pdf": "big_dataset/picked_a.pdf",
        "picked_b.txt": "big_dataset/picked_b.txt",
    }
    assert discovery["missing_papers"] == []


@pytest.mark.asyncio
async def test_discover_papers_does_not_add_cloud_docs_when_picked_docs_are_local(
    isolated_dirs, monkeypatch
):
    """First extraction of a cloud subset session uses local docs only."""
    _work_dir, data_dir = isolated_dirs
    session_id = "sess-cloud-subset-local"
    docs_dir = data_dir / session_id / "documents"
    docs_dir.mkdir(parents=True)
    (docs_dir / "picked_a.txt").write_text("a", encoding="utf-8")
    (docs_dir / "picked_b.txt").write_text("b", encoding="utf-8")
    session = _make_session(
        session_id,
        cloud_dataset="big_dataset",
        uploaded_documents=["picked_a.txt", "picked_b.txt"],
    )
    storage = _big_dataset_storage()
    monkeypatch.setattr("app.services.reextraction_service.get_storage", lambda: storage)

    discovery = await _make_service(session).discover_papers(session_id)

    assert sorted(discovery["local_papers"]) == ["picked_a.txt", "picked_b.txt"]
    assert discovery["cloud_papers"] == {}
    assert discovery["missing_papers"] == []


@pytest.mark.asyncio
async def test_discover_papers_marks_picked_doc_missing_when_not_in_cloud(
    isolated_dirs, monkeypatch
):
    """A recorded doc absent from disk and from the dataset folder is reported missing."""
    session_id = "sess-cloud-subset-gone"
    session = _make_session(
        session_id,
        cloud_dataset="big_dataset",
        uploaded_documents=["picked_b.txt", "removed.txt"],
    )
    storage = _big_dataset_storage()
    monkeypatch.setattr("app.services.reextraction_service.get_storage", lambda: storage)

    discovery = await _make_service(session).discover_papers(session_id)

    assert discovery["cloud_papers"] == {"picked_b.txt": "big_dataset/picked_b.txt"}
    assert discovery["missing_papers"] == ["removed.txt"]


@pytest.mark.asyncio
async def test_discover_papers_ignores_dataset_folder_without_recorded_docs(
    isolated_dirs, monkeypatch
):
    """With no record of which docs were used, never fall back to the whole folder."""
    session_id = "sess-cloud-unrecorded"
    session = _make_session(session_id, cloud_dataset="big_dataset")
    storage = _big_dataset_storage()
    monkeypatch.setattr("app.services.reextraction_service.get_storage", lambda: storage)

    discovery = await _make_service(session).discover_papers(session_id)

    assert discovery["cloud_papers"] == {}
    assert discovery["available_papers"] == []


@pytest.mark.asyncio
async def test_precheck_lists_recorded_uploaded_documents_when_data_and_disk_missing(
    isolated_dirs, monkeypatch
):
    """Recorded uploaded_documents appear as missing when disk is empty, and resolve once re-uploaded."""
    _work_dir, data_dir = isolated_dirs
    session_id = "sess-upload-missing-data"
    session = _make_session(
        session_id,
        uploaded_documents=["paper_a.pdf", "paper_b.pdf"],
    )

    storage = MockDatasetStorage()
    monkeypatch.setattr(
        "app.services.reextraction_service.get_storage",
        lambda: storage,
    )

    service = _make_service(session)
    initial = await service.precheck_document_availability(session_id)

    assert initial["can_proceed"] is False
    assert initial["local_documents"] == []
    assert initial["cloud_documents"] == []
    assert [d["name"] for d in initial["missing_documents"]] == [
        "paper_a.pdf",
        "paper_b.pdf",
    ]

    # Re-uploading paper_a.pdf to data/{sid}/documents/ makes it available and leaves paper_b.pdf missing.
    docs_dir = data_dir / session_id / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "paper_a.pdf").write_bytes(b"%PDF-1.4 fake")

    after_one = await service.precheck_document_availability(session_id)
    assert after_one["can_proceed"] is True
    assert [d["name"] for d in after_one["local_documents"]] == ["paper_a.pdf"]
    assert [d["name"] for d in after_one["missing_documents"]] == ["paper_b.pdf"]

    # Preprocessing that commits paper_b.pdf as paper_b.txt also satisfies paper_b.pdf by stem.
    (docs_dir / "paper_b.txt").write_text("converted text", encoding="utf-8")

    after_both = await service.precheck_document_availability(session_id)
    assert after_both["can_proceed"] is True
    assert {d["name"] for d in after_both["local_documents"]} == {
        "paper_a.pdf",
        "paper_b.txt",
    }
    assert after_both["missing_documents"] == []


@pytest.mark.asyncio
async def test_run_reextraction_creates_missing_session_data_dir_before_writing_schema(
    isolated_dirs, monkeypatch
):
    """_run_reextraction creates data/{sid} if absent (e.g. docs live only in schematiq_work)."""
    work_dir, data_dir = isolated_dirs
    session_id = "sess-no-data-dir"
    session = _make_session(session_id)

    # Documents exist only under schematiq_work/{sid}/datasets/ds; data/{sid} does NOT exist!
    ds_dir = work_dir / session_id / "datasets" / "ds"
    ds_dir.mkdir(parents=True)
    (ds_dir / "doc1.txt").write_text("Study content", encoding="utf-8")
    assert not (data_dir / session_id).exists()

    storage = MockDatasetStorage()
    monkeypatch.setattr(
        "app.services.reextraction_service.get_storage",
        lambda: storage,
    )

    written_schema_snapshot: dict = {}

    def _fake_build_table_jsonl(*, schema_path, output_path, **kwargs):
        with open(schema_path, encoding="utf-8") as f:
            written_schema_snapshot.update(json.load(f))
        Path(output_path).write_text("", encoding="utf-8")

    monkeypatch.setattr(
        "app.services.reextraction_service.build_table_jsonl",
        _fake_build_table_jsonl,
    )

    async def _noop_merge(*_args, **_kwargs):
        return None

    async def _noop_broadcast(*_args, **_kwargs):
        return None

    service = _make_service(session)
    service.get_cached_retriever = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(service, "_merge_reextracted_data", _noop_merge)
    monkeypatch.setattr(service, "broadcast_event", _noop_broadcast)
    monkeypatch.setattr(
        service, "_summarize_reextraction", lambda *_a, **_kw: _noop_broadcast()
    )

    discovery = await service.discover_papers(session_id)
    op = ReextractionOperation(
        operation_id="op123456",
        session_id=session_id,
        columns=["col1", "col2"],
        status="starting",
    )
    op.total_documents = 1
    op.paper_discovery = discovery
    op.llm = MagicMock()
    service.active_operations[op.operation_id] = op

    await service._run_reextraction(op.operation_id)

    assert op.status == "completed", f"Unexpected error: {op.error}"
    assert (data_dir / session_id).is_dir()
    assert [c["column"] for c in written_schema_snapshot["schema"]] == ["col1", "col2"]
