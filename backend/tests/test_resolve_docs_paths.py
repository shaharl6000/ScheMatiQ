"""Tests for resolve_docs_paths — the pipeline's source-document resolution.

This is the glue that decides whether a run actually finds a session's source
documents. For imported sessions it is the difference between re-extracting a
re-attached document and silently running in schema-only mode (the failure the
availability work set out to fix). It must return BOTH pending_documents/ and
documents/ when populated, so freshly re-attached files and previously-committed
originals are both fed to the pipeline.

These are pure filesystem tests: no LLM, no pipeline run, no network. The cloud
branch (download_supabase_dataset) is only reached when there are no local docs
and is covered separately by leaving both dirs empty with no docs_path.
"""

import pytest

from app.models.schematiq import LLMConfig, ScheMatiQConfig
from app.services.pipeline.config_handler import resolve_docs_paths


def _config(docs_path=None) -> ScheMatiQConfig:
    backend = LLMConfig(provider="openai")
    return ScheMatiQConfig(
        query="q",
        docs_path=docs_path,
        schema_creation_backend=backend,
        value_extraction_backend=backend,
        output_path="outputs/out.json",
    )


def _write(dir_path, name):
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / name).write_text("content", encoding="utf-8")


@pytest.mark.asyncio
async def test_resolves_documents_dir_for_imported_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-docs"
    work_dir = tmp_path / "schematiq_work"
    _write(tmp_path / "data" / session_id / "documents", "CASA2025-06-27SCt.txt")

    resolved = await resolve_docs_paths(_config(), session_id, work_dir)

    assert len(resolved) == 1
    assert resolved[0].endswith(f"data/{session_id}/documents")


@pytest.mark.asyncio
async def test_returns_both_pending_and_documents_when_populated(tmp_path, monkeypatch):
    """The key guarantee: a re-attached file in pending_documents/ AND the
    committed originals in documents/ are BOTH resolved, so no document is
    silently missed (which would drop the run to schema-only mode)."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-both"
    work_dir = tmp_path / "schematiq_work"
    _write(tmp_path / "data" / session_id / "documents", "already_committed.txt")
    _write(tmp_path / "data" / session_id / "pending_documents", "reattached.txt")

    resolved = await resolve_docs_paths(_config(), session_id, work_dir)

    assert len(resolved) == 2
    # pending_documents/ is returned first (it takes precedence on de-dup).
    assert resolved[0].endswith("pending_documents")
    assert resolved[1].endswith("documents")


@pytest.mark.asyncio
async def test_dotfiles_do_not_count_as_documents(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-dot"
    work_dir = tmp_path / "schematiq_work"
    docs = tmp_path / "data" / session_id / "documents"
    docs.mkdir(parents=True)
    (docs / ".DS_Store").write_text("", encoding="utf-8")  # only a dotfile

    resolved = await resolve_docs_paths(_config(docs_path=[]), session_id, work_dir)

    # No real local documents and no configured docs_path -> nothing resolved
    # (crucially, the dir with only a dotfile is not treated as populated).
    assert resolved == []


@pytest.mark.asyncio
async def test_no_local_docs_and_no_config_path_resolves_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-empty"
    work_dir = tmp_path / "schematiq_work"

    resolved = await resolve_docs_paths(_config(docs_path=[]), session_id, work_dir)

    assert resolved == []
