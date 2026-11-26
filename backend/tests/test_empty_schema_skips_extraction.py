"""An empty discovered schema must skip value extraction and tell the user why.

With zero columns every extracted row is dropped, yet value extraction would
still pay one unit-identification LLM call per document. The runner skips it
and flags the completion so the Workspace can explain the empty table.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_empty_schema_skips_value_extraction_and_flags_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from app.models.schematiq import LLMConfig, ScheMatiQConfig
    from app.models.session import (
        SchemaEvolution,
        SessionMetadata,
        SessionStatus,
        SessionType,
        VisualizationSession,
    )
    from app.services import schematiq_runner as runner_mod
    from app.services.schematiq_runner import ScheMatiQRunner
    from app.services.session_manager import SessionManager
    from app.storage.local_backend import LocalStorageBackend
    from schematiq.core.schema import Schema

    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    sessions_dir = tmp_path / "sessions"
    docs_dir = tmp_path / "input_docs"
    for d in (data_dir, work_dir, sessions_dir, docs_dir):
        d.mkdir(parents=True, exist_ok=True)
    (docs_dir / "01_alpha.txt").write_text("First document.", encoding="utf-8")
    (docs_dir / "02_beta.txt").write_text("Second document.", encoding="utf-8")

    session_id = "sess_empty_schema"
    storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
    )
    session_manager = SessionManager(storage=storage)
    session_manager.create_session(
        VisualizationSession(
            id=session_id,
            type=SessionType.SCHEMATIQ,
            status=SessionStatus.PROCESSING,
            metadata=SessionMetadata(source="test"),
        )
    )

    ws = MagicMock()
    ws.broadcast_progress = AsyncMock()
    ws.broadcast_to_session = AsyncMock()
    ws.broadcast_log = AsyncMock()
    ws.broadcast_completion = AsyncMock()
    ws.broadcast_error = AsyncMock()

    runner = ScheMatiQRunner(
        work_dir=str(work_dir),
        websocket_manager=ws,
        session_manager=session_manager,
    )

    empty_schema = Schema(query="sdfsfd", columns=[])
    value_extraction = AsyncMock(return_value=[])
    monkeypatch.setattr(runner_mod, "DEFAULT_DATA_DIR", str(data_dir))
    monkeypatch.setattr(runner_mod, "resolve_docs_paths", AsyncMock(return_value=[str(docs_dir)]))
    monkeypatch.setattr(runner_mod, "enforce_release_llm_config", lambda cfg, **_kw: cfg)
    monkeypatch.setattr(runner_mod, "build_llm_interface", lambda **_kw: MagicMock())
    monkeypatch.setattr(
        runner_mod,
        "run_schema_discovery",
        AsyncMock(return_value=(empty_schema, SchemaEvolution())),
    )
    monkeypatch.setattr(runner_mod, "run_value_extraction", value_extraction)
    monkeypatch.setattr(runner, "check_global_quota", lambda *_args, **_kwargs: None)

    backend = LLMConfig(provider="openai", model="fake-deterministic", context_window_size=16384)
    config = ScheMatiQConfig(
        query="sdfsfd",
        docs_path=str(docs_dir),
        skip_value_extraction=False,
        count_toward_quota=False,
        output_path=str(work_dir / session_id / "output.json"),
        schema_creation_backend=backend,
        value_extraction_backend=backend,
    )

    await runner._execute_schematiq(session_id, config)

    value_extraction.assert_not_awaited()
    ws.broadcast_error.assert_not_awaited()
    ws.broadcast_completion.assert_awaited_once()
    _sid, payload = ws.broadcast_completion.call_args.args
    assert _sid == session_id
    assert payload["no_columns_discovered"] is True
    assert payload["schema_only"] is True
    assert payload["schema_columns"] == 0
    assert payload["message"].startswith("Schema discovery found no columns for this question")

    saved = session_manager.get_session(session_id)
    assert saved is not None
    assert saved.status == SessionStatus.COMPLETED
