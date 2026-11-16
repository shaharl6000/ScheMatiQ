"""Backend async wrapper around ``schematiq.discovery.discover_schema``."""

import asyncio
import concurrent.futures
import functools
import inspect
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.models.session import SchemaEvolution, SchemaSnapshot
from app.services import schematiq_thread_pool
from schematiq.core.llm_backends import LLMInterface
from schematiq.core.schema import Schema, SchemaEvolution as LibSchemaEvolution
from schematiq.discovery import (
    DiscoveryConfig,
    discover_schema,
    load_initial_schema_from_config,
)

logger = logging.getLogger(__name__)


def run_callback_on_loop(
    loop: asyncio.AbstractEventLoop,
    cancel_event: threading.Event,
    coro_fn,
    *args,
) -> Any:
    """Run a sync/async callback on ``loop`` without hanging if ``loop`` stops or caller cancels."""
    if coro_fn is None or cancel_event.is_set() or loop.is_closed() or not loop.is_running():
        return None

    async def _invoke():
        res = coro_fn(*args)
        if inspect.isawaitable(res):
            return await res
        return res

    coro = _invoke()
    try:
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        coro.close()
        return None

    while True:
        try:
            return fut.result(timeout=0.5)
        except concurrent.futures.TimeoutError:
            if cancel_event.is_set() or loop.is_closed() or not loop.is_running():
                fut.cancel()
                return None


async def run_schema_discovery(
    session_id: str,
    documents: List[str],
    filenames: List[str],
    schematiq_config: Dict[str, Any],
    llm: LLMInterface,
    retriever,
    progress_callback,
    is_stop_requested: Callable[[str], bool],
    ws_manager=None,
    work_dir: Optional[Path] = None,
):
    """Run schema discovery in the worker thread pool via ``schematiq.discovery``.

    Args:
        session_id: Session identifier
        documents: List of document text contents
        filenames: Corresponding filenames
        schematiq_config: Full ScheMatiQ configuration dict
        llm: LLM interface for schema generation
        retriever: Embedding retriever (or None)
        progress_callback: Async callback for progress updates
        is_stop_requested: Callable that checks if stop was requested
        ws_manager: WebSocket manager for broadcasting schema progress
        work_dir: Working directory for saving partial schemas

    Returns:
        Tuple of (discovered_schema, schema_evolution)
    """
    query = schematiq_config["query"]
    config = DiscoveryConfig.from_dict(schematiq_config)
    loop = asyncio.get_running_loop()
    caller_cancelled = threading.Event()

    batch_state: Dict[str, int] = {"iteration": 0, "max_iterations": 0}

    def _on_progress(stage: str, fraction: float, info: Dict[str, Any]) -> None:
        if isinstance(info, dict) and "iteration" in info:
            batch_state["iteration"] = info["iteration"]
            batch_state["max_iterations"] = info.get("max_iterations", 0)
        run_callback_on_loop(
            loop,
            caller_cancelled,
            progress_callback,
            stage,
            fraction,
            dict(info) if info is not None else {},
        )

    def _should_stop() -> bool:
        if caller_cancelled.is_set():
            return True
        if is_stop_requested is None:
            return False
        return bool(is_stop_requested(session_id))

    def _on_snapshot(schema: Schema, lib_evolution: LibSchemaEvolution) -> None:
        if work_dir:
            _save_partial_schema(schema, query, work_dir / session_id)
        if (
            ws_manager
            and lib_evolution.snapshots
            and batch_state["iteration"] > 0
            and lib_evolution.snapshots[-1].iteration == batch_state["iteration"]
        ):
            latest = lib_evolution.snapshots[-1]
            run_callback_on_loop(
                loop,
                caller_cancelled,
                ws_manager.broadcast_to_session,
                session_id,
                {
                    "type": "schema_progress",
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "columns_discovered": len(schema.columns),
                        "iteration": latest.iteration,
                        "max_iterations": batch_state["max_iterations"],
                        "new_columns": list(latest.new_columns),
                    },
                },
            )

    def _on_warning(
        level: str,
        message: str,
        skip_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        if skip_info is not None and work_dir:
            _log_skipped_batch(
                work_dir,
                session_id,
                skip_info.get("filenames", []),
                reason=skip_info.get("reason", "single_document_exceeds_token_limit"),
                error=skip_info.get("error", ""),
            )
        if ws_manager:
            run_callback_on_loop(
                loop,
                caller_cancelled,
                ws_manager.broadcast_log,
                session_id,
                {"level": level, "message": message},
            )

    try:
        discovered_schema, lib_evolution = await loop.run_in_executor(
            schematiq_thread_pool,
            functools.partial(
                discover_schema,
                documents=documents,
                filenames=filenames,
                query=query,
                config=config,
                llm=llm,
                retriever=retriever,
                on_progress=_on_progress,
                should_stop=_should_stop,
                on_snapshot=_on_snapshot,
                on_warning=_on_warning,
            ),
        )
    finally:
        caller_cancelled.set()

    evolution = SchemaEvolution(
        snapshots=[
            SchemaSnapshot(
                iteration=snap.iteration,
                documents_processed=list(snap.documents_processed),
                total_columns=snap.total_columns,
                new_columns=list(snap.new_columns),
                cumulative_documents=snap.cumulative_documents,
            )
            for snap in lib_evolution.snapshots
        ],
        column_sources=dict(lib_evolution.column_sources),
    )

    return discovered_schema, evolution


def _load_initial_schema(
    schematiq_config: Dict[str, Any], query: str, max_keys: int
) -> Optional[Schema]:
    """Load initial schema from inline config or file path."""
    return load_initial_schema_from_config(
        DiscoveryConfig.from_dict(schematiq_config), query, max_keys
    )


def _log_skipped_batch(
    work_dir: Optional[Path],
    session_id: str,
    filenames: List[str],
    reason: str,
    error: str,
) -> None:
    """Append a skipped-batch record to a per-session JSON artifact."""
    if not work_dir:
        return

    try:
        session_dir = work_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        skip_file = session_dir / "schema_discovery_skips.json"

        records: List[Dict[str, Any]] = []
        if skip_file.exists():
            try:
                with open(skip_file) as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        records = loaded
            except Exception:
                records = []

        records.append({
            "timestamp": datetime.now().isoformat(),
            "filenames": filenames,
            "reason": reason,
            "error": error[:2000],
        })

        with open(skip_file, "w") as f:
            json.dump(records, f, indent=2)
        logger.debug("Recorded skipped batch artifact (%d total) at %s", len(records), skip_file)
    except Exception as e:
        logger.warning("Could not write skipped-batch artifact: %s", e)


def _save_partial_schema(schema: Schema, query: str, session_dir: Path) -> None:
    """Save partial schema to disk for stop resilience."""
    session_dir.mkdir(parents=True, exist_ok=True)
    partial_schema_file = session_dir / "discovered_schema.json"
    frontend_schema = []
    for col in schema.columns:
        col_dict = col.to_dict()
        frontend_col = {
            "name": col_dict.get("column", col.name),
            "definition": col_dict.get("definition", ""),
            "rationale": col_dict.get("explanation", col.rationale),
        }
        if col_dict.get("allowed_values"):
            frontend_col["allowed_values"] = col_dict["allowed_values"]
        frontend_col["auto_expand_threshold"] = col_dict.get("auto_expand_threshold")
        if col_dict.get("value_kind"):
            frontend_col["value_kind"] = col_dict["value_kind"]
        frontend_schema.append(frontend_col)
    partial_schema_data = {"query": query, "schema": frontend_schema}
    if schema.observation_unit:
        partial_schema_data["observation_unit"] = schema.observation_unit.to_dict()
    with open(partial_schema_file, "w") as f:
        json.dump(partial_schema_data, f, indent=2)
    logger.debug("Saved partial schema with %d columns", len(schema.columns))
