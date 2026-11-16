"""Public schema discovery orchestration for ``schematiq-lib``."""

from __future__ import annotations

import copy
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from schematiq.core import schematiq as ScheMatiQ
from schematiq.core.llm_backends import (
    LLMInterface,
    LLMRunAborted,
    is_outage_error,
    should_abort_run,
)
from schematiq.core.schema import (
    Column,
    ObservationUnit,
    Schema,
    SchemaEvolution,
    SchemaSnapshot,
)
from schematiq.core.schematiq import (
    ObservationUnitDiscoveryError,
    discover_observation_unit,
)
from schematiq.discovery.batch_execution import generate_schema_with_split
from schematiq.discovery.batch_planner import plan_batches

logger = logging.getLogger(__name__)

# Discovery stops when this many batches in a row fail with an outage error
# (after each call's own retries), instead of grinding through the corpus.
_MAX_CONSECUTIVE_FAILED_BATCHES = 3

# Callback type aliases
OnProgressCallback = Callable[[str, float, Dict[str, Any]], None]
ShouldStopCallback = Callable[[], bool]
OnSnapshotCallback = Callable[[Schema, SchemaEvolution], None]
OnWarningCallback = Callable[[str, str, Optional[Dict[str, Any]]], None]


@dataclass
class DiscoveryConfig:
    """Configuration for iterative schema discovery."""

    batch_size: int = 1
    batch_strategy: str = "smart"
    convergence_threshold: int = 5
    max_keys: int = 100
    initial_schema: Optional[Union[Schema, List[Union[Column, Dict[str, Any]]]]] = None
    initial_schema_path: Optional[Union[str, Path]] = None
    initial_observation_unit: Optional[Union[ObservationUnit, Dict[str, Any]]] = None
    context_window_size: Optional[int] = None
    identify_observation_unit: bool = True
    strict: bool = False
    max_iters: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DiscoveryConfig":
        """Build a ``DiscoveryConfig`` from a ScheMatiQ config mapping."""
        batch_size = (
            data.get("batch_size")
            if "batch_size" in data
            else data.get("documents_batch_size", 1)
        ) or 1
        batch_strategy = (data.get("batch_strategy") or "smart").lower()
        convergence_threshold = data.get("convergence_threshold") or 5
        max_keys = (
            data["max_keys"]
            if "max_keys" in data
            else data.get("max_keys_schema", 100)
        )

        # Inline initial schema takes priority over file path.
        initial_schema = None
        initial_schema_path = None
        if "initial_schema" in data and data["initial_schema"] is not None:
            initial_schema = data["initial_schema"]
        elif "initial_schema_path" in data:
            initial_schema_path = data["initial_schema_path"]

        if "context_window_size" in data:
            context_window_size = data.get("context_window_size")
        else:
            schema_backend = data.get("schema_creation_backend") or {}
            context_window_size = schema_backend.get("context_window_size")

        if "identify_observation_unit" in data:
            identify_obs_unit = bool(data["identify_observation_unit"])
        elif "discover_observation_unit" in data:
            identify_obs_unit = bool(data["discover_observation_unit"])
        else:
            identify_obs_unit = True

        raw_max_iters = data.get("max_iters")
        max_iters = int(raw_max_iters) if raw_max_iters is not None else None

        return cls(
            batch_size=batch_size,
            batch_strategy=batch_strategy,
            convergence_threshold=convergence_threshold,
            max_keys=max_keys,
            initial_schema=initial_schema,
            initial_schema_path=initial_schema_path,
            initial_observation_unit=data.get("initial_observation_unit"),
            context_window_size=context_window_size,
            identify_observation_unit=identify_obs_unit,
            strict=bool(data.get("strict", False)),
            max_iters=max_iters,
        )


def _coerce_config(
    config: Optional[Union[DiscoveryConfig, Mapping[str, Any]]],
) -> DiscoveryConfig:
    if config is None:
        return DiscoveryConfig()
    if isinstance(config, DiscoveryConfig):
        return config
    if isinstance(config, Mapping):
        return DiscoveryConfig.from_dict(config)
    raise TypeError(
        f"config must be a DiscoveryConfig, mapping, or None; got {type(config).__name__}"
    )


def load_initial_schema_from_config(
    config: DiscoveryConfig,
    query: str,
    max_keys: int,
    strict: bool = False,
) -> Optional[Schema]:
    """Load an initial ``Schema`` from ``config.initial_schema`` or ``config.initial_schema_path``.

    When ``strict`` (or ``config.strict``) is ``True``, missing files, invalid JSON,
    unrecognized structures, or invalid inline column definitions raise instead of
    logging a warning and returning ``None``.
    """
    effective_strict = bool(strict or config.strict)

    if config.initial_schema is not None:
        try:
            if isinstance(config.initial_schema, Schema):
                copied_columns = [copy.deepcopy(c) for c in config.initial_schema.columns]
                copied_unit = (
                    copy.deepcopy(config.initial_schema.observation_unit)
                    if config.initial_schema.observation_unit is not None
                    else None
                )
                effective_max_keys = (
                    config.initial_schema.max_keys
                    if config.initial_schema.max_keys is not None
                    else max_keys
                )
                return Schema(
                    query=config.initial_schema.query or query,
                    columns=copied_columns,
                    max_keys=effective_max_keys,
                    observation_unit=copied_unit,
                )

            if isinstance(config.initial_schema, (str, bytes, Mapping)):
                raise ValueError(
                    f"Invalid inline initial_schema: expected Schema or sequence of columns, "
                    f"got {type(config.initial_schema).__name__}"
                )

            columns = [
                copy.deepcopy(col) if isinstance(col, Column) else Column.from_dict(col)
                for col in config.initial_schema
            ]
            initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
            logger.info("Loaded inline initial schema with %d columns", len(columns))
            return initial_schema
        except Exception as e:
            if effective_strict:
                if isinstance(e, ValueError):
                    raise
                raise ValueError(f"Could not load inline initial schema: {e}") from e
            logger.warning("Could not load inline initial schema: %s", e)
            return None

    if config.initial_schema_path is not None:
        try:
            with open(config.initial_schema_path, encoding="utf-8") as f:
                initial_data = json.load(f)
            if isinstance(initial_data, list):
                columns = [Column.from_dict(col_data) for col_data in initial_data]
                initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
            elif isinstance(initial_data, dict) and (
                "schema" in initial_data or "columns" in initial_data
            ):
                raw_cols = initial_data.get("schema", initial_data.get("columns", []))
                if not isinstance(raw_cols, list):
                    raise ValueError("Initial schema 'schema'/'columns' field must be a list")
                columns = [Column.from_dict(col_data) for col_data in raw_cols]
                initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
            else:
                raise ValueError(
                    "Initial schema file must contain a JSON list of columns or an object with 'schema' or 'columns'"
                )
            logger.info("Loaded initial schema from file with %d columns", len(columns))
            return initial_schema
        except Exception as e:
            if effective_strict:
                if isinstance(e, (FileNotFoundError, ValueError)):
                    raise
                raise ValueError(f"Could not load initial schema from file: {e}") from e
            logger.warning("Could not load initial schema from file: %s", e)
            return None

    return None


def _noop_progress(_stage: str, _fraction: float, _info: Dict[str, Any]) -> None:
    return None


def _noop_should_stop() -> bool:
    return False


def _noop_snapshot(_schema: Schema, _evolution: SchemaEvolution) -> None:
    return None


def _noop_warning(
    _level: str,
    _message: str,
    _skip_info: Optional[Dict[str, Any]] = None,
) -> None:
    return None


def _extract_unit_fields(
    unit_cfg: Union[ObservationUnit, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[List[str]]]:
    if isinstance(unit_cfg, ObservationUnit):
        return unit_cfg.name, unit_cfg.definition, unit_cfg.example_names
    return (
        unit_cfg.get("name"),
        unit_cfg.get("definition"),
        unit_cfg.get("example_names"),
    )


def _run_query_only_discovery(
    *,
    query: str,
    max_keys: int,
    cfg: DiscoveryConfig,
    llm: LLMInterface,
    current_schema: Schema,
    evolution: SchemaEvolution,
    on_progress: OnProgressCallback,
) -> Tuple[Schema, SchemaEvolution]:
    """Handle QUERY_ONLY mode: generate schema from query without documents."""
    logger.debug("QUERY_ONLY mode - generating schema from query without documents")
    on_progress(
        "Schema Discovery: Planning from query",
        0.5,
        {"mode": "query_only", "current_columns": len(current_schema.columns)},
    )

    try:
        observation_unit: Optional[ObservationUnit] = None
        obs_unit_config = cfg.initial_observation_unit
        if obs_unit_config is not None:
            obs_name, obs_def, obs_examples = _extract_unit_fields(obs_unit_config)
            if obs_name:
                observation_unit = ObservationUnit(
                    name=obs_name,
                    definition=obs_def or "",
                    example_names=obs_examples,
                    source_document="query_only",
                    discovery_iteration=1,
                )
                logger.info(
                    "QUERY_ONLY: using pre-configured observation unit: %s",
                    observation_unit.name,
                )

        if observation_unit is None:
            if current_schema.observation_unit is not None:
                observation_unit = current_schema.observation_unit
            elif cfg.identify_observation_unit:
                logger.info("QUERY_ONLY: discovering observation unit from query")
                observation_unit = discover_observation_unit(
                    query=query,
                    passages=None,
                    llm=llm,
                    source_document="query_only",
                )
                logger.info(
                    "QUERY_ONLY: discovered observation unit: %s",
                    observation_unit.name if observation_unit else None,
                )

        if observation_unit:
            current_schema.observation_unit = observation_unit

        ctx_window = cfg.context_window_size or getattr(llm, "context_window_size", 8192)

        if any(ScheMatiQ._is_partial(c) for c in current_schema.columns):
            logger.info("QUERY_ONLY: completing partial user-seeded columns")
            current_schema.columns = ScheMatiQ.complete_partial_columns(
                columns=current_schema.columns,
                query=query,
                observation_unit=observation_unit,
                passages=[],
                llm=llm,
                context_window_size=ctx_window,
            )

        schema_result = ScheMatiQ.generate_schema(
            passages=[],
            query=query,
            max_keys_schema=max_keys,
            current_schema=current_schema,
            llm=llm,
            context_window_size=ctx_window,
            observation_unit=observation_unit,
        )
        new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
        logger.debug("QUERY_ONLY generated schema with %d columns", len(new_schema.columns))

        for col in new_schema.columns:
            col.source_document = "query_only"
            col.discovery_iteration = 1

        if current_schema.columns:
            merged_schema = current_schema.merge(new_schema)
        else:
            merged_schema = new_schema

        if observation_unit and not merged_schema.observation_unit:
            merged_schema.observation_unit = observation_unit

        existing_names_lower = {c.name.lower() for c in current_schema.columns}
        new_column_names = [
            col.name for col in merged_schema.columns if col.name.lower() not in existing_names_lower
        ]

        for col_name in new_column_names:
            evolution.record_column_source(col_name, "query_only")

        evolution.add_snapshot(
            iteration=1,
            documents=["query_only"],
            total_columns=len(merged_schema.columns),
            new_columns=new_column_names,
            cumulative_documents=0,
        )

        logger.debug(
            "QUERY_ONLY mode completed with %d columns: %s",
            len(merged_schema.columns),
            [c.name for c in merged_schema.columns],
        )
        return merged_schema, evolution

    except Exception as e:
        logger.error("ERROR in QUERY_ONLY generate_schema: %s", e)
        raise


def discover_schema(
    documents: Sequence[str],
    filenames: Optional[Sequence[str]] = None,
    query: Optional[str] = "",
    config: Optional[Union[DiscoveryConfig, Mapping[str, Any]]] = None,
    llm: Optional[LLMInterface] = None,
    retriever: Any = None,
    *,
    on_progress: Optional[OnProgressCallback] = None,
    should_stop: Optional[ShouldStopCallback] = None,
    on_snapshot: Optional[OnSnapshotCallback] = None,
    on_warning: Optional[OnWarningCallback] = None,
) -> Tuple[Schema, SchemaEvolution]:
    """Discover a table schema iteratively from documents and/or a research query.

    Args:
        documents: Sequence of document text contents (empty for query-only mode).
        filenames: Sequence of filenames aligned with ``documents``. When None,
            defaults to ``["doc_1", ...]`` (or ``[]`` if ``documents`` is empty).
        query: Research question (may be empty/None for document-only mode).
        config: ``DiscoveryConfig`` or configuration mapping (optional).
        llm: ``LLMInterface`` instance used for discovery calls.
        retriever: Optional retriever for selecting relevant passages per document.
        on_progress: Optional callback ``(stage, fraction, info)`` invoked per batch.
        should_stop: Optional callback returning True to stop discovery gracefully.
        on_snapshot: Optional callback ``(schema, evolution)`` called after partial
            updates for incremental persistence or UI notifications.
        on_warning: Optional callback ``(level, message, skip_info)`` for split/skip
            notices on token-limit errors.

    Returns:
        Tuple of ``(discovered_schema, schema_evolution)``.
    """
    if llm is None:
        raise TypeError("discover_schema() requires an 'llm' instance")

    cfg = _coerce_config(config)
    query_str = query or ""
    max_keys = cfg.max_keys
    convergence_threshold = cfg.convergence_threshold or 5

    progress_cb = on_progress or _noop_progress
    stop_cb = should_stop or _noop_should_stop
    snapshot_cb = on_snapshot or _noop_snapshot
    warning_cb = on_warning or _noop_warning

    docs_list = list(documents) if documents else []
    if filenames is None:
        names_list = [f"doc_{i + 1}" for i in range(len(docs_list))]
    else:
        names_list = list(filenames)
        if len(names_list) != len(docs_list):
            raise ValueError(
                f"filenames length ({len(names_list)}) must match documents length ({len(docs_list)})"
            )

    if not (query_str and query_str.strip()) and not docs_list:
        raise ValueError("At least one of query or documents must be provided")

    initial_schema = load_initial_schema_from_config(
        cfg, query_str, max_keys, strict=cfg.strict
    )
    current_schema = (
        initial_schema
        if initial_schema is not None
        else Schema(query=query_str, columns=[], max_keys=max_keys)
    )

    evolution = SchemaEvolution(snapshots=[], column_sources={})

    # Record initial columns (if any) as iteration 0
    if len(current_schema.columns) > 0:
        initial_column_names = [col.name for col in current_schema.columns]
        evolution.add_snapshot(
            iteration=0,
            documents=["initial_schema"],
            total_columns=len(current_schema.columns),
            new_columns=initial_column_names,
            cumulative_documents=0,
        )
        for col in current_schema.columns:
            if col.source_document is None:
                col.source_document = "initial_schema"
            if col.discovery_iteration is None:
                col.discovery_iteration = 0
            evolution.record_column_source(col.name, col.source_document)

    # Handle QUERY_ONLY mode
    if not docs_list:
        return _run_query_only_discovery(
            query=query_str,
            max_keys=max_keys,
            cfg=cfg,
            llm=llm,
            current_schema=current_schema,
            evolution=evolution,
            on_progress=progress_cb,
        )

    planned_batches = plan_batches(
        docs_list,
        names_list,
        batch_strategy=cfg.batch_strategy,
        batch_size=cfg.batch_size,
        context_window_size=cfg.context_window_size,
    )
    batches = [pb.documents for pb in planned_batches]
    filename_batches = [pb.filenames for pb in planned_batches]
    if cfg.max_iters is not None:
        cap = max(0, cfg.max_iters)
        batches = batches[:cap]
        filename_batches = filename_batches[:cap]

    strategy = (cfg.batch_strategy or "smart").lower()
    logger.debug(
        "Document batching (%s) - %d docs, %d batches",
        strategy,
        len(docs_list),
        len(batches),
    )

    unchanged_count = 0
    cumulative_docs = 0
    completion_done = False
    unit_discovery_done = False
    attempted_batches = 0
    failed_batches = 0
    consecutive_failed_batches = 0
    last_batch_error: Optional[Exception] = None
    last_batch_stage: str = "llm"

    # Handle pre-configured observation unit
    pending_observation_unit_name: Optional[str] = None
    initial_obs_unit = cfg.initial_observation_unit
    if initial_obs_unit is not None:
        unit_name, unit_definition, unit_examples = _extract_unit_fields(initial_obs_unit)
        if unit_definition:
            current_schema.observation_unit = ObservationUnit(
                name=unit_name or "",
                definition=unit_definition,
                example_names=unit_examples,
            )
            logger.info(
                "Using pre-configured observation unit: %s - %s",
                unit_name,
                unit_definition,
            )
        elif unit_name:
            pending_observation_unit_name = unit_name
            logger.info(
                "Observation unit name pre-configured: %s (definition will be discovered)",
                pending_observation_unit_name,
            )

    ctx_window = cfg.context_window_size or getattr(llm, "context_window_size", 8192)

    for iteration, (batch_docs, batch_names) in enumerate(zip(batches, filename_batches)):
        if stop_cb():
            logger.warning(
                "Stop requested during schema discovery - saving partial schema with %d columns",
                len(current_schema.columns),
            )
            break

        logger.debug(
            "Schema discovery batch %d/%d (%d docs: %s)",
            iteration + 1,
            len(batches),
            len(batch_docs),
            batch_names,
        )
        progress_cb(
            f"Schema Discovery: Batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs)",
            iteration / len(batches),
            {
                "iteration": iteration + 1,
                "max_iterations": len(batches),
                "batch_docs": len(batch_docs),
                "current_columns": len(current_schema.columns),
            },
        )

        columns_before = {col.name.lower() for col in current_schema.columns}
        cumulative_docs += len(batch_docs)

        try:
            relevant_content = ScheMatiQ.select_relevant_content(
                docs=batch_docs,
                query=query_str,
                retriever=retriever,
            )
        except Exception as exc:
            if should_abort_run(exc):
                raise
            logger.warning(
                "Iteration %d — retrieval failed, skipping batch %s: %s",
                iteration + 1,
                batch_names,
                exc,
            )
            skipped_label = ", ".join(batch_names) if batch_names else f"batch {iteration + 1}"
            warning_cb(
                "warning",
                f"Skipped '{skipped_label}' during schema discovery due to retrieval error: {exc}",
                {
                    "filenames": list(batch_names),
                    "reason": "retrieval_failed",
                    "error": str(exc)[:2000],
                },
            )
            evolution.non_contributing_files.extend(batch_names)
            evolution.record_skipped_batch("retrieval_failed", batch_names)
            evolution.add_snapshot(
                iteration=iteration + 1,
                documents=list(batch_names),
                total_columns=len(current_schema.columns),
                new_columns=[],
                cumulative_documents=cumulative_docs,
            )
            continue

        logger.debug("Selected %d relevant passages from batch", len(relevant_content))

        if stop_cb():
            logger.warning("Stop requested after content retrieval - saving partial schema")
            break

        def _run_generate(
            sub_docs: List[str],
            _batch_docs: List[str] = batch_docs,
            _relevant_content: List[str] = relevant_content,
            _current_schema: Schema = current_schema,
        ) -> Any:
            if sub_docs is _batch_docs:
                sub_content = _relevant_content
            else:
                sub_content = ScheMatiQ.select_relevant_content(
                    docs=sub_docs,
                    query=query_str,
                    retriever=retriever,
                )
            return ScheMatiQ.generate_schema(
                passages=sub_content,
                query=query_str,
                max_keys_schema=max_keys,
                current_schema=_current_schema,
                llm=llm,
                context_window_size=ctx_window,
                observation_unit=_current_schema.observation_unit,
            )

        batch_contributing: List[str] = []
        batch_non_contributing: List[str] = []
        batch_skipped_token_limit: List[List[str]] = []

        def _record_subbatch_outcome(
            sub_names: List[str],
            sub_schema: Optional[Schema],
            doc_helpful: bool,
            _iter_num: int = iteration + 1,
        ) -> None:
            if sub_schema is None:
                batch_non_contributing.extend(sub_names)
                batch_skipped_token_limit.append(list(sub_names))
                return

            source_doc = sub_names[0] if sub_names else "unknown"
            for col in sub_schema.columns:
                col.source_document = source_doc
                col.discovery_iteration = _iter_num

            if doc_helpful and len(sub_schema.columns) > 0:
                batch_contributing.extend(sub_names)
            else:
                batch_non_contributing.extend(sub_names)

        attempted_batches += 1
        stage_in_batch = "llm"
        try:
            # Discover observation unit on the first batch that succeeds (if not already set)
            if (
                not unit_discovery_done
                and cfg.identify_observation_unit
                and (query_str or relevant_content)
                and not current_schema.observation_unit
            ):
                stage_in_batch = "observation_unit"
                logger.info("Discovering observation unit from batch %d...", iteration + 1)
                obs_unit = discover_observation_unit(
                    query=query_str,
                    passages=relevant_content,
                    llm=llm,
                    context_window_size=ctx_window,
                    source_document=batch_names[0] if batch_names else None,
                )
                if obs_unit is not None and pending_observation_unit_name:
                    logger.info(
                        "Overriding discovered name '%s' with pre-configured name '%s'",
                        obs_unit.name,
                        pending_observation_unit_name,
                    )
                    obs_unit.name = pending_observation_unit_name
                current_schema.observation_unit = obs_unit
                unit_discovery_done = True
                if obs_unit is not None:
                    logger.info(
                        "Observation unit set: %s - %s", obs_unit.name, obs_unit.definition
                    )
                    if obs_unit.example_names:
                        logger.info("   Examples: %s", obs_unit.example_names)

            if stop_cb():
                logger.warning("Stop requested after observation unit discovery - saving partial schema")
                break

            stage_in_batch = "llm"
            # Complete any partial user-seeded columns once, with passages as supporting context.
            if not completion_done and any(ScheMatiQ._is_partial(c) for c in current_schema.columns):
                logger.info("Completing partial user-seeded columns (batch %d)", iteration + 1)
                current_schema.columns = ScheMatiQ.complete_partial_columns(
                    columns=current_schema.columns,
                    query=query_str,
                    observation_unit=current_schema.observation_unit,
                    passages=relevant_content,
                    llm=llm,
                    context_window_size=ctx_window,
                )
            completion_done = True

            new_schema = generate_schema_with_split(
                batch_docs=batch_docs,
                batch_names=batch_names,
                run_generate=_run_generate,
                should_stop=stop_cb,
                on_warning=warning_cb,
                on_subbatch_outcome=_record_subbatch_outcome,
            )
        except Exception as exc:
            if should_abort_run(exc):
                if stage_in_batch == "observation_unit":
                    if isinstance(exc, ObservationUnitDiscoveryError):
                        raise RuntimeError(
                            f"Failed to discover observation unit: {exc}. "
                            "Ensure documents contain extractable entities."
                        ) from exc
                    raise RuntimeError(
                        f"Observation unit discovery failed unexpectedly: {exc}"
                    ) from exc
                raise
            failed_batches += 1
            consecutive_failed_batches = (
                consecutive_failed_batches + 1 if is_outage_error(exc) else 0
            )
            last_batch_error = exc
            last_batch_stage = stage_in_batch
            skipped_label = ", ".join(batch_names) if batch_names else f"batch {iteration + 1}"
            if stage_in_batch == "observation_unit":
                skip_reason = "observation_unit_discovery_failed"
                logger.warning(
                    "Iteration %d — observation unit discovery failed, skipping batch %s: %s",
                    iteration + 1,
                    batch_names,
                    exc,
                    exc_info=True,
                )
                warn_message = (
                    f"Skipped '{skipped_label}' during schema discovery because observation unit discovery failed: {exc}"
                )
            else:
                skip_reason = "llm_call_failed"
                logger.warning(
                    "Iteration %d — LLM call failed, skipping batch %s: %s",
                    iteration + 1,
                    batch_names,
                    exc,
                    exc_info=True,
                )
                warn_message = (
                    f"Skipped '{skipped_label}' during schema discovery because the LLM call failed: {exc}"
                )
            warning_cb(
                "warning",
                warn_message,
                {
                    "filenames": list(batch_names),
                    "reason": skip_reason,
                    "error": str(exc)[:2000],
                },
            )
            evolution.non_contributing_files.extend(batch_names)
            evolution.record_skipped_batch(skip_reason, batch_names)
            evolution.add_snapshot(
                iteration=iteration + 1,
                documents=list(batch_names),
                total_columns=len(current_schema.columns),
                new_columns=[],
                cumulative_documents=cumulative_docs,
            )
            if consecutive_failed_batches >= _MAX_CONSECUTIVE_FAILED_BATCHES:
                raise LLMRunAborted(
                    f"Schema discovery stopped after {consecutive_failed_batches} "
                    f"batches failed in a row. Last error: {exc}"
                ) from exc
            continue
        consecutive_failed_batches = 0

        if new_schema is None:
            if stop_cb():
                break
            evolution.non_contributing_files.extend(batch_non_contributing)
            for skipped_names in batch_skipped_token_limit:
                evolution.record_skipped_batch("single_document_exceeds_token_limit", skipped_names)
            logger.warning(
                "Batch %d/%d produced no schema (all documents skipped for token limits); continuing",
                iteration + 1,
                len(batches),
            )
            continue

        logger.debug("Generated schema with %d columns", len(new_schema.columns))

        if stop_cb():
            logger.warning("Stop requested after schema generation - saving partial schema")
            break

        evolution.contributing_files.extend(batch_contributing)
        evolution.non_contributing_files.extend(batch_non_contributing)
        for skipped_names in batch_skipped_token_limit:
            evolution.record_skipped_batch("single_document_exceeds_token_limit", skipped_names)

        # Merge with existing schema
        logger.debug("Current schema has %d columns", len(current_schema.columns))
        logger.debug("New schema has %d columns", len(new_schema.columns))
        try:
            merged_schema = current_schema.merge(new_schema)
            logger.debug("Merged schema has %d columns", len(merged_schema.columns))
        except Exception as e:
            logger.error("ERROR in schema merge: %s", e)
            raise

        if stop_cb():
            logger.warning("Stop requested after schema merge - saving partial schema")
            current_schema = merged_schema
            snapshot_cb(current_schema, evolution)
            break

        # Identify NEW columns added in this iteration
        columns_after = {col.name.lower() for col in merged_schema.columns}
        new_column_names_lower = columns_after - columns_before
        new_columns = [
            col.name
            for col in merged_schema.columns
            if col.name.lower() in new_column_names_lower
        ]

        batch_source = ", ".join(batch_names) if batch_names else f"batch_{iteration + 1}"
        for col_name in new_columns:
            evolution.record_column_source(col_name, batch_source)

        evolution.add_snapshot(
            iteration=iteration + 1,
            documents=batch_names,
            total_columns=len(merged_schema.columns),
            new_columns=new_columns,
            cumulative_documents=cumulative_docs,
        )

        logger.debug(
            "Evolution - batch %d: %d new columns: %s",
            iteration + 1,
            len(new_columns),
            new_columns,
        )

        # Check convergence
        converged = ScheMatiQ.evaluate_schema_convergence(current_schema, merged_schema)
        if converged:
            unchanged_count += 1
            logger.debug(
                "Schema unchanged (count: %d/%d)", unchanged_count, convergence_threshold
            )
            if unchanged_count >= convergence_threshold:
                current_schema = merged_schema
                logger.debug("Schema converged after %d batches", iteration + 1)
                break
        else:
            unchanged_count = 0
            logger.debug("Schema changed, continuing to next batch")

        current_schema = merged_schema
        logger.debug("Completed batch %d, moving to next", iteration + 1)

        snapshot_cb(current_schema, evolution)

    if attempted_batches and failed_batches == attempted_batches and last_batch_error is not None:
        if last_batch_stage == "observation_unit":
            if isinstance(last_batch_error, ObservationUnitDiscoveryError):
                raise RuntimeError(
                    f"Failed to discover observation unit: {last_batch_error}. "
                    "Ensure documents contain extractable entities."
                ) from last_batch_error
            raise RuntimeError(
                f"Observation unit discovery failed unexpectedly: {last_batch_error}"
            ) from last_batch_error
        raise last_batch_error

    logger.debug(
        "Schema discovery completed with %d columns after %d batches",
        len(current_schema.columns),
        len(evolution.snapshots),
    )
    logger.debug(
        "Evolution tracking: %d snapshots, %d column sources",
        len(evolution.snapshots),
        len(evolution.column_sources),
    )
    return current_schema, evolution
