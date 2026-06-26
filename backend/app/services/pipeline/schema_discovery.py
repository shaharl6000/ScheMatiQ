"""Schema discovery pipeline — iterative batch processing with convergence."""

import asyncio
import functools
import json
import logging
from typing import Dict, Any, List, Callable, Optional
from pathlib import Path
from datetime import datetime

from app.services import schematiq_thread_pool

from schematiq.core import schematiq as ScheMatiQ
from schematiq.core.schema import Schema, Column, ObservationUnit
from schematiq.core.llm_backends import LLMInterface
from schematiq import discover_observation_unit, ObservationUnitDiscoveryError

logger = logging.getLogger(__name__)


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
    """Run real schema discovery using ScheMatiQ pipeline.

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
    from app.models.session import SchemaEvolution, SchemaSnapshot

    query = schematiq_config["query"]
    max_keys = schematiq_config.get("max_keys_schema", 100)

    # Initialize schema (inline schema takes priority over file path)
    initial_schema = _load_initial_schema(schematiq_config, query, max_keys)
    current_schema = initial_schema or Schema(query=query, columns=[], max_keys=max_keys)

    batch_size = schematiq_config.get("documents_batch_size", 1)
    convergence_threshold = schematiq_config.get("convergence_threshold") or 5
    unchanged_count = 0

    batches = [documents[i:i+batch_size] for i in range(0, len(documents), batch_size)]
    filename_batches = [filenames[i:i+batch_size] for i in range(0, len(filenames), batch_size)]

    logger.debug("Document batching - %d docs, batch_size=%d, %d batches", len(documents), batch_size, len(batches))

    evolution = SchemaEvolution(snapshots=[], column_sources={})
    cumulative_docs = 0

    # Record initial columns (if any) as iteration 0
    if len(current_schema.columns) > 0:
        initial_column_names = [col.name for col in current_schema.columns]
        evolution.snapshots.append(SchemaSnapshot(
            iteration=0,
            documents_processed=["initial_schema"],
            total_columns=len(current_schema.columns),
            new_columns=initial_column_names,
            cumulative_documents=0
        ))
        for col in current_schema.columns:
            evolution.column_sources[col.name] = "initial_schema"

    # Handle QUERY_ONLY mode
    if not documents:
        return await _run_query_only_discovery(
            session_id, query, max_keys, schematiq_config, llm, current_schema,
            evolution, progress_callback
        )

    # Tracks whether complete_partial_columns has already run for this discovery.
    completion_done = False

    # Handle pre-configured observation unit
    pending_observation_unit_name = None
    initial_obs_unit = schematiq_config.get("initial_observation_unit")
    if initial_obs_unit:
        if initial_obs_unit.get("definition"):
            current_schema.observation_unit = ObservationUnit(
                name=initial_obs_unit["name"],
                definition=initial_obs_unit["definition"]
            )
            logger.info("Using pre-configured observation unit: %s - %s", initial_obs_unit['name'], initial_obs_unit['definition'])
        else:
            pending_observation_unit_name = initial_obs_unit["name"]
            logger.info("Observation unit name pre-configured: %s (definition will be discovered)", pending_observation_unit_name)

    for iteration, (batch_docs, batch_names) in enumerate(zip(batches, filename_batches)):
        if is_stop_requested(session_id):
            logger.warning("Stop requested during schema discovery - saving partial schema with %d columns", len(current_schema.columns))
            break

        logger.debug("Schema discovery batch %d/%d (%d docs: %s)", iteration + 1, len(batches), len(batch_docs), batch_names)
        await progress_callback(f"Schema Discovery: Batch {iteration + 1}/{len(batches)} ({len(batch_docs)} docs)", iteration / len(batches), {
            "iteration": iteration + 1,
            "max_iterations": len(batches),
            "batch_docs": len(batch_docs),
            "current_columns": len(current_schema.columns)
        })

        columns_before = {col.name.lower() for col in current_schema.columns}
        cumulative_docs += len(batch_docs)

        loop = asyncio.get_running_loop()
        logger.debug("[%s] Offloading select_relevant_content to thread pool", session_id)
        relevant_content = await loop.run_in_executor(
            schematiq_thread_pool,
            functools.partial(
                ScheMatiQ.select_relevant_content,
                docs=batch_docs,
                query=query,
                retriever=retriever,
            )
        )
        logger.debug("Selected %d relevant passages from batch", len(relevant_content))

        if is_stop_requested(session_id):
            logger.warning("Stop requested after content retrieval - saving partial schema")
            break

        # Discover observation unit in first iteration (if not already set)
        if iteration == 0 and (query or relevant_content) and not current_schema.observation_unit:
            logger.info("Discovering observation unit from first batch...")
            try:
                logger.debug("[%s] Offloading discover_observation_unit to thread pool", session_id)
                obs_unit = await loop.run_in_executor(
                    schematiq_thread_pool,
                    functools.partial(
                        discover_observation_unit,
                        query=query,
                        passages=relevant_content,
                        llm=llm,
                        context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                        source_document=batch_names[0] if batch_names else None,
                    )
                )
                if pending_observation_unit_name:
                    logger.info("Overriding discovered name '%s' with pre-configured name '%s'", obs_unit.name, pending_observation_unit_name)
                    obs_unit.name = pending_observation_unit_name
                current_schema.observation_unit = obs_unit
                logger.info("Observation unit set: %s - %s", obs_unit.name, obs_unit.definition)
                if obs_unit.example_names:
                    logger.info("   Examples: %s", obs_unit.example_names)
            except ObservationUnitDiscoveryError as e:
                logger.error("Observation unit discovery failed: %s", e)
                raise RuntimeError(
                    f"Failed to discover observation unit: {e}. "
                    "Ensure documents contain extractable entities."
                ) from e
            except Exception as e:
                logger.error("Unexpected error during observation unit discovery: %s", e)
                raise RuntimeError(
                    f"Observation unit discovery failed unexpectedly: {e}"
                ) from e

        if is_stop_requested(session_id):
            logger.warning("Stop requested after observation unit discovery - saving partial schema")
            break

        # Complete any partial user-seeded columns once, with passages as supporting context.
        if not completion_done and any(ScheMatiQ._is_partial(c) for c in current_schema.columns):
            logger.info("Completing partial user-seeded columns (batch %d)", iteration + 1)
            current_schema.columns = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(
                    ScheMatiQ.complete_partial_columns,
                    columns=current_schema.columns,
                    query=query,
                    observation_unit=current_schema.observation_unit,
                    passages=relevant_content,
                    llm=llm,
                    context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                ),
            )
        completion_done = True

        # Generate schema for this iteration
        logger.debug("[%s] Offloading generate_schema to thread pool", session_id)
        try:
            schema_result = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(
                    ScheMatiQ.generate_schema,
                    passages=relevant_content,
                    query=query,
                    max_keys_schema=schematiq_config.get("max_keys_schema", 100),
                    current_schema=current_schema,
                    llm=llm,
                    context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                )
            )
            new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
            logger.debug("Generated schema with %d columns", len(new_schema.columns))
        except Exception as e:
            logger.error("ERROR in generate_schema: %s", e)
            raise

        if is_stop_requested(session_id):
            logger.warning("Stop requested after schema generation - saving partial schema")
            break

        # Merge with existing schema
        logger.debug("[%s] Offloading schema merge to thread pool", session_id)
        logger.debug("Current schema has %d columns", len(current_schema.columns))
        logger.debug("New schema has %d columns", len(new_schema.columns))
        try:
            merged_schema = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(current_schema.merge, new_schema),
            )
            logger.debug("Merged schema has %d columns", len(merged_schema.columns))
        except Exception as e:
            logger.error("ERROR in schema merge: %s", e)
            import traceback
            traceback.print_exc()
            raise

        if is_stop_requested(session_id):
            logger.warning("Stop requested after schema merge - saving partial schema")
            break

        # Identify NEW columns added in this iteration
        columns_after = {col.name.lower() for col in merged_schema.columns}
        new_column_names_lower = columns_after - columns_before
        new_columns = [col.name for col in merged_schema.columns if col.name.lower() in new_column_names_lower]

        batch_source = ", ".join(batch_names) if batch_names else f"batch_{iteration + 1}"
        for col_name in new_columns:
            if col_name not in evolution.column_sources:
                evolution.column_sources[col_name] = batch_source

        evolution.snapshots.append(SchemaSnapshot(
            iteration=iteration + 1,
            documents_processed=batch_names,
            total_columns=len(merged_schema.columns),
            new_columns=new_columns,
            cumulative_documents=cumulative_docs
        ))

        logger.debug("Evolution - batch %d: %d new columns: %s", iteration + 1, len(new_columns), new_columns)

        # Check convergence
        logger.debug("[%s] Offloading evaluate_schema_convergence to thread pool", session_id)
        converged = await loop.run_in_executor(
            schematiq_thread_pool,
            functools.partial(ScheMatiQ.evaluate_schema_convergence, current_schema, merged_schema),
        )
        if converged:
            unchanged_count += 1
            logger.debug("Schema unchanged (count: %d/%d)", unchanged_count, convergence_threshold)
            if unchanged_count >= convergence_threshold:
                logger.debug("Schema converged after %d batches", iteration + 1)
                break
        else:
            unchanged_count = 0
            logger.debug("Schema changed, continuing to next batch")

        current_schema = merged_schema
        logger.debug("Completed batch %d, moving to next", iteration + 1)

        # Save partial schema to disk after each batch
        if work_dir:
            _save_partial_schema(current_schema, query, work_dir / session_id)

        # Notify frontend about partial schema availability
        if ws_manager:
            await ws_manager.broadcast_to_session(session_id, {
                "type": "schema_progress",
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "columns_discovered": len(current_schema.columns),
                    "iteration": iteration + 1,
                    "max_iterations": len(batches),
                    "new_columns": new_columns,
                }
            })

        await asyncio.sleep(0.1)

    logger.debug("Schema discovery completed with %d columns after %d batches", len(current_schema.columns), len(evolution.snapshots))
    logger.debug("Evolution tracking: %d snapshots, %d column sources", len(evolution.snapshots), len(evolution.column_sources))
    return current_schema, evolution


async def _run_query_only_discovery(
    session_id: str,
    query: str,
    max_keys: int,
    schematiq_config: Dict[str, Any],
    llm: LLMInterface,
    current_schema: Schema,
    evolution,
    progress_callback,
):
    """Handle QUERY_ONLY mode: generate schema from query without documents."""
    from app.models.session import SchemaSnapshot

    logger.debug("QUERY_ONLY mode - generating schema from query without documents")
    await progress_callback("Schema Discovery: Planning from query", 0.5, {
        "mode": "query_only",
        "current_columns": len(current_schema.columns)
    })

    try:
        loop = asyncio.get_running_loop()

        obs_unit_config = schematiq_config.get("initial_observation_unit")
        if obs_unit_config and obs_unit_config.get("name"):
            observation_unit = ObservationUnit(
                name=obs_unit_config["name"],
                definition=obs_unit_config.get("definition", ""),
                source_document="query_only",
                discovery_iteration=1,
            )
            logger.info("[%s] QUERY_ONLY: using pre-configured observation unit: %s", session_id, observation_unit.name)
        else:
            logger.info("[%s] QUERY_ONLY: discovering observation unit from query", session_id)
            observation_unit = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(
                    discover_observation_unit,
                    query=query,
                    passages=None,
                    llm=llm,
                    source_document="query_only",
                )
            )
            logger.info("[%s] QUERY_ONLY: discovered observation unit: %s", session_id, observation_unit.name if observation_unit else None)

        if observation_unit:
            current_schema.observation_unit = observation_unit

        # Complete any partial user-seeded columns before schema generation (QUERY_ONLY: no passages).
        if any(ScheMatiQ._is_partial(c) for c in current_schema.columns):
            logger.info("[%s] QUERY_ONLY: completing partial user-seeded columns", session_id)
            current_schema.columns = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(
                    ScheMatiQ.complete_partial_columns,
                    columns=current_schema.columns,
                    query=query,
                    observation_unit=observation_unit,
                    passages=[],
                    llm=llm,
                    context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                ),
            )

        logger.debug("[%s] Offloading QUERY_ONLY generate_schema to thread pool", session_id)
        schema_result = await loop.run_in_executor(
            schematiq_thread_pool,
            functools.partial(
                ScheMatiQ.generate_schema,
                passages=[],
                query=query,
                max_keys_schema=schematiq_config.get("max_keys_schema", 100),
                current_schema=current_schema,
                llm=llm,
                context_window_size=schematiq_config["schema_creation_backend"].get("context_window_size") or getattr(llm, 'context_window_size', 8192),
                observation_unit=observation_unit,
            )
        )
        new_schema = schema_result[0] if isinstance(schema_result, tuple) else schema_result
        logger.debug("QUERY_ONLY generated schema with %d columns", len(new_schema.columns))

        if current_schema.columns:
            logger.debug("[%s] Offloading QUERY_ONLY schema merge to thread pool", session_id)
            merged_schema = await loop.run_in_executor(
                schematiq_thread_pool,
                functools.partial(current_schema.merge, new_schema),
            )
        else:
            merged_schema = new_schema

        if observation_unit and not merged_schema.observation_unit:
            merged_schema.observation_unit = observation_unit

        new_column_names = [col.name for col in merged_schema.columns
                           if col.name not in {c.name for c in current_schema.columns}]

        for col_name in new_column_names:
            evolution.column_sources[col_name] = "query_only"

        evolution.snapshots.append(SchemaSnapshot(
            iteration=1,
            documents_processed=["query_only"],
            total_columns=len(merged_schema.columns),
            new_columns=new_column_names,
            cumulative_documents=0
        ))

        logger.debug("QUERY_ONLY mode completed with %d columns: %s", len(merged_schema.columns), [c.name for c in merged_schema.columns])
        return merged_schema, evolution

    except Exception as e:
        logger.error("ERROR in QUERY_ONLY generate_schema: %s", e)
        import traceback
        traceback.print_exc()
        raise


def _load_initial_schema(schematiq_config: Dict[str, Any], query: str, max_keys: int) -> Optional[Schema]:
    """Load initial schema from inline config or file path."""
    if "initial_schema" in schematiq_config:
        try:
            columns = [Column.from_dict(col_data) for col_data in schematiq_config["initial_schema"]]
            initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
            logger.info("Loaded inline initial schema with %d columns", len(columns))
            return initial_schema
        except Exception as e:
            logger.warning("Could not load inline initial schema: %s", e)

    elif "initial_schema_path" in schematiq_config:
        try:
            with open(schematiq_config["initial_schema_path"]) as f:
                initial_data = json.load(f)
                if isinstance(initial_data, list):
                    columns = [Column.from_dict(col_data) for col_data in initial_data]
                    initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
                elif isinstance(initial_data, dict) and "schema" in initial_data:
                    columns = [Column.from_dict(col_data) for col_data in initial_data["schema"]]
                    initial_schema = Schema(query=query, columns=columns, max_keys=max_keys)
                else:
                    return None
            logger.info("Loaded initial schema from file with %d columns", len(columns))
            return initial_schema
        except Exception as e:
            logger.warning("Could not load initial schema from file: %s", e)

    return None


def _save_partial_schema(schema: Schema, query: str, session_dir: Path) -> None:
    """Save partial schema to disk for stop resilience."""
    partial_schema_file = session_dir / "discovered_schema.json"
    frontend_schema = []
    for col in schema.columns:
        col_dict = col.to_dict()
        frontend_col = {
            "name": col_dict.get("column", col.name),
            "definition": col_dict.get("definition", ""),
            "rationale": col_dict.get("explanation", col.rationale)
        }
        if col_dict.get("allowed_values"):
            frontend_col["allowed_values"] = col_dict["allowed_values"]
        frontend_schema.append(frontend_col)
    partial_schema_data = {"query": query, "schema": frontend_schema}
    if schema.observation_unit:
        partial_schema_data["observation_unit"] = schema.observation_unit.to_dict()
    with open(partial_schema_file, 'w') as f:
        json.dump(partial_schema_data, f, indent=2)
    logger.debug("Saved partial schema with %d columns", len(schema.columns))
