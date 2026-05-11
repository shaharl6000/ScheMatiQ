"""Value extraction pipeline — extracts structured data from documents using a discovered schema."""

import asyncio
import json
import logging
import time
from typing import Dict, Any, List, Callable, Optional
from pathlib import Path
from datetime import datetime

from app.services import schematiq_thread_pool
from app.services.pipeline.callbacks import create_value_extracted_callback, create_warning_callback

from schematiq.core.schema import Schema
from schematiq.core.llm_backends import LLMInterface
from schematiq.value_extraction.main import build_table_jsonl

logger = logging.getLogger(__name__)


async def run_value_extraction(
    session_id: str,
    schematiq_config: Dict[str, Any],
    schema: Schema,
    llm: LLMInterface,
    retriever,
    progress_callback,
    is_stop_requested: Callable[[str], bool],
    ws_mixin,
    ws_manager,
    session_manager,
    work_dir: Path,
) -> List[str]:
    """Run real value extraction using the value extraction pipeline.

    Returns:
        List of skipped document names (documents with no observation units found)
    """
    session_dir = work_dir / session_id

    # Save schema in value extraction format
    schema_data = schema.to_full_dict()
    value_extraction_schema_path = session_dir / "value_extraction_schema.json"
    with open(value_extraction_schema_path, 'w') as f:
        json.dump(schema_data, f, indent=2)

    # Prepare documents directories
    docs_paths = schematiq_config["docs_path"]
    if isinstance(docs_paths, str):
        docs_paths = [docs_paths]

    docs_directories = [Path(path) for path in docs_paths]
    output_path = session_dir / "extracted_data.jsonl"

    # Count total documents for progress tracking
    total_documents = 0
    for docs_dir in docs_directories:
        if docs_dir.exists():
            doc_files = list(docs_dir.glob("*.txt")) + list(docs_dir.glob("*.md"))
            total_documents += len(doc_files)

    # Update session metadata
    session = session_manager.get_session(session_id)
    session.metadata.total_documents = total_documents
    session.metadata.processed_documents = 0
    session_manager.update_session(session)

    loop = asyncio.get_event_loop()

    on_value_extracted = create_value_extracted_callback(ws_mixin, session_id, loop)
    on_warning = create_warning_callback(ws_manager, session_id, loop)

    extraction_result = {}

    def should_stop():
        return is_stop_requested(session_id)

    def run_extraction():
        nonlocal extraction_result
        extraction_result = build_table_jsonl(
            schema_path=value_extraction_schema_path,
            docs_directories=docs_directories,
            output_path=output_path,
            llm=llm,
            retriever=retriever,
            resume=False,
            mode="all",
            retrieval_k=8,
            max_workers=1,
            on_value_extracted=on_value_extracted,
            should_stop=should_stop,
            on_warning=on_warning
        )
        return extraction_result

    # Track progress by monitoring output file
    extraction_task = loop.run_in_executor(schematiq_thread_pool, run_extraction)

    start_time = time.time()
    last_line_count = 0
    last_update_time = time.time()
    prev_completed_documents = set()

    stopped_early = False
    stop_requested_at = None
    MAX_STOP_WAIT = 120

    while not extraction_task.done():
        if is_stop_requested(session_id):
            if stop_requested_at is None:
                logger.warning("Stop requested during value extraction - waiting for graceful stop")
                stop_requested_at = time.time()
                stopped_early = True

            elapsed = time.time() - stop_requested_at
            if elapsed > MAX_STOP_WAIT:
                logger.warning("Graceful stop timeout after %ds - forcing exit", MAX_STOP_WAIT)
                break

            await asyncio.sleep(0.5)
            continue

        try:
            current_time = time.time()

            if output_path.exists():
                completed_documents = set()
                current_line_count = 0
                with open(output_path, 'r') as f:
                    for line in f:
                        current_line_count += 1
                        try:
                            row_data = json.loads(line)
                            metadata = row_data.get("_metadata", {})
                            doc_name = metadata.get("base_row_name") or row_data.get("_row_name")
                            if doc_name:
                                completed_documents.add(doc_name)
                        except json.JSONDecodeError:
                            pass

                completed_doc_count = len(completed_documents)

                if current_line_count > last_line_count:
                    session = session_manager.get_session(session_id)
                    session.metadata.processed_documents = min(completed_doc_count, total_documents)
                    session_manager.update_session(session)

                    newly_completed = list(completed_documents - prev_completed_documents)

                    await ws_mixin.broadcast_row_completed(session_id, {
                        "row_index": completed_doc_count,
                        "total_rows": total_documents,
                        "completed_at": datetime.now().isoformat(),
                        "document_names": newly_completed,
                        "elapsed_seconds": int(current_time - start_time),
                    })

                    prev_completed_documents = set(completed_documents)
                    last_line_count = current_line_count
                    last_update_time = current_time

            await asyncio.sleep(2)

        except Exception as e:
            logger.warning("Progress monitoring error: %s", e)
            await asyncio.sleep(2)

    # Wait for completion
    if not stopped_early:
        try:
            extraction_result = await extraction_task
        except Exception as e:
            raise RuntimeError(f"Value extraction failed: {e}")
    else:
        try:
            await asyncio.wait_for(asyncio.shield(extraction_task), timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass

    # Final progress update
    final_line_count = 0
    if output_path.exists():
        with open(output_path, 'r') as f:
            final_line_count = sum(1 for _ in f)

    session = session_manager.get_session(session_id)
    session.metadata.processed_documents = final_line_count
    session_manager.update_session(session)

    if stopped_early:
        logger.warning("Value extraction stopped early with %d rows extracted", final_line_count)
        return []

    suggested_values = extraction_result.get("suggested_values", {}) if extraction_result else {}
    skipped_documents = extraction_result.get("skipped_documents", []) if extraction_result else []

    if skipped_documents:
        names = ', '.join(skipped_documents[:5])
        suffix = f' and {len(skipped_documents) - 5} more' if len(skipped_documents) > 5 else ''
        await ws_manager.broadcast_log(session_id, {
            "level": "warning",
            "message": f"{len(skipped_documents)} document(s) skipped: {names}{suffix}"
        })

    if suggested_values:
        await process_suggested_values(session, schema, suggested_values, ws_manager)

    session_manager.update_session(session)

    await progress_callback("Value Extraction: Complete", 1.0, {
        "rows_extracted": final_line_count,
        "total_documents": total_documents,
        "elapsed_time": int(time.time() - start_time),
        "suggested_values_count": sum(len(vals) for vals in suggested_values.values()) if suggested_values else 0,
        "skipped_documents": skipped_documents,
        "skipped_documents_count": len(skipped_documents)
    })

    return skipped_documents


async def process_suggested_values(
    session,
    schema: Schema,
    suggested_values: Dict[str, Dict[str, Any]],
    ws_manager,
):
    """Process suggested values from value extraction for schema evolution.

    For each column with allowed_values:
    - Auto-add values that meet the column's auto_expand_threshold
    - Store remaining values as pending_values for user review
    """
    from app.models.session import PendingValue

    if not suggested_values:
        return

    logger.info("Processing %d suggested values for schema evolution", sum(len(vals) for vals in suggested_values.values()))

    for col in session.columns:
        if col.name not in suggested_values:
            continue

        col_suggestions = suggested_values[col.name]
        if not col_suggestions:
            continue

        threshold = col.auto_expand_threshold if col.auto_expand_threshold is not None else 2

        auto_added = []
        pending = []

        for value, details in col_suggestions.items():
            doc_count = details.get("count", 0)
            documents = details.get("documents", [])

            if col.allowed_values and value in col.allowed_values:
                continue

            if threshold > 0 and doc_count >= threshold:
                if col.allowed_values is None:
                    col.allowed_values = []
                col.allowed_values.append(value)
                auto_added.append(value)
                logger.info("  Auto-added '%s' to %s (appeared in %d docs, threshold=%d)", value, col.name, doc_count, threshold)
            else:
                pending.append(PendingValue(
                    value=value,
                    document_count=doc_count,
                    first_seen=datetime.now(),
                    documents=documents[:10]
                ))

        if pending:
            if col.pending_values is None:
                col.pending_values = []
            col.pending_values.extend(pending)
            logger.info("  Added %d pending values to %s for review", len(pending), col.name)

        if auto_added:
            logger.info("  Auto-expanded %s allowed_values with %d new values", col.name, len(auto_added))

    total_auto_added = sum(1 for col in session.columns if col.allowed_values)
    total_pending = sum(len(col.pending_values or []) for col in session.columns)

    if total_auto_added > 0 or total_pending > 0:
        await ws_manager.broadcast_schema_updated(session.id, {
            "operation": "schema_evolution",
            "auto_added_values": total_auto_added,
            "pending_values": total_pending,
            "columns": [col.model_dump(mode='json') for col in session.columns]
        })
