"""Data query, schema retrieval, status reporting, and statistics computation."""

import json
import logging
import math
from typing import Dict, Any, Optional, List
from pathlib import Path

from app.models.session import (
    ColumnInfo, DataStatistics, DataRow, PaginatedData, SessionStatus,
    SchemaEvolution, VisualizationSession, SkippedDocumentInfo
)
from app.models.schematiq import ScheMatiQStatus

from schematiq.core.schema import Schema

logger = logging.getLogger(__name__)


def compute_statistics(
    session_id: str,
    schema: Schema,
    schema_evolution: Optional[SchemaEvolution] = None,
    skipped_documents: Optional[List[SkippedDocumentInfo]] = None,
    work_dir: Path = None,
) -> Optional[DataStatistics]:
    """Compute statistics from extracted JSONL data."""
    from app.services.data_utils import collect_all_data_rows, normalize_row_data

    data_rows = collect_all_data_rows(session_id, work_dir=work_dir)

    if not data_rows:
        logger.warning("Statistics: No data found for session %s (schema-only mode)", session_id)
        columns = []
        for col in schema.columns:
            col_info = ColumnInfo(
                name=col.name,
                definition=col.definition,
                rationale=col.rationale,
                data_type="object",
                non_null_count=0,
                unique_count=0,
                source_document=col.source_document,
                discovery_iteration=col.discovery_iteration,
                allowed_values=col.allowed_values
            )
            columns.append(col_info)

        return DataStatistics(
            total_rows=0,
            total_columns=len(schema.columns),
            total_documents=0,
            completeness=0.0,
            column_stats=columns,
            schema_evolution=schema_evolution,
            skipped_documents=skipped_documents or [],
        )

    # Count unique documents from papers field
    unique_documents = set()
    for row in data_rows:
        papers = row.get('papers', row.get('_papers', []))
        if isinstance(papers, list):
            unique_documents.update(papers)
        elif isinstance(papers, str) and papers:
            unique_documents.add(papers)
    total_documents = len(unique_documents) if unique_documents else len(data_rows)

    columns = []
    for col in schema.columns:
        def is_valid_value(value):
            if value is None:
                return False
            if isinstance(value, dict):
                answer = value.get("answer")
                if answer is None or answer == "None" or answer == "" or answer == "[]":
                    return False
                return True
            return value != "None" and value != "" and value != "[]"

        non_null_count = 0
        unique_values = set()

        for row in data_rows:
            row_data = normalize_row_data(row)

            if col.name in row_data:
                value = row_data[col.name]
                if is_valid_value(value):
                    non_null_count += 1
                canonical = value.get("answer") if isinstance(value, dict) else value
                try:
                    unique_values.add(json.dumps(canonical, sort_keys=True))
                except (TypeError, ValueError):
                    unique_values.add(str(canonical))
        unique_count = len(unique_values)

        source_document = None
        if schema_evolution and col.name in schema_evolution.column_sources:
            source_document = schema_evolution.column_sources[col.name]

        col_info = ColumnInfo(
            name=col.name,
            definition=col.definition,
            rationale=col.rationale,
            data_type="object",
            non_null_count=non_null_count,
            unique_count=unique_count,
            source_document=source_document,
            discovery_iteration=getattr(col, 'discovery_iteration', None),
            allowed_values=getattr(col, 'allowed_values', None),
            auto_expand_threshold=getattr(col, 'auto_expand_threshold', 2)
        )
        columns.append(col_info)

    total_cells = len(data_rows) * len(columns)
    non_null_cells = sum(col.non_null_count or 0 for col in columns)
    completeness = (non_null_cells / total_cells * 100) if total_cells > 0 else 0.0

    if math.isnan(completeness) or math.isinf(completeness):
        completeness = 0.0

    stats = DataStatistics(
        total_rows=len(data_rows),
        total_columns=len(columns),
        total_documents=total_documents,
        completeness=completeness,
        column_stats=columns,
        schema_evolution=schema_evolution,
        skipped_documents=skipped_documents or [],
    )

    logger.info("Statistics computed: %d rows, %d documents, %d columns, %.1f%% complete", len(data_rows), total_documents, len(columns), completeness)
    if skipped_documents:
        logger.info("Skipped documents: %d", len(skipped_documents))
    if schema_evolution:
        logger.info("Schema evolution: %d snapshots, %d column sources", len(schema_evolution.snapshots), len(schema_evolution.column_sources))
    return stats


async def get_status(
    session_id: str,
    session_manager,
    running_sessions: Dict[str, Any],
    state_lock,
    work_dir: Path,
) -> ScheMatiQStatus:
    """Get current status of ScheMatiQ execution."""
    with state_lock:
        is_running = session_id in running_sessions

    session = session_manager.get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    schema_completed = session.metadata.schema_discovery_completed
    columns_discovered = len(session.columns) if session.columns else 0

    if session.status == SessionStatus.COMPLETED:
        status = "completed"
        progress = 1.0
    elif session.status == SessionStatus.ERROR:
        status = "error"
        progress = 0.0
    elif session.status == SessionStatus.STOPPED:
        status = "stopped"
        progress = 1.0
    elif is_running:
        status = "processing"
        progress = 0.5
    else:
        if session.status == SessionStatus.DOCUMENTS_UPLOADED:
            status = "documents_uploaded"
            progress = 1.0
        elif session.status == SessionStatus.PROCESSING_DOCUMENTS:
            status = "processing_documents"
            progress = 0.5
        elif session.status == SessionStatus.OBSERVATION_UNIT_REVIEW:
            status = "observation_unit_review"
            progress = 0.3
        else:
            status = "idle"
            progress = 0.0

    config_path = work_dir / session_id / "config.json"
    schema_only = False
    try:
        if config_path.exists():
            cfg = json.loads(config_path.read_text())
            schema_only = cfg.get("skip_value_extraction", False)
    except (OSError, ValueError) as exc:  # missing/unreadable file or bad JSON
        logger.debug("Could not read skip_value_extraction from %s: %s", config_path, exc)

    # Deferred extraction completed — table data exists even if config still has skip flag
    if schema_only:
        extracted_file = work_dir / session_id / "extracted_data.jsonl"
        if extracted_file.exists() and extracted_file.stat().st_size > 0:
            schema_only = False
        elif session.statistics and (session.statistics.total_rows or 0) > 0:
            schema_only = False

    return ScheMatiQStatus(
        session_id=session_id,
        status=status,
        progress=progress,
        current_step="Running" if status == "processing" else status.title(),
        steps_completed=3 if status == "processing" else (7 if status == "completed" else 0),
        total_steps=7,
        schema_completed=schema_completed,
        columns_discovered=columns_discovered,
        total_documents=session.metadata.total_documents or 0,
        processed_documents=session.metadata.processed_documents or 0,
        llm_stats=session.metadata.processing_stats.get("llm_stats"),
        schema_only=schema_only,
    )


async def get_schema(session_id: str, session_manager, work_dir: Path) -> Dict[str, Any]:
    """Get discovered schema."""
    session = session_manager.get_session(session_id)
    if session and session.columns:
        result = {
            "query": session.schema_query or "",
            "schema": [col.model_dump() for col in session.columns]
        }
        if session.observation_unit:
            result["observation_unit"] = session.observation_unit.model_dump()
        return result

    schema_file = work_dir / session_id / "discovered_schema.json"
    if schema_file.exists():
        with open(schema_file) as f:
            return json.load(f)

    if session:
        return {"query": session.schema_query or "", "schema": []}
    return {"query": "", "schema": []}


async def get_data(
    session_id: str,
    work_dir: Path,
    page: int = 0,
    page_size: int = 50,
    filters: Optional[List[Dict]] = None,
    sort: Optional[List[Dict]] = None,
    search: Optional[str] = None,
    document_filter: Optional[List[str]] = None
) -> PaginatedData:
    """Get extracted data from all possible locations with optional filtering and sorting."""
    from app.services.data_utils import get_data_dir, resolve_session_data_files

    data_files = await resolve_session_data_files(
        session_id,
        work_dir=work_dir,
        data_dir=get_data_dir(),
    )

    if not data_files:
        return PaginatedData(rows=[], total_count=0, filtered_count=None, page=page, page_size=page_size, has_more=False)

    from app.services.data_utils import row_dedup_key

    def normalize_row(row_data: dict) -> dict:
        if '_row_name' in row_data:
            return {
                'row_name': row_data.get('_row_name'),
                'papers': row_data.get('_papers', []),
                'data': {k: v for k, v in row_data.items() if not k.startswith('_')},
                'unit_name': row_data.get('_unit_name'),
                'source_document': row_data.get('_source_document'),
                'parent_document': row_data.get('_parent_document'),
                '_cell_status': row_data.get('_cell_status'),
            }
        return row_data

    needs_processing = bool(filters or sort or search or document_filter)

    if needs_processing:
        all_rows = []
        seen_keys: set = set()
        for data_file in data_files:
            file_keys: set = set()
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            row_data = json.loads(line.strip())
                            key = row_dedup_key(row_data)
                            if key[0] and key in seen_keys:
                                continue
                            if key[0]:
                                file_keys.add(key)
                            all_rows.append(normalize_row(row_data))
                        except (json.JSONDecodeError, TypeError):
                            pass
            seen_keys.update(file_keys)

        if document_filter:
            doc_set = set(document_filter)
            all_rows = [
                r for r in all_rows
                if (r.get('source_document') or r.get('_source_document') or '') in doc_set
            ]

        total_count = len(all_rows)

        from app.services.file_parser import FileParser
        parser = FileParser()

        if search and search.strip():
            all_rows = parser._apply_search(all_rows, search.strip())

        if filters:
            all_rows = parser._apply_filters(all_rows, filters)

        filtered_count = len(all_rows)

        if sort:
            all_rows = parser._apply_sort(all_rows, sort)

        start = page * page_size
        end = start + page_size
        page_rows = all_rows[start:end]

        rows = [DataRow(**row_data) for row_data in page_rows]

        return PaginatedData(
            rows=rows,
            total_count=total_count,
            filtered_count=filtered_count,
            page=page,
            page_size=page_size,
            has_more=end < filtered_count
        )
    else:
        # Efficient pagination (no filtering/sorting)
        total_count = 0
        seen_keys: set = set()
        for data_file in data_files:
            file_keys: set = set()
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        row_data = json.loads(line.strip())
                        key = row_dedup_key(row_data)
                        if key[0] and key in seen_keys:
                            continue
                        if key[0]:
                            file_keys.add(key)
                        total_count += 1
                    except (json.JSONDecodeError, TypeError):
                        pass
            seen_keys.update(file_keys)

        rows = []
        start_line = page * page_size
        end_line = start_line + page_size
        global_line = 0
        seen_keys_page: set = set()

        for data_file in data_files:
            if global_line >= end_line:
                break

            file_keys: set = set()
            with open(data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        row_data = json.loads(line.strip())
                    except (json.JSONDecodeError, TypeError):
                        continue
                    key = row_dedup_key(row_data)
                    if key[0] and key in seen_keys_page:
                        continue
                    if key[0]:
                        file_keys.add(key)

                    if global_line >= end_line:
                        break
                    if global_line >= start_line:
                        normalized = normalize_row(row_data)
                        data_row = DataRow(**normalized)
                        rows.append(data_row)
                    global_line += 1
            seen_keys_page.update(file_keys)

        return PaginatedData(
            rows=rows,
            total_count=total_count,
            filtered_count=None,
            page=page,
            page_size=page_size,
            has_more=end_line < total_count
        )
