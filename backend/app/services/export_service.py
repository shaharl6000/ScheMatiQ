"""Shared data and schema export helpers for upload and ScheMatiQ routes."""

from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.core.config import DEFAULT_DATA_DIR
from app.models.session import PaginatedData, VisualizationSession
from app.services.document_files import gather_source_documents
from app.services.file_parser import format_column_header
from app.services.grounding_flags import (
    UNGROUNDED_COLUMNS_FIELD,
    any_ungrounded,
    ungrounded_columns,
)
from app.utils.csv_helpers import format_excerpt_for_csv

logger = logging.getLogger(__name__)

_STANDARD_METADATA_COLUMNS = frozenset(
    {"_row_name", "row_name", "papers", "_unit_name", "_source_document"}
)
_UPLOAD_META_COLUMNS = ("_row_name", "_unit_name", "_source_document")


def _sanitize_source_name(source: Optional[str], default: str) -> str:
    source_name = source or default
    if source_name.lower().endswith((".csv", ".json", ".jsonl")):
        source_name = Path(source_name).stem
    return "".join(c for c in source_name if c.isalnum() or c in (" ", "-", "_")).rstrip()


def _user_timestamp(tz_offset: Optional[int] = None, fmt: str = "%Y-%m-%d_%H-%M-%S") -> str:
    if tz_offset is None:
        now = datetime.now()
    else:
        now = datetime.utcnow() - timedelta(minutes=tz_offset)
    return now.strftime(fmt)


def _csv_response(content: str, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _json_response(content: str, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _zip_response(content: bytes, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _flatten_cell_value(value: Any, *, stringify_containers: bool = False) -> Any:
    """Convert ScheMatiQ answer format {'answer': ..., 'excerpts': [...]} to plain value."""
    if isinstance(value, dict) and "answer" in value:
        return value["answer"]
    if stringify_containers and isinstance(value, (list, dict)):
        return str(value)
    return value


def _write_display_header_csv(column_names: List[str], rows: Iterable[Dict[str, Any]]) -> str:
    output = io.StringIO()
    display_headers = {c: format_column_header(c) for c in column_names}
    writer = csv.DictWriter(output, fieldnames=column_names, extrasaction="ignore")
    writer.writerow(display_headers)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _load_schematiq_llm_configuration(session_id: str) -> Optional[Dict[str, Any]]:
    schematiq_config_file = Path(DEFAULT_DATA_DIR) / session_id / "schematiq_config.json"
    if not schematiq_config_file.exists():
        return None
    try:
        with open(schematiq_config_file, encoding="utf-8") as f:
            schematiq_config = json.load(f)
        return {
            "schema_creation_backend": schematiq_config.get("schema_creation_backend"),
            "value_extraction_backend": schematiq_config.get("value_extraction_backend"),
        }
    except Exception as e:
        logger.warning("Could not load LLM configuration for session %s: %s", session_id, e)
        return None


def _attach_common_session_sections(
    payload: Dict[str, Any],
    session: VisualizationSession,
    *,
    include_skipped_in_metadata: bool = False,
) -> None:
    if session.statistics and session.statistics.schema_evolution:
        payload["schema_evolution"] = session.statistics.schema_evolution.model_dump()
    if (
        include_skipped_in_metadata
        and session.statistics
        and session.statistics.skipped_documents
    ):
        payload["metadata"]["skipped_documents"] = [
            doc.model_dump() if hasattr(doc, "model_dump") else doc
            for doc in session.statistics.skipped_documents
        ]
    if session.observation_unit:
        payload["observation_unit"] = session.observation_unit.model_dump()


async def _build_project_bundle_bytes(
    session: VisualizationSession,
    session_id: str,
    export_data: Dict[str, Any],
) -> bytes:
    documents = await gather_source_documents(session, session_id)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "project.json",
            json.dumps(export_data, indent=2, ensure_ascii=False, default=str),
        )
        for name, content in documents:
            safe = Path(name).name
            if safe:
                zip_file.writestr(f"documents/{safe}", content)
    return buf.getvalue()


def _build_column_metadata_csv(
    columns: List[Dict[str, Any]],
    special_rows: List[List[str]],
) -> str:
    metadata_output = io.StringIO()
    metadata_writer = csv.writer(metadata_output)
    metadata_writer.writerow(["Column Name", "Definition", "Rationale", "Data Type"])
    for col in columns:
        metadata_writer.writerow(
            [
                col["name"],
                col.get("definition", ""),
                col.get("rationale", ""),
                col.get("data_type", "text"),
            ]
        )
    for special in special_rows:
        metadata_writer.writerow(special)
    return metadata_output.getvalue()


def _build_column_metadata_lookup(session: VisualizationSession) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    for col in session.columns:
        if col.name:
            lookup[col.name] = {
                "definition": col.definition or "",
                "rationale": col.rationale or "",
                "allowed_values": ", ".join(col.allowed_values) if col.allowed_values else "",
            }
    return lookup


def _write_rich_csv_comments(
    output: io.StringIO,
    *,
    generated_str: str,
    session_id: str,
    session: VisualizationSession,
    include_source: bool,
    always_include_query: bool,
) -> None:
    output.write("# Metadata-Rich CSV Export\n")
    output.write(f"# Generated: {generated_str}\n")
    output.write(f"# Session ID: {session_id}\n")
    if include_source:
        output.write(f"# Source: {session.metadata.source}\n")
    if always_include_query:
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
    elif session.schema_query:
        output.write(f"# Query: {session.schema_query}\n")
    if session.observation_unit:
        obs_unit_json = json.dumps(session.observation_unit.model_dump())
        output.write(f"# Observation Unit: {obs_unit_json}\n")
    output.write("# Format: Each data column has corresponding _definition and _rationale columns\n")
    output.write("#\n")


# ==================== Standard CSV Exports ====================


def _parse_requested_columns(
    column_order: str,
    available_columns: set[str],
) -> List[str]:
    """Parse comma-separated ``column_order`` into deduplicated valid column names.

    Supports column names that themselves contain commas (e.g. ``'Notes, misc'``)
    by matching the longest comma-joined span present in ``available_columns``
    before falling back to a single comma-delimited token.
    """
    parts = column_order.split(",")
    has_comma_col = any("," in col for col in available_columns)
    ordered: List[str] = []
    seen: set[str] = set()
    i = 0
    while i < len(parts):
        matched: Optional[str] = None
        if has_comma_col:
            for j in range(len(parts), i, -1):
                candidate = ",".join(parts[i:j]).strip()
                if candidate and candidate in available_columns:
                    matched = candidate
                    i = j
                    break
        if matched is None:
            candidate = parts[i].strip()
            if candidate and candidate in available_columns:
                matched = candidate
            i += 1
        if matched is not None and matched not in seen:
            seen.add(matched)
            ordered.append(matched)
    return ordered


def _order_upload_csv_columns(
    available_columns: set[str],
    session: VisualizationSession,
    column_order: Optional[str] = None,
) -> List[str]:
    """Return upload CSV columns in a deterministic order independent of hash seed.

    Requested columns in ``column_order`` come first (deduplicated, preserving
    request order). All remaining columns — or all columns when ``column_order``
    is omitted — follow the canonical default order: metadata columns, schema
    columns, sorted extra columns, and ``_ungrounded_columns`` last.

    Extra columns are data columns outside the schema. Internal fields of a
    flat row (``_papers``, ``_cell_status``, ``_parent_document``,
    ``_row_index``, ...) start with ``_`` and are left out unless requested.
    """
    column_names: List[str] = []
    seen: set[str] = set()

    def _add(col_name: str) -> None:
        if col_name in available_columns and col_name not in seen:
            seen.add(col_name)
            column_names.append(col_name)

    if column_order:
        for col_name in _parse_requested_columns(column_order, available_columns):
            _add(col_name)

    for meta_col in _UPLOAD_META_COLUMNS:
        _add(meta_col)

    for col in session.columns:
        if col.name:
            _add(col.name)

    for extra_col in sorted(
        col
        for col in available_columns
        if col not in seen and not col.startswith("_")
    ):
        _add(extra_col)

    _add(UNGROUNDED_COLUMNS_FIELD)
    return column_names


def export_upload_csv(
    session: VisualizationSession,
    data_file: Path,
    column_order: Optional[str] = None,
    tz_offset: int = 0,
) -> StreamingResponse:
    """Build standard CSV export response for an uploaded/imported session."""
    if not data_file.exists():
        raise HTTPException(status_code=404, detail="No data found for export")

    schema_col_names = {col.name for col in session.columns if col.name}
    raw_rows: List[Dict[str, Any]] = []
    has_row_name = False
    has_unit_name = False
    has_source_document = False

    with open(data_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row_data = json.loads(line)
                raw_rows.append(row_data)
                if "data" in row_data:
                    if row_data.get("row_name"):
                        has_row_name = True
                    if row_data.get("_unit_name"):
                        has_unit_name = True
                    if row_data.get("_source_document"):
                        has_source_document = True

    has_ungrounded = any_ungrounded(
        row_data.get("data", {})
        for row_data in raw_rows
        if isinstance(row_data.get("data"), dict)
    )
    schema_col_order = [col.name for col in session.columns if col.name]

    rows: List[Dict[str, Any]] = []
    for row_data in raw_rows:
        flat_row: Dict[str, Any] = {}
        if "data" in row_data:
            for key, value in row_data["data"].items():
                if key in schema_col_names:
                    flat_row[key] = _flatten_cell_value(value)
            if has_ungrounded:
                flat_row[UNGROUNDED_COLUMNS_FIELD] = ungrounded_columns(
                    row_data["data"], schema_col_order
                )
            if has_row_name:
                flat_row["_row_name"] = row_data.get("row_name", "")
            if has_unit_name:
                flat_row["_unit_name"] = row_data.get("_unit_name", "")
            if has_source_document:
                flat_row["_source_document"] = row_data.get("_source_document", "")
        else:
            for key, value in row_data.items():
                flat_row[key] = _flatten_cell_value(value)
        rows.append(flat_row)

    if not rows:
        raise HTTPException(status_code=404, detail="No data to export")

    all_keys: set[str] = set()
    for row in rows:
        all_keys.update(row.keys())
    sorted_keys = sorted(all_keys)
    for row in rows:
        for col in sorted_keys:
            if col not in row:
                row[col] = ""

    column_names = _order_upload_csv_columns(all_keys, session, column_order)

    content = _write_display_header_csv(column_names, rows)
    safe_name = _sanitize_source_name(session.metadata.source, "uploaded_data")
    timestamp = _user_timestamp(tz_offset)
    return _csv_response(content, f"{safe_name}_{timestamp}.csv")


def export_schematiq_csv(
    session: VisualizationSession,
    data: PaginatedData,
    column_order: Optional[str] = None,
    tz_offset: int = 0,
) -> StreamingResponse:
    """Build standard CSV export response for a ScheMatiQ session."""
    is_schema_only = not data.rows
    schema_col_names = {col.name for col in session.columns if col.name}

    if is_schema_only:
        column_names = ["row_name"] + [col.name for col in session.columns if col.name]
    else:
        has_row_name = any(row.row_name for row in data.rows)
        has_unit_name = any(row.unit_name for row in data.rows)
        has_source_document = any(row.source_document for row in data.rows)

        if column_order:
            column_names = _parse_requested_columns(column_order, schema_col_names)
            column_set = set(column_names)
            remaining = [col for col in schema_col_names if col not in column_set]
            column_names.extend(sorted(remaining))
        else:
            column_names = [col.name for col in session.columns if col.name]

        id_cols: List[str] = []
        if has_row_name:
            id_cols.append("row_name")
        if has_unit_name:
            id_cols.append("_unit_name")
        if has_source_document:
            id_cols.append("_source_document")
        column_names = id_cols + column_names

        if any_ungrounded(row.data for row in data.rows):
            column_names.append(UNGROUNDED_COLUMNS_FIELD)

    include_ungrounded = UNGROUNDED_COLUMNS_FIELD in column_names
    csv_rows: List[Dict[str, Any]] = []
    for row in data.rows:
        csv_row: Dict[str, Any] = {}
        if row.row_name:
            csv_row["row_name"] = row.row_name
        if row.unit_name:
            csv_row["_unit_name"] = row.unit_name
        if row.source_document:
            csv_row["_source_document"] = row.source_document
        for col_name, value in row.data.items():
            if col_name not in schema_col_names:
                continue
            csv_row[col_name] = _flatten_cell_value(value, stringify_containers=True)
        if include_ungrounded:
            csv_row[UNGROUNDED_COLUMNS_FIELD] = ungrounded_columns(row.data, column_names)
        csv_rows.append(csv_row)

    content = _write_display_header_csv(column_names, csv_rows)
    timestamp = _user_timestamp(tz_offset)
    filename = f"ScheMatiQ_{'schema_only_' if is_schema_only else ''}{timestamp}.csv"
    return _csv_response(content, filename)


# ==================== Complete Exports (JSON / Bundle / ZIP) ====================


async def export_complete_upload(
    session: VisualizationSession,
    session_id: str,
    data_file: Path,
    format: str = "json",
) -> StreamingResponse:
    """Export complete upload session data in JSON, bundle ZIP, or separate-files ZIP format."""
    export_data: Dict[str, Any] = {
        "session_id": session_id,
        "session_type": session.type.value,
        "created": session.metadata.created.isoformat(),
        "last_modified": session.metadata.last_modified.isoformat(),
        "query": session.schema_query,
        "schema": {
            "columns": [
                {
                    "name": col.name,
                    "definition": col.definition or "",
                    "rationale": col.rationale or "",
                    "data_type": col.data_type,
                    "source_document": col.source_document,
                    "discovery_iteration": col.discovery_iteration,
                    "allowed_values": col.allowed_values,
                }
                for col in session.columns
            ]
        },
        "metadata": {
            "total_rows": session.statistics.total_rows if session.statistics else 0,
            "total_columns": len(session.columns),
            "source": session.metadata.source,
            "file_size": session.metadata.file_size,
            "cloud_dataset": session.metadata.cloud_dataset,
            "document_metadata": session.metadata.document_metadata or {},
        },
        "data": [],
    }

    _attach_common_session_sections(
        export_data, session, include_skipped_in_metadata=True
    )

    session_dir = Path(DEFAULT_DATA_DIR) / session_id
    schematiq_config_file = session_dir / "schematiq_config.json"
    if schematiq_config_file.exists():
        try:
            with open(schematiq_config_file, encoding="utf-8") as f:
                schematiq_config = json.load(f)
                if "documents_batch_size" in schematiq_config:
                    export_data["metadata"]["documents_batch_size"] = schematiq_config[
                        "documents_batch_size"
                    ]
        except Exception:
            pass

    if data_file.exists():
        with open(data_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    export_data["data"].append(json.loads(line))

    fmt = format.lower()
    if fmt == "json":
        content = json.dumps(export_data, indent=2, ensure_ascii=False)
        return _json_response(content, f"{session_id[:8]}_complete_export.json")

    if fmt == "bundle":
        bundle_bytes = await _build_project_bundle_bytes(session, session_id, export_data)
        return _zip_response(bundle_bytes, f"{session_id[:8]}_bundle.zip")

    if fmt == "zip":
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zip_file:
            schema_data: Dict[str, Any] = {
                "query": export_data["query"],
                "schema": export_data["schema"]["columns"],
            }
            parsed_schema_file = session_dir / "parsed_schema.json"
            if parsed_schema_file.exists():
                try:
                    with open(parsed_schema_file, encoding="utf-8") as f:
                        parsed_schema = json.load(f)
                        if "llm_configuration" in parsed_schema:
                            schema_data["llm_configuration"] = parsed_schema[
                                "llm_configuration"
                            ]
                except Exception:
                    pass

            zip_file.writestr("schema.json", json.dumps(schema_data, indent=2))
            zip_file.writestr(
                "column_metadata.csv",
                _build_column_metadata_csv(
                    export_data["schema"]["columns"],
                    [
                        [
                            "row_name",
                            "Identifier for this data row",
                            "Standard upload metadata field",
                            "text",
                        ],
                        [
                            "papers",
                            "Source documents (if applicable)",
                            "Standard upload metadata field",
                            "text",
                        ],
                    ],
                ),
            )

            if export_data["data"]:
                output = io.StringIO()
                all_columns = set()
                for row in export_data["data"]:
                    if "data" in row:
                        all_columns.update(row["data"].keys())
                    if "row_name" in row:
                        all_columns.add("row_name")
                    if "papers" in row:
                        all_columns.add("papers")

                column_names = sorted(all_columns)
                writer = csv.DictWriter(output, fieldnames=column_names)
                writer.writeheader()
                for row in export_data["data"]:
                    csv_row: Dict[str, Any] = {}
                    if "row_name" in row:
                        csv_row["row_name"] = row["row_name"]
                    if "papers" in row:
                        csv_row["papers"] = row["papers"]
                    if "data" in row:
                        for col, value in row["data"].items():
                            csv_row[col] = str(value) if value is not None else ""
                    writer.writerow(csv_row)
                zip_file.writestr("data.csv", output.getvalue())

            zip_file.writestr("metadata.json", json.dumps(export_data["metadata"], indent=2))

        return _zip_response(buf.getvalue(), f"{session_id[:8]}_complete_export.zip")

    raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'zip'")


async def export_complete_schematiq(
    session: VisualizationSession,
    session_id: str,
    data: PaginatedData,
    format: str = "json",
    tz_offset: int = 0,
) -> StreamingResponse:
    """Export complete ScheMatiQ session data in JSON, bundle ZIP, or separate-files ZIP format."""
    llm_configuration = _load_schematiq_llm_configuration(session_id)

    export_data: Dict[str, Any] = {
        "session_id": session_id,
        "session_type": session.type.value,
        "created": session.metadata.created.isoformat(),
        "last_modified": session.metadata.last_modified.isoformat(),
        "query": session.schema_query,
        "schema": {
            "columns": [
                {
                    "name": col.name,
                    "display_name": col.display_name,
                    "definition": col.definition or "",
                    "rationale": col.rationale or "",
                    "data_type": col.data_type,
                    "source_document": col.source_document,
                    "discovery_iteration": col.discovery_iteration,
                    "allowed_values": col.allowed_values,
                }
                for col in session.columns
                if col.name and not col.name.lower().endswith("_excerpt")
            ]
        },
        "metadata": {
            "total_rows": data.total_count,
            "total_columns": len(
                [col for col in session.columns if not col.name.lower().endswith("_excerpt")]
            ),
            "source": session.metadata.source,
            "schema_discovery_completed": session.metadata.schema_discovery_completed,
            "total_documents": session.metadata.total_documents,
            "processed_documents": session.metadata.processed_documents,
        },
        "data": [
            {
                "row_name": row.row_name,
                "papers": row.papers,
                "data": row.data,
                "_unit_name": row.unit_name,
                "_source_document": row.source_document,
                "_parent_document": row.parent_document,
            }
            for row in data.rows
        ],
    }

    _attach_common_session_sections(
        export_data, session, include_skipped_in_metadata=True
    )

    if llm_configuration and any(llm_configuration.values()):
        export_data["llm_configuration"] = llm_configuration

    timestamp = _user_timestamp(tz_offset)
    fmt = format.lower()
    if fmt == "json":
        content = json.dumps(export_data, indent=2, ensure_ascii=False, default=str)
        return _json_response(content, f"ScheMatiQ_{timestamp}_complete.json")

    if fmt == "bundle":
        bundle_bytes = await _build_project_bundle_bytes(session, session_id, export_data)
        return _zip_response(bundle_bytes, f"ScheMatiQ_{timestamp}_bundle.zip")

    if fmt == "zip":
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zip_file:
            schema_data: Dict[str, Any] = {
                "query": export_data["query"],
                "schema": [
                    {
                        "name": col["name"],
                        "definition": col["definition"],
                        "rationale": col["rationale"],
                    }
                    for col in export_data["schema"]["columns"]
                ],
            }
            if llm_configuration and any(llm_configuration.values()):
                schema_data["llm_configuration"] = llm_configuration
            zip_file.writestr("schema.json", json.dumps(schema_data, indent=2))

            zip_file.writestr(
                "column_metadata.csv",
                _build_column_metadata_csv(
                    export_data["schema"]["columns"],
                    [
                        [
                            "row_name",
                            "Identifier for this data row",
                            "Standard ScheMatiQ metadata field",
                            "text",
                        ],
                        [
                            "papers",
                            "Source documents used for extraction",
                            "Standard ScheMatiQ metadata field",
                            "text",
                        ],
                        [
                            "{column}_excerpt",
                            "Supporting evidence from documents",
                            "Generated for each ScheMatiQ data column",
                            "text",
                        ],
                    ],
                ),
            )

            if export_data["data"]:
                output = io.StringIO()
                all_columns = set()
                for row in export_data["data"]:
                    if row["row_name"]:
                        all_columns.add("row_name")
                    if row["papers"]:
                        all_columns.add("papers")
                    for col_name, val in row["data"].items():
                        all_columns.add(col_name)
                        if isinstance(val, dict) and "excerpts" in val:
                            all_columns.add(f"{col_name}_excerpt")

                column_names = sorted(all_columns)
                writer = csv.DictWriter(output, fieldnames=column_names)
                writer.writeheader()
                for row in export_data["data"]:
                    csv_row: Dict[str, Any] = {}
                    if row["row_name"]:
                        csv_row["row_name"] = row["row_name"]
                    if row["papers"]:
                        csv_row["papers"] = (
                            "; ".join(row["papers"])
                            if isinstance(row["papers"], list)
                            else str(row["papers"])
                        )
                    for col_name, value in row["data"].items():
                        if isinstance(value, dict) and "answer" in value:
                            csv_row[col_name] = value["answer"]
                            if "excerpts" in value and value["excerpts"]:
                                excerpt_col = f"{col_name}_excerpt"
                                if isinstance(value["excerpts"], list):
                                    csv_row[excerpt_col] = " | ".join(
                                        format_excerpt_for_csv(ex) for ex in value["excerpts"]
                                    )
                                else:
                                    csv_row[excerpt_col] = str(value["excerpts"])
                        elif isinstance(value, (list, dict)):
                            csv_row[col_name] = str(value)
                        else:
                            csv_row[col_name] = value
                    writer.writerow(csv_row)
                zip_file.writestr("data.csv", output.getvalue())

            zip_file.writestr(
                "metadata.json",
                json.dumps(export_data["metadata"], indent=2, default=str),
            )

        return _zip_response(buf.getvalue(), f"ScheMatiQ_{timestamp}_complete.zip")

    raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'zip'")


# ==================== Metadata-Rich CSV Exports ====================


def export_upload_rich_csv_data(
    session: VisualizationSession,
    session_id: str,
    data_file: Path,
) -> StreamingResponse:
    """Build metadata-rich CSV export response for an uploaded/imported session."""
    if not data_file.exists():
        raise HTTPException(status_code=404, detail="No data found for export")

    rows: List[Dict[str, Any]] = []
    with open(data_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row_data = json.loads(line)
                if "data" in row_data:
                    flat_row = row_data["data"].copy()
                    if row_data.get("row_name"):
                        flat_row["_row_name"] = row_data["row_name"]
                    if row_data.get("papers"):
                        flat_row["papers"] = row_data["papers"]
                    if row_data.get("_unit_name"):
                        flat_row["_unit_name"] = row_data["_unit_name"]
                    if row_data.get("_source_document"):
                        flat_row["_source_document"] = row_data["_source_document"]
                    rows.append(flat_row)
                else:
                    rows.append(row_data)

    if not rows:
        raise HTTPException(status_code=404, detail="No data to export")

    base_columns = set()
    for row in rows:
        base_columns.update(row.keys())

    enhanced_columns: List[str] = []
    for col_name in sorted(base_columns):
        enhanced_columns.append(col_name)
        if col_name not in _STANDARD_METADATA_COLUMNS:
            enhanced_columns.append(f"{col_name}_definition")
            enhanced_columns.append(f"{col_name}_rationale")
            enhanced_columns.append(f"{col_name}_allowed_values")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=enhanced_columns)
    _write_rich_csv_comments(
        output,
        generated_str=_user_timestamp(None, "%Y-%m-%d %H:%M:%S"),
        session_id=session_id,
        session=session,
        include_source=True,
        always_include_query=False,
    )
    writer.writeheader()

    column_metadata = _build_column_metadata_lookup(session)
    for row in rows:
        csv_row: Dict[str, Any] = {}
        for col_name, value in row.items():
            csv_row[col_name] = str(value) if value is not None else ""
            if col_name not in _STANDARD_METADATA_COLUMNS and col_name in column_metadata:
                csv_row[f"{col_name}_definition"] = column_metadata[col_name]["definition"]
                csv_row[f"{col_name}_rationale"] = column_metadata[col_name]["rationale"]
                csv_row[f"{col_name}_allowed_values"] = column_metadata[col_name][
                    "allowed_values"
                ]
        writer.writerow(csv_row)

    safe_name = _sanitize_source_name(session.metadata.source, "uploaded_data")
    timestamp = _user_timestamp(None)
    return _csv_response(output.getvalue(), f"{safe_name}_{timestamp}_rich.csv")


def export_schematiq_rich_csv_data(
    session: VisualizationSession,
    session_id: str,
    data: PaginatedData,
    tz_offset: int = 0,
) -> StreamingResponse:
    """Build metadata-rich CSV export response for a ScheMatiQ session."""
    if not data.rows:
        raise HTTPException(status_code=404, detail="No data to export")

    base_columns = set()
    for row in data.rows:
        if row.row_name:
            base_columns.add("row_name")
        if row.papers:
            base_columns.add("papers")
        if row.unit_name:
            base_columns.add("_unit_name")
        if row.source_document:
            base_columns.add("_source_document")
        for col_name, val in row.data.items():
            base_columns.add(col_name)
            if isinstance(val, dict) and "excerpts" in val:
                base_columns.add(f"{col_name}_excerpt")

    has_ungrounded = any_ungrounded(row.data for row in data.rows)
    enhanced_columns: List[str] = []
    for col_name in sorted(base_columns):
        enhanced_columns.append(col_name)
        if col_name not in _STANDARD_METADATA_COLUMNS and not col_name.endswith("_excerpt"):
            enhanced_columns.append(f"{col_name}_definition")
            enhanced_columns.append(f"{col_name}_rationale")
            enhanced_columns.append(f"{col_name}_allowed_values")
            if has_ungrounded:
                enhanced_columns.append(f"{col_name}_grounded")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=enhanced_columns)
    _write_rich_csv_comments(
        output,
        generated_str=_user_timestamp(tz_offset, "%Y-%m-%d %H:%M:%S"),
        session_id=session_id,
        session=session,
        include_source=False,
        always_include_query=True,
    )
    writer.writeheader()

    column_metadata = _build_column_metadata_lookup(session)
    for row in data.rows:
        csv_row: Dict[str, Any] = {}
        if row.row_name:
            csv_row["row_name"] = row.row_name
        if row.papers:
            csv_row["papers"] = (
                "; ".join(row.papers) if isinstance(row.papers, list) else str(row.papers)
            )
        if row.unit_name:
            csv_row["_unit_name"] = row.unit_name
        if row.source_document:
            csv_row["_source_document"] = row.source_document

        for col_name, value in row.data.items():
            if isinstance(value, dict) and "answer" in value:
                csv_row[col_name] = value["answer"]
                if has_ungrounded and value["answer"] not in (None, ""):
                    csv_row[f"{col_name}_grounded"] = (
                        "false" if value.get("_ungrounded") else "true"
                    )
                if "excerpts" in value and value["excerpts"]:
                    excerpt_col = f"{col_name}_excerpt"
                    if isinstance(value["excerpts"], list):
                        csv_row[excerpt_col] = " | ".join(
                            format_excerpt_for_csv(ex) for ex in value["excerpts"]
                        )
                    else:
                        csv_row[excerpt_col] = str(value["excerpts"])
            elif isinstance(value, (list, dict)):
                csv_row[col_name] = str(value)
            else:
                csv_row[col_name] = value

            if col_name in column_metadata:
                csv_row[f"{col_name}_definition"] = column_metadata[col_name]["definition"]
                csv_row[f"{col_name}_rationale"] = column_metadata[col_name]["rationale"]
                csv_row[f"{col_name}_allowed_values"] = column_metadata[col_name][
                    "allowed_values"
                ]

        writer.writerow(csv_row)

    timestamp = _user_timestamp(tz_offset)
    return _csv_response(output.getvalue(), f"ScheMatiQ_{timestamp}_rich.csv")


# ==================== Schema-Only JSON Exports ====================


def _build_schema_only_payload(
    session: VisualizationSession,
    session_id: str,
    *,
    include_display_name: bool,
    include_discovery_completed: bool,
    llm_configuration: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    schema_columns: List[Dict[str, Any]] = []
    for col in session.columns:
        if col.name and not col.name.lower().endswith("_excerpt"):
            col_export: Dict[str, Any] = {
                "name": col.name,
                "definition": col.definition or "",
                "rationale": col.rationale or "",
                "source_document": col.source_document,
                "discovery_iteration": col.discovery_iteration,
            }
            if include_display_name and col.display_name:
                col_export["display_name"] = col.display_name
            if col.allowed_values:
                col_export["allowed_values"] = col.allowed_values
            schema_columns.append(col_export)

    metadata: Dict[str, Any] = {
        "session_id": session_id,
        "session_type": session.type.value,
        "created": session.metadata.created.isoformat(),
        "source": session.metadata.source,
        "total_columns": len(
            [col for col in session.columns if not col.name.lower().endswith("_excerpt")]
        ),
    }
    if include_discovery_completed:
        metadata["schema_discovery_completed"] = session.metadata.schema_discovery_completed
    metadata["export_timestamp"] = datetime.now().isoformat()

    schema_export: Dict[str, Any] = {
        "query": session.schema_query or "",
        "schema": schema_columns,
        "metadata": metadata,
    }
    _attach_common_session_sections(schema_export, session)
    if llm_configuration and any(llm_configuration.values()):
        schema_export["llm_configuration"] = llm_configuration
    return schema_export


def export_upload_schema_json(
    session: VisualizationSession,
    session_id: str,
) -> StreamingResponse:
    """Export only the schema metadata for an uploaded/imported session."""
    payload = _build_schema_only_payload(
        session,
        session_id,
        include_display_name=False,
        include_discovery_completed=False,
    )
    content = json.dumps(payload, indent=2, ensure_ascii=False)
    safe_name = _sanitize_source_name(session.metadata.source, "schema")
    return _json_response(content, f"{safe_name}_schema_{session_id[:8]}.json")


def export_schematiq_schema_json(
    session: VisualizationSession,
    session_id: str,
    tz_offset: int = 0,
) -> StreamingResponse:
    """Export only the schema metadata for a ScheMatiQ session."""
    llm_configuration = _load_schematiq_llm_configuration(session_id)
    payload = _build_schema_only_payload(
        session,
        session_id,
        include_display_name=True,
        include_discovery_completed=True,
        llm_configuration=llm_configuration,
    )
    content = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    timestamp = _user_timestamp(tz_offset)
    return _json_response(content, f"ScheMatiQ_{timestamp}_schema.json")
