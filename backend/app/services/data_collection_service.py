"""Research data collection service.

After a QBSD session completes (creation, reextraction, or continue discovery),
bundles all session data into a ZIP and uploads it to Google Drive.
Optionally logs a summary row to a Google Sheet.

This runs asynchronously in the background — zero impact on user latency.
Enabled only in release mode with valid Google credentials configured.
"""

import asyncio
import csv
import io
import json
import logging
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from app.core.config import DATA_COLLECTION_ENABLED, DEVELOPER_MODE, MAX_DOCUMENTS
from app.utils.csv_helpers import format_excerpt_for_csv

logger = logging.getLogger(__name__)


class DataCollectionService:
    """Archives QBSD session data to Google Drive for research."""

    def __init__(self, session_manager, uploader=None, sheets_logger=None):
        self._session_manager = session_manager
        self._uploader = uploader
        self._sheets_logger = sheets_logger
        # Accessed only from the asyncio event loop (trigger_archive + done callbacks),
        # so no additional synchronization is needed.
        self._active_tasks: Set[asyncio.Task] = set()

    @property
    def is_enabled(self) -> bool:
        return DATA_COLLECTION_ENABLED and self._uploader is not None

    async def trigger_archive(self, session_id: str, trigger_source: str) -> None:
        """Fire-and-forget entry point. Never raises."""
        if not self.is_enabled:
            return
        try:
            task = asyncio.create_task(self._archive_in_background(session_id, trigger_source))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
        except Exception as e:
            logger.error("[data-collection] Failed to spawn archive task: %s", e)

    async def _archive_in_background(self, session_id: str, trigger_source: str) -> None:
        """Run the blocking build+upload on the shared thread pool."""
        try:
            from app.services import qbsd_thread_pool
            import functools

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                qbsd_thread_pool,
                functools.partial(self._build_and_upload, session_id, trigger_source),
            )
        except Exception as e:
            logger.error("[data-collection] Archive failed for %s: %s", session_id[:8], e)

    # ── Sync (runs in thread pool) ─────────────────────────────────

    def _build_and_upload(self, session_id: str, trigger_source: str) -> None:
        """Build ZIP in memory and upload to Google Drive."""
        session = self._session_manager.get_session(session_id)
        if not session:
            logger.warning("[data-collection] Session %s not found — skipping", session_id[:8])
            return

        # Gather components
        query = session.schema_query or ""
        schema_json = self._build_schema_json(session)
        metadata_json = self._build_metadata(session, trigger_source)
        data_bytes = self._read_data_file(session_id)
        documents = self._gather_documents(session_id)
        config_json = self._read_and_sanitize_config(session_id)
        export_csv = self._build_export_csv(session, session_id, config_json)

        # Build ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("metadata.json", json.dumps(metadata_json, indent=2, default=str))
            zf.writestr("schema.json", json.dumps(schema_json, indent=2, default=str))
            if data_bytes:
                zf.writestr("data.jsonl", data_bytes)
            if export_csv:
                zf.writestr("export.csv", export_csv)
            if config_json:
                zf.writestr("config.json", json.dumps(config_json, indent=2, default=str))
            for doc_name, doc_content in documents:
                zf.writestr(f"documents/{doc_name}", doc_content)

        zip_bytes = zip_buffer.getvalue()
        filename = self._make_filename(session_id, query)
        logger.info(
            "[data-collection] Built archive %s (%.1f KB, %d docs)",
            filename, len(zip_bytes) / 1024, len(documents),
        )

        # Upload to Drive
        file_id = self._uploader.upload_file(filename, zip_bytes)

        # Log to Google Sheet
        if self._sheets_logger:
            stats = session.statistics
            obs_unit = ""
            if session.observation_unit:
                obs_unit = session.observation_unit.name or ""

            # Read LLM call count from session stats file
            llm_calls = 0
            try:
                llm_stats_file = Path("./qbsd_work") / session_id / "llm_call_stats.json"
                if llm_stats_file.exists():
                    llm_stats = json.loads(llm_stats_file.read_text(encoding="utf-8"))
                    llm_calls = llm_stats.get("total_calls", 0)
            except Exception as e:
                logger.debug("[data-collection] Could not read LLM call stats: %s", e)

            self._sheets_logger.log_session(
                session_id=session_id,
                query=query,
                doc_count=stats.total_documents if stats else 0,
                column_count=stats.total_columns if stats else len(session.columns),
                row_count=stats.total_rows if stats else 0,
                completeness=stats.completeness if stats else 0.0,
                observation_unit=obs_unit,
                trigger_source=trigger_source,
                drive_file_id=file_id,
                llm_calls=llm_calls,
            )

    # ── Helpers ─────────────────────────────────────────────────────

    def _build_schema_json(self, session) -> Dict[str, Any]:
        """Build schema representation for archival."""
        columns = []
        for col in session.columns:
            columns.append({
                "name": col.name,
                "definition": col.definition,
                "rationale": col.rationale,
                "allowed_values": col.allowed_values,
            })

        result: Dict[str, Any] = {"columns": columns, "query": session.schema_query}

        if session.observation_unit:
            result["observation_unit"] = {
                "name": session.observation_unit.name,
                "definition": session.observation_unit.definition,
            }
            if session.observation_unit.example_names:
                result["observation_unit"]["example_names"] = session.observation_unit.example_names

        if session.statistics and session.statistics.schema_evolution:
            evo = session.statistics.schema_evolution
            result["schema_evolution"] = {
                "snapshots": [
                    s.model_dump() if hasattr(s, "model_dump") else s
                    for s in (evo.snapshots or [])
                ],
                "column_sources": evo.column_sources or {},
            }

        return result

    def _build_metadata(self, session, trigger_source: str) -> Dict[str, Any]:
        """Build metadata.json content."""
        stats = session.statistics
        return {
            "session_id": session.id,
            "query": session.schema_query,
            "trigger_source": trigger_source,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "session_type": session.type.value if hasattr(session.type, "value") else str(session.type),
            "session_status": session.status.value if hasattr(session.status, "value") else str(session.status),
            "observation_unit": (
                session.observation_unit.name if session.observation_unit else None
            ),
            "stats": {
                "total_rows": stats.total_rows if stats else 0,
                "total_columns": stats.total_columns if stats else len(session.columns),
                "total_documents": stats.total_documents if stats else 0,
                "completeness": stats.completeness if stats else 0.0,
            },
        }

    def _read_data_file(self, session_id: str) -> Optional[str]:
        """Read the primary data file (extracted_data.jsonl or data.jsonl)."""
        from app.services import find_session_data_file

        data_path = find_session_data_file(session_id)
        if data_path and data_path.exists():
            try:
                return data_path.read_text(encoding="utf-8")
            except Exception as e:
                logger.warning("[data-collection] Could not read data file: %s", e)
        return None

    def _gather_documents(self, session_id: str):
        """Collect uploaded documents as (name, bytes) pairs.

        PDFs are converted to text to reduce archive size.
        Limited to MAX_DOCUMENTS files.
        """
        from app.services.pdf_utils import extract_text_from_pdf

        seen_names: set = set()
        results = []

        candidate_dirs = [
            Path("./data") / session_id / "documents",
            Path("./data") / session_id / "pending_documents",
        ]
        # Supabase datasets: qbsd_work/{id}/datasets/{dataset_name}/
        datasets_root = Path("./qbsd_work") / session_id / "datasets"
        if datasets_root.exists():
            for sub in sorted(datasets_root.iterdir()):
                if sub.is_dir():
                    candidate_dirs.append(sub)

        for docs_dir in candidate_dirs:
            if not docs_dir.exists():
                continue
            for f in sorted(docs_dir.iterdir()):
                if len(results) >= MAX_DOCUMENTS:
                    break
                if f.is_file() and not f.name.startswith(".") and f.name not in seen_names:
                    try:
                        if f.suffix.lower() == ".pdf":
                            text = extract_text_from_pdf(f)
                            txt_name = f.stem + ".txt"
                            results.append((txt_name, text.encode("utf-8")))
                        else:
                            results.append((f.name, f.read_bytes()))
                        seen_names.add(f.name)
                    except Exception as e:
                        logger.warning("[data-collection] Could not read document %s: %s", f.name, e)
            if len(results) >= MAX_DOCUMENTS:
                break

        return results

    def _build_export_csv(self, session, session_id: str, config: Optional[Dict[str, Any]]) -> Optional[str]:
        """Build a CSV export matching the /export endpoint format."""
        from app.services import find_session_data_file

        data_path = find_session_data_file(session_id)
        if not data_path or not data_path.exists():
            return None

        # Parse all rows from JSONL
        rows: List[Dict[str, Any]] = []
        try:
            for line in data_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    row_data = json.loads(line.strip())
                    # Normalize _row_name format to standard format
                    if "_row_name" in row_data:
                        rows.append({
                            "row_name": row_data.get("_row_name"),
                            "papers": row_data.get("_papers", []),
                            "unit_name": row_data.get("_unit_name"),
                            "source_document": row_data.get("_source_document"),
                            "data": {k: v for k, v in row_data.items() if not k.startswith("_")},
                        })
                    else:
                        rows.append(row_data)
        except Exception as e:
            logger.warning("[data-collection] Could not parse data for CSV export: %s", e)
            return None

        if not rows:
            return None

        output = io.StringIO()

        # Write metadata header comments (matches /export endpoint)
        output.write("# QBSD Export with Schema Metadata\n")
        output.write(f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        output.write(f"# Session ID: {session_id}\n")
        output.write(f"# Query: {session.schema_query or 'N/A'}\n")
        if session.observation_unit:
            output.write(f"# Observation Unit: {session.observation_unit.name} - {session.observation_unit.definition}\n")
        if config:
            sc = config.get("schema_creation_backend", {})
            ve = config.get("value_extraction_backend", {})
            if sc:
                output.write(f"# Schema Creation: {sc.get('provider', '?')} {sc.get('model', '?')}\n")
            if ve:
                output.write(f"# Value Extraction: {ve.get('provider', '?')} {ve.get('model', '?')}\n")
        output.write("#\n")
        output.write("# Column Definitions:\n")
        for col in session.columns:
            if col.name and not col.name.lower().endswith("_excerpt"):
                output.write(f"# {col.name}: {col.definition or 'No definition'}\n")
                if col.allowed_values:
                    output.write(f"#   Allowed Values: {', '.join(col.allowed_values)}\n")
        output.write("#\n")

        # Determine all column names
        all_columns: set = set()
        for row in rows:
            if row.get("row_name"):
                all_columns.add("row_name")
            if row.get("papers"):
                all_columns.add("papers")
            if row.get("unit_name"):
                all_columns.add("_unit_name")
            if row.get("source_document"):
                all_columns.add("_source_document")
            for col_name, value in row.get("data", {}).items():
                all_columns.add(col_name)
                if isinstance(value, dict) and "excerpts" in value:
                    all_columns.add(f"{col_name}_excerpt")

        column_names = sorted(all_columns)
        writer = csv.DictWriter(output, fieldnames=column_names)
        writer.writeheader()

        for row in rows:
            csv_row: Dict[str, Any] = {}
            if row.get("row_name"):
                csv_row["row_name"] = row["row_name"]
            if row.get("papers"):
                papers = row["papers"]
                csv_row["papers"] = "; ".join(papers) if isinstance(papers, list) else str(papers)
            if row.get("unit_name"):
                csv_row["_unit_name"] = row["unit_name"]
            if row.get("source_document"):
                csv_row["_source_document"] = row["source_document"]

            for col_name, value in row.get("data", {}).items():
                if isinstance(value, dict) and "answer" in value:
                    csv_row[col_name] = value["answer"]
                    if "excerpts" in value and value["excerpts"]:
                        excerpts = value["excerpts"]
                        if isinstance(excerpts, list):
                            csv_row[f"{col_name}_excerpt"] = " | ".join(format_excerpt_for_csv(ex) for ex in excerpts)
                        else:
                            csv_row[f"{col_name}_excerpt"] = str(excerpts)
                elif isinstance(value, (list, dict)):
                    csv_row[col_name] = str(value)
                else:
                    csv_row[col_name] = value

            writer.writerow(csv_row)

        return output.getvalue()

    def _read_and_sanitize_config(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Read QBSD config, strip secrets."""
        config_path = Path("./qbsd_work") / session_id / "qbsd_config.json"
        if not config_path.exists():
            return None
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
            return self._sanitize_config(config)
        except Exception as e:
            logger.warning("[data-collection] Could not read config: %s", e)
            return None

    # Prefixes that indicate an API key value
    _SECRET_VALUE_PREFIXES = ("sk-", "key_", "AIza", "ya29.", "bearer ", "token ")

    @staticmethod
    def _sanitize_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively strip keys that look like secrets and values that look like API keys."""
        secret_key_re = re.compile(r"(api_key|secret|token|password|credential)\b", re.IGNORECASE)
        sanitized = {}
        for key, value in config.items():
            if secret_key_re.search(key):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, dict):
                sanitized[key] = DataCollectionService._sanitize_config(value)
            elif isinstance(value, str) and any(
                value.startswith(p) for p in DataCollectionService._SECRET_VALUE_PREFIXES
            ):
                sanitized[key] = "***REDACTED***"
            else:
                sanitized[key] = value
        return sanitized

    @staticmethod
    def _make_filename(session_id: str, query: str) -> str:
        """Generate archive filename: YYYYMMDD_query_slug_session_id.zip"""
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        # Slugify query: lowercase, replace non-alnum with underscore, truncate
        slug = re.sub(r"[^a-z0-9]+", "_", query.lower()).strip("_")[:40]
        short_id = session_id[:8]
        return f"{date_str}_{slug}_{short_id}.zip"
