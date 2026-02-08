"""Research data collection service.

After a QBSD session completes (creation, reextraction, or continue discovery),
bundles all session data into a ZIP and uploads it to Google Drive.
Optionally logs a summary row to a Google Sheet.

This runs asynchronously in the background — zero impact on user latency.
Enabled only in release mode with valid Google credentials configured.
"""

import asyncio
import io
import json
import logging
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set

from app.core.config import DATA_COLLECTION_ENABLED, DEVELOPER_MODE

logger = logging.getLogger(__name__)


class DataCollectionService:
    """Archives QBSD session data to Google Drive for research."""

    def __init__(self, session_manager, uploader=None, sheets_logger=None):
        self._session_manager = session_manager
        self._uploader = uploader
        self._sheets_logger = sheets_logger
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

        # Build ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("metadata.json", json.dumps(metadata_json, indent=2, default=str))
            zf.writestr("schema.json", json.dumps(schema_json, indent=2, default=str))
            if data_bytes:
                zf.writestr("data.jsonl", data_bytes)
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
        """Collect uploaded documents as (name, bytes) pairs."""
        results = []
        docs_dir = Path("./data") / session_id / "documents"
        if not docs_dir.exists():
            return results

        for f in sorted(docs_dir.iterdir()):
            if f.is_file() and not f.name.startswith("."):
                try:
                    results.append((f.name, f.read_bytes()))
                except Exception as e:
                    logger.warning("[data-collection] Could not read document %s: %s", f.name, e)
        return results

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
        secret_key_re = re.compile(r"(api_key|secret|token|password|credential|key)", re.IGNORECASE)
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
