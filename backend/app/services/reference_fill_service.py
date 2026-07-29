"""Fill an existing column from an external reference document.

Runs one focused model call per observation-unit row (with per-row retrieval for
large references), writes each cell as it is produced, and streams progress over
the WebSocket so the table fills in live. The work runs as a background task so it
never blocks the request that started it (a whole-column fill can far exceed the
HTTP timeout).

The single model touchpoint is ``_extract_value_for_row`` so the surrounding loop,
retrieval, writes, streaming, quota accounting and cancellation can be tested
deterministically with a stubbed model.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from app.core.config import LLM_CALL_GLOBAL_LIMIT
from schematiq.core.llm_call_tracker import QuotaExceededError

logger = logging.getLogger(__name__)

# Reference text above this size is retrieved per row; below it, the whole
# reference is handed to each per-row model call.
REFERENCE_FULL_INJECT_MAX_CHARS = 12000
_NO_VALUE_SENTINELS = {"", "N/A", "NA", "NONE", "NULL", "UNKNOWN"}
_MAX_PAGE_ITERATIONS = 200  # safety valve for the row pager


@dataclass
class FillOperation:
    fill_id: str
    session_id: str
    column: str
    reference_filename: str
    total: int
    status: str = "running"  # running | completed | stopped | error
    filled: int = 0
    skipped: int = 0
    calls: int = 0
    message: str = ""


class ReferenceFillService:
    def __init__(self, websocket_manager, session_manager, data_editor, schematiq_runner):
        self._ws = websocket_manager
        self._sessions = session_manager
        self._data_editor = data_editor
        self._runner = schematiq_runner
        self._ops: dict[str, FillOperation] = {}
        self._stop: dict[str, bool] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._client: Any = None
        self._lock = threading.Lock()

    # ---- public API -------------------------------------------------------

    async def start_fill(
        self, session_id: str, column: str, reference_id: str
    ) -> dict[str, Any]:
        """Validate the request, then start the fill in the background.

        Returns immediately with the operation id and the number of rows to fill.
        """
        column = (column or "").strip()
        reference_id = (reference_id or "").strip()
        if not column or not reference_id:
            raise ValueError("Both 'column' and 'reference_id' are required.")

        session = self._sessions.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        if not any(c.name == column for c in session.columns):
            raise ValueError(f"Column '{column}' does not exist")

        from app.services import reference_document_service as refsvc

        ref = refsvc.get_reference_document(session, reference_id)
        if not ref:
            raise ValueError(f"Reference document '{reference_id}' not found")

        reference_text = await refsvc.load_reference_text(session_id, ref)
        if not reference_text.strip():
            raise ValueError(f"Reference '{ref.filename}' is empty.")

        rows = await self._load_all_rows(session_id)
        if not rows:
            raise ValueError("No rows to fill.")

        column_definition = next(
            (c.definition for c in session.columns if c.name == column), None
        )
        fill_id = f"fill-{session_id[:8]}-{uuid.uuid4().hex[:8]}"
        op = FillOperation(
            fill_id=fill_id,
            session_id=session_id,
            column=column,
            reference_filename=ref.filename,
            total=len(rows),
        )
        with self._lock:
            # One fill at a time per session: concurrent fills would both
            # read-modify-write the same data file and lose each other's updates.
            for existing in self._ops.values():
                if existing.session_id == session_id and existing.status == "running":
                    raise ValueError(
                        "A column fill is already running for this session; "
                        "wait for it to finish before starting another."
                    )
            self._ops[fill_id] = op
            self._stop[fill_id] = False

        task = asyncio.create_task(
            self._run_fill(op, rows, reference_text, column_definition)
        )
        self._tasks[fill_id] = task

        logger.info(
            "reference fill started: id=%s column=%r reference=%r rows=%d",
            fill_id, column, ref.filename, len(rows),
        )
        return {
            "status": "started",
            "fill_id": fill_id,
            "column": column,
            "reference": ref.filename,
            "total": len(rows),
            "message": (
                f"Filling '{column}' for {len(rows)} rows from '{ref.filename}' in the "
                "background. Cells will appear as each row completes."
            ),
        }

    def request_stop(self, fill_id: str) -> dict[str, Any]:
        with self._lock:
            if fill_id not in self._ops:
                return {"accepted": False, "message": "Fill operation not found."}
            self._stop[fill_id] = True
        return {"accepted": True, "message": "Stop requested."}

    def get_status(self, fill_id: str) -> Optional[dict[str, Any]]:
        op = self._ops.get(fill_id)
        if not op:
            return None
        return {
            "fill_id": op.fill_id, "status": op.status, "filled": op.filled,
            "skipped": op.skipped, "total": op.total, "message": op.message,
        }

    # ---- background worker ------------------------------------------------

    async def _run_fill(
        self,
        op: FillOperation,
        rows: list[dict[str, Any]],
        reference_text: str,
        column_definition: Optional[str],
    ) -> None:
        from schematiq.value_extraction.utils.reference_retrieval import (
            ReferenceRetriever,
            build_reference_query,
        )

        retriever = (
            ReferenceRetriever(reference_text)
            if len(reference_text) > REFERENCE_FULL_INJECT_MAX_CHARS
            else None
        )
        try:
            client = self._get_client()
            for index, row in enumerate(rows):
                if self._stop.get(op.fill_id):
                    op.status = "stopped"
                    break
                unit = row.get("unit_name") or row.get("row_name")
                if not unit:
                    op.skipped += 1
                    continue
                # Count each per-row model call toward the quota; stop cleanly when
                # the limit is reached rather than blowing far past it.
                try:
                    self._runner.check_global_quota(LLM_CALL_GLOBAL_LIMIT)
                except QuotaExceededError:
                    op.status = "stopped"
                    op.message = "Stopped early: the LLM quota was reached."
                    break

                if retriever is not None:
                    query = build_reference_query(
                        unit, [{"column": op.column, "definition": column_definition}]
                    )
                    passages = retriever.retrieve(query, k=5)
                    context = "\n\n".join(passages) if passages else ""
                else:
                    context = reference_text

                try:
                    value = await self._extract_value_for_row(
                        client, unit, op.column, column_definition, context
                    )
                except Exception as exc:  # a single row's model error must not abort the run
                    logger.warning("reference fill row %r failed: %s", unit, exc)
                    op.skipped += 1
                    continue
                op.calls += 1
                logger.info(
                    "reference fill %s row %d/%d unit=%r -> %r",
                    op.fill_id, index + 1, op.total, unit, value,
                )

                if value and value.strip().upper() not in _NO_VALUE_SENTINELS:
                    try:
                        await self._data_editor.update_cell(
                            op.session_id, unit, op.column, value.strip(),
                            source_document=row.get("source_document"),
                            reference_source=op.reference_filename,
                        )
                        await self._broadcast_cell(op.session_id, unit, op.column, value.strip())
                        op.filled += 1
                    except Exception as exc:  # a single row's write must not abort the run
                        logger.warning("reference fill write for %r failed: %s", unit, exc)
                        op.skipped += 1
                else:
                    op.skipped += 1

            if op.status == "running":
                op.status = "completed"
        except Exception as exc:  # unexpected failure of the whole run
            op.status = "error"
            op.message = op.message or f"Fill failed: {exc}"
            logger.exception("reference fill %s failed", op.fill_id)
        finally:
            if op.calls:
                try:
                    record_id = f"{op.fill_id}-{int(time.time())}"
                    await asyncio.to_thread(
                        self._runner.record_external_usage, record_id, {"chat": op.calls}
                    )
                except Exception as exc:
                    logger.debug("Could not record fill LLM usage: %s", exc)
            if not op.message:
                op.message = (
                    f"Filled {op.filled} of {op.total} rows in '{op.column}' "
                    f"from '{op.reference_filename}'."
                )
            await self._broadcast_complete(op)
            with self._lock:
                self._stop.pop(op.fill_id, None)
                self._tasks.pop(op.fill_id, None)

    # ---- helpers (isolated for testing) -----------------------------------

    async def _load_all_rows(self, session_id: str) -> list[dict[str, Any]]:
        from app.services.pipeline.data_query import get_data as query_get_data
        from app.services.chat.deps import WORK_DIR

        rows: list[dict[str, Any]] = []
        page = 0
        page_size = 200
        while True:
            data = await query_get_data(session_id, WORK_DIR, page=page, page_size=page_size)
            batch = [r.model_dump() for r in data.rows]
            rows.extend(batch)
            if len(batch) < page_size or page >= _MAX_PAGE_ITERATIONS:
                break
            page += 1
        return rows

    def _get_client(self) -> Any:
        if self._client is None:
            from google import genai
            from app.services.chat.deps import get_gemini_api_key

            self._client = genai.Client(api_key=get_gemini_api_key())
        return self._client

    async def _extract_value_for_row(
        self,
        client: Any,
        unit: str,
        column: str,
        column_definition: Optional[str],
        context: str,
    ) -> str:
        from app.core.config import REFERENCE_FILL_MODEL

        definition_line = f" (defined as: {column_definition})" if column_definition else ""
        prompt = (
            "You are filling a single cell in a table using an external reference "
            "document.\n\n"
            f"Row (observation unit): {unit}\n"
            f"Column to fill: {column}{definition_line}\n\n"
            "Reference excerpts:\n"
            f"{context}\n\n"
            f"Return ONLY the value of '{column}' for {unit}, with no extra words or "
            "punctuation. If the reference does not contain it, return exactly: N/A"
        )
        response = await client.aio.models.generate_content(
            model=REFERENCE_FILL_MODEL, contents=prompt
        )
        return (getattr(response, "text", None) or "").strip()

    async def _broadcast_cell(self, session_id: str, row_name: str, column: str, value: str) -> None:
        try:
            await self._ws.broadcast_to_session(
                session_id,
                {"type": "cell_extracted",
                 "data": {"row_name": row_name, "column": column, "value": value}},
            )
        except Exception:  # streaming is best-effort
            pass

    async def _broadcast_complete(self, op: FillOperation) -> None:
        try:
            await self._ws.broadcast_to_session(
                op.session_id,
                {
                    "type": "reference_fill_completed",
                    "data": {
                        "fill_id": op.fill_id, "status": op.status, "column": op.column,
                        "filled": op.filled, "skipped": op.skipped, "total": op.total,
                        "message": op.message,
                    },
                },
            )
        except Exception:
            pass
