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
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from app.core.config import LLM_CALL_GLOBAL_LIMIT
from schematiq.core.llm_call_tracker import QuotaExceededError

logger = logging.getLogger(__name__)

# If the whole reference fits this many chars it is injected verbatim (no retrieval)
# — sized to the fill model's context window with margin. Default ~2M chars ≈ a few
# hundred K tokens, safe for a ~1M-token model (Gemini Flash / Flash-Lite, both 1M).
# Raise it (and point REFERENCE_FILL_MODEL at a larger-window model) to inject bigger
# references whole.
REFERENCE_CONTEXT_BUDGET_CHARS = int(
    os.getenv("REFERENCE_CONTEXT_BUDGET_CHARS", "2000000")
)
# When the reference is larger than the window we retrieve per row. This is how much
# retrieved content (top-ranked rows, header de-duplicated) to hand the model each
# time. Far more than a fixed handful — enough to surface the right row even when the
# join key is a common value whose target ranks deep — but bounded so we do not flood
# the model (and inflate latency) with the whole file. Raise it to trade cost/latency
# for recall on very large references.
REFERENCE_RAG_BUDGET_CHARS = int(
    os.getenv("REFERENCE_RAG_BUDGET_CHARS", "400000")
)
# Upper bound on chunks pulled from the retriever; the char budget above is the real
# limit, this is just a ceiling so we never rank-sort an unbounded list.
REFERENCE_RAG_MAX_CHUNKS = int(os.getenv("REFERENCE_RAG_MAX_CHUNKS", "4000"))
_NO_VALUE_SENTINELS = {"", "N/A", "NA", "NONE", "NULL", "UNKNOWN"}
_MAX_PAGE_ITERATIONS = 200  # safety valve for the row pager


def _retrieval_diagnostic(unit: str, context: str) -> str:
    """Answer the debugging question 'did retrieval surface this unit's row?'.

    The 300-char context preview is mostly the replicated table header, so it can't
    reveal whether the unit's own row made it into the context. Instead report how
    many of the unit's name tokens appear in the context and show a window around
    the strongest match (or the head of the context when nothing matched). If the
    tokens are missing, the model answering N/A is a retrieval miss, not a model
    miss.
    """
    ctx_lower = context.lower()
    tokens = [t for t in re.findall(r"[A-Za-z0-9]+", unit) if len(t) > 2]
    present = [t for t in tokens if t.lower() in ctx_lower]
    if present:
        anchor = max(present, key=len)
        pos = ctx_lower.find(anchor.lower())
        window = context[max(0, pos - 60):pos + 100]
    else:
        window = context[:160]
    window = window.replace("\n", " ⏎ ")
    return f"unit tokens in context: {len(present)}/{len(tokens)} {present} | window: {window}"


def _join_reference_passages(passages: list, char_budget: int) -> str:
    """Join retrieved chunks into a context that fills the char budget, keeping a
    shared table header only once.

    Tabular chunks each begin with the same replicated header line, so a naive join
    repeats that (often large) header once per chunk and burns most of the budget on
    duplication. When every passage shares the same first line we emit it once, then
    append row bodies (highest-ranked first) until the budget is reached. For prose
    (no shared header) we join and stop at the budget the same way. This lets us hand
    the model as many relevant rows as the window allows instead of a fixed few.
    """
    if not passages:
        return ""
    header = passages[0].split("\n", 1)[0]
    tabular = bool(header) and all(p.startswith(header) for p in passages)
    prefix = (header + "\n") if tabular else ""
    sep = "\n" if tabular else "\n\n"
    parts: list = []
    used = len(prefix)
    for passage in passages:
        body = passage[len(header):].lstrip("\n") if tabular else passage
        if not body:
            continue
        if parts and used + len(body) + len(sep) > char_budget:
            break
        parts.append(body)
        used += len(body) + len(sep)
    return prefix + sep.join(parts)


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

        column_obj = next(
            (c for c in session.columns if c.name == column), None
        )
        column_definition = column_obj.definition if column_obj else None
        allowed_values = column_obj.allowed_values if column_obj else None
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
            self._run_fill(op, rows, reference_text, column_definition, allowed_values)
        )
        self._tasks[fill_id] = task

        logger.info(
            "reference fill started: id=%s column=%r reference=%r rows=%d",
            fill_id, column, ref.filename, len(rows),
        )
        message = (
            f"Filling '{column}' for {len(rows)} rows from '{ref.filename}' in the "
            "background. Cells will appear as each row completes."
        )
        if len(reference_text) > REFERENCE_CONTEXT_BUDGET_CHARS:
            # The reference does not fit the model context window, so we retrieve the
            # most relevant rows per cell instead of reading all of it. Say so, and
            # suggest narrowing — a smaller, focused reference is read in full.
            message += (
                f" Note: this reference is large ({len(reference_text):,} characters) "
                "and does not fit the model's context window, so I'm retrieving the "
                "most relevant rows for each cell rather than reading all of it. If a "
                "value comes back empty, narrowing the reference to the columns/rows "
                "you need can improve accuracy."
            )
        return {
            "status": "started",
            "fill_id": fill_id,
            "column": column,
            "reference": ref.filename,
            "total": len(rows),
            "mode": "retrieval" if len(reference_text) > REFERENCE_CONTEXT_BUDGET_CHARS else "full",
            "message": message,
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
        allowed_values: Optional[list] = None,
    ) -> None:
        from schematiq.value_extraction.utils.reference_retrieval import (
            ReferenceRetriever,
            build_reference_query,
        )

        retriever = (
            ReferenceRetriever(reference_text)
            if len(reference_text) > REFERENCE_CONTEXT_BUDGET_CHARS
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
                    passages = retriever.retrieve(
                        query, k=REFERENCE_RAG_MAX_CHUNKS, rel_threshold=0.0
                    )
                    context = _join_reference_passages(
                        passages, REFERENCE_RAG_BUDGET_CHARS
                    )
                else:
                    context = reference_text

                # Diagnostic logging for the examination phase: what RAG returned
                # (whether the unit's own row is actually present) and, below, what
                # the model answered — so a wrong or empty value can be traced to
                # retrieval (row missing) vs. the model (row present but misread).
                logger.info(
                    "reference fill %s row %d/%d unit=%r | retrieved %d chars from %s | %s",
                    op.fill_id, index + 1, op.total, unit, len(context),
                    "RAG" if retriever is not None else "full reference",
                    _retrieval_diagnostic(unit, context),
                )
                try:
                    value = await self._extract_value_for_row(
                        client, unit, op.column, column_definition, context,
                        allowed_values,
                    )
                except Exception as exc:  # a single row's model error must not abort the run
                    logger.warning("reference fill row %r failed: %s", unit, exc)
                    op.skipped += 1
                    continue
                op.calls += 1
                logger.info(
                    "reference fill %s row %d/%d unit=%r -> model answered %r",
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
        allowed_values: Optional[list] = None,
    ) -> str:
        from app.core.config import REFERENCE_FILL_MODEL

        definition_line = f" (defined as: {column_definition})" if column_definition else ""
        # Mirror the canonical value-extraction prompt: when the column is categorical
        # the answer must be snapped to its closed set, not free-form (otherwise the
        # model returns near-misses like "Republican" for an "Other Republican"
        # category, or invents labels).
        allowed_line = f"\nallowed_values: {allowed_values}" if allowed_values else ""
        allowed_instruction = (
            " The value MUST be exactly one of allowed_values, copied verbatim — do not "
            "invent, rephrase, merge, or abbreviate a category."
            if allowed_values
            else ""
        )
        prompt = (
            "You are filling a single cell in a table using an external reference "
            "document.\n\n"
            f"Row (observation unit): {unit}\n"
            f"Column to fill: {column}{definition_line}{allowed_line}\n\n"
            "Reference excerpts:\n"
            f"{context}\n\n"
            f"Return ONLY the value of '{column}' for {unit}, with no extra words or "
            f"punctuation.{allowed_instruction} If the reference does not contain it, "
            "return exactly: N/A"
        )
        from google.genai import types

        # Deterministic decoding: this is a narrow extraction, not open generation.
        # At the default (sampled) temperature the model intermittently bails to N/A
        # for a row whose data is right there in the context, so the same unit gets a
        # value in one table row and a blank in another. temperature=0 makes the
        # answer reproducible and removes that flakiness.
        response = await client.aio.models.generate_content(
            model=REFERENCE_FILL_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0),
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
