"""Grounded web enrichment for columns that explicitly opt into web access."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

from app.models.session import ColumnInfo
from app.services.atomic_jsonl import write_jsonl_atomic
from app.services.data_utils import (
    _resolve_source_document,
    extract_papers,
    persist_session_data_file,
    resolve_session_data_files,
    row_name_of,
)
from app.services.session_manager import SessionManager
from schematiq.value_extraction.config.prompts import (
    SYSTEM_PROMPT_WEB_VALUE,
    USER_PROMPT_WEB_VALUE,
)
from schematiq.value_extraction.core.json_parser import JSONResponseParser

logger = logging.getLogger(__name__)

CellCallback = Callable[[str, str, Any], Optional[Awaitable[None]]]

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_IDENTIFYING_KEY_PARTS = (
    "court",
    "case",
    "docket",
    "jurisdiction",
    "document",
    "citation",
)
_CACHE_KEY_PARTS = ("court", "jurisdiction")


def _cell_container(row: Dict[str, Any]) -> Dict[str, Any]:
    data = row.get("data")
    return data if isinstance(data, dict) else row


def _answer(value: Any) -> Any:
    if isinstance(value, dict) and "answer" in value:
        return value.get("answer")
    return value


def _is_empty(value: Any) -> bool:
    value = _answer(value)
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _text(value: Any) -> str:
    value = _answer(value)
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()
    return ""


def _matches_scope(
    row: Dict[str, Any],
    documents: Optional[Iterable[str]],
    rows: Optional[Iterable[str]],
) -> bool:
    if rows is not None and row_name_of(row) not in set(rows):
        return False
    if documents is None:
        return True
    wanted = {Path(str(value)).stem.lower() for value in documents}
    source = Path(_resolve_source_document(row) or "").stem.lower()
    papers = {
        Path(str(value)).stem.lower()
        for value in extract_papers(row)
    }
    return source in wanted or bool(papers & wanted)


def _identifying_context(row: Dict[str, Any], excluded: Iterable[str]) -> str:
    container = _cell_container(row)
    excluded_names = {name.lower() for name in excluded}
    parts: List[str] = []

    source = _resolve_source_document(row)
    if source:
        parts.append(f"source_document={source}")

    for key, value in container.items():
        key_lower = str(key).lower()
        if key_lower in excluded_names or key_lower.startswith("_"):
            continue
        if not any(part in key_lower for part in _IDENTIFYING_KEY_PARTS):
            continue
        rendered = _text(value)
        if rendered:
            parts.append(f"{key}={rendered[:240]}")
        if len(parts) >= 6:
            break
    return "; ".join(parts)[:1000] or "No additional identifying context available"


def _entity_cache_context(row: Dict[str, Any]) -> str:
    """Stable disambiguator that still caches one entity across many cases."""
    container = _cell_container(row)
    parts = []
    for key, value in container.items():
        key_lower = str(key).lower()
        if any(part in key_lower for part in _CACHE_KEY_PARTS):
            rendered = _text(value)
            if rendered:
                parts.append(f"{key_lower}={rendered.lower()}")
    if parts:
        return "|".join(sorted(parts))

    # Without a stable court/jurisdiction identifier, prefer correctness over
    # cross-document cache hits: a namesake in another case must not reuse this
    # entity's answer.
    source = _resolve_source_document(row)
    if source:
        parts.append(f"source={Path(source).stem.lower()}")
    for key, value in container.items():
        key_lower = str(key).lower()
        if "case" in key_lower or "docket" in key_lower:
            rendered = _text(value)
            if rendered:
                parts.append(f"{key_lower}={rendered.lower()}")
    return "|".join(sorted(parts))


def _parse_grounded_value(text: str) -> Any:
    candidate = text.strip()
    match = _JSON_FENCE_RE.search(candidate)
    if match:
        candidate = match.group(1).strip()
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            parsed = json.loads(candidate[start : end + 1])
        except json.JSONDecodeError:
            return None
    if not isinstance(parsed, dict):
        return None
    return parsed.get("value")


def _source_excerpts(sources: List[Dict[str, str]]) -> List[Dict[str, str]]:
    excerpts = []
    for source in sources[:5]:
        url = str(source.get("url") or "").strip()
        if not url:
            continue
        title = str(source.get("title") or "Web source").strip()
        excerpts.append({"text": f"{title}: {url}", "source": title})
    return excerpts


class WebEnrichmentService:
    """Resolve opted-in cells with Gemini Google Search grounding."""

    def __init__(self, session_manager: SessionManager):
        self._session_manager = session_manager
        self._parser = JSONResponseParser()

    async def enrich_columns(
        self,
        session_id: str,
        columns: List[ColumnInfo],
        llm: Any,
        *,
        documents: Optional[List[str]] = None,
        rows: Optional[List[str]] = None,
        only_empty: bool = False,
        should_stop: Optional[Callable[[], bool]] = None,
        on_cell: Optional[CellCallback] = None,
    ) -> Dict[str, int]:
        """Enrich selected rows in-place and persist every changed data file.

        ``document_then_web`` always fills only gaps left by document extraction.
        ``web`` overwrites on a normal re-extraction and respects ``only_empty``
        for targeted gap-fill requests.
        """
        data_files = await resolve_session_data_files(session_id)
        if not data_files:
            raise ValueError("No table rows are available for web enrichment.")

        cache: Dict[Tuple[str, str, str], Optional[Dict[str, Any]]] = {}
        stats = {"lookups": 0, "cache_hits": 0, "updated_cells": 0}
        requested_names = [column.name for column in columns]
        row_scope = set(rows) if rows is not None else None

        for data_file in data_files:
            file_rows = self._load_rows(data_file)
            changed = False
            for row in file_rows:
                if should_stop and should_stop():
                    if changed:
                        write_jsonl_atomic(data_file, file_rows, ensure_ascii=False)
                        await persist_session_data_file(session_id, data_file)
                    return stats
                if not _matches_scope(row, documents, row_scope):
                    continue
                entity_name = row_name_of(row)
                if not entity_name:
                    continue

                container = _cell_container(row)
                context = _identifying_context(row, requested_names)
                cache_context = _entity_cache_context(row)
                for column in columns:
                    existing = container.get(column.name)
                    fill_gaps_only = (
                        only_empty
                        or column.extraction_strategy == "document_then_web"
                    )
                    if fill_gaps_only and not _is_empty(existing):
                        continue

                    cache_key = (
                        column.name,
                        entity_name.strip().lower(),
                        cache_context,
                    )
                    if cache_key in cache:
                        cell = cache[cache_key]
                        stats["cache_hits"] += 1
                    else:
                        stats["lookups"] += 1
                        cell = await self._resolve_cell(
                            llm, entity_name, context, column
                        )
                        cache[cache_key] = cell

                    # A failed or ungrounded lookup never destroys an existing value.
                    if cell is None:
                        continue
                    container[column.name] = dict(cell)
                    status = row.get("_cell_status")
                    if not isinstance(status, dict):
                        status = {}
                        row["_cell_status"] = status
                    status[column.name] = "external_source"
                    changed = True
                    stats["updated_cells"] += 1

                    if on_cell:
                        result = on_cell(entity_name, column.name, dict(cell))
                        if inspect.isawaitable(result):
                            await result

            if changed:
                write_jsonl_atomic(data_file, file_rows, ensure_ascii=False)
                await persist_session_data_file(session_id, data_file)

        logger.info(
            "[web-enrichment] session=%s lookups=%d cache_hits=%d updated_cells=%d",
            session_id[:8],
            stats["lookups"],
            stats["cache_hits"],
            stats["updated_cells"],
        )
        return stats

    async def _resolve_cell(
        self,
        llm: Any,
        entity_name: str,
        context: str,
        column: ColumnInfo,
    ) -> Optional[Dict[str, Any]]:
        allowed_values = (
            json.dumps(column.allowed_values, ensure_ascii=False)
            if column.allowed_values
            else "none"
        )
        user_prompt = USER_PROMPT_WEB_VALUE.format(
            entity_name=entity_name,
            entity_context=context,
            column_name=column.name,
            column_definition=column.definition or f"Data field: {column.name}",
            column_rationale=column.rationale or "none",
            allowed_values=allowed_values,
        )
        text, sources = await asyncio.to_thread(
            llm.generate_grounded,
            [
                {"role": "system", "content": SYSTEM_PROMPT_WEB_VALUE},
                {"role": "user", "content": user_prompt},
            ],
            max_output_tokens=256,
            # Do not pass response_schema here: Gemini 3.1 Flash-Lite supports
            # each feature separately but currently rejects structured output
            # combined with built-in tools. The strict JSON prompt is parsed below.
        )
        excerpts = _source_excerpts(sources)
        if not excerpts:
            return None

        value = _parse_grounded_value(text)
        if value is None or isinstance(value, (dict, list)):
            return None
        parsed = {
            column.name: {
                "answer": str(value).strip(),
                "excerpts": [],
            }
        }
        normalized, _unmatched = self._parser.postprocess(
            parsed,
            [column.name],
            {column.name: column.allowed_values or []},
        )
        cell = normalized.get(column.name)
        if not cell or _is_empty(cell):
            return None
        cell["excerpts"] = excerpts
        return cell

    @staticmethod
    def _load_rows(path: Path) -> List[Dict[str, Any]]:
        rows = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        return rows
