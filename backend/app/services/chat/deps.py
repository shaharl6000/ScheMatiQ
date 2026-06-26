"""Shared dependencies for chat tool execution."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app.core.config import RELEASE_CONFIG
from app.services import (
    session_manager,
    websocket_manager,
)
from app.services.continue_discovery_service import ContinueDiscoveryService
from app.services.data_editor import DataEditor
from app.services.observation_unit_manager import ObservationUnitManager
from app.services.reextraction_service import ReextractionService
from app.services.schema_manager import SchemaManager
from app.services.schematiq_runner import ScheMatiQRunner
from app.services import pubmed_enrichment_service, uniprot_enrichment_service

from app.services import data_collection_service

schema_manager = SchemaManager(websocket_manager, session_manager)
reextraction_service = ReextractionService(
    websocket_manager,
    session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)
continue_discovery_service = ContinueDiscoveryService(
    websocket_manager,
    session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)
observation_unit_manager = ObservationUnitManager(websocket_manager, session_manager)
data_editor = DataEditor()
schematiq_runner = ScheMatiQRunner(
    websocket_manager=websocket_manager,
    session_manager=session_manager,
    data_collection_service=data_collection_service,
    pubmed_enrichment_service=pubmed_enrichment_service,
    uniprot_enrichment_service=uniprot_enrichment_service,
)

CHAT_MODEL = "gemini-3.1-flash-lite"
WORK_DIR = Path("./schematiq_work")


def get_default_llm_config() -> dict[str, Any]:
    return {
        "provider": RELEASE_CONFIG["llm_provider"],
        "model": RELEASE_CONFIG["value_extraction_model"],
        "temperature": RELEASE_CONFIG["llm_temperature"],
    }


def load_user_llm_config(session_id: str) -> dict[str, Any]:
    user_config_file = Path("./data") / session_id / "user_llm_config.json"
    if user_config_file.exists():
        with open(user_config_file, encoding="utf-8") as handle:
            return json.load(handle)
    return get_default_llm_config()


def get_gemini_api_key() -> str:
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        raise ValueError("GEMINI_API_KEY is not configured on the server.")
    return key


def truncate_result(value: Any, max_chars: int = 8000) -> Any:
    text = json.dumps(value, ensure_ascii=False, default=str)
    if len(text) <= max_chars:
        return value

    def _size(item: Any) -> int:
        return len(json.dumps(item, ensure_ascii=False, default=str))

    # Preserve structure instead of collapsing the whole payload into an opaque
    # preview string. Callers (and the model) rely on specific keys — e.g.
    # get_schema's `column_names`/`column_count` drive the "(N columns)" summary
    # and tell the agent which columns exist. A blunt preview drops those keys
    # and reports 0 columns while feeding the model a JSON string cut mid-object.
    if isinstance(value, dict):
        # Small, high-signal keys are always kept verbatim first; the rest of the
        # budget is filled with heavier keys (lists are kept partially).
        summary_keys = (
            "status", "message", "error", "hint", "truncated",
            "column_names", "column_count", "total_count", "query",
            "observation_unit", "format", "operation", "reprocessing",
        )
        kept: dict[str, Any] = {}
        for key in summary_keys:
            if key in value:
                kept[key] = value[key]
        remaining = max(max_chars - _size(kept), 0)
        for key, item in value.items():
            if key in kept:
                continue
            chunk = _size(item)
            if chunk <= remaining:
                kept[key] = item
                remaining -= chunk
            elif isinstance(item, list):
                subset: list[Any] = []
                for entry in item:
                    entry_size = _size(entry)
                    if entry_size > remaining:
                        break
                    subset.append(entry)
                    remaining -= entry_size
                kept[key] = subset
                if len(subset) < len(item):
                    kept[f"{key}_omitted"] = len(item) - len(subset)
            # Oversized non-list values are dropped (key omitted) to stay in budget.
        kept["truncated"] = True
        return kept

    if isinstance(value, list):
        subset = []
        remaining = max_chars
        for entry in value:
            entry_size = _size(entry)
            if entry_size > remaining:
                break
            subset.append(entry)
            remaining -= entry_size
        return {"truncated": True, "items": subset, "omitted": len(value) - len(subset)}

    return {"truncated": True, "preview": text[:max_chars]}
