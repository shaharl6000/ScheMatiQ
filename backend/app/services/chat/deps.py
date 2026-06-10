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


def truncate_result(value: Any, max_chars: int = 4000) -> Any:
    text = json.dumps(value, ensure_ascii=False, default=str)
    if len(text) <= max_chars:
        return value
    return {"truncated": True, "preview": text[:max_chars]}
