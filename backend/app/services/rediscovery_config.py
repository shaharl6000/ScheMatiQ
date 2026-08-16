"""Resolve the LLM backends for a rediscovery run.

Both rediscovery entrypoints — the chat ``rediscover`` tool and
``POST /load/rediscover`` — build a ``ScheMatiQConfig`` for an imported session
before kicking off the pipeline. Historically both synthesized the backends from
``RELEASE_CONFIG`` defaults, which discards the project's ORIGINAL models. Once a
complete-export bundle is re-imported, the project's real backends are persisted
in ``schematiq_config.json`` (and in session metadata), so a rediscovery run
should prefer those and fall back to defaults only when nothing was persisted.

Release-mode enforcement still happens later in the pipeline
(``enforce_release_llm_config``); this module only restores fidelity where the
pipeline honors the config (``ALLOW_LLM_CONFIG``), matching how a natively
configured schematiq session already behaves.
"""

import json
import logging
from pathlib import Path
from typing import Optional, Tuple

from app.core.config import DEFAULT_DATA_DIR, RELEASE_CONFIG
from app.models.schematiq import LLMConfig

logger = logging.getLogger(__name__)

# Fields we are willing to carry from a persisted backend dict into an LLMConfig.
# (api_key is intentionally excluded — it is resolved separately at run time.)
_LLM_FIELDS = ("provider", "model", "temperature", "max_output_tokens", "context_window_size")


def _backend_from_dict(raw) -> Optional[LLMConfig]:
    """Build an LLMConfig from a persisted backend dict, or None if unusable."""
    if not isinstance(raw, dict):
        return None
    if not raw.get("provider"):  # provider is the one required field
        return None
    fields = {k: raw[k] for k in _LLM_FIELDS if raw.get(k) is not None}
    try:
        return LLMConfig(**fields)
    except Exception as e:  # malformed persisted value: fall through to default
        logger.debug("Ignoring malformed persisted backend %s: %s", raw, e)
        return None


def _persisted_llm_configuration(session, session_id: str) -> dict:
    """Return the persisted ``llm_configuration``-shaped dict for a session.

    Order: session metadata first (mirrors the re-extraction resolver's Priority
    1), then the on-disk ``schematiq_config.json`` written on import by the
    complete-export round-trip and by ``/schematiq/configure``.
    """
    try:
        extracted = getattr(session.metadata, "extracted_schema", None) or {}
        cfg = extracted.get("llm_configuration") if isinstance(extracted, dict) else None
        if isinstance(cfg, dict) and (
            cfg.get("schema_creation_backend") or cfg.get("value_extraction_backend")
        ):
            return cfg
    except Exception as e:
        logger.debug("Could not read llm_configuration from session metadata: %s", e)

    try:
        config_file = Path(DEFAULT_DATA_DIR) / session_id / "schematiq_config.json"
        if config_file.exists():
            with open(config_file) as f:
                data = json.load(f)
            if data.get("schema_creation_backend") or data.get("value_extraction_backend"):
                return data
    except Exception as e:
        logger.debug("Could not read schematiq_config.json for %s: %s", session_id, e)

    return {}


def _release_backend(is_schema_creation: bool) -> LLMConfig:
    return LLMConfig(
        provider=RELEASE_CONFIG["llm_provider"],
        model=(
            RELEASE_CONFIG["schema_creation_model"]
            if is_schema_creation
            else RELEASE_CONFIG["value_extraction_model"]
        ),
        temperature=RELEASE_CONFIG["llm_temperature"],
    )


def build_rediscovery_backends(session, session_id: str) -> Tuple[LLMConfig, LLMConfig]:
    """Return ``(schema_creation_backend, value_extraction_backend)`` for a run.

    Prefers the project's persisted backends so a re-imported project rediscovers
    with its original models; falls back to RELEASE_CONFIG per backend when
    nothing usable was persisted (e.g. an older import), which reproduces the
    previous behavior exactly.
    """
    cfg = _persisted_llm_configuration(session, session_id)
    schema_backend = _backend_from_dict(cfg.get("schema_creation_backend")) or _release_backend(True)
    value_backend = _backend_from_dict(cfg.get("value_extraction_backend")) or _release_backend(False)
    return schema_backend, value_backend
