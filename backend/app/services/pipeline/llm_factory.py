"""LLM interface construction and release-mode enforcement."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import (
    ALLOW_LLM_CONFIG,
    DEFAULT_DATA_DIR,
    DEFAULT_SCHEMATIQ_WORK_DIR,
    DEFAULT_TEMPERATURE,
    RELEASE_CONFIG,
)

logger = logging.getLogger(__name__)

from schematiq.core import utils as schematiq_utils
from schematiq.core.llm_backends import LLMInterface, TogetherLLM, OpenAILLM, GeminiLLM
from schematiq.core.model_specs import ModelNames


def build_llm_interface(
    provider: str,
    model: str,
    max_output_tokens: Optional[int],
    temperature: float,
    api_key: str = None,
    context_window_size: Optional[int] = None
) -> LLMInterface:
    """Build LLM interface based on provider.

    Args:
        provider: LLM provider name (together, openai, gemini)
        model: Model name/identifier (empty string uses provider default)
        max_output_tokens: Maximum tokens the model can generate in its response.
            If None, auto-detected from model specs.
        temperature: Sampling temperature
        api_key: Optional user-provided API key (falls back to env var if None)
        context_window_size: Maximum context window size. If None, auto-detected from model specs.
    """
    if provider.lower() == "together":
        if not model:
            raise ValueError("Model must be specified for Together AI provider")
        return TogetherLLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            context_window_size=context_window_size,
            api_key=api_key
        )
    elif provider.lower() == "openai":
        if not model:
            raise ValueError("Model must be specified for OpenAI provider")
        return OpenAILLM(
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            context_window_size=context_window_size,
            api_key=api_key
        )
    if provider.lower() == "gemini":
        kwargs = {
            "max_output_tokens": max_output_tokens,
            "temperature": temperature,
            "context_window_size": context_window_size,
            "api_key": api_key
        }
        if model:
            kwargs["model"] = model
        llm = GeminiLLM(**kwargs)
        logger.info(f"Gemini LLM created: model={llm.model}, max_output_tokens={llm.max_output_tokens}, context_window={llm.context_window_size}")
        return llm
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


def enforce_release_llm_config(backend_config: dict, is_schema_creation: bool = False) -> dict:
    """Override LLM config with release-mode defaults if not in developer mode.

    Args:
        backend_config: The original LLM backend configuration dict
        is_schema_creation: True for schema creation LLM, False for value extraction

    Returns:
        The config dict, potentially with provider/model/temperature overridden
    """
    role = "schema_creation" if is_schema_creation else "value_extraction"

    if ALLOW_LLM_CONFIG:
        resolved, source = backend_config, "user-selected"
    else:
        resolved = {
            **backend_config,
            "provider": RELEASE_CONFIG["llm_provider"],
            "model": RELEASE_CONFIG["schema_creation_model"] if is_schema_creation else RELEASE_CONFIG["value_extraction_model"],
            "temperature": RELEASE_CONFIG["llm_temperature"],
        }
        source = "release-enforced"

    logger.info(
        "LLM config (%s): provider=%s model=%s (%s)",
        role, resolved["provider"], resolved["model"], source,
    )
    return resolved


def _pick_backend_from_config(
    config: Dict[str, Any],
    *,
    is_schema_creation: bool = False,
) -> Optional[Dict[str, Any]]:
    """Select the primary or fallback backend dict from a schema/ScheMatiQ config."""
    primary = "schema_creation_backend" if is_schema_creation else "value_extraction_backend"
    secondary = "value_extraction_backend" if is_schema_creation else "schema_creation_backend"
    return config.get(primary) or config.get(secondary) or config.get("backend")


def resolve_llm_for_session(
    session_id: str,
    session_manager: Any = None,
    *,
    is_schema_creation: bool = False,
    honor_user_llm_config: bool = True,
    extracted_schema: Optional[Dict[str, Any]] = None,
) -> LLMInterface:
    """Resolve and build an ``LLMInterface`` for a session's saved configuration.

    Checks config sources in priority order and funnels every resolved config
    through ``enforce_release_llm_config`` so ``ALLOW_LLM_CONFIG`` and user API
    keys are respected consistently across services:
      0. ``data/{session_id}/user_llm_config.json`` (when ``honor_user_llm_config``)
      1. ``schematiq_work/{session_id}/schematiq_config.json``
      2. ``extracted_schema`` argument or ``session.metadata.extracted_schema``
      3. ``data/{session_id}/parsed_schema.json``
      4. ``data/{session_id}/schematiq_config.json``
      5. Default Gemini configuration for the requested stage.
    """
    data_session_dir = Path(DEFAULT_DATA_DIR) / session_id
    schematiq_session_dir = Path(DEFAULT_SCHEMATIQ_WORK_DIR) / session_id

    if honor_user_llm_config:
        try:
            user_config_file = data_session_dir / "user_llm_config.json"
            if user_config_file.exists():
                with open(user_config_file, encoding="utf-8") as f:
                    user_config = json.load(f)
                if isinstance(user_config, dict) and user_config:
                    logger.debug(
                        "Using LLM config from user_llm_config.json: %s %s, api_key=%s",
                        user_config.get("provider"),
                        user_config.get("model"),
                        "present" if user_config.get("api_key") else "MISSING",
                    )
                    enforced = enforce_release_llm_config(
                        user_config, is_schema_creation=is_schema_creation
                    )
                    return schematiq_utils.build_llm(enforced)
        except Exception as e:
            logger.debug("Could not load user LLM config: %s", e)

    try:
        work_config_file = schematiq_session_dir / "schematiq_config.json"
        if work_config_file.exists():
            with open(work_config_file, encoding="utf-8") as f:
                schematiq_config = json.load(f)
            backend_config = _pick_backend_from_config(
                schematiq_config, is_schema_creation=is_schema_creation
            )
            if backend_config:
                logger.debug(
                    "Using LLM config from schematiq_config.json (work dir): %s %s",
                    backend_config.get("provider"),
                    backend_config.get("model"),
                )
                enforced = enforce_release_llm_config(
                    backend_config, is_schema_creation=is_schema_creation
                )
                return schematiq_utils.build_llm(enforced)
    except Exception as e:
        logger.debug("Could not load LLM config from schematiq_config.json (work dir): %s", e)

    schema_candidates: list[Dict[str, Any]] = []
    if isinstance(extracted_schema, dict):
        schema_candidates.append(extracted_schema)
    if session_manager is not None:
        try:
            session = session_manager.get_session(session_id)
            if session and session.metadata and session.metadata.extracted_schema:
                schema_candidates.append(session.metadata.extracted_schema)
        except Exception as e:
            logger.debug("Could not load LLM config from session metadata: %s", e)

    for candidate in schema_candidates:
        llm_config = candidate.get("llm_configuration")
        if isinstance(llm_config, dict):
            backend_config = _pick_backend_from_config(
                llm_config, is_schema_creation=is_schema_creation
            )
            if backend_config:
                logger.debug(
                    "Using LLM config from session schema metadata: %s %s",
                    backend_config.get("provider"),
                    backend_config.get("model"),
                )
                enforced = enforce_release_llm_config(
                    backend_config, is_schema_creation=is_schema_creation
                )
                return schematiq_utils.build_llm(enforced)

    try:
        parsed_schema_file = data_session_dir / "parsed_schema.json"
        if parsed_schema_file.exists():
            with open(parsed_schema_file, encoding="utf-8") as f:
                parsed_schema = json.load(f)
            llm_config = parsed_schema.get("llm_configuration")
            if isinstance(llm_config, dict):
                backend_config = _pick_backend_from_config(
                    llm_config, is_schema_creation=is_schema_creation
                )
                if backend_config:
                    logger.debug(
                        "Using LLM config from parsed_schema.json: %s %s",
                        backend_config.get("provider"),
                        backend_config.get("model"),
                    )
                    enforced = enforce_release_llm_config(
                        backend_config, is_schema_creation=is_schema_creation
                    )
                    return schematiq_utils.build_llm(enforced)
    except Exception as e:
        logger.debug("Could not load LLM config from parsed_schema.json: %s", e)

    try:
        legacy_config_file = data_session_dir / "schematiq_config.json"
        if legacy_config_file.exists():
            with open(legacy_config_file, encoding="utf-8") as f:
                schematiq_config = json.load(f)
            backend_config = _pick_backend_from_config(
                schematiq_config, is_schema_creation=is_schema_creation
            )
            if backend_config:
                logger.debug(
                    "Using LLM config from schematiq_config.json (data dir): %s %s",
                    backend_config.get("provider"),
                    backend_config.get("model"),
                )
                enforced = enforce_release_llm_config(
                    backend_config, is_schema_creation=is_schema_creation
                )
                return schematiq_utils.build_llm(enforced)
    except Exception as e:
        logger.debug("Could not load LLM config from schematiq_config.json (data dir): %s", e)

    default_model = (
        ModelNames.DEFAULT_SCHEMA_CREATION
        if is_schema_creation
        else ModelNames.DEFAULT_VALUE_EXTRACTION
    )
    default_config = enforce_release_llm_config(
        {
            "provider": "gemini",
            "model": default_model,
            "temperature": DEFAULT_TEMPERATURE,
        },
        is_schema_creation=is_schema_creation,
    )
    return schematiq_utils.build_llm(default_config)
