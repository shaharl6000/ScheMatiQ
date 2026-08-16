"""Tests for rediscovery backend resolution.

Both rediscovery entrypoints build a ScheMatiQConfig before running the pipeline.
They must prefer the project's persisted backends (restored on import from a
complete export) so a re-imported project rediscovers with its ORIGINAL models,
falling back to RELEASE_CONFIG defaults only when nothing usable was persisted.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from app.core.config import RELEASE_CONFIG
from app.services.rediscovery_config import build_rediscovery_backends


def _session(extracted_schema=None):
    return SimpleNamespace(metadata=SimpleNamespace(extracted_schema=extracted_schema))


def _write_config(tmp_path, session_id, obj):
    d = tmp_path / session_id
    d.mkdir(parents=True, exist_ok=True)
    (d / "schematiq_config.json").write_text(json.dumps(obj))


ORIG_SCHEMA = {"provider": "openai", "model": "gpt-4o", "temperature": 0.2}
ORIG_VALUE = {"provider": "together", "model": "mixtral", "temperature": 0}


def test_no_persisted_config_uses_release_defaults(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.rediscovery_config.DEFAULT_DATA_DIR", str(tmp_path))
    schema, value = build_rediscovery_backends(_session(), "sess-none")
    assert schema.model == RELEASE_CONFIG["schema_creation_model"]
    assert value.model == RELEASE_CONFIG["value_extraction_model"]


def test_persisted_config_restores_original_models(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.rediscovery_config.DEFAULT_DATA_DIR", str(tmp_path))
    _write_config(
        tmp_path,
        "sess-file",
        {"schema_creation_backend": ORIG_SCHEMA, "value_extraction_backend": ORIG_VALUE},
    )
    schema, value = build_rediscovery_backends(_session(), "sess-file")
    assert (schema.provider, schema.model) == ("openai", "gpt-4o")
    assert (value.provider, value.model) == ("together", "mixtral")


def test_session_metadata_takes_priority_over_file(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.rediscovery_config.DEFAULT_DATA_DIR", str(tmp_path))
    _write_config(
        tmp_path,
        "sess-meta",
        {"schema_creation_backend": ORIG_SCHEMA, "value_extraction_backend": ORIG_VALUE},
    )
    meta = {
        "llm_configuration": {
            "schema_creation_backend": {"provider": "gemini", "model": "meta-schema"},
            "value_extraction_backend": {"provider": "gemini", "model": "meta-value"},
        }
    }
    schema, value = build_rediscovery_backends(_session(meta), "sess-meta")
    assert schema.model == "meta-schema"
    assert value.model == "meta-value"


def test_partial_persisted_falls_back_per_backend(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.rediscovery_config.DEFAULT_DATA_DIR", str(tmp_path))
    _write_config(tmp_path, "sess-partial", {"value_extraction_backend": ORIG_VALUE})
    schema, value = build_rediscovery_backends(_session(), "sess-partial")
    assert schema.model == RELEASE_CONFIG["schema_creation_model"]  # missing -> default
    assert (value.provider, value.model) == ("together", "mixtral")  # present -> original


def test_malformed_persisted_backends_are_safe(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.rediscovery_config.DEFAULT_DATA_DIR", str(tmp_path))
    _write_config(
        tmp_path,
        "sess-bad",
        {"schema_creation_backend": {"model": "no-provider"}, "value_extraction_backend": "oops"},
    )
    schema, value = build_rediscovery_backends(_session(), "sess-bad")
    assert schema.model == RELEASE_CONFIG["schema_creation_model"]
    assert value.model == RELEASE_CONFIG["value_extraction_model"]
