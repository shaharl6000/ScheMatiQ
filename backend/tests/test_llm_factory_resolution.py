"""Which provider, model and API key resolve_llm_for_session picks (#635).

Column reprocessing and adding documents to an imported project build their LLM
here. These tests pin two rules across every config source:
- the user's API key is used whenever a session config has it;
- the model follows ALLOW_LLM_CONFIG (user's choice when on, release model when off).
"""

import json
from types import SimpleNamespace

import pytest

from app.core.config import RELEASE_CONFIG
from app.services.pipeline import llm_factory

USER_BACKEND = {"provider": "openai", "model": "gpt-x", "api_key": "USERKEY", "temperature": 0.7}
SESSION_ID = "s1"


class _FakeSessionManager:
    def __init__(self, extracted_schema=None):
        self._session = SimpleNamespace(
            metadata=SimpleNamespace(extracted_schema=extracted_schema)
        )

    def get_session(self, session_id):
        return self._session


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    (data_dir / SESSION_ID).mkdir(parents=True)
    (work_dir / SESSION_ID).mkdir(parents=True)
    monkeypatch.setattr(llm_factory, "DEFAULT_DATA_DIR", str(data_dir))
    monkeypatch.setattr(llm_factory, "DEFAULT_SCHEMATIQ_WORK_DIR", str(work_dir))
    # Capture the config instead of building a real client.
    monkeypatch.setattr(llm_factory.schematiq_utils, "build_llm", lambda cfg: dict(cfg))
    return data_dir / SESSION_ID, work_dir / SESSION_ID


def _write(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def _setup_source(source, data_session, work_session):
    """Write one config source; return the extracted_schema to put on the session."""
    if source == "user_key":
        _write(data_session / "user_llm_config.json", USER_BACKEND)
    elif source == "user_nokey":
        _write(data_session / "user_llm_config.json", {"provider": "openai", "model": "gpt-x"})
    elif source == "work_config":
        _write(work_session / "schematiq_config.json", {"value_extraction_backend": USER_BACKEND})
    elif source == "metadata":
        return {"llm_configuration": {"value_extraction_backend": USER_BACKEND}}
    elif source == "parsed_schema":
        _write(data_session / "parsed_schema.json", {"llm_configuration": {"value_extraction_backend": USER_BACKEND}})
    elif source == "data_config":
        _write(data_session / "schematiq_config.json", {"value_extraction_backend": USER_BACKEND})
    return None


SOURCES_WITH_USER_KEY = ["user_key", "work_config", "metadata", "parsed_schema", "data_config"]


@pytest.mark.parametrize("allow_llm_config", [True, False])
@pytest.mark.parametrize("source", SOURCES_WITH_USER_KEY)
def test_user_key_is_used_whenever_a_config_has_it(dirs, monkeypatch, source, allow_llm_config):
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", allow_llm_config)
    extracted = _setup_source(source, *dirs)

    cfg = llm_factory.resolve_llm_for_session(
        SESSION_ID, session_manager=_FakeSessionManager(extracted)
    )

    assert cfg["api_key"] == "USERKEY"
    if allow_llm_config:
        assert (cfg["provider"], cfg["model"]) == ("openai", "gpt-x")
    else:
        assert cfg["provider"] == RELEASE_CONFIG["llm_provider"]
        assert cfg["model"] == RELEASE_CONFIG["value_extraction_model"]
        assert cfg["temperature"] == RELEASE_CONFIG["llm_temperature"]


@pytest.mark.parametrize("allow_llm_config", [True, False])
def test_user_choice_without_key_falls_back_to_the_server_key(dirs, monkeypatch, allow_llm_config):
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", allow_llm_config)
    _setup_source("user_nokey", *dirs)

    cfg = llm_factory.resolve_llm_for_session(SESSION_ID, session_manager=_FakeSessionManager())

    assert not cfg.get("api_key")  # build_llm then reads the provider's env var
    expected_model = "gpt-x" if allow_llm_config else RELEASE_CONFIG["value_extraction_model"]
    assert cfg["model"] == expected_model


@pytest.mark.parametrize("allow_llm_config", [True, False])
def test_no_config_uses_the_default_gemini_model(dirs, monkeypatch, allow_llm_config):
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", allow_llm_config)

    cfg = llm_factory.resolve_llm_for_session(SESSION_ID, session_manager=_FakeSessionManager())

    assert cfg["provider"] == "gemini"
    assert not cfg.get("api_key")


def test_user_llm_config_wins_over_other_sources(dirs, monkeypatch):
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", True)
    data_session, work_session = dirs
    _write(data_session / "user_llm_config.json", {**USER_BACKEND, "model": "from-user"})
    _write(work_session / "schematiq_config.json", {"value_extraction_backend": {**USER_BACKEND, "model": "from-work"}})

    cfg = llm_factory.resolve_llm_for_session(SESSION_ID, session_manager=_FakeSessionManager())

    assert cfg["model"] == "from-user"


def test_honor_user_llm_config_false_skips_it(dirs, monkeypatch):
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", True)
    data_session, work_session = dirs
    _write(data_session / "user_llm_config.json", {**USER_BACKEND, "model": "from-user"})
    _write(work_session / "schematiq_config.json", {"value_extraction_backend": {**USER_BACKEND, "model": "from-work"}})

    cfg = llm_factory.resolve_llm_for_session(
        SESSION_ID, session_manager=_FakeSessionManager(), honor_user_llm_config=False
    )

    assert cfg["model"] == "from-work"
