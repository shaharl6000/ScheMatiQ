"""``_get_llm_from_session`` resolves the project's own configured model.

Fill Cells / Wrong Try Again must use the same value_extraction_backend the
project was created with (schematiq_config.json under the schematiq work
dir), not a stale session-wide user_llm_config.json override left behind by
an unrelated ReextractionDialog run, and not the coarse DEVELOPER_MODE lock
-- release-mode enforcement now goes through enforce_release_llm_config
(ALLOW_LLM_CONFIG), matching project creation.
"""

import json
from unittest.mock import MagicMock, patch

from app.services.reextraction_service import ReextractionService


def _make_service() -> ReextractionService:
    return ReextractionService(
        websocket_manager=MagicMock(),
        session_manager=MagicMock(),
    )


def _write_schematiq_config(tmp_path, session_id: str, value_extraction_backend: dict):
    session_dir = tmp_path / "schematiq_work" / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "schematiq_config.json").write_text(
        json.dumps({"value_extraction_backend": value_extraction_backend}),
        encoding="utf-8",
    )


def _write_user_llm_config(tmp_path, session_id: str, config: dict):
    session_dir = tmp_path / "data" / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "user_llm_config.json").write_text(json.dumps(config), encoding="utf-8")


def test_uses_project_configured_model(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-1"
    _write_schematiq_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "gemini-3.1-pro-preview", "temperature": 0},
    )

    service = _make_service()
    with patch("app.services.reextraction_service.schematiq_utils.build_llm") as mock_build:
        service._get_llm_from_session(session_id, honor_user_llm_config=False)

    mock_build.assert_called_once()
    (config,), _ = mock_build.call_args
    assert config["model"] == "gemini-3.1-pro-preview"
    assert config["provider"] == "gemini"


def test_stale_user_llm_config_ignored_when_not_honored(tmp_path, monkeypatch):
    """The Bug 3 regression: a leftover ReextractionDialog override must not
    win over the project's own config for Fill Cells / Wrong Try Again."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-2"
    _write_schematiq_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "project-model", "temperature": 0},
    )
    _write_user_llm_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "stale-dialog-model", "api_key": "k"},
    )

    service = _make_service()
    with patch("app.services.reextraction_service.schematiq_utils.build_llm") as mock_build:
        service._get_llm_from_session(session_id, honor_user_llm_config=False)

    (config,), _ = mock_build.call_args
    assert config["model"] == "project-model"


def test_user_llm_config_honored_when_requested(tmp_path, monkeypatch):
    """ReextractionDialog's own explicit override still works as before."""
    monkeypatch.chdir(tmp_path)
    session_id = "sess-3"
    _write_schematiq_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "project-model", "temperature": 0},
    )
    _write_user_llm_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "dialog-chosen-model", "api_key": "k"},
    )

    service = _make_service()
    with patch("app.services.reextraction_service.schematiq_utils.build_llm") as mock_build:
        service._get_llm_from_session(session_id, honor_user_llm_config=True)

    (config,), _ = mock_build.call_args
    assert config["model"] == "dialog-chosen-model"


def test_allow_llm_config_false_locks_to_release_model(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-4"
    _write_schematiq_config(
        tmp_path, session_id,
        {"provider": "gemini", "model": "project-model", "temperature": 0},
    )
    monkeypatch.setattr("app.services.pipeline.llm_factory.ALLOW_LLM_CONFIG", False)

    service = _make_service()
    with patch("app.services.reextraction_service.schematiq_utils.build_llm") as mock_build:
        service._get_llm_from_session(session_id, honor_user_llm_config=False)

    (config,), _ = mock_build.call_args
    from app.core.config import RELEASE_CONFIG
    assert config["model"] == RELEASE_CONFIG["value_extraction_model"]


def test_no_saved_config_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-5"

    service = _make_service()
    with patch("app.services.reextraction_service.GeminiLLM") as mock_gemini:
        service._get_llm_from_session(session_id, honor_user_llm_config=False)

    mock_gemini.assert_called_once()
