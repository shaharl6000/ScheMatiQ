"""User API keys stay in memory and never reach session files, imports or exports.

Before this, the key in a request's llm_config was written as plaintext into
user_llm_config.json, config.json, schematiq_config.json and the
continue-discovery configs, and an imported project file's key was adopted and
copied back into exports (CWE-312).
"""

import json
from types import SimpleNamespace

import pytest

from app.models.schematiq import LLMConfig, ScheMatiQConfig
from app.services import export_service, llm_api_keys
from app.services.file_parser import FileParser
from app.services.llm_api_keys import (
    forget_api_keys,
    remember_api_keys,
    restore_api_keys,
    strip_api_keys,
)
from app.services.pipeline import llm_factory
from app.services.schematiq_runner import ScheMatiQRunner

SID = "key-session"
KEY = "user-key-123"


@pytest.fixture(autouse=True)
def _clean_keys():
    llm_api_keys._keys.clear()
    yield
    llm_api_keys._keys.clear()


def test_strip_removes_keys_at_any_depth_and_does_not_mutate():
    cfg = {"schema_creation_backend": {"provider": "openai", "api_key": KEY},
           "llm_config": {"provider": "gemini", "api_key": KEY}, "query": "q"}
    stripped = strip_api_keys(cfg)
    assert KEY not in json.dumps(stripped)
    assert stripped["query"] == "q"
    assert cfg["schema_creation_backend"]["api_key"] == KEY


def test_restore_puts_back_the_key_for_the_same_session_and_provider():
    remember_api_keys(SID, {"provider": "OpenAI", "model": "gpt-4.1", "api_key": KEY})
    nested = {"value_extraction_backend": {"provider": "openai", "model": "gpt-4.1"},
              "schema_creation_backend": {"provider": "gemini", "model": "g"}}

    restored = restore_api_keys(SID, nested)

    assert restored["value_extraction_backend"]["api_key"] == KEY
    assert "api_key" not in restored["schema_creation_backend"]  # other provider
    assert "api_key" not in restore_api_keys("other-session", nested)["value_extraction_backend"]


def test_restore_keeps_a_key_that_is_already_present():
    remember_api_keys(SID, {"provider": "openai", "api_key": KEY})
    cfg = {"provider": "openai", "api_key": "explicit"}
    assert restore_api_keys(SID, cfg)["api_key"] == "explicit"


def test_forget_drops_the_session_keys():
    remember_api_keys(SID, {"provider": "openai", "api_key": KEY})
    forget_api_keys(SID)
    assert "api_key" not in restore_api_keys(SID, {"provider": "openai"})


def _config(api_key=KEY):
    backend = LLMConfig(provider="openai", model="gpt-4.1", api_key=api_key)
    return ScheMatiQConfig(query="q", schema_creation_backend=backend,
                           value_extraction_backend=backend, output_path="out")


@pytest.mark.asyncio
async def test_runner_config_json_has_no_key_and_the_run_still_gets_it(tmp_path):
    runner = SimpleNamespace(work_dir=tmp_path)
    await ScheMatiQRunner.save_config(runner, SID, _config())

    on_disk = (tmp_path / SID / "config.json").read_text()
    assert KEY not in on_disk

    # What the runner does when it starts the run.
    loaded = ScheMatiQConfig(**restore_api_keys(SID, json.loads(on_disk)))
    assert loaded.schema_creation_backend.api_key == KEY
    assert loaded.value_extraction_backend.api_key == KEY


def test_factory_builds_with_the_remembered_key(tmp_path, monkeypatch):
    data_dir, work_dir = tmp_path / "data", tmp_path / "work"
    (data_dir / SID).mkdir(parents=True)
    monkeypatch.setattr(llm_factory, "DEFAULT_DATA_DIR", str(data_dir))
    monkeypatch.setattr(llm_factory, "DEFAULT_SCHEMATIQ_WORK_DIR", str(work_dir))
    monkeypatch.setattr(llm_factory, "ALLOW_LLM_CONFIG", True)
    monkeypatch.setattr(llm_factory.schematiq_utils, "build_llm", lambda cfg: dict(cfg))
    user_config = {"provider": "openai", "model": "gpt-4.1", "api_key": KEY}
    # What the upload/reextraction routes now do.
    remember_api_keys(SID, user_config)
    (data_dir / SID / "user_llm_config.json").write_text(json.dumps(strip_api_keys(user_config)))

    cfg = llm_factory.resolve_llm_for_session(SID)

    assert cfg["api_key"] == KEY


@pytest.mark.asyncio
async def test_import_does_not_adopt_a_key_from_the_project_file(tmp_path):
    session_dir = tmp_path / SID
    session_dir.mkdir()
    backend = {"provider": "openai", "model": "gpt-4.1", "temperature": 0, "api_key": KEY}
    (session_dir / "project.json").write_text(json.dumps({
        "query": "q",
        "schema": {"columns": [{"name": "drug", "definition": "d", "rationale": ""}]},
        "llm_configuration": {"schema_creation_backend": backend,
                              "value_extraction_backend": backend},
        "metadata": {"documents_batch_size": 4},
        "data": [{"row_name": "r1", "papers": ["a.pdf"], "data": {"drug": "x"}}],
    }))

    result = await FileParser(data_dir=str(tmp_path)).parse_file(SID)

    restored = (session_dir / "schematiq_config.json").read_text()
    assert KEY not in restored
    assert json.loads(restored)["value_extraction_backend"]["model"] == "gpt-4.1"
    assert KEY not in json.dumps(result.get("extracted_metadata"), default=str)


def test_export_never_contains_a_key(tmp_path, monkeypatch):
    monkeypatch.setattr(export_service, "DEFAULT_DATA_DIR", str(tmp_path))
    (tmp_path / SID).mkdir()
    backend = {"provider": "openai", "model": "gpt-4.1", "api_key": KEY}
    (tmp_path / SID / "schematiq_config.json").write_text(json.dumps(
        {"schema_creation_backend": backend, "value_extraction_backend": backend}
    ))

    exported = export_service._load_schematiq_llm_configuration(SID)

    assert KEY not in json.dumps(exported)
    assert exported["value_extraction_backend"]["model"] == "gpt-4.1"
