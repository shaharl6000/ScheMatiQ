"""Tests for CWD-relative data path resolution (dev.sh isolation)."""

from app.services import data_utils


def test_get_schematiq_work_dir_uses_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert data_utils.get_schematiq_work_dir() == (tmp_path / "schematiq_work").resolve()
    assert data_utils.get_schematiq_work_dir().is_dir()


def test_enumerate_finds_cwd_schematiq_work_not_backend_module_path(
    tmp_path, monkeypatch
):
    """dev.sh runs from .dev-data/instance-N; data must not require backend/schematiq_work."""
    runtime = tmp_path / "instance-0"
    runtime.mkdir()
    monkeypatch.chdir(runtime)

    session_id = "sess-path-1"
    extracted = runtime / "schematiq_work" / session_id / "extracted_data.jsonl"
    extracted.parent.mkdir(parents=True)
    extracted.write_text('{"_row_name": "R1"}\n', encoding="utf-8")

    found = data_utils.enumerate_session_data_files(session_id)
    assert len(found) == 1
    assert found[0].resolve() == extracted.resolve()
