"""Tests for units._find_figure, the lookup backing GET /units/figure-content.

Scoped to the plain lookup helper (not the full endpoint / FastAPI route) —
it has no session_manager or request dependency, only candidate_data_dirs(),
so it can be exercised directly against a tmp_path data dir the same way
test_update_cell_disambiguation.py's editor_env fixture does, without
bootstrapping the whole app.

_find_figure doubles as the allowlist for the endpoint: a figure_id is only
ever servable if it actually appears in one of this session's own
documents/figures/*/manifest.json files, never a raw caller-supplied path.
These tests cover that it finds a real figure, and refuses everything else
(wrong id, wrong session, missing image file, malformed manifest).
"""

import json
from pathlib import Path

import pytest

from app.api.routes.units import _find_figure


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    # candidate_data_dirs() checks Path.cwd() / "data" first.
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "data"
    root.mkdir()
    return root


def _write_manifest(figures_dir: Path, figures: list[dict]) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source_document": "paper1.pdf",
        "figure_count": len(figures),
        "figures": figures,
        "status": "ok",
    }
    (figures_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for fig in figures:
        image_filename = fig.get("image_filename")
        if image_filename:
            (figures_dir / image_filename).write_bytes(b"fake-png-bytes")


def test_finds_a_real_figure(data_root):
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    _write_manifest(figures_dir, [
        {"figure_id": "paper1_fig003", "image_filename": "fig003.png", "caption": "c"},
    ])

    found = _find_figure("session1", "paper1_fig003")

    assert found is not None
    image_path, fig = found
    assert image_path == figures_dir / "fig003.png"
    assert fig["figure_id"] == "paper1_fig003"
    assert fig["caption"] == "c"


def test_unknown_figure_id_is_not_found(data_root):
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    _write_manifest(figures_dir, [
        {"figure_id": "paper1_fig003", "image_filename": "fig003.png"},
    ])

    assert _find_figure("session1", "paper1_fig999") is None


def test_figure_id_from_a_different_session_is_not_found(data_root):
    # This is the allowlist property: a figure_id that's real, just not
    # under *this* session, must not resolve.
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    _write_manifest(figures_dir, [
        {"figure_id": "paper1_fig003", "image_filename": "fig003.png"},
    ])

    assert _find_figure("session2", "paper1_fig003") is None


def test_manifest_entry_referencing_a_missing_image_file_is_not_found(data_root):
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    figures_dir.mkdir(parents=True)
    manifest = {
        "source_document": "paper1.pdf", "figure_count": 1, "status": "ok",
        "figures": [{"figure_id": "paper1_fig003", "image_filename": "fig003.png"}],
    }
    (figures_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    # Note: fig003.png deliberately not written.

    assert _find_figure("session1", "paper1_fig003") is None


def test_manifest_entry_with_no_image_filename_is_not_found(data_root):
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    figures_dir.mkdir(parents=True)
    manifest = {
        "source_document": "paper1.pdf", "figure_count": 1, "status": "ok",
        "figures": [{"figure_id": "paper1_fig003"}],
    }
    (figures_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    assert _find_figure("session1", "paper1_fig003") is None


def test_malformed_manifest_json_is_skipped_not_raised(data_root):
    figures_dir = data_root / "session1" / "documents" / "figures" / "paper1"
    figures_dir.mkdir(parents=True)
    (figures_dir / "manifest.json").write_text("not valid json{{{", encoding="utf-8")

    assert _find_figure("session1", "paper1_fig003") is None


def test_no_figures_directory_at_all_is_not_found(data_root):
    (data_root / "session1").mkdir()
    assert _find_figure("session1", "paper1_fig003") is None


def test_finds_the_right_figure_among_multiple_documents(data_root):
    figures_root = data_root / "session1" / "documents" / "figures"
    _write_manifest(figures_root / "paper1", [
        {"figure_id": "paper1_fig001", "image_filename": "fig001.png"},
    ])
    _write_manifest(figures_root / "paper2", [
        {"figure_id": "paper2_fig001", "image_filename": "fig001.png"},
    ])

    found = _find_figure("session1", "paper2_fig001")

    assert found is not None
    image_path, fig = found
    assert image_path == figures_root / "paper2" / "fig001.png"
