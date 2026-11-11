"""Focused tests for UnitViewService, data_query.load_all_rows, figure cache, and sync bridge optimizations."""

import json
import threading
from pathlib import Path

import pytest

from app.api.routes.units import _FIGURE_INDEX_CACHE, _find_figure
from app.services.pipeline.data_query import get_data, load_all_rows
from app.services.unit_view_service import UnitViewService
from app.storage.supabase_backend import SupabaseStorageBackend


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


class TestUnitViewServiceOptimizations:
    def test_row_cache_lru_bound_and_recency_promotion(self, tmp_path):
        data_root = tmp_path / "data"
        work_root = tmp_path / "work"
        svc = UnitViewService(data_dir=str(data_root))
        svc.work_dir = work_root
        svc._row_cache.clear()

        try:
            total_sessions = svc._MAX_ROW_CACHE_SESSIONS + 5
            for idx in range(total_sessions):
                sid = f"sess_{idx:02d}"
                _write_jsonl(
                    data_root / sid / "data.jsonl",
                    [{"row_name": f"r_{idx}", "data": {"v": idx}}],
                )

            # Populate first MAX_ROW_CACHE_SESSIONS entries
            for idx in range(svc._MAX_ROW_CACHE_SESSIONS):
                rows = svc._load_all_rows(f"sess_{idx:02d}")
                assert len(rows) == 1

            assert len(svc._row_cache) == svc._MAX_ROW_CACHE_SESSIONS

            # Touch sess_00 so it moves to MRU; sess_01 becomes LRU
            assert svc._load_all_rows("sess_00")[0]["row_name"] == "r_0"

            # Load one more session -> sess_01 evicted, sess_00 retained
            next_sid = f"sess_{svc._MAX_ROW_CACHE_SESSIONS:02d}"
            assert len(svc._load_all_rows(next_sid)) == 1
            assert len(svc._row_cache) == svc._MAX_ROW_CACHE_SESSIONS
            assert "sess_00" in svc._row_cache
            assert "sess_01" not in svc._row_cache
        finally:
            svc._row_cache.clear()

    def test_save_all_rows_writes_atomically_and_invalidates_cache(self, tmp_path):
        data_root = tmp_path / "data"
        svc = UnitViewService(data_dir=str(data_root))
        svc.work_dir = tmp_path / "work"
        svc._row_cache.clear()

        sid = "atomic_session"
        target = data_root / sid / "data.jsonl"
        _write_jsonl(
            target,
            [
                {"row_name": "p1", "_unit_name": "gpt 4", "data": {"acc": "90"}},
                {"row_name": "p2", "_unit_name": "GPT-4", "data": {"acc": "92"}},
            ],
        )

        assert len(svc._load_all_rows(sid)) == 2
        assert sid in svc._row_cache

        from app.models.unit import MergeUnitsRequest

        res = svc.merge_units(
            sid,
            MergeUnitsRequest(
                source_units=["gpt 4", "GPT-4"],
                target_unit="GPT-4",
                strategy="rename",
            ),
        )
        assert res.success is True
        assert res.rows_affected == 2
        # No leftover atomic temp files
        leftovers = list((data_root / sid).glob("*.tmp"))
        assert leftovers == []
        # Reloaded rows reflect merged unit name
        reloaded = svc._load_all_rows(sid)
        assert [r["_unit_name"] for r in reloaded] == ["GPT-4", "GPT-4"]
        svc._row_cache.clear()

    def test_calculate_suggestions_matches_pairwise_similarity_exactly(self, tmp_path):
        data_root = tmp_path / "data"
        svc = UnitViewService(data_dir=str(data_root))
        svc.work_dir = tmp_path / "work"
        svc._row_cache.clear()

        unit_names = [
            "GPT-4",
            "gpt-4",
            "GPT 4 Turbo",
            "GPT-4o mini",
            "Claude 3.5 Sonnet",
            "Claude-3.5-Sonnet",
            "Llama 3 70B Instruct",
            "Llama-3-70B",
            "BERT-base-uncased",
            "RoBERTa-large",
            "ResNet-50",
            "Vision Transformer (ViT-B/16)",
        ]
        sid = "sim_session"
        _write_jsonl(
            data_root / sid / "data.jsonl",
            [
                {"row_name": f"doc_{i}", "_unit_name": name, "data": {"x": i}}
                for i, name in enumerate(unit_names)
            ],
        )

        for threshold in (0.5, 0.75, 0.9, 1.0):
            actual = svc._calculate_suggestions(sid, threshold=threshold)

            expected = []
            for i, n1 in enumerate(unit_names):
                for n2 in unit_names[i + 1 :]:
                    sim = svc._calculate_similarity(n1, n2)
                    if sim >= threshold:
                        suggested_name, reason = svc._generate_merge_suggestion(n1, n2, sim)
                        expected.append((n1, n2, round(sim, 3), suggested_name, reason))
            expected.sort(key=lambda item: item[2], reverse=True)

            assert [
                (s.units[0], s.units[1], s.similarity, s.suggested_name, s.reason)
                for s in actual
            ] == expected
        svc._row_cache.clear()


@pytest.mark.asyncio
async def test_load_all_rows_matches_get_data_model_dump_across_formats_and_dedup(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    sid = "sess_hydration"
    work_dir = tmp_path / "schematiq_work"
    data_dir = tmp_path / "data"

    # Primary work file (ScheMatiQ format + nested data format + cell status)
    _write_jsonl(
        work_dir / sid / "extracted_data.jsonl",
        [
            {
                "_row_name": "ModelA",
                "_papers": ["paper1.pdf"],
                "_unit_name": "ModelA",
                "_source_document": "paper1.pdf",
                "accuracy": {"answer": "88.1", "excerpts": ["achieved 88.1%"]},
            },
            {
                "row_name": "ModelB",
                "papers": ["paper2.pdf"],
                "_unit_name": "ModelB",
                "_source_document": "paper2.pdf",
                "_parent_document": "bundle.zip",
                "_cell_status": {"accuracy": "verified"},
                "data": {"accuracy": "91.0"},
            },
        ],
    )
    # Secondary file containing a duplicate key (ModelA@paper1.pdf) and one distinct row
    _write_jsonl(
        data_dir / sid / "data.jsonl",
        [
            {
                "row_name": "ModelA",
                "papers": ["paper1.pdf"],
                "_unit_name": "ModelA",
                "_source_document": "paper1.pdf",
                "data": {"accuracy": "STALE_DUPLICATE"},
            },
            {
                "row_name": "ModelC",
                "papers": ["paper3.pdf"],
                "data": {"accuracy": "77.4"},
            },
        ],
    )

    paginated = await get_data(sid, work_dir, page=0, page_size=200)
    expected = [row.model_dump() for row in paginated.rows]
    actual = await load_all_rows(sid, work_dir)

    assert len(actual) == 3
    assert actual == expected


def test_find_figure_caches_manifests_and_invalidates_on_update(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _FIGURE_INDEX_CACHE.clear()
    figures_dir = tmp_path / "data" / "s_fig" / "documents" / "figures" / "paper1"
    figures_dir.mkdir(parents=True)
    manifest_path = figures_dir / "manifest.json"
    img1 = figures_dir / "fig1.png"
    img1.write_bytes(b"png1")
    manifest_path.write_text(
        json.dumps(
            {
                "figures": [
                    {"figure_id": "p1_f1", "image_filename": "fig1.png", "caption": "first"}
                ]
            }
        ),
        encoding="utf-8",
    )

    read_calls = 0
    orig_read_text = Path.read_text

    def counting_read_text(self: Path, *args, **kwargs):
        nonlocal read_calls
        if self.name == "manifest.json":
            read_calls += 1
        return orig_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read_text)

    found1 = _find_figure("s_fig", "p1_f1")
    assert found1 is not None and found1[0] == img1
    assert read_calls == 1

    # Repeated lookups hit cache without re-reading manifest.json
    assert _find_figure("s_fig", "p1_f1") is not None
    assert _find_figure("s_fig", "unknown_fig") is None
    assert read_calls == 1

    # Modifying manifest.json (new size/mtime) invalidates cache automatically
    img2 = figures_dir / "fig2.png"
    img2.write_bytes(b"png2")
    manifest_path.write_text(
        json.dumps(
            {
                "figures": [
                    {"figure_id": "p1_f1", "image_filename": "fig1.png", "caption": "first"},
                    {"figure_id": "p1_f2", "image_filename": "fig2.png", "caption": "second"},
                ]
            }
        ),
        encoding="utf-8",
    )

    found2 = _find_figure("s_fig", "p1_f2")
    assert found2 is not None and found2[0] == img2
    assert read_calls == 2
    _FIGURE_INDEX_CACHE.clear()


@pytest.mark.asyncio
async def test_supabase_sync_helpers_reuse_single_daemon_loop_inside_running_loop():
    backend = SupabaseStorageBackend.__new__(SupabaseStorageBackend)
    seen_threads: list[int] = []
    store: dict[str, dict] = {}

    async def fake_save(session_id: str, data: dict) -> bool:
        seen_threads.append(threading.get_ident())
        store[session_id] = dict(data)
        return True

    async def fake_get(session_id: str):
        seen_threads.append(threading.get_ident())
        return store.get(session_id)

    backend.save_session = fake_save  # type: ignore[method-assign]
    backend.get_session = fake_get  # type: ignore[method-assign]

    caller_tid = threading.get_ident()
    for i in range(5):
        assert backend.save_session_sync(f"s_{i}", {"idx": i}) is True
        assert backend.get_session_sync(f"s_{i}") == {"idx": i}

    # All 10 sync-in-async calls executed on the SAME persistent background daemon thread
    assert len(seen_threads) == 10
    assert len(set(seen_threads)) == 1
    assert seen_threads[0] != caller_tid
