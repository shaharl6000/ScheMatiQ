"""Tests for device-aware embedding concurrency and load locking."""

from __future__ import annotations

import concurrent.futures
import threading
import time
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from schematiq.core import embedding_model, schema as schema_mod
from schematiq.core.retrievers import EmbeddingRetriever


class _FakeEncodeModel:
    """Fake SentenceTransformer tracking concurrency and optionally waiting on a barrier."""

    def __init__(self, device, *, barrier: threading.Barrier | None = None, sleep_s: float = 0.0):
        self.device = device
        self._barrier = barrier
        self._sleep_s = sleep_s
        self._counter_lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def encode(self, texts, **_kwargs):
        with self._counter_lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self._barrier is not None:
                self._barrier.wait()
            if self._sleep_s > 0:
                time.sleep(self._sleep_s)
            if isinstance(texts, str):
                return np.ones(4, dtype=np.float32)
            return np.ones((len(texts), 4), dtype=np.float32)
        finally:
            with self._counter_lock:
                self.active -= 1


def test_is_mps_model_device_detection() -> None:
    assert embedding_model.is_mps_model(SimpleNamespace(device=torch.device("mps"))) is True
    assert embedding_model.is_mps_model(SimpleNamespace(device=torch.device("mps", 0))) is True
    assert embedding_model.is_mps_model(SimpleNamespace(device="mps")) is True
    assert embedding_model.is_mps_model(SimpleNamespace(device="mps:0")) is True
    assert (
        embedding_model.is_mps_model(
            SimpleNamespace(model=SimpleNamespace(device=torch.device("mps", 0)))
        )
        is True
    )

    assert embedding_model.is_mps_model(SimpleNamespace(device=torch.device("cpu"))) is False
    assert embedding_model.is_mps_model(SimpleNamespace(device=torch.device("cuda", 0))) is False
    assert embedding_model.is_mps_model(SimpleNamespace(device="cpu")) is False
    assert embedding_model.is_mps_model(SimpleNamespace(device="cuda:0")) is False
    assert embedding_model.is_mps_model(object()) is False


@pytest.mark.parametrize("device", [torch.device("cpu"), torch.device("cuda", 0)])
def test_encode_runs_concurrently_on_cpu_and_cuda(
    device: torch.device, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Barrier(2) inside encode proves two threads enter encode simultaneously;
    # any global lock around encode would deadlock until BrokenBarrierError.
    schema_barrier = threading.Barrier(2, timeout=2.0)
    fake_schema_model = _FakeEncodeModel(device, barrier=schema_barrier)
    monkeypatch.setattr(schema_mod, "_EMB_MODEL", fake_schema_model)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        vecs = list(pool.map(lambda i: schema_mod._embed(f"column_{i}"), range(2)))
    assert len(vecs) == 2
    assert fake_schema_model.max_active == 2

    ret_barrier = threading.Barrier(2, timeout=2.0)
    fake_ret_model = _FakeEncodeModel(device, barrier=ret_barrier)
    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: fake_ret_model
    )
    retriever = EmbeddingRetriever(
        model_name="fake-model", enable_preprocessing=False, enable_embedding_cache=False
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda i: retriever._query_sentence_transformer_with_cache(
                    ["passage a", "passage b"], f"question {i}"
                ),
                range(2),
            )
        )
    assert len(results) == 2
    assert fake_ret_model.max_active == 2


def test_encode_is_serialized_on_mps(monkeypatch: pytest.MonkeyPatch) -> None:
    mps_dev = torch.device("mps", 0)
    fake_schema_model = _FakeEncodeModel(mps_dev, sleep_s=0.05)
    monkeypatch.setattr(schema_mod, "_EMB_MODEL", fake_schema_model)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda i: schema_mod._embed(f"column_{i}"), range(4)))
    assert fake_schema_model.max_active == 1

    fake_ret_model = _FakeEncodeModel(mps_dev, sleep_s=0.05)
    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: fake_ret_model
    )
    retriever = EmbeddingRetriever(
        model_name="fake-model", enable_preprocessing=False, enable_embedding_cache=False
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(
            pool.map(
                lambda i: retriever._query_sentence_transformer_with_cache(
                    ["passage a", "passage b"], f"question {i}"
                ),
                range(4),
            )
        )
    assert fake_ret_model.max_active == 1


def test_load_lock_applies_to_all_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    counter_lock = threading.Lock()
    active_loads = 0
    max_active_loads = 0
    total_loads = 0

    class _FakeST:
        def __init__(self, _model_name: str, **_kwargs):
            nonlocal active_loads, max_active_loads, total_loads
            with counter_lock:
                active_loads += 1
                max_active_loads = max(max_active_loads, active_loads)
                total_loads += 1
            try:
                time.sleep(0.05)
                self.device = torch.device("cpu")
            finally:
                with counter_lock:
                    active_loads -= 1

    import sentence_transformers

    monkeypatch.setattr(sentence_transformers, "SentenceTransformer", _FakeST)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        models = list(
            pool.map(
                lambda _: embedding_model.load_sentence_transformer("fake", device="cpu"),
                range(4),
            )
        )
    assert len(models) == 4
    assert max_active_loads == 1
    assert total_loads == 4

    monkeypatch.setattr(schema_mod, "_EMB_MODEL", None)
    before = total_loads
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        singletons = list(pool.map(lambda _: schema_mod._get_emb_model(), range(4)))
    assert total_loads - before == 1
    assert len({id(m) for m in singletons}) == 1


def test_cross_instance_mps_encodes_are_serialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent encodes across distinct MPS model instances share one process-wide lock."""
    stats_lock = threading.Lock()
    stats = {"active": 0, "max_active": 0}

    class _SharedStatsModel:
        def __init__(self, device, *, sleep_s: float = 0.0, barrier=None):
            self.device = device
            self._sleep_s = sleep_s
            self._barrier = barrier

        def encode(self, texts, **_kwargs):
            with stats_lock:
                stats["active"] += 1
                stats["max_active"] = max(stats["max_active"], stats["active"])
            try:
                if self._barrier is not None:
                    self._barrier.wait()
                if self._sleep_s > 0:
                    time.sleep(self._sleep_s)
                if isinstance(texts, str):
                    return np.ones(4, dtype=np.float32)
                return np.ones((len(texts), 4), dtype=np.float32)
            finally:
                with stats_lock:
                    stats["active"] -= 1

    mps_dev = torch.device("mps", 0)
    m_schema = _SharedStatsModel(mps_dev, sleep_s=0.05)
    m_ret_a = _SharedStatsModel(mps_dev, sleep_s=0.05)
    m_ret_b = _SharedStatsModel(mps_dev, sleep_s=0.05)

    monkeypatch.setattr(schema_mod, "_EMB_MODEL", m_schema)
    models_iter = iter([m_ret_a, m_ret_b])
    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: next(models_iter)
    )
    ret_a = EmbeddingRetriever(
        model_name="fake-model-a", enable_preprocessing=False, enable_embedding_cache=False
    )
    ret_b = EmbeddingRetriever(
        model_name="fake-model-b", enable_preprocessing=False, enable_embedding_cache=False
    )

    def _worker(idx: int) -> None:
        if idx < 2:
            schema_mod._embed(f"col_{idx}")
        elif idx < 4:
            ret_a._query_sentence_transformer_with_cache(
                ["passage a", "passage b"], f"q_{idx}"
            )
        else:
            ret_b._query_sentence_transformer_with_cache(
                ["passage c", "passage d"], f"q_{idx}"
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(_worker, range(6)))

    assert stats["max_active"] == 1

    # On CPU, distinct instances (schema._embed + EmbeddingRetriever) must NOT block each other.
    stats["active"] = 0
    stats["max_active"] = 0
    cpu_barrier = threading.Barrier(2, timeout=2.0)
    cpu_schema = _SharedStatsModel(torch.device("cpu"), barrier=cpu_barrier)
    cpu_ret_model = _SharedStatsModel(torch.device("cpu"), barrier=cpu_barrier)

    monkeypatch.setattr(schema_mod, "_EMB_MODEL", cpu_schema)
    monkeypatch.setattr(
        embedding_model, "load_sentence_transformer", lambda *_a, **_kw: cpu_ret_model
    )
    cpu_ret = EmbeddingRetriever(
        model_name="fake-cpu", enable_preprocessing=False, enable_embedding_cache=False
    )
    cached = np.ones((2, 4), dtype=np.float32)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(schema_mod._embed, "col_cpu")
        f2 = pool.submit(
            cpu_ret._query_sentence_transformer_with_cache,
            ["passage a", "passage b"],
            "q_cpu",
            cached_embeddings=cached,
        )
        f1.result(timeout=5.0)
        f2.result(timeout=5.0)

    assert stats["max_active"] == 2

