"""Per-document state must not leak between threads.

TableBuilder can process documents concurrently on one PaperProcessor and one
GeminiLLM. The context cache, figure images and last-call diagnostics are per
thread, so one document never reads another's state.
"""

import threading

from schematiq.core.llm_backends import GeminiLLM
from schematiq.core.thread_local import ThreadLocalAttribute
from schematiq.value_extraction.core.paper_processor import PaperProcessor


def _run_in_threads(obj, attr, values):
    barrier = threading.Barrier(len(values))
    seen = {}

    def work(value):
        setattr(obj, attr, value)
        barrier.wait()  # every thread has written before anyone reads
        seen[value if not isinstance(value, (list, dict)) else str(value)] = getattr(obj, attr)

    threads = [threading.Thread(target=work, args=(v,)) for v in values]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return seen


def test_context_cache_is_per_thread():
    pp = object.__new__(PaperProcessor)
    seen = _run_in_threads(pp, "_active_context_cache", ["cache-doc-a", "cache-doc-b"])
    assert seen == {"cache-doc-a": "cache-doc-a", "cache-doc-b": "cache-doc-b"}


def test_figure_images_are_per_thread():
    pp = object.__new__(PaperProcessor)
    a, b = [("Figure 1", b"a", "image/png")], [("Figure 2", b"b", "image/png")]
    seen = _run_in_threads(pp, "_active_figure_images", [a, b])
    assert seen == {str(a): a, str(b): b}


def test_last_call_diag_is_per_thread():
    llm = object.__new__(GeminiLLM)
    a, b = {"finish_reason": "STOP"}, {"finish_reason": "MAX_TOKENS"}
    seen = _run_in_threads(llm, "last_call_diag", [a, b])
    assert seen == {str(a): a, str(b): b}


def test_defaults_and_same_thread_behavior_unchanged():
    pp = object.__new__(PaperProcessor)
    assert pp._active_context_cache is None
    assert pp._active_figure_images == []
    assert pp._active_figures_by_id == {}
    pp._active_figure_images = [("Figure 1", b"x", "image/png")]
    assert len(pp._active_figure_images) == 1


def test_new_thread_starts_from_default():
    class Holder:
        value = ThreadLocalAttribute(list)

    h = Holder()
    h.value.append(1)
    out = []
    t = threading.Thread(target=lambda: out.append(list(h.value)))
    t.start()
    t.join()
    assert h.value == [1]
    assert out == [[]]


def test_uncached_doc_layout_is_per_thread():
    # A short document (no context cache) is sent as (rules, document); a
    # concurrent document must never be sent that other document's text.
    pp = object.__new__(PaperProcessor)
    assert pp._uncached_doc_layout is None
    a, b = ("rules", "text of doc A"), ("rules", "text of doc B")
    seen = _run_in_threads(pp, "_uncached_doc_layout", [a, b])
    assert seen == {a: a, b: b}
