"""Tests for PaperProcessor._ground_and_enforce — the hallucination guard.

Regression coverage for a real bug: a fabricated answer (e.g. a supporting
figure/chart that doesn't exist in the source document) was sailing through
unchecked, because ExcerptGrounder — which already verifies whether an
answer's excerpt actually appears in the source text — was never wired into
extract_values_for_unit() (the method the pipeline actually calls), and even
where it WAS wired in (extract_values_for_paper(), the old path), it only
logged stats instead of rejecting the answer.

Driven directly against a minimal self stand-in with a real ExcerptGrounder,
following this repo's pattern of testing PaperProcessor methods unbound
(see test_extract_values_for_unit_narrowing.py) — no LLM/API calls needed.
"""

from types import SimpleNamespace

from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder


def _make_self(active_figure_images=None):
    return SimpleNamespace(
        excerpt_grounder=ExcerptGrounder(),
        _active_figure_images=active_figure_images or [],
    )


SOURCE_TEXT = (
    "The CD45 phosphatase has a D1 domain with tyrosine phosphatase activity. "
    "Splenocytes were activated with anti-CD3 and IL-2 for 72 hours."
)


def test_grounded_answer_with_exact_excerpt_is_kept():
    s = _make_self()
    cleaned = {
        "region_name": {
            "answer": "d1_domain",
            "excerpts": [{"text": "The CD45 phosphatase has a D1 domain", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["region_name"]["answer"] == "d1_domain"


def test_fabricated_answer_with_unmatched_excerpt_is_nulled():
    """The reported bug: a fabricated figure ("Figure 5A, blue, bar chart")
    whose claimed excerpt doesn't exist anywhere in the source text must be
    nulled out, not passed through as a real answer.
    """
    s = _make_self()
    cleaned = {
        "supporting_figure": {
            "answer": "Figure 5A",
            "excerpts": [{"text": "Figure 5A shows a blue bar chart of PTP signature motifs", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["supporting_figure"]["answer"] is None
    assert result["supporting_figure"]["excerpts"] == []


def test_answer_with_no_excerpts_at_all_is_nulled():
    s = _make_self()
    cleaned = {"region_name": {"answer": "d1_domain", "excerpts": []}}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["region_name"]["answer"] is None


def test_paraphrased_fuzzy_match_is_tolerated_not_nulled():
    s = _make_self()
    cleaned = {
        "activation_protocol": {
            "answer": "anti-CD3 and IL-2",
            # Close paraphrase of the real sentence, not a verbatim quote.
            "excerpts": [{"text": "splenocytes activated with anti CD3 and IL2 for 72 h", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["activation_protocol"]["answer"] == "anti-CD3 and IL-2"


def test_null_answer_is_left_untouched():
    s = _make_self()
    cleaned = {"region_name": {"answer": None, "excerpts": []}}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["region_name"]["answer"] is None


def test_metadata_keys_and_non_dict_values_are_skipped():
    s = _make_self()
    cleaned = {"_row_name": "Unit 1", "document_directory": "/tmp/x"}
    # Must not raise despite non-column entries mixed into the dict.
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["_row_name"] == "Unit 1"


def test_excerpt_split_across_a_line_wrap_is_still_grounded():
    """Regression test: a real answer whose excerpt is correct verbatim but
    spans a line-wrap or extra alignment whitespace in the source (very
    common in extracted PDF/markdown-table text) must not be nulled just
    because ExcerptGrounder's fuzzy-match re-verification is whitespace-
    sensitive.
    """
    s = _make_self()
    source = "patients with moderate-to-severe rheumatoid\narthritis (RA) were enrolled."
    cleaned = {
        "condition": {
            "answer": "Rheumatoid Arthritis",
            "excerpts": [{"text": "moderate-to-severe rheumatoid arthritis", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, source, "doc")
    assert result["condition"]["answer"] == "Rheumatoid Arthritis"


def test_excerpt_across_markdown_table_padding_is_still_grounded():
    s = _make_self()
    source = "| Model | MMLU |\n| GPT-4    | 86.4 | 92.0  | 96.3 |\n"
    cleaned = {
        "mmlu_accuracy": {
            "answer": "86.4",
            "excerpts": [{"text": "GPT-4 | 86.4", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, source, "doc")
    assert result["mmlu_accuracy"]["answer"] == "86.4"


def test_empty_source_text_is_a_noop():
    s = _make_self()
    cleaned = {"region_name": {"answer": "d1_domain", "excerpts": [{"text": "anything", "source": "doc"}]}}
    result = PaperProcessor._ground_and_enforce(s, cleaned, "", "doc")
    assert result["region_name"]["answer"] == "d1_domain"


def test_vision_derived_answer_is_not_nulled_when_figure_images_attached():
    """A "what color appears in this figure" answer has no text excerpt to
    ground it in by nature — it came from looking at the attached image, not
    the document text. When the unit has figure images attached, grounding
    must not null out an otherwise-unsupported answer.
    """
    s = _make_self(active_figure_images=[("Fig. 1: A diagram.", b"PNGDATA", "image/png")])
    cleaned = {
        "color_name": {
            "answer": "gray",
            "excerpts": [{"text": "gray", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["color_name"]["answer"] == "gray"


def test_grounding_still_runs_for_text_excerpts_when_figure_images_attached():
    """Regression test: a unit with an attached figure image (e.g. a
    "Figure" observation unit) can still have OTHER columns whose excerpt
    is genuine, literal source text (a caption quote, say) -- that excerpt
    must still get real char_start/grounding_status so the frontend can
    highlight it, instead of being left with none merely because some
    OTHER column in the same unit is vision-derived.
    """
    s = _make_self(active_figure_images=[("Fig. 1: A diagram.", b"PNGDATA", "image/png")])
    cleaned = {
        "region_name": {
            "answer": "d1_domain",
            "excerpts": [{"text": "The CD45 phosphatase has a D1 domain", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    exc = result["region_name"]["excerpts"][0]
    assert exc["grounding_status"] == "exact"
    assert exc["char_start"] is not None


def test_figure_typed_excerpt_is_not_nulled_without_attached_images():
    """A figure-typed excerpt (caption fuzzy-matched via
    _match_excerpt_to_figure_caption, not an attached image) is legitimate
    support and must not be nulled, even when _active_figure_images is
    empty for this call -- distinct from the attached-image carve-out
    above.
    """
    s = _make_self()
    cleaned = {
        "figure_caption_title": {
            "answer": "Galectin-1 death pathway",
            "excerpts": [
                {
                    "type": "figure",
                    "figure_id": "fig-5",
                    "source": "doc",
                    "caption": "Figure 5. A proposed model.",
                    "image_filename": "fig5.png",
                }
            ],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["figure_caption_title"]["answer"] == "Galectin-1 death pathway"


def test_fabricated_answer_is_still_nulled_when_no_figure_images_attached():
    """The exemption above must not weaken the guard for ordinary text-only
    units — same fabricated-answer case as
    test_fabricated_answer_with_unmatched_excerpt_is_nulled, but going through
    _make_self() with the new default (no images) to confirm that default
    doesn't accidentally exempt everything.
    """
    s = _make_self()
    cleaned = {
        "supporting_figure": {
            "answer": "Figure 5A",
            "excerpts": [{"text": "Figure 5A shows a blue bar chart of PTP signature motifs", "source": "doc"}],
        },
    }
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "doc")
    assert result["supporting_figure"]["answer"] is None
