"""Tests for the figure-citation pipeline added on top of figure vision context:
the model can cite a `figure_id` it was shown (see _build_figure_label /
test_figure_vision_context.py) via a new `figure_refs` field, and that gets
validated and folded into the cell's `excerpts` as a distinct figure-typed
entry, all the way through row merging.

Covers, in order of the data flow:
1. schema_builder — figure_refs is offered to the model, optional.
2. json_parser — figure_refs survives _normalize_parsed and postprocess
   (both were previously reconstructing their output dicts from scratch
   without it), and doesn't cause a figure-only-backed answer to be treated
   as an unsupported placeholder.
3. paper_processor._attach_source_to_excerpts — a validated figure_refs
   entry becomes a real figure excerpt; an unresolvable one is dropped.
4. row_manager.merge_row_data — figure excerpts survive a row merge instead
   of being silently dropped by the text-based dedup key.

Follows the repo pattern of driving PaperProcessor methods unbound with a
MagicMock `self` (see test_figure_vision_context.py), so no real LLM or
on-disk PDF pipeline is needed.
"""
from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from schematiq.value_extraction.core.json_parser import JSONResponseParser
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.core.row_manager import RowDataManager
from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder
from schematiq.value_extraction.utils.schema_builder import build_extraction_response_schema


# ---------------------------------------------------------------------------
# 1. schema_builder: figure_refs is offered to the model
# ---------------------------------------------------------------------------

@dataclass
class FakeColumn:
    name: str
    rationale: str = ""
    definition: str = ""
    allowed_values: Optional[List[str]] = None


class TestSchemaOffersFigureRefs:
    def test_figure_refs_present_on_every_column_and_optional(self):
        schema, _key_map = build_extraction_response_schema([FakeColumn(name="model_type")])
        col_schema = schema["properties"]["model_type"]

        assert col_schema["properties"]["figure_refs"] == {
            "type": "ARRAY", "items": {"type": "STRING"},
        }
        # Optional: a text-backed answer must still validate without it.
        assert "figure_refs" not in col_schema["required"]
        assert set(col_schema["required"]) == {"answer", "excerpts"}


# ---------------------------------------------------------------------------
# 2. json_parser: figure_refs survives normalization and postprocessing
# ---------------------------------------------------------------------------

@pytest.fixture
def parser() -> JSONResponseParser:
    return JSONResponseParser()


class TestNormalizeParsedCarriesFigureRefs:
    def test_figure_refs_list_is_preserved(self, parser: JSONResponseParser) -> None:
        norm = parser._normalize_parsed({
            "model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]},
        })
        assert norm["model_type"]["figure_refs"] == ["paper1_fig003"]

    def test_missing_figure_refs_defaults_to_empty_list(self, parser: JSONResponseParser) -> None:
        norm = parser._normalize_parsed({"model_type": {"answer": "mamba", "excerpts": []}})
        assert norm["model_type"]["figure_refs"] == []

    def test_non_list_figure_refs_is_coerced_to_a_list(self, parser: JSONResponseParser) -> None:
        # Same defensive coercion pattern already applied to excerpts: a
        # malformed (non-list) value from the model must not raise downstream.
        norm = parser._normalize_parsed({
            "model_type": {"answer": "mamba", "excerpts": [], "figure_refs": "paper1_fig003"},
        })
        assert norm["model_type"]["figure_refs"] == ["paper1_fig003"]

    def test_non_dict_column_value_still_gets_empty_figure_refs(self, parser: JSONResponseParser) -> None:
        norm = parser._normalize_parsed({"model_type": "mamba"})
        assert norm["model_type"]["figure_refs"] == []


class TestPostprocessCarriesFigureRefs:
    def test_figure_refs_reaches_the_final_output_dict(self, parser: JSONResponseParser) -> None:
        # postprocess() rebuilds out[col] from scratch — this pins that
        # figure_refs is explicitly copied over, not silently dropped.
        parsed = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]}}
        out, _unmatched = parser.postprocess(parsed, ["model_type"])
        assert out["model_type"]["figure_refs"] == ["paper1_fig003"]

    def test_absent_figure_refs_key_is_not_added(self, parser: JSONResponseParser) -> None:
        # Matches the existing normalized_to_allowed/unmatched_value pattern:
        # only present when non-empty, keeping a plain text-backed cell's
        # output shape unchanged.
        parsed = {"model_type": {"answer": "mamba", "excerpts": ["found it"], "figure_refs": []}}
        out, _unmatched = parser.postprocess(parsed, ["model_type"])
        assert "figure_refs" not in out["model_type"]

    def test_figure_only_backed_answer_is_not_treated_as_placeholder(
        self, parser: JSONResponseParser,
    ) -> None:
        # An answer with no text excerpt but a real figure_refs citation must
        # not be dropped by _is_placeholder just because its (text) excerpts
        # list is empty — the figure citation counts as "gave evidence" too.
        parsed = {
            "chart_color": {
                "answer": "unknown",  # matches the placeholder regex on its own
                "excerpts": [],
                "figure_refs": ["paper1_fig003"],
            },
        }
        out, _unmatched = parser.postprocess(parsed, ["chart_color"])
        assert "chart_color" in out
        assert out["chart_color"]["answer"] == "unknown"

    def test_placeholder_with_no_excerpts_and_no_figure_refs_is_still_dropped(
        self, parser: JSONResponseParser,
    ) -> None:
        # Regression check: the placeholder guard itself must still work
        # for a genuinely unsupported answer (no text excerpt, no figure).
        parsed = {"chart_color": {"answer": "unknown", "excerpts": [], "figure_refs": []}}
        out, _unmatched = parser.postprocess(parsed, ["chart_color"])
        assert "chart_color" not in out


# ---------------------------------------------------------------------------
# 3. paper_processor._attach_source_to_excerpts: validate + attach
# ---------------------------------------------------------------------------

def _make_self(figures_by_id=None):
    s = MagicMock()
    s._active_figures_by_id = figures_by_id or {}
    # _attach_source_to_excerpts now calls self._match_excerpt_to_figure_caption
    # and self.excerpt_grounder internally -- a bare MagicMock attribute for
    # either would return a truthy Mock instead of running the real caption
    # matching, so every plain-text excerpt would spuriously "match" and
    # KeyError. Wire the real bound method + a real grounder onto the mock.
    s.excerpt_grounder = ExcerptGrounder()
    s._match_excerpt_to_figure_caption = lambda text: PaperProcessor._match_excerpt_to_figure_caption(s, text)
    return s


class TestAttachSourceToExcerptsFigureHandling:
    def test_validated_figure_ref_becomes_a_figure_excerpt(self):
        s = _make_self(figures_by_id={
            "paper1_fig003": {"caption": "Fig 3. Accuracy by model size.", "image_filename": "fig003.png"},
        })
        data = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        excerpts = result["model_type"]["excerpts"]
        assert excerpts == [{
            "type": "figure",
            "figure_id": "paper1_fig003",
            "source": "paper1.pdf",
            "caption": "Fig 3. Accuracy by model size.",
            "image_filename": "fig003.png",
        }]

    def test_figure_refs_key_is_consumed_not_stored(self):
        s = _make_self(figures_by_id={"paper1_fig003": {}})
        data = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        assert "figure_refs" not in result["model_type"]

    def test_unresolvable_figure_id_is_dropped_not_stored(self):
        # The model cited a figure_id that was never actually attached this
        # run (hallucinated, or from a different document) — must not be
        # trusted as a real citation.
        s = _make_self(figures_by_id={"paper1_fig001": {}})
        data = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig999"]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        assert result["model_type"]["excerpts"] == []

    def test_figure_refs_with_no_active_figures_is_a_safe_noop(self):
        # No figures were loaded/attached for this document at all — every
        # cited figure_id is unresolvable by definition. Must not raise.
        s = _make_self(figures_by_id={})
        data = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        assert result["model_type"]["excerpts"] == []

    def test_missing_active_figures_by_id_attribute_is_a_safe_noop(self):
        # Defensive: a `self` that never set up _active_figures_by_id at all
        # (getattr fallback) must not raise, matching _active_figure_images'
        # own degrade-gracefully contract.
        s = MagicMock(spec=[])
        data = {"model_type": {"answer": "mamba", "excerpts": [], "figure_refs": ["paper1_fig003"]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        assert result["model_type"]["excerpts"] == []

    def test_text_excerpts_still_wrapped_as_before(self):
        # Regression: plain-string excerpts must still get {text, source}.
        s = _make_self()
        data = {"model_type": {"answer": "mamba", "excerpts": ["We propose Mamba..."]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        assert result["model_type"]["excerpts"] == [
            {"text": "We propose Mamba...", "source": "paper1.pdf"},
        ]

    def test_text_and_figure_excerpts_coexist_on_one_cell(self):
        s = _make_self(figures_by_id={"paper1_fig003": {"caption": "c", "image_filename": "f.png"}})
        data = {
            "model_type": {
                "answer": "mamba",
                "excerpts": ["We propose Mamba..."],
                "figure_refs": ["paper1_fig003"],
            },
        }

        result = PaperProcessor._attach_source_to_excerpts(s, data, "paper1.pdf")

        excerpts = result["model_type"]["excerpts"]
        assert len(excerpts) == 2
        assert excerpts[0] == {"text": "We propose Mamba...", "source": "paper1.pdf"}
        assert excerpts[1]["type"] == "figure"
        assert excerpts[1]["figure_id"] == "paper1_fig003"


# ---------------------------------------------------------------------------
# 3b. _match_excerpt_to_figure_caption: catch the model quoting a caption as
# a plain text excerpt instead of using figure_refs (the actual failure mode
# observed against a real session — see class docstring).
# ---------------------------------------------------------------------------

# A real caption pulled from a live session's manifest.json (Docling extraction
# of a real paper). Deliberately kept verbatim, including its double-space
# PDF-justification artifacts ("Fig.  1 a,", "Stimulation  by  SEM") — this
# whitespace irregularity is exactly what broke the naive first version of
# this matcher (confirmed empirically before writing this test): the model's
# quoted excerpt uses normal single spacing, so without normalizing both
# strings before comparing, ground_excerpt's own re-verification step
# (`source_text.find(single_spaced_match)` against the *original*
# double-spaced caption) silently failed and reported "not_found" even
# though the words matched exactly.
REAL_CAPTION_WITH_DOUBLE_SPACES = (
    "Fig.  1 a, Stimulation  by  SEM of POE production  by  chondrocytes. "
    "The results represent the mean ± standard error of determinations "
    "in quadruplicate wells. b, Stimulation  by  SEM  of  production  of  "
    "plasminogen  activator  by  human  articular chondrocytes."
)
REAL_MODEL_QUOTED_EXCERPT = "Fig. 1 a, Stimulation by SEM of POE production by chondrocytes."


class TestMatchExcerptToFigureCaption:
    def test_real_observed_case_matches_despite_caption_double_spacing(self):
        # This is the exact case that motivated this feature: without
        # whitespace normalization, this returns not_found even though the
        # excerpt is a verbatim (single-spaced) quote of the caption.
        s = _make_self(figures_by_id={
            "286888a0_fig004": {"caption": REAL_CAPTION_WITH_DOUBLE_SPACES},
        })

        figure_id = s._match_excerpt_to_figure_caption(REAL_MODEL_QUOTED_EXCERPT)

        assert figure_id == "286888a0_fig004"

    def test_attach_source_converts_the_real_observed_cell(self):
        # End-to-end through the public method, matching the actual shape
        # pulled from a live session's extracted_data.jsonl.
        s = _make_self(figures_by_id={
            "286888a0_fig004": {
                "caption": REAL_CAPTION_WITH_DOUBLE_SPACES,
                "image_filename": "fig004.png",
            },
        })
        data = {"figure_id": {"answer": "Fig. 1", "excerpts": [REAL_MODEL_QUOTED_EXCERPT]}}

        result = PaperProcessor._attach_source_to_excerpts(s, data, "286888a0")

        excerpts = result["figure_id"]["excerpts"]
        assert len(excerpts) == 1
        assert excerpts[0]["type"] == "figure"
        assert excerpts[0]["figure_id"] == "286888a0_fig004"
        assert excerpts[0]["image_filename"] == "fig004.png"

    def test_unrelated_excerpt_does_not_match(self):
        # False-positive guard: a real, substantial excerpt about something
        # else entirely must not spuriously match an unrelated caption.
        s = _make_self(figures_by_id={
            "286888a0_fig004": {"caption": REAL_CAPTION_WITH_DOUBLE_SPACES},
        })

        figure_id = s._match_excerpt_to_figure_caption(
            "Patients were recruited from three tertiary care centers between 2019 and 2021.",
        )

        assert figure_id is None

    def test_short_generic_caption_does_not_match_unrelated_text(self):
        # The false-positive trade-off flagged in the plan: a short/generic
        # caption is exactly the risky case. ground_excerpt's own >=3-word
        # requirement for the fuzzy phase (and no exact/case-insensitive
        # substring hit) should keep this safe.
        s = _make_self(figures_by_id={
            "paper1_fig001": {"caption": "Figure 1: Results."},
        })

        figure_id = s._match_excerpt_to_figure_caption(
            "The results of this study demonstrate a significant effect across all cohorts tested.",
        )

        assert figure_id is None

    def test_matches_the_right_one_of_several_figures(self):
        s = _make_self(figures_by_id={
            "paper1_fig001": {"caption": "Fig. 1. Distribution of patient ages across the cohort."},
            "paper1_fig002": {"caption": REAL_CAPTION_WITH_DOUBLE_SPACES},
            "paper1_fig003": {"caption": "Fig. 3. Kaplan-Meier survival curve stratified by treatment arm."},
        })

        figure_id = s._match_excerpt_to_figure_caption(REAL_MODEL_QUOTED_EXCERPT)

        assert figure_id == "paper1_fig002"

    def test_best_scoring_match_wins_not_the_first_to_clear_the_threshold(self):
        # Regression for the "shows me the wrong figure" bug: when more than
        # one caption clears the 0.6 gate, the closest match must win even if
        # a weaker match appears earlier in the manifest. caption_low
        # trivially clears the gate (the excerpt is verbatim embedded in a
        # much longer, mostly-unrelated caption -- an "exact" substring hit)
        # but is a poor overall match; caption_high is a near-verbatim quote
        # (differs by one digit) with a much higher whole-string similarity.
        # caption_low is listed first, so the old first-match behavior would
        # have picked it.
        excerpt = "Revenue growth of twenty percent was observed across all regions in 2020."
        caption_low = (
            "Figure 7. Overview of full company operations. Revenue growth of "
            "twenty percent was observed across all regions in 2020. Additional "
            "discussion of staffing, logistics, and five-year strategic plan "
            "follows in the appendix below for reference."
        )
        caption_high = "Revenue growth of twenty percent was observed across all regions in 2019."
        s = _make_self(figures_by_id={
            "paper1_fig007": {"caption": caption_low},
            "paper1_fig008": {"caption": caption_high},
        })

        figure_id = s._match_excerpt_to_figure_caption(excerpt)

        assert figure_id == "paper1_fig008"

    def test_no_figures_attached_returns_none_without_raising(self):
        s = _make_self(figures_by_id={})
        assert s._match_excerpt_to_figure_caption(REAL_MODEL_QUOTED_EXCERPT) is None

    def test_figure_with_no_caption_is_skipped_not_raised(self):
        s = _make_self(figures_by_id={"paper1_fig001": {}})
        assert s._match_excerpt_to_figure_caption(REAL_MODEL_QUOTED_EXCERPT) is None

    def test_explicit_figure_refs_skips_caption_fallback_for_that_column(self):
        # A column with a resolved figure_refs citation trusts that explicit
        # signal and does not also run the caption-match fallback on its text
        # excerpts -- even one that would otherwise caption-match the same
        # figure stays as plain text, so the model's quote isn't silently
        # discarded, and the fallback never gets a chance to (mis)guess a
        # *different* figure for an unrelated excerpt in the same column.
        s = _make_self(figures_by_id={
            "286888a0_fig004": {
                "caption": REAL_CAPTION_WITH_DOUBLE_SPACES,
                "image_filename": "fig004.png",
            },
        })
        data = {
            "figure_id": {
                "answer": "Fig. 1",
                "excerpts": [REAL_MODEL_QUOTED_EXCERPT],
                "figure_refs": ["286888a0_fig004"],
            },
        }

        result = PaperProcessor._attach_source_to_excerpts(s, data, "286888a0")

        excerpts = result["figure_id"]["excerpts"]
        assert len(excerpts) == 2
        assert excerpts[0] == {"text": REAL_MODEL_QUOTED_EXCERPT, "source": "286888a0"}
        assert excerpts[1]["type"] == "figure"
        assert excerpts[1]["figure_id"] == "286888a0_fig004"


# ---------------------------------------------------------------------------
# 4. row_manager.merge_row_data: figure excerpts survive a merge
# ---------------------------------------------------------------------------

def _figure_excerpt(figure_id, source="paper1.pdf", caption="c"):
    return {"type": "figure", "figure_id": figure_id, "source": source, "caption": caption}


class TestMergeRowDataPreservesFigureExcerpts:
    def test_figure_excerpt_survives_merge_with_an_empty_existing_cell(self):
        # Before the fix: excerpt_text defaulted to '' for a figure entry,
        # which is falsy, so it was unconditionally dropped by the `if
        # excerpt_clean and ...` guard regardless of dedup.
        rm = RowDataManager()
        existing = {"_row_name": "Unit A", "_papers": ["paper1"], "col": {"answer": "", "excerpts": []}}
        new = {"_row_name": "Unit A", "col": {
            "answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig003")],
        }}

        merged = rm.merge_row_data(existing, new, "paper1")

        assert merged["col"]["excerpts"] == [_figure_excerpt("paper1_fig003")]

    def test_duplicate_figure_id_is_deduped(self):
        rm = RowDataManager()
        existing = {
            "_row_name": "Unit A", "_papers": ["paper1"],
            "col": {"answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig003")]},
        }
        new = {"_row_name": "Unit A", "col": {
            "answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig003")],
        }}

        merged = rm.merge_row_data(existing, new, "paper1")

        assert len(merged["col"]["excerpts"]) == 1

    def test_distinct_figure_ids_both_survive(self):
        rm = RowDataManager()
        existing = {
            "_row_name": "Unit A", "_papers": ["paper1"],
            "col": {"answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig001")]},
        }
        new = {"_row_name": "Unit A", "col": {
            "answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig002")],
        }}

        merged = rm.merge_row_data(existing, new, "paper1")

        figure_ids = {exc["figure_id"] for exc in merged["col"]["excerpts"]}
        assert figure_ids == {"paper1_fig001", "paper1_fig002"}

    def test_text_and_figure_excerpts_both_survive_together(self):
        rm = RowDataManager()
        existing = {
            "_row_name": "Unit A", "_papers": ["paper1"],
            "col": {"answer": "mamba", "excerpts": [{"text": "We propose Mamba...", "source": "paper1.pdf"}]},
        }
        new = {"_row_name": "Unit A", "col": {
            "answer": "mamba", "excerpts": [_figure_excerpt("paper1_fig003")],
        }}

        merged = rm.merge_row_data(existing, new, "paper1")

        assert len(merged["col"]["excerpts"]) == 2

    def test_duplicate_text_excerpts_are_still_deduped_by_text(self):
        # Regression: the pre-existing text-dedup behavior must be unchanged.
        rm = RowDataManager()
        existing = {
            "_row_name": "Unit A", "_papers": ["paper1"],
            "col": {"answer": "mamba", "excerpts": [{"text": "We propose Mamba...", "source": "paper1.pdf"}]},
        }
        new = {"_row_name": "Unit A", "col": {
            "answer": "mamba",
            "excerpts": [{"text": "We propose Mamba...", "source": "paper1.pdf"}],
        }}

        merged = rm.merge_row_data(existing, new, "paper1")

        assert len(merged["col"]["excerpts"]) == 1
