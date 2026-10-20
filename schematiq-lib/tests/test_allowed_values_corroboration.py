"""Tests for PaperProcessor._enforceable_allowed_values /
_record_allowed_value_confirmations and their effect on cross-document
allowed_values enforcement.

Reproduces the actual bug: a schema's allowed_values, auto-discovered from
one document, must not be force-applied to a different, unrelated
document's real answers until corroborated by enough independent
documents. Follows the repo pattern of driving PaperProcessor methods
unbound with a plain object standing in for `self` (see
test_figure_citations.py), so no real LLM or on-disk pipeline is needed.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from schematiq.core.schema import Column
from schematiq.value_extraction.core.json_parser import JSONResponseParser
from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder


@dataclass
class FakeColumn:
    name: str
    allowed_values: Optional[List[str]] = None
    auto_expand_threshold: Optional[int] = 2


class FakeLLM:
    """Stand-in for the LLM backend, used to test _semantic_matcher without
    a real API call."""

    def __init__(self, reply: str = "NONE"):
        self.reply = reply
        self.calls: List[str] = []

    def generate(self, prompt, **kwargs):
        self.calls.append(prompt)
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


class FakeSelf:
    """Minimal stand-in for a PaperProcessor instance."""

    def __init__(self, llm: Optional[FakeLLM] = None):
        self.json_parser = JSONResponseParser()
        self._allowed_value_confirmations: Dict[str, Dict[str, Set[str]]] = {}
        if llm is not None:
            self.llm = llm


def _enforceable(fake_self: FakeSelf, column: FakeColumn) -> List[str]:
    return PaperProcessor._enforceable_allowed_values(fake_self, column)


def _record(fake_self: FakeSelf, parsed: dict, columns: List[FakeColumn], document: str) -> None:
    PaperProcessor._record_allowed_value_confirmations(fake_self, parsed, columns, document)


class TestEnforceableAllowedValues:
    def test_freshly_seeded_value_not_yet_enforceable(self):
        """A value seen in only the single document that produced it (no
        recorded confirmations yet) is below threshold 2 -- not enforceable."""
        s = FakeSelf()
        column = FakeColumn(name="key_finding", allowed_values=["CD45 D1D2 tandem domains"])
        assert _enforceable(s, column) == []

    def test_value_becomes_enforceable_once_corroborated(self):
        """Once threshold (2) DISTINCT documents have independently produced
        the same value, it graduates to enforceable."""
        s = FakeSelf()
        column = FakeColumn(name="graph_type", allowed_values=["bar chart"])
        s._allowed_value_confirmations["graph_type"] = {"bar chart": {"paper1.pdf", "paper2.pdf"}}
        assert _enforceable(s, column) == ["bar chart"]

    def test_only_corroborated_values_pass_through_others_stay_suppressed(self):
        """Filtering is per-value, not all-or-nothing for the column."""
        s = FakeSelf()
        column = FakeColumn(
            name="key_finding",
            allowed_values=["bar chart finding", "CD45 D1D2 tandem domains"],
        )
        s._allowed_value_confirmations["key_finding"] = {
            "bar chart finding": {"paper1.pdf", "paper2.pdf"},
            "CD45 D1D2 tandem domains": {"paper3.pdf"},
        }
        assert _enforceable(s, column) == ["bar chart finding"]

    def test_seeding_document_alone_is_not_enough(self):
        """Regression test for the real cross-document-contamination bug:
        a value confirmed only by the single document that seeded it in
        the schema (no independent second document) must NOT be
        enforceable at the default threshold=2 -- otherwise that
        document's own answer (e.g. one figure's own title) gets pushed
        onto every other, unrelated document as a "preferred" value the
        moment schema discovery happens to seed it. This is exactly what
        was observed in production: a figure_title seeded from one paper
        was broadcast, unenforced-by-anyone-else, into six other
        documents' extraction prompts."""
        s = FakeSelf()
        column = FakeColumn(name="figure_title", allowed_values=["Some Figure's Own Title"])
        s._allowed_value_confirmations["figure_title"] = {
            "Some Figure's Own Title": {"the_seeding_document.pdf"},
        }
        assert _enforceable(s, column) == []

    def test_higher_threshold_needs_more_corroboration(self):
        s = FakeSelf()
        column = FakeColumn(name="col", allowed_values=["v"], auto_expand_threshold=3)
        s._allowed_value_confirmations["col"] = {"v": {"paper1.pdf", "paper2.pdf"}}
        assert _enforceable(s, column) == []  # only 2 documents, needs 3

        s._allowed_value_confirmations["col"]["v"].add("paper3.pdf")
        assert _enforceable(s, column) == ["v"]

    def test_disabled_threshold_passes_through_unchanged(self):
        """auto_expand_threshold None/0 disables the gate -- today's behavior."""
        s = FakeSelf()
        column = FakeColumn(name="col", allowed_values=["v"], auto_expand_threshold=None)
        assert _enforceable(s, column) == ["v"]

        column2 = FakeColumn(name="col", allowed_values=["v"], auto_expand_threshold=0)
        assert _enforceable(s, column2) == ["v"]

    def test_no_allowed_values_returns_empty(self):
        s = FakeSelf()
        column = FakeColumn(name="col", allowed_values=None)
        assert _enforceable(s, column) == []

    def test_type_constraints_are_always_enforceable_not_gated(self):
        """A single-item allowed_values list is a TYPE/format constraint
        (date/number/min-max range) in _normalize_to_allowed_values, not a
        categorical value -- it must never be gated behind corroboration,
        since _record_allowed_value_confirmations records the ANSWER's own
        parsed value (e.g. "100.0"), never the literal constraint token
        ("0-100"), so a gated type constraint could never accumulate enough
        corroboration to become enforceable and would stay permanently
        disabled."""
        s = FakeSelf()
        for allowed in (["0-100"], ["number"], ["date"], ["date:us"]):
            column = FakeColumn(name="col", allowed_values=allowed)
            assert _enforceable(s, column) == allowed, allowed

    def test_record_confirmations_skips_type_constraint_columns(self):
        """Recording should not create noise entries for type-constraint
        columns -- there's no categorical value there to corroborate."""
        s = FakeSelf()
        column = FakeColumn(name="accuracy_pct", allowed_values=["0-100"])
        parsed = {"accuracy_pct": {"answer": "142", "excerpts": []}}

        _record(s, parsed, [column], "doc1")

        assert s._allowed_value_confirmations == {}


class TestRecordAllowedValueConfirmations:
    def test_matching_answer_recorded_under_canonical_value(self):
        """A document whose raw answer fuzzy-matches an allowed_value gets
        recorded under the CANONICAL allowed_value, not its own raw
        wording, so slightly different phrasings all count toward the same
        corroboration bucket."""
        s = FakeSelf()
        column = FakeColumn(name="graph_type", allowed_values=["bar chart"])
        parsed = {"graph_type": {"answer": "Bar Chart", "excerpts": []}}

        _record(s, parsed, [column], "doc1")

        assert s._allowed_value_confirmations["graph_type"]["bar chart"] == {"doc1"}

    def test_non_matching_answer_not_recorded(self):
        s = FakeSelf()
        column = FakeColumn(name="key_finding", allowed_values=["CD45 D1D2 tandem domains"])
        parsed = {"key_finding": {"answer": "Expression of B3/25 glycoprotein", "excerpts": []}}

        _record(s, parsed, [column], "doc1")

        assert s._allowed_value_confirmations == {}

    def test_column_without_allowed_values_skipped(self):
        s = FakeSelf()
        column = FakeColumn(name="free_text_col", allowed_values=None)
        parsed = {"free_text_col": {"answer": "anything", "excerpts": []}}

        _record(s, parsed, [column], "doc1")

        assert s._allowed_value_confirmations == {}


class TestTypeConstraintEndToEnd:
    def test_numeric_range_clamps_from_the_first_document(self):
        """A numeric-range constraint must clamp immediately -- it's a
        format rule declared by the schema itself, not a value borrowed
        from another document, so it must not wait for corroboration."""
        s = FakeSelf()
        column = FakeColumn(name="accuracy_pct", allowed_values=["0-100"])

        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        parsed = {column.name: {"answer": "142", "excerpts": [{"text": "142%", "source": "doc1"}]}}
        out, _ = s.json_parser.postprocess(parsed, [column.name], {column.name: enforceable})

        assert out[column.name]["answer"] == "100.0"


class TestNormalizationDoesNotInteractBadlyWithGroundAndEnforce:
    """Regression test for a real interaction bug: postprocess previously
    cleared an excerpt whenever normalization changed the answer's content
    (to avoid pairing a changed answer with a possibly-unrelated excerpt).
    But paper_processor.py's _ground_and_enforce -- called right after
    postprocess for any non-vision-derived unit -- nulls the WHOLE answer
    whenever its excerpts end up empty ("claims an answer with no excerpts
    at all"). So clearing a real, source-grounded excerpt didn't just lose
    the citation, it silently deleted the entire answer. postprocess must
    leave the excerpt in place and let _ground_and_enforce's own
    source-text verification decide its fate.
    """

    def test_fuzzy_normalized_answer_with_a_real_excerpt_survives_ground_and_enforce(self):
        s = FakeSelf()
        s.excerpt_grounder = ExcerptGrounder()
        s._active_figure_images = []  # non-vision path -- _ground_and_enforce runs fully

        allowed = ["accuracy of 86.4 percent on the MMLU benchmark for large models"]
        column = FakeColumn(name="key_finding", allowed_values=allowed)
        # already corroborated by threshold (2) distinct documents
        s._allowed_value_confirmations["key_finding"] = {allowed[0]: {"doc0", "doc1"}}

        source_text = (
            "In our experiments we report accuracy of 86.4 percent on the "
            "MMLU benchmark for large models, a strong result."
        )
        real_excerpt_text = "accuracy of 86.4 percent on the MMLU benchmark for large models"
        parsed = {
            column.name: {
                "answer": "accuracy of 86.4% on MMLU benchmark for large models",
                "excerpts": [{"text": real_excerpt_text, "source": "doc2"}],
            }
        }

        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        cleaned, _ = s.json_parser.postprocess(parsed, [column.name], {column.name: enforceable})
        # Confirm normalization actually fired (the interaction only exists
        # when it does) before checking the survival property below.
        assert cleaned[column.name]["answer"] == allowed[0]
        assert cleaned[column.name].get("normalized_to_allowed") is True

        result = PaperProcessor._ground_and_enforce(s, cleaned, source_text, "doc2")

        assert result[column.name]["answer"] is not None, (
            "a normalized answer with a real, source-grounded excerpt must not be nulled"
        )
        assert result[column.name]["excerpts"], "the real excerpt must survive"


class TestEndToEndCrossDocumentContamination:
    """Reproduces the actual observed bug end-to-end, driving the same
    _enforceable_allowed_values -> postprocess -> _record_allowed_value_confirmations
    sequence every real call site uses."""

    def test_second_documents_real_answer_is_not_overwritten(self):
        s = FakeSelf()
        column = FakeColumn(
            name="key_features_visualized",
            allowed_values=["Diffraction resolution, space groups, and crystallographic R-factors"],
        )

        # Document 2 is a real, unrelated document with its own genuine
        # answer -- the CD45-specific value above was seeded by a different
        # document entirely and has no independent corroboration yet.
        doc2_answer = "Expression of B3/25 glycoprotein on induced HL-60 cells"
        parsed = {
            column.name: {
                "answer": doc2_answer,
                "excerpts": [{"text": "B3/25 glycoprotein expression assay", "source": "doc2"}],
            }
        }
        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        out, _ = s.json_parser.postprocess(parsed, [column.name], {column.name: enforceable})
        _record(s, parsed, [column], "doc2")

        # Bug reproduction check: doc2's real answer must survive untouched.
        assert out[column.name]["answer"] == doc2_answer
        assert out[column.name]["excerpts"] == [
            {"text": "B3/25 glycoprotein expression assay", "source": "doc2"}
        ]
        assert "normalized_to_allowed" not in out[column.name]

    def test_value_confirmed_by_a_second_document_then_enforces_on_a_third(self):
        s = FakeSelf()
        column = FakeColumn(name="graph_type", allowed_values=["bar chart"])

        # Doc 1 (the document that seeded this value): not yet corroborated
        # by anyone else, so its own answer isn't force-enforced either --
        # it's recorded as a real, matched-but-untrusted occurrence instead.
        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        assert enforceable == []
        parsed1 = {column.name: {"answer": "bar chart", "excerpts": [{"text": "a bar chart", "source": "doc1"}]}}
        out1, _ = s.json_parser.postprocess(parsed1, [column.name], {column.name: enforceable})
        assert "normalized_to_allowed" not in out1[column.name]
        _record(s, parsed1, [column], "doc1")

        # Doc 1 alone is still not enough -- threshold (2) requires a
        # second, genuinely independent document before this graduates to
        # enforceable (the seeding document's own match doesn't count as
        # its own corroboration).
        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        assert enforceable == []
        parsed2 = {column.name: {"answer": "Bar Chart", "excerpts": [{"text": "shown as bars", "source": "doc2"}]}}
        out2, _ = s.json_parser.postprocess(parsed2, [column.name], {column.name: enforceable})
        assert out2[column.name]["answer"] == "Bar Chart"
        assert "normalized_to_allowed" not in out2[column.name]
        _record(s, parsed2, [column], "doc2")

        # Now two distinct documents (doc1, doc2) have independently
        # produced this value -- threshold (2) is met, so it graduates to
        # enforceable for doc 3 onward.
        enforceable = PaperProcessor._enforceable_allowed_values(s, column)
        assert enforceable == ["bar chart"]
        parsed3 = {column.name: {"answer": "Bar chart", "excerpts": [{"text": "displayed via bars", "source": "doc3"}]}}
        out3, _ = s.json_parser.postprocess(parsed3, [column.name], {column.name: enforceable})
        assert out3[column.name]["answer"] == "bar chart"
        assert out3[column.name].get("normalized_to_allowed") is True


class TestColumnDictForPrompt:
    """PaperProcessor._column_dict_for_prompt: the column spec dict actually
    sent to the extraction LLM must never include a categorical
    allowed_values list (that's the whole point of this redesign -- the
    model should never be able to just copy a previously-accepted answer
    instead of extracting its own), but real format constraints (date /
    number / range) should still be shown, since those describe expected
    OUTPUT FORMAT, not "borrowed content" risk."""

    def _dict_for_prompt(self, col: Column) -> dict:
        s = FakeSelf()
        return PaperProcessor._column_dict_for_prompt(s, col)

    def test_categorical_allowed_values_is_stripped(self):
        col = Column(name="graph_type", allowed_values=["bar chart", "line plot"])
        d = self._dict_for_prompt(col)
        assert "allowed_values" not in d

    def test_type_constraint_allowed_values_is_kept(self):
        col = Column(name="accuracy_pct", allowed_values=["0-100"])
        d = self._dict_for_prompt(col)
        assert d["allowed_values"] == ["0-100"]

    def test_column_with_no_allowed_values_is_unaffected(self):
        col = Column(name="free_text_col")
        d = self._dict_for_prompt(col)
        assert "allowed_values" not in d
        assert d["column"] == "free_text_col"


class TestSemanticMatcher:
    """PaperProcessor._semantic_matcher: the post-hoc, LLM-backed
    equivalence check that reconciles wording across documents WITHOUT
    ever having shown the model those values during extraction."""

    def test_matching_reply_returns_the_canonical_candidate_verbatim(self):
        s = FakeSelf(llm=FakeLLM(reply="bar chart"))
        result = PaperProcessor._semantic_matcher(
            s, "a chart made of vertical bars", ["bar chart", "line plot"]
        )
        assert result == "bar chart"
        assert len(s.llm.calls) == 1

    def test_none_reply_returns_none(self):
        s = FakeSelf(llm=FakeLLM(reply="NONE"))
        result = PaperProcessor._semantic_matcher(
            s, "something unrelated", ["bar chart", "line plot"]
        )
        assert result is None

    def test_reply_not_in_candidates_is_rejected_not_trusted_verbatim(self):
        """The model must not be able to invent a value that isn't
        actually one of the real candidates -- only an exact, verbatim
        match against allowed_values counts."""
        s = FakeSelf(llm=FakeLLM(reply="a completely made up value"))
        result = PaperProcessor._semantic_matcher(s, "some answer", ["bar chart"])
        assert result is None

    def test_llm_call_failure_is_treated_as_no_match_not_an_exception(self):
        s = FakeSelf(llm=FakeLLM(reply=RuntimeError("API down")))
        result = PaperProcessor._semantic_matcher(s, "some answer", ["bar chart"])
        assert result is None

    def test_no_candidates_short_circuits_without_calling_the_llm(self):
        s = FakeSelf(llm=FakeLLM(reply="bar chart"))
        result = PaperProcessor._semantic_matcher(s, "some answer", [])
        assert result is None
        assert s.llm.calls == []


class TestPostprocessWithSemanticMatcher:
    """postprocess()'s categorical branch, driven with a real
    semantic_matcher callable (as PaperProcessor supplies in production),
    instead of the difflib fallback the rest of this file's tests exercise
    when no matcher is given."""

    def test_semantic_match_normalizes_answer_but_leaves_excerpt_untouched(self):
        s = FakeSelf()
        column = FakeColumn(name="graph_type", allowed_values=["bar chart"])
        matcher = lambda answer, candidates: "bar chart"  # noqa: E731

        real_excerpt = [{"text": "shown as vertical bars in the figure", "source": "doc1"}]
        parsed = {column.name: {"answer": "a bar-style chart", "excerpts": real_excerpt}}

        out, _ = s.json_parser.postprocess(
            parsed, [column.name], {column.name: column.allowed_values}, semantic_matcher=matcher
        )

        assert out[column.name]["answer"] == "bar chart"
        assert out[column.name]["excerpts"] == real_excerpt
        assert out[column.name]["normalized_to_allowed"] is True

    def test_semantic_no_match_keeps_raw_answer_and_flags_unmatched(self):
        s = FakeSelf()
        column = FakeColumn(name="graph_type", allowed_values=["bar chart"])
        matcher = lambda answer, candidates: None  # noqa: E731

        parsed = {column.name: {"answer": "a totally different kind of figure", "excerpts": []}}

        out, unmatched = s.json_parser.postprocess(
            parsed, [column.name], {column.name: column.allowed_values}, semantic_matcher=matcher
        )

        assert out[column.name]["answer"] == "a totally different kind of figure"
        assert "normalized_to_allowed" not in out[column.name]
        assert unmatched.get(column.name) == ["a totally different kind of figure"]
