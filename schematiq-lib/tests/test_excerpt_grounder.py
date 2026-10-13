"""Tests for excerpt grounding / hallucination detection."""

import pytest

from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder


class TestExcerptGrounder:
    def setup_method(self):
        self.grounder = ExcerptGrounder()

    def test_exact_match(self):
        source = "GPT-4 achieves 86.4% accuracy on the MMLU benchmark."
        start, end, status = self.grounder.ground_excerpt("86.4% accuracy", source)
        assert status == "exact"
        assert source[start:end] == "86.4% accuracy"

    def test_case_insensitive_match(self):
        source = "The BERT model was fine-tuned on SQuAD."
        start, end, status = self.grounder.ground_excerpt("the bert model", source)
        assert status == "case_insensitive"

    def test_fuzzy_match(self):
        source = "The model achieved an accuracy of 86.4 percent on MMLU."
        start, end, status = self.grounder.ground_excerpt(
            "accuracy of 86.4% on MMLU", source
        )
        assert status == "fuzzy"

    def test_not_found(self):
        source = "This paper discusses climate change."
        start, end, status = self.grounder.ground_excerpt(
            "GPT-4 achieves 86.4%", source
        )
        assert status == "not_found"
        assert start is None
        assert end is None

    def test_empty_excerpt(self):
        start, end, status = self.grounder.ground_excerpt("", "some text")
        assert status == "not_found"

    def test_empty_source(self):
        start, end, status = self.grounder.ground_excerpt("test", "")
        assert status == "not_found"

    def test_short_excerpt_no_fuzzy(self):
        """Excerpts shorter than 3 words skip fuzzy matching."""
        source = "The cat sat on the mat."
        start, end, status = self.grounder.ground_excerpt("dog", source)
        assert status == "not_found"

    def test_fuzzy_match_with_irregular_spacing(self):
        """A real fuzzy match must not be lost to double-spaced source text.

        Docling leaves PDF-justification double-space artifacts in extracted
        text (e.g. "Fig.  1 a, Stimulation  by  SEM..."). Rejoining the
        matched word window with single spaces and re-`.find()`-ing it
        against the original (irregularly spaced) source used to make a real
        match report "not_found" purely on whitespace.
        """
        source = "The  model  achieved  an  accuracy  of  86.4  percent  on  MMLU."
        start, end, status = self.grounder.ground_excerpt(
            "accuracy of 86.4% on MMLU", source
        )
        assert status == "fuzzy"
        assert start is not None and end is not None
        # 5-word window (matching the excerpt's own 5 tokens); the excerpt's
        # "86.4%" folds source's "86.4"+"percent" into one token, so the
        # window can't also reach "MMLU" -- the important thing is the
        # double-spacing between real words was preserved, not collapsed or
        # mis-searched.
        assert source[start:end] == "accuracy  of  86.4  percent  on"

    def test_fuzzy_match_offsets_point_at_real_span(self):
        """The returned (start, end) must slice out the actual matched words,
        not an offset recovered by re-searching a differently-spaced string."""
        source = "Intro text. The model achieved an accuracy of 86.4 percent on MMLU. Outro."
        start, end, status = self.grounder.ground_excerpt(
            "accuracy of 86.4% on MMLU", source
        )
        assert status == "fuzzy"
        assert source[start:end] == "accuracy of 86.4 percent on"


class TestGroundAllExcerpts:
    def setup_method(self):
        self.grounder = ExcerptGrounder()

    def test_ground_string_excerpts(self):
        source = "GPT-4 achieves 86.4% accuracy on MMLU."
        result = {
            "model": {
                "answer": "GPT-4",
                "excerpts": ["GPT-4 achieves 86.4% accuracy"],
            },
        }
        stats = self.grounder.ground_all_excerpts(result, source)
        assert stats["exact"] == 1
        # Excerpts should now be dicts with grounding info
        exc = result["model"]["excerpts"][0]
        assert isinstance(exc, dict)
        assert exc["grounding_status"] == "exact"
        assert exc["char_start"] is not None

    def test_skip_metadata_keys(self):
        result = {
            "_source_document": "test.pdf",
            "model": {"answer": "GPT-4", "excerpts": ["GPT-4"]},
        }
        stats = self.grounder.ground_all_excerpts(result, "GPT-4 is great")
        assert stats["exact"] == 1
        # Metadata key should be unchanged
        assert result["_source_document"] == "test.pdf"

    def test_mixed_grounding_statuses(self):
        source = "GPT-4 achieves 86.4% accuracy on MMLU benchmark."
        result = {
            "model": {
                "answer": "GPT-4",
                "excerpts": [
                    "GPT-4 achieves 86.4%",
                    "This excerpt does not exist in the source at all whatsoever",
                ],
            },
        }
        stats = self.grounder.ground_all_excerpts(result, source)
        assert stats["exact"] >= 1
        assert stats["not_found"] >= 1

    def test_empty_extraction(self):
        stats = self.grounder.ground_all_excerpts({}, "some source text")
        assert stats == {"exact": 0, "case_insensitive": 0, "fuzzy": 0, "not_found": 0}

    def test_duplicate_excerpt_text_both_resolve_to_the_same_sentence(self):
        """Two different columns can legitimately cite the exact same
        sentence (it supports two different extracted facts). Both must
        resolve to that real occurrence -- nothing should force them apart
        onto different, unrelated occurrences just because their text is
        identical."""
        source = (
            "Results showed wild-type vs ST3Gal-I deficient mice differed. "
            "Figure 2 legend: wild-type vs ST3Gal-I deficient comparison shown."
        )
        result = {
            "finding_1": {
                "answer": "difference observed",
                "excerpts": ["wild-type vs ST3Gal-I deficient"],
            },
            "finding_2": {
                "answer": "figure comparison",
                "excerpts": ["wild-type vs ST3Gal-I deficient"],
            },
        }
        self.grounder.ground_all_excerpts(result, source)
        first_start = result["finding_1"]["excerpts"][0]["char_start"]
        second_start = result["finding_2"]["excerpts"][0]["char_start"]
        assert first_start == second_start == source.index("wild-type vs ST3Gal-I deficient")

    def test_short_excerpt_with_irregular_spacing_still_grounds(self):
        """A short (<3-word) excerpt that only differs from the source by
        whitespace can't reach the fuzzy phase (which requires >= 3 words),
        so it needs the whitespace-stripped phase to ground at all."""
        source = "The comparison of ST3Gal-I  deficient mice was notable."
        start, end, status = self.grounder.ground_excerpt("ST3Gal-I deficient", source)
        assert status != "not_found"
        assert start is not None and end is not None
        assert source[start:end] == "ST3Gal-I  deficient"

    def test_excerpt_grounds_against_source_with_missing_whitespace(self):
        """Real extracted PDF text can drop inter-word spacing entirely
        across a stretch (a font/encoding artifact distinct from
        PDF-justification double-spacing), sometimes with a line-wrap
        newline where the model's own quoted excerpt has a plain space.
        Collapsing whitespace RUNS doesn't help when there's no whitespace
        there to collapse -- only stripping it entirely on both sides
        does. Modeled on a real example found in production data."""
        source = (
            "unaffected(rightpanel).Inexperimentsabove,resultswithmice\n"
            "homozygousforwild-type(whiteboxes)ordeleted(blackboxes)ST3Gal-Igenotypesarepresented."
        )
        excerpt = (
            "Inexperimentsabove,resultswithmice "
            "homozygousforwild-type(whiteboxes)ordeleted(blackboxes)ST3Gal-Igenotypesarepresented."
        )
        start, end, status = self.grounder.ground_excerpt(excerpt, source)
        assert status != "not_found"
        assert start is not None and end is not None

    def test_excerpt_grounds_despite_unicode_dash_variant(self):
        """Real example: PDF text extraction used an en dash (U+2013) where
        the model's own quoted excerpt used a plain ASCII hyphen. Modeled
        on a real production example ("a2–3-linked" in the source vs
        "a2-3-linked" in the excerpt)."""
        source = "reducedcellsurfacelevelsofa2–3-linkedsialicacidsareevidentoncells."
        excerpt = "reduced cell surface levels of a2-3-linked sialic acids"
        start, end, status = self.grounder.ground_excerpt(excerpt, source)
        assert status != "not_found"
        assert start is not None and end is not None

    def test_excerpt_grounds_despite_line_wrap_hyphenation(self):
        """Real example: a print-layout line wrap hyphenated a word
        ("recombina-\\ntion") in the source, but the model's excerpt quotes
        it as one unbroken word ("recombination")."""
        source = "thymicTcellsundergorecombina-\ntionandexhibitincreasedPNAbinding."
        excerpt = "thymic T cells undergo recombination and exhibit increased PNA binding"
        start, end, status = self.grounder.ground_excerpt(excerpt, source)
        assert status != "not_found"
        assert start is not None and end is not None

    def test_genuine_compound_hyphen_is_not_altered(self):
        """A real compound word's hyphen (no adjacent whitespace, e.g.
        "T-cell") must not be treated as a line-wrap artifact and dropped
        -- only a hyphen immediately followed by whitespace containing a
        newline should be removed."""
        normalized, _ = self.grounder._normalize_for_matching("T-cell activation")
        assert normalized == "T-cellactivation"

    def test_figure_typed_excerpt_does_not_crash_and_is_left_unchanged(self):
        """A figure-derived excerpt ({"type": "figure", ...}, produced by
        PaperProcessor._attach_source_to_excerpts for vision-derived
        answers) has no "text" key. ground_all_excerpts must skip it
        rather than crash, and must not stamp grounding fields onto it --
        it's rendered via a separate figure/image UI path, not text
        highlighting."""
        source = "Some unrelated body text about a different topic entirely."
        figure_excerpt = {
            "type": "figure",
            "figure_id": "fig-3",
            "source": "paper.pdf",
            "caption": "Figure 3. A schematic model.",
            "image_filename": "fig3.png",
        }
        result = {
            "col_a": {"answer": "a", "excerpts": [dict(figure_excerpt)]},
        }
        stats = self.grounder.ground_all_excerpts(result, source)
        assert stats == {"exact": 0, "case_insensitive": 0, "fuzzy": 0, "not_found": 0}
        exc = result["col_a"]["excerpts"][0]
        assert exc == figure_excerpt
        assert "grounding_status" not in exc

    def test_figure_typed_excerpt_mixed_with_text_excerpt(self):
        """A figure excerpt alongside a normal text excerpt in the same
        call: the text excerpt still grounds normally and the figure
        excerpt is skipped without affecting it."""
        source = "GPT-4 achieves 86.4% accuracy on MMLU."
        result = {
            "col_a": {
                "answer": "a",
                "excerpts": [
                    {"type": "figure", "figure_id": "fig-1", "source": "p.pdf"},
                    "GPT-4 achieves 86.4% accuracy",
                ],
            },
        }
        stats = self.grounder.ground_all_excerpts(result, source)
        assert stats["exact"] == 1
        excerpts = result["col_a"]["excerpts"]
        assert excerpts[0].get("type") == "figure"
        assert "grounding_status" not in excerpts[0]
        assert excerpts[1]["grounding_status"] == "exact"
