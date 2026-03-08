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
