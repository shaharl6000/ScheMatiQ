"""Unit tests for O(N)-key sorting and precompiled filtering in row_filtering."""

import random
from functools import cmp_to_key

import pytest

from app.services.row_filtering import (
    _get_sortable_value,
    apply_filters,
    apply_search,
    apply_sort,
)


def _reference_apply_sort(rows: list[dict], sort_columns: list[dict]) -> list[dict]:
    """Original O(N log N) pairwise comparator from main for parity verification."""
    if not sort_columns:
        return rows

    sorted_cols = sorted(sort_columns, key=lambda x: x.get("priority", 1))

    def compare_rows(a: dict, b: dict) -> int:
        for sort_col in sorted_cols:
            column = sort_col.get("column", "")
            direction = sort_col.get("direction", "asc")

            a_val = _get_sortable_value(a, column)
            b_val = _get_sortable_value(b, column)

            a_null = a_val is None or a_val == ""
            b_null = b_val is None or b_val == ""

            if a_null and b_null:
                continue
            if a_null:
                return 1
            if b_null:
                return -1

            if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                cmp = (a_val > b_val) - (a_val < b_val)
            else:
                cmp = (str(a_val).lower() > str(b_val).lower()) - (
                    str(a_val).lower() < str(b_val).lower()
                )

            if cmp != 0:
                return cmp if direction == "asc" else -cmp

        return 0

    return sorted(rows, key=cmp_to_key(compare_rows))


class TestApplySort:
    def test_null_and_empty_always_sort_last_in_asc_and_desc(self):
        rows = [
            {"row_name": "r_none", "data": {"score": None}},
            {"row_name": "r_20", "data": {"score": "20"}},
            {"row_name": "r_empty", "data": {"score": ""}},
            {"row_name": "r_neg", "data": {"score": -3.5}},
            {"row_name": "r_ans_empty", "data": {"score": {"answer": ""}}},
            {"row_name": "r_2", "data": {"score": "2"}},
        ]

        asc = apply_sort(rows, [{"column": "score", "direction": "asc", "priority": 1}])
        assert [r["row_name"] for r in asc[:3]] == ["r_neg", "r_2", "r_20"]
        assert [r["row_name"] for r in asc[3:]] == ["r_none", "r_empty", "r_ans_empty"]
        assert asc == _reference_apply_sort(
            rows, [{"column": "score", "direction": "asc", "priority": 1}]
        )

        desc = apply_sort(rows, [{"column": "score", "direction": "desc", "priority": 1}])
        assert [r["row_name"] for r in desc[:3]] == ["r_20", "r_2", "r_neg"]
        assert [r["row_name"] for r in desc[3:]] == ["r_none", "r_empty", "r_ans_empty"]
        assert desc == _reference_apply_sort(
            rows, [{"column": "score", "direction": "desc", "priority": 1}]
        )

    def test_mixed_numbers_strings_case_insensitivity_and_nulls(self):
        rows = [
            {"row_name": "null_row", "data": {"col": None}},
            {"row_name": "str_zeta", "data": {"col": "Zeta"}},
            {"row_name": "num_10", "data": {"col": "10"}},
            {"row_name": "str_alpha", "data": {"col": "alpha"}},
            {"row_name": "num_neg5", "data": {"col": -5}},
            {"row_name": "num_2_5", "data": {"col": {"answer": "2.5"}}},
            {"row_name": "str_beta", "data": {"col": "BETA"}},
        ]

        asc_spec = [{"column": "col", "direction": "asc", "priority": 1}]
        desc_spec = [{"column": "col", "direction": "desc", "priority": 1}]

        asc = apply_sort(rows, asc_spec)
        desc = apply_sort(rows, desc_spec)

        assert [r["row_name"] for r in asc] == [
            "num_neg5",
            "num_2_5",
            "num_10",
            "str_alpha",
            "str_beta",
            "str_zeta",
            "null_row",
        ]
        assert [r["row_name"] for r in desc] == [
            "str_zeta",
            "str_beta",
            "str_alpha",
            "num_10",
            "num_2_5",
            "num_neg5",
            "null_row",
        ]
        assert asc == _reference_apply_sort(rows, asc_spec)
        assert desc == _reference_apply_sort(rows, desc_spec)

    def test_multi_column_priority_and_mixed_directions(self):
        rows = [
            {"row_name": "r1", "papers": ["paper_b"], "data": {"group": "A", "val": 10}},
            {"row_name": "r2", "papers": ["paper_a"], "data": {"group": "a", "val": 30}},
            {"row_name": "r3", "papers": [], "data": {"group": None, "val": 99}},
            {"row_name": "r4", "papers": ["paper_c"], "data": {"group": "B", "val": None}},
            {"row_name": "r5", "papers": ["paper_a"], "data": {"group": "B", "val": 5}},
            {"row_name": "r6", "papers": ["paper_a"], "data": {"group": "A", "val": 30}},
        ]
        # Intentionally pass priorities out of order to verify priority sorting
        sort_cols = [
            {"column": "_papers", "direction": "asc", "priority": 3},
            {"column": "group", "direction": "asc", "priority": 1},
            {"column": "val", "direction": "desc", "priority": 2},
        ]

        actual = apply_sort(rows, sort_cols)
        expected = _reference_apply_sort(rows, sort_cols)
        assert actual == expected
        assert [r["row_name"] for r in actual] == ["r2", "r6", "r1", "r5", "r4", "r3"]

    def test_randomized_parity_against_reference_comparator(self):
        rng = random.Random(42)
        pool_a = [None, "", -100, -0.5, 0, 1, 2, 10, "2.5", "alpha", "Alpha", "beta", "Zebra"]
        pool_b = [None, "", "x", "Y", "z", "-7", "42", 42.0, {"answer": "15"}, {"answer": None}]

        for trial in range(25):
            rows = [
                {
                    "row_name": f"Row_{i}",
                    "papers": [] if i % 5 == 0 else [f"Paper_{i % 4}"],
                    "data": {
                        "a": rng.choice(pool_a),
                        "b": rng.choice(pool_b),
                    },
                }
                for i in range(40)
            ]
            sort_cols = [
                {"column": "a", "direction": rng.choice(["asc", "desc"]), "priority": 1},
                {"column": "b", "direction": rng.choice(["asc", "desc"]), "priority": 2},
                {"column": "_row_name", "direction": rng.choice(["asc", "desc"]), "priority": 3},
            ]
            assert apply_sort(rows, sort_cols) == _reference_apply_sort(rows, sort_cols), f"trial {trial}"


class TestApplyFiltersAndSearch:
    @pytest.fixture
    def sample_rows(self):
        return [
            {
                "row_name": "AlphaStudy",
                "papers": ["smith2023.pdf"],
                "_unit_name": "GPT-4",
                "data": {
                    "acc": {"answer": "91.5"},
                    "verified": "true",
                    "notes": " Strong baseline on MMLU ",
                    "tag": "LLM",
                },
            },
            {
                "row_name": "BetaStudy",
                "papers": ["jones2024.pdf"],
                "_unit_name": "BERT",
                "data": {
                    "acc": "78.0",
                    "verified": False,
                    "notes": "N/A",
                    "tag": "encoder",
                },
            },
            {
                "row_name": "GammaStudy",
                "papers": [],
                "_unit_name": "Llama-3",
                "data": {
                    "acc": None,
                    "verified": "0",
                    "notes": "Evaluated with regex [a-z]+ pattern",
                    "tag": "llm",
                },
            },
        ]

    def test_null_and_boolean_operators(self, sample_rows):
        null_notes = apply_filters(sample_rows, [{"column": "notes", "operator": "isNull"}])
        assert [r["row_name"] for r in null_notes] == ["BetaStudy"]

        non_null_acc = apply_filters(sample_rows, [{"column": "acc", "operator": "isNotNull"}])
        assert [r["row_name"] for r in non_null_acc] == ["AlphaStudy", "BetaStudy"]

        verified_true = apply_filters(sample_rows, [{"column": "verified", "operator": "isTrue"}])
        assert [r["row_name"] for r in verified_true] == ["AlphaStudy"]

        verified_false = apply_filters(sample_rows, [{"column": "verified", "operator": "isFalse"}])
        assert [r["row_name"] for r in verified_false] == ["BetaStudy", "GammaStudy"]

    def test_string_and_regex_operators(self, sample_rows):
        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "tag", "operator": "equals", "value": "llm"}]
            )
        ] == ["AlphaStudy", "GammaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows,
                [{"column": "tag", "operator": "equals", "value": "LLM", "caseSensitive": True}],
            )
        ] == ["AlphaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "_row_name", "operator": "startsWith", "value": "beta"}]
            )
        ] == ["BetaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "_row_name", "operator": "endsWith", "value": "STUDY"}]
            )
        ] == ["AlphaStudy", "BetaStudy", "GammaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "notes", "operator": "regex", "value": r"baseline\s+on"}]
            )
        ] == ["AlphaStudy"]

        # Invalid regex safely returns empty list rather than raising re.error
        assert (
            apply_filters(sample_rows, [{"column": "notes", "operator": "regex", "value": "[unclosed"}])
            == []
        )

    def test_numeric_between_membership_and_combined_and_filters(self, sample_rows):
        assert [
            r["row_name"]
            for r in apply_filters(sample_rows, [{"column": "acc", "operator": "gt", "value": 80}])
        ] == ["AlphaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "acc", "operator": "between", "value": [75, 85]}]
            )
        ] == ["BetaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "tag", "operator": "in", "value": ["Encoder", "Vision"]}]
            )
        ] == ["BetaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "tag", "operator": "notIn", "value": ["encoder"]}]
            )
        ] == ["AlphaStudy", "GammaStudy"]

        assert [
            r["row_name"]
            for r in apply_filters(
                sample_rows, [{"column": "tag", "operator": "not_in", "value": ["encoder"]}]
            )
        ] == ["AlphaStudy", "GammaStudy"]

        combined = apply_filters(
            sample_rows,
            [
                {"column": "tag", "operator": "equals", "value": "llm"},
                {"column": "acc", "operator": "gte", "value": "90"},
            ],
        )
        assert [r["row_name"] for r in combined] == ["AlphaStudy"]

    def test_apply_search_checks_extra_top_level_fields(self, sample_rows):
        assert [r["row_name"] for r in apply_search(sample_rows, "llama-3")] == []
        assert [
            r["row_name"]
            for r in apply_search(sample_rows, "llama-3", extra_top_level_fields=["_unit_name"])
        ] == ["GammaStudy"]
