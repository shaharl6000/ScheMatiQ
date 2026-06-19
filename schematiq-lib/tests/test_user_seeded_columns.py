"""Tests for user-seeded columns feature (Sections A, B, C).

Covers:
- Column.locked field (A1)
- Optional rationale/definition (A2)
- Column.from_dict (A3, A4)
- _prune protects locked columns (B1)
- merge preserves locked name and flag (B2)
- to_llm_dict surfaces locked (B3)
- complete_partial_columns (C1, C2)
"""

import json
from unittest.mock import MagicMock

import pytest

from schematiq.core.schema import Column, Schema


# ── Section A: Data Model ──────────────────────────────────────────────────


class TestColumnLockedField:
    """A1: Column.locked field and to_dict serialization."""

    def test_default_locked_is_false(self):
        col = Column(name="x", rationale="r", definition="d")
        assert col.locked is False

    def test_locked_true(self):
        col = Column(name="x", rationale="r", definition="d", locked=True)
        assert col.locked is True

    def test_to_dict_omits_locked_when_false(self):
        col = Column(name="x", rationale="r", definition="d")
        assert "locked" not in col.to_dict()

    def test_to_dict_includes_locked_when_true(self):
        col = Column(name="x", rationale="r", definition="d", locked=True)
        assert col.to_dict()["locked"] is True


class TestColumnOptionalFields:
    """A2: rationale and definition default to empty string."""

    def test_name_only_construction(self):
        col = Column(name="X")
        assert col.name == "X"
        assert col.rationale == ""
        assert col.definition == ""

    def test_partial_construction(self):
        col = Column(name="X", definition="some def")
        assert col.rationale == ""
        assert col.definition == "some def"

    def test_schema_post_init_with_empty_strings(self):
        col = Column(name="x", locked=True)
        schema = Schema(query="q", columns=[col], max_keys=10)
        assert len(schema.columns) == 1
        assert schema.columns[0].rationale == ""


class TestColumnFromDict:
    """A3: Column.from_dict accepts both legacy and current keys."""

    def test_current_keys(self):
        data = {
            "name": "age",
            "rationale": "demographic",
            "definition": "age in years",
        }
        col = Column.from_dict(data)
        assert col.name == "age"
        assert col.rationale == "demographic"
        assert col.definition == "age in years"

    def test_legacy_keys(self):
        data = {
            "column": "age",
            "explanation": "demographic",
            "definition": "age in years",
        }
        col = Column.from_dict(data)
        assert col.name == "age"
        assert col.rationale == "demographic"
        assert col.definition == "age in years"

    def test_legacy_and_current_produce_equal_columns(self):
        legacy = Column.from_dict({"column": "x", "explanation": "r", "definition": "d"})
        current = Column.from_dict({"name": "x", "rationale": "r", "definition": "d"})
        assert legacy.name == current.name
        assert legacy.rationale == current.rationale
        assert legacy.definition == current.definition

    def test_locked_passthrough(self):
        col = Column.from_dict({"name": "x", "locked": True})
        assert col.locked is True

    def test_locked_default_false(self):
        col = Column.from_dict({"name": "x"})
        assert col.locked is False

    def test_missing_name_raises(self):
        with pytest.raises(KeyError):
            Column.from_dict({"rationale": "r"})

    def test_name_only_dict(self):
        col = Column.from_dict({"name": "x"})
        assert col.name == "x"
        assert col.rationale == ""
        assert col.definition == ""

    def test_all_fields(self):
        data = {
            "name": "x",
            "rationale": "r",
            "definition": "d",
            "source_document": "doc1.pdf",
            "discovery_iteration": 2,
            "allowed_values": ["a", "b"],
            "auto_expand_threshold": 3,
            "locked": True,
        }
        col = Column.from_dict(data)
        assert col.source_document == "doc1.pdf"
        assert col.discovery_iteration == 2
        assert col.allowed_values == ["a", "b"]
        assert col.auto_expand_threshold == 3
        assert col.locked is True


class TestSchemaFromDictPreservesLocked:
    """A4: Schema.from_dict routes through Column.from_dict and preserves locked."""

    def test_locked_through_schema_from_dict(self):
        data = {
            "query": "q",
            "columns": [
                {"name": "a", "locked": True},
                {"name": "b"},
            ],
        }
        schema = Schema.from_dict(data)
        assert schema.columns[0].locked is True
        assert schema.columns[1].locked is False


# ── Section B: Locked Protection ────────────────────────────────────────────


class TestPruneProtectsLocked:
    """B1: _prune never drops locked columns even if over max_keys."""

    def test_locked_overflow_kept(self):
        cols = [
            Column(name=f"locked_{i}", rationale=f"r{i}", definition=f"d{i}", locked=True)
            for i in range(4)
        ] + [
            Column(name=f"unlocked_{i}", rationale=f"r{i}", definition=f"d{i}")
            for i in range(2)
        ]
        schema = Schema(query="research query", columns=cols, max_keys=3)
        locked_names = {c.name for c in schema.columns if c.locked}
        assert locked_names == {"locked_0", "locked_1", "locked_2", "locked_3"}

    def test_locked_within_budget_keeps_some_unlocked(self):
        cols = [
            Column(name="locked_a", rationale="r", definition="d", locked=True),
            Column(name="unlocked_b", rationale="r", definition="d"),
            Column(name="unlocked_c", rationale="r", definition="d"),
            Column(name="unlocked_d", rationale="r", definition="d"),
        ]
        schema = Schema(query="research query", columns=cols, max_keys=3)
        names = {c.name for c in schema.columns}
        assert "locked_a" in names
        assert len(schema.columns) == 3

    def test_no_locked_unchanged_behavior(self):
        cols = [
            Column(name=f"c_{i}", rationale=f"r{i}", definition=f"d{i}")
            for i in range(5)
        ]
        schema = Schema(query="research query", columns=cols, max_keys=3)
        assert len(schema.columns) == 3


class TestMergePreservesLocked:
    """B2: merge keeps the locked column's name and flag even on case-insensitive / semantic match."""

    def test_locked_name_preserved_on_case_insensitive_match(self):
        # "age" locked, "Age" proposed — exact-match (case-insensitive) keeps the locked column.
        locked = Schema(
            query="patient ages study",
            columns=[Column(name="age", rationale="years", definition="patient age", locked=True)],
            max_keys=10,
        )
        proposal = Schema(
            query="patient ages study",
            columns=[Column(name="Age", rationale="age in years", definition="patient age")],
            max_keys=10,
        )
        merged = locked.merge(proposal)
        names = {c.name for c in merged.columns}
        assert "age" in names
        assert "Age" not in names
        age_col = next(c for c in merged.columns if c.name == "age")
        assert age_col.locked is True
        # Longer rationale from candidate should win (existing merge behavior).
        assert age_col.rationale == "age in years"

    def test_locked_name_preserved_on_semantic_match(self):
        # Force semantic match by stubbing the cosine similarity.
        from schematiq.core import schema as schema_mod

        locked = Schema(
            query="patient study",
            columns=[Column(name="age", rationale="years", definition="patient age", locked=True)],
            max_keys=10,
        )
        proposal = Schema(
            query="patient study",
            columns=[Column(name="patient_age", rationale="age", definition="patient age")],
            max_keys=10,
        )
        # Patch util.cos_sim to always return >= SIM_THRESHOLD so the semantic-match branch fires.
        original = schema_mod.util.cos_sim
        schema_mod.util.cos_sim = lambda a, b: 1.0
        try:
            merged = locked.merge(proposal)
        finally:
            schema_mod.util.cos_sim = original
        names = {c.name for c in merged.columns}
        assert "age" in names
        assert "patient_age" not in names
        age_col = next(c for c in merged.columns if c.name == "age")
        assert age_col.locked is True

    def test_locked_flag_survives_merge(self):
        locked = Schema(
            query="q",
            columns=[Column(name="x", rationale="r", definition="d", locked=True)],
            max_keys=10,
        )
        proposal = Schema(query="q", columns=[Column(name="y", rationale="r", definition="d")], max_keys=10)
        merged = locked.merge(proposal)
        x_col = next(c for c in merged.columns if c.name == "x")
        assert x_col.locked is True


class TestToLLMDictLocked:
    """B3: to_llm_dict includes locked flag."""

    def test_locked_in_llm_dict(self):
        cols = [
            Column(name="x", rationale="r", definition="d", locked=True),
            Column(name="y", rationale="r", definition="d"),
        ]
        schema = Schema(query="q", columns=cols, max_keys=10)
        llm_dict = schema.to_llm_dict()
        x_entry = next(c for c in llm_dict if c["name"] == "x")
        y_entry = next(c for c in llm_dict if c["name"] == "y")
        assert x_entry["locked"] is True
        assert "locked" not in y_entry


# ── Section C: Partial Column Completion ────────────────────────────────────


class TestCompletePartialColumnsPrompt:
    """C1: Prompt construction surfaces required context."""

    def test_user_prompt_contains_required_parts(self):
        from schematiq.core.prompts import USER_PROMPT_TMPL_COMPLETE_COLUMNS

        rendered = USER_PROMPT_TMPL_COMPLETE_COLUMNS.format(
            query="What is X?",
            observation_unit="Patient: a single patient encounter",
            passages="passage 1\npassage 2",
            columns_block='- name="age" | definition=<MISSING> | rationale="user provided"',
        )
        assert "What is X?" in rendered
        assert "Patient: a single patient encounter" in rendered
        assert "passage 1" in rendered
        assert 'name="age"' in rendered

    def test_system_prompt_demands_json_only(self):
        from schematiq.core.prompts import SYSTEM_PROMPT_COMPLETE_COLUMNS

        assert "JSON" in SYSTEM_PROMPT_COMPLETE_COLUMNS
        assert "Do NOT invent" in SYSTEM_PROMPT_COMPLETE_COLUMNS


class TestCompletePartialColumns:
    """C2: complete_partial_columns identifies and fills missing fields only."""

    def test_name_only_locked_gets_filled(self):
        from schematiq.core.schematiq import complete_partial_columns
        from schematiq.core.schema import ObservationUnit

        llm = MagicMock()
        llm.generate.return_value = json.dumps({
            "columns": [
                {
                    "name": "age",
                    "definition": "patient age in years",
                    "rationale": "demographic context",
                }
            ]
        })
        llm.max_tokens_for_task = MagicMock(return_value=1024)
        ou = ObservationUnit(name="Patient", definition="one encounter")
        cols = [Column(name="age", locked=True)]
        result = complete_partial_columns(
            columns=cols,
            query="study of patients",
            observation_unit=ou,
            passages=["a passage"],
            llm=llm,
            context_window_size=1000,
        )
        assert result[0].name == "age"
        assert result[0].definition == "patient age in years"
        assert result[0].rationale == "demographic context"
        assert result[0].locked is True
        assert llm.generate.call_count == 1

    def test_full_locked_skips_llm(self):
        from schematiq.core.schematiq import complete_partial_columns

        llm = MagicMock()
        cols = [Column(name="age", rationale="r", definition="d", locked=True)]
        result = complete_partial_columns(
            columns=cols,
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert result == cols
        assert llm.generate.call_count == 0

    def test_non_locked_partial_untouched(self):
        from schematiq.core.schematiq import complete_partial_columns

        llm = MagicMock()
        cols = [Column(name="age", locked=False)]
        result = complete_partial_columns(
            columns=cols,
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert result[0].definition == ""
        assert result[0].rationale == ""
        assert llm.generate.call_count == 0

    def test_existing_fields_not_overwritten(self):
        from schematiq.core.schematiq import complete_partial_columns

        llm = MagicMock()
        llm.generate.return_value = json.dumps({
            "columns": [
                {"name": "age", "definition": "NEW DEF", "rationale": "NEW RAT"}
            ]
        })
        llm.max_tokens_for_task = MagicMock(return_value=1024)
        # Only rationale missing; definition is preset.
        cols = [Column(name="age", definition="EXISTING DEF", locked=True)]
        result = complete_partial_columns(
            columns=cols,
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert result[0].definition == "EXISTING DEF"  # not overwritten
        assert result[0].rationale == "NEW RAT"  # filled

    def test_llm_cannot_invent_new_columns(self):
        from schematiq.core.schematiq import complete_partial_columns

        llm = MagicMock()
        # LLM returns an extra column not in the input.
        llm.generate.return_value = json.dumps({
            "columns": [
                {"name": "age", "definition": "d", "rationale": "r"},
                {"name": "rogue_column", "definition": "d", "rationale": "r"},
            ]
        })
        llm.max_tokens_for_task = MagicMock(return_value=1024)
        cols = [Column(name="age", locked=True)]
        result = complete_partial_columns(
            columns=cols,
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert len(result) == 1
        assert result[0].name == "age"


# ── Section F: Integration ────────────────────────────────────────────────


class TestLockedSurvivesIterations:
    """F1: Locked columns persist across many merge cycles even with paraphrasing proposals."""

    def test_locked_survives_repeated_merges(self):
        from schematiq.core import schema as schema_mod

        locked = Schema(
            query="patient study",
            columns=[
                Column(name="age", rationale="years", definition="patient age", locked=True),
                Column(name="weight", rationale="kg", definition="patient weight", locked=True),
            ],
            max_keys=10,
        )

        # Simulate three iterations of proposals that include semantic duplicates of locked cols.
        original = schema_mod.util.cos_sim
        schema_mod.util.cos_sim = lambda a, b: 1.0  # force semantic match
        try:
            for i in range(3):
                proposal = Schema(
                    query="patient study",
                    columns=[
                        Column(name=f"alt_age_{i}", rationale="r", definition="d"),
                        Column(name=f"new_col_{i}", rationale="r", definition="d"),
                    ],
                    max_keys=10,
                )
                locked = locked.merge(proposal)
        finally:
            schema_mod.util.cos_sim = original

        names = {c.name for c in locked.columns}
        assert "age" in names
        assert "weight" in names
        age_col = next(c for c in locked.columns if c.name == "age")
        assert age_col.locked is True


class TestDiscoverSchemaQueryOnlyCompletion:
    """F1: discover_schema (QUERY_ONLY mode) runs completion exactly once when seeded with partial columns."""

    def test_completion_runs_once_query_only(self):
        from schematiq.core.schematiq import discover_schema
        from schematiq.core.schema import ObservationUnit

        # Mock LLM: first call is complete_partial_columns, second is generate_schema.
        # We use a custom side_effect to detect ordering.
        calls = []

        def llm_generate(prompt, **kwargs):
            calls.append(prompt)
            # Find the stage by looking at the system prompt content.
            text = json.dumps(prompt) if not isinstance(prompt, str) else prompt
            if "Do NOT invent new columns" in text:
                # completion call
                return json.dumps({
                    "columns": [
                        {"name": "age", "definition": "patient age in years", "rationale": "demographic"}
                    ]
                })
            # generate_schema call
            return json.dumps({
                "document_helpful": False,
                "columns": [],
                "suggested_value_additions": [],
            })

        llm = MagicMock()
        llm.generate.side_effect = llm_generate
        llm.max_tokens_for_task = MagicMock(return_value=1024)
        llm._provider = "openai"  # avoid gemini-specific kwargs path

        seeded = Schema(
            query="study of patients",
            columns=[Column(name="age", locked=True)],
            max_keys=10,
            observation_unit=ObservationUnit(name="Patient", definition="one encounter"),
        )

        # QUERY_ONLY: empty documents/filenames
        result_schema, _, _, _ = discover_schema(
            query="study of patients",
            documents=[],
            filenames=[],
            max_keys_schema=10,
            llm=llm,
            retriever=None,
            documents_batch_size=1,
            context_window_size=4000,
            initial_schema=seeded,
            max_iters=3,
            discover_observation_unit=False,  # OU is pre-set
        )

        # Completion + generate_schema = 2 LLM calls.
        assert llm.generate.call_count == 2
        # Verify the locked column was filled.
        age = next(c for c in result_schema.columns if c.name == "age")
        assert age.definition == "patient age in years"
        assert age.rationale == "demographic"
        assert age.locked is True


class TestNoSeedRegression:
    """F1: With no initial schema, the path is unchanged — completion is never invoked."""

    def test_no_seed_no_completion(self):
        from schematiq.core.schematiq import complete_partial_columns

        # Empty columns list → no LLM call.
        llm = MagicMock()
        result = complete_partial_columns(
            columns=[],
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert result == []
        assert llm.generate.call_count == 0

    def test_unlocked_partial_no_completion(self):
        from schematiq.core.schematiq import complete_partial_columns

        # Unlocked columns with missing fields → NOT considered partial.
        llm = MagicMock()
        cols = [Column(name="x"), Column(name="y", definition="d")]
        result = complete_partial_columns(
            columns=cols,
            query="q",
            observation_unit=None,
            passages=[],
            llm=llm,
            context_window_size=1000,
        )
        assert result == cols
        assert llm.generate.call_count == 0
