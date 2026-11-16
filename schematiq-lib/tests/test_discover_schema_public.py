"""Unit tests for the public ``schematiq.discover_schema`` API."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from schematiq import (
    Column,
    DiscoveryConfig,
    ObservationUnit,
    ObservationUnitDiscoveryError,
    Schema,
    SchemaEvolution,
    discover_schema,
)
from schematiq.core.llm_backends import LLMInterface


class FakeDiscoveryLLM(LLMInterface):
    """Deterministic fake LLM for testing ``schematiq.discover_schema``."""

    _provider = "openai"

    def __init__(
        self,
        *,
        context_window_size: int = 16384,
        fail_combined_marker: Optional[str] = None,
        fail_single_marker: Optional[str] = None,
        bad_obs_unit_json: bool = False,
    ):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = context_window_size
        self.fail_combined_marker = fail_combined_marker
        self.fail_single_marker = fail_single_marker
        self.bad_obs_unit_json = bad_obs_unit_json
        self.calls: List[Dict[str, str]] = []

    def max_tokens_for_task(self, task: Optional[str] = None) -> int:
        return 2048

    def generate(self, prompt: Any, **kwargs) -> str:
        if isinstance(prompt, list):
            system = prompt[0].get("content", "") if len(prompt) > 0 else ""
            user = prompt[1].get("content", "") if len(prompt) > 1 else ""
        else:
            system = ""
            user = str(prompt)

        if "ObservationUnitLLM" in system:
            self.calls.append({"stage": "observation_unit", "user": user})
            if self.bad_obs_unit_json:
                return "not valid json"
            has_passages = "<SAMPLE_PASSAGES>" in user
            return json.dumps(
                {
                    "observation_unit": {
                        "name": "Court Ruling",
                        "definition": "A single judicial decision on an appeal",
                        "example_names": ["Appeal 101"] if has_passages else [],
                    },
                    "reasoning": "Deterministic test unit",
                },
                indent=2,
            )

        if "Do NOT invent new columns" in system:
            self.calls.append({"stage": "complete_partial_columns", "user": user})
            names = re.findall(r'- name="([^"]+)"', user)
            return json.dumps(
                {
                    "columns": [
                        {
                            "name": col_name,
                            "definition": f"Completed definition for {col_name}",
                            "rationale": f"Completed rationale for {col_name}",
                            "value_kind": "free_text",
                        }
                        for col_name in names
                    ]
                }
            )

        if "SchemaLLM" in system:
            self.calls.append({"stage": "schema_discovery", "user": user})
            passages_match = re.search(
                r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL
            )
            if passages_match is None:
                return json.dumps(
                    {
                        "columns": [
                            {
                                "name": "case_outcome",
                                "definition": "Final appeal outcome",
                                "rationale": "Measures decision result",
                                "value_kind": "category",
                                "allowed_values": ["granted", "denied"],
                            }
                        ]
                    }
                )

            passages_text = passages_match.group(1)

            if self.fail_single_marker and self.fail_single_marker in passages_text:
                raise RuntimeError("Request fails: too many tokens in prompt")

            if (
                self.fail_combined_marker
                and self.fail_combined_marker in passages_text
                and "DOC_ALPHA" in passages_text
                and "DOC_BETA" in passages_text
            ):
                raise RuntimeError("This model's maximum context length exceeded")

            columns: List[Dict[str, Any]] = []
            if "DOC_ALPHA" in passages_text:
                columns.append(
                    {
                        "name": "case_identifier",
                        "definition": "Unique docket identifier",
                        "rationale": "Identifies the case",
                        "value_kind": "identifier",
                    }
                )
            if "DOC_BETA" in passages_text:
                columns.append(
                    {
                        "name": "court_level",
                        "definition": "Court hierarchy level",
                        "rationale": "Captures jurisdiction tier",
                        "value_kind": "category",
                        "allowed_values": ["district", "appellate"],
                    }
                )
            return json.dumps(
                {
                    "document_helpful": True,
                    "columns": columns,
                    "suggested_value_additions": [],
                }
            )

        raise AssertionError(f"Unexpected prompt: {system[:100]}")


def test_no_backend_imports() -> None:
    assert not any(
        mod == "app" or mod.startswith("app.") for mod in sys.modules
    ), "schematiq-lib must not import backend modules"


def test_documents_mode_smart_and_fixed() -> None:
    llm_smart = FakeDiscoveryLLM()
    schema_smart, evo_smart = discover_schema(
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard in appellate court.",
            "DOC_GAMMA: Procedural details.",
        ],
        filenames=["alpha.txt", "beta.txt", "gamma.txt"],
        query="What factors drive appeal outcomes?",
        config=DiscoveryConfig(batch_strategy="smart", convergence_threshold=5),
        llm=llm_smart,
    )
    assert isinstance(schema_smart, Schema)
    assert isinstance(evo_smart, SchemaEvolution)
    assert {c.name for c in schema_smart.columns} == {"case_identifier", "court_level"}
    assert schema_smart.observation_unit is not None
    assert schema_smart.observation_unit.name == "Court Ruling"
    assert len(evo_smart.snapshots) == 1  # smart mode packed all 3 short docs into 1 batch

    llm_fixed = FakeDiscoveryLLM()
    schema_fixed, evo_fixed = discover_schema(
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard in appellate court.",
            "DOC_GAMMA: Procedural details.",
        ],
        filenames=["alpha.txt", "beta.txt", "gamma.txt"],
        query="What factors drive appeal outcomes?",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
        llm=llm_fixed,
    )
    assert {c.name for c in schema_fixed.columns} == {"case_identifier", "court_level"}
    assert len(evo_fixed.snapshots) == 3
    assert evo_fixed.column_sources == {
        "case_identifier": "alpha.txt",
        "court_level": "beta.txt",
    }


def test_query_only_and_document_only_modes() -> None:
    llm_q = FakeDiscoveryLLM()
    schema_q, evo_q = discover_schema(
        documents=[],
        filenames=[],
        query="What factors drive appeal outcomes?",
        llm=llm_q,
    )
    assert [c.name for c in schema_q.columns] == ["case_outcome"]
    assert schema_q.observation_unit is not None
    assert schema_q.observation_unit.source_document == "query_only"
    assert evo_q.column_sources == {"case_outcome": "query_only"}
    assert len(evo_q.snapshots) == 1
    assert evo_q.snapshots[0].documents_processed == ["query_only"]

    llm_d = FakeDiscoveryLLM()
    schema_d, evo_d = discover_schema(
        documents=["DOC_ALPHA: Appeal 101."],
        filenames=None,  # tests auto-generated doc_1 filename
        query="",
        llm=llm_d,
    )
    assert [c.name for c in schema_d.columns] == ["case_identifier"]
    assert evo_d.column_sources == {"case_identifier": "doc_1"}
    assert evo_d.snapshots[0].documents_processed == ["doc_1"]


def test_config_dict_and_initial_schema_variants(tmp_path: Path) -> None:
    # 1. Plain dict config with backend-style keys + locked partial column
    llm = FakeDiscoveryLLM()
    schema, evo = discover_schema(
        documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
        filenames=["a.txt", "b.txt"],
        query="Analyze rulings",
        config={
            "documents_batch_size": 1,
            "batch_strategy": "fixed",
            "max_keys_schema": 15,
            "convergence_threshold": 4,
            "initial_schema": [{"name": "seeded_judge", "locked": True}],
            "schema_creation_backend": {"context_window_size": 8192},
        },
        llm=llm,
    )
    assert schema.max_keys == 15
    assert [c.name for c in schema.columns] == [
        "seeded_judge",
        "case_identifier",
        "court_level",
    ]
    seeded = next(c for c in schema.columns if c.name == "seeded_judge")
    assert seeded.definition == "Completed definition for seeded_judge"
    assert seeded.locked is True
    assert evo.snapshots[0].iteration == 0
    assert evo.column_sources["seeded_judge"] == "initial_schema"
    assert [c["stage"] for c in llm.calls].count("complete_partial_columns") == 1

    # 2. Schema instance with preset observation unit and empty columns
    preset_unit = ObservationUnit(name="Judgment", definition="A court judgment")
    seed_schema = Schema(
        query="Analyze rulings",
        columns=[],
        max_keys=12,
        observation_unit=preset_unit,
    )
    llm_schema_obj = FakeDiscoveryLLM()
    res_obj, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(initial_schema=seed_schema, batch_strategy="fixed"),
        llm=llm_schema_obj,
    )
    assert res_obj.observation_unit == preset_unit
    assert res_obj.max_keys == 12
    assert "observation_unit" not in [c["stage"] for c in llm_schema_obj.calls]
    assert len(seed_schema.columns) == 0  # caller's Schema was not mutated

    # 3. initial_schema_path (list JSON and {"schema": [...]} JSON)
    list_path = tmp_path / "list_schema.json"
    list_path.write_text(
        json.dumps([{"name": "seeded_topic", "definition": "Topic", "rationale": "Why"}]),
        encoding="utf-8",
    )
    res_list, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(initial_schema_path=list_path, batch_strategy="fixed"),
        llm=FakeDiscoveryLLM(),
    )
    assert "seeded_topic" in {c.name for c in res_list.columns}

    dict_path = tmp_path / "dict_schema.json"
    dict_path.write_text(
        json.dumps({"schema": [{"name": "seeded_date", "definition": "Date", "rationale": "When"}]}),
        encoding="utf-8",
    )
    res_dict, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(initial_schema_path=dict_path, batch_strategy="fixed"),
        llm=FakeDiscoveryLLM(),
    )
    assert "seeded_date" in {c.name for c in res_dict.columns}


def test_preconfigured_observation_unit_modes() -> None:
    # Name-only: discovers definition and overrides name
    llm_name = FakeDiscoveryLLM()
    schema_name, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            initial_observation_unit={"name": "Appeal Case"},
        ),
        llm=llm_name,
    )
    assert schema_name.observation_unit is not None
    assert schema_name.observation_unit.name == "Appeal Case"
    assert schema_name.observation_unit.definition == "A single judicial decision on an appeal"
    assert [c["stage"] for c in llm_name.calls] == ["observation_unit", "schema_discovery"]

    # Name + definition: skips observation unit LLM call
    llm_full = FakeDiscoveryLLM()
    schema_full, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            initial_observation_unit=ObservationUnit(
                name="Appeal Case",
                definition="Custom definition",
            ),
        ),
        llm=llm_full,
    )
    assert schema_full.observation_unit is not None
    assert schema_full.observation_unit.name == "Appeal Case"
    assert schema_full.observation_unit.definition == "Custom definition"
    assert [c["stage"] for c in llm_full.calls] == ["schema_discovery"]

    # Query-only with pre-configured unit
    llm_q = FakeDiscoveryLLM()
    schema_q, _ = discover_schema(
        documents=[],
        filenames=[],
        query="Analyze rulings",
        config=DiscoveryConfig(initial_observation_unit={"name": "Appeal Case"}),
        llm=llm_q,
    )
    assert schema_q.observation_unit is not None
    assert schema_q.observation_unit.name == "Appeal Case"
    assert [c["stage"] for c in llm_q.calls] == ["schema_discovery"]


def test_convergence_stop_and_callbacks() -> None:
    llm = FakeDiscoveryLLM()
    progress_calls: List[tuple] = []
    snapshot_calls: List[tuple] = []

    schema, evo = discover_schema(
        documents=[
            "DOC_ALPHA: Text",
            "DOC_GAMMA: Nothing new",
            "DOC_DELTA: Nothing new",
            "DOC_BETA: Should not be reached",
        ],
        filenames=["a.txt", "g.txt", "d.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=1,
            convergence_threshold=2,
        ),
        llm=llm,
        on_progress=lambda stage, frac, info: progress_calls.append((stage, frac, info)),
        on_snapshot=lambda s, e: snapshot_calls.append((len(s.columns), len(e.snapshots))),
    )

    assert [c.name for c in schema.columns] == ["case_identifier"]
    assert len(evo.snapshots) == 3
    assert len(progress_calls) == 3
    # Converged batch breaks before calling on_snapshot for batch 3
    assert snapshot_calls == [(1, 1), (1, 2)]


def test_should_stop_between_and_post_merge() -> None:
    # 1. Stop requested after batch 1 completes (before batch 2)
    stop_flag = False
    llm = FakeDiscoveryLLM()

    def on_snapshot(_s: Schema, _e: SchemaEvolution) -> None:
        nonlocal stop_flag
        stop_flag = True

    schema, evo = discover_schema(
        documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
        filenames=["a.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
        llm=llm,
        should_stop=lambda: stop_flag,
        on_snapshot=on_snapshot,
    )
    assert [c.name for c in schema.columns] == ["case_identifier"]
    assert len(evo.snapshots) == 1

    # 2. Stop requested right after Schema.merge on batch 2: promotes merged schema and fires on_snapshot
    stop_after_merge = False
    snapshots: List[List[str]] = []
    orig_merge = Schema.merge

    def merge_and_stop(self: Schema, other: Schema) -> Schema:
        nonlocal stop_after_merge
        res = orig_merge(self, other)
        if any(c.name == "court_level" for c in res.columns):
            stop_after_merge = True
        return res

    with patch.object(Schema, "merge", merge_and_stop):
        schema2, evo2 = discover_schema(
            documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
            filenames=["a.txt", "b.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
            llm=FakeDiscoveryLLM(),
            should_stop=lambda: stop_after_merge,
            on_snapshot=lambda s, _e: snapshots.append([c.name for c in s.columns]),
        )

    assert {c.name for c in schema2.columns} == {"case_identifier", "court_level"}
    # Post-merge stop promotes merged schema and triggers on_snapshot before appending batch 2 snapshot
    assert len(evo2.snapshots) == 1
    assert snapshots == [["case_identifier"], ["case_identifier", "court_level"]]


def test_token_limit_batch_split_and_single_doc_skip() -> None:
    # Split batch of 2 docs on token-limit overflow
    warnings_split: List[tuple] = []
    llm_split = FakeDiscoveryLLM(fail_combined_marker="DOC_ALPHA")
    schema_split, evo_split = discover_schema(
        documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
        filenames=["a.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=2),
        llm=llm_split,
        on_warning=lambda lvl, msg, skip_info: warnings_split.append((lvl, msg, skip_info)),
    )
    assert {c.name for c in schema_split.columns} == {"case_identifier", "court_level"}
    assert len(warnings_split) == 1
    assert warnings_split[0][0] == "warning"
    assert warnings_split[0][2] is None
    assert len(evo_split.snapshots) == 1

    # Single overflowing doc is skipped with skip_info and discovery continues
    warnings_skip: List[tuple] = []
    llm_skip = FakeDiscoveryLLM(fail_single_marker="DOC_TOO_BIG")
    schema_skip, evo_skip = discover_schema(
        documents=["DOC_TOO_BIG: Text", "DOC_BETA: Text"],
        filenames=["huge.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
        llm=llm_skip,
        on_warning=lambda lvl, msg, skip_info: warnings_skip.append((lvl, msg, skip_info)),
    )
    assert [c.name for c in schema_skip.columns] == ["court_level"]
    assert len(warnings_skip) == 1
    assert warnings_skip[0][2]["filenames"] == ["huge.txt"]
    assert warnings_skip[0][2]["reason"] == "single_document_exceeds_token_limit"
    assert len(evo_skip.snapshots) == 1


def test_observation_unit_error_handling_parity() -> None:
    # Query-only mode lets ObservationUnitDiscoveryError propagate directly
    with pytest.raises(ObservationUnitDiscoveryError):
        discover_schema(
            documents=[],
            filenames=[],
            query="Analyze rulings",
            llm=FakeDiscoveryLLM(bad_obs_unit_json=True),
        )

    # Documents mode wraps ObservationUnitDiscoveryError in RuntimeError
    with pytest.raises(
        RuntimeError,
        match=r"Failed to discover observation unit: .* Ensure documents contain extractable entities\.",
    ) as exc_info:
        discover_schema(
            documents=["DOC_ALPHA: Text"],
            filenames=["a.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(batch_strategy="fixed"),
            llm=FakeDiscoveryLLM(bad_obs_unit_json=True),
        )
    assert isinstance(exc_info.value.__cause__, ObservationUnitDiscoveryError)

    # Documents mode wraps unexpected errors during observation unit discovery
    with patch(
        "schematiq.discovery.main.discover_observation_unit",
        side_effect=ValueError("unexpected failure"),
    ):
        with pytest.raises(
            RuntimeError,
            match=r"Observation unit discovery failed unexpectedly: unexpected failure",
        ):
            discover_schema(
                documents=["DOC_ALPHA: Text"],
                filenames=["a.txt"],
                query="Analyze rulings",
                config=DiscoveryConfig(batch_strategy="fixed"),
                llm=FakeDiscoveryLLM(),
            )


def test_filenames_length_mismatch_raises() -> None:
    llm = FakeDiscoveryLLM()

    with pytest.raises(ValueError, match="filenames length \\(1\\) must match documents length \\(2\\)"):
        discover_schema(
            documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
            filenames=["only_one.txt"],
            query="Analyze rulings",
            llm=llm,
        )

    with pytest.raises(ValueError, match="filenames length \\(2\\) must match documents length \\(1\\)"):
        discover_schema(
            documents=["DOC_ALPHA: Text"],
            filenames=["a.txt", "b.txt"],
            query="Analyze rulings",
            llm=llm,
        )

    with pytest.raises(ValueError, match="filenames length \\(1\\) must match documents length \\(0\\)"):
        discover_schema(
            documents=[],
            filenames=["extra.txt"],
            query="Analyze rulings",
            llm=llm,
        )

    assert len(llm.calls) == 0


def test_discover_observation_unit_false_skips_unit_discovery() -> None:
    llm_docs = FakeDiscoveryLLM()
    schema_docs, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            discover_observation_unit=False,
        ),
        llm=llm_docs,
    )
    assert schema_docs.observation_unit is None
    assert [c["stage"] for c in llm_docs.calls] == ["schema_discovery"]

    llm_q = FakeDiscoveryLLM()
    schema_q, _ = discover_schema(
        documents=[],
        filenames=[],
        query="Analyze rulings",
        config=DiscoveryConfig(discover_observation_unit=False),
        llm=llm_q,
    )
    assert schema_q.observation_unit is None
    assert [c["stage"] for c in llm_q.calls] == ["schema_discovery"]
