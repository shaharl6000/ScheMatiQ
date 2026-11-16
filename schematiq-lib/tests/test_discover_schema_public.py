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

            if "DOC_TRANSIENT_FAIL" in passages_text:
                raise RuntimeError("transient network error")

            if (
                self.fail_combined_marker
                and self.fail_combined_marker in passages_text
                and "DOC_ALPHA" in passages_text
                and "DOC_BETA" in passages_text
            ):
                raise RuntimeError("This model's maximum context length exceeded")

            if "DOC_UNHELPFUL" in passages_text:
                return json.dumps(
                    {
                        "document_helpful": False,
                        "columns": [],
                        "suggested_value_additions": [],
                    }
                )

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
            if "DOC_GAMMA_COL" in passages_text:
                columns.append(
                    {
                        "name": "filing_year",
                        "definition": "Filing year",
                        "rationale": "Tracks filing year",
                        "value_kind": "number",
                    }
                )
            if "DOC_DELTA_COL" in passages_text:
                columns.append(
                    {
                        "name": "country_of_origin",
                        "definition": "Country of origin",
                        "rationale": "Tracks origin",
                        "value_kind": "free_text",
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


def test_identify_observation_unit_false_skips_unit_discovery() -> None:
    llm_docs = FakeDiscoveryLLM()
    schema_docs, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            identify_observation_unit=False,
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
        config=DiscoveryConfig(identify_observation_unit=False),
        llm=llm_q,
    )
    assert schema_q.observation_unit is None
    assert [c["stage"] for c in llm_q.calls] == ["schema_discovery"]

    # from_dict accepts both the new key and the legacy discover_observation_unit key.
    assert DiscoveryConfig.from_dict({"identify_observation_unit": False}).identify_observation_unit is False
    assert DiscoveryConfig.from_dict({"discover_observation_unit": False}).identify_observation_unit is False
    assert (
        DiscoveryConfig.from_dict(
            {"identify_observation_unit": True, "discover_observation_unit": False}
        ).identify_observation_unit
        is True
    )


def test_strict_initial_schema_loading(tmp_path: Path) -> None:
    from schematiq.discovery import load_initial_schema_from_config

    missing_path = tmp_path / "does_not_exist.json"
    malformed_path = tmp_path / "malformed.json"
    malformed_path.write_text("{not valid json", encoding="utf-8")
    unrecognized_path = tmp_path / "unrecognized.json"
    unrecognized_path.write_text(json.dumps({"unexpected": []}), encoding="utf-8")

    # Non-strict (default) preserves existing behavior: returns None on errors.
    for cfg in [
        DiscoveryConfig(initial_schema_path=missing_path),
        DiscoveryConfig(initial_schema_path=malformed_path),
        DiscoveryConfig(initial_schema_path=unrecognized_path),
        DiscoveryConfig(initial_schema=[{}]),
        DiscoveryConfig(initial_schema="not_a_list"),  # type: ignore[arg-type]
    ]:
        assert load_initial_schema_from_config(cfg, "query", 10) is None

    # strict=True raises clear exceptions via function argument or DiscoveryConfig.
    with pytest.raises(FileNotFoundError):
        load_initial_schema_from_config(
            DiscoveryConfig(initial_schema_path=missing_path), "query", 10, strict=True
        )
    with pytest.raises(ValueError):
        load_initial_schema_from_config(
            DiscoveryConfig(initial_schema_path=malformed_path), "query", 10, strict=True
        )
    with pytest.raises(ValueError, match="JSON list"):
        load_initial_schema_from_config(
            DiscoveryConfig(initial_schema_path=unrecognized_path), "query", 10, strict=True
        )
    with pytest.raises(ValueError):
        load_initial_schema_from_config(
            DiscoveryConfig(initial_schema=[{}], strict=True), "query", 10
        )
    with pytest.raises(ValueError, match="expected Schema or sequence"):
        load_initial_schema_from_config(
            DiscoveryConfig(initial_schema="not_a_list", strict=True), "query", 10  # type: ignore[arg-type]
        )

    # discover_schema honors strict from both DiscoveryConfig and dict config.
    llm = FakeDiscoveryLLM()
    with pytest.raises(FileNotFoundError):
        discover_schema(
            documents=["DOC_ALPHA: Text"],
            filenames=["a.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(initial_schema_path=missing_path, strict=True),
            llm=llm,
        )
    with pytest.raises(ValueError):
        discover_schema(
            documents=["DOC_ALPHA: Text"],
            filenames=["a.txt"],
            query="Analyze rulings",
            config={"initial_schema_path": str(unrecognized_path), "strict": True},
            llm=llm,
        )
    assert len(llm.calls) == 0

    # strict=True without any seed schema still succeeds cleanly.
    assert load_initial_schema_from_config(DiscoveryConfig(strict=True), "query", 10) is None
    schema_ok, _ = discover_schema(
        documents=["DOC_ALPHA: Text"],
        filenames=["a.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", strict=True),
        llm=llm,
    )
    assert [c.name for c in schema_ok.columns] == ["case_identifier"]


def test_public_discover_observation_unit_import() -> None:
    import schematiq
    from schematiq.core import schematiq as core_sq
    from schematiq.discovery import main as discovery_main

    assert schematiq.discover_observation_unit is core_sq.discover_observation_unit
    assert discovery_main.discover_observation_unit is core_sq.discover_observation_unit
    assert core_sq.discover_observation_unit.__name__ == "discover_observation_unit"
    assert not hasattr(core_sq, "_discover_observation_unit")


def test_max_iters_cap_and_from_dict() -> None:
    llm = FakeDiscoveryLLM()
    schema, evo = discover_schema(
        documents=[
            "DOC_ALPHA: Case 1.",
            "DOC_BETA: Case 2.",
            "DOC_GAMMA_COL: Case 3.",
            "DOC_DELTA_COL: Case 4.",
        ],
        filenames=["d1.txt", "d2.txt", "d3.txt", "d4.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=1, max_iters=2),
        llm=llm,
    )
    assert [c.name for c in schema.columns] == ["case_identifier", "court_level"]
    assert len(evo.snapshots) == 2
    assert [c["stage"] for c in llm.calls].count("schema_discovery") == 2

    cfg_from_dict = DiscoveryConfig.from_dict({"max_iters": 3})
    assert cfg_from_dict.max_iters == 3
    assert DiscoveryConfig().max_iters is None


def test_contributing_non_contributing_and_column_tagging() -> None:
    warnings_seen: List[tuple] = []
    llm = FakeDiscoveryLLM(fail_single_marker="DOC_TOO_BIG")
    schema, evo = discover_schema(
        documents=[
            "DOC_ALPHA: Helpful with columns.",
            "DOC_UNHELPFUL: Boilerplate.",
            "DOC_EMPTY_HELPFUL: No columns produced.",
            "DOC_TOO_BIG: Overflow doc.",
            "DOC_TRANSIENT_FAIL: Transient fail.",
            "DOC_BETA: Court level.",
        ],
        filenames=[
            "alpha.txt",
            "unhelpful.txt",
            "empty.txt",
            "big.txt",
            "broken.txt",
            "beta.txt",
        ],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=1,
            initial_schema=[
                {"name": "seeded_col", "definition": "Seeded", "rationale": "Seeded"}
            ],
        ),
        llm=llm,
        on_warning=lambda lvl, msg, info: warnings_seen.append((lvl, msg, info)),
    )

    assert evo.contributing_files == ["alpha.txt", "beta.txt"]
    assert evo.non_contributing_files == ["unhelpful.txt", "empty.txt", "big.txt", "broken.txt"]
    assert evo.skipped_batches_by_reason == {
        "single_document_exceeds_token_limit": 1,
        "llm_call_failed": 1,
    }
    assert evo.skipped_filenames == ["big.txt", "broken.txt"]
    assert evo.total_skipped_batches == 2
    # to_dict() remains compatible with downstream SchemaEvolution parsers
    assert set(evo.to_dict().keys()) == {
        "snapshots",
        "column_sources",
        "skipped_batches_by_reason",
        "skipped_filenames",
    }

    by_name = {c.name: c for c in schema.columns}
    assert by_name["seeded_col"].source_document == "initial_schema"
    assert by_name["seeded_col"].discovery_iteration == 0
    assert by_name["case_identifier"].source_document == "alpha.txt"
    assert by_name["case_identifier"].discovery_iteration == 1
    assert by_name["court_level"].source_document == "beta.txt"
    # big.txt was skipped before snapshot numbering, broken.txt ran as batch 5 -> beta.txt is batch 6
    assert by_name["court_level"].discovery_iteration == 6

    reasons = [info["reason"] for _, _, info in warnings_seen if info]
    assert reasons == ["single_document_exceeds_token_limit", "llm_call_failed"]

    # Query-only mode tags new columns with ("query_only", 1)
    q_schema, _ = discover_schema(
        documents=[],
        filenames=[],
        query="Analyze rulings",
        config=DiscoveryConfig(identify_observation_unit=False),
        llm=FakeDiscoveryLLM(),
    )
    assert q_schema.columns[0].source_document == "query_only"
    assert q_schema.columns[0].discovery_iteration == 1


def test_deprecated_core_wrappers_warn_and_main_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import warnings
    from schematiq.core import schematiq as core_sq
    from schematiq.core import utils as core_utils

    missing_seed = tmp_path / "missing.json"
    with pytest.warns(DeprecationWarning, match="load_initial_schema_from_config"):
        empty_schema = core_sq.load_initial_schema(missing_seed, "Analyze rulings", 10)
    assert len(empty_schema.columns) == 0

    with pytest.warns(DeprecationWarning, match="schematiq.discover_schema"):
        schema_old, contrib, non_contrib, evo = core_sq.discover_schema(
            query="Analyze rulings",
            documents=["DOC_ALPHA: Text", "DOC_BETA: Text"],
            filenames=["a.txt", "b.txt"],
            max_keys_schema=10,
            llm=FakeDiscoveryLLM(),
            retriever=None,
            documents_batch_size=1,
            context_window_size=16384,
        )
    assert [c.name for c in schema_old.columns] == ["case_identifier", "court_level"]
    assert contrib == ["a.txt", "b.txt"]
    assert non_contrib == []
    assert len(evo.snapshots) == 2

    # Verify CLI main() calls the unified loop without emitting DeprecationWarning.
    docs_dir = tmp_path / "cli_docs"
    docs_dir.mkdir()
    (docs_dir / "a.txt").write_text("DOC_ALPHA: Text", encoding="utf-8")
    out_file = tmp_path / "cli_out.json"
    cfg_file = tmp_path / "cli_cfg.json"
    cfg_file.write_text(
        json.dumps(
            {
                "query": "Analyze rulings",
                "docs_path": str(docs_dir),
                "document_randomization_seed": None,
                "documents_batch_size": 1,
                "max_keys_schema": 10,
                "output_path": str(out_file),
                "backend": {"provider": "openai", "model": "fake-deterministic"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(core_utils, "build_llm", lambda _cfg: FakeDiscoveryLLM())
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always", DeprecationWarning)
        core_sq.main(cfg_file)
    assert [w for w in recorded if issubclass(w.category, DeprecationWarning)] == []
    assert out_file.exists()


def test_observation_unit_failure_skips_batch_and_retries_next_batch() -> None:
    from google.genai import errors as genai_errors

    class ObsUnitFlakyLLM(FakeDiscoveryLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            if isinstance(prompt, list):
                system = prompt[0].get("content", "") if len(prompt) > 0 else ""
                user = prompt[1].get("content", "") if len(prompt) > 1 else ""
            else:
                system = ""
                user = str(prompt)

            if "ObservationUnitLLM" in system:
                self.calls.append({"stage": "observation_unit", "user": user})
                if "DOC_BAD_UNIT" in user:
                    return "{not valid json"
                if "DOC_AUTH_FAIL" in user:
                    with pytest.raises(genai_errors.APIError) as excinfo:
                        genai_errors.APIError.raise_error(
                            401,
                            {
                                "error": {
                                    "code": 401,
                                    "status": "UNAUTHENTICATED",
                                    "message": "API key not valid",
                                }
                            },
                            None,
                        )
                    raise excinfo.value
                return json.dumps(
                    {
                        "observation_unit": {
                            "name": "Court Ruling",
                            "definition": "A single judicial decision on an appeal",
                            "example_names": ["Appeal 101"],
                        },
                        "reasoning": "Deterministic test unit",
                    }
                )
            return super().generate(prompt, **kwargs)

    warnings_seen: List[tuple] = []
    llm = ObsUnitFlakyLLM()
    schema, evo = discover_schema(
        documents=[
            "DOC_BAD_UNIT: Unparseable intro.",
            "DOC_BETA: Appellate court decision.",
        ],
        filenames=["bad_unit.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
        llm=llm,
        on_warning=lambda lvl, msg, info: warnings_seen.append((lvl, msg, info)),
    )

    assert schema.observation_unit is not None
    assert schema.observation_unit.name == "Court Ruling"
    assert schema.observation_unit.source_document == "b.txt"
    assert [c.name for c in schema.columns] == ["court_level"]
    assert evo.contributing_files == ["b.txt"]
    assert evo.non_contributing_files == ["bad_unit.txt"]
    assert evo.skipped_batches_by_reason == {"observation_unit_discovery_failed": 1}
    assert evo.skipped_filenames == ["bad_unit.txt"]
    assert evo.total_skipped_batches == 1
    assert len(warnings_seen) == 1
    assert warnings_seen[0][2]["reason"] == "observation_unit_discovery_failed"
    assert warnings_seen[0][2]["filenames"] == ["bad_unit.txt"]

    # Fatal should_abort_run error during observation unit discovery aborts immediately.
    llm_fatal = ObsUnitFlakyLLM()
    with pytest.raises(RuntimeError, match="Failed to discover observation unit") as exc_info:
        discover_schema(
            documents=[
                "DOC_AUTH_FAIL: Bad key.",
                "DOC_BETA: Should never be reached.",
            ],
            filenames=["bad_key.txt", "b.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
            llm=llm_fatal,
        )
    assert isinstance(exc_info.value.__cause__, ObservationUnitDiscoveryError)
    assert [c["stage"] for c in llm_fatal.calls] == ["observation_unit"]


def test_max_consecutive_failed_batches_in_discovery_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.genai import errors as genai_errors
    import schematiq.discovery as discovery_pkg
    from schematiq.core import schematiq as core_sq
    from schematiq.core.llm_backends import LLMRunAborted
    from schematiq.discovery import main as discovery_main

    assert discovery_pkg._MAX_CONSECUTIVE_FAILED_BATCHES == 3
    assert discovery_main._MAX_CONSECUTIVE_FAILED_BATCHES == 3
    assert not hasattr(core_sq, "_MAX_CONSECUTIVE_FAILED_BATCHES")

    class OutageLLM(FakeDiscoveryLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            if isinstance(prompt, list) and "SchemaLLM" in (prompt[0].get("content", "")):
                self.calls.append({"stage": "schema_discovery", "user": ""})
                with pytest.raises(genai_errors.APIError) as excinfo:
                    genai_errors.APIError.raise_error(
                        503,
                        {
                            "error": {
                                "code": 503,
                                "status": "UNAVAILABLE",
                                "message": "Overloaded",
                            }
                        },
                        None,
                    )
                raise excinfo.value
            return super().generate(prompt, **kwargs)

    monkeypatch.setattr(discovery_main, "_MAX_CONSECUTIVE_FAILED_BATCHES", 2)
    llm = OutageLLM()
    with pytest.raises(LLMRunAborted, match="2 batches failed in a row"):
        discover_schema(
            documents=["DOC_1", "DOC_2", "DOC_3"],
            filenames=["d1.txt", "d2.txt", "d3.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed",
                batch_size=1,
                identify_observation_unit=False,
            ),
            llm=llm,
        )
    assert [c["stage"] for c in llm.calls] == ["schema_discovery", "schema_discovery"]


def test_observation_unit_attempts_are_capped() -> None:
    docs = [f"DOC_{i}: Unparseable unit text." for i in range(5)]
    names = [f"doc_{i}.txt" for i in range(5)]

    # Default cap: stop after 3 failed observation unit attempts, not 5.
    llm_capped = FakeDiscoveryLLM(bad_obs_unit_json=True)
    warnings_seen: List[tuple] = []
    with pytest.raises(
        RuntimeError,
        match=r"Failed to discover observation unit \(after 3 attempts\): .* "
        r"Ensure documents contain extractable entities\.",
    ) as exc_info:
        discover_schema(
            documents=docs,
            filenames=names,
            query="Analyze rulings",
            config=DiscoveryConfig(batch_strategy="fixed", batch_size=1),
            llm=llm_capped,
            on_warning=lambda lvl, msg, info: warnings_seen.append((lvl, msg, info)),
        )
    assert isinstance(exc_info.value.__cause__, ObservationUnitDiscoveryError)
    assert [c["stage"] for c in llm_capped.calls] == ["observation_unit"] * 3
    assert [w[2]["filenames"] for w in warnings_seen] == [[n] for n in names[:3]]

    # Custom cap.
    llm_custom = FakeDiscoveryLLM(bad_obs_unit_json=True)
    with pytest.raises(RuntimeError, match=r"\(after 2 attempts\)"):
        discover_schema(
            documents=docs,
            filenames=names,
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed", batch_size=1, max_observation_unit_attempts=2
            ),
            llm=llm_custom,
        )
    assert len(llm_custom.calls) == 2

    # No cap: every batch is attempted, then the all-batches-failed error is raised.
    llm_uncapped = FakeDiscoveryLLM(bad_obs_unit_json=True)
    with pytest.raises(
        RuntimeError, match=r"^Failed to discover observation unit: "
    ):
        discover_schema(
            documents=docs,
            filenames=names,
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed", batch_size=1, max_observation_unit_attempts=None
            ),
            llm=llm_uncapped,
        )
    assert len(llm_uncapped.calls) == 5


def test_observation_unit_attempts_from_dict() -> None:
    assert DiscoveryConfig().max_observation_unit_attempts == 3
    assert DiscoveryConfig.from_dict({}).max_observation_unit_attempts == 3
    assert (
        DiscoveryConfig.from_dict({"max_observation_unit_attempts": 5})
        .max_observation_unit_attempts
        == 5
    )
    assert (
        DiscoveryConfig.from_dict({"max_observation_unit_attempts": None})
        .max_observation_unit_attempts
        is None
    )


def test_split_batch_partial_sibling_failure_preserves_successful_subbatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from google.genai import errors as genai_errors
    from schematiq.core import schematiq as core_sq
    from schematiq.core.llm_backends import LLMRunAborted
    from schematiq.core.schema import ObservationUnit

    def _outage_err() -> genai_errors.APIError:
        with pytest.raises(genai_errors.APIError) as excinfo:
            genai_errors.APIError.raise_error(
                503,
                {
                    "error": {
                        "code": 503,
                        "status": "UNAVAILABLE",
                        "message": "Overloaded",
                    }
                },
                None,
            )
        return excinfo.value

    class SplitPartialFailLLM(FakeDiscoveryLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            if isinstance(prompt, list) and "SchemaLLM" in (prompt[0].get("content", "")):
                user = prompt[1].get("content", "") if len(prompt) > 1 else ""
                match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
                text = match.group(1) if match else user
                marker_count = len(
                    re.findall(
                        r"DOC_(?:ALPHA|BETA|TRANSIENT_FAIL|RET_FAIL|OUTAGE|FAIL_ONE|FAIL_TWO)\b",
                        text,
                    )
                )
                if marker_count > 1:
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise RuntimeError("This model's maximum context length exceeded")
                if "DOC_OUTAGE" in text:
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise _outage_err()
                if "DOC_FAIL_ONE" in text:
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise RuntimeError("fail 1")
                if "DOC_FAIL_TWO" in text:
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise RuntimeError("fail 2")
            return super().generate(prompt, **kwargs)

    # 1. Left succeeds, right fails transiently -> left columns and contribution kept
    warnings_lr: List[tuple] = []
    schema_lr, evo_lr = discover_schema(
        documents=["DOC_ALPHA: Ruling 101.", "DOC_TRANSIENT_FAIL: Glitch."],
        filenames=["a.txt", "broken.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=2,
            initial_observation_unit=ObservationUnit(
                name="Court Ruling", definition="Single ruling"
            ),
        ),
        llm=SplitPartialFailLLM(),
        on_warning=lambda lvl, msg, info: warnings_lr.append((lvl, msg, info)),
    )
    assert [c.name for c in schema_lr.columns] == ["case_identifier"]
    assert schema_lr.columns[0].source_document == "a.txt"
    assert evo_lr.column_sources == {"case_identifier": "a.txt"}
    assert evo_lr.contributing_files == ["a.txt"]
    assert evo_lr.non_contributing_files == ["broken.txt"]
    assert evo_lr.skipped_batches_by_reason == {"llm_call_failed": 1}
    assert evo_lr.skipped_filenames == ["broken.txt"]
    assert evo_lr.snapshots[0].new_columns == ["case_identifier"]
    assert [info["reason"] for _, _, info in warnings_lr if info] == ["llm_call_failed"]

    # 2. Left fails transiently, right succeeds -> right columns and contribution kept
    schema_rl, evo_rl = discover_schema(
        documents=["DOC_TRANSIENT_FAIL: Glitch.", "DOC_BETA: Appellate court."],
        filenames=["broken.txt", "b.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=2,
            initial_observation_unit=ObservationUnit(
                name="Court Ruling", definition="Single ruling"
            ),
        ),
        llm=SplitPartialFailLLM(),
    )
    assert [c.name for c in schema_rl.columns] == ["court_level"]
    assert schema_rl.columns[0].source_document == "b.txt"
    assert evo_rl.column_sources == {"court_level": "b.txt"}
    assert evo_rl.contributing_files == ["b.txt"]
    assert evo_rl.non_contributing_files == ["broken.txt"]
    assert evo_rl.skipped_filenames == ["broken.txt"]

    # 3. Left succeeds, right fails retrieval inside split -> left preserved, reason='retrieval_failed'
    orig_select = core_sq.select_relevant_content

    def flaky_select(docs, query, retriever):
        if len(docs) == 1 and any("DOC_RET_FAIL" in d for d in docs):
            raise RuntimeError("split retrieval connection lost")
        return orig_select(docs, query, retriever)

    monkeypatch.setattr(core_sq, "select_relevant_content", flaky_select)
    warnings_ret: List[tuple] = []
    schema_ret, evo_ret = discover_schema(
        documents=["DOC_ALPHA: Ruling 101.", "DOC_RET_FAIL: Retriever fails after split."],
        filenames=["a.txt", "ret_bad.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=2,
            initial_observation_unit=ObservationUnit(
                name="Court Ruling", definition="Single ruling"
            ),
        ),
        llm=SplitPartialFailLLM(),
        on_warning=lambda lvl, msg, info: warnings_ret.append((lvl, msg, info)),
    )
    monkeypatch.undo()
    assert [c.name for c in schema_ret.columns] == ["case_identifier"]
    assert evo_ret.contributing_files == ["a.txt"]
    assert evo_ret.non_contributing_files == ["ret_bad.txt"]
    assert evo_ret.skipped_batches_by_reason == {"retrieval_failed": 1}
    assert evo_ret.skipped_filenames == ["ret_bad.txt"]
    assert [info["reason"] for _, _, info in warnings_ret if info] == ["retrieval_failed"]

    # 4. 3 consecutive outage errors across split leaves abort with LLMRunAborted
    llm_outage = SplitPartialFailLLM()
    with pytest.raises(LLMRunAborted, match="3 batches failed in a row"):
        discover_schema(
            documents=[
                "DOC_ALPHA: Success.",
                "DOC_OUTAGE: 1.",
                "DOC_OUTAGE: 2.",
                "DOC_OUTAGE: 3.",
                "DOC_BETA: Should not be reached.",
            ],
            filenames=["a.txt", "o1.txt", "o2.txt", "o3.txt", "b.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed",
                batch_size=4,
                initial_observation_unit=ObservationUnit(
                    name="Court Ruling", definition="Single ruling"
                ),
            ),
            llm=llm_outage,
        )
    assert not any("DOC_BETA" in c["user"] for c in llm_outage.calls)

    # 5. All split leaves failing on the sole batch in corpus re-raises last leaf error
    with pytest.raises(RuntimeError, match="fail 2"):
        discover_schema(
            documents=["DOC_FAIL_ONE: 1.", "DOC_FAIL_TWO: 2."],
            filenames=["f1.txt", "f2.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed",
                batch_size=2,
                initial_observation_unit=ObservationUnit(
                    name="Court Ruling", definition="Single ruling"
                ),
            ),
            llm=SplitPartialFailLLM(),
        )


def test_unified_column_provenance_across_unsplit_and_split_batches() -> None:
    from schematiq.core.schema import ObservationUnit

    class MultiDocSplitLLM(FakeDiscoveryLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            if isinstance(prompt, list) and "SchemaLLM" in (prompt[0].get("content", "")):
                user = prompt[1].get("content", "") if len(prompt) > 1 else ""
                match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
                text = match.group(1) if match else user
                if (
                    "DOC_ALPHA" in text
                    and "DOC_BETA" in text
                    and "DOC_GAMMA_COL" in text
                    and "DOC_DELTA_COL" in text
                ):
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise RuntimeError("This model's maximum context length exceeded")
            return super().generate(prompt, **kwargs)

    unit = ObservationUnit(name="Court Ruling", definition="Single ruling")

    # Unsplit multi-document batch -> both Column.source_document and column_sources agree
    schema_unsplit, evo_unsplit = discover_schema(
        documents=["DOC_GAMMA_COL: Year.", "DOC_ALPHA: Ruling 101."],
        filenames=["gamma.txt", "alpha.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed", batch_size=2, initial_observation_unit=unit
        ),
        llm=MultiDocSplitLLM(),
    )
    assert len(schema_unsplit.columns) == 2
    for col in schema_unsplit.columns:
        assert col.source_document == "gamma.txt, alpha.txt"
        assert evo_unsplit.column_sources[col.name] == col.source_document

    # Split batch with oversized doc skipped + surviving single-doc leaf
    schema_skip, evo_skip = discover_schema(
        documents=["DOC_TOO_BIG: Oversized.", "DOC_ALPHA: Ruling 101."],
        filenames=["huge.txt", "alpha.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed", batch_size=2, initial_observation_unit=unit
        ),
        llm=MultiDocSplitLLM(fail_single_marker="DOC_TOO_BIG"),
    )
    assert [c.name for c in schema_skip.columns] == ["case_identifier"]
    assert schema_skip.columns[0].source_document == "alpha.txt"
    assert evo_skip.column_sources == {"case_identifier": "alpha.txt"}

    # Split 4-doc batch -> ["a.txt", "b.txt"] and ["c.txt", "d.txt"] sub-batches
    schema_sub, evo_sub = discover_schema(
        documents=[
            "DOC_ALPHA: 1.",
            "DOC_GAMMA_COL: 2.",
            "DOC_BETA: 3.",
            "DOC_DELTA_COL: 4.",
        ],
        filenames=["a.txt", "b.txt", "c.txt", "d.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed", batch_size=4, initial_observation_unit=unit
        ),
        llm=MultiDocSplitLLM(),
    )
    by_name = {c.name: c for c in schema_sub.columns}
    assert by_name["case_identifier"].source_document == "a.txt, b.txt"
    assert by_name["filing_year"].source_document == "a.txt, b.txt"
    assert by_name["court_level"].source_document == "c.txt, d.txt"
    assert by_name["country_of_origin"].source_document == "c.txt, d.txt"
    for name, col in by_name.items():
        assert evo_sub.column_sources[name] == col.source_document


def test_obs_unit_cap_with_split_subbatch_error_handling() -> None:
    class ObsUnitAndSplitLLM(FakeDiscoveryLLM):
        def generate(self, prompt: Any, **kwargs) -> str:
            if isinstance(prompt, list):
                system = prompt[0].get("content", "") if len(prompt) > 0 else ""
                user = prompt[1].get("content", "") if len(prompt) > 1 else ""
            else:
                system = ""
                user = str(prompt)

            if "ObservationUnitLLM" in system:
                self.calls.append({"stage": "observation_unit", "user": user})
                if "DOC_BAD_OBS" in user:
                    return "{not valid json"
                return json.dumps(
                    {
                        "observation_unit": {
                            "name": "Court Ruling",
                            "definition": "A single judicial decision on an appeal",
                            "example_names": ["Appeal 101"],
                        },
                        "reasoning": "Deterministic test unit",
                    }
                )

            if "SchemaLLM" in system:
                match = re.search(r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL)
                text = match.group(1) if match else user
                if (
                    len(
                        re.findall(
                            r"DOC_(?:ALPHA|TRANSIENT_FAIL)\b",
                            text,
                        )
                    )
                    > 1
                ):
                    self.calls.append({"stage": "schema_discovery", "user": user})
                    raise RuntimeError("This model's maximum context length exceeded")
            return super().generate(prompt, **kwargs)

    # Batch 1 fails observation unit discovery (1 < cap 2), then batch 2 recovers the unit
    # and splits on token limits while preserving the surviving sub-batch.
    schema, evo = discover_schema(
        documents=[
            "DOC_BAD_OBS: Cover noise 1.",
            "DOC_BAD_OBS: Cover noise 2.",
            "DOC_ALPHA: Ruling 101.",
            "DOC_TRANSIENT_FAIL: Glitch.",
        ],
        filenames=["bad_obs1.txt", "bad_obs2.txt", "a.txt", "broken.txt"],
        query="Analyze rulings",
        config=DiscoveryConfig(
            batch_strategy="fixed",
            batch_size=2,
            max_observation_unit_attempts=2,
        ),
        llm=ObsUnitAndSplitLLM(),
    )
    assert schema.observation_unit is not None
    assert schema.observation_unit.source_document == "a.txt"
    assert [c.name for c in schema.columns] == ["case_identifier"]
    assert schema.columns[0].source_document == "a.txt"
    assert evo.column_sources == {"case_identifier": "a.txt"}
    assert evo.contributing_files == ["a.txt"]
    assert evo.non_contributing_files == ["bad_obs1.txt", "bad_obs2.txt", "broken.txt"]
    assert evo.skipped_batches_by_reason == {
        "observation_unit_discovery_failed": 1,
        "llm_call_failed": 1,
    }
    assert evo.skipped_filenames == ["bad_obs1.txt", "bad_obs2.txt", "broken.txt"]

    # Exceeding max_observation_unit_attempts=2 aborts before reaching the split batch.
    with pytest.raises(
        RuntimeError, match=r"Failed to discover observation unit \(after 2 attempts\)"
    ):
        discover_schema(
            documents=[
                "DOC_BAD_OBS: Cover 1.",
                "DOC_BAD_OBS: Cover 2.",
                "DOC_ALPHA: Ruling 101.",
                "DOC_TRANSIENT_FAIL: Glitch.",
            ],
            filenames=["bad1.txt", "bad2.txt", "a.txt", "broken.txt"],
            query="Analyze rulings",
            config=DiscoveryConfig(
                batch_strategy="fixed",
                batch_size=1,
                max_observation_unit_attempts=2,
            ),
            llm=ObsUnitAndSplitLLM(),
        )
