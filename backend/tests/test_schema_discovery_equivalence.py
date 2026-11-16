"""Pre-refactor equivalence tests for backend schema discovery.

Runs ``app.services.pipeline.schema_discovery.run_schema_discovery`` with a
deterministic fake LLM and fixed documents across all major discovery modes and
compares the full output (schema, evolution, saved partial schema, progress
callbacks, WebSocket broadcasts, and LLM call count) against a golden fixture
captured prior to refactoring the loop into ``schematiq-lib``.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.services.pipeline.schema_discovery import run_schema_discovery
from schematiq.core.llm_backends import LLMInterface

GOLDEN_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "schema_discovery_equivalence_golden.json"
)


class FakeDiscoveryLLM(LLMInterface):
    """Deterministic fake LLM for schema discovery equivalence testing."""

    _provider = "openai"

    def __init__(self, context_window_size: int = 16384):
        super().__init__()
        self.model = "fake-deterministic"
        self.context_window_size = context_window_size
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
            has_passages = "<SAMPLE_PASSAGES>" in user
            return json.dumps(
                {
                    "observation_unit": {
                        "name": "Court Ruling",
                        "definition": "A single judicial decision on an appeal",
                        "example_names": ["Appeal 101", "Appeal 202"]
                        if has_passages
                        else [],
                    },
                    "reasoning": "Deterministic test unit",
                }
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
                # Query-only mode
                return json.dumps(
                    {
                        "columns": [
                            {
                                "name": "case_outcome",
                                "definition": "Final appeal outcome",
                                "rationale": "Measures decision result",
                                "value_kind": "category",
                                "allowed_values": ["granted", "denied", "remanded"],
                            },
                            {
                                "name": "applicant_origin",
                                "definition": "Country of origin of the applicant",
                                "rationale": "Tracks demographic context",
                                "value_kind": "free_text",
                                "allowed_values": None,
                            },
                        ]
                    }
                )

            passages_text = passages_match.group(1)
            columns: List[Dict[str, Any]] = []
            if "DOC_ALPHA" in passages_text:
                columns.extend(
                    [
                        {
                            "name": "case_identifier",
                            "definition": "Unique docket identifier of the case",
                            "rationale": "Identifies each ruling uniquely",
                            "value_kind": "identifier",
                            "allowed_values": None,
                        },
                        {
                            "name": "ruling_outcome",
                            "definition": "Outcome of the appeal ruling",
                            "rationale": "Primary dependent outcome variable",
                            "value_kind": "category",
                            "allowed_values": ["granted", "denied"],
                        },
                    ]
                )
            if "DOC_BETA" in passages_text:
                columns.append(
                    {
                        "name": "court_level",
                        "definition": "Level of court issuing the decision",
                        "rationale": "Captures institutional hierarchy",
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

        raise AssertionError(f"Unexpected prompt sent to FakeDiscoveryLLM: {system[:120]}")


class RecordingWSManager:
    """Fake WebSocket manager recording broadcasts and optionally triggering stop."""

    def __init__(self, stop_on_progress_event: bool = False):
        self.events: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []
        self.stop_requested = False
        self.stop_on_progress_event = stop_on_progress_event

    async def broadcast_to_session(self, session_id: str, message: Dict[str, Any]) -> None:
        self.events.append(
            {
                "type": message.get("type"),
                "data": message.get("data"),
            }
        )
        if self.stop_on_progress_event and message.get("type") == "schema_progress":
            self.stop_requested = True

    async def broadcast_log(self, session_id: str, log_data: Dict[str, Any]) -> None:
        self.logs.append(dict(log_data))


def _base_config(**overrides: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "query": "What factors drive asylum appeal outcomes?",
        "max_keys_schema": 20,
        "documents_batch_size": 1,
        "batch_strategy": "fixed",
        "convergence_threshold": 5,
        "schema_creation_backend": {
            "provider": "openai",
            "model": "fake-deterministic",
            "context_window_size": 16384,
        },
    }
    cfg.update(overrides)
    return cfg


async def _run_scenario(
    tmp_path: Path,
    *,
    session_id: str,
    documents: List[str],
    filenames: List[str],
    config: Dict[str, Any],
    stop_on_first_progress: bool = False,
) -> Dict[str, Any]:
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    llm = FakeDiscoveryLLM()
    ws_manager = RecordingWSManager(stop_on_progress_event=stop_on_first_progress)
    progress_events: List[List[Any]] = []

    async def progress_callback(
        stage: str, fraction: float, info: Optional[Dict[str, Any]] = None
    ) -> None:
        progress_events.append([stage, fraction, info])

    def is_stop_requested(sid: str) -> bool:
        assert sid == session_id
        return ws_manager.stop_requested

    schema, evolution = await run_schema_discovery(
        session_id=session_id,
        documents=documents,
        filenames=filenames,
        schematiq_config=config,
        llm=llm,
        retriever=None,
        progress_callback=progress_callback,
        is_stop_requested=is_stop_requested,
        ws_manager=ws_manager,
        work_dir=tmp_path,
    )

    partial_path = session_dir / "discovered_schema.json"
    saved_partial = (
        json.loads(partial_path.read_text(encoding="utf-8"))
        if partial_path.exists()
        else None
    )

    return {
        "schema": schema.to_full_dict(),
        "evolution": evolution.model_dump(),
        "saved_partial_schema": saved_partial,
        "progress_events": progress_events,
        "ws_events": ws_manager.events,
        "ws_logs": ws_manager.logs,
        "llm_call_stages": [c["stage"] for c in llm.calls],
        "llm_call_count": len(llm.calls),
    }


async def _collect_all_scenarios(tmp_path: Path) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}

    results["documents_mode"] = await _run_scenario(
        tmp_path,
        session_id="scen_documents",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard at the appellate court level.",
            "DOC_GAMMA: Additional procedural background.",
        ],
        filenames=["alpha.txt", "beta.txt", "gamma.txt"],
        config=_base_config(),
    )

    results["query_only_mode"] = await _run_scenario(
        tmp_path,
        session_id="scen_query_only",
        documents=[],
        filenames=[],
        config=_base_config(),
    )

    results["initial_schema"] = await _run_scenario(
        tmp_path,
        session_id="scen_initial_schema",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard at the appellate court level.",
        ],
        filenames=["alpha.txt", "beta.txt"],
        config=_base_config(
            initial_schema=[
                {"name": "seeded_judge", "locked": True},
                {
                    "name": "seeded_year",
                    "definition": "Year of ruling",
                    "rationale": "Temporal trend",
                    "locked": True,
                },
            ]
        ),
    )

    results["preconfigured_unit_name_only"] = await _run_scenario(
        tmp_path,
        session_id="scen_unit_name_only",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard at the appellate court level.",
        ],
        filenames=["alpha.txt", "beta.txt"],
        config=_base_config(
            initial_observation_unit={"name": "Appeal Case"}
        ),
    )

    results["preconfigured_unit_name_and_definition"] = await _run_scenario(
        tmp_path,
        session_id="scen_unit_full",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard at the appellate court level.",
        ],
        filenames=["alpha.txt", "beta.txt"],
        config=_base_config(
            initial_observation_unit={
                "name": "Appeal Case",
                "definition": "Pre-defined single appeal ruling",
            }
        ),
    )

    results["convergence_stop"] = await _run_scenario(
        tmp_path,
        session_id="scen_convergence",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_GAMMA: No new columns.",
            "DOC_DELTA: Still no new columns.",
            "DOC_EPSILON: Should not be processed after convergence.",
            "DOC_ZETA: Should not be processed after convergence.",
            "DOC_BETA: Should not be reached after convergence.",
        ],
        filenames=[
            "alpha.txt",
            "gamma.txt",
            "delta.txt",
            "epsilon.txt",
            "zeta.txt",
            "beta.txt",
        ],
        config=_base_config(convergence_threshold=2),
    )

    results["stop_request"] = await _run_scenario(
        tmp_path,
        session_id="scen_stop",
        documents=[
            "DOC_ALPHA: Appeal 101 was granted.",
            "DOC_BETA: Heard at the appellate court level.",
            "DOC_GAMMA: Extra doc.",
            "DOC_DELTA: Extra doc 2.",
        ],
        filenames=["alpha.txt", "beta.txt", "gamma.txt", "delta.txt"],
        config=_base_config(),
        stop_on_first_progress=True,
    )

    return results


@pytest.mark.asyncio
async def test_schema_discovery_equivalence(tmp_path: Path) -> None:
    actual = await _collect_all_scenarios(tmp_path)

    if os.environ.get("SCHEMATIQ_CAPTURE_GOLDEN") == "1":
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(actual, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    assert GOLDEN_PATH.exists(), (
        f"Golden fixture missing at {GOLDEN_PATH}; run with SCHEMATIQ_CAPTURE_GOLDEN=1 first."
    )
    expected = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert actual == expected
