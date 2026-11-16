"""Pre-refactor golden tests for CLI ``main()`` and legacy ``core.schematiq.discover_schema``.

Captured on ``main`` before consolidating duplicate discovery loops into
``schematiq.discovery.discover_schema``. Must keep passing unchanged after
routing ``core.schematiq.main()`` and deprecated ``core.schematiq.discover_schema``
through the single library implementation.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from google.genai import errors as genai_errors

from schematiq.core import schematiq as sq
from schematiq.core import utils as core_utils
from schematiq.core.llm_backends import LLMInterface, LLMRunAborted
from schematiq.core.schema import ObservationUnit

GOLDEN_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "legacy_discovery_equivalence_golden.json"
)


def _outage_error() -> genai_errors.APIError:
    with pytest.raises(genai_errors.APIError) as excinfo:
        genai_errors.APIError.raise_error(
            503,
            {
                "error": {
                    "code": 503,
                    "status": "UNAVAILABLE",
                    "message": "The model is overloaded",
                }
            },
            None,
        )
    return excinfo.value


class FakeLegacyDiscoveryLLM(LLMInterface):
    """Deterministic fake LLM for CLI main() and legacy core.schematiq.discover_schema."""

    _provider = "openai"

    def __init__(self, context_window_size: int = 16384):
        super().__init__()
        self.model = "fake-legacy-deterministic"
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
            has_passages = "<PASSAGES>" in user
            return json.dumps(
                {
                    "observation_unit": {
                        "name": "Court Ruling",
                        "definition": "A single judicial decision on an appeal",
                        "example_names": ["Appeal 101", "Appeal 202"] if has_passages else [],
                    },
                    "reasoning": "Deterministic test unit",
                }
            )

        if "SchemaLLM" in system:
            self.calls.append({"stage": "schema_discovery", "user": user})
            passages_match = re.search(
                r"<PASSAGES>(.*?)</PASSAGES>", user, flags=re.DOTALL
            )
            passages_text = passages_match.group(1) if passages_match else user

            if "DOC_TRANSIENT_FAIL" in passages_text:
                raise RuntimeError("transient timeout")
            if "DOC_OUTAGE" in passages_text:
                raise _outage_error()
            if "DOC_FAIL_ONE" in passages_text:
                raise RuntimeError("fail one")
            if "DOC_FAIL_TWO" in passages_text:
                raise RuntimeError("fail two")

            if "DOC_UNHELPFUL" in passages_text:
                return json.dumps(
                    {
                        "document_helpful": False,
                        "columns": [],
                        "suggested_value_additions": [],
                    }
                )

            if "DOC_NO_NEW" in passages_text:
                return json.dumps(
                    {
                        "document_helpful": True,
                        "columns": [],
                        "suggested_value_additions": [],
                    }
                )

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
            if "DOC_GAMMA_COL" in passages_text:
                columns.append(
                    {
                        "name": "filing_year",
                        "definition": "Year the appeal was filed",
                        "rationale": "Tracks temporal filing pattern",
                        "value_kind": "number",
                        "allowed_values": None,
                    }
                )
            if "DOC_DELTA_COL" in passages_text:
                columns.append(
                    {
                        "name": "country_of_origin",
                        "definition": "Appellant country of origin",
                        "rationale": "Tracks regional claim trends",
                        "value_kind": "free_text",
                        "allowed_values": None,
                    }
                )

            return json.dumps(
                {
                    "document_helpful": True,
                    "columns": columns,
                    "suggested_value_additions": [],
                }
            )

        raise AssertionError(
            f"Unexpected prompt sent to FakeLegacyDiscoveryLLM: {system[:120]}"
        )


def _run_cli_scenario(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    scenario_name: str,
    files: List[tuple[str, str]],
    seed_columns: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    root = tmp_path / scenario_name
    docs_dir = root / "docs"
    file_dirs: List[str] = []
    for idx, (fname, text) in enumerate(files):
        # One file per subdirectory makes load_documents' rglob("*") order
        # OS-independent (macOS APFS vs Linux ext4/tmpfs) across runners.
        sub = docs_dir / f"d{idx}"
        sub.mkdir(parents=True, exist_ok=True)
        (sub / fname).write_text(text, encoding="utf-8")
        file_dirs.append(str(sub))

    output_path = root / "schema_output.json"
    cfg: Dict[str, Any] = {
        "query": "What factors drive asylum appeal outcomes?",
        "docs_path": file_dirs,
        "document_randomization_seed": None,
        "documents_batch_size": 1,
        "max_keys_schema": 15,
        "output_path": str(output_path),
        "backend": {
            "provider": "openai",
            "model": "fake-legacy-deterministic",
            "context_window_size": 16384,
        },
    }
    if seed_columns is not None:
        seed_path = root / "seed_schema.json"
        seed_path.write_text(json.dumps(seed_columns), encoding="utf-8")
        cfg["initial_schema_path"] = "seed_schema.json"

    cfg_path = root / "config.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    fake_llm = FakeLegacyDiscoveryLLM()
    monkeypatch.setattr(core_utils, "build_llm", lambda _backend_cfg: fake_llm)

    sq.main(cfg_path)

    artifact = json.loads(output_path.read_text(encoding="utf-8"))
    artifact["docs_path"] = "docs"
    return {
        "artifact": artifact,
        "llm_call_stages": [c["stage"] for c in fake_llm.calls],
        "llm_call_count": len(fake_llm.calls),
    }


def _collect_all(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    # A1. CLI main() with relative initial_schema_path and early convergence (thresh=1)
    results["cli_seed_and_early_convergence"] = _run_cli_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="cli_seed",
        files=[
            ("01_alpha.txt", "DOC_ALPHA: Ruling 101 granted asylum."),
            ("02_unhelpful.txt", "DOC_UNHELPFUL: Boilerplate cover page."),
            ("03_beta.txt", "DOC_BETA: Heard at appellate court level."),
        ],
        seed_columns=[
            {
                "name": "seeded_court",
                "definition": "Name of the reviewing court",
                "rationale": "Tracks jurisdiction",
            }
        ],
    )

    # A2. CLI main() multi-batch with helpful/empty non-contributing doc + convergence stop
    results["cli_multi_batch_contributions"] = _run_cli_scenario(
        tmp_path,
        monkeypatch,
        scenario_name="cli_multi",
        files=[
            ("01_alpha.txt", "DOC_ALPHA: Ruling 101 granted asylum."),
            ("02_beta.txt", "DOC_BETA: Heard at appellate court level."),
            ("03_nonew.txt", "DOC_NO_NEW: Repeats existing column info only."),
            ("04_extra.txt", "DOC_GAMMA_COL: Should not be reached after convergence."),
        ],
    )

    # B1. Direct core.schematiq.discover_schema: skips single transient batch failure
    llm_fail_skip = FakeLegacyDiscoveryLLM()
    schema_skip, contrib_skip, non_contrib_skip, evo_skip = sq.discover_schema(
        query="What factors drive asylum appeal outcomes?",
        documents=[
            "DOC_ALPHA: Ruling 101 granted asylum.",
            "DOC_TRANSIENT_FAIL: Transient network glitch.",
            "DOC_BETA: Heard at appellate court level.",
        ],
        filenames=["a.txt", "bad.txt", "b.txt"],
        max_keys_schema=15,
        llm=llm_fail_skip,
        retriever=None,
        documents_batch_size=1,
        context_window_size=16384,
        max_iters=6,
    )
    results["core_batch_failure_skipped"] = {
        "schema": schema_skip.to_full_dict(),
        "contributing_files": contrib_skip,
        "non_contributing_files": non_contrib_skip,
        "evolution": evo_skip.to_dict(),
        "llm_call_stages": [c["stage"] for c in llm_fail_skip.calls],
        "llm_call_count": len(llm_fail_skip.calls),
    }

    # B2. Direct core.schematiq.discover_schema: 3 consecutive outage errors abort with LLMRunAborted
    llm_outage = FakeLegacyDiscoveryLLM()
    with pytest.raises(LLMRunAborted) as outage_exc:
        sq.discover_schema(
            query="What factors drive asylum appeal outcomes?",
            documents=[
                "DOC_ALPHA: Ruling 101 granted asylum.",
                "DOC_OUTAGE: Service unavailable 1.",
                "DOC_OUTAGE: Service unavailable 2.",
                "DOC_OUTAGE: Service unavailable 3.",
                "DOC_BETA: Should not be attempted after 3 outages.",
            ],
            filenames=["a.txt", "o1.txt", "o2.txt", "o3.txt", "b.txt"],
            max_keys_schema=15,
            llm=llm_outage,
            retriever=None,
            documents_batch_size=1,
            context_window_size=16384,
            max_iters=6,
        )
    results["core_three_consecutive_outages"] = {
        "exception_type": type(outage_exc.value).__name__,
        "mentions_consecutive_count": "3 batches failed in a row" in str(outage_exc.value),
        "llm_call_stages": [c["stage"] for c in llm_outage.calls],
        "llm_call_count": len(llm_outage.calls),
    }

    # B3. Direct core.schematiq.discover_schema: all batches fail -> re-raises last error
    llm_all_fail = FakeLegacyDiscoveryLLM()
    with pytest.raises(RuntimeError) as all_fail_exc:
        sq.discover_schema(
            query="What factors drive asylum appeal outcomes?",
            documents=["DOC_FAIL_ONE: First fail.", "DOC_FAIL_TWO: Second fail."],
            filenames=["f1.txt", "f2.txt"],
            max_keys_schema=15,
            llm=llm_all_fail,
            retriever=None,
            documents_batch_size=1,
            context_window_size=16384,
            max_iters=6,
            initial_observation_unit=ObservationUnit(
                name="Court Ruling",
                definition="A single judicial decision on an appeal",
            ),
        )
    results["core_all_batches_failed"] = {
        "exception_type": type(all_fail_exc.value).__name__,
        "exception_message": str(all_fail_exc.value),
        "llm_call_stages": [c["stage"] for c in llm_all_fail.calls],
        "llm_call_count": len(llm_all_fail.calls),
    }

    # B4. Direct core.schematiq.discover_schema: max_iters caps batches processed
    llm_cap = FakeLegacyDiscoveryLLM()
    schema_cap, contrib_cap, non_contrib_cap, evo_cap = sq.discover_schema(
        query="What factors drive asylum appeal outcomes?",
        documents=[
            "DOC_ALPHA: Ruling 101.",
            "DOC_BETA: Appellate court.",
            "DOC_GAMMA_COL: Filing year 2024.",
            "DOC_DELTA_COL: Country X.",
        ],
        filenames=["d1.txt", "d2.txt", "d3.txt", "d4.txt"],
        max_keys_schema=15,
        llm=llm_cap,
        retriever=None,
        documents_batch_size=1,
        context_window_size=16384,
        max_iters=2,
    )
    results["core_max_iters_cap"] = {
        "schema": schema_cap.to_full_dict(),
        "contributing_files": contrib_cap,
        "non_contributing_files": non_contrib_cap,
        "evolution": evo_cap.to_dict(),
        "llm_call_stages": [c["stage"] for c in llm_cap.calls],
        "llm_call_count": len(llm_cap.calls),
    }

    return results


def test_legacy_discovery_equivalence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    actual = _collect_all(tmp_path, monkeypatch)

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
