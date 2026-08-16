"""End-to-end schema discovery over real documents, with a mocked LLM.

The engine covered here is exactly what the backend's rediscover / reextract
flows drive when they re-evaluate an imported project's source documents under a
fixed observation unit. The existing suite covers query-only discovery; this
covers the document-driven path — batching, per-document contribution tracking,
and column extraction from actual document content — deterministically, with no
network, by stubbing the LLM's ``generate`` to return canned schema JSON.
"""

import json

from unittest.mock import MagicMock

from schematiq.core.schematiq import discover_schema
from schematiq.core.schema import ObservationUnit


def _mock_llm(schema_response: dict) -> MagicMock:
    """An LLM whose generate() returns a fixed generate_schema payload.

    Mirrors the contract the lib's own tests rely on: the schema stage emits a
    dict with document_helpful / columns / suggested_value_additions; the
    partial-column completion stage (system prompt contains "Do NOT invent new
    columns") is not exercised here because we start from an empty schema.
    """
    def generate(prompt, **kwargs):
        text = json.dumps(prompt) if not isinstance(prompt, str) else prompt
        if "Do NOT invent new columns" in text:
            return json.dumps({"columns": []})
        return json.dumps(schema_response)

    llm = MagicMock()
    llm.generate.side_effect = generate
    llm.max_tokens_for_task = MagicMock(return_value=1024)
    llm._provider = "openai"  # avoid gemini-specific kwargs path
    return llm


def test_discovers_columns_from_a_document():
    """A single helpful document yields a schema with the returned columns."""
    llm = _mock_llm({
        "document_helpful": True,
        "columns": [
            {"name": "case_name", "definition": "Name of the case", "rationale": "id"},
            {"name": "judge_name", "definition": "Name of the judge", "rationale": "unit"},
        ],
        "suggested_value_additions": [],
    })

    schema, contributing, non_contributing, evolution = discover_schema(
        query="judges voting on immigration policy",
        documents=["Trump v. Casa, Inc. — opinion text mentioning Justice X."],
        filenames=["CASA2025-06-27SCt.txt"],
        max_keys_schema=10,
        llm=llm,
        retriever=None,
        documents_batch_size=1,
        context_window_size=4000,
        initial_observation_unit=ObservationUnit(name="Judge", definition="A single judge"),
        discover_observation_unit=False,  # fixed OU, as rediscovery seeds it
        max_iters=3,
    )

    names = {c.name for c in schema.columns}
    assert {"case_name", "judge_name"} <= names
    # The observation unit we seeded is preserved through discovery.
    assert schema.observation_unit is not None
    assert schema.observation_unit.name == "Judge"
    # The document that produced columns is tracked as contributing.
    assert "CASA2025-06-27SCt.txt" in contributing
    # The engine actually called the (mocked) LLM.
    assert llm.generate.call_count >= 1


def test_unhelpful_document_contributes_no_columns():
    """A document the LLM marks unhelpful adds no columns and is non-contributing."""
    llm = _mock_llm({
        "document_helpful": False,
        "columns": [],
        "suggested_value_additions": [],
    })

    schema, contributing, non_contributing, evolution = discover_schema(
        query="judges voting on immigration policy",
        documents=["A document about unrelated equitable remedies."],
        filenames=["UNRELATED.txt"],
        max_keys_schema=10,
        llm=llm,
        retriever=None,
        documents_batch_size=1,
        context_window_size=4000,
        initial_observation_unit=ObservationUnit(name="Judge", definition="A single judge"),
        discover_observation_unit=False,
        max_iters=3,
    )

    assert len(schema.columns) == 0
    assert "UNRELATED.txt" in non_contributing
    assert "UNRELATED.txt" not in contributing
