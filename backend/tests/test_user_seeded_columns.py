"""Tests for user-seeded columns: backend wiring (Section D)."""

import json
import tempfile
from pathlib import Path

import pytest

from app.models.schematiq import (
    InitialSchemaColumn,
    ScheMatiQConfig,
    LLMConfig,
)
from app.services.pipeline.config_handler import convert_config_to_schematiq_format
from app.services.pipeline.schema_discovery import _load_initial_schema


def _make_config(initial_schema=None) -> ScheMatiQConfig:
    return ScheMatiQConfig(
        query="test query",
        docs_path="dummy",
        schema_creation_backend=LLMConfig(provider="gemini"),
        value_extraction_backend=LLMConfig(provider="gemini"),
        output_path="out",
        initial_schema=initial_schema,
    )


class TestInitialSchemaColumnModel:
    """D1: InitialSchemaColumn accepts partial input and locked."""

    def test_name_only_with_locked(self):
        col = InitialSchemaColumn(name="x", locked=True)
        assert col.name == "x"
        assert col.definition == ""
        assert col.rationale == ""
        assert col.locked is True

    def test_defaults(self):
        col = InitialSchemaColumn(name="x")
        assert col.definition == ""
        assert col.rationale == ""
        assert col.locked is False
        assert col.allowed_values is None

    def test_full_column(self):
        col = InitialSchemaColumn(
            name="x",
            definition="d",
            rationale="r",
            allowed_values=["a", "b"],
            locked=True,
        )
        assert col.definition == "d"
        assert col.rationale == "r"
        assert col.allowed_values == ["a", "b"]
        assert col.locked is True


class TestConfigConversionPassesLocked:
    """D1: convert_config_to_schematiq_format passes locked through."""

    def test_locked_in_converted_config(self):
        cfg = _make_config(initial_schema=[
            InitialSchemaColumn(name="x", locked=True),
            InitialSchemaColumn(name="y", definition="d", rationale="r"),
        ])
        # Pass minimal extras the converter needs; resolved_docs_paths can be empty list.
        with tempfile.TemporaryDirectory() as td:
            schematiq_config = convert_config_to_schematiq_format(
                cfg,
                session_id="sess1",
                work_dir=Path(td),
                resolved_docs_paths=[],
            )
        assert "initial_schema" in schematiq_config
        x = next(c for c in schematiq_config["initial_schema"] if c["name"] == "x")
        y = next(c for c in schematiq_config["initial_schema"] if c["name"] == "y")
        assert x["locked"] is True
        assert x["definition"] == ""
        assert x["rationale"] == ""
        assert y["locked"] is False


class TestBackendLoadInitialSchema:
    """D2: _load_initial_schema propagates locked through all three formats."""

    def test_inline_list_format(self):
        schematiq_config = {
            "initial_schema": [
                {"name": "x", "locked": True},
                {"name": "y"},
            ]
        }
        schema = _load_initial_schema(schematiq_config, query="q", max_keys=100)
        assert schema is not None
        x = next(c for c in schema.columns if c.name == "x")
        y = next(c for c in schema.columns if c.name == "y")
        assert x.locked is True
        assert y.locked is False

    def test_file_list_format(self, tmp_path):
        data = [{"name": "x", "locked": True, "definition": "d", "rationale": "r"}]
        p = tmp_path / "schema.json"
        p.write_text(json.dumps(data))
        schematiq_config = {"initial_schema_path": str(p)}
        schema = _load_initial_schema(schematiq_config, query="q", max_keys=100)
        assert schema is not None
        assert schema.columns[0].locked is True

    def test_file_dict_with_schema_key(self, tmp_path):
        data = {"schema": [{"name": "x", "locked": True}]}
        p = tmp_path / "schema.json"
        p.write_text(json.dumps(data))
        schematiq_config = {"initial_schema_path": str(p)}
        schema = _load_initial_schema(schematiq_config, query="q", max_keys=100)
        assert schema is not None
        assert schema.columns[0].locked is True

    def test_empty_definition_rationale_pass_through(self):
        schematiq_config = {
            "initial_schema": [
                {"name": "x", "locked": True},
            ]
        }
        schema = _load_initial_schema(schematiq_config, query="q", max_keys=100)
        col = schema.columns[0]
        assert col.definition == ""
        assert col.rationale == ""
