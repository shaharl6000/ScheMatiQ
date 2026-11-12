"""value_kind from schema discovery is kept on the session and in the saved schema."""

import json
from types import SimpleNamespace

from schematiq.core.schema import Column, Schema

from app.models.session import ColumnInfo
from app.services.continue_discovery_service import ContinueDiscoveryService
from app.services.schematiq_runner import ScheMatiQRunner


class _FakeSessionManager:
    def __init__(self):
        self.session = SimpleNamespace(
            status=None,
            metadata=SimpleNamespace(schema_discovery_completed=False),
            columns=[],
            schema_query=None,
            observation_unit=None,
        )

    def get_session(self, session_id):
        return self.session

    def update_session(self, session):
        self.session = session


def test_save_schema_keeps_value_kind(tmp_path):
    runner = ScheMatiQRunner.__new__(ScheMatiQRunner)
    runner.work_dir = tmp_path
    runner.session_manager = _FakeSessionManager()
    (tmp_path / "s1").mkdir()
    schema = Schema(
        query="q",
        columns=[
            Column(name="judge_name", definition="d", rationale="r", value_kind="identifier"),
            Column(name="court_level", definition="d", rationale="r",
                   allowed_values=["district", "appeals"], value_kind="category"),
            Column(name="notes", definition="d", rationale="r"),
        ],
        max_keys=10,
    )

    runner._save_schema_to_session("s1", schema, {"query": "q"}, schema_evolution=None)

    kinds = {c.name: c.value_kind for c in runner.session_manager.session.columns}
    assert kinds == {"judge_name": "identifier", "court_level": "category", "notes": None}
    saved = json.loads((tmp_path / "s1" / "discovered_schema.json").read_text())
    saved_kinds = {c["name"]: c.get("value_kind") for c in saved["schema"]}
    assert saved_kinds == {"judge_name": "identifier", "court_level": "category", "notes": None}


def test_continue_discovery_passes_value_kind_back_to_the_lib():
    service = ContinueDiscoveryService.__new__(ContinueDiscoveryService)
    columns = [
        ColumnInfo(name="case_name", value_kind="identifier"),
        ColumnInfo(name="court_level", allowed_values=["district"], value_kind="category"),
        ColumnInfo(name="old_column"),
    ]

    schema = service._convert_session_columns_to_schema(columns, "q")

    assert {c.name: c.value_kind for c in schema.columns} == {
        "case_name": "identifier",
        "court_level": "category",
        "old_column": None,
    }


def test_schema_export_and_import_keep_value_kind(tmp_path):
    import asyncio

    from app.services.export_service import _build_schema_only_payload
    from app.services.file_parser import FileParser

    columns = [
        ColumnInfo(name="judge_name", definition="d", value_kind="identifier"),
        ColumnInfo(name="court_level", definition="d", allowed_values=["district", "appeals"], value_kind="category"),
    ]
    session = SimpleNamespace(
        columns=columns,
        type=SimpleNamespace(value="schematiq"),
        metadata=SimpleNamespace(
            created=__import__("datetime").datetime(2026, 11, 12),
            last_modified=__import__("datetime").datetime(2026, 11, 12),
            schema_discovery_completed=True,
            source="test",
        ),
        schema_query="q",
        observation_unit=None,
        statistics=None,
    )
    payload = _build_schema_only_payload(
        session, "s1", include_display_name=True, include_discovery_completed=True
    )
    exported = {c["name"]: c.get("value_kind") for c in payload["schema"]}
    assert exported == {"judge_name": "identifier", "court_level": "category"}

    export_file = tmp_path / "export.json"
    export_file.write_text(json.dumps({
        "session_id": "s1",
        "query": "q",
        "schema": {"columns": payload["schema"]},
        "data": [{"row_name": "Judge A", "papers": ["a.txt"],
                  "data": {"judge_name": "Edward M. Chen", "court_level": "district"}}],
    }))
    parsed = asyncio.run(FileParser()._parse_json(export_file))
    imported = {c.name: c.value_kind for c in parsed["columns"]}
    assert imported["judge_name"] == "identifier"
    assert imported["court_level"] == "category"
