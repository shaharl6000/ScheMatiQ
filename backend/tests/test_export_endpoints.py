"""End-to-end HTTP regression tests for /api/load and /api/schematiq export routes.

Covers GitHub Issues #636 (deterministic column ordering in export_upload_csv
when column_order is supplied) and #637 (regression coverage for export,
export-complete, export-rich-csv, and export-schema across UPLOAD and SCHEMATIQ
sessions with commas, double quotes, Hebrew text, lists, and {answer, excerpts}
cells).
"""

from __future__ import annotations

import csv
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx
import pytest
from fastapi import FastAPI

from app.models.session import (
    ColumnInfo,
    DataStatistics,
    ObservationUnitInfo,
    SchemaEvolution,
    SchemaSnapshot,
    SessionMetadata,
    SessionStatus,
    SessionType,
    SkippedDocumentInfo,
    VisualizationSession,
)
from app.services.session_manager import SessionManager
from app.storage.local_backend import LocalStorageBackend

UPLOAD_SESSION_ID = "upload-session-12345678"
FLAT_UPLOAD_SESSION_ID = "flat-upload-87654321"
SCHEMATIQ_SESSION_ID = "schematiq-session-abcdef12"
EMPTY_SCHEMATIQ_SESSION_ID = "schematiq-empty-00000000"

RULING_DOC_BYTES = 'בית המשפט העליון\nהעתירה מתקבלת ("חלקית")\n'.encode("utf-8")
SKIPPED_PDF_BYTES = b"%PDF-1.4\n%fake-skipped-document\n"


def _parse_csv(text: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """Parse CSV text into (fieldnames, rows) using standard csv.DictReader."""
    reader = csv.DictReader(io.StringIO(text))
    assert reader.fieldnames is not None
    return list(reader.fieldnames), list(reader)


def _split_rich_csv(text: str) -> Tuple[List[str], List[str], List[Dict[str, str]]]:
    """Separate leading '#' comment lines from CSV body and parse the CSV body."""
    lines = text.splitlines(keepends=True)
    comments: List[str] = []
    body_start = 0
    for idx, line in enumerate(lines):
        if line.startswith("#"):
            comments.append(line.rstrip("\r\n"))
            body_start = idx + 1
        else:
            break
    csv_body = "".join(lines[body_start:])
    headers, rows = _parse_csv(csv_body)
    return comments, headers, rows


def _shared_columns(*, include_dummy_excerpt: bool = False) -> List[ColumnInfo]:
    cols = [
        ColumnInfo(
            name="case_summary",
            display_name="Case Summary (Official)",
            definition="Summary of the ruling, with commas",
            rationale="Captures core holding",
            data_type="text",
            source_document="ruling_1.txt",
            discovery_iteration=1,
        ),
        ColumnInfo(
            name="Notes, misc",
            definition='Miscellaneous notes ("quoted")',
            rationale="Captures edge remarks",
            data_type="text",
            source_document="ruling_1.txt",
            discovery_iteration=1,
        ),
        ColumnInfo(
            name="hebrew_holding",
            definition="הכרעת בית המשפט",
            rationale="שפת המקור של ההחלטה",
            data_type="text",
            allowed_values=["נדחה", "התקבל", "התקבל חלקית"],
            source_document="ruling_1.txt",
            discovery_iteration=2,
        ),
        ColumnInfo(
            name="keywords",
            definition="List of legal topics",
            rationale="Topic tagging",
            data_type="list",
            source_document="ruling_1.txt",
            discovery_iteration=2,
        ),
        ColumnInfo(
            name="grounded_outcome",
            definition="Outcome with supporting excerpt",
            rationale="Auditability",
            data_type="object",
            allowed_values=["Granted, in part", "Dismissed"],
            source_document="ruling_1.txt",
            discovery_iteration=2,
        ),
        ColumnInfo(
            name="ungrounded_remark",
            definition="Unverified comment",
            rationale="Flags missing evidence",
            data_type="object",
            source_document="ruling_1.txt",
            discovery_iteration=2,
        ),
    ]
    if include_dummy_excerpt:
        cols.append(
            ColumnInfo(
                name="grounded_outcome_excerpt",
                definition="Internal excerpt column",
                rationale="Must be filtered from schema exports",
                data_type="text",
            )
        )
    return cols


def _shared_observation_unit() -> ObservationUnitInfo:
    return ObservationUnitInfo(
        name="Court Ruling",
        definition="A single court ruling, including dissenting opinions",
        example_names=["HCJ 101/24", 'בג"ץ 202/24'],
        source_document="ruling_1.txt",
        discovery_iteration=1,
    )


def _shared_statistics(columns: List[ColumnInfo], total_rows: int = 2) -> DataStatistics:
    real_cols = [c for c in columns if not c.name.lower().endswith("_excerpt")]
    return DataStatistics(
        total_rows=total_rows,
        total_columns=len(real_cols),
        total_documents=2 if total_rows else 0,
        completeness=91.7 if total_rows else 0.0,
        column_stats=real_cols,
        schema_evolution=SchemaEvolution(
            snapshots=[
                SchemaSnapshot(
                    iteration=1,
                    documents_processed=["ruling_1.txt"],
                    total_columns=len(real_cols),
                    new_columns=["case_summary", "Notes, misc"],
                    cumulative_documents=1,
                )
            ],
            column_sources={"case_summary": "ruling_1.txt", "Notes, misc": "ruling_1.txt"},
        ),
        skipped_documents=[
            SkippedDocumentInfo(document="skipped_doc.pdf", reason="Unreadable scan")
        ],
    )


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _seed_session_documents(data_dir: Path, session_id: str) -> None:
    docs_dir = data_dir / session_id / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "ruling_1.txt").write_bytes(RULING_DOC_BYTES)
    (docs_dir / "skipped_doc.pdf").write_bytes(SKIPPED_PDF_BYTES)


def _seed_upload_session(sm: SessionManager, data_dir: Path) -> VisualizationSession:
    cols = _shared_columns(include_dummy_excerpt=False)
    session = VisualizationSession(
        id=UPLOAD_SESSION_ID,
        type=SessionType.UPLOAD,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="hebrew_rulings_import.csv",
            file_size=4096,
            cloud_dataset="supreme_court_hcj",
            document_metadata={"ruling_1.txt": {"url": "https://example.org/ruling_1"}},
        ),
        schema_query="Analyze Israeli Supreme Court rulings — פסקי דין",
        columns=cols,
        statistics=_shared_statistics(cols, total_rows=2),
        observation_unit=_shared_observation_unit(),
    )
    sm.create_session(session)

    session_dir = data_dir / UPLOAD_SESSION_ID
    session_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "row_name": "HCJ 101/24",
            "papers": ["ruling_1.txt"],
            "_unit_name": "Justice Barak, dissenting",
            "_source_document": "ruling_1.txt",
            "data": {
                "case_summary": "alpha, beta, gamma",
                "Notes, misc": 'Court held "dismissed" in §4',
                "hebrew_holding": "בית המשפט העליון — פסק דין",
                "keywords": ["asylum", "שיהוי", "appeal"],
                "grounded_outcome": {
                    "answer": "Granted, in part",
                    "excerpts": [
                        {
                            "text": 'העתירה מתקבלת ("חלקית")',
                            "source": "ruling_1.txt",
                        }
                    ],
                },
                "ungrounded_remark": {
                    "answer": "Unverified note",
                    "excerpts": [],
                    "_ungrounded": True,
                },
            },
        },
        {
            "row_name": 'בג"ץ 202/24',
            "papers": ["ruling_1.txt"],
            "_unit_name": "President Hayut",
            "_source_document": "ruling_1.txt",
            "data": {
                "case_summary": 'Second row, with "quotes" and פסיקה',
                "Notes, misc": "plain note",
                "hebrew_holding": "נדחה",
                "keywords": ["constitutional"],
                "grounded_outcome": {
                    "answer": "Dismissed",
                    "excerpts": [{"text": "העתירה נדחית", "source": "ruling_1.txt"}],
                },
                # Intentionally omit ungrounded_remark to test missing-key fill
            },
        },
    ]
    _write_jsonl(session_dir / "data.jsonl", rows)

    (session_dir / "schematiq_config.json").write_text(
        json.dumps({"documents_batch_size": 4}),
        encoding="utf-8",
    )
    (session_dir / "parsed_schema.json").write_text(
        json.dumps(
            {
                "query": session.schema_query,
                "llm_configuration": {
                    "schema_creation_backend": {
                        "provider": "gemini",
                        "model": "gemini-2.5-flash",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    _seed_session_documents(data_dir, UPLOAD_SESSION_ID)
    return session


def _seed_flat_upload_session(sm: SessionManager, data_dir: Path) -> VisualizationSession:
    cols = [
        ColumnInfo(name="schema_first", definition="First schema column"),
        ColumnInfo(name="Notes, misc", definition="Notes column with comma"),
    ]
    session = VisualizationSession(
        id=FLAT_UPLOAD_SESSION_ID,
        type=SessionType.UPLOAD,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(source="flat_import.jsonl", file_size=512),
        columns=cols,
        statistics=_shared_statistics(cols, total_rows=1),
    )
    sm.create_session(session)

    rows = [
        {
            "_row_name": "doc_flat_1",
            "zeta_extra": "extra, value",
            "alpha_extra": 'quoted "extra"',
            "Notes, misc": "misc remark",
            "schema_first": {
                "answer": "שלום רב",
                "excerpts": [{"text": "שלום רב", "source": "doc_flat_1.txt"}],
            },
        }
    ]
    _write_jsonl(data_dir / FLAT_UPLOAD_SESSION_ID / "data.jsonl", rows)
    return session


def _seed_schematiq_session(
    sm: SessionManager, data_dir: Path, work_dir: Path
) -> VisualizationSession:
    cols = _shared_columns(include_dummy_excerpt=True)
    session = VisualizationSession(
        id=SCHEMATIQ_SESSION_ID,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="ScheMatiQ Query: Analyze Israeli Supreme Court rulings",
            schema_discovery_completed=True,
            total_documents=2,
            processed_documents=2,
        ),
        schema_query="Analyze Israeli Supreme Court rulings — פסקי דין",
        columns=cols,
        statistics=_shared_statistics(cols, total_rows=2),
        observation_unit=_shared_observation_unit(),
    )
    sm.create_session(session)

    extracted_rows = [
        {
            "_row_name": "HCJ 101/24",
            "_papers": ["ruling_1.txt", "annex_a.txt"],
            "_unit_name": "Justice Barak, dissenting",
            "_source_document": "ruling_1.txt",
            "_parent_document": "hcj_bundle.pdf",
            "case_summary": "alpha, beta, gamma",
            "Notes, misc": 'Court held "dismissed" in §4',
            "hebrew_holding": {
                "answer": "בית המשפט העליון — פסק דין",
                "excerpts": [{"text": "פסק דין מנומק", "source": "ruling_1.txt"}],
            },
            "keywords": ["asylum", "שיהוי", "appeal"],
            "grounded_outcome": {
                "answer": "Granted, in part",
                "excerpts": [
                    {
                        "text": 'העתירה מתקבלת ("חלקית")',
                        "source": "ruling_1.txt",
                    }
                ],
            },
            "ungrounded_remark": {
                "answer": "Unverified note",
                "excerpts": [],
                "_ungrounded": True,
            },
        },
        {
            "_row_name": 'בג"ץ 202/24',
            "_papers": ["ruling_1.txt"],
            "_unit_name": "President Hayut",
            "_source_document": "ruling_1.txt",
            "_parent_document": "hcj_bundle.pdf",
            "case_summary": 'Second row, with "quotes" and פסיקה',
            "Notes, misc": "plain note",
            "hebrew_holding": {
                "answer": "נדחה",
                "excerpts": [{"text": "העתירה נדחית", "source": "ruling_1.txt"}],
            },
            "keywords": ["constitutional"],
            "grounded_outcome": {
                "answer": "Dismissed",
                "excerpts": [{"text": "העתירה נדחית", "source": "ruling_1.txt"}],
            },
        },
    ]
    _write_jsonl(work_dir / SCHEMATIQ_SESSION_ID / "extracted_data.jsonl", extracted_rows)

    session_data_dir = data_dir / SCHEMATIQ_SESSION_ID
    session_data_dir.mkdir(parents=True, exist_ok=True)
    (session_data_dir / "schematiq_config.json").write_text(
        json.dumps(
            {
                "schema_creation_backend": {
                    "provider": "gemini",
                    "model": "gemini-2.5-pro",
                },
                "value_extraction_backend": {
                    "provider": "gemini",
                    "model": "gemini-2.5-flash",
                },
            }
        ),
        encoding="utf-8",
    )
    _seed_session_documents(data_dir, SCHEMATIQ_SESSION_ID)
    return session


def _seed_empty_schematiq_session(
    sm: SessionManager, work_dir: Path
) -> VisualizationSession:
    cols = [
        ColumnInfo(name="case_summary", definition="Summary"),
        ColumnInfo(name="hebrew_holding", definition="הכרעה"),
    ]
    session = VisualizationSession(
        id=EMPTY_SCHEMATIQ_SESSION_ID,
        type=SessionType.SCHEMATIQ,
        status=SessionStatus.COMPLETED,
        metadata=SessionMetadata(
            source="ScheMatiQ Schema Only",
            schema_discovery_completed=True,
            total_documents=1,
            processed_documents=1,
        ),
        schema_query="Schema-only query",
        columns=cols,
        statistics=_shared_statistics(cols, total_rows=0),
    )
    sm.create_session(session)
    empty_file = work_dir / EMPTY_SCHEMATIQ_SESSION_ID / "extracted_data.jsonl"
    empty_file.parent.mkdir(parents=True, exist_ok=True)
    empty_file.write_text("", encoding="utf-8")
    return session


@pytest.fixture
def export_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Build an isolated FastAPI app mounting load and schematiq routers."""
    monkeypatch.chdir(tmp_path)

    sessions_dir = tmp_path / "sessions"
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "schematiq_work"
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir(parents=True, exist_ok=True)

    local_storage = LocalStorageBackend(
        sessions_dir=str(sessions_dir),
        data_dir=str(data_dir),
        schematiq_work_dir=str(work_dir),
        datasets_dir=str(datasets_dir),
        templates_dir=str(tmp_path / "templates"),
        initial_schemas_dir=str(tmp_path / "initial_schemas"),
    )
    isolated_sm = SessionManager(storage=local_storage)

    import app.api.routes.load as load_routes
    import app.api.routes.schematiq as schematiq_routes
    import app.services.data_utils as data_utils
    import app.services.document_files as document_files
    import app.services.orchestration as orchestration
    import app.services.session_documents as session_documents
    import app.storage as storage_pkg
    import app.storage.factory as storage_factory

    monkeypatch.setattr(load_routes, "session_manager", isolated_sm)
    monkeypatch.setattr(schematiq_routes, "session_manager", isolated_sm)
    monkeypatch.setattr(orchestration.schematiq_runner, "session_manager", isolated_sm)
    monkeypatch.setattr(orchestration.schematiq_runner, "work_dir", work_dir)
    monkeypatch.setattr(storage_pkg, "get_storage", lambda: local_storage)
    monkeypatch.setattr(storage_factory, "get_storage", lambda: local_storage)
    monkeypatch.setattr(data_utils, "candidate_data_dirs", lambda: [data_dir])
    monkeypatch.setattr(data_utils, "candidate_work_dirs", lambda: [work_dir])
    monkeypatch.setattr(data_utils, "get_data_dir", lambda: data_dir)
    monkeypatch.setattr(data_utils, "get_schematiq_work_dir", lambda: work_dir)
    monkeypatch.setattr(session_documents, "candidate_data_dirs", lambda: [data_dir])
    monkeypatch.setattr(
        document_files.unit_view_service,
        "get_source_documents",
        lambda sid: [{"name": "ruling_1.txt", "row_count": 2}],
    )

    _seed_upload_session(isolated_sm, data_dir)
    _seed_flat_upload_session(isolated_sm, data_dir)
    _seed_schematiq_session(isolated_sm, data_dir, work_dir)
    _seed_empty_schematiq_session(isolated_sm, work_dir)

    test_app = FastAPI()
    test_app.include_router(load_routes.router, prefix="/api/load")
    test_app.include_router(schematiq_routes.router, prefix="/api/schematiq")

    return {
        "app": test_app,
        "session_manager": isolated_sm,
        "data_dir": data_dir,
        "work_dir": work_dir,
    }


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


# ==================== /api/load Export Tests ====================


async def test_load_export_csv_default_and_column_order(export_env):
    async with _client(export_env["app"]) as client:
        # 1. Default order (no column_order): meta -> schema -> _ungrounded_columns
        resp = await client.get(f"/api/load/export/{UPLOAD_SESSION_ID}")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "hebrew_rulings_import_" in resp.headers["content-disposition"]

        headers, rows = _parse_csv(resp.text)
        assert headers == [
            "Doc Name",
            "Observation Unit",
            "Source Document",
            "Case Summary",
            "Notes, misc",
            "Hebrew Holding",
            "Keywords",
            "Grounded Outcome",
            "Ungrounded Remark",
            "Ungrounded Columns",
        ]
        assert len(rows) == 2

        row0, row1 = rows
        assert row0["Doc Name"] == "HCJ 101/24"
        assert row0["Observation Unit"] == "Justice Barak, dissenting"
        assert row0["Source Document"] == "ruling_1.txt"
        assert row0["Case Summary"] == "alpha, beta, gamma"
        assert row0["Notes, misc"] == 'Court held "dismissed" in §4'
        assert row0["Hebrew Holding"] == "בית המשפט העליון — פסק דין"
        assert row0["Keywords"] == "['asylum', 'שיהוי', 'appeal']"
        assert row0["Grounded Outcome"] == "Granted, in part"
        assert row0["Ungrounded Remark"] == "Unverified note"
        assert row0["Ungrounded Columns"] == "Ungrounded Remark"

        assert row1["Doc Name"] == 'בג"ץ 202/24'
        assert row1["Case Summary"] == 'Second row, with "quotes" and פסיקה'
        assert row1["Ungrounded Remark"] == ""
        assert row1["Ungrounded Columns"] == ""

        # 2. Partial column_order (Issue #636): remaining columns follow
        #    meta ('Doc Name', ...) -> schema ('Notes, misc', ...) -> _ungrounded_columns
        partial_resp = await client.get(
            f"/api/load/export/{UPLOAD_SESSION_ID}",
            params={"column_order": "grounded_outcome, grounded_outcome, unknown_col"},
        )
        assert partial_resp.status_code == 200
        partial_headers, _ = _parse_csv(partial_resp.text)
        assert partial_headers == [
            "Grounded Outcome",
            "Doc Name",
            "Observation Unit",
            "Source Document",
            "Case Summary",
            "Notes, misc",
            "Hebrew Holding",
            "Keywords",
            "Ungrounded Remark",
            "Ungrounded Columns",
        ]
        assert partial_headers.index("Doc Name") < partial_headers.index("Notes, misc")

        # 3. Explicit reorder including comma-containing column name 'Notes, misc'
        reorder_resp = await client.get(
            f"/api/load/export/{UPLOAD_SESSION_ID}",
            params={"column_order": "Notes, misc,_row_name"},
        )
        assert reorder_resp.status_code == 200
        reorder_headers, _ = _parse_csv(reorder_resp.text)
        assert reorder_headers[:4] == [
            "Notes, misc",
            "Doc Name",
            "Observation Unit",
            "Source Document",
        ]


async def test_load_export_csv_flat_jsonl_extra_columns(export_env):
    async with _client(export_env["app"]) as client:
        # Default: meta -> schema -> sorted remaining extra columns
        resp = await client.get(f"/api/load/export/{FLAT_UPLOAD_SESSION_ID}")
        assert resp.status_code == 200
        headers, rows = _parse_csv(resp.text)
        assert headers == [
            "Doc Name",
            "Schema First",
            "Notes, misc",
            "Alpha Extra",
            "Zeta Extra",
        ]
        assert rows[0]["Doc Name"] == "doc_flat_1"
        assert rows[0]["Schema First"] == "שלום רב"
        assert rows[0]["Alpha Extra"] == 'quoted "extra"'
        assert rows[0]["Zeta Extra"] == "extra, value"

        # Partial column_order on flat JSONL
        resp_ordered = await client.get(
            f"/api/load/export/{FLAT_UPLOAD_SESSION_ID}",
            params={"column_order": "zeta_extra"},
        )
        assert resp_ordered.status_code == 200
        ordered_headers, _ = _parse_csv(resp_ordered.text)
        assert ordered_headers == [
            "Zeta Extra",
            "Doc Name",
            "Schema First",
            "Notes, misc",
            "Alpha Extra",
        ]


def test_load_export_csv_deterministic_across_pythonhashseed(tmp_path: Path):
    """Issue #636: CSV bytes must be identical across different PYTHONHASHSEED runs."""
    data_file = tmp_path / "data.jsonl"
    _write_jsonl(
        data_file,
        [
            {
                "row_name": "HCJ 101/24",
                "_unit_name": "Justice Barak",
                "_source_document": "ruling_1.txt",
                "data": {
                    "case_summary": "alpha, beta",
                    "Notes, misc": 'Court held "dismissed"',
                    "hebrew_holding": "בית המשפט העליון",
                    "ungrounded_remark": {
                        "answer": "Unverified",
                        "excerpts": [],
                        "_ungrounded": True,
                    },
                },
            }
        ],
    )

    repo_root = Path(__file__).resolve().parents[2]
    pythonpath = os.pathsep.join(
        [str(repo_root / "schematiq-lib"), str(repo_root / "backend")]
    )
    script = """
import asyncio, sys
from pathlib import Path
from app.models.session import ColumnInfo, SessionMetadata, SessionType, VisualizationSession
from app.services.export_service import export_upload_csv

session = VisualizationSession(
    id="hashseed-check",
    type=SessionType.UPLOAD,
    metadata=SessionMetadata(source="hashseed.csv"),
    columns=[
        ColumnInfo(name="case_summary"),
        ColumnInfo(name="Notes, misc"),
        ColumnInfo(name="hebrew_holding"),
        ColumnInfo(name="ungrounded_remark"),
    ],
)

async def main():
    resp = export_upload_csv(session, Path(sys.argv[1]), column_order="case_summary")
    chunks = [chunk async for chunk in resp.body_iterator]
    sys.stdout.buffer.write(b"".join(chunks))

asyncio.run(main())
"""

    outputs: Dict[str, bytes] = {}
    for seed in ("1", "42"):
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = seed
        env["PYTHONPATH"] = pythonpath
        proc = subprocess.run(
            [sys.executable, "-c", script, str(data_file)],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            check=True,
        )
        outputs[seed] = proc.stdout

    assert outputs["1"] == outputs["42"]
    headers, _ = _parse_csv(outputs["1"].decode("utf-8"))
    assert headers == [
        "Case Summary",
        "Doc Name",
        "Observation Unit",
        "Source Document",
        "Notes, misc",
        "Hebrew Holding",
        "Ungrounded Remark",
        "Ungrounded Columns",
    ]


async def test_load_export_complete_json_zip_bundle_and_invalid_format(export_env):
    async with _client(export_env["app"]) as client:
        # 1. format=json
        json_resp = await client.get(
            f"/api/load/export-complete/{UPLOAD_SESSION_ID}",
            params={"format": "json"},
        )
        assert json_resp.status_code == 200
        assert json_resp.headers["content-type"].startswith("application/json")
        assert (
            f"{UPLOAD_SESSION_ID[:8]}_complete_export.json"
            in json_resp.headers["content-disposition"]
        )
        payload = json_resp.json()
        assert payload["session_id"] == UPLOAD_SESSION_ID
        assert payload["session_type"] == "upload"
        assert payload["query"] == "Analyze Israeli Supreme Court rulings — פסקי דין"
        assert len(payload["schema"]["columns"]) == 6
        assert payload["metadata"]["total_rows"] == 2
        assert payload["metadata"]["documents_batch_size"] == 4
        assert payload["metadata"]["skipped_documents"] == [
            {"document": "skipped_doc.pdf", "reason": "Unreadable scan"}
        ]
        assert payload["observation_unit"]["name"] == "Court Ruling"
        assert payload["schema_evolution"]["column_sources"]["case_summary"] == "ruling_1.txt"
        assert len(payload["data"]) == 2
        assert (
            payload["data"][0]["data"]["hebrew_holding"]
            == "בית המשפט העליון — פסק דין"
        )
        assert (
            payload["data"][0]["data"]["grounded_outcome"]["excerpts"][0]["text"]
            == 'העתירה מתקבלת ("חלקית")'
        )

        # 2. format=zip
        zip_resp = await client.get(
            f"/api/load/export-complete/{UPLOAD_SESSION_ID}",
            params={"format": "zip"},
        )
        assert zip_resp.status_code == 200
        assert zip_resp.headers["content-type"].startswith("application/zip")
        with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as zf:
            assert set(zf.namelist()) == {
                "schema.json",
                "column_metadata.csv",
                "data.csv",
                "metadata.json",
            }
            schema_json = json.loads(zf.read("schema.json").decode("utf-8"))
            assert schema_json["llm_configuration"]["schema_creation_backend"]["model"] == "gemini-2.5-flash"

            meta_headers, meta_rows = _parse_csv(
                zf.read("column_metadata.csv").decode("utf-8")
            )
            assert meta_headers == ["Column Name", "Definition", "Rationale", "Data Type"]
            meta_col_names = [r["Column Name"] for r in meta_rows]
            assert "Notes, misc" in meta_col_names
            assert "row_name" in meta_col_names
            assert "papers" in meta_col_names

            data_headers, data_rows = _parse_csv(zf.read("data.csv").decode("utf-8"))
            assert data_headers == sorted(data_headers)
            assert len(data_rows) == 2
            assert data_rows[0]["case_summary"] == "alpha, beta, gamma"
            assert data_rows[0]["Notes, misc"] == 'Court held "dismissed" in §4'
            assert data_rows[0]["hebrew_holding"] == "בית המשפט העליון — פסק דין"

            metadata_json = json.loads(zf.read("metadata.json").decode("utf-8"))
            assert metadata_json["documents_batch_size"] == 4

        # 3. format=bundle
        bundle_resp = await client.get(
            f"/api/load/export-complete/{UPLOAD_SESSION_ID}",
            params={"format": "bundle"},
        )
        assert bundle_resp.status_code == 200
        assert bundle_resp.headers["content-type"].startswith("application/zip")
        with zipfile.ZipFile(io.BytesIO(bundle_resp.content)) as zf:
            names = set(zf.namelist())
            assert "project.json" in names
            assert "documents/ruling_1.txt" in names
            assert "documents/skipped_doc.pdf" in names
            assert zf.read("documents/ruling_1.txt") == RULING_DOC_BYTES
            assert zf.read("documents/skipped_doc.pdf") == SKIPPED_PDF_BYTES
            bundled_project = json.loads(zf.read("project.json").decode("utf-8"))
            assert bundled_project["session_id"] == UPLOAD_SESSION_ID

        # 4. Unsupported format -> 400
        bad_resp = await client.get(
            f"/api/load/export-complete/{UPLOAD_SESSION_ID}",
            params={"format": "xml"},
        )
        assert bad_resp.status_code == 400


async def test_load_export_rich_csv_and_schema(export_env):
    async with _client(export_env["app"]) as client:
        # export-rich-csv
        rich_resp = await client.get(f"/api/load/export-rich-csv/{UPLOAD_SESSION_ID}")
        assert rich_resp.status_code == 200
        assert rich_resp.headers["content-type"].startswith("text/csv")
        comments, headers, rows = _split_rich_csv(rich_resp.text)

        assert any(c == "# Metadata-Rich CSV Export" for c in comments)
        assert any(c == f"# Session ID: {UPLOAD_SESSION_ID}" for c in comments)
        assert any(c == "# Source: hebrew_rulings_import.csv" for c in comments)
        assert any(c.startswith("# Query: Analyze Israeli Supreme Court") for c in comments)
        assert any(c.startswith("# Observation Unit: ") for c in comments)

        # Metadata columns do not get _definition/_rationale/_allowed_values
        assert "_row_name" in headers
        assert "_row_name_definition" not in headers
        assert "hebrew_holding_definition" in headers
        assert "hebrew_holding_rationale" in headers
        assert "hebrew_holding_allowed_values" in headers

        assert len(rows) == 2
        assert rows[0]["_row_name"] == "HCJ 101/24"
        assert rows[0]["case_summary"] == "alpha, beta, gamma"
        assert rows[0]["Notes, misc"] == 'Court held "dismissed" in §4'
        assert rows[0]["hebrew_holding"] == "בית המשפט העליון — פסק דין"
        assert rows[0]["hebrew_holding_definition"] == "הכרעת בית המשפט"
        assert rows[0]["hebrew_holding_allowed_values"] == "נדחה, התקבל, התקבל חלקית"

        # export-schema
        schema_resp = await client.get(f"/api/load/export-schema/{UPLOAD_SESSION_ID}")
        assert schema_resp.status_code == 200
        assert schema_resp.headers["content-type"].startswith("application/json")
        schema_payload = schema_resp.json()
        assert schema_payload["query"] == "Analyze Israeli Supreme Court rulings — פסקי דין"
        assert schema_payload["metadata"]["session_id"] == UPLOAD_SESSION_ID
        assert schema_payload["metadata"]["session_type"] == "upload"
        assert schema_payload["metadata"]["total_columns"] == 6
        assert "schema_discovery_completed" not in schema_payload["metadata"]
        assert "display_name" not in schema_payload["schema"][0]
        assert schema_payload["observation_unit"]["name"] == "Court Ruling"
        assert "schema_evolution" in schema_payload


# ==================== /api/schematiq Export Tests ====================


async def test_schematiq_export_csv_default_column_order_and_schema_only(export_env):
    async with _client(export_env["app"]) as client:
        # 1. Default order
        resp = await client.get(f"/api/schematiq/export/{SCHEMATIQ_SESSION_ID}")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "ScheMatiQ_" in resp.headers["content-disposition"]

        headers, rows = _parse_csv(resp.text)
        assert headers == [
            "Doc Name",
            "Observation Unit",
            "Source Document",
            "Case Summary",
            "Notes, misc",
            "Hebrew Holding",
            "Keywords",
            "Grounded Outcome",
            "Ungrounded Remark",
            "Grounded Outcome Excerpt",
            "Ungrounded Columns",
        ]
        assert len(rows) == 2
        assert rows[0]["Doc Name"] == "HCJ 101/24"
        assert rows[0]["Observation Unit"] == "Justice Barak, dissenting"
        assert rows[0]["Source Document"] == "ruling_1.txt"
        assert rows[0]["Case Summary"] == "alpha, beta, gamma"
        assert rows[0]["Notes, misc"] == 'Court held "dismissed" in §4'
        assert rows[0]["Hebrew Holding"] == "בית המשפט העליון — פסק דין"
        assert rows[0]["Keywords"] == "['asylum', 'שיהוי', 'appeal']"
        assert rows[0]["Grounded Outcome"] == "Granted, in part"
        assert rows[0]["Ungrounded Remark"] == "Unverified note"
        assert rows[0]["Ungrounded Columns"] == "Ungrounded Remark"
        assert rows[1]["Ungrounded Columns"] == ""

        # 2. With column_order (including duplicate and comma-containing column name)
        ordered_resp = await client.get(
            f"/api/schematiq/export/{SCHEMATIQ_SESSION_ID}",
            params={"column_order": "hebrew_holding, Notes, misc, hebrew_holding"},
        )
        assert ordered_resp.status_code == 200
        ordered_headers, _ = _parse_csv(ordered_resp.text)
        assert ordered_headers == [
            "Doc Name",
            "Observation Unit",
            "Source Document",
            "Hebrew Holding",
            "Notes, misc",
            "Case Summary",
            "Grounded Outcome",
            "Grounded Outcome Excerpt",
            "Keywords",
            "Ungrounded Remark",
            "Ungrounded Columns",
        ]

        # 3. Schema-only (0 rows) session
        empty_resp = await client.get(
            f"/api/schematiq/export/{EMPTY_SCHEMATIQ_SESSION_ID}"
        )
        assert empty_resp.status_code == 200
        assert "ScheMatiQ_schema_only_" in empty_resp.headers["content-disposition"]
        empty_headers, empty_rows = _parse_csv(empty_resp.text)
        assert empty_headers == ["Doc Name", "Case Summary", "Hebrew Holding"]
        assert empty_rows == []


async def test_schematiq_export_complete_json_zip_bundle_and_invalid_format(export_env):
    async with _client(export_env["app"]) as client:
        # 1. format=json
        json_resp = await client.get(
            f"/api/schematiq/export-complete/{SCHEMATIQ_SESSION_ID}",
            params={"format": "json"},
        )
        assert json_resp.status_code == 200
        payload = json_resp.json()
        assert payload["session_id"] == SCHEMATIQ_SESSION_ID
        assert payload["session_type"] == "schematiq"
        # *_excerpt dummy column filtered out of schema export
        exported_col_names = [c["name"] for c in payload["schema"]["columns"]]
        assert "grounded_outcome_excerpt" not in exported_col_names
        assert len(exported_col_names) == 6
        assert payload["schema"]["columns"][0]["display_name"] == "Case Summary (Official)"
        assert payload["metadata"]["total_columns"] == 6
        assert payload["metadata"]["total_rows"] == 2
        assert payload["metadata"]["schema_discovery_completed"] is True
        assert payload["metadata"]["skipped_documents"] == [
            {"document": "skipped_doc.pdf", "reason": "Unreadable scan"}
        ]
        assert payload["llm_configuration"]["schema_creation_backend"]["model"] == "gemini-2.5-pro"
        assert payload["observation_unit"]["name"] == "Court Ruling"
        assert payload["data"][0]["_parent_document"] == "hcj_bundle.pdf"

        # 2. format=zip
        zip_resp = await client.get(
            f"/api/schematiq/export-complete/{SCHEMATIQ_SESSION_ID}",
            params={"format": "zip"},
        )
        assert zip_resp.status_code == 200
        with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as zf:
            assert set(zf.namelist()) == {
                "schema.json",
                "column_metadata.csv",
                "data.csv",
                "metadata.json",
            }
            schema_json = json.loads(zf.read("schema.json").decode("utf-8"))
            assert all(
                not c["name"].endswith("_excerpt") for c in schema_json["schema"]
            )
            assert schema_json["llm_configuration"]["value_extraction_backend"]["model"] == "gemini-2.5-flash"

            _, meta_rows = _parse_csv(zf.read("column_metadata.csv").decode("utf-8"))
            meta_names = [r["Column Name"] for r in meta_rows]
            assert "{column}_excerpt" in meta_names
            assert "grounded_outcome_excerpt" not in meta_names

            data_headers, data_rows = _parse_csv(zf.read("data.csv").decode("utf-8"))
            assert data_headers == sorted(data_headers)
            assert "grounded_outcome_excerpt" in data_headers
            assert data_rows[0]["papers"] == "ruling_1.txt; annex_a.txt"
            assert data_rows[0]["grounded_outcome"] == "Granted, in part"
            assert (
                data_rows[0]["grounded_outcome_excerpt"]
                == '[ruling_1.txt] העתירה מתקבלת ("חלקית")'
            )
            assert data_rows[0]["hebrew_holding"] == "בית המשפט העליון — פסק דין"
            assert data_rows[0]["keywords"] == "['asylum', 'שיהוי', 'appeal']"

        # 3. format=bundle
        bundle_resp = await client.get(
            f"/api/schematiq/export-complete/{SCHEMATIQ_SESSION_ID}",
            params={"format": "bundle"},
        )
        assert bundle_resp.status_code == 200
        with zipfile.ZipFile(io.BytesIO(bundle_resp.content)) as zf:
            names = set(zf.namelist())
            assert "project.json" in names
            assert "documents/ruling_1.txt" in names
            assert "documents/skipped_doc.pdf" in names
            assert zf.read("documents/ruling_1.txt") == RULING_DOC_BYTES
            assert zf.read("documents/skipped_doc.pdf") == SKIPPED_PDF_BYTES

        # 4. Unsupported format -> 400
        bad_resp = await client.get(
            f"/api/schematiq/export-complete/{SCHEMATIQ_SESSION_ID}",
            params={"format": "xml"},
        )
        assert bad_resp.status_code == 400


async def test_schematiq_export_rich_csv_and_schema(export_env):
    async with _client(export_env["app"]) as client:
        # 1. export-rich-csv
        rich_resp = await client.get(
            f"/api/schematiq/export-rich-csv/{SCHEMATIQ_SESSION_ID}"
        )
        assert rich_resp.status_code == 200
        comments, headers, rows = _split_rich_csv(rich_resp.text)

        assert any(c == "# Metadata-Rich CSV Export" for c in comments)
        assert any(c == f"# Session ID: {SCHEMATIQ_SESSION_ID}" for c in comments)
        assert not any(c.startswith("# Source:") for c in comments)
        assert any(c.startswith("# Query: Analyze Israeli Supreme Court") for c in comments)
        assert any(c.startswith("# Observation Unit: ") for c in comments)

        assert "grounded_outcome_excerpt" in headers
        assert "grounded_outcome_excerpt_definition" not in headers
        assert "grounded_outcome_grounded" in headers
        assert "ungrounded_remark_grounded" in headers

        assert len(rows) == 2
        assert rows[0]["papers"] == "ruling_1.txt; annex_a.txt"
        assert rows[0]["grounded_outcome"] == "Granted, in part"
        assert (
            rows[0]["grounded_outcome_excerpt"]
            == '[ruling_1.txt] העתירה מתקבלת ("חלקית")'
        )
        assert rows[0]["grounded_outcome_grounded"] == "true"
        assert rows[0]["grounded_outcome_allowed_values"] == "Granted, in part, Dismissed"
        assert rows[0]["ungrounded_remark"] == "Unverified note"
        assert rows[0]["ungrounded_remark_grounded"] == "false"
        assert rows[1]["ungrounded_remark_grounded"] == ""

        # Schema-only (0 rows) rich CSV -> 404
        empty_rich = await client.get(
            f"/api/schematiq/export-rich-csv/{EMPTY_SCHEMATIQ_SESSION_ID}"
        )
        assert empty_rich.status_code == 404

        # 2. export-schema
        schema_resp = await client.get(
            f"/api/schematiq/export-schema/{SCHEMATIQ_SESSION_ID}"
        )
        assert schema_resp.status_code == 200
        schema_payload = schema_resp.json()
        assert schema_payload["query"] == "Analyze Israeli Supreme Court rulings — פסקי דין"
        col_names = [c["name"] for c in schema_payload["schema"]]
        assert "grounded_outcome_excerpt" not in col_names
        assert len(col_names) == 6
        assert schema_payload["schema"][0]["display_name"] == "Case Summary (Official)"
        assert schema_payload["metadata"]["schema_discovery_completed"] is True
        assert schema_payload["metadata"]["total_columns"] == 6
        assert schema_payload["observation_unit"]["name"] == "Court Ruling"
        assert "schema_evolution" in schema_payload
        assert (
            schema_payload["llm_configuration"]["schema_creation_backend"]["model"]
            == "gemini-2.5-pro"
        )


@pytest.mark.parametrize(
    "endpoint_template",
    [
        "/api/load/export/{sid}",
        "/api/load/export-complete/{sid}",
        "/api/load/export-rich-csv/{sid}",
        "/api/load/export-schema/{sid}",
        "/api/schematiq/export/{sid}",
        "/api/schematiq/export-complete/{sid}",
        "/api/schematiq/export-rich-csv/{sid}",
        "/api/schematiq/export-schema/{sid}",
    ],
)
async def test_export_endpoints_unknown_session_returns_404(
    export_env, endpoint_template: str
):
    async with _client(export_env["app"]) as client:
        resp = await client.get(endpoint_template.format(sid="missing-session-id"))
        assert resp.status_code == 404


async def test_load_export_missing_or_empty_data_file_returns_404(export_env):
    sm: SessionManager = export_env["session_manager"]
    data_dir: Path = export_env["data_dir"]

    nodata_id = "upload-nodata-00000001"
    sm.create_session(
        VisualizationSession(
            id=nodata_id,
            type=SessionType.UPLOAD,
            status=SessionStatus.COMPLETED,
            metadata=SessionMetadata(source="nodata.csv"),
            columns=[ColumnInfo(name="case_summary")],
        )
    )

    async with _client(export_env["app"]) as client:
        # Missing data.jsonl -> 404
        assert (await client.get(f"/api/load/export/{nodata_id}")).status_code == 404
        assert (
            await client.get(f"/api/load/export-rich-csv/{nodata_id}")
        ).status_code == 404

        # Empty data.jsonl -> 404
        empty_path = data_dir / nodata_id / "data.jsonl"
        empty_path.parent.mkdir(parents=True, exist_ok=True)
        empty_path.write_text("\n", encoding="utf-8")
        assert (await client.get(f"/api/load/export/{nodata_id}")).status_code == 404
        assert (
            await client.get(f"/api/load/export-rich-csv/{nodata_id}")
        ).status_code == 404
