"""Tests for the background reference-fill service.

The model touchpoint (_extract_value_for_row) and IO are stubbed so the loop,
per-row writes, live streaming, completion event, quota accounting and
cancellation are verified deterministically.
"""

import pytest

from app.services.reference_fill_service import ReferenceFillService
from schematiq.core.llm_call_tracker import QuotaExceededError


class _Col:
    def __init__(self, name, definition=""):
        self.name = name
        self.definition = definition


class _Session:
    def __init__(self):
        self.columns = [_Col("appointee", "Appointing president")]


class _Ref:
    id = "ref-1"
    filename = "FJC.csv"


class _WS:
    def __init__(self):
        self.messages = []

    async def broadcast_to_session(self, session_id, message):
        self.messages.append(message)


class _DataEditor:
    def __init__(self):
        self.updates = []

    async def update_cell(self, session_id, row_name, column, value, **kw):
        self.updates.append((row_name, column, value, kw.get("reference_source")))
        return {"status": "success"}


class _Runner:
    def __init__(self, raise_after=None):
        self.recorded = []
        self._n = 0
        self._raise_after = raise_after

    def check_global_quota(self, limit):
        self._n += 1
        if self._raise_after is not None and self._n > self._raise_after:
            raise QuotaExceededError(used=999, limit=limit)

    def record_external_usage(self, source_id, counts):
        self.recorded.append(counts)


def _build(monkeypatch, rows, values, runner=None, reference_text="small ref"):
    ws = _WS()
    editor = _DataEditor()
    runner = runner or _Runner()
    svc = ReferenceFillService(
        websocket_manager=ws,
        session_manager=type("S", (), {"get_session": staticmethod(lambda sid: _Session())})(),
        data_editor=editor,
        schematiq_runner=runner,
    )

    import app.services.reference_document_service as refsvc

    monkeypatch.setattr(refsvc, "get_reference_document", lambda session, rid: _Ref())

    async def fake_load_text(session_id, ref):
        return reference_text

    monkeypatch.setattr(refsvc, "load_reference_text", fake_load_text)

    async def fake_load_all_rows(self, session_id):
        return rows

    async def fake_extract(self, client, unit, column, definition, context):
        # allow a per-row side effect (e.g. request stop) via the values callable
        return values(self, unit) if callable(values) else values[unit]

    monkeypatch.setattr(ReferenceFillService, "_load_all_rows", fake_load_all_rows)
    monkeypatch.setattr(ReferenceFillService, "_extract_value_for_row", fake_extract)
    monkeypatch.setattr(ReferenceFillService, "_get_client", lambda self: object())
    return svc, ws, editor, runner


async def _run_to_completion(svc, result):
    task = svc._tasks[result["fill_id"]]
    await task
    return svc._ops[result["fill_id"]]


@pytest.mark.asyncio
async def test_start_returns_immediately_then_fills_each_row(monkeypatch):
    rows = [
        {"unit_name": "Canby", "source_document": "docA"},
        {"unit_name": "Forrest", "source_document": "docA"},
        {"unit_name": "Unknown", "source_document": "docB"},
    ]
    values = {"Canby": "Democratic", "Forrest": "Trump", "Unknown": "N/A"}
    svc, ws, editor, runner = _build(monkeypatch, rows, values)

    result = await svc.start_fill("sess-1", "appointee", "ref-1")
    assert result["status"] == "started" and result["total"] == 3

    op = await _run_to_completion(svc, result)
    assert op.status == "completed"
    assert op.filled == 2 and op.skipped == 1
    assert [u[0] for u in editor.updates] == ["Canby", "Forrest"]  # N/A not written
    assert all(u[3] == "FJC.csv" for u in editor.updates)  # attributed to reference
    cell_events = [m for m in ws.messages if m["type"] == "cell_extracted"]
    assert len(cell_events) == 2
    complete = [m for m in ws.messages if m["type"] == "reference_fill_completed"]
    assert len(complete) == 1 and complete[0]["data"]["filled"] == 2
    assert runner.recorded == [{"chat": 3}]  # one call per row counted


@pytest.mark.asyncio
async def test_stops_when_quota_reached(monkeypatch):
    rows = [
        {"unit_name": "Canby", "source_document": "docA"},
        {"unit_name": "Forrest", "source_document": "docA"},
    ]
    values = {"Canby": "Democratic", "Forrest": "Trump"}
    svc, ws, editor, runner = _build(monkeypatch, rows, values, runner=_Runner(raise_after=1))

    result = await svc.start_fill("sess-1", "appointee", "ref-1")
    op = await _run_to_completion(svc, result)

    assert op.status == "stopped"
    assert op.filled == 1 and [u[0] for u in editor.updates] == ["Canby"]


@pytest.mark.asyncio
async def test_stop_request_halts_the_loop(monkeypatch):
    rows = [
        {"unit_name": "Canby", "source_document": "docA"},
        {"unit_name": "Forrest", "source_document": "docA"},
        {"unit_name": "Smith", "source_document": "docA"},
    ]

    def values(svc, unit):
        # After the first row is produced, request stop; the loop should end
        # before processing the remaining rows.
        if unit == "Canby":
            svc.request_stop(next(iter(svc._ops)))
        return "X"

    svc, ws, editor, runner = _build(monkeypatch, rows, values)
    result = await svc.start_fill("sess-1", "appointee", "ref-1")
    op = await _run_to_completion(svc, result)

    assert op.status == "stopped"
    assert op.filled == 1  # only the first row was written


@pytest.mark.asyncio
async def test_start_validates_column(monkeypatch):
    svc, *_ = _build(monkeypatch, [], {})
    with pytest.raises(ValueError):
        await svc.start_fill("sess-1", "nope", "ref-1")


@pytest.mark.asyncio
async def test_start_rejects_empty_reference(monkeypatch):
    svc, *_ = _build(monkeypatch, [{"unit_name": "A"}], {"A": "x"}, reference_text="   ")
    with pytest.raises(ValueError):
        await svc.start_fill("sess-1", "appointee", "ref-1")


def test_request_stop_unknown_returns_not_accepted(monkeypatch):
    svc, *_ = _build(monkeypatch, [], {})
    assert svc.request_stop("does-not-exist")["accepted"] is False


@pytest.mark.asyncio
async def test_endpoint_starts_fill_and_maps_errors(monkeypatch):
    from app.api.routes import reference as ref_route
    import app.services.chat.deps as deps

    async def fake_start(session_id, column, reference_id):
        return {"status": "started", "fill_id": "f1", "total": 2}

    monkeypatch.setattr(deps.reference_fill_service, "start_fill", fake_start)
    body = ref_route.FillColumnRequest(column="appointee", reference_id="ref-1")
    result = await ref_route.fill_column_from_reference("sess-1", body)
    assert result["status"] == "started"

    async def fake_err(session_id, column, reference_id):
        raise ValueError("bad request")

    monkeypatch.setattr(deps.reference_fill_service, "start_fill", fake_err)
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        await ref_route.fill_column_from_reference("sess-1", body)
    assert excinfo.value.status_code == 400


@pytest.mark.asyncio
async def test_rejects_concurrent_fill_same_session(monkeypatch):
    from app.services.reference_fill_service import FillOperation

    svc, *_ = _build(monkeypatch, [{"unit_name": "A", "source_document": "d"}], {"A": "x"})
    svc._ops["existing"] = FillOperation(
        fill_id="existing", session_id="sess-1", column="appointee",
        reference_filename="R", total=1, status="running",
    )
    with pytest.raises(ValueError, match="already running"):
        await svc.start_fill("sess-1", "appointee", "ref-1")


@pytest.mark.asyncio
async def test_write_failure_skips_row_not_aborts(monkeypatch):
    rows = [
        {"unit_name": "A", "source_document": "d"},
        {"unit_name": "B", "source_document": "d"},
    ]
    values = {"A": "x", "B": "y"}
    svc, ws, editor, runner = _build(monkeypatch, rows, values)

    good_update = editor.update_cell

    async def failing_update(session_id, row_name, column, value, **kw):
        if row_name == "A":
            raise RuntimeError("boom")
        return await good_update(session_id, row_name, column, value, **kw)

    monkeypatch.setattr(editor, "update_cell", failing_update)

    result = await svc.start_fill("sess-1", "appointee", "ref-1")
    op = await _run_to_completion(svc, result)

    assert op.status == "completed"  # one bad write did not abort the whole run
    assert op.filled == 1 and op.skipped == 1
    assert [u[0] for u in editor.updates] == ["B"]


@pytest.mark.asyncio
async def test_per_row_call_uses_reference_fill_model(monkeypatch):
    """The per-row lookup uses the dedicated REFERENCE_FILL_MODEL (a lightweight
    extraction model), not the chat model."""
    from app.core.config import REFERENCE_FILL_MODEL

    svc = ReferenceFillService(None, None, None, None)
    captured: dict = {}

    class _FakeModels:
        async def generate_content(self, model, contents):
            captured["model"] = model
            return type("R", (), {"text": "Democratic"})()

    class _FakeClient:
        aio = type("A", (), {"models": _FakeModels()})()

    value = await svc._extract_value_for_row(_FakeClient(), "Canby", "appointee", "def", "ctx")
    assert value == "Democratic"
    assert captured["model"] == REFERENCE_FILL_MODEL


@pytest.mark.asyncio
async def test_load_all_rows_import_path_resolves(monkeypatch):
    """Exercises the real _load_all_rows (not stubbed) so a wrong deps import path
    surfaces here. Regression for the 'app.services.deps' ModuleNotFoundError."""
    import app.services.pipeline.data_query as dq

    class _Row:
        def model_dump(self):
            return {"unit_name": "A"}

    class _Data:
        rows = [_Row()]

    async def fake_get_data(session_id, work_dir, page, page_size):
        return _Data()

    monkeypatch.setattr(dq, "get_data", fake_get_data)
    svc = ReferenceFillService(None, None, None, None)
    rows = await svc._load_all_rows("sess-1")  # imports WORK_DIR from chat.deps for real
    assert rows == [{"unit_name": "A"}]


def test_get_client_import_path_resolves(monkeypatch):
    """Exercises the real _get_client so a wrong deps import path surfaces here."""
    import app.services.chat.deps as cdeps

    monkeypatch.setattr(cdeps, "get_gemini_api_key", lambda: "test-key")
    svc = ReferenceFillService(None, None, None, None)
    client = svc._get_client()  # imports get_gemini_api_key from chat.deps for real
    assert client is not None
