"""data_missing means the session had rows and they are gone, not "no data yet"."""

import asyncio
from types import SimpleNamespace

import pytest

import app.services as services
import app.services.data_utils as data_utils
from app.services.pipeline import data_query


@pytest.fixture
def no_data_files(monkeypatch):
    async def _none(session_id, work_dir=None, data_dir=None):
        return []

    monkeypatch.setattr(data_utils, "resolve_session_data_files", _none)


def _with_session(monkeypatch, session):
    monkeypatch.setattr(services.session_manager, "get_session", lambda sid: session)


def _get(tmp_path):
    return asyncio.run(data_query.get_data("s1", tmp_path))


@pytest.mark.parametrize(
    "statistics",
    [None, SimpleNamespace(total_rows=0)],
    ids=["schema_only_or_stopped_before_extraction", "zero_rows"],
)
def test_no_rows_yet_is_not_data_missing(tmp_path, monkeypatch, no_data_files, statistics):
    _with_session(monkeypatch, SimpleNamespace(statistics=statistics))

    page = _get(tmp_path)

    assert page.rows == []
    assert page.data_missing is False


def test_session_that_had_rows_is_data_missing(tmp_path, monkeypatch, no_data_files):
    _with_session(monkeypatch, SimpleNamespace(statistics=SimpleNamespace(total_rows=6)))

    assert _get(tmp_path).data_missing is True


def test_unknown_session_still_reports_missing(tmp_path, monkeypatch, no_data_files):
    _with_session(monkeypatch, None)

    assert _get(tmp_path).data_missing is True
