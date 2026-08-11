"""Tests for the guard that tells "could not read" apart from "no rows".

``session_data_read_failed`` decides whether an empty page means the session
genuinely has nothing or that its data could not be read. Callers use it to
explain an empty table instead of rendering bare column headers, so a wrong
answer in either direction is user-visible: a false negative shows an
unexplained empty grid, a false positive tells someone their data is missing
when they simply deleted every row.

The four conditions are ordered cheapest-first and only the last touches
storage. That ordering is asserted here, not just the return value, because it
is the reason the guard is safe to call on every read.
"""

from types import SimpleNamespace

import pytest

from app.models.session import PaginatedData
from app.services.pipeline.data_query import session_data_read_failed

SESSION_ID = "sess-guard-1"


def _page(total_count: int) -> PaginatedData:
    return PaginatedData(
        rows=[],
        total_count=total_count,
        filtered_count=None,
        page=0,
        page_size=50,
        has_more=False,
    )


def _session(total_rows):
    if total_rows is None:
        return SimpleNamespace(statistics=None)
    return SimpleNamespace(statistics=SimpleNamespace(total_rows=total_rows))


@pytest.fixture
def spies(monkeypatch):
    """Patch the two helpers and record whether each was consulted."""
    calls = {"enumerate": 0, "storage": 0}
    state = {"local_files": [], "in_storage": False}

    def fake_enumerate(session_id, *args, **kwargs):
        calls["enumerate"] += 1
        return list(state["local_files"])

    async def fake_stored(session_id, *args, **kwargs):
        calls["storage"] += 1
        return state["in_storage"]

    monkeypatch.setattr(
        "app.services.data_utils.enumerate_session_data_files", fake_enumerate
    )
    monkeypatch.setattr(
        "app.services.data_utils.session_has_stored_data", fake_stored
    )
    return calls, state


async def test_rows_present_is_not_a_failure(spies):
    """A page with rows is never a read failure, and costs nothing to check."""
    calls, _state = spies

    assert await session_data_read_failed(SESSION_ID, _page(3), _session(3)) is False
    assert calls == {"enumerate": 0, "storage": 0}


@pytest.mark.parametrize(
    "total_rows",
    [None, 0],
    ids=["no-statistics", "statistics-report-zero"],
)
async def test_session_never_had_rows_is_not_a_failure(spies, total_rows):
    """Without an independent record of rows there is nothing to contradict."""
    calls, _state = spies

    result = await session_data_read_failed(SESSION_ID, _page(0), _session(total_rows))

    assert result is False
    # Short-circuits before touching the filesystem or storage.
    assert calls == {"enumerate": 0, "storage": 0}


async def test_local_file_present_is_not_a_failure(spies, tmp_path):
    """An emptied table keeps its file; that is deletion, not a failed read."""
    calls, state = spies
    data_file = tmp_path / "extracted_data.jsonl"
    data_file.write_text("")
    state["local_files"] = [data_file]

    result = await session_data_read_failed(SESSION_ID, _page(0), _session(12))

    assert result is False
    assert calls["enumerate"] == 1
    # The expensive check is skipped once a local file is found.
    assert calls["storage"] == 0


async def test_no_local_file_and_nothing_stored_is_not_a_failure(spies):
    """Stale statistics with no data anywhere is not a hydration failure."""
    calls, state = spies
    state["local_files"] = []
    state["in_storage"] = False

    result = await session_data_read_failed(SESSION_ID, _page(0), _session(12))

    assert result is False
    assert calls == {"enumerate": 1, "storage": 1}


async def test_rows_in_storage_but_not_local_is_a_failure(spies):
    """The case the guard exists for: the rows exist remotely, hydration failed."""
    calls, state = spies
    state["local_files"] = []
    state["in_storage"] = True

    result = await session_data_read_failed(SESSION_ID, _page(0), _session(12))

    assert result is True
    assert calls == {"enumerate": 1, "storage": 1}


async def test_filtered_page_with_matches_elsewhere_is_not_a_failure(spies):
    """total_count is the unfiltered count, so a filter matching nothing still
    reports rows and must not be mistaken for a failed read."""
    calls, _state = spies
    page = PaginatedData(
        rows=[],
        total_count=40,
        filtered_count=0,
        page=0,
        page_size=50,
        has_more=False,
    )

    assert await session_data_read_failed(SESSION_ID, page, _session(40)) is False
    assert calls == {"enumerate": 0, "storage": 0}
