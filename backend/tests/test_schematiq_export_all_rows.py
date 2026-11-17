"""Exports must include every row, not just the first read's worth.

Before this, the three /api/schematiq export routes read page_size=10000 once,
so a larger session exported a silently truncated CSV / project file.
"""

from __future__ import annotations

from typing import List

import pytest

from app.api.routes import schematiq as schematiq_routes


class _FakeData:
    def __init__(self, shown: int, total: int):
        self.rows = list(range(shown))
        self.total_count = total
        self.has_more = shown < total


class _FakeRunner:
    def __init__(self, total: int):
        self.total = total
        self.page_sizes: List[int] = []

    async def get_data(self, session_id, page=0, page_size=50, **_kwargs):
        self.page_sizes.append(page_size)
        return _FakeData(min(page_size, self.total), self.total)


@pytest.mark.asyncio
async def test_small_session_reads_once(monkeypatch):
    runner = _FakeRunner(total=120)
    monkeypatch.setattr(schematiq_routes, "schematiq_runner", runner)

    data = await schematiq_routes._get_all_rows_for_export("s")

    assert len(data.rows) == 120
    assert runner.page_sizes == [schematiq_routes._EXPORT_FIRST_READ_ROWS]


@pytest.mark.asyncio
async def test_large_session_is_reread_with_full_count(monkeypatch):
    total = schematiq_routes._EXPORT_FIRST_READ_ROWS + 2500
    runner = _FakeRunner(total=total)
    monkeypatch.setattr(schematiq_routes, "schematiq_runner", runner)

    data = await schematiq_routes._get_all_rows_for_export("s")

    assert len(data.rows) == total
    assert data.has_more is False
    assert len(runner.page_sizes) == 2
