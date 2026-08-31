"""Tests for _scope_known_units on-disk document handling.

A scoped (rows/documents) re-extraction builds its skip map from row/skip
metadata. On-disk files absent from that metadata -- notably collision-renamed
``<name>_N`` duplicates from unique_dest_path -- must be force-skipped ([]) so
the library does not run full observation-unit discovery on them, which would
balloon a targeted fill into unrequested units. See fix:
"force-skip on-disk documents absent from scope metadata".
"""

from unittest.mock import MagicMock

from app.services.reextraction_service import ReextractionService


def _svc() -> ReextractionService:
    return ReextractionService(MagicMock(), MagicMock())


TARGET = "AmericanGateways2025-07-21DDC"
ORPHAN = "Chung2025-06-05SDNY_1"   # collision-renamed duplicate, no metadata row


def test_orphan_on_disk_stem_is_force_skipped():
    """An on-disk file with no metadata entry must be mapped to [] (skip), not
    left absent (which the library treats as 'run discovery')."""
    svc = _svc()
    known_units = {TARGET: ["Amir H. Ali"], "Chung2025-06-05SDNY": []}
    all_stems = set(known_units)
    on_disk = set(known_units) | {ORPHAN}

    scoped = svc._scope_known_units(
        known_units,
        documents=None,
        rows=["Amir H. Ali"],
        all_paper_stems=all_stems,
        on_disk_stems=on_disk,
    )
    # Present in the map and explicitly empty -> skipped for free.
    assert ORPHAN in scoped
    assert scoped[ORPHAN] == []


def test_orphan_without_on_disk_stems_is_absent_regression():
    """Guards the pre-fix behavior: without on_disk_stems the orphan is absent
    from the map (the library would then re-discover it). This documents exactly
    what the fix prevents."""
    svc = _svc()
    known_units = {TARGET: ["Amir H. Ali"], "Chung2025-06-05SDNY": []}
    scoped = svc._scope_known_units(
        known_units,
        documents=None,
        rows=["Amir H. Ali"],
        all_paper_stems=set(known_units),
        on_disk_stems=None,
    )
    assert ORPHAN not in scoped


def test_requested_row_keeps_its_unit():
    """The scoped run still extracts exactly the requested unit."""
    svc = _svc()
    known_units = {TARGET: ["Amir H. Ali"], "Other2025": ["Someone Else"]}
    scoped = svc._scope_known_units(
        known_units,
        documents=None,
        rows=["Amir H. Ali"],
        all_paper_stems=set(known_units),
        on_disk_stems=set(known_units),
    )
    assert scoped[TARGET] == ["Amir H. Ali"]
    # A non-target paper is skipped, not extracted.
    assert scoped["Other2025"] == []


def test_no_scope_returns_known_units_unchanged():
    """With neither documents nor rows, scoping is a no-op."""
    svc = _svc()
    known_units = {TARGET: ["Amir H. Ali"]}
    scoped = svc._scope_known_units(
        known_units, documents=None, rows=None,
        all_paper_stems=set(known_units), on_disk_stems={ORPHAN},
    )
    assert scoped is known_units
