"""Tests for _match_extracted_row source disambiguation.

Rows are keyed by (row_name, source_document), so the same unit name appearing
in several documents is stored as separate rows. When a run re-extracts one of
those rows, the matcher must not merge that extraction into a *different*
same-name row from another document. See fix: "stop same-name unit from another
document bleeding into a row".
"""

from unittest.mock import MagicMock

from app.services.reextraction_service import ReextractionService


def _make_service() -> ReextractionService:
    return ReextractionService(MagicMock(), MagicMock())


def _extracted(name: str, source: str, **cols) -> dict:
    row = {"_row_name": name, "_source_document": source}
    row.update(cols)
    return row


NAME = "Randolph D. Moss"
DOC_Y = "AmicaDocY"
DOC_X = "ImmigrantDocX"


def _indexes(extracted_rows):
    """Build the three lookup structures the matcher consumes."""
    by_key = {}
    by_row_name: dict = {}
    by_paper_stem: dict = {}
    for row in extracted_rows:
        src = row["_source_document"]
        by_key[(row["_row_name"], src)] = row
        by_row_name.setdefault(row["_row_name"], []).append(row)
        by_paper_stem.setdefault(src.lower(), []).append(row)
    return by_key, by_row_name, by_paper_stem


def test_exact_key_row_matches_its_own_extraction():
    svc = _make_service()
    only = _extracted(NAME, DOC_Y, policy="Biden")
    by_key, by_row_name, by_paper_stem = _indexes([only])

    match = svc._match_extracted_row(
        NAME, DOC_Y, [f"{DOC_Y}.pdf"], by_key, by_row_name, by_paper_stem, set()
    )
    assert match is only


def test_same_name_row_from_other_document_is_not_matched():
    """The regression guard: only DOC_Y was re-extracted; the DOC_X row with the
    same unit name must NOT pick up DOC_Y's extraction."""
    svc = _make_service()
    only = _extracted(NAME, DOC_Y, policy="Biden")
    by_key, by_row_name, by_paper_stem = _indexes([only])

    match = svc._match_extracted_row(
        NAME, DOC_X, [f"{DOC_X}.pdf"], by_key, by_row_name, by_paper_stem, set()
    )
    assert match is None


def test_row_without_source_falls_back_to_single_same_name_candidate():
    """A row with no source document to disambiguate on still matches the single
    same-name extraction (preserved behavior)."""
    svc = _make_service()
    only = _extracted(NAME, DOC_Y, policy="Biden")
    by_key, by_row_name, by_paper_stem = _indexes([only])

    match = svc._match_extracted_row(
        NAME, "", [], by_key, by_row_name, by_paper_stem, set()
    )
    assert match is only


def test_row_matched_by_paper_when_source_field_absent():
    """A row whose source is only known via its papers list still matches the
    candidate from that same paper."""
    svc = _make_service()
    only = _extracted(NAME, DOC_Y, policy="Biden")
    by_key, by_row_name, by_paper_stem = _indexes([only])

    match = svc._match_extracted_row(
        NAME, "", [f"{DOC_Y}.pdf"], by_key, by_row_name, by_paper_stem, set()
    )
    assert match is only


def test_correct_row_still_matches_when_both_documents_reextracted():
    """When both same-name rows are re-extracted, each row gets its own source's
    extraction via the exact-key path."""
    svc = _make_service()
    ext_y = _extracted(NAME, DOC_Y, policy="Biden")
    ext_x = _extracted(NAME, DOC_X, policy="Trump")
    by_key, by_row_name, by_paper_stem = _indexes([ext_y, ext_x])

    match_x = svc._match_extracted_row(
        NAME, DOC_X, [f"{DOC_X}.pdf"], by_key, by_row_name, by_paper_stem, set()
    )
    match_y = svc._match_extracted_row(
        NAME, DOC_Y, [f"{DOC_Y}.pdf"], by_key, by_row_name, by_paper_stem, set()
    )
    assert match_x is ext_x
    assert match_y is ext_y
