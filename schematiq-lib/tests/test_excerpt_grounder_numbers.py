"""Numbers must match exactly in fuzzy excerpt grounding (issue #557).

The fuzzy phase compares whole strings, so an excerpt with a wrong number
could ground on a window that differs only in that number. A wrong Westlaw
citation was accepted this way.
"""

from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder

SOURCE = (
    "2025 WL 1325305 Only the Westlaw citation is currently available. "
    "United States Court of Appeals, Ninth Circuit. In all other respects, "
    "the district court's February 28, 2025 preliminary injunction order is "
    "stayed. See Trump v. Hawaii, 585 U.S. 667, 684 (2018) (explaining the "
    "standard). All Citations Not Reported in Fed. Rptr., 2025 WL 1325305"
)


def _status(excerpt):
    return ExcerptGrounder().ground_excerpt(excerpt, SOURCE)[2]


def test_wrong_citation_number_is_not_grounded():
    assert _status("2025 WL 1825431") == "not_found"


def test_correct_citation_still_grounds():
    assert _status("2025 WL 1325305") == "exact"


def test_wrong_page_number_is_not_grounded():
    assert _status("Trump v. Hawaii, 585 U.S. 999, 684 (2018)") == "not_found"


def test_wrong_date_is_not_grounded():
    assert _status("the district court's February 27, 2025 preliminary injunction order") == "not_found"


def test_wording_difference_with_correct_numbers_still_fuzzy():
    assert _status("See Trump versus Hawaii, 585 U.S. 667, 684 (2018)") == "fuzzy"


def test_text_only_fuzzy_match_unchanged():
    assert _status("In all other respect, the district court preliminary injunction order is stayed") == "fuzzy"
