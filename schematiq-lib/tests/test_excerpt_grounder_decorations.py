"""Excerpt decoration handling in ExcerptGrounder (issue #556).

The model wraps quotes in quotation marks or ellipses that are not in the
source. Short decorated excerpts cannot reach the fuzzy phase, so they were
reported "not_found" and _ground_and_enforce nulled their answers.
"""

from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder

SOURCE = (
    "Before NIEMEYER, KING, and HARRIS, Circuit Judges. Judge Niemeyer wrote "
    "the opinion, in which Judge King joined. The district court granted the "
    "motion for a preliminary injunction."
)


def _status(excerpt):
    return ExcerptGrounder().ground_excerpt(excerpt, SOURCE)[2]


def test_trailing_ellipsis_on_short_excerpt_grounds():
    assert _status("Judge Niemeyer...") != "not_found"


def test_unicode_ellipsis_grounds():
    assert _status("\u2026Judge Niemeyer\u2026") != "not_found"


def test_wrapping_straight_and_curly_quotes_ground():
    assert _status('"Judge Niemeyer wrote"') != "not_found"
    assert _status("\u201cJudge Niemeyer wrote\u201d") != "not_found"


def test_quotes_and_ellipsis_combined_ground():
    assert _status('"...Judge Niemeyer wrote..."') != "not_found"


def test_stripped_match_points_at_real_span():
    start, end, _ = ExcerptGrounder().ground_excerpt('"Judge Niemeyer wrote"', SOURCE)
    assert SOURCE[start:end] == "Judge Niemeyer wrote"


def test_internal_ellipsis_grounds_when_every_segment_exists():
    g = ExcerptGrounder()
    excerpt = "Judge Niemeyer wrote the opinion ... granted the motion"
    start, end, status = g._ground_elided(excerpt, SOURCE, SOURCE.lower())
    assert status == "fuzzy"
    assert SOURCE[start:end] == "Judge Niemeyer wrote the opinion"
    assert g.ground_excerpt(excerpt, SOURCE)[2] != "not_found"


def test_internal_ellipsis_fails_when_a_segment_is_fabricated():
    assert (
        _status("Judge Niemeyer wrote the opinion ... appointed by President Trump")
        == "not_found"
    )


def test_internal_ellipsis_of_only_short_segments_is_not_grounded():
    # Segments too short to be evidence must not pass the elided path.
    g = ExcerptGrounder()
    assert g._ground_elided("Judge ... King", SOURCE, SOURCE.lower())[2] == "not_found"


def test_fabricated_quoted_excerpt_still_not_found():
    assert _status('"appointed by President Trump"') == "not_found"


def test_apostrophe_inside_text_is_not_stripped():
    source = "The court's ruling was reversed on appeal."
    start, end, status = ExcerptGrounder().ground_excerpt("court's ruling", source)
    assert status == "exact"
    assert source[start:end] == "court's ruling"
