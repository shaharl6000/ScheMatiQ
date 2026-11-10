"""Follow-ups to #613: bracket direction per page, and word gaps vs row joins."""

from app.services.document_conversion.convert_to_txt import (
    _fix_bracket_direction,
    _join_rows,
)


def test_brackets_stored_logically_are_put_back():
    # Some producers store RTL brackets in logical form, so the mirroring done
    # while reversing a visual line turns "(א)" into ")א(".
    page = "1. )א( אלה בתי המשפט\n)1( בית המשפט העליון;\nלמשיב )המוסד לביטוח לאומי(, מאפשרת"
    assert _fix_bracket_direction(page) == (
        "1. (א) אלה בתי המשפט\n(1) בית המשפט העליון;\nלמשיב (המוסד לביטוח לאומי), מאפשרת"
    )


def test_correct_brackets_are_kept():
    page = "מ״ח 6148/95 עזריה נ׳ מדינת ישראל, פ״ד נא(2) 334.\n[10] מ״ח 6/84 מאמא נ׳ מדינת ישראל"
    assert _fix_bracket_direction(page) == page


def test_english_brackets_are_not_touched():
    page = "f(X) = exp(2x), where X = (x0, x1)\n)not RTL("
    assert _fix_bracket_direction(page) == page


def test_trailing_space_glyph_is_not_glued_across():
    # Full boxes touch because the first line ends with a space glyph; the
    # inked parts are apart, so the words keep their space.
    built = [((10, 0, 50, 10), "foo"), ((50.5, 0, 90, 10), "bar")]
    visible = [(10, 0, 46, 10), (50.5, 0, 90, 10)]
    assert _join_rows(built, visible) == ["foo bar"]


def test_justified_line_with_a_wide_gap_stays_one_line():
    # A long justified line split at a wide word gap: the full boxes are under
    # a line height apart, so it is one row even though the inked gap is wider.
    left = "La pollution atmosphérique est un problème de santé publique à Paris, " * 2
    built = [((10, 0, 300, 10), left.strip()), ((306, 0, 400, 10), "et, depuis 2001, des")]
    visible = [(10, 0, 296, 10), (306, 0, 400, 10)]
    assert _join_rows(built, visible) == [left.strip() + " et, depuis 2001, des"]
