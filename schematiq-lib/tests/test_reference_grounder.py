"""Tests for grounding excerpts in attached reference documents.

Regression coverage for a real bug: a value the model correctly took from an
attached reference document carries an excerpt quoted from the reference, so
source-text grounding reports it ``not_found`` and _ground_and_enforce flagged
(or nulled) a correct answer.
"""

from types import SimpleNamespace

from schematiq.value_extraction.core.paper_processor import PaperProcessor
from schematiq.value_extraction.utils.excerpt_grounder import ExcerptGrounder
from schematiq.value_extraction.utils.reference_grounder import ReferenceGrounder

SOURCE_TEXT = (
    "Judge Maria Lopez wrote the majority opinion. "
    "The appeal was dismissed on procedural grounds."
)

JUDGES_CSV = (
    "judge,appointed_by,year\n"
    "Maria Lopez,Barack Obama,2010\n"
    "John Smith,George W. Bush,2004\n"
)

COURTS_TXT = "The Ninth Circuit covers nine western states and two territories."

# Same layout as backend build_reference_context.
REFERENCE_CONTEXT = (
    "--- Reference document: judges.csv ---\n" + JUDGES_CSV
    + "\n\n"
    + "--- Reference document: courts.txt ---\n" + COURTS_TXT
)


def _make_self(reference_context=REFERENCE_CONTEXT, ungrounded_policy="flag"):
    return SimpleNamespace(
        excerpt_grounder=ExcerptGrounder(),
        reference_grounder=ReferenceGrounder(reference_context),
        _active_figure_images=[],
        ungrounded_policy=ungrounded_policy,
    )


def _cell(answer, *texts):
    return {"answer": answer, "excerpts": [{"text": t, "source": "case.txt"} for t in texts]}


def test_reference_excerpt_keeps_answer_and_is_attributed():
    s = _make_self()
    cleaned = {"appointed_by": _cell("Barack Obama", "Maria Lopez,Barack Obama,2010")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    col = result["appointed_by"]
    assert col["answer"] == "Barack Obama"
    assert "_ungrounded" not in col
    [exc] = col["excerpts"]
    assert exc["source"] == "judges.csv"
    assert exc["grounding_status"] == "exact"
    # Offsets into the reference must not be applied to the source document.
    assert exc["char_start"] is None and exc["char_end"] is None


def test_reference_excerpt_survives_drop_policy():
    s = _make_self(ungrounded_policy="drop")
    cleaned = {"appointed_by": _cell("Barack Obama", "Maria Lopez,Barack Obama,2010")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    assert result["appointed_by"]["answer"] == "Barack Obama"


def test_attributed_to_the_right_reference_document():
    s = _make_self()
    cleaned = {"circuit_scope": _cell("nine states", "covers nine western states")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    assert result["circuit_scope"]["excerpts"][0]["source"] == "courts.txt"


def test_case_and_whitespace_differences_still_match():
    s = _make_self()
    cleaned = {"appointed_by": _cell("Barack Obama", "maria lopez,barack   obama,2010")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    exc = result["appointed_by"]["excerpts"][0]
    assert exc["source"] == "judges.csv"
    assert exc["grounding_status"] == "case_insensitive"


def test_wrapping_quotes_are_stripped():
    s = _make_self()
    cleaned = {"appointed_by": _cell("Barack Obama", '"Maria Lopez,Barack Obama,2010"')}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    assert result["appointed_by"]["excerpts"][0]["source"] == "judges.csv"


def test_excerpt_in_neither_document_is_still_flagged():
    s = _make_self()
    cleaned = {"appointed_by": _cell("Ronald Reagan", "Maria Lopez was appointed by Ronald Reagan")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    col = result["appointed_by"]
    assert col["_ungrounded"] is True
    assert col["excerpts"] == []


def test_paraphrase_of_reference_is_not_accepted():
    """No fuzzy phase for references: a loose paraphrase of a table row is not support."""
    s = _make_self()
    cleaned = {"appointed_by": _cell("Barack Obama", "Lopez appointed by Obama in 2010")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    assert result["appointed_by"]["_ungrounded"] is True


def test_single_cell_excerpt_is_accepted_from_reference():
    """In a lookup table the model often quotes just the cell it used."""
    s = _make_self()
    cleaned = {"appointed_by": _cell("George W. Bush", "Bush")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    col = result["appointed_by"]
    assert "_ungrounded" not in col
    assert col["excerpts"][0]["source"] == "judges.csv"


def test_blank_excerpt_is_not_located_in_reference():
    grounder = ReferenceGrounder(REFERENCE_CONTEXT)
    assert grounder.locate("   ") is None
    assert grounder.locate("") is None


def test_source_grounded_excerpt_is_untouched():
    s = _make_self()
    cleaned = {"author": _cell("Maria Lopez", "Judge Maria Lopez wrote the majority opinion")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    exc = result["author"]["excerpts"][0]
    assert exc["source"] == "case.txt"
    assert exc["grounding_status"] == "exact"
    assert exc["char_start"] == SOURCE_TEXT.index("Judge Maria Lopez")


def test_no_reference_keeps_previous_behavior():
    s = _make_self(reference_context=None)
    cleaned = {"appointed_by": _cell("Barack Obama", "Maria Lopez,Barack Obama,2010")}
    result = PaperProcessor._ground_and_enforce(s, cleaned, SOURCE_TEXT, "case")
    assert result["appointed_by"]["_ungrounded"] is True


def test_sections_are_parsed_without_separators():
    grounder = ReferenceGrounder(REFERENCE_CONTEXT)
    assert grounder.active
    names = [name for name, _ in grounder._sections]
    texts = [prepared.source_text for _, prepared in grounder._sections]
    assert names == ["judges.csv", "courts.txt"]
    assert texts == [JUDGES_CSV, COURTS_TXT]


def test_empty_reference_is_inactive():
    assert not ReferenceGrounder(None).active
    assert not ReferenceGrounder("").active
    assert not ReferenceGrounder("no headers here").active


def test_paper_processor_builds_grounder_for_large_reference():
    """The grounder must see the full reference even when it is large enough
    that the prompt gets only retrieved chunks."""
    from schematiq.value_extraction.core.paper_processor import (
        REFERENCE_FULL_INJECT_MAX_CHARS,
    )

    filler = "row,value\n" * (REFERENCE_FULL_INJECT_MAX_CHARS // 10 + 1)
    big = "--- Reference document: big.csv ---\n" + filler + "Maria Lopez,Barack Obama\n"
    pp = PaperProcessor(llm=None, reference_context=big)
    assert pp.reference_grounder.active
    assert pp.reference_grounder.locate("Maria Lopez,Barack Obama") == ("big.csv", "exact")
    assert not PaperProcessor(llm=None).reference_grounder.active
