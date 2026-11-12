"""Schema discovery keeps allowed_values only for columns that hold categories.

Discovery sees one or a few documents, so a model often fills allowed_values
with values it saw there (a judge's name, case names, a citation) or with a
description of the value. Extraction then treats that column as a category and
pulls every document toward those values. Discovery now also reports what each
column holds (value_kind), and only a category column keeps a list.
"""

import json

import pytest

from schematiq.core.schema import Column, Schema
from schematiq.core.schematiq import (
    SCHEMA_DISCOVERY_RESPONSE_SCHEMA,
    VALUE_KINDS,
    _parse_schema_from_llm,
    normalize_value_kind,
    sanitize_discovered_allowed_values,
)


def _parse(columns):
    raw = json.dumps({"document_helpful": True, "columns": columns})
    schema, _, _ = _parse_schema_from_llm(raw, query="q", max_keys_schema=50)
    return {c.name: c for c in schema.columns}


def _col(name, kind, values):
    col = {"name": name, "definition": "d", "rationale": "r", "allowed_values": values}
    if kind is not None:
        col["value_kind"] = kind
    return col


# ---------------------------------------------------------------- by value_kind

def test_identifier_and_free_text_columns_get_no_list_whatever_their_name():
    cols = _parse([
        _col("case_name", "identifier", ["National TPS Alliance v. Noem", "Ramos v. Wolf", "Texas v. United States"]),
        _col("protein", "identifier", ["BRCA1", "TP53"]),
        _col("שם_השופט", "identifier", ["אהרן ברק", "דורית ביניש"]),
        _col("challenged_policy_description", "free_text", ["Alien Enemies Act deportations", "Border wall funding"]),
    ])
    assert all(c.allowed_values is None for c in cols.values())
    assert cols["case_name"].value_kind == "identifier"
    assert cols["challenged_policy_description"].value_kind == "free_text"


def test_category_columns_keep_their_lists():
    cols = _parse([
        _col("court_level", "category", ["district", "appeals", "supreme"]),
        _col("presidential_appointer", "category", ["Donald Trump", "Barack Obama", "George W. Bush"]),
        _col("party_name", "category", ["Democratic", "Republican"]),
        _col("injunction_geographic_scope", "category", ["nationwide", "circuit-wide", "plaintiff-only / limited", "none"]),
    ])
    assert cols["court_level"].allowed_values == ["district", "appeals", "supreme"]
    assert cols["presidential_appointer"].allowed_values == ["Donald Trump", "Barack Obama", "George W. Bush"]
    assert cols["party_name"].allowed_values == ["Democratic", "Republican"]
    assert cols["injunction_geographic_scope"].allowed_values[-1] == "none"


def test_number_and_date_columns_keep_only_constraints():
    cols = _parse([
        _col("damages", "number", ["number"]),
        _col("score", "number", ["0-100"]),
        _col("plaintiff_count", "number", ["12", "40"]),
        _col("decided_on", "date", ["date:us"]),
        _col("filed_on", "date", ["April 13, 2021"]),
    ])
    assert cols["damages"].allowed_values == ["number"]
    assert cols["score"].allowed_values == ["0-100"]
    assert cols["plaintiff_count"].allowed_values is None
    assert cols["decided_on"].allowed_values == ["date:us"]
    assert cols["filed_on"].allowed_values is None


def test_a_single_value_is_never_a_category():
    cols = _parse([_col("court_level", "category", ["district"])])
    assert cols["court_level"].allowed_values is None


# -------------------------------------------- model did not report value_kind

@pytest.mark.parametrize(
    "values",
    [["Edward M. Chen"], ["773 F.Supp.3d 807"], ["null"], ["YYYY-MM-DD"], ["judge's full name"]],
)
def test_without_value_kind_a_single_non_constraint_value_is_dropped(values):
    assert _parse([_col("x", None, values)])["x"].allowed_values is None


def test_without_value_kind_lists_and_constraints_are_kept():
    cols = _parse([
        _col("court_level", None, ["district", "appellate", "supreme_court"]),
        _col("damages", None, ["number"]),
        _col("decided_on", None, ["date"]),
    ])
    assert cols["court_level"].allowed_values == ["district", "appellate", "supreme_court"]
    assert cols["damages"].allowed_values == ["number"]
    assert cols["decided_on"].allowed_values == ["date"]


# ----------------------------------------------------------------- helpers

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("category", "category"), ("Categorical", "category"), ("identifier", "identifier"),
        ("ID", "identifier"), ("free-form", "free_text"), ("free text", "free_text"),
        ("numeric", "number"), ("date", "date"), ("banana", None), (None, None), (3, None),
    ],
)
def test_normalize_value_kind(raw, expected):
    assert normalize_value_kind(raw) == expected


def test_sanitize_trims_and_drops_empty():
    assert sanitize_discovered_allowed_values([" yes ", "no", None, "  "], "c", "category") == ["yes", "no"]
    assert sanitize_discovered_allowed_values([None, "  "], "c", "category") is None
    assert sanitize_discovered_allowed_values(None, "c", "category") is None


def test_response_schema_asks_for_value_kind_before_allowed_values():
    item = SCHEMA_DISCOVERY_RESPONSE_SCHEMA["properties"]["columns"]["items"]
    assert item["properties"]["value_kind"]["enum"] == list(VALUE_KINDS)
    order = item["propertyOrdering"]
    assert order.index("value_kind") < order.index("allowed_values")


# ------------------------------------------------------------------- merge

def _merge(existing, candidate):
    base = Schema(query="q", columns=[existing], max_keys=10)
    proposal = Schema(query="q", columns=[candidate], max_keys=10)
    return next(c for c in base.merge(proposal).columns if c.name == existing.name)


def test_merge_never_adds_a_list_to_an_identifier_column():
    existing = Column(name="case_name", rationale="r", definition="d", value_kind="identifier")
    candidate = Column(name="case_name", rationale="r", definition="d", allowed_values=["Ramos v. Wolf", "Texas v. United States"])
    assert _merge(existing, candidate).allowed_values is None


def test_merge_adopts_value_kind_from_a_later_batch():
    existing = Column(name="case_name", rationale="r", definition="d")
    candidate = Column(name="case_name", rationale="r", definition="d", value_kind="identifier")
    assert _merge(existing, candidate).value_kind == "identifier"


def test_merge_still_grows_a_discovered_category_list():
    existing = Column(name="court", rationale="r", definition="d", allowed_values=["district"], value_kind="category")
    candidate = Column(name="court", rationale="r", definition="d", allowed_values=["appeals"], value_kind="category")
    assert _merge(existing, candidate).allowed_values == ["district", "appeals"]


def test_value_kind_round_trips_through_to_dict():
    col = Column(name="case_name", rationale="r", definition="d", value_kind="identifier")
    assert Column.from_dict(col.to_dict()).value_kind == "identifier"


def test_discovery_prompts_define_kind_by_whether_the_full_list_is_knowable():
    # Discovery sees a sample of the corpus. A category is a set whose full
    # list is known from the domain; an open pool of names (most people, cases,
    # proteins) is not, because the rest of the corpus keeps adding values,
    # even if some values repeat in the sample. Examples stay generic so the prompt is
    # not tuned to one corpus (no legal terms from the queries we test with).
    from schematiq.core import prompts

    for name in ("SYSTEM_PROMPT_STANDARD", "SYSTEM_PROMPT_DOCUMENT_ONLY", "SYSTEM_PROMPT_QUERY_ONLY"):
        text = getattr(prompts, name)
        section = text.split("### value_kind", 1)[1].split("### allowed_values", 1)[0]
        assert "Would the rest of the corpus bring values that are not in your list?" in section
        for corpus_term in ("judge", "court", "president", "appointing"):
            assert corpus_term not in section.lower()


# ------------------------------------------- the column that names the row

def test_list_of_the_units_own_names_is_cleared():
    # A real run: the model called judge_name a category because the same
    # justices recur, and listed the unit's own example names.
    from schematiq.core.schema import ObservationUnit
    from schematiq.core.schematiq import _drop_unit_name_lists

    unit = ObservationUnit(
        name="Judge",
        definition="An individual federal judge.",
        example_names=["Amy Coney Barrett", "Deborah L. Boardman", "John C. Coughenour"],
    )
    schema = Schema(
        query="q",
        columns=[
            Column(name="judge_name", rationale="r", definition="d", value_kind="category",
                   allowed_values=["Amy Coney Barrett", "John G. Roberts", "deborah l. boardman"]),
            Column(name="appointing_president", rationale="r", definition="d", value_kind="category",
                   allowed_values=["Donald Trump", "Barack Obama"]),
            Column(name="seeded", rationale="r", definition="d", locked=True,
                   allowed_values=["Amy Coney Barrett", "Other"]),
        ],
        max_keys=10,
    )

    _drop_unit_name_lists(schema, unit)

    by_name = {c.name: c for c in schema.columns}
    assert by_name["judge_name"].allowed_values is None
    assert by_name["judge_name"].value_kind == "identifier"
    assert by_name["appointing_president"].allowed_values == ["Donald Trump", "Barack Obama"]
    # A list the user set is never changed.
    assert by_name["seeded"].allowed_values == ["Amy Coney Barrett", "Other"]


def test_no_unit_or_no_examples_changes_nothing():
    from schematiq.core.schema import ObservationUnit
    from schematiq.core.schematiq import _drop_unit_name_lists

    col = Column(name="x", rationale="r", definition="d", allowed_values=["a", "b"])
    schema = Schema(query="q", columns=[col], max_keys=10)
    _drop_unit_name_lists(schema, None)
    _drop_unit_name_lists(schema, ObservationUnit(name="Paper", definition="d", example_names=[]))
    assert col.allowed_values == ["a", "b"]


