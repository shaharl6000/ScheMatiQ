"""Schema.merge must not merge numbered columns whose numbers differ."""

import pytest

import schematiq.core.schema as schema_mod
from schematiq.core.schema import Column, Schema, _numbers_conflict


_PARAPHRASES = {"outcome_of_case": "case_outcome", "petitioner_1_type": "petitioner_type_1"}


@pytest.fixture
def always_similar(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake embeddings with no model load.

    Names with the same word stem (digits ignored) score as paraphrases, like
    the real model does for "justice_1" / "justice_2" (cosine 0.92). Two known
    paraphrase pairs also score as similar.
    """

    def _fake_embed(text: str) -> str:
        text = _PARAPHRASES.get(text, text)
        return schema_mod._DIGIT_RUN.sub("#", text)

    monkeypatch.setattr(schema_mod, "_embed", _fake_embed)
    monkeypatch.setattr(schema_mod.util, "cos_sim", lambda a, b: 1.0 if a == b else 0.0)


def _schema(*names: str) -> Schema:
    return Schema(
        query="q",
        columns=[Column(name=n, rationale="r", definition="d") for n in names],
    )


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("justice_1", "justice_2", True),
        ("lawyer_r1", "lawyer_r2", True),
        ("date_2019", "date_2020", True),
        ("top_5_results", "top_10_results", True),
        ("item_01", "item_1", False),
        ("petitioner_type_1", "petitioner_1_type", False),
        ("justice_1", "justice", False),
        ("case_outcome", "outcome_of_case", False),
    ],
)
def test_numbers_conflict(a: str, b: str, expected: bool) -> None:
    assert _numbers_conflict(a, b) is expected
    assert _numbers_conflict(b, a) is expected


def test_numbered_columns_stay_separate(always_similar: None) -> None:
    merged = _schema("justice_1", "lawyer_r1").merge(
        _schema("justice_2", "justice_3", "lawyer_r2")
    )
    assert [c.name for c in merged.columns] == [
        "justice_1",
        "lawyer_r1",
        "justice_2",
        "justice_3",
        "lawyer_r2",
    ]


def test_semantic_merge_still_works(always_similar: None) -> None:
    # Same numbers, or no numbers: the semantic match still merges.
    merged = _schema("petitioner_type_1", "case_outcome").merge(
        _schema("petitioner_1_type", "outcome_of_case")
    )
    assert [c.name for c in merged.columns] == ["petitioner_type_1", "case_outcome"]
