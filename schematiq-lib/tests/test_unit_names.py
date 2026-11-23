"""Domain-neutral name checks for unit dedup (no embedding model needed)."""

import pytest

from schematiq.value_extraction.utils.unit_names import (
    is_mostly_latin,
    name_tokens,
    names_contained,
)


def test_tokens_keep_inner_apostrophes_and_hyphens():
    assert name_tokens("השופט ס' ג'ובראן") == {"השופט", "ס", "ג'ובראן"}
    assert name_tokens("TNF-alpha, (human)") == {"tnf-alpha", "human"}


@pytest.mark.parametrize("a, b", [
    ("D1 domain", "The D1 domain"),
    ("השופט א' רובינשטיין", "השופט רובינשטיין"),
    ("חלבון p53", "p53"),
    ("domain D1", "The D1 domain"),  # word order does not matter
])
def test_contained(a, b):
    assert names_contained(a, b)


@pytest.mark.parametrize("a, b", [
    ("השופט א' רובינשטיין", "השופט א' גרוניס"),
    ("חלבון p53", "חלבון BRCA1"),
    ("Smith", "Smithson"),
    ("A", "A B"),  # too short to identify anything
    ("", "anything"),
])
def test_not_contained(a, b):
    assert not names_contained(a, b)


def test_is_mostly_latin():
    assert is_mostly_latin("Justice Kagan")
    assert is_mostly_latin("p53")
    assert not is_mostly_latin("חלבון p53")
    assert not is_mostly_latin("השופט א' רובינשטיין")
    assert is_mostly_latin("1234")
