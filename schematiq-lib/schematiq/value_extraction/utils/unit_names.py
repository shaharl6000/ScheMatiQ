"""Domain-neutral name checks for unit deduplication.

No word lists: these rules know nothing about titles, roles or any field.

* names_contained: all tokens of one name appear in the other ("D1 domain" /
  "The D1 domain", "X Smith" / "Smith"). A token-level version of the
  substring check, so word order and an extra word in the middle do not
  matter, and "Smith" is not found inside "Smithson".
* is_mostly_latin: the default embedding model (all-MiniLM-L6-v2) is
  English-only, so its similarity scores are not reliable for other scripts.
  Two different Hebrew names scored 0.86 with it, and up to 0.97 with a
  multilingual model.
"""

from __future__ import annotations

import re
import unicodedata
from typing import FrozenSet

# Split on whitespace and punctuation. An apostrophe or hyphen inside a token
# stays part of it ("O'Brien", "ג'ובראן", "TNF-alpha").
_SPLIT = re.compile(r"[\s,;:()\[\]{}/|]+")
_EDGE_PUNCT = ".,;:!?\"'`׳״‘’“”-"


def name_tokens(name: str) -> FrozenSet[str]:
    """Casefolded tokens of ``name``, with punctuation trimmed from their edges."""
    text = unicodedata.normalize("NFKC", name or "").casefold()
    tokens = (raw.strip(_EDGE_PUNCT) for raw in _SPLIT.split(text))
    return frozenset(t for t in tokens if t)


def names_contained(a: str, b: str) -> bool:
    """True when every token of the shorter name is in the longer one."""
    ta, tb = name_tokens(a), name_tokens(b)
    if not ta or not tb:
        return False
    smaller = ta if len(ta) <= len(tb) else tb
    # Same guard as the substring check: a one- or two-letter name is too
    # short to identify anything.
    if len("".join(smaller)) <= 2:
        return False
    return ta <= tb or tb <= ta


def is_mostly_latin(text: str) -> bool:
    """True when at least half of the letters in ``text`` are Latin script."""
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return True
    latin = sum(1 for c in letters if "LATIN" in unicodedata.name(c, ""))
    return latin * 2 >= len(letters)
