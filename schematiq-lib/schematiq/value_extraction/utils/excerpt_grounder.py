"""Verify extraction excerpts against source text for hallucination detection."""

import difflib
import re
from typing import Dict, List, Optional, Tuple

# Unicode dash/hyphen variants folded to ASCII '-' for matching purposes.
# PDF text extraction and the model's own quoting don't always agree on
# which one was used for the same character (e.g. an en dash in extracted
# text vs a plain hyphen in a quoted excerpt).
_DASH_FOLD: Dict[str, str] = {
    "‐": "-",  # hyphen
    "‑": "-",  # non-breaking hyphen
    "‒": "-",  # figure dash
    "–": "-",  # en dash
    "—": "-",  # em dash
    "−": "-",  # minus sign
}


# An ellipsis the model inserted to mark elided text: three or more dots, or
# the single Unicode ellipsis character.
_ELLIPSIS_RE = re.compile(r"\s*(?:\.{3,}|\u2026)\s*")
_LEADING_ELLIPSIS_RE = re.compile(r"^(?:\.{3,}|\u2026)\s*")
_TRAILING_ELLIPSIS_RE = re.compile(r"\s*(?:\.{3,}|\u2026)$")
_QUOTE_CHARS = "\"'\u201c\u201d\u2018\u2019"
# The longest kept segment of an elided excerpt must be at least this long,
# so "the ... of" cannot ground on common words alone.
_MIN_ELIDED_SEGMENT_WORDS = 3

# A number, with internal separators kept ("1,000", "3.5", "12/31", "1:30").
# Same pattern as json_parser._NUMBER_RE, so excerpts and allowed values treat
# numbers the same way.
_NUMBER_RE = re.compile(r"\d+(?:[.,:/]\d+)*")


def _numbers(text: str) -> set:
    """Set of numbers in ``text``, as written."""
    return set(_NUMBER_RE.findall(text))


def _strip_decorations(excerpt: str) -> str:
    """Remove decoration the model adds around a quote but which is not in
    the source: leading/trailing ellipses and one pair of wrapping quotes.
    Repeats until stable, so '"...text..."' is fully unwrapped."""
    text = excerpt
    while True:
        prev = text
        text = text.strip()
        text = _LEADING_ELLIPSIS_RE.sub("", text)
        text = _TRAILING_ELLIPSIS_RE.sub("", text)
        if len(text) >= 2 and text[0] in _QUOTE_CHARS and text[-1] in _QUOTE_CHARS:
            text = text[1:-1]
        if text == prev:
            return text


class _PreparedSource:
    """Per-document lazy caches shared across all excerpts in a document."""

    __slots__ = (
        "source_text",
        "source_lower",
        "_norm_source_lower",
        "_norm_index_map",
        "_word_spans",
        "_source_words",
        "_source_words_lower",
    )

    def __init__(self, source_text: str, source_lower: Optional[str] = None):
        self.source_text = source_text
        self.source_lower = (
            source_lower if source_lower is not None else source_text.lower()
        )
        self._norm_source_lower: Optional[str] = None
        self._norm_index_map: Optional[List[int]] = None
        self._word_spans: Optional[List[Tuple[str, int, int]]] = None
        self._source_words: Optional[List[str]] = None
        self._source_words_lower: Optional[List[str]] = None

    def get_normalized(self) -> Tuple[str, List[int]]:
        if self._norm_source_lower is None or self._norm_index_map is None:
            norm_source, index_map = ExcerptGrounder._normalize_for_matching(
                self.source_text
            )
            self._norm_source_lower = norm_source.lower()
            self._norm_index_map = index_map
        return self._norm_source_lower, self._norm_index_map

    def get_word_tokens(
        self,
    ) -> Tuple[List[Tuple[str, int, int]], List[str], List[str]]:
        if (
            self._word_spans is None
            or self._source_words is None
            or self._source_words_lower is None
        ):
            spans = [
                (m.group(), m.start(), m.end())
                for m in re.finditer(r"\S+", self.source_text)
            ]
            words = [w for w, _, _ in spans]
            self._word_spans = spans
            self._source_words = words
            self._source_words_lower = [w.lower() for w in words]
        return self._word_spans, self._source_words, self._source_words_lower


class ExcerptGrounder:
    """Verify extraction excerpts against source text.

    Uses exact matching first, then fuzzy sliding-window matching
    for paraphrased excerpts.
    """

    def __init__(self, fuzzy_threshold: float = 0.6):
        self.fuzzy_threshold = fuzzy_threshold

    def ground_excerpt(
        self,
        excerpt: str,
        source_text: str,
        source_lower: Optional[str] = None,
        _prepared: Optional[_PreparedSource] = None,
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Find excerpt location in source text.

        Args:
            excerpt: The excerpt text to locate.
            source_text: The original source document text.
            source_lower: Optional pre-lowercased source_text to avoid
                recomputing it for every excerpt.
            _prepared: Optional internal per-document cache of normalized text
                and tokenized word spans.

        Returns:
            (start_pos, end_pos, status) where status is
            "exact", "case_insensitive", "fuzzy", or "not_found".

        The prompts ask for verbatim spans, but the model still decorates
        quotes: wrapping quotation marks, a leading/trailing "...", or an
        internal "..." joining two pieces of one sentence. Those never
        appear in the source, so a short decorated excerpt (too short for
        the fuzzy phase) was reported "not_found" and its answer nulled.
        When the raw excerpt fails, retry with the decoration stripped,
        then with an internal ellipsis split into separately grounded
        segments.
        """
        if not excerpt or not source_text:
            return None, None, "not_found"

        prepared = (
            _prepared
            if _prepared is not None
            else _PreparedSource(source_text, source_lower=source_lower)
        )

        result = self._ground_direct(
            excerpt, prepared.source_text, prepared.source_lower, _prepared=prepared
        )
        if result[2] != "not_found":
            return result

        cleaned = _strip_decorations(excerpt)
        if cleaned and cleaned != excerpt:
            result = self._ground_direct(
                cleaned, prepared.source_text, prepared.source_lower, _prepared=prepared
            )
            if result[2] != "not_found":
                return result

        return self._ground_elided(
            cleaned or excerpt,
            prepared.source_text,
            prepared.source_lower,
            _prepared=prepared,
        )

    def _ground_elided(
        self,
        excerpt: str,
        source_text: str,
        source_lower: Optional[str] = None,
        _prepared: Optional[_PreparedSource] = None,
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Ground an excerpt with an internal ellipsis ("A ... B").

        Every segment must ground on its own, and the longest segment must
        have at least _MIN_ELIDED_SEGMENT_WORDS words. The returned span is
        the longest segment's span, and the status is "fuzzy" because the
        excerpt as a whole is not a verbatim span.
        """
        prepared = (
            _prepared
            if _prepared is not None
            else _PreparedSource(source_text, source_lower=source_lower)
        )
        segments = [
            _strip_decorations(seg) for seg in _ELLIPSIS_RE.split(excerpt)
        ]
        segments = [seg for seg in segments if seg]
        if len(segments) < 2:
            return None, None, "not_found"
        longest = max(segments, key=lambda seg: len(seg.split()))
        if len(longest.split()) < _MIN_ELIDED_SEGMENT_WORDS:
            return None, None, "not_found"
        longest_span: Tuple[Optional[int], Optional[int]] = (None, None)
        for seg in segments:
            start, end, status = self._ground_direct(
                seg, prepared.source_text, prepared.source_lower, _prepared=prepared
            )
            if status == "not_found":
                return None, None, "not_found"
            if seg is longest:
                longest_span = (start, end)
        return longest_span[0], longest_span[1], "fuzzy"

    def _ground_direct(
        self,
        excerpt: str,
        source_text: str,
        source_lower: Optional[str] = None,
        _prepared: Optional[_PreparedSource] = None,
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Exact, case-insensitive, normalized, then fuzzy matching of one
        excerpt as given (no decoration handling)."""
        if not excerpt:
            return None, None, "not_found"

        prepared = (
            _prepared
            if _prepared is not None
            else _PreparedSource(source_text, source_lower=source_lower)
        )
        source_text = prepared.source_text
        source_lower = prepared.source_lower

        # Phase 1: Exact substring search
        pos = source_text.find(excerpt)
        if pos >= 0:
            return pos, pos + len(excerpt), "exact"

        # Phase 2: Case-insensitive search
        excerpt_lower = excerpt.lower()
        pos = source_lower.find(excerpt_lower)
        if pos >= 0:
            return pos, pos + len(excerpt), "case_insensitive"

        # Phase 2b: Normalized search -- whitespace stripped entirely (not
        # just collapsed), Unicode dash variants folded to '-', and
        # line-wrap hyphens dropped (see _normalize_for_matching). Real
        # extracted PDF text sometimes drops inter-word spacing entirely
        # across whole stretches (a font/encoding artifact, distinct from
        # the "double-space" PDF-justification noise elsewhere in this
        # file) -- e.g. source text reading
        # "...resultswithmice\nhomozygousforwild-type..." where the
        # model's own quoted excerpt has a plain space in place of that
        # newline. Collapsing whitespace RUNS to one space (as Phase 2
        # above effectively does via case-insensitive-on-original) can't
        # fix this since there's often no whitespace there to collapse;
        # stripping it entirely from both sides makes the comparison
        # robust to whitespace being added OR dropped. This also rescues
        # short excerpts (under 3 words) that are too short to reach the
        # fuzzy phase below and would otherwise have no fallback at all.
        norm_result = self._search_stripped(
            excerpt, source_text, _prepared=prepared
        )
        if norm_result[2] != "not_found":
            return norm_result

        # Phase 3: Fuzzy sliding window with Jaccard pre-filter
        return self._find_fuzzy(excerpt, excerpt_lower, prepared)

    def _find_fuzzy(
        self, excerpt: str, excerpt_lower: str, prepared: _PreparedSource
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Fuzzy sliding-window search using pre-tokenized document words."""
        excerpt_words = excerpt.split()
        if len(excerpt_words) < 3:
            return None, None, "not_found"

        # Tokenize with each word's own (start, end) span in source_text, so
        # the matched window's real character range can be read off directly
        # instead of rejoining words with single spaces and re-`.find()`-ing
        # that string in source_text — which silently fails (reports
        # "not_found") whenever the source has irregular spacing, e.g.
        # Docling's PDF-justification double-spaces.
        word_spans, source_words, source_words_lower = prepared.get_word_tokens()
        window_size = len(excerpt_words)

        if window_size > len(source_words):
            return None, None, "not_found"

        best_ratio = 0.0
        best_pos: Optional[int] = None

        # Pre-compute lowercased token set for Jaccard filter
        excerpt_token_set = frozenset(excerpt_lower.split())
        excerpt_token_count = len(excerpt_token_set)
        # Fuzzy tolerance is for wording, not numbers. A window may only match
        # if it contains every number in the excerpt exactly. Otherwise a
        # citation like "2025 WL 1825431" grounds on "2025 WL 1325305", and a
        # wrong page number or date passes as "fuzzy" (issue #557).
        excerpt_numbers = _numbers(excerpt)
        sm = difflib.SequenceMatcher(None, excerpt_lower, "")

        for i in range(len(source_words) - window_size + 1):
            # Jaccard pre-filter: skip windows with zero or low token overlap
            window_slice_lower = source_words_lower[i : i + window_size]
            if excerpt_token_set.isdisjoint(window_slice_lower):
                continue
            window_words_lower = set(window_slice_lower)
            inter = len(excerpt_token_set.intersection(window_words_lower))
            union = excerpt_token_count + len(window_words_lower) - inter
            if union > 0 and inter / union < 0.3:
                continue

            window = " ".join(source_words[i : i + window_size])
            if excerpt_numbers and not excerpt_numbers <= _numbers(window):
                continue

            sm.set_seq2(window.lower())
            gate = (
                best_ratio
                if best_ratio >= self.fuzzy_threshold
                else max(0.0, self.fuzzy_threshold - 1e-9)
            )
            if sm.real_quick_ratio() <= gate or sm.quick_ratio() <= gate:
                continue

            ratio = sm.ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_pos = i

        if best_ratio >= self.fuzzy_threshold and best_pos is not None:
            char_start = word_spans[best_pos][1]
            char_end = word_spans[best_pos + window_size - 1][2]
            return char_start, char_end, "fuzzy"

        return None, None, "not_found"

    @staticmethod
    def _normalize_for_matching(text: str) -> Tuple[str, List[int]]:
        """Normalize text for whitespace/formatting-tolerant comparison:

        - Remove ALL whitespace (not just collapse runs -- real extracted
          PDF text can drop inter-word spacing entirely across a stretch).
        - Fold Unicode dash variants (en dash, em dash, minus sign, ...) to
          ASCII '-', since PDF text extraction and the model's own quoting
          don't always agree on which one was used (e.g. source "a2–3"
          [en dash] vs excerpt "a2-3" [hyphen]).
        - Drop a hyphen that sits immediately before a whitespace run
          containing a newline -- a print line-wrap artifact (e.g. source
          "recombina-\ntion" should match excerpt "recombination"). Scoped
          narrowly to hyphen-then-newline-containing-whitespace so real
          compound words ("T-cell", hyphen with no adjacent whitespace)
          are never touched.

        Returns the normalized string plus a map from each kept
        character's index in the normalized string back to its index in
        the original string.
        """
        out_chars: List[str] = []
        index_map: List[int] = []
        n = len(text)
        i = 0
        while i < n:
            ch = text[i]
            folded = _DASH_FOLD.get(ch, ch)
            if folded == "-":
                j = i + 1
                saw_newline = False
                while j < n and text[j].isspace():
                    if text[j] == "\n":
                        saw_newline = True
                    j += 1
                if saw_newline and j > i + 1:
                    # Line-wrap hyphen: drop it and the whitespace run after it.
                    i = j
                    continue
            if ch.isspace():
                i += 1
                continue
            out_chars.append(folded)
            index_map.append(i)
            i += 1
        return "".join(out_chars), index_map

    def _search_stripped(
        self,
        excerpt: str,
        source_text: str,
        _prepared: Optional[_PreparedSource] = None,
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Case-insensitive search with whitespace/dash/line-wrap-hyphen
        normalization applied on both sides (see _normalize_for_matching),
        mapping the match back to offsets in the original source_text."""
        norm_excerpt, _ = self._normalize_for_matching(excerpt)
        norm_excerpt = norm_excerpt.lower()
        if not norm_excerpt:
            return None, None, "not_found"
        if _prepared is not None:
            norm_source_lower, index_map = _prepared.get_normalized()
        else:
            norm_source, index_map = self._normalize_for_matching(source_text)
            norm_source_lower = norm_source.lower()
        pos = norm_source_lower.find(norm_excerpt)
        if pos < 0:
            return None, None, "not_found"
        start = index_map[pos]
        end = index_map[pos + len(norm_excerpt) - 1] + 1
        return start, end, "case_insensitive"

    def ground_all_excerpts(self, extraction_result: dict, source_text: str) -> dict:
        """Add grounding info to all excerpts in an extraction result.

        Modifies extraction_result in-place, converting string excerpts
        to dicts with grounding metadata.

        Figure-typed excerpts ({"type": "figure", "figure_id": ..., ...},
        produced by PaperProcessor._attach_source_to_excerpts for answers
        derived from reading a figure image) have no "text" to search
        for and are left untouched -- they're rendered via a separate
        figure/image UI path on the frontend, not text highlighting.

        Returns:
            Dict with grounding statistics
            {"exact": N, "case_insensitive": N, "fuzzy": N, "not_found": N}.
        """
        stats = {"exact": 0, "case_insensitive": 0, "fuzzy": 0, "not_found": 0}

        # Pre-compute per-document caches (lowercased source, lazy normalized
        # source + offset map, lazy word spans) once for all excerpts.
        prepared = _PreparedSource(source_text)

        for col_name, col_data in extraction_result.items():
            if col_name.startswith("_"):
                continue
            if not isinstance(col_data, dict):
                continue
            excerpts = col_data.get("excerpts", [])
            grounded_excerpts = []
            for exc in excerpts:
                if isinstance(exc, dict) and exc.get("type") == "figure":
                    grounded_excerpts.append(exc)
                    continue
                text = exc["text"] if isinstance(exc, dict) else exc
                start, end, status = self.ground_excerpt(
                    text,
                    source_text,
                    source_lower=prepared.source_lower,
                    _prepared=prepared,
                )
                stats[status] += 1
                if isinstance(exc, dict):
                    exc["char_start"] = start
                    exc["char_end"] = end
                    exc["grounding_status"] = status
                    grounded_excerpts.append(exc)
                else:
                    grounded_excerpts.append(
                        {
                            "text": text,
                            "char_start": start,
                            "char_end": end,
                            "grounding_status": status,
                        }
                    )
            col_data["excerpts"] = grounded_excerpts

        return stats
