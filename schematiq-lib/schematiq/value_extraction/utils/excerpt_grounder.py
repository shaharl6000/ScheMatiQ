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
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Find excerpt location in source text.

        Args:
            excerpt: The excerpt text to locate.
            source_text: The original source document text.
            source_lower: Optional pre-lowercased source_text to avoid
                recomputing it for every excerpt.

        Returns:
            (start_pos, end_pos, status) where status is
            "exact", "case_insensitive", "fuzzy", or "not_found".
        """
        if not excerpt or not source_text:
            return None, None, "not_found"

        if source_lower is None:
            source_lower = source_text.lower()

        # Phase 1: Exact substring search
        pos = source_text.find(excerpt)
        if pos >= 0:
            return pos, pos + len(excerpt), "exact"

        # Phase 2: Case-insensitive search
        pos = source_lower.find(excerpt.lower())
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
        norm_result = self._search_stripped(excerpt, source_text)
        if norm_result[2] != "not_found":
            return norm_result

        # Phase 3: Fuzzy sliding window with Jaccard pre-filter
        excerpt_words = excerpt.split()
        if len(excerpt_words) < 3:
            return None, None, "not_found"

        # Tokenize with each word's own (start, end) span in source_text, so
        # the matched window's real character range can be read off directly
        # instead of rejoining words with single spaces and re-`.find()`-ing
        # that string in source_text — which silently fails (reports
        # "not_found") whenever the source has irregular spacing, e.g.
        # Docling's PDF-justification double-spaces.
        word_spans = [(m.group(), m.start(), m.end()) for m in re.finditer(r"\S+", source_text)]
        source_words = [w for w, _, _ in word_spans]
        window_size = len(excerpt_words)

        if window_size > len(source_words):
            return None, None, "not_found"

        best_ratio = 0.0
        best_pos = None

        # Pre-compute lowercased token set for Jaccard filter
        excerpt_token_set = set(excerpt.lower().split())

        for i in range(len(source_words) - window_size + 1):
            # Jaccard pre-filter: skip windows with low token overlap
            window_token_set_lower = set(
                w.lower() for w in source_words[i : i + window_size]
            )
            intersection = len(excerpt_token_set & window_token_set_lower)
            union = len(excerpt_token_set | window_token_set_lower)
            if union > 0 and intersection / union < 0.3:
                continue

            window = " ".join(source_words[i : i + window_size])
            ratio = difflib.SequenceMatcher(
                None, excerpt.lower(), window.lower()
            ).ratio()
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
        self, excerpt: str, source_text: str
    ) -> Tuple[Optional[int], Optional[int], str]:
        """Case-insensitive search with whitespace/dash/line-wrap-hyphen
        normalization applied on both sides (see _normalize_for_matching),
        mapping the match back to offsets in the original source_text."""
        norm_excerpt, _ = self._normalize_for_matching(excerpt)
        norm_excerpt = norm_excerpt.lower()
        if not norm_excerpt:
            return None, None, "not_found"
        norm_source, index_map = self._normalize_for_matching(source_text)
        pos = norm_source.lower().find(norm_excerpt)
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

        # Pre-compute lowercased source once for all excerpts
        source_lower = source_text.lower()

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
                    text, source_text, source_lower=source_lower
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
