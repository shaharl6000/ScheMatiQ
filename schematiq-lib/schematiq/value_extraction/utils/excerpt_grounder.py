"""Verify extraction excerpts against source text for hallucination detection."""

import difflib
from typing import Dict, List, Optional, Tuple


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

        # Phase 1: Exact substring search
        pos = source_text.find(excerpt)
        if pos >= 0:
            return pos, pos + len(excerpt), "exact"

        # Phase 2: Case-insensitive search
        if source_lower is None:
            source_lower = source_text.lower()
        pos = source_lower.find(excerpt.lower())
        if pos >= 0:
            return pos, pos + len(excerpt), "case_insensitive"

        # Phase 3: Fuzzy sliding window with Jaccard pre-filter
        excerpt_words = excerpt.split()
        if len(excerpt_words) < 3:
            return None, None, "not_found"

        source_words = source_text.split()
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
            matched_text = " ".join(
                source_words[best_pos : best_pos + window_size]
            )
            char_start = source_text.find(matched_text)
            if char_start >= 0:
                return char_start, char_start + len(matched_text), "fuzzy"

        return None, None, "not_found"

    def ground_all_excerpts(
        self, extraction_result: dict, source_text: str
    ) -> dict:
        """Add grounding info to all excerpts in an extraction result.

        Modifies extraction_result in-place, converting string excerpts
        to dicts with grounding metadata.

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
