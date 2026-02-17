"""Document preprocessing for LLM API efficiency optimization.

This module provides document type detection and preprocessing to reduce
token usage by removing non-relevant sections from academic papers.
"""

import re
from dataclasses import dataclass
from typing import List


@dataclass
class PreprocessorConfig:
    """Configuration for document preprocessing."""
    remove_references: bool = True
    remove_acknowledgments: bool = True
    min_text_length: int = 1000  # Don't process very short documents


class DocumentPreprocessor:
    """Detects academic papers and removes non-relevant sections.

    This preprocessor identifies academic papers through heuristic detection
    and removes sections that typically don't contain information useful for
    schema discovery or value extraction, such as:
    - References/Bibliography sections
    - Acknowledgments sections

    Non-paper documents are returned unchanged.
    """

    # Reference section patterns - match common ways references are titled
    REFERENCE_START_PATTERNS = [
        r"(?im)^\s*(?:\d+\.?\s+)?references?\s*$",
        r"(?im)^\s*bibliography\s*$",
        r"(?im)^\s*literature\s+cited\s*$",
        r"(?i)\\section\*?\{references?\}",
    ]

    # Acknowledgment section patterns
    ACKNOWLEDGMENT_PATTERNS = [
        r"(?im)^\s*acknowledgments?\s*$",
        r"(?im)^\s*acknowledgements?\s*$",
    ]

    def __init__(self, config: PreprocessorConfig = None):
        """Initialize the preprocessor.

        Args:
            config: Optional configuration. Uses defaults if not provided.
        """
        self.config = config or PreprocessorConfig()

    def preprocess(self, text: str) -> str:
        """Detect document type and remove non-relevant sections.

        Args:
            text: The document text to preprocess.

        Returns:
            Preprocessed text with non-relevant sections removed (if academic paper),
            or the original text unchanged (if not an academic paper or too short).
        """
        if len(text) < self.config.min_text_length:
            return text

        if self._is_academic_paper(text):
            return self._preprocess_paper(text)
        return text

    def _is_academic_paper(self, text: str) -> bool:
        """Heuristic detection of academic papers.

        Uses multiple indicators to determine if a document is likely
        an academic paper. Requires at least 4 indicators to match.

        Args:
            text: Document text to analyze.

        Returns:
            True if the document appears to be an academic paper.
        """
        indicators = 0
        text_lower = text.lower()

        # Check for abstract near the beginning (strong indicator)
        if re.search(r'\babstract\b', text_lower[:2000]):
            indicators += 2

        # Check for introduction near the beginning
        if re.search(r'\bintroduction\b', text_lower[:5000]):
            indicators += 1

        # Check for references/bibliography near the end (strong indicator)
        if re.search(r'\breferences?\b|\bbibliography\b', text_lower[-5000:]):
            indicators += 2

        # Check for conclusion anywhere
        if re.search(r'\bconclusion\b', text_lower):
            indicators += 1

        # Check for method/methodology section
        if re.search(r'\bmethod(s|ology)?\b', text_lower):
            indicators += 1

        # Check for experiments/results section
        if re.search(r'\bexperiment(s|al)?\b|\bresults?\b', text_lower):
            indicators += 1

        return indicators >= 4

    def _preprocess_paper(self, text: str) -> str:
        """Remove non-relevant sections from an academic paper.

        Args:
            text: Academic paper text.

        Returns:
            Paper text with references and acknowledgments removed.
        """
        if self.config.remove_references:
            text = self._remove_references(text)
        if self.config.remove_acknowledgments:
            text = self._remove_acknowledgments(text)
        return text.strip()

    def _remove_references(self, text: str) -> str:
        """Remove references section from end of paper.

        Only removes references if they appear in the last 30% of the document,
        to avoid accidentally removing inline references to other sections.

        Args:
            text: Paper text.

        Returns:
            Paper text with references section removed.
        """
        for pattern in self.REFERENCE_START_PATTERNS:
            match = re.search(pattern, text)
            if match:
                # Only remove if this is near the end (last 30% of document)
                if match.start() > len(text) * 0.7:
                    return text[:match.start()].strip()
        return text

    def _remove_acknowledgments(self, text: str) -> str:
        """Remove acknowledgments section.

        Removes the acknowledgments section and everything up to the next
        section heading. If no next section is found and acknowledgments
        are near the end, removes everything from acknowledgments onward.

        Args:
            text: Paper text.

        Returns:
            Paper text with acknowledgments section removed.
        """
        for pattern in self.ACKNOWLEDGMENT_PATTERNS:
            match = re.search(pattern, text)
            if match:
                # Find the next section heading or end
                next_section = re.search(
                    r'(?im)^\s*(?:\d+\.?\s+)?[A-Z][a-z]+',
                    text[match.end():]
                )
                if next_section:
                    end_pos = match.end() + next_section.start()
                    text = text[:match.start()] + text[end_pos:]
                elif match.start() > len(text) * 0.8:
                    # Acknowledgments near end with no following section
                    text = text[:match.start()]
        return text

    def get_stats(self, original: str, preprocessed: str) -> dict:
        """Get preprocessing statistics for debugging/monitoring.

        Args:
            original: Original document text.
            preprocessed: Preprocessed document text.

        Returns:
            Dict with original_length, preprocessed_length, reduction_percent,
            and is_paper flag.
        """
        original_len = len(original)
        preprocessed_len = len(preprocessed)
        reduction = (original_len - preprocessed_len) / original_len * 100 if original_len > 0 else 0

        return {
            "original_length": original_len,
            "preprocessed_length": preprocessed_len,
            "reduction_percent": round(reduction, 2),
            "is_paper": self._is_academic_paper(original),
        }
