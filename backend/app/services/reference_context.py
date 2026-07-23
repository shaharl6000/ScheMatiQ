"""Build the external-reference context string passed into value extraction.

A session may carry external reference documents (supplementary lookup material,
e.g. a spreadsheet mapping each judge to the president who appointed them). This
helper concatenates their extracted text into a single labelled blob that the
extraction pipeline injects into each per-document prompt as supplementary
context — clearly marked as external so it never yields observation-unit rows.

Reads ``session.reference_documents`` defensively via ``getattr`` so this module
is safe even where that field is not present on the session model.
"""

from typing import Optional

# Guard so a single oversized reference can't blow up the extraction prompt.
MAX_REFERENCE_CONTEXT_CHARS = 200_000


def build_reference_context(session) -> Optional[str]:
    """Return the combined reference text for a session, or None if there is none."""
    refs = getattr(session, "reference_documents", None) or []
    if not refs:
        return None

    parts = []
    for ref in refs:
        filename = getattr(ref, "filename", "reference")
        content = getattr(ref, "content", "") or ""
        if not content.strip():
            continue
        parts.append(f"--- Reference document: {filename} ---\n{content}")

    if not parts:
        return None

    combined = "\n\n".join(parts)
    if len(combined) > MAX_REFERENCE_CONTEXT_CHARS:
        combined = combined[:MAX_REFERENCE_CONTEXT_CHARS]
    return combined
