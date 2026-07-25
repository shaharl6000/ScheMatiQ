"""Build the external-reference context string passed into value extraction.

A session may carry external reference documents (supplementary lookup material,
e.g. a spreadsheet mapping each judge to the president who appointed them). This
helper concatenates their text into a single labelled blob that the extraction
pipeline injects (or, when large, retrieves from) per document prompt as
supplementary context — clearly marked as external so it never yields rows.

The text now lives in the storage backend rather than inline on the session, so
this is async and loads each document's text on demand.
"""

from typing import Optional

# Upper bound on the combined reference text handed to extraction. Large enough
# to let the retrieval layer (which chunks and selects per unit) do its job.
MAX_REFERENCE_CONTEXT_CHARS = 10_000_000


async def build_reference_context(session) -> Optional[str]:
    """Return the combined reference text for a session, or None if there is none."""
    refs = getattr(session, "reference_documents", None) or []
    if not refs:
        return None

    from app.services.reference_document_service import load_reference_text

    session_id = getattr(session, "id", None)
    parts = []
    for ref in refs:
        filename = getattr(ref, "filename", "reference")
        try:
            text = await load_reference_text(session_id, ref)
        except Exception:
            text = ""
        if not text or not text.strip():
            continue
        parts.append(f"--- Reference document: {filename} ---\n{text}")

    if not parts:
        return None

    combined = "\n\n".join(parts)
    if len(combined) > MAX_REFERENCE_CONTEXT_CHARS:
        combined = combined[:MAX_REFERENCE_CONTEXT_CHARS]
    return combined
