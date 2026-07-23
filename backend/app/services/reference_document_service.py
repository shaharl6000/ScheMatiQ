"""Service for managing external reference documents attached to a session.

A *reference document* is supplementary lookup material the user uploads to help
answer questions or fill columns — for example, an Excel sheet mapping each judge
to the president who appointed them. It is deliberately distinct from the *source
documents* that define observation units (one row per document/unit): a reference
document never yields rows on its own; it is extra context the assistant may
consult.

The extracted plain text is stored inline on the session
(``VisualizationSession.reference_documents``) so it persists through the normal
session storage backend (local filesystem or Supabase) without introducing a new
file-path convention that could silently no-op against Supabase.
"""

from __future__ import annotations

import io
import tempfile
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

from app.models.session import ReferenceDocument, VisualizationSession

# Cap stored text so a single reference doc can't bloat the session blob
# unboundedly (the session is serialized on every update).
MAX_REFERENCE_CHARS = 200_000

# File types we can turn into useful text. Tabular and text-native formats are
# handled in-process; pdf/docx reuse the existing document_conversion helpers.
TEXT_NATIVE_EXTENSIONS = {".txt", ".md", ".markdown", ".json"}
CSV_EXTENSIONS = {".csv"}
TSV_EXTENSIONS = {".tsv"}
EXCEL_EXTENSIONS = {".xlsx", ".xls"}
PDF_EXTENSIONS = {".pdf"}
DOCX_EXTENSIONS = {".docx"}

SUPPORTED_EXTENSIONS = (
    TEXT_NATIVE_EXTENSIONS
    | CSV_EXTENSIONS
    | TSV_EXTENSIONS
    | EXCEL_EXTENSIONS
    | PDF_EXTENSIONS
    | DOCX_EXTENSIONS
)


class UnsupportedReferenceFormat(ValueError):
    """Raised when a reference document's file type can't be converted to text."""


class ReferenceExtractionError(RuntimeError):
    """Raised when a supported file type fails to convert (corrupt/empty/etc.)."""


def _decode_text(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _excel_to_text(raw: bytes) -> str:
    """Render every sheet of a workbook as CSV under a labelled header."""
    import pandas as pd  # local import: pandas is heavy and only needed here

    buf = io.BytesIO(raw)
    # dtype=str keeps values verbatim (no float coercion of e.g. years/ids).
    sheets = pd.read_excel(buf, sheet_name=None, dtype=str)
    parts: List[str] = []
    for name, df in sheets.items():
        df = df.fillna("")
        parts.append(f"## Sheet: {name}\n{df.to_csv(index=False)}".rstrip())
    text = "\n\n".join(parts).strip()
    if not text:
        raise ReferenceExtractionError("Workbook contained no readable cells.")
    return text


def _converted_text_via_tempfile(filename: str, raw: bytes, converter) -> str:
    """Run a (input_path, output_dir) -> (ok, msg) converter over in-memory bytes."""
    safe_name = Path(filename).name or "reference"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        in_path = tmp_dir / safe_name
        in_path.write_bytes(raw)
        out_dir = tmp_dir / "out"
        out_dir.mkdir()
        ok, msg = converter(in_path, out_dir)
        if not ok:
            raise ReferenceExtractionError(msg)
        out_path = out_dir / (in_path.stem + ".txt")
        if not out_path.exists():
            raise ReferenceExtractionError(f"Converter produced no output: {msg}")
        return out_path.read_text(encoding="utf-8", errors="replace").strip()


def extract_text(filename: str, raw: bytes) -> Tuple[str, bool]:
    """Convert an uploaded reference file to plain text.

    Returns ``(text, truncated)`` where ``truncated`` is True if the text was
    capped at ``MAX_REFERENCE_CHARS``.

    Raises ``UnsupportedReferenceFormat`` for unknown extensions and
    ``ReferenceExtractionError`` when a supported type fails to convert.
    """
    ext = Path(filename).suffix.lower()

    if ext in TEXT_NATIVE_EXTENSIONS or ext in CSV_EXTENSIONS or ext in TSV_EXTENSIONS:
        # Already text/tabular text; keep verbatim, just normalise line endings.
        text = _decode_text(raw)
    elif ext in EXCEL_EXTENSIONS:
        text = _excel_to_text(raw)
    elif ext in PDF_EXTENSIONS:
        from app.services.document_conversion.convert_to_txt import convert_pdf_to_txt

        text = _converted_text_via_tempfile(filename, raw, convert_pdf_to_txt)
    elif ext in DOCX_EXTENSIONS:
        from app.services.document_conversion.convert_to_txt import convert_docx_to_txt

        text = _converted_text_via_tempfile(filename, raw, convert_docx_to_txt)
    else:
        raise UnsupportedReferenceFormat(
            f"Unsupported reference file type '{ext or filename}'. Supported: "
            + ", ".join(sorted(SUPPORTED_EXTENSIONS))
        )

    text = text.strip()
    if not text:
        raise ReferenceExtractionError("No text could be extracted from the file.")

    truncated = False
    if len(text) > MAX_REFERENCE_CHARS:
        text = text[:MAX_REFERENCE_CHARS]
        truncated = True
    return text, truncated


def build_reference_document(filename: str, raw: bytes) -> ReferenceDocument:
    """Create a ReferenceDocument (with extracted text) from uploaded bytes."""
    text, truncated = extract_text(filename, raw)
    return ReferenceDocument(
        id=str(uuid.uuid4()),
        filename=filename,
        content=text,
        char_count=len(text),
        truncated=truncated,
    )


def list_reference_documents(session: VisualizationSession) -> List[ReferenceDocument]:
    return list(session.reference_documents or [])


def get_reference_document(
    session: VisualizationSession, ref_id: str
) -> Optional[ReferenceDocument]:
    for ref in session.reference_documents or []:
        if ref.id == ref_id:
            return ref
    return None


def add_reference_document(
    session: VisualizationSession, ref: ReferenceDocument
) -> None:
    """Append a reference document to the session (in place)."""
    if session.reference_documents is None:  # defensive; default is a list
        session.reference_documents = []
    session.reference_documents.append(ref)


def remove_reference_document(session: VisualizationSession, ref_id: str) -> bool:
    """Remove a reference document by id (in place). Returns True if removed."""
    existing = session.reference_documents or []
    remaining = [r for r in existing if r.id != ref_id]
    if len(remaining) == len(existing):
        return False
    session.reference_documents = remaining
    return True
