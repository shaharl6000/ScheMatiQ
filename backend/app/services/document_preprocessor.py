"""Convert uploaded documents to plain text before the ScheMatiQ pipeline."""

from __future__ import annotations

import logging
import shutil
import tempfile
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from app.services.document_conversion.convert_to_txt import (
    convert_file,
    get_libreoffice_path,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Document-extension sets.
#
# These look similar but encode DIFFERENT intents and have different reasons to
# change, so they are deliberately kept as separate constants rather than
# derived from one another. The two that MUST stay equal (the "loadable as text"
# sets used by continue_discovery) are already derived from MATERIALIZABLE_
# EXTENSIONS at their definitions; do not collapse the rest.
# ---------------------------------------------------------------------------

# Capability: source types we can normalize/convert to plain text.
# PLAIN = read/normalize in place; CONVERT = needs pymupdf (pdf) or LibreOffice
# (office). Changing these tracks what the conversion layer can handle.
PLAIN_EXTENSIONS = {".txt", ".md", ".json"}
CONVERT_EXTENSIONS = {".pdf", ".docx", ".doc", ".rtf"}

# What read_document_text() returns text for WITHOUT conversion. Includes .json
# (we read it as text). Note the asymmetry with LIB_READABLE_STORAGE_EXTENSIONS
# below: that set excludes .json because the schematiq-lib reader cannot load it.
READABLE_TEXT_EXTENSIONS = (".txt", ".md", ".html", ".htm", ".json")

# Single source of truth for "files the pipeline can load as text" — exactly what
# read_document_text() can produce (plain-text family read directly + .pdf
# converted). Callers gating which session documents are loadable derive from
# this so the selectable set never drifts from what read_document_text supports
# (see ORIGINAL_DOC_EXTENSIONS / INCREMENTAL_EXTRACTION_DOC_EXTENSIONS).
MATERIALIZABLE_EXTENSIONS = READABLE_TEXT_EXTENSIONS + (".pdf",)

# Extensions left as-is when committing into data/{session}/documents/ (the rest
# are converted to .txt). This MUST mirror the `exts` set in the schematiq-lib
# reader (schematiq/core/schematiq.py::load_documents) — an external library
# constraint that cannot be bound in code, so it is intentionally NOT derived.
# Excludes .json on purpose: the lib reader does not load .json.
LIB_READABLE_STORAGE_EXTENSIONS = frozenset({".txt", ".md", ".html", ".htm"})


def unique_dest_path(dest_dir: Path, filename: str) -> Path:
    """Return a non-colliding path under dest_dir (adds _N suffix if needed)."""
    dest_path = dest_dir / filename
    if not dest_path.exists():
        return dest_path
    base_name = Path(filename).stem
    extension = Path(filename).suffix
    counter = 1
    while dest_path.exists():
        dest_path = dest_dir / f"{base_name}_{counter}{extension}"
        counter += 1
    return dest_path


def commit_document_to_documents_dir(
    source_path: Path,
    documents_dir: Path,
    *,
    worker_id: Optional[str] = None,
) -> Optional[Path]:
    """Convert if needed and place a lib-readable document in documents/.

    Used when moving files from pending_documents/ into documents/. Converts
    PDF/office/JSON sources to ``{stem}.txt`` via preprocess_uploaded_file.
    Already-readable text types are moved unchanged. Returns the final path, or
    None on failure (logged; ingestion continues for other files).
    """
    if not source_path.is_file() or source_path.name.startswith("."):
        return None

    documents_dir.mkdir(parents=True, exist_ok=True)
    ext = source_path.suffix.lower()

    if ext in LIB_READABLE_STORAGE_EXTENSIONS:
        dest = unique_dest_path(documents_dir, source_path.name)
        shutil.move(str(source_path), str(dest))
        return dest

    if ext in CONVERT_EXTENSIONS or ext in PLAIN_EXTENSIONS:
        result = preprocess_uploaded_file(
            source_path,
            worker_id=worker_id,
            original_filename=source_path.name,
        )
        if not result.success:
            logger.warning(
                "Skipping document %s: conversion failed (%s)",
                source_path.name,
                result.status,
            )
            return None
        ready_path = result.output_path
        dest = unique_dest_path(documents_dir, ready_path.name)
        shutil.move(str(ready_path), str(dest))
        return dest

    logger.warning(
        "Skipping document %s: unsupported type %s for documents/",
        source_path.name,
        ext,
    )
    return None


def commit_bytes_to_documents_dir(
    content: bytes,
    filename: str,
    documents_dir: Path,
    *,
    worker_id: Optional[str] = None,
) -> Optional[Path]:
    """Persist downloaded/uploaded bytes into documents/ as a lib-readable file.

    For callers that receive raw bytes (cloud downloads, direct uploads) rather
    than a file already on disk. Stages the bytes in a private temp directory
    under their real *filename*, then routes through
    commit_document_to_documents_dir so PDF/office/JSON inputs are converted to
    ``{stem}.txt`` exactly like the pending->documents move. The temp directory
    is always cleaned up. Returns the committed path, or None on failure.
    """
    staging_dir = Path(tempfile.mkdtemp(prefix="docstage_"))
    try:
        staged = staging_dir / filename
        staged.write_bytes(content)
        return commit_document_to_documents_dir(
            staged, documents_dir, worker_id=worker_id
        )
    except Exception as e:
        logger.warning("Could not commit bytes for %s: %s", filename, e)
        return None
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)


def read_document_text(path: Path) -> Optional[str]:
    """Read a source document to plain text for the extraction pipeline.

    Centralizes the "if .pdf convert, else read_text" dispatch that was
    duplicated across services. Handles the plain-text family directly and
    converts PDFs via pymupdf. Office formats (.docx/.doc/.rtf) are expected to
    have been converted to .txt at upload time (see preprocess_uploaded_file)
    and are NOT handled here — they return None so callers warn and skip rather
    than feed binary to the LLM. Returns None when the type is unsupported or no
    usable (non-whitespace) text is produced.
    """
    ext = path.suffix.lower()
    try:
        if ext in READABLE_TEXT_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="replace")
        elif ext == ".pdf":
            from app.services.pdf_utils import extract_text_from_pdf
            text = extract_text_from_pdf(path)
        else:
            return None
    except Exception as e:
        logger.warning("Could not read document %s: %s", path.name, e)
        return None
    return text if text and text.strip() else None

_soffice_lock = threading.Lock()
_soffice_path: Optional[str] = None
_soffice_resolved = False


def _get_soffice_path() -> str:
    """Resolve LibreOffice path once per process (empty string if unavailable)."""
    global _soffice_path, _soffice_resolved
    if _soffice_resolved:
        return _soffice_path or ""
    with _soffice_lock:
        if _soffice_resolved:
            return _soffice_path or ""
        try:
            _soffice_path = get_libreoffice_path()
        except FileNotFoundError as e:
            logger.warning("LibreOffice not available: %s", e)
            _soffice_path = ""
        _soffice_resolved = True
    return _soffice_path or ""


@dataclass
class ExtractionResult:
    output_path: Path
    display_name: str
    method: str
    status: str
    success: bool
    original_filename: Optional[str] = None


def _fail(source_path: Path, status: str, orig_name: str) -> ExtractionResult:
    return ExtractionResult(
        output_path=source_path,
        display_name=source_path.name,
        method="failed",
        status=status,
        success=False,
        original_filename=orig_name,
    )


def _status_from_message(ext: str, success: bool, message: str) -> tuple[str, str]:
    """Map conversion message to (method, UI status)."""
    if not success:
        return "failed", f"failed: {message}"

    msg_lower = message.lower()
    if ext == ".pdf":
        if "ocr" in msg_lower:
            return "ocr", "extracted via OCR"
        return "pdf", "extracted from pdf"
    if ext == ".docx":
        if "libreoffice" in msg_lower or "soffice" in msg_lower:
            return "libreoffice", "extracted via LibreOffice"
        return "docx", "extracted from docx"
    if ext in {".doc", ".rtf"}:
        return "libreoffice", "extracted via LibreOffice"
    return "plain", "extracted from text"


def _normalize_plain_text(source_path: Path, original_filename: str) -> ExtractionResult:
    """Normalize .txt/.md/.json uploads to .txt in the same directory.

    JSON is not parsed — the file bytes are used as-is so any schema works.
    """
    ext = source_path.suffix.lower()
    text = source_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return _fail(source_path, "failed: empty file", original_filename)

    if ext == ".txt":
        output_path = source_path
    else:
        output_path = source_path.with_suffix(".txt")
        output_path.write_text(text, encoding="utf-8")
        source_path.unlink(missing_ok=True)

    if ext == ".json":
        method, status = "json", "extracted from json"
    else:
        method, status = "plain", "extracted from text"

    return ExtractionResult(
        output_path=output_path,
        display_name=output_path.name,
        method=method,
        status=status,
        success=True,
        original_filename=original_filename,
    )


def preprocess_uploaded_file(
    source_path: Path,
    *,
    worker_id: Optional[str] = None,
    original_filename: Optional[str] = None,
) -> ExtractionResult:
    """Convert an uploaded file to plain text in-place.

    On success the original binary is replaced by ``{stem}.txt`` in the same
    directory.  On failure the original is left untouched.
    """
    orig_name = original_filename or source_path.name

    if not source_path.exists():
        return _fail(source_path, "failed: file not found", orig_name)

    ext = source_path.suffix.lower()

    if ext in PLAIN_EXTENSIONS:
        try:
            return _normalize_plain_text(source_path, orig_name)
        except Exception as e:
            return _fail(source_path, f"failed: {e}", orig_name)

    if ext not in CONVERT_EXTENSIONS:
        return _fail(source_path, f"failed: Unsupported file type: {ext}", orig_name)

    soffice_path = _get_soffice_path()
    if ext in {".doc", ".rtf"} and not soffice_path:
        return _fail(
            source_path,
            "failed: LibreOffice required for .doc/.rtf but not found",
            orig_name,
        )

    wid = worker_id or uuid.uuid4().hex[:8]
    pending_dir = source_path.parent
    final_output = pending_dir / f"{source_path.stem}.txt"

    try:
        success, message = convert_file(source_path, pending_dir, soffice_path, wid)
        method_key, status = _status_from_message(ext, success, message)

        if not success:
            return ExtractionResult(
                output_path=source_path,
                display_name=source_path.name,
                method=method_key,
                status=status,
                success=False,
                original_filename=orig_name,
            )

        if not final_output.exists():
            return _fail(source_path, "failed: conversion produced no output file", orig_name)

        text = final_output.read_text(encoding="utf-8", errors="replace").strip()
        if not text:
            final_output.unlink(missing_ok=True)
            return _fail(source_path, "failed: No text extracted from document", orig_name)

        if source_path != final_output:
            source_path.unlink(missing_ok=True)

        return ExtractionResult(
            output_path=final_output,
            display_name=final_output.name,
            method=method_key,
            status=status,
            success=True,
            original_filename=orig_name,
        )
    except Exception as e:
        logger.exception("Document preprocessing failed for %s", source_path)
        if final_output != source_path:
            final_output.unlink(missing_ok=True)
        return _fail(source_path, f"failed: {e}", orig_name)
