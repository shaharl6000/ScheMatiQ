"""Convert uploaded documents to plain text before the ScheMatiQ pipeline."""

from __future__ import annotations

import logging
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

PLAIN_EXTENSIONS = {".txt", ".md"}
CONVERT_EXTENSIONS = {".pdf", ".docx", ".doc", ".rtf"}

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
    """Normalize .txt/.md uploads to .txt in the same directory."""
    text = source_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return _fail(source_path, "failed: empty text file", original_filename)

    if source_path.suffix.lower() == ".txt":
        output_path = source_path
    else:
        output_path = source_path.with_suffix(".txt")
        output_path.write_text(text, encoding="utf-8")
        source_path.unlink(missing_ok=True)

    return ExtractionResult(
        output_path=output_path,
        display_name=output_path.name,
        method="plain",
        status="extracted from text",
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
