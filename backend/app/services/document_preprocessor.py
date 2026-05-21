"""Convert uploaded documents to plain text before ScheMatiQ pipeline."""

from __future__ import annotations

import logging
import shutil
import tempfile
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

_soffice_path: Optional[str] = None
_soffice_resolved = False


def _get_soffice_path() -> str:
    """Resolve LibreOffice path once per process (empty string if unavailable)."""
    global _soffice_path, _soffice_resolved
    if _soffice_resolved:
        return _soffice_path or ""
    _soffice_resolved = True
    try:
        _soffice_path = get_libreoffice_path()
    except FileNotFoundError as e:
        logger.warning("LibreOffice not available for document conversion: %s", e)
        _soffice_path = ""
    return _soffice_path or ""


@dataclass
class ExtractionResult:
    output_path: Path
    display_name: str
    method: str
    status: str
    success: bool
    original_filename: Optional[str] = None


def _status_from_message(ext: str, success: bool, message: str) -> tuple[str, str]:
    """Map conversion message to UI status and method label."""
    if not success:
        detail = message
        if not detail.lower().startswith("failed"):
            detail = message
        return "failed", f"failed: {detail}"

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
        return ExtractionResult(
            output_path=source_path,
            display_name=source_path.name,
            method="failed",
            status="failed: empty text file",
            success=False,
            original_filename=original_filename,
        )

    if source_path.suffix.lower() == ".txt":
        output_path = source_path
    else:
        output_path = source_path.with_suffix(".txt")
        output_path.write_text(text, encoding="utf-8")
        if source_path.exists():
            source_path.unlink()

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
    """
    Convert an uploaded file in pending_documents/ to plain text.

    The source file may be deleted on success; output is always {stem}.txt
    alongside other pending documents.
    """
    if not source_path.exists():
        return ExtractionResult(
            output_path=source_path,
            display_name=source_path.name,
            method="failed",
            status="failed: file not found",
            success=False,
            original_filename=original_filename or source_path.name,
        )

    ext = source_path.suffix.lower()
    orig_name = original_filename or source_path.name

    if ext in PLAIN_EXTENSIONS:
        try:
            return _normalize_plain_text(source_path, orig_name)
        except Exception as e:
            return ExtractionResult(
                output_path=source_path,
                display_name=source_path.name,
                method="failed",
                status=f"failed: {e}",
                success=False,
                original_filename=orig_name,
            )

    if ext not in CONVERT_EXTENSIONS:
        return ExtractionResult(
            output_path=source_path,
            display_name=source_path.name,
            method="failed",
            status=f"failed: Unsupported file type: {ext}",
            success=False,
            original_filename=orig_name,
        )

    soffice_path = _get_soffice_path()
    if ext in {".docx", ".doc", ".rtf"} and not soffice_path:
        # DOCX may still work via python-docx; DOC/RTF need LibreOffice
        if ext in {".doc", ".rtf"}:
            return ExtractionResult(
                output_path=source_path,
                display_name=source_path.name,
                method="failed",
                status="failed: LibreOffice not found. Please install LibreOffice or provide the path manually.",
                success=False,
                original_filename=orig_name,
            )

    wid = worker_id or f"{uuid.uuid4().hex[:8]}"
    pending_dir = source_path.parent
    final_output = pending_dir / f"{source_path.stem}.txt"

    try:
        with tempfile.TemporaryDirectory(prefix="doc_convert_") as tmp:
            tmp_dir = Path(tmp)
            success, message = convert_file(
                source_path,
                tmp_dir,
                soffice_path,
                wid,
            )
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

            converted = tmp_dir / f"{source_path.stem}.txt"
            if not converted.exists():
                return ExtractionResult(
                    output_path=source_path,
                    display_name=source_path.name,
                    method="failed",
                    status="failed: conversion produced no output file",
                    success=False,
                    original_filename=orig_name,
                )

            text = converted.read_text(encoding="utf-8", errors="replace").strip()
            if len(text) < 1:
                return ExtractionResult(
                    output_path=source_path,
                    display_name=source_path.name,
                    method="failed",
                    status="failed: No text extracted from document",
                    success=False,
                    original_filename=orig_name,
                )

            if final_output.exists():
                final_output.unlink()
            shutil.copy2(converted, final_output)
            if source_path.exists() and source_path != final_output:
                source_path.unlink()

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
        if final_output.exists() and final_output != source_path:
            final_output.unlink(missing_ok=True)
        return ExtractionResult(
            output_path=source_path,
            display_name=source_path.name,
            method="failed",
            status=f"failed: {e}",
            success=False,
            original_filename=orig_name,
        )
