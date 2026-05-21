"""Multi-format document to plain-text conversion.

Converts DOCX, DOC, RTF, and PDF files to UTF-8 text.

- DOCX: python-docx first, LibreOffice headless fallback
- DOC / RTF: LibreOffice headless
- PDF: pdfplumber first, OCR fallback (pdf2image + tesseract)

Vendored from Legal_Schema_Generator; adapted for use as a library inside
the ScheMatiQ backend (no CLI entry point, no process-wide signal handlers,
structured logging instead of print()).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple

import pdfplumber
import pytesseract
from docx import Document
from pdf2image import convert_from_path

logger = logging.getLogger(__name__)

LIBREOFFICE_EXTENSIONS = {".rtf", ".docx", ".doc"}
PDF_EXTENSION = ".pdf"
SUPPORTED_EXTENSIONS = LIBREOFFICE_EXTENSIONS | {PDF_EXTENSION}


# ---------------------------------------------------------------------------
# LibreOffice helpers
# ---------------------------------------------------------------------------

def get_libreoffice_path() -> str:
    """Return the LibreOffice soffice executable path.

    Raises FileNotFoundError when LibreOffice is not installed.
    """
    candidates = {
        "darwin": "/Applications/LibreOffice.app/Contents/MacOS/soffice",
        "linux": "/usr/bin/soffice",
        "win32": r"C:\Program Files\LibreOffice\program\soffice.exe",
    }
    default_path = candidates.get(sys.platform, "/usr/bin/soffice")
    if os.path.exists(default_path):
        return default_path

    found = shutil.which("soffice")
    if found:
        return found

    raise FileNotFoundError(
        "LibreOffice not found. Install it or add soffice to PATH."
    )


def _run_libreoffice(
    input_path: Path,
    output_dir: Path,
    soffice_path: str,
    *,
    profile_dir: Optional[Path] = None,
    max_retries: int = 3,
) -> Tuple[bool, str]:
    """Run LibreOffice headless to convert *input_path* to text.

    Each call uses its own user-profile directory (under *profile_dir*, or a
    fresh tempdir) so concurrent invocations don't collide.
    """
    cleanup_profile = False
    if profile_dir is None:
        profile_dir = Path(tempfile.mkdtemp(prefix="lo_profile_"))
        cleanup_profile = True

    cmd = [
        soffice_path,
        "--headless",
        f"-env:UserInstallation=file://{profile_dir}",
        "--convert-to", "txt:Text",
        "--outdir", str(output_dir),
        str(input_path),
    ]

    last_error = ""
    try:
        for attempt in range(max_retries):
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=120,
                )
                expected = output_dir / (input_path.stem + ".txt")
                if result.returncode == 0 and expected.exists():
                    return True, f"Converted: {expected}"

                last_error = (result.stderr.decode() if result.stderr else "Unknown error")
            except subprocess.TimeoutExpired:
                last_error = "Conversion timed out after 120 s"
            except Exception as e:
                last_error = str(e)

            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
    finally:
        if cleanup_profile:
            shutil.rmtree(profile_dir, ignore_errors=True)

    return False, f"LibreOffice conversion failed after {max_retries} attempts: {last_error}"


# ---------------------------------------------------------------------------
# DOCX
# ---------------------------------------------------------------------------

def convert_docx_to_txt(input_path: Path, output_dir: Path) -> Tuple[bool, str]:
    """Extract text from a DOCX using python-docx (paragraphs + tables)."""
    output_path = output_dir / (input_path.stem + ".txt")
    try:
        doc = Document(input_path)
        parts: list[str] = [p.text for p in doc.paragraphs]
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    t = cell.text.strip()
                    if t:
                        parts.append(t)
        if not parts:
            return False, "No text extracted from DOCX"
        output_path.write_text("\n".join(parts), encoding="utf-8")
        return True, f"Converted: {output_path}"
    except Exception as e:
        return False, f"DOCX extraction failed: {e}"


# ---------------------------------------------------------------------------
# PDF (pdfplumber → OCR fallback)
# ---------------------------------------------------------------------------

_CHARS_PER_PAGE_THRESHOLD = 200


def convert_pdf_to_txt(input_path: Path, output_dir: Path) -> Tuple[bool, str]:
    """Convert a PDF to text via pdfplumber; fall back to OCR for scanned pages."""
    output_path = output_dir / (input_path.stem + ".txt")

    with pdfplumber.open(input_path) as pdf:
        page_count = max(1, len(pdf.pages))
        text_parts = [p.extract_text() or "" for p in pdf.pages]

    total_chars = sum(len(t) for t in text_parts)

    if total_chars / page_count >= _CHARS_PER_PAGE_THRESHOLD:
        output_path.write_text(
            "\n\n".join(t for t in text_parts if t), encoding="utf-8",
        )
        return True, f"Converted: {output_path}"

    # Scanned PDF — OCR page-by-page to avoid loading all images into RAM.
    with tempfile.TemporaryDirectory(prefix="ocr_pages_") as tmpdir:
        try:
            page_images = convert_from_path(
                str(input_path),
                dpi=300,
                output_folder=tmpdir,
                fmt="png",
            )
        except Exception as e:
            return False, f"pdf2image failed: {e}"

        ocr_parts: list[str] = []
        for img in page_images:
            text = pytesseract.image_to_string(img)
            if text.strip():
                ocr_parts.append(text)
            # Let each PIL image be freed before the next page is processed.
            img.close()

    if not ocr_parts:
        return False, "No text extracted from PDF (tried pdfplumber and OCR)"

    output_path.write_text("\n\n".join(ocr_parts), encoding="utf-8")
    return True, f"Converted (OCR): {output_path}"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def convert_file(
    input_path: Path,
    output_dir: Path,
    soffice_path: str,
    worker_id: Optional[str] = None,
) -> Tuple[bool, str]:
    """Convert a single file to text based on its extension.

    For DOCX, tries python-docx first and falls back to LibreOffice.
    """
    ext = input_path.suffix.lower()

    if ext == ".docx":
        ok, msg = convert_docx_to_txt(input_path, output_dir)
        if ok:
            return ok, msg
        logger.debug("python-docx failed for %s, falling back to LibreOffice", input_path.name)
        return _run_libreoffice(input_path, output_dir, soffice_path)

    if ext in {".doc", ".rtf"}:
        return _run_libreoffice(input_path, output_dir, soffice_path)

    if ext == PDF_EXTENSION:
        return convert_pdf_to_txt(input_path, output_dir)

    return False, f"Unsupported file type: {ext}"
