"""Child-process side of Docling figure extraction.

Docling's native PDF parser (docling_parse) can hit an access violation on
some PDFs, which kills whatever process it runs in. figure_extraction_service
therefore runs the conversion here, in a separate worker process, so a crash
costs only that PDF's figures instead of the whole backend.

This module deliberately lives outside ``app.services``: importing that
package runs its ``__init__``, which starts Supabase/Drive/Sheets clients that
a worker must not create. The worker initializer registers a bare
``app.services`` package so ``figure_extraction_service`` can be imported
without it.
"""

from __future__ import annotations

import faulthandler
import sys
import types
from pathlib import Path
from typing import List


def init_worker(crash_log_path: str | None = None) -> None:
    """Prepare a fresh worker process (runs once per worker)."""
    import app

    if "app.services" not in sys.modules:
        pkg = types.ModuleType("app.services")
        pkg.__path__ = [str(Path(app.__file__).parent / "services")]
        sys.modules["app.services"] = pkg

    # Record where a native crash happened; the parent only sees the process die.
    if crash_log_path:
        crash_file = open(crash_log_path, "a", encoding="utf-8")
        faulthandler.enable(file=crash_file, all_threads=True)
        init_worker._crash_file = crash_file


def convert_pdf_figures(pdf_path: str, tmp_dir: str, source_document: str) -> List[dict]:
    """Run Docling on pdf_path, save figure images into tmp_dir, and return the
    ExtractedFigure records as JSON-ready dicts. Raises on failure."""
    from app.services import figure_extraction_service as fes

    converter = fes._get_doc_converter()
    if converter is None:
        raise RuntimeError("Docling converter unavailable")
    conv_res = converter.convert(pdf_path)
    figures = fes._build_figure_records(
        conv_res.document, Path(tmp_dir), source_document=source_document
    )
    return [fig.model_dump(mode="json") for fig in figures]
