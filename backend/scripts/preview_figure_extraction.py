"""Preview what the Docling figure-extraction pipeline pulls out of a PDF.

Runs the same extract_figures()/persist_figures() the app uses, against an
arbitrary local PDF, and prints the resulting figure list without needing a
session/upload flow. Output (PNGs + manifest.json) is written to
backend/scripts/_preview_output/{pdf_stem}/ for inspection.

Usage:
    python backend/scripts/preview_figure_extraction.py [path/to/paper.pdf]

With no argument, defaults to C:\\Users\\Yaara\\20041890.pdf.
"""

from __future__ import annotations

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.figure_extraction_service import extract_figures, persist_figures

DEFAULT_PDF = Path(r"C:\Users\Yaara\20041890.pdf")
OUTPUT_ROOT = Path(__file__).resolve().parent / "_preview_output"


def main() -> None:
    pdf_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PDF
    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}")
        sys.exit(1)

    # documents_dir is OUTPUT_ROOT itself (not OUTPUT_ROOT/{stem}) because
    # persist_figures() already nests output under documents_dir/figures/{stem} —
    # nesting under {stem} here too would duplicate a long filename twice in the
    # path and can exceed Windows' 260-char MAX_PATH, causing os.rename() to fail
    # silently (persist_figures() never raises, matching the real pipeline).
    documents_dir = OUTPUT_ROOT
    documents_dir.mkdir(parents=True, exist_ok=True)

    print(f"Running Docling on: {pdf_path}")
    print("(first run in this process loads model weights, may take a moment)\n")

    build_result = extract_figures(pdf_path, documents_dir, source_document=pdf_path.name)
    if build_result is None:
        print("Extraction returned None (ENABLE_FIGURE_EXTRACTION is off, or the "
              "Docling converter failed to build). Nothing to show.")
        sys.exit(1)

    if build_result.status == "failed":
        print(f"Conversion failed: {build_result.error}")
        sys.exit(1)

    target_dir = persist_figures(
        build_result, documents_dir, pdf_path.stem, source_document=pdf_path.name
    )

    figures = build_result.figures
    print(f"Found {len(figures)} captioned figure(s)/table(s):\n")
    for fig in figures:
        caption = fig.caption or ""
        if len(caption) > 160:
            caption = caption[:160] + "..."
        print(f"  [{fig.figure_id}] {fig.figure_label}  page={fig.page_no}  file={fig.image_filename}")
        print(f"      caption: {caption}\n")

    if target_dir:
        print(f"Images + manifest.json written to: {target_dir}")
    else:
        print("No figures found — nothing was written to disk.")


if __name__ == "__main__":
    main()
