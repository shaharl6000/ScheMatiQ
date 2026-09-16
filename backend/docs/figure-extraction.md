# Figure extraction (Docling)

## Problem

PDF ingestion (see `document-preprocessing.md`) converts every upload to plain `.txt`, discarding images, captions, and layout. There's no way for a later step to "look at" a figure — everything the paper says visually is lost.

## Solution

For every uploaded PDF, run [Docling](https://github.com/docling-project/docling) alongside the existing text conversion. Docling's layout model detects every picture and table on the page and can crop each one to an image directly (`element.get_image(document)`); we filter that down to only items with a resolvable caption (`element.caption_text(document)`), matching the previous PDFFigures 2.0-based implementation's "captioned only" behavior — a captionless embedded image (e.g. a signature block) legitimately produces zero figures. Images + a JSON manifest are persisted per document.

This step only extracts and stores the data — it does not itself read the images with a vision model. That happens downstream, in `schematiq-lib`'s value-extraction pipeline (`paper_processor.py`'s `_load_document_figures`), which is unaffected by this extraction engine and reads the same manifest.json contract regardless of which tool produced it. `ExtractedFigure` carries empty `vision_*` fields for that step to fill in place.

## Code touchpoints

- `backend/app/services/figure_extraction_service.py` — `extract_figures()` / `persist_figures()`: a cached `DocumentConverter` singleton, per-document conversion, and captioned-figure/table extraction
- `backend/app/services/document_preprocessor.py` — hooked into `preprocess_uploaded_file()` (the single point every upload path funnels through), runs before the original PDF is deleted
- `backend/app/models/figures.py` — `ExtractedFigure` / `FigureExtractionManifest`
- `backend/app/core/config.py` — `ENABLE_FIGURE_EXTRACTION`, `DOCLING_ARTIFACTS_PATH`
- `backend/Dockerfile` — installs `docling` via `requirements.txt`, then prefetches its model weights at build time via `docling-tools models download`

## Output

```
data/{session_id}/documents/
    {stem}.txt
    figures/
        {stem}/              # only created when >=1 captioned figure/table was found
            manifest.json     # FigureExtractionManifest
            fig001.png        # sequential per-document, matches figure_id's numeric suffix
            fig002.png
```

A missing/broken model install, a conversion crash, or a malformed PDF all degrade to "no figures extracted" — the existing `.txt` text pipeline is never affected.

There is deliberately no hard wall-clock timeout on a single conversion (unlike the old PDFFigures2 subprocess, which had one for free via `subprocess.run(timeout=...)`). Docling runs in-process; `ENABLE_FIGURE_EXTRACTION` remains the operational kill-switch if it ever needs to be disabled without a redeploy.

## Local setup

Docling downloads its model weights (layout + table-structure) from HuggingFace automatically on first use — no manual build step is required, unlike the old PDFFigures2 jar.

For offline development or to avoid a first-request download, prefetch them explicitly:

```bash
pip install docling
docling-tools models download --output-dir /opt/docling-models
export DOCLING_ARTIFACTS_PATH=/opt/docling-models
```

Without this, `ENABLE_FIGURE_EXTRACTION` still defaults to true and figures are extracted normally — Docling just downloads to its own default cache location on first use instead.

## Tests

```bash
PYENV_VERSION=3.11.9 PYTHONPATH=. pytest tests/test_figure_extraction.py -v
```

The `_build_figure_records()` mapping logic and all wiring tests run fast and offline (no real Docling models loaded — a fake document object stands in for a converted one). The real end-to-end conversion tests are opt-in only: set `RUN_DOCLING_MODEL_TESTS=1` to run them (they use real Docling models and may download weights on first run).

Fixtures: `backend/tests/fixtures/figures/` (synthetic PDF with a heading, image, and caption) plus the existing `backend/tests/fixtures/legal_corpus/seattle_homeland.pdf` as a captionless-image zero-case.
