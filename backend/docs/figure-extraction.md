# Figure extraction (PDFFigures 2.0)

## Problem

PDF ingestion (see `document-preprocessing.md`) converts every upload to plain `.txt`, discarding images, captions, and layout. There's no way for a later step to "look at" a figure — everything the paper says visually is lost.

## Solution

For every uploaded PDF, run [PDFFigures 2.0](https://github.com/allenai/pdffigures2) (allenai) alongside the existing text conversion. It detects captioned figures/tables, and — combined with its `-g` section-title mode — the document's heading/paragraph structure. We match each figure to its nearest preceding heading and surrounding paragraphs ourselves (the tool doesn't bundle them), and persist images + a JSON manifest per document.

This step only extracts and stores the data — it does not (yet) read the images with a vision model or feed them into the extraction table. `ExtractedFigure` carries empty `vision_*` fields for that future step to fill in place.

Captionless images (e.g. a signature block) legitimately produce zero figures — PDFFigures 2.0 only extracts figures/tables that have a caption, by design.

## Code touchpoints

- `backend/app/services/figure_extraction_service.py` — `extract_figures()` / `persist_figures()`, subprocess wrapper + heading/paragraph matching
- `backend/app/services/document_preprocessor.py` — hooked into `preprocess_uploaded_file()` (the single point every upload path funnels through), runs before the original PDF is deleted
- `backend/app/models/figures.py` — `ExtractedFigure` / `FigureExtractionManifest`
- `backend/app/core/config.py` — `ENABLE_FIGURE_EXTRACTION`, `FIGURE_EXTRACTION_TIMEOUT_SECONDS`, `PDFFIGURES2_JAR_PATH`
- `backend/Dockerfile` — multi-stage build: `pdffigures2-builder` stage compiles the assembly jar via `sbt assembly` (no prebuilt release is published upstream); only the jar is copied into the final image, plus `openjdk-11-jre-headless` to run it

## Output

```
data/{session_id}/documents/
    {stem}.txt
    figures/
        {stem}/                    # only created when >=1 captioned figure was found
            manifest.json           # FigureExtractionManifest
            {stem}-Figure1-1.png    # PDFFigures2's own filenames
```

A missing jar/Java, a subprocess crash, or a malformed PDF all degrade to "no figures extracted" — the existing `.txt` text pipeline is never affected.

## Local setup

PDFFigures 2.0 is a JVM tool with no published prebuilt jar — build it once locally. Verified working on 2026-08-23 with JDK 17 + sbt 1.9.9 (sbt bootstraps whatever version `project/build.properties` pins — 1.7.1 as of the commit below).

**Build from `master`, not the `v0.0.11` tag.** That tag's build (sbt 0.13.8 + the `allenai-sbt-plugins` plugin) depends on Bintray, which shut down in 2021 — its dependency resolution hangs/fails indefinitely. `master` was modernized afterward (sbt 1.7.1, Maven-Central-only deps) and is what actually builds. Pin to a specific commit for reproducibility rather than tracking `master`'s tip.

```bash
# Requires a JDK (11+; works fine with a JRE mismatch too, e.g. built with 17)
# and sbt: https://www.scala-sbt.org/download/
git clone https://github.com/allenai/pdffigures2.git /tmp/pdffigures2
cd /tmp/pdffigures2 && git checkout 3d7ad46753d4a315cccd1c2bcab398380e88c534
sbt assembly
# build.sbt sets a custom assemblyOutputPath — the jar lands at the project
# root as `pdffigures2.jar`, NOT under target/scala-2.12/ (sbt-assembly's default).
mkdir -p /opt/pdffigures2
cp pdffigures2.jar /opt/pdffigures2/pdffigures2-assembly.jar
export PDFFIGURES2_JAR_PATH=/opt/pdffigures2/pdffigures2-assembly.jar
```

On Windows (no package manager for sbt): download the sbt zip release directly (`https://github.com/sbt/sbt/releases`), unzip it anywhere, and invoke `java -jar <unzip-dir>/bin/sbt-launch.jar assembly` from inside the cloned repo instead of a bare `sbt` command.

Without this, `ENABLE_FIGURE_EXTRACTION` still defaults to true but the feature silently no-ops — uploads and tests are unaffected.

## Tests

```bash
PYENV_VERSION=3.11.9 PYTHONPATH=. pytest tests/test_figure_extraction.py -v
```

Unit tests for the heading/paragraph matching logic run without Java. The end-to-end test is skipped automatically when `java`/the jar aren't available (`_pdffigures2_available()`).

Fixtures: `backend/tests/fixtures/figures/` (synthetic PDF with a heading, image, and caption) plus the existing `backend/tests/fixtures/legal_corpus/seattle_homeland.pdf` as a captionless-image zero-case.
