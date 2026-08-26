"""Extract captioned figures/tables from PDFs via Docling.

Docling (docling-project/docling) is a Python library, so unlike the prior
PDFFigures 2.0-based implementation (a JVM tool invoked as a subprocess) it
runs in-process via a cached DocumentConverter. It detects every picture/
table on the page, whether captioned or not; this module filters down to
only those with a resolvable caption, matching the old PDFFigures2 behavior
(a captionless embedded image, e.g. a signature block, legitimately yields
zero results, which is not a failure).

There is deliberately no hard wall-clock timeout here. The old subprocess
call got one for free from `subprocess.run(timeout=...)`, which also gave
OS-level crash isolation (a JVM hang only killed that subprocess). Docling
runs on this process directly, so replicating both properties would require
a persistent worker process; that complexity isn't taken on for this pass
since Docling doesn't have PDFFigures2's known hang history. If it proves
necessary, `extract_figures()` is the place to add it.

Called from document_preprocessor.preprocess_uploaded_file() for every
uploaded PDF. Never raises: a missing/broken model install, a crashed
conversion, or a malformed PDF all degrade to "no figures extracted" without
touching the existing text-extraction path.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc import BoundingBox, PictureItem, TableItem

from app.core.config import DOCLING_ARTIFACTS_PATH, ENABLE_FIGURE_EXTRACTION
from app.models.figures import ExtractedFigure, FigureExtractionManifest

# scale=1 ~ 72 DPI; the old PDFFigures2 pipeline rendered at 150 DPI (~2.08x).
_IMAGES_SCALE = 2.0


@dataclass
class _FigureBuildResult:
    figures: List[ExtractedFigure]
    tmp_dir: Path
    status: Literal["ok", "failed"]
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# DocumentConverter resolution (lazy singleton — construction loads ML model
# weights, so it's built at most once per process, mirroring the old
# _get_jar_path()'s "resolve once" pattern).
# ---------------------------------------------------------------------------

_converter_lock = threading.Lock()
_doc_converter: Optional[DocumentConverter] = None
_converter_resolved = False


def _build_converter() -> DocumentConverter:
    pipeline_options = PdfPipelineOptions()
    pipeline_options.images_scale = _IMAGES_SCALE
    pipeline_options.generate_picture_images = True
    # Needed so document.pages[n].image is populated — _merge_adjacent_pictures()
    # crops arbitrary (merged) regions from the full page bitmap, which
    # per-picture images (generate_picture_images) alone can't provide.
    pipeline_options.generate_page_images = True
    # OCR is a significant fraction of Docling's per-document conversion time
    # (RapidOCR runs on every page by default) and we don't consume its
    # output: _build_figure_records() reads captions via caption_text(),
    # which resolves the PDF's native text layer, not OCR text. Skipping it
    # only matters for a caption that exists solely as a scanned image with
    # no text layer — an edge case we're accepting to keep conversion fast.
    pipeline_options.do_ocr = False
    if DOCLING_ARTIFACTS_PATH:
        pipeline_options.artifacts_path = DOCLING_ARTIFACTS_PATH
    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )


def _get_doc_converter() -> Optional[DocumentConverter]:
    """Build (once per process) and cache the Docling DocumentConverter.

    A construction failure (missing/corrupt model weights, etc.) is cached
    too — the feature no-ops for the rest of the process's lifetime, matching
    the old "missing jar" degrade-and-move-on behavior.
    """
    global _doc_converter, _converter_resolved
    if _converter_resolved:
        return _doc_converter
    with _converter_lock:
        if _converter_resolved:
            return _doc_converter
        try:
            _doc_converter = _build_converter()
        except Exception:
            _doc_converter = None
        _converter_resolved = True
        return _doc_converter


# ---------------------------------------------------------------------------
# Docling document -> ExtractedFigure records (pure aside from image I/O —
# unit-testable against a fake document object exposing iterate_items())
# ---------------------------------------------------------------------------

# Max gap (PDF points) between two picture bboxes for them to be treated as
# panels of the same figure. Docling's layout model sometimes detects a
# multi-panel figure (e.g. panel A + panel B) as two separate PictureItems
# instead of one region, and its caption-clustering links the caption to only
# one of them; _merge_adjacent_pictures() re-attaches the other panel(s) by
# proximity. 20pt comfortably covers the ~12pt gaps seen between real panels
# while staying well short of the spacing between genuinely distinct figures.
_PANEL_MERGE_GAP_PT = 20.0


def _panel_gap(a: BoundingBox, b: BoundingBox) -> Optional[float]:
    """Gap between two bboxes if they read as stacked/adjacent panels of one
    figure (overlapping on one axis, offset along the other). None if neither
    axis overlaps — not a plausible panel-stacking relationship."""
    if a.overlaps_horizontally(b):
        if a.b >= b.t:
            return a.b - b.t
        if b.b >= a.t:
            return b.b - a.t
        return 0.0  # also overlap vertically -> touching/overlapping
    if a.overlaps_vertically(b):
        if a.r <= b.l:
            return b.l - a.r
        if b.r <= a.l:
            return a.l - b.r
        return 0.0
    return None


def _merge_adjacent_pictures(candidates: list) -> dict:
    """For each captioned PictureItem candidate, absorb nearby uncaptioned
    PictureItem candidates on the same page (see _PANEL_MERGE_GAP_PT) into a
    merged bbox, growing iteratively so a 3+-panel figure with only one
    captioned panel still merges fully. Each uncaptioned candidate is
    absorbed by at most one figure.

    Returns {id(candidate): merged_bbox} only for candidates that actually
    absorbed >=1 neighbor — callers should fall back to the candidate's own
    element.get_image() otherwise, matching pre-merge behavior exactly.
    """
    merge_bbox: dict = {}
    consumed: set = set()

    pictures = [c for c in candidates if c["label"] == "Figure"]
    pages = {c["page_no"] for c in pictures}
    for page_no in pages:
        page_pics = [c for c in pictures if c["page_no"] == page_no]
        captioned = [c for c in page_pics if c["caption"]]
        uncaptioned = [c for c in page_pics if not c["caption"]]

        for cap_c in captioned:
            merged_boxes = [cap_c["bbox"]]
            grew = True
            while grew:
                grew = False
                current_union = BoundingBox.enclosing_bbox(merged_boxes)
                for unc_c in uncaptioned:
                    if id(unc_c) in consumed:
                        continue
                    gap = _panel_gap(current_union, unc_c["bbox"])
                    if gap is not None and gap <= _PANEL_MERGE_GAP_PT:
                        merged_boxes.append(unc_c["bbox"])
                        consumed.add(id(unc_c))
                        grew = True
                        break  # bbox grew; re-scan from the updated union
            if len(merged_boxes) > 1:
                merge_bbox[id(cap_c)] = BoundingBox.enclosing_bbox(merged_boxes)

    return merge_bbox


def _crop_page_region(document, page_no: int, bbox: BoundingBox):
    """Crop an arbitrary bbox from document.pages[page_no]'s full-page image
    (requires PdfPipelineOptions.generate_page_images=True). Mirrors the
    transform docling_core's DocItem.get_image() uses internally, just with a
    caller-supplied bbox instead of the item's own provenance bbox."""
    page = document.pages.get(page_no)
    if page is None or page.size is None or page.image is None:
        return None
    page_image = page.image.pil_image
    if not page_image:
        return None
    crop_bbox = bbox.to_top_left_origin(page_height=page.size.height).scale_to_size(
        old_size=page.size, new_size=page.image.size
    )
    return page_image.crop(crop_bbox.as_tuple())


def _build_figure_records(document, tmp_dir: Path, *, source_document: str) -> List[ExtractedFigure]:
    """Walk a converted Docling document, saving one PNG per captioned
    figure/table into tmp_dir and returning their ExtractedFigure records.

    Captionless pictures/tables are skipped (`if not caption`) to match
    PDFFigures2's old "captioned only" behavior — this is the single toggle
    point if that decision changes later. A figure/table missing provenance
    (page number) or whose image can't be read/saved is also skipped rather
    than emitted with a fabricated/absent required field; none of these are
    expected in practice for items reaching iterate_items(), so they're
    defensive rather than routine paths.

    Before filtering, a captioned PictureItem with an adjacent uncaptioned
    PictureItem on the same page (see _merge_adjacent_pictures) has its crop
    region expanded to cover both — Docling sometimes splits one multi-panel
    figure into separate regions and only associates the caption with one.
    """
    candidates = []
    for element, _level in document.iterate_items():
        if isinstance(element, PictureItem):
            label: Literal["Figure", "Table"] = "Figure"
        elif isinstance(element, TableItem):
            label = "Table"
        else:
            continue

        if not element.prov:
            continue

        try:
            caption = element.caption_text(document) or None
        except Exception:
            caption = None

        candidates.append({
            "element": element,
            "label": label,
            "page_no": element.prov[0].page_no,
            "bbox": element.prov[0].bbox,
            "caption": caption,
        })

    merge_bbox = _merge_adjacent_pictures(candidates)

    figures: List[ExtractedFigure] = []
    seq = 0
    for c in candidates:
        if not c["caption"]:
            continue

        element = c["element"]
        merged = merge_bbox.get(id(c))
        try:
            img = _crop_page_region(document, c["page_no"], merged) if merged is not None \
                else element.get_image(document)
        except Exception:
            img = None
        if img is None:
            continue

        seq += 1
        image_filename = f"fig{seq:03d}.png"
        try:
            img.save(tmp_dir / image_filename, "PNG")
        except Exception:
            seq -= 1
            continue

        figures.append(ExtractedFigure(
            figure_id=f"fig{seq:03d}",  # doc_stem prefix added by persist_figures()
            source_document=source_document,
            figure_label=c["label"],
            page_no=c["page_no"],
            image_filename=image_filename,
            caption=c["caption"],
            origin_name=getattr(element, "self_ref", None),
        ))
    return figures


# ---------------------------------------------------------------------------
# Extraction + persistence
# ---------------------------------------------------------------------------

_STALE_STAGING_AGE_SECONDS = 3600  # sweep .extract_* dirs abandoned by a crashed/killed prior run


def cleanup_staging_dir(tmp_dir: Path) -> None:
    """Remove a .extract_* staging dir, then prune its documents_dir/figures/
    parent too if that leaves it empty.

    extract_figures() always creates that parent (staging_root.mkdir(...))
    before it knows whether anything will end up being extracted, so on a
    no-figures or failed-conversion outcome it would otherwise survive as a
    stray empty directory inside documents/ forever. Never raises.
    """
    shutil.rmtree(tmp_dir, ignore_errors=True)
    try:
        tmp_dir.parent.rmdir()  # no-op (fails silently) unless truly empty
    except OSError:
        pass


def _sweep_stale_staging_dirs(staging_root: Path) -> None:
    """Best-effort cleanup of .extract_* dirs left behind by a process that died
    mid-extraction (e.g. a server restart between conversion finishing and
    persist_figures() moving its output). Never raises."""
    try:
        now = time.time()
        for entry in staging_root.iterdir():
            if not entry.is_dir() or not entry.name.startswith(".extract_"):
                continue
            try:
                if now - entry.stat().st_mtime > _STALE_STAGING_AGE_SECONDS:
                    shutil.rmtree(entry, ignore_errors=True)
            except OSError:
                pass
    except OSError:
        pass


def extract_figures(pdf_path: Path, documents_dir: Path, *, source_document: str) -> Optional[_FigureBuildResult]:
    """Run Docling on pdf_path (still on disk) into a staging dir under
    documents_dir/figures/.

    The staging dir lives inside documents_dir/figures/ (not the OS temp dir) so
    that if the process is killed between conversion finishing and
    persist_figures() renaming it into place, whatever Docling already found
    is sitting right there in the project, not lost under a system temp
    directory nobody looks at.

    Never raises. Returns None when extraction is disabled or the converter is
    unavailable (no staging dir is created in that case). Otherwise always
    returns a result whose tmp_dir the caller (persist_figures) is responsible
    for consuming (renaming into place) or cleaning up.
    """
    if not ENABLE_FIGURE_EXTRACTION:
        return None
    converter = _get_doc_converter()
    if converter is None:
        return None

    staging_root = documents_dir / "figures"
    staging_root.mkdir(parents=True, exist_ok=True)
    _sweep_stale_staging_dirs(staging_root)
    tmp_dir = Path(tempfile.mkdtemp(prefix=".extract_", dir=str(staging_root)))

    try:
        conv_res = converter.convert(str(pdf_path))
        figures = _build_figure_records(conv_res.document, tmp_dir, source_document=source_document)
    except Exception as e:
        return _FigureBuildResult([], tmp_dir, status="failed", error=str(e))

    return _FigureBuildResult(figures, tmp_dir, status="ok", error=None)


def persist_figures(
    build_result: Optional[_FigureBuildResult],
    documents_dir: Path,
    doc_stem: str,
    *,
    source_document: str,
) -> Optional[Path]:
    """Finalize build_result's staging dir into documents_dir/figures/{doc_stem}/.

    Writes manifest.json into the staging dir, strips any scratch files not
    named in the manifest so only the wanted images + manifest.json remain,
    then does a single atomic rename of the staging dir into place — there is
    no window where a partially-moved folder can exist.

    Never raises. No-op (no directory created) when build_result is None or has
    no figures. Always cleans up build_result.tmp_dir (a no-op after a
    successful rename, since the dir no longer exists at that path).
    """
    if build_result is None:
        return None
    try:
        if not build_result.figures:
            return None

        for fig in build_result.figures:
            fig.figure_id = f"{doc_stem}_{fig.figure_id}"

        wanted = {fig.image_filename for fig in build_result.figures if fig.image_filename}

        manifest = FigureExtractionManifest(
            source_document=source_document,
            figure_count=len(build_result.figures),
            figures=build_result.figures,
            status=build_result.status,
            error=build_result.error,
        )
        (build_result.tmp_dir / "manifest.json").write_text(
            manifest.model_dump_json(indent=2), encoding="utf-8",
        )

        for f in build_result.tmp_dir.iterdir():
            if f.is_file() and f.name not in wanted and f.name != "manifest.json":
                f.unlink()

        target_dir = documents_dir / "figures" / doc_stem
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        os.rename(build_result.tmp_dir, target_dir)  # atomic: same filesystem (both under documents_dir/figures/)
        return target_dir
    except Exception:
        return None
    finally:
        cleanup_staging_dir(build_result.tmp_dir)
