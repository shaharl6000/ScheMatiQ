"""Extract captioned figures/tables from PDFs via PDFFigures 2.0.

PDFFigures 2.0 (github.com/allenai/pdffigures2) is a JVM tool, so it's invoked
as a subprocess (same pattern as `_run_libreoffice()` in
document_conversion/convert_to_txt.py) rather than a library call. It only
detects figures/tables that carry a caption; a captionless embedded image
(e.g. a signature block) legitimately yields zero results, which is not a
failure.

Called from document_preprocessor.preprocess_uploaded_file() for every
uploaded PDF. Never raises: a missing jar, a crashed subprocess, or a
malformed PDF all degrade to "no figures extracted" without touching the
existing text-extraction path.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional, Tuple

import pymupdf

from app.core.config import (
    ENABLE_FIGURE_EXTRACTION,
    FIGURE_EXTRACTION_TIMEOUT_SECONDS,
    PDFFIGURES2_JAR_PATH,
)
from app.models.figures import (
    ExtractedFigure,
    FigureBBox,
    FigureContextParagraph,
    FigureExtractionManifest,
)

_CONTEXT_WINDOW = 3
_DPI = 150

# A figure's own region_bbox is trusted unless it looks implausible next to its
# (reliably-detected) caption_bbox — see _is_region_degenerate(). Found against a
# real case: an NIH-PA "Author Manuscript" PDF where PDFFigures2's region-grower
# locked onto the page's rotated watermark sidebar instead of the actual figure,
# for every figure in the document (same bogus box, unrelated content).
_MIN_REGION_TO_CAPTION_WIDTH_RATIO = 0.2
_MAX_REGION_ASPECT_RATIO = 4.0
# Only used when there's no preceding item on the page to anchor to at all —
# NOT a routine ceiling. A real figure taller than this must never be clamped;
# see _fallback_crop_rect().
_MAX_FALLBACK_HEIGHT = 650.0
_FALLBACK_CAPTION_GAP = 4.0
_FALLBACK_WIDTH_MARGIN = 20.0  # padding beyond the caption's own left/right, in points


@dataclass
class _FigureBuildResult:
    figures: List[ExtractedFigure]
    tmp_dir: Path
    status: Literal["ok", "failed"]
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Jar resolution (lazy singleton, mirrors _get_soffice_path in
# document_preprocessor.py)
# ---------------------------------------------------------------------------

_jar_lock = threading.Lock()
_jar_path: Optional[str] = None
_jar_resolved = False


def _get_jar_path() -> Optional[str]:
    """Resolve the pdffigures2 jar path once per process (None if unavailable)."""
    global _jar_path, _jar_resolved
    if _jar_resolved:
        return _jar_path
    with _jar_lock:
        if _jar_resolved:
            return _jar_path
        java_available = shutil.which("java") is not None
        jar_file = Path(PDFFIGURES2_JAR_PATH)
        _jar_path = str(jar_file) if java_available and jar_file.is_file() else None
        _jar_resolved = True
        return _jar_path


# ---------------------------------------------------------------------------
# Heading / paragraph matching (pure, no subprocess — unit-testable directly)
# ---------------------------------------------------------------------------

def _flatten_positioned_items(sections: List[dict]) -> List[dict]:
    """Flatten PDFFigures2's `sections` into a page/y-ordered list of titles+paragraphs.

    Carries each item's `x1` too (not just `y1`) so callers can tell which
    column of a multi-column page an item belongs to — see _column_split().
    """
    items: List[dict] = []
    for section in sections:
        title = section.get("title")
        if title:
            items.append({
                "page": title["page"], "y1": title["region"]["y1"], "x1": title["region"]["x1"],
                "kind": "title", "text": title["text"],
            })
        for para in section.get("paragraphs") or []:
            items.append({
                "page": para["page"], "y1": para["region"]["y1"], "x1": para["region"]["x1"],
                "kind": "paragraph", "text": para["text"],
            })
    items.sort(key=lambda it: (it["page"], it["y1"]))
    return items


def _insertion_pos(page: int, y1: float, items: List[dict]) -> int:
    """Index of the first item at or after (page, y1) in reading order."""
    pos = 0
    for i, it in enumerate(items):
        if (it["page"], it["y1"]) < (page, y1):
            pos = i + 1
        else:
            break
    return pos


def _match_figure_context(
    page: int, y1: float, items: List[dict],
) -> Tuple[Optional[str], List[FigureContextParagraph]]:
    """Nearest preceding heading + up to _CONTEXT_WINDOW paragraphs before/after."""
    pos = _insertion_pos(page, y1, items)

    nearby_heading: Optional[str] = None
    for i in range(pos - 1, -1, -1):
        if items[i]["kind"] == "title":
            nearby_heading = items[i]["text"]
            break

    context: List[FigureContextParagraph] = []
    collected = 0
    for i in range(pos - 1, -1, -1):
        if items[i]["kind"] == "paragraph":
            context.append(FigureContextParagraph(
                text=items[i]["text"], position="before", distance=pos - i,
            ))
            collected += 1
            if collected >= _CONTEXT_WINDOW:
                break

    collected = 0
    for i in range(pos, len(items)):
        if items[i]["kind"] == "paragraph":
            context.append(FigureContextParagraph(
                text=items[i]["text"], position="after", distance=i - pos,
            ))
            collected += 1
            if collected >= _CONTEXT_WINDOW:
                break

    return nearby_heading, context


def _to_bbox(box: Optional[dict], page: int) -> Optional[FigureBBox]:
    if not box:
        return None
    return FigureBBox(left=box["x1"], top=box["y1"], right=box["x2"], bottom=box["y2"], page_no=page)


_MIN_COLUMN_SPREAD = 150.0  # min x1 range on a page before we trust a two-column split at all


def _column_split(page: int, items: List[dict]) -> Optional[float]:
    """Midpoint x for a simple two-column split on this page, or None when the
    page's items don't show enough horizontal spread to reliably imply two
    columns (a single-column page, or too few items to tell).
    """
    xs = [it["x1"] for it in items if it["page"] == page]
    if len(xs) < 4:
        return None
    lo, hi = min(xs), max(xs)
    if hi - lo < _MIN_COLUMN_SPREAD:
        return None
    return (lo + hi) / 2.0


def _column_of(x: float, split: Optional[float]) -> int:
    """0 (left/only column) or 1 (right column), given a column split midpoint."""
    if split is None:
        return 0
    return 1 if x >= split else 0


def _is_region_degenerate(
    region: Optional[FigureBBox],
    caption: Optional[FigureBBox],
    *,
    page: Optional[int] = None,
    items: Optional[List[dict]] = None,
) -> bool:
    """True when PDFFigures2's own region_bbox is implausible next to its own
    caption_bbox (which is detected far more reliably) — either a narrow
    sliver unrelated to the actual figure, or (when page/items are given) a
    region pulled from the wrong column of a multi-column page — rather than
    a real detection failure we have no better signal for.
    """
    if region is None:
        return caption is not None
    width = region.right - region.left
    height = region.bottom - region.top
    if width <= 0 or height <= 0:
        return True
    if height / width > _MAX_REGION_ASPECT_RATIO:
        return True
    if caption is not None:
        caption_width = caption.right - caption.left
        if caption_width > 0 and width / caption_width < _MIN_REGION_TO_CAPTION_WIDTH_RATIO:
            return True
        if page is not None and items is not None:
            split = _column_split(page, items)
            if split is not None and _column_of(region.left, split) != _column_of(caption.left, split):
                return True
    return False


def _fallback_crop_rect(
    page: int, caption: FigureBBox, items: List[dict], *, label: str = "Figure",
) -> Tuple[float, float, float, float]:
    """Caption-anchored crop box (left, top, right, bottom) for a figure whose
    own region_bbox is degenerate: the caption's extent padded by
    _FALLBACK_WIDTH_MARGIN on each side, vertically bounded by the caption's
    edge and the nearest heading/paragraph on the same page *and same column*
    (restricting to the caption's own column matters on multi-column pages —
    otherwise "nearest item" can pull in unrelated content from the other
    column, which is exactly how the region we're replacing went wrong).

    Direction depends on `label`: Tables conventionally have their caption
    ABOVE the content (bound down to the next same-column item), Figures
    BELOW it (bound up to the preceding same-column item) — this is the
    near-universal academic-paper convention, and far more reliable than
    trying to infer direction from the (already untrustworthy) region.

    _MAX_FALLBACK_HEIGHT only applies when there's no neighboring item at all
    to anchor to (e.g. the figure/table is the first or last thing on the
    page) — a real, known neighbor position is always honored as-is, however
    far from the caption it sits, since a tall multi-panel figure or a
    multi-section table legitimately spans that whole gap and clamping it
    would crop real content off.
    """
    split = _column_split(page, items)
    caption_col = _column_of(caption.left, split)
    same_column = [it for it in items if it["page"] == page and _column_of(it["x1"], split) == caption_col]

    if label == "Table":
        pos = _insertion_pos(page, caption.bottom, same_column)
        following_y = same_column[pos]["y1"] if pos < len(same_column) else None
        top = caption.bottom + _FALLBACK_CAPTION_GAP
        bottom = (following_y - _FALLBACK_CAPTION_GAP) if following_y is not None else top + _MAX_FALLBACK_HEIGHT
        if top >= bottom:
            bottom = top + 20.0
    else:
        pos = _insertion_pos(page, caption.top, same_column)
        preceding_y = same_column[pos - 1]["y1"] if pos > 0 else None
        bottom = caption.top - _FALLBACK_CAPTION_GAP
        top = preceding_y if preceding_y is not None else max(0.0, caption.top - _MAX_FALLBACK_HEIGHT)
        if top >= bottom:
            top = max(0.0, bottom - 20.0)

    return caption.left - _FALLBACK_WIDTH_MARGIN, top, caption.right + _FALLBACK_WIDTH_MARGIN, bottom


def _build_figure_records(raw: dict, *, source_document: str) -> List[ExtractedFigure]:
    """Reshape PDFFigures2's DocumentWithSavedFigures JSON into ExtractedFigure records.

    When PDFFigures2's own region_bbox looks implausible next to its caption_bbox,
    substitutes a caption-anchored fallback box (region_source="caption_fallback")
    and matches heading/context against the caption's position instead of the bad
    region's. The actual pixel re-render for that fallback box happens back in
    extract_figures(), which has the open PDF; this function only decides the box.
    """
    items = _flatten_positioned_items(raw.get("sections") or [])
    figures: List[ExtractedFigure] = []
    for idx, fig_raw in enumerate(raw.get("figures") or [], start=1):
        page = fig_raw["page"]
        label = fig_raw.get("figType", "Figure")
        region_bbox = _to_bbox(fig_raw.get("regionBoundary"), page)
        caption_bbox = _to_bbox(fig_raw.get("captionBoundary"), page)

        region_source: Literal["detected", "caption_fallback"] = "detected"
        match_y1 = region_bbox.top if region_bbox else 0.0
        if caption_bbox is not None and _is_region_degenerate(region_bbox, caption_bbox, page=page, items=items):
            region_source = "caption_fallback"
            left, top, right, bottom = _fallback_crop_rect(page, caption_bbox, items, label=label)
            region_bbox = FigureBBox(left=left, top=top, right=right, bottom=bottom, page_no=page)
            match_y1 = caption_bbox.top

        heading, context = _match_figure_context(page, match_y1, items)

        render_url = fig_raw.get("renderURL") or ""
        image_filename = Path(render_url).name if render_url else ""
        if not image_filename and region_source == "caption_fallback":
            image_filename = f"fallback-fig{idx:03d}.png"

        figures.append(ExtractedFigure(
            figure_id=f"fig{idx:03d}",  # doc_stem prefix added by persist_figures()
            source_document=source_document,
            figure_label=label,
            page_no=page,
            region_bbox=region_bbox,
            caption_bbox=caption_bbox,
            image_filename=image_filename,
            caption=fig_raw.get("caption"),
            image_text=fig_raw.get("imageText") or [],
            nearby_heading=heading,
            context_paragraphs=context,
            origin_name=fig_raw.get("name"),
            region_source=region_source,
        ))
    return figures


def _render_fallback_crops(figures: List[ExtractedFigure], pdf_path: Path, tmp_dir: Path) -> None:
    """Re-render each caption_fallback figure's region_bbox directly from the
    PDF, overwriting whatever (possibly degenerate) image PDFFigures2 itself
    produced at the same image_filename. Best-effort: a figure that fails to
    render here just keeps PDFFigures2's own image, if any.
    """
    fallback_figures = [f for f in figures if f.region_source == "caption_fallback" and f.region_bbox]
    if not fallback_figures:
        return
    try:
        doc = pymupdf.open(str(pdf_path))
    except Exception:
        return
    try:
        for fig in fallback_figures:
            try:
                page = doc[fig.page_no]
                box = fig.region_bbox
                rect = pymupdf.Rect(box.left, box.top, box.right, box.bottom) & page.rect
                if rect.is_empty:
                    continue
                pixmap = page.get_pixmap(clip=rect, dpi=_DPI)
                pixmap.save(str(tmp_dir / fig.image_filename))
            except Exception:
                continue
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Extraction (subprocess) + persistence
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
    mid-extraction (e.g. a server restart between pdffigures2 finishing and
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
    """Run pdffigures2 on pdf_path (still on disk) into a staging dir under
    documents_dir/figures/.

    The staging dir lives inside documents_dir/figures/ (not the OS temp dir) so
    that if the process is killed between pdffigures2 finishing and
    persist_figures() renaming it into place, whatever pdffigures2 already found
    is sitting right there in the project, not lost under a system temp
    directory nobody looks at.

    Never raises. Returns None when extraction is disabled or unavailable (no
    staging dir is created in that case). Otherwise always returns a result
    whose tmp_dir the caller (persist_figures) is responsible for consuming
    (renaming into place) or cleaning up.
    """
    if not ENABLE_FIGURE_EXTRACTION:
        return None
    jar_path = _get_jar_path()
    if not jar_path:
        return None

    staging_root = documents_dir / "figures"
    staging_root.mkdir(parents=True, exist_ok=True)
    _sweep_stale_staging_dirs(staging_root)
    tmp_dir = Path(tempfile.mkdtemp(prefix=".extract_", dir=str(staging_root)))

    # Copy to a fixed ASCII-only filename before invoking the jar. On Windows,
    # the JVM decodes argv/filenames using the system's legacy codepage, not
    # UTF-8 (this is NOT reliably fixable via -Dsun.jnu.encoding=UTF-8 — that
    # property is derived from the OS locale very early in JVM bootstrap and
    # ignores that flag in practice). A non-ASCII character anywhere in the
    # PDF's name (e.g. a unicode hyphen "‐") then gets mangled, the resulting
    # File no longer matches anything on disk, and pdffigures2 exits 1 with
    # "is not a PDF file" despite the file existing. Sidestep the whole class
    # of encoding issues by never passing a non-ASCII path to java at all.
    safe_input = tmp_dir / "input.pdf"
    try:
        shutil.copyfile(str(pdf_path), str(safe_input))
    except Exception as e:
        return _FigureBuildResult([], tmp_dir, status="failed", error=str(e))

    prefix = str(tmp_dir) + os.sep
    cmd = [
        "java",
        # pdffigures2 writes its output JSON via `new PrintWriter(file)` with no
        # explicit charset (FigureRenderer.scala), so it uses the JVM's default
        # charset — on Windows that's the system codepage, not UTF-8. Any
        # typographic character in the PDF's text (smart quotes, en-dashes —
        # common in real papers) then gets written as non-UTF-8 bytes, and
        # Python's read_text(encoding="utf-8") below fails to decode them.
        # Unlike sun.jnu.encoding (filename/argv decoding, NOT overridable this
        # way — see the ASCII-safe-copy above), file.encoding IS honored via
        # -D for JVM Writers/PrintStreams, so this fixes it at the source.
        "-Dfile.encoding=UTF-8",
        "-jar", jar_path, str(safe_input),
        "-g", prefix, "-m", prefix,
        "-i", str(_DPI), "-f", "png", "-e", "-q",
    ]

    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=FIGURE_EXTRACTION_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return _FigureBuildResult([], tmp_dir, status="failed", error="timed out")
    except Exception as e:
        return _FigureBuildResult([], tmp_dir, status="failed", error=str(e))

    output_json = tmp_dir / f"{safe_input.stem}.json"
    if not output_json.exists():
        if result.returncode != 0:
            stderr_tail = (result.stderr or b"").decode(errors="replace")[-500:]
            return _FigureBuildResult([], tmp_dir, status="failed", error=f"non-zero exit, no output: {stderr_tail}")
        # Exit 0, no output file: no captioned figures found — not a failure.
        return _FigureBuildResult([], tmp_dir, status="ok", error=None)

    try:
        raw = json.loads(output_json.read_text(encoding="utf-8"))
        figures = _build_figure_records(raw, source_document=source_document)
    except Exception as e:
        return _FigureBuildResult([], tmp_dir, status="failed", error=str(e))

    _render_fallback_crops(figures, safe_input, tmp_dir)

    return _FigureBuildResult(figures, tmp_dir, status="ok", error=None)


def persist_figures(
    build_result: Optional[_FigureBuildResult],
    documents_dir: Path,
    doc_stem: str,
    *,
    source_document: str,
) -> Optional[Path]:
    """Finalize build_result's staging dir into documents_dir/figures/{doc_stem}/.

    Writes manifest.json into the staging dir, strips pdffigures2's own scratch
    files (the staged PDF copy + its raw JSON) so only the wanted images +
    manifest.json remain, then does a single atomic rename of the staging dir
    into place — there is no window where a partially-moved folder can exist.

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

        # Drop pdffigures2's own scratch files (staged PDF copy + raw JSON) so
        # only the wanted images + manifest.json survive into the final folder.
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
