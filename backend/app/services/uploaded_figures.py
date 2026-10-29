"""Attach images uploaded next to a paper (supplementary figures, scanned tables)
to that paper's figures, so value extraction shows them to the model exactly
like the figures Docling extracts from the paper's PDF.

An uploaded image is not a document: it is held in
``pending_documents/uploaded_figures/`` until the paper it belongs to is
present, then added to that paper's ``figures/{stem}/manifest.json`` (the
manifest paper_processor._load_document_figures reads). The paper is the
document whose name shares the most leading ``_``-separated parts with the
image's, at least two (e.g. ``STAT1_12588893_supp_Fig1.jpg`` ->
``STAT1_12588893_full``) or all of a shorter document name. Among tied
documents, those that already have figures from their PDF win (the paper, not
its converted supplementary tables); if that leaves several, the image goes to
each (e.g. a paper uploaded both as ``_full`` and as ``_article_plus_supplement``).

Uploads arrive in shuffled batches, so an image often comes before its paper;
attach_held_images() therefore runs after every upload batch and again when a
run resolves its documents (pipeline.config_handler.resolve_docs_paths).
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from pathlib import Path
from typing import Dict, List, Optional

from app.models.figures import ExtractedFigure, FigureExtractionManifest

logger = logging.getLogger(__name__)

# Image types accepted at upload. Attached images are re-encoded as PNG or JPEG,
# the types the figure loader labels correctly (mimetypes may not know .webp).
IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".webp"})

# Every figure of a document goes inline into each extraction call, so a camera-
# sized image is scaled down to what a figure page renders at.
MAX_IMAGE_SIDE = 2048

HOLD_DIRNAME = "uploaded_figures"

# Document files whose stems an image can be attached to (what the pipeline reads).
_DOCUMENT_EXTENSIONS = frozenset({".txt", ".md", ".html", ".htm"})

_MIN_SHARED_PARTS = 2

# An upload batch and a run start can attach at the same time; one at a time
# keeps a manifest's read-modify-write whole.
_attach_lock = threading.Lock()
_UPLOADED_RE = re.compile(r"^upl(\d+)\.")


class NotAnImage(Exception):
    """An uploaded file that PIL cannot read as an image."""


def is_image(filename: Optional[str]) -> bool:
    return bool(filename) and Path(filename).suffix.lower() in IMAGE_EXTENSIONS


def hold_dir(pending_dir: Path) -> Path:
    return pending_dir / HOLD_DIRNAME


def _parts(stem: str) -> List[str]:
    return [p for p in stem.lower().split("_") if p]


def _shared_parts(a: List[str], b: List[str]) -> int:
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    return n


def match_documents(image_stem: str, document_stems: List[str]) -> List[str]:
    """The document stems sharing the most leading name parts with the image; [] when none
    shares enough (see the module docstring). Ties are broken by attach_held_images."""
    image_parts = _parts(image_stem)
    best, matches = 0, []
    for stem in document_stems:
        doc_parts = _parts(stem)
        shared = _shared_parts(image_parts, doc_parts)
        if shared == 0 or shared < min(_MIN_SHARED_PARTS, len(doc_parts)):
            continue
        if shared > best:
            best, matches = shared, [stem]
        elif shared == best:
            matches.append(stem)
    return matches


def _document_dirs_by_stem(document_dirs: List[Path]) -> Dict[str, Path]:
    """Document stem -> the directory holding its text (first dir wins)."""
    found: Dict[str, Path] = {}
    for d in document_dirs:
        if not d.is_dir():
            continue
        for f in d.iterdir():
            if f.is_file() and not f.name.startswith(".") and f.suffix.lower() in _DOCUMENT_EXTENSIONS:
                found.setdefault(f.stem, d)
    return found


def _read_manifest(figures_dir: Path) -> Optional[FigureExtractionManifest]:
    path = figures_dir / "manifest.json"
    if not path.is_file():
        return None
    return FigureExtractionManifest.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _has_pdf_figures(figures_dir: Path) -> bool:
    try:
        manifest = _read_manifest(figures_dir)
    except Exception:
        return False
    return bool(manifest) and any(not _UPLOADED_RE.match(f.image_filename) for f in manifest.figures)


def _write_image(image: Path, dest_base: Path) -> Path:
    """Re-encode image as PNG (or JPEG if it was one), scaled to MAX_IMAGE_SIDE.
    Raises on a file that is not a readable image."""
    from PIL import Image  # local import: only needed when an image is attached

    try:
        img = Image.open(image)
        img.load()
    except Exception as e:  # PIL's UnidentifiedImageError is an OSError: keep it apart from disk errors
        raise NotAnImage(f"not a readable image: {e}") from e
    dest_base.parent.mkdir(parents=True, exist_ok=True)
    with img:
        is_jpeg = img.format == "JPEG"
        if max(img.size) > MAX_IMAGE_SIDE:
            img.thumbnail((MAX_IMAGE_SIDE, MAX_IMAGE_SIDE))
        if is_jpeg:
            dest = dest_base.with_suffix(".jpg")
            img.convert("RGB").save(dest, "JPEG", quality=90)
        else:
            dest = dest_base.with_suffix(".png")
            img.save(dest, "PNG")
    return dest


def _write_manifest(figures_dir: Path, manifest: FigureExtractionManifest) -> None:
    """Write via a temp file and rename, so a reader never sees half a manifest."""
    tmp = figures_dir / "manifest.json.tmp"
    tmp.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    os.replace(tmp, figures_dir / "manifest.json")


def _add_to_manifest(image: Path, figures_dir: Path) -> bool:
    """Add image to the figures in figures_dir. False if an image of that name is already there."""
    manifest = _read_manifest(figures_dir) or FigureExtractionManifest(
        source_document=figures_dir.name, figure_count=0, status="ok")
    if any(fig.origin_name == image.name for fig in manifest.figures):
        return False

    used = [int(m.group(1)) for f in manifest.figures if (m := _UPLOADED_RE.match(f.image_filename))]
    seq = max(used, default=0) + 1
    dest = _write_image(image, figures_dir / f"upl{seq:03d}")
    manifest.figures.append(ExtractedFigure(
        figure_id=f"{figures_dir.name}_upl{seq:03d}",
        source_document=image.name,
        figure_label="Figure",
        page_no=0,  # not from a PDF page
        image_filename=dest.name,
        caption=f"Image uploaded with this paper as '{image.name}' (supplementary material; no caption).",
        origin_name=image.name,
    ))
    manifest.figure_count = len(manifest.figures)
    try:
        _write_manifest(figures_dir, manifest)
    except Exception:
        dest.unlink(missing_ok=True)
        raise
    return True


def attach_held_images(pending_dir: Path, document_dirs: List[Path]) -> Dict[str, List[str]]:
    """Attach every held image whose paper is now present; leave the rest held.

    Returns {image filename: [document stems it was attached to]} for the images
    attached by this call. Never raises: an image that fails stays held, and an
    image that is not a readable image file is dropped.
    """
    with _attach_lock:
        return _attach_held_images(pending_dir, document_dirs)


def _attach_held_images(pending_dir: Path, document_dirs: List[Path]) -> Dict[str, List[str]]:
    attached: Dict[str, List[str]] = {}
    held = hold_dir(pending_dir)
    try:
        if not held.is_dir():
            return attached
        docs = _document_dirs_by_stem(document_dirs)
        for image in sorted(held.iterdir()):
            if not image.is_file() or not is_image(image.name):
                continue
            stems = match_documents(image.stem, list(docs))
            if len(stems) > 1:
                with_figures = [s for s in stems if _has_pdf_figures(docs[s] / "figures" / s)]
                stems = with_figures or stems
            if not stems:
                continue
            try:
                for stem in stems:
                    _add_to_manifest(image, docs[stem] / "figures" / stem)
            except NotAnImage as e:
                logger.warning("Uploaded image %s dropped: %s", image.name, e)
                image.unlink(missing_ok=True)
                continue
            except Exception as e:
                logger.warning("Could not attach uploaded image %s (kept for the next try): %s", image.name, e)
                continue
            image.unlink(missing_ok=True)
            attached[image.name] = stems
            logger.info("Attached uploaded image %s to the figures of %s", image.name, ", ".join(stems))
        if not any(held.iterdir()):
            held.rmdir()
    except Exception as e:  # runs on the upload and run-start paths: never fail them
        logger.warning("Could not attach held uploaded images in %s: %s", held, e)
    return attached


def held_images(pending_dir: Path) -> List[str]:
    """Images still waiting for their paper."""
    held = hold_dir(pending_dir)
    try:
        return sorted(f.name for f in held.iterdir() if f.is_file()) if held.is_dir() else []
    except OSError:
        return []
