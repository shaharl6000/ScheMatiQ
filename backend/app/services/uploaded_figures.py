"""Attach images uploaded next to a paper (supplementary figures, scanned tables)
to that paper's figures, so value extraction shows them to the model exactly
like the figures Docling extracts from the paper's PDF.

An uploaded image is not a document: it is held in
``pending_documents/uploaded_figures/`` until the paper it belongs to is
present, then added to that paper's ``figures/{stem}/manifest.json`` (the
manifest paper_processor._load_document_figures reads). The paper is the
document whose name shares the most leading ``_``-separated parts with the
image's, at least two (e.g. ``STAT1_12588893_supp_Fig1.jpg`` ->
``STAT1_12588893_full``) or all of a shorter document name. Ties attach the
image to every tied document (e.g. a paper uploaded both as ``_full`` and as
``_article_plus_supplement``).

Uploads arrive in shuffled batches, so an image often comes before its paper;
attach_held_images() therefore runs after every upload batch and again when a
run resolves its documents (pipeline.config_handler.resolve_docs_paths).
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Dict, List

from app.models.figures import ExtractedFigure, FigureExtractionManifest

logger = logging.getLogger(__name__)

# Image types the vision model accepts.
IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".webp"})

HOLD_DIRNAME = "uploaded_figures"

# Document files whose stems an image can be attached to (what the pipeline reads).
_DOCUMENT_EXTENSIONS = frozenset({".txt", ".md", ".html", ".htm"})

_MIN_SHARED_PARTS = 2


def is_image(filename: str) -> bool:
    return Path(filename).suffix.lower() in IMAGE_EXTENSIONS


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
    """The document stems the image belongs to (see the module docstring); [] when none."""
    image_parts = _parts(image_stem)
    best, matches = 0, []
    for stem in document_stems:
        doc_parts = _parts(stem)
        shared = _shared_parts(image_parts, doc_parts)
        if shared < min(_MIN_SHARED_PARTS, len(doc_parts)) or shared == 0:
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


def _add_to_manifest(image: Path, figures_dir: Path) -> bool:
    """Copy image into figures_dir and list it in its manifest. False if already listed."""
    manifest_path = figures_dir / "manifest.json"
    if manifest_path.is_file():
        manifest = FigureExtractionManifest.model_validate(json.loads(manifest_path.read_text(encoding="utf-8")))
    else:
        manifest = FigureExtractionManifest(source_document=figures_dir.name, figure_count=0, status="ok")
    if any(fig.origin_name == image.name for fig in manifest.figures):
        return False

    seq = 1 + sum(1 for fig in manifest.figures if fig.image_filename.startswith("upl"))
    image_filename = f"upl{seq:03d}{image.suffix.lower()}"
    figures_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(image, figures_dir / image_filename)
    manifest.figures.append(ExtractedFigure(
        figure_id=f"{figures_dir.name}_upl{seq:03d}",
        source_document=image.name,
        figure_label="Figure",
        page_no=0,  # not from a PDF page
        image_filename=image_filename,
        caption=f"Image uploaded with this paper as '{image.name}' (supplementary material; no caption).",
        origin_name=image.name,
    ))
    manifest.figure_count = len(manifest.figures)
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return True


def attach_held_images(pending_dir: Path, document_dirs: List[Path]) -> Dict[str, List[str]]:
    """Attach every held image whose paper is now present; leave the rest held.

    Returns {image filename: [document stems it was attached to]} for the images
    attached by this call. Never raises: an image that fails stays held.
    """
    held = hold_dir(pending_dir)
    if not held.is_dir():
        return {}
    docs = _document_dirs_by_stem(document_dirs)
    attached: Dict[str, List[str]] = {}
    for image in sorted(held.iterdir()):
        if not image.is_file() or not is_image(image.name):
            continue
        stems = match_documents(image.stem, list(docs))
        if not stems:
            continue
        try:
            for stem in stems:
                _add_to_manifest(image, docs[stem] / "figures" / stem)
            image.unlink()
            attached[image.name] = stems
            logger.info("Attached uploaded image %s to the figures of %s", image.name, ", ".join(stems))
        except Exception as e:
            logger.warning("Could not attach uploaded image %s: %s", image.name, e)
    if not any(held.iterdir()):
        held.rmdir()
    return attached


def held_images(pending_dir: Path) -> List[str]:
    """Images still waiting for their paper."""
    held = hold_dir(pending_dir)
    return sorted(f.name for f in held.iterdir() if f.is_file()) if held.is_dir() else []
