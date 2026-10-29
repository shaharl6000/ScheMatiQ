"""Content-addressed cache of converted uploads (text + extracted figures).

Converting a PDF (Docling figure extraction plus Gemini caption verification)
takes ~30s, so re-uploading a few hundred papers after an upload was cut off
used to repeat hours of work. Each successful conversion is stored under a key
derived from the file's bytes, its name and the figure settings, and a later
upload of the same file restores the result instead of converting again.

Enabled for local runs (``backend/conversion_cache/``); off on Railway. Set
``DOCUMENT_CONVERSION_CACHE_DIR`` to choose another location, or to an empty
string to disable it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Bump when the conversion output changes, so stale entries are not reused.
# 2: em-relative word gaps, C<n>/H<n> glyph names
# 3: PyMuPDF line order, Symbol / Math Pi / cmsy glyphs
_CACHE_VERSION = "3"


def _resolve_cache_dir() -> Optional[Path]:
    configured = os.environ.get("DOCUMENT_CONVERSION_CACHE_DIR")
    if configured is not None:
        return Path(configured) if configured.strip() else None
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        return None
    return Path(__file__).resolve().parents[2] / "conversion_cache"


CACHE_DIR = _resolve_cache_dir()


def cache_key(source_path: Path, original_filename: str, figure_stem: str) -> Optional[str]:
    """Key for this file's conversion, or None when caching is off."""
    if CACHE_DIR is None:
        return None
    from app.services.figure_extraction_service import (
        ENABLE_FIGURE_CAPTION_VERIFICATION,
        ENABLE_FIGURE_EXTRACTION,
    )

    digest = hashlib.sha256()
    with open(source_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    settings = "|".join([
        _CACHE_VERSION,
        original_filename,
        figure_stem,
        f"figures={int(bool(ENABLE_FIGURE_EXTRACTION))}",
        f"verify={int(bool(ENABLE_FIGURE_CAPTION_VERIFICATION))}",
    ])
    digest.update(b"\0" + settings.encode("utf-8"))
    return digest.hexdigest()


def restore(key: Optional[str], text_output: Path, figures_target: Path) -> Optional[dict]:
    """Restore a cached conversion. Returns its metadata, or None on a miss.

    Writes the text to text_output and, when the entry has figures, replaces
    figures_target with a copy of them.
    """
    if not key or CACHE_DIR is None:
        return None
    entry = CACHE_DIR / key
    try:
        meta = json.loads((entry / "meta.json").read_text(encoding="utf-8"))
        text = (entry / "text.txt").read_text(encoding="utf-8")
    except (OSError, ValueError):
        return None
    from app.services.document_conversion.convert_to_txt import looks_garbled
    if looks_garbled(text):
        # Converted before unreadable text layers were sent to OCR: drop the
        # entry so the fresh conversion is stored in its place.
        shutil.rmtree(entry, ignore_errors=True)
        return None
    try:
        text_output.write_text(text, encoding="utf-8")
        cached_figures = entry / "figures"
        if cached_figures.is_dir():
            if figures_target.exists():
                shutil.rmtree(figures_target, ignore_errors=True)
            figures_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(cached_figures, figures_target)
    except OSError as e:
        logger.warning("Could not restore cached conversion %s: %s", key[:12], e)
        text_output.unlink(missing_ok=True)
        return None
    return meta


def store(key: Optional[str], text_path: Path, figures_dir: Optional[Path], meta: dict) -> None:
    """Save a successful conversion. Never raises."""
    if not key or CACHE_DIR is None:
        return
    entry = CACHE_DIR / key
    if entry.exists():
        return
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".store_", dir=str(CACHE_DIR)))
        try:
            shutil.copyfile(text_path, staging / "text.txt")
            if figures_dir is not None and figures_dir.is_dir():
                shutil.copytree(figures_dir, staging / "figures")
            (staging / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
            os.rename(staging, entry)  # atomic: a reader never sees a partial entry
        except FileExistsError:
            shutil.rmtree(staging, ignore_errors=True)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    except Exception as e:
        logger.warning("Could not cache conversion of %s: %s", text_path.name, e)
