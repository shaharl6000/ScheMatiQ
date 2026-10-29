"""Tests for attaching uploaded images to their paper's figures.

Pure filesystem tests: the held image must end up in the paper's
figures/{stem}/manifest.json next to any figures Docling extracted, in the form
paper_processor._load_document_figures reads.
"""

import json
import os

import pytest

from app.models.figures import ExtractedFigure, FigureExtractionManifest
from app.models.schematiq import LLMConfig, ScheMatiQConfig
from app.services.pipeline.config_handler import resolve_docs_paths
from app.services.uploaded_figures import (
    attach_held_images,
    held_images,
    hold_dir,
    match_documents,
)

DOCS = ["STAT1_12588893_full", "STAT1_10973496_full", "AP1-gamma_26673895_full",
        "AP1-gamma_26673895_supp5", "P53_9254608_abstract"]


@pytest.mark.parametrize("image, expected", [
    ("STAT1_12588893_supp_Fig1", ["STAT1_12588893_full"]),
    # Same PMID, two documents: attached to both.
    ("AP1-gamma_26673895_Fig7-figsupp1", ["AP1-gamma_26673895_full", "AP1-gamma_26673895_supp5"]),
    ("STAT1_12588893_full", ["STAT1_12588893_full"]),
    ("stat1_12588893_supp_fig2", ["STAT1_12588893_full"]),
    # Only the protein name in common: not enough.
    ("STAT1_99999999_Fig1", []),
    ("P53_supp", []),
    ("Unrelated", []),
])
def test_match_documents(image, expected):
    assert match_documents(image, DOCS) == expected


def _hold(pending, name, data=b"\x89PNG fake"):
    hold_dir(pending).mkdir(parents=True, exist_ok=True)
    (hold_dir(pending) / name).write_bytes(data)


def test_attaches_next_to_extracted_figures(tmp_path):
    pending = tmp_path / "pending_documents"
    pending.mkdir()
    (pending / "STAT1_12588893_full.txt").write_text("paper", encoding="utf-8")
    figures = pending / "figures" / "STAT1_12588893_full"
    figures.mkdir(parents=True)
    (figures / "fig001.png").write_bytes(b"png")
    (figures / "manifest.json").write_text(FigureExtractionManifest(
        source_document="STAT1_12588893_full.pdf", figure_count=1, status="ok",
        figures=[ExtractedFigure(figure_id="STAT1_12588893_full_fig001", source_document="STAT1_12588893_full.pdf",
                                 figure_label="Figure", page_no=3, image_filename="fig001.png", caption="Fig. 1")],
    ).model_dump_json(), encoding="utf-8")
    _hold(pending, "STAT1_12588893_supp_Fig1.jpg", b"jpeg bytes")

    attached = attach_held_images(pending, [pending])

    assert attached == {"STAT1_12588893_supp_Fig1.jpg": ["STAT1_12588893_full"]}
    manifest = json.loads((figures / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["figure_count"] == 2
    new = manifest["figures"][1]
    assert new["figure_id"] == "STAT1_12588893_full_upl001"
    assert new["image_filename"] == "upl001.jpg"
    assert new["origin_name"] == "STAT1_12588893_supp_Fig1.jpg"
    assert (figures / "upl001.jpg").read_bytes() == b"jpeg bytes"
    assert not hold_dir(pending).exists()


def test_creates_manifest_for_paper_without_figures(tmp_path):
    pending, documents = tmp_path / "pending_documents", tmp_path / "documents"
    pending.mkdir()
    documents.mkdir()
    (documents / "STAT1_12588893_full.txt").write_text("paper", encoding="utf-8")
    _hold(pending, "STAT1_12588893_supp_Fig1.png")
    _hold(pending, "STAT1_12588893_supp_Fig2.png")

    attach_held_images(pending, [pending, documents])

    # The paper is committed, so its figures go next to it in documents/.
    manifest = json.loads((documents / "figures" / "STAT1_12588893_full" / "manifest.json").read_text(encoding="utf-8"))
    assert [f["image_filename"] for f in manifest["figures"]] == ["upl001.png", "upl002.png"]


def test_image_waits_for_its_paper_and_is_attached_once(tmp_path):
    pending = tmp_path / "pending_documents"
    pending.mkdir()
    _hold(pending, "STAT1_12588893_supp_Fig1.png")

    assert attach_held_images(pending, [pending]) == {}
    assert held_images(pending) == ["STAT1_12588893_supp_Fig1.png"]

    (pending / "STAT1_12588893_full.txt").write_text("paper", encoding="utf-8")
    attach_held_images(pending, [pending])
    # The same image uploaded again is not listed twice.
    _hold(pending, "STAT1_12588893_supp_Fig1.png")
    attach_held_images(pending, [pending])

    manifest = json.loads((pending / "figures" / "STAT1_12588893_full" / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["figures"]) == 1
    assert held_images(pending) == []


@pytest.mark.asyncio
async def test_run_start_attaches_images_uploaded_before_their_paper(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    session_id = "sess-images"
    pending = tmp_path / "data" / session_id / "pending_documents"
    pending.mkdir(parents=True)
    (pending / "STAT1_12588893_full.txt").write_text("paper", encoding="utf-8")
    _hold(pending, "STAT1_12588893_supp_Table1.jpg")
    backend = LLMConfig(provider="openai")
    config = ScheMatiQConfig(query="q", docs_path=None, schema_creation_backend=backend,
                             value_extraction_backend=backend, output_path="outputs/out.json")

    resolved = await resolve_docs_paths(config, session_id, tmp_path / "schematiq_work")

    assert resolved[0].endswith(os.path.join(session_id, "pending_documents"))
    assert (pending / "figures" / "STAT1_12588893_full" / "upl001.jpg").exists()
