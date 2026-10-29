"""Tests for sending PDFs with an unreadable text layer to OCR.

Some PDFs (e.g. older papers made by Acrobat Distiller 4) embed subset fonts
with no Unicode map, so pdfplumber returns "(cid:N)" placeholders instead of
letters. Such a page has plenty of "text", so the chars-per-page check alone
never sent it to OCR.
"""

import pytest

pytest.importorskip("reportlab")

from app.services import conversion_cache
from app.services.document_conversion import convert_to_txt
from app.services.document_conversion.convert_to_txt import convert_pdf_to_txt, looks_garbled
from tests.helpers.pdf_fixtures import build_single_column_pdf

_PROSE = (
    "Deletion of NES1 in 4E-T 321-504 or NES2 in 4E-T 505-658 abolished the "
    "nuclear export activity of these fragments. "
) * 20
_GARBLED = "(cid:24)(cid:4)#(cid:11))*(cid:1)(cid:2)(cid:15) n$clear e:port " * 200


def _type1_font(differences, base_font="IDBEIL+AdvUnivers-r"):
    from pdfminer.pdffont import PDFType1Font
    from pdfminer.psparser import LIT

    spec = {
        "Subtype": LIT("Type1"),
        "BaseFont": LIT(base_font),
        "FirstChar": 1,
        "LastChar": 140,
        "Widths": [500] * 140,
        "FontDescriptor": {"FontBBox": [0, 0, 1000, 1000]},
    }
    if differences is not None:
        spec["Encoding"] = {"Type": LIT("Encoding"), "Differences": differences}
    return PDFType1Font(None, spec)


def test_c_number_glyph_names_decode_to_standard_encoding():
    from pdfminer.psparser import LIT

    font = _type1_font([1, LIT("C86"), LIT("C111"), LIT("C108"), LIT("C174")])

    assert [font.to_unichr(code) for code in (1, 2, 3, 4)] == ["V", "o", "l", "ﬁ"]


def test_c_number_glyph_name_overrides_the_base_encoding_character():
    from pdfminer.psparser import LIT

    # Without the C<n> name pdfminer keeps StandardEncoding's "#" for code 35.
    font = _type1_font([35, LIT("C117"), LIT("A")])

    assert font.to_unichr(35) == "u"
    assert font.to_unichr(36) == "A"


def test_math_pi_glyph_names_decode_to_greek_and_symbols():
    from pdfminer.psparser import LIT

    # IκBε as typeset in a JBC paper, then "±" and an unmapped name.
    font = _type1_font([1, LIT("H9260"), LIT("H9280"), LIT("H9251"), LIT("H11006"), LIT("H17188")])

    assert [font.to_unichr(code) for code in (1, 2, 3, 4)] == ["κ", "ϵ", "α", "±"]
    with pytest.raises(Exception):
        font.to_unichr(5)


def test_elsevier_math_font_small_c_names_are_cmsy_positions():
    from pdfminer.psparser import LIT

    # "−80 °C", "16000 × g", "n ≥ 3" in Elsevier papers; C15 is not mapped.
    font = _type1_font([1, LIT("C0"), LIT("C14"), LIT("C2"), LIT("C21"), LIT("C15")], "ABCDEF+AdvP4C4E74")

    assert [font.to_unichr(code) for code in (1, 2, 3, 4)] == ["−", "°", "×", "≥"]
    with pytest.raises(Exception):
        font.to_unichr(5)


def test_elsevier_math_font_c_names_are_never_read_as_character_codes():
    from pdfminer.psparser import LIT

    # In cmsy, C48 is the prime; as a StandardEncoding code it would be "0".
    font = _type1_font([1, LIT("C48")], "ABCDEF+AdvP4C4E74")

    with pytest.raises(Exception):
        font.to_unichr(1)


def test_font_with_a_non_utf8_name_still_loads():
    from pdfminer.pdffont import PDFType1Font
    from pdfminer.psparser import LIT, PSLiteral

    # Seen in a real paper: BaseFont b"ABCDEE+\xcb\xce\xcc\xe5" (GBK); glyph
    # names can come as bytes the same way.
    spec = {
        "Subtype": LIT("Type1"),
        "BaseFont": PSLiteral(b"ABCDEE+\xcb\xce\xcc\xe5"),
        "FirstChar": 1,
        "LastChar": 140,
        "Widths": [500] * 140,
        "FontDescriptor": {"FontBBox": [0, 0, 1000, 1000]},
        "Encoding": {"Type": LIT("Encoding"), "Differences": [1, PSLiteral(b"C86")]},
    }

    assert PDFType1Font(None, spec).to_unichr(1) == "V"


def test_small_c_names_in_other_fonts_stay_unmapped():
    from pdfminer.psparser import LIT

    font = _type1_font([1, LIT("C0"), LIT("C21")])

    for code in (1, 2):
        with pytest.raises(Exception):
            font.to_unichr(code)


def test_unnamed_math_pi_codes_decode_like_symbol():
    # "importin α/β", "100 μg/ml", "−80 °C", "3′": codes with no glyph name.
    font = _type1_font(None, "ABCDEF+MathematicalPi-One")

    assert [font.to_unichr(code) for code in (97, 98, 109, 50, 56, 57)] == ["α", "β", "μ", "−", "°", "′"]


def test_unnamed_code_table_does_not_leak_into_other_fonts():
    # pdfminer shares one StandardEncoding dict between fonts without
    # /Differences; filling a Math Pi font's codes must not change it.
    _type1_font(None, "ABCDEF+MathematicalPi-One")

    assert _type1_font(None, "ABCDEF+TrumpMediaeval-Roman").to_unichr(97) == "a"


def test_advtimes_ffi_ligature():
    assert _type1_font(None, "ABCDEF+AdvTimes").to_unichr(129) == "ffi"


def test_normal_prose_is_not_garbled():
    assert not looks_garbled(_PROSE)
    assert not looks_garbled("")


def test_a_few_cid_symbols_in_prose_are_not_garbled():
    assert not looks_garbled(_PROSE + " (cid:177) (cid:3)")


def test_mostly_cid_glyphs_is_garbled():
    assert looks_garbled(_GARBLED)


@pytest.fixture
def pdf_path(tmp_path):
    path = tmp_path / "paper.pdf"
    build_single_column_pdf(path, "placeholder")
    return path


def _stub_pages(monkeypatch, text):
    monkeypatch.setattr(convert_to_txt, "_extract_page_text", lambda page, line_layout=None: text)


def test_garbled_text_layer_is_replaced_by_ocr(monkeypatch, pdf_path, tmp_path):
    _stub_pages(monkeypatch, _GARBLED)
    monkeypatch.setattr(convert_to_txt, "_ocr_pdf", lambda path: (_PROSE, ""))

    success, message = convert_pdf_to_txt(pdf_path, tmp_path)

    assert success
    assert "OCR" in message
    assert (tmp_path / "paper.txt").read_text(encoding="utf-8") == _PROSE


def test_garbled_text_is_kept_when_ocr_fails(monkeypatch, pdf_path, tmp_path):
    _stub_pages(monkeypatch, _GARBLED)
    monkeypatch.setattr(convert_to_txt, "_ocr_pdf", lambda path: (None, "OCR failed: no tesseract"))

    success, message = convert_pdf_to_txt(pdf_path, tmp_path)

    assert success
    assert "OCR" not in message
    assert (tmp_path / "paper.txt").read_text(encoding="utf-8") == _GARBLED


def test_readable_text_layer_skips_ocr(monkeypatch, pdf_path, tmp_path):
    _stub_pages(monkeypatch, _PROSE)

    def _fail(path):
        raise AssertionError("OCR should not run on a readable PDF")

    monkeypatch.setattr(convert_to_txt, "_ocr_pdf", _fail)

    success, _ = convert_pdf_to_txt(pdf_path, tmp_path)

    assert success
    assert (tmp_path / "paper.txt").read_text(encoding="utf-8") == _PROSE


def test_cache_drops_a_garbled_entry(monkeypatch, tmp_path):
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(conversion_cache, "CACHE_DIR", cache_dir)
    for key, text in (("garbled", _GARBLED), ("readable", _PROSE)):
        source = tmp_path / f"{key}.txt"
        source.write_text(text, encoding="utf-8")
        conversion_cache.store(key, source, None, {"method": "pdf"})

    out = tmp_path / "out.txt"
    assert conversion_cache.restore("garbled", out, tmp_path / "figs") is None
    assert not (cache_dir / "garbled").exists()
    assert conversion_cache.restore("readable", out, tmp_path / "figs") == {"method": "pdf"}
    assert out.read_text(encoding="utf-8") == _PROSE
