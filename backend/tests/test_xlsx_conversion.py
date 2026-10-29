"""Tests for converting an uploaded .xlsx (e.g. a paper's supplementary table) to text."""

import pandas as pd

from app.services import document_preprocessor as dp
from app.services.document_conversion import convert_to_txt as conv


def _workbook(path, sheets):
    with pd.ExcelWriter(path) as writer:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(writer, sheet_name=name, header=False, index=False)


def test_every_sheet_verbatim(tmp_path):
    src = tmp_path / "AP1-gamma_26673895_supp1.xlsx"
    _workbook(src, {"Index": [["Table S1. Oocyte localization", None]],
                    "Main": [["Protein", "NES"], ["PKIA", "LALKLAGLDI"], [None, None], ["Year", "2001"]]})

    ok, _ = conv.convert_xlsx_to_txt(src, tmp_path)

    assert ok
    text = (tmp_path / "AP1-gamma_26673895_supp1.txt").read_text(encoding="utf-8")
    assert text == ("## Sheet: Index\nTable S1. Oocyte localization\n\n"
                    "## Sheet: Main\nProtein,NES\nPKIA,LALKLAGLDI\nYear,2001\n")


def test_large_sheets_and_workbooks_are_cut(tmp_path, monkeypatch):
    monkeypatch.setattr(conv, "XLSX_SHEET_MAX_CHARS", 100)
    monkeypatch.setattr(conv, "XLSX_MAX_CHARS", 250)
    src = tmp_path / "big.xlsx"
    rows = [[f"protein{i}", "x" * 20] for i in range(50)]
    _workbook(src, {"A": rows, "B": rows, "C": rows, "D": rows})

    ok, _ = conv.convert_xlsx_to_txt(src, tmp_path)

    text = (tmp_path / "big.txt").read_text(encoding="utf-8")
    assert ok and len(text) < 500
    assert "more rows not included]" in text
    assert "protein49" not in text
    assert "[... 2 more sheets not included: C, D]" in text


def test_preprocess_uploaded_xlsx(tmp_path):
    src = tmp_path / "supp.xlsx"
    _workbook(src, {"S1": [["CRM1 cargo", "Score"], ["RanBP1", "9"]]})

    result = dp.preprocess_uploaded_file(src)

    assert result.success and result.method == "xlsx"
    assert result.output_path == tmp_path / "supp.txt"
    assert "RanBP1,9" in result.output_path.read_text(encoding="utf-8")
    assert not src.exists()
