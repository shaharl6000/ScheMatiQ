"""Tests for JSON document upload preprocessing."""

import json
from pathlib import Path

from app.services.document_preprocessor import preprocess_uploaded_file


def test_json_upload_uses_raw_file_as_text(tmp_path: Path) -> None:
    payload = {"anything": 1, "nested": {"x": "y"}}
    raw = json.dumps(payload, ensure_ascii=False)
    source = tmp_path / "doc.json"
    source.write_text(raw, encoding="utf-8")

    result = preprocess_uploaded_file(source, original_filename="doc.json")
    assert result.success
    assert result.output_path.name == "doc.txt"
    assert result.output_path.read_text(encoding="utf-8") == raw
    assert not source.exists()


def test_arbitrary_json_shape_is_preserved(tmp_path: Path) -> None:
    raw = '{"weird": true, "sections": [{"speaker": "A", "text": "Hi"}]}'
    source = tmp_path / "protocol.json"
    source.write_text(raw, encoding="utf-8")

    result = preprocess_uploaded_file(source)
    assert result.success
    assert result.output_path.read_text(encoding="utf-8") == raw
