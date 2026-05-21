# Document preprocessing (fix/document-preprocessing)

## Problem

Uploaded DOCX files (especially Westlaw-style layouts) were stored as raw binaries in `pending_documents/`. Value extraction read them as UTF-8 text, so the LLM saw OOXML/zip content instead of case text. Schema often formed around the few readable documents, producing duplicated values across rows.

## Solution

Convert every upload to plain `.txt` at upload time—one entry point, same logic as `Legal_Schema_Generator` `convert_to_txt.py`:

| Format | Primary | Fallback |
|--------|---------|----------|
| DOCX | python-docx | LibreOffice headless |
| PDF | pdfplumber | OCR (pdf2image + tesseract) |
| DOC / RTF | LibreOffice | — |
| TXT / MD | Normalize to `.txt` | — |

## Code touchpoints

- `backend/app/services/document_conversion/convert_to_txt.py` — vendored conversion
- `backend/app/services/document_preprocessor.py` — `preprocess_uploaded_file()`
- `backend/app/api/routes/load.py` — `add_documents`, `add_cloud-documents`
- `backend/Dockerfile` — `libreoffice-writer`, `poppler-utils`, `tesseract-ocr`
- `backend/requirements.txt` — `python-docx`, `pdfplumber`, `pdf2image`, `pytesseract`
- Frontend — `document_metadata` / `document_extraction` status in upload UI

## API / UI

Successful uploads are stored as `{stem}.txt`. Each file gets `extraction_status` (e.g. `extracted from docx`, `extracted via OCR`, `failed: …`). Failures are returned in `failed_files`; nothing is silently dropped.

## Local setup

```bash
cd backend && source .venv/bin/activate
pip install -r requirements.txt
pip install -e ../schematiq-lib/
# Optional macOS: brew install poppler tesseract libreoffice
```

## Tests

```bash
PYENV_VERSION=3.11.9 PYTHONPATH=. pytest tests/test_document_preprocessor.py -v
```

Fixtures: `backend/tests/fixtures/legal_corpus/` (2 Westlaw DOCX + 1 PDF).

## Note

Observation-unit JSON parse errors are separate from extraction. Lenient JSON parsing for that step was added in `schematiq-lib/schematiq/core/schematiq.py`.
