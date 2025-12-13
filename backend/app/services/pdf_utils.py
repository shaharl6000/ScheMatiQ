"""PDF text extraction utilities."""

import pdfplumber
from pathlib import Path
from typing import Optional


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF file.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        Extracted text content
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text() or ""
                full_text += text + "\n"
        return full_text
    except Exception as e:
        raise ValueError(f"Failed to extract text from PDF: {e}")


def convert_pdf_to_txt(pdf_path: Path, output_path: Optional[Path] = None) -> Path:
    """Convert PDF to text file.

    Args:
        pdf_path: Path to the PDF file
        output_path: Optional output path. If None, uses same name with .txt extension

    Returns:
        Path to the created text file
    """
    if output_path is None:
        output_path = pdf_path.with_suffix('.txt')

    text = extract_text_from_pdf(pdf_path)
    output_path.write_text(text, encoding='utf-8')
    return output_path
