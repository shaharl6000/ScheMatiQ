"""The one lock every PyMuPDF call in the backend is made under.

MuPDF is not thread-safe, not even for separate documents, and PDFs are read
on worker threads (asyncio.to_thread) by text conversion, figure extraction
and pdf_utils. Each of them must hold this same lock, from opening a document
until the last PyMuPDF object is freed; separate locks would not keep them
apart.
"""

import threading

PYMUPDF_LOCK = threading.Lock()
