"""Storage abstraction layer for QueryDiscovery backend.

This package provides a storage abstraction that allows the backend to use
either local filesystem or Supabase cloud storage, controlled by environment
variables.

Usage:
    from app.storage import get_storage

    storage = get_storage()
    await storage.save_session(session_id, data)
    await storage.upload_file("documents", f"{session_id}/file.pdf", content)

Configuration:
    Set STORAGE_BACKEND environment variable:
    - "local" (default): Use local filesystem
    - "supabase": Use Supabase cloud storage (requires SUPABASE_URL and SUPABASE_KEY)
"""

from app.storage.interface import (
    StorageInterface,
    DatasetInfo,
    FileInfo,
    TemplateInfo,
    InitialSchemaInfo,
)
from app.storage.factory import get_storage, reset_storage
from app.storage.local_backend import LocalStorageBackend

__all__ = [
    "StorageInterface",
    "DatasetInfo",
    "FileInfo",
    "TemplateInfo",
    "InitialSchemaInfo",
    "get_storage",
    "reset_storage",
    "LocalStorageBackend",
]

# Note: SupabaseStorageBackend is not exported by default to avoid
# import errors when supabase package is not installed.
# Import it directly: from app.storage.supabase_backend import SupabaseStorageBackend
