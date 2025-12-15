"""Storage backend factory for selecting storage implementation."""

from typing import Optional

from app.storage.interface import StorageInterface

# Singleton storage instance
_storage_instance: Optional[StorageInterface] = None


def get_storage() -> StorageInterface:
    """Get the storage backend instance.

    The backend is selected based on the STORAGE_BACKEND environment variable:
    - "local" (default): Uses local filesystem storage
    - "supabase": Uses Supabase cloud storage

    The instance is cached as a singleton for the application lifetime.

    Returns:
        StorageInterface implementation
    """
    global _storage_instance

    if _storage_instance is not None:
        return _storage_instance

    # Import config here to avoid circular imports
    from app.core.config import (
        STORAGE_BACKEND,
        SUPABASE_URL,
        SUPABASE_KEY,
        DEFAULT_SESSIONS_DIR,
        DEFAULT_DATA_DIR,
        DEFAULT_QBSD_WORK_DIR,
    )

    if STORAGE_BACKEND == "supabase" and SUPABASE_URL and SUPABASE_KEY:
        try:
            from app.storage.supabase_backend import SupabaseStorageBackend
            _storage_instance = SupabaseStorageBackend(SUPABASE_URL, SUPABASE_KEY)
            print(f"Initialized Supabase storage backend")
        except Exception as e:
            print(f"Failed to initialize Supabase storage: {e}")
            print("Falling back to local storage")
            from app.storage.local_backend import LocalStorageBackend
            _storage_instance = LocalStorageBackend(
                sessions_dir=DEFAULT_SESSIONS_DIR,
                data_dir=DEFAULT_DATA_DIR,
                qbsd_work_dir=DEFAULT_QBSD_WORK_DIR,
            )
    else:
        from app.storage.local_backend import LocalStorageBackend
        _storage_instance = LocalStorageBackend(
            sessions_dir=DEFAULT_SESSIONS_DIR,
            data_dir=DEFAULT_DATA_DIR,
            qbsd_work_dir=DEFAULT_QBSD_WORK_DIR,
        )
        print(f"Initialized local storage backend")

    return _storage_instance


def reset_storage() -> None:
    """Reset the storage singleton (useful for testing)."""
    global _storage_instance
    _storage_instance = None
