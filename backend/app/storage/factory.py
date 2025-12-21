"""Storage backend factory for selecting storage implementation."""

import logging
from typing import Optional

from app.storage.interface import StorageInterface

logger = logging.getLogger(__name__)

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

    # Log configuration (without exposing secrets)
    logger.info(f"Storage config: STORAGE_BACKEND='{STORAGE_BACKEND}', SUPABASE_URL set={bool(SUPABASE_URL)}, SUPABASE_KEY set={bool(SUPABASE_KEY)}")

    if STORAGE_BACKEND == "supabase" and SUPABASE_URL and SUPABASE_KEY:
        try:
            logger.info("Attempting to initialize Supabase storage backend...")
            from app.storage.supabase_backend import SupabaseStorageBackend
            _storage_instance = SupabaseStorageBackend(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Initialized Supabase storage backend successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase storage: {e}", exc_info=True)
            logger.warning("Falling back to local storage")
            from app.storage.local_backend import LocalStorageBackend
            _storage_instance = LocalStorageBackend(
                sessions_dir=DEFAULT_SESSIONS_DIR,
                data_dir=DEFAULT_DATA_DIR,
                qbsd_work_dir=DEFAULT_QBSD_WORK_DIR,
            )
    else:
        logger.info(f"Using local storage backend (STORAGE_BACKEND='{STORAGE_BACKEND}')")
        from app.storage.local_backend import LocalStorageBackend
        _storage_instance = LocalStorageBackend(
            sessions_dir=DEFAULT_SESSIONS_DIR,
            data_dir=DEFAULT_DATA_DIR,
            qbsd_work_dir=DEFAULT_QBSD_WORK_DIR,
        )
        logger.info("Initialized local storage backend")

    return _storage_instance


def reset_storage() -> None:
    """Reset the storage singleton (useful for testing)."""
    global _storage_instance
    _storage_instance = None
