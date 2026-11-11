"""Session management service."""

import logging
import threading
from typing import Any, Dict, Mapping, Optional
from datetime import datetime

from app.models.session import VisualizationSession, SessionStatus, SchemaBaseline
from app.models.modification import CreationMetadata
from app.storage import get_storage, StorageInterface

logger = logging.getLogger(__name__)

ORPHANABLE_RUNNING_STATUSES = (
    SessionStatus.PROCESSING,
    SessionStatus.SCHEMA_READY,
)


class SessionManager:
    """Manages visualization sessions using storage abstraction."""

    def __init__(self, storage: Optional[StorageInterface] = None):
        """Initialize session manager.

        Args:
            storage: Storage backend instance. If None, uses default from factory.
        """
        self._storage = storage or get_storage()
        # Cache of sessions loaded so far, not a complete mirror of storage.
        # Sessions are fetched on first access; see get_session.
        self._sessions: Dict[str, VisualizationSession] = {}
        self._lock = threading.Lock()

    def _build_session(self, session_data: dict) -> Optional[VisualizationSession]:
        """Turn a stored session dict into a migrated model, or None if unusable."""
        # Migrate old "qbsd" type to "schematiq" before validation
        if session_data.get("type") == "qbsd":
            session_data["type"] = "schematiq"
        session = VisualizationSession(**session_data)
        # Migrate session to include new fields if missing
        return self.migrate_session(session)

    def _load_session(
        self, session_id: str, *, reconcile: bool = True
    ) -> Optional[VisualizationSession]:
        """Fetch one session from storage and cache it. None if absent or invalid."""
        try:
            session_data = self._storage.get_session_sync(session_id)
        except Exception as e:
            logger.error(f"Error reading session {session_id} from storage: {e}")
            return None
        if not session_data:
            return None
        try:
            session = self._build_session(session_data)
        except Exception as e:
            logger.error(f"Error loading session {session_id}: {e}")
            return None
        if session is None:
            return None
        with self._lock:
            # Another thread may have cached it first; that copy is equivalent.
            cached = self._sessions.setdefault(session.id, session)
        if reconcile:
            self.reconcile_orphaned_session(cached)
        return cached

    def reconcile_orphaned_session(
        self,
        session: VisualizationSession,
        running_sessions: Optional[Mapping[str, Any]] = None,
        state_lock: Optional[threading.Lock] = None,
    ) -> bool:
        """Mark a PROCESSING or SCHEMA_READY session as STOPPED if no task or slot owns it."""
        if session.status not in ORPHANABLE_RUNNING_STATUSES:
            return False

        from app.services import concurrency_limiter

        if running_sessions is None:
            try:
                from app.services.orchestration import schematiq_runner

                running_sessions = schematiq_runner.running_sessions
                if state_lock is None:
                    state_lock = schematiq_runner._state_lock
            except Exception:
                running_sessions = {}

        if state_lock is not None:
            with state_lock:
                has_active_task = session.id in running_sessions
        else:
            has_active_task = session.id in running_sessions

        has_active_slot = concurrency_limiter.is_session_active(session.id)
        if not has_active_task and not has_active_slot:
            logger.info(
                "Reconciling orphaned %s session %s to STOPPED",
                session.status.value,
                session.id,
            )
            session.status = SessionStatus.STOPPED
            self.update_session(session)
            return True
        return False

    def reconcile_orphaned_sessions(
        self,
        running_sessions: Optional[Mapping[str, Any]] = None,
        state_lock: Optional[threading.Lock] = None,
    ) -> int:
        """Sweep stored and cached sessions and mark any orphaned running sessions as STOPPED."""
        reconciled = 0
        try:
            stored_ids = list(self._storage.list_sessions_sync())
        except Exception as e:
            logger.error(f"Error listing sessions for orphan reconciliation: {e}")
            stored_ids = []

        with self._lock:
            cached_ids = list(self._sessions.keys())

        seen = set()
        for session_id in [*cached_ids, *stored_ids]:
            if session_id in seen:
                continue
            seen.add(session_id)
            with self._lock:
                session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id, reconcile=False)
            if session is None:
                continue
            if self.reconcile_orphaned_session(
                session, running_sessions=running_sessions, state_lock=state_lock
            ):
                reconciled += 1
        return reconciled

    def _save_session(self, session: VisualizationSession):
        """Save session to storage."""
        self._storage.save_session_sync(session.id, session.model_dump())

    def create_session(self, session: VisualizationSession) -> str:
        """Create a new session."""
        with self._lock:
            self._sessions[session.id] = session
        self._save_session(session)
        return session.id

    def get_session(self, session_id: str) -> Optional[VisualizationSession]:
        """Get session by ID, fetching it from storage on a cache miss."""
        with self._lock:
            cached = self._sessions.get(session_id)
        if cached is not None:
            return cached
        return self._load_session(session_id)

    def update_session(self, session: VisualizationSession):
        """Update existing session."""
        session.metadata.last_modified = datetime.now()
        with self._lock:
            self._sessions[session.id] = session
        self._save_session(session)

    def delete_session(self, session_id: str) -> bool:
        """Delete session and all associated data."""
        # Resolve through get_session: an untouched session is absent from the
        # cache but present in storage, and must still be deletable.
        if self.get_session(session_id) is None:
            return False
        with self._lock:
            self._sessions.pop(session_id, None)

        # Remove from storage (this also cleans up associated data)
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context, schedule the deletion
                asyncio.create_task(self._storage.delete_session(session_id))
                return True
            else:
                return loop.run_until_complete(self._storage.delete_session(session_id))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self._storage.delete_session(session_id))
            finally:
                loop.close()

    def capture_schema_baseline(self, session_id: str) -> bool:
        """
        Capture the current schema state as a baseline for change detection.
        Call this after schema discovery completes or after loading a dataset.

        Returns:
            True if baseline was captured successfully, False otherwise.
        """
        session = self.get_session(session_id)
        if not session:
            return False

        session.schema_baseline = SchemaBaseline.from_columns(session.columns)

        self.update_session(session)
        logger.debug(
            f"Captured schema baseline for session {session_id} with {len(session.schema_baseline.columns)} columns"
        )
        return True

    def migrate_session(self, session: VisualizationSession) -> VisualizationSession:
        """
        Migrate a loaded session to include new fields if missing.
        Call this when loading sessions from storage to ensure compatibility.

        Args:
            session: The session to migrate

        Returns:
            The migrated session with all new fields initialized.
        """
        modified = False

        # Initialize modification_history if missing
        if not hasattr(session, 'modification_history') or session.modification_history is None:
            session.modification_history = []
            modified = True

        # Create creation_metadata from available data if missing
        if not hasattr(session, 'creation_metadata') or session.creation_metadata is None:
            # Try to infer creation metadata from existing session data
            llm_model = ""
            llm_provider = ""

            # Try to get LLM info from extracted_schema
            if session.metadata and hasattr(session.metadata, 'extracted_schema') and session.metadata.extracted_schema:
                llm_config = session.metadata.extracted_schema.get('llm_configuration', {})
                schema_backend = llm_config.get('schema_creation_backend', {})
                llm_model = schema_backend.get('model', '')
                llm_provider = schema_backend.get('provider', '')

            # Calculate iterations count from schema_evolution if available
            iterations_count = 0
            if session.statistics and session.statistics.schema_evolution:
                iterations_count = len(session.statistics.schema_evolution.snapshots)

            session.creation_metadata = CreationMetadata(
                created_at=session.metadata.created if session.metadata else datetime.now(),
                creation_query=session.schema_query or "",
                llm_model=llm_model,
                llm_provider=llm_provider,
                iterations_count=iterations_count,
                final_schema_size=len([c for c in session.columns if not c.name.lower().endswith('_excerpt')]),
                convergence_achieved=(session.status == SessionStatus.COMPLETED)
            )
            modified = True

        # Initialize write_artifacts if missing
        if not hasattr(session, 'write_artifacts') or session.write_artifacts is None:
            from app.core.config import DEVELOPER_MODE
            session.write_artifacts = DEVELOPER_MODE
            modified = True

        if modified:
            logger.debug(f"Migrated session {session.id} with new fields")

        return session
