"""Session management service."""

import hashlib
import logging
import threading
from typing import Dict, Optional
from datetime import datetime

from app.models.session import VisualizationSession, SessionStatus, ColumnBaseline, SchemaBaseline
from app.models.modification import CreationMetadata
from app.storage import get_storage, StorageInterface

logger = logging.getLogger(__name__)


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

    def _load_session(self, session_id: str) -> Optional[VisualizationSession]:
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
            self._sessions.setdefault(session.id, session)
            return self._sessions[session.id]

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

        columns_dict = {}
        for col in session.columns:
            if col.name and not col.name.lower().endswith('_excerpt'):
                # Calculate checksum from definition + rationale + allowed_values
                content = f"{col.definition or ''}{col.rationale or ''}"
                if col.allowed_values:
                    content += "|".join(sorted(col.allowed_values))
                checksum = hashlib.md5(content.encode()).hexdigest()

                columns_dict[col.name] = ColumnBaseline(
                    name=col.name,
                    definition=col.definition or "",
                    rationale=col.rationale or "",
                    allowed_values=col.allowed_values,
                    checksum=checksum
                )

        session.schema_baseline = SchemaBaseline(
            columns=columns_dict,
            captured_at=datetime.now()
        )

        self.update_session(session)
        logger.debug(f"Captured schema baseline for session {session_id} with {len(columns_dict)} columns")
        return True

    def finalize_creation(self, session_id: str, llm_model: str = "", llm_provider: str = "") -> bool:
        """
        Finalize ScheMatiQ creation by capturing immutable creation metadata.
        Call this when ScheMatiQ schema discovery or loading completes.

        Args:
            session_id: The session ID
            llm_model: The LLM model used for schema creation
            llm_provider: The LLM provider (e.g., "gemini", "openai")

        Returns:
            True if creation was finalized successfully, False otherwise.
        """
        session = self.get_session(session_id)
        if not session:
            return False

        # Only finalize if not already finalized
        if session.creation_metadata is not None:
            logger.debug(f"Session {session_id} already has creation metadata, skipping finalize")
            return True

        # Create immutable creation metadata
        session.creation_metadata = CreationMetadata(
            created_at=session.metadata.created,
            creation_query=session.schema_query or "",
            llm_model=llm_model,
            llm_provider=llm_provider,
            iterations_count=len(session.statistics.schema_evolution.snapshots) if session.statistics and session.statistics.schema_evolution else 0,
            final_schema_size=len([c for c in session.columns if not c.name.lower().endswith('_excerpt')]),
            convergence_achieved=(session.status == SessionStatus.COMPLETED)
        )

        self.update_session(session)
        logger.debug(f"Finalized creation for session {session_id}")
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
