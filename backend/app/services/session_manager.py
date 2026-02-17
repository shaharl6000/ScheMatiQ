"""Session management service."""

import hashlib
import logging
import threading
from typing import Dict, List, Optional
from datetime import datetime

from app.models.session import VisualizationSession, SessionType, SessionStatus, ColumnInfo, ColumnBaseline, SchemaBaseline
from app.models.modification import CreationMetadata, ModificationAction
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
        self._sessions: Dict[str, VisualizationSession] = {}
        self._lock = threading.Lock()
        self._load_sessions()

    def _load_sessions(self):
        """Load existing sessions from storage."""
        try:
            # Get list of session IDs using sync method
            session_ids = self._storage.list_sessions_sync()

            # Load each session
            for session_id in session_ids:
                try:
                    session_data = self._storage.get_session_sync(session_id)
                    if session_data:
                        session = VisualizationSession(**session_data)
                        # Migrate session to include new fields if missing
                        session = self.migrate_session(session)
                        self._sessions[session.id] = session
                except Exception as e:
                    logger.error(f"Error loading session {session_id}: {e}")
        except Exception as e:
            logger.error(f"Error loading sessions: {e}")

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
        """Get session by ID."""
        with self._lock:
            return self._sessions.get(session_id)

    def update_session(self, session: VisualizationSession):
        """Update existing session."""
        session.metadata.last_modified = datetime.now()
        with self._lock:
            self._sessions[session.id] = session
        self._save_session(session)

    def delete_session(self, session_id: str) -> bool:
        """Delete session and all associated data."""
        with self._lock:
            if session_id not in self._sessions:
                return False
            # Remove from memory
            del self._sessions[session_id]

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

    def list_sessions(self, session_type: Optional[SessionType] = None) -> List[VisualizationSession]:
        """List all sessions, optionally filtered by type."""
        with self._lock:
            sessions = list(self._sessions.values())
        if session_type:
            sessions = [s for s in sessions if s.type == session_type]
        return sorted(sessions, key=lambda s: s.metadata.created, reverse=True)

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

        if modified:
            logger.debug(f"Migrated session {session.id} with new fields")

        return session
