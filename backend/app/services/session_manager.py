"""Session management service."""

import hashlib
from typing import Dict, List, Optional
from datetime import datetime

from app.models.session import VisualizationSession, SessionType, ColumnInfo, ColumnBaseline, SchemaBaseline
from app.storage import get_storage, StorageInterface


class SessionManager:
    """Manages visualization sessions using storage abstraction."""

    def __init__(self, storage: Optional[StorageInterface] = None):
        """Initialize session manager.

        Args:
            storage: Storage backend instance. If None, uses default from factory.
        """
        self._storage = storage or get_storage()
        self._sessions: Dict[str, VisualizationSession] = {}
        self._load_sessions()

    def _load_sessions(self):
        """Load existing sessions from storage."""
        try:
            # Get list of session IDs
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in an async context, use sync helper
                    session_ids = self._list_sessions_sync()
                else:
                    session_ids = loop.run_until_complete(self._storage.list_sessions())
            except RuntimeError:
                session_ids = self._list_sessions_sync()

            # Load each session
            for session_id in session_ids:
                try:
                    session_data = self._storage.get_session_sync(session_id)
                    if session_data:
                        session = VisualizationSession(**session_data)
                        self._sessions[session.id] = session
                except Exception as e:
                    print(f"Error loading session {session_id}: {e}")
        except Exception as e:
            print(f"Error loading sessions: {e}")

    def _list_sessions_sync(self) -> List[str]:
        """Synchronously list sessions."""
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self._storage.list_sessions())
        finally:
            loop.close()

    def _save_session(self, session: VisualizationSession):
        """Save session to storage."""
        self._storage.save_session_sync(session.id, session.model_dump())

    def create_session(self, session: VisualizationSession) -> str:
        """Create a new session."""
        self._sessions[session.id] = session
        self._save_session(session)
        return session.id

    def get_session(self, session_id: str) -> Optional[VisualizationSession]:
        """Get session by ID."""
        return self._sessions.get(session_id)

    def update_session(self, session: VisualizationSession):
        """Update existing session."""
        session.metadata.last_modified = datetime.now()
        self._sessions[session.id] = session
        self._save_session(session)

    def delete_session(self, session_id: str) -> bool:
        """Delete session and all associated data."""
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
        print(f"DEBUG: Captured schema baseline for session {session_id} with {len(columns_dict)} columns")
        return True
