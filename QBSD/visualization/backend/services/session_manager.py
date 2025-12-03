"""Session management service."""

import json
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime

from models.session import VisualizationSession, SessionType

class SessionManager:
    """Manages visualization sessions."""
    
    def __init__(self, storage_path: str = "./sessions"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        self._sessions: Dict[str, VisualizationSession] = {}
        self._load_sessions()
    
    def _load_sessions(self):
        """Load existing sessions from storage."""
        session_files = self.storage_path.glob("*.json")
        for session_file in session_files:
            try:
                with open(session_file) as f:
                    session_data = json.load(f)
                session = VisualizationSession(**session_data)
                self._sessions[session.id] = session
            except Exception as e:
                print(f"Error loading session {session_file}: {e}")
    
    def _save_session(self, session: VisualizationSession):
        """Save session to storage."""
        session_file = self.storage_path / f"{session.id}.json"
        with open(session_file, 'w') as f:
            json.dump(session.model_dump(), f, indent=2, default=str)
    
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
        """Delete session."""
        if session_id not in self._sessions:
            return False
        
        # Remove from memory
        del self._sessions[session_id]
        
        # Remove from storage
        session_file = self.storage_path / f"{session_id}.json"
        if session_file.exists():
            session_file.unlink()
        
        # Remove associated data files
        data_dir = Path("./data") / session_id
        if data_dir.exists():
            import shutil
            shutil.rmtree(data_dir)
        
        return True
    
    def list_sessions(self, session_type: Optional[SessionType] = None) -> List[VisualizationSession]:
        """List all sessions, optionally filtered by type."""
        sessions = list(self._sessions.values())
        if session_type:
            sessions = [s for s in sessions if s.type == session_type]
        return sorted(sessions, key=lambda s: s.metadata.created, reverse=True)