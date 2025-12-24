"""Models for tracking schema modifications and creation metadata."""

from datetime import datetime
from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field


class ModificationAction(BaseModel):
    """Single modification action on the schema."""
    timestamp: datetime = Field(default_factory=datetime.now)
    action_type: Literal["column_added", "column_edited", "column_deleted"]
    column_name: str
    details: Dict[str, Any] = Field(default_factory=dict)

    # Example details for each action type:
    # column_added: {"rationale": "...", "definition": "..."}
    # column_edited: {"field_changed": "rationale", "old_value": "...", "new_value": "..."}
    # column_deleted: {"had_definition": True, "had_rationale": True}


class CreationMetadata(BaseModel):
    """Immutable metadata about QBSD creation."""
    created_at: datetime
    creation_query: str
    llm_model: str = ""
    iterations_count: int = 0
    final_schema_size: int = 0
    convergence_achieved: bool = False
    llm_provider: str = ""
