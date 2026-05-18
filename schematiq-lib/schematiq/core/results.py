from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

@dataclass
class UnitIdentificationResult:
    """Result of identifying observation units in a document."""
    units: List[Dict[str, Any]] = field(default_factory=list)
    skip_reason: Optional[str] = None

@dataclass
class ExtractionResult:
    """Result of extracting values for observation units in a document."""
    rows: List[Dict[str, Any]] = field(default_factory=list)
    skip_reason: Optional[str] = None

@dataclass
class SkippedDocument:
    """Metadata for a document that was skipped during extraction."""
    document: str
    reason: Optional[str] = None

    @property
    def to_dict(self) -> Dict[str, str]:
        """Backward compatibility for dictionary-based skip reporting."""
        return {
            "document": self.document,
            "reason": self.reason or ""
        }
