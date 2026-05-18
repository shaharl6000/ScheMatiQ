from dataclasses import dataclass
import os

@dataclass
class LibConfig:
    """Global configuration for the schematiq library."""
    # Whether to write debug artifacts (like skip rationales) to disk
    write_artifacts: bool = os.environ.get("SCHEMATIQ_WRITE_ARTIFACTS", "false").lower() == "true"

# Global instance
config = LibConfig()
