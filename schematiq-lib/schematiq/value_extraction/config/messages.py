"""Human-readable messages and fallback rationales for extraction."""

# Skip reasons
NO_UNITS_FOUND = "No observation units matched the schema definition for this document."
NO_KNOWN_UNITS = "No known observation units provided for this document."
KNOWN_UNITS_EMPTY = "No observation units could be built from the known-units list."
DEFAULT_SKIP_REASON = "No observation units found for this document."

# Summary messages
def skipped_summary(count: int, names: list[str]) -> str:
    return f"No observation units found in {count} document(s): {', '.join(names)}"
