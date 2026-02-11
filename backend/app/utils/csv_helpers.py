def format_excerpt_for_csv(ex) -> str:
    """Format an excerpt dict as '[source] text' for CSV export."""
    if isinstance(ex, dict):
        text = ex.get('text', '')
        source = ex.get('source', '')
        if source:
            return f"[{source}] {text}"
        return str(text)
    return str(ex)
