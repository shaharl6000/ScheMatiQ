"""/load/process-dual and /load/extract-schema had no callers (frontend, tests or
scripts) and rebuilt session columns without allowed_values or value_kind, so a
call would silently drop category lists. They are removed."""

from app.main import app


def test_unused_schema_rebuild_endpoints_are_removed():
    paths = set(app.openapi()["paths"])
    assert "/api/load/process-dual/{session_id}" not in paths
    assert "/api/load/extract-schema/{session_id}" not in paths
    # Neighbouring load endpoints stay.
    assert "/api/load/add-documents/{session_id}" in paths
    assert "/api/load/export/{session_id}" in paths
