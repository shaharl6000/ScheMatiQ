"""After #548, allowed_values change only when the user edits them, so the
suggestion and auto-expand endpoints are gone (#642)."""

from app.main import app

REMOVED = (
    "/api/schema/suggestions/{session_id}",
    "/api/schema/approve-suggestion/{session_id}",
    "/api/schema/reject-suggestion/{session_id}",
    "/api/schema/auto-expand-threshold/{session_id}/{column_name:path}",
    "/api/schema/bulk-approve/{session_id}",
)


def test_suggestion_and_auto_expand_routes_are_removed():
    paths = set(app.openapi()["paths"])
    assert not paths.intersection(REMOVED)
    # Unit-merge suggestions are a different feature and stay.
    assert any(p.startswith("/api/units/suggestions") for p in paths)
