"""Tests for the session capability predicates.

Phase A is behavior-preserving: these assert the predicates return exactly the
boolean the old ``session.type == ...`` comparisons produced, including the
``None``-session case that call sites used to guard with ``not session or``.
"""

from __future__ import annotations

from app.models.session import SessionMetadata, SessionType, VisualizationSession
from app.services.session_capabilities import has_live_pipeline, is_imported


def _session(session_type: SessionType) -> VisualizationSession:
    return VisualizationSession(
        id="s1",
        type=session_type,
        metadata=SessionMetadata(source="test"),
    )


def test_has_live_pipeline_true_only_for_schematiq():
    assert has_live_pipeline(_session(SessionType.SCHEMATIQ)) is True
    assert has_live_pipeline(_session(SessionType.UPLOAD)) is False


def test_is_imported_true_only_for_upload():
    assert is_imported(_session(SessionType.UPLOAD)) is True
    assert is_imported(_session(SessionType.SCHEMATIQ)) is False


def test_none_session_is_neither():
    # Call sites previously wrote `not session or session.type != SCHEMATIQ`;
    # folding the null check into the predicate must reproduce that.
    assert has_live_pipeline(None) is False
    assert is_imported(None) is False


def test_predicates_are_exact_complements_on_real_sessions():
    # Two enum values today, so on a real (non-None) session the predicates are
    # complementary. They are deliberately defined independently (not one as the
    # negation of the other) because Phase B will make has_live_pipeline
    # state-based while is_imported stays a creation fact.
    for t in (SessionType.SCHEMATIQ, SessionType.UPLOAD):
        s = _session(t)
        assert has_live_pipeline(s) != is_imported(s)
