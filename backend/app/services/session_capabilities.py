"""Capability predicates for a session — the single place that reads SessionType.

Phase A of the SessionType-removal plan: every gate that used to compare
``session.type`` directly now asks one of these predicates instead. The bodies
currently just wrap the type check, so behavior is identical; centralizing the
reads here means a later switch to state-based signals (does the session have a
live pipeline? are its documents available?) becomes a one-file change rather
than a hunt across the codebase.

Guideline: do NOT re-introduce ``session.type == ...`` comparisons at call
sites. Add or extend a predicate here instead. Session *creation* and
*serialization* sites legitimately set/record the raw type and are out of scope.
"""

from typing import Optional

from app.models.session import SessionType, VisualizationSession


def has_live_pipeline(session: Optional[VisualizationSession]) -> bool:
    """Whether the session is backed by a live ScheMatiQ pipeline run.

    True for pipeline (``SCHEMATIQ``) sessions — including imported sessions that
    were promoted after a rediscovery run, which by then own a self-sufficient
    ``schematiq_work/{id}`` output. Gates that need pipeline artifacts use this:
    the pipeline-only endpoints, ``run_schematiq``, and the ``rediscover`` config
    reuse-vs-synthesize branch.

    A ``None`` session yields ``False`` so callers can drop separate null guards.
    Phase B will switch the body to a state check (presence of the session's
    pipeline output) so it no longer depends on the persisted type.
    """
    return bool(session) and session.type == SessionType.SCHEMATIQ


def is_imported(session: Optional[VisualizationSession]) -> bool:
    """Whether the session was created by importing/uploading an existing table.

    A creation-identity fact (kept as such through Phase B). Gates that are
    genuinely about "this is an imported project" use this: the upload/rediscover
    entrypoints and the pubmed cloud-dataset fallback. Note that an imported
    session which has been promoted to a pipeline session is no longer
    ``is_imported`` — matching the previous ``== UPLOAD`` behavior exactly.

    A ``None`` session yields ``False`` so callers can drop separate null guards.
    """
    return bool(session) and session.type == SessionType.UPLOAD
