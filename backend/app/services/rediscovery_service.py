"""Shared preparation for schema + observation-unit rediscovery.

Both the HTTP route ``POST /load/rediscover`` (workspace "Rediscover schema"
button) and the chat ``rediscover`` tool run the same preparation sequence:
gate on a configured observation unit and available source documents, check the
global LLM quota, synthesize a runnable ``config.json`` for imported sessions,
mark the run as an observation-unit rediscovery, and call ``prepare_resume``.
That sequence used to be duplicated in both call sites, so a change in one could
silently drift from the other.

``prepare_rediscovery`` is that single implementation. It is dependency-injected
(the caller passes its own ``runner`` and ``reextraction_service``), so it is
correct regardless of whether those services are shared singletons yet, and it
raises typed :class:`RediscoveryError` subclasses instead of choosing a user
surface. Each caller translates those into its own error type and message
(``HTTPException`` for the route, ``ValueError`` for the chat tool) and spawns
the run in its own way (FastAPI ``BackgroundTasks`` vs ``asyncio.create_task``).

The function stops at ``prepare_resume`` and does NOT start the pipeline or
release the concurrency slot on failure — callers keep their existing spawn and
cleanup semantics.
"""

from __future__ import annotations

from typing import Any

from app.core.logging_utils import set_session_context
from app.services import session_manager
from app.services.session_capabilities import is_imported


class RediscoveryError(Exception):
    """Base class for rediscovery preparation failures."""


class RediscoverySessionNotFound(RediscoveryError):
    """The session id does not resolve to a session."""


class RediscoveryNotImported(RediscoveryError):
    """require_imported was set but the session is not an imported session."""

    def __init__(self, session_type: Any) -> None:
        super().__init__(f"Session is not an imported session (type='{session_type}').")
        self.session_type = session_type


class RediscoveryNoObservationUnit(RediscoveryError):
    """No observation unit is configured for the session."""


class RediscoveryDocumentsUnavailable(RediscoveryError):
    """No source documents resolve for the session, so rediscovery is a no-op."""


class RediscoveryQuotaExceeded(RediscoveryError):
    """The global LLM usage quota has been reached."""

    def __init__(self, used: int) -> None:
        super().__init__("The global LLM usage quota has been reached.")
        self.used = used


class RediscoveryPipelineBusy(RediscoveryError):
    """prepare_resume could not acquire the slot because a run is still stopping."""


class RediscoveryPrepareFailed(RediscoveryError):
    """prepare_resume failed with a non-transient RuntimeError."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


async def prepare_rediscovery(
    session_id: str,
    *,
    runner: Any,
    reextraction_service: Any,
    require_imported: bool,
) -> None:
    """Run the shared rediscovery preparation up to and including prepare_resume.

    On success the caller may spawn ``runner.run_schematiq(session_id)``. On any
    gate failure a :class:`RediscoveryError` subclass is raised; unrelated
    exceptions (e.g. from ``discover_papers``) propagate unchanged so callers'
    outer handlers see them exactly as before.
    """
    from app.core.config import DEVELOPER_MODE, LLM_CALL_GLOBAL_LIMIT
    from app.models.schematiq import ScheMatiQConfig
    from app.services.rediscovery_config import build_rediscovery_backends
    from schematiq.core.llm_call_tracker import QuotaExceededError

    set_session_context(session_id)
    session = session_manager.get_session(session_id)
    if not session:
        raise RediscoverySessionNotFound()
    if require_imported and not is_imported(session):
        raise RediscoveryNotImported(session.type)
    if not session.observation_unit:
        raise RediscoveryNoObservationUnit()

    # Same gate re-extraction uses: rediscovery is only meaningful if the
    # session's rows actually resolve to real source documents somewhere
    # (local pending_documents/documents, or the session's cloud dataset).
    paper_discovery = await reextraction_service.discover_papers(session_id)
    availability = await reextraction_service.precheck_document_availability(
        session_id, operation_type="reextraction", paper_discovery=paper_discovery,
    )
    if not availability.get("can_proceed", False):
        raise RediscoveryDocumentsUnavailable()

    if not DEVELOPER_MODE:
        try:
            runner.check_global_quota(LLM_CALL_GLOBAL_LIMIT)
        except QuotaExceededError as exc:
            from app.core.email_alerts import send_quota_exceeded_alert
            send_quota_exceeded_alert(total_used=exc.used)
            raise RediscoveryQuotaExceeded(exc.used) from exc

    # Imported session: no runnable config.json exists, so synthesize one.
    # Prefer the project's persisted backends (restored on import from a complete
    # export) so rediscovery uses the ORIGINAL models rather than RELEASE_CONFIG
    # defaults; fall back to defaults for an older import. docs_path is left
    # unset: resolve_docs_paths() auto-detects the session-local
    # pending_documents/documents where imported files live. A SCHEMATIQ session
    # already has a config.json from /schematiq/configure and keeps it.
    if is_imported(session):
        schema_backend, value_backend = build_rediscovery_backends(session, session_id)
        config = ScheMatiQConfig(
            query=session.schema_query or "",
            docs_path=None,
            schema_creation_backend=schema_backend,
            value_extraction_backend=value_backend,
            output_path="outputs/rediscovered_output.json",
        )
        await runner.save_config(session_id, config)

    # Mark this run as an observation-unit rediscovery so prepare_resume clears
    # prior schema/data artifacts and seeds initial_observation_unit from
    # session.observation_unit.
    session = session_manager.get_session(session_id)
    session.metadata.pending_observation_unit_rediscovery = True
    session_manager.update_session(session)

    try:
        await runner.prepare_resume(session_id)
    except RuntimeError as e:
        msg = str(e)
        if "already has an active operation" in msg or "Timed out waiting" in msg:
            raise RediscoveryPipelineBusy(msg) from e
        raise RediscoveryPrepareFailed(msg) from e
