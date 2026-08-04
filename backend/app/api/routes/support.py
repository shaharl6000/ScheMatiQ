"""Support endpoint for user-submitted issue reports.

Always available (not gated by DEVELOPER_MODE). Emails the report to
SUPPORT_EMAIL_TO via the shared Gmail sender, attaching the project's complete
export JSON when the client supplies it. Fire-and-forget: always returns 200.
"""

import io
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.email_alerts import send_issue_report
from app.services import session_manager
from app.services.document_files import gather_source_documents

logger = logging.getLogger(__name__)

router = APIRouter(tags=["support"])

# Guard rail: skip attaching very large project JSON (Gmail caps at 25 MB).
_MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024

# Set default CC recipients here (or load from config)


class IssueReportRequest(BaseModel):
    session_id: Optional[str] = Field(None, max_length=200)
    description: str = Field(..., min_length=1, max_length=5000)
    project_json: Optional[str] = Field(None)          # full export JSON as text
    client_context: Optional[Dict[str, Any]] = Field(None)


def _build_context_rows(
    session_id: str,
    client_context: Optional[Dict[str, Any]],
) -> List[Tuple[str, str]]:
    """Best-effort context summary from the session and client. Never raises."""
    rows: List[Tuple[str, str]] = []
    if session_id:
        rows.append(("Session", session_id))
    try:
        session = session_manager.get_session(session_id) if session_id else None
    except Exception:
        session = None

    if session is not None:
        try:
            if getattr(session, "schema_query", None):
                rows.append(("Research question", session.schema_query))
            status = getattr(session, "status", None)
            if status is not None:
                rows.append(("Status", getattr(status, "value", str(status))))
            columns = getattr(session, "columns", None)
            if columns is not None:
                rows.append(("Schema columns", len(columns)))
            stats = getattr(session, "statistics", None)
            if stats is not None:
                rows.append(("Rows", getattr(stats, "total_rows", "")))
                rows.append(("Documents", getattr(stats, "total_documents", "")))
            if getattr(session, "error_message", None):
                rows.append(("Last error", session.error_message))
        except Exception as e:
            logger.debug("[support] Could not read session context: %s", e)

    for key in ("url", "activeSheet", "userAgent"):
        if client_context and client_context.get(key):
            rows.append((key, str(client_context[key])[:500]))

    return rows

async def _gather_documents(session_id: str) -> List[Tuple[str, bytes]]:
    """Best-effort fetch of the session's source documents. Never raises."""
    if not session_id:
        return []
    try:
        session = session_manager.get_session(session_id)
        if not session:
            return []
        return await gather_source_documents(session, session_id)
    except Exception as e:
        logger.debug("[support] Could not gather documents: %s", e)
        return []


def _build_bundle_zip(
    project_bytes: Optional[bytes],
    documents: List[Tuple[str, bytes]],
) -> bytes:
    """Zip the project JSON together with the user's source documents.

    Mirrors the 'bundle' export: project.json at the root plus documents/<name>.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zip_file:
        if project_bytes is not None:
            zip_file.writestr("project.json", project_bytes)
        for name, content in documents:
            safe = Path(name).name
            if safe:
                zip_file.writestr(f"documents/{safe}", content)
    return buf.getvalue()


@router.post("/report", summary="Submit a user issue report")
async def submit_issue_report(request: IssueReportRequest):
    """Email a user's issue report. Always returns 200; email is best-effort."""
    try:
        session_id = request.session_id or ""
        context_rows = _build_context_rows(session_id, request.client_context)

        project_bytes: Optional[bytes] = (
            request.project_json.encode("utf-8") if request.project_json else None
        )

        # Attach the documents the user is working with (when any exist), bundled
        # with the project JSON as a .zip — same shape as the 'bundle' export.
        # Fall back to the plain project JSON when there are no documents.
        documents = await _gather_documents(session_id)

        attachment_bytes: Optional[bytes] = None
        attachment_name = ""
        attachment_subtype = "json"
        attachment_note = ""
        short = session_id[:8] if session_id else ""

        if documents:
            zip_bytes = _build_bundle_zip(project_bytes, documents)
            if len(zip_bytes) <= _MAX_ATTACHMENT_BYTES:
                attachment_bytes = zip_bytes
                attachment_name = f"ScheMatiQ_{short}_bundle.zip" if short else "bundle.zip"
                attachment_subtype = "zip"
                context_rows.append(("Attached documents", len(documents)))
            elif project_bytes is not None and len(project_bytes) <= _MAX_ATTACHMENT_BYTES:
                attachment_bytes = project_bytes
                attachment_name = f"ScheMatiQ_{short}_project.json" if short else "project.json"
                attachment_note = (
                    f"Documents omitted (bundle larger than "
                    f"{_MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB); project JSON attached."
                )
            else:
                attachment_note = (
                    f"Attachment omitted (larger than "
                    f"{_MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB)."
                )
        elif project_bytes is not None:
            if len(project_bytes) <= _MAX_ATTACHMENT_BYTES:
                attachment_bytes = project_bytes
                attachment_name = f"ScheMatiQ_{short}_project.json" if short else "project.json"
            else:
                attachment_note = (
                    f"Project export omitted (larger than "
                    f"{_MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB)."
                )

        send_issue_report(
            description=request.description,
            session_id=session_id,
            context_rows=context_rows,
            attachment_bytes=attachment_bytes,
            attachment_name=attachment_name,
            attachment_subtype=attachment_subtype,
            attachment_note=attachment_note,
        )
        logger.info("[support] Issue report queued for session %s", session_id or "(none)")
    except Exception as e:
        logger.error("[support] Failed to handle issue report: %s", e)

    return {"status": "ok"}