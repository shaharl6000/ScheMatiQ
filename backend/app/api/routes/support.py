"""Support endpoint for user-submitted issue reports.

Always available (not gated by DEVELOPER_MODE). Emails the report to
SUPPORT_EMAIL_TO via the shared Gmail sender, attaching the project's complete
export JSON when the client supplies it. Fire-and-forget: always returns 200.
"""

import base64
import io
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from app.core.email_alerts import send_issue_report
from app.services import session_manager
from app.services.document_files import gather_source_documents

logger = logging.getLogger(__name__)

router = APIRouter(tags=["support"])

# Gmail rejects a message larger than 25 MB *after* base64 encoding (~4/3
# overhead). Cap the raw attachment so the encoded message stays under that,
# leaving headroom for the body and headers.
_GMAIL_MAX_ENCODED_BYTES = 25 * 1024 * 1024
_MAX_ATTACHMENT_BYTES = int(_GMAIL_MAX_ENCODED_BYTES * 3 / 4) - 512 * 1024  # ~18 MB raw

# Set default CC recipients here (or load from config)


class ScreenshotAttachment(BaseModel):
    name: Optional[str] = Field(None, max_length=300)
    mime: str = Field(..., max_length=100)   # e.g. "image/png"
    data_b64: str = Field(...)               # base64-encoded image bytes


class IssueReportRequest(BaseModel):
    session_id: Optional[str] = Field(None, max_length=200)
    description: str = Field(..., min_length=1, max_length=5000)
    reporter_email: Optional[str] = Field(None, max_length=254)
    project_json: Optional[str] = Field(None)          # full export JSON as text
    client_context: Optional[Dict[str, Any]] = Field(None)
    screenshots: Optional[List[ScreenshotAttachment]] = Field(None)


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


def _decode_screenshots(
    screenshots: Optional[List[ScreenshotAttachment]],
    budget: int,
) -> Tuple[List[Tuple[str, bytes, str]], int]:
    """Decode user screenshots into ``(name, bytes, subtype)`` within a byte budget.

    Only image/* mimes are accepted. Returns the decoded list plus a count of
    screenshots skipped because they did not fit the remaining budget. Never raises.
    """
    out: List[Tuple[str, bytes, str]] = []
    skipped = 0
    used = 0
    for i, shot in enumerate(screenshots or []):
        try:
            mime = (shot.mime or "").lower().strip()
            if not mime.startswith("image/"):
                continue
            subtype = mime.split("/", 1)[1] or "png"
            data = base64.b64decode(shot.data_b64 or "", validate=False)
            if not data:
                continue
            if used + len(data) > budget:
                skipped += 1
                continue
            used += len(data)
            name = Path(shot.name or "").name or f"screenshot_{i + 1}.{subtype}"
            out.append((name, data, subtype))
        except Exception as e:
            logger.debug("[support] Skipped a screenshot: %s", e)
    return out, skipped



@router.post("/report", summary="Submit a user issue report")
async def submit_issue_report(request: IssueReportRequest):
    """Email a user's issue report. Always returns 200; email is best-effort."""
    try:
        session_id = request.session_id or ""
        context_rows = _build_context_rows(session_id, request.client_context)

        # Optional reporter email: show it in the report and use it as Reply-To so
        # a reply reaches them. Guard against header injection (no CR/LF) and only
        # trust something that at least looks like an address.
        reporter_email = (request.reporter_email or "").strip()
        valid_email = (
            reporter_email
            and "@" in reporter_email
            and "\n" not in reporter_email
            and "\r" not in reporter_email
        )
        reply_to = reporter_email if valid_email else None
        if reporter_email:
            context_rows.insert(0, ("Reporter email", reporter_email))

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

        # Screenshots the user attached to illustrate the bug. They share the
        # attachment budget with the project bundle so the message stays under
        # Gmail's size cap; anything that does not fit is skipped with a note.
        used = len(attachment_bytes) if attachment_bytes else 0
        shots, skipped_shots = _decode_screenshots(
            request.screenshots, _MAX_ATTACHMENT_BYTES - used
        )
        if shots:
            context_rows.append(("Attached images", len(shots)))
        if skipped_shots:
            note_extra = f"{skipped_shots} image(s) omitted (attachment size limit)."
            attachment_note = (attachment_note + " " + note_extra).strip()

        send_error = await run_in_threadpool(
            lambda: send_issue_report(
                description=request.description,
                session_id=session_id,
                context_rows=context_rows,
                attachment_bytes=attachment_bytes,
                attachment_name=attachment_name,
                attachment_subtype=attachment_subtype,
                attachment_note=attachment_note,
                screenshots=shots,
                reply_to=reply_to,
                wait=True,
            )
        )
        if send_error:
            logger.error("[support] Report email failed for session %s: %s", session_id or "(none)", send_error)
            return {"status": "error", "detail": send_error}
        logger.info("[support] Issue report sent for session %s", session_id or "(none)")
        return {"status": "ok"}
    except Exception as e:
        logger.error("[support] Failed to handle issue report: %s", e, exc_info=True)
        return {"status": "error", "detail": "internal error"}