"""Lightweight email alerts for quota and system events.

Sends via Gmail API using dedicated Google OAuth credentials
(SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON). No extra passwords or SMTP config needed.

Disabled when credentials or ALERT_EMAIL_TO are not set — never raises.
"""

import base64
import html
import json
import logging
import re
import threading
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Tuple, Union

from app.core.config import (
    ALERT_EMAIL_TO,
    LLM_CALL_GLOBAL_LIMIT,
    SUPPORT_CC_EMAILS,
    SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON,
    SUPPORT_EMAIL_TO,
)

logger = logging.getLogger(__name__)

# Attachment subtypes that should be sent as real image parts (image/<subtype>)
# so mail clients like Gmail render an inline preview instead of a generic file.
_IMAGE_SUBTYPES = {"png", "jpeg", "jpg", "gif", "webp"}

# Track whether we already sent the quota alert (avoid spamming)
_quota_alert_sent = False
_quota_warning_sent = False
_lock = threading.Lock()


def _build_gmail_service():
    """Build Gmail API service directly from SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/gmail.send"]

        if not SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON:
            logger.error(
                "[email-alert] SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON is missing or empty!"
            )
            return None

        cleaned = re.sub(r"[\n\r\t]+\s*", "", SUPPORT_EMAIL_OAUTH_CREDENTIALS_JSON)
        creds_data = json.loads(cleaned)

        # Handle nested keys if user pasted raw client secrets alongside tokens
        if "installed" in creds_data or "web" in creds_data:
            creds_data = creds_data.get("installed") or creds_data.get("web")

        credentials = Credentials(
            token=creds_data.get("token"),
            refresh_token=creds_data["refresh_token"],
            token_uri=creds_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=creds_data["client_id"],
            client_secret=creds_data["client_secret"],
            scopes=creds_data.get("scopes", scopes),
        )
        return build("gmail", "v1", credentials=credentials, cache_discovery=False)

    except Exception as e:
        logger.error("[email-alert] Failed to build Gmail service: %s", e, exc_info=True)
        return None


def _send_email(
    subject: str,
    html_body: str,
    recipient: Optional[str] = None,
    cc: Optional[Union[List[str], str]] = None,
    attachment: Optional[Tuple[str, bytes, str]] = None,
    attachments: Optional[List[Tuple[str, bytes, str]]] = None,
    wait: bool = False,
) -> Optional[str]:
    """Send an email via Gmail API. Never raises.

    By default the send runs in a background thread and returns None. When
    ``wait`` is True the send runs synchronously and returns None on success or
    an error message string on failure (so callers can report real status).
    """
    to_addr = recipient if recipient is not None else ALERT_EMAIL_TO
    if not to_addr:
        logger.warning(
            "[email-alert] No recipient configured (SUPPORT_EMAIL_TO / ALERT_EMAIL_TO is empty) — skipping"
        )
        return "No recipient configured"

    # Normalize CC list to a clean list of strings
    cc_list: List[str] = []
    if isinstance(cc, str):
        cc_list = [c.strip() for c in cc.split(",") if c.strip()]
    elif isinstance(cc, list):
        cc_list = [c.strip() for c in cc if c.strip()]

    def _send() -> Optional[str]:
        try:
            service = _build_gmail_service()
            if not service:
                logger.error("[email-alert] Gmail service creation returned None — skipping send")
                return "Gmail service unavailable"

            all_attachments: List[Tuple[str, bytes, str]] = list(attachments or [])
            if attachment is not None:
                all_attachments.append(attachment)

            if all_attachments:
                msg = MIMEMultipart("mixed")
                msg["Subject"] = subject
                msg["To"] = to_addr
                if cc_list:
                    msg["Cc"] = ", ".join(cc_list)
                body = MIMEMultipart("alternative")
                body.attach(MIMEText(html_body, "html"))
                msg.attach(body)
                for filename, content, mime_subtype in all_attachments:
                    subtype = (mime_subtype or "octet-stream").lower()
                    if subtype in _IMAGE_SUBTYPES:
                        # Real image parts (image/png etc.) so Gmail previews them inline.
                        part = MIMEImage(content, _subtype=("jpeg" if subtype == "jpg" else subtype))
                    else:
                        part = MIMEApplication(content, _subtype=subtype)
                    part.add_header("Content-Disposition", "attachment", filename=filename)
                    msg.attach(part)
            else:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["To"] = to_addr
                if cc_list:
                    msg["Cc"] = ", ".join(cc_list)
                msg.attach(MIMEText(html_body, "html"))

            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            service.users().messages().send(
                userId="me",
                body={"raw": raw},
            ).execute()

            cc_info = f" (Cc: {', '.join(cc_list)})" if cc_list else ""
            logger.info("[email-alert] Sent: %s → %s%s", subject, to_addr, cc_info)
            return None
        except Exception as e:
            logger.error("[email-alert] Failed to send email: %s", e, exc_info=True)
            return str(e)

    if wait:
        return _send()
    # Run as a background daemon thread to avoid blocking response handling
    threading.Thread(target=_send, daemon=True).start()
    return None


def send_quota_exceeded_alert(total_used: int) -> None:
    """Send a one-time email when the LLM quota is exceeded.

    Only sends once per process lifetime (resets on redeploy).
    """
    global _quota_alert_sent
    with _lock:
        if _quota_alert_sent:
            return
        _quota_alert_sent = True

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = "ScheMatiQ — LLM Usage Limit Reached"
    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #e67e22;">LLM Usage Limit Reached</h2>
        <p>The ScheMatiQ system has reached its API call limit and is no longer accepting new processing sessions.</p>
        <table style="border-collapse: collapse; margin: 16px 0;">
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Calls Used</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{total_used:,}</td>
            </tr>
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Limit</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{LLM_CALL_GLOBAL_LIMIT:,}</td>
            </tr>
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Time</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{now}</td>
            </tr>
        </table>
        <p style="color: #666; font-size: 14px;">
            To resume service, either increase <code>LLM_CALL_GLOBAL_LIMIT</code> in Railway
            environment variables, or reset the usage counter.
        </p>
    </div>
    """
    _send_email(subject, html_body)


def send_quota_warning_alert(used: int, limit: int) -> None:
    """Send a one-time email when LLM usage approaches the quota.

    Fires before the limit is reached (see LLM_CALL_WARN_THRESHOLD) so service
    can be extended before it blocks. Sent once per process lifetime.
    """
    global _quota_warning_sent
    with _lock:
        if _quota_warning_sent:
            return
        _quota_warning_sent = True

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pct = int(round(100 * used / limit)) if limit else 0
    subject = "ScheMatiQ — LLM Usage Approaching Limit"
    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #e67e22;">LLM Usage Approaching Limit</h2>
        <p>ScheMatiQ has used <strong>{pct}%</strong> of its API call quota.
           Service is still running, but will stop accepting new work once the
           limit is reached.</p>
        <table style="border-collapse: collapse; margin: 16px 0;">
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Calls Used</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{used:,}</td>
            </tr>
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Limit</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{limit:,}</td>
            </tr>
            <tr>
                <td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">Time</td>
                <td style="padding: 8px 16px; border: 1px solid #ddd;">{now}</td>
            </tr>
        </table>
        <p style="color: #666; font-size: 14px;">
            To avoid interruption, raise <code>LLM_CALL_GLOBAL_LIMIT</code> in Railway
            environment variables, or set a rolling window via
            <code>LLM_CALL_LIMIT_WINDOW_DAYS</code>.
        </p>
    </div>
    """
    _send_email(subject, html_body)


def send_issue_report(
    *,
    description: str,
    session_id: str = "",
    context_rows: Optional[list] = None,
    attachment_bytes: Optional[bytes] = None,
    attachment_name: str = "",
    attachment_subtype: str = "json",
    project_json_bytes: Optional[bytes] = None,
    project_json_name: str = "project.json",
    attachment_note: str = "",
    screenshots: Optional[List[Tuple[str, bytes, str]]] = None,
    cc: Optional[Union[List[str], str]] = None,
    wait: bool = False,
) -> Optional[str]:
    """Email a user-submitted issue report to SUPPORT_EMAIL_TO. Never raises.

    ``context_rows`` is a list of ``(label, value)`` pairs rendered as a table.
    The attachment is ``(attachment_name, attachment_bytes, attachment_subtype)``
    when provided (e.g. a project bundle .zip); otherwise it falls back to
    ``project_json_bytes`` as a .json for backward compatibility. Unlike the
    quota alerts, this sends on every call (no once-per-process guard).
    """
    short = (session_id or "")[:8]
    subject = f"ScheMatiQ — Issue report ({short or 'no session'})"

    safe_desc = html.escape(description or "").replace("\n", "<br>")
    rows_html = ""
    for label, value in context_rows or []:
        rows_html += (
            "<tr>"
            f'<td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">{html.escape(str(label))}</td>'
            f'<td style="padding: 8px 16px; border: 1px solid #ddd;">{html.escape(str(value))}</td>'
            "</tr>"
        )
    table_html = (
        f'<table style="border-collapse: collapse; margin: 16px 0;">{rows_html}</table>'
        if rows_html
        else ""
    )
    note_html = (
        f'<p style="color: #666; font-size: 14px;">{html.escape(attachment_note)}</p>'
        if attachment_note
        else ""
    )

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 640px; margin: 0 auto;">
        <h2 style="color: #2563eb;">New issue report</h2>
        <p style="white-space: pre-wrap;">{safe_desc}</p>
        {table_html}
        {note_html}
    </div>
    """

    attachment = None
    if attachment_bytes:
        attachment = (attachment_name or "attachment", attachment_bytes, attachment_subtype)
    elif project_json_bytes:
        attachment = (project_json_name, project_json_bytes, "json")

    all_attachments: List[Tuple[str, bytes, str]] = []
    if attachment is not None:
        all_attachments.append(attachment)
    if screenshots:
        all_attachments.extend(screenshots)

    # If CC is not explicitly passed, fall back to global SUPPORT_CC_EMAILS
    target_cc = cc if cc is not None else SUPPORT_CC_EMAILS

    return _send_email(
        subject,
        html_body,
        recipient=SUPPORT_EMAIL_TO,
        cc=target_cc,
        attachments=all_attachments,
        wait=wait,
    )