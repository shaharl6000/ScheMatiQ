"""Lightweight email alerts for quota and system events.

Sends via Gmail API using the same Google OAuth credentials already
configured for Google Sheets (GOOGLE_OAUTH_CREDENTIALS_JSON).
No extra passwords or SMTP config needed.

Disabled when credentials or ALERT_EMAIL_TO are not set — never raises.
"""

import base64
import html
import json
import logging
import re
import threading
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import Optional, Tuple

from app.core.config import (
    ALERT_EMAIL_TO,
    SUPPORT_EMAIL_TO,
    GOOGLE_OAUTH_CREDENTIALS_JSON,
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    LLM_CALL_GLOBAL_LIMIT,
)

logger = logging.getLogger(__name__)

# Track whether we already sent the quota alert (avoid spamming)
_quota_alert_sent = False
_quota_warning_sent = False
_lock = threading.Lock()


def _build_gmail_service():
    """Build Gmail API service from existing Google OAuth credentials."""
    try:
        from googleapiclient.discovery import build

        SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

        # Option 1: OAuth2 user credentials (same as Google Sheets)
        if GOOGLE_OAUTH_CREDENTIALS_JSON:
            from google.oauth2.credentials import Credentials
            cleaned = re.sub(r"[\n\r\t]+\s*", "", GOOGLE_OAUTH_CREDENTIALS_JSON)
            creds_data = json.loads(cleaned)
            credentials = Credentials(
                token=creds_data.get("token"),
                refresh_token=creds_data["refresh_token"],
                token_uri="https://oauth2.googleapis.com/token",
                client_id=creds_data["client_id"],
                client_secret=creds_data["client_secret"],
                scopes=SCOPES,
            )
            return build("gmail", "v1", credentials=credentials, cache_discovery=False)

        # Option 2: Service account (needs domain-wide delegation for Gmail)
        if GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE:
            from google.oauth2 import service_account
            if GOOGLE_SERVICE_ACCOUNT_JSON:
                cleaned = re.sub(r"[\n\r\t]+\s*", "", GOOGLE_SERVICE_ACCOUNT_JSON)
                info = json.loads(cleaned)
                credentials = service_account.Credentials.from_service_account_info(
                    info, scopes=SCOPES
                )
            else:
                credentials = service_account.Credentials.from_service_account_file(
                    GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
                )
            return build("gmail", "v1", credentials=credentials, cache_discovery=False)

    except Exception as e:
        logger.debug("[email-alert] Could not build Gmail service: %s", e)
    return None


def _send_email(
    subject: str,
    html_body: str,
    recipient: Optional[str] = None,
    attachment: Optional[Tuple[str, bytes, str]] = None,
) -> None:
    """Send an email via Gmail API in a background thread. Never raises.

    ``recipient`` overrides the default ``ALERT_EMAIL_TO``. ``attachment`` is an
    optional ``(filename, content_bytes, mime_subtype)`` tuple; when given, the
    message becomes a ``mixed`` multipart carrying the HTML body plus the file.
    """
    to_addr = recipient if recipient is not None else ALERT_EMAIL_TO
    if not to_addr:
        logger.debug("[email-alert] No recipient configured — skipping")
        return

    def _send():
        try:
            service = _build_gmail_service()
            if not service:
                logger.debug("[email-alert] Gmail service not available — skipping")
                return

            if attachment is not None:
                filename, content, mime_subtype = attachment
                msg = MIMEMultipart("mixed")
                msg["Subject"] = subject
                msg["To"] = to_addr
                body = MIMEMultipart("alternative")
                body.attach(MIMEText(html_body, "html"))
                msg.attach(body)
                part = MIMEApplication(content, _subtype=mime_subtype)
                part.add_header(
                    "Content-Disposition", "attachment", filename=filename
                )
                msg.attach(part)
            else:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["To"] = to_addr
                msg.attach(MIMEText(html_body, "html"))

            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            service.users().messages().send(
                userId="me",
                body={"raw": raw},
            ).execute()

            logger.info("[email-alert] Sent: %s → %s", subject, to_addr)
        except Exception as e:
            logger.error("[email-alert] Failed to send email: %s", e)

    threading.Thread(target=_send, daemon=True).start()


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
    project_json_bytes: Optional[bytes] = None,
    project_json_name: str = "project.json",
    attachment_note: str = "",
) -> None:
    """Email a user-submitted issue report to SUPPORT_EMAIL_TO. Never raises.

    ``context_rows`` is a list of ``(label, value)`` pairs rendered as a table.
    ``project_json_bytes`` is attached as a .json file when provided. Unlike the
    quota alerts, this sends on every call (no once-per-process guard).
    """
    short = (session_id or "")[:8]
    subject = f"ScheMatiQ — Issue report ({short or 'no session'})"

    safe_desc = html.escape(description or "").replace("\n", "<br>")
    rows_html = ""
    for label, value in (context_rows or []):
        rows_html += (
            '<tr>'
            f'<td style="padding: 8px 16px; border: 1px solid #ddd; font-weight: bold;">{html.escape(str(label))}</td>'
            f'<td style="padding: 8px 16px; border: 1px solid #ddd;">{html.escape(str(value))}</td>'
            '</tr>'
        )
    table_html = (
        f'<table style="border-collapse: collapse; margin: 16px 0;">{rows_html}</table>'
        if rows_html else ""
    )
    note_html = (
        f'<p style="color: #666; font-size: 14px;">{html.escape(attachment_note)}</p>'
        if attachment_note else ""
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
    if project_json_bytes:
        attachment = (project_json_name, project_json_bytes, "json")

    _send_email(subject, html_body, recipient=SUPPORT_EMAIL_TO, attachment=attachment)
