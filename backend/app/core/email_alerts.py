"""Lightweight email alerts for quota and system events.

Sends via Gmail API using the same Google OAuth credentials already
configured for Google Sheets (GOOGLE_OAUTH_CREDENTIALS_JSON).
No extra passwords or SMTP config needed.

Disabled when credentials or ALERT_EMAIL_TO are not set — never raises.
"""

import base64
import json
import logging
import re
import threading
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.core.config import (
    ALERT_EMAIL_TO,
    GOOGLE_OAUTH_CREDENTIALS_JSON,
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    LLM_CALL_GLOBAL_LIMIT,
)

logger = logging.getLogger(__name__)

# Track whether we already sent the quota alert (avoid spamming)
_quota_alert_sent = False
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
            return build("gmail", "v1", credentials=credentials)

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
            return build("gmail", "v1", credentials=credentials)

    except Exception as e:
        logger.debug("[email-alert] Could not build Gmail service: %s", e)
    return None


def _send_email(subject: str, html_body: str) -> None:
    """Send an email via Gmail API in a background thread. Never raises."""
    if not ALERT_EMAIL_TO:
        logger.debug("[email-alert] ALERT_EMAIL_TO not set — skipping")
        return

    def _send():
        try:
            service = _build_gmail_service()
            if not service:
                logger.debug("[email-alert] Gmail service not available — skipping")
                return

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["To"] = ALERT_EMAIL_TO
            msg.attach(MIMEText(html_body, "html"))

            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            service.users().messages().send(
                userId="me",
                body={"raw": raw},
            ).execute()

            logger.info("[email-alert] Sent: %s → %s", subject, ALERT_EMAIL_TO)
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
