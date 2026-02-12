"""Lightweight email alerts for quota and system events.

Sends via SMTP (e.g. Gmail with App Password).  Disabled when env vars
are not configured — never raises, only logs.
"""

import logging
import smtplib
import threading
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.core.config import (
    ALERT_EMAIL_TO,
    ALERT_EMAIL_FROM,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_PASSWORD,
    LLM_CALL_GLOBAL_LIMIT,
)

logger = logging.getLogger(__name__)

_EMAIL_ENABLED = bool(ALERT_EMAIL_TO and ALERT_EMAIL_FROM and SMTP_PASSWORD)

# Track whether we already sent the quota alert (avoid spamming)
_quota_alert_sent = False
_lock = threading.Lock()


def _send_email(subject: str, html_body: str) -> None:
    """Send an email in a background thread. Never raises."""
    if not _EMAIL_ENABLED:
        logger.debug("[email-alert] Email not configured — skipping")
        return

    def _send():
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = ALERT_EMAIL_FROM
            msg["To"] = ALERT_EMAIL_TO
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(ALERT_EMAIL_FROM, SMTP_PASSWORD)
                server.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO.split(","), msg.as_string())

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
    subject = "QueryDiscovery — LLM Usage Limit Reached"
    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #e67e22;">LLM Usage Limit Reached</h2>
        <p>The QueryDiscovery system has reached its API call limit and is no longer accepting new processing sessions.</p>
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

