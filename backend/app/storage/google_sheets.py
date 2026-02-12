"""Google Sheets summary logger for research data collection.

Appends one row per archived session to a summary spreadsheet.
Entirely optional — skipped if GOOGLE_SHEETS_SPREADSHEET_ID is not set.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_SHEETS_SPREADSHEET_ID,
    GOOGLE_SHEETS_LLM_USAGE_ID,
    GOOGLE_OAUTH_CREDENTIALS_JSON,
)

logger = logging.getLogger(__name__)

# Column headers for the summary sheet (must match append_row order)
# A-N: session data + feedback in a single row
HEADER_ROW = [
    "Timestamp",
    "Session ID",
    "Query",
    "Doc Count",
    "Column Count",
    "Row Count",
    "Completeness %",
    "Observation Unit",
    "Trigger Source",
    "Drive File ID",
    "Rating",
    "Comment",
    "LLM Calls",  # Total LLM API calls made during this session
    "Data Saved",  # Yes/No — whether session data was archived to Drive
]

# LLM usage tracking lives in a separate spreadsheet (GOOGLE_SHEETS_LLM_USAGE_ID)
LLM_USAGE_HEADER = ["Timestamp", "Session ID", "LLM Calls", "Per Stage"]


class GoogleSheetsLogger:
    """Singleton logger that appends summary rows to a Google Sheet."""

    _instance: Optional["GoogleSheetsLogger"] = None

    def __init__(self):
        self._service = None
        self._enabled = False
        self._init_service()

    @classmethod
    def get_instance(cls) -> Optional["GoogleSheetsLogger"]:
        """Return the singleton, or None if Sheets logging is not configured."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance if cls._instance._enabled else None

    def _init_service(self):
        """Build the Sheets API service from credentials."""
        if not GOOGLE_SHEETS_SPREADSHEET_ID:
            logger.debug("[data-collection] GOOGLE_SHEETS_SPREADSHEET_ID not set — Sheets logging disabled")
            return

        try:
            from googleapiclient.discovery import build

            credentials = self._build_credentials()
            if credentials is None:
                return

            self._service = build("sheets", "v4", credentials=credentials)
            self._enabled = True
            logger.info("[data-collection] Google Sheets logger initialized")
        except ImportError:
            logger.debug("[data-collection] google-api-python-client not installed — Sheets logging disabled")
        except Exception as e:
            logger.error("[data-collection] Failed to init Google Sheets: %s", e)

    def _build_credentials(self):
        """Try OAuth2 user credentials first, then service account."""
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

        # Option 1: OAuth2 user credentials (works with personal Gmail)
        if GOOGLE_OAUTH_CREDENTIALS_JSON:
            try:
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
                logger.info("[data-collection] Sheets using OAuth2 user credentials")
                return credentials
            except Exception as e:
                logger.error("[data-collection] Failed to load OAuth2 credentials for Sheets: %s", e)

        # Option 2: Service account (Google Workspace only)
        if GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE:
            try:
                from google.oauth2 import service_account
                if GOOGLE_SERVICE_ACCOUNT_JSON:
                    cleaned = re.sub(r"[\n\r\t]+\s*", "", GOOGLE_SERVICE_ACCOUNT_JSON)
                    info = json.loads(cleaned)
                    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
                else:
                    return service_account.Credentials.from_service_account_file(
                        GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
                    )
            except Exception as e:
                logger.error("[data-collection] Failed to load service account credentials for Sheets: %s", e)

        logger.debug("[data-collection] No credentials for Sheets")
        return None

    def append_row(self, values: List[Any]) -> bool:
        """Append a single row to the summary sheet.

        Returns True on success, False on failure.
        """
        if not self._enabled or not self._service:
            return False

        try:
            body = {"values": [values]}
            self._service.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                range="Sheet1!A:N",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            logger.debug("[data-collection] Appended summary row to Google Sheet")
            return True
        except Exception as e:
            logger.error("[data-collection] Sheets append failed: %s", e)
            return False

    def log_session(
        self,
        session_id: str,
        query: str,
        doc_count: int,
        column_count: int,
        row_count: int,
        completeness: float,
        observation_unit: str,
        trigger_source: str,
        drive_file_id: Optional[str],
        llm_calls: int = 0,
        data_saved: bool = True,
    ) -> bool:
        """Log a session summary row with default Rating=N/A."""
        return self.append_row([
            datetime.now(timezone.utc).isoformat(),
            session_id,
            query[:200],  # Truncate long queries
            doc_count,
            column_count,
            row_count,
            f"{completeness:.1f}",
            observation_unit,
            trigger_source,
            drive_file_id or "",
            "N/A",  # Rating — updated later if user submits feedback
            "",     # Comment
            llm_calls,
            "Yes" if data_saved else "No",
        ])

    def _ensure_llm_usage_headers(self) -> bool:
        """Write headers to the LLM usage spreadsheet if row 1 is empty."""
        if not GOOGLE_SHEETS_LLM_USAGE_ID:
            return False
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEETS_LLM_USAGE_ID,
                range="Sheet1!A1:D1",
            ).execute()
            if not result.get("values"):
                self._service.spreadsheets().values().update(
                    spreadsheetId=GOOGLE_SHEETS_LLM_USAGE_ID,
                    range="Sheet1!A1:D1",
                    valueInputOption="USER_ENTERED",
                    body={"values": [LLM_USAGE_HEADER]},
                ).execute()
                logger.info("[llm-usage] Wrote headers to LLM usage spreadsheet")
            return True
        except Exception as e:
            logger.error("[llm-usage] Failed to ensure headers: %s", e)
            return False

    def log_llm_usage(self, session_id: str, llm_calls: int, per_stage: Optional[Dict] = None) -> bool:
        """Append a row to the separate LLM usage spreadsheet.

        Args:
            session_id: Session that generated the calls.
            llm_calls: Total LLM API calls in this session.
            per_stage: Optional per-stage breakdown dict.

        Returns True on success.
        """
        if not self._enabled or not self._service or not GOOGLE_SHEETS_LLM_USAGE_ID:
            return False

        self._ensure_llm_usage_headers()

        try:
            stage_str = json.dumps(per_stage) if per_stage else ""
            body = {"values": [[
                datetime.now(timezone.utc).isoformat(),
                session_id,
                llm_calls,
                stage_str,
            ]]}
            self._service.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEETS_LLM_USAGE_ID,
                range="Sheet1!A:D",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            logger.debug("[llm-usage] Logged %d LLM calls for session %s", llm_calls, session_id[:8])
            return True
        except Exception as e:
            logger.error("[llm-usage] Failed to log LLM usage: %s", e)
            return False

    def read_total_llm_calls(self) -> int:
        """Read the sum of all LLM calls from the LLM usage spreadsheet (column C).

        Returns 0 if the spreadsheet is empty or not configured.
        """
        if not self._enabled or not self._service or not GOOGLE_SHEETS_LLM_USAGE_ID:
            return 0

        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEETS_LLM_USAGE_ID,
                range="Sheet1!C:C",
            ).execute()
            values = result.get("values", [])
            total = 0
            for row in values[1:]:  # skip header row
                if row and row[0]:
                    try:
                        total += int(row[0])
                    except (ValueError, TypeError):
                        pass
            logger.debug("[llm-usage] Read total LLM calls: %d", total)
            return total
        except Exception as e:
            logger.debug("[llm-usage] Could not read LLM usage spreadsheet: %s", e)
            return 0

    def log_feedback(
        self,
        session_id: str,
        rating: str,
        comment: Optional[str],
        table_row_count: int,
        table_column_count: int,
    ) -> bool:
        """Update the Rating + Comment columns in the session's existing row.

        Finds the row by session_id in column B, then updates columns K-L.
        Falls back to appending a new row if the session row doesn't exist yet.
        Returns True on success, False on failure.
        """
        if not self._enabled or not self._service:
            return False

        try:
            # Read column B (Session ID) to find the row
            result = self._service.spreadsheets().values().get(
                spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                range="Sheet1!B:B",
            ).execute()
            values = result.get("values", [])

            # Only search last 10 rows for efficiency
            search_start = max(0, len(values) - 10)
            row_index = None
            for i in range(len(values) - 1, search_start - 1, -1):
                if values[i] and values[i][0] == session_id:
                    row_index = i + 1  # Sheets rows are 1-based
                    break

            if row_index:
                # Update Rating (K) + Comment (L) in the existing row
                self._service.spreadsheets().values().update(
                    spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                    range=f"Sheet1!K{row_index}:L{row_index}",
                    valueInputOption="USER_ENTERED",
                    body={"values": [[rating, comment or ""]]},
                ).execute()
                logger.debug("[data-collection] Updated feedback in row %d for session %s", row_index, session_id[:8])
            else:
                # Fallback: session row not yet written, append a minimal row
                self._service.spreadsheets().values().append(
                    spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                    range="Sheet1!A:N",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": [[
                        datetime.now(timezone.utc).isoformat(),
                        session_id, "", "", "", "", "", "", "feedback_only", "",
                        rating, comment or "", "", "",
                    ]]},
                ).execute()
                logger.debug("[data-collection] Appended feedback-only row for session %s", session_id[:8])

            return True
        except Exception as e:
            logger.error("[data-collection] Sheets feedback update failed: %s", e)
            return False
