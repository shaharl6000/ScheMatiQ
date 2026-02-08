"""Google Drive upload utility for research data collection.

Uploads session archives to a shared Google Drive folder.
Supports two auth modes:
  1. Service account (Google Workspace only) — set GOOGLE_SERVICE_ACCOUNT_JSON/FILE
  2. OAuth2 user credentials (personal Gmail) — set GOOGLE_OAUTH_CREDENTIALS_JSON
     (run `python -m app.storage.google_drive` once to generate the credentials)

All errors are caught internally — callers never see exceptions.
"""

import json
import logging
import os
from io import BytesIO
from typing import Optional

from app.core.config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_DRIVE_FOLDER_ID,
)

logger = logging.getLogger(__name__)

# OAuth2 user credentials (JSON string with token, refresh_token, client_id, client_secret)
GOOGLE_OAUTH_CREDENTIALS_JSON = os.environ.get("GOOGLE_OAUTH_CREDENTIALS_JSON", "")


class GoogleDriveUploader:
    """Singleton uploader that authenticates via service account or OAuth2."""

    _instance: Optional["GoogleDriveUploader"] = None

    def __init__(self):
        self._service = None
        self._enabled = False
        self._init_service()

    @classmethod
    def get_instance(cls) -> Optional["GoogleDriveUploader"]:
        """Return the singleton, or None if Drive is not configured."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance if cls._instance._enabled else None

    def _init_service(self):
        """Build the Drive API service from credentials."""
        try:
            from googleapiclient.discovery import build

            credentials = self._build_credentials()
            if credentials is None:
                return

            self._service = build("drive", "v3", credentials=credentials)
            self._enabled = True
            logger.info("[data-collection] Google Drive uploader initialized")
        except ImportError:
            logger.debug("[data-collection] google-api-python-client not installed — Drive upload disabled")
        except Exception as e:
            logger.error("[data-collection] Failed to init Google Drive: %s", e)

    def _build_credentials(self):
        """Try OAuth2 user credentials first, then service account."""
        # Option 1: OAuth2 user credentials (works with personal Gmail)
        if GOOGLE_OAUTH_CREDENTIALS_JSON:
            try:
                from google.oauth2.credentials import Credentials
                creds_data = json.loads(GOOGLE_OAUTH_CREDENTIALS_JSON)
                credentials = Credentials(
                    token=creds_data.get("token"),
                    refresh_token=creds_data["refresh_token"],
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=creds_data["client_id"],
                    client_secret=creds_data["client_secret"],
                    scopes=["https://www.googleapis.com/auth/drive.file"],
                )
                logger.info("[data-collection] Using OAuth2 user credentials")
                return credentials
            except Exception as e:
                logger.error("[data-collection] Failed to load OAuth2 credentials: %s", e)

        # Option 2: Service account (Google Workspace only)
        if GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE:
            try:
                from google.oauth2 import service_account
                SCOPES = ["https://www.googleapis.com/auth/drive.file"]

                if GOOGLE_SERVICE_ACCOUNT_JSON:
                    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
                    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
                else:
                    return service_account.Credentials.from_service_account_file(
                        GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
                    )
            except Exception as e:
                logger.error("[data-collection] Failed to load service account credentials: %s", e)

        logger.debug("[data-collection] No Google credentials configured")
        return None

    def upload_file(
        self,
        filename: str,
        data_bytes: bytes,
        mime_type: str = "application/zip",
    ) -> Optional[str]:
        """Upload a file to the configured Drive folder.

        Returns the file ID on success, None on failure.
        """
        if not self._enabled or not self._service:
            return None

        try:
            from googleapiclient.http import MediaIoBaseUpload

            media = MediaIoBaseUpload(
                BytesIO(data_bytes), mimetype=mime_type, resumable=True
            )
            file_metadata = {"name": filename}
            if GOOGLE_DRIVE_FOLDER_ID:
                file_metadata["parents"] = [GOOGLE_DRIVE_FOLDER_ID]

            result = (
                self._service.files()
                .create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )
            file_id = result.get("id")
            logger.info("[data-collection] Uploaded %s → Drive file ID %s", filename, file_id)
            return file_id
        except Exception as e:
            logger.error("[data-collection] Drive upload failed for %s: %s", filename, e, exc_info=True)
            return None


# ── CLI: one-time OAuth2 setup for personal Gmail accounts ──────
if __name__ == "__main__":
    """Run this once locally to generate OAuth2 credentials for Railway.

    Prerequisites:
      1. Go to GCP Console → APIs & Services → Credentials
      2. Create an OAuth 2.0 Client ID (Desktop app type)
      3. Download the client secrets JSON file

    Usage:
      python -m app.storage.google_drive <path-to-client-secrets.json>

    This opens a browser for Google login, then prints the credentials JSON
    to paste into GOOGLE_OAUTH_CREDENTIALS_JSON on Railway.
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.storage.google_drive <client_secrets.json>")
        print()
        print("Steps:")
        print("  1. GCP Console → APIs & Services → Credentials")
        print("  2. Create OAuth 2.0 Client ID (Desktop app)")
        print("  3. Download the JSON file")
        print("  4. Run this command with the downloaded file path")
        sys.exit(1)

    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_secrets_file(
        sys.argv[1],
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    credentials = flow.run_local_server(port=0)

    creds_json = json.dumps({
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
    })

    print()
    print("=== Set this as GOOGLE_OAUTH_CREDENTIALS_JSON in Railway ===")
    print(creds_json)
    print()
    print("Done! You can now delete the client_secrets.json file.")
