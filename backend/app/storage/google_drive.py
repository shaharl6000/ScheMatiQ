"""Google Drive upload utility for research data collection.

Uploads session archives to a shared Google Drive folder using a service account.
All errors are caught internally — callers never see exceptions.
"""

import json
import logging
from io import BytesIO
from typing import Optional

from app.core.config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_DRIVE_FOLDER_ID,
)

logger = logging.getLogger(__name__)


class GoogleDriveUploader:
    """Singleton uploader that authenticates via service account."""

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
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            SCOPES = ["https://www.googleapis.com/auth/drive.file"]

            if GOOGLE_SERVICE_ACCOUNT_JSON:
                info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
                credentials = service_account.Credentials.from_service_account_info(
                    info, scopes=SCOPES
                )
            elif GOOGLE_SERVICE_ACCOUNT_FILE:
                credentials = service_account.Credentials.from_service_account_file(
                    GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
                )
            else:
                logger.debug("[data-collection] No Google credentials configured")
                return

            self._service = build("drive", "v3", credentials=credentials)
            self._enabled = True
            logger.info("[data-collection] Google Drive uploader initialized")
        except ImportError:
            logger.debug("[data-collection] google-api-python-client not installed — Drive upload disabled")
        except Exception as e:
            logger.error("[data-collection] Failed to init Google Drive: %s", e)

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
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            file_id = result.get("id")
            logger.info("[data-collection] Uploaded %s → Drive file ID %s", filename, file_id)
            return file_id
        except Exception as e:
            logger.error("[data-collection] Drive upload failed for %s: %s", filename, e, exc_info=True)
            return None
