"""Feedback endpoint for collecting user ratings on extracted tables.

Only active in release mode (DEVELOPER_MODE=false). Logs feedback to Google Sheets.
"""

import asyncio
import functools
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import DEVELOPER_MODE
from app.services import qbsd_thread_pool

logger = logging.getLogger(__name__)

router = APIRouter()


class TableFeedbackRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=200)
    rating: str = Field(..., pattern="^(positive|negative)$")
    comment: Optional[str] = Field(None, max_length=1000)
    table_row_count: int = Field(..., ge=0)
    table_column_count: int = Field(..., ge=0)


@router.post("/table", summary="Submit table quality feedback")
async def submit_table_feedback(request: TableFeedbackRequest):
    """Collect user feedback on extracted table quality.

    Only available in release mode. Feedback is logged to a Google Sheet
    for research purposes. Fire-and-forget: always returns 200.
    """
    if DEVELOPER_MODE:
        raise HTTPException(status_code=404, detail="Not found")

    try:
        from app.storage.google_sheets import GoogleSheetsLogger

        sheets_logger = GoogleSheetsLogger.get_instance()
        if sheets_logger:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                qbsd_thread_pool,
                functools.partial(
                    sheets_logger.log_feedback,
                    session_id=request.session_id,
                    rating=request.rating,
                    comment=request.comment,
                    table_row_count=request.table_row_count,
                    table_column_count=request.table_column_count,
                ),
            )
            logger.info("[feedback] Logged feedback for session %s: %s", request.session_id, request.rating)
        else:
            logger.debug("[feedback] Google Sheets not configured — feedback not logged")
    except Exception as e:
        logger.error("[feedback] Failed to log feedback: %s", e)

    return {"status": "ok"}
