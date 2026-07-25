"""API endpoints for external reference documents attached to a session.

Reference documents are supplementary lookup material (e.g. a spreadsheet that
maps each judge to the president who appointed them). They are distinct from the
source documents that define observation units: a reference document never yields
rows, it is extra context the chat assistant (and, later, value extraction) may
consult. The extracted text is stored inline on the session.
"""

import logging
from typing import List

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from app.services import session_manager, websocket_manager
from app.services import reference_document_service as refsvc

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reference"])


class ReferenceDocumentInfo(BaseModel):
    """Reference document metadata (without the full text body)."""
    id: str
    filename: str
    char_count: int
    truncated: bool


class ReferenceDocumentList(BaseModel):
    session_id: str
    reference_documents: List[ReferenceDocumentInfo]


def _to_info(ref) -> ReferenceDocumentInfo:
    return ReferenceDocumentInfo(
        id=ref.id,
        filename=ref.filename,
        char_count=ref.char_count,
        truncated=ref.truncated,
    )


async def _broadcast_updated(session_id: str, session) -> None:
    try:
        await websocket_manager.broadcast_to_session(
            session_id,
            {
                "type": "reference_documents_updated",
                "data": {
                    "reference_documents": [
                        _to_info(r).model_dump()
                        for r in refsvc.list_reference_documents(session)
                    ]
                },
            },
        )
    except Exception:  # broadcasting is best-effort; never fail the request on it
        logger.debug("Failed to broadcast reference update for %s", session_id, exc_info=True)


@router.get("/{session_id}", response_model=ReferenceDocumentList)
async def list_reference_documents(session_id: str) -> ReferenceDocumentList:
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return ReferenceDocumentList(
        session_id=session_id,
        reference_documents=[_to_info(r) for r in refsvc.list_reference_documents(session)],
    )


@router.post("/{session_id}/upload", response_model=ReferenceDocumentInfo)
async def upload_reference_document(
    session_id: str, file: UploadFile = File(...)
) -> ReferenceDocumentInfo:
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    filename = file.filename or "reference"
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        ref = await refsvc.store_reference_document(session_id, filename, raw)
    except refsvc.UnsupportedReferenceFormat as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except refsvc.ReferenceExtractionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - unexpected conversion failure
        logger.error("Reference extraction failed for %s: %s", filename, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to process reference file") from exc

    refsvc.add_reference_document(session, ref)
    session_manager.update_session(session)
    await _broadcast_updated(session_id, session)
    logger.info(
        "Added reference document %s (%s, %d chars) to session %s",
        ref.id, filename, ref.char_count, session_id,
    )
    return _to_info(ref)


@router.delete("/{session_id}/{reference_id}")
async def delete_reference_document(session_id: str, reference_id: str) -> dict:
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    removed = refsvc.remove_reference_document(session, reference_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Reference document not found")
    await refsvc.delete_reference_storage(session_id, reference_id)
    session_manager.update_session(session)
    await _broadcast_updated(session_id, session)
    return {"status": "success", "removed": reference_id}
