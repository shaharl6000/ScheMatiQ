"""
Observation unit management API endpoints.
Handles adding and removing observation units (rows) from extraction results.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

from app.services.session_manager import SessionManager
from app.services.observation_unit_manager import ObservationUnitManager
from app.services import session_manager, websocket_manager

router = APIRouter(tags=["observation-unit"])

# Create observation unit manager instance
observation_unit_manager = ObservationUnitManager(websocket_manager, session_manager)


# Request/Response Models
class RemoveObservationUnitRequest(BaseModel):
    """Request to remove an observation unit."""
    unit_name: str = Field(..., description="Name of the observation unit to remove")


class AddObservationUnitRequest(BaseModel):
    """Request to add a new observation unit."""
    unit_name: str = Field(..., description="Name of the observation unit to add")
    document_id: Optional[str] = Field(None, description="Optional document ID associated with this unit")
    relevant_passages: List[str] = Field(default_factory=list, description="Relevant text passages for this unit")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Confidence score for this unit")


class ObservationUnitResponse(BaseModel):
    """Response after observation unit operation."""
    status: str
    message: str
    session_id: str
    observation_units: List[dict]
    row_count: int


@router.delete("/remove/{session_id}")
async def remove_observation_unit(
    session_id: str,
    request: RemoveObservationUnitRequest
) -> ObservationUnitResponse:
    """
    Remove an observation unit from the session.

    This will:
    1. Remove the unit from the observation_units list in the session
    2. Remove the corresponding row from the data table
    3. Update the data file

    Args:
        session_id: Session identifier
        request: Contains unit_name to remove

    Returns:
        ObservationUnitResponse with updated state

    Raises:
        HTTPException: If session not found or unit doesn't exist
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    try:
        result = await observation_unit_manager.remove_observation_unit(
            session_id=session_id,
            unit_name=request.unit_name
        )

        return ObservationUnitResponse(
            status="success",
            message=f"Successfully removed observation unit '{request.unit_name}'",
            session_id=session_id,
            observation_units=result["observation_units"],
            row_count=result["row_count"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error removing observation unit: {str(e)}"
        )


@router.post("/add/{session_id}")
async def add_observation_unit(
    session_id: str,
    request: AddObservationUnitRequest,
    background_tasks: BackgroundTasks
) -> ObservationUnitResponse:
    """
    Add a new observation unit and optionally extract values for it.

    This will:
    1. Add the unit to the observation_units list
    2. Add a new row to the data table
    3. Optionally trigger value extraction for this unit (background task)

    Args:
        session_id: Session identifier
        request: Contains unit details (name, document_id, passages, confidence)
        background_tasks: FastAPI background tasks for async extraction

    Returns:
        ObservationUnitResponse with updated state

    Raises:
        HTTPException: If session not found or unit already exists
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    try:
        result = await observation_unit_manager.add_observation_unit(
            session_id=session_id,
            unit_name=request.unit_name,
            document_id=request.document_id,
            relevant_passages=request.relevant_passages,
            confidence=request.confidence
        )

        return ObservationUnitResponse(
            status="success",
            message=f"Successfully added observation unit '{request.unit_name}'",
            session_id=session_id,
            observation_units=result["observation_units"],
            row_count=result["row_count"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error adding observation unit: {str(e)}"
        )


@router.get("/list/{session_id}")
async def list_observation_units(session_id: str) -> dict:
    """
    List all observation units for a session.

    Args:
        session_id: Session identifier

    Returns:
        Dict with observation_units list and count

    Raises:
        HTTPException: If session not found
    """
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    observation_units = observation_unit_manager.get_observation_units(session_id)

    return {
        "session_id": session_id,
        "observation_units": observation_units,
        "count": len(observation_units)
    }
