"""
Observation unit management API endpoints.
Handles adding and removing observation units (rows) from extraction results.
"""

import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger(__name__)

from app.core.logging_utils import set_session_context
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


class RemoveBulkObservationUnitsRequest(BaseModel):
    """Request to remove multiple observation units."""
    unit_names: List[str] = Field(..., description="Names of the observation units to remove", min_length=1)


class RemoveBulkObservationUnitsResponse(BaseModel):
    """Response after bulk observation unit removal."""
    status: str
    message: str
    session_id: str
    deleted_count: int
    failed: List[str] = Field(default_factory=list, description="Units that failed to delete")


class UpdateObservationUnitDefinitionRequest(BaseModel):
    """Request to update the observation unit definition (schema-level)."""
    name: str = Field(..., min_length=1, max_length=100, description="Name of the observation unit type")
    definition: str = Field(..., min_length=10, max_length=500, description="Definition of what constitutes one row")
    example_names: Optional[List[str]] = Field(
        default=None,
        description="Example names of observation units",
        max_length=20
    )


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


class UpdateObservationUnitDefinitionResponse(BaseModel):
    """Response after updating observation unit definition."""
    status: str
    message: str
    observation_unit: dict
    warning: Optional[str] = None


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
    set_session_context(session_id)
    logger.info("Removing observation unit '%s' from session %s", request.unit_name, session_id)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    try:
        result = await observation_unit_manager.remove_observation_unit(
            session_id=session_id,
            unit_name=request.unit_name
        )
        logger.info("Successfully removed observation unit '%s' from session %s", request.unit_name, session_id)

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


@router.delete("/remove-bulk/{session_id}")
async def remove_bulk_observation_units(
    session_id: str,
    request: RemoveBulkObservationUnitsRequest
) -> RemoveBulkObservationUnitsResponse:
    """
    Remove multiple observation units from the session.

    This will:
    1. Remove each unit from the observation_units list in the session
    2. Remove the corresponding rows from the data table
    3. Update the data file
    4. Report any failures individually

    Args:
        session_id: Session identifier
        request: Contains unit_names list to remove

    Returns:
        RemoveBulkObservationUnitsResponse with deletion results

    Raises:
        HTTPException: If session not found
    """
    set_session_context(session_id)
    logger.info("Bulk removing %d observation units from session %s", len(request.unit_names), session_id)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    deleted_count = 0
    failed: List[str] = []

    for unit_name in request.unit_names:
        try:
            await observation_unit_manager.remove_observation_unit(
                session_id=session_id,
                unit_name=unit_name
            )
            deleted_count += 1
            logger.debug("Removed observation unit '%s' from session %s", unit_name, session_id)
        except ValueError as e:
            logger.warning("Failed to remove unit '%s': %s", unit_name, str(e))
            failed.append(unit_name)
        except Exception as e:
            logger.error("Error removing unit '%s': %s", unit_name, str(e))
            failed.append(unit_name)

    logger.info(
        "Bulk removal complete for session %s: %d deleted, %d failed",
        session_id, deleted_count, len(failed)
    )

    status = "success" if not failed else "partial" if deleted_count > 0 else "failed"
    message = f"Deleted {deleted_count} observation unit(s)"
    if failed:
        message += f". Failed to delete {len(failed)}: {', '.join(failed[:5])}"
        if len(failed) > 5:
            message += f" and {len(failed) - 5} more"

    return RemoveBulkObservationUnitsResponse(
        status=status,
        message=message,
        session_id=session_id,
        deleted_count=deleted_count,
        failed=failed
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
    set_session_context(session_id)
    logger.info("Adding observation unit '%s' to session %s", request.unit_name, session_id)
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
        logger.info("Successfully added observation unit '%s' to session %s", request.unit_name, session_id)

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
    set_session_context(session_id)
    logger.debug("Listing observation units for session %s", session_id)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    observation_units = observation_unit_manager.get_observation_units(session_id)
    logger.debug("Found %d observation units for session %s", len(observation_units), session_id)

    return {
        "session_id": session_id,
        "observation_units": observation_units,
        "count": len(observation_units)
    }


@router.patch("/definition/{session_id}")
async def update_observation_unit_definition(
    session_id: str,
    request: UpdateObservationUnitDefinitionRequest
) -> UpdateObservationUnitDefinitionResponse:
    """
    Update the observation unit definition (schema-level concept).

    This updates what constitutes a single row in the extracted table,
    not the individual row instances.

    Args:
        session_id: Session identifier
        request: Contains name, definition, and optional example_names

    Returns:
        UpdateObservationUnitDefinitionResponse with updated definition

    Raises:
        HTTPException: If session not found or validation fails
    """
    set_session_context(session_id)
    logger.info("Updating observation unit definition for session %s: name='%s'", session_id, request.name)
    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    # Validate example_names if provided
    if request.example_names:
        if len(request.example_names) > 20:
            raise HTTPException(
                status_code=400,
                detail="Maximum 20 example names allowed"
            )
        for name in request.example_names:
            if len(name) > 100:
                raise HTTPException(
                    status_code=400,
                    detail=f"Example name '{name[:50]}...' exceeds 100 character limit"
                )

    try:
        result = await observation_unit_manager.update_observation_unit_definition(
            session_id=session_id,
            name=request.name,
            definition=request.definition,
            example_names=request.example_names
        )
        logger.info("Successfully updated observation unit definition for session %s", session_id)

        return UpdateObservationUnitDefinitionResponse(
            status="success",
            message=f"Successfully updated observation unit definition to '{request.name}'",
            observation_unit=result["observation_unit"],
            warning="Existing data was extracted with the previous definition. Consider re-extraction if granularity changed."
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error updating observation unit definition: {str(e)}"
        )
