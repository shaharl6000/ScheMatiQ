"""Auth introspection.

Exists so the frontend (and a curl from a laptop) can confirm that a Supabase
access token is being issued, sent, and verified correctly — before ownership
enforcement is switched on and a misconfiguration would lock people out.
"""

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.core.auth import AuthenticatedUser, get_optional_user
from app.core.config import AUTH_ENFORCED

router = APIRouter(tags=["auth"])


class AuthStatus(BaseModel):
    authenticated: bool
    user_id: Optional[str] = None
    email: Optional[str] = None
    # Lets the frontend know whether ownership is being enforced yet, so it can
    # decide between prompting for sign-in and staying out of the way.
    enforced: bool


@router.get("/me", response_model=AuthStatus)
async def read_current_user(
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
) -> AuthStatus:
    """Report who the caller is, without requiring them to be anyone."""
    if user is None:
        return AuthStatus(authenticated=False, enforced=AUTH_ENFORCED)
    return AuthStatus(
        authenticated=True,
        user_id=user.id,
        email=user.email,
        enforced=AUTH_ENFORCED,
    )
