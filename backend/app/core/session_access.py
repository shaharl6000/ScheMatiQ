"""Ownership of workspace sessions.

Applied as a router-level dependency, so one piece of code covers every
session-scoped route instead of ~70 handlers each remembering to check. FastAPI
resolves ``session_id`` from the path for routes that declare it and passes None
for routes that don't, which makes the dependency safe to attach wholesale.

Rollout is deliberately two-phase, because turning ownership on in one step on a
live deployment locks real users out of their own projects:

  Phase 1 — AUTH_ENFORCED off (the default). Nothing is denied. But an
  authenticated caller reaching an unowned session *claims* it. Real users
  signing in therefore backfill ownership of their own projects just by using
  the app, with no manual mapping and no downtime.

  Phase 2 — AUTH_ENFORCED on. Ownership is required. By then the sessions people
  actually use already have owners from phase 1.

AUTH_LEGACY_SESSION_POLICY decides what happens in phase 2 to sessions that
still have no owner, which is the part worth thinking about before flipping the
switch. See the constant's docstring in config.py.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import Depends, HTTPException

from app.core.auth import AuthenticatedUser, get_optional_user
from app.core.config import AUTH_ENFORCED, AUTH_LEGACY_SESSION_POLICY
from app.services import session_manager

logger = logging.getLogger(__name__)

# Returned instead of 403 when a caller asks for someone else's session. A 403
# would confirm that the id exists, which is exactly the enumeration signal the
# session listing endpoints used to hand out. 404 keeps "not yours" and "not
# real" indistinguishable.
_NOT_FOUND = HTTPException(status_code=404, detail="Session not found.")


async def _claim(session, user: AuthenticatedUser) -> None:
    """Record this user as the owner of a previously unowned session.

    update_session writes through to storage synchronously, so it goes to a
    thread. Best effort: a failed claim must not break the request. The next
    access simply tries again.
    """
    try:
        session.owner_id = user.id
        await asyncio.to_thread(session_manager.update_session, session)
        logger.info(
            "session %s claimed by user %s", session.id, user.short_id
        )
    except Exception as exc:
        logger.warning("could not claim session %s: %s", session.id, exc)


async def require_session_access(
    session_id: Optional[str] = None,
    user: Optional[AuthenticatedUser] = Depends(get_optional_user),
) -> None:
    """Enforce (or, in phase 1, establish) ownership of a session.

    Silent and permissive by design when there is nothing to decide: routes with
    no session in the path, and sessions that don't exist, are left to the route
    to handle so error semantics stay where they already are.
    """
    if session_id is None:
        # Not a session-scoped route. Note that for such routes FastAPI would
        # read `session_id` as a query parameter, so a caller can pass one — it
        # is only ever used to check access, never to select data, so a bogus
        # value can at worst produce a 404 on a route that ignores it.
        return

    session = session_manager.get_session(session_id)
    if session is None:
        # Let the route produce its own 404 (chat already does), and keep this
        # dependency from becoming a second, subtly different not-found path.
        return

    owner_id = getattr(session, "owner_id", None)

    if owner_id is None:
        if user is not None:
            await _claim(session, user)
            return
        # Unowned and anonymous.
        if AUTH_ENFORCED and AUTH_LEGACY_SESSION_POLICY == "deny":
            raise _NOT_FOUND
        if AUTH_ENFORCED:
            logger.warning(
                "anonymous access to unowned session %s allowed by "
                "AUTH_LEGACY_SESSION_POLICY=%s",
                session_id,
                AUTH_LEGACY_SESSION_POLICY,
            )
        return

    if not AUTH_ENFORCED:
        # Ownership is recorded but not yet load-bearing.
        return

    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if owner_id != user.id:
        logger.warning(
            "user %s denied access to session %s owned by %s",
            user.short_id,
            session_id,
            owner_id[:8],
        )
        raise _NOT_FOUND
