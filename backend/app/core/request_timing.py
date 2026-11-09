"""Logs slow requests, so a browser "timeout of 30000ms exceeded" can be traced
to the request behind it. The access log is off, so nothing else records them."""

import logging
import os
import time

from fastapi import Request

logger = logging.getLogger(__name__)

# 0 turns the logging off.
SLOW_REQUEST_SECONDS = float(os.getenv("SCHEMATIQ_SLOW_REQUEST_SECONDS", "10"))


async def log_slow_requests(request: Request, call_next):
    started = time.monotonic()
    outcome = "failed"
    try:
        response = await call_next(request)
        outcome = response.status_code
        return response
    finally:
        elapsed = time.monotonic() - started
        if 0 < SLOW_REQUEST_SECONDS <= elapsed:
            logger.warning(
                "Slow request: %s %s -> %s in %.1fs",
                request.method, request.url.path, outcome, elapsed,
            )
