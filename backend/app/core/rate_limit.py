"""Per-client rate limits for endpoints that start LLM work.

The deployment has no user accounts, and when a session has no user API key
the pipeline runs on the server's own key. The only cap on that spend is the
global LLM quota (schematiq_runner), so a single client could exhaust it and
lock every other user out (CWE-770, OWASP API4:2023). These limits bound how
often one client can start LLM work.

Two buckets, both keyed by client IP:
  * "runs": pipeline starts (run, resume, re-extract, discovery, fill column)
  * "chat": chat messages and confirmed chat actions

Client IP: behind a reverse proxy, ``request.client.host`` is the proxy.
Set ``RATE_LIMIT_TRUSTED_PROXY_HOPS`` to the number of trusted proxies in
front of the app (1 on Railway). The client IP is then read at that position
from the RIGHT of ``X-Forwarded-For``, the entries the proxies appended. The
leftmost entries are client-controlled and are never used (CWE-348).

Storage is in-process memory, so limits are per worker. The backend runs one
uvicorn worker; with more workers, switch ``limits`` to a shared storage.
"""

from __future__ import annotations

import logging
import math
import os
import time

from fastapi import HTTPException, Request
from limits import RateLimitItem, parse
from limits.storage import MemoryStorage
from limits.strategies import MovingWindowRateLimiter

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes"}


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, default)))
    except ValueError:
        logger.warning("Invalid %s; using %d", name, default)
        return default


RATE_LIMIT_ENABLED = _env_flag("RATE_LIMIT_ENABLED", True)
TRUSTED_PROXY_HOPS = _env_int("RATE_LIMIT_TRUSTED_PROXY_HOPS", 0)

_RUNS_LIMIT: RateLimitItem = parse(os.environ.get("RATE_LIMIT_RUNS", "30/hour"))
_CHAT_LIMIT: RateLimitItem = parse(os.environ.get("RATE_LIMIT_CHAT", "120/hour"))

_limiter = MovingWindowRateLimiter(MemoryStorage())
_warned_unconfigured_proxy = False


def client_ip(request: Request) -> str:
    """Return the client IP, trusting only the configured proxy hops."""
    global _warned_unconfigured_proxy
    forwarded = request.headers.get("x-forwarded-for", "")
    if TRUSTED_PROXY_HOPS > 0 and forwarded:
        chain = [part.strip() for part in forwarded.split(",") if part.strip()]
        if len(chain) >= TRUSTED_PROXY_HOPS:
            return chain[-TRUSTED_PROXY_HOPS]
    elif forwarded and not _warned_unconfigured_proxy:
        _warned_unconfigured_proxy = True
        logger.warning(
            "Requests carry X-Forwarded-For but RATE_LIMIT_TRUSTED_PROXY_HOPS=0, "
            "so all clients may share the proxy's rate-limit bucket. "
            "Set RATE_LIMIT_TRUSTED_PROXY_HOPS to the number of trusted proxies."
        )
    return request.client.host if request.client else "unknown"


def _check(request: Request, bucket: str, item: RateLimitItem) -> None:
    if not RATE_LIMIT_ENABLED:
        return
    ip = client_ip(request)
    if _limiter.hit(item, bucket, ip):
        return
    reset_at, _ = _limiter.get_window_stats(item, bucket, ip)
    retry_after = max(1, math.ceil(reset_at - time.time()))
    logger.warning("Rate limit hit: bucket=%s ip=%s retry_after=%ss", bucket, ip, retry_after)
    raise HTTPException(
        status_code=429,
        detail=(
            "Too many requests from your network. "
            f"Please wait about {max(1, math.ceil(retry_after / 60))} minute(s) and try again."
        ),
        headers={"Retry-After": str(retry_after)},
    )


async def limit_llm_runs(request: Request) -> None:
    """Dependency for endpoints that start pipeline LLM work."""
    _check(request, "runs", _RUNS_LIMIT)


async def limit_chat(request: Request) -> None:
    """Dependency for chat endpoints that call the LLM."""
    _check(request, "chat", _CHAT_LIMIT)


def reset_for_tests() -> None:
    """Clear all counters (tests only)."""
    _limiter.storage.reset()
