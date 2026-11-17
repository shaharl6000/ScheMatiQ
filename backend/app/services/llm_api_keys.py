"""User-provided LLM API keys, kept in process memory only.

A request can carry the user's own provider key (``llm_config.api_key``,
``LLMConfig.api_key``). The session config files that the pipeline writes and
reads back (``config.json``, ``schematiq_config.json``,
``user_llm_config.json``, continue-discovery configs) must not contain it:
those are plaintext files on a shared server (CWE-312), and the import and
export paths copy their LLM backends around.

Rules:
- A writer calls ``remember_api_keys(session_id, cfg)`` and dumps
  ``strip_api_keys(cfg)``.
- Code that builds an LLM from a config read back from disk calls
  ``restore_api_keys(session_id, cfg)`` first.
- Configs that came from another user (an imported project file) are passed
  through ``strip_api_keys`` and never remembered.

Keys are lost on restart. A config without a key falls back to the server key
(env var), which is what already happens on a redeploy, since that wipes the
session directories.
"""

from __future__ import annotations

import copy
import threading
from typing import Any, Dict

API_KEY_FIELD = "api_key"

_lock = threading.Lock()
# session_id -> provider (lowercase) -> key
_keys: Dict[str, Dict[str, str]] = {}


def _iter_dicts(obj: Any):
    """Yield every dict inside ``obj`` (``obj`` included), depth first."""
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_dicts(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            yield from _iter_dicts(value)


def _provider_of(backend: Dict[str, Any]) -> str:
    provider = backend.get("provider")
    return provider.strip().lower() if isinstance(provider, str) else ""


def remember_api_keys(session_id: str, obj: Any) -> None:
    """Keep every ``api_key`` in ``obj`` for ``session_id``, by provider."""
    if not session_id:
        return
    found = {}
    for backend in _iter_dicts(obj):
        key = backend.get(API_KEY_FIELD)
        provider = _provider_of(backend)
        if provider and isinstance(key, str) and key.strip():
            found[provider] = key.strip()
    if found:
        with _lock:
            _keys.setdefault(session_id, {}).update(found)


def strip_api_keys(obj: Any) -> Any:
    """Return a deep copy of ``obj`` with every ``api_key`` field removed."""
    result = copy.deepcopy(obj)
    for backend in _iter_dicts(result):
        backend.pop(API_KEY_FIELD, None)
    return result


def restore_api_keys(session_id: str, obj: Any) -> Any:
    """Return a deep copy of ``obj`` with remembered keys put back.

    A backend dict (one with a ``provider``) that has no key gets the key
    remembered for that provider in this session, if any. A key that is
    already present is left as is.
    """
    result = copy.deepcopy(obj)
    if not session_id:
        return result
    with _lock:
        session_keys = dict(_keys.get(session_id, {}))
    if not session_keys:
        return result
    for backend in _iter_dicts(result):
        if backend.get(API_KEY_FIELD):
            continue
        key = session_keys.get(_provider_of(backend))
        if key:
            backend[API_KEY_FIELD] = key
    return result


def forget_api_keys(session_id: str) -> None:
    """Drop the keys remembered for ``session_id``."""
    with _lock:
        _keys.pop(session_id, None)
