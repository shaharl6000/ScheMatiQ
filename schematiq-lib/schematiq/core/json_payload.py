"""Find the JSON payload in an LLM response.

One helper for every parser that reads JSON from model output (schema
discovery, observation-unit discovery, column completion, unit
identification, value extraction). Parsing itself is ``json.loads``.
"""

import json
import re

# A Markdown code fence with an optional language tag (```json, ```JSON, ```).
_CODE_FENCE = re.compile(r"```[A-Za-z]*\s*(.*?)\s*```", re.DOTALL)


def _is_json(text: str) -> bool:
    try:
        json.loads(text)
    except ValueError:
        return False
    return True


def extract_json_payload(text: str) -> str:
    """Return the JSON payload in an LLM response, as text.

    Controlled generation returns bare JSON, which is returned unchanged.
    For free-text responses:

    1. The contents of the first Markdown code fence, if there is one.
    2. Otherwise the span from the first '{' to the last '}', which drops
       prose before or after the object.
    3. If that is not valid JSON, '{{' / '}}' are collapsed to '{' / '}'.
       The model sometimes copies the escaped braces of str.format prompt
       templates. The collapsed text is used only when it parses: valid
       compact JSON also contains '}}' (e.g. {"a": {"b": 1}}).

    The result can still be invalid JSON. Callers parse it and handle errors.
    """
    stripped = (text or "").strip()
    if _is_json(stripped):
        return stripped

    match = _CODE_FENCE.search(stripped)
    if match:
        candidate = match.group(1)
    else:
        start, end = stripped.find("{"), stripped.rfind("}")
        candidate = stripped[start : end + 1] if 0 <= start < end else stripped

    if _is_json(candidate):
        return candidate
    collapsed = candidate.replace("{{", "{").replace("}}", "}")
    return collapsed if _is_json(collapsed) else candidate
