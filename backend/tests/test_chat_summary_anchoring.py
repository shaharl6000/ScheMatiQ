"""Tests for anchoring WS-only summaries back into the restored transcript.

Re-extraction and reference-fill recaps are produced by background operations and
pushed to the client over the WebSocket; they never enter the Gemini model
history. They are persisted separately (``chat_summary_store``) keyed by the
operation id, and on reload ``_reconstruct_transcript`` splices each one back in
right after the model's acknowledgement of the tool call that started it — the
same chronological spot the user saw it live.

The anchor is exact rather than heuristic: the operation id already lives in the
persisted history, inside the ``function_response`` of the starting tool call.
These tests build realistic histories (round-tripped through the same serialize/
deserialize the persistence uses) and assert placement, including the awkward
cases: no following model text, two operations on the same column, and an empty
summary map.
"""

from __future__ import annotations

from google.genai import types

from app.services.chat.agent_service import ChatAgentService
from app.models.chat import ChatTurnMessage


def _roundtrip(contents: list) -> list:
    """Serialize + deserialize like the persistence layer, so tests exercise the
    same shape restore produces rather than in-memory objects."""
    return [
        types.Content.model_validate(c.model_dump(mode="json", exclude_none=True))
        for c in contents
    ]


def _reextract_call(op_id: str, ack: str | None = "Re-extraction started.") -> list:
    """A tool call + its function_response (carrying ``op_id``) + optional ack."""
    turns = [
        types.Content(
            role="model",
            parts=[
                types.Part(
                    function_call=types.FunctionCall(name="reextract", args={})
                )
            ],
        ),
        types.Content(
            role="user",
            parts=[
                types.Part.from_function_response(
                    name="reextract",
                    response={"result": {"status": "started", "operation_id": op_id}},
                )
            ],
        ),
    ]
    if ack is not None:
        turns.append(types.Content(role="model", parts=[types.Part(text=ack)]))
    return turns


def test_summary_anchored_after_ack_before_later_message():
    history = _roundtrip(
        [
            types.Content(role="user", parts=[types.Part(text="add column ruling_date")]),
            *_reextract_call("71cbbc6d"),
            types.Content(role="user", parts=[types.Part(text="now add another column")]),
            types.Content(role="model", parts=[types.Part(text="Sure, which column?")]),
        ]
    )
    summaries = {"71cbbc6d": "Re-extraction finished. 37 of 40 rows filled."}
    messages = ChatAgentService._reconstruct_transcript(history, summaries)

    contents = [m.get("content", "") for m in messages]
    i_ack = contents.index("Re-extraction started.")
    i_sum = next(i for i, c in enumerate(contents) if "37 of 40" in c)
    i_later = contents.index("now add another column")
    assert i_ack < i_sum < i_later


def test_summary_without_following_model_text_is_still_emitted():
    history = _roundtrip(_reextract_call("z9", ack=None))
    messages = ChatAgentService._reconstruct_transcript(history, {"z9": "LEFTOVER"})
    assert any("LEFTOVER" in (m.get("content") or "") for m in messages)


def test_two_operations_same_column_anchor_distinctly_by_id():
    history = _roundtrip(
        [
            *_reextract_call("aaa", ack="Started A."),
            *_reextract_call("bbb", ack="Started B."),
        ]
    )
    messages = ChatAgentService._reconstruct_transcript(
        history, {"aaa": "SUM_A", "bbb": "SUM_B"}
    )
    contents = [m.get("content", "") for m in messages]
    assert contents.index("Started A.") < contents.index("SUM_A")
    assert contents.index("SUM_A") < contents.index("Started B.")
    assert contents.index("Started B.") < contents.index("SUM_B")


def test_empty_summary_map_injects_nothing():
    history = _roundtrip(_reextract_call("op1"))
    with_summary = ChatAgentService._reconstruct_transcript(history, {"op1": "S"})
    without = ChatAgentService._reconstruct_transcript(history, {})
    assert len(with_summary) == len(without) + 1
    assert not any("S" == (m.get("content") or "") for m in without)


def test_reference_fill_anchored_by_fill_id():
    history = _roundtrip(
        [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_function_response(
                        name="fill_column_from_reference",
                        response={"result": {"status": "started", "fill_id": "f1"}},
                    )
                ],
            ),
            types.Content(role="model", parts=[types.Part(text="Filling started.")]),
        ]
    )
    messages = ChatAgentService._reconstruct_transcript(history, {"f1": "FILL_SUM"})
    contents = [m.get("content", "") for m in messages]
    assert contents.index("Filling started.") < contents.index("FILL_SUM")


def test_no_summary_for_unrelated_tool_result():
    """A tool result without operation_id/fill_id must not pull in any summary."""
    history = _roundtrip(
        [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_function_response(
                        name="add_column", response={"result": {"message": "ok"}}
                    )
                ],
            ),
        ]
    )
    messages = ChatAgentService._reconstruct_transcript(history, {"x": "should-not-appear"})
    assert not any("should-not-appear" in (m.get("content") or "") for m in messages)


def test_injected_summaries_validate_as_chat_turn_messages():
    history = _roundtrip(_reextract_call("op1"))
    for msg in ChatAgentService._reconstruct_transcript(history, {"op1": "recap text"}):
        ChatTurnMessage(**msg)  # raises if a field is missing or mistyped


def test_summary_id_extraction_handles_missing_and_malformed():
    extract = ChatAgentService._summary_id_from_function_response
    # A plain text part has no function_response.
    assert extract(types.Part(text="hi")) is None
    # A function_response with operation_id.
    op_part = types.Part.from_function_response(
        name="reextract", response={"result": {"operation_id": "op7"}}
    )
    assert extract(op_part) == "op7"
    # A function_response with fill_id.
    fill_part = types.Part.from_function_response(
        name="fill_column_from_reference", response={"result": {"fill_id": "f7"}}
    )
    assert extract(fill_part) == "f7"
    # A function_response with neither id.
    plain = types.Part.from_function_response(
        name="add_column", response={"result": {"message": "ok"}}
    )
    assert extract(plain) is None
