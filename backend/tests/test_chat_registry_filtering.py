"""Tests for the workspace chat tool registry.

These cover the parts of the registry that the rest of the chat agent relies on
for safety and correctness: the single source of truth for tools, how tools are
filtered by session/mode, what gets exposed to the model vs. the UI, and which
tools are gated as expensive. They run without any LLM (see conftest stubs).
"""

from __future__ import annotations

from app.services.chat.tool_registry import (
    TOOL_BY_NAME,
    ToolSpec,
    _all_tools,
    get_tools_for_context,
    to_function_declarations,
    to_public_tool_list,
    tool_running_label,
)

# The four operations that spend the backbone LLM over project documents. These
# MUST stay expensive so the confirmation gate keeps protecting them.
#
# fill_column_from_reference is deliberately NOT here. It does spend the model
# once per row, but it hands off to a background job rather than running inline,
# and it uses the cheap REFERENCE_FILL_MODEL — so it is registered as cheap and
# runs without a confirmation gate (see the comment on its ToolSpec).
EXPECTED_EXPENSIVE = {"run_schematiq", "reextract", "continue_discovery", "reprocess"}


def test_registry_is_single_source_of_truth() -> None:
    tools = _all_tools()
    names = [tool.name for tool in tools]
    assert len(names) == len(set(names)), "tool names must be unique"
    assert set(TOOL_BY_NAME) == set(names)
    assert all(isinstance(tool, ToolSpec) for tool in tools)


def test_expensive_tools_are_exactly_the_llm_operations() -> None:
    expensive = {tool.name for tool in _all_tools() if tool.cost_class == "expensive"}
    assert expensive == EXPECTED_EXPENSIVE


def test_editing_tools_are_cheap() -> None:
    # Metadata edits only *suggest* a re-run; they must not be gated themselves,
    # otherwise every column rename would pop a confirmation card.
    for name in ("edit_observation_unit", "edit_column", "add_column", "update_cell"):
        assert TOOL_BY_NAME[name].cost_class == "cheap"


def test_read_tools_are_cheap() -> None:
    for name in (
        "get_status",
        "get_schema",
        "get_observation_unit",
        "preview_data",
        "get_validation",
    ):
        assert TOOL_BY_NAME[name].cost_class == "cheap"


def test_schematiq_only_tools_hidden_in_load_mode() -> None:
    schematiq = {t.name for t in get_tools_for_context("s1", "schematiq")}
    load = {t.name for t in get_tools_for_context("s1", "load")}
    # Extraction-style operations need source documents, so they are unavailable
    # in an imported/static load session.
    assert {"run_schematiq", "reextract", "continue_discovery"} <= schematiq
    assert {"run_schematiq", "reextract", "continue_discovery"}.isdisjoint(load)


def test_no_session_exposes_only_entry_point_tools() -> None:
    names = {t.name for t in get_tools_for_context(None, "schematiq")}
    assert names == {"create_project", "import_project", "web_search"}


def test_function_declarations_exclude_unavailable_tools() -> None:
    # web_search is declared but not yet available; the model must not be able to
    # call it, so it is filtered out of the function declarations.
    decl_names = {d.name for d in to_function_declarations(get_tools_for_context("s1", "schematiq"))}
    assert "web_search" not in decl_names
    # ...but a real tool is present.
    assert "get_schema" in decl_names


def test_public_list_marks_planned_tools_unavailable() -> None:
    public = {t["name"]: t for t in to_public_tool_list(get_tools_for_context(None, "schematiq"))}
    assert public["web_search"]["available"] is False


def test_public_list_reports_cost_class() -> None:
    public = {t["name"]: t for t in to_public_tool_list(get_tools_for_context("s1", "schematiq"))}
    assert public["reextract"]["cost_class"] == "expensive"
    assert public["get_schema"]["cost_class"] == "cheap"


def test_every_tool_has_a_running_label() -> None:
    # Falls back to a generic label, so this never raises; we assert the known
    # ones are human-readable rather than the generic fallback.
    assert tool_running_label("reextract") == "Starting re-extraction"
    assert tool_running_label("get_schema") == "Loading schema"
    assert tool_running_label("totally_unknown_tool") == "Running totally_unknown_tool"


def test_server_injected_identifiers_not_in_model_parameters() -> None:
    # Infrastructure identifiers (session_id) are injected server-side and must
    # never appear as model-visible parameters.
    for tool in _all_tools():
        properties = tool.parameters.get("properties", {})
        for injected in tool.server_injects:
            assert injected not in properties, (
                f"{tool.name} exposes server-injected '{injected}' to the model"
            )
