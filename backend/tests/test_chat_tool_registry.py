"""Tests for chat tool registry."""

from app.services.chat.tool_registry import (
    get_tools_for_context,
    to_function_declarations,
    to_public_tool_list,
)


def test_no_session_shows_placeholder_tools():
    tools = get_tools_for_context(None, "schematiq")
    names = {tool.name for tool in tools}
    assert "create_project" in names
    assert "get_schema" not in names


def test_schematiq_session_includes_read_and_write_tools():
    tools = get_tools_for_context("session-1", "schematiq")
    names = {tool.name for tool in tools}
    assert "get_schema" in names
    assert "edit_column" in names
    assert "edit_observation_unit" in names
    assert "run_schematiq" in names
    assert "web_search" not in names


def test_load_mode_hides_run_style_tools():
    tools = get_tools_for_context("session-1", "load")
    names = {tool.name for tool in tools}
    assert "get_schema" in names
    assert "run_schematiq" not in names
    assert "continue_discovery" not in names


def test_public_tool_list_shape():
    tools = get_tools_for_context("session-1", "schematiq")
    public = to_public_tool_list(tools)
    assert public
    assert {"name", "description", "cost_class", "available", "parameters"} <= set(public[0])


def test_function_declarations_skip_unavailable():
    tools = get_tools_for_context(None, "schematiq")
    declarations = to_function_declarations(tools)
    names = {decl.name for decl in declarations}
    assert "web_search" not in names
