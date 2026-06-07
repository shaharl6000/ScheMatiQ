"""Declarative tool registry for the workspace chat agent."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

try:
    from google.genai import types
except ImportError:  # pragma: no cover - optional at import time in tests
    types = None  # type: ignore

CostClass = Literal["cheap", "expensive"]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    cost_class: CostClass
    handler: str
    available: bool = True
    server_injects: tuple[str, ...] = ("session_id",)
    requires_session: bool = True
    session_modes: tuple[str, ...] = ("schematiq", "load")
    hidden_in_load: bool = False


EMPTY_PARAMS: dict[str, Any] = {"type": "object", "properties": {}, "required": []}


def _all_tools() -> list[ToolSpec]:
    return [
        ToolSpec(
            name="get_status",
            description="Get the current project execution status and progress.",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="get_status",
        ),
        ToolSpec(
            name="get_schema",
            description="Get the current table schema including column names and definitions.",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="get_schema",
        ),
        ToolSpec(
            name="get_observation_unit",
            description=(
                "Get the observation unit definition: the schema-level concept of what each "
                "table row represents (shown on the Observation Unit tab). This is NOT the list "
                "of column names — use get_schema for columns."
            ),
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="get_observation_unit",
        ),
        ToolSpec(
            name="edit_observation_unit",
            description=(
                "Update the observation unit definition (Observation Unit tab): what entity each "
                "row represents. Use this when the user wants to change the row entity type "
                "(e.g. from 'federal judge' to 'court judge'). This is NOT edit_column — columns "
                "are separate schema fields. Call get_observation_unit first, then provide the "
                "updated name and/or definition. After a meaningful change, suggest reextract "
                "(refresh values from documents) or run_schematiq (rediscover schema)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Observation unit name (e.g. 'Court Judge').",
                    },
                    "definition": {
                        "type": "string",
                        "description": (
                            "What constitutes one row (at least 10 characters). "
                            "Omit to keep the current definition."
                        ),
                    },
                    "example_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional example row names.",
                    },
                },
                "required": ["name"],
            },
            cost_class="cheap",
            handler="edit_observation_unit",
        ),
        ToolSpec(
            name="preview_data",
            description="Preview table rows with optional pagination.",
            parameters={
                "type": "object",
                "properties": {
                    "offset": {"type": "integer", "description": "Row offset (default 0)."},
                    "limit": {"type": "integer", "description": "Max rows to return (default 10)."},
                },
                "required": [],
            },
            cost_class="cheap",
            handler="preview_data",
        ),
        ToolSpec(
            name="get_validation",
            description="Validate the schema for duplicate names, missing definitions, and other issues.",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="get_validation",
        ),
        ToolSpec(
            name="add_column",
            description="Add a new column to the schema. Values are extracted later via re-extraction.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "New column name."},
                    "definition": {"type": "string", "description": "What this column captures."},
                },
                "required": ["name", "definition"],
            },
            cost_class="cheap",
            handler="add_column",
        ),
        ToolSpec(
            name="edit_column",
            description=(
                "Rename or update a SCHEMA COLUMN (data table field). Does not change the "
                "observation unit definition — use edit_observation_unit for that. "
                "Call get_schema first for exact column names. Does not re-run extraction; "
                "use reprocess separately if needed."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "old_name": {"type": "string", "description": "Current column name."},
                    "new_name": {"type": "string", "description": "New column name (optional)."},
                    "definition": {"type": "string", "description": "Updated definition (optional)."},
                },
                "required": ["old_name"],
            },
            cost_class="cheap",
            handler="edit_column",
        ),
        ToolSpec(
            name="delete_column",
            description="Delete a column from the schema and remove its data.",
            parameters={
                "type": "object",
                "properties": {
                    "column_name": {"type": "string", "description": "Column to delete."},
                },
                "required": ["column_name"],
            },
            cost_class="cheap",
            handler="delete_column",
        ),
        ToolSpec(
            name="merge_columns",
            description="Merge two columns into one target column.",
            parameters={
                "type": "object",
                "properties": {
                    "column_a": {"type": "string", "description": "First source column."},
                    "column_b": {"type": "string", "description": "Second source column."},
                    "target_name": {
                        "type": "string",
                        "description": "Target column name (defaults to column_a).",
                    },
                },
                "required": ["column_a", "column_b"],
            },
            cost_class="cheap",
            handler="merge_columns",
        ),
        ToolSpec(
            name="update_cell",
            description="Update a single cell value in the data table.",
            parameters={
                "type": "object",
                "properties": {
                    "row": {"type": "string", "description": "Row name / observation unit name."},
                    "column": {"type": "string", "description": "Column name."},
                    "value": {"type": "string", "description": "New cell value."},
                },
                "required": ["row", "column", "value"],
            },
            cost_class="cheap",
            handler="update_cell",
            server_injects=("session_id", "source_document"),
        ),
        ToolSpec(
            name="add_unit",
            description="Add a new table row (one observation-unit instance). Not the observation unit definition.",
            parameters={
                "type": "object",
                "properties": {
                    "unit_name": {"type": "string", "description": "Name of the new unit."},
                },
                "required": ["unit_name"],
            },
            cost_class="cheap",
            handler="add_unit",
        ),
        ToolSpec(
            name="remove_unit",
            description="Remove a table row (one observation-unit instance). Not the observation unit definition.",
            parameters={
                "type": "object",
                "properties": {
                    "unit_name": {"type": "string", "description": "Name of the unit to remove."},
                },
                "required": ["unit_name"],
            },
            cost_class="cheap",
            handler="remove_unit",
        ),
        ToolSpec(
            name="export_table",
            description="Export the table (csv, rich csv with excerpts, or schema only).",
            parameters={
                "type": "object",
                "properties": {
                    "format": {
                        "type": "string",
                        "enum": ["csv", "rich", "schema"],
                        "description": "Export format.",
                    },
                },
                "required": ["format"],
            },
            cost_class="cheap",
            handler="export_table",
        ),
        ToolSpec(
            name="run_schematiq",
            description="Start the full ScheMatiQ discovery and extraction pipeline (expensive).",
            parameters=EMPTY_PARAMS,
            cost_class="expensive",
            handler="run_schematiq",
            session_modes=("schematiq",),
        ),
        ToolSpec(
            name="reextract",
            description="Re-run value extraction for schema columns (expensive).",
            parameters={
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "enum": ["all", "edited_only"],
                        "description": "Re-extract all columns or only edited/new columns.",
                    },
                },
                "required": ["scope"],
            },
            cost_class="expensive",
            handler="reextract",
            session_modes=("schematiq",),
        ),
        ToolSpec(
            name="continue_discovery",
            description="Extend schema discovery with more documents (expensive).",
            parameters=EMPTY_PARAMS,
            cost_class="expensive",
            handler="continue_discovery",
            hidden_in_load=True,
            session_modes=("schematiq",),
        ),
        ToolSpec(
            name="reprocess",
            description="Re-run backbone LLM extraction for specified or all columns (expensive).",
            parameters={
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to reprocess (omit for all columns).",
                    },
                },
                "required": [],
            },
            cost_class="expensive",
            handler="reprocess",
        ),
        ToolSpec(
            name="web_search",
            description="Search the web for external information (planned, not yet available).",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query."},
                },
                "required": ["query"],
            },
            cost_class="cheap",
            handler="web_search",
            available=False,
        ),
        ToolSpec(
            name="create_project",
            description="Create a new ScheMatiQ project (requires workspace UI).",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="create_project",
            requires_session=False,
            available=False,
        ),
        ToolSpec(
            name="import_project",
            description="Import an existing project file (requires workspace UI).",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="import_project",
            requires_session=False,
            available=False,
        ),
    ]


TOOL_BY_NAME: dict[str, ToolSpec] = {tool.name: tool for tool in _all_tools()}


def get_tools_for_context(
    session_id: Optional[str],
    session_mode: str = "schematiq",
) -> list[ToolSpec]:
    """Return tools available for the current workspace context."""
    tools: list[ToolSpec] = []
    for tool in _all_tools():
        if not tool.available:
            if not session_id and tool.name in ("create_project", "import_project", "web_search"):
                tools.append(tool)
            continue
        if tool.requires_session and not session_id:
            continue
        if session_id and session_mode not in tool.session_modes:
            continue
        if session_id and session_mode == "load" and tool.hidden_in_load:
            continue
        tools.append(tool)
    return tools


def to_public_tool_list(tools: list[ToolSpec]) -> list[dict[str, Any]]:
    return [
        {
            "name": tool.name,
            "description": tool.description,
            "cost_class": tool.cost_class,
            "available": tool.available,
            "parameters": tool.parameters,
        }
        for tool in tools
    ]


def to_function_declarations(tools: list[ToolSpec]) -> list[Any]:
    if types is None:
        raise ImportError("google-genai is required for function declarations")
    return [
        types.FunctionDeclaration(
            name=tool.name,
            description=tool.description,
            parameters_json_schema=tool.parameters,
        )
        for tool in tools
        if tool.available
    ]


def tool_running_label(tool_name: str) -> str:
    labels = {
        "get_status": "Checking project status",
        "get_schema": "Loading schema",
        "get_observation_unit": "Loading observation unit",
        "edit_observation_unit": "Updating observation unit definition",
        "preview_data": "Loading data preview",
        "get_validation": "Validating schema",
        "add_column": "Adding column",
        "edit_column": "Editing column",
        "delete_column": "Deleting column",
        "merge_columns": "Merging columns",
        "update_cell": "Updating cell",
        "add_unit": "Adding observation unit",
        "remove_unit": "Removing observation unit",
        "export_table": "Preparing export",
        "run_schematiq": "Starting ScheMatiQ extraction",
        "reextract": "Starting re-extraction",
        "continue_discovery": "Starting continue discovery",
        "reprocess": "Starting reprocessing",
    }
    return labels.get(tool_name, f"Running {tool_name}")
