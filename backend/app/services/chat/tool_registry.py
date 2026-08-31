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
    # Document-backed extraction tools: offered outside their declared
    # session_modes (i.e. in load mode) only when the session's source documents
    # are available. See get_tools_for_context's extraction_capable parameter.
    requires_documents: bool = False


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
            name="explain_cell",
            description=(
                "Explain the state of a single cell (read-only): whether it has a "
                "value and where that value came from, or — if it is empty — why. "
                "Distinguishes three empty cases so you know whether filling it will "
                "help: 'confirmed_empty' (the model already inspected the source and "
                "found no value, so re-extracting is unlikely to help), 'not_extracted' "
                "(no extraction has produced a value yet, so extract_cells can fill it), "
                "and 'document_skipped' (the source document was skipped entirely — see "
                "list_skipped_documents). For filled cells it returns the value plus any "
                "source excerpts. Use row/column names from preview_data."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "row": {
                        "type": "string",
                        "description": "Observation-unit / row name (as shown by preview_data).",
                    },
                    "column": {
                        "type": "string",
                        "description": "Column name to inspect.",
                    },
                },
                "required": ["row", "column"],
            },
            cost_class="cheap",
            handler="explain_cell",
        ),
        ToolSpec(
            name="get_validation",
            description="Validate the schema for duplicate names, missing definitions, and other issues.",
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="get_validation",
        ),
        ToolSpec(
            name="data_summary",
            description=(
                "Table-health overview (read-only): the total row count and, for each "
                "column, how many cells are filled vs. empty, the coverage percentage, "
                "and how many of the empties are 'confirmed empty' (the model already "
                "checked and found no value, so filling them will not help). Also "
                "reports how many source documents were skipped. Use this to see where "
                "the gaps are before running extract_cells or reprocess."
            ),
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="data_summary",
        ),
        ToolSpec(
            name="find_rows",
            description=(
                "Find the rows whose value in a given column matches a condition "
                "(read-only). Use it to target other tools precisely — e.g. list the "
                "rows missing a column before extract_cells, or find rows whose name "
                "contains a string before rename_unit / merge_units. Returns matching "
                "row names (and their value); large result sets are truncated to `limit`."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "column": {
                        "type": "string",
                        "description": "Column to test on each row.",
                    },
                    "match": {
                        "type": "string",
                        "enum": ["empty", "filled", "equals", "contains"],
                        "description": (
                            "How to test the cell: 'empty' / 'filled', or 'equals' / "
                            "'contains' compared against `value`. Defaults to 'empty'."
                        ),
                    },
                    "value": {
                        "type": "string",
                        "description": "Comparison text for match='equals' or 'contains'.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (default 50, capped at 500).",
                    },
                },
                "required": ["column"],
            },
            cost_class="cheap",
            handler="find_rows",
        ),
        ToolSpec(
            name="list_skipped_documents",
            description=(
                "List the source documents that were skipped during extraction and "
                "the recorded reason for each — documents that produced no table rows "
                "because no matching observation unit was found in them. Use this FIRST "
                "when the user asks why a document is missing, has no data, or was not "
                "extracted. A skipped document will NOT be re-included by reextract or "
                "reprocess (both keep skipped documents skipped); re-including one "
                "requires changing the observation unit so the document matches. Read "
                "the reason and explain it to the user before proposing any re-run."
            ),
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="list_skipped_documents",
            # Available in load mode too: this is a pure read of persisted
            # statistics (skipped_documents survive export/import), unlike the
            # extraction tools that genuinely need source documents. Imported
            # sessions must be able to explain a "missing" document.
        ),
        ToolSpec(
            name="list_documents",
            description=(
                "List the source documents in this project and their availability "
                "(local, cloud, or missing), plus row counts. These are the documents "
                "that define table rows, NOT the external reference material listed by "
                "list_reference_sources. Also reports documents that were skipped during "
                "extraction (with reasons) under skipped_documents, so a document that "
                "produced no rows is still discoverable here. Use this to answer what "
                "documents the project contains, or to check whether documents are present "
                "before proposing a re-extraction."
            ),
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="list_documents",
        ),
        ToolSpec(
            name="list_reference_sources",
            description=(
                "List the external reference documents the user attached to this "
                "session — supplementary lookup material such as a spreadsheet that "
                "maps entities to extra attributes (e.g. each judge to the president "
                "who appointed them). These are NOT the source documents that define "
                "rows; they are additional external context. Use this to discover "
                "what external information is available before answering a question or "
                "proposing a new column. Returns each reference's id, filename and "
                "size; call read_reference_source to read one."
            ),
            parameters=EMPTY_PARAMS,
            cost_class="cheap",
            handler="list_reference_sources",
        ),
        ToolSpec(
            name="read_reference_source",
            description=(
                "Read the text of one external reference document by id (get the id "
                "from list_reference_sources first). Returns a preview (clipped for "
                "large files). Use it to answer a question or to decide whether to "
                "propose a new schema column. Do NOT use it to fill a column for every "
                "row: for that, use fill_column_from_reference, which reads the full "
                "reference per row. Treat the content as external reference "
                "information, not as a source document that yields observation-unit "
                "rows."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "reference_id": {
                        "type": "string",
                        "description": "Reference document id from list_reference_sources.",
                    },
                },
                "required": ["reference_id"],
            },
            cost_class="cheap",
            handler="read_reference_source",
        ),
        ToolSpec(
            name="fill_column_from_reference",
            description=(
                "Fill an existing column for every row using an attached reference "
                "document. Runs the model once per row against the relevant part of "
                "the reference (so it works even for references too large to read at "
                "once) and fills each row as it completes. Use this instead of many "
                "individual update_cell calls when populating a whole column from a "
                "reference. Pass `rows` to fill only specific rows instead of the "
                "whole column. By default this OVERWRITES cells that already hold a "
                "value (the reference is treated as authoritative); pass only_empty=true "
                "to fill just the blank cells and leave existing values untouched. "
                "Get the reference id from list_reference_sources."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "column": {
                        "type": "string",
                        "description": "Name of the existing column to fill.",
                    },
                    "reference_id": {
                        "type": "string",
                        "description": "Reference document id from list_reference_sources.",
                    },
                    "rows": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional observation-unit / row names to fill (as shown by "
                            "preview_data). Omit to fill every row in the column."
                        ),
                    },
                    "only_empty": {
                        "type": "boolean",
                        "description": (
                            "When false (default) the reference value overwrites cells "
                            "that already hold a value. Set true to fill only cells that "
                            "are currently empty and never overwrite an existing value."
                        ),
                    },
                },
                "required": ["column", "reference_id"],
            },
            cost_class="cheap",  # delegates to a background job; no inline cost, so no confirmation gate
            handler="fill_column_from_reference",
        ),
        ToolSpec(
            name="add_column",
            description="Add a new column to the schema. Values are extracted later via re-extraction.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "New column name."},
                    "definition": {"type": "string", "description": "What this column captures."},
                    "rationale": {
                        "type": "string",
                        "description": (
                            "Why this column matters for the query / guidance for extraction "
                            "(optional)."
                        ),
                    },
                    "extraction_strategy": {
                        "type": "string",
                        "enum": ["document", "web", "document_then_web"],
                        "description": (
                            "Where values come from: document (default), web (always use "
                            "grounded web search), or document_then_web (use the source "
                            "document first and search the web only when it has no value)."
                        ),
                    },
                },
                "required": ["name", "definition"],
            },
            cost_class="cheap",
            handler="add_column",
        ),
        ToolSpec(
            name="edit_column",
            description=(
                "Rename or update a SCHEMA COLUMN (data table field): its name, definition "
                "(what the column captures), and/or rationale (why the column matters / how to "
                "extract it), including whether values come from documents or grounded web "
                "search. Does not change the observation unit definition — use "
                "edit_observation_unit for that. Call get_schema first for the exact column "
                "name and to confirm WHICH column the user means before editing; never edit "
                "multiple columns unless the user explicitly asked for all of them. Does not "
                "re-run extraction; use reextract separately if needed."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "old_name": {"type": "string", "description": "Current column name."},
                    "new_name": {"type": "string", "description": "New column name (optional)."},
                    "definition": {"type": "string", "description": "Updated definition (optional)."},
                    "rationale": {
                        "type": "string",
                        "description": (
                            "Updated rationale: why this column matters for the query / guidance "
                            "for extraction (optional). Pass an empty string to clear it."
                        ),
                    },
                    "extraction_strategy": {
                        "type": "string",
                        "enum": ["document", "web", "document_then_web"],
                        "description": (
                            "Where values come from: document (default), web (always use "
                            "grounded web search), or document_then_web (use the source "
                            "document first and search the web only when it has no value)."
                        ),
                    },
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
            description=(
                "Update a single cell value in the data table. Identify the row by "
                "its observation-unit name (the 'row' argument). If the same unit "
                "name appears in more than one row (the same-named unit occurring in "
                "different source documents), also pass source_document — shown per "
                "row by preview_data — to target the exact row; otherwise the first "
                "matching row is updated."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "row": {"type": "string", "description": "Row name / observation unit name."},
                    "column": {"type": "string", "description": "Column name."},
                    "value": {"type": "string", "description": "New cell value."},
                    "source_document": {
                        "type": "string",
                        "description": (
                            "Optional. The row's source document (from preview_data). "
                            "Provide it to disambiguate when several rows share the "
                            "same unit name."
                        ),
                    },
                    "reference_id": {
                        "type": "string",
                        "description": (
                            "Optional. If this value was taken from an attached "
                            "reference document, pass that reference's id (from "
                            "list_reference_sources) so the cell is marked as sourced "
                            "from it. Omit for normal manual edits."
                        ),
                    },
                },
                "required": ["row", "column", "value"],
            },
            cost_class="cheap",
            handler="update_cell",
            server_injects=("session_id",),
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
            name="merge_units",
            description=(
                "Merge two or more table rows that represent the SAME observation unit "
                "under different names into one row, keeping their data. Use this when "
                "the same entity appears as separate rows (for example the same judge "
                "listed once by full name and once as 'Judge X'). This merges DATA ROWS, "
                "not schema columns (use merge_columns for columns) and not the "
                "observation unit definition (use edit_observation_unit for that). Get "
                "the exact row names from preview_data first. Pass the row names in "
                "`units` (at least two) and optionally a `target_name` for the merged "
                "row; when omitted the first name in `units` is used."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "units": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 2,
                        "description": (
                            "Row / observation-unit names to merge (at least two), "
                            "exactly as shown by preview_data."
                        ),
                    },
                    "target_name": {
                        "type": "string",
                        "description": (
                            "Name for the merged row. Defaults to the first name in "
                            "`units`."
                        ),
                    },
                },
                "required": ["units"],
            },
            cost_class="cheap",
            handler="merge_units",
        ),
        ToolSpec(
            name="rename_unit",
            description=(
                "Rename a single table row / observation unit, keeping all of its "
                "data. Use this when a row's label is wrong or inconsistent and you "
                "want to change just its name (not merge it with another row — use "
                "merge_units for that, and edit_observation_unit to change the unit "
                "definition). Get the exact current name from preview_data first. Note: "
                "if you rename to a name that already exists, the two rows will share "
                "that name (an effective merge)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "old_name": {
                        "type": "string",
                        "description": "Current row / unit name, exactly as shown by preview_data.",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New name for the row / unit.",
                    },
                },
                "required": ["old_name", "new_name"],
            },
            cost_class="cheap",
            handler="rename_unit",
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
            description=(
                "Re-run value extraction for SPECIFIC schema columns you added or edited "
                "(expensive). Pass the `columns` you changed so only those are re-extracted; "
                "when omitted it defaults to the edited/new columns only (scope='edited_only'). "
                "Does not touch other columns or the observation unit unless scope='all'. Use "
                "this for a targeted refresh after add_column/edit_column; use `reprocess` to "
                "refresh the entire table. By default this OVERWRITES existing values in the "
                "targeted columns; pass only_empty=true to fill just the blank cells and leave "
                "existing values untouched (the safe choice when the user says fill/complete "
                "missing cells rather than redo the column). Does NOT re-include a document that was skipped "
                "during extraction (skipped documents stay skipped): if the user asks about a "
                "missing document, call list_skipped_documents first."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Specific column(s) to re-extract. Use this to target exactly the "
                            "columns you edited. Omit only when scope is given."
                        ),
                    },
                    "scope": {
                        "type": "string",
                        "enum": ["all", "edited_only"],
                        "description": (
                            "Fallback when `columns` is omitted: 'edited_only' re-extracts the "
                            "edited/new columns, 'all' re-extracts every column."
                        ),
                    },
                    "only_empty": {
                        "type": "boolean",
                        "description": (
                            "When false (default) every targeted cell is re-extracted and "
                            "existing values are overwritten. Set true to fill only blank "
                            "cells and never overwrite a value that is already present."
                        ),
                    },
                },
                "required": [],
            },
            cost_class="expensive",
            handler="reextract",
            session_modes=("schematiq",),
            requires_documents=True,
        ),
        ToolSpec(
            name="extract_cells",
            description=(
                "Fill specific cells by running value extraction only where you ask "
                "(expensive). This is the granular counterpart to reextract/reprocess: "
                "target specific `documents` (source files), specific `rows` "
                "(observation-unit names), and/or specific `columns`, and set "
                "`only_empty` to fill just the blank cells without overwriting values "
                "that are already there. The most common use is filling gaps: call it "
                "with only_empty=true (the default) to fill every empty cell, or add "
                "documents/rows/columns to narrow the scope. A document listed in "
                "`documents` that was previously skipped is re-evaluated, so this also "
                "retries skipped documents. only_empty=true also retries cells the model "
                "previously inspected and confirmed empty, without overwriting any cell "
                "that already holds a value, so it is the correct choice for 'fill the "
                "missing/blank cells' even when those blanks were checked before — do NOT "
                "reach for only_empty=false to re-check confirmed-empty cells. "
                "only_empty=false re-extracts and OVERWRITES every targeted cell, "
                "including ones that already hold correct values; this is destructive and "
                "not reversible, so use it only when the user explicitly asks to discard "
                "and redo existing values. If a fill request is ambiguous about whether to "
                "overwrite existing values, default to only_empty=true (or ask). "
                "Note: re-extraction can only fill values that are actually present in the "
                "source documents. If a column holds information that is typically not in "
                "the documents themselves (e.g. biographical facts), extract_cells will "
                "keep confirming those cells empty; suggest fill_column_from_reference with "
                "an external source instead. "
                "Get exact names from list_documents, "
                "list_skipped_documents, or preview_data first."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "documents": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Restrict to these source documents (names from "
                            "list_documents / list_skipped_documents). Omit for all."
                        ),
                    },
                    "rows": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Restrict to these observation-unit / row names (as shown by "
                            "preview_data). Omit for all rows."
                        ),
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Restrict to these columns. Omit to consider every column."
                        ),
                    },
                    "only_empty": {
                        "type": "boolean",
                        "description": (
                            "When true (default), only blank cells are filled -- including "
                            "cells previously confirmed empty -- and existing values are "
                            "never overwritten. Set false only to re-extract and OVERWRITE "
                            "every targeted cell even when it already holds a correct value "
                            "(destructive, not reversible); requires an explicit user "
                            "request to discard and redo values."
                        ),
                    },
                },
                "required": [],
            },
            cost_class="expensive",
            handler="extract_cells",
            session_modes=("schematiq",),
            requires_documents=True,
        ),
        ToolSpec(
            name="continue_discovery",
            description=(
                "Extend schema discovery by extracting from more documents (expensive). "
                "By default it continues over the project's original source documents. "
                "Pass document_source='upload' to instead extract the documents the user "
                "just added through the Documents tab (the upload itself happens there; "
                "this only runs the extraction on the already-added files)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "document_source": {
                        "type": "string",
                        "enum": ["original", "upload"],
                        "description": (
                            "'original' (default) continues over the project's source "
                            "documents; 'upload' extracts documents added via the "
                            "Documents tab."
                        ),
                    },
                },
                "required": [],
            },
            cost_class="expensive",
            handler="continue_discovery",
            # Gated on document availability, identical to reextract/extract_cells:
            # offered in schematiq always, and in load mode once source documents
            # are present. The service resolves documents from the session-local
            # documents/pending_documents dirs and builds its initial schema from
            # session.columns, so it is session-type-agnostic; it raises clearly
            # if no documents are found.
            session_modes=("schematiq",),
            requires_documents=True,
        ),
        ToolSpec(
            name="reprocess",
            description=(
                "Re-run value extraction for the ENTIRE table — every column — from the source "
                "documents (expensive). When `columns` is omitted it re-extracts all columns "
                "(scope='all'). Use this for a full refresh of every value; prefer `reextract` "
                "when you only changed specific columns and want to refresh just those. Does "
                "NOT re-include a document that was skipped during extraction (skipped "
                "documents stay skipped): if the user asks about a missing document, call "
                "list_skipped_documents first."
            ),
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
            name="rediscover",
            description=(
                "Re-run full schema and observation-unit discovery from the "
                "source documents (expensive). This rebuilds the schema and "
                "re-discovers rows across the WHOLE project from scratch under "
                "the current observation unit, so — unlike reextract/reprocess — "
                "it re-evaluates documents that were previously skipped. Use it "
                "to pull a skipped document into the table, or after "
                "edit_observation_unit when the unit definition changed and the "
                "table should be rebuilt to match. Because it discards the "
                "current schema and rows and re-extracts everything, always "
                "confirm with the user first. Requires the source documents to "
                "be available; if they are not, tell the user to re-attach the "
                "originals via \"Show source document\" first, then retry."
            ),
            parameters={
                "type": "object",
                "properties": {},
                "required": [],
            },
            cost_class="expensive",
            handler="rediscover",
            # Same gating as reextract/extract_cells: offered in schematiq always,
            # and in load mode once source documents are available.
            session_modes=("schematiq",),
            requires_documents=True,
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
    extraction_capable: bool = False,
) -> list[ToolSpec]:
    """Return tools available for the current workspace context.

    ``extraction_capable`` unlocks the document-backed extraction tools (those
    flagged ``requires_documents``) outside their declared ``session_modes`` —
    in practice, in ``load`` mode once the session's source documents are
    available. This mirrors the frontend rule
    ``canRediscoverSchema = sessionMode == 'schematiq' || hasSourceDocuments``:
    the correct gate for extraction is document availability, not session type.

    It defaults to ``False`` so every existing caller keeps today's mode-only
    gating, and it affects ONLY ``requires_documents`` tools — every other tool
    stays gated exactly as before.
    """
    tools: list[ToolSpec] = []
    for tool in _all_tools():
        if not tool.available:
            if not session_id and tool.name in ("create_project", "import_project", "web_search"):
                tools.append(tool)
            continue
        if tool.requires_session and not session_id:
            continue
        # A document-backed extraction tool is offered outside its declared
        # session_modes (and past hidden_in_load) only when the session is
        # extraction-capable — i.e. load mode with source documents present.
        doc_unlocked = tool.requires_documents and extraction_capable
        if session_id and session_mode not in tool.session_modes and not doc_unlocked:
            continue
        if session_id and session_mode == "load" and tool.hidden_in_load and not doc_unlocked:
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


def available_tools_prompt_addendum(tools: list[ToolSpec]) -> str:
    """A mode-aware system-prompt footer naming the tools that are callable now.

    The static chat system prompt names tools (reextract, extract_cells,
    run_schematiq, continue_discovery, ...) that ``get_tools_for_context`` may
    filter out for this session — e.g. extraction tools in a load session with
    no source documents available. Without this footer the model follows the
    static prompt and offers such a tool, then contradicts itself when the call
    turns out to be unavailable. Built from the SAME tool list as
    ``to_function_declarations`` (only ``available`` tools are callable) so the
    advertised set can never drift from what the model can actually call.
    """
    names = sorted(tool.name for tool in tools if tool.available)
    if not names:
        return ""
    return (
        "\n\nTools available in THIS session: "
        + ", ".join(names)
        + ".\nUse and offer ONLY these tools. If a request would require a tool "
        "that is not listed here — for example re-running extraction or "
        "rediscovering the schema when the session has no source documents "
        "available — tell the user plainly that it is not available in this "
        "session instead of offering or attempting it."
    )


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
        "explain_cell": "Explaining cell",
        "get_validation": "Validating schema",
        "data_summary": "Summarizing the table",
        "find_rows": "Searching rows",
        "list_skipped_documents": "Listing skipped documents",
        "list_documents": "Listing documents",
        "list_reference_sources": "Listing reference documents",
        "read_reference_source": "Reading reference document",
        "add_column": "Adding column",
        "edit_column": "Editing column",
        "delete_column": "Deleting column",
        "merge_columns": "Merging columns",
        "update_cell": "Updating cell",
        "add_unit": "Adding observation unit",
        "remove_unit": "Removing observation unit",
        "merge_units": "Merging observation units",
        "rename_unit": "Renaming row",
        "export_table": "Preparing export",
        "run_schematiq": "Starting ScheMatiQ extraction",
        "reextract": "Starting re-extraction",
        "extract_cells": "Filling cells",
        "continue_discovery": "Starting continue discovery",
        "reprocess": "Starting reprocessing",
        "rediscover": "Rediscovering schema",
    }
    return labels.get(tool_name, f"Running {tool_name}")
