"""Execute chat tools by calling existing backend services."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from app.core.config import DEFAULT_DATA_DIR, DEVELOPER_MODE, LLM_CALL_GLOBAL_LIMIT
from app.core.logging_utils import set_session_context
from app.models.modification import ModificationAction
from app.models.session import ColumnInfo
from app.services.session_capabilities import has_live_pipeline
from app.services import concurrency_limiter, session_manager
from app.services.data_utils import _resolve_source_document, canonicalize_column_name
from app.services.pipeline.data_query import get_data as query_get_data
from schematiq.core.cost_estimator import estimate_from_config
from .deps import (
    WORK_DIR,
    continue_discovery_service,
    data_editor,
    load_user_llm_config,
    observation_unit_manager,
    reextraction_service,
    reference_fill_service,
    schema_manager,
    schematiq_runner,
    truncate_result,
    websocket_manager,
)
from .tool_registry import TOOL_BY_NAME

logger = logging.getLogger(__name__)

# Max characters of a reference document returned by read_reference_source. Kept
# below the tool-result truncation budget so the body is never dropped wholesale.
READ_REFERENCE_CHAT_BUDGET = 6000
VALID_EXTRACTION_STRATEGIES = {"document", "web", "document_then_web"}


def _extraction_strategy(args: dict[str, Any], default: str = "document") -> str:
    strategy = args.get("extraction_strategy", default)
    if strategy not in VALID_EXTRACTION_STRATEGIES:
        raise ValueError(
            "extraction_strategy must be document, web, or document_then_web"
        )
    return strategy


class ToolExecutor:
  async def execute(
      self,
      tool_name: str,
      session_id: str,
      session_mode: str,
      args: dict[str, Any],
  ) -> dict[str, Any]:
      tool = TOOL_BY_NAME.get(tool_name)
      if not tool:
          raise ValueError(f"Unknown tool: {tool_name}")
      set_session_context(session_id)
      handler = getattr(self, f"_handle_{tool.handler}", None)
      if handler is None:
          raise ValueError(f"No handler implemented for tool: {tool_name}")
      result = await handler(session_id, session_mode, args)
      return truncate_result(result)

  async def estimate_cost(
      self,
      tool_name: str,
      session_id: str,
      args: dict[str, Any],
  ) -> str:
      if tool_name == "run_schematiq":
          config_file = WORK_DIR / session_id / "config.json"
          if not config_file.exists():
              return "Cost estimate unavailable (project not configured)."
          with open(config_file, encoding="utf-8") as handle:
              config_data = json.load(handle)
          docs_dir = WORK_DIR / session_id / "pending_documents"
          documents = []
          if docs_dir.exists():
              for doc_file in sorted(docs_dir.iterdir()):
                  if doc_file.is_file() and doc_file.suffix in (".txt", ".md"):
                      documents.append(doc_file.read_text(encoding="utf-8"))
          result = estimate_from_config(documents, config_data)
          cost = result.get("total_cost_usd", 0)
          calls = result.get("total_api_calls", 0)
          return f"Estimated cost: ${cost:.4f}, {calls} API calls."
      if tool_name in ("reextract", "reprocess", "continue_discovery"):
          return "This operation runs the backbone LLM over project documents. Confirm to proceed."
      if tool_name == "fill_column_from_reference":
          return (
              "This runs the model once per row to fill "
              f"'{args.get('column')}' from the reference, in the background. "
              "Confirm to proceed."
          )
      return "This is an expensive operation. Confirm to proceed."

  async def _handle_get_status(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      if session_mode == "schematiq":
          status = await schematiq_runner.get_status(session_id)
          return status.model_dump()
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      completed = session.status.value in ("completed", "schema_extracted")
      return {
          "session_id": session_id,
          "status": session.status.value,
          "progress": 1.0 if completed else 0.0,
          "current_step": "Imported project loaded" if completed else session.status.value,
          "columns_discovered": len(session.columns or []),
      }

  async def _handle_get_schema(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if session and session.columns:
          # Return a compact, agent-focused view (name/definition/rationale/
          # allowed_values) instead of the full model_dump. Dropping noise fields
          # (counts, timestamps, pending/value details) keeps the payload small so
          # the whole schema survives result-size limits and reaches the model
          # intact — a truncated schema is what makes the agent mis-handle edits.
          def _compact(col: ColumnInfo) -> dict[str, Any]:
              entry: dict[str, Any] = {"name": col.name}
              if col.display_name:
                  entry["display_name"] = col.display_name
              if col.definition:
                  entry["definition"] = col.definition
              if col.rationale:
                  entry["rationale"] = col.rationale
              if col.allowed_values:
                  entry["allowed_values"] = col.allowed_values
              return entry

          return {
              "query": session.schema_query or "",
              "column_count": len(session.columns),
              "column_names": [col.name for col in session.columns],
              "schema": [_compact(col) for col in session.columns],
              "observation_unit": (
                  session.observation_unit.model_dump() if session.observation_unit else None
              ),
          }
      return await schematiq_runner.get_schema(session_id)

  async def _handle_get_observation_unit(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if session and session.observation_unit:
          return {"observation_unit": session.observation_unit.model_dump()}
      schema = await schematiq_runner.get_schema(session_id)
      unit = schema.get("observation_unit")
      if not unit:
          return {"message": "No observation unit defined yet."}
      return {"observation_unit": unit}

  async def _handle_edit_observation_unit(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      current = session.observation_unit
      name = args.get("name")
      if not name:
          raise ValueError("name is required")
      definition = args.get("definition")
      if not definition and current:
          definition = current.definition
      if not definition or len(definition.strip()) < 10:
          raise ValueError(
              "definition must be at least 10 characters. "
              "Call get_observation_unit first or provide a full definition."
          )
      example_names = args.get("example_names")
      if example_names is None and current:
          example_names = current.example_names
      result = await observation_unit_manager.update_observation_unit_definition(
          session_id=session_id,
          name=name,
          definition=definition.strip(),
          example_names=example_names,
      )
      return {
          "status": "success",
          "message": f"Observation unit updated to '{name}'.",
          "observation_unit": result["observation_unit"],
          "warning": (
              "Existing rows were extracted with the previous definition. "
              "Consider re-extraction if the row entity meaning changed."
          ),
      }

  async def _handle_preview_data(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      offset = int(args.get("offset", 0))
      limit = int(args.get("limit", 10))
      page = offset // max(limit, 1)
      data = await query_get_data(session_id, WORK_DIR, page=page, page_size=limit)
      return data.model_dump()

  async def _handle_get_validation(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      errors: list[str] = []
      warnings: list[str] = []
      missing_definitions: list[str] = []
      column_names = [col.name for col in session.columns]
      if len(column_names) != len(set(column_names)):
          errors.append("Duplicate column names found")
      for col in session.columns:
          if not col.definition or not col.definition.strip():
              missing_definitions.append(col.name)
              warnings.append(f"Column '{col.name}' has no definition")
          if len(col.name) < 3:
              warnings.append(f"Column '{col.name}' has a very short name")
      return {
          "is_valid": len(errors) == 0,
          "errors": errors,
          "warnings": warnings,
          "column_count": len(session.columns),
          "missing_definitions": missing_definitions,
      }

  async def _load_table_rows(self, session_id: str) -> list[dict[str, Any]]:
      # Canonical, deduplicated row list — the same source preview_data uses — so
      # summaries and row searches agree with what the user sees. Cell values are
      # kept raw (answer-dicts, including _confirmed_empty), which the coverage
      # breakdown relies on. Paginated to avoid materializing huge tables at once.
      from app.services.chat.deps import WORK_DIR

      rows: list[dict[str, Any]] = []
      page = 0
      page_size = 200
      while page < 500:  # hard cap: 100k rows
          data = await query_get_data(session_id, WORK_DIR, page=page, page_size=page_size)
          batch = [r.model_dump() for r in data.rows]
          rows.extend(batch)
          if len(batch) < page_size:
              break
          page += 1
      return rows

  async def _handle_data_summary(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Read-only table-health overview: row count and, per column, how many cells
      # are filled vs. empty (with the confirmed-empty subset broken out, since
      # those are resolved gaps that filling will not help). Makes extract_cells /
      # reprocess actionable by showing where the gaps are.
      from app.services.reextraction_service import _is_empty_cell_value

      session = session_manager.get_session(session_id)
      column_names = (
          [c.name for c in session.columns if c.name]
          if session and session.columns
          else []
      )
      rows = await self._load_table_rows(session_id)
      total = len(rows)

      if not column_names:  # fall back to columns seen in the data
          seen: list[str] = []
          for row in rows:
              for key in (row.get("data") or {}):
                  if key not in seen:
                      seen.append(key)
          column_names = seen

      counts = {c: {"filled": 0, "empty": 0, "confirmed_empty": 0} for c in column_names}
      for row in rows:
          data = row.get("data") or {}
          for col in column_names:
              value = data.get(col)
              if _is_empty_cell_value(value):
                  counts[col]["empty"] += 1
                  if isinstance(value, dict) and value.get("_confirmed_empty"):
                      counts[col]["confirmed_empty"] += 1
              else:
                  counts[col]["filled"] += 1

      columns = []
      for col in column_names:
          c = counts[col]
          coverage = round(100 * c["filled"] / total) if total else 0
          columns.append(
              {
                  "column": col,
                  "filled": c["filled"],
                  "empty": c["empty"],
                  "confirmed_empty": c["confirmed_empty"],
                  "coverage_pct": coverage,
              }
          )

      skipped = (
          len(session.statistics.skipped_documents)
          if session and session.statistics and session.statistics.skipped_documents
          else 0
      )
      return {
          "total_rows": total,
          "columns": columns,
          "skipped_documents": skipped,
      }

  async def _handle_find_rows(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Read-only: return the rows whose cell in `column` matches a predicate, so
      # the agent can target rename_unit / merge_units / update_cell / extract_cells
      # precisely instead of paging the whole table.
      from app.services.reextraction_service import _is_empty_cell_value

      column = (args.get("column") or "").strip()
      if not column:
          raise ValueError("'column' is required.")
      match = (args.get("match") or "empty").strip().lower()
      if match not in ("empty", "filled", "equals", "contains"):
          raise ValueError("match must be one of: empty, filled, equals, contains.")
      value = args.get("value")
      if match in ("equals", "contains") and not (isinstance(value, str) and value.strip()):
          raise ValueError(f"match '{match}' requires a non-empty 'value'.")
      try:
          limit = int(args.get("limit") or 50)
      except (TypeError, ValueError):
          limit = 50
      limit = max(1, min(limit, 500))

      def cell_text(v: Any) -> str:
          if isinstance(v, dict):
              answer = v.get("answer")
              return answer if isinstance(answer, str) else ("" if answer is None else str(answer))
          if v is None:
              return ""
          return v if isinstance(v, str) else str(v)

      needle = value.strip().lower() if isinstance(value, str) else ""
      rows = await self._load_table_rows(session_id)
      matches: list[dict[str, Any]] = []
      total_matched = 0
      for row in rows:
          v = (row.get("data") or {}).get(column)
          if match == "empty":
              ok = _is_empty_cell_value(v)
          elif match == "filled":
              ok = not _is_empty_cell_value(v)
          elif match == "equals":
              ok = cell_text(v).strip().lower() == needle
          else:  # contains
              ok = needle in cell_text(v).lower()
          if ok:
              total_matched += 1
              if len(matches) < limit:
                  matches.append(
                      {
                          "row": row.get("row_name") or row.get("unit_name"),
                          "value": cell_text(v) or None,
                      }
                  )

      return {
          "column": column,
          "match": match,
          "value": value,
          "count": total_matched,
          "returned": len(matches),
          "rows": matches,
          "truncated": total_matched > len(matches),
      }

  async def _handle_list_skipped_documents(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Surface documents that produced no rows during extraction, with the
      # recorded reason. Neither reextract nor reprocess re-includes these, so the
      # agent needs this to explain a "missing" document instead of proposing a
      # re-run that cannot bring it back.
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      skipped = (
          session.statistics.skipped_documents
          if session.statistics and session.statistics.skipped_documents
          else []
      )
      return {
          "status": "success",
          "count": len(skipped),
          "skipped_documents": [
              {"document": entry.document, "reason": entry.reason}
              for entry in skipped
          ],
      }

  async def _handle_list_documents(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Report the source documents that define rows and their availability. Uses
      # the same discovery the re-extraction precheck relies on, compacted to
      # names-by-status so the payload stays small.
      availability = await reextraction_service.precheck_document_availability(session_id)
      # Skipped documents produce no rows, so they never appear in the row-based
      # availability lists above. Surface them here (with reasons) so a document
      # that was skipped during extraction is discoverable from the primary
      # document listing instead of looking absent from the project.
      session = session_manager.get_session(session_id)
      skipped = (
          session.statistics.skipped_documents
          if session and session.statistics and session.statistics.skipped_documents
          else []
      )
      return {
          "status": "success",
          "total_documents": availability.get("total_documents", 0),
          "total_rows": availability.get("total_rows", 0),
          "local_documents": [
              doc["name"] for doc in availability.get("local_documents", [])
          ],
          "cloud_documents": [
              doc["name"] for doc in availability.get("cloud_documents", [])
          ],
          "missing_documents": [
              doc["name"] for doc in availability.get("missing_documents", [])
          ],
          "rows_with_missing_docs": availability.get("rows_with_missing_docs", 0),
          "skipped_documents": [
              {"document": entry.document, "reason": entry.reason}
              for entry in skipped
          ],
      }

  async def _handle_list_reference_sources(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      from app.services import reference_document_service as refsvc

      refs = refsvc.list_reference_documents(session)
      return {
          "status": "success",
          "count": len(refs),
          "reference_sources": [
              {
                  "id": ref.id,
                  "filename": ref.filename,
                  "char_count": ref.char_count,
                  "truncated": ref.truncated,
              }
              for ref in refs
          ],
      }

  async def _handle_read_reference_source(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      reference_id = (args.get("reference_id") or "").strip()
      if not reference_id:
          raise ValueError("reference_id is required")
      from app.services import reference_document_service as refsvc

      ref = refsvc.get_reference_document(session, reference_id)
      if not ref:
          raise ValueError(f"Reference document '{reference_id}' not found")
      full_text = await refsvc.load_reference_text(session_id, ref)
      # Cap the returned text so the whole result stays within the chat
      # tool-result budget. truncate_result drops an oversized string value
      # outright, which would hand the model an empty body; clip it here instead
      # and flag that more text exists. The full document is still used during
      # value extraction.
      content = full_text
      clipped = len(content) > READ_REFERENCE_CHAT_BUDGET
      if clipped:
          content = content[:READ_REFERENCE_CHAT_BUDGET]
      result = {
          "status": "success",
          "id": ref.id,
          "filename": ref.filename,
          "char_count": ref.char_count,
          "content_clipped": clipped,
          "content": content,
      }
      if clipped:
          result["hint"] = (
              f"Only the first {READ_REFERENCE_CHAT_BUDGET} of {ref.char_count} "
              "characters are shown. Do NOT fill a whole column from this preview: "
              "rows not shown here would be guesses. To populate a column for all "
              "rows from the full reference, call fill_column_from_reference."
          )
      return result

  async def _handle_fill_column_from_reference(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Delegate to the background fill service: it runs one model call per row and
      # streams cells as they complete, returning immediately so a whole-column
      # fill never blocks (or times out) the chat turn. `rows` optionally scopes the
      # fill to specific observation-unit / row names.
      return await reference_fill_service.start_fill(
          session_id,
          args.get("column"),
          args.get("reference_id"),
          rows=args.get("rows"),
          only_empty=args.get("only_empty", False),
      )

  async def _handle_explain_cell(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Read-only: classify one cell so the agent knows whether filling would help.
      from app.services.data_utils import (
          get_extraction_column_value,
          resolve_session_data_files,
      )
      from app.services.reextraction_service import _is_empty_cell_value

      row_name = (args.get("row") or "").strip()
      column = (args.get("column") or "").strip()
      if not row_name or not column:
          raise ValueError("Both 'row' and 'column' are required.")

      def _row_id(row: dict[str, Any]) -> Optional[str]:
          return row.get("_row_name") or row.get("row_name") or row.get("_unit_name")

      def _papers(row: dict[str, Any]) -> list[str]:
          return row.get("_papers") or row.get("papers") or []

      data_files = await resolve_session_data_files(session_id)
      matched: Optional[dict[str, Any]] = None
      for path in data_files:
          try:
              with open(path, "r") as handle:
                  for line in handle:
                      if not line.strip():
                          continue
                      try:
                          row = json.loads(line)
                      except json.JSONDecodeError:
                          continue
                      if _row_id(row) == row_name:
                          matched = row
                          break
          except OSError:
              continue
          if matched is not None:
              break

      documents = [Path(p).stem for p in _papers(matched)] if matched else []

      # Was the owning document skipped entirely?
      session = session_manager.get_session(session_id)
      skipped = {}
      if session and session.statistics and session.statistics.skipped_documents:
          skipped = {
              Path(s.document).stem: s.reason
              for s in session.statistics.skipped_documents
          }

      if matched is None:
          skip_hit = next((skipped[d] for d in documents if d in skipped), None)
          if documents and skip_hit is not None:
              return {
                  "row": row_name,
                  "column": column,
                  "state": "document_skipped",
                  "reason": skip_hit,
                  "documents": documents,
              }
          return {
              "row": row_name,
              "column": column,
              "state": "row_not_found",
              "message": (
                  "No row with that name was found. Check the name via preview_data."
              ),
          }

      value = get_extraction_column_value(matched, column)
      if not _is_empty_cell_value(value):
          excerpts = value.get("excerpts") if isinstance(value, dict) else None
          answer = value.get("answer") if isinstance(value, dict) else value
          return {
              "row": row_name,
              "column": column,
              "state": "has_value",
              "value": answer,
              "excerpts": excerpts or [],
              "documents": documents,
          }

      confirmed = isinstance(value, dict) and bool(value.get("_confirmed_empty"))
      skip_hit = next((skipped[d] for d in documents if d in skipped), None)
      if skip_hit is not None:
          return {
              "row": row_name,
              "column": column,
              "state": "document_skipped",
              "reason": skip_hit,
              "documents": documents,
          }
      return {
          "row": row_name,
          "column": column,
          "state": "confirmed_empty" if confirmed else "not_extracted",
          "message": (
              "The model inspected the source for this cell and found no value; "
              "re-extraction is unlikely to fill it."
              if confirmed
              else "No extraction has produced a value yet; extract_cells can fill it."
          ),
          "documents": documents,
      }

  async def _handle_add_column(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      definition = args["definition"]
      rationale = args.get("rationale", "") or ""
      extraction_strategy = _extraction_strategy(args)
      name, display_name = canonicalize_column_name(args["name"])
      if not name:
          raise ValueError("Column name cannot be empty")
      for col in session.columns:
          if col.name == name:
              raise ValueError(f"Column '{name}' already exists")
      new_column = ColumnInfo(
          name=name,
          display_name=display_name,
          definition=definition,
          rationale=rationale,
          extraction_strategy=extraction_strategy,
      )
      session.columns.append(new_column)
      session.modification_history.append(
          ModificationAction(
              action_type="column_added",
              column_name=name,
              details={
                  "definition": definition,
                  "rationale": rationale,
                  "extraction_strategy": extraction_strategy,
              },
          )
      )
      session.metadata.last_modified = datetime.now()
      session_manager.update_session(session)
      await websocket_manager.broadcast_schema_updated(
          session_id,
          {
              "operation": "add_column",
              "column": new_column.model_dump(),
              "columns": [col.model_dump() for col in session.columns],
          },
      )
      return {
          "status": "success",
          "message": f"Column '{name}' added.",
          "column": new_column.model_dump(),
      }

  async def _handle_edit_column(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      old_name = args["old_name"]
      definition = args.get("definition")
      rationale = args.get("rationale")
      extraction_strategy = (
          _extraction_strategy(args) if "extraction_strategy" in args else None
      )
      new_name: Optional[str] = None
      new_display_name: Optional[str] = None
      if args.get("new_name"):
          new_name, new_display_name = canonicalize_column_name(args["new_name"])
          if not new_name:
              raise ValueError("Column name cannot be empty")
          if new_name != old_name and any(col.name == new_name for col in session.columns):
              raise ValueError(f"Column '{new_name}' already exists")
      column_found = False
      for col in session.columns:
          if col.name == old_name:
              if new_name:
                  if session.schema_baseline and old_name in session.schema_baseline.columns:
                      old_baseline = session.schema_baseline.columns.pop(old_name)
                      session.schema_baseline.columns[new_name] = old_baseline
                  col.name = new_name
                  col.display_name = new_display_name
              if definition is not None:
                  col.definition = definition
              if rationale is not None:
                  col.rationale = rationale
              if extraction_strategy is not None:
                  col.extraction_strategy = extraction_strategy
              column_found = True
              break
      if not column_found:
          raise ValueError(f"Column '{old_name}' not found")
      if (
          new_name is None
          and definition is None
          and rationale is None
          and extraction_strategy is None
      ):
          raise ValueError(
              "Nothing to update: provide new_name, definition, rationale, "
              "and/or extraction_strategy."
          )
      session.modification_history.append(
          ModificationAction(
              action_type="column_edited",
              column_name=new_name or old_name,
              details={
                  "original_name": old_name,
                  "new_name": new_name,
                  "definition_changed": definition is not None,
                  "rationale_changed": rationale is not None,
                  "extraction_strategy_changed": extraction_strategy is not None,
              },
          )
      )
      session.metadata.last_modified = datetime.now()
      session_manager.update_session(session)
      if new_name:
          try:
              await data_editor.rename_column(session_id, old_name, new_name)
          except FileNotFoundError:
              # Expected on storage backends with no local data file yet (e.g. a
              # freshly created session, or the Supabase backend where this path
              # no-ops). The schema rename already succeeded above; the data-file
              # rename is best-effort. Log rather than swallow silently so a real
              # failure is visible in diagnostics.
              logger.warning(
                  "rename_column: no data file to rename for session %s (%s -> %s); "
                  "schema updated, data rename skipped.",
                  session_id, old_name, new_name,
              )
      await websocket_manager.broadcast_schema_updated(
          session_id,
          {
              "operation": "edit_column",
              "old_name": old_name,
              "new_name": new_name,
              "columns": [col.model_dump() for col in session.columns],
          },
      )
      changed: list[str] = []
      if new_name:
          changed.append("name")
      if definition is not None:
          changed.append("definition")
      if rationale is not None:
          changed.append("rationale")
      if extraction_strategy is not None:
          changed.append("extraction_strategy")
      changed_label = ", ".join(changed) if changed else "no fields"
      return {
          "status": "success",
          "message": f"Column '{new_name or old_name}' updated ({changed_label}).",
          "reprocessing": False,
      }

  async def _handle_delete_column(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      column_name = args["column_name"]
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      original_len = len(session.columns)
      session.columns = [col for col in session.columns if col.name != column_name]
      if len(session.columns) == original_len:
          raise ValueError(f"Column '{column_name}' not found")
      session.modification_history.append(
          ModificationAction(
              action_type="column_deleted",
              column_name=column_name,
              details={},
          )
      )
      session.metadata.last_modified = datetime.now()
      session_manager.update_session(session)
      await schema_manager.remove_column_data(session_id, column_name)
      await websocket_manager.broadcast_schema_updated(
          session_id,
          {
              "operation": "delete_column",
              "column_name": column_name,
              "columns": [col.model_dump() for col in session.columns],
              "refresh_data": True,
          },
      )
      return {"status": "success", "message": f"Column '{column_name}' deleted."}

  async def _handle_merge_columns(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      column_a = args["column_a"]
      column_b = args["column_b"]
      source_columns = [column_a, column_b]
      for col_name in source_columns:
          if not any(col.name == col_name for col in session.columns):
              raise ValueError(f"Column '{col_name}' not found")
      target_name, target_display_name = canonicalize_column_name(
          args.get("target_name") or column_a
      )
      if not target_name:
          raise ValueError("Target column name cannot be empty")
      if target_name not in source_columns and any(
          col.name == target_name for col in session.columns
      ):
          raise ValueError(f"Target column '{target_name}' already exists")
      merged_column = ColumnInfo(
          name=target_name,
          display_name=target_display_name,
          definition=f"Merged from: {', '.join(source_columns)}",
          rationale="Merged via chat tool",
          data_type="text",
      )
      session.columns = [col for col in session.columns if col.name not in source_columns]
      session.columns.append(merged_column)
      session.metadata.last_modified = datetime.now()
      session_manager.update_session(session)
      await schema_manager.merge_column_data(
          session_id, source_columns, target_name, "concatenate", " | "
      )
      await websocket_manager.broadcast_schema_updated(
          session_id,
          {
              "operation": "merge_columns",
              "source_columns": source_columns,
              "target_column": target_name,
              "columns": [col.model_dump() for col in session.columns],
              "refresh_data": True,
          },
      )
      return {
          "status": "success",
          "message": f"Merged {source_columns} into '{target_name}'.",
      }

  async def _handle_update_cell(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      row_name = args["row"]
      column = args["column"]
      value = args["value"]
      # Prefer an explicit source_document from the agent (needed to disambiguate
      # same-named units in different documents); otherwise resolve it server-side.
      source_document = args.get("source_document") or await self._resolve_source_document(
          session_id, row_name
      )
      # If the value came from a reference document, resolve its filename so the
      # cell can be attributed to it.
      reference_source = None
      reference_id = (args.get("reference_id") or "").strip()
      if reference_id:
          session = session_manager.get_session(session_id)
          if session:
              from app.services import reference_document_service as refsvc

              ref = refsvc.get_reference_document(session, reference_id)
              if ref:
                  reference_source = ref.filename
      result = await data_editor.update_cell(
          session_id, row_name, column, value, source_document=source_document,
          reference_source=reference_source,
      )
      # Stream the write so the cell appears in the table as it is written, rather
      # than only after the whole chat turn completes (the workspace refreshes on
      # cell_extracted). Best-effort — a streaming hiccup must never fail the write.
      try:
          await websocket_manager.broadcast_to_session(
              session_id,
              {"type": "cell_extracted", "data": {"row": row_name, "column": column}},
          )
      except Exception:  # streaming is best-effort
          pass
      return result

  async def _resolve_source_document(self, session_id: str, row_name: str) -> Optional[str]:
      data_file = None
      for candidate in (
          WORK_DIR / session_id / "extracted_data.jsonl",
          WORK_DIR / session_id / "data.jsonl",
          Path(DEFAULT_DATA_DIR) / session_id / "data.jsonl",
      ):
          if candidate.exists():
              data_file = candidate
              break
      if not data_file:
          return None
      matches: list[str] = []
      with open(data_file, encoding="utf-8") as handle:
          for line in handle:
              if not line.strip():
                  continue
              row = json.loads(line)
              current_name = row.get("row_name") or row.get("_row_name")
              if current_name != row_name:
                  continue
              src = _resolve_source_document(row)
              if src:
                  matches.append(src)
      unique = list(dict.fromkeys(matches))
      if len(unique) == 1:
          return unique[0]
      return None

  async def _handle_add_unit(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      return await observation_unit_manager.add_observation_unit(
          session_id=session_id,
          unit_name=args["unit_name"],
      )

  async def _handle_remove_unit(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      return await observation_unit_manager.remove_observation_unit(
          session_id=session_id,
          unit_name=args["unit_name"],
      )

  async def _handle_merge_units(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Wire the existing By Unit view merge (unit_view_service.merge_units) to the
      # chat. It relabels the source rows to a single target unit name (strategy
      # 'rename'), keeping their data. The service is synchronous and touches the
      # storage backend, so run it off the event loop.
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      raw_units = args.get("units") or []
      units = [u.strip() for u in raw_units if isinstance(u, str) and u.strip()]
      if len(units) < 2:
          raise ValueError("merge_units needs at least two distinct row names in 'units'.")
      target = (args.get("target_name") or units[0]).strip()
      if not target:
          raise ValueError("Target unit name cannot be empty.")

      from app.models.unit import MergeUnitsRequest
      from app.services.unit_view_service import unit_view_service

      request = MergeUnitsRequest(source_units=units, target_unit=target, strategy="rename")
      loop = asyncio.get_running_loop()
      response = await loop.run_in_executor(
          None, unit_view_service.merge_units, session_id, request
      )

      if response.success and response.rows_affected:
          # Data-only change: row_completed makes the workspace silently re-fetch
          # rows. Best-effort — a streaming hiccup must never fail the merge.
          try:
              await websocket_manager.broadcast_row_completed(
                  session_id,
                  {"operation": "merge_units", "target_unit": target},
              )
          except Exception:  # streaming is best-effort
              pass

      return {
          "status": "success" if response.success else "error",
          "message": response.message,
          "target_unit": target,
          "rows_affected": response.rows_affected,
      }

  async def _handle_rename_unit(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Rename one row / observation unit in place via unit_view_service.rename_unit,
      # which reuses the merge relabel path (keeps data, records _original_units).
      # The service is synchronous and touches storage, so run it off the loop.
      old_name = (args.get("old_name") or "").strip()
      new_name = (args.get("new_name") or "").strip()
      if not old_name or not new_name:
          raise ValueError("Both 'old_name' and 'new_name' are required.")

      from app.services.unit_view_service import unit_view_service

      loop = asyncio.get_running_loop()
      response = await loop.run_in_executor(
          None, unit_view_service.rename_unit, session_id, old_name, new_name
      )

      if response.success and response.rows_affected:
          # Data-only change: row_completed makes the workspace silently re-fetch rows.
          try:
              await websocket_manager.broadcast_row_completed(
                  session_id,
                  {"operation": "rename_unit", "old_name": old_name, "new_name": new_name},
              )
          except Exception:  # streaming is best-effort
              pass

      return {
          "status": "success" if response.success else "error",
          "message": response.message,
          "old_name": old_name,
          "new_name": new_name,
          "rows_affected": response.rows_affected,
      }

  async def _handle_export_table(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      fmt = args.get("format", "csv")
      paths = {
          "csv": f"/api/schematiq/export/{session_id}",
          "rich": f"/api/schematiq/export-rich-csv/{session_id}",
          "schema": f"/api/schematiq/export-schema/{session_id}",
      }
      if session_mode == "load":
          paths = {
              "csv": f"/api/load/export/{session_id}",
              "rich": f"/api/load/export-rich-csv/{session_id}",
              "schema": f"/api/load/export-schema/{session_id}",
          }
      path = paths.get(fmt)
      if not path:
          raise ValueError(f"Unsupported export format: {fmt}")
      data = await query_get_data(session_id, WORK_DIR, page=0, page_size=1)
      return {
          "status": "success",
          "format": fmt,
          "download_path": path,
          "total_rows": data.total_count,
          "message": f"Export ready ({fmt}, {data.total_count} rows).",
      }

  async def _handle_run_schematiq(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not has_live_pipeline(session):
          raise ValueError("ScheMatiQ session not found")
      if not DEVELOPER_MODE:
          schematiq_runner.check_global_quota(LLM_CALL_GLOBAL_LIMIT)
      await concurrency_limiter.acquire(session_id, "schematiq")
      asyncio.create_task(self._run_schematiq_task(session_id))
      return {"status": "started", "message": "ScheMatiQ execution started."}

  async def _run_schematiq_task(self, session_id: str) -> None:
      try:
          await schematiq_runner.run_schematiq(session_id)
      except Exception as exc:
          logger.error("Chat-triggered ScheMatiQ run failed for %s: %s", session_id, exc)

  async def _handle_reextract(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Funnel through the same gated entry point as the manual workspace
      # button: scope resolution (explicit / all / edited_only), baseline
      # capture, document precheck, then start. Keeps both routes identical.
      return await reextraction_service.start_gated_reextraction_guarded(
          session_id,
          columns=args.get("columns"),
          scope=args.get("scope", "edited_only"),
          only_empty=args.get("only_empty", False),
      )

  async def _handle_extract_cells(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Granular fill: route through the same gated re-extraction path, but scoped
      # to specific documents / rows / columns and (by default) only empty cells.
      # With no columns given, consider all columns (scope='all') and let
      # only_empty / the document+row scope narrow the actual work.
      columns = args.get("columns")
      scope = "explicit" if columns else "all"
      return await reextraction_service.start_gated_reextraction_guarded(
          session_id,
          columns=columns,
          scope=scope,
          documents=args.get("documents"),
          rows=args.get("rows"),
          only_empty=args.get("only_empty", True),
      )

  async def _handle_continue_discovery(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      document_source = (args.get("document_source") or "original").strip().lower()
      if document_source not in ("original", "upload"):
          raise ValueError(
              "document_source must be 'original' or 'upload'."
          )
      llm_config = load_user_llm_config(session_id)
      await concurrency_limiter.acquire(session_id, "continue_discovery")
      try:
          result = await continue_discovery_service.start_continue_discovery(
              session_id=session_id,
              document_source=document_source,
              llm_config=llm_config,
          )
      except Exception:
          await concurrency_limiter.release(session_id)
          raise
      return result

  async def _handle_reprocess(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Route through start_gated_reextraction so column scope uses
      # resolve_reextraction_columns (schema validation + _excerpt stripping),
      # same as the reextract tool. The legacy schema_manager.reprocess_documents
      # path only extracts when ./data/<session_id>/documents exists locally,
      # so on Supabase it silently no-ops; the gated path materializes sources
      # from storage and fails clearly when none are available.
      columns = args.get("columns")
      scope = args.get("scope", "all" if not columns else "edited_only")
      return await reextraction_service.start_gated_reextraction_guarded(
          session_id,
          operation_label="reprocess",
          columns=columns,
          scope=scope,
      )

  async def _handle_rediscover(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      # Full schema + observation-unit rediscovery from chat. The preparation
      # sequence (observation-unit + document gates, quota check, config
      # synthesis for imported sessions, pending_observation_unit_rediscovery
      # flag, prepare_resume) is shared with POST /load/rediscover via
      # prepare_rediscovery. This handler only translates the typed gate errors
      # into tool-facing ValueErrors and spawns the run as an asyncio task
      # (the route uses FastAPI BackgroundTasks). require_imported=False: unlike
      # the route, the chat tool rediscovers both imported and SCHEMATIQ sessions.
      from app.services.rediscovery_service import (
          prepare_rediscovery,
          RediscoverySessionNotFound,
          RediscoveryNoObservationUnit,
          RediscoveryDocumentsUnavailable,
          RediscoveryQuotaExceeded,
          RediscoveryPipelineBusy,
      )

      try:
          await prepare_rediscovery(
              session_id,
              runner=schematiq_runner,
              reextraction_service=reextraction_service,
              require_imported=False,
          )
      except RediscoverySessionNotFound:
          raise ValueError("Session not found.")
      except RediscoveryNoObservationUnit:
          raise ValueError(
              "This project has no observation unit configured. Set one "
              "(edit_observation_unit) before rediscovering."
          )
      except RediscoveryDocumentsUnavailable:
          raise ValueError(
              "No source documents are available for this project. Open a row and "
              'use "Show source document" to re-attach the original files, then '
              "try again."
          )
      except RediscoveryQuotaExceeded:
          raise ValueError(
              "The global usage limit has been reached. Please try again later."
          )
      except RediscoveryPipelineBusy:
          raise ValueError(
              "The pipeline is still stopping. Wait a few seconds and try again."
          )

      asyncio.create_task(self._run_schematiq_task(session_id))
      return {
          "status": "started",
          "message": (
              "Schema rediscovery started. The schema and rows are being rebuilt "
              "from the source documents under the current observation unit; "
              "previously-skipped documents are re-evaluated."
          ),
      }

  async def _handle_web_search(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      raise ValueError("Web search is not available yet.")

  async def _handle_create_project(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      raise ValueError("Use File > New Project in the workspace UI.")

  async def _handle_import_project(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      raise ValueError("Use File > Import Project in the workspace UI.")


tool_executor = ToolExecutor()
