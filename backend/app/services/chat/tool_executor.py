"""Execute chat tools by calling existing backend services."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from app.core.config import DEVELOPER_MODE, LLM_CALL_GLOBAL_LIMIT
from app.core.logging_utils import set_session_context
from app.models.modification import ModificationAction
from app.models.session import ColumnInfo, SessionType
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
      # fill never blocks (or times out) the chat turn.
      return await reference_fill_service.start_fill(
          session_id, args.get("column"), args.get("reference_id")
      )

  async def _handle_add_column(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      session = session_manager.get_session(session_id)
      if not session:
          raise ValueError("Session not found")
      definition = args["definition"]
      rationale = args.get("rationale", "") or ""
      name, display_name = canonicalize_column_name(args["name"])
      if not name:
          raise ValueError("Column name cannot be empty")
      for col in session.columns:
          if col.name == name:
              raise ValueError(f"Column '{name}' already exists")
      new_column = ColumnInfo(
          name=name, display_name=display_name, definition=definition, rationale=rationale
      )
      session.columns.append(new_column)
      session.modification_history.append(
          ModificationAction(
              action_type="column_added",
              column_name=name,
              details={"definition": definition, "rationale": rationale},
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
              column_found = True
              break
      if not column_found:
          raise ValueError(f"Column '{old_name}' not found")
      if new_name is None and definition is None and rationale is None:
          raise ValueError(
              "Nothing to update: provide new_name, definition, and/or rationale."
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
      return await data_editor.update_cell(
          session_id, row_name, column, value, source_document=source_document,
          reference_source=reference_source,
      )

  async def _resolve_source_document(self, session_id: str, row_name: str) -> Optional[str]:
      data_file = None
      for candidate in (
          WORK_DIR / session_id / "extracted_data.jsonl",
          WORK_DIR / session_id / "data.jsonl",
          Path("./data") / session_id / "data.jsonl",
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
      if not session or session.type != SessionType.SCHEMATIQ:
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
      await concurrency_limiter.acquire(session_id, "reextraction")
      try:
          result = await reextraction_service.start_gated_reextraction(
              session_id,
              columns=args.get("columns"),
              scope=args.get("scope", "edited_only"),
          )
      except Exception:
          await concurrency_limiter.release(session_id)
          raise
      return result

  async def _handle_continue_discovery(
      self, session_id: str, session_mode: str, args: dict[str, Any]
  ) -> dict[str, Any]:
      llm_config = load_user_llm_config(session_id)
      await concurrency_limiter.acquire(session_id, "continue_discovery")
      try:
          result = await continue_discovery_service.start_continue_discovery(
              session_id=session_id,
              document_source="original",
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
      await concurrency_limiter.acquire(session_id, "reprocess")
      try:
          result = await reextraction_service.start_gated_reextraction(
              session_id,
              columns=columns,
              scope=scope,
          )
      except Exception:
          await concurrency_limiter.release(session_id)
          raise
      return result

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
