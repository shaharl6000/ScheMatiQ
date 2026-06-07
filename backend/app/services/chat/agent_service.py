"""Gemini chat agent with manual function-calling loop."""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, Optional

from schematiq.core.llm_call_tracker import LLMCallTracker, QuotaExceededError

from .deps import CHAT_MODEL, get_gemini_api_key
from .session_store import ChatSessionState, PendingToolCall, chat_session_store
from .tool_executor import tool_executor
from .tool_registry import (
    TOOL_BY_NAME,
    get_tools_for_context,
    to_function_declarations,
    tool_running_label,
)

logger = logging.getLogger(__name__)

MAX_TOOL_ITERATIONS = 10

CHAT_SYSTEM_PROMPT = """You are the ScheMatiQ workspace assistant.

Terminology (do not confuse these):
- Observation unit: the entity each table row represents (Observation Unit tab). Tools: get_observation_unit, edit_observation_unit.
- Schema columns: fields in the data table (Schema tab). Tools: get_schema, edit_column, add_column, delete_column.
- Table rows: individual instances of the observation unit. Tools: preview_data, update_cell, add_unit, remove_unit.

Rules:
- Call read tools before edits: get_schema for columns, get_observation_unit for the row entity definition.
- Never guess column names. If the user mentions the Observation Unit tab or what a row represents, use edit_observation_unit — not edit_column.
- After edit_observation_unit or schema definition changes (edit_column, add_column, delete_column), existing table values may be stale. Offer reextract to refresh values from documents, or run_schematiq / continue_discovery when the user wants schema rediscovery.
- Manual update_cell edits do not require re-extraction unless the user asks to repopulate from documents.
- Cheap tools run immediately. Expensive tools (reextract, reprocess, continue_discovery, run_schematiq) require user confirmation.
- Reply concisely in plain English after completing the requested work.
"""


class ChatAgentService:
    def __init__(self) -> None:
        self._executor = tool_executor
        self._genai_client: Any = None
        self._client_lock = threading.Lock()

    async def list_tools(
        self,
        session_id: Optional[str],
        session_mode: str,
    ) -> list[dict[str, Any]]:
        from .tool_registry import to_public_tool_list

        tools = get_tools_for_context(session_id, session_mode)
        return to_public_tool_list(tools)

    async def send_message(
        self,
        session_id: str,
        message: str,
        session_mode: str,
        chat_id: Optional[str] = None,
        pinned_tool: Optional[str] = None,
    ) -> dict[str, Any]:
        state = self._get_or_create_session(session_id, session_mode, chat_id, pinned_tool)
        outbound_messages: list[dict[str, Any]] = []
        user_text = message
        if pinned_tool:
            user_text = f"[User pinned tool: {pinned_tool}]\n{message}"

        try:
            result = await self._run_loop(state, user_text, outbound_messages)
            return {
                "chat_id": state.chat_id,
                "status": result["status"],
                "messages": outbound_messages,
                "pending_action": result.get("pending_action"),
            }
        except QuotaExceededError:
            outbound_messages.append(
                self._text_message(
                    "The system has reached its LLM processing capacity. Please try again later."
                )
            )
            return {
                "chat_id": state.chat_id,
                "status": "complete",
                "messages": outbound_messages,
            }
        except Exception as exc:
            if self._is_stale_chat_error(exc) and chat_id:
                logger.warning("Stale chat session %s, starting fresh: %s", chat_id, exc)
                chat_session_store.delete(chat_id)
                state = self._get_or_create_session(session_id, session_mode, None, pinned_tool)
                result = await self._run_loop(state, user_text, outbound_messages)
                return {
                    "chat_id": state.chat_id,
                    "status": result["status"],
                    "messages": outbound_messages,
                    "pending_action": result.get("pending_action"),
                }
            raise

    async def confirm_pending(
        self,
        session_id: str,
        chat_id: str,
    ) -> dict[str, Any]:
        state = chat_session_store.get(chat_id)
        if not state or state.workspace_session_id != session_id:
            raise ValueError("Chat session not found. Start a new conversation.")
        if not state.pending:
            raise ValueError("No pending action to confirm.")

        pending = state.pending
        state.pending = None
        outbound_messages: list[dict[str, Any]] = []
        outbound_messages.append(
            self._tool_log(pending.tool_name, "running", f"...{tool_running_label(pending.tool_name).lower()}")
        )
        try:
            tool_result = await self._executor.execute(
                pending.tool_name,
                session_id,
                state.session_mode,
                pending.args,
            )
            outbound_messages.append(
                self._tool_log(
                    pending.tool_name,
                    "done",
                    self._tool_done_message(pending.tool_name, tool_result),
                )
            )
            response = await self._send_function_response(state, pending, tool_result)
            loop_result = await self._continue_after_tool(state, response, outbound_messages)
            return {
                "chat_id": chat_id,
                "status": loop_result["status"],
                "messages": outbound_messages,
                "pending_action": loop_result.get("pending_action"),
            }
        except Exception as exc:
            outbound_messages.append(
                self._tool_log(pending.tool_name, "error", f"Tool failed: {exc}")
            )
            return {
                "chat_id": chat_id,
                "status": "complete",
                "messages": outbound_messages,
            }

    def _get_or_create_session(
        self,
        session_id: str,
        session_mode: str,
        chat_id: Optional[str],
        pinned_tool: Optional[str],
    ) -> ChatSessionState:
        if chat_id:
            existing = chat_session_store.get(chat_id)
            if existing and existing.workspace_session_id == session_id:
                existing.pinned_tool = pinned_tool
                return existing

        client, chat = self._create_gemini_chat(session_id, session_mode)
        state = ChatSessionState(
            client=client,
            chat=chat,
            workspace_session_id=session_id,
            session_mode=session_mode,
            pinned_tool=pinned_tool,
        )
        new_id = chat_session_store.create(state)
        state.chat_id = new_id
        return state

    @staticmethod
    def _is_stale_chat_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return "client has been closed" in message or "session not found" in message

    def _get_genai_client(self) -> Any:
        """Return a process-wide Gemini client (must outlive Chat sessions)."""
        if self._genai_client is None:
            with self._client_lock:
                if self._genai_client is None:
                    from google import genai

                    self._genai_client = genai.Client(api_key=get_gemini_api_key())
        return self._genai_client

    def _create_gemini_chat(self, session_id: str, session_mode: str) -> tuple[Any, Any]:
        from google.genai import types

        tools = get_tools_for_context(session_id, session_mode)
        declarations = to_function_declarations(tools)
        tool = types.Tool(function_declarations=declarations)
        config = types.GenerateContentConfig(
            tools=[tool],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(mode="AUTO"),
            ),
            system_instruction=CHAT_SYSTEM_PROMPT,
        )
        client = self._get_genai_client()
        chat = client.aio.chats.create(model=CHAT_MODEL, config=config)
        return client, chat

    async def _run_loop(
        self,
        state: ChatSessionState,
        user_text: str,
        outbound_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response = await self._send_user_message(state, user_text)
        return await self._continue_after_tool(state, response, outbound_messages)

    async def _continue_after_tool(
        self,
        state: ChatSessionState,
        response: Any,
        outbound_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        for _ in range(MAX_TOOL_ITERATIONS):
            function_calls = getattr(response, "function_calls", None) or []
            if function_calls:
                for function_call in function_calls:
                    tool_name = function_call.name
                    args = dict(function_call.args or {})
                    tool = TOOL_BY_NAME.get(tool_name)
                    if not tool:
                        raise ValueError(f"Unknown tool requested by model: {tool_name}")

                    logger.info(
                        "Chat tool selected: %s (cost=%s, session=%s)",
                        tool_name,
                        tool.cost_class,
                        state.workspace_session_id,
                    )

                    outbound_messages.append(
                        self._tool_log(tool_name, "running", f"...{tool_running_label(tool_name).lower()}")
                    )

                    if tool.cost_class == "expensive":
                        description = await self._executor.estimate_cost(
                            tool_name, state.workspace_session_id, args
                        )
                        state.pending = PendingToolCall(
                            tool_name=tool_name,
                            args=args,
                            function_call_part=function_call,
                        )
                        return {
                            "status": "pending_confirmation",
                            "pending_action": {
                                "tool_name": tool_name,
                                "label": tool_running_label(tool_name),
                                "description": description,
                                "args": args,
                            },
                        }

                    try:
                        tool_result = await self._executor.execute(
                            tool_name,
                            state.workspace_session_id,
                            state.session_mode,
                            args,
                        )
                    except Exception as exc:
                        tool_result = {
                            "error": str(exc),
                            "hint": self._tool_error_hint(tool_name),
                        }
                        outbound_messages.append(
                            self._tool_log(tool_name, "error", f"...{exc}")
                        )
                    else:
                        outbound_messages.append(
                            self._tool_log(
                                tool_name,
                                "done",
                                self._tool_done_message(tool_name, tool_result),
                                columns=self._affected_columns(tool_name, args),
                            )
                        )
                    response = await self._send_function_response(
                        state,
                        PendingToolCall(tool_name, args, function_call),
                        tool_result,
                    )
                continue

            text = (getattr(response, "text", None) or "").strip()
            if text:
                outbound_messages.append(self._text_message(text))
                return {"status": "complete"}

            break

        outbound_messages.append(
            self._text_message("I could not complete that request. Please try rephrasing.")
        )
        return {"status": "complete"}

    async def _send_user_message(self, state: ChatSessionState, user_text: str) -> Any:
        LLMCallTracker.get_instance().set_stage("chat")
        return await state.chat.send_message(user_text)

    async def _send_function_response(
        self,
        state: ChatSessionState,
        pending: PendingToolCall,
        tool_result: dict[str, Any],
    ) -> Any:
        from google.genai import types

        part = types.Part.from_function_response(
            name=pending.tool_name,
            response={"result": tool_result},
        )
        LLMCallTracker.get_instance().set_stage("chat")
        return await state.chat.send_message(part)

    def _text_message(self, content: str) -> dict[str, Any]:
        return {
            "id": str(uuid.uuid4()),
            "role": "assistant",
            "kind": "text",
            "content": content,
        }

    def _tool_log(
        self,
        tool_name: str,
        status: str,
        content: str,
        columns: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        message: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "role": "tool",
            "kind": "tool_log",
            "tool_name": tool_name,
            "tool_status": status,
            "content": content,
        }
        if columns:
            message["columns"] = columns
        return message

    @staticmethod
    def _affected_columns(tool_name: str, args: dict[str, Any]) -> list[str]:
        """Schema column(s) a successful edit touches, so the UI can scope the
        follow-up re-extract to exactly those columns (mirrors a manual edit)."""
        if tool_name == "add_column":
            name = args.get("name")
            return [name] if name else []
        if tool_name == "edit_column":
            name = args.get("new_name") or args.get("old_name")
            return [name] if name else []
        if tool_name == "merge_columns":
            name = args.get("target_name") or args.get("column_a")
            return [name] if name else []
        return []

    @staticmethod
    def _tool_error_hint(tool_name: str) -> str:
        if tool_name == "edit_column":
            return (
                "Call get_schema for exact column names. "
                "For Observation Unit tab changes use edit_observation_unit instead."
            )
        if tool_name == "edit_observation_unit":
            return "Call get_observation_unit first, then retry with name and definition (10+ chars)."
        return "Call the appropriate read tool first, then retry."

    def _tool_done_message(self, tool_name: str, result: dict[str, Any]) -> str:
        if "message" in result:
            return f"...{result['message']}"
        if tool_name == "get_schema":
            names = result.get("column_names") or [
                col.get("name") for col in result.get("schema", []) if col.get("name")
            ]
            count = len(names)
            preview = ", ".join(names[:8])
            suffix = f": {preview}" if preview else ""
            return f"...schema loaded ({count} columns{suffix})"
        if tool_name == "get_observation_unit" and result.get("observation_unit"):
            ou = result["observation_unit"]
            return f"...observation unit: {ou.get('name', 'unnamed')}"
        if tool_name == "edit_observation_unit" and result.get("observation_unit"):
            ou = result["observation_unit"]
            return f"...observation unit set to {ou.get('name', 'unnamed')}"
        if tool_name == "preview_data":
            total = result.get("total_count", 0)
            preview = len(result.get("rows", []))
            return f"...data preview loaded ({preview} of {total} rows)"
        if tool_name == "get_status":
            return f"...status: {result.get('status', 'unknown')}"
        return f"...{tool_name} completed"


chat_agent_service = ChatAgentService()
