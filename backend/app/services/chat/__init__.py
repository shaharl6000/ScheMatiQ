"""Chat agent services for workspace tool-calling."""

__all__ = [
    "ChatAgentService",
    "get_tools_for_context",
    "to_function_declarations",
    "to_public_tool_list",
]


def __getattr__(name: str):
    if name == "ChatAgentService":
        from .agent_service import ChatAgentService
        return ChatAgentService
    if name in {"get_tools_for_context", "to_function_declarations", "to_public_tool_list"}:
        from . import tool_registry
        return getattr(tool_registry, name)
    raise AttributeError(name)
