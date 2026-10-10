"""Test doubles follow HA's old and new ToolResult contracts."""

from typing import Any

from homeassistant.helpers import llm


def tool_result(data: dict[str, Any]) -> Any:
    """Use a typed result in HA 2026.10, a plain result in older versions."""
    return (
        llm.ToolResult(data=data, error="error" in data)
        if hasattr(llm, "ToolResult")
        else data
    )
