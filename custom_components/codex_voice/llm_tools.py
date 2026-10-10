"""Shared Home Assistant LLM tool schema serialization."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from importlib.util import find_spec
from typing import Any, cast

import voluptuous as vol
from homeassistant.helpers import llm
from voluptuous_openapi import UNSUPPORTED, convert  # type: ignore[import-untyped]

from .api import normalize_conversation_value

_PROBATIO = import_module("probatio") if find_spec("probatio") else None


def _schema(parameters: Any, serializer: Callable[[Any], Any]) -> dict[str, Any]:
    """Use HA 2026.10's Probatio codec or the legacy Voluptuous codec."""
    native = _PROBATIO is not None and isinstance(parameters, _PROBATIO.Schema)
    unsupported = (
        _PROBATIO.UNSUPPORTED if _PROBATIO is not None and native else UNSUPPORTED
    )

    def adapter(value: Any) -> Any:
        converted = serializer(value)
        if (
            converted is None
            or converted is UNSUPPORTED
            or (_PROBATIO is not None and converted is _PROBATIO.UNSUPPORTED)
        ):
            return unsupported
        return converted

    if native:
        assert _PROBATIO is not None
        return cast(
            "dict[str, Any]",
            _PROBATIO.to_openapi(parameters, custom_serializer=adapter),
        )
    if isinstance(parameters, vol.Schema):
        return cast("dict[str, Any]", convert(parameters, custom_serializer=adapter))
    raise TypeError("Unsupported Home Assistant tool schema type")


def serialize_llm_tools(
    api_instance: llm.APIInstance | None,
) -> list[dict[str, Any]]:
    """Convert a captured Home Assistant LLM API to bridge-safe schemas."""
    if api_instance is None:
        return []
    tools = [
        {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": _schema(
                tool.parameters,
                serializer=(api_instance.custom_serializer or llm.selector_serializer),
            ),
        }
        for tool in api_instance.tools
    ]
    return cast(
        "list[dict[str, Any]]", normalize_conversation_value(tools, path="$.tools")
    )


def tool_result_data(result: object) -> dict[str, Any]:
    """Unwrap actual HA ToolResult objects without accepting arbitrary objects."""
    result_type = getattr(llm, "ToolResult", None)
    if result_type is not None and isinstance(result, result_type):
        if not isinstance(result.data, dict):
            raise TypeError("Home Assistant tool returned non-object data")
        data = dict(result.data)
        if result.error:
            data.setdefault("error", "tool_failed")
        return data
    if isinstance(result, dict):
        return dict(result)
    raise TypeError("Home Assistant tool returned a non-object")
