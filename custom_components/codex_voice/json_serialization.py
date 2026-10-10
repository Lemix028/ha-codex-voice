"""Typed JSON normalization with content-free structural error locations."""

from __future__ import annotations

import json
import math
from collections import deque
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time
from enum import Enum
from pathlib import PurePath
from typing import Any, NoReturn
from uuid import UUID

import orjson

_SAFE_KEYS = frozenset(
    {
        "type",
        "conversation_id",
        "text",
        "language",
        "model",
        "effort",
        "service_tier",
        "instructions",
        "messages",
        "tools",
        "role",
        "content",
        "tool_calls",
        "tool_call_id",
        "tool_name",
        "result",
        "arguments",
        "name",
        "id",
        "request_id",
        "call_id",
        "success",
        "parameters",
        "description",
        "properties",
        "required",
        "items",
        "enum",
        "default",
        "anyOf",
        "allOf",
        "oneOf",
        "additionalProperties",
        "minimum",
        "maximum",
        "format",
        "correlation_id",
        "session_idle_seconds",
        "session_restored",
        "history_complete",
    }
)
_MAX_DEPTH = 64


class SerializationFailure(ValueError):
    """A safe path/type/reason, never the rejected value or its repr."""

    def __init__(self, path: str, value: Any, reason: str = "unsupported_type") -> None:
        """Keep only static type metadata and a redacted structural path."""
        self.path = path[:512]
        self.python_type = f"{type(value).__module__}.{type(value).__qualname__}"[:160]
        self.reason = reason
        super().__init__(
            f"JSON serialization failed at {self.path} ({self.python_type}; {reason})"
        )


def _child_path(path: str, key: Any, index: int) -> str:
    """Hide free-form dictionary keys while preserving protocol field names."""
    return (
        f"{path}.{key}"
        if isinstance(key, str) and key in _SAFE_KEYS
        else f"{path}[<key#{index}>]"
    )


def _fail(path: str, value: Any, reason: str = "unsupported_type") -> NoReturn:
    """Raise a content-free rejection without touching the value's repr."""
    raise SerializationFailure(path, value, reason)


def _json_string(value: str, path: str) -> str:
    """Reject invalid Unicode at its exact structural location."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        _fail(path, value, "invalid_unicode")
    return value


def _json_integer(value: int, path: str) -> int:
    """Match HA's orjson integer range rather than failing without a path."""
    if not -(2**63) <= value < 2**64:
        _fail(path, value, "integer_range")
    return value


def normalize_json(value: Any, *, path: str = "$") -> Any:
    """Normalize HA types; reject unknown objects, bytes, cycles and NaN.

    No str/repr fallback. HA's as_dict protocol and dataclasses are recursively
    handled; schema sentinels remain errors. Arbitrary keys never enter logs.
    """
    active: set[int] = set()

    def walk(item: Any, location: str, depth: int) -> Any:
        if depth > _MAX_DEPTH:
            raise SerializationFailure(location, item, "depth_limit")
        if isinstance(item, Enum):
            return walk(item.value, location, depth + 1)
        if item is None or isinstance(item, bool):
            return item
        if isinstance(item, str):
            return _json_string(item, location)
        if isinstance(item, int):
            return _json_integer(item, location)
        if isinstance(item, float):
            if not math.isfinite(item):
                raise SerializationFailure(location, item, "non_finite_number")
            return float(item)
        if isinstance(item, (datetime, date, time)):
            return item.isoformat()
        if isinstance(item, UUID):
            return str(item)
        if isinstance(item, PurePath):
            return item.as_posix()
        identity = id(item)
        if identity in active:
            raise SerializationFailure(location, item, "cycle")
        active.add(identity)
        try:
            if isinstance(item, Mapping):
                output: dict[str, Any] = {}
                for index, (key, child) in enumerate(item.items()):
                    key_location = f"{location}[<key#{index}>]"
                    normalized_key = walk(key, key_location, depth + 1)
                    if isinstance(normalized_key, str):
                        json_key = normalized_key
                    elif normalized_key is None or isinstance(
                        normalized_key, (bool, int, float)
                    ):
                        json_key = json.dumps(normalized_key, allow_nan=False)
                    else:
                        _fail(key_location, key, "unsupported_key")
                    if json_key in output:
                        _fail(key_location, key, "key_collision")
                    output[json_key] = walk(
                        child, _child_path(location, key, index), depth + 1
                    )
                return output
            if isinstance(item, (list, tuple, set, frozenset, deque)):
                output_list = [
                    walk(child, f"{location}[{index}]", depth + 1)
                    for index, child in enumerate(item)
                ]
                if isinstance(item, (set, frozenset)):
                    output_list.sort(
                        key=lambda child: json.dumps(
                            child, sort_keys=True, ensure_ascii=True, allow_nan=False
                        )
                    )
                return output_list
            if is_dataclass(item) and not isinstance(item, type):
                return walk(
                    {field.name: getattr(item, field.name) for field in fields(item)},
                    location,
                    depth + 1,
                )
            as_dict = getattr(item, "as_dict", None)
            if callable(as_dict):
                return walk(as_dict(), location, depth + 1)
            if isinstance(item, orjson.Fragment):
                return walk(json.loads(orjson.dumps(item)), location, depth + 1)
            _fail(location, item)
        except SerializationFailure:
            raise
        except (
            TypeError,
            ValueError,
            AttributeError,
            OverflowError,
            RuntimeError,
            KeyError,
            OSError,
        ):
            raise SerializationFailure(location, item, "conversion_failed") from None
        finally:
            active.remove(identity)

    return walk(value, path, 0)
