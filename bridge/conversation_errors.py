"""Safely classify Codex App Server conversation errors."""

from __future__ import annotations

import re
from collections.abc import Mapping

from .errors import RpcError

_QUOTA = re.compile(
    r"(?:usage[\s_-]*limit(?:[\s_-]*(?:reached|exceeded))?|quota[\s_-]*(?:exhausted|exceeded)|"
    r"insufficient[\s_-]*quota|exceeded (?:your )?current quota)",
    re.IGNORECASE,
)


def _provider_fields(value: object, depth: int = 0) -> list[str]:
    """Inspect only known error fields with a bounded recursion limit."""
    if depth >= 5:
        return []
    if isinstance(value, str):
        return [value[:2048]]
    if isinstance(value, Mapping):
        fields: list[str] = []
        for key in ("code", "type", "message", "error", "details", "turn"):
            if key in value:
                fields.extend(_provider_fields(value[key], depth + 1))
        return fields
    if isinstance(value, Exception):
        nested = getattr(value, "error", None)
        fields = _provider_fields(nested, depth + 1) if nested is not None else []
        return [*fields, str(value)[:2048]]
    return []


def quota_exhausted(value: object) -> bool:
    """Recognize usage quotas without confusing transient rate limiting."""
    return any(_QUOTA.search(field) for field in _provider_fields(value))


def provider_error(value: object) -> dict[str, str]:
    """Return a classified and redacted wire error."""
    if quota_exhausted(value):
        return {
            "type": "error",
            "code": "quota_exhausted",
            "error": "Codex usage limit reached",
        }
    return {"type": "error", "error": "Codex request failed"}


def bridge_error(value: Exception) -> dict[str, str]:
    """Preserve internal protocol detail, but redact provider failures."""
    if quota_exhausted(value) or isinstance(value, RpcError):
        return provider_error(value)
    return {"type": "error", "error": str(value)}
