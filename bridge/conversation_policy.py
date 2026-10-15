"""Strict optional contract for one fresh App Server turn per voice request."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .conversation_context import conversation_history
from .errors import ProtocolError

SINGLE_TURN_FIRST = "single_turn_first"
CONTEXT_CHOICES = frozenset({"independent", "follow_up", "question_answer", "explicit"})


def adaptive_request(payload: Mapping[str, Any]) -> bool:
    """Old clients omit this contract; unsupported modes never weaken it."""
    policy = payload.get("context_policy")
    if policy is None:
        return False
    selection = payload.get("context_selection")
    if (
        policy != SINGLE_TURN_FIRST
        or not isinstance(selection, str)
        or selection not in CONTEXT_CHOICES
    ):
        raise ProtocolError("Unsupported conversation context policy")
    text = payload.get("text")
    messages = payload.get("messages")
    if not isinstance(text, str) or not text.strip() or not isinstance(messages, list):
        raise ProtocolError("Single-turn request requires text and selected messages")
    if not messages or messages[-1] != {"role": "user", "content": text}:
        raise ProtocolError(
            "Single-turn request must end with the current user message"
        )
    if payload["context_selection"] == "independent":
        if messages != [{"role": "user", "content": text}]:
            raise ProtocolError("Independent request must not contain prior context")
    elif not conversation_history(payload, text):
        raise ProtocolError("Follow-up request has no selected context")
    request_id = payload.get("request_id")
    if (
        not isinstance(request_id, str)
        or len(request_id) != 32
        or any(char not in "0123456789abcdef" for char in request_id)
    ):
        raise ProtocolError("Single-turn request requires an opaque request ID")
    return True


def registered_tools(
    tools: list[dict[str, Any]], namespace: str | None = None
) -> set[tuple[str | None, str]]:
    """Never grant a provider tool just because it emitted a tool-call event."""
    allowed: set[tuple[str | None, str]] = set()
    for tool in tools:
        if tool.get("type") == "namespace":
            allowed.update(registered_tools(tool["tools"], tool["name"]))
        else:
            allowed.add((namespace, tool["name"]))
    return allowed
