"""Diagnostics for Codex Voice."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.const import CONF_PROMPT
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant

from . import CodexVoiceConfigEntry
from .api import BridgeError
from .const import CONF_ACCESS_TOKEN, CONF_INSTRUCTIONS
from .metrics import DiagnosticBuffer

_TO_REDACT = {
    CONF_ACCESS_TOKEN,
    CONF_INSTRUCTIONS,
    CONF_PROMPT,
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "email",
    "refresh_token",
    "token",
    "session_fallback",
    "text",
    "transcript",
    "audio",
    "sdp",
    "messages",
    "tool_args",
    "tool_result",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: CodexVoiceConfigEntry,
) -> dict[str, Any]:
    """Return redacted config entry and bridge diagnostics."""
    try:
        health: dict[str, Any] = await entry.runtime_data.async_health()
    except BridgeError as err:
        health = {"error": type(err).__name__}

    diagnostics = getattr(entry.runtime_data, "conversation_diagnostics", None)
    return {
        "config_entry": async_redact_data(entry.as_dict(), _TO_REDACT),
        "bridge": _redact_nested(health),
        "recommended_stt_architecture": "home-assistant-wyoming-local",
        "codex_stt_adapter": "experimental-conversational-best-effort",
        "conversation_traces": diagnostics.export()
        if isinstance(diagnostics, DiagnosticBuffer)
        else [],
        "timing_scope": "conversation-api-boundary; null means not measurable here",
        "software": {"home_assistant": HA_VERSION},
        "hardware": {
            "haos_vm_ram_mb": None,
            "haos_vm_cpu_count": None,
            "echo_firmware": None,
            "wyoming_stt_version": None,
        },
    }


def _redact_nested(value: Any, key: str | None = None) -> Any:
    """Recursively redact secrets and account-identifying fields."""
    if key and any(secret in key.lower() for secret in _TO_REDACT):
        return "**REDACTED**"
    if isinstance(value, dict):
        return {
            item_key: _redact_nested(item_value, str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_nested(item) for item in value]
    return value
