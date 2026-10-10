"""Tests for the Codex Voice conversation entity."""

from __future__ import annotations

from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import voluptuous as vol
from homeassistant.components import conversation
from homeassistant.config_entries import ConfigSubentry
from homeassistant.core import Context, HomeAssistant
from homeassistant.helpers import llm

from custom_components.codex_voice import CodexVoiceConfigEntry
from custom_components.codex_voice.api import BridgeToolCall
from custom_components.codex_voice.const import (
    CONF_MODEL,
    CONF_REASONING_EFFORT,
    CONF_SERVICE_TIER,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    LEGACY_CONVERSATION_SERVICE_TIER,
)
from custom_components.codex_voice.conversation import CodexVoiceConversationEntity

from .helpers import tool_result


def _make_entry(client: Any) -> CodexVoiceConfigEntry:
    """Create the minimal config-entry interface required by an entity."""
    return cast(
        "CodexVoiceConfigEntry",
        SimpleNamespace(
            runtime_data=client,
            async_start_reauth=Mock(),
            entry_id="entry-1",
        ),
    )


def _make_subentry() -> ConfigSubentry:
    """Create a conversation subentry."""
    return ConfigSubentry(
        data=MappingProxyType({CONF_MODEL: "gpt-test"}),
        subentry_type="conversation",
        title="Test conversation",
        unique_id=None,
    )


def _make_input() -> conversation.ConversationInput:
    """Create a conversation input."""
    return conversation.ConversationInput(
        text="Hello",
        context=Context(),
        conversation_id="conversation-1",
        device_id=None,
        satellite_id=None,
        language="en-US",
        agent_id="conversation.codex_voice",
    )


def _make_chat_log(hass: HomeAssistant) -> conversation.ChatLog:
    """Create a chat log containing the current user message."""
    return conversation.ChatLog(
        hass=hass,
        conversation_id="conversation-1",
        content=[
            conversation.SystemContent(content="System"),
            conversation.UserContent(content="Hello"),
        ],
    )


async def test_conversation_streams_assistant_text(hass: HomeAssistant) -> None:
    """Bridge text is recorded in ChatLog and returned as Assist speech."""
    captured: dict[str, Any] = {}

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
    ) -> dict[str, Any]:
        captured.update(start)
        await async_handle_delta("Hello from Codex")
        return {"type": "done"}

    client = SimpleNamespace(async_converse=converse)
    entity = CodexVoiceConversationEntity(_make_entry(client), _make_subentry())
    entity.entity_id = "conversation.codex_voice"
    entity.hass = hass
    chat_log = _make_chat_log(hass)

    with patch.object(chat_log, "async_provide_llm_data", new_callable=AsyncMock):
        result = await entity._async_handle_message(
            _make_input(),
            chat_log,
        )

    assert result.response.speech["plain"]["speech"] == "Hello from Codex"
    assert captured["model"] == "gpt-test"
    assert captured["effort"] == "low"
    assert captured["service_tier"] == LEGACY_CONVERSATION_SERVICE_TIER
    assert captured["messages"][-1] == {"role": "user", "content": "Hello"}


async def test_conversation_preserves_explicit_standard_service_tier(
    hass: HomeAssistant,
) -> None:
    """An explicitly selected standard tier is sent to the bridge."""
    captured: dict[str, Any] = {}

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
    ) -> dict[str, Any]:
        captured.update(start)
        await async_handle_delta("Okay")
        return {"type": "done"}

    client = SimpleNamespace(async_converse=converse)
    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_MODEL: "gpt-test", CONF_SERVICE_TIER: "standard"}),
        subentry_type="conversation",
        title="Test conversation",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(_make_entry(client), subentry)
    entity.entity_id = "conversation.codex_voice"
    entity.hass = hass
    chat_log = _make_chat_log(hass)

    with patch.object(chat_log, "async_provide_llm_data", new_callable=AsyncMock):
        await entity._async_handle_message(_make_input(), chat_log)

    assert captured["service_tier"] == "standard"


async def test_conversation_falls_back_from_legacy_unsupported_options(
    hass: HomeAssistant,
) -> None:
    """Older unsupported values cannot make an otherwise valid turn fail."""
    captured: dict[str, Any] = {}

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
    ) -> dict[str, Any]:
        captured.update(start)
        await async_handle_delta("Okay")
        return {"type": "done"}

    client = SimpleNamespace(async_converse=converse)
    subentry = ConfigSubentry(
        data=MappingProxyType(
            {
                CONF_MODEL: "gpt-test",
                CONF_REASONING_EFFORT: "none",
                CONF_SERVICE_TIER: "retired-tier",
            }
        ),
        subentry_type="conversation",
        title="Legacy conversation",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(_make_entry(client), subentry)
    entity.entity_id = "conversation.codex_voice"
    entity.hass = hass
    chat_log = _make_chat_log(hass)

    with patch.object(chat_log, "async_provide_llm_data", new_callable=AsyncMock):
        await entity._async_handle_message(_make_input(), chat_log)

    assert captured["effort"] == DEFAULT_CONVERSATION_REASONING_EFFORT
    assert captured["service_tier"] == LEGACY_CONVERSATION_SERVICE_TIER


async def test_conversation_executes_home_assistant_tool(
    hass: HomeAssistant,
) -> None:
    """Bridge tool calls execute through ChatLog's validated LLM API."""
    received_result: dict[str, Any] = {}

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
    ) -> dict[str, Any]:
        assert start["tools"][0]["name"] == "HassTurnOn"
        received_result.update(
            await async_handle_tool(
                BridgeToolCall(
                    call_id="call-1",
                    name="HassTurnOn",
                    arguments={"name": "Kitchen"},
                )
            )
        )
        await async_handle_delta("The kitchen light is on.")
        return {"type": "done"}

    tool = SimpleNamespace(
        name="HassTurnOn",
        description="Turn on an exposed Home Assistant entity",
        parameters=vol.Schema({vol.Required("name"): str}),
    )
    async_call_tool = AsyncMock(return_value=tool_result({"success": True}))
    llm_api = cast(
        "llm.APIInstance",
        SimpleNamespace(
            tools=[tool],
            custom_serializer=None,
            async_call_tool=async_call_tool,
        ),
    )
    client = SimpleNamespace(async_converse=converse)
    entity = CodexVoiceConversationEntity(_make_entry(client), _make_subentry())
    entity.entity_id = "conversation.codex_voice"
    entity.hass = hass
    chat_log = _make_chat_log(hass)
    chat_log.llm_api = llm_api

    with patch.object(chat_log, "async_provide_llm_data", new_callable=AsyncMock):
        result = await entity._async_handle_message(
            _make_input(),
            chat_log,
        )

    assert received_result == {"success": True}
    async_call_tool.assert_awaited_once()
    assert any(
        isinstance(item, conversation.ToolResultContent) for item in chat_log.content
    )
    assert result.response.speech["plain"]["speech"] == "The kitchen light is on."
