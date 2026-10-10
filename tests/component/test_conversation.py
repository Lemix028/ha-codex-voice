"""Tests for the Codex Voice conversation entity."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
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
    CONF_SESSION_CONTINUITY,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    LEGACY_CONVERSATION_SERVICE_TIER,
)
from custom_components.codex_voice.conversation import CodexVoiceConversationEntity

from .helpers import tool_result


async def test_cancelled_turn_rotates_context_and_does_not_retry(
    hass: HomeAssistant,
) -> None:
    """T14: cancellation tombstones the binding without transport retry."""
    entered = asyncio.Event()
    client = SimpleNamespace(async_converse=AsyncMock())

    async def hang(*args: Any, **kwargs: Any) -> None:
        entered.set()
        await asyncio.Event().wait()

    client.async_converse.side_effect = hang
    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_SESSION_CONTINUITY: True}),
        subentry_type="conversation",
        title="Computer",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(_make_entry(client), subentry)
    entity.entity_id, entity.hass = "conversation.computer", hass
    user = replace(_make_input(), device_id="echo-a", conversation_id=None)
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        task = asyncio.create_task(entity.async_process(user))
        await entered.wait()
        sent_id = client.async_converse.call_args.args[0]["conversation_id"]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    client.async_converse.assert_awaited_once()
    assert entity._sessions.snapshot()["sessions"][0]["conversation_id"] != sent_id
    assert entity._diagnostics.export()[-1]["error"] == "cancelled_outcome_unknown"


async def test_unverified_local_action_requires_named_target(
    hass: HomeAssistant,
) -> None:
    """B05 fallback: local response prose does not verify the action target."""
    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_SESSION_CONTINUITY: True}),
        subentry_type="conversation",
        title="Computer",
        unique_id=None,
    )
    client = SimpleNamespace(async_converse=AsyncMock())
    entity = CodexVoiceConversationEntity(_make_entry(client), subentry)
    entity.entity_id, entity.hass = "conversation.computer", hass
    user = replace(
        _make_input(), device_id="echo-a", language="de-DE", text="Mach es wieder aus"
    )
    chat_log = _make_chat_log(hass)
    chat_log.content.insert(
        1,
        conversation.AssistantContent(
            agent_id=conversation.HOME_ASSISTANT_AGENT, content="Das Licht ist an."
        ),
    )
    owner, source = entity._sessions.identity(
        device_id="echo-a", satellite_id=None, user_id=None, incoming_id=None
    )
    async with entity._sessions.lease(owner) as session:
        token = entity._active_session.set((session, source))
        try:
            result = await entity._async_handle_message(user, chat_log)
        finally:
            entity._active_session.reset(token)
    assert result.response.speech["plain"]["speech"] == "Welches Gerät meinst du?"
    client.async_converse.assert_not_awaited()


async def test_tool_failure_stops_turn_without_model_retry(hass: HomeAssistant) -> None:
    """T11: an HA failure is terminal, including possibly completed side effects."""
    call_tool = AsyncMock(return_value=tool_result({"error": "TimeoutError"}))

    async def converse(
        start: dict[str, Any], *, async_handle_delta: Any, async_handle_tool: Any
    ) -> dict[str, Any]:
        await async_handle_tool(
            BridgeToolCall(call_id="call-1", name="HassTurnOn", arguments={})
        )
        pytest.fail("Failure must stop the model turn")

    entity = CodexVoiceConversationEntity(
        _make_entry(SimpleNamespace(async_converse=converse)), _make_subentry()
    )
    entity.entity_id, entity.hass = "conversation.computer", hass
    log = _make_chat_log(hass)
    log.llm_api = cast(
        "llm.APIInstance",
        SimpleNamespace(tools=[], custom_serializer=None, async_call_tool=call_tool),
    )
    with patch.object(log, "async_provide_llm_data", new_callable=AsyncMock):
        result = await entity._async_handle_message(
            replace(_make_input(), language="de-DE"), log
        )
    call_tool.assert_awaited_once()
    assert "könnte bereits erfolgt sein" in result.response.speech["plain"]["speech"]
    assert entity._diagnostics.export()[-1]["error"] == "tool_failed_outcome_unknown"


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


async def test_wakewords_reuse_device_session_without_replaying_messages(
    hass: HomeAssistant,
) -> None:
    """T04/T05/T07: real HA ChatLog semantics with changing incoming IDs."""

    payloads: list[dict[str, Any]] = []

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
        async_handle_started: Any,
    ) -> dict[str, Any]:
        payloads.append(start)
        await async_handle_started({"context_status": "reused"})
        await async_handle_delta("Synthetische Antwort")
        return {"type": "done"}

    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_MODEL: "gpt-test", CONF_SESSION_CONTINUITY: True}),
        subentry_type="conversation",
        title="Computer",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(
        _make_entry(
            SimpleNamespace(
                async_converse=converse, async_reset_conversation=AsyncMock()
            )
        ),
        subentry,
    )
    entity.entity_id, entity.hass = "conversation.computer", hass
    first = replace(
        _make_input(),
        device_id="echo-a",
        conversation_id=None,
        text="Wie viel Strom verbraucht mein NUC?",
    )
    second = replace(first, conversation_id=None, text="Und was kostet das im Monat?")
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        await entity.async_process(first)
        await entity.async_process(second)
        await entity.async_process(replace(first, device_id="echo-b"))
        await entity.async_process(replace(first, text="neues Gespräch"))
        await entity.async_process(replace(first, text="Was war meine vorige Frage?"))
    assert payloads[0]["conversation_id"] == payloads[1]["conversation_id"]
    assert payloads[2]["conversation_id"] != payloads[0]["conversation_id"]
    assert payloads[3]["conversation_id"] != payloads[0]["conversation_id"]
    assert payloads[3]["messages"] == [
        {"role": "user", "content": "Was war meine vorige Frage?"}
    ]
    assert payloads[1]["messages"][0]["content"] == first.text
    assert (
        sum(item.get("content") == second.text for item in payloads[1]["messages"]) == 1
    )
    assert payloads[0]["request_id"] != payloads[1]["request_id"]
    assert payloads[0]["session_idle_seconds"] == 900


async def test_lost_context_is_announced_after_metadata_only_restore(
    hass: HomeAssistant,
) -> None:
    """T06: never claim to remember a deleted model thread."""

    async def converse(
        start: dict[str, Any],
        *,
        async_handle_delta: Any,
        async_handle_tool: Any,
        async_handle_started: Any,
    ) -> dict[str, Any]:
        await async_handle_started({"context_status": "lost"})
        await async_handle_delta("Welche Frage meinst du?")
        return {"type": "done"}

    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_SESSION_CONTINUITY: True}),
        subentry_type="conversation",
        title="Computer",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(
        _make_entry(SimpleNamespace(async_converse=converse)), subentry
    )
    entity.entity_id, entity.hass = "conversation.computer", hass
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        result = await entity.async_process(
            replace(
                _make_input(),
                device_id="echo-a",
                conversation_id=None,
                language="de-DE",
            )
        )
    assert (
        "Bitte nenne nötige Details erneut" in result.response.speech["plain"]["speech"]
    )
