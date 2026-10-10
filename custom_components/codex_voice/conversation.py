"""Conversation agent for Codex Voice."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any, Literal, override

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_LLM_HASS_API, CONF_PROMPT, MATCH_ALL
from homeassistant.core import HomeAssistant
from homeassistant.helpers import intent, llm
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import CodexVoiceConfigEntry
from .api import (
    BridgeAuthenticationError,
    BridgeBusyError,
    BridgeError,
    BridgeQuotaError,
    BridgeToolCall,
)
from .const import (
    CONF_MODEL,
    CONF_REASONING_EFFORT,
    CONF_SERVICE_TIER,
    DEFAULT_CONVERSATION_MODEL,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    DOMAIN,
    LEGACY_CONVERSATION_SERVICE_TIER,
    SUBENTRY_TYPE_CONVERSATION,
    SUPPORTED_REASONING_EFFORTS,
    SUPPORTED_SERVICE_TIERS,
)
from .entity import CodexVoiceEntity
from .llm_tools import serialize_llm_tools, tool_result_data

_LOGGER = logging.getLogger(__name__)

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: CodexVoiceConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Codex Voice conversation entities."""
    for subentry in config_entry.subentries.values():
        if subentry.subentry_type != SUBENTRY_TYPE_CONVERSATION:
            continue
        async_add_entities(
            [CodexVoiceConversationEntity(config_entry, subentry)],
            config_subentry_id=subentry.subentry_id,
        )


class CodexVoiceConversationEntity(
    conversation.ConversationEntity,
    conversation.AbstractConversationAgent,
    CodexVoiceEntity,
):
    """A subscription-backed Home Assistant conversation agent."""

    _attr_supports_streaming = True
    _attr_translation_key = "conversation"

    def __init__(
        self,
        entry: CodexVoiceConfigEntry,
        subentry: ConfigSubentry,
    ) -> None:
        """Initialize the conversation agent."""
        super().__init__(entry, subentry)
        if subentry.data.get(CONF_LLM_HASS_API):
            self._attr_supported_features = (
                conversation.ConversationEntityFeature.CONTROL
            )

    @property
    @override
    def supported_languages(self) -> list[str] | Literal["*"]:
        """Return supported languages."""
        return MATCH_ALL

    @override
    async def async_added_to_hass(self) -> None:
        """Register this entity as a conversation agent."""
        await super().async_added_to_hass()
        conversation.async_set_agent(self.hass, self.entry, self)

    @override
    async def async_will_remove_from_hass(self) -> None:
        """Unregister this conversation agent."""
        conversation.async_unset_agent(self.hass, self.entry)
        await super().async_will_remove_from_hass()

    @override
    async def _async_handle_message(
        self,
        user_input: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
    ) -> conversation.ConversationResult:
        """Process a message and run requested Home Assistant tools."""
        options = self.subentry.data
        try:
            await chat_log.async_provide_llm_data(
                user_input.as_llm_context(DOMAIN),
                options.get(CONF_LLM_HASS_API),
                options.get(CONF_PROMPT),
                user_input.extra_system_prompt,
            )
        except conversation.ConverseError as err:
            return err.as_conversation_result()

        pending_text: list[str] = []
        stream_started = False

        async def async_flush_text() -> None:
            nonlocal stream_started
            if not pending_text:
                return
            text = "".join(pending_text)
            pending_text.clear()
            chat_log.async_add_assistant_content_without_tools(
                conversation.AssistantContent(
                    agent_id=self.entity_id,
                    content=text,
                )
            )
            stream_started = False

        async def async_handle_delta(delta: str) -> None:
            nonlocal stream_started
            if not stream_started:
                if chat_log.delta_listener:
                    chat_log.delta_listener(chat_log, {"role": "assistant"})
                stream_started = True
            pending_text.append(delta)
            if chat_log.delta_listener:
                chat_log.delta_listener(chat_log, {"content": delta})

        async def async_handle_tool(tool_call: BridgeToolCall) -> dict[str, Any]:
            await async_flush_text()
            if chat_log.llm_api is None:
                return {
                    "error": "tool_not_available",
                    "error_text": "No Home Assistant LLM API is configured",
                }

            tool_input = llm.ToolInput(
                id=tool_call.call_id,
                tool_name=tool_call.name,
                tool_args=tool_call.arguments,
            )
            if chat_log.delta_listener:
                chat_log.delta_listener(
                    chat_log,
                    {"role": "assistant", "tool_calls": [tool_input]},
                )

            assistant_content = conversation.AssistantContent(
                agent_id=self.entity_id,
                tool_calls=[tool_input],
            )
            result: dict[str, Any] = {
                "error": "tool_failed",
                "error_text": "The Home Assistant tool returned no result",
            }
            async for tool_result in chat_log.async_add_assistant_content(
                assistant_content
            ):
                result = _tool_result_data(tool_result)
                if chat_log.delta_listener:
                    chat_log.delta_listener(chat_log, asdict(tool_result))
            return result

        effort = options.get(
            CONF_REASONING_EFFORT,
            DEFAULT_CONVERSATION_REASONING_EFFORT,
        )
        if effort not in SUPPORTED_REASONING_EFFORTS:
            effort = DEFAULT_CONVERSATION_REASONING_EFFORT
        service_tier = options.get(
            CONF_SERVICE_TIER,
            LEGACY_CONVERSATION_SERVICE_TIER,
        )
        if service_tier not in SUPPORTED_SERVICE_TIERS:
            service_tier = LEGACY_CONVERSATION_SERVICE_TIER

        start_payload = {
            "conversation_id": chat_log.conversation_id,
            "text": user_input.text,
            "language": user_input.language,
            "model": options.get(CONF_MODEL, DEFAULT_CONVERSATION_MODEL),
            "effort": effort,
            "service_tier": service_tier,
            "instructions": _conversation_instructions(chat_log),
            "messages": _serialize_chat_log(chat_log),
            "tools": _serialize_tools(chat_log),
        }

        try:
            await self.entry.runtime_data.async_converse(
                start_payload,
                async_handle_delta=async_handle_delta,
                async_handle_tool=async_handle_tool,
            )
            await async_flush_text()
        except BridgeAuthenticationError:
            self.entry.async_start_reauth(self.hass)
            return _error_result(
                user_input,
                chat_log,
                "Codex Voice needs to be authenticated again.",
            )
        except BridgeQuotaError:
            return _error_result(
                user_input,
                chat_log,
                "The ChatGPT subscription quota is currently exhausted.",
            )
        except BridgeBusyError:
            return _error_result(
                user_input,
                chat_log,
                "Codex Voice is busy with another speech request. Please try again.",
            )
        except BridgeError:
            _LOGGER.exception("Error communicating with the Codex Voice bridge")
            return _error_result(
                user_input,
                chat_log,
                "Sorry, I could not reach Codex Voice.",
            )

        return conversation.async_get_result_from_chat_log(user_input, chat_log)


def _tool_result_data(content: conversation.ToolResultContent) -> dict[str, Any]:
    """Use HA 2026.10's typed result while retaining HA 2026.8 compatibility."""
    if (result := getattr(content, "result", None)) is not None:
        return tool_result_data(result)
    return dict(content.tool_result)


def _serialize_chat_log(chat_log: conversation.ChatLog) -> list[dict[str, Any]]:
    """Convert Home Assistant chat content to bridge-safe JSON."""
    serialized: list[dict[str, Any]] = []
    for item in chat_log.content:
        message: dict[str, Any] = {"role": item.role}
        if isinstance(item, conversation.ToolResultContent):
            message.update(
                {
                    "tool_call_id": item.tool_call_id,
                    "tool_name": item.tool_name,
                    "result": _tool_result_data(item),
                }
            )
        else:
            message["content"] = item.content
            if isinstance(item, conversation.AssistantContent) and item.tool_calls:
                message["tool_calls"] = [
                    {
                        "id": tool.id,
                        "name": tool.tool_name,
                        "arguments": tool.tool_args,
                    }
                    for tool in item.tool_calls
                    if not tool.external
                ]
        serialized.append(message)
    return serialized


def _serialize_tools(chat_log: conversation.ChatLog) -> list[dict[str, Any]]:
    """Convert selected Home Assistant LLM tools to JSON Schema."""
    return serialize_llm_tools(chat_log.llm_api)


def _conversation_instructions(chat_log: conversation.ChatLog) -> str:
    """Build developer instructions from Home Assistant's system content."""
    return "\n\n".join(
        item.content
        for item in chat_log.content
        if isinstance(item, conversation.SystemContent) and item.content
    )


def _error_result(
    user_input: conversation.ConversationInput,
    chat_log: conversation.ChatLog,
    message: str,
) -> conversation.ConversationResult:
    """Create a standard Home Assistant conversation error response."""
    response = intent.IntentResponse(language=user_input.language)
    response.async_set_error(intent.IntentResponseErrorCode.UNKNOWN, message)
    return conversation.ConversationResult(
        response=response,
        conversation_id=chat_log.conversation_id,
    )
