"""Conversation agent for Codex Voice."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import asdict
from datetime import datetime, timedelta
from time import monotonic
from typing import Any, Literal, override

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_LLM_HASS_API, CONF_PROMPT, MATCH_ALL
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import intent, llm
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store

from . import CodexVoiceConfigEntry
from .api import (
    BridgeAuthenticationError,
    BridgeBusyError,
    BridgeError,
    BridgeProtocolError,
    BridgeQuotaError,
    BridgeToolCall,
    ToolHandler,
)
from .const import (
    CONF_MODEL,
    CONF_REASONING_EFFORT,
    CONF_SERVICE_TIER,
    CONF_SESSION_CONTINUITY,
    CONF_SESSION_FALLBACK,
    CONF_SESSION_METADATA,
    CONF_SESSION_TTL,
    DEFAULT_CONVERSATION_MODEL,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    DOMAIN,
    LEGACY_CONVERSATION_SERVICE_TIER,
    SUBENTRY_TYPE_CONVERSATION,
    SUPPORTED_REASONING_EFFORTS,
    SUPPORTED_SERVICE_TIERS,
)
from .conversation_sessions import (
    Session,
    SessionBusyError,
    SessionManager,
    is_reset_command,
)
from .entity import CodexVoiceEntity
from .llm_tools import serialize_llm_tools, tool_result_data
from .metrics import DiagnosticBuffer, TurnTrace

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
        diagnostics = getattr(entry.runtime_data, "conversation_diagnostics", None)
        if not isinstance(diagnostics, DiagnosticBuffer):
            diagnostics = DiagnosticBuffer()
            entry.runtime_data.conversation_diagnostics = diagnostics
        self._diagnostics: DiagnosticBuffer = diagnostics
        self._sessions = SessionManager(
            ttl_minutes=subentry.data.get(CONF_SESSION_TTL, 15),
            fallback=subentry.data.get(CONF_SESSION_FALLBACK, ""),
        )
        self._active_session: ContextVar[tuple[Session, str] | None] = ContextVar(
            "computer_active_session", default=None
        )
        self._session_store: Store[dict[str, Any]] | None = None
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
        if (
            self.subentry.data.get(CONF_SESSION_METADATA) is False
            or self.subentry.data.get(CONF_SESSION_CONTINUITY) is False
        ):
            await Store(
                self.hass,
                1,
                f"{DOMAIN}.sessions.{self.entry.entry_id}.{self.subentry.subentry_id}",
            ).async_remove()
        if self.subentry.data.get(
            CONF_SESSION_CONTINUITY, False
        ) and self.subentry.data.get(CONF_SESSION_METADATA, True):
            self._session_store = Store(
                self.hass,
                1,
                f"{DOMAIN}.sessions.{self.entry.entry_id}.{self.subentry.subentry_id}",
            )
            try:
                metadata = await self._session_store.async_load()
                if metadata is not None:
                    self._sessions.restore(metadata)
            except ValueError, TypeError, OSError:
                self._session_store = None
                _LOGGER.warning(
                    "Session metadata unavailable; starting isolated sessions"
                )
        self.async_on_remove(
            async_track_time_interval(
                self.hass, self._expire_sessions, timedelta(seconds=30)
            )
        )
        conversation.async_set_agent(self.hass, self.entry, self)

    @callback
    def _expire_sessions(self, _now: datetime) -> None:
        """Delete expired RAM context and stale metadata without a new wakeword."""
        if self._sessions.expire() and self._session_store is not None:
            self._session_store.async_delay_save(self._sessions.snapshot, 1)

    @override
    async def async_will_remove_from_hass(self) -> None:
        """Unregister this conversation agent."""
        conversation.async_unset_agent(self.hass, self.entry)
        await super().async_will_remove_from_hass()

    @override
    async def async_process(
        self, user_input: conversation.ConversationInput
    ) -> conversation.ConversationResult:
        """Bind a bridge session without replacing Home Assistant's ChatLog."""
        if not self.subentry.data.get(CONF_SESSION_CONTINUITY, False):
            return await super().async_process(user_input)
        owner, source = self._sessions.identity(
            device_id=user_input.device_id,
            satellite_id=user_input.satellite_id,
            user_id=user_input.context.user_id,
            incoming_id=user_input.conversation_id,
        )
        try:
            async with self._sessions.lease(owner) as session:
                token = self._active_session.set((session, source))
                try:
                    return await super().async_process(user_input)
                finally:
                    self._active_session.reset(token)
                    if self._session_store is not None:
                        self._sessions.touch(session)
                        try:
                            await self._session_store.async_save(
                                self._sessions.snapshot()
                            )
                        except OSError:
                            _LOGGER.warning(
                                "Session metadata checkpoint failed; context may be lost after restart"
                            )
        except SessionBusyError, TimeoutError:
            response = intent.IntentResponse(language=user_input.language)
            response.async_set_error(
                intent.IntentResponseErrorCode.UNKNOWN,
                "Diese Unterhaltung ist beschäftigt. Bitte versuche es gleich erneut."
                if user_input.language.startswith("de")
                else "This conversation is busy. Please try again shortly.",
            )
            return conversation.ConversationResult(
                response=response, conversation_id=user_input.conversation_id
            )

    @override
    async def _async_handle_message(
        self,
        user_input: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
    ) -> conversation.ConversationResult:
        """Process a message and run requested Home Assistant tools."""
        trace = TurnTrace(incoming_id_present=bool(user_input.conversation_id))
        trace.mark("conversation_received")
        active = self._active_session.get()
        session = active[0] if active else None
        if active and session is not None:
            trace.identity_source = active[1]
            trace.incoming_id_changed = (
                session.previous_incoming != user_input.conversation_id
                if session.previous_incoming is not None
                else None
            )
            session.previous_incoming = user_input.conversation_id
        if session is not None and is_reset_command(user_input.text):
            previous_id = session.conversation_id
            self._sessions.reset(session)
            try:
                await self.entry.runtime_data.async_reset_conversation(previous_id)
            except BridgeError:
                # Binding is already rotated; the old remote cache expires.
                trace.error = "remote_reset_unavailable"
            trace.path, trace.context_status = "local", "reset"
            message = (
                "Ein neues Gespräch beginnt."
                if user_input.language.startswith("de")
                else "A new conversation starts now."
            )
            chat_log.async_add_assistant_content_without_tools(
                conversation.AssistantContent(agent_id=self.entity_id, content=message)
            )
            result = conversation.async_get_result_from_chat_log(user_input, chat_log)
            self._diagnostics.finish(trace)
            return result
        try:
            if session is not None and self._session_store is not None:
                try:
                    await self._session_store.async_save(self._sessions.snapshot())
                except OSError:
                    _LOGGER.warning(
                        "Session metadata checkpoint failed; context may be lost after restart"
                    )
            return await self._async_run_message(user_input, chat_log, trace)
        except asyncio.CancelledError:
            trace.path, trace.error = "error", "cancelled_outcome_unknown"
            raise
        except BridgeError, TypeError, ValueError, KeyError, AttributeError:
            trace.path, trace.error = "error", "internal_failed"
            _LOGGER.error("Conversation failed; correlation=%s", trace.correlation_id)
            return _error_result(
                user_input,
                chat_log,
                _bridge_error_message(user_input.language, bool(trace.tools_ms)),
            )
        finally:
            if session is not None and trace.path == "error":
                self._sessions.reset(session)
                session.restored = True
            self._diagnostics.finish(trace)

    async def _async_run_message(
        self,
        user_input: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
        trace: TurnTrace,
    ) -> conversation.ConversationResult:
        """Run a turn through HA's selected LLM API."""
        options = self.subentry.data
        active = self._active_session.get()
        session = active[0] if active else None
        if session is not None and _unverified_local_reference(
            session, chat_log, user_input.text
        ):
            trace.path = "clarification"
            chat_log.async_add_assistant_content_without_tools(
                conversation.AssistantContent(
                    agent_id=self.entity_id,
                    content="Welches Gerät meinst du?"
                    if user_input.language.startswith("de")
                    else "Which device do you mean?",
                )
            )
            return conversation.async_get_result_from_chat_log(user_input, chat_log)
        try:
            await chat_log.async_provide_llm_data(
                user_input.as_llm_context(DOMAIN),
                options.get(CONF_LLM_HASS_API),
                options.get(CONF_PROMPT),
                user_input.extra_system_prompt,
            )
        except conversation.ConverseError as err:
            trace.path, trace.error = "error", "llm_preparation_failed"
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
            trace.mark("first_text_delta")
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

        original_tool_handler = async_handle_tool

        async def measured_tool_handler(tool_call: BridgeToolCall) -> dict[str, Any]:
            return await _execute_measured_tool(original_tool_handler, tool_call, trace)

        effort, service_tier = _conversation_settings(options)

        start_payload = {
            "correlation_id": trace.correlation_id,
            "conversation_id": session.conversation_id
            if session
            else chat_log.conversation_id,
            "text": user_input.text,
            "language": user_input.language,
            "model": options.get(CONF_MODEL, DEFAULT_CONVERSATION_MODEL),
            "effort": effort,
            "service_tier": service_tier,
            "instructions": _conversation_instructions(chat_log),
            "messages": [*session.history, {"role": "user", "content": user_input.text}]
            if session
            else _serialize_chat_log(chat_log),
            "tools": _serialize_tools(chat_log),
        }
        if session is not None:
            start_payload.update(
                {
                    "request_id": trace.correlation_id,
                    "session_idle_seconds": self._sessions.ttl,
                    "session_restored": session.restored,
                    "history_complete": session.history_complete,
                }
            )

        async def handle_started(event: dict[str, Any]) -> None:
            trace.context_status = _session_context_status(event)
            if trace.context_status in {"lost", "partial"}:
                message = (
                    "Der frühere Gesprächskontext ist nur teilweise verfügbar. Bitte nenne nötige Details erneut. "
                    if user_input.language.startswith("de")
                    else "Earlier conversation context is incomplete. Please repeat any needed details. "
                )
                await async_handle_delta(message)

        callbacks: dict[str, Any] = {}
        if session is not None:
            callbacks["async_handle_started"] = handle_started

        try:
            trace.mark("codex_requested")
            await self.entry.runtime_data.async_converse(
                start_payload,
                async_handle_delta=async_handle_delta,
                async_handle_tool=measured_tool_handler,
                **callbacks,
            )
            trace.mark("codex_completed")
            await async_flush_text()
            if session is not None:
                current = _serialize_chat_log(chat_log)
                # Retain only this successful turn, never foreign HA histories.
                current_user = max(
                    index
                    for index, item in enumerate(current)
                    if item.get("role") == "user"
                )
                session.remember_turn(current[current_user:])
                session.restored = False
                session.ha_id, session.ha_size = (
                    chat_log.conversation_id,
                    len(chat_log.content),
                )
        except BridgeAuthenticationError:
            trace.path, trace.error = "error", "authentication_required"
            self.entry.async_start_reauth(self.hass)
            return _error_result(
                user_input,
                chat_log,
                "Codex Voice needs to be authenticated again.",
            )
        except BridgeQuotaError:
            trace.path, trace.error = "error", "quota_exhausted"
            return _error_result(
                user_input,
                chat_log,
                "The ChatGPT subscription quota is currently exhausted.",
            )
        except BridgeBusyError:
            trace.path, trace.error = "error", "busy"
            return _error_result(
                user_input,
                chat_log,
                "Codex Voice is busy with another speech request. Please try again.",
            )
        except BridgeError:
            trace.path, trace.error = "error", trace.error or "bridge_unavailable"
            _LOGGER.warning(
                "Conversation bridge failed; correlation=%s", trace.correlation_id
            )
            return _error_result(
                user_input,
                chat_log,
                _bridge_error_message(user_input.language, bool(trace.tools_ms)),
            )

        return conversation.async_get_result_from_chat_log(user_input, chat_log)


async def _execute_measured_tool(
    handler: ToolHandler, tool_call: BridgeToolCall, trace: TurnTrace
) -> dict[str, Any]:
    """Bound execution and prevent automatic retries after a tool failure."""
    started = monotonic()
    try:
        async with asyncio.timeout(25):
            result = await handler(tool_call)
    except TimeoutError:
        trace.error = "tool_outcome_unknown"
        raise BridgeProtocolError("Tool outcome unknown; do not retry") from None
    else:
        return _require_tool_success(result, trace)
    finally:
        trace.tools_ms.append(round((monotonic() - started) * 1000, 3))


def _tool_result_data(content: conversation.ToolResultContent) -> dict[str, Any]:
    """Use HA 2026.10's typed result while retaining HA 2026.8 compatibility."""
    if (result := getattr(content, "result", None)) is not None:
        return tool_result_data(result)
    return dict(content.tool_result)


def _require_tool_success(result: dict[str, Any], trace: TurnTrace) -> dict[str, Any]:
    """Stop failed calls before a model can initiate an implicit retry."""
    if "error" in result:
        trace.error = "tool_failed_outcome_unknown"
        raise BridgeProtocolError("Tool failed; do not retry")
    return result


def _session_context_status(event: dict[str, Any]) -> str:
    """Require an explicit restart contract when continuity is enabled."""
    status = event.get("context_status")
    if not isinstance(status, str) or status not in {
        "new",
        "reused",
        "recovered",
        "partial",
        "lost",
    }:
        raise BridgeProtocolError("Session-aware bridge required; update the bridge")
    return status


def _conversation_settings(options: Mapping[str, Any]) -> tuple[str, str]:
    """Preserve supported model settings and legacy service-tier behavior."""
    effort = options.get(CONF_REASONING_EFFORT, DEFAULT_CONVERSATION_REASONING_EFFORT)
    if effort not in SUPPORTED_REASONING_EFFORTS:
        effort = DEFAULT_CONVERSATION_REASONING_EFFORT
    tier = options.get(CONF_SERVICE_TIER, LEGACY_CONVERSATION_SERVICE_TIER)
    if tier not in SUPPORTED_SERVICE_TIERS:
        tier = LEGACY_CONVERSATION_SERVICE_TIER
    return effort, tier


def _bridge_error_message(language: str, tool_dispatched: bool) -> str:
    """Explain uncertain actions without claiming that they were undone."""
    if language.startswith("de"):
        return (
            "Ich konnte die Anfrage nicht abschließen. Eine Geräteaktion könnte bereits erfolgt sein; bitte prüfe den Zustand."
            if tool_dispatched
            else "Ich konnte Codex Voice nicht erreichen. Bitte versuche es später erneut."
        )
    return (
        "I could not complete the request. A device action may already have happened; please check its state."
        if tool_dispatched
        else "Sorry, I could not reach Codex Voice."
    )


def _unverified_local_reference(
    session: Session, chat_log: conversation.ChatLog, text: str
) -> bool:
    """Never promote local-response prose to a verified device/action target."""
    prior = (
        chat_log.content[session.ha_size :]
        if session.ha_id == chat_log.conversation_id
        else chat_log.content
    )
    if any(
        isinstance(item, conversation.AssistantContent)
        and item.agent_id in (conversation.HOME_ASSISTANT_AGENT, "sentence_trigger")
        for item in prior
    ):
        session.local_action_unverified = True
    words = set(text.casefold().strip().rstrip(".!?").split())
    return (
        session.local_action_unverified
        and bool(words & {"es", "das", "ihn", "sie", "it", "that"})
        and bool(
            words
            & {"mach", "mache", "schalte", "stell", "stelle", "turn", "switch", "set"}
        )
    )


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
