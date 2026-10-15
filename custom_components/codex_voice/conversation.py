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
from uuid import uuid4

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
    DeltaHandler,
    ToolHandler,
)
from .const import (
    CONF_CONTEXT_MODE,
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
from .context_policy import (
    CONTEXT_ADAPTIVE,
    CONTEXT_LEGACY,
    ContextDecision,
    PendingQuestion,
    pending_question,
    select_context,
)
from .conversation_sessions import (
    Session,
    SessionBusyError,
    SessionManager,
    is_reset_command,
)
from .entity import CodexVoiceEntity
from .llm_tools import serialize_llm_tools, tool_result_data
from .local_intents import LocalResult, async_try_local_intent
from .metrics import DiagnosticBuffer, TurnTrace
from .tool_policy import (
    ADAPTIVE_INSTRUCTIONS,
    ToolAuthorizationRequired,
    ToolSelection,
    authorize_tool,
    select_tools,
)

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
        self._adaptive = (
            subentry.data.get(CONF_CONTEXT_MODE, CONTEXT_LEGACY) == CONTEXT_ADAPTIVE
        )
        self._current_trace: ContextVar[TurnTrace | None] = ContextVar(
            "computer_turn_trace", default=None
        )
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
            self._adaptive
            or self.subentry.data.get(CONF_SESSION_METADATA) is False
            or self.subentry.data.get(CONF_SESSION_CONTINUITY) is False
        ):
            await Store(
                self.hass,
                1,
                f"{DOMAIN}.sessions.{self.entry.entry_id}.{self.subentry.subentry_id}",
            ).async_remove()
        if (
            not self._adaptive
            and self.subentry.data.get(CONF_SESSION_CONTINUITY, False)
            and self.subentry.data.get(CONF_SESSION_METADATA, True)
        ):
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
        trace = TurnTrace(incoming_id_present=bool(user_input.conversation_id))
        token = self._current_trace.set(trace)
        try:
            return await self._async_process_bound(user_input)
        finally:
            self._current_trace.reset(token)
            self._diagnostics.finish(trace)

    async def _async_process_bound(
        self, user_input: conversation.ConversationInput
    ) -> conversation.ConversationResult:
        """Retain legacy binding while adaptive profiles also bind UI users."""
        if not self._adaptive and not self.subentry.data.get(
            CONF_SESSION_CONTINUITY, False
        ):
            return await super().async_process(user_input)
        owner, source = self._sessions.identity(
            device_id=user_input.device_id,
            satellite_id=user_input.satellite_id,
            user_id=user_input.context.user_id,
            incoming_id=user_input.conversation_id,
            bind_user=self._adaptive,
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
            if trace := self._current_trace.get():
                trace.path, trace.error = "error", "busy"
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
        trace = self._current_trace.get() or TurnTrace(
            incoming_id_present=bool(user_input.conversation_id)
        )
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
            if not self._adaptive:
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
            self._finish_trace(trace)
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
                _bridge_error_message(
                    user_input.language, bool(trace.tool_calls_executed)
                ),
            )
        finally:
            if session is not None and trace.path == "error":
                self._sessions.reset(session)
                session.restored = True
            self._finish_trace(trace)

    def _finish_trace(self, trace: TurnTrace) -> None:
        """Freeze direct handler calls; normal process calls include cleanup time."""
        if self._current_trace.get() is None:
            self._diagnostics.finish(trace)

    def _select_request_context(
        self, text: str, session: Session | None, trace: TurnTrace
    ) -> tuple[ContextDecision, ToolSelection]:
        """Select history and capabilities without any model or API request."""
        if not self._adaptive:
            return ContextDecision("independent"), select_tools(text)
        trace.mark("context_selection_started")
        decision = select_context(
            text,
            has_context=bool(session and session.history),
            question=session.pending_question if session else None,
            now=monotonic(),
        )
        selection = select_tools(
            text,
            inherited_home=bool(
                decision.use_context and session and session.home_context
            ),
            inherited_action=bool(
                decision.kind == "question_answer"
                and session
                and session.action_context
            ),
        )
        trace.context_choice = decision.kind
        trace.mark("context_selection_completed")
        return decision, selection

    def _clarify_request(
        self,
        user: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
        session: Session | None,
        decision: ContextDecision,
        selection: ToolSelection,
        trace: TurnTrace,
    ) -> conversation.ConversationResult | None:
        """Ask locally for missing context; never infer targets from local prose."""
        unverified = session is not None and _unverified_local_reference(
            session, chat_log, user.text
        )
        if not unverified and decision.kind != "clarify":
            return None
        trace.path = "clarification"
        if unverified:
            message = (
                "Welches Gerät meinst du?"
                if user.language.startswith("de")
                else "Which device do you mean?"
            )
            if self._adaptive and session is not None:
                self._sessions.reset(session)
                session.remember_turn(
                    [
                        {"role": "user", "content": user.text},
                        {"role": "assistant", "content": message},
                    ]
                )
                session.pending_question = pending_question(message, now=monotonic())
                session.home_context, session.action_context = (
                    True,
                    selection.writes_requested,
                )
        else:
            message = (
                "Worauf bezieht sich deine Frage? Bitte nenne das Thema."
                if user.language.startswith("de")
                else "What does your question refer to? Please name the topic."
            )
        chat_log.async_add_assistant_content_without_tools(
            conversation.AssistantContent(agent_id=self.entity_id, content=message)
        )
        return conversation.async_get_result_from_chat_log(user, chat_log)

    async def _async_authorize_tool(
        self,
        call: BridgeToolCall,
        chat_log: conversation.ChatLog,
        user: conversation.ConversationInput,
        selection: ToolSelection,
        trace: TurnTrace,
    ) -> None:
        """Authorize the actual registered tool before any HA dispatch."""
        if not self._adaptive or chat_log.llm_api is None:
            return
        available = next(
            (tool for tool in chat_log.llm_api.tools if tool.name == call.name), None
        )
        if available is None:
            raise BridgeProtocolError("Unregistered Home Assistant tool")
        authenticated_ui = False
        if user.context.user_id and not user.device_id and not user.satellite_id:
            account = await self.hass.auth.async_get_user(user.context.user_id)
            authenticated_ui = bool(
                account and account.is_active and not account.system_generated
            )
        try:
            authorize_tool(
                available,
                call.arguments,
                writes_requested=selection.writes_requested,
                authenticated_ui=authenticated_ui,
            )
        except ToolAuthorizationRequired:
            trace.error = "tool_authorization_required"
            raise BridgeProtocolError(
                "Explicit action authorization required"
            ) from None

    async def _async_try_local(
        self,
        user: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
        session: Session | None,
        decision: ContextDecision,
        selection: ToolSelection,
        pending_action: str | None,
        trace: TurnTrace,
    ) -> conversation.ConversationResult | None:
        """Prefer native local intents only within the selected Assist capability."""
        apis = self.subentry.data.get(CONF_LLM_HASS_API, [])
        if isinstance(apis, str):
            apis = [apis]
        if (
            not self._adaptive
            or not selection.home_assistant
            or llm.LLM_API_ASSIST not in apis
        ):
            return None
        trace.mark("local_intent_started")
        try:
            local = await async_try_local_intent(
                self.hass,
                user,
                chat_log,
                pending_light_action=pending_action,
                allow_native=decision.kind == "independent",
            )
        finally:
            trace.mark("local_intent_completed")
        if local is None:
            return None
        trace.path = "clarification" if local.pending_light_action else "local"
        if (
            local.response.response_type is intent.IntentResponseType.ERROR
            or local.response.failed_results
        ):
            trace.path, trace.error = "error", "local_intent_failed"
        speech = local.response.speech.get("plain", {}).get("speech", "")
        chat_log.async_add_assistant_content_without_tools(
            conversation.AssistantContent(agent_id=self.entity_id, content=speech)
        )
        if session is not None and trace.path != "error":
            self._remember_local_turn(session, user, chat_log, local)
        return conversation.ConversationResult(
            response=local.response,
            conversation_id=chat_log.conversation_id,
            continue_conversation=bool(local.pending_light_action),
        )

    def _remember_local_turn(
        self,
        session: Session,
        user: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
        local: LocalResult,
    ) -> None:
        """Retain structured native outcomes, never guess a target from speech."""
        speech = local.response.speech.get("plain", {}).get("speech", "")
        messages: list[dict[str, Any]] = [
            {"role": "user", "content": user.text},
            {"role": "assistant", "content": speech},
        ]
        if local.pending_light_action:
            session.pending_question = PendingQuestion("entity", monotonic())
            session.pending_light_action = local.pending_light_action
        else:
            messages.append(
                {
                    "role": "tool",
                    "tool_name": "local_intent",
                    "result": local.response.as_dict(),
                }
            )
        session.remember_turn(messages)
        session.home_context = True
        session.action_context = (
            local.response.response_type is intent.IntentResponseType.ACTION_DONE
        )
        session.ha_id, session.ha_size = chat_log.conversation_id, len(chat_log.content)
        if self.subentry.data.get(CONF_SESSION_CONTINUITY) is False:
            self._sessions.reset(session)

    def _conversation_payload(
        self,
        user: conversation.ConversationInput,
        chat_log: conversation.ChatLog,
        session: Session | None,
        decision: ContextDecision,
        trace: TurnTrace,
    ) -> dict[str, Any]:
        """Keep legacy messages intact and send only selected adaptive history."""
        options = self.subentry.data
        effort, tier = _conversation_settings(options)
        payload = {
            "correlation_id": trace.correlation_id,
            "conversation_id": session.conversation_id
            if session
            else chat_log.conversation_id,
            "text": user.text,
            "language": user.language,
            "model": options.get(CONF_MODEL, DEFAULT_CONVERSATION_MODEL),
            "effort": effort,
            "service_tier": tier,
            "instructions": _conversation_instructions(chat_log),
            "messages": [*session.history, {"role": "user", "content": user.text}]
            if session
            else _serialize_chat_log(chat_log),
            "tools": _serialize_tools(chat_log),
        }
        if self._adaptive:
            payload.update(
                {
                    "context_policy": CONTEXT_ADAPTIVE,
                    "context_selection": decision.kind,
                    "conversation_id": session.conversation_id
                    if session
                    else uuid4().hex,
                    "messages": [
                        *(session.history if session and decision.use_context else []),
                        {"role": "user", "content": user.text},
                    ],
                    "instructions": _conversation_instructions(chat_log)
                    + "\n\n"
                    + ADAPTIVE_INSTRUCTIONS,
                    "request_id": trace.correlation_id,
                }
            )
        if session is not None:
            payload.update(
                {
                    "request_id": trace.correlation_id,
                    "session_idle_seconds": self._sessions.ttl,
                    "session_restored": session.restored,
                    "history_complete": session.history_complete,
                }
            )
        return payload

    def _remember_bridge_turn(
        self,
        chat_log: conversation.ChatLog,
        session: Session | None,
        selection: ToolSelection,
        answer: str,
    ) -> None:
        """Retain only the successful current turn from the native HA chat log."""
        if session is None:
            return
        current = _serialize_chat_log(chat_log)
        current_user = max(
            index for index, item in enumerate(current) if item.get("role") == "user"
        )
        session.remember_turn(current[current_user:])
        if self._adaptive:
            session.pending_question = pending_question(answer, now=monotonic())
            session.home_context, session.action_context = (
                selection.home_assistant,
                selection.writes_requested,
            )
        session.restored = False
        session.ha_id, session.ha_size = chat_log.conversation_id, len(chat_log.content)
        if self._adaptive and self.subentry.data.get(CONF_SESSION_CONTINUITY) is False:
            self._sessions.reset(session)

    async def _async_handle_started(
        self,
        event: dict[str, Any],
        user: conversation.ConversationInput,
        trace: TurnTrace,
        handle_delta: DeltaHandler,
    ) -> None:
        """Require the bridge contract and disclose incomplete selected context."""
        if self._adaptive and event.get("context_policy") != CONTEXT_ADAPTIVE:
            raise BridgeProtocolError(
                "Single-turn-first bridge required; update the bridge"
            )
        trace.context_status = _session_context_status(event)
        if trace.context_status in {"lost", "partial"}:
            await handle_delta(
                "Der frühere Gesprächskontext ist nur teilweise verfügbar. Bitte nenne nötige Details erneut. "
                if user.language.startswith("de")
                else "Earlier conversation context is incomplete. Please repeat any needed details. "
            )

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
        decision, selection = self._select_request_context(
            user_input.text, session, trace
        )
        if clarification := self._clarify_request(
            user_input, chat_log, session, decision, selection, trace
        ):
            return clarification
        pending_action = (
            session.pending_light_action
            if session and decision.kind == "question_answer"
            else None
        )
        if self._adaptive:
            if session is not None:
                if not decision.use_context:
                    self._sessions.reset(session)
                session.conversation_id = uuid4().hex
                session.pending_question = None
                session.pending_light_action = None
            # HA otherwise carries this prompt over from an older ChatLog turn.
            chat_log.extra_system_prompt = None
        if local := await self._async_try_local(
            user_input, chat_log, session, decision, selection, pending_action, trace
        ):
            return local
        try:
            await chat_log.async_provide_llm_data(
                user_input.as_llm_context(DOMAIN),
                options.get(CONF_LLM_HASS_API)
                if not self._adaptive or selection.home_assistant
                else None,
                options.get(CONF_PROMPT),
                user_input.extra_system_prompt,
            )
        except conversation.ConverseError as err:
            trace.path, trace.error = "error", "llm_preparation_failed"
            return err.as_conversation_result()

        pending_text: list[str] = []
        answer_parts: list[str] = []
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
            answer_parts.append(delta)
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
            await self._async_authorize_tool(
                tool_call, chat_log, user_input, selection, trace
            )

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
            trace.tool_calls_executed += 1
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

        start_payload = self._conversation_payload(
            user_input, chat_log, session, decision, trace
        )
        trace.tools_available = bool(start_payload["tools"])

        async def handle_started(event: dict[str, Any]) -> None:
            await self._async_handle_started(
                event, user_input, trace, async_handle_delta
            )

        callbacks: dict[str, Any] = {}
        if session is not None or self._adaptive:
            callbacks["async_handle_started"] = handle_started

        try:
            trace.mark("codex_requested")
            trace.bridge_turn_requests += 1
            await self.entry.runtime_data.async_converse(
                start_payload,
                async_handle_delta=async_handle_delta,
                async_handle_tool=measured_tool_handler,
                **callbacks,
            )
            trace.mark("codex_completed")
            await async_flush_text()
            self._remember_bridge_turn(
                chat_log, session, selection, "".join(answer_parts)
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
                "Dein Codex-Nutzungslimit ist erreicht. Bitte versuche es später erneut."
                if user_input.language.startswith("de")
                else "Your Codex usage limit has been reached. Please try again later.",
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
                "Für diese Aktion ist eine eindeutige Geräteart und gegebenenfalls eine gesonderte Autorisierung erforderlich."
                if trace.error == "tool_authorization_required"
                and user_input.language.startswith("de")
                else "This action requires an unambiguous device domain and, where applicable, separate authorization."
                if trace.error == "tool_authorization_required"
                else _bridge_error_message(
                    user_input.language, bool(trace.tool_calls_executed)
                ),
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
