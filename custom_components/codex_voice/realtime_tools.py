"""Outbound Home Assistant tool broker for realtime Codex Voice sessions."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Final

from aiohttp import (
    ClientError,
    ClientSession,
    ClientWebSocketResponse,
    WSMsgType,
    WSServerHandshakeError,
)
from homeassistant.components.conversation.const import (
    DOMAIN as CONVERSATION_DOMAIN,
)
from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.const import CONF_LLM_HASS_API, CONF_PROMPT
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import llm, template
from homeassistant.helpers.json import json_dumps_sorted
from homeassistant.util.json import (
    JSON_DECODE_EXCEPTIONS,
    json_loads,
    json_loads_object,
)

from .const import (
    CONF_ACCESS_TOKEN,
    CONF_BRIDGE_URL,
    CONF_REALTIME_AUTHORITY,
    CONF_REALTIME_LANGUAGE,
    DEFAULT_REALTIME_LANGUAGE,
    DOMAIN,
    MAX_REALTIME_TOOL_ARGUMENT_BYTES,
    MAX_REALTIME_TOOL_MESSAGE_BYTES,
    MAX_REALTIME_TOOL_REGISTRATION_BYTES,
    MAX_REALTIME_TOOL_RESULT_BYTES,
    MAX_REALTIME_TOOL_SCHEMA_BYTES,
    MAX_REALTIME_TOOLS,
    REALTIME_TOOL_PATH,
    REALTIME_TOOL_PROTOCOL_VERSION,
    SUBENTRY_TYPE_CONVERSATION,
    SUPPORTED_LANGUAGES,
)
from .llm_tools import serialize_llm_tools, tool_result_data

_LOGGER = logging.getLogger(__name__)

DATA_REALTIME_TOOL_BROKERS: Final = f"{DOMAIN}_realtime_tool_brokers"

_MAX_AUTHORITY_ID_CHARS: Final = 256
_MAX_GENERATION_CHARS: Final = 256
_MAX_CALL_ID_CHARS: Final = 256
_MAX_TOOL_NAME_CHARS: Final = 256
_MAX_TOOL_DESCRIPTION_CHARS: Final = 8 * 1024
_MAX_INSTRUCTIONS_CHARS: Final = 64 * 1024
_MAX_PENDING_TOOL_CALLS: Final = 16
_MAX_CALLS_PER_GENERATION: Final = 1024
_REGISTRATION_TIMEOUT: Final = 10.0
_TOOL_CALL_TIMEOUT: Final = 25.0
_SEND_TIMEOUT: Final = 5.0
_INITIAL_RECONNECT_DELAY: Final = 1.0
_MAX_RECONNECT_DELAY: Final = 60.0
_TOOL_NAME: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,255}\Z")


class RealtimeToolBrokerError(Exception):
    """Base error for a fail-closed realtime tool broker connection."""


class RealtimeToolBrokerProtocolError(RealtimeToolBrokerError):
    """The bridge sent an invalid realtime tool protocol event."""


@dataclass(frozen=True, slots=True)
class _RegistrationSnapshot:
    """One immutable API/tool view captured for a single connection."""

    serialized: str
    api_instance: llm.APIInstance
    tool_names: frozenset[str]


def _bounded_string(
    value: object,
    field: str,
    limit: int,
    *,
    allow_empty: bool = False,
) -> str:
    """Validate text using the bridge protocol's character policy."""
    if (
        not isinstance(value, str)
        or (not allow_empty and not value)
        or len(value) > limit
        or any(character < " " and character not in "\n\t" for character in value)
    ):
        raise RealtimeToolBrokerProtocolError(f"{field} must be non-empty text")
    return value


def _bounded_tool_name(value: object) -> str:
    """Validate a tool name exactly as the bridge registration parser does."""
    name = _bounded_string(value, "tool name", _MAX_TOOL_NAME_CHARS)
    if _TOOL_NAME.fullmatch(name) is None:
        raise RealtimeToolBrokerProtocolError("tool name is invalid")
    return name


def _serialized_size(value: object) -> int:
    """Return the size produced by the bridge's strict JSON bound policy."""
    try:
        normalized = json_loads(json_dumps_sorted(value))
        return len(
            json.dumps(
                normalized,
                ensure_ascii=True,
                allow_nan=False,
                separators=(",", ":"),
            ).encode()
        )
    except (TypeError, ValueError) as err:
        raise RealtimeToolBrokerProtocolError(
            "Value contains unsupported JSON data"
        ) from err


def select_realtime_authority(entry: ConfigEntry) -> ConfigSubentry | None:
    """Select exactly one explicitly enabled Conversation authority."""
    authorities = [
        subentry
        for subentry in entry.subentries.values()
        if subentry.subentry_type == SUBENTRY_TYPE_CONVERSATION
        and subentry.data.get(CONF_REALTIME_AUTHORITY) is True
    ]
    if len(authorities) == 1:
        return authorities[0]

    _LOGGER.error(
        "Realtime Home Assistant tools are disabled for config entry %s: "
        "expected exactly one Conversation authority, found %d",
        entry.entry_id,
        len(authorities),
    )
    return None


class RealtimeToolBroker:
    """Maintain one authenticated, generation-scoped bridge connection."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        authority: ConfigSubentry,
        session: ClientSession,
        *,
        initial_reconnect_delay: float = _INITIAL_RECONNECT_DELAY,
        max_reconnect_delay: float = _MAX_RECONNECT_DELAY,
        registration_timeout: float = _REGISTRATION_TIMEOUT,
        tool_call_timeout: float = _TOOL_CALL_TIMEOUT,
        send_timeout: float = _SEND_TIMEOUT,
    ) -> None:
        """Initialize the outbound broker."""
        self._hass = hass
        self._entry = entry
        self._authority = authority
        self._session = session
        self._url = (
            f"{str(entry.data[CONF_BRIDGE_URL]).rstrip('/')}{REALTIME_TOOL_PATH}"
        )
        self._headers = {"Authorization": f"Bearer {entry.data[CONF_ACCESS_TOKEN]}"}
        self._initial_reconnect_delay = initial_reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        if registration_timeout <= 0:
            raise ValueError("registration_timeout must be positive")
        self._registration_timeout = registration_timeout
        if tool_call_timeout <= 0:
            raise ValueError("tool_call_timeout must be positive")
        self._tool_call_timeout = tool_call_timeout
        if send_timeout <= 0:
            raise ValueError("send_timeout must be positive")
        self._send_timeout = send_timeout
        self._stopped = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._connected = False
        self._reauth_started = False
        self._send_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        """Return whether the bridge accepted the current registration."""
        return self._connected

    @callback
    def async_start(self) -> None:
        """Start reconnecting in a config-entry-owned background task."""
        if self._task is not None:
            return
        self._task = self._entry.async_create_background_task(
            self._hass,
            self._async_run(),
            f"{DOMAIN} realtime Home Assistant tool broker",
        )

    async def async_stop(self) -> None:
        """Stop promptly without delaying Home Assistant unload."""
        self._stopped.set()
        task = self._task
        self._task = None
        if task is None or task.done():
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _async_run(self) -> None:
        """Reconnect with bounded exponential backoff until entry unload."""
        delay = self._initial_reconnect_delay
        while not self._stopped.is_set():
            registered = False
            try:
                snapshot = await self._async_prepare_registration()
                registered = await self._async_connect(snapshot)
            except asyncio.CancelledError:
                raise
            except WSServerHandshakeError as err:
                if err.status in (401, 403) and not self._reauth_started:
                    self._reauth_started = True
                    self._entry.async_start_reauth(self._hass)
                _LOGGER.warning(
                    "Realtime Home Assistant tool broker connection was rejected "
                    "with HTTP %s",
                    err.status,
                )
            except (ClientError, TimeoutError, HomeAssistantError) as err:
                _LOGGER.warning(
                    "Realtime Home Assistant tool broker is unavailable: %s",
                    err,
                )
            except RealtimeToolBrokerError as err:
                _LOGGER.error(
                    "Realtime Home Assistant tool broker failed closed: %s",
                    err,
                )
            except Exception:
                _LOGGER.exception(
                    "Unexpected realtime Home Assistant tool broker failure"
                )
            finally:
                self._connected = False

            if self._stopped.is_set():
                return
            if registered:
                delay = self._initial_reconnect_delay
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=delay)
            delay = min(
                max(delay * 2, self._initial_reconnect_delay),
                self._max_reconnect_delay,
            )

    async def _async_prepare_registration(self) -> _RegistrationSnapshot:
        """Capture the configured LLM API and serialize a bounded registration."""
        authority_id = _bounded_string(
            self._authority.subentry_id,
            "authority_id",
            _MAX_AUTHORITY_ID_CHARS,
        )
        language = self._authority.data.get(
            CONF_REALTIME_LANGUAGE, DEFAULT_REALTIME_LANGUAGE
        )
        if language not in SUPPORTED_LANGUAGES:
            raise RealtimeToolBrokerError(
                f"Unsupported realtime authority language: {language!r}"
            )

        api_ids = self._authority.data.get(CONF_LLM_HASS_API)
        if not api_ids:
            raise RealtimeToolBrokerError(
                "The realtime authority has no Home Assistant LLM API configured"
            )
        if isinstance(api_ids, str):
            normalized_api_ids: str | list[str] = _bounded_string(
                api_ids, "llm_hass_api", _MAX_TOOL_NAME_CHARS
            )
        elif (
            isinstance(api_ids, list)
            and api_ids
            and len(api_ids) <= MAX_REALTIME_TOOLS
            and all(isinstance(api_id, str) and api_id for api_id in api_ids)
        ):
            normalized_api_ids = [
                _bounded_string(api_id, "llm_hass_api", _MAX_TOOL_NAME_CHARS)
                for api_id in api_ids
            ]
        else:
            raise RealtimeToolBrokerError(
                "The realtime authority has an invalid Home Assistant LLM API selection"
            )

        llm_context = llm.LLMContext(
            platform=DOMAIN,
            context=None,
            language=language,
            # Entity exposure is keyed by the assistant that owns the request.
            # Home Assistant currently supports the built-in "conversation"
            # assistant here; using our integration domain produced an empty
            # exposure set and silently omitted entity-dependent tools.
            assistant=CONVERSATION_DOMAIN,
            device_id=None,
        )
        api_instance = await llm.async_get_api(
            self._hass,
            normalized_api_ids,
            llm_context,
        )
        tools = serialize_llm_tools(api_instance)
        if len(tools) > MAX_REALTIME_TOOLS:
            raise RealtimeToolBrokerError("Realtime LLM API exposes too many tools")

        tool_names: set[str] = set()
        for tool_schema in tools:
            name = _bounded_tool_name(tool_schema.get("name"))
            if name in tool_names:
                raise RealtimeToolBrokerError(
                    f"Realtime LLM API exposes duplicate tool name {name!r}"
                )
            tool_names.add(name)
            _bounded_string(
                tool_schema.get("description", ""),
                f"description for {name}",
                _MAX_TOOL_DESCRIPTION_CHARS,
                allow_empty=True,
            )
            if not isinstance(tool_schema.get("parameters"), Mapping):
                raise RealtimeToolBrokerError(
                    f"Schema for realtime tool {name!r} must be an object"
                )
            if _serialized_size(tool_schema.get("parameters")) > (
                MAX_REALTIME_TOOL_SCHEMA_BYTES
            ):
                raise RealtimeToolBrokerError(
                    f"Schema for realtime tool {name!r} exceeds its size limit"
                )

        captured_names = frozenset(tool_names)
        # Refuse to register a view that does not exactly match the tools we will
        # authorize for this generation.
        if captured_names != frozenset(tool.name for tool in api_instance.tools):
            raise RealtimeToolBrokerError(
                "Serialized realtime tools do not match the captured LLM API"
            )

        instructions = self._render_instructions(api_instance, language)
        _bounded_string(instructions, "instructions", _MAX_INSTRUCTIONS_CHARS)
        registration = {
            "type": "register",
            "protocol_version": REALTIME_TOOL_PROTOCOL_VERSION,
            "authority_id": authority_id,
            "language": language,
            "instructions": instructions,
            "tools": tools,
        }
        serialized = json_dumps_sorted(registration)
        if len(serialized.encode()) > MAX_REALTIME_TOOL_REGISTRATION_BYTES:
            raise RealtimeToolBrokerError(
                "Realtime tool registration exceeds its size limit"
            )
        return _RegistrationSnapshot(
            serialized=serialized,
            api_instance=api_instance,
            tool_names=captured_names,
        )

    def _render_instructions(
        self,
        api_instance: llm.APIInstance,
        language: str,
    ) -> str:
        """Render the configured prompt and append the captured API prompt."""
        prompt = (
            self._authority.data.get(CONF_PROMPT) or llm.DEFAULT_INSTRUCTIONS_PROMPT
        )
        if not isinstance(prompt, str):
            raise RealtimeToolBrokerError("Realtime authority prompt must be text")
        variables = {
            "ha_name": self._hass.config.location_name,
            "user_name": None,
            "llm_context": api_instance.llm_context,
        }
        rendered_prompt = template.Template(prompt, self._hass).async_render(
            variables,
            parse_result=False,
        )
        parts = [rendered_prompt, api_instance.api_prompt]
        if not any(tool.name.endswith("GetDateTime") for tool in api_instance.tools):
            parts.append(
                template.Template(llm.DATE_TIME_PROMPT, self._hass).async_render(
                    variables,
                    parse_result=False,
                )
            )
        parts.append(f"Respond using language and locale {language}.")
        return "\n".join(part for part in parts if part)

    async def _async_connect(self, snapshot: _RegistrationSnapshot) -> bool:
        """Serve one WebSocket generation, returning whether it registered."""
        async with self._session.ws_connect(
            self._url,
            headers=self._headers,
            heartbeat=30,
            max_msg_size=MAX_REALTIME_TOOL_MESSAGE_BYTES,
        ) as websocket:
            await self._async_send_text(websocket, snapshot.serialized)
            return await self._async_serve_connection(websocket, snapshot)

    async def _async_serve_connection(
        self,
        websocket: ClientWebSocketResponse,
        snapshot: _RegistrationSnapshot,
    ) -> bool:
        """Validate and handle events for exactly one bridge generation."""
        generation: str | None = None
        seen_call_ids: set[str] = set()
        pending_calls: set[asyncio.Task[None]] = set()
        registered = False
        registration_deadline = (
            asyncio.get_running_loop().time() + self._registration_timeout
        )
        try:
            while True:
                if generation is None:
                    async with asyncio.timeout_at(registration_deadline):
                        message = await websocket.receive()
                else:
                    message = await websocket.receive()
                if message.type is WSMsgType.ERROR:
                    raise RealtimeToolBrokerError("Realtime tool WebSocket failed")
                if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
                if message.type is not WSMsgType.TEXT:
                    raise RealtimeToolBrokerProtocolError(
                        "Realtime tool protocol only accepts text events"
                    )
                if len(message.data.encode()) > MAX_REALTIME_TOOL_MESSAGE_BYTES:
                    raise RealtimeToolBrokerProtocolError(
                        "Realtime tool event exceeds its size limit"
                    )
                try:
                    event = json_loads_object(message.data)
                except (*JSON_DECODE_EXCEPTIONS, ValueError) as err:
                    raise RealtimeToolBrokerProtocolError(
                        "Realtime tool event is not a JSON object"
                    ) from err

                event_type = event.get("type")
                if event_type == "ping":
                    await self._async_send_text(
                        websocket, json_dumps_sorted({"type": "pong"})
                    )
                    continue
                if event_type == "pong":
                    continue
                if event_type == "registered":
                    if generation is not None:
                        raise RealtimeToolBrokerProtocolError(
                            "Bridge registered the realtime tool broker twice"
                        )
                    if event.get("protocol_version") != REALTIME_TOOL_PROTOCOL_VERSION:
                        raise RealtimeToolBrokerProtocolError(
                            "Bridge selected an unsupported realtime tool protocol"
                        )
                    generation = _bounded_string(
                        event.get("generation"),
                        "generation",
                        _MAX_GENERATION_CHARS,
                    )
                    registered = True
                    self._connected = True
                    self._reauth_started = False
                    continue
                if event_type == "tool_call":
                    if generation is None:
                        raise RealtimeToolBrokerProtocolError(
                            "Bridge sent a tool call before registration"
                        )
                    if len(pending_calls) >= _MAX_PENDING_TOOL_CALLS:
                        raise RealtimeToolBrokerProtocolError(
                            "Bridge exceeded the pending tool-call limit"
                        )
                    if len(seen_call_ids) >= _MAX_CALLS_PER_GENERATION:
                        raise RealtimeToolBrokerProtocolError(
                            "Bridge exceeded the generation tool-call limit"
                        )
                    task = self._hass.async_create_task(
                        self._async_handle_tool_call_safely(
                            websocket,
                            snapshot,
                            generation,
                            seen_call_ids,
                            event,
                        ),
                        f"{DOMAIN} realtime Home Assistant tool call",
                        eager_start=True,
                    )
                    pending_calls.add(task)
                    task.add_done_callback(pending_calls.discard)
                    continue
                if event_type == "error":
                    raise RealtimeToolBrokerProtocolError(
                        "Bridge rejected the realtime tool registration"
                    )
                raise RealtimeToolBrokerProtocolError(
                    f"Unknown realtime tool event type: {event_type!r}"
                )
            return registered
        finally:
            self._connected = False
            for task in pending_calls:
                task.cancel()
            if pending_calls:
                await asyncio.gather(*pending_calls, return_exceptions=True)

    async def _async_handle_tool_call_safely(
        self,
        websocket: ClientWebSocketResponse,
        snapshot: _RegistrationSnapshot,
        generation: str,
        seen_call_ids: set[str],
        event: Mapping[str, Any],
    ) -> None:
        """Run one call task and close the generation on transport failure."""
        try:
            await self._async_handle_tool_call(
                websocket,
                snapshot,
                generation,
                seen_call_ids,
                event,
            )
        except asyncio.CancelledError:
            raise
        except ClientError, RealtimeToolBrokerError:
            _LOGGER.exception("Realtime Home Assistant tool result delivery failed")
            await websocket.close()

    async def _async_handle_tool_call(
        self,
        websocket: ClientWebSocketResponse,
        snapshot: _RegistrationSnapshot,
        generation: str,
        seen_call_ids: set[str],
        event: Mapping[str, Any],
    ) -> None:
        """Execute one live, known, generation-scoped tool exactly once."""
        event_generation = _bounded_string(
            event.get("generation"), "generation", _MAX_GENERATION_CHARS
        )
        if event_generation != generation:
            _LOGGER.warning("Ignored stale realtime Home Assistant tool call")
            return

        call_id = _bounded_string(event.get("call_id"), "call_id", _MAX_CALL_ID_CHARS)
        if call_id in seen_call_ids:
            _LOGGER.warning("Ignored duplicate realtime Home Assistant tool call")
            return
        seen_call_ids.add(call_id)

        name = _bounded_tool_name(event.get("name"))
        arguments = event.get("arguments")
        if not isinstance(arguments, dict):
            await self._async_send_tool_result(
                websocket,
                generation,
                call_id,
                False,
                _tool_error("invalid_arguments", "Tool arguments must be an object"),
            )
            return
        if _serialized_size(arguments) > MAX_REALTIME_TOOL_ARGUMENT_BYTES:
            await self._async_send_tool_result(
                websocket,
                generation,
                call_id,
                False,
                _tool_error(
                    "invalid_arguments", "Tool arguments exceed the size limit"
                ),
            )
            return
        if name not in snapshot.tool_names or websocket.closed:
            await self._async_send_tool_result(
                websocket,
                generation,
                call_id,
                False,
                _tool_error("tool_not_available", "Tool is not available"),
            )
            return

        try:
            async with asyncio.timeout(self._tool_call_timeout):
                result = _validate_tool_result(
                    await snapshot.api_instance.async_call_tool(
                        llm.ToolInput(id=call_id, tool_name=name, tool_args=arguments)
                    )
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            _LOGGER.warning("Realtime Home Assistant tool call timed out")
            result = {
                "error": "tool_timeout",
                "error_text": (
                    "Home Assistant tool call timed out; outcome is unknown; "
                    "do not retry"
                ),
                "do_not_retry": True,
            }
            success = False
        except Exception:
            # Third-party/custom LLM tools are not required to normalize their
            # exceptions. Keep all details in HA logs and return a stable error.
            _LOGGER.exception("Realtime Home Assistant tool call failed")
            result = _tool_error("tool_failed", "Home Assistant tool call failed")
            success = False
        else:
            success = True

        if websocket.closed:
            # The action was never retried and no result is retained for a future
            # generation. A disconnect therefore cannot authorize more work.
            return
        await self._async_send_tool_result(
            websocket,
            generation,
            call_id,
            success,
            result,
        )

    async def _async_send_tool_result(
        self,
        websocket: ClientWebSocketResponse,
        generation: str,
        call_id: str,
        success: bool,
        result: Mapping[str, Any],
    ) -> None:
        """Send one bounded canonical result without retaining it for retry."""
        message = json_dumps_sorted(
            {
                "type": "tool_result",
                "generation": generation,
                "call_id": call_id,
                "success": success,
                "result": dict(result),
            }
        )
        if len(message.encode()) > MAX_REALTIME_TOOL_MESSAGE_BYTES:
            raise RealtimeToolBrokerProtocolError(
                "Realtime tool result event exceeds its size limit"
            )
        await self._async_send_text(websocket, message)

    async def _async_send_text(
        self,
        websocket: ClientWebSocketResponse,
        message: str,
    ) -> None:
        """Serialize concurrent result and keepalive writes per connection."""
        try:
            async with asyncio.timeout(self._send_timeout):
                async with self._send_lock:
                    if websocket.closed:
                        raise RealtimeToolBrokerError(
                            "Realtime Home Assistant tool broker disconnected"
                        )
                    await websocket.send_str(message)
        except TimeoutError as err:
            raise RealtimeToolBrokerError(
                "Realtime Home Assistant tool broker send timed out"
            ) from err


def _tool_error(code: str, message: str) -> dict[str, str]:
    """Return a stable result object that never exposes exception details."""
    return {"error": code, "error_text": message}


def _validate_tool_result(result: object) -> dict[str, Any]:
    """Validate a tool result without leaking serialization errors."""
    try:
        result = tool_result_data(result)
    except TypeError:
        raise HomeAssistantError("Home Assistant tool returned a non-object") from None
    try:
        size = _serialized_size(result)
    except RealtimeToolBrokerProtocolError as err:
        raise HomeAssistantError("Home Assistant tool result is not JSON") from err
    if size > MAX_REALTIME_TOOL_RESULT_BYTES:
        raise HomeAssistantError("Home Assistant tool result is too large")
    return result


@callback
def async_start_realtime_tool_broker(
    hass: HomeAssistant,
    entry: ConfigEntry,
    session: ClientSession,
) -> RealtimeToolBroker | None:
    """Start the broker when the config entry has exactly one authority."""
    if (authority := select_realtime_authority(entry)) is None:
        return None
    broker = RealtimeToolBroker(hass, entry, authority, session)
    brokers: dict[str, RealtimeToolBroker] = hass.data.setdefault(
        DATA_REALTIME_TOOL_BROKERS, {}
    )
    brokers[entry.entry_id] = broker

    async def unload_broker() -> None:
        """Stop before removing the broker from Home Assistant runtime data."""
        await broker.async_stop()
        brokers.pop(entry.entry_id, None)
        if not brokers:
            hass.data.pop(DATA_REALTIME_TOOL_BROKERS, None)

    entry.async_on_unload(unload_broker)
    broker.async_start()
    return broker
