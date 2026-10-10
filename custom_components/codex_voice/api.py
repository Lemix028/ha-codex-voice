"""Authenticated client for the local Codex Voice bridge."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Callable,
    Collection,
    Mapping,
)
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Final, NoReturn

from aiohttp import (
    ClientError,
    ClientResponse,
    ClientSession,
    ClientTimeout,
    WSMsgType,
    WSServerHandshakeError,
)
from homeassistant.helpers import chat_session
from homeassistant.helpers.json import JSON_DUMP

from .const import (
    CONVERSATION_TIMEOUT,
    HEALTH_TIMEOUT,
    MAX_AUDIO_BYTES,
    MAX_SYNTHESIZED_AUDIO_BYTES,
    MAX_TOOL_CALLS,
    REQUEST_TIMEOUT,
)
from .json_serialization import SerializationFailure, normalize_json

_LOGGER = logging.getLogger(__name__)

_JSON_CONTENT_TYPES: Final = ("application/json", "text/json")
_WAV_CONTENT_TYPES: Final = ("audio/wav", "audio/wave", "audio/x-wav")
_DEFAULT_SYNTHESIS_SAMPLE_RATE: Final = 24_000
_DEFAULT_SYNTHESIS_CHANNELS: Final = 1
_DEFAULT_SYNTHESIS_SAMPLE_WIDTH: Final = 2
_SUPPORTED_SYNTHESIS_SAMPLE_RATES: Final = frozenset({16_000, 24_000})


def _streaming_wav_header(
    sample_rate: int,
    channels: int,
    sample_width: int,
) -> bytes:
    """Return the bridge's canonical EOF-terminated PCM WAV header."""
    block_align = channels * sample_width
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        0xFFFFFFFF,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        sample_rate,
        sample_rate * block_align,
        block_align,
        sample_width * 8,
        b"data",
        0xFFFFFFFF,
    )


_STREAMING_WAV_HEADER: Final = _streaming_wav_header(
    _DEFAULT_SYNTHESIS_SAMPLE_RATE,
    _DEFAULT_SYNTHESIS_CHANNELS,
    _DEFAULT_SYNTHESIS_SAMPLE_WIDTH,
)
_AUDIO_STREAM_CHUNK_BYTES: Final = 64 * 1024
_SPEECH_SESSION_HANDOFF_VERSION: Final = 1
_SPEECH_SESSION_HANDOFF_OPTION: Final = "_codex_voice_pipeline_handoff"
_SPEECH_SESSION_HANDOFF_OPTION_VALUE: Final = 1

JsonObject = dict[str, Any]
DeltaHandler = Callable[[str], Awaitable[None]]
ToolHandler = Callable[["BridgeToolCall"], Awaitable[JsonObject]]


class BridgeError(Exception):
    """Base class for bridge errors."""


class BridgeAuthenticationError(BridgeError):
    """The bridge rejected the configured access token."""


class BridgeConnectionError(BridgeError):
    """The bridge could not be reached."""


class BridgeProtocolError(BridgeError):
    """The bridge returned an invalid or incompatible response."""


class BridgeBusyError(BridgeError):
    """The bridge cannot accept another speech operation right now."""


class BridgeQuotaError(BridgeError):
    """The ChatGPT subscription quota is exhausted."""


class BridgeStreamingUnsupported(BridgeError):
    """The bridge predates the streaming transcription endpoint."""


def _serialize_conversation_message(payload: JsonObject) -> str:
    """Serialize an outbound conversation event using Home Assistant's policy."""
    try:
        return JSON_DUMP(normalize_conversation_value(payload))
    except TypeError, ValueError:
        raise BridgeProtocolError(
            "Conversation payload contains unsupported JSON values"
        ) from None


def normalize_conversation_value(value: Any, *, path: str = "$") -> Any:
    """Diagnose unsupported data using only redacted path and Python type."""
    try:
        return normalize_json(value, path=path)
    except SerializationFailure as err:
        _LOGGER.warning(
            "Conversation JSON serialization failed: path=%s type=%s reason=%s",
            err.path,
            err.python_type,
            err.reason,
        )
        raise BridgeProtocolError(
            "Conversation payload contains unsupported JSON values"
        ) from None


def _validate_synthesis_audio_preferences(
    *,
    sample_rate: object = None,
    channels: object = None,
    sample_width: object = None,
) -> tuple[int | None, int | None, int | None]:
    """Validate optional bridge output preferences and normalize integers."""

    def optional_int(value: object, key: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise BridgeProtocolError(f"Synthesis {key} must be an integer")
        try:
            return int(value)
        except ValueError as err:
            raise BridgeProtocolError(f"Synthesis {key} must be an integer") from err

    normalized_sample_rate = optional_int(sample_rate, "sample_rate")
    normalized_channels = optional_int(channels, "channels")
    normalized_sample_width = optional_int(sample_width, "sample_width")
    if (
        normalized_sample_rate is not None
        and normalized_sample_rate not in _SUPPORTED_SYNTHESIS_SAMPLE_RATES
    ):
        raise BridgeProtocolError("Synthesis sample_rate must be 16000 or 24000")
    if normalized_channels is not None and normalized_channels != 1:
        raise BridgeProtocolError("Synthesis channels must be 1")
    if normalized_sample_width is not None and normalized_sample_width != 2:
        raise BridgeProtocolError("Synthesis sample_width must be 2")
    return (
        normalized_sample_rate,
        normalized_channels,
        normalized_sample_width,
    )


@dataclass(frozen=True, slots=True)
class BridgeToolCall:
    """A tool request received during a conversation turn."""

    call_id: str
    name: str
    arguments: JsonObject
    request_id: int | str | None = None


@dataclass(frozen=True, slots=True)
class BridgeAudio:
    """Audio returned by the bridge."""

    data: bytes
    audio_format: str
    sample_rate: int = 24000
    channels: int = 1
    sample_width: int = 2


@dataclass(frozen=True, slots=True)
class _SpeechSessionHandoffRequest:
    """Private correlation for one Assist pipeline transcription."""

    client: BridgeClient
    session: chat_session.ChatSession
    preparation: _SpeechSessionHandoffPreparation
    voice: str
    language: str


@dataclass(slots=True)
class _SpeechSessionHandoffPreparation:
    """One pre-STT pipeline preparation shared by copied async contexts."""

    client: BridgeClient
    session: chat_session.ChatSession
    voice: str
    consumed: bool = False


@dataclass(slots=True)
class _PendingSpeechSessionHandoff:
    """Mutable one-shot ticket shared by copied async contexts."""

    client: BridgeClient
    session: chat_session.ChatSession
    preparation: _SpeechSessionHandoffPreparation
    token: str = field(repr=False)
    voice: str
    language: str
    expires_at: float
    consumed_or_revoked: bool = False
    expiry_handle: asyncio.TimerHandle | None = None


_SPEECH_SESSION_HANDOFF_PREPARATION: ContextVar[
    _SpeechSessionHandoffPreparation | None
] = ContextVar("codex_voice_speech_session_handoff_preparation", default=None)
_PENDING_SPEECH_SESSION_HANDOFF: ContextVar[_PendingSpeechSessionHandoff | None] = (
    ContextVar(
        "codex_voice_pending_speech_session_handoff",
        default=None,
    )
)
_HANDOFF_RELEASE_TASKS: set[asyncio.Task[None]] = set()


def normalize_bridge_url(url: str) -> str:
    """Return a stable bridge URL without a trailing slash."""
    return url.strip().rstrip("/")


class BridgeClient:
    """Small, explicit client for the bridge's supported API surface."""

    def __init__(
        self,
        session: ClientSession,
        base_url: str,
        access_token: str,
    ) -> None:
        """Initialize the bridge client."""
        self._session = session
        self.base_url = normalize_bridge_url(base_url)
        self._headers = {"Authorization": f"Bearer {access_token}"}
        self._handoff_release_tasks: set[asyncio.Task[None]] = set()

    def cancel_handoff_release_tasks(self) -> None:
        """Cancel best-effort cleanup jobs when the owning entry unloads."""
        for task in tuple(self._handoff_release_tasks):
            task.cancel()

    def _url(self, path: str) -> str:
        """Build a bridge URL."""
        return f"{self.base_url}{path}"

    async def async_health(self) -> JsonObject:
        """Return bridge health and authentication metadata."""
        try:
            async with self._session.get(
                self._url("/health"),
                headers=self._headers,
                timeout=ClientTimeout(total=HEALTH_TIMEOUT),
            ) as response:
                await self._raise_for_status(response)
                payload = await self._read_json(response)
        except BridgeError:
            raise
        except (TimeoutError, ClientError) as err:
            raise BridgeConnectionError(
                "Unable to connect to the Codex bridge"
            ) from err

        if payload.get("status") not in (None, "ok", "ready"):
            raise BridgeConnectionError("The Codex bridge is not ready")
        return payload

    async def async_converse(
        self,
        start: Mapping[str, Any],
        *,
        async_handle_delta: DeltaHandler,
        async_handle_tool: ToolHandler,
    ) -> JsonObject:
        """Run one conversation turn over the bridge WebSocket."""
        start_message = _serialize_conversation_message({"type": "start", **start})
        try:
            async with asyncio.timeout(CONVERSATION_TIMEOUT):
                async with self._session.ws_connect(
                    self._url("/v1/conversation"),
                    headers=self._headers,
                    heartbeat=30,
                ) as websocket:
                    await websocket.send_str(start_message)
                    tool_calls = 0
                    received_delta = False
                    async for message in websocket:
                        if message.type is WSMsgType.ERROR:
                            raise BridgeConnectionError(
                                "The Codex bridge WebSocket failed"
                            )
                        if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                            break
                        if message.type is not WSMsgType.TEXT:
                            continue

                        event = self._decode_event(message.data)
                        event_type = event.get("type")
                        if event_type == "delta":
                            delta = event.get("delta", event.get("content", ""))
                            if not isinstance(delta, str):
                                raise BridgeProtocolError(
                                    "Conversation delta must be text"
                                )
                            if delta:
                                received_delta = True
                                await async_handle_delta(delta)
                            continue

                        if event_type == "tool_call":
                            tool_calls += 1
                            if tool_calls > MAX_TOOL_CALLS:
                                raise BridgeProtocolError(
                                    "Conversation exceeded the tool-call limit"
                                )
                            tool_call = self._decode_tool_call(event)
                            result = await async_handle_tool(tool_call)
                            await websocket.send_str(
                                _serialize_conversation_message(
                                    {
                                        "type": "tool_result",
                                        "request_id": tool_call.request_id,
                                        "call_id": tool_call.call_id,
                                        "result": result,
                                        "success": "error" not in result,
                                    }
                                )
                            )
                            continue

                        if event_type == "done":
                            final_text = event.get("text", event.get("content"))
                            if (
                                not received_delta
                                and isinstance(final_text, str)
                                and final_text
                            ):
                                await async_handle_delta(final_text)
                            return event

                        if event_type == "error":
                            self._raise_event_error(event)

                        if event_type not in (
                            "started",
                            "start",
                            "item",
                            "event",
                            "pong",
                        ):
                            raise BridgeProtocolError(
                                f"Unknown conversation event: {event_type!r}"
                            )
        except BridgeError:
            raise
        except TimeoutError as err:
            raise BridgeConnectionError("The conversation request timed out") from err
        except ClientError as err:
            raise BridgeConnectionError("The conversation connection failed") from err

        raise BridgeConnectionError("The conversation connection closed unexpectedly")

    async def async_transcribe(
        self,
        audio: bytes,
        metadata: Mapping[str, Any],
        *,
        prompt: str | None,
        speech_session_handoff: _SpeechSessionHandoffRequest | None = None,
    ) -> str:
        """Transcribe raw PCM with the bridge."""
        if len(audio) > MAX_AUDIO_BYTES:
            raise BridgeProtocolError("Audio input exceeds the 16 MiB limit")

        payload: JsonObject = {
            "audio": base64.b64encode(audio).decode("ascii"),
            "encoding": "base64",
            "format": "pcm",
            **dict(metadata),
        }
        if prompt:
            payload["prompt"] = prompt
        self._add_speech_session_handoff_request(payload, speech_session_handoff)

        response_payload = await self._async_post_json("/v1/transcribe", payload)
        text = response_payload.get("text", response_payload.get("transcript"))
        if not isinstance(text, str):
            raise BridgeProtocolError("Transcription response did not contain text")
        self._store_speech_session_handoff(response_payload, speech_session_handoff)
        return text

    async def async_transcribe_stream(
        self,
        stream: AsyncIterable[bytes],
        metadata: Mapping[str, Any],
        *,
        prompt: str | None,
        speech_session_handoff: _SpeechSessionHandoffRequest | None = None,
    ) -> str:
        """Transcribe a bounded PCM stream over an authenticated WebSocket."""
        start = self._transcription_start(metadata, prompt=prompt)
        self._add_speech_session_handoff_request(start, speech_session_handoff)

        try:
            async with asyncio.timeout(REQUEST_TIMEOUT):
                try:
                    async with self._session.ws_connect(
                        self._url("/v1/transcribe/stream"),
                        headers=self._headers,
                    ) as websocket:
                        try:
                            await websocket.send_json(start)
                            started = await self._receive_transcription_event(websocket)
                            self._validate_transcription_started(started)

                            total_bytes = 0
                            pending_sample = b""
                            async for chunk in stream:
                                if not isinstance(chunk, bytes):
                                    raise BridgeProtocolError(
                                        "Transcription audio chunks must be bytes"
                                    )
                                if not chunk:
                                    continue
                                total_bytes += len(chunk)
                                if total_bytes > MAX_AUDIO_BYTES:
                                    raise BridgeProtocolError(
                                        "Audio input exceeds the 16 MiB limit"
                                    )
                                if pending_sample:
                                    chunk = pending_sample + chunk
                                complete_bytes = len(chunk) - (len(chunk) % 2)
                                for offset in range(
                                    0, complete_bytes, _AUDIO_STREAM_CHUNK_BYTES
                                ):
                                    await websocket.send_bytes(
                                        chunk[
                                            offset : min(
                                                offset + _AUDIO_STREAM_CHUNK_BYTES,
                                                complete_bytes,
                                            )
                                        ]
                                    )
                                pending_sample = chunk[complete_bytes:]

                            if pending_sample:
                                raise BridgeProtocolError(
                                    "Transcription audio contained incomplete PCM16 data"
                                )

                            await websocket.send_json({"type": "end"})
                            result = await self._receive_transcription_event(websocket)
                            text = self._decode_transcription_result(result)
                            self._store_speech_session_handoff(
                                result, speech_session_handoff
                            )
                            return text
                        finally:
                            await websocket.close()
                except WSServerHandshakeError as err:
                    self._raise_transcription_handshake_error(err)
        except BridgeError:
            raise
        except TimeoutError as err:
            raise BridgeConnectionError("Speech transcription timed out") from err
        except (ClientError, ConnectionError) as err:
            raise BridgeConnectionError("Speech transcription failed") from err

    async def async_synthesize(
        self,
        text: str,
        *,
        language: str,
        voice: str,
        instructions: str | None,
        speech_session_handoff_token: str | None = None,
        sample_rate: int | None = None,
        channels: int | None = None,
        sample_width: int | None = None,
    ) -> BridgeAudio:
        """Synthesize text and return WAV or PCM audio."""
        sample_rate, channels, sample_width = _validate_synthesis_audio_preferences(
            sample_rate=sample_rate,
            channels=channels,
            sample_width=sample_width,
        )
        payload: JsonObject = {
            "text": text,
            "language": language,
            "voice": voice,
            "format": "wav",
        }
        if instructions:
            payload["instructions"] = instructions
        if speech_session_handoff_token:
            payload["speech_session_handoff_token"] = speech_session_handoff_token
        if sample_rate is not None:
            payload["sample_rate"] = sample_rate
        if channels is not None:
            payload["channels"] = channels
        if sample_width is not None:
            payload["sample_width"] = sample_width

        try:
            async with self._session.post(
                self._url("/v1/synthesize"),
                headers=self._headers,
                json=payload,
                timeout=ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                await self._raise_for_status(response)
                content_type = response.content_type.lower()
                if content_type in _JSON_CONTENT_TYPES:
                    result = await self._read_json(response)
                    return self._decode_audio_json(result)
                if content_type not in _WAV_CONTENT_TYPES:
                    raise BridgeProtocolError(
                        f"Synthesis returned unsupported content type {content_type!r}"
                    )
                audio = await self._read_bounded_audio(response)
                self._validate_wav_header(audio)
                return BridgeAudio(data=audio, audio_format="wav")
        except BridgeError:
            raise
        except (TimeoutError, ClientError) as err:
            raise BridgeConnectionError("Speech synthesis failed") from err

    async def async_synthesize_stream(
        self,
        text: str,
        *,
        language: str,
        voice: str,
        instructions: str | None,
        speech_session_handoff_token: str | None = None,
        sample_rate: int | None = None,
        channels: int | None = None,
        sample_width: int | None = None,
    ) -> AsyncGenerator[bytes]:
        """Yield a bounded WAV response as the bridge produces it."""
        sample_rate, channels, sample_width = _validate_synthesis_audio_preferences(
            sample_rate=sample_rate,
            channels=channels,
            sample_width=sample_width,
        )
        payload: JsonObject = {
            "text": text,
            "language": language,
            "voice": voice,
            "format": "wav",
        }
        if instructions:
            payload["instructions"] = instructions
        if speech_session_handoff_token:
            payload["speech_session_handoff_token"] = speech_session_handoff_token
        if sample_rate is not None:
            payload["sample_rate"] = sample_rate
        if channels is not None:
            payload["channels"] = channels
        if sample_width is not None:
            payload["sample_width"] = sample_width

        effective_sample_rate = sample_rate or _DEFAULT_SYNTHESIS_SAMPLE_RATE
        effective_channels = channels or _DEFAULT_SYNTHESIS_CHANNELS
        effective_sample_width = sample_width or _DEFAULT_SYNTHESIS_SAMPLE_WIDTH
        expected_header = _streaming_wav_header(
            effective_sample_rate,
            effective_channels,
            effective_sample_width,
        )
        acceptable_headers = (
            (expected_header,)
            if expected_header == _STREAMING_WAV_HEADER
            else (expected_header, _STREAMING_WAV_HEADER)
        )
        frame_bytes = effective_channels * effective_sample_width
        preamble_bytes = len(expected_header) + frame_bytes

        try:
            async with self._session.post(
                self._url("/v1/synthesize/stream"),
                headers=self._headers,
                json=payload,
                timeout=ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                try:
                    await self._raise_for_status(response)
                    content_type = response.content_type.lower()
                    if content_type not in _WAV_CONTENT_TYPES:
                        raise BridgeProtocolError(
                            "Synthesis stream returned unsupported content type "
                            f"{content_type!r}"
                        )
                    if (
                        response.content_length is not None
                        and response.content_length > MAX_SYNTHESIZED_AUDIO_BYTES
                    ):
                        raise BridgeProtocolError(
                            "Synthesized audio exceeds the 50 MiB limit"
                        )

                    total_bytes = 0
                    header = bytearray()
                    header_validated = False
                    async for chunk in response.content.iter_chunked(
                        _AUDIO_STREAM_CHUNK_BYTES
                    ):
                        if not chunk:
                            continue
                        total_bytes += len(chunk)
                        if total_bytes > MAX_SYNTHESIZED_AUDIO_BYTES:
                            raise BridgeProtocolError(
                                "Synthesized audio exceeds the 50 MiB limit"
                            )

                        if header_validated:
                            yield chunk
                            continue

                        header.extend(chunk)
                        if len(header) < preamble_bytes:
                            continue
                        self._validate_streaming_wav_header(
                            bytes(header), acceptable_headers
                        )
                        header_validated = True
                        yield bytes(header)

                    if not header_validated:
                        self._validate_streaming_wav_header(
                            bytes(header), acceptable_headers
                        )
                    if (total_bytes - len(expected_header)) % frame_bytes:
                        raise BridgeProtocolError(
                            "Synthesis stream contained an incomplete audio frame"
                        )
                finally:
                    # aiohttp's context manager also releases the response, but
                    # closing explicitly makes generator cancellation immediate.
                    response.close()
        except BridgeError:
            raise
        except (TimeoutError, ClientError) as err:
            raise BridgeConnectionError("Speech synthesis failed") from err

    async def async_release_speech_session_handoff(self, token: str) -> None:
        """Best-effort release is exposed for private component coordination."""
        try:
            async with self._session.post(
                self._url("/v1/speech-session/release"),
                headers=self._headers,
                json={"speech_session_handoff_token": token},
                timeout=ClientTimeout(total=HEALTH_TIMEOUT),
            ) as response:
                await self._raise_for_status(response)
        except BridgeError:
            raise
        except (TimeoutError, ClientError) as err:
            raise BridgeConnectionError("Speech session release failed") from err

    async def _async_post_json(self, path: str, payload: JsonObject) -> JsonObject:
        """POST JSON and return a JSON object."""
        try:
            async with self._session.post(
                self._url(path),
                headers=self._headers,
                json=payload,
                timeout=ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                await self._raise_for_status(response)
                return await self._read_json(response)
        except BridgeError:
            raise
        except (TimeoutError, ClientError) as err:
            raise BridgeConnectionError("The Codex bridge request failed") from err

    @staticmethod
    async def _raise_for_status(response: ClientResponse) -> None:
        """Translate HTTP errors into stable integration errors."""
        if response.status < 400:
            return
        if response.status in (401, 403):
            raise BridgeAuthenticationError("The bridge access token was rejected")
        if response.status == 409:
            raise BridgeBusyError("The bridge is busy")
        if response.status == 429:
            raise BridgeQuotaError("The subscription quota is exhausted")
        if response.status >= 500:
            raise BridgeConnectionError(f"The bridge returned HTTP {response.status}")
        raise BridgeProtocolError(f"The bridge returned HTTP {response.status}")

    @staticmethod
    async def _read_json(response: ClientResponse) -> JsonObject:
        """Read and validate an object JSON response."""
        try:
            payload = await response.json(content_type=None)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as err:
            raise BridgeProtocolError("The bridge returned invalid JSON") from err
        if not isinstance(payload, dict):
            raise BridgeProtocolError("The bridge JSON response must be an object")
        return payload

    @staticmethod
    async def _read_bounded_audio(response: ClientResponse) -> bytes:
        """Stream an audio response while enforcing the configured size cap."""
        audio = bytearray()
        async for chunk in response.content.iter_chunked(64 * 1024):
            audio.extend(chunk)
            if len(audio) > MAX_SYNTHESIZED_AUDIO_BYTES:
                raise BridgeProtocolError("Synthesized audio exceeds the 50 MiB limit")
        return bytes(audio)

    @staticmethod
    def _decode_event(raw_event: str) -> JsonObject:
        """Decode a WebSocket event."""
        try:
            event = json.loads(raw_event)
        except (json.JSONDecodeError, TypeError) as err:
            raise BridgeProtocolError("The bridge sent invalid WebSocket JSON") from err
        if not isinstance(event, dict):
            raise BridgeProtocolError("The bridge WebSocket event must be an object")
        return event

    @staticmethod
    async def _receive_transcription_event(websocket: Any) -> JsonObject:
        """Receive one transcription protocol event."""
        while True:
            message = await websocket.receive()
            if message.type is WSMsgType.TEXT:
                event = BridgeClient._decode_event(message.data)
                if event.get("type") == "error":
                    BridgeClient._raise_event_error(event)
                return event
            if message.type is WSMsgType.ERROR:
                raise BridgeConnectionError(
                    "The transcription WebSocket connection failed"
                )
            if message.type in (
                WSMsgType.CLOSE,
                WSMsgType.CLOSED,
                WSMsgType.CLOSING,
            ):
                raise BridgeConnectionError(
                    "The transcription WebSocket closed unexpectedly"
                )
            if message.type is WSMsgType.BINARY:
                raise BridgeProtocolError(
                    "The transcription WebSocket returned unexpected binary data"
                )

    @staticmethod
    def _transcription_start(
        metadata: Mapping[str, Any], *, prompt: str | None
    ) -> JsonObject:
        """Build and validate a streaming transcription start event."""
        start: JsonObject = {
            "type": "start",
            "protocol_version": 1,
            "format": "pcm",
            "codec": "pcm",
        }
        for key in ("sample_rate", "bit_rate", "channels"):
            value = metadata.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise BridgeProtocolError(
                    f"Transcription {key} must be a positive integer"
                )
            start[key] = value
        language = metadata.get("language")
        if not isinstance(language, str) or not language:
            raise BridgeProtocolError("Transcription language must be text")
        start["language"] = language
        if prompt:
            start["prompt"] = prompt
        return start

    @staticmethod
    def _validate_transcription_started(event: JsonObject) -> None:
        """Validate the streaming transcription acknowledgement."""
        if event.get("type") != "started":
            raise BridgeProtocolError(
                "Transcription stream did not begin with a started event"
            )
        version = event.get("protocol_version")
        if not isinstance(version, int) or isinstance(version, bool) or version != 1:
            raise BridgeProtocolError(
                "Transcription stream used an incompatible protocol version"
            )

    @staticmethod
    def _decode_transcription_result(event: JsonObject) -> str:
        """Validate and return a final streaming transcription result."""
        if event.get("type") != "result":
            raise BridgeProtocolError(
                "Transcription stream did not return a result event"
            )
        text = event.get("text")
        if not isinstance(text, str):
            raise BridgeProtocolError("Transcription result did not contain text")
        if "language" in event and not isinstance(event["language"], str):
            raise BridgeProtocolError("Transcription result language must be text")
        return text

    def _add_speech_session_handoff_request(
        self,
        payload: JsonObject,
        request: _SpeechSessionHandoffRequest | None,
    ) -> None:
        """Add the private v1 handoff opt-in for this exact client."""
        if request is None or request.client is not self:
            return
        payload["speech_session_handoff"] = {
            "version": _SPEECH_SESSION_HANDOFF_VERSION,
            "voice": request.voice,
            "language": request.language,
        }

    def _store_speech_session_handoff(
        self,
        payload: JsonObject,
        request: _SpeechSessionHandoffRequest | None,
    ) -> None:
        """Privately retain valid one-time result metadata for downstream TTS."""
        if request is None or request.client is not self:
            return
        _store_pending_speech_session_handoff(payload, request)

    @staticmethod
    def _raise_transcription_handshake_error(
        error: WSServerHandshakeError,
    ) -> NoReturn:
        """Translate a failed streaming WebSocket upgrade."""
        status = error.status
        if status in (404, 405, 426):
            raise BridgeStreamingUnsupported(
                "The bridge does not support streaming transcription"
            ) from error
        if status in (401, 403):
            raise BridgeAuthenticationError(
                "The bridge access token was rejected"
            ) from error
        if status == 409:
            raise BridgeBusyError("The bridge is busy") from error
        if status == 429:
            raise BridgeQuotaError("The subscription quota is exhausted") from error
        if status >= 500:
            raise BridgeConnectionError(f"The bridge returned HTTP {status}") from error
        raise BridgeProtocolError(f"The bridge returned HTTP {status}") from error

    @staticmethod
    def _decode_tool_call(event: JsonObject) -> BridgeToolCall:
        """Validate a tool-call event."""
        call_id = event.get("id", event.get("call_id"))
        request_id = event.get("request_id")
        name = event.get("name", event.get("tool_name"))
        arguments = event.get("arguments", event.get("args", {}))
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError as err:
                raise BridgeProtocolError("Tool arguments are invalid JSON") from err
        if not isinstance(call_id, str) or not call_id:
            raise BridgeProtocolError("Tool call is missing an id")
        if not isinstance(name, str) or not name:
            raise BridgeProtocolError("Tool call is missing a name")
        if not isinstance(arguments, dict):
            raise BridgeProtocolError("Tool arguments must be an object")
        if request_id is not None and not isinstance(request_id, (int, str)):
            raise BridgeProtocolError("Tool call request id is invalid")
        return BridgeToolCall(
            call_id=call_id,
            name=name,
            arguments=arguments,
            request_id=request_id,
        )

    @staticmethod
    def _raise_event_error(event: JsonObject) -> None:
        """Translate a bridge error event."""
        code = event.get("code")
        message = event.get("message", event.get("error"))
        detail = message if isinstance(message, str) else "The bridge reported an error"
        if code in ("invalid_auth", "authentication_required"):
            raise BridgeAuthenticationError(detail)
        if code in ("quota_exhausted", "rate_limited"):
            raise BridgeQuotaError(detail)
        if code in ("busy", "operation_in_progress"):
            raise BridgeBusyError(detail)
        raise BridgeProtocolError(detail)

    @staticmethod
    def _decode_audio_json(payload: JsonObject) -> BridgeAudio:
        """Decode a base64 audio response."""
        encoded_audio = payload.get("audio")
        if not isinstance(encoded_audio, str):
            raise BridgeProtocolError("Synthesis response did not contain audio")
        try:
            audio = base64.b64decode(encoded_audio, validate=True)
        except (ValueError, TypeError) as err:
            raise BridgeProtocolError("Synthesis audio was not valid base64") from err
        BridgeClient._validate_audio_size(audio)

        audio_format = payload.get("format", "pcm")
        if not isinstance(audio_format, str):
            raise BridgeProtocolError("Synthesis audio format must be text")
        if audio_format.lower() in ("wav", "wave"):
            BridgeClient._validate_wav_header(audio)
        return BridgeAudio(
            data=audio,
            audio_format=audio_format.lower(),
            sample_rate=BridgeClient._positive_int(payload, "sample_rate", 24000),
            channels=BridgeClient._positive_int(payload, "channels", 1),
            sample_width=BridgeClient._positive_int(payload, "sample_width", 2),
        )

    @staticmethod
    def _positive_int(payload: JsonObject, key: str, default: int) -> int:
        """Read a positive integer from a bridge response."""
        value = payload.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise BridgeProtocolError(f"Synthesis {key} must be a positive integer")
        return value

    @staticmethod
    def _validate_audio_size(audio: bytes) -> None:
        """Reject unexpectedly large synthesis responses."""
        if len(audio) > MAX_SYNTHESIZED_AUDIO_BYTES:
            raise BridgeProtocolError("Synthesized audio exceeds the 50 MiB limit")

    @staticmethod
    def _validate_wav_header(audio: bytes) -> None:
        """Reject mislabeled or truncated WAV responses."""
        if len(audio) < 12 or audio[:4] != b"RIFF" or audio[8:12] != b"WAVE":
            raise BridgeProtocolError("Synthesis response was not a valid WAV file")

    @staticmethod
    def _validate_streaming_wav_header(
        audio: bytes,
        expected_headers: Collection[bytes] = (_STREAMING_WAV_HEADER,),
    ) -> None:
        """Require the bridge's canonical EOF-terminated PCM16 WAV framing."""
        if len(audio) < len(_STREAMING_WAV_HEADER) + 2:
            raise BridgeProtocolError("Synthesis stream was not a valid WAV file")
        if not any(
            audio[: len(expected_header)] == expected_header
            for expected_header in expected_headers
        ):
            raise BridgeProtocolError("Synthesis stream was not a valid WAV file")


def _prepare_speech_session_handoff(
    client: BridgeClient,
    *,
    voice: str,
) -> bool:
    """Mark one active-session TTS preparation before pipeline STT begins."""
    session = chat_session.current_session.get()
    if session is None:
        return False

    preparation = _SPEECH_SESSION_HANDOFF_PREPARATION.get()
    if preparation is not None:
        if preparation.session is session:
            return (
                preparation.client is client
                and preparation.voice == voice
                and not preparation.consumed
            )
        if not preparation.consumed:
            return False

    _SPEECH_SESSION_HANDOFF_PREPARATION.set(
        _SpeechSessionHandoffPreparation(
            client=client,
            session=session,
            voice=voice,
        )
    )
    return True


def _begin_speech_session_handoff(
    client: BridgeClient,
    *,
    language: str,
) -> _SpeechSessionHandoffRequest | None:
    """Consume only the exact pipeline preparation made before this STT call."""
    _revoke_pending_speech_session_handoff()
    session = chat_session.current_session.get()
    if session is None:
        return None

    preparation = _SPEECH_SESSION_HANDOFF_PREPARATION.get()
    if (
        preparation is None
        or preparation.consumed
        or preparation.client is not client
        or preparation.session is not session
    ):
        return None

    normalized_language = _normalize_speech_language(language)
    if not normalized_language:
        return None

    preparation.consumed = True
    return _SpeechSessionHandoffRequest(
        client=client,
        session=session,
        preparation=preparation,
        voice=preparation.voice,
        language=normalized_language,
    )


def _claim_speech_session_handoff(
    client: BridgeClient,
    *,
    language: str,
    voice: str,
    instructions: str | None,
    options: dict[str, Any],
) -> str | None:
    """Strip the pipeline marker and atomically claim before the first await."""
    marker = options.pop(_SPEECH_SESSION_HANDOFF_OPTION, None)
    if marker != _SPEECH_SESSION_HANDOFF_OPTION_VALUE:
        return None

    pending = _PENDING_SPEECH_SESSION_HANDOFF.get()
    if pending is None:
        return None
    if pending.consumed_or_revoked:
        _PENDING_SPEECH_SESSION_HANDOFF.set(None)
        return None

    session = chat_session.current_session.get()
    if (
        pending.client is not client
        or pending.session is not session
        or pending.preparation is not _SPEECH_SESSION_HANDOFF_PREPARATION.get()
    ):
        return None

    if pending.expires_at <= monotonic():
        _revoke_pending_speech_session_handoff()
        return None
    if (
        pending.voice != voice
        or pending.language != _normalize_speech_language(language)
        or instructions
    ):
        _revoke_pending_speech_session_handoff()
        return None

    pending.consumed_or_revoked = True
    if pending.expiry_handle is not None:
        pending.expiry_handle.cancel()
        pending.expiry_handle = None
    _PENDING_SPEECH_SESSION_HANDOFF.set(None)
    return pending.token


def _revoke_pending_speech_session_handoff() -> None:
    """Atomically revoke and best-effort release this context's pending ticket."""
    pending = _PENDING_SPEECH_SESSION_HANDOFF.get()
    _PENDING_SPEECH_SESSION_HANDOFF.set(None)
    if pending is None or pending.consumed_or_revoked:
        return
    pending.consumed_or_revoked = True
    if pending.expiry_handle is not None:
        pending.expiry_handle.cancel()
        pending.expiry_handle = None
    _schedule_speech_session_handoff_release(pending.client, pending.token)


def _schedule_speech_session_handoff_release(
    client: BridgeClient,
    token: str,
) -> None:
    """Schedule an idempotent release without retaining secrets in task names."""
    task = asyncio.create_task(
        _async_release_speech_session_handoff(client, token),
        name="codex-voice-speech-session-release",
    )
    client_tasks = getattr(client, "_handoff_release_tasks", None)
    if client_tasks is None:
        client_tasks = set()
        client._handoff_release_tasks = client_tasks  # noqa: SLF001
    client_tasks.add(task)
    _HANDOFF_RELEASE_TASKS.add(task)

    def release_done(done: asyncio.Task[None]) -> None:
        """Drop ownership and always retrieve a best-effort task exception."""
        client_tasks.discard(done)
        _HANDOFF_RELEASE_TASKS.discard(done)
        with suppress(asyncio.CancelledError):
            done.exception()

    task.add_done_callback(release_done)


async def _async_release_speech_session_handoff(
    client: BridgeClient,
    token: str,
) -> None:
    """Release a ticket without logging or surfacing best-effort failures."""
    with suppress(BridgeError, RuntimeError):
        await client.async_release_speech_session_handoff(token)


def _store_pending_speech_session_handoff(
    payload: JsonObject,
    request: _SpeechSessionHandoffRequest,
) -> None:
    """Validate opaque result metadata and bind it to the originating context."""
    value = payload.get("speech_session_handoff")
    if not isinstance(value, Mapping):
        return
    version = value.get("version")
    token = value.get("token")
    expires_in_ms = value.get("expires_in_ms")
    voice = value.get("voice")
    language = value.get("language")
    if (
        type(version) is not int
        or version != _SPEECH_SESSION_HANDOFF_VERSION
        or not isinstance(token, str)
        or not token
        or len(token) > 4096
        or type(expires_in_ms) is not int
        or expires_in_ms <= 0
        or not isinstance(voice, str)
        or not voice
        or not isinstance(language, str)
        or not language
    ):
        return

    normalized_language = _normalize_speech_language(language)

    if (
        chat_session.current_session.get() is not request.session
        or _SPEECH_SESSION_HANDOFF_PREPARATION.get() is not request.preparation
        or not request.preparation.consumed
        or voice != request.voice
        or normalized_language != request.language
    ):
        _schedule_speech_session_handoff_release(request.client, token)
        return

    _revoke_pending_speech_session_handoff()
    loop = asyncio.get_running_loop()
    pending = _PendingSpeechSessionHandoff(
        client=request.client,
        session=request.session,
        preparation=request.preparation,
        token=token,
        voice=voice,
        language=normalized_language,
        expires_at=monotonic() + expires_in_ms / 1000,
    )
    _PENDING_SPEECH_SESSION_HANDOFF.set(pending)
    pending.expiry_handle = loop.call_at(
        pending.expires_at,
        _expire_speech_session_handoff,
        pending,
    )


def _expire_speech_session_handoff(
    pending: _PendingSpeechSessionHandoff,
) -> None:
    """Revoke an unclaimed ticket at its local monotonic deadline."""
    pending.expiry_handle = None
    if pending.consumed_or_revoked:
        return
    pending.consumed_or_revoked = True
    _schedule_speech_session_handoff_release(pending.client, pending.token)


def _normalize_speech_language(language: str) -> str:
    """Return the private handoff's stable BCP-47 comparison form."""
    parts = language.strip().replace("_", "-").split("-")
    if not parts or any(not part for part in parts):
        return ""
    return "-".join(
        [
            parts[0].lower(),
            *(
                part.upper() if len(part) == 2 and part.isalpha() else part
                for part in parts[1:]
            ),
        ]
    )
