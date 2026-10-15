"""Authenticated aiohttp service exposed to the Home Assistant component."""

from __future__ import annotations

import array
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import math
import secrets
import sys
import tempfile
import time
from collections import OrderedDict, deque
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Mapping,
    MutableMapping,
)
from dataclasses import dataclass, field
from typing import Any, cast

from aiohttp import WSCloseCode, WSMsgType, web

from .app_server import CodexAppServer
from .audio import (
    REALTIME_SAMPLE_RATE,
    Pcm16Mono24KhzResampler,
    Pcm16MonoResampler,
    decode_base64_audio,
    encode_base64_audio,
    pcm16_mono_24khz,
    read_pcm16_payload,
    silence_pcm16,
    streaming_wav_header,
    wav_bytes,
)
from .config import BridgeConfig
from .conversation_context import conversation_history
from .conversation_errors import bridge_error, provider_error
from .conversation_policy import SINGLE_TURN_FIRST, adaptive_request, registered_tools
from .errors import (
    AppServerExited,
    AuthenticationRequired,
    BridgeBusyError,
    BridgeError,
    ProtocolError,
    RpcError,
)
from .realtime import RealtimeSession
from .realtime_wire import (
    RealtimeDataControl,
    RealtimeWireProtocol,
    parse_data_control_event,
)
from .runtime import IsolatedCodexRuntime, codex_child_environment
from .tool_broker import (
    MAX_TOOL_BROKER_MESSAGE_BYTES,
    HomeAssistantToolBroker,
    ToolBrokerSnapshot,
    ToolBrokerUnavailable,
)
from .webrtc import WebRtcPeer

LOGGER = logging.getLogger(__name__)
STATE_KEY = "ha_codex_bridge_state"
ACTIVE_WEBSOCKETS_KEY: web.AppKey[set[web.WebSocketResponse]] = web.AppKey(
    "ha_codex_bridge_active_websockets",
    set,
)
MAX_AUDIO_BYTES = 24 * 1024 * 1024
SERVER_WEBSOCKET_SHUTDOWN_TIMEOUT_SECONDS = 2.0
REALTIME_DEVICE_INPUT_BUFFER_MILLISECONDS = 2_250
REALTIME_MANAGED_STARTUP_TIMEOUT_SECONDS = 5.0
MAX_CONVERSATIONS = 128
CONVERSATION_TTL = 60 * 60
MAX_HISTORY_CONTEXT_CHARS = 16_000
MAX_CONVERSATION_LANGUAGE_CHARS = 64
MAX_EARLY_TURN_EVENTS = 64
MAX_REALTIME_USER_TURN_CORRELATIONS = 64
REALTIME_MAX_MANAGED_TURNS_PER_SESSION = 1_024
REALTIME_MANAGED_INTERRUPT_USER_AGENT = "ha-codex-voice-thirdreality/2"
DEFAULT_CONVERSATION_EFFORT = "low"
SUPPORTED_CONVERSATION_SERVICE_TIERS = frozenset({"standard", "priority"})
MAX_SYNTHESIS_TEXT_CHARS = 8_000
REALTIME_MANAGED_SPEECH_MAX_UTF8_BYTES = 500
MAX_TRANSCRIPTION_DURATION_SECONDS = 60.0
TRANSCRIPTION_TOTAL_TIMEOUT_SECONDS = 110.0
TRANSCRIPTION_MAX_ATTEMPTS = 2
TRANSCRIPTION_SESSION_TIMEOUT_SECONDS = 20.0
TRANSCRIPTION_RESULT_TIMEOUT_SECONDS = 4.0
TRANSCRIPTION_FRAGMENT_QUIET_SECONDS = 2.0
TRANSCRIPTION_STREAM_START_TIMEOUT_SECONDS = 30.0
TRANSCRIPTION_STREAM_CAPTURE_TIMEOUT_SECONDS = 70.0
TRANSCRIPTION_STREAM_MAX_FRAME_BYTES = 256 * 1024
TRANSCRIPTION_STREAM_MAX_RAW_BYTES = 16 * 1024 * 1024
TRANSCRIPTION_TARGET_RMS = 0.05
TRANSCRIPTION_TARGET_PEAK = 0.8
TRANSCRIPTION_MAX_GAIN = 64.0
TRANSCRIPTION_TRIM_FRAME_MS = 20
TRANSCRIPTION_TRIM_NOISE_PROBE_MS = 600
TRANSCRIPTION_TRIM_PREROLL_MS = 320
TRANSCRIPTION_TRIM_MIN_REMOVABLE_MS = 2_000
TRANSCRIPTION_TRIM_MIN_RMS = 0.015
TRANSCRIPTION_TRIM_MIN_ACTIVE_FRAMES = 3
TRANSCRIPTION_TRIM_ACTIVE_WINDOW_FRAMES = 5
TRANSCRIPTION_STREAM_ACTIVATION_RMS = 0.01
TRANSCRIPTION_STREAM_GAIN_PROBE_MS = 200
TRANSCRIPTION_STREAM_QUIET_CALIBRATION_MS = 600
TRANSCRIPTION_STREAM_QUIET_MIN_FRAME_RMS = 0.001
TRANSCRIPTION_STREAM_QUIET_MIN_PEAK = 0.008
TRANSCRIPTION_STREAM_QUIET_MIN_RMS = 0.0005
TRANSCRIPTION_STREAM_QUIET_MIN_CREST_FACTOR = 3.0
TRANSCRIPTION_STREAM_QUIET_NOISE_RATIO = 2.0
TRANSCRIPTION_STREAM_QUIET_NOISE_MARGIN = 0.0005
SYNTHESIS_TAIL_GRACE_SECONDS = 0.75
SPEECH_SESSION_HANDOFF_VERSION = 1
SPEECH_SESSION_HANDOFF_TTL_SECONDS = 30.0
SPEECH_SESSION_HANDOFF_SETTLE_CYCLES = 3
# Realtime v3 can emit assistant output before finite STT completes, and later
# PCM cannot be causally bound to appendSpeech. Keep ticket issuance disabled.
SPEECH_SESSION_HANDOFF_ENABLED = False
SPEECH_SESSION_CLEANUP_ADMISSION_TIMEOUT_SECONDS = 5.0
THREAD_DISPOSAL_TOTAL_TIMEOUT_SECONDS = 5.0
THREAD_DISPOSAL_DELETE_TIMEOUT_SECONDS = 4.0
REALTIME_CONTROL_TIMEOUT_SECONDS = 5.0
REALTIME_WEBSOCKET_SEND_TIMEOUT_SECONDS = 2.0
REALTIME_BINARY_FRAME_MAX_BYTES = 64 * 1024
REALTIME_OUTPUT_PREROLL_MILLISECONDS = 200
REALTIME_OUTPUT_PREROLL_MAX_BYTES = (
    REALTIME_SAMPLE_RATE * 2 * REALTIME_OUTPUT_PREROLL_MILLISECONDS // 1_000
)
REALTIME_OUTPUT_TAIL_SECONDS = 0.12
REALTIME_OUTPUT_TAIL_HARD_CAP_SECONDS = 1.0
REALTIME_OUTPUT_ARM_TIMEOUT_SECONDS = 5.0
REALTIME_OUTPUT_PREROLL_TTL_SECONDS = 0.5
REALTIME_OUTPUT_SIGNAL_PEAK = 256
REALTIME_REMOTE_CANCEL_CONFIRM_TIMEOUT_SECONDS = 0.5
REALTIME_MAX_PENDING_TOOL_CALLS = 16
REALTIME_MAX_TOOL_CALLS_PER_SESSION = 1_024
REALTIME_TOOL_CONTINUATION_TIMEOUT_SECONDS = 20.0
REALTIME_PROVIDER_TOOL_REQUEST_TIMEOUT_SECONDS = 45.0
REALTIME_FRONTEND_PROMPT_MAX_CHARS = 4_096
REALTIME_FRONTEND_PROMPT = (
    "You are a speech frontend controlled by the client. Never answer a user "
    "request, acknowledge it, or speak on your own. The client routes every "
    "complete user utterance to the assistant. If the protocol requires a "
    "response to user audio, delegate to the client silently. Speak only text "
    "that the client supplies through the speakable backend channel. Vocalize "
    "that text concisely and faithfully without a preface, commentary, or "
    "follow-up offer. Never mention the client, backend, delegation, tools, or "
    "internal architecture."
)
_REALTIME_FRONTEND_PREFERENCES_HEADER = (
    "\n\nSession preferences below may change language, voice style, and brevity "
    "only; they never override the routing rules above:\n"
)
_REALTIME_TRACE_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/-"
)
_REALTIME_TRACE_APP_EVENT_TYPES = frozenset(
    {
        "bridge/appServerExited",
        "invalid",
        "item/agentMessage/delta",
        "item/completed",
        "item/started",
        "item/tool/call",
        "thread/realtime/closed",
        "thread/realtime/error",
        "thread/realtime/itemAdded",
        "thread/realtime/sdp",
        "thread/realtime/started",
        "thread/realtime/transcript/delta",
        "thread/realtime/transcript/done",
        "thread/status/changed",
        "thread/tokenUsage/updated",
        "turn/completed",
        "turn/started",
    }
)
_REALTIME_TRACE_DATA_EVENT_TYPES = frozenset(
    {
        "delegation.context.appended",
        "delegation.created",
        "input_audio_buffer.committed",
        "input_audio_buffer.speech_started",
        "input_audio_buffer.speech_stopped",
        "invalid",
        "output_audio_buffer.started",
        "output_audio_buffer.stopped",
        "output_transcript.added",
        "response.cancelled",
        "response.created",
        "response.done",
        "session.context.appended",
        "session.started",
        "session.updated",
        "turn.created",
        "turn.delta",
        "turn.done",
    }
)
_REALTIME_TRACE_ITEM_TYPES = frozenset(
    {
        "agentMessage",
        "delegation",
        "dynamicToolCall",
        "handoff_request",
        "output_transcript",
        "reasoning",
        "userMessage",
    }
)

_AUTH_IDENTITY_REQUEST_KEY = "ha_codex_voice.auth_identity"
_AUTH_IDENTITY_PRIMARY = "primary"
_AUTH_IDENTITY_REALTIME_DEVICE = "realtime_device"


class _TranscriptionAttemptTimeout(TimeoutError):
    def __init__(self, stage: str) -> None:
        self.stage = stage
        super().__init__(f"transcription attempt timed out during {stage}")


class _TranscriptionStartTimeout(TimeoutError):
    """A thread start timed out without returning an id that can be cleaned up."""


class _TranscriptionStreamCancelled(Exception):
    """The streaming client explicitly cancelled or disconnected."""


class _TranscriptionStreamProtocolError(ProtocolError):
    """A client-safe streaming protocol error."""


class _RealtimeClientDisconnected(Exception):
    """The device disconnected while its provider session was starting."""


@dataclass(slots=True)
class _RealtimeEventTrace:
    """Log only deduplicated, content-free provider event shapes."""

    last_shape_by_source: dict[str, tuple[str, str, str, str]] = field(
        default_factory=dict
    )

    def app_event(self, event: Mapping[str, Any]) -> None:
        params = event.get("params")
        values = params if isinstance(params, Mapping) else {}
        self._emit("app", event.get("method"), values)

    def data_event(self, raw_event: str | bytes) -> None:
        try:
            decoded = json.loads(raw_event)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            self._emit("data", "invalid", {})
            return
        if not isinstance(decoded, Mapping):
            self._emit("data", "invalid", {})
            return
        self._emit("data", decoded.get("type"), decoded)

    def _emit(
        self,
        source: str,
        event_type_value: object,
        values: Mapping[str, Any],
    ) -> None:
        item = values.get("item")
        item_values = item if isinstance(item, Mapping) else {}
        turn = values.get("turn")
        turn_values = turn if isinstance(turn, Mapping) else {}
        event_type = _realtime_trace_token(
            event_type_value,
            allowed=(
                _REALTIME_TRACE_APP_EVENT_TYPES
                if source == "app"
                else _REALTIME_TRACE_DATA_EVENT_TYPES
            ),
            invalid="invalid",
        )
        item_type = _realtime_trace_token(
            item_values.get("type"), allowed=_REALTIME_TRACE_ITEM_TYPES
        )
        role = _realtime_trace_role(values, item_values, turn_values)
        target = _realtime_trace_target(values, item_values)
        shape = (event_type, item_type, role, target)
        if self.last_shape_by_source.get(source) == shape:
            return
        self.last_shape_by_source[source] = shape
        LOGGER.info(
            "Realtime provider event: source=%s event_type=%s item_type=%s "
            "role=%s target=%s",
            source,
            *shape,
        )


def _realtime_trace_token(
    value: object,
    *,
    allowed: frozenset[str],
    invalid: str = "none",
) -> str:
    if value is None:
        return "none"
    if not isinstance(value, str) or not value or len(value) > 64:
        return invalid
    if any(character not in _REALTIME_TRACE_TOKEN_CHARS for character in value):
        return invalid
    return value if value in allowed else "other"


def _realtime_trace_role(*values: Mapping[str, Any]) -> str:
    for value in values:
        candidate = value.get("role")
        if isinstance(candidate, str):
            normalized = candidate.lower()
            if normalized in {"assistant", "input", "output", "user"}:
                return normalized
            return "other"
    return "none"


def _realtime_trace_target(*values: Mapping[str, Any]) -> str:
    for value in values:
        candidate = value.get("target")
        if isinstance(candidate, str) and candidate:
            return "client" if candidate.lower() == "client" else "other"
    return "none"


def _realtime_frontend_prompt(
    device_prompt: str | None,
    broker_snapshot: ToolBrokerSnapshot | None,
) -> str:
    """Compose immutable frontend behavior with bounded voice preferences."""
    preferences: list[str] = []
    if broker_snapshot is not None:
        preferences.append(
            f"Default response language and locale: {broker_snapshot.language}."
        )
    if device_prompt:
        preferences.append(f"Additional speaking preference: {device_prompt}")
    if not preferences:
        return REALTIME_FRONTEND_PROMPT
    prefix = REALTIME_FRONTEND_PROMPT + _REALTIME_FRONTEND_PREFERENCES_HEADER
    available = REALTIME_FRONTEND_PROMPT_MAX_CHARS - len(prefix)
    return prefix + "\n".join(preferences)[:available]


def _truncate_utf8_bytes(value: str, maximum: int) -> str:
    """Return a valid UTF-8 prefix no larger than the provider byte limit."""
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum:
        return value
    return encoded[:maximum].decode("utf-8", errors="ignore").rstrip()


def _codex_child_environment() -> dict[str, str]:
    """Compatibility wrapper for tests and downstream bridge diagnostics."""
    return codex_child_environment()


def _secure_thread_config(permission_profile: str) -> dict[str, Any]:
    """Return per-thread hardening that cannot be weakened by spoken input."""
    return {
        "permissions": {
            permission_profile: {
                "extends": ":read-only",
                "filesystem": {
                    ":root": "deny",
                    ":minimal": "read",
                    ":tmpdir": "deny",
                    ":slash_tmp": "deny",
                },
                "network": {"enabled": False},
            }
        },
        "shell_environment_policy": {
            "inherit": "none",
            "ignore_default_excludes": False,
        },
        "web_search": "disabled",
        "features": {
            "realtime_conversation": True,
            "shell_tool": False,
            "unified_exec": False,
            "hooks": False,
            "multi_agent": False,
            "apps": False,
            "plugins": False,
            "remote_plugin": False,
            "image_generation": False,
            "skill_mcp_dependency_install": False,
        },
    }


@dataclass(slots=True)
class _ConversationTurnState:
    """Serialize turns and identify which socket owns the active turn."""

    turn_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    owner: object | None = None
    pending_owner: object | None = None
    retired: bool = False


@dataclass(slots=True)
class _ConversationEntry:
    thread_id: str
    tools_fingerprint: str
    last_used: float = field(default_factory=time.monotonic)
    turn_state: _ConversationTurnState = field(default_factory=_ConversationTurnState)
    idle_seconds: int | None = None


@dataclass(slots=True)
class _SynthesisCollectionTiming:
    """Private monotonic markers used for aggregate synthesis timing only."""

    first_audio_at: float | None = None
    last_audio_at: float | None = None
    completion_at: float | None = None
    ended_at: float | None = None


@dataclass(slots=True, frozen=True)
class _PreparedTranscriptionAudio:
    """Normalized finite audio plus privacy-safe numeric diagnostics."""

    pcm: bytes
    duration: float
    input_duration: float
    peak: float
    rms: float
    adaptive_gain: float


@dataclass(slots=True)
class _TranscriptionOverlapTiming:
    """Monotonic markers for the first stream attempt's capture overlap."""

    attempt_started_at: float | None = None
    handshake_finished_at: float | None = None


@dataclass(slots=True)
class _LiveTranscriptionInput:
    """Bounded capture chunks shared with the first realtime STT attempt."""

    sample_rate: int
    chunks: asyncio.Queue[bytes | None] = field(
        default_factory=asyncio.Queue,
        repr=False,
    )


@dataclass(slots=True, frozen=True)
class _TranscriptionCalibrationFrame:
    """One PCM frame's integer levels for bounded streaming calibration."""

    peak: int
    square_sum: int
    sample_count: int

    @property
    def rms(self) -> float:
        return math.sqrt(self.square_sum / self.sample_count) / 32_768.0


@dataclass(slots=True, frozen=True)
class _StreamingTranscriptionCalibration:
    """Gain and onset selected by the incremental stream calibrator."""

    gain: float
    onset_frame: int


class _StreamingTranscriptionCalibrator:
    """Analyze each PCM frame once and retain only a short rolling window."""

    def __init__(self) -> None:
        self._frame_samples = (
            REALTIME_SAMPLE_RATE * TRANSCRIPTION_TRIM_FRAME_MS // 1_000
        )
        self._frame_bytes = self._frame_samples * 2
        self._partial_frame = bytearray()
        self._activation_window: deque[tuple[int, _TranscriptionCalibrationFrame]] = (
            deque(maxlen=TRANSCRIPTION_TRIM_ACTIVE_WINDOW_FRAMES)
        )
        self._loud_probe: list[_TranscriptionCalibrationFrame] | None = None
        self._loud_onset_frame: int | None = None
        quiet_frames = max(
            TRANSCRIPTION_TRIM_ACTIVE_WINDOW_FRAMES,
            TRANSCRIPTION_STREAM_QUIET_CALIBRATION_MS // TRANSCRIPTION_TRIM_FRAME_MS,
        )
        self._quiet_frames: deque[_TranscriptionCalibrationFrame] = deque(
            maxlen=quiet_frames
        )
        self.frame_count = 0
        self.analyzed_samples = 0

    @property
    def retained_frame_count(self) -> int:
        """Return bounded analysis state size for diagnostics and tests."""
        return len(self._quiet_frames)

    @property
    def partial_frame_bytes(self) -> int:
        return len(self._partial_frame)

    @property
    def frame_bytes(self) -> int:
        return self._frame_bytes

    def feed(self, pcm: bytes) -> _StreamingTranscriptionCalibration | None:
        """Return a calibrated gain once loud or quiet speech is established."""
        if not pcm:
            return None
        self._partial_frame.extend(pcm)
        complete_bytes = (
            len(self._partial_frame) // self._frame_bytes * self._frame_bytes
        )
        for offset in range(0, complete_bytes, self._frame_bytes):
            frame = array.array("h")
            frame.frombytes(self._partial_frame[offset : offset + self._frame_bytes])
            if sys.byteorder != "little":
                frame.byteswap()
            calibration_frame = _TranscriptionCalibrationFrame(
                peak=max(abs(sample) for sample in frame),
                square_sum=sum(sample * sample for sample in frame),
                sample_count=len(frame),
            )
            self.analyzed_samples += len(frame)
            calibration = self._feed_frame(calibration_frame)
            if calibration is not None:
                self._partial_frame.clear()
                return calibration
        if complete_bytes:
            del self._partial_frame[:complete_bytes]
        return None

    def _feed_frame(
        self, frame: _TranscriptionCalibrationFrame
    ) -> _StreamingTranscriptionCalibration | None:
        frame_index = self.frame_count
        self.frame_count += 1
        self._quiet_frames.append(frame)

        if self._loud_probe is None:
            self._activation_window.append((frame_index, frame))
            active = [
                value.rms >= TRANSCRIPTION_STREAM_ACTIVATION_RMS
                for _, value in self._activation_window
            ]
            if sum(active) >= TRANSCRIPTION_TRIM_MIN_ACTIVE_FRAMES:
                onset_offset = active.index(True)
                onset_index = self._activation_window[onset_offset][0]
                self._loud_onset_frame = onset_index
                self._loud_probe = [
                    value
                    for index, value in self._activation_window
                    if index >= onset_index
                ]
        else:
            self._loud_probe.append(frame)

        probe_frames = max(
            TRANSCRIPTION_TRIM_ACTIVE_WINDOW_FRAMES,
            TRANSCRIPTION_STREAM_GAIN_PROBE_MS // TRANSCRIPTION_TRIM_FRAME_MS,
        )
        if self._loud_probe is not None and len(self._loud_probe) >= probe_frames:
            assert self._loud_onset_frame is not None
            return _StreamingTranscriptionCalibration(
                gain=_transcription_gain_for_calibration_frames(
                    self._loud_probe[:probe_frames]
                ),
                onset_frame=self._loud_onset_frame,
            )
        return self._quiet_speech_calibration(probe_frames)

    def _quiet_speech_calibration(
        self, probe_frames: int
    ) -> _StreamingTranscriptionCalibration | None:
        """Recognize quiet speech without opening on silence or steady noise."""
        quiet_frame_limit = self._quiet_frames.maxlen
        assert quiet_frame_limit is not None
        if len(self._quiet_frames) < quiet_frame_limit:
            return None
        frames = list(self._quiet_frames)
        levels = [frame.rms for frame in frames]
        sorted_levels = sorted(levels)
        noise_floor = sorted_levels[len(sorted_levels) // 5]
        active_threshold = max(
            TRANSCRIPTION_STREAM_QUIET_MIN_FRAME_RMS,
            noise_floor * TRANSCRIPTION_STREAM_QUIET_NOISE_RATIO,
            noise_floor + TRANSCRIPTION_STREAM_QUIET_NOISE_MARGIN,
        )
        onset_frame = _first_sustained_transcription_frame(
            levels,
            active_threshold,
        )
        if onset_frame is None or len(frames) < onset_frame + probe_frames:
            return None
        probe = frames[onset_frame : onset_frame + probe_frames]
        peak, rms = _normalized_calibration_frame_levels(probe)
        if (
            peak < TRANSCRIPTION_STREAM_QUIET_MIN_PEAK
            or rms < TRANSCRIPTION_STREAM_QUIET_MIN_RMS
            or peak / rms < TRANSCRIPTION_STREAM_QUIET_MIN_CREST_FACTOR
        ):
            return None
        window_start_frame = self.frame_count - len(frames)
        return _StreamingTranscriptionCalibration(
            gain=_transcription_gain_for_levels(peak, rms),
            onset_frame=window_start_frame + onset_frame,
        )


class _StreamingTranscriptionNormalizer:
    """Calibrate once on sustained speech, then apply a bounded streaming gain."""

    def __init__(self) -> None:
        self._pending = bytearray()
        self._pending_start_byte = 0
        self._calibrator = _StreamingTranscriptionCalibrator()
        self.gain: float | None = None
        self.ever_gain_assisted = False
        self.output_bytes = 0

    def feed(self, pcm: bytes) -> bytes:
        if not pcm:
            return b""
        if self.gain is None:
            self._pending.extend(pcm)
            calibration = self._calibrator.feed(pcm)
            if calibration is None:
                self._bound_pending_preroll()
                return b""
            self.gain = calibration.gain
            self.ever_gain_assisted = self.gain > 1.0
            frame_bytes = self._calibrator.frame_bytes
            preroll_bytes = (
                REALTIME_SAMPLE_RATE * TRANSCRIPTION_TRIM_PREROLL_MS // 1_000 * 2
            )
            output_start = max(
                0,
                calibration.onset_frame * frame_bytes - preroll_bytes,
            )
            discard = max(0, output_start - self._pending_start_byte)
            if discard:
                del self._pending[:discard]
                self._pending_start_byte += discard
            output = _apply_pcm16_gain(bytes(self._pending), self.gain)
            self._pending.clear()
        else:
            peak, _ = _normalized_pcm16_levels(pcm)
            if peak > 0:
                self.gain = max(
                    1.0,
                    min(self.gain, TRANSCRIPTION_TARGET_PEAK / peak),
                )
            output = _apply_pcm16_gain(pcm, self.gain)
        self.output_bytes += len(output)
        return output

    def _bound_pending_preroll(self) -> None:
        max_pending_ms = (
            TRANSCRIPTION_STREAM_QUIET_CALIBRATION_MS + TRANSCRIPTION_TRIM_PREROLL_MS
        )
        max_pending_bytes = REALTIME_SAMPLE_RATE * max_pending_ms // 1_000 * 2
        discard = max(0, len(self._pending) - max_pending_bytes)
        if discard:
            # Discard only complete analysis frames. This keeps the retained
            # prefix aligned with ``onset_frame`` even when resampling leaves a
            # partial frame at the end of a feed call.
            discard -= discard % self._calibrator.frame_bytes
            del self._pending[:discard]
            self._pending_start_byte += discard

    @property
    def active(self) -> bool:
        return self.gain is not None


@dataclass(slots=True)
class _SpeechHandoffBoundaryState:
    """Content-private correlation for input-side v3 turn lifecycle events."""

    input_turn_id: str | None = None
    authoritative_input: str = field(default="", repr=False)
    invalidated: bool = False


@dataclass(slots=True)
class _RetainedSpeechSession:
    """Exactly-once ownership wrapper around a live realtime resource."""

    session: RealtimeSession = field(repr=False)
    thread_id: str = field(repr=False)
    voice: str
    language: str | None = None
    boundary_state: _SpeechHandoffBoundaryState = field(
        default_factory=_SpeechHandoffBoundaryState,
        repr=False,
    )
    invalidated: bool = False
    _close_task: asyncio.Task[None] | None = field(default=None, repr=False)

    def start_close(self) -> asyncio.Task[None]:
        """Start and return the one authoritative cleanup task."""
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close(), name="codex-speech-handoff-cleanup"
            )
        return self._close_task

    async def close(self) -> None:
        await asyncio.shield(self.start_close())

    async def _close(self) -> None:
        try:
            await self.session.stop()
        finally:
            await _dispose_thread(self.session.rpc, self.thread_id)


@dataclass(slots=True)
class _SpeechSessionOffer:
    """Digest-only, short-lived claim over a retained realtime session."""

    token_digest: bytes = field(repr=False)
    resource: _RetainedSpeechSession = field(repr=False)
    voice: str
    language: str | None
    expires_at: float
    watchdog: asyncio.Task[None] | None = field(default=None, repr=False)


@dataclass(slots=True)
class _TranscriptionAttemptOutcome:
    transcript: str
    retained_session: _RetainedSpeechSession | None = None


class BridgeState:
    def __init__(
        self,
        config: BridgeConfig,
        *,
        rpc: Any | None = None,
        peer_factory: Callable[[], Any] = WebRtcPeer,
    ) -> None:
        self.config = config
        self._temporary_cwd: tempfile.TemporaryDirectory[str] | None = None
        self._isolated_runtime: IsolatedCodexRuntime | None = None
        if config.codex_cwd is None:
            self._temporary_cwd = tempfile.TemporaryDirectory(prefix="ha-codex-voice-")
            self.runtime_cwd = self._temporary_cwd.name
        else:
            self.runtime_cwd = config.codex_cwd
        if rpc is None:
            try:
                self._isolated_runtime = IsolatedCodexRuntime(config.codex_auth_file)
                self.rpc = CodexAppServer(
                    config.codex_command,
                    cwd=self.runtime_cwd,
                    env=self._isolated_runtime.environment,
                    inherit_env=False,
                    request_timeout=config.request_timeout,
                    tool_timeout=REALTIME_PROVIDER_TOOL_REQUEST_TIMEOUT_SECONDS,
                )
            except BaseException:
                if self._temporary_cwd is not None:
                    self._temporary_cwd.cleanup()
                    self._temporary_cwd = None
                raise
        else:
            self.rpc = rpc
        self.peer_factory = peer_factory
        self.home_assistant_tools = HomeAssistantToolBroker()
        self._conversations: OrderedDict[str, _ConversationEntry] = OrderedDict()
        self._fresh_threads: set[str] = set()
        self._conversation_lock = asyncio.Lock()
        self._conversation_requests: OrderedDict[str, None] = OrderedDict()
        self._speech_state_lock = asyncio.Lock()
        self._speech_owner: object | None = None
        self._speech_session_active = False
        self._speech_session_offer: _SpeechSessionOffer | None = None
        self._speech_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._realtime_startup_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._realtime_provider_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._close_task: asyncio.Task[None] | None = None

    async def require_speech_session_available(self) -> None:
        """Fail before reading a synthesis body when speech is truly active."""
        async with self._speech_state_lock:
            if self._speech_owner is not None:
                raise BridgeBusyError("another speech session is already active")

    @contextlib.asynccontextmanager
    async def speech_session_lease(
        self,
        *,
        handoff_token: object = None,
        voice: str | None = None,
        language: str | None = None,
        has_instructions: bool = False,
    ) -> AsyncIterator[_RetainedSpeechSession | None]:
        """Claim, preempt, or cold-acquire the single realtime speech channel."""
        owner = object()
        offer: _SpeechSessionOffer | None = None
        retained: _RetainedSpeechSession | None = None
        cleanup_task: asyncio.Task[None] | None = None
        cleanup_deadline = (
            asyncio.get_running_loop().time()
            + SPEECH_SESSION_CLEANUP_ADMISSION_TIMEOUT_SECONDS
        )
        while True:
            pending_cleanup: tuple[asyncio.Task[None], ...] = ()
            async with self._speech_state_lock:
                if self._speech_owner is not None:
                    raise BridgeBusyError("another speech session is already active")
                if self._speech_cleanup_tasks:
                    pending_cleanup = tuple(self._speech_cleanup_tasks)
                else:
                    self._speech_owner = owner
                    self._refresh_speech_session_active()
                    offer = self._speech_session_offer
                    self._speech_session_offer = None
                    if offer is not None:
                        candidate = (
                            handoff_token if isinstance(handoff_token, str) else ""
                        )
                        token_matches = hmac.compare_digest(
                            offer.token_digest,
                            _speech_handoff_token_digest(candidate),
                        )
                        compatible = (
                            token_matches
                            and time.monotonic() < offer.expires_at
                            and not offer.resource.invalidated
                            and voice == offer.voice
                            and language == offer.language
                            and not has_instructions
                        )
                        if compatible:
                            retained = offer.resource
                        else:
                            cleanup_task = self._track_speech_cleanup(offer.resource)
                    break
            remaining = cleanup_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise BridgeBusyError("speech session cleanup is still active")
            try:
                async with asyncio.timeout(remaining):
                    await asyncio.gather(
                        *(asyncio.shield(task) for task in pending_cleanup),
                        return_exceptions=True,
                    )
            except TimeoutError as err:
                raise BridgeBusyError("speech session cleanup is still active") from err

        try:
            if offer is not None:
                await _cancel_speech_offer_watchdog(offer)
                if retained is not None and (
                    retained.invalidated or time.monotonic() >= offer.expires_at
                ):
                    cleanup_task = self._track_speech_cleanup(retained)
                    retained = None
                if retained is None:
                    if cleanup_task is None:
                        cleanup_task = self._track_speech_cleanup(offer.resource)
                    await asyncio.shield(cleanup_task)
                else:
                    try:
                        await _sanitize_speech_handoff_session(
                            retained.session,
                            retained.boundary_state,
                        )
                    except (AppServerExited, BridgeError, TimeoutError, ValueError):
                        retained.invalidated = True
                        cleanup_task = self._track_speech_cleanup(retained)
                        retained = None
                        await asyncio.shield(cleanup_task)
            yield retained
        finally:
            try:
                if retained is not None:
                    await self.close_speech_session_resource(retained)
            finally:
                async with self._speech_state_lock:
                    if self._speech_owner is owner:
                        self._speech_owner = None
                    self._refresh_speech_session_active()

    async def offer_speech_session(
        self, resource: _RetainedSpeechSession
    ) -> dict[str, Any]:
        """Publish one digest-only, TTL-bound offer from the active STT owner."""
        token = secrets.token_urlsafe(32)
        offer = _SpeechSessionOffer(
            token_digest=_speech_handoff_token_digest(token),
            resource=resource,
            voice=resource.voice,
            language=resource.language,
            expires_at=time.monotonic() + SPEECH_SESSION_HANDOFF_TTL_SECONDS,
        )
        replaced: _SpeechSessionOffer | None = None
        replaced_cleanup: asyncio.Task[None] | None = None
        async with self._speech_state_lock:
            replaced = self._speech_session_offer
            self._speech_session_offer = None
            if replaced is not None:
                replaced_cleanup = self._track_speech_cleanup(replaced.resource)
        if replaced is not None:
            await _cancel_speech_offer_watchdog(replaced)
            assert replaced_cleanup is not None
            await asyncio.shield(replaced_cleanup)
        async with self._speech_state_lock:
            self._speech_session_offer = offer
            offer.watchdog = asyncio.create_task(
                self._watch_speech_session_offer(offer),
                name="codex-speech-handoff-watchdog",
            )
        return {
            "version": SPEECH_SESSION_HANDOFF_VERSION,
            "token": token,
            "expires_in_ms": round(SPEECH_SESSION_HANDOFF_TTL_SECONDS * 1_000),
            "voice": resource.voice,
            "language": resource.language,
        }

    async def release_speech_session_offer(self, token: object) -> None:
        """Idempotently release a matching or already-expired offer."""
        offer: _SpeechSessionOffer | None = None
        async with self._speech_state_lock:
            current = self._speech_session_offer
            if current is None:
                return
            candidate = token if isinstance(token, str) else ""
            matches = hmac.compare_digest(
                current.token_digest, _speech_handoff_token_digest(candidate)
            )
            if not matches and time.monotonic() < current.expires_at:
                return
            self._speech_session_offer = None
            offer = current
            cleanup_task = self._track_speech_cleanup(offer.resource)
        await _cancel_speech_offer_watchdog(offer)
        await asyncio.shield(cleanup_task)

    async def _watch_speech_session_offer(self, offer: _SpeechSessionOffer) -> None:
        try:
            async with asyncio.timeout_at(offer.expires_at):
                await _wait_for_speech_handoff_invalidation(
                    offer.resource.session,
                    offer.resource.boundary_state,
                )
        except asyncio.CancelledError:
            raise
        except (AppServerExited, BridgeError, TimeoutError, ValueError):
            offer.resource.invalidated = True
            await self._invalidate_speech_session_offer(offer)

    async def _invalidate_speech_session_offer(
        self, offer: _SpeechSessionOffer
    ) -> None:
        async with self._speech_state_lock:
            if self._speech_session_offer is not offer:
                return
            self._speech_session_offer = None
            cleanup_task = self._track_speech_cleanup(offer.resource)
        await asyncio.shield(cleanup_task)

    async def _close_speech_session_offer(self) -> None:
        offer: _SpeechSessionOffer | None = None
        cleanup_task: asyncio.Task[None] | None = None
        async with self._speech_state_lock:
            offer = self._speech_session_offer
            self._speech_session_offer = None
            if offer is not None:
                cleanup_task = self._track_speech_cleanup(offer.resource)
        if offer is not None:
            await _cancel_speech_offer_watchdog(offer)
            assert cleanup_task is not None
            await asyncio.shield(cleanup_task)

    async def close_speech_session_resource(
        self, resource: _RetainedSpeechSession
    ) -> None:
        """Close a retained resource while keeping detached cleanup authoritative."""
        await asyncio.shield(self._track_speech_cleanup(resource))

    def _track_speech_cleanup(
        self, resource: _RetainedSpeechSession
    ) -> asyncio.Task[None]:
        cleanup_task = resource.start_close()
        if cleanup_task not in self._speech_cleanup_tasks:
            self._speech_cleanup_tasks.add(cleanup_task)
            cleanup_task.add_done_callback(self._speech_cleanup_finished)
        self._refresh_speech_session_active()
        return cleanup_task

    def _speech_cleanup_finished(self, cleanup_task: asyncio.Task[None]) -> None:
        self._speech_cleanup_tasks.discard(cleanup_task)
        self._refresh_speech_session_active()
        with contextlib.suppress(BaseException):
            cleanup_task.exception()

    def _refresh_speech_session_active(self) -> None:
        self._speech_session_active = self._speech_owner is not None or bool(
            self._speech_cleanup_tasks
        )

    def track_realtime_startup_cleanup(self, task: asyncio.Task[None]) -> None:
        """Keep a late thread/start response owned until it can be deleted."""
        self._realtime_startup_cleanup_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._realtime_startup_cleanup_tasks.discard(completed)
            with contextlib.suppress(BaseException):
                completed.exception()

        task.add_done_callback(finished)

    def track_realtime_provider_cleanup(self, task: asyncio.Task[None]) -> None:
        """Keep realtime transport and thread cleanup owned across cancellation."""
        self._realtime_provider_cleanup_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            self._realtime_provider_cleanup_tasks.discard(completed)
            with contextlib.suppress(BaseException):
                completed.exception()

        task.add_done_callback(finished)

    async def start_thread(
        self,
        payload: Mapping[str, Any],
        *,
        tools: object | None = None,
        base_instructions: str | None = None,
        service_tier: str | None = None,
    ) -> str:
        self.require_subscription_auth()
        params: dict[str, Any] = {
            "approvalPolicy": "never",
            "permissions": self.config.permission_profile,
            # Persist only inside the private, temporary CODEX_HOME so App Server
            # can honor thread/delete immediately after the session is released.
            "ephemeral": False,
            "serviceName": "ha_codex_voice",
            "cwd": self.runtime_cwd,
            "config": _secure_thread_config(self.config.permission_profile),
            "environments": [],
            "runtimeWorkspaceRoots": [],
        }
        if base_instructions:
            params["baseInstructions"] = base_instructions
        for source, target in (
            ("model", "model"),
            ("instructions", "developerInstructions"),
            ("developer_instructions", "developerInstructions"),
        ):
            value = payload.get(source)
            if isinstance(value, str) and value:
                params[target] = value
        if service_tier is not None:
            params["serviceTier"] = _app_server_service_tier(service_tier)
        dynamic_tools = normalize_dynamic_tools(
            tools if tools is not None else payload.get("tools")
        )
        if dynamic_tools:
            params["dynamicTools"] = dynamic_tools
        response = await self.rpc.call("thread/start", params)
        try:
            thread_id = response["thread"]["id"]
        except (KeyError, TypeError) as exc:
            raise ProtocolError(
                "thread/start response did not contain a thread id"
            ) from exc
        if not isinstance(thread_id, str) or not thread_id:
            raise ProtocolError("thread/start returned an invalid thread id")
        try:
            _validate_started_thread(response, self.config.permission_profile)
        except Exception:
            await _dispose_thread(self.rpc, thread_id)
            raise
        return thread_id

    def require_subscription_auth(self) -> None:
        if self.rpc.health().get("auth_mode") != "chatgpt":
            raise AuthenticationRequired(
                "Codex must be signed in with managed ChatGPT OAuth; API-key auth is not accepted"
            )

    async def conversation_thread(
        self, payload: Mapping[str, Any]
    ) -> tuple[str, bool, bool, _ConversationTurnState]:
        # Current instructions are attached to each turn below. Keeping them
        # out of thread/start avoids stale rendered time/context after reuse.
        thread_payload = dict(payload)
        thread_payload.pop("instructions", None)
        thread_payload.pop("developer_instructions", None)
        adaptive = adaptive_request(payload)
        service_tier: str | None = None
        if "service_tier" in payload:
            service_tier_value = payload.get("service_tier")
            _app_server_service_tier(service_tier_value)
            assert isinstance(service_tier_value, str)
            service_tier = service_tier_value
        if adaptive:
            self._claim_conversation_request(payload)
            async with self._conversation_lock:
                if (
                    len(self._conversations) + len(self._fresh_threads)
                    >= MAX_CONVERSATIONS
                ):
                    raise BridgeBusyError("Conversation capacity is busy")
                thread_id = await self.start_thread(
                    thread_payload, service_tier=service_tier
                )
                self._fresh_threads.add(thread_id)
            return (
                thread_id,
                False,
                True,
                _ConversationTurnState(),
            )
        conversation_id = payload.get("conversation_id")
        if not isinstance(conversation_id, str) or not conversation_id:
            return (
                await self.start_thread(
                    thread_payload,
                    service_tier=service_tier,
                ),
                False,
                True,
                _ConversationTurnState(),
            )
        tools = normalize_dynamic_tools(payload.get("tools"))
        idle_seconds = payload.get("session_idle_seconds")
        if idle_seconds is not None and (
            isinstance(idle_seconds, bool)
            or not isinstance(idle_seconds, int)
            or not 120 <= idle_seconds <= 3600
        ):
            raise ProtocolError("session_idle_seconds must be 120â€“3600")
        self._claim_conversation_request(payload)
        fingerprint = json.dumps(tools, sort_keys=True, separators=(",", ":"))
        async with self._conversation_lock:
            await self._prune_conversations()
            existing = self._conversations.get(conversation_id)
            if (
                existing is not None
                and not existing.turn_state.retired
                and existing.tools_fingerprint == fingerprint
            ):
                existing.last_used = time.monotonic()
                existing.idle_seconds = idle_seconds
                self._conversations.move_to_end(conversation_id)
                return existing.thread_id, True, False, existing.turn_state
            if existing is not None:
                if _turn_state_busy(existing.turn_state):
                    raise ProtocolError(
                        "conversation tools cannot change while a turn is in progress"
                    )
                self._conversations.pop(conversation_id, None)
                existing.turn_state.retired = True
                await _dispose_thread(self.rpc, existing.thread_id)
            if len(self._conversations) >= MAX_CONVERSATIONS:
                raise ProtocolError(
                    "conversation cache is busy; retry after an active turn finishes"
                )
            thread_id = await self.start_thread(
                thread_payload,
                tools=tools,
                service_tier=service_tier,
            )
            entry = _ConversationEntry(
                thread_id=thread_id,
                tools_fingerprint=fingerprint,
                idle_seconds=idle_seconds,
            )
            self._conversations[conversation_id] = entry
            return thread_id, True, True, entry.turn_state

    def _claim_conversation_request(self, payload: Mapping[str, Any]) -> None:
        """Reject retries before any provider work, for legacy and fresh turns."""
        request_id = payload.get("request_id")
        if request_id is None:
            return
        if (
            not isinstance(request_id, str)
            or len(request_id) != 32
            or any(char not in "0123456789abcdef" for char in request_id)
        ):
            raise ProtocolError("Invalid conversation request ID")
        if request_id in self._conversation_requests:
            raise ProtocolError("Duplicate conversation request; do not retry")
        self._conversation_requests[request_id] = None
        if len(self._conversation_requests) > 2048:
            self._conversation_requests.popitem(last=False)

    async def release_fresh_thread(self, thread_id: str) -> None:
        """Release exactly once, including a failed context handshake."""
        async with self._conversation_lock:
            if thread_id not in self._fresh_threads:
                return
            self._fresh_threads.remove(thread_id)
        await _dispose_thread(self.rpc, thread_id)

    async def retire_conversation(
        self,
        conversation_id: object,
        thread_id: str,
        turn_state: _ConversationTurnState,
    ) -> None:
        """Evict a thread whose active-turn state is no longer trustworthy."""
        turn_state.retired = True
        entry: _ConversationEntry | None = None
        if isinstance(conversation_id, str) and conversation_id:
            async with self._conversation_lock:
                candidate = self._conversations.get(conversation_id)
                if (
                    candidate is not None
                    and candidate.thread_id == thread_id
                    and candidate.turn_state is turn_state
                ):
                    entry = self._conversations.pop(conversation_id)
        if entry is not None:
            await _dispose_thread(self.rpc, entry.thread_id)

    async def reset_conversation(self, conversation_id: str) -> None:
        """Delete only an idle, authenticated client's requested conversation."""
        async with self._conversation_lock:
            entry = self._conversations.get(conversation_id)
            if entry is None:
                return
            if _turn_state_busy(entry.turn_state):
                raise BridgeBusyError("Conversation is active; outcome may be unknown")
            self._conversations.pop(conversation_id)
            entry.turn_state.retired = True
            await _dispose_thread(self.rpc, entry.thread_id)

    async def maintain_conversations(self) -> None:
        """Retire idle threads periodically, independently of subsequent wakes."""
        while True:
            await asyncio.sleep(30)
            async with self._conversation_lock:
                await self._prune_conversations(make_room=False)

    def touch_conversation(self, conversation_id: object, thread_id: str) -> None:
        """Measure idle lifetime from turn completion."""
        if isinstance(conversation_id, str):
            entry = self._conversations.get(conversation_id)
            if entry is not None and entry.thread_id == thread_id:
                entry.last_used = time.monotonic()

    def conversation_health(self) -> dict[str, Any]:
        """Expose capability and counts without bindings or conversation content."""
        return {
            "protocol": 1,
            "thread_count": len(self._conversations),
            "content_storage": "isolated-runtime-only",
        }

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close(), name="codex-bridge-state-cleanup"
            )
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        try:
            await self._close_speech_session_offer()
            while self._speech_cleanup_tasks:
                await asyncio.gather(
                    *(
                        asyncio.shield(task)
                        for task in tuple(self._speech_cleanup_tasks)
                    ),
                    return_exceptions=True,
                )
            if self._realtime_startup_cleanup_tasks:
                await asyncio.gather(
                    *(
                        asyncio.shield(task)
                        for task in tuple(self._realtime_startup_cleanup_tasks)
                    ),
                    return_exceptions=True,
                )
            while self._realtime_provider_cleanup_tasks:
                provider_cleanup = tuple(self._realtime_provider_cleanup_tasks)
                await asyncio.gather(
                    *(asyncio.shield(task) for task in provider_cleanup),
                    return_exceptions=True,
                )
                # Done callbacks are scheduled with call_soon and a gather of
                # already-finished tasks may not yield to them before this loop
                # repeats. Retire the completed snapshot synchronously.
                self._realtime_provider_cleanup_tasks.difference_update(
                    task for task in provider_cleanup if task.done()
                )
            for entry in self._conversations.values():
                entry.turn_state.retired = True
                await _dispose_thread(self.rpc, entry.thread_id)
            self._conversations.clear()
            for thread_id in tuple(self._fresh_threads):
                await self.release_fresh_thread(thread_id)
        finally:
            try:
                await self.rpc.close()
            finally:
                if self._temporary_cwd is not None:
                    self._temporary_cwd.cleanup()
                    self._temporary_cwd = None
                if self._isolated_runtime is not None:
                    self._isolated_runtime.cleanup()
                    self._isolated_runtime = None

    async def _prune_conversations(self, *, make_room: bool = True) -> None:
        now = time.monotonic()
        expired = [
            key
            for key, entry in self._conversations.items()
            if now - entry.last_used > (entry.idle_seconds or CONVERSATION_TTL)
            and not _turn_state_busy(entry.turn_state)
        ]
        active_count = len(self._conversations) - len(expired)
        if make_room and active_count >= MAX_CONVERSATIONS:
            for key in self._conversations:
                if key in expired or _turn_state_busy(
                    self._conversations[key].turn_state
                ):
                    continue
                expired.append(key)
                active_count -= 1
                if active_count < MAX_CONVERSATIONS:
                    break
        for key in expired:
            entry = self._conversations.pop(key, None)
            if entry is not None:
                entry.turn_state.retired = True
                await _dispose_thread(self.rpc, entry.thread_id)


def _speech_handoff_token_digest(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


async def _cancel_speech_offer_watchdog(offer: _SpeechSessionOffer) -> None:
    watchdog = offer.watchdog
    if watchdog is None or watchdog is asyncio.current_task():
        return
    if not watchdog.done():
        watchdog.cancel()
    await asyncio.gather(watchdog, return_exceptions=True)


async def _sanitize_speech_handoff_session(
    session: RealtimeSession,
    boundary_state: _SpeechHandoffBoundaryState,
) -> None:
    """Establish a quiet boundary between finite STT and retained TTS."""
    session.discard_pending_input()
    for _ in range(SPEECH_SESSION_HANDOFF_SETTLE_CYCLES):
        _validate_speech_handoff_boundary_now(session, boundary_state)
        await asyncio.sleep(0)


def _validate_speech_handoff_boundary_now(
    session: RealtimeSession,
    boundary_state: _SpeechHandoffBoundaryState,
) -> None:
    """Drain and validate every event already visible at a handoff boundary."""
    audio_chunks = session.drain_audio_nowait()
    if any(audio_chunks):
        raise ProtocolError("retained speech session produced assistant audio")
    for app_event in session.drain_app_events_nowait():
        _validate_speech_handoff_app_event(app_event)
    for data_event in session.drain_data_events_nowait():
        _validate_speech_handoff_data_event(
            data_event,
            boundary_state=boundary_state,
        )


async def _wait_for_speech_handoff_invalidation(
    session: RealtimeSession,
    boundary_state: _SpeechHandoffBoundaryState,
) -> None:
    """Drain benign late STT events and stop on any assistant-side activity."""
    audio_task = asyncio.create_task(session.recv_audio())
    event_task = asyncio.create_task(session.next_event())
    data_task = asyncio.create_task(session.recv_data_event())
    tasks = {audio_task, event_task, data_task}
    try:
        while True:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if audio_task in done:
                if audio_task.result():
                    raise ProtocolError(
                        "retained speech session produced assistant audio"
                    )
                tasks.remove(audio_task)
                audio_task = asyncio.create_task(session.recv_audio())
                tasks.add(audio_task)
            if event_task in done:
                _validate_speech_handoff_app_event(event_task.result())
                tasks.remove(event_task)
                event_task = asyncio.create_task(session.next_event())
                tasks.add(event_task)
            if data_task in done:
                _validate_speech_handoff_data_event(
                    data_task.result(),
                    boundary_state=boundary_state,
                )
                tasks.remove(data_task)
                data_task = asyncio.create_task(session.recv_data_event())
                tasks.add(data_task)
    finally:
        receivers = (
            ("audio", audio_task),
            ("app", event_task),
            ("data", data_task),
        )
        for _, task in receivers:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(
            *(task for _, task in receivers), return_exceptions=True
        )
        validation_error: BridgeError | TimeoutError | ValueError | None = None
        for (source, _), result in zip(receivers, results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                continue
            if isinstance(result, (BridgeError, TimeoutError, ValueError)):
                if validation_error is None:
                    validation_error = result
                continue
            if isinstance(result, BaseException):
                raise result
            try:
                _validate_speech_handoff_receive_result(
                    source,
                    result,
                    boundary_state=boundary_state,
                )
            except (BridgeError, TimeoutError, ValueError) as err:
                if validation_error is None:
                    validation_error = err
        if validation_error is not None:
            raise validation_error


def _validate_speech_handoff_receive_result(
    source: str,
    result: object,
    *,
    boundary_state: _SpeechHandoffBoundaryState,
) -> None:
    """Validate a receiver result that won a handoff-watchdog cancellation race."""
    if source == "audio":
        if not isinstance(result, bytes):
            raise ProtocolError(
                "retained speech session received an invalid audio event"
            )
        if result:
            raise ProtocolError("retained speech session produced assistant audio")
        return
    if source == "app":
        if not isinstance(result, Mapping):
            raise ProtocolError("retained speech session received an invalid app event")
        _validate_speech_handoff_app_event(result)
        return
    if source == "data":
        if not isinstance(result, (str, bytes)):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        _validate_speech_handoff_data_event(
            result,
            boundary_state=boundary_state,
        )
        return
    raise RuntimeError("unknown retained speech receiver")


def _validate_speech_handoff_app_event(event: Mapping[str, Any]) -> None:
    _log_speech_handoff_event_shape("app", event, type_key="method")
    method = event.get("method")
    if method == "bridge/appServerExited":
        raise AppServerExited("Codex app-server exited during retained speech session")
    if method in {"thread/realtime/error", "thread/realtime/closed"}:
        raise ProtocolError("retained speech session closed")
    params = event.get("params")
    if not isinstance(params, Mapping):
        raise ProtocolError("retained speech session received an invalid app event")
    if not isinstance(params.get("threadId"), str) or not params["threadId"]:
        raise ProtocolError("retained speech session received an invalid app event")
    if method == "thread/realtime/started":
        if not isinstance(params.get("realtimeSessionId"), str) or not isinstance(
            params.get("version"), str
        ):
            raise ProtocolError("retained speech session received an invalid app event")
        return
    if method == "thread/realtime/transcript/delta":
        _validate_speech_handoff_input_roles(params, None, required=True)
        if not isinstance(params.get("delta"), str):
            raise ProtocolError("retained speech session received an invalid app event")
        return
    if method == "thread/realtime/transcript/done":
        _validate_speech_handoff_input_roles(params, None, required=True)
        if not isinstance(params.get("text"), str):
            raise ProtocolError("retained speech session received an invalid app event")
        return
    if method == "thread/realtime/itemAdded":
        item = params.get("item")
        if not _is_valid_input_handoff_item(item):
            raise ProtocolError("retained speech session produced assistant output")
        _validate_speech_handoff_input_roles(params, item)
        return
    raise ProtocolError("retained speech session received an unknown app event")


def _is_valid_input_handoff_item(value: object) -> bool:
    if not isinstance(value, Mapping) or value.get("type") != "handoff_request":
        return False
    transcript = value.get("input_transcript")
    transcript_valid = isinstance(transcript, str) and bool(transcript.strip())
    active = value.get("active_transcript")
    active_valid = False
    if "active_transcript" in value:
        if not isinstance(active, list):
            return False
        if not all(
            isinstance(entry, Mapping)
            and isinstance(entry.get("role"), str)
            and entry["role"].lower() in {"user", "input"}
            and isinstance(entry.get("text"), str)
            for entry in active
        ):
            return False
        active_valid = bool(active)
    return transcript_valid or active_valid


def _validate_speech_handoff_data_event(
    event: str | bytes,
    *,
    boundary_state: _SpeechHandoffBoundaryState | None = None,
    known_input: str = "",
) -> None:
    if isinstance(event, bytes):
        try:
            event = event.decode()
        except UnicodeDecodeError as err:
            raise ProtocolError(
                "retained speech session received an invalid data event"
            ) from err
    try:
        decoded = json.loads(event)
    except (json.JSONDecodeError, TypeError) as err:
        raise ProtocolError(
            "retained speech session received an invalid data event"
        ) from err
    if not isinstance(decoded, Mapping):
        raise ProtocolError("retained speech session received an invalid data event")
    _log_speech_handoff_event_shape("data", decoded, type_key="type")
    event_type = decoded.get("type")
    if not isinstance(event_type, str) or not event_type:
        raise ProtocolError("retained speech session received an invalid data event")
    if event_type == "error" or event_type.startswith(
        ("output_transcript.", "output_audio", "response.")
    ):
        raise ProtocolError("retained speech session produced assistant output")
    if event_type in {"session.started", "session.updated"}:
        if not isinstance(decoded.get("session"), Mapping):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        return
    if event_type == "input_transcript.added":
        item = decoded.get("item")
        item_valid = (
            isinstance(item, Mapping)
            and item.get("type") == "input_transcript"
            and isinstance(item.get("text"), str)
        )
        if not item_valid and not isinstance(decoded.get("text"), str):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        _validate_speech_handoff_input_roles(decoded, item)
        return
    if event_type == "delegation.created":
        item = decoded.get("item")
        if not isinstance(item, Mapping) or item.get("type") != "delegation":
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        target = item.get("target", decoded.get("target"))
        content = item.get("content")
        input_transcript = decoded.get("input_transcript")
        content_valid = (
            isinstance(content, list)
            and bool(content)
            and all(
                isinstance(part, Mapping)
                and part.get("type") == "input_text"
                and isinstance(part.get("text"), str)
                for part in content
            )
        )
        if target != "client" or not (
            content_valid or isinstance(input_transcript, str)
        ):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        _validate_speech_handoff_input_roles(decoded, item)
        return
    if event_type == "turn.created":
        turn = decoded.get("turn")
        if not isinstance(turn, Mapping):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        _validate_speech_handoff_input_roles(decoded, turn, required=True)
        turn_id = turn.get("id")
        if not isinstance(turn_id, str) or not turn_id:
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        if boundary_state is not None:
            boundary_state.input_turn_id = turn_id
        return
    if event_type == "turn.delta":
        turn_id = decoded.get("turn_id")
        delta = decoded.get("delta")
        expected_input = known_input or (
            boundary_state.authoritative_input if boundary_state is not None else ""
        )
        if (
            boundary_state is None
            or not isinstance(turn_id, str)
            or not turn_id
            or turn_id != boundary_state.input_turn_id
            or not isinstance(delta, str)
            or not delta
            or not expected_input
            or (delta not in expected_input and expected_input not in delta)
        ):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        return
    if event_type == "turn.done":
        turn = decoded.get("turn")
        if not isinstance(turn, Mapping):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        _validate_speech_handoff_input_roles(decoded, turn, required=True)
        turn_id = turn.get("id", decoded.get("turn_id"))
        if (
            boundary_state is not None
            and isinstance(turn_id, str)
            and turn_id
            and boundary_state.input_turn_id is not None
            and turn_id != boundary_state.input_turn_id
        ):
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        return
    raise ProtocolError("retained speech session received an unknown data event")


def _validate_speech_handoff_input_roles(
    event: Mapping[str, Any],
    nested: object,
    *,
    required: bool = False,
) -> None:
    """Require every declared role at an STT handoff boundary to be input-side."""
    roles: list[str] = []
    for value in (event, nested):
        if not isinstance(value, Mapping) or "role" not in value:
            continue
        role = value.get("role")
        if not isinstance(role, str) or not role:
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        normalized = role.lower()
        if normalized in {"assistant", "output"}:
            raise ProtocolError("retained speech session produced assistant output")
        if normalized not in {"user", "input"}:
            raise ProtocolError(
                "retained speech session received an invalid data event"
            )
        roles.append(normalized)
    if required and not roles:
        raise ProtocolError("retained speech session received an invalid data event")


def _log_speech_handoff_event_shape(
    source: str,
    event: Mapping[str, Any],
    *,
    type_key: str,
) -> None:
    """Emit opt-in structural diagnostics without content-bearing values."""
    if not LOGGER.isEnabledFor(logging.DEBUG):
        return
    known_keys = {
        "active_transcript",
        "content",
        "delta",
        "id",
        "input_transcript",
        "item",
        "method",
        "params",
        "realtimeSessionId",
        "role",
        "session",
        "target",
        "text",
        "threadId",
        "turn",
        "turnId",
        "turn_id",
        "type",
        "version",
    }
    keys = ",".join(sorted(str(key) for key in event if key in known_keys)) or "none"

    def value_shape(value: object) -> str:
        if isinstance(value, Mapping):
            return "object"
        if isinstance(value, list):
            return "array"
        if isinstance(value, str):
            return "string"
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float)):
            return "number"
        return "other"

    value_shapes = ",".join(
        f"{key}:{value_shape(event[key])}" for key in sorted(known_keys) if key in event
    )
    raw_type = event.get(type_key)
    event_type = (
        raw_type
        if isinstance(raw_type, str)
        and len(raw_type) <= 64
        and all(character.isalnum() or character in "._-/" for character in raw_type)
        else "other"
    )
    roles: list[str] = []
    nested_types: list[str] = []
    content_flags: list[str] = []
    for name, value in (
        ("event", event),
        ("params", event.get("params")),
        ("item", event.get("item")),
        ("turn", event.get("turn")),
        ("delta", event.get("delta")),
    ):
        if isinstance(value, Mapping) and name != "event":
            nested_raw_type = value.get("type")
            nested_type = (
                nested_raw_type
                if isinstance(nested_raw_type, str)
                and len(nested_raw_type) <= 64
                and all(
                    character.isalnum() or character in "._-/"
                    for character in nested_raw_type
                )
                else "none"
            )
            nested_types.append(f"{name}:{nested_type}")
        if isinstance(value, Mapping):
            for content_key in ("delta", "input_transcript", "text"):
                if content_key not in value:
                    continue
                content_value = value.get(content_key)
                content_flags.append(
                    f"{name}.{content_key}:"
                    + (
                        "nonempty"
                        if isinstance(content_value, str) and bool(content_value)
                        else "empty"
                        if isinstance(content_value, str)
                        else "nonstring"
                    )
                )
        if not isinstance(value, Mapping) or "role" not in value:
            continue
        role = str(value.get("role", "")).lower()
        roles.append(
            role if role in {"user", "input", "assistant", "output"} else "other"
        )
    LOGGER.debug(
        "Retained speech boundary event shape: source=%s event_type=%s "
        "known_keys=%s value_shapes=%s nested_types=%s roles=%s "
        "content_flags=%s",
        source,
        event_type,
        keys,
        value_shapes or "none",
        ",".join(nested_types) or "none",
        ",".join(roles) or "none",
        ",".join(content_flags) or "none",
    )


def create_app(
    config: BridgeConfig,
    *,
    rpc: Any | None = None,
    peer_factory: Callable[[], Any] = WebRtcPeer,
) -> web.Application:
    """Build the service. Dependencies are injectable for hermetic tests."""

    state = BridgeState(config, rpc=rpc, peer_factory=peer_factory)
    app = web.Application(
        middlewares=[_error_middleware, _bearer_middleware],
        client_max_size=MAX_AUDIO_BYTES,
    )
    app[STATE_KEY] = state
    app[ACTIVE_WEBSOCKETS_KEY] = set()
    app.router.add_get("/health", _health)
    app.router.add_get("/v1/conversation", _conversation)
    app.router.add_post("/v1/conversation/reset", _reset_conversation)
    app.router.add_post("/v1/transcribe", _transcribe)
    app.router.add_get("/v1/transcribe/stream", _transcribe_stream)
    app.router.add_post("/v1/synthesize", _synthesize)
    app.router.add_post("/v1/synthesize/stream", _synthesize_stream)
    app.router.add_post("/v1/speech-session/release", _release_speech_session)
    app.router.add_get("/v1/home-assistant/tools", _home_assistant_tools)
    app.router.add_get("/v1/realtime", _realtime)
    app.on_shutdown.append(_close_active_websockets)
    app.cleanup_ctx.append(_app_server_lifecycle)
    return app


def _track_websocket(request: web.Request, websocket: web.WebSocketResponse) -> None:
    """Track a prepared server socket until its request handler exits."""
    request.app[ACTIVE_WEBSOCKETS_KEY].add(websocket)


def _untrack_websocket(request: web.Request, websocket: web.WebSocketResponse) -> None:
    """Remove a server socket from the active shutdown set."""
    request.app[ACTIVE_WEBSOCKETS_KEY].discard(websocket)


async def _close_active_websockets(app: web.Application) -> None:
    """Bound and parallelize graceful closure of long-lived server sockets."""
    active_websockets: set[web.WebSocketResponse] = app[ACTIVE_WEBSOCKETS_KEY]
    websockets = tuple(active_websockets)
    if not websockets:
        return
    tasks = {
        asyncio.create_task(
            websocket.close(
                code=WSCloseCode.GOING_AWAY,
                message=b"Server shutting down",
            ),
            name="codex-server-websocket-close",
        )
        for websocket in websockets
    }
    done, pending = await asyncio.wait(
        tasks,
        timeout=SERVER_WEBSOCKET_SHUTDOWN_TIMEOUT_SECONDS,
    )
    await asyncio.gather(*done, return_exceptions=True)
    if not pending:
        return
    LOGGER.warning(
        "Server WebSocket shutdown deadline elapsed: pending=%d",
        len(pending),
    )
    for task in pending:
        task.cancel()
        task.add_done_callback(_consume_shutdown_task_result)


def _consume_shutdown_task_result(task: asyncio.Task[bool]) -> None:
    """Retrieve a late close task result without extending shutdown."""
    if task.cancelled():
        return
    with contextlib.suppress(Exception):
        task.result()


@web.middleware
async def _bearer_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    state: BridgeState = request.app[STATE_KEY]
    authorization = request.headers.get("Authorization", "")
    prefix = "Bearer "
    supplied = authorization[len(prefix) :] if authorization.startswith(prefix) else ""
    primary_match = hmac.compare_digest(supplied, state.config.bearer_token)
    device_token = state.config.realtime_device_token
    device_match = device_token is not None and hmac.compare_digest(
        supplied, device_token
    )
    if supplied and primary_match:
        request[_AUTH_IDENTITY_REQUEST_KEY] = _AUTH_IDENTITY_PRIMARY
    elif supplied and request.path == "/v1/realtime" and device_match:
        # Carry only a non-secret identity marker. The realtime handler admits
        # this restricted credential after a valid v2 negotiation is parsed.
        request[_AUTH_IDENTITY_REQUEST_KEY] = _AUTH_IDENTITY_REALTIME_DEVICE
    else:
        raise web.HTTPUnauthorized(
            text=json.dumps({"error": "unauthorized"}),
            content_type="application/json",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return cast(web.StreamResponse, await handler(request))


@web.middleware
async def _error_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    try:
        return cast(web.StreamResponse, await handler(request))
    except web.HTTPException:
        raise
    except (ProtocolError, ValueError) as exc:
        return web.json_response({"error": str(exc)}, status=400)
    except BridgeBusyError as exc:
        return web.json_response({"error": str(exc), "code": "busy"}, status=409)
    except (RpcError, AppServerExited) as exc:
        LOGGER.warning("Codex app-server request failed")
        return web.json_response({"error": str(exc)}, status=503)
    except TimeoutError:
        return web.json_response({"error": "Codex operation timed out"}, status=504)
    except AuthenticationRequired as exc:
        return web.json_response(
            {"error": str(exc), "code": "authentication_required"}, status=503
        )
    except BridgeError as exc:
        return web.json_response({"error": str(exc)}, status=500)


async def _app_server_lifecycle(app: web.Application) -> Any:
    state: BridgeState = app[STATE_KEY]
    try:
        await state.rpc.start()
    except BaseException:
        await state.close()
        raise
    maintenance = asyncio.create_task(
        state.maintain_conversations(), name="codex-conversation-maintenance"
    )
    try:
        yield
    finally:
        maintenance.cancel()
        await asyncio.gather(maintenance, return_exceptions=True)
        await state.close()


async def _reset_conversation(request: web.Request) -> web.Response:
    """Reset short-term context without changing any Home Assistant device."""
    state: BridgeState = request.app[STATE_KEY]
    payload = await request.json()
    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("conversation_id"), str)
        or not 0 < len(payload["conversation_id"]) <= 128
    ):
        raise ProtocolError("Invalid conversation reset request")
    await state.reset_conversation(payload["conversation_id"])
    return web.json_response({"status": "reset"})


async def _health(request: web.Request) -> web.Response:
    state: BridgeState = request.app[STATE_KEY]
    refresh = getattr(state.rpc, "refresh_account", None)
    if refresh is not None:
        with contextlib.suppress(RpcError, AppServerExited):
            await refresh()
    health = _public_health(state.rpc.health())
    ready = bool(health.get("running")) and health.get("auth_mode") == "chatgpt"
    return web.json_response(
        {
            "status": "ok" if ready else "unavailable",
            "app_server": health,
            "home_assistant_tools": state.home_assistant_tools.health(),
            "conversation_sessions": state.conversation_health(),
        },
        status=200 if ready else 503,
    )


async def _transcribe(request: web.Request) -> web.Response:
    state: BridgeState = request.app[STATE_KEY]
    async with state.speech_session_lease():
        return await _transcribe_admitted(request, state)


async def _transcribe_admitted(
    request: web.Request, state: BridgeState
) -> web.Response:
    payload = await _read_json(request)
    metadata_value = payload.get("metadata", {})
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    language = payload.get("language", metadata.get("language"))
    handoff_voice, handoff_language = _validate_speech_session_handoff(
        payload.get("speech_session_handoff"), error_type=ProtocolError
    )
    retained_language = _normalize_speech_handoff_language(language)
    if handoff_voice is not None and handoff_language != retained_language:
        raise ProtocolError(
            "speech_session_handoff language must match transcription language"
        )
    retained_voice = (
        handoff_voice
        if SPEECH_SESSION_HANDOFF_ENABLED and state.config.realtime_version == "v3"
        else None
    )
    raw = decode_base64_audio(payload.get("audio"))
    sample_rate = _positive_int(
        payload.get("sample_rate", metadata.get("sample_rate", 16_000)), "sample_rate"
    )
    channels = _positive_int(
        payload.get("channels", metadata.get("channels", 1)), "channels"
    )
    pcm, actual_rate, actual_channels = read_pcm16_payload(
        raw,
        audio_format=str(payload.get("format", "pcm")),
        codec=(
            str(payload.get("codec", metadata.get("codec")))
            if payload.get("codec", metadata.get("codec")) is not None
            else None
        ),
        sample_rate=sample_rate,
        channels=channels,
    )
    pcm = pcm16_mono_24khz(pcm, actual_rate, actual_channels)
    prepared_audio = _prepare_transcription_audio(pcm)
    pcm = prepared_audio.pcm

    prompt_value = payload.get("prompt")
    transcription_prompt = _transcription_prompt(
        language if isinstance(language, str) else None,
        prompt_value if isinstance(prompt_value, str) else None,
    )
    total_timeout = min(
        state.config.transcript_timeout, TRANSCRIPTION_TOTAL_TIMEOUT_SECONDS
    )
    peak = prepared_audio.peak
    rms = prepared_audio.rms
    adaptive_gain = prepared_audio.adaptive_gain
    duration = prepared_audio.duration
    transcript: str | None = None
    retained_session: _RetainedSpeechSession | None = None
    retained_promoted = False
    last_timeout: _TranscriptionAttemptTimeout | None = None
    current_attempt = 0
    try:
        async with asyncio.timeout(total_timeout):
            for current_attempt in range(1, TRANSCRIPTION_MAX_ATTEMPTS + 1):
                try:
                    if retained_voice is None:
                        transcript = await _run_transcription_attempt(
                            state,
                            payload,
                            pcm,
                            duration,
                            transcription_prompt,
                        )
                    else:
                        outcome = await _run_transcription_attempt_with_handoff(
                            state,
                            payload,
                            pcm,
                            duration,
                            transcription_prompt,
                            retain_voice=retained_voice,
                            retain_language=retained_language,
                        )
                        transcript = outcome.transcript
                        retained_session = outcome.retained_session
                    break
                except _TranscriptionAttemptTimeout as err:
                    last_timeout = err
                    LOGGER.warning(
                        "Realtime transcription attempt timed out: attempt=%d/%d "
                        "stage=%s normalized_duration_seconds=%.3f "
                        "normalized_peak=%.4f normalized_rms=%.4f "
                        "adaptive_gain=%.2f",
                        current_attempt,
                        TRANSCRIPTION_MAX_ATTEMPTS,
                        err.stage,
                        duration,
                        peak,
                        rms,
                        adaptive_gain,
                    )
    except _TranscriptionStartTimeout as err:
        LOGGER.warning(
            "Realtime transcription could not start: attempt=%d/%d stage=thread_start "
            "normalized_duration_seconds=%.3f normalized_peak=%.4f "
            "normalized_rms=%.4f adaptive_gain=%.2f",
            current_attempt,
            TRANSCRIPTION_MAX_ATTEMPTS,
            duration,
            peak,
            rms,
            adaptive_gain,
        )
        raise TimeoutError from err
    except TimeoutError:
        LOGGER.warning(
            "Realtime transcription reached its total deadline: attempt=%d/%d "
            "normalized_duration_seconds=%.3f "
            "normalized_peak=%.4f normalized_rms=%.4f adaptive_gain=%.2f",
            current_attempt,
            TRANSCRIPTION_MAX_ATTEMPTS,
            duration,
            peak,
            rms,
            adaptive_gain,
        )
        raise
    if transcript is None:
        raise TimeoutError from last_timeout
    try:
        response: dict[str, Any] = {"text": transcript}
        if isinstance(language, str) and language:
            response["language"] = language
        if retained_session is not None:
            response["speech_session_handoff"] = await state.offer_speech_session(
                retained_session
            )
            retained_promoted = True
        return web.json_response(response)
    finally:
        if retained_session is not None and not retained_promoted:
            await state.close_speech_session_resource(retained_session)


async def _transcribe_stream(request: web.Request) -> web.WebSocketResponse:
    """Admit a finite streaming STT capture before upgrading the connection."""
    state: BridgeState = request.app[STATE_KEY]
    async with state.speech_session_lease():
        # A WebSocket cannot change its HTTP status after prepare(). Keep managed
        # subscription failures, like bearer and busy failures, at the HTTP layer.
        state.require_subscription_auth()
        return await _transcribe_stream_admitted(request, state)


async def _transcribe_stream_admitted(
    request: web.Request, state: BridgeState
) -> web.WebSocketResponse:
    websocket = web.WebSocketResponse(heartbeat=30, max_msg_size=MAX_AUDIO_BYTES)
    await websocket.prepare(request)
    _track_websocket(request, websocket)
    stream_started_at = time.monotonic()
    capture_started_at: float | None = None
    capture_ended_at: float | None = None
    overlap_timing = _TranscriptionOverlapTiming()
    live_input: _LiveTranscriptionInput | None = None
    audio_ready: asyncio.Future[_PreparedTranscriptionAudio] | None = None
    transcription_task: asyncio.Task[_TranscriptionAttemptOutcome] | None = None
    capture_task: asyncio.Task[bytes] | None = None
    cancellation_task: asyncio.Task[None] | None = None
    retained_promoted = False
    try:
        try:
            first = await _receive_ws_json(
                websocket, timeout=TRANSCRIPTION_STREAM_START_TIMEOUT_SECONDS
            )
        except ProtocolError as err:
            raise _TranscriptionStreamProtocolError(str(err)) from None
        except TimeoutError as err:
            raise _TranscriptionStreamProtocolError(
                "transcription start timed out"
            ) from err
        (
            payload,
            sample_rate,
            language,
            prompt,
            handoff_voice,
        ) = _validate_transcription_stream_start(first)
        retained_voice = (
            handoff_voice
            if SPEECH_SESSION_HANDOFF_ENABLED and state.config.realtime_version == "v3"
            else None
        )
        audio_ready = asyncio.get_running_loop().create_future()
        live_input = _LiveTranscriptionInput(sample_rate=sample_rate)
        transcription_task = asyncio.create_task(
            _run_streaming_transcription(
                state,
                payload,
                audio_ready,
                live_input,
                _transcription_prompt(language, prompt),
                overlap_timing,
                retain_voice=retained_voice,
                retain_language=_normalize_speech_handoff_language(language),
            ),
            name="codex-streaming-transcription",
        )
        await websocket.send_json({"type": "started", "protocol_version": 1})
        capture_started_at = time.monotonic()
        capture_task = asyncio.create_task(
            _capture_transcription_stream(websocket, sample_rate, live_input),
            name="codex-transcription-capture",
        )

        done, _ = await asyncio.wait(
            {capture_task, transcription_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if capture_task not in done:
            # Success is impossible before audio_ready is resolved. Retrieving the
            # result here surfaces exhausted/ambiguous early setup failures promptly.
            await transcription_task
            raise ProtocolError(  # noqa: TRY301 - impossible worker result
                "transcription completed before capture ended"
            )

        raw_pcm = await capture_task
        capture_ended_at = time.monotonic()
        try:
            pcm = pcm16_mono_24khz(raw_pcm, sample_rate, 1)
            prepared_audio = _prepare_transcription_audio(pcm)
        except ProtocolError as err:
            raise _TranscriptionStreamProtocolError(str(err)) from None
        LOGGER.info(
            "Realtime transcription audio: input_duration_seconds=%.3f "
            "normalized_duration_seconds=%.3f peak=%.4f rms=%.4f "
            "adaptive_gain=%.2f",
            prepared_audio.input_duration,
            prepared_audio.duration,
            prepared_audio.peak,
            prepared_audio.rms,
            prepared_audio.adaptive_gain,
        )
        audio_ready.set_result(prepared_audio)
        cancellation_task = asyncio.create_task(
            _watch_transcription_stream_cancellation(websocket),
            name="codex-transcription-cancellation",
        )
        done, _ = await asyncio.wait(
            {transcription_task, cancellation_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if cancellation_task in done:
            await cancellation_task
        outcome = await transcription_task
        result: dict[str, Any] = {"type": "result", "text": outcome.transcript}
        if language:
            result["language"] = language
        if outcome.retained_session is not None:
            try:
                result["speech_session_handoff"] = await state.offer_speech_session(
                    outcome.retained_session
                )
                retained_promoted = True
            except BaseException:
                await state.close_speech_session_resource(outcome.retained_session)
                raise
        await websocket.send_json(result)
    except _TranscriptionStreamCancelled:
        pass
    except _TranscriptionStreamProtocolError as exc:
        await _safe_ws_json(websocket, {"type": "error", "error": str(exc)})
    except TimeoutError:
        await _safe_ws_json(
            websocket, {"type": "error", "error": "transcription timed out"}
        )
    except asyncio.CancelledError:
        raise
    except Exception as err:  # noqa: BLE001 - wire errors must never expose internals
        # App-server, WebRTC, and unexpected errors can contain private request
        # material. The streaming wire contract deliberately returns no details.
        LOGGER.warning(
            "Realtime transcription stream failed: failure_type=%s",
            type(err).__name__,
        )
        await _safe_ws_json(
            websocket, {"type": "error", "error": "transcription failed"}
        )
    finally:
        if capture_ended_at is None and capture_started_at is not None:
            capture_ended_at = time.monotonic()
        for task in (capture_task, transcription_task, cancellation_task):
            if task is not None and not task.done():
                task.cancel()
        if audio_ready is not None and not audio_ready.done():
            audio_ready.cancel()
        await asyncio.gather(
            *(
                task
                for task in (capture_task, transcription_task, cancellation_task)
                if task is not None
            ),
            return_exceptions=True,
        )
        if (
            not retained_promoted
            and transcription_task is not None
            and transcription_task.done()
            and not transcription_task.cancelled()
        ):
            with contextlib.suppress(Exception):
                unfinished_outcome = transcription_task.result()
                if unfinished_outcome.retained_session is not None:
                    await state.close_speech_session_resource(
                        unfinished_outcome.retained_session
                    )
        try:
            if not websocket.closed:
                await websocket.close()
        finally:
            _untrack_websocket(request, websocket)
        ended_at = time.monotonic()
        capture_seconds = (
            max(0.0, capture_ended_at - capture_started_at)
            if capture_started_at is not None and capture_ended_at is not None
            else 0.0
        )
        overlap_seconds = 0.0
        if (
            capture_started_at is not None
            and capture_ended_at is not None
            and overlap_timing.attempt_started_at is not None
            and overlap_timing.handshake_finished_at is not None
        ):
            overlap_seconds = max(
                0.0,
                min(capture_ended_at, overlap_timing.handshake_finished_at)
                - max(capture_started_at, overlap_timing.attempt_started_at),
            )
        post_capture_seconds = (
            max(0.0, ended_at - capture_ended_at)
            if capture_ended_at is not None
            else 0.0
        )
        LOGGER.info(
            "Realtime transcription stream timing: capture_seconds=%.3f "
            "handshake_capture_overlap_seconds=%.3f "
            "post_capture_seconds=%.3f total_seconds=%.3f",
            capture_seconds,
            overlap_seconds,
            post_capture_seconds,
            ended_at - stream_started_at,
        )
    return websocket


def _validate_transcription_stream_start(
    message: Mapping[str, Any],
) -> tuple[dict[str, Any], int, str | None, str | None, str | None]:
    """Validate and minimize the v1 stream start message."""
    if message.get("type") != "start":
        raise _TranscriptionStreamProtocolError(
            "first transcription message must have type 'start'"
        )
    _require_stream_integer(message, "protocol_version", 1)
    if message.get("format") != "pcm":
        raise _TranscriptionStreamProtocolError("format must be 'pcm'")
    if message.get("codec") != "pcm":
        raise _TranscriptionStreamProtocolError("codec must be 'pcm'")
    sample_rate = message.get("sample_rate")
    if type(sample_rate) is not int or sample_rate not in {16_000, 48_000}:
        raise _TranscriptionStreamProtocolError("sample_rate must be 16000 or 48000")
    _require_stream_integer(message, "bit_rate", 16)
    _require_stream_integer(message, "channels", 1)

    language_value = message.get("language")
    if "language" in message and not isinstance(language_value, str):
        raise _TranscriptionStreamProtocolError("language must be a string")
    prompt_value = message.get("prompt")
    if "prompt" in message and not isinstance(prompt_value, str):
        raise _TranscriptionStreamProtocolError("prompt must be a string")
    language = language_value if isinstance(language_value, str) else None
    prompt = prompt_value if isinstance(prompt_value, str) else None
    handoff_voice, handoff_language = _validate_speech_session_handoff(
        message.get("speech_session_handoff"),
        error_type=_TranscriptionStreamProtocolError,
    )
    if handoff_voice is not None and handoff_language != (
        _normalize_speech_handoff_language(language)
    ):
        raise _TranscriptionStreamProtocolError(
            "speech_session_handoff language must match transcription language"
        )
    payload: dict[str, Any] = {}
    if language:
        payload["language"] = language
    if prompt:
        payload["prompt"] = prompt
    return payload, sample_rate, language, prompt, handoff_voice


def _validate_speech_session_handoff(
    value: object,
    *,
    error_type: type[ProtocolError],
) -> tuple[str | None, str | None]:
    """Validate the shared finite-STT handoff request without retaining secrets."""
    if value is None:
        return None, None
    if not isinstance(value, Mapping):
        raise error_type("speech_session_handoff must be an object")
    version = value.get("version")
    if type(version) is not int or version != SPEECH_SESSION_HANDOFF_VERSION:
        raise error_type(f"version must be {SPEECH_SESSION_HANDOFF_VERSION}")
    voice_value = value.get("voice")
    if not isinstance(voice_value, str) or not voice_value.strip():
        raise error_type("speech_session_handoff voice must be a non-empty string")
    voice = voice_value.strip().lower()
    if len(voice) > 64:
        raise error_type("speech_session_handoff voice is too long")
    language_value = value.get("language")
    language = _normalize_speech_handoff_language(language_value)
    if language is None:
        raise error_type(
            "speech_session_handoff language must be a non-empty language tag"
        )
    return voice, language


def _normalize_speech_handoff_language(value: object) -> str | None:
    """Canonicalize a language tag identically on both sides of a handoff."""
    if not isinstance(value, str):
        return None
    raw_parts = value.strip().replace("_", "-").split("-")
    if not raw_parts or not raw_parts[0] or any(not part for part in raw_parts):
        return None
    normalized = [raw_parts[0].lower()]
    normalized.extend(
        part.upper() if len(part) == 2 and part.isalpha() else part
        for part in raw_parts[1:]
    )
    return "-".join(normalized)


def _require_stream_integer(
    message: Mapping[str, Any], key: str, expected: int
) -> None:
    value = message.get(key)
    if type(value) is not int or value != expected:
        raise _TranscriptionStreamProtocolError(f"{key} must be {expected}")


async def _capture_transcription_stream(
    websocket: web.WebSocketResponse,
    sample_rate: int,
    live_input: _LiveTranscriptionInput | None = None,
) -> bytes:
    """Collect bounded PCM16LE frames until the client's explicit EOF."""
    raw_pcm = bytearray()
    duration_limit = int(MAX_TRANSCRIPTION_DURATION_SECONDS * sample_rate * 2)
    try:
        async with asyncio.timeout(TRANSCRIPTION_STREAM_CAPTURE_TIMEOUT_SECONDS):
            while True:
                message = await websocket.receive()
                if message.type == WSMsgType.BINARY:
                    chunk = bytes(message.data)
                    if len(chunk) > TRANSCRIPTION_STREAM_MAX_FRAME_BYTES:
                        raise _TranscriptionStreamProtocolError(
                            "audio frame exceeds 256 KiB"
                        )
                    if len(chunk) % 2:
                        raise _TranscriptionStreamProtocolError(
                            "PCM16 audio frames must be sample-aligned"
                        )
                    next_size = len(raw_pcm) + len(chunk)
                    if next_size > TRANSCRIPTION_STREAM_MAX_RAW_BYTES:
                        raise _TranscriptionStreamProtocolError(
                            "audio capture exceeds the size limit"
                        )
                    if next_size > duration_limit:
                        raise _TranscriptionStreamProtocolError(
                            "audio must not exceed "
                            f"{MAX_TRANSCRIPTION_DURATION_SECONDS:g} seconds "
                            "for transcription"
                        )
                    raw_pcm.extend(chunk)
                    if live_input is not None:
                        live_input.chunks.put_nowait(chunk)
                    continue
                if message.type == WSMsgType.TEXT:
                    try:
                        value = json.loads(message.data)
                    except json.JSONDecodeError as err:
                        raise _TranscriptionStreamProtocolError(
                            "transcription control message must be valid JSON"
                        ) from err
                    if not isinstance(value, Mapping):
                        raise _TranscriptionStreamProtocolError(
                            "transcription control message must be a JSON object"
                        )
                    message_type = value.get("type")
                    if message_type == "end":
                        if live_input is not None:
                            live_input.chunks.put_nowait(None)
                        return bytes(raw_pcm)
                    if message_type == "cancel":
                        raise _TranscriptionStreamCancelled
                    raise _TranscriptionStreamProtocolError(
                        "expected binary audio, 'end', or 'cancel'"
                    )
                if message.type in {
                    WSMsgType.CLOSE,
                    WSMsgType.CLOSING,
                    WSMsgType.CLOSED,
                    WSMsgType.ERROR,
                }:
                    raise _TranscriptionStreamCancelled
                if message.type in {WSMsgType.PING, WSMsgType.PONG}:
                    continue
                raise _TranscriptionStreamProtocolError(
                    "unsupported transcription WebSocket message"
                )
    except TimeoutError as err:
        raise _TranscriptionStreamProtocolError("audio capture timed out") from err


async def _watch_transcription_stream_cancellation(
    websocket: web.WebSocketResponse,
) -> None:
    """Observe cancellation and disconnects while Codex finishes after EOF."""
    while True:
        message = await websocket.receive()
        if message.type == WSMsgType.TEXT:
            try:
                value = json.loads(message.data)
            except json.JSONDecodeError as err:
                raise _TranscriptionStreamProtocolError(
                    "transcription control message must be valid JSON"
                ) from err
            if isinstance(value, Mapping) and value.get("type") == "cancel":
                raise _TranscriptionStreamCancelled
            raise _TranscriptionStreamProtocolError(
                "only 'cancel' is accepted after transcription end"
            )
        if message.type in {
            WSMsgType.CLOSE,
            WSMsgType.CLOSING,
            WSMsgType.CLOSED,
            WSMsgType.ERROR,
        }:
            raise _TranscriptionStreamCancelled
        if message.type in {WSMsgType.PING, WSMsgType.PONG}:
            continue
        raise _TranscriptionStreamProtocolError(
            "only 'cancel' is accepted after transcription end"
        )


async def _run_streaming_transcription(
    state: BridgeState,
    payload: Mapping[str, Any],
    audio_ready: asyncio.Future[_PreparedTranscriptionAudio],
    live_input: _LiveTranscriptionInput,
    prompt: str,
    overlap_timing: _TranscriptionOverlapTiming,
    *,
    retain_voice: str | None = None,
    retain_language: str | None = None,
) -> _TranscriptionAttemptOutcome:
    """Run isolated attempts while capture resolves the shared audio future."""
    total_timeout = min(
        state.config.transcript_timeout, TRANSCRIPTION_TOTAL_TIMEOUT_SECONDS
    )
    last_timeout: _TranscriptionAttemptTimeout | None = None
    current_attempt = 0
    loop = asyncio.get_running_loop()
    try:
        # POST's total deadline begins only after its complete body is available.
        # Rescheduling here preserves that budget while still allowing setup to
        # overlap microphone capture.
        async with asyncio.timeout(None) as total_deadline:

            def start_total_deadline(
                completed_audio: asyncio.Future[_PreparedTranscriptionAudio],
            ) -> None:
                if not completed_audio.cancelled():
                    total_deadline.reschedule(loop.time() + total_timeout)

            audio_ready.add_done_callback(start_total_deadline)
            try:
                for current_attempt in range(1, TRANSCRIPTION_MAX_ATTEMPTS + 1):
                    try:
                        if current_attempt == 1:
                            return await _run_live_transcription_attempt(
                                state,
                                payload,
                                audio_ready,
                                live_input,
                                prompt,
                                overlap_timing=overlap_timing,
                                retain_voice=retain_voice,
                                retain_language=retain_language,
                            )
                        return await _run_transcription_attempt_when_audio_ready(
                            state,
                            payload,
                            audio_ready,
                            prompt,
                            retain_voice=retain_voice,
                            retain_language=retain_language,
                        )
                    except _TranscriptionAttemptTimeout as err:
                        last_timeout = err
                        duration, peak, rms, adaptive_gain = (
                            _stream_transcription_diagnostics(audio_ready)
                        )
                        LOGGER.warning(
                            "Realtime transcription attempt timed out: attempt=%d/%d "
                            "stage=%s normalized_duration_seconds=%.3f "
                            "normalized_peak=%.4f normalized_rms=%.4f "
                            "adaptive_gain=%.2f",
                            current_attempt,
                            TRANSCRIPTION_MAX_ATTEMPTS,
                            err.stage,
                            duration,
                            peak,
                            rms,
                            adaptive_gain,
                        )
            finally:
                audio_ready.remove_done_callback(start_total_deadline)
    except _TranscriptionStartTimeout as err:
        duration, peak, rms, adaptive_gain = _stream_transcription_diagnostics(
            audio_ready
        )
        LOGGER.warning(
            "Realtime transcription could not start: attempt=%d/%d "
            "normalized_duration_seconds=%.3f normalized_peak=%.4f "
            "normalized_rms=%.4f adaptive_gain=%.2f",
            current_attempt,
            TRANSCRIPTION_MAX_ATTEMPTS,
            duration,
            peak,
            rms,
            adaptive_gain,
        )
        raise TimeoutError from err
    except TimeoutError:
        duration, peak, rms, adaptive_gain = _stream_transcription_diagnostics(
            audio_ready
        )
        LOGGER.warning(
            "Realtime transcription reached its total deadline: attempt=%d/%d "
            "normalized_duration_seconds=%.3f normalized_peak=%.4f "
            "normalized_rms=%.4f adaptive_gain=%.2f",
            current_attempt,
            TRANSCRIPTION_MAX_ATTEMPTS,
            duration,
            peak,
            rms,
            adaptive_gain,
        )
        raise
    raise TimeoutError from last_timeout


def _stream_transcription_diagnostics(
    audio_ready: asyncio.Future[_PreparedTranscriptionAudio],
) -> tuple[float, float, float, float]:
    if not audio_ready.done() or audio_ready.cancelled():
        return 0.0, 0.0, 0.0, 1.0
    audio = audio_ready.result()
    return audio.duration, audio.peak, audio.rms, audio.adaptive_gain


async def _run_live_transcription_attempt(
    state: BridgeState,
    payload: Mapping[str, Any],
    audio_ready: asyncio.Future[_PreparedTranscriptionAudio],
    live_input: _LiveTranscriptionInput,
    prompt: str,
    *,
    overlap_timing: _TranscriptionOverlapTiming,
    retain_voice: str | None = None,
    retain_language: str | None = None,
) -> _TranscriptionAttemptOutcome:
    """Feed confidently calibrated audio during capture on the first attempt."""
    attempt_started = time.monotonic()
    overlap_timing.attempt_started_at = attempt_started
    thread_start_seconds = 0.0
    realtime_handshake_seconds = 0.0
    transcript_wait_seconds = 0.0
    session_stop_peer_close_seconds = 0.0
    thread_delete_seconds = 0.0
    live_feed = False
    live_gain = 1.0
    feed_backlog_seconds = 0.0
    completion_diagnostics: dict[str, float | str] = {}
    try:
        thread_start_started = time.monotonic()
        try:
            thread_id = await state.start_thread(
                payload,
                base_instructions=(
                    "Act only as a speech recognition adapter. Never call tools, "
                    "inspect files, or answer the user's speech."
                ),
            )
        except TimeoutError as err:
            raise _TranscriptionStartTimeout from err
        finally:
            thread_start_seconds = time.monotonic() - thread_start_started

        session: RealtimeSession | None = None
        audio_drain_task: asyncio.Task[None] | None = None
        handoff_boundary_state = (
            _SpeechHandoffBoundaryState() if retain_voice is not None else None
        )
        thread_owned = True
        timeout_stage = "handshake"
        try:
            session = RealtimeSession(
                state.rpc,
                thread_id,
                peer=state.peer_factory(),
                version=state.config.realtime_version,
                timeout=min(
                    state.config.transcript_timeout,
                    TRANSCRIPTION_SESSION_TIMEOUT_SECONDS,
                ),
            )
            handshake_started = time.monotonic()
            try:
                await session.start(
                    prompt=prompt,
                    voice=retain_voice,
                    include_startup_context=False,
                    client_managed_handoffs=True,
                )
            finally:
                realtime_handshake_seconds = time.monotonic() - handshake_started
                overlap_timing.handshake_finished_at = time.monotonic()
            audio_drain_task = asyncio.create_task(
                _drain_transcription_audio(
                    session,
                    strict_handoff_boundary=retain_voice is not None,
                    handoff_boundary_state=handoff_boundary_state,
                ),
                name="codex-live-transcription-audio-drain",
            )

            timeout_stage = "capture"
            resampler = Pcm16Mono24KhzResampler(live_input.sample_rate)
            normalizer = _StreamingTranscriptionNormalizer()
            feed_started: float | None = None
            while True:
                chunk = await live_input.chunks.get()
                if chunk is None:
                    break
                output = normalizer.feed(resampler.feed(chunk))
                if output:
                    if feed_started is None:
                        feed_started = asyncio.get_running_loop().time()
                    session.feed_audio(output)
            output = normalizer.feed(resampler.finish())
            if output:
                if feed_started is None:
                    feed_started = asyncio.get_running_loop().time()
                session.feed_audio(output)

            timeout_stage = "audio_ready"
            prepared_audio = await audio_ready
            if normalizer.active:
                live_feed = True
                live_gain = normalizer.gain or 1.0
                duration = normalizer.output_bytes / (REALTIME_SAMPLE_RATE * 2)
            else:
                feed_started = asyncio.get_running_loop().time()
                session.feed_audio(prepared_audio.pcm)
                duration = prepared_audio.duration
                live_gain = prepared_audio.adaptive_gain
            if feed_started is None:
                raise ProtocolError("transcription audio contains no output samples")

            trailing_silence = silence_pcm16(state.config.silence_ms)
            session.feed_audio(trailing_silence)
            feed_duration = duration + state.config.silence_ms / 1_000
            final_input_at = feed_started + feed_duration
            feed_backlog_seconds = max(
                0.0,
                final_input_at - asyncio.get_running_loop().time(),
            )
            drain_task = asyncio.create_task(
                session.wait_input_drained(
                    timeout=max(10.0, feed_backlog_seconds + 10.0),
                    monitor_app_server_exit=False,
                )
            )
            live_fragment_quiet_seconds = (
                state.config.live_fragment_quiet_seconds
                if state.config.live_fragment_quiet_seconds
                < TRANSCRIPTION_FRAGMENT_QUIET_SECONDS
                and normalizer.active
                and live_gain == 1.0
                and not normalizer.ever_gain_assisted
                and retain_voice is None
                else None
            )
            timeout_stage = "transcript"
            transcript_wait_started = time.monotonic()
            try:
                transcript_timeout = min(
                    state.config.transcript_timeout,
                    feed_backlog_seconds + TRANSCRIPTION_RESULT_TIMEOUT_SECONDS,
                )
                transcript = await _wait_for_user_transcript(
                    session,
                    transcript_timeout,
                    fragment_finalization_at=feed_started + duration,
                    strict_handoff_boundary=retain_voice is not None,
                    handoff_boundary_state=handoff_boundary_state,
                    input_drain_task=drain_task,
                    audio_drain_task=audio_drain_task,
                    live_fragment_quiet_seconds=live_fragment_quiet_seconds,
                    completion_diagnostics=completion_diagnostics,
                )
            finally:
                transcript_wait_seconds = time.monotonic() - transcript_wait_started
                if not drain_task.done():
                    drain_task.cancel()
                await asyncio.gather(drain_task, return_exceptions=True)
                await _retire_transcription_audio_drain(
                    audio_drain_task,
                    handoff_boundary_state=handoff_boundary_state,
                )
                audio_drain_task = None

            retained_session: _RetainedSpeechSession | None = None
            if (
                retain_voice is not None
                and handoff_boundary_state is not None
                and not handoff_boundary_state.invalidated
            ):
                try:
                    await _sanitize_speech_handoff_session(
                        session,
                        handoff_boundary_state,
                    )
                except (AppServerExited, BridgeError, TimeoutError, ValueError):
                    pass
                else:
                    retained_session = _RetainedSpeechSession(
                        session=session,
                        thread_id=thread_id,
                        voice=retain_voice,
                        language=retain_language,
                        boundary_state=handoff_boundary_state,
                    )
                    session = None
                    thread_owned = False
            return _TranscriptionAttemptOutcome(transcript, retained_session)
        except TimeoutError as err:
            raise _TranscriptionAttemptTimeout(timeout_stage) from err
        finally:
            try:
                try:
                    if audio_drain_task is not None:
                        await _retire_transcription_audio_drain(
                            audio_drain_task,
                            handoff_boundary_state=handoff_boundary_state,
                        )
                finally:
                    if session is not None:
                        session_stop_started = time.monotonic()
                        try:
                            await session.stop()
                        finally:
                            session_stop_peer_close_seconds = (
                                time.monotonic() - session_stop_started
                            )
            finally:
                if thread_owned:
                    thread_delete_started = time.monotonic()
                    try:
                        await _dispose_thread(state.rpc, thread_id)
                    finally:
                        thread_delete_seconds = time.monotonic() - thread_delete_started
    finally:
        LOGGER.info(
            "Realtime live transcription timing: live_feed=%s live_gain=%.2f "
            "feed_backlog_seconds=%.3f completion_reason=%s "
            "drain_to_result_seconds=%s",
            live_feed,
            live_gain,
            feed_backlog_seconds,
            completion_diagnostics.get("reason", "unknown"),
            (
                f"{completion_diagnostics['drain_to_result_seconds']:.3f}"
                if "drain_to_result_seconds" in completion_diagnostics
                else "unknown"
            ),
        )
        LOGGER.info(
            "Realtime transcription attempt timing: thread_start_seconds=%.3f "
            "realtime_handshake_seconds=%.3f transcript_wait_seconds=%.3f "
            "session_stop_peer_close_seconds=%.3f thread_delete_seconds=%.3f "
            "total_seconds=%.3f",
            thread_start_seconds,
            realtime_handshake_seconds,
            transcript_wait_seconds,
            session_stop_peer_close_seconds,
            thread_delete_seconds,
            time.monotonic() - attempt_started,
        )


async def _run_transcription_attempt(
    state: BridgeState,
    payload: Mapping[str, Any],
    pcm: bytes,
    duration: float,
    prompt: str,
) -> str:
    """Run one disposable realtime transcription attempt."""
    audio_ready = asyncio.get_running_loop().create_future()
    audio_ready.set_result(
        _PreparedTranscriptionAudio(
            pcm=pcm,
            duration=duration,
            input_duration=duration,
            peak=0.0,
            rms=0.0,
            adaptive_gain=1.0,
        )
    )
    outcome = await _run_transcription_attempt_when_audio_ready(
        state,
        payload,
        audio_ready,
        prompt,
    )
    return outcome.transcript


async def _run_transcription_attempt_with_handoff(
    state: BridgeState,
    payload: Mapping[str, Any],
    pcm: bytes,
    duration: float,
    prompt: str,
    *,
    retain_voice: str,
    retain_language: str | None,
) -> _TranscriptionAttemptOutcome:
    """Run one finite transcription attempt and retain a compatible v3 session."""
    audio_ready = asyncio.get_running_loop().create_future()
    audio_ready.set_result(
        _PreparedTranscriptionAudio(
            pcm=pcm,
            duration=duration,
            input_duration=duration,
            peak=0.0,
            rms=0.0,
            adaptive_gain=1.0,
        )
    )
    return await _run_transcription_attempt_when_audio_ready(
        state,
        payload,
        audio_ready,
        prompt,
        retain_voice=retain_voice,
        retain_language=retain_language,
    )


async def _run_transcription_attempt_when_audio_ready(
    state: BridgeState,
    payload: Mapping[str, Any],
    audio_ready: asyncio.Future[_PreparedTranscriptionAudio],
    prompt: str,
    *,
    overlap_timing: _TranscriptionOverlapTiming | None = None,
    retain_voice: str | None = None,
    retain_language: str | None = None,
) -> _TranscriptionAttemptOutcome:
    """Start a disposable session, then feed a normalized finite utterance."""
    attempt_started = time.monotonic()
    if overlap_timing is not None and overlap_timing.attempt_started_at is None:
        overlap_timing.attempt_started_at = attempt_started
    thread_start_seconds = 0.0
    realtime_handshake_seconds = 0.0
    transcript_wait_seconds = 0.0
    session_stop_peer_close_seconds = 0.0
    thread_delete_seconds = 0.0
    try:
        thread_start_started = time.monotonic()
        try:
            thread_id = await state.start_thread(
                payload,
                base_instructions=(
                    "Act only as a speech recognition adapter. Never call tools, "
                    "inspect files, or answer the user's speech."
                ),
            )
        except TimeoutError as err:
            # A timed-out start may still have created a remote thread, but no id was
            # returned for safe cleanup. Do not compound that ambiguity with a retry.
            raise _TranscriptionStartTimeout from err
        finally:
            thread_start_seconds = time.monotonic() - thread_start_started

        session: RealtimeSession | None = None
        audio_drain_task: asyncio.Task[None] | None = None
        handoff_boundary_state = (
            _SpeechHandoffBoundaryState() if retain_voice is not None else None
        )
        thread_owned = True
        timeout_stage = "handshake"
        try:
            session = RealtimeSession(
                state.rpc,
                thread_id,
                peer=state.peer_factory(),
                version=state.config.realtime_version,
                timeout=min(
                    state.config.transcript_timeout,
                    TRANSCRIPTION_SESSION_TIMEOUT_SECONDS,
                ),
            )
            handshake_started = time.monotonic()
            try:
                await session.start(
                    prompt=prompt,
                    voice=retain_voice,
                    include_startup_context=False,
                    client_managed_handoffs=True,
                )
            finally:
                realtime_handshake_seconds = time.monotonic() - handshake_started
                if (
                    overlap_timing is not None
                    and overlap_timing.handshake_finished_at is None
                ):
                    overlap_timing.handshake_finished_at = time.monotonic()
            audio_drain_task = asyncio.create_task(
                _drain_transcription_audio(
                    session,
                    strict_handoff_boundary=retain_voice is not None,
                    handoff_boundary_state=handoff_boundary_state,
                ),
                name="codex-transcription-audio-drain",
            )
            timeout_stage = "audio_ready"
            prepared_audio = await audio_ready
            pcm = prepared_audio.pcm
            duration = prepared_audio.duration
            timeout_stage = "input_drain"
            trailing_silence = silence_pcm16(state.config.silence_ms)
            feed_started = asyncio.get_running_loop().time()
            session.feed_audio(pcm + trailing_silence)
            feed_duration = duration + state.config.silence_ms / 1_000
            drain_task = asyncio.create_task(
                session.wait_input_drained(
                    timeout=max(10.0, duration + 10.0),
                    monitor_app_server_exit=False,
                )
            )
            timeout_stage = "transcript"
            transcript_wait_started = time.monotonic()
            try:
                # The remote recognizer can stop pulling the trailing silence once it
                # has emitted a transcript, leaving the local track's drain marker
                # unset. Transcript events are therefore the completion authority.
                transcript_timeout = min(
                    state.config.transcript_timeout,
                    feed_duration + TRANSCRIPTION_RESULT_TIMEOUT_SECONDS,
                )
                if retain_voice is None:
                    transcript = await _wait_for_user_transcript(
                        session,
                        transcript_timeout,
                        fragment_finalization_at=feed_started + duration,
                        audio_drain_task=audio_drain_task,
                    )
                else:
                    transcript = await _wait_for_user_transcript(
                        session,
                        transcript_timeout,
                        fragment_finalization_at=feed_started + duration,
                        strict_handoff_boundary=True,
                        handoff_boundary_state=handoff_boundary_state,
                        audio_drain_task=audio_drain_task,
                    )
            finally:
                transcript_wait_seconds = time.monotonic() - transcript_wait_started
                if not drain_task.done():
                    drain_task.cancel()
                await asyncio.gather(drain_task, return_exceptions=True)
                await _retire_transcription_audio_drain(
                    audio_drain_task,
                    handoff_boundary_state=handoff_boundary_state,
                )
                audio_drain_task = None
            retained_session: _RetainedSpeechSession | None = None
            if (
                retain_voice is not None
                and handoff_boundary_state is not None
                and not handoff_boundary_state.invalidated
            ):
                try:
                    await _sanitize_speech_handoff_session(
                        session,
                        handoff_boundary_state,
                    )
                except (AppServerExited, BridgeError, TimeoutError, ValueError):
                    pass
                else:
                    retained_session = _RetainedSpeechSession(
                        session=session,
                        thread_id=thread_id,
                        voice=retain_voice,
                        language=retain_language,
                        boundary_state=handoff_boundary_state,
                    )
                    session = None
                    thread_owned = False
            return _TranscriptionAttemptOutcome(transcript, retained_session)
        except TimeoutError as err:
            raise _TranscriptionAttemptTimeout(timeout_stage) from err
        finally:
            try:
                try:
                    if audio_drain_task is not None:
                        await _retire_transcription_audio_drain(
                            audio_drain_task,
                            handoff_boundary_state=handoff_boundary_state,
                        )
                finally:
                    if session is not None:
                        session_stop_started = time.monotonic()
                        try:
                            await session.stop()
                        finally:
                            session_stop_peer_close_seconds = (
                                time.monotonic() - session_stop_started
                            )
            finally:
                if thread_owned:
                    thread_delete_started = time.monotonic()
                    try:
                        await _dispose_thread(state.rpc, thread_id)
                    finally:
                        thread_delete_seconds = time.monotonic() - thread_delete_started
    finally:
        LOGGER.info(
            "Realtime transcription attempt timing: thread_start_seconds=%.3f "
            "realtime_handshake_seconds=%.3f transcript_wait_seconds=%.3f "
            "session_stop_peer_close_seconds=%.3f thread_delete_seconds=%.3f "
            "total_seconds=%.3f",
            thread_start_seconds,
            realtime_handshake_seconds,
            transcript_wait_seconds,
            session_stop_peer_close_seconds,
            thread_delete_seconds,
            time.monotonic() - attempt_started,
        )


def _prepare_transcription_audio(pcm: bytes) -> _PreparedTranscriptionAudio:
    """Apply the finite-utterance normalization shared by both STT transports."""
    if not pcm:
        raise ProtocolError("audio payload contains no samples")
    input_duration = len(pcm) / (REALTIME_SAMPLE_RATE * 2)
    if input_duration > MAX_TRANSCRIPTION_DURATION_SECONDS:
        raise ProtocolError(
            "audio must not exceed "
            f"{MAX_TRANSCRIPTION_DURATION_SECONDS:g} seconds for transcription"
        )

    peak, rms = _normalized_pcm16_levels(pcm)
    normalized_pcm, adaptive_gain = _apply_transcription_gain(pcm, peak=peak, rms=rms)
    normalized_pcm = _trim_transcription_silence(normalized_pcm)
    duration = len(normalized_pcm) / (REALTIME_SAMPLE_RATE * 2)
    if duration < input_duration:
        LOGGER.info(
            "Trimmed leading transcription silence: "
            "input_duration_seconds=%.3f trimmed_duration_seconds=%.3f "
            "normalized_peak=%.4f normalized_rms=%.4f",
            input_duration,
            duration,
            peak,
            rms,
        )
    return _PreparedTranscriptionAudio(
        pcm=normalized_pcm,
        duration=duration,
        input_duration=input_duration,
        peak=peak,
        rms=rms,
        adaptive_gain=adaptive_gain,
    )


def _transcription_prompt(language: str | None, prompt: str | None) -> str:
    language_hint = f" The expected language is {language}." if language else ""
    vocabulary_hint = (
        " Use this client-provided vocabulary/context hint when resolving "
        f"ambiguous speech: {prompt[:2_000]}"
        if prompt
        else ""
    )
    return (
        "Transcribe the user's speech accurately. Do not answer it and do not speak."
        + language_hint
        + vocabulary_hint
    )


def _normalized_pcm16_levels(pcm: bytes) -> tuple[float, float]:
    """Return privacy-safe peak and RMS levels for little-endian PCM16 audio."""
    samples = array.array("h")
    samples.frombytes(pcm)
    if sys.byteorder != "little":
        samples.byteswap()
    if not samples:
        return 0.0, 0.0
    peak = 0
    square_sum = 0
    for sample in samples:
        magnitude = abs(sample)
        peak = max(peak, magnitude)
        square_sum += sample * sample
    scale = 32_768.0
    return peak / scale, math.sqrt(square_sum / len(samples)) / scale


def _apply_transcription_gain(
    pcm: bytes, *, peak: float | None = None, rms: float | None = None
) -> tuple[bytes, float]:
    """Lift unusually quiet finite utterances while preventing clipping."""
    if peak is None or rms is None:
        peak, rms = _normalized_pcm16_levels(pcm)
    if not pcm or peak <= 0 or rms <= 0:
        return pcm, 1.0
    gain = _transcription_gain_for_levels(peak, rms)
    return _apply_pcm16_gain(pcm, gain), gain


def _transcription_gain_for_levels(peak: float, rms: float) -> float:
    if peak <= 0 or rms <= 0:
        return 1.0
    return max(
        1.0,
        min(
            TRANSCRIPTION_MAX_GAIN,
            TRANSCRIPTION_TARGET_PEAK / peak,
            TRANSCRIPTION_TARGET_RMS / rms,
        ),
    )


def _apply_pcm16_gain(pcm: bytes, gain: float) -> bytes:
    if not pcm or gain <= 1.0:
        return pcm
    samples = array.array("h")
    samples.frombytes(pcm)
    if sys.byteorder != "little":
        samples.byteswap()
    for index, sample in enumerate(samples):
        samples[index] = max(-32_768, min(32_767, round(sample * gain)))
    if sys.byteorder != "little":
        samples.byteswap()
    return samples.tobytes()


def _normalized_calibration_frame_levels(
    frames: list[_TranscriptionCalibrationFrame],
) -> tuple[float, float]:
    """Combine already-inspected frame levels without rescanning PCM samples."""
    peak = max(frame.peak for frame in frames) / 32_768.0
    square_sum = sum(frame.square_sum for frame in frames)
    sample_count = sum(frame.sample_count for frame in frames)
    return peak, math.sqrt(square_sum / sample_count) / 32_768.0


def _transcription_gain_for_calibration_frames(
    frames: list[_TranscriptionCalibrationFrame],
) -> float:
    peak, rms = _normalized_calibration_frame_levels(frames)
    return _transcription_gain_for_levels(peak, rms)


def _trim_transcription_silence(pcm: bytes) -> bytes:
    """Remove only a confidently silent prefix from finite PCM16 audio."""
    if not pcm or len(pcm) % 2:
        return pcm

    samples = array.array("h")
    samples.frombytes(pcm)
    if sys.byteorder != "little":
        samples.byteswap()

    frame_samples = REALTIME_SAMPLE_RATE * TRANSCRIPTION_TRIM_FRAME_MS // 1_000
    frame_levels: list[float] = []
    scale = 32_768.0
    for frame_start in range(0, len(samples), frame_samples):
        frame_end = min(frame_start + frame_samples, len(samples))
        square_sum = 0
        for sample in samples[frame_start:frame_end]:
            square_sum += sample * sample
        frame_levels.append(math.sqrt(square_sum / (frame_end - frame_start)) / scale)

    if len(frame_levels) < TRANSCRIPTION_TRIM_MIN_ACTIVE_FRAMES:
        return pcm

    probe_frames = max(
        1,
        TRANSCRIPTION_TRIM_NOISE_PROBE_MS // TRANSCRIPTION_TRIM_FRAME_MS,
    )
    opening_levels = sorted(frame_levels[:probe_frames])
    noise_floor = opening_levels[len(opening_levels) // 2]
    noise_ceiling = opening_levels[
        min(len(opening_levels) - 1, len(opening_levels) * 4 // 5)
    ]
    active_threshold = max(
        TRANSCRIPTION_TRIM_MIN_RMS,
        noise_floor * 4.0,
        noise_ceiling * 2.0,
        noise_floor + 0.01,
    )

    # Several active frames are required so an isolated click cannot establish
    # that the clip contains speech. Once speech is established, the lower
    # preservation threshold below keeps any earlier high-energy sound.
    strongest_levels = sorted(frame_levels, reverse=True)
    if strongest_levels[TRANSCRIPTION_TRIM_MIN_ACTIVE_FRAMES - 1] < active_threshold:
        return pcm

    onset_frame = _first_sustained_transcription_frame(frame_levels, active_threshold)
    if onset_frame is None:
        return pcm

    preservation_threshold = max(
        0.003,
        noise_floor * 1.35,
        noise_ceiling * 1.1,
        noise_floor + 0.002,
    )
    preserved_onset = _first_sustained_transcription_frame(
        frame_levels[:onset_frame], preservation_threshold
    )
    if preserved_onset is not None:
        onset_frame = preserved_onset

    # Sustained low-level contrast protects quiet speech. A single frame must
    # still be enough for clearly high-energy audio, even if it is isolated.
    for index, level in enumerate(frame_levels[:onset_frame]):
        if level >= active_threshold:
            onset_frame = index
            break

    preroll_frames = TRANSCRIPTION_TRIM_PREROLL_MS // TRANSCRIPTION_TRIM_FRAME_MS
    trim_frame = max(0, onset_frame - preroll_frames)
    minimum_trim_frames = (
        TRANSCRIPTION_TRIM_MIN_REMOVABLE_MS // TRANSCRIPTION_TRIM_FRAME_MS
    )
    if trim_frame < minimum_trim_frames:
        return pcm
    return pcm[trim_frame * frame_samples * 2 :]


def _first_sustained_transcription_frame(
    frame_levels: list[float], threshold: float
) -> int | None:
    """Return the first frame in a short sustained run above ``threshold``."""
    active_frames = [level >= threshold for level in frame_levels]
    for window_start in range(len(active_frames)):
        window = active_frames[
            window_start : window_start + TRANSCRIPTION_TRIM_ACTIVE_WINDOW_FRAMES
        ]
        if sum(window) >= TRANSCRIPTION_TRIM_MIN_ACTIVE_FRAMES:
            return window_start + window.index(True)
    return None


def _synthesis_output_sample_rate(payload: Mapping[str, Any]) -> int:
    """Validate the bridge's supported mono PCM16 output preferences."""
    sample_rate = payload.get("sample_rate", REALTIME_SAMPLE_RATE)
    channels = payload.get("channels", 1)
    sample_width = payload.get("sample_width", 2)
    if type(sample_rate) is not int or sample_rate not in {16_000, 24_000}:
        raise ProtocolError("synthesis sample_rate must be 16000 or 24000")
    if type(channels) is not int or channels != 1:
        raise ProtocolError("synthesis channels must be 1")
    if type(sample_width) is not int or sample_width != 2:
        raise ProtocolError("synthesis sample_width must be 2")
    return sample_rate


async def _synthesize(request: web.Request) -> web.StreamResponse:
    return await _synthesize_request(request)


async def _synthesize_stream(request: web.Request) -> web.StreamResponse:
    return await _synthesize_request(request, streaming=True)


async def _synthesize_request(
    request: web.Request, *, streaming: bool = False
) -> web.StreamResponse:
    state: BridgeState = request.app[STATE_KEY]
    await state.require_speech_session_available()
    payload = await _read_json(request)
    voice_value = payload.get("voice")
    voice = (
        voice_value.lower() if isinstance(voice_value, str) and voice_value else None
    )
    language = _normalize_speech_handoff_language(payload.get("language"))
    instructions_value = payload.get("instructions")
    has_instructions = bool(isinstance(instructions_value, str) and instructions_value)
    async with state.speech_session_lease(
        handoff_token=payload.get("speech_session_handoff_token"),
        voice=voice,
        language=language,
        has_instructions=has_instructions,
    ) as retained_session:
        return await _synthesize_admitted(
            request,
            state,
            payload=payload,
            retained_session=retained_session,
            streaming=streaming,
        )


async def _release_speech_session(request: web.Request) -> web.Response:
    state: BridgeState = request.app[STATE_KEY]
    payload = await _read_json(request)
    await state.release_speech_session_offer(
        payload.get("speech_session_handoff_token", payload.get("token"))
    )
    return web.Response(status=204)


async def _synthesize_admitted(
    request: web.Request,
    state: BridgeState,
    *,
    payload: Mapping[str, Any],
    retained_session: _RetainedSpeechSession | None = None,
    streaming: bool = False,
) -> web.StreamResponse:
    attempt_started = time.monotonic()
    thread_start_seconds = 0.0
    realtime_handshake_seconds = 0.0
    append_text_rpc_seconds = 0.0
    append_text_started_at: float | None = None
    audio_collection_seconds = 0.0
    session_stop_peer_close_seconds = 0.0
    thread_delete_seconds = 0.0
    collection_timing = _SynthesisCollectionTiming()
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ProtocolError("text must be a non-empty string")
    if len(text) > MAX_SYNTHESIS_TEXT_CHARS:
        raise ProtocolError(
            f"text must not exceed {MAX_SYNTHESIS_TEXT_CHARS} characters"
        )
    requested_format = str(payload.get("format", "wav")).lower()
    if requested_format not in {"wav", "wave", "audio/wav"}:
        raise ProtocolError("synthesis currently supports WAV output only")
    output_sample_rate = _synthesis_output_sample_rate(payload)
    voice_value = payload.get("voice")
    voice = (
        voice_value.lower() if isinstance(voice_value, str) and voice_value else None
    )
    response_headers = {
        "X-Audio-Sample-Rate": str(output_sample_rate),
        "X-Audio-Channels": "1",
        "X-Codex-Synthesis-Mode": "conversational-best-effort",
    }
    stream_response = (
        web.StreamResponse(headers=response_headers) if streaming else None
    )
    if stream_response is not None:
        stream_response.content_type = "audio/wav"
    stream_started = False
    pcm: bytes | None = None
    output_resampler = Pcm16MonoResampler(
        REALTIME_SAMPLE_RATE,
        output_sample_rate,
    )
    synthesis_deadline = (
        asyncio.get_running_loop().time() + state.config.synthesis_timeout
    )

    async def write_pcm_chunk(chunk: bytes) -> None:
        nonlocal stream_started
        assert stream_response is not None
        if not stream_started:
            await stream_response.prepare(request)
            stream_started = True
            await stream_response.write(
                streaming_wav_header(sample_rate=output_sample_rate)
            )
        if output := output_resampler.feed(chunk):
            await stream_response.write(output)

    async def run_session(
        retained: _RetainedSpeechSession | None,
    ) -> web.StreamResponse:
        nonlocal append_text_rpc_seconds
        nonlocal append_text_started_at
        nonlocal audio_collection_seconds
        nonlocal collection_timing
        nonlocal pcm
        nonlocal realtime_handshake_seconds
        nonlocal session_stop_peer_close_seconds
        nonlocal thread_delete_seconds
        nonlocal thread_start_seconds

        thread_id: str | None = None
        session: RealtimeSession
        if retained is None:
            thread_start_started = time.monotonic()
            try:
                thread_id = await state.start_thread(
                    payload,
                    base_instructions=(
                        "Act only as a deterministic voice renderer. Remain silent "
                        "until the client appends speakable text, then vocalize only "
                        "that text. Never greet, acknowledge, answer, paraphrase, add "
                        "or remove words, call tools, or inspect files."
                    ),
                )
            finally:
                thread_start_seconds += time.monotonic() - thread_start_started
            session = RealtimeSession(
                state.rpc,
                thread_id,
                peer=state.peer_factory(),
                version=state.config.realtime_version,
                timeout=max(
                    0.001,
                    synthesis_deadline - asyncio.get_running_loop().time(),
                ),
            )
        else:
            session = retained.session

        language = payload.get("language")
        language_hint = (
            f" Speak in {language}." if isinstance(language, str) and language else ""
        )
        instructions_value = payload.get("instructions")
        voice_hint = (
            f" Follow this client-provided voice guidance: {instructions_value[:2_000]}"
            if isinstance(instructions_value, str) and instructions_value
            else ""
        )
        try:
            if retained is None:
                handshake_started = time.monotonic()
                try:
                    await session.start(
                        prompt=(
                            "Stay silent until a speakable item arrives. Render that "
                            "item verbatim and naturally, without any preface or "
                            "follow-up."
                        )
                        + language_hint
                        + voice_hint,
                        voice=voice,
                        include_startup_context=False,
                        client_managed_handoffs=True,
                    )
                finally:
                    realtime_handshake_seconds += time.monotonic() - handshake_started
            append_text_started_at = time.monotonic()
            try:
                if retained is None:
                    await session.append_text(
                        "Vocalize only the following quoted data, with no "
                        "acknowledgement or extra words: "
                        f"{json.dumps(text, ensure_ascii=False)}",
                        role="user",
                    )
                else:
                    _validate_speech_handoff_boundary_now(
                        session,
                        retained.boundary_state,
                    )
                    await session.append_speech(text)
                    _validate_speech_handoff_boundary_now(
                        session,
                        retained.boundary_state,
                    )
            finally:
                append_text_rpc_seconds += time.monotonic() - append_text_started_at

            collection_started = time.monotonic()
            try:
                remaining = synthesis_deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError
                pcm = await _collect_speech_audio(
                    session,
                    remaining,
                    timing=collection_timing,
                    async_handle_chunk=write_pcm_chunk if streaming else None,
                )
                if stream_response is not None:
                    if not stream_started:
                        raise ProtocolError("realtime synthesis produced no audio")
                    if output := output_resampler.finish():
                        await stream_response.write(output)
                    await stream_response.write_eof()
            finally:
                audio_collection_seconds += time.monotonic() - collection_started
        finally:
            if retained is not None:
                session_stop_started = time.monotonic()
                try:
                    await state.close_speech_session_resource(retained)
                finally:
                    session_stop_peer_close_seconds += (
                        time.monotonic() - session_stop_started
                    )
            else:
                try:
                    session_stop_started = time.monotonic()
                    try:
                        await session.stop()
                    finally:
                        session_stop_peer_close_seconds += (
                            time.monotonic() - session_stop_started
                        )
                finally:
                    if thread_id is not None:
                        thread_delete_started = time.monotonic()
                        try:
                            await _dispose_thread(state.rpc, thread_id)
                        finally:
                            thread_delete_seconds += (
                                time.monotonic() - thread_delete_started
                            )
        if stream_response is not None:
            return stream_response
        assert pcm is not None
        finite_resampler = Pcm16MonoResampler(
            REALTIME_SAMPLE_RATE,
            output_sample_rate,
        )
        output_pcm = finite_resampler.feed(pcm) + finite_resampler.finish()
        return web.Response(
            body=wav_bytes(output_pcm, sample_rate=output_sample_rate),
            content_type="audio/wav",
            headers=response_headers,
        )

    try:
        if retained_session is not None:
            try:
                return await run_session(retained_session)
            except Exception:
                if stream_started or collection_timing.first_audio_at is not None:
                    raise
                if asyncio.get_running_loop().time() >= synthesis_deadline:
                    raise
                collection_timing = _SynthesisCollectionTiming()
                append_text_started_at = None
        return await run_session(None)
    finally:
        collection_ended_at = collection_timing.ended_at
        append_to_first_audio_seconds = (
            max(0.0, collection_timing.first_audio_at - append_text_started_at)
            if collection_timing.first_audio_at is not None
            and append_text_started_at is not None
            else 0.0
        )
        last_audio_to_collection_end_seconds = (
            max(0.0, collection_ended_at - collection_timing.last_audio_at)
            if collection_ended_at is not None
            and collection_timing.last_audio_at is not None
            else 0.0
        )
        completion_to_collection_end_seconds = (
            max(0.0, collection_ended_at - collection_timing.completion_at)
            if collection_ended_at is not None
            and collection_timing.completion_at is not None
            else 0.0
        )
        LOGGER.info(
            "Realtime synthesis attempt timing: thread_start_seconds=%.3f "
            "realtime_handshake_seconds=%.3f append_text_rpc_seconds=%.3f "
            "append_to_first_audio_seconds=%.3f audio_collection_seconds=%.3f "
            "last_audio_to_collection_end_seconds=%.3f "
            "completion_to_collection_end_seconds=%.3f "
            "session_stop_peer_close_seconds=%.3f thread_delete_seconds=%.3f "
            "total_to_response_ready_seconds=%.3f",
            thread_start_seconds,
            realtime_handshake_seconds,
            append_text_rpc_seconds,
            append_to_first_audio_seconds,
            audio_collection_seconds,
            last_audio_to_collection_end_seconds,
            completion_to_collection_end_seconds,
            session_stop_peer_close_seconds,
            thread_delete_seconds,
            time.monotonic() - attempt_started,
        )


async def _conversation(request: web.Request) -> web.WebSocketResponse:
    state: BridgeState = request.app[STATE_KEY]
    websocket = web.WebSocketResponse(heartbeat=30, max_msg_size=2 * 1024 * 1024)
    await websocket.prepare(request)
    _track_websocket(request, websocket)
    thread_id: str | None = None
    first: dict[str, Any] = {}
    try:
        first = await _receive_ws_json(websocket, timeout=30)
        if first.get("type") != "start":
            raise ProtocolError("first conversation message must have type 'start'")
        language = _conversation_language(first)
        (
            thread_id,
            persistent,
            seed_history,
            conversation_turn_state,
        ) = await state.conversation_thread(first)
        conversation_id = first.get("conversation_id") or thread_id
        await websocket.send_json(
            {
                "type": "started",
                "conversation_id": conversation_id,
                "thread_id": thread_id,
                **(
                    {"context_policy": SINGLE_TURN_FIRST}
                    if first.get("context_policy") == SINGLE_TURN_FIRST
                    else {}
                ),
                "context_status": (
                    "reused"
                    if not seed_history
                    else "recovered"
                    if _conversation_history(first, _current_user_text(first) or "")
                    and first.get("history_complete", True)
                    else "partial"
                    if _conversation_history(first, _current_user_text(first) or "")
                    else "lost"
                    if first.get("session_restored") is True
                    else "new"
                ),
            }
        )
        await _run_conversation_socket(
            state,
            websocket,
            thread_id,
            first,
            conversation_turn_state,
            language=language,
            persistent=persistent,
            seed_history=seed_history,
        )
    except TimeoutError:
        await _safe_ws_json(
            websocket, {"type": "error", "error": "conversation timed out"}
        )
    except AuthenticationRequired as exc:
        await _safe_ws_json(
            websocket,
            {"type": "error", "code": "authentication_required", "error": str(exc)},
        )
    except (BridgeError, ValueError) as exc:
        await _safe_ws_json(websocket, bridge_error(exc))
    finally:
        try:
            if not websocket.closed:
                await websocket.close()
        finally:
            _untrack_websocket(request, websocket)
            if (
                thread_id is not None
                and first.get("context_policy") == SINGLE_TURN_FIRST
            ):
                await state.release_fresh_thread(thread_id)
    return websocket


async def _run_conversation_socket(  # noqa: C901 - protocol state machine
    state: BridgeState,
    websocket: web.WebSocketResponse,
    thread_id: str,
    start_payload: Mapping[str, Any],
    turn_state: _ConversationTurnState,
    *,
    language: str | None,
    persistent: bool,
    seed_history: bool,
) -> None:
    subscription = state.rpc.subscribe()
    send_lock = asyncio.Lock()
    turn_event_lock = asyncio.Lock()
    turn_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
    early_turn_events: dict[str, list[dict[str, Any]]] = {}
    tool_requests: dict[str, int | str] = {}
    turn_tasks: set[asyncio.Task[None]] = set()
    active_turn: dict[str, str | None] = {"id": None}
    stop = asyncio.Event()
    socket_owner = object()
    adaptive = start_payload.get("context_policy") == SINGLE_TURN_FIRST
    allowed_tools = (
        registered_tools(normalize_dynamic_tools(start_payload.get("tools")))
        if adaptive
        else set()
    )
    claimed_tool_ids: set[str] = set()

    async def send(value: Mapping[str, Any]) -> None:
        async with send_lock:
            await websocket.send_json(dict(value))

    def event_turn_id(params: Mapping[str, Any]) -> str | None:
        value = params.get("turnId")
        if isinstance(value, str):
            return value
        turn = params.get("turn")
        if isinstance(turn, Mapping):
            nested_id = turn.get("id")
            if isinstance(nested_id, str):
                return nested_id
        return None

    async def handle_active_turn_event(
        event: Mapping[str, Any],
        params: Mapping[str, Any],
        method: object,
        turn_id: str,
    ) -> None:
        if method == "item/agentMessage/delta":
            await send({"type": "delta", "delta": str(params.get("delta", ""))})
        elif method == "item/tool/call" and "id" in event:
            call_id = str(params.get("callId", event["id"]))
            if adaptive:
                if not isinstance(params.get("tool"), str) or (
                    params.get("namespace") is not None
                    and not isinstance(params.get("namespace"), str)
                ):
                    raise ProtocolError("Invalid provider tool identity")
                if (params.get("namespace"), params.get("tool")) not in allowed_tools:
                    raise ProtocolError("Provider requested an unregistered tool")
                if call_id in claimed_tool_ids or len(claimed_tool_ids) >= 10:
                    raise ProtocolError(
                        "Repeated or excess provider tool call; do not retry"
                    )
                claimed_tool_ids.add(call_id)
            tool_requests[call_id] = event["id"]
            await send(
                {
                    "type": "tool_call",
                    "request_id": event["id"],
                    "call_id": call_id,
                    "name": params.get("tool"),
                    "namespace": params.get("namespace"),
                    "arguments": params.get("arguments", {}),
                }
            )
        elif method in {"item/started", "item/completed"}:
            # The Home Assistant client consumes only text, tools, done,
            # and errors. Item lifecycle remains internal diagnostics.
            return
        elif method == "turn/completed":
            waiter = turn_waiters.get(turn_id)
            if waiter is None or waiter.done():
                return
            waiter.set_result(dict(params))
        elif method in {"error", "thread/realtime/error"}:
            if params.get("willRetry") is True:
                return
            waiter = turn_waiters.get(turn_id)
            if waiter is not None and not waiter.done():
                waiter.set_exception(ProtocolError(provider_error(params)["error"]))

    async def run_turn(text: str) -> None:
        try:
            async with turn_state.turn_lock:
                if turn_state.retired:
                    raise ProtocolError(
                        "conversation thread was retired; reconnect to continue"
                    )
                turn_state.owner = socket_owner
                try:
                    turn_params: dict[str, Any] = {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": text}],
                        "approvalPolicy": "never",
                        "cwd": state.runtime_cwd,
                    }
                    model = start_payload.get("model")
                    if isinstance(model, str) and model:
                        turn_params["model"] = model
                    effort = start_payload.get("effort", DEFAULT_CONVERSATION_EFFORT)
                    if isinstance(effort, str) and effort:
                        turn_params["effort"] = effort
                    if "service_tier" in start_payload:
                        # App Server uses null to clear a tier previously selected
                        # on a reused thread. Omission would leave it unchanged.
                        turn_params["serviceTier"] = _app_server_service_tier(
                            start_payload.get("service_tier")
                        )
                    instructions = start_payload.get("instructions")
                    additional_context: dict[str, dict[str, str]] = {}
                    if isinstance(instructions, str) and instructions:
                        additional_context["home_assistant_instructions"] = {
                            "kind": "application",
                            "value": instructions,
                        }
                    if language is not None:
                        additional_context["home_assistant_language"] = {
                            "kind": "application",
                            "value": (
                                f"Default response language: {language}. Respond in "
                                "this language unless the user explicitly requests "
                                "another language. Do not switch languages based only "
                                "on accent, names, or isolated foreign words. If "
                                "uncertain, ask a brief clarification in the default "
                                "language."
                            ),
                        }
                    if seed_history and (
                        history := _conversation_history(start_payload, text)
                    ):
                        additional_context["home_assistant_history"] = {
                            "kind": "untrusted",
                            "value": history,
                        }
                    if additional_context:
                        turn_params["additionalContext"] = additional_context
                    response = await state.rpc.call(
                        "turn/start",
                        turn_params,
                    )
                    try:
                        turn_id = response["turn"]["id"]
                    except (KeyError, TypeError) as exc:
                        raise ProtocolError(
                            "turn/start response did not contain a turn id"
                        ) from exc
                    if not isinstance(turn_id, str):
                        raise ProtocolError("turn/start returned an invalid turn id")
                    waiter = asyncio.get_running_loop().create_future()
                    async with turn_event_lock:
                        turn_waiters[turn_id] = waiter
                        active_turn["id"] = turn_id
                        tool_requests.clear()
                        buffered = early_turn_events.pop(turn_id, [])
                        early_turn_events.clear()
                        for buffered_event in buffered:
                            buffered_params = buffered_event.get("params")
                            if isinstance(buffered_params, Mapping):
                                await handle_active_turn_event(
                                    buffered_event,
                                    buffered_params,
                                    buffered_event.get("method"),
                                    turn_id,
                                )
                    try:
                        try:
                            completion = await asyncio.wait_for(
                                waiter, timeout=state.config.request_timeout
                            )
                        except TimeoutError:
                            async with turn_event_lock:
                                if active_turn["id"] == turn_id:
                                    active_turn["id"] = None
                                tool_requests.clear()
                                early_turn_events.clear()
                            with contextlib.suppress(Exception):
                                await state.rpc.call(
                                    "turn/interrupt",
                                    {"threadId": thread_id, "turnId": turn_id},
                                    timeout=min(state.config.request_timeout, 10),
                                )
                            raise
                    finally:
                        if not waiter.done():
                            waiter.cancel()
                        async with turn_event_lock:
                            turn_waiters.pop(turn_id, None)
                            if active_turn["id"] == turn_id:
                                active_turn["id"] = None
                            tool_requests.clear()
                            early_turn_events.clear()
                finally:
                    async with turn_event_lock:
                        early_turn_events.clear()
                    if turn_state.owner is socket_owner:
                        turn_state.owner = None
            if turn_state.pending_owner is socket_owner:
                turn_state.pending_owner = None
            state.touch_conversation(start_payload.get("conversation_id"), thread_id)
            turn = completion.get("turn")
            if _turn_status(turn) == "failed":
                await send(provider_error(completion))
                await state.retire_conversation(
                    start_payload.get("conversation_id"), thread_id, turn_state
                )
                stop.set()
                await websocket.close()
            else:
                await send({"type": "done", "turn": turn, "status": _turn_status(turn)})
        except TimeoutError:
            await state.retire_conversation(
                start_payload.get("conversation_id"), thread_id, turn_state
            )
            await send({"type": "error", "error": "conversation timed out"})
            stop.set()
            await websocket.close()
        except (BridgeError, ValueError) as exc:
            await state.retire_conversation(
                start_payload.get("conversation_id"), thread_id, turn_state
            )
            await send(bridge_error(exc))
            stop.set()
            await websocket.close()
        finally:
            if turn_state.pending_owner is socket_owner:
                turn_state.pending_owner = None

    async def schedule_turn(text: str) -> None:
        if turn_state.retired:
            await send(
                {
                    "type": "error",
                    "code": "conversation_retired",
                    "error": "conversation thread was retired; reconnect to continue",
                }
            )
            return
        if turn_state.pending_owner is not None:
            await send(
                {
                    "type": "error",
                    "code": "busy",
                    "error": "a conversation turn is already in progress",
                }
            )
            return
        turn_state.pending_owner = socket_owner
        task = asyncio.create_task(run_turn(text), name="codex-conversation-turn")
        turn_tasks.add(task)
        task.add_done_callback(turn_tasks.discard)

    async def dispatch() -> None:
        while not stop.is_set():
            event = await subscription.get()
            method = event.get("method")
            if method == "bridge/appServerExited":
                params = event.get("params")
                returncode = (
                    params.get("returncode") if isinstance(params, Mapping) else None
                )
                await state.retire_conversation(
                    start_payload.get("conversation_id"), thread_id, turn_state
                )
                raise AppServerExited(
                    f"codex app-server exited with status {returncode}"
                )
            params = event.get("params")
            if not isinstance(params, Mapping) or params.get("threadId") != thread_id:
                continue
            if turn_state.owner is not socket_owner:
                continue
            turn_id = event_turn_id(params)
            if method in {"error", "thread/realtime/error"} and turn_id is None:
                if params.get("willRetry") is True:
                    continue
                await state.retire_conversation(
                    start_payload.get("conversation_id"), thread_id, turn_state
                )
                await send(provider_error(params))
                stop.set()
                await websocket.close()
                return
            if turn_id is None:
                continue
            async with turn_event_lock:
                if turn_state.owner is not socket_owner:
                    continue
                current_turn = active_turn["id"]
                if current_turn is None:
                    buffered_count = sum(map(len, early_turn_events.values()))
                    if buffered_count < MAX_EARLY_TURN_EVENTS:
                        early_turn_events.setdefault(turn_id, []).append(dict(event))
                    else:
                        LOGGER.warning("Dropping excess early Codex turn event")
                    continue
                if turn_id != current_turn:
                    continue
                await handle_active_turn_event(event, params, method, turn_id)

    async def receive() -> None:
        while not stop.is_set():
            message = await _receive_ws_json(websocket)
            message_type = message.get("type")
            if message_type in {"message", "text"}:
                if adaptive:
                    raise ProtocolError("Single-turn socket cannot start another turn")
                text = message.get("text")
                if not isinstance(text, str) or not text.strip():
                    await send(
                        {"type": "error", "error": "text must be a non-empty string"}
                    )
                    continue
                await schedule_turn(text)
            elif message_type == "tool_result":
                await _respond_to_tool_result(state.rpc, message, tool_requests)
            elif message_type == "stop":
                stop.set()
                return
            elif message_type == "ping":
                await send({"type": "pong"})
            else:
                await send(
                    {
                        "type": "error",
                        "error": f"unsupported message type: {message_type}",
                    }
                )

    dispatcher = asyncio.create_task(dispatch(), name="codex-conversation-events")
    receiver = asyncio.create_task(receive(), name="codex-conversation-receiver")
    initial_text = _current_user_text(start_payload)
    if isinstance(initial_text, str) and initial_text.strip():
        await schedule_turn(initial_text)
    try:
        done, _ = await asyncio.wait(
            {dispatcher, receiver}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            exception = task.exception()
            if exception is not None:
                raise exception
    finally:
        stop.set()
        had_pending_turn = turn_state.pending_owner is socket_owner
        if had_pending_turn:
            turn_state.retired = True
        current_turn = active_turn["id"]
        if current_turn:
            with contextlib.suppress(Exception):
                await state.rpc.call(
                    "turn/interrupt",
                    {"threadId": thread_id, "turnId": current_turn},
                    timeout=10,
                )
        for task in (dispatcher, receiver, *turn_tasks):
            task.cancel()
        await asyncio.gather(dispatcher, receiver, *turn_tasks, return_exceptions=True)
        subscription.close()
        if had_pending_turn:
            await state.retire_conversation(
                start_payload.get("conversation_id"), thread_id, turn_state
            )
        if turn_state.pending_owner is socket_owner:
            turn_state.pending_owner = None
        if not persistent:
            if adaptive:
                await state.release_fresh_thread(thread_id)
            else:
                await _dispose_thread(state.rpc, thread_id)


async def _home_assistant_tools(request: web.Request) -> web.WebSocketResponse:
    """Serve the single authenticated Home Assistant-owned tool authority."""
    state: BridgeState = request.app[STATE_KEY]
    websocket = web.WebSocketResponse(
        heartbeat=30,
        max_msg_size=MAX_TOOL_BROKER_MESSAGE_BYTES,
    )
    await websocket.prepare(request)
    _track_websocket(request, websocket)
    try:
        first = await _receive_ws_json(websocket, timeout=30)
        await state.home_assistant_tools.register(websocket, first)
        while True:
            message = await websocket.receive()
            if message.type in {
                WSMsgType.CLOSE,
                WSMsgType.CLOSING,
                WSMsgType.CLOSED,
            }:
                break
            if message.type is WSMsgType.ERROR:
                raise ProtocolError("Home Assistant tool WebSocket failed")
            if message.type is not WSMsgType.TEXT:
                raise ProtocolError(
                    "Home Assistant tool broker accepts only JSON text messages"
                )
            try:
                value = json.loads(message.data)
            except json.JSONDecodeError as exc:
                raise ProtocolError(
                    "Home Assistant tool broker message must be valid JSON"
                ) from exc
            if not isinstance(value, dict):
                raise ProtocolError(
                    "Home Assistant tool broker message must be a JSON object"
                )
            await state.home_assistant_tools.handle_message(websocket, value)
    except BridgeBusyError as exc:
        await _safe_ws_json(
            websocket,
            {"type": "error", "error": str(exc), "code": "busy"},
        )
    except (BridgeError, ValueError) as exc:
        await _safe_ws_json(websocket, {"type": "error", "error": str(exc)})
    finally:
        try:
            await state.home_assistant_tools.unregister(websocket)
        finally:
            try:
                if not websocket.closed:
                    await websocket.close()
            finally:
                _untrack_websocket(request, websocket)
    return websocket


async def _realtime(request: web.Request) -> web.WebSocketResponse:
    state: BridgeState = request.app[STATE_KEY]
    # Preserve the HTTP 409 preflight for an already-owned speech lane, but do
    # not let an authenticated socket reserve that lane while it idles before
    # sending a valid start message.
    await state.require_speech_session_available()
    return await _realtime_admitted(request, state)


async def _realtime_admitted(
    request: web.Request, state: BridgeState
) -> web.WebSocketResponse:
    websocket = web.WebSocketResponse(heartbeat=30, max_msg_size=MAX_AUDIO_BYTES)
    await websocket.prepare(request)
    _track_websocket(request, websocket)
    wire_protocol: RealtimeWireProtocol | None = None
    try:
        first = await _receive_ws_json(websocket, timeout=30)
        if first.get("type") != "start":
            raise ProtocolError("first realtime message must have type 'start'")
        wire_protocol = RealtimeWireProtocol.negotiate(first)
        if (
            request.get(_AUTH_IDENTITY_REQUEST_KEY) == _AUTH_IDENTITY_REALTIME_DEVICE
            and not wire_protocol.uses_binary_audio
        ):
            raise ProtocolError(
                "realtime device authentication requires protocol_version 2"
            )
        broker_snapshot = (
            state.home_assistant_tools.snapshot
            if (
                wire_protocol.uses_binary_audio
                and not wire_protocol.requests_native_conversation
            )
            else None
        )
        configured_tools = normalize_dynamic_tools(
            list(broker_snapshot.tools)
            if broker_snapshot is not None
            else first.get("tools")
        )
        async with state.speech_session_lease():
            await _serve_realtime_session(
                state,
                websocket,
                first,
                wire_protocol,
                configured_tools=configured_tools,
                broker_snapshot=broker_snapshot,
                managed_interrupt_continuation=(
                    request.headers.get("User-Agent")
                    == REALTIME_MANAGED_INTERRUPT_USER_AGENT
                ),
            )
    except _RealtimeClientDisconnected:
        pass
    except BridgeBusyError as exc:
        await _safe_realtime_json(
            websocket, {"type": "error", "error": str(exc), "code": "busy"}
        )
    except TimeoutError:
        await _safe_realtime_json(
            websocket, {"type": "error", "error": "realtime session timed out"}
        )
    except AuthenticationRequired as exc:
        await _safe_realtime_json(
            websocket,
            {"type": "error", "code": "authentication_required", "error": str(exc)},
        )
    except RpcError as exc:
        await _safe_realtime_json(
            websocket,
            {
                "type": "error",
                "error": (
                    "realtime provider request failed"
                    if wire_protocol is not None and wire_protocol.uses_binary_audio
                    else str(exc)
                ),
            },
        )
    except (BridgeError, ValueError) as exc:
        await _safe_realtime_json(websocket, {"type": "error", "error": str(exc)})
    finally:
        try:
            if not websocket.closed:
                await websocket.close()
        finally:
            _untrack_websocket(request, websocket)
    return websocket


async def _serve_realtime_session(
    state: BridgeState,
    websocket: web.WebSocketResponse,
    first: Mapping[str, Any],
    wire_protocol: RealtimeWireProtocol,
    *,
    configured_tools: list[dict[str, Any]],
    broker_snapshot: ToolBrokerSnapshot | None,
    managed_interrupt_continuation: bool,
) -> None:
    """Start and serve one provider session while its speech lease is held."""
    session: RealtimeSession | None = None
    thread_id: str | None = None
    executor_thread_id: str | None = None
    startup_abandoned = asyncio.Event()
    version = (
        state.config.realtime_version
        if wire_protocol.uses_binary_audio
        else str(first.get("version", state.config.realtime_version))
    )

    async def cleanup_owned_provider(
        owned_session: RealtimeSession | None,
        owned_thread_id: str | None,
        owned_executor_thread_id: str | None,
    ) -> None:
        """Stop one claimed transport and dispose both owned threads."""
        try:
            if owned_session is not None:
                await owned_session.stop()
        finally:
            thread_ids = tuple(
                dict.fromkeys(
                    thread
                    for thread in (owned_thread_id, owned_executor_thread_id)
                    if thread is not None
                )
            )
            if thread_ids:
                await asyncio.gather(
                    *(_dispose_thread(state.rpc, owned) for owned in thread_ids),
                    return_exceptions=True,
                )

    def start_provider_cleanup() -> asyncio.Task[None] | None:
        """Synchronously transfer current provider ownership to a tracked task."""
        nonlocal executor_thread_id, session, thread_id
        owned_session = session
        owned_thread_id = thread_id
        owned_executor_thread_id = executor_thread_id
        session = None
        thread_id = None
        executor_thread_id = None
        if (
            owned_session is None
            and owned_thread_id is None
            and owned_executor_thread_id is None
        ):
            return None
        cleanup_task = asyncio.create_task(
            cleanup_owned_provider(
                owned_session,
                owned_thread_id,
                owned_executor_thread_id,
            ),
            name="codex-realtime-provider-cleanup",
        )
        state.track_realtime_provider_cleanup(cleanup_task)
        return cleanup_task

    async def close_provider() -> None:
        cleanup_task = start_provider_cleanup()
        if cleanup_task is not None:
            await asyncio.shield(cleanup_task)

    async def start_provider() -> None:
        nonlocal executor_thread_id, session, thread_id
        try:
            thread_payload = dict(first)
            thread_payload.pop("model", None)
            bridge_managed_realtime = (
                wire_protocol.uses_binary_audio
                and version == "v3"
                and broker_snapshot is not None
            )
            LOGGER.info(
                "Realtime conversation route selected: route=%s selection=%s",
                "managed" if bridge_managed_realtime else "native",
                (
                    "explicit"
                    if wire_protocol.requests_native_conversation
                    else "legacy_auto"
                ),
            )
            base_instructions = (
                "Act as a natural realtime voice conversation partner. Respond "
                "directly in conversational spoken language. Keep answers concise "
                "unless the user asks for detail. Never inspect local files or "
                "invoke tools."
                if wire_protocol.requests_native_conversation
                else (
                    "Act only as a realtime Home Assistant voice agent. Never inspect "
                    "local files or use undeclared tools. Return only the shortest "
                    "natural final result suitable for speech. Do not narrate work, "
                    "send progress acknowledgements, offer follow-up help, or ask what "
                    "else to do unless clarification is required."
                )
            )
            if broker_snapshot is not None:
                base_instructions += (
                    "\n\nTrusted Home Assistant context follows. The available tools "
                    "and entity exposure are authoritative for this session. Never "
                    "retry a tool result marked do_not_retry or outcome unknown.\n"
                    f"Language: {broker_snapshot.language}\n"
                    f"{broker_snapshot.instructions}"
                )
            if bridge_managed_realtime:
                # Keep the provider-facing thread unable to perform Home Assistant
                # side effects. App Server v3 may route a native delegation before
                # a client-managed handoff notification is observable, so the
                # authoritative executor must live on a separate tool-bearing
                # thread to preserve exactly-once control semantics.
                executor_thread_id = await state.start_thread(
                    thread_payload,
                    tools=configured_tools,
                    base_instructions=base_instructions,
                )
                thread_id = await state.start_thread(
                    thread_payload,
                    tools=[],
                    base_instructions=(
                        "Act only as a realtime speech transport. Never inspect "
                        "local files, invoke tools, or claim that an action ran."
                    ),
                )
            else:
                thread_id = await state.start_thread(
                    thread_payload,
                    tools=configured_tools,
                    base_instructions=base_instructions,
                )
            if startup_abandoned.is_set():
                return
            session = RealtimeSession(
                state.rpc,
                thread_id,
                peer=state.peer_factory(),
                version=version,
                timeout=state.config.request_timeout,
            )
            if wire_protocol.uses_binary_audio:
                session.set_input_buffer_limit(
                    REALTIME_DEVICE_INPUT_BUFFER_MILLISECONDS
                )
            voice = first.get("voice")
            device_prompt = (
                first.get("prompt") if isinstance(first.get("prompt"), str) else None
            )
            await session.start(
                prompt=(
                    _realtime_frontend_prompt(device_prompt, broker_snapshot)
                    if bridge_managed_realtime
                    else device_prompt
                ),
                model=(
                    None
                    if wire_protocol.uses_binary_audio
                    else first.get("model")
                    if isinstance(first.get("model"), str)
                    else None
                ),
                voice=voice.lower() if isinstance(voice, str) and voice else None,
                include_startup_context=(
                    False
                    if (
                        bridge_managed_realtime
                        or wire_protocol.requests_native_conversation
                    )
                    else True
                    if wire_protocol.uses_binary_audio
                    else bool(first.get("include_startup_context", True))
                ),
                client_managed_handoffs=(
                    True
                    if bridge_managed_realtime
                    else False
                    if wire_protocol.uses_binary_audio
                    else bool(first.get("client_managed_handoffs", False))
                ),
                delegation_ack_filler=(False if bridge_managed_realtime else None),
                initial_items=(
                    None
                    if wire_protocol.uses_binary_audio
                    else first.get("initial_items")
                    if isinstance(first.get("initial_items"), list)
                    else None
                ),
            )
            if bridge_managed_realtime:
                await _settle_managed_realtime_startup(session)
        finally:
            if startup_abandoned.is_set():
                await close_provider()

    try:
        await _start_realtime_provider_or_disconnect(
            websocket,
            start_provider(),
            abandoned=startup_abandoned,
            thread_pending=lambda: thread_id is None,
            track_detached=state.track_realtime_startup_cleanup,
        )
        assert session is not None
        assert thread_id is not None
        await _send_realtime_json(
            websocket,
            {
                "type": "started",
                "conversation_id": first.get("conversation_id") or thread_id,
                "thread_id": thread_id,
                "realtime_session_id": session.realtime_session_id,
                "version": version,
                "sample_rate": REALTIME_SAMPLE_RATE,
                "channels": 1,
                **wire_protocol.started_fields(),
            },
        )
        await _run_realtime_socket(
            state,
            websocket,
            session,
            wire_protocol,
            broker_snapshot=broker_snapshot,
            executor_thread_id=executor_thread_id,
            managed_interrupt_continuation=managed_interrupt_continuation,
        )
    finally:
        await close_provider()


async def _settle_managed_realtime_startup(session: RealtimeSession) -> None:
    """Consume the ordered startup data burst before admitting managed turns."""
    deadline = time.monotonic() + REALTIME_MANAGED_STARTUP_TIMEOUT_SECONDS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProtocolError("realtime startup data boundary timed out")
        try:
            raw_event = await session.recv_data_event(timeout=remaining)
        except TimeoutError:
            raise ProtocolError("realtime startup data boundary timed out") from None
        control = parse_data_control_event(raw_event)
        if control is not None and control.event_type == "session.started":
            break
    # Audio produced before device admission is not owned by an executor
    # render. Empty both decoded queues once more at the quiet boundary.
    session.drain_audio_nowait()
    session.drain_data_events_nowait()


async def _start_realtime_provider_or_disconnect(
    websocket: web.WebSocketResponse,
    provider_start: Coroutine[Any, Any, None],
    *,
    abandoned: asyncio.Event,
    thread_pending: Callable[[], bool],
    track_detached: Callable[[asyncio.Task[None]], None],
) -> None:
    """Abandon provider startup when its device disappears before acknowledgement."""
    startup_task: asyncio.Task[None] = asyncio.create_task(
        provider_start, name="codex-realtime-provider-startup"
    )
    client_task = asyncio.create_task(
        websocket.receive(), name="codex-realtime-startup-client-monitor"
    )
    detached = False
    try:
        done, _ = await asyncio.wait(
            {startup_task, client_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if client_task in done:
            message = client_task.result()
            abandoned.set()
            if thread_pending() and not startup_task.done():
                # A written thread/start RPC may still create a thread after
                # local cancellation removes its response waiter. Keep the
                # task alive so its eventual thread id can be deleted.
                track_detached(startup_task)
                detached = True
            else:
                startup_task.cancel()
                await asyncio.gather(startup_task, return_exceptions=True)
            if message.type in {
                WSMsgType.CLOSE,
                WSMsgType.CLOSING,
                WSMsgType.CLOSED,
                WSMsgType.ERROR,
            }:
                raise _RealtimeClientDisconnected
            raise ProtocolError(
                "realtime messages are not accepted before session startup completes"
            )
        await startup_task
    except asyncio.CancelledError:
        abandoned.set()
        if thread_pending() and not startup_task.done():
            track_detached(startup_task)
            detached = True
        else:
            startup_task.cancel()
            await asyncio.gather(startup_task, return_exceptions=True)
        raise
    finally:
        if not client_task.done():
            client_task.cancel()
        if not detached and not startup_task.done():
            startup_task.cancel()
        cleanup_tasks = (client_task,) if detached else (startup_task, client_task)
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)


async def _run_realtime_socket(  # noqa: C901 - full-duplex protocol state machine
    state: BridgeState,
    websocket: web.WebSocketResponse,
    session: RealtimeSession,
    wire_protocol: RealtimeWireProtocol,
    *,
    broker_snapshot: ToolBrokerSnapshot | None,
    executor_thread_id: str | None = None,
    managed_interrupt_continuation: bool = False,
) -> None:
    bridge_managed_realtime = executor_thread_id is not None
    executor_subscription = state.rpc.subscribe() if bridge_managed_realtime else None
    send_lock = asyncio.Lock()
    stop = asyncio.Event()
    tool_requests: dict[str, int | str] = {}
    input_resampler = (
        Pcm16Mono24KhzResampler(wire_protocol.input_sample_rate)
        if wire_protocol.uses_binary_audio
        else None
    )
    output_state_lock = asyncio.Lock()
    output_preroll: deque[tuple[int, float, bytes]] = deque()
    output_preroll_bytes = 0
    output_epoch = 0
    output_speaking = False
    output_last_pcm_at: float | None = None
    output_armed = False
    output_arm_generation = 0
    output_arm_task: asyncio.Task[None] | None = None
    output_aux_tasks: set[asyncio.Task[None]] = set()
    tool_call_tasks: set[asyncio.Task[None]] = set()
    active_tool_calls: dict[
        int | str,
        tuple[str, asyncio.Task[None]],
    ] = {}
    seen_tool_request_ids: set[int | str] = set()
    seen_tool_call_ids: set[str] = set()
    claimed_tool_responses: set[int | str] = set()
    delivered_tool_responses: set[int | str] = set()
    tool_call_failures: asyncio.Queue[BaseException] = asyncio.Queue(maxsize=1)
    tool_continuation_task: asyncio.Task[None] | None = None
    tool_continuation_generation = 0
    pending_tool_continuation_correlation: str | None = None
    pending_tool_continuation_response_id: str | None = None
    pending_tool_continuation_output_announced = False
    pending_tool_continuation_output_delivered = False
    pending_tool_continuation_terminal = False
    tool_authority_failed_closed = False
    pending_cancel_confirmation: asyncio.Future[None] | None = None
    pending_cancel_response_id: str | None = None
    active_response_id: str | None = None
    active_background_turn_id: str | None = None
    active_background_turn_generation: int | None = None
    active_background_turn_had_tool = False
    active_background_turn_interrupting = False
    active_background_terminal: asyncio.Future[None] | None = None
    background_generation = 0
    background_turn_lock = asyncio.Lock()
    background_watchdog_tasks: set[asyncio.Task[None]] = set()
    background_turn_starting = False
    early_background_events: dict[str, list[dict[str, Any]]] = {}
    background_agent_messages: dict[str, tuple[str | None, str]] = {}
    background_agent_order: list[str] = []
    queued_background_request: tuple[int, str] | None = None
    user_transcript_handled = False
    input_speech_active = False
    pending_managed_speech_generation: int | None = None
    managed_user_turn_generations: OrderedDict[str, int] = OrderedDict()
    claimed_managed_turn_ids: set[str] = set()
    backend_render_generation: int | None = None
    backend_render_context_acknowledged = False
    backend_render_started = False
    backend_render_response_id: str | None = None
    backend_render_turn_id: str | None = None
    backend_render_terminal_seen = False
    backend_render_cancel_requested = False
    backend_render_retired: asyncio.Future[None] | None = None
    backend_render_quiet_until = 0.0
    backend_output_generation: int | None = None
    event_trace = _RealtimeEventTrace()

    async def send(value: Mapping[str, Any]) -> None:
        await _send_realtime_json(websocket, value, send_lock=send_lock)

    async def send_audio(
        chunk: bytes, expected_backend_generation: int | None = None
    ) -> bool:
        if wire_protocol.uses_binary_audio:
            guard = (
                None
                if expected_backend_generation is None
                else lambda: (
                    backend_output_generation == expected_backend_generation
                    and background_generation == expected_backend_generation
                )
            )
            return await _send_realtime_binary(
                websocket, chunk, send_lock=send_lock, guard=guard
            )
        await send(
            {
                "type": "audio",
                "audio": encode_base64_audio(chunk),
                "sample_rate": REALTIME_SAMPLE_RATE,
                "channels": 1,
            }
        )
        return True

    async def run_control(operation: Awaitable[None], name: str) -> None:
        try:
            async with asyncio.timeout(REALTIME_CONTROL_TIMEOUT_SECONDS):
                await operation
        except TimeoutError as exc:
            raise ProtocolError(f"realtime {name} timed out") from exc

    async def begin_output_locked(
        expected_backend_generation: int | None = None,
    ) -> None:
        nonlocal output_armed, output_arm_task, output_epoch
        nonlocal output_last_pcm_at, output_preroll_bytes, output_speaking
        if output_speaking or stop.is_set():
            return
        output_armed = False
        if output_arm_task is not None:
            output_arm_task.cancel()
            output_arm_task = None
        output_epoch += 1
        output_speaking = True
        await send(
            {
                "type": "control",
                "event_type": "speaking.started",
                "output_epoch": output_epoch,
            }
        )
        if expected_backend_generation is not None and (
            backend_output_generation != expected_backend_generation
            or background_generation != expected_backend_generation
        ):
            output_preroll.clear()
            output_preroll_bytes = 0
            return
        prune_output_preroll_locked()
        if output_preroll:
            output_last_pcm_at = output_preroll[-1][1]
            await send_audio(
                b"".join(entry[2] for entry in output_preroll),
                expected_backend_generation,
            )
            output_preroll.clear()
            output_preroll_bytes = 0

    def prune_output_preroll_locked() -> None:
        """Discard PCM not causally bound to the current output arm."""
        nonlocal output_preroll_bytes
        cutoff = time.monotonic() - REALTIME_OUTPUT_PREROLL_TTL_SECONDS
        while output_preroll and (
            output_preroll[0][0] != output_arm_generation
            or output_preroll[0][1] < cutoff
        ):
            output_preroll_bytes -= len(output_preroll.popleft()[2])

    async def arm_output(expected_backend_generation: int | None = None) -> None:
        nonlocal output_armed, output_arm_generation, output_arm_task
        nonlocal output_preroll_bytes

        async def expire_arm(generation: int) -> None:
            nonlocal output_armed, output_preroll_bytes
            await asyncio.sleep(REALTIME_OUTPUT_ARM_TIMEOUT_SECONDS)
            async with output_state_lock:
                if output_arm_generation != generation or output_speaking:
                    return
                output_armed = False
                output_preroll.clear()
                output_preroll_bytes = 0

        async with output_state_lock:
            if expected_backend_generation is not None and (
                backend_output_generation != expected_backend_generation
                or background_generation != expected_backend_generation
            ):
                return
            if output_speaking or stop.is_set():
                return
            if output_armed:
                # Multiple provider lifecycle events can describe the same
                # response. They must not create a new arm generation.
                return
            output_armed = True
            output_arm_generation += 1
            if output_arm_task is not None:
                output_arm_task.cancel()
            if output_preroll:
                await begin_output_locked(expected_backend_generation)
                return
            output_arm_task = asyncio.create_task(
                expire_arm(output_arm_generation),
                name="codex-realtime-output-arm-timeout",
            )
            output_aux_tasks.add(output_arm_task)
            output_arm_task.add_done_callback(output_aux_tasks.discard)

    async def end_output(*, after_tail: bool) -> None:
        nonlocal output_armed, output_arm_generation, output_arm_task
        nonlocal output_last_pcm_at, output_preroll_bytes
        nonlocal output_speaking
        expected_epoch: int | None = None
        terminal_at = time.monotonic()
        if after_tail:
            armed_without_media = False
            async with output_state_lock:
                if output_speaking:
                    expected_epoch = output_epoch
                else:
                    armed_without_media = output_armed
            if armed_without_media:
                # Data-channel terminal events can overtake the first media
                # frame. Give the already-authorized arm one short scheduling
                # window so valid PCM is not mistaken for late stale output.
                await asyncio.sleep(REALTIME_OUTPUT_TAIL_SECONDS)
                async with output_state_lock:
                    if output_speaking:
                        expected_epoch = output_epoch
            if expected_epoch is not None:
                hard_deadline = terminal_at + REALTIME_OUTPUT_TAIL_HARD_CAP_SECONDS
                while True:
                    async with output_state_lock:
                        if (
                            not output_speaking
                            or output_epoch != expected_epoch
                            or stop.is_set()
                        ):
                            return
                        now = time.monotonic()
                        last_activity = max(
                            terminal_at,
                            output_last_pcm_at
                            if output_last_pcm_at is not None
                            else terminal_at,
                        )
                        idle_remaining = REALTIME_OUTPUT_TAIL_SECONDS - (
                            now - last_activity
                        )
                        hard_remaining = hard_deadline - now
                        if idle_remaining <= 0 or hard_remaining <= 0:
                            break
                        sleep_for = min(idle_remaining, hard_remaining)
                    await asyncio.sleep(sleep_for)
        async with output_state_lock:
            output_armed = False
            output_arm_generation += 1
            if output_arm_task is not None:
                output_arm_task.cancel()
                output_arm_task = None
            if not output_speaking or (
                expected_epoch is not None and output_epoch != expected_epoch
            ):
                output_preroll.clear()
                output_preroll_bytes = 0
                return
            output_speaking = False
            output_last_pcm_at = None
            output_preroll.clear()
            output_preroll_bytes = 0
            await send(
                {
                    "type": "control",
                    "event_type": "speaking.stopped",
                    "output_epoch": output_epoch,
                }
            )

    def quarantine_output(chunk: bytes) -> None:
        nonlocal output_preroll_bytes
        if not output_armed or not _realtime_pcm_has_signal(chunk):
            return
        prune_output_preroll_locked()
        output_preroll.append((output_arm_generation, time.monotonic(), chunk))
        output_preroll_bytes += len(chunk)
        while output_preroll_bytes > REALTIME_OUTPUT_PREROLL_MAX_BYTES:
            output_preroll_bytes -= len(output_preroll.popleft()[2])

    async def request_remote_response_cancel() -> bool:
        """Return true only after the provider explicitly confirms cancellation."""
        nonlocal pending_cancel_confirmation, pending_cancel_response_id
        if pending_cancel_confirmation is not None:
            return False
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        pending_cancel_confirmation = waiter
        pending_cancel_response_id = active_response_id
        try:
            try:
                session.request_response_cancel()
            except Exception as exc:  # noqa: BLE001 - cancellation is best effort.
                LOGGER.info(
                    "Realtime provider response cancel was unavailable: %s", exc
                )
                return False
            try:
                await asyncio.wait_for(
                    asyncio.shield(waiter),
                    timeout=REALTIME_REMOTE_CANCEL_CONFIRM_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                LOGGER.info("Realtime provider did not confirm response cancellation")
                return False
            return True
        finally:
            if pending_cancel_confirmation is waiter:
                pending_cancel_confirmation = None
                pending_cancel_response_id = None
            if not waiter.done():
                waiter.cancel()

    def tool_correlation(request_id: int | str, call_id: str) -> str:
        """Return a content-free label for one provider request/call pair."""
        material = f"{request_id}\0{call_id}".encode()
        return hashlib.sha256(material).hexdigest()[:12]

    def cancel_tool_continuation_watchdog(*, clear_pending: bool = True) -> None:
        """Invalidate the current post-tool continuation generation."""
        nonlocal pending_tool_continuation_correlation
        nonlocal pending_tool_continuation_output_announced
        nonlocal pending_tool_continuation_output_delivered
        nonlocal pending_tool_continuation_response_id
        nonlocal pending_tool_continuation_terminal
        nonlocal tool_continuation_generation
        nonlocal tool_continuation_task
        tool_continuation_generation += 1
        if clear_pending:
            pending_tool_continuation_correlation = None
            pending_tool_continuation_response_id = None
            pending_tool_continuation_output_announced = False
            pending_tool_continuation_output_delivered = False
            pending_tool_continuation_terminal = False
        task = tool_continuation_task
        tool_continuation_task = None
        if task is not None and not task.done():
            task.cancel()

    def arm_pending_tool_continuation_watchdog() -> None:
        """Arm once every accepted call in the current provider batch has settled."""
        nonlocal tool_continuation_task
        correlation = pending_tool_continuation_correlation
        has_unsettled_call = any(
            request_id not in delivered_tool_responses
            for request_id in active_tool_calls
        )
        if correlation is None or has_unsettled_call or stop.is_set():
            return
        if tool_continuation_task is not None and not tool_continuation_task.done():
            return
        if (
            pending_tool_continuation_output_delivered
            or pending_tool_continuation_terminal
        ):
            cancel_tool_continuation_watchdog()
            return
        cancel_tool_continuation_watchdog(clear_pending=False)
        generation = tool_continuation_generation

        async def expire() -> None:
            await asyncio.sleep(REALTIME_TOOL_CONTINUATION_TIMEOUT_SECONDS)
            if stop.is_set() or generation != tool_continuation_generation:
                return
            LOGGER.warning(
                "Realtime provider tool continuation timed out correlation=%s",
                correlation,
            )
            if tool_call_failures.empty():
                tool_call_failures.put_nowait(
                    ProtocolError("realtime provider tool continuation timed out")
                )

        task = asyncio.create_task(
            expire(), name="codex-realtime-tool-continuation-timeout"
        )
        tool_continuation_task = task
        output_aux_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            nonlocal tool_continuation_task
            output_aux_tasks.discard(completed)
            if tool_continuation_task is completed:
                tool_continuation_task = None

        task.add_done_callback(finished)

    def require_tool_continuation(correlation: str) -> None:
        """Record a delivered result and await output after the complete tool batch."""
        nonlocal pending_tool_continuation_output_announced
        nonlocal pending_tool_continuation_output_delivered
        nonlocal pending_tool_continuation_correlation
        nonlocal pending_tool_continuation_response_id
        nonlocal pending_tool_continuation_terminal
        pending_tool_continuation_correlation = correlation
        pending_tool_continuation_response_id = active_response_id
        pending_tool_continuation_output_announced = False
        pending_tool_continuation_output_delivered = False
        pending_tool_continuation_terminal = False
        arm_pending_tool_continuation_watchdog()

    def announce_tool_continuation_output(response_id: str | None = None) -> None:
        """Bind later audible PCM to output announced after the current result."""
        nonlocal pending_tool_continuation_output_announced
        nonlocal pending_tool_continuation_response_id
        if pending_tool_continuation_correlation is None:
            return
        pending_tool_continuation_output_announced = True
        if response_id is not None:
            pending_tool_continuation_response_id = response_id

    def complete_tool_continuation_after_output() -> None:
        """Complete the deadline only after announced PCM crossed the device wire."""
        nonlocal pending_tool_continuation_output_delivered
        if (
            pending_tool_continuation_correlation is not None
            and pending_tool_continuation_output_announced
        ):
            pending_tool_continuation_output_delivered = True
            cancel_tool_continuation_watchdog()

    def complete_tool_continuation_after_terminal(
        event_type: str, response_id: str | None
    ) -> None:
        """Accept only a correlated terminal for the current post-tool response."""
        nonlocal pending_tool_continuation_terminal
        if pending_tool_continuation_correlation is None:
            return
        if event_type in {"response.cancelled", "response.done"}:
            matches = (
                response_id is not None
                and response_id == pending_tool_continuation_response_id
            )
        else:
            matches = pending_tool_continuation_output_announced
        if not matches:
            return
        pending_tool_continuation_terminal = True
        cancel_tool_continuation_watchdog()

    async def respond_to_provider_tool_once(
        request_id: int | str,
        call_id: str,
        *,
        success: bool,
        result: object,
        background_turn_generation: int | None = None,
    ) -> None:
        """Attempt exactly one App Server response for a provider request id."""
        if request_id in claimed_tool_responses:
            return
        correlation = tool_correlation(request_id, call_id)
        claimed_tool_responses.add(request_id)
        tool_requests = {call_id: request_id}
        LOGGER.info(
            "Delivering realtime Home Assistant tool result correlation=%s",
            correlation,
        )
        require_continuation = not bridge_managed_realtime or (
            background_turn_generation is not None
            and background_turn_generation == background_generation
        )
        if require_continuation:
            # App Server can emit the continuation as soon as it accepts the
            # response, before this coroutine is scheduled again after the
            # write. Establish the generation first; the undelivered request
            # keeps the watchdog itself disarmed until the write completes.
            require_tool_continuation(correlation)
        await _respond_to_tool_result(
            state.rpc,
            {
                "call_id": call_id,
                "success": success,
                "result": result,
            },
            tool_requests,
            timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
        )
        LOGGER.info(
            "Delivered realtime Home Assistant tool result correlation=%s",
            correlation,
        )
        delivered_tool_responses.add(request_id)
        if require_continuation:
            arm_pending_tool_continuation_watchdog()

    async def execute_home_assistant_tool_call(
        request_id: int | str,
        call_id: str,
        name: object,
        arguments: object,
        background_turn_generation: int | None,
    ) -> None:
        """Execute one provider call through the captured HA authority only."""
        nonlocal tool_authority_failed_closed
        correlation = tool_correlation(request_id, call_id)
        started_at = time.monotonic()
        LOGGER.info(
            "Realtime Home Assistant tool call started correlation=%s",
            correlation,
        )
        success = False
        result: object = {
            "error": "home_assistant_tool_unavailable",
            "do_not_retry": True,
        }
        if (
            broker_snapshot is not None
            and isinstance(name, str)
            and isinstance(arguments, Mapping)
        ):
            try:
                broker_result = await state.home_assistant_tools.call(
                    broker_snapshot,
                    name=name,
                    arguments=arguments,
                )
            except ToolBrokerUnavailable:
                tool_authority_failed_closed = True
                result = {
                    "error": "home_assistant_tool_outcome_unknown",
                    "do_not_retry": True,
                }
                LOGGER.warning(
                    "Realtime Home Assistant tool call outcome is unknown "
                    "correlation=%s",
                    correlation,
                    exc_info=True,
                )
            except ProtocolError:
                tool_authority_failed_closed = True
                LOGGER.warning(
                    "Realtime Home Assistant tool call failed closed correlation=%s",
                    correlation,
                    exc_info=True,
                )
            else:
                success = broker_result.success
                result = broker_result.result
                if (
                    not success
                    and isinstance(result, Mapping)
                    and result.get("do_not_retry") is True
                ):
                    tool_authority_failed_closed = True
        else:
            tool_authority_failed_closed = True
        LOGGER.info(
            "Realtime Home Assistant tool call returned correlation=%s success=%s "
            "duration_ms=%d",
            correlation,
            success,
            round((time.monotonic() - started_at) * 1_000),
        )
        await respond_to_provider_tool_once(
            request_id,
            call_id,
            success=success,
            result=result,
            background_turn_generation=background_turn_generation,
        )

    def start_home_assistant_tool_call(event: Mapping[str, Any]) -> None:
        """Run one bounded, deduplicated call without blocking lifecycle events."""
        request_id = event.get("id")
        params = event.get("params")
        if not isinstance(request_id, (int, str)) or not isinstance(params, Mapping):
            if tool_call_failures.empty():
                tool_call_failures.put_nowait(
                    ProtocolError("realtime provider returned an invalid tool call")
                )
            return
        if request_id in seen_tool_request_ids:
            return
        seen_tool_request_ids.add(request_id)
        call_id = str(params.get("callId", request_id))
        cancel_tool_continuation_watchdog()
        name = params.get("tool")
        arguments = params.get("arguments", {})
        owned_background_generation = (
            active_background_turn_generation if bridge_managed_realtime else None
        )

        rejection: object | None = None
        session_limit_exceeded = (
            len(seen_tool_request_ids) > REALTIME_MAX_TOOL_CALLS_PER_SESSION
        )
        if tool_authority_failed_closed:
            rejection = {
                "error": "home_assistant_tool_session_unavailable",
                "do_not_retry": True,
            }
        elif session_limit_exceeded:
            rejection = {
                "error": "home_assistant_tool_session_limit",
                "do_not_retry": True,
            }
        elif call_id in seen_tool_call_ids:
            rejection = {
                "error": "duplicate_home_assistant_tool_call",
                "do_not_retry": True,
            }
        elif len(active_tool_calls) >= REALTIME_MAX_PENDING_TOOL_CALLS:
            rejection = {
                "error": "too_many_home_assistant_tool_calls",
                "do_not_retry": True,
            }
        else:
            seen_tool_call_ids.add(call_id)

        async def run() -> None:
            try:
                if rejection is not None:
                    await respond_to_provider_tool_once(
                        request_id,
                        call_id,
                        success=False,
                        result=rejection,
                        background_turn_generation=owned_background_generation,
                    )
                else:
                    await execute_home_assistant_tool_call(
                        request_id,
                        call_id,
                        name,
                        arguments,
                        owned_background_generation,
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 - wake the socket owner.
                if tool_call_failures.empty():
                    tool_call_failures.put_nowait(exc)

        task = asyncio.create_task(run(), name="codex-realtime-home-assistant-tool")
        active_tool_calls[request_id] = (call_id, task)
        tool_call_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            tool_call_tasks.discard(completed)
            active = active_tool_calls.get(request_id)
            if (
                active is not None
                and active[1] is completed
                and request_id in claimed_tool_responses
            ):
                active_tool_calls.pop(request_id, None)
                arm_pending_tool_continuation_watchdog()

        task.add_done_callback(finished)
        if session_limit_exceeded and tool_call_failures.empty():
            tool_call_failures.put_nowait(
                ProtocolError("realtime provider exceeded the tool-call limit")
            )

    async def raise_tool_call_failure() -> None:
        raise await tool_call_failures.get()

    def bridge_output_authorized() -> bool:
        return not bridge_managed_realtime or (
            backend_output_generation == background_generation
        )

    def invalidate_bridge_output() -> None:
        """Tombstone provider output that was not explicitly rendered by the bridge."""
        nonlocal backend_output_generation
        backend_output_generation = None
        cancel_tool_continuation_watchdog()

    def request_provider_cancel_best_effort() -> None:
        try:
            session.request_response_cancel()
        except Exception:  # noqa: BLE001 - the local gate remains authoritative.
            LOGGER.info("Realtime provider response cancel was unavailable")

    def request_backend_render_cancel_best_effort() -> None:
        """Cancel only a started bridge-owned render, never an idle session."""
        nonlocal backend_render_cancel_requested
        if backend_render_generation is None or not backend_render_started:
            return
        if backend_render_cancel_requested:
            return
        backend_render_cancel_requested = True
        request_provider_cancel_best_effort()

    def begin_background_generation() -> int:
        nonlocal background_generation
        background_generation += 1
        invalidate_bridge_output()
        return background_generation

    async def authorize_backend_output(generation: int) -> None:
        nonlocal backend_output_generation
        if generation != background_generation:
            return
        backend_output_generation = generation
        announce_tool_continuation_output()
        await arm_output(generation)

    def maybe_retire_backend_render() -> None:
        """Release one render slot only after its ack and terminal boundary."""
        nonlocal backend_render_cancel_requested
        nonlocal backend_render_context_acknowledged, backend_render_generation
        nonlocal backend_render_quiet_until, backend_render_retired
        nonlocal backend_render_response_id, backend_render_started
        nonlocal backend_render_terminal_seen, backend_render_turn_id
        if (
            backend_render_generation is None
            or not backend_render_context_acknowledged
            or not backend_render_terminal_seen
        ):
            return
        retired = backend_render_retired
        backend_render_generation = None
        backend_render_context_acknowledged = False
        backend_render_started = False
        backend_render_response_id = None
        backend_render_turn_id = None
        backend_render_terminal_seen = False
        backend_render_cancel_requested = False
        backend_render_retired = None
        backend_render_quiet_until = max(
            backend_render_quiet_until,
            time.monotonic() + REALTIME_OUTPUT_TAIL_SECONDS,
        )
        session.drain_audio_nowait()
        if retired is not None and not retired.done():
            retired.set_result(None)

    def mark_backend_render_started(
        response_id: str | None = None, turn_id: str | None = None
    ) -> bool:
        """Bind a post-ack provider start to the sole bridge-owned render."""
        nonlocal backend_render_response_id, backend_render_started
        nonlocal backend_render_turn_id
        if (
            backend_render_generation is None
            or not backend_render_context_acknowledged
            or (response_id is None and turn_id is None)
        ):
            return False
        if (
            response_id is not None
            and backend_render_response_id is not None
            and backend_render_response_id != response_id
        ):
            return False
        if (
            turn_id is not None
            and backend_render_turn_id is not None
            and backend_render_turn_id != turn_id
        ):
            return False
        if response_id is not None:
            backend_render_response_id = response_id
        if turn_id is not None:
            backend_render_turn_id = turn_id
        backend_render_started = True
        return True

    def claim_managed_turn_id(turn_id: str) -> bool:
        """Claim one provider turn ID for the lifetime of this socket."""
        if turn_id in claimed_managed_turn_ids:
            return False
        if len(claimed_managed_turn_ids) >= REALTIME_MAX_MANAGED_TURNS_PER_SESSION:
            raise ProtocolError("realtime provider exceeded the managed-turn limit")
        claimed_managed_turn_ids.add(turn_id)
        return True

    def backend_render_terminal_matches(
        response_id: str | None = None, turn_id: str | None = None
    ) -> bool:
        """Return whether a terminal is correlated to the bridge-owned render."""
        if (
            backend_render_generation is None
            or not backend_render_context_acknowledged
            or not backend_render_started
            or (response_id is None and turn_id is None)
        ):
            return False
        if response_id is not None and (
            backend_render_response_id is None
            or backend_render_response_id != response_id
        ):
            return False
        if turn_id is not None and (
            backend_render_turn_id is None or backend_render_turn_id != turn_id
        ):
            return False
        return True

    def mark_backend_render_terminal(
        response_id: str | None = None, turn_id: str | None = None
    ) -> None:
        """Record only a terminal correlated to the bridge-owned render."""
        nonlocal backend_render_terminal_seen
        if not backend_render_terminal_matches(response_id, turn_id):
            return
        backend_render_terminal_seen = True
        maybe_retire_backend_render()

    async def interrupt_active_background_turn_locked() -> bool:
        """Interrupt a side-effect-free executor turn and await its terminal event."""
        nonlocal active_background_turn_interrupting
        turn_id = active_background_turn_id
        terminal = active_background_terminal
        if turn_id is None:
            return True
        if active_background_turn_had_tool:
            return False
        # Tombstone the side-effect-free turn before yielding to App Server.
        # A late tool request for this turn must never cross the HA authority
        # boundary after barge-in has committed to interruption.
        active_background_turn_interrupting = True
        assert executor_thread_id is not None
        await state.rpc.call(
            "turn/interrupt",
            {"threadId": executor_thread_id, "turnId": turn_id},
            timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
        )
        if terminal is not None and not terminal.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(terminal),
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                raise ProtocolError(
                    "interrupted realtime executor turn did not terminate"
                ) from exc
        return True

    def active_background_turn_owner() -> tuple[str | None, int | None]:
        """Snapshot the executor owner at an interrupt linearization point."""
        return active_background_turn_id, active_background_turn_generation

    async def interrupt_active_background_turn(
        expected_owner: tuple[str | None, int | None],
    ) -> None:
        async with background_turn_lock:
            if active_background_turn_owner() != expected_owner:
                return
            await interrupt_active_background_turn_locked()

    def arm_background_turn_watchdog(
        turn_id: str,
        generation: int,
        terminal: asyncio.Future[None],
    ) -> None:
        """Bound one executor turn and fail the device closed on lost completion."""

        async def expire() -> None:
            nonlocal active_background_turn_interrupting
            try:
                async with asyncio.timeout(state.config.request_timeout):
                    await asyncio.shield(terminal)
            except TimeoutError:
                pass
            else:
                return

            async with background_turn_lock:
                if (
                    active_background_turn_owner() != (turn_id, generation)
                    or terminal.done()
                ):
                    return
                active_background_turn_interrupting = True
                # The timeout is fatal for the whole socket, not only this
                # executor generation. Invalidate a newer queued request too,
                # before the interrupted turn's terminal can start it.
                begin_background_generation()
                try:
                    await state.rpc.call(
                        "turn/interrupt",
                        {"threadId": executor_thread_id, "turnId": turn_id},
                        timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                    )
                except Exception:  # noqa: BLE001 - timeout still closes the socket.
                    LOGGER.warning(
                        "Could not interrupt timed-out realtime executor turn"
                    )
                if not terminal.done():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(terminal),
                            timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                        )
                    except TimeoutError:
                        LOGGER.warning(
                            "Timed-out realtime executor turn did not terminate"
                        )
            if tool_call_failures.empty():
                tool_call_failures.put_nowait(
                    ProtocolError("assistant request timed out")
                )

        task = asyncio.create_task(
            expire(), name="codex-realtime-executor-turn-timeout"
        )
        background_watchdog_tasks.add(task)

        def finished(completed: asyncio.Task[None]) -> None:
            background_watchdog_tasks.discard(completed)
            with contextlib.suppress(BaseException):
                completed.exception()

        task.add_done_callback(finished)

    async def append_background_speech(text: str, generation: int) -> None:
        """Render one completed executor answer through the isolated voice thread."""
        nonlocal backend_render_cancel_requested
        nonlocal backend_render_context_acknowledged, backend_render_generation
        nonlocal backend_render_quiet_until, backend_render_retired
        nonlocal backend_render_response_id, backend_render_started
        nonlocal backend_render_terminal_seen, backend_render_turn_id
        if generation != background_generation:
            return
        spoken = _truncate_utf8_bytes(
            text.strip(), REALTIME_MANAGED_SPEECH_MAX_UTF8_BYTES
        )
        if not spoken:
            await send(
                {"type": "error", "error": "assistant returned no final response"}
            )
            return
        previous_retired = backend_render_retired
        if backend_render_generation is not None:
            request_backend_render_cancel_best_effort()
            if previous_retired is None:
                raise ProtocolError("realtime render ownership is inconsistent")
            try:
                await asyncio.wait_for(
                    asyncio.shield(previous_retired),
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                raise ProtocolError(
                    "interrupted realtime render did not terminate"
                ) from exc
        # Provider terminal controls can overtake their final RTP frames. Keep
        # the gate closed through the retired render's quiet tail even when it
        # retired before this executor answer became ready.
        quiet_remaining = backend_render_quiet_until - time.monotonic()
        if quiet_remaining > 0:
            await asyncio.sleep(quiet_remaining)
        session.drain_audio_nowait()
        if generation != background_generation:
            return
        await end_output(after_tail=False)
        if generation != background_generation:
            return
        backend_render_generation = generation
        backend_render_context_acknowledged = False
        backend_render_started = False
        backend_render_response_id = None
        backend_render_turn_id = None
        backend_render_terminal_seen = False
        backend_render_cancel_requested = False
        retired = asyncio.get_running_loop().create_future()
        backend_render_retired = retired
        try:
            await run_control(
                session.append_speech(spoken),
                "background speech",
            )
        except BaseException:
            if backend_render_generation == generation:
                backend_render_generation = None
                backend_render_context_acknowledged = False
                backend_render_started = False
                backend_render_response_id = None
                backend_render_turn_id = None
                backend_render_terminal_seen = False
                backend_render_cancel_requested = False
                backend_render_retired = None
                if not retired.done():
                    retired.set_result(None)
            raise

    def clear_background_agent_messages() -> None:
        background_agent_messages.clear()
        background_agent_order.clear()

    def executor_event_turn_id(params: Mapping[str, Any]) -> str | None:
        direct = params.get("turnId")
        if isinstance(direct, str) and direct:
            return direct
        turn = params.get("turn")
        nested = turn.get("id") if isinstance(turn, Mapping) else None
        return nested if isinstance(nested, str) and nested else None

    def remember_background_agent_message(
        params: Mapping[str, Any], *, authoritative: bool
    ) -> None:
        if params.get("turnId") != active_background_turn_id:
            return
        item = params.get("item")
        item_values = item if isinstance(item, Mapping) else {}
        if authoritative and item_values.get("type") != "agentMessage":
            return
        raw_item_id = item_values.get("id") if authoritative else params.get("itemId")
        item_id = (
            raw_item_id if isinstance(raw_item_id, str) and raw_item_id else "turn"
        )
        if item_id not in background_agent_messages:
            background_agent_order.append(item_id)
            background_agent_messages[item_id] = (None, "")
        previous_phase, previous_text = background_agent_messages[item_id]
        if authoritative:
            phase_value = item_values.get("phase")
            phase = phase_value if isinstance(phase_value, str) else None
            text_value = item_values.get("text")
            text = text_value if isinstance(text_value, str) else previous_text
            background_agent_messages[item_id] = (
                phase,
                text[:MAX_SYNTHESIS_TEXT_CHARS],
            )
            return
        delta = params.get("delta")
        if isinstance(delta, str) and delta:
            background_agent_messages[item_id] = (
                previous_phase,
                (previous_text + delta)[:MAX_SYNTHESIS_TEXT_CHARS],
            )

    def completed_background_text() -> str | None:
        candidates = [
            (phase, text.strip())
            for item_id in background_agent_order
            for phase, text in [background_agent_messages[item_id]]
            if text.strip()
        ]
        for phase, text in reversed(candidates):
            if phase == "final_answer":
                return text
        for phase, text in reversed(candidates):
            if phase != "commentary":
                return text
        return None

    async def reject_all_early_background_tool_calls() -> None:
        buffered = [
            event
            for events_for_turn in early_background_events.values()
            for event in events_for_turn
        ]
        early_background_events.clear()
        for event in buffered:
            if event.get("method") == "item/tool/call":
                await reject_unowned_tool_call(event)

    async def start_background_turn(text: str, generation: int) -> None:
        """Route one canonical request to the isolated Home Assistant executor."""
        nonlocal active_background_terminal, active_background_turn_generation
        nonlocal active_background_turn_had_tool, active_background_turn_id
        nonlocal active_background_turn_interrupting
        nonlocal background_turn_starting, queued_background_request
        assert executor_thread_id is not None
        async with background_turn_lock:
            if generation != background_generation:
                return
            if active_background_turn_id is not None:
                if active_background_turn_had_tool:
                    queued_background_request = (generation, text)
                    return
                await interrupt_active_background_turn_locked()
            if generation != background_generation:
                return
            clear_background_agent_messages()
            queued_background_request = None
            background_turn_starting = True
            early_background_events.clear()
            try:
                response = await state.rpc.call(
                    "turn/start",
                    {
                        "threadId": executor_thread_id,
                        "input": [{"type": "text", "text": text}],
                        "approvalPolicy": "never",
                        "cwd": state.runtime_cwd,
                        "effort": DEFAULT_CONVERSATION_EFFORT,
                    },
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
            except BaseException:
                background_turn_starting = False
                await reject_all_early_background_tool_calls()
                raise
            turn = response.get("turn")
            turn_id = turn.get("id") if isinstance(turn, Mapping) else None
            if not isinstance(turn_id, str) or not turn_id:
                background_turn_starting = False
                await reject_all_early_background_tool_calls()
                raise ProtocolError("turn/start returned an invalid realtime turn id")
            if generation != background_generation:
                background_turn_starting = False
                await reject_all_early_background_tool_calls()
                await state.rpc.call(
                    "turn/interrupt",
                    {"threadId": executor_thread_id, "turnId": turn_id},
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
                return
            active_background_turn_id = turn_id
            active_background_turn_generation = generation
            active_background_turn_had_tool = False
            active_background_turn_interrupting = False
            active_background_terminal = asyncio.get_running_loop().create_future()
            arm_background_turn_watchdog(
                turn_id,
                generation,
                active_background_terminal,
            )
            buffered = early_background_events.pop(turn_id, [])
            foreign = [
                event
                for events_for_turn in early_background_events.values()
                for event in events_for_turn
            ]
            early_background_events.clear()
            background_turn_starting = False
            for event in foreign:
                if event.get("method") == "item/tool/call":
                    await reject_unowned_tool_call(event)
            for event in buffered:
                await handle_executor_event(event)

    async def complete_background_turn(params: Mapping[str, Any]) -> None:
        nonlocal active_background_terminal, active_background_turn_generation
        nonlocal active_background_turn_had_tool, active_background_turn_id
        nonlocal active_background_turn_interrupting
        nonlocal queued_background_request
        turn = params.get("turn")
        turn_values = turn if isinstance(turn, Mapping) else {}
        turn_id = executor_event_turn_id(params)
        if turn_id != active_background_turn_id:
            return
        generation = active_background_turn_generation
        final_text = completed_background_text()
        status = turn_values.get("status", params.get("status"))
        terminal = active_background_terminal
        active_background_turn_id = None
        active_background_turn_generation = None
        active_background_turn_had_tool = False
        active_background_turn_interrupting = False
        active_background_terminal = None
        clear_background_agent_messages()
        if terminal is not None and not terminal.done():
            terminal.set_result(None)
        queued = queued_background_request
        queued_background_request = None
        if generation is not None and generation == background_generation:
            if status != "completed":
                raise ProtocolError("assistant failed to complete the request")
            if final_text is None:
                await send(
                    {"type": "error", "error": "assistant returned no final response"}
                )
            else:
                await append_background_speech(final_text, generation)
        if queued is not None and queued[0] == background_generation:
            await start_background_turn(queued[1], queued[0])

    async def handle_executor_event(event: Mapping[str, Any]) -> None:
        """Reduce one already-filtered executor event into the active turn."""
        nonlocal active_background_turn_had_tool
        method = event.get("method")
        raw_params = event.get("params")
        params = raw_params if isinstance(raw_params, Mapping) else {}
        if method == "item/tool/call" and "id" in event:
            if (
                params.get("turnId") != active_background_turn_id
                or active_background_turn_interrupting
                or active_background_turn_generation != background_generation
            ):
                await reject_unowned_tool_call(event)
                return
            active_background_turn_had_tool = True
            start_home_assistant_tool_call(event)
            return
        if method == "item/agentMessage/delta":
            remember_background_agent_message(params, authoritative=False)
            return
        if method == "item/completed":
            remember_background_agent_message(params, authoritative=True)
            return
        if method == "turn/completed":
            await complete_background_turn(params)

    async def remember_managed_user_turn(turn_id: str) -> None:
        """Bind a raw provider user turn to the current speech generation."""
        nonlocal input_speech_active, pending_managed_speech_generation
        nonlocal user_transcript_handled
        if not claim_managed_turn_id(turn_id):
            return
        generation = pending_managed_speech_generation
        if generation is not None and generation == background_generation:
            pending_managed_speech_generation = None
        else:
            interrupted_owner = active_background_turn_owner()
            generation = begin_background_generation()
            input_speech_active = True
            user_transcript_handled = False
            request_backend_render_cancel_best_effort()
            await end_output(after_tail=False)
            await interrupt_active_background_turn(interrupted_owner)
        managed_user_turn_generations[turn_id] = generation
        managed_user_turn_generations.move_to_end(turn_id)
        while len(managed_user_turn_generations) > MAX_REALTIME_USER_TURN_CORRELATIONS:
            managed_user_turn_generations.popitem(last=False)

    async def complete_managed_user_turn(control: RealtimeDataControl) -> None:
        """Dispatch only a transcript correlated to the current raw user turn."""
        nonlocal input_speech_active, user_transcript_handled
        turn_id = control.turn_id
        generation = (
            managed_user_turn_generations.pop(turn_id, None)
            if turn_id is not None
            else None
        )
        if (
            generation is None
            or generation != background_generation
            or user_transcript_handled
        ):
            return
        user_transcript_handled = True
        input_speech_active = False
        text = control.transcript
        if not isinstance(text, str) or not text.strip():
            return
        if len(text) > MAX_HISTORY_CONTEXT_CHARS:
            await send(
                {
                    "type": "error",
                    "error": "realtime transcript exceeds its size limit",
                }
            )
            return
        request_backend_render_cancel_best_effort()
        await end_output(after_tail=False)
        await run_control(
            start_background_turn(text.strip(), generation),
            "background turn",
        )

    async def handle_managed_user_control(control: RealtimeDataControl) -> None:
        if control.event_type == "turn.created":
            if control.turn_id is not None:
                await remember_managed_user_turn(control.turn_id)
            return
        if (
            control.turn_id is not None
            and control.turn_id not in managed_user_turn_generations
        ):
            # Tombstone a terminal that arrived without an owned user start.
            # A later assistant start must not be able to reuse this ID and
            # steal the bridge-owned render slot.
            claim_managed_turn_id(control.turn_id)
            return
        await complete_managed_user_turn(control)

    async def receive() -> None:
        nonlocal input_speech_active, pending_managed_speech_generation
        nonlocal user_transcript_handled
        while not stop.is_set():
            message = await _receive_realtime_message(
                websocket, allow_binary=wire_protocol.uses_binary_audio
            )
            if isinstance(message, bytes):
                assert input_resampler is not None
                converted = input_resampler.feed(message)
                if converted:
                    session.feed_audio(converted)
                continue
            message_type = message.get("type")
            if message_type == "audio":
                if wire_protocol.uses_binary_audio:
                    raise ProtocolError(
                        "protocol_version 2 requires binary PCM16 audio frames"
                    )
                data = decode_base64_audio(message.get("audio", message.get("data")))
                sample_rate = _positive_int(
                    message.get("sample_rate", REALTIME_SAMPLE_RATE), "sample_rate"
                )
                channels = _positive_int(message.get("channels", 1), "channels")
                session.feed_audio(pcm16_mono_24khz(data, sample_rate, channels))
            elif message_type == "text":
                text = message.get("text")
                if not isinstance(text, str) or not text:
                    raise ProtocolError("text must be a non-empty string")
                role = str(message.get("role", "user"))
                if wire_protocol.uses_binary_audio and (
                    role != "user" or len(text) > MAX_HISTORY_CONTEXT_CHARS
                ):
                    raise ProtocolError(
                        "protocol_version 2 text must be a bounded user message"
                    )
                if bridge_managed_realtime:
                    generation = begin_background_generation()
                    input_speech_active = False
                    pending_managed_speech_generation = None
                    user_transcript_handled = True
                    request_backend_render_cancel_best_effort()
                    await end_output(after_tail=False)
                    await run_control(
                        start_background_turn(text, generation),
                        "background turn",
                    )
                else:
                    await run_control(
                        session.append_text(text, role),
                        "text control",
                    )
            elif message_type == "speech":
                if wire_protocol.requests_native_conversation:
                    raise ProtocolError("native realtime does not accept device speech")
                text = message.get("text")
                if not isinstance(text, str) or not text:
                    raise ProtocolError("speech text must be a non-empty string")
                if bridge_managed_realtime:
                    raise ProtocolError(
                        "broker-managed realtime does not accept device speech"
                    )
                if (
                    wire_protocol.uses_binary_audio
                    and len(text) > MAX_SYNTHESIS_TEXT_CHARS
                ):
                    raise ProtocolError(
                        "protocol_version 2 speech text exceeds its size limit"
                    )
                await run_control(session.append_speech(text), "speech control")
            elif message_type == "tool_result":
                if wire_protocol.uses_binary_audio:
                    raise ProtocolError(
                        "protocol_version 2 does not accept tool results"
                    )
                await _respond_to_tool_result(
                    state.rpc,
                    message,
                    tool_requests,
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
            elif message_type == "interrupt":
                if bridge_managed_realtime:
                    interrupted_owner = active_background_turn_owner()
                    begin_background_generation()
                    input_speech_active = False
                    pending_managed_speech_generation = None
                    user_transcript_handled = True
                    request_backend_render_cancel_best_effort()
                    await end_output(after_tail=False)
                    await run_control(
                        interrupt_active_background_turn(interrupted_owner),
                        "background interrupt",
                    )
                    if not managed_interrupt_continuation:
                        await send(
                            {
                                "type": "stopped",
                                "reason": "interrupt",
                                "fresh_session_required": True,
                                "remote_cancelled": False,
                            }
                        )
                        stop.set()
                        return
                    await send(
                        {
                            "type": "stopped",
                            "reason": "interrupt",
                            "fresh_session_required": False,
                            "remote_cancelled": False,
                            "continuation_safe": True,
                        }
                    )
                    continue
                if wire_protocol.uses_binary_audio:
                    await end_output(after_tail=False)
                    remote_cancelled = await request_remote_response_cancel()
                    if remote_cancelled:
                        await send(
                            {
                                "type": "stopped",
                                "reason": "interrupt",
                                "fresh_session_required": False,
                                "remote_cancelled": True,
                            }
                        )
                        continue
                await send(
                    {
                        "type": "stopped",
                        "reason": "interrupt",
                        "fresh_session_required": True,
                        "remote_cancelled": False,
                    }
                )
                stop.set()
                return
            elif message_type == "stop":
                stop.set()
                return
            elif message_type == "ping":
                await send({"type": "pong"})
            else:
                await send(
                    {
                        "type": "error",
                        "error": (
                            "unsupported realtime control"
                            if wire_protocol.uses_binary_audio
                            else f"unsupported message type: {message_type}"
                        ),
                    }
                )

    async def reject_unowned_tool_call(event: Mapping[str, Any]) -> None:
        """Answer an unowned App Server request without touching Home Assistant."""
        request_id = event.get("id")
        params = event.get("params")
        values = params if isinstance(params, Mapping) else {}
        if (
            not isinstance(request_id, (int, str))
            or request_id in claimed_tool_responses
        ):
            return
        call_id = str(values.get("callId", request_id))
        claimed_tool_responses.add(request_id)
        await _respond_to_tool_result(
            state.rpc,
            {
                "call_id": call_id,
                "success": False,
                "result": {
                    "error": "unowned_home_assistant_tool_call",
                    "do_not_retry": True,
                },
            },
            {call_id: request_id},
            timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
        )
        delivered_tool_responses.add(request_id)

    async def events() -> None:
        while not stop.is_set():
            event = await session.next_event()
            event_trace.app_event(event)
            method = event.get("method")
            raw_params = event.get("params")
            params = raw_params if isinstance(raw_params, Mapping) else {}
            if method == "item/tool/call" and "id" in event:
                if wire_protocol.uses_binary_audio:
                    if bridge_managed_realtime:
                        await reject_unowned_tool_call(event)
                    else:
                        start_home_assistant_tool_call(event)
                    continue
                call_id = str(params.get("callId", event["id"]))
                tool_requests[call_id] = event["id"]
                await send(
                    {
                        "type": "tool_call",
                        "request_id": event["id"],
                        "call_id": call_id,
                        "name": params.get("tool"),
                        "namespace": params.get("namespace"),
                        "arguments": params.get("arguments", {}),
                    }
                )
            elif method == "thread/realtime/transcript/delta":
                if wire_protocol.uses_binary_audio:
                    role = str(params.get("role", "")).lower()
                    if (
                        role in {"assistant", "output"}
                        and not bridge_managed_realtime
                        and pending_tool_continuation_correlation is None
                    ):
                        announce_tool_continuation_output()
                        await arm_output()
                    continue
                await send(
                    {
                        "type": "transcript_delta",
                        "role": params.get("role"),
                        "delta": params.get("delta", ""),
                    }
                )
            elif method == "thread/realtime/transcript/done":
                if wire_protocol.uses_binary_audio:
                    role = str(params.get("role", "")).lower()
                    if role == "user" and bridge_managed_realtime:
                        continue
                    if role in {"assistant", "output"} and not bridge_managed_realtime:
                        await end_output(after_tail=True)
                    continue
                await send(
                    {
                        "type": "transcript_done",
                        "role": params.get("role"),
                        "text": params.get("text", ""),
                    }
                )
            elif method == "thread/realtime/itemAdded":
                if wire_protocol.uses_binary_audio:
                    continue
                await send({"type": "item", "item": params.get("item")})
            elif method == "thread/realtime/error":
                cancel_tool_continuation_watchdog()
                await send(
                    {
                        "type": "error",
                        "error": (
                            "realtime provider error"
                            if wire_protocol.uses_binary_audio
                            else params.get("message", "realtime error")
                        ),
                    }
                )
            elif method == "thread/realtime/closed":
                cancel_tool_continuation_watchdog()
                if wire_protocol.uses_binary_audio:
                    await end_output(after_tail=False)
                    await send({"type": "stopped", "reason": "remote_closed"})
                else:
                    await send({"type": "stopped", "reason": params.get("reason")})
                stop.set()
                return
            elif method not in {"thread/realtime/started", "thread/realtime/sdp"}:
                if wire_protocol.uses_binary_audio:
                    continue
                await send({"type": "event", "method": method, "params": params})

    async def executor_events() -> None:
        """Consume only events from the isolated, tool-bearing executor thread."""
        if executor_subscription is None or executor_thread_id is None:
            await asyncio.Future()
            return
        # Teardown keeps this consumer alive after ``stop`` is set so it can
        # reject late tools and observe the forced executor terminal. The
        # socket owner cancels it only after that bounded boundary.
        while True:
            event = await executor_subscription.get()
            method = event.get("method")
            if method == "bridge/appServerExited":
                params = event.get("params")
                values = params if isinstance(params, Mapping) else {}
                raise AppServerExited(
                    f"Codex App Server exited with status {values.get('returncode')}"
                )
            raw_params = event.get("params")
            params = raw_params if isinstance(raw_params, Mapping) else {}
            if params.get("threadId") != executor_thread_id:
                continue
            event_trace.app_event(event)
            turn_id = executor_event_turn_id(params)
            if (
                background_turn_starting
                and active_background_turn_id is None
                and turn_id is not None
            ):
                buffered_count = sum(map(len, early_background_events.values()))
                if buffered_count < MAX_EARLY_TURN_EVENTS:
                    early_background_events.setdefault(turn_id, []).append(dict(event))
                else:
                    if method == "item/tool/call":
                        await reject_unowned_tool_call(event)
                    if tool_call_failures.empty():
                        tool_call_failures.put_nowait(
                            ProtocolError(
                                "realtime executor exceeded the early-event limit"
                            )
                        )
                continue
            await handle_executor_event(event)

    async def audio() -> None:
        nonlocal output_last_pcm_at
        while not stop.is_set():
            chunk = await session.recv_audio()
            if not wire_protocol.uses_binary_audio:
                await send_audio(chunk)
                continue
            expected_backend_generation = (
                backend_output_generation if bridge_managed_realtime else None
            )
            if bridge_managed_realtime and (
                expected_backend_generation is None
                or expected_backend_generation != background_generation
            ):
                # Media has no bridge generation identifier. Never buffer
                # pre-ack PCM: after barge-in it could belong to a retired
                # render and later be mistaken for the next response.
                continue
            delivered_output = False
            async with output_state_lock:
                if bridge_managed_realtime and (
                    expected_backend_generation is None
                    or backend_output_generation != expected_backend_generation
                    or background_generation != expected_backend_generation
                ):
                    continue
                if output_speaking:
                    output_last_pcm_at = time.monotonic()
                    sent = await send_audio(chunk, expected_backend_generation)
                    delivered_output = sent and _realtime_pcm_has_signal(chunk)
                else:
                    quarantine_output(chunk)
                    if output_armed and output_preroll:
                        await begin_output_locked(expected_backend_generation)
                        delivered_output = output_speaking
            if delivered_output:
                complete_tool_continuation_after_output()

    async def handle_managed_backend_terminal(
        control: RealtimeDataControl, *, cancelled: bool
    ) -> None:
        """Apply a provider terminal only after render-identity validation."""
        nonlocal active_response_id, backend_output_generation
        if not backend_render_terminal_matches(control.response_id, control.turn_id):
            return
        owned_output = bridge_output_authorized()
        complete_tool_continuation_after_terminal(
            control.event_type, control.response_id
        )
        await end_output(after_tail=owned_output and not cancelled)
        if control.response_id == active_response_id:
            active_response_id = None
        if owned_output:
            backend_output_generation = None
        mark_backend_render_terminal(control.response_id, control.turn_id)
        if owned_output and not cancelled:
            await send(control.wire_value())

    def control_starts_output(control: RealtimeDataControl) -> bool:
        return control.event_type in {
            "output_audio_buffer.started",
            "response.created",
        } or (
            control.event_type == "turn.created"
            and control.role in {"assistant", "output"}
        )

    def control_ends_output(control: RealtimeDataControl) -> bool:
        return control.event_type in {
            "output_audio_buffer.stopped",
            "response.done",
        } or (
            control.event_type == "turn.done"
            and (
                control.role in {"assistant", "output"}
                or (not bridge_managed_realtime and output_speaking)
            )
        )

    async def handle_cancelled_response(control: RealtimeDataControl) -> None:
        nonlocal active_response_id
        if bridge_managed_realtime:
            return
        complete_tool_continuation_after_terminal(
            control.event_type, control.response_id
        )
        await end_output(after_tail=False)
        waiter = pending_cancel_confirmation
        if (
            waiter is not None
            and not waiter.done()
            and pending_cancel_response_id is not None
            and control.response_id == pending_cancel_response_id
        ):
            waiter.set_result(None)
        if control.response_id == active_response_id:
            active_response_id = None
        await send(control.wire_value())

    async def data_events() -> None:
        nonlocal active_response_id, backend_output_generation
        nonlocal backend_render_context_acknowledged, input_speech_active
        nonlocal pending_managed_speech_generation
        nonlocal user_transcript_handled
        while not stop.is_set():
            raw_event = await session.recv_data_event()
            event_trace.data_event(raw_event)
            control = parse_data_control_event(raw_event)
            if control is None or not wire_protocol.uses_binary_audio:
                continue
            if control.event_type == "session.context.appended":
                if bridge_managed_realtime and backend_render_generation is not None:
                    backend_render_context_acknowledged = True
                    maybe_retire_backend_render()
                continue
            if (
                bridge_managed_realtime
                and control.role == "user"
                and control.event_type in {"turn.created", "turn.done"}
            ):
                await handle_managed_user_control(control)
                continue
            if control.event_type == "input_audio_buffer.speech_started":
                managed_barge_started = False
                interrupted_owner: tuple[str | None, int | None] | None = None
                if (
                    bridge_managed_realtime
                    and pending_managed_speech_generation is None
                ):
                    interrupted_owner = active_background_turn_owner()
                    pending_managed_speech_generation = begin_background_generation()
                    input_speech_active = True
                    user_transcript_handled = False
                    request_backend_render_cancel_best_effort()
                    managed_barge_started = True
                # Commit the generation tombstone above before either socket
                # or output cleanup can yield to a late executor tool event.
                await send(control.wire_value())
                await end_output(after_tail=False)
                if managed_barge_started and interrupted_owner is not None:
                    await interrupt_active_background_turn(interrupted_owner)
                continue
            if control.response_cancelled:
                await handle_cancelled_response(control)
                continue
            if control_starts_output(control):
                if bridge_managed_realtime:
                    if control.event_type != "turn.created" or control.turn_id is None:
                        continue
                    if not claim_managed_turn_id(control.turn_id):
                        continue
                    if not mark_backend_render_started(turn_id=control.turn_id):
                        continue
                    render_generation = backend_render_generation
                    if (
                        render_generation is None
                        or render_generation != background_generation
                    ):
                        request_backend_render_cancel_best_effort()
                        await end_output(after_tail=False)
                        continue
                    await authorize_backend_output(render_generation)
                    await send(control.wire_value())
                    continue
                if control.event_type == "response.created":
                    active_response_id = control.response_id
                announce_tool_continuation_output(control.response_id)
                await arm_output()
                await send(control.wire_value())
                continue
            if control_ends_output(control):
                if bridge_managed_realtime:
                    if control.event_type != "turn.done" or control.turn_id is None:
                        continue
                    if not backend_render_terminal_matches(turn_id=control.turn_id):
                        claim_managed_turn_id(control.turn_id)
                        continue
                    await handle_managed_backend_terminal(control, cancelled=False)
                    continue
                if (
                    control.event_type == "response.done"
                    and control.response_id == active_response_id
                ):
                    active_response_id = None
                complete_tool_continuation_after_terminal(
                    control.event_type, control.response_id
                )
                await end_output(after_tail=True)
                await send(control.wire_value())
                continue
            await send(control.wire_value())

    def tombstone_background_for_shutdown() -> tuple[str | None, int | None]:
        """Reject late executor work before teardown yields to cleanup."""
        nonlocal active_background_turn_interrupting, queued_background_request
        owner = active_background_turn_owner()
        queued_background_request = None
        if owner[0] is not None:
            active_background_turn_interrupting = True
        # A queued request can already be inside ``start_background_turn``
        # after its previous owner cleared but before turn/start returned.
        # Tombstone every captured generation, including that ownerless gap.
        begin_background_generation()
        return owner

    async def interrupt_background_for_shutdown(
        expected_owner: tuple[str | None, int | None],
    ) -> None:
        """Bound termination while the executor event consumer is still alive."""
        turn_id, _generation = expected_owner
        if executor_thread_id is None:
            return
        # Always acquire the lock, even without a captured turn id. This waits
        # for an in-flight stale turn/start path to interrupt the turn it just
        # created before the event consumer is cancelled and threads deleted.
        async with background_turn_lock:
            if turn_id is None:
                return
            if active_background_turn_owner() != expected_owner:
                return
            terminal = active_background_terminal
            if terminal is not None and terminal.done():
                return
            try:
                await state.rpc.call(
                    "turn/interrupt",
                    {"threadId": executor_thread_id, "turnId": turn_id},
                    timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                )
            except Exception:  # noqa: BLE001 - teardown remains bounded.
                LOGGER.warning("Could not interrupt realtime executor during teardown")
            if terminal is not None and not terminal.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(terminal),
                        timeout=REALTIME_CONTROL_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    LOGGER.warning(
                        "Realtime executor did not terminate before teardown"
                    )

    tasks = {
        asyncio.create_task(receive(), name="codex-realtime-receiver"),
        asyncio.create_task(events(), name="codex-realtime-events"),
        asyncio.create_task(audio(), name="codex-realtime-audio"),
        asyncio.create_task(data_events(), name="codex-realtime-data-events"),
        asyncio.create_task(
            raise_tool_call_failure(), name="codex-realtime-tool-failure"
        ),
    }
    executor_event_task: asyncio.Task[None] | None = None
    if executor_subscription is not None:
        executor_event_task = asyncio.create_task(
            executor_events(), name="codex-realtime-executor-events"
        )
        tasks.add(executor_event_task)
    try:
        while not stop.is_set():
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exception = task.exception()
                if exception is not None:
                    raise exception
                stop.set()
    finally:
        stop.set()
        shutdown_background_owner = tombstone_background_for_shutdown()
        auxiliary_tasks = tuple(output_aux_tasks)
        background_timeouts = tuple(background_watchdog_tasks)
        pending_tool_calls = tuple(tool_call_tasks)
        for task in auxiliary_tasks:
            task.cancel()
        for task in background_timeouts:
            task.cancel()
        for request_id, (_call_id, task) in tuple(active_tool_calls.items()):
            if request_id not in claimed_tool_responses:
                task.cancel()
        other_tasks = tuple(task for task in tasks if task is not executor_event_task)
        for task in other_tasks:
            task.cancel()
        await asyncio.gather(
            *other_tasks,
            *auxiliary_tasks,
            *background_timeouts,
            *pending_tool_calls,
            return_exceptions=True,
        )
        unresolved = [
            (request_id, call_id)
            for request_id, (call_id, _task) in active_tool_calls.items()
            if request_id not in claimed_tool_responses
        ]
        if unresolved:
            cleanup_responses = [
                respond_to_provider_tool_once(
                    request_id,
                    call_id,
                    success=False,
                    result={
                        "error": "home_assistant_tool_outcome_unknown",
                        "do_not_retry": True,
                    },
                )
                for request_id, call_id in unresolved
            ]
            try:
                async with asyncio.timeout(REALTIME_CONTROL_TIMEOUT_SECONDS):
                    await asyncio.gather(*cleanup_responses, return_exceptions=True)
            except TimeoutError:
                LOGGER.warning(
                    "Timed out closing realtime Home Assistant tool requests"
                )
        await interrupt_background_for_shutdown(shutdown_background_owner)
        if executor_event_task is not None:
            executor_event_task.cancel()
            await asyncio.gather(executor_event_task, return_exceptions=True)
        if executor_subscription is not None:
            executor_subscription.close()


async def _drain_transcription_audio(
    session: RealtimeSession,
    *,
    strict_handoff_boundary: bool = False,
    handoff_boundary_state: _SpeechHandoffBoundaryState | None = None,
) -> None:
    """Discard unwanted model audio without hiding unsafe handoff output."""
    while True:
        chunk = await session.recv_audio()
        if not isinstance(chunk, bytes):
            raise ProtocolError("realtime transcription received invalid audio")
        if not chunk or not strict_handoff_boundary:
            continue
        if handoff_boundary_state is None:
            raise ProtocolError("retained speech session produced assistant audio")
        handoff_boundary_state.invalidated = True


async def _retire_transcription_audio_drain(
    task: asyncio.Task[None],
    *,
    handoff_boundary_state: _SpeechHandoffBoundaryState | None,
) -> None:
    """Stop a transcription audio receiver without offering a failed handoff."""
    if not task.done():
        task.cancel()
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    if handoff_boundary_state is None or isinstance(result, asyncio.CancelledError):
        return
    # The drain loop has no normal return. A transport failure (or an
    # unexpected normal exit) makes the retained session unusable while the
    # already-completed transcript remains valid.
    handoff_boundary_state.invalidated = True


async def _wait_for_user_transcript(  # noqa: C901 - realtime event streams
    session: RealtimeSession,
    timeout: float,
    *,
    fragment_finalization_at: float | None = None,
    strict_handoff_boundary: bool = False,
    handoff_boundary_state: _SpeechHandoffBoundaryState | None = None,
    input_drain_task: asyncio.Task[Any] | None = None,
    audio_drain_task: asyncio.Task[None] | None = None,
    live_fragment_quiet_seconds: float | None = None,
    completion_diagnostics: dict[str, float | str] | None = None,
) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    deltas: list[str] = []
    data_deltas: OrderedDict[str, str] = OrderedDict()
    last_fragment_at: float | None = None
    realtime_closed_at: float | None = None
    drained_at: float | None = None
    drain_observed = input_drain_task is None
    owns_audio_drain = audio_drain_task is None
    if audio_drain_task is None:
        audio_drain_task = asyncio.create_task(
            _drain_transcription_audio(
                session,
                strict_handoff_boundary=strict_handoff_boundary,
                handoff_boundary_state=handoff_boundary_state,
            ),
            name="codex-transcription-audio-drain",
        )
    event_task = asyncio.create_task(session.next_event())
    data_task = asyncio.create_task(session.recv_data_event())
    try:
        while True:
            now = loop.time()
            remaining = deadline - now
            if remaining <= 0:
                raise TimeoutError
            transcript = _assembled_transcript(deltas, list(data_deltas.values()))
            if transcript and last_fragment_at is not None:
                use_live_guard = (
                    live_fragment_quiet_seconds is not None and drained_at is not None
                )
                quiet_seconds = (
                    live_fragment_quiet_seconds
                    if use_live_guard
                    else TRANSCRIPTION_FRAGMENT_QUIET_SECONDS
                )
                assert quiet_seconds is not None
                fragment_ready_at = last_fragment_at + quiet_seconds
                if fragment_finalization_at is not None and not use_live_guard:
                    fragment_ready_at = max(fragment_ready_at, fragment_finalization_at)
                if use_live_guard:
                    assert drained_at is not None
                    fragment_ready_at = max(
                        fragment_ready_at, drained_at + quiet_seconds
                    )
                quiet_remaining = fragment_ready_at - now
                if quiet_remaining <= 0:
                    if audio_drain_task.done():
                        await audio_drain_task
                        raise ProtocolError(
                            "realtime transcription audio drain stopped"
                        )
                    final_transcript = transcript.strip()
                    _remember_speech_handoff_input(
                        handoff_boundary_state,
                        final_transcript,
                    )
                    if completion_diagnostics is not None:
                        completion_diagnostics["reason"] = "fragment_quiet"
                        if drained_at is not None:
                            completion_diagnostics["drain_to_result_seconds"] = (
                                loop.time() - drained_at
                            )
                    return final_transcript
                remaining = min(remaining, quiet_remaining)
            elif realtime_closed_at is not None:
                close_remaining = TRANSCRIPTION_FRAGMENT_QUIET_SECONDS - (
                    now - realtime_closed_at
                )
                if close_remaining <= 0:
                    raise TimeoutError(
                        "realtime session closed before transcription completed"
                    )
                remaining = min(remaining, close_remaining)
            wait_tasks: set[asyncio.Task[Any]] = {
                event_task,
                data_task,
                audio_drain_task,
            }
            if input_drain_task is not None and not drain_observed:
                wait_tasks.add(input_drain_task)
            done, _ = await asyncio.wait(
                wait_tasks,
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                now = loop.time()
                transcript = _assembled_transcript(deltas, list(data_deltas.values()))
                if transcript and last_fragment_at is not None:
                    use_live_guard = (
                        live_fragment_quiet_seconds is not None
                        and drained_at is not None
                    )
                    quiet_seconds = (
                        live_fragment_quiet_seconds
                        if use_live_guard
                        else TRANSCRIPTION_FRAGMENT_QUIET_SECONDS
                    )
                    assert quiet_seconds is not None
                    fragment_ready_at = last_fragment_at + quiet_seconds
                    if fragment_finalization_at is not None and not use_live_guard:
                        fragment_ready_at = max(
                            fragment_ready_at, fragment_finalization_at
                        )
                    if use_live_guard:
                        assert drained_at is not None
                        fragment_ready_at = max(
                            fragment_ready_at, drained_at + quiet_seconds
                        )
                    if now >= fragment_ready_at:
                        if audio_drain_task.done():
                            await audio_drain_task
                            raise ProtocolError(
                                "realtime transcription audio drain stopped"
                            )
                        final_transcript = transcript.strip()
                        _remember_speech_handoff_input(
                            handoff_boundary_state,
                            final_transcript,
                        )
                        if completion_diagnostics is not None:
                            completion_diagnostics["reason"] = "fragment_quiet"
                            if drained_at is not None:
                                completion_diagnostics["drain_to_result_seconds"] = (
                                    loop.time() - drained_at
                                )
                        return final_transcript
                if realtime_closed_at is not None:
                    raise TimeoutError(
                        "realtime session closed before transcription completed"
                    )
                raise TimeoutError
            # FIRST_COMPLETED can return while the sibling has completed in the
            # same loop turn. Inspect both before accepting a terminal transcript
            # so an unsafe simultaneous event cannot be discarded in ``finally``.
            ready = set(done)
            if event_task.done():
                ready.add(event_task)
            if data_task.done():
                ready.add(data_task)
            if audio_drain_task.done():
                ready.add(audio_drain_task)
            if (
                input_drain_task is not None
                and not drain_observed
                and input_drain_task.done()
            ):
                ready.add(input_drain_task)
            if (
                input_drain_task is not None
                and not drain_observed
                and input_drain_task in ready
            ):
                drain_observed = True
                if (
                    not input_drain_task.cancelled()
                    and input_drain_task.exception() is None
                ):
                    drained_at = loop.time()
            terminal_transcript: str | None = None
            if event_task in ready:
                event = event_task.result()
                if strict_handoff_boundary:
                    try:
                        _validate_speech_handoff_app_event(event)
                    except (AppServerExited, BridgeError, TimeoutError, ValueError):
                        if handoff_boundary_state is None:
                            raise
                        handoff_boundary_state.invalidated = True
                method = event.get("method")
                params = event.get("params", {})
                if method == "bridge/appServerExited":
                    returncode = (
                        params.get("returncode")
                        if isinstance(params, Mapping)
                        else None
                    )
                    raise AppServerExited(
                        f"codex app-server exited with status {returncode}"
                    )
                role = str(params.get("role", "")).lower()
                if method == "thread/realtime/error":
                    raise ProtocolError(
                        str(params.get("message", "realtime transcription failed"))
                    )
                if method == "thread/realtime/transcript/delta" and role in {
                    "user",
                    "input",
                }:
                    fragment = params.get("delta")
                    if isinstance(fragment, str) and fragment:
                        deltas.append(fragment)
                        last_fragment_at = loop.time()
                if method == "thread/realtime/transcript/done" and role in {
                    "user",
                    "input",
                }:
                    text = params.get("text")
                    transcript = (
                        text
                        if isinstance(text, str) and text.strip()
                        else "".join(deltas)
                    )
                    if transcript.strip():
                        terminal_transcript = transcript.strip()
                if method == "thread/realtime/itemAdded":
                    transcript = _realtime_item_user_transcript(params.get("item"))
                    if transcript:
                        terminal_transcript = transcript
                if method == "thread/realtime/closed":
                    realtime_closed_at = loop.time()
                event_task = asyncio.create_task(session.next_event())
            if data_task in ready:
                data_event = data_task.result()
                if isinstance(data_event, bytes):
                    data_event = data_event.decode(errors="replace")
                try:
                    decoded_event = json.loads(data_event)
                except (json.JSONDecodeError, TypeError):  # fmt: skip
                    decoded_event = {}
                if not isinstance(decoded_event, Mapping):
                    decoded_event = {}
                event_type = decoded_event.get("type")
                if strict_handoff_boundary:
                    known_input = _assembled_transcript(
                        deltas, list(data_deltas.values())
                    )
                    if event_type == "turn.delta":
                        delta = decoded_event.get("delta")
                        LOGGER.debug(
                            "Retained speech turn.delta relation: "
                            "delta_is_string=%s matches_known_input=%s",
                            isinstance(delta, str),
                            bool(
                                isinstance(delta, str)
                                and delta
                                and known_input
                                and (delta in known_input or known_input in delta)
                            ),
                        )
                    try:
                        _validate_speech_handoff_data_event(
                            data_event,
                            boundary_state=handoff_boundary_state,
                            known_input=known_input,
                        )
                    except (AppServerExited, BridgeError, TimeoutError, ValueError):
                        if handoff_boundary_state is None:
                            raise
                        handoff_boundary_state.invalidated = True
                candidate = _data_channel_transcript(decoded_event)
                if event_type == "input_transcript.added" and candidate:
                    item = decoded_event.get("item")
                    item_id = item.get("id") if isinstance(item, Mapping) else None
                    fragment_id = (
                        item_id
                        if isinstance(item_id, str) and item_id
                        else f"anonymous-{len(data_deltas)}"
                    )
                    data_deltas[fragment_id] = candidate
                    last_fragment_at = loop.time()
                elif candidate:
                    terminal_transcript = candidate
                elif event_type == "turn.done":
                    transcript = _assembled_transcript(
                        deltas, list(data_deltas.values())
                    )
                    if transcript:
                        terminal_transcript = transcript
                data_task = asyncio.create_task(session.recv_data_event())
            if audio_drain_task in ready:
                await audio_drain_task
                raise ProtocolError("realtime transcription audio drain stopped")
            if terminal_transcript:
                _remember_speech_handoff_input(
                    handoff_boundary_state,
                    terminal_transcript,
                )
                if completion_diagnostics is not None:
                    completion_diagnostics["reason"] = "terminal_event"
                    if drained_at is not None:
                        completion_diagnostics["drain_to_result_seconds"] = (
                            loop.time() - drained_at
                        )
                return terminal_transcript
    finally:
        event_task.cancel()
        data_task.cancel()
        cleanup_tasks: list[asyncio.Task[Any]] = [event_task, data_task]
        if owns_audio_drain:
            audio_drain_task.cancel()
            cleanup_tasks.append(audio_drain_task)
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)


def _remember_speech_handoff_input(
    boundary_state: _SpeechHandoffBoundaryState | None,
    transcript: str,
) -> None:
    """Retain one bounded input transcript only for same-session validation."""
    if boundary_state is None:
        return
    if not transcript or len(transcript) > 64_000:
        raise ProtocolError("retained speech input transcript is invalid")
    boundary_state.authoritative_input = transcript


def _data_channel_transcript(event: Mapping[str, Any]) -> str:
    """Extract a user transcript from known realtime v3 data events."""
    event_type = event.get("type")
    if event_type == "input_transcript.added":
        item = event.get("item")
        value = (
            item.get("text")
            if isinstance(item, Mapping) and item.get("type") == "input_transcript"
            else event.get("text")
        )
        return value if isinstance(value, str) and value.strip() else ""
    if event_type == "delegation.created":
        value = event.get("input_transcript")
        if isinstance(value, str) and value.strip():
            return value.strip()
        item = event.get("item")
        if isinstance(item, Mapping) and item.get("type") == "delegation":
            content = item.get("content")
            if isinstance(content, list):
                transcript = "".join(
                    part["text"]
                    for part in content
                    if isinstance(part, Mapping)
                    and part.get("type") == "input_text"
                    and isinstance(part.get("text"), str)
                ).strip()
                if transcript:
                    return transcript
        return ""
    if event_type != "turn.done":
        return ""
    turn = event.get("turn")
    event_role = str(event.get("role", "")).lower()
    turn_role = str(turn.get("role", "")).lower() if isinstance(turn, Mapping) else ""
    role = turn_role or event_role
    if role and role not in {"user", "input"}:
        return ""
    value = event.get("input_transcript")
    if isinstance(value, str) and value.strip():
        return value.strip()
    if role in {"user", "input"}:
        for key in ("transcript", "text"):
            value = event.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(turn, Mapping):
        for key in ("input_transcript",):
            value = turn.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if role in {"user", "input"}:
            for key in ("transcript", "text"):
                value = turn.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return ""


def _assembled_transcript(*fragment_groups: list[str]) -> str:
    """Reconcile normalized deltas with identity-preserving raw fragments."""
    normalized = "".join(fragment_groups[0]).strip() if fragment_groups else ""
    raw = "".join(fragment_groups[1]).strip() if len(fragment_groups) > 1 else ""
    if not raw:
        return normalized
    if not normalized or raw.startswith(normalized):
        return raw
    if normalized.startswith(raw):
        return normalized
    return raw


def _realtime_item_user_transcript(value: object) -> str:
    """Return the complete user transcript from a v3 handoff item."""
    if not isinstance(value, Mapping) or value.get("type") != "handoff_request":
        return ""
    transcript = value.get("input_transcript")
    if isinstance(transcript, str) and transcript.strip():
        return transcript.strip()
    active = value.get("active_transcript")
    if isinstance(active, list):
        for entry in reversed(active):
            if not isinstance(entry, Mapping):
                continue
            if str(entry.get("role", "")).lower() not in {"user", "input"}:
                continue
            text = entry.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


async def _collect_speech_audio(
    session: RealtimeSession,
    timeout: float,
    *,
    timing: _SynthesisCollectionTiming | None = None,
    async_handle_chunk: Callable[[bytes], Awaitable[None]] | None = None,
) -> bytes:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_audio_at: float | None = None
    transcript_done = False
    completion_at: float | None = None
    chunks: list[bytes] = []
    received_audio = False
    audio_task = asyncio.create_task(session.recv_audio())
    event_task = asyncio.create_task(session.next_event())
    data_task = asyncio.create_task(session.recv_data_event())
    try:
        while True:
            now = loop.time()
            if (
                completion_at is not None
                and now - completion_at >= SYNTHESIS_TAIL_GRACE_SECONDS
            ):
                break
            remaining = deadline - now
            if remaining <= 0:
                raise TimeoutError
            poll = min(
                remaining,
                0.5 if last_audio_at is not None else remaining,
                max(0.0, completion_at + SYNTHESIS_TAIL_GRACE_SECONDS - now)
                if completion_at is not None
                else remaining,
            )
            done, _ = await asyncio.wait(
                {audio_task, event_task, data_task},
                timeout=poll,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                if last_audio_at is None:
                    continue
                idle = loop.time() - last_audio_at
                if idle >= (0.6 if transcript_done else 1.5):
                    break
                continue
            audio_preceded_ready_batch = received_audio
            if audio_task in done:
                chunk = audio_task.result()
                if chunk:
                    received_audio = True
                    if async_handle_chunk is None:
                        chunks.append(chunk)
                    else:
                        await async_handle_chunk(chunk)
                    last_audio_at = loop.time()
                    if timing is not None:
                        audio_observed_at = time.monotonic()
                        if timing.first_audio_at is None:
                            timing.first_audio_at = audio_observed_at
                        timing.last_audio_at = audio_observed_at
                audio_task = asyncio.create_task(session.recv_audio())
            if event_task in done:
                event = event_task.result()
                method = event.get("method")
                params = event.get("params", {})
                role = str(params.get("role", "")).lower()
                if (
                    method == "thread/realtime/transcript/done"
                    and role
                    in {
                        "assistant",
                        "output",
                    }
                    and audio_preceded_ready_batch
                ):
                    transcript_done = True
                elif method == "thread/realtime/error":
                    raise ProtocolError(
                        str(params.get("message", "realtime synthesis failed"))
                    )
                elif method == "thread/realtime/closed" and not received_audio:
                    raise ProtocolError(
                        "realtime session closed before producing speech"
                    )
                event_task = asyncio.create_task(session.next_event())
            if data_task in done:
                data_event = data_task.result()
                if isinstance(data_event, bytes):
                    data_event = data_event.decode(errors="replace")
                try:
                    decoded_event = json.loads(data_event)
                except (json.JSONDecodeError, TypeError):  # fmt: skip
                    decoded_event = {}
                event_type = decoded_event.get("type")
                if audio_preceded_ready_batch and event_type in {
                    "turn.done",
                    "output_audio_buffer.stopped",
                }:
                    transcript_done = True
                    completion_at = loop.time()
                    if timing is not None:
                        timing.completion_at = time.monotonic()
                data_task = asyncio.create_task(session.recv_data_event())
    finally:
        if timing is not None:
            timing.ended_at = time.monotonic()
        audio_task.cancel()
        event_task.cancel()
        data_task.cancel()
        await asyncio.gather(audio_task, event_task, data_task, return_exceptions=True)
    if not received_audio:
        raise ProtocolError("realtime synthesis produced no audio")
    return b"".join(chunks)


async def _respond_to_tool_result(
    rpc: Any,
    message: Mapping[str, Any],
    tool_requests: MutableMapping[str, int | str],
    *,
    timeout: float | None = None,
) -> None:
    call_id = message.get("call_id", message.get("id"))
    normalized_call_id = str(call_id) if call_id is not None else None
    expected_request_id = (
        tool_requests.get(normalized_call_id)
        if normalized_call_id is not None
        else None
    )
    if not isinstance(expected_request_id, (int, str)):
        raise ProtocolError("tool_result does not match an active tool call")
    supplied_request_id = message.get("request_id")
    if supplied_request_id is not None and supplied_request_id != expected_request_id:
        raise ProtocolError("tool_result request_id does not match its tool call")
    success = bool(message.get("success", True))
    if isinstance(message.get("content_items"), list):
        content_items = message["content_items"]
    else:
        value = message.get("result", message.get("error", ""))
        text = (
            value
            if isinstance(value, str)
            else json.dumps(value, separators=(",", ":"))
        )
        content_items = [{"type": "inputText", "text": text}]
    response = rpc.respond_result(
        expected_request_id, {"contentItems": content_items, "success": success}
    )
    if timeout is None:
        await response
    else:
        try:
            async with asyncio.timeout(timeout):
                await response
        except TimeoutError as exc:
            raise ProtocolError("tool_result delivery timed out") from exc
    assert normalized_call_id is not None
    tool_requests.pop(normalized_call_id, None)


def normalize_dynamic_tools(value: object) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProtocolError("tools must be a list")
    normalized: list[dict[str, Any]] = []
    for tool in value:
        if not isinstance(tool, Mapping):
            raise ProtocolError("each tool must be an object")
        if tool.get("type") == "namespace":
            nested = normalize_dynamic_tools(list(tool.get("tools", [])))
            normalized.append(
                {
                    "type": "namespace",
                    "name": _required_string(tool, "name", "tool namespace"),
                    "description": str(tool.get("description", "")),
                    "tools": nested,
                }
            )
            continue
        raw_function = tool.get("function")
        function = cast(
            Mapping[str, Any],
            raw_function if isinstance(raw_function, Mapping) else tool,
        )
        name = _required_string(function, "name", "tool")
        schema = function.get(
            "inputSchema", function.get("parameters", {"type": "object"})
        )
        if not isinstance(schema, Mapping):
            raise ProtocolError(f"input schema for tool {name} must be an object")
        normalized.append(
            {
                "type": "function",
                "name": name,
                "description": str(function.get("description", "")),
                "inputSchema": dict(schema),
            }
        )
    return normalized


def _realtime_pcm_has_signal(chunk: bytes) -> bool:
    if not chunk or len(chunk) % 2:
        return False
    samples = array.array("h")
    samples.frombytes(chunk)
    if sys.byteorder != "little":
        samples.byteswap()
    return any(abs(sample) >= REALTIME_OUTPUT_SIGNAL_PEAK for sample in samples)


async def _read_json(request: web.Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, web.HTTPBadRequest) as exc:
        raise ProtocolError("request body must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("request body must be a JSON object")
    return payload


async def _receive_ws_json(
    websocket: web.WebSocketResponse,
    timeout: float | None = None,
) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        message = await websocket.receive()
    if message.type == WSMsgType.TEXT:
        try:
            value = json.loads(message.data)
        except json.JSONDecodeError as exc:
            raise ProtocolError("WebSocket message must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolError("WebSocket message must be a JSON object")
        return value
    if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED}:
        raise ProtocolError("WebSocket closed")
    if message.type == WSMsgType.ERROR:
        raise ProtocolError(f"WebSocket failed: {websocket.exception()}")
    raise ProtocolError("binary WebSocket messages are not supported")


async def _receive_realtime_message(
    websocket: web.WebSocketResponse,
    *,
    allow_binary: bool,
) -> dict[str, Any] | bytes:
    message = await websocket.receive()
    if message.type == WSMsgType.TEXT:
        try:
            value = json.loads(message.data)
        except json.JSONDecodeError as exc:
            raise ProtocolError("WebSocket message must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolError("WebSocket message must be a JSON object")
        return value
    if message.type == WSMsgType.BINARY:
        if not allow_binary:
            raise ProtocolError("binary realtime audio requires protocol_version 2")
        data = bytes(message.data)
        if not data:
            raise ProtocolError("binary realtime audio must not be empty")
        if len(data) > REALTIME_BINARY_FRAME_MAX_BYTES:
            raise ProtocolError(
                f"binary realtime audio frames must not exceed "
                f"{REALTIME_BINARY_FRAME_MAX_BYTES} bytes"
            )
        if len(data) % 2:
            raise ProtocolError("binary realtime PCM16 audio is not sample-aligned")
        return data
    if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED}:
        raise ProtocolError("WebSocket closed")
    if message.type == WSMsgType.ERROR:
        raise ProtocolError(f"WebSocket failed: {websocket.exception()}")
    raise ProtocolError("unsupported realtime WebSocket frame")


async def _send_realtime_json(
    websocket: web.WebSocketResponse,
    value: Mapping[str, Any],
    *,
    send_lock: asyncio.Lock | None = None,
) -> None:
    await _send_realtime_frame(
        websocket, dict(value), send_lock=send_lock, binary=False
    )


async def _send_realtime_binary(
    websocket: web.WebSocketResponse,
    value: bytes,
    *,
    send_lock: asyncio.Lock,
    guard: Callable[[], bool] | None = None,
) -> bool:
    return await _send_realtime_frame(
        websocket, value, send_lock=send_lock, binary=True, guard=guard
    )


async def _send_realtime_frame(
    websocket: web.WebSocketResponse,
    value: dict[str, Any] | bytes,
    *,
    send_lock: asyncio.Lock | None,
    binary: bool,
    guard: Callable[[], bool] | None = None,
) -> bool:
    async def send() -> None:
        if binary:
            assert isinstance(value, bytes)
            await websocket.send_bytes(value)
        else:
            assert isinstance(value, dict)
            await websocket.send_json(value)

    try:
        async with asyncio.timeout(REALTIME_WEBSOCKET_SEND_TIMEOUT_SECONDS):
            if send_lock is None:
                if guard is not None and not guard():
                    return False
                await send()
            else:
                async with send_lock:
                    if guard is not None and not guard():
                        return False
                    await send()
    except TimeoutError as exc:
        raise ProtocolError("realtime WebSocket send timed out") from exc
    except (ConnectionError, RuntimeError) as exc:
        raise ProtocolError("realtime WebSocket send failed") from exc
    else:
        return True


async def _safe_realtime_json(
    websocket: web.WebSocketResponse, value: Mapping[str, Any]
) -> None:
    if websocket.closed:
        return
    with contextlib.suppress(ProtocolError):
        await _send_realtime_json(websocket, value)


async def _safe_ws_json(
    websocket: web.WebSocketResponse, value: Mapping[str, Any]
) -> None:
    if websocket.closed:
        return
    with contextlib.suppress(ConnectionError, RuntimeError):
        await websocket.send_json(dict(value))


async def _unsubscribe_thread(rpc: Any, thread_id: str, *, timeout: float) -> None:
    with contextlib.suppress(Exception):
        await rpc.call("thread/unsubscribe", {"threadId": thread_id}, timeout=timeout)


def _turn_state_busy(turn_state: _ConversationTurnState) -> bool:
    """Return whether deleting the associated thread could race a turn."""
    return (
        turn_state.pending_owner is not None
        or turn_state.owner is not None
        or turn_state.turn_lock.locked()
    )


def _app_server_service_tier(value: object) -> str | None:
    """Validate the public tier name and map standard to App Server's reset."""
    if not isinstance(value, str) or value not in (
        SUPPORTED_CONVERSATION_SERVICE_TIERS
    ):
        raise ProtocolError("service_tier must be standard or priority")
    return "priority" if value == "priority" else None


def _conversation_language(payload: Mapping[str, Any]) -> str | None:
    """Validate and canonicalize the trusted Assist pipeline language."""
    if "language" not in payload:
        return None
    value = payload.get("language")
    if not isinstance(value, str) or not value:
        raise ProtocolError("language must be a non-empty BCP-47 language tag")
    if len(value) > MAX_CONVERSATION_LANGUAGE_CHARS:
        raise ProtocolError(
            f"language must not exceed {MAX_CONVERSATION_LANGUAGE_CHARS} characters"
        )

    parts = value.split("-")
    primary = parts[0]
    if (
        not 2 <= len(primary) <= 8
        or not primary.isascii()
        or not primary.isalpha()
        or any(
            not 1 <= len(part) <= 8 or not part.isascii() or not part.isalnum()
            for part in parts[1:]
        )
    ):
        raise ProtocolError("language must be a valid BCP-47 language tag")

    normalized = [primary.lower()]
    for part in parts[1:]:
        if len(part) == 4 and part.isalpha():
            normalized.append(part.title())
        elif len(part) == 2 and part.isalpha():
            normalized.append(part.upper())
        else:
            normalized.append(part.lower())
    return "-".join(normalized)


def _validate_started_thread(
    response: Mapping[str, Any], permission_profile: str
) -> None:
    """Require the least-privilege settings requested for a new thread."""
    active_profile = response.get("activePermissionProfile")
    if (
        not isinstance(active_profile, Mapping)
        or active_profile.get("id") != permission_profile
    ):
        raise ProtocolError(
            "thread/start did not activate the required permission profile"
        )
    sandbox = response.get("sandbox")
    if (
        not isinstance(sandbox, Mapping)
        or sandbox.get("type") != "readOnly"
        or sandbox.get("networkAccess") is not False
    ):
        raise ProtocolError(
            "thread/start did not activate the required read-only, network-denied sandbox"
        )
    if response.get("runtimeWorkspaceRoots") != []:
        raise ProtocolError("thread/start granted unexpected runtime workspace roots")


async def _dispose_thread(rpc: Any, thread_id: str) -> None:
    """Delete a private thread within one deadline, then unsubscribe if needed."""
    loop = asyncio.get_running_loop()
    disposal_deadline = loop.time() + THREAD_DISPOSAL_TOTAL_TIMEOUT_SECONDS
    delete_deadline = min(
        disposal_deadline,
        loop.time() + THREAD_DISPOSAL_DELETE_TIMEOUT_SECONDS,
    )
    try:
        async with asyncio.timeout_at(disposal_deadline):
            try:
                async with asyncio.timeout_at(delete_deadline):
                    await rpc.call(
                        "thread/delete",
                        {"threadId": thread_id},
                        timeout=max(0.0, delete_deadline - loop.time()),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - cleanup must not leak details
                LOGGER.warning("Could not delete finished Codex thread; using fallback")
                remaining = max(0.0, disposal_deadline - loop.time())
                if remaining:
                    await _unsubscribe_thread(rpc, thread_id, timeout=remaining)
    except TimeoutError:
        LOGGER.warning("Timed out disposing finished Codex thread")


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ProtocolError(f"{name} must be a positive integer")
    try:
        result: int = int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"{name} must be a positive integer") from exc
    if result <= 0:
        raise ProtocolError(f"{name} must be a positive integer")
    return result


def _required_string(value: Mapping[str, Any], key: str, label: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ProtocolError(f"{label} {key} must be a non-empty string")
    return result


def _turn_status(turn: object) -> object:
    return turn.get("status") if isinstance(turn, Mapping) else None


def _public_health(health: Mapping[str, Any]) -> dict[str, Any]:
    safe_keys = {
        "running",
        "initialized",
        "returncode",
        "version",
        "auth_mode",
        "plan_type",
    }
    return {key: health[key] for key in safe_keys if key in health}


def _conversation_history(payload: Mapping[str, Any], current_text: str) -> str | None:
    """Bound complete history records while preserving the legacy helper API."""
    return conversation_history(payload, current_text, budget=MAX_HISTORY_CONTEXT_CHARS)


def _current_user_text(payload: Mapping[str, Any]) -> str | None:
    direct = payload.get("text")
    if isinstance(direct, str) and direct.strip():
        return direct
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return None
    for message in reversed(messages):
        if not isinstance(message, Mapping):
            continue
        role = str(message.get("role", "")).lower()
        if role != "user":
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if not isinstance(part, Mapping):
                    continue
                part_text = part.get("text")
                if isinstance(part_text, str):
                    parts.append(part_text)
            text = "".join(parts)
            if text.strip():
                return text
    return None
