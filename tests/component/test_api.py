"""Tests for the Codex Voice bridge client."""

from __future__ import annotations

import asyncio
import base64
import io
import wave
from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime, time
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from homeassistant.helpers import chat_session

from custom_components.codex_voice import api as api_module
from custom_components.codex_voice.api import (
    BridgeAuthenticationError,
    BridgeBusyError,
    BridgeClient,
    BridgeConnectionError,
    BridgeProtocolError,
    BridgeQuotaError,
    BridgeStreamingUnsupported,
    BridgeToolCall,
)


async def test_duplicate_tool_call_is_never_executed_twice(
    aiohttp_client: Any, socket_enabled: None
) -> None:
    """T14: replayed call IDs cannot execute another side effect."""

    async def replay(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        call = {
            "type": "tool_call",
            "call_id": "same",
            "name": "HassTurnOn",
            "arguments": {},
        }
        await ws.send_json(call)
        await ws.receive_json()
        await ws.send_json(call)
        await ws.close()
        return ws

    app = web.Application()
    app.router.add_get("/v1/conversation", replay)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    tool = AsyncMock(return_value={"success": True})
    with pytest.raises(BridgeProtocolError, match="Duplicate tool"):
        await client.async_converse(
            {}, async_handle_delta=AsyncMock(), async_handle_tool=tool
        )
    tool.assert_awaited_once()


async def test_failed_terminal_is_not_reported_as_success(
    aiohttp_client: Any, socket_enabled: None
) -> None:
    """Failed provider completion cannot produce a successful response."""

    async def fail(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "done", "status": "failed", "text": "Unverified success"}
        )
        await ws.close()
        return ws

    app = web.Application()
    app.router.add_get("/v1/conversation", fail)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    delta = AsyncMock()
    with pytest.raises(BridgeProtocolError, match="did not complete"):
        await client.async_converse(
            {}, async_handle_delta=delta, async_handle_tool=AsyncMock()
        )
    delta.assert_not_awaited()


def _wav_audio(
    pcm: bytes = b"\x00\x01\x02\x03",
    *,
    sample_rate: int = 24000,
) -> bytes:
    """Return a small, valid PCM16 WAV file."""
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(pcm)
    return output.getvalue()


def _streaming_wav_audio(
    pcm: bytes = b"\x00\x01\x02\x03",
    *,
    sample_rate: int = 24000,
) -> bytes:
    """Return the bridge's canonical EOF-terminated PCM16 WAV framing."""
    audio = bytearray(_wav_audio(pcm, sample_rate=sample_rate))
    audio[4:8] = b"\xff\xff\xff\xff"
    audio[40:44] = b"\xff\xff\xff\xff"
    return bytes(audio)


def _prepare_pipeline_handoff(
    client: BridgeClient,
    *,
    voice: str = "cove",
    language: str = "en-US",
) -> tuple[api_module._SpeechSessionHandoffRequest, dict[str, Any]]:
    """Simulate HA preparing its TTS ResultStream before pipeline STT."""
    assert api_module._prepare_speech_session_handoff(client, voice=voice)
    request = api_module._begin_speech_session_handoff(
        client,
        language=language,
    )
    assert request is not None
    return request, {
        api_module._SPEECH_SESSION_HANDOFF_OPTION: (
            api_module._SPEECH_SESSION_HANDOFF_OPTION_VALUE
        )
    }


async def test_health_and_authentication(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Health requests authenticate and reject an invalid token."""

    async def health(request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != "Bearer good-token":
            raise web.HTTPUnauthorized
        return web.json_response({"status": "ok", "version": "1.0"})

    app = web.Application()
    app.router.add_get("/health", health)
    test_client = await aiohttp_client(app)

    client = BridgeClient(
        test_client.session, str(test_client.make_url("")), "good-token"
    )
    assert await client.async_health() == {"status": "ok", "version": "1.0"}

    invalid_client = BridgeClient(
        test_client.session,
        str(test_client.make_url("")),
        "bad-token",
    )
    with pytest.raises(BridgeAuthenticationError):
        await invalid_client.async_health()


async def test_conversation_stream_and_tool_result(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Conversation deltas and Home Assistant tool results are bidirectional."""
    received: dict[str, Any] = {}

    async def conversation(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        received["start"] = await websocket.receive_json()
        await websocket.send_json(
            {"type": "started", "conversation_id": "conversation-1"}
        )
        await websocket.send_json(
            {"type": "item", "event": "started", "item": {"type": "message"}}
        )
        await websocket.send_json(
            {"type": "event", "method": "turn/started", "params": {}}
        )
        await websocket.send_json({"type": "delta", "delta": "Lights "})
        await websocket.send_json(
            {
                "type": "tool_call",
                "request_id": 42,
                "call_id": "call-1",
                "name": "HassTurnOn",
                "arguments": {"name": "Kitchen"},
            }
        )
        received["tool_result"] = await websocket.receive_json()
        await websocket.send_json({"type": "delta", "content": "are on."})
        await websocket.send_json({"type": "done"})
        await websocket.close()
        return websocket

    app = web.Application()
    app.router.add_get("/v1/conversation", conversation)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    deltas: list[str] = []
    tool_calls: list[BridgeToolCall] = []

    async def handle_delta(delta: str) -> None:
        deltas.append(delta)

    async def handle_tool(tool_call: BridgeToolCall) -> dict[str, Any]:
        tool_calls.append(tool_call)
        return {"success": True}

    done = await client.async_converse(
        {"conversation_id": "conversation-1", "messages": []},
        async_handle_delta=handle_delta,
        async_handle_tool=handle_tool,
    )

    assert done == {"type": "done"}
    assert deltas == ["Lights ", "are on."]
    assert tool_calls == [
        BridgeToolCall(
            call_id="call-1",
            name="HassTurnOn",
            arguments={"name": "Kitchen"},
            request_id=42,
        )
    ]
    assert received["start"]["type"] == "start"
    assert received["tool_result"] == {
        "type": "tool_result",
        "request_id": 42,
        "call_id": "call-1",
        "result": {"success": True},
        "success": True,
    }


async def test_conversation_normalizes_nested_temporal_values(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Home Assistant temporal values use its canonical JSON representation."""
    received: dict[str, Any] = {}

    async def conversation(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        received["start"] = await websocket.receive_json()
        await websocket.send_json(
            {
                "type": "tool_call",
                "call_id": "call-1",
                "name": "GetSchedule",
                "arguments": {},
            }
        )
        received["tool_result"] = await websocket.receive_json()
        await websocket.send_json({"type": "done"})
        return websocket

    app = web.Application()
    app.router.add_get("/v1/conversation", conversation)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    async def handle_delta(_delta: str) -> None:
        pass

    async def handle_tool(_tool_call: BridgeToolCall) -> dict[str, Any]:
        return {
            "schedule": {
                "day": date(2026, 8, 9),
                "starts_at": time(7, 5, 3, 123456),
                "updated_at": datetime(
                    2026,
                    8,
                    9,
                    12,
                    30,
                    tzinfo=UTC,
                ),
            }
        }

    await client.async_converse(
        {
            "messages": [
                {
                    "role": "tool",
                    "result": {
                        "speech_slots": {
                            "alarm": time(6, 45),
                            "date": date(2026, 8, 10),
                        },
                        "preserved": [None, True, 42, 1.5, "ready"],
                    },
                }
            ]
        },
        async_handle_delta=handle_delta,
        async_handle_tool=handle_tool,
    )

    assert received["start"] == {
        "type": "start",
        "messages": [
            {
                "role": "tool",
                "result": {
                    "speech_slots": {
                        "alarm": "06:45:00",
                        "date": "2026-08-10",
                    },
                    "preserved": [None, True, 42, 1.5, "ready"],
                },
            }
        ],
    }
    assert received["tool_result"] == {
        "type": "tool_result",
        "request_id": None,
        "call_id": "call-1",
        "result": {
            "schedule": {
                "day": "2026-08-09",
                "starts_at": "07:05:03.123456",
                "updated_at": "2026-08-09T12:30:00+00:00",
            }
        },
        "success": True,
    }


async def test_conversation_rejects_unsupported_values_without_leaking_data(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Unsupported values fail before connection with a data-safe error."""
    connection_attempted = False

    async def conversation(request: web.Request) -> web.WebSocketResponse:
        nonlocal connection_attempted
        connection_attempted = True
        return web.WebSocketResponse()

    app = web.Application()
    app.router.add_get("/v1/conversation", conversation)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    class UnsupportedValue:
        def __repr__(self) -> str:
            return "private-automation-secret"

    async def handle_delta(_delta: str) -> None:
        pass

    async def handle_tool(_tool_call: BridgeToolCall) -> dict[str, Any]:
        return {}

    with pytest.raises(BridgeProtocolError) as err:
        await client.async_converse(
            {"messages": [{"content": UnsupportedValue()}]},
            async_handle_delta=handle_delta,
            async_handle_tool=handle_tool,
        )

    assert str(err.value) == "Conversation payload contains unsupported JSON values"
    assert "private-automation-secret" not in str(err.value)
    assert not connection_attempted


async def test_transcribe_sends_base64_pcm(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Transcription sends bounded base64 PCM and metadata."""
    received: dict[str, Any] = {}

    async def transcribe(request: web.Request) -> web.Response:
        received.update(await request.json())
        return web.json_response({"text": "Turn on the kitchen"})

    app = web.Application()
    app.router.add_post("/v1/transcribe", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    result = await client.async_transcribe(
        b"\x01\x02",
        {"sample_rate": 16000, "channels": 1, "language": "en-US"},
        prompt=None,
    )

    assert result == "Turn on the kitchen"
    assert base64.b64decode(received["audio"]) == b"\x01\x02"
    assert received["format"] == "pcm"
    assert received["sample_rate"] == 16000


async def test_finite_transcription_handoff_is_private_and_one_time(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Finite STT metadata is retained privately and sent once to matching TTS."""
    received: dict[str, Any] = {}

    async def transcribe(request: web.Request) -> web.Response:
        received["transcribe"] = await request.json()
        return web.json_response(
            {
                "text": "Private transcript",
                "speech_session_handoff": {
                    "version": 1,
                    "token": "opaque-ticket",
                    "expires_in_ms": 30_000,
                    "voice": "cove",
                    "language": "en-US",
                },
            }
        )

    async def synthesize(request: web.Request) -> web.Response:
        received["synthesize"] = await request.json()
        return web.json_response(
            {
                "audio": base64.b64encode(b"\x00\x01").decode(),
                "format": "pcm",
            }
        )

    app = web.Application()
    app.router.add_post("/v1/transcribe", transcribe)
    app.router.add_post("/v1/synthesize", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    session = chat_session.ChatSession("assist-session")
    session_context = chat_session.current_session.set(session)
    try:
        handoff, claim_options = _prepare_pipeline_handoff(client)
        transcript = await client.async_transcribe(
            b"\x00\x01",
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
            speech_session_handoff=handoff,
        )
        handoff_token = api_module._claim_speech_session_handoff(
            client,
            language="en-US",
            voice="cove",
            instructions=None,
            options=dict(claim_options),
        )
        assert transcript == "Private transcript"
        assert handoff_token == "opaque-ticket"
        assert (
            api_module._claim_speech_session_handoff(
                client,
                language="en-US",
                voice="cove",
                instructions=None,
                options=dict(claim_options),
            )
            is None
        )
        await client.async_synthesize(
            "Spoken response",
            language="en-US",
            voice="cove",
            instructions=None,
            speech_session_handoff_token=handoff_token,
        )
    finally:
        chat_session.current_session.reset(session_context)

    assert received["transcribe"]["speech_session_handoff"] == {
        "version": 1,
        "voice": "cove",
        "language": "en-US",
    }
    assert received["synthesize"]["speech_session_handoff_token"] == ("opaque-ticket")
    assert "opaque-ticket" not in transcript


async def test_transcription_without_handoff_metadata_stays_cold(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """An old bridge can ignore the opt-in without affecting transcription."""

    async def transcribe(request: web.Request) -> web.Response:
        assert (await request.json())["speech_session_handoff"]["version"] == 1
        return web.json_response({"text": "Cold transcript"})

    app = web.Application()
    app.router.add_post("/v1/transcribe", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    session_context = chat_session.current_session.set(
        chat_session.ChatSession("assist-session")
    )
    try:
        handoff, claim_options = _prepare_pipeline_handoff(client)
        result = await client.async_transcribe(
            b"\x00\x01",
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
            speech_session_handoff=handoff,
        )
        claimed = api_module._claim_speech_session_handoff(
            client,
            language="en-US",
            voice="cove",
            instructions=None,
            options=claim_options,
        )
    finally:
        chat_session.current_session.reset(session_context)

    assert result == "Cold transcript"
    assert claimed is None


async def test_finite_handoff_rejects_mismatched_result_language(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """A finite bridge result cannot change the normalized STT language binding."""
    released: list[str] = []

    async def transcribe(request: web.Request) -> web.Response:
        payload = await request.json()
        assert payload["speech_session_handoff"]["language"] == "en-US"
        return web.json_response(
            {
                "text": "Cold transcript",
                "speech_session_handoff": {
                    "version": 1,
                    "token": "wrong-language-ticket",
                    "expires_in_ms": 30_000,
                    "voice": "cove",
                    "language": "en-GB",
                },
            }
        )

    async def release(request: web.Request) -> web.Response:
        released.append((await request.json())["speech_session_handoff_token"])
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/v1/transcribe", transcribe)
    app.router.add_post("/v1/speech-session/release", release)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    session_context = chat_session.current_session.set(
        chat_session.ChatSession("assist-session")
    )
    try:
        handoff, claim_options = _prepare_pipeline_handoff(
            client,
            language=" EN_us ",
        )
        result = await client.async_transcribe(
            b"\x00\x01",
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
            speech_session_handoff=handoff,
        )
        claimed = api_module._claim_speech_session_handoff(
            client,
            language="en-US",
            voice="cove",
            instructions=None,
            options=claim_options,
        )
        while client._handoff_release_tasks:
            await asyncio.gather(*tuple(client._handoff_release_tasks))
            await asyncio.sleep(0)
    finally:
        chat_session.current_session.reset(session_context)

    assert result == "Cold transcript"
    assert claimed is None
    assert released == ["wrong-language-ticket"]


def test_handoff_claim_always_strips_private_pipeline_marker() -> None:
    """Private correlation metadata never reaches supported bridge options."""
    options = {
        api_module._SPEECH_SESSION_HANDOFF_OPTION: (
            api_module._SPEECH_SESSION_HANDOFF_OPTION_VALUE
        )
    }

    assert (
        api_module._claim_speech_session_handoff(
            cast("BridgeClient", object()),
            language="en-US",
            voice="cove",
            instructions=None,
            options=options,
        )
        is None
    )
    assert api_module._SPEECH_SESSION_HANDOFF_OPTION not in options


async def test_transcribe_stream_opens_before_consuming_and_preserves_chunks(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """The streaming handshake precedes bounded, ordered PCM transmission."""
    start_received = asyncio.Event()
    iterator_advanced = asyncio.Event()
    received: dict[str, Any] = {"audio": []}

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        assert request.headers["Authorization"] == "Bearer token"
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        received["start"] = await websocket.receive_json()
        start_received.set()
        await websocket.send_json({"type": "started", "protocol_version": 1})
        async for message in websocket:
            if message.type is web.WSMsgType.BINARY:
                received["audio"].append(message.data)
                continue
            assert message.type is web.WSMsgType.TEXT
            received["end"] = message.json()
            await websocket.send_json(
                {
                    "type": "result",
                    "text": "Streamed speech",
                    "language": "en-US",
                    "speech_session_handoff": {
                        "version": 1,
                        "token": "stream-ticket",
                        "expires_in_ms": 30_000,
                        "voice": "cove",
                        "language": "en-US",
                    },
                }
            )
            break
        return websocket

    async def audio_stream() -> AsyncGenerator[bytes]:
        assert start_received.is_set()
        iterator_advanced.set()
        yield b"a" * (64 * 1024 + 1)
        yield b"bc"
        yield b"d"

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    session_context = chat_session.current_session.set(
        chat_session.ChatSession("assist-session")
    )
    try:
        handoff, claim_options = _prepare_pipeline_handoff(client)
        result = await client.async_transcribe_stream(
            audio_stream(),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt="Home automation",
            speech_session_handoff=handoff,
        )
        claimed = api_module._claim_speech_session_handoff(
            client,
            language="en-US",
            voice="cove",
            instructions=None,
            options=claim_options,
        )
    finally:
        chat_session.current_session.reset(session_context)

    assert result == "Streamed speech"
    assert claimed == "stream-ticket"
    assert iterator_advanced.is_set()
    assert received["start"] == {
        "type": "start",
        "protocol_version": 1,
        "format": "pcm",
        "codec": "pcm",
        "sample_rate": 16000,
        "bit_rate": 16,
        "channels": 1,
        "language": "en-US",
        "prompt": "Home automation",
        "speech_session_handoff": {
            "version": 1,
            "voice": "cove",
            "language": "en-US",
        },
    }
    assert received["audio"] == [b"a" * (64 * 1024), b"ab", b"cd"]
    assert received["end"] == {"type": "end"}


async def test_transcribe_stream_enforces_input_size_limit(
    aiohttp_client: Any,
    socket_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Streaming transcription rejects audio beyond the raw input cap."""
    monkeypatch.setattr(api_module, "MAX_AUDIO_BYTES", 4)
    socket_closed = asyncio.Event()
    received_audio: list[bytes] = []

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json({"type": "started", "protocol_version": 1})
        try:
            async for message in websocket:
                if message.type is web.WSMsgType.BINARY:
                    received_audio.extend([message.data])
        finally:
            socket_closed.set()
        return websocket

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError, match="16 MiB"):
        await client.async_transcribe_stream(
            _async_chunks(b"1234", b"5"),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )

    await asyncio.wait_for(socket_closed.wait(), timeout=1)
    assert received_audio == [b"1234"]


async def test_transcribe_stream_rejects_incomplete_pcm16_sample(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Odd source boundaries are joined, but an odd final byte is rejected."""
    socket_closed = asyncio.Event()
    received_audio: list[bytes] = []

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json({"type": "started", "protocol_version": 1})
        try:
            async for message in websocket:
                if message.type is web.WSMsgType.BINARY:
                    received_audio.extend([message.data])
        finally:
            socket_closed.set()
        return websocket

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError, match="incomplete PCM16"):
        await client.async_transcribe_stream(
            _async_chunks(b"123"),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )

    await asyncio.wait_for(socket_closed.wait(), timeout=1)
    assert received_audio == [b"12"]


@pytest.mark.parametrize("status", [404, 405, 426])
async def test_transcribe_stream_reports_pre_upgrade_unsupported_without_consuming(
    aiohttp_client: Any,
    socket_enabled: None,
    status: int,
) -> None:
    """Only legacy HTTP upgrade failures report streaming as unsupported."""
    consumed = False

    async def unsupported(request: web.Request) -> web.Response:
        return web.Response(status=status)

    async def audio_stream() -> AsyncGenerator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"audio"

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", unsupported)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeStreamingUnsupported):
        await client.async_transcribe_stream(
            audio_stream(),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )

    assert not consumed


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, BridgeAuthenticationError),
        (403, BridgeAuthenticationError),
        (409, BridgeBusyError),
        (429, BridgeQuotaError),
        (500, BridgeConnectionError),
        (400, BridgeProtocolError),
    ],
)
async def test_transcribe_stream_maps_non_fallback_http_errors(
    aiohttp_client: Any,
    socket_enabled: None,
    status: int,
    error_type: type[Exception],
) -> None:
    """Authentication, capacity, server, and protocol failures never fall back."""
    consumed = False

    async def reject(request: web.Request) -> web.Response:
        return web.Response(status=status)

    async def audio_stream() -> AsyncGenerator[bytes]:
        nonlocal consumed
        consumed = True
        yield b"audio"

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", reject)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(error_type):
        await client.async_transcribe_stream(
            audio_stream(),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )

    assert not consumed


@pytest.mark.parametrize(
    ("event", "error_type"),
    [
        ({"type": "error", "code": "invalid_auth"}, BridgeAuthenticationError),
        ({"type": "error", "code": "busy"}, BridgeBusyError),
        ({"type": "error", "code": "quota_exhausted"}, BridgeQuotaError),
        ({"type": "error", "code": "unknown"}, BridgeProtocolError),
    ],
)
async def test_transcribe_stream_maps_error_events(
    aiohttp_client: Any,
    socket_enabled: None,
    event: dict[str, Any],
    error_type: type[Exception],
) -> None:
    """Safe post-upgrade error events retain the stable client error types."""

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json(event)
        return websocket

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(error_type):
        await client.async_transcribe_stream(
            _async_chunks(b"audio"),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )


@pytest.mark.parametrize(
    "event",
    [
        {"type": "started", "protocol_version": 2},
        {"type": "result", "text": "too soon"},
    ],
)
async def test_transcribe_stream_rejects_invalid_started_events(
    aiohttp_client: Any,
    socket_enabled: None,
    event: dict[str, Any],
) -> None:
    """Streaming transcription requires its versioned started acknowledgement."""

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json(event)
        return websocket

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError):
        await client.async_transcribe_stream(
            _async_chunks(b"audio"),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )


@pytest.mark.parametrize(
    "result_event",
    [
        {"type": "done", "text": "wrong type"},
        {"type": "result"},
        {"type": "result", "text": 1},
        {"type": "result", "text": "speech", "language": 1},
        {"type": "result", "text": "speech", "language": None},
    ],
)
async def test_transcribe_stream_validates_result_events(
    aiohttp_client: Any,
    socket_enabled: None,
    result_event: dict[str, Any],
) -> None:
    """The final streaming event must contain a valid transcription result."""

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json({"type": "started", "protocol_version": 1})
        await websocket.receive_json()
        await websocket.send_json(result_event)
        return websocket

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError):
        await client.async_transcribe_stream(
            _async_chunks(),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )


async def test_transcribe_stream_cancellation_closes_socket(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Cancelling microphone capture promptly closes the upgraded socket."""
    iterator_waiting = asyncio.Event()
    socket_closed = asyncio.Event()

    async def transcribe(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.receive_json()
        await websocket.send_json({"type": "started", "protocol_version": 1})
        try:
            async for _message in websocket:
                pass
        finally:
            socket_closed.set()
        return websocket

    async def audio_stream() -> AsyncGenerator[bytes]:
        iterator_waiting.set()
        await asyncio.Event().wait()
        yield b"unreachable"

    app = web.Application()
    app.router.add_get("/v1/transcribe/stream", transcribe)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    task = asyncio.create_task(
        client.async_transcribe_stream(
            audio_stream(),
            {
                "sample_rate": 16000,
                "bit_rate": 16,
                "channels": 1,
                "language": "en-US",
            },
            prompt=None,
        )
    )

    await asyncio.wait_for(iterator_waiting.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(socket_closed.wait(), timeout=1)


async def _async_chunks(*chunks: bytes) -> AsyncGenerator[bytes]:
    """Yield byte chunks for streaming client tests."""
    for chunk in chunks:
        yield chunk


async def test_synthesize_decodes_json_pcm(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Synthesis accepts the bridge's base64 PCM response."""
    received: dict[str, Any] = {}

    async def synthesize(request: web.Request) -> web.Response:
        payload = await request.json()
        received.update(payload)
        return web.json_response(
            {
                "audio": base64.b64encode(b"\x00\x01").decode(),
                "format": "pcm",
                "sample_rate": 24000,
                "channels": 1,
                "sample_width": 2,
            }
        )

    app = web.Application()
    app.router.add_post("/v1/synthesize", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    audio = await client.async_synthesize(
        "Hello",
        language="en-US",
        voice="alloy",
        instructions=None,
    )

    assert audio.data == b"\x00\x01"
    assert audio.audio_format == "pcm"
    assert audio.sample_rate == 24000
    assert received == {
        "text": "Hello",
        "language": "en-US",
        "voice": "alloy",
        "format": "wav",
    }


async def test_synthesize_forwards_native_audio_preferences(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Finite synthesis sends normalized native WAV preferences."""
    received: dict[str, Any] = {}

    async def synthesize(request: web.Request) -> web.Response:
        received.update(await request.json())
        return web.json_response(
            {
                "audio": base64.b64encode(b"\x00\x01").decode(),
                "format": "pcm",
                "sample_rate": 16000,
                "channels": 1,
                "sample_width": 2,
            }
        )

    app = web.Application()
    app.router.add_post("/v1/synthesize", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    audio = await client.async_synthesize(
        "Hello",
        language="en-US",
        voice="alloy",
        instructions=None,
        sample_rate=16000,
        channels=1,
        sample_width=2,
    )

    assert audio.sample_rate == 16000
    assert received == {
        "text": "Hello",
        "language": "en-US",
        "voice": "alloy",
        "format": "wav",
        "sample_rate": 16000,
        "channels": 1,
        "sample_width": 2,
    }


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"sample_rate": 22050}, "sample_rate"),
        ({"sample_rate": True}, "sample_rate"),
        ({"channels": 2}, "channels"),
        ({"sample_width": 1}, "sample_width"),
    ],
)
async def test_synthesize_rejects_invalid_audio_preferences(
    kwargs: dict[str, Any],
    error: str,
) -> None:
    """Finite synthesis rejects unsupported output preferences before I/O."""
    client = BridgeClient(cast("Any", object()), "http://bridge.test", "token")

    with pytest.raises(BridgeProtocolError, match=error):
        await client.async_synthesize(
            "Hello",
            language="en-US",
            voice="alloy",
            instructions=None,
            **kwargs,
        )


async def test_release_speech_session_handoff_is_authenticated_and_body_only(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Best-effort release keeps the opaque ticket out of URLs and headers."""
    received: dict[str, Any] = {}

    async def release(request: web.Request) -> web.Response:
        received["path"] = request.path
        received["authorization"] = request.headers.get("Authorization")
        received["payload"] = await request.json()
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/v1/speech-session/release", release)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    await client.async_release_speech_session_handoff("opaque-ticket")

    assert received == {
        "path": "/v1/speech-session/release",
        "authorization": "Bearer token",
        "payload": {"speech_session_handoff_token": "opaque-ticket"},
    }


async def test_handoff_release_jobs_are_client_owned_and_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entry unload cancellation reaches every release job owned by its client."""
    client = BridgeClient(cast("Any", object()), "http://bridge.test", "token")
    release_started = asyncio.Event()

    async def release(token: str) -> None:
        assert token == "private-ticket"
        release_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "async_release_speech_session_handoff", release)
    api_module._schedule_speech_session_handoff_release(client, "private-ticket")
    await asyncio.wait_for(release_started.wait(), timeout=1)
    assert len(client._handoff_release_tasks) == 1
    task = next(iter(client._handoff_release_tasks))

    client.cancel_handoff_release_tasks()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)

    assert not client._handoff_release_tasks
    assert task not in api_module._HANDOFF_RELEASE_TASKS


async def test_handoff_release_suppresses_closed_session_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleanup racing aiohttp shutdown completes without an orphaned failure."""
    client = BridgeClient(cast("Any", object()), "http://bridge.test", "token")

    async def release(token: str) -> None:
        assert token == "private-ticket"
        raise RuntimeError("Session is closed")

    monkeypatch.setattr(client, "async_release_speech_session_handoff", release)
    api_module._schedule_speech_session_handoff_release(client, "private-ticket")
    task = next(iter(client._handoff_release_tasks))

    await task
    await asyncio.sleep(0)

    assert task.exception() is None
    assert not client._handoff_release_tasks
    assert task not in api_module._HANDOFF_RELEASE_TASKS


async def test_synthesize_rejects_mislabeled_wav(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Binary synthesis responses must contain a recognizable WAV file."""

    async def synthesize(request: web.Request) -> web.Response:
        return web.Response(body=b"not a wave file", content_type="audio/wav")

    app = web.Application()
    app.router.add_post("/v1/synthesize", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError, match="valid WAV"):
        await client.async_synthesize(
            "Hello",
            language="en-US",
            voice="alloy",
            instructions=None,
        )


async def test_synthesize_stream_yields_before_response_completes(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """Streaming synthesis validates its header and applies backpressure."""
    audio = _streaming_wav_audio(sample_rate=16000)
    header_started = asyncio.Event()
    release_header = asyncio.Event()
    release_audio = asyncio.Event()
    received: dict[str, Any] = {}

    async def synthesize(request: web.Request) -> web.StreamResponse:
        assert request.headers["Authorization"] == "Bearer token"
        received.update(await request.json())
        response = web.StreamResponse(headers={"Content-Type": "audio/wav"})
        await response.prepare(request)
        await response.write(audio[:5])
        header_started.set()
        await release_header.wait()
        await response.write(audio[5:44])
        await release_audio.wait()
        await response.write(audio[44:])
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/synthesize/stream", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")
    stream = client.async_synthesize_stream(
        "Hello",
        language="en-US",
        voice="cove",
        instructions="Be brief",
        speech_session_handoff_token="stream-ticket",
        sample_rate=16000,
        channels=1,
        sample_width=2,
    )

    first_chunk_task = asyncio.create_task(anext(stream))
    await asyncio.wait_for(header_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not first_chunk_task.done()

    release_header.set()
    try:
        await asyncio.sleep(0)
        assert not first_chunk_task.done()
        release_audio.set()
        first_chunk = await asyncio.wait_for(first_chunk_task, timeout=1)
        assert len(first_chunk) >= 46
        assert first_chunk[:4] == b"RIFF"
        assert first_chunk[8:12] == b"WAVE"
        remaining = [chunk async for chunk in stream]
    finally:
        release_header.set()
        release_audio.set()
        await stream.aclose()

    assert b"".join([first_chunk, *remaining]) == audio
    assert received == {
        "text": "Hello",
        "language": "en-US",
        "voice": "cove",
        "format": "wav",
        "instructions": "Be brief",
        "speech_session_handoff_token": "stream-ticket",
        "sample_rate": 16000,
        "channels": 1,
        "sample_width": 2,
    }


async def test_synthesize_stream_rejects_invalid_audio_preferences() -> None:
    """Streaming synthesis validates output preferences before HTTP I/O."""
    client = BridgeClient(cast("Any", object()), "http://bridge.test", "token")

    with pytest.raises(BridgeProtocolError, match="sample_rate"):
        _ = [
            chunk
            async for chunk in client.async_synthesize_stream(
                "Hello",
                language="en-US",
                voice="cove",
                instructions=None,
                sample_rate=22050,
                channels=1,
                sample_width=2,
            )
        ]


async def test_synthesize_stream_accepts_legacy_default_output(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """A rolling-upgrade bridge may ignore native output preferences."""
    audio = _streaming_wav_audio(sample_rate=24000)

    async def synthesize(request: web.Request) -> web.Response:
        payload = await request.json()
        assert payload["sample_rate"] == 16000
        return web.Response(body=audio, content_type="audio/wav")

    app = web.Application()
    app.router.add_post("/v1/synthesize/stream", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    result = b"".join(
        [
            chunk
            async for chunk in client.async_synthesize_stream(
                "Hello",
                language="en-US",
                voice="cove",
                instructions=None,
                sample_rate=16000,
                channels=1,
                sample_width=2,
            )
        ]
    )

    assert result == audio


async def test_synthesize_stream_rejects_fragmented_invalid_header(
    aiohttp_client: Any,
    socket_enabled: None,
) -> None:
    """No bytes are exposed before a fragmented WAV header is validated."""

    async def synthesize(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "audio/wav"})
        await response.prepare(request)
        await response.write(b"not ")
        await response.write(b"a wav header")
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/synthesize/stream", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError, match="valid WAV"):
        _ = [
            chunk
            async for chunk in client.async_synthesize_stream(
                "Hello",
                language="en-US",
                voice="cove",
                instructions=None,
            )
        ]


async def test_synthesize_stream_enforces_size_limit(
    aiohttp_client: Any,
    socket_enabled: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Chunked synthesis cannot exceed the client-side audio size limit."""
    monkeypatch.setattr(api_module, "MAX_SYNTHESIZED_AUDIO_BYTES", 16)
    audio = _streaming_wav_audio()

    async def synthesize(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "audio/wav"})
        await response.prepare(request)
        await response.write(audio[:12])
        await response.write(audio[12:17])
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/synthesize/stream", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(BridgeProtocolError, match="50 MiB"):
        _ = [
            chunk
            async for chunk in client.async_synthesize_stream(
                "Hello",
                language="en-US",
                voice="cove",
                instructions=None,
            )
        ]


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (401, BridgeAuthenticationError),
        (409, BridgeBusyError),
        (429, BridgeQuotaError),
        (500, BridgeConnectionError),
        (400, BridgeProtocolError),
    ],
)
async def test_synthesize_stream_maps_http_errors(
    aiohttp_client: Any,
    socket_enabled: None,
    status: int,
    error_type: type[Exception],
) -> None:
    """Streaming synthesis preserves the bridge's stable HTTP error mapping."""

    async def synthesize(request: web.Request) -> web.Response:
        return web.Response(status=status)

    app = web.Application()
    app.router.add_post("/v1/synthesize/stream", synthesize)
    test_client = await aiohttp_client(app)
    client = BridgeClient(test_client.session, str(test_client.make_url("")), "token")

    with pytest.raises(error_type):
        _ = [
            chunk
            async for chunk in client.async_synthesize_stream(
                "Hello",
                language="en-US",
                voice="cove",
                instructions=None,
            )
        ]


async def test_synthesize_stream_closes_response_when_consumer_stops() -> None:
    """Closing a partially consumed generator promptly closes its HTTP response."""
    audio = _streaming_wav_audio()

    class StreamContent:
        async def iter_chunked(self, chunk_size: int) -> AsyncGenerator[bytes]:
            yield audio[:46]
            await asyncio.Event().wait()

    class StreamResponse:
        status = 200
        content_type = "audio/wav"
        content_length = None
        content = StreamContent()
        closed = False

        def close(self) -> None:
            self.closed = True

    response = StreamResponse()

    class RequestContext:
        async def __aenter__(self) -> StreamResponse:
            return response

        async def __aexit__(self, *args: Any) -> None:
            response.close()

    class Session:
        def post(self, *args: Any, **kwargs: Any) -> RequestContext:
            return RequestContext()

    client = BridgeClient(cast("Any", Session()), "http://bridge", "token")
    stream = client.async_synthesize_stream(
        "Hello",
        language="en-US",
        voice="cove",
        instructions=None,
    )

    _ = await asyncio.wait_for(anext(stream), timeout=1)
    await stream.aclose()

    assert response.closed
