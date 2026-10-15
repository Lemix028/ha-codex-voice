"""Opt-in live probe for Codex App Server subscription WebRTC audio.

This diagnostic never reads OAuth files. Codex App Server uses its own managed
login. Running it consumes a small amount of the signed-in ChatGPT quota.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import deque
from contextlib import suppress
from fractions import Fraction
from tempfile import TemporaryDirectory
from typing import Any

from aiortc import (
    MediaStreamError,
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
)
from av import AudioFrame

from bridge.webrtc import PcmAudioTrack


class AppServerError(RuntimeError):
    """An error returned by Codex App Server."""


class SilenceAudioTrack(MediaStreamTrack):
    """Paced silence that makes the negotiated WebRTC audio m-line active."""

    kind = "audio"
    rate = 48_000
    samples_per_frame = 960

    def __init__(self) -> None:
        super().__init__()
        self._pts = 0
        self._started: float | None = None

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_running_loop()
        if self._started is None:
            self._started = loop.time()
        else:
            target = self._started + (self._pts / self.rate)
            await asyncio.sleep(max(0, target - loop.time()))

        frame = AudioFrame(format="s16", layout="mono", samples=self.samples_per_frame)
        for plane in frame.planes:
            plane.update(bytes(plane.buffer_size))
        frame.pts = self._pts
        frame.sample_rate = self.rate
        frame.time_base = Fraction(1, self.rate)
        self._pts += self.samples_per_frame
        return frame


class AppServer:
    """Small newline-delimited JSON-RPC client used only by this probe."""

    def __init__(self, codex_bin: str) -> None:
        self._codex_bin = codex_bin
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.notifications: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.stderr: deque[str] = deque(maxlen=20)

    async def start(self) -> None:
        """Start and initialize App Server."""
        self._process = await asyncio.create_subprocess_exec(
            self._codex_bin,
            "app-server",
            "--enable",
            "realtime_conversation",
            "--stdio",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        await self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "ha_codex_voice_probe",
                    "title": "Codex Voice WebRTC Probe",
                    "version": "0.8.0",
                },
                "capabilities": {"experimentalApi": True},
            },
        )
        await self.notify("initialized", {})

    async def request(
        self, method: str, params: dict[str, Any], timeout: float = 30
    ) -> dict[str, Any]:
        """Send a request and await its response."""
        request_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self._write({"id": request_id, "method": method, "params": params})
        response = await asyncio.wait_for(future, timeout)
        if error := response.get("error"):
            raise AppServerError(f"{method}: {error.get('message', 'unknown error')}")
        return response.get("result", {})

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        """Send a notification."""
        await self._write({"method": method, "params": params})

    async def close(self) -> None:
        """Stop App Server."""
        if self._process is not None and self._process.returncode is None:
            self._process.terminate()
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._process.wait(), 3)
            if self._process.returncode is None:
                self._process.kill()
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task),
            return_exceptions=True,
        )

    async def _write(self, message: dict[str, Any]) -> None:
        assert self._process is not None and self._process.stdin is not None
        self._process.stdin.write((json.dumps(message) + "\n").encode())
        await self._process.stdin.drain()

    async def _read_stdout(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        while line := await self._process.stdout.readline():
            message = json.loads(line)
            if "id" in message and ("result" in message or "error" in message):
                future = self._pending.pop(message["id"], None)
                if future is not None and not future.done():
                    future.set_result(message)
            elif "id" in message and "method" in message:
                await self._write(
                    {
                        "id": message["id"],
                        "error": {
                            "code": -32000,
                            "message": "Probe denies server-initiated operations",
                        },
                    }
                )
            elif "method" in message:
                await self.notifications.put(message)

    async def _read_stderr(self) -> None:
        assert self._process is not None and self._process.stderr is not None
        while line := await self._process.stderr.readline():
            self.stderr.append(line.decode(errors="replace").rstrip())


async def run_probe(
    codex_bin: str, phrase: str, *, bridge_track: bool = False
) -> dict[str, Any]:
    """Run one subscription-backed TTS-like WebRTC turn."""
    app_server = AppServer(codex_bin)
    peer = RTCPeerConnection()
    if bridge_track:
        source = PcmAudioTrack()
    else:
        source = SilenceAudioTrack()
    runtime_dir = TemporaryDirectory(prefix="ha-codex-voice-probe-")
    received_frames = 0
    received_samples = 0
    audio_received = asyncio.Event()
    peer_connected = asyncio.Event()
    consumer_tasks: set[asyncio.Task[None]] = set()

    @peer.on("connectionstatechange")
    async def on_connectionstatechange() -> None:
        if peer.connectionState == "connected":
            peer_connected.set()

    @peer.on("track")
    def on_track(track: Any) -> None:
        if track.kind != "audio":
            return

        async def consume_audio() -> None:
            nonlocal received_frames, received_samples
            while True:
                try:
                    frame = await track.recv()
                except MediaStreamError:
                    return
                received_frames += 1
                received_samples += frame.samples
                audio_received.set()

        task = asyncio.create_task(consume_audio())
        consumer_tasks.add(task)
        task.add_done_callback(consumer_tasks.discard)

    try:
        await app_server.start()
        account = await app_server.request("account/read", {"refreshToken": False})
        thread = await app_server.request(
            "thread/start",
            {
                "cwd": runtime_dir.name,
                "ephemeral": True,
                "sandbox": "read-only",
                "approvalPolicy": "never",
                "baseInstructions": "Respond only as a voice assistant.",
                "developerInstructions": "Never call tools or inspect files.",
            },
        )
        thread_id = thread["thread"]["id"]

        peer.addTrack(source)
        peer.createDataChannel("oai-events")
        await peer.setLocalDescription(await peer.createOffer())
        assert peer.localDescription is not None

        await app_server.request(
            "thread/realtime/start",
            {
                "threadId": thread_id,
                "outputModality": "audio",
                "version": "v3",
                "transport": {
                    "type": "webrtc",
                    "sdp": peer.localDescription.sdp,
                },
                "includeStartupContext": False,
                "clientManagedHandoffs": True,
                "prompt": (
                    "Speak text supplied on the speakable channel exactly once. "
                    "Do not add or remove words."
                ),
                "voice": "cove",
            },
            timeout=45,
        )

        started: dict[str, Any] | None = None
        answer_sdp: str | None = None
        while started is None or answer_sdp is None:
            note = await asyncio.wait_for(app_server.notifications.get(), 45)
            if note["method"] == "thread/realtime/started":
                started = note["params"]
            elif note["method"] == "thread/realtime/sdp":
                answer_sdp = note["params"]["sdp"]
            elif note["method"] == "thread/realtime/error":
                raise AppServerError(note["params"]["message"])

        await peer.setRemoteDescription(RTCSessionDescription(answer_sdp, "answer"))
        await asyncio.wait_for(peer_connected.wait(), 15)
        await app_server.request(
            "thread/realtime/appendSpeech",
            {"threadId": thread_id, "text": phrase},
        )

        transcript = ""
        deadline = asyncio.get_running_loop().time() + 45
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                note = await asyncio.wait_for(app_server.notifications.get(), remaining)
            except TimeoutError:
                break
            if note["method"] == "thread/realtime/transcript/done":
                params = note["params"]
                if params.get("role") == "assistant":
                    transcript = params.get("text", "")
                    break
            if note["method"] == "thread/realtime/error":
                raise AppServerError(note["params"]["message"])

        await asyncio.wait_for(audio_received.wait(), 10)
        await asyncio.sleep(0.5)
        await app_server.request("thread/realtime/stop", {"threadId": thread_id})
        return {
            "auth_mode": account.get("account", {}).get("type"),
            "realtime_version": started.get("version") if started else None,
            "transcript": transcript,
            "audio_frames": received_frames,
            "audio_samples": received_samples,
        }
    finally:
        source.stop()
        await peer.close()
        for task in consumer_tasks:
            task.cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)
        runtime_dir.cleanup()
        await app_server.close()


async def list_voices(codex_bin: str) -> dict[str, Any]:
    """Return the safe, non-account-specific realtime voice catalog."""
    app_server = AppServer(codex_bin)
    try:
        await app_server.start()
        return await app_server.request("thread/realtime/listVoices", {})
    finally:
        await app_server.close()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--phrase", default="Codex Voice audio test.")
    parser.add_argument("--list-voices", action="store_true")
    parser.add_argument(
        "--bridge-track",
        action="store_true",
        help="Use the bridge's paced PCM track in the direct WebRTC probe.",
    )
    args = parser.parse_args()
    operation = (
        list_voices(args.codex_bin)
        if args.list_voices
        else run_probe(args.codex_bin, args.phrase, bridge_track=args.bridge_track)
    )
    print(json.dumps(asyncio.run(operation), indent=2))


if __name__ == "__main__":
    main()
