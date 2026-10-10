"""Read existing HA pipeline debug events and export only safe measurements."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiohttp

# HA stage events are not proof of device capture or audible playback times.
EVENTS = (
    "run-start",
    "wake_word-start",
    "wake_word-end",
    "stt-start",
    "stt-vad-start",
    "stt-vad-end",
    "stt-end",
    "intent-start",
    "intent-end",
    "tts-start",
    "tts-end",
    "run-end",
)


def sanitize_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Discard all text, IDs, URLs, provider errors and audio before export."""
    times: dict[str, datetime] = {}
    path = "not_observed"
    failed = False
    for event in events:
        event_type = event.get("type")
        if event_type == "error":
            failed = True
        if event_type in EVENTS and isinstance(event.get("timestamp"), str):
            try:
                timestamp = datetime.fromisoformat(event["timestamp"])
                if timestamp.tzinfo is not None:
                    times.setdefault(event_type, timestamp)
            except ValueError:
                pass
        if event_type == "intent-end":
            data = event.get("data")
            if isinstance(data, dict) and isinstance(
                data.get("processed_locally"), bool
            ):
                path = "local" if data["processed_locally"] else "conversation"
    origin = min(times.values()) if times else None
    return {
        "correlation_id": uuid4().hex,
        "path": "error" if failed else path,
        "error": "pipeline_failed" if failed else None,
        "stages_ms": {
            stage: round((times[stage] - origin).total_seconds() * 1000, 3)
            if stage in times and origin is not None
            else None
            for stage in EVENTS
        },
        "capture_started": None,
        "capture_ended": None,
        "first_tts_chunk": None,
        "audio_ended": None,
        "scope": "HA stage events; VAD is speech detection, run-end is not audio-end",
    }


async def collect(
    url: str, token: str, pipeline_id: str, run_id: str | None
) -> dict[str, Any]:
    """Fetch bounded existing debug data without starting or changing a pipeline."""
    async with (
        aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as client,
        client.ws_connect(
            url.rstrip("/") + "/api/websocket", max_msg_size=2 * 1024 * 1024
        ) as ws,
    ):
        if (await ws.receive_json()).get("type") != "auth_required":
            raise ValueError("Unexpected authentication handshake")
        await ws.send_json({"type": "auth", "access_token": token})
        if (await ws.receive_json()).get("type") != "auth_ok":
            raise ValueError("HA authentication failed")
        if run_id is None:
            await ws.send_json(
                {
                    "id": 1,
                    "type": "assist_pipeline/pipeline_debug/list",
                    "pipeline_id": pipeline_id,
                }
            )
            listing = await ws.receive_json()
            if not listing.get("success"):
                raise ValueError("Pipeline debug listing unavailable")
            runs = listing["result"]["pipeline_runs"]
            if not runs:
                raise ValueError("No recorded pipeline runs available")
            run_id = runs[-1]["pipeline_run_id"]
        await ws.send_json(
            {
                "id": 2,
                "type": "assist_pipeline/pipeline_debug/get",
                "pipeline_id": pipeline_id,
                "pipeline_run_id": run_id,
            }
        )
        result = await ws.receive_json()
        if not result.get("success"):
            raise ValueError("Pipeline debug data unavailable")
        return sanitize_events(result["result"]["events"])


def main() -> None:
    """Export one existing pipeline trace; credentials come from a secret file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        token = Path(os.environ["HASS_TOKEN_FILE"]).read_text(encoding="utf-8").strip()
        result = asyncio.run(
            collect(os.environ["HASS_URL"], token, args.pipeline_id, args.run_id)
        )
    except (KeyError, OSError, ValueError, aiohttp.ClientError, TimeoutError):
        raise SystemExit(
            "Trace unavailable; check HA connection, secret file and pipeline ID"
        ) from None
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
