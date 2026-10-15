"""Bounded, content-free measurements at the Conversation API boundary."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from time import monotonic
from typing import Any
from uuid import uuid4

STAGES = (
    "wake_detected",
    "capture_started",
    "capture_ended",
    "stt_received",
    "stt_completed",
    "local_intent_started",
    "local_intent_completed",
    "conversation_received",
    "context_selection_started",
    "context_selection_completed",
    "codex_requested",
    "codex_completed",
    "first_text_delta",
    "first_tts_chunk",
    "audio_ended",
)


@dataclass(slots=True)
class TurnTrace:
    """Measure relative monotonic times without storing request contents."""

    correlation_id: str = field(default_factory=lambda: uuid4().hex)
    started: float = field(default_factory=monotonic, repr=False)
    stages: dict[str, float] = field(default_factory=dict)
    tools_ms: list[float] = field(default_factory=list)
    path: str = "codex"
    error: str | None = None
    identity_source: str = "not_observed"
    incoming_id_present: bool = False
    incoming_id_changed: bool | None = None
    context_status: str = "not_observed"
    context_choice: str = "legacy"
    tools_available: bool = False
    tool_calls_executed: int = 0
    bridge_turn_requests: int = 0

    def mark(self, stage: str) -> None:
        """Record an allowlisted boundary once."""
        if stage not in STAGES:
            raise ValueError("Unknown diagnostic stage")
        self.stages.setdefault(stage, round((monotonic() - self.started) * 1000, 3))

    def export(self) -> dict[str, Any]:
        """Export numeric measurements; unavailable boundaries remain null."""
        return {
            "correlation_id": self.correlation_id,
            "path": self.path,
            "error": self.error,
            "identity_source": self.identity_source,
            "incoming_id_present": self.incoming_id_present,
            "incoming_id_changed": self.incoming_id_changed,
            "context_status": self.context_status,
            "context_choice": self.context_choice,
            "tools_available": self.tools_available,
            "tool_calls_executed": self.tool_calls_executed,
            "bridge_turn_requests": self.bridge_turn_requests,
            "stages_ms": {stage: self.stages.get(stage) for stage in STAGES},
            "tools_ms": list(self.tools_ms),
            "context_selection_ms": self._duration(
                "context_selection_started", "context_selection_completed"
            ),
            "local_intent_ms": self._duration(
                "local_intent_started", "local_intent_completed"
            ),
            "model_wait_ms": max(0.0, duration - sum(self.tools_ms))
            if (duration := self._duration("codex_requested", "codex_completed"))
            is not None
            else None,
            "total_ms": round((monotonic() - self.started) * 1000, 3),
        }

    def _duration(self, start: str, end: str) -> float | None:
        """Exclude unknown boundaries rather than inventing zero durations."""
        if start not in self.stages or end not in self.stages:
            return None
        return round(self.stages[end] - self.stages[start], 3)


class DiagnosticBuffer:
    """Keep the last 64 completed turns in RAM only."""

    def __init__(self) -> None:
        """Initialize the bounded diagnostic ring."""
        self._turns: deque[dict[str, Any]] = deque(maxlen=64)

    def finish(self, trace: TurnTrace) -> None:
        """Freeze a completed trace."""
        self._turns.append(trace.export())

    def export(self) -> list[dict[str, Any]]:
        """Return content-free completed measurements."""
        return list(self._turns)
