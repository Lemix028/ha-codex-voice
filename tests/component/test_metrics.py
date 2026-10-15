"""Content-free diagnostic measurement tests."""

from custom_components.codex_voice.metrics import DiagnosticBuffer, TurnTrace


def test_diagnostic_buffer_is_bounded_and_explicit_about_missing_times() -> None:
    """Only the last 64 completed traces are retained in RAM."""
    buffer = DiagnosticBuffer()
    for _ in range(70):
        trace = TurnTrace()
        trace.mark("conversation_received")
        buffer.finish(trace)
    assert len(buffer.export()) == 64
    result = buffer.export()[-1]
    assert result["stages_ms"]["conversation_received"] is not None
    assert result["stages_ms"]["wake_detected"] is None
    assert result["stages_ms"]["first_tts_chunk"] is None
    assert len({item["correlation_id"] for item in buffer.export()}) == 64


def test_separate_durations_and_no_content_fields() -> None:
    """Model/transport wait excludes tool execution and unknown stages stay null."""
    trace = TurnTrace(
        stages={
            "context_selection_started": 1,
            "context_selection_completed": 3,
            "local_intent_started": 3,
            "local_intent_completed": 8,
            "codex_requested": 10,
            "codex_completed": 60,
        },
        tools_ms=[12, 8],
    )
    value = trace.export()
    assert value["context_selection_ms"] == 2
    assert value["local_intent_ms"] == 5
    assert value["model_wait_ms"] == 30
    assert (
        not {
            "text",
            "messages",
            "arguments",
            "result",
            "user_id",
            "device_id",
            "thread_id",
        }
        & value.keys()
    )
    assert TurnTrace().export()["model_wait_ms"] is None
