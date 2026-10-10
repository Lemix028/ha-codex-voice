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
