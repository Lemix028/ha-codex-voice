"""Privacy and measurement tests use synthetic pipeline events only."""

from scripts.collect_assist_trace import sanitize_events


def test_trace_only_exports_observed_boundaries() -> None:
    events = [
        {"type": "stt-start", "timestamp": "2026-10-10T10:00:00+00:00"},
        {
            "type": "stt-end",
            "timestamp": "2026-10-10T10:00:02+00:00",
            "data": {"text": "private phrase", "token": "secret"},
        },
        {"type": "intent-end", "data": {"processed_locally": True}},
    ]
    result = sanitize_events(events)
    assert result["stages_ms"]["stt-end"] == 2000
    assert result["stages_ms"]["wake_word-end"] is None
    assert result["first_tts_chunk"] is None
    assert result["audio_ended"] is None
    assert result["path"] == "local"
    assert "private phrase" not in str(result)
    assert "secret" not in str(result)


def test_trace_errors_do_not_export_remote_details() -> None:
    result = sanitize_events(
        [{"type": "error", "data": {"message": "secret transcript"}}]
    )
    assert result["error"] == "pipeline_failed"
    assert "secret" not in str(result)
