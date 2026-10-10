"""Context reconstruction never truncates JSON or duplicates the current turn."""

import json

from bridge.conversation_context import conversation_history


def test_budget_removes_whole_records_and_preserves_unicode() -> None:
    messages = [
        {"role": "user", "content": "old" * 1000},
        {"role": "assistant", "content": "Stehlampe ist aus."},
        {"role": "user", "content": "Weiter?"},
    ]
    context = conversation_history({"messages": messages}, "Weiter?", budget=100)
    assert context is not None
    retained = json.loads(context.split("\n", 1)[1])
    assert retained == [{"role": "assistant", "content": "Stehlampe ist aus."}]


def test_system_content_and_current_utterance_are_not_replayed() -> None:
    messages = [
        {"role": "system", "content": "Never import this"},
        {"role": "user", "content": "same"},
        {"role": "user", "content": "same"},
    ]
    context = conversation_history({"messages": messages}, "same")
    assert context is not None
    assert json.loads(context.split("\n", 1)[1]) == [
        {"role": "user", "content": "same"}
    ]
