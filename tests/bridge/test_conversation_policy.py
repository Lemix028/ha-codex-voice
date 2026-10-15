from typing import Any

import pytest

from bridge.conversation_policy import (
    SINGLE_TURN_FIRST,
    adaptive_request,
    registered_tools,
)
from bridge.errors import ProtocolError


def request(**overrides: Any) -> dict[str, Any]:
    return {
        "context_policy": SINGLE_TURN_FIRST,
        "context_selection": "independent",
        "text": "synthetic question",
        "request_id": "a" * 32,
        "messages": [{"role": "user", "content": "synthetic question"}],
        **overrides,
    }


def test_legacy_request_is_unchanged() -> None:
    assert not adaptive_request({"text": "legacy question"})


def test_independent_and_selected_context_contracts() -> None:
    assert adaptive_request(request())
    for choice in ("follow_up", "question_answer", "explicit"):
        assert adaptive_request(
            request(
                context_selection=choice,
                messages=[
                    {"role": "user", "content": "prior question"},
                    {"role": "assistant", "content": "prior answer"},
                    {"role": "user", "content": "synthetic question"},
                ],
            )
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"context_policy": "unknown"},
        {"context_selection": "clarify"},
        {"context_selection": []},
        {"text": ""},
        {"messages": []},
        {"request_id": "x" * 32},
        {"request_id": "a" * 31},
        {"context_selection": "follow_up"},
        {"messages": [{"role": "user", "content": "foreign text"}]},
        {
            "messages": [
                {"role": "assistant", "content": "old answer"},
                {"role": "user", "content": "synthetic question"},
            ]
        },
        {
            "context_selection": "follow_up",
            "messages": [{"role": "user", "content": "old question"}],
        },
    ],
)
def test_invalid_contract_fails_before_io(overrides: dict[str, Any]) -> None:
    with pytest.raises(ProtocolError):
        adaptive_request(request(**overrides))


def test_namespaces_do_not_authorize_other_tools() -> None:
    assert registered_tools(
        [
            {"type": "namespace", "name": "home", "tools": [{"name": "HassTurnOn"}]},
            {"name": "GetLiveContext"},
        ]
    ) == {("home", "HassTurnOn"), (None, "GetLiveContext")}
