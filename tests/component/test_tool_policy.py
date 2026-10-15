"""Capability selection and pre-dispatch authorization without model routing."""

from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.codex_voice.tool_policy import (
    ToolAuthorizationRequired,
    authorize_tool,
    light_action,
    select_tools,
)


@pytest.mark.parametrize(
    "text",
    [
        "Was ist Photosynthese?",
        "Wie funktioniert Licht?",
        "Was ist Energie?",
        "Erklär Quantenphysik",
        "Mach mir einen Witz",
        "Mach ein Gedicht",
        "What is a thermostat?",
        "Was kostet ein NUC?",
        "Wie viel Strom verbraucht ein NUC?",
        "How much power does a NUC use?",
        "Mach ein Gedicht über Licht",
    ],
)
def test_background_knowledge_needs_no_home_tools(text: str) -> None:
    """Knowledge and creative tasks neither prepare HA nor authorize writes."""
    selected = select_tools(text)
    assert not selected.home_assistant
    assert not selected.writes_requested


@pytest.mark.parametrize(
    ("text", "writes"),
    [
        ("Wie viel Strom verbraucht mein NUC?", False),
        ("Wie funktioniert meine Heizung?", False),
        ("Ist das Licht an?", False),
        ("Wie ist das Licht an der Wand?", False),
        ("Schalte die Lampe an", True),
        ("Licht an", True),
        ("Licht aus", True),
        ("Computer, Licht", True),
    ],
)
def test_live_home_questions_and_explicit_actions(text: str, writes: bool) -> None:
    """Read questions and explicit actions have separate authority."""
    selected = select_tools(text)
    assert selected.home_assistant
    assert selected.writes_requested is writes


@pytest.mark.parametrize(
    ("text", "action"),
    [
        ("Computer, Licht.", "HassToggle"),
        ("das Licht", "HassToggle"),
        ("Licht an!", "HassTurnOn"),
        ("mach das Licht aus", "HassTurnOff"),
        ("schalte die Lampen um", "HassToggle"),
        ("Ist das Licht an?", None),
        ("Was bedeutet Licht?", None),
        ("Licht an oder aus?", None),
    ],
)
def test_light_shortcuts_are_whole_commands(text: str, action: str | None) -> None:
    """A question or explanation request never becomes a toggle."""
    assert light_action(text) == action


def test_current_and_private_capabilities_are_separate_and_not_executed() -> None:
    """Recognizing a capability grants no web, Memory or account access."""
    current = select_tools("Was sind die neuesten Nachrichten heute?")
    assert current.current_information and not current.home_assistant
    private = select_tools("Was steht in meinen ChatGPT-Memory Notizen?")
    assert private.personal_information and not private.home_assistant
    inherited = select_tools("Wohnzimmer", inherited_home=True, inherited_action=True)
    assert inherited.home_assistant and inherited.writes_requested
    assert not select_tools("Wohnzimmer").writes_requested


@pytest.mark.parametrize(
    ("name", "arguments", "writes", "ui", "allowed"),
    [
        ("GetLiveContext", {}, False, False, True),
        ("HassTurnOn", {"domain": "light"}, True, False, True),
        ("HassToggle", {"entity_id": "light.room"}, True, False, True),
        ("HassTurnOff", {"domain": "light"}, False, True, False),
        ("HassTurnOn", {"name": "Kitchen"}, True, False, False),
        ("HassTurnOn", {"entity_id": ["light.room", "lock.door"]}, True, False, False),
        ("HassTurnOn", {"domain": "lock"}, True, False, False),
        ("HassTurnOn", {"domain": "lock"}, True, True, True),
        ("run_shell", {}, True, True, False),
        ("plugin__HassTurnOn", {"domain": "light"}, True, False, True),
    ],
)
def test_authority_is_checked_before_dispatch(
    name: str,
    arguments: dict[str, Any],
    writes: bool,
    ui: bool,
    allowed: bool,
) -> None:
    """Neither model-selected names nor shared speakers authorize sensitive writes."""
    tool = SimpleNamespace(name=name)
    if allowed:
        authorize_tool(tool, arguments, writes_requested=writes, authenticated_ui=ui)
    else:
        with pytest.raises(ToolAuthorizationRequired):
            authorize_tool(
                tool, arguments, writes_requested=writes, authenticated_ui=ui
            )


def test_native_read_only_metadata_is_respected() -> None:
    """An API's explicit read-only annotation can authorize a custom reader."""
    authorize_tool(
        SimpleNamespace(
            name="read_custom", annotations=SimpleNamespace(read_only=True)
        ),
        {},
        writes_requested=False,
        authenticated_ui=False,
    )


@pytest.mark.parametrize(
    "text",
    [
        "nein",
        "nein danke",
        "no",
        "Schalte die Lampe nicht an",
        "Don't turn on the light",
    ],
)
def test_negative_answer_or_command_never_authorizes_a_write(text: str) -> None:
    """A rejection revokes even previously requested action authority."""
    selected = select_tools(text, inherited_home=True, inherited_action=True)
    assert not selected.writes_requested
