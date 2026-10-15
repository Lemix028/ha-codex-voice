"""Local light commands use real HA target matching and native intent handlers."""

from __future__ import annotations

from dataclasses import replace
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant.components import conversation
from homeassistant.components.homeassistant.const import DATA_EXPOSED_ENTITIES
from homeassistant.components.homeassistant.exposed_entities import ExposedEntities
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import intent
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.codex_voice.local_intents import async_try_local_intent

from .test_conversation import _make_chat_log, _make_input


@pytest.fixture(name="light_calls")
def mock_light_calls(hass: HomeAssistant) -> list[ServiceCall]:
    """Register native light intents, real services and synthetic exposure policy."""
    calls: list[ServiceCall] = []
    hass.data[DATA_EXPOSED_ENTITIES] = cast(
        "ExposedEntities",
        Mock(
            async_should_expose=Mock(
                side_effect=lambda assistant, entity_id: (
                    not entity_id.endswith("hidden")
                )
            )
        ),
    )

    async def control(call: ServiceCall) -> None:
        calls.append(call)
        entity_ids = call.data["entity_id"]
        if isinstance(entity_ids, str):
            entity_ids = [entity_ids]
        for entity_id in entity_ids:
            state = hass.states.get(entity_id)
            assert state is not None
            value = (
                "on"
                if call.service == "turn_on"
                or (call.service == "toggle" and state.state == "off")
                else "off"
            )
            hass.states.async_set(entity_id, value, state.attributes)

    for name, service in (
        (intent.INTENT_TURN_ON, "turn_on"),
        (intent.INTENT_TURN_OFF, "turn_off"),
        (intent.INTENT_TOGGLE, "toggle"),
    ):
        intent.async_register(hass, intent.ServiceIntentHandler(name, "light", service))
        hass.services.async_register("light", service, control)
    return calls


def add_light(hass: HomeAssistant, name: str, *, area: str | None = None) -> str:
    """Create a synthetic registered light for HA's native area/name resolver."""
    entry = er.async_get(hass).async_get_or_create(
        "light", "test", name, original_name=name
    )
    if area:
        er.async_get(hass).async_update_entity(entry.entity_id, area_id=area)
    hass.states.async_set(entry.entity_id, "off", {"friendly_name": name})
    return entry.entity_id


@pytest.mark.parametrize(
    ("text", "service"),
    [
        ("Computer, Licht", "toggle"),
        ("Licht an", "turn_on"),
        ("Licht aus", "turn_off"),
    ],
)
async def test_shortcut_uses_single_exposed_light(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
    text: str,
    service: str,
) -> None:
    """No model or live-state narration is needed for an unambiguous light."""
    entity_id = add_light(hass, "lamp")
    add_light(hass, "hidden")
    user = replace(_make_input(), text=text, language="de-DE")
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None and result.pending_light_action is None
    assert result.response.speech["plain"]["speech"] == "Okay."
    assert len(light_calls) == 1
    assert light_calls[0].service == service
    assert entity_id in light_calls[0].data["entity_id"]
    assert light_calls[0].context == user.context


async def test_toggle_really_reverses_state(
    hass: HomeAssistant, light_calls: list[ServiceCall]
) -> None:
    """Success means the native toggle executed once per wakeword, in both states."""
    entity_id = add_light(hass, "lamp")
    user = replace(_make_input(), text="Licht", language="de-DE")
    for expected in ("on", "off"):
        result = await async_try_local_intent(hass, user, _make_chat_log(hass))
        assert result is not None
        assert hass.states.get(entity_id).state == expected
    assert len(light_calls) == 2


async def test_room_scope_does_not_toggle_another_room(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """All exposed lights in the satellite room are allowed, others stay untouched."""
    kitchen = ar.async_get(hass).async_create("Küche")
    bedroom = ar.async_get(hass).async_create("Schlafzimmer")
    first = add_light(hass, "kitchen-a", area=kitchen.id)
    second = add_light(hass, "kitchen-b", area=kitchen.id)
    other = add_light(hass, "bedroom", area=bedroom.id)
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    echo = dr.async_get(hass).async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "echo")}
    )
    dr.async_get(hass).async_update_device(echo.id, area_id=kitchen.id)
    user = replace(
        _make_input(), text="Computer, Licht", device_id=echo.id, language="de-DE"
    )
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None and not result.pending_light_action
    assert {call.data["entity_id"] for call in light_calls} == {first, second}
    assert hass.states.get(other).state == "off"


async def test_missing_room_light_never_falls_back_to_whole_house(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """A satellite with an empty room cannot accidentally control other rooms."""
    empty = ar.async_get(hass).async_create("Empty")
    other = ar.async_get(hass).async_create("Other")
    add_light(hass, "other", area=other.id)
    satellite = er.async_get(hass).async_get_or_create(
        "assist_satellite", "test", "echo"
    )
    er.async_get(hass).async_update_entity(satellite.entity_id, area_id=empty.id)
    user = replace(
        _make_input(), text="Licht", satellite_id=satellite.entity_id, language="de-DE"
    )
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None and result.pending_light_action == "HassToggle"
    assert result.response.speech["plain"]["speech"].endswith("?")
    assert light_calls == []


async def test_ambiguous_light_asks_and_answer_uses_explicit_room(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """Only a matching named room answers a pending local target question."""
    kitchen = ar.async_get(hass).async_create("Küche")
    target = add_light(hass, "kitchen", area=kitchen.id)
    add_light(hass, "other")
    user = replace(_make_input(), text="Licht", language="de-DE")
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None and result.pending_light_action == "HassToggle"
    assert light_calls == []
    follow_up = replace(user, text="Küche")
    result = await async_try_local_intent(
        hass, follow_up, _make_chat_log(hass), pending_light_action="HassToggle"
    )
    assert result is not None and result.pending_light_action is None
    assert light_calls[0].data["entity_id"] == target


async def test_failed_action_is_final_and_not_shorted_to_okay(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """A possibly completed write is not repeated or described as successful."""
    add_light(hass, "lamp")
    user = replace(_make_input(), text="Licht an", language="de-DE")
    with patch(
        "homeassistant.helpers.intent.async_handle",
        new_callable=AsyncMock,
        side_effect=intent.IntentHandleError("synthetic failure"),
    ) as dispatch:
        result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None
    assert result.response.response_type is intent.IntentResponseType.ERROR
    assert result.response.speech["plain"]["speech"] != "Okay."
    dispatch.assert_awaited_once()
    assert light_calls == []


async def test_state_question_is_not_a_toggle(
    hass: HomeAssistant, light_calls: list[ServiceCall]
) -> None:
    """State questions can reach native read recognition, without write dispatch."""
    add_light(hass, "lamp")
    with patch.object(
        conversation, "async_handle_intents", new_callable=AsyncMock, return_value=None
    ) as recognize:
        result = await async_try_local_intent(
            hass, replace(_make_input(), text="Ist das Licht an?"), _make_chat_log(hass)
        )
    assert result is None
    recognize.assert_awaited_once()
    assert light_calls == []


async def test_partial_native_failure_is_not_hidden(hass: HomeAssistant) -> None:
    """The short acknowledgement never replaces error or partial-failure speech."""
    response = intent.IntentResponse("de")
    response.async_set_speech("Eine Lampe konnte nicht geschaltet werden.")
    response.failed_results.append(
        intent.IntentResponseTarget(
            "Lamp", intent.IntentResponseTargetType.ENTITY, "light.lamp"
        )
    )
    with patch.object(
        conversation,
        "async_handle_intents",
        new_callable=AsyncMock,
        return_value=response,
    ):
        result = await async_try_local_intent(
            hass,
            replace(_make_input(), text="Schalte die Küche aus"),
            _make_chat_log(hass),
        )
    assert result is not None
    assert (
        result.response.speech["plain"]["speech"]
        == "Eine Lampe konnte nicht geschaltet werden."
    )


async def test_full_sentence_uses_native_recognition_and_intent(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """A normal named command uses the actual HA recognizer and no LLM router."""
    assert await async_setup_component(hass, "homeassistant", {})
    assert await async_setup_component(hass, "conversation", {})
    # Conversation dependencies may install their generic handlers; exercise those.
    entity_id = add_light(hass, "Leselampe")
    user = replace(_make_input(), text="Schalte die Leselampe ein", language="de-DE")
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is not None
    assert result.response.speech["plain"]["speech"] == "Okay."
    assert len(light_calls) == 1
    assert entity_id in light_calls[0].data["entity_id"]


async def test_native_recognition_filters_sensitive_action_before_execution(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """A parsed lock command cannot pass the ordinary local action filter."""
    assert await async_setup_component(hass, "conversation", {})
    entry = er.async_get(hass).async_get_or_create(
        "lock", "test", "door", original_name="Haustür"
    )
    hass.states.async_set(entry.entity_id, "locked", {"friendly_name": "Haustür"})
    lock = AsyncMock()
    hass.services.async_register("lock", "unlock", lock)
    user = replace(_make_input(), text="Öffne die Haustür", language="de-DE")
    result = await async_try_local_intent(hass, user, _make_chat_log(hass))
    assert result is None
    lock.assert_not_awaited()
    assert light_calls == []
