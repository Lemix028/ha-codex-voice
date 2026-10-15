"""Single-turn-first behavior through native HA ChatLogs and bridge callbacks."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.components import conversation
from homeassistant.config_entries import ConfigSubentry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import llm

from custom_components.codex_voice.api import BridgeToolCall
from custom_components.codex_voice.const import (
    CONF_CONTEXT_MODE,
    CONF_SESSION_CONTINUITY,
)
from custom_components.codex_voice.context_policy import CONTEXT_ADAPTIVE
from custom_components.codex_voice.conversation import CodexVoiceConversationEntity

from .helpers import tool_result
from .test_conversation import _make_entry, _make_input
from .test_local_intents import (
    add_light,
    mock_light_calls,  # noqa: F401 - shared pytest fixture
)


def adaptive_entity(
    hass: HomeAssistant,
    client: Any,
    **options: Any,
) -> CodexVoiceConversationEntity:
    """Construct an adaptive profile without altering legacy fixture defaults."""
    subentry = ConfigSubentry(
        data=MappingProxyType({CONF_CONTEXT_MODE: CONTEXT_ADAPTIVE, **options}),
        subentry_type="conversation",
        title="Computer",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(_make_entry(client), subentry)
    entity.entity_id, entity.hass = "conversation.computer", hass
    return entity


def user(text: str, *, device: str | None = "echo-a") -> conversation.ConversationInput:
    """Keep native HA IDs stable to test exclusion of old ChatLog history."""
    return replace(_make_input(), text=text, device_id=device, language="de-DE")


async def acknowledge(callbacks: dict[str, Any]) -> None:
    """Echo the versioned fresh-thread contract, without contacting a provider."""
    await callbacks["async_handle_started"](
        {"context_policy": CONTEXT_ADAPTIVE, "context_status": "new"}
    )


async def test_independent_follow_up_topic_change_and_explicit_continuation(
    hass: HomeAssistant,
) -> None:
    """One bridge turn per question, and only intentionally selected RAM history."""
    requests: list[dict[str, Any]] = []

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        requests.append(payload)
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"]("Eine vollständige Antwort.")

    entity = adaptive_entity(
        hass,
        SimpleNamespace(async_converse=AsyncMock(side_effect=converse)),
        llm_hass_api="assist",
    )
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ) as prepare:
        for text in (
            "Was ist ein NUC?",
            "Und was kostet das?",
            "Was ist Photosynthese?",
            "Erklär das genauer",
            "weiter",
        ):
            await entity.async_process(user(text))
    assert [payload["context_selection"] for payload in requests] == [
        "independent",
        "follow_up",
        "independent",
        "follow_up",
        "explicit",
    ]
    assert requests[0]["messages"] == [{"role": "user", "content": "Was ist ein NUC?"}]
    assert requests[2]["messages"] == [
        {"role": "user", "content": "Was ist Photosynthese?"}
    ]
    assert "NUC" in str(requests[1]["messages"])
    assert "NUC" not in str(requests[3]["messages"])
    assert len({payload["conversation_id"] for payload in requests}) == 5
    assert len({payload["request_id"] for payload in requests}) == 5
    assert all(not payload["tools"] for payload in requests)
    # NUC is a live-home heuristic, so the explicit concept question must override it.
    assert all(call.args[1] is None for call in prepare.await_args_list)
    assert all(
        trace["bridge_turn_requests"] == 1 for trace in entity._diagnostics.export()
    )


async def test_matching_answer_uses_question_and_new_command_does_not(
    hass: HomeAssistant,
) -> None:
    """Model questions use metadata, not an extra model-based context classifier."""
    requests: list[dict[str, Any]] = []

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        requests.append(payload)
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"](
            "Welchen Stromtarif hast du?"
            if len(requests) == 1
            else "Die Kosten sind berechnet."
        )

    entity = adaptive_entity(hass, SimpleNamespace(async_converse=converse))
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        await entity.async_process(user("Berechne Stromkosten"))
        await entity.async_process(user("30 Cent pro kWh"))
        await entity.async_process(user("Licht an"))
    assert requests[1]["context_selection"] == "question_answer"
    assert "Stromkosten" in str(requests[1]["messages"])
    assert requests[2]["context_selection"] == "independent"
    assert len(requests[2]["messages"]) == 1


async def test_other_satellite_cannot_read_context_even_with_same_ha_id(
    hass: HomeAssistant,
) -> None:
    """Foreign HA ChatLog contents never become a different speaker's context."""

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"]("synthetic private answer")

    client = SimpleNamespace(async_converse=AsyncMock(side_effect=converse))
    entity = adaptive_entity(hass, client)
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        await entity.async_process(user("Was ist ein NUC?"))
        result = await entity.async_process(user("Erklär das genauer", device="echo-b"))
    assert "synthetic private" not in str(result.response.as_dict())
    assert "Thema" in result.response.speech["plain"]["speech"]
    client.async_converse.assert_awaited_once()


async def test_reset_is_local_and_context_is_not_reused(hass: HomeAssistant) -> None:
    """Adaptive reset needs neither a bridge nor a cached remote thread."""

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"]("Eine Antwort.")

    client = SimpleNamespace(
        async_converse=AsyncMock(side_effect=converse),
        async_reset_conversation=AsyncMock(),
    )
    entity = adaptive_entity(hass, client)
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        await entity.async_process(user("Eine Frage"))
        await entity.async_process(user("Computer, neues Gespräch"))
        result = await entity.async_process(user("weiter"))
    assert "Thema" in result.response.speech["plain"]["speech"]
    client.async_converse.assert_awaited_once()
    client.async_reset_conversation.assert_not_awaited()


async def test_shortcut_and_target_answer_need_no_model(
    hass: HomeAssistant,
    light_calls: list[ServiceCall],
) -> None:
    """A local target question is answered locally, with structured success context."""
    kitchen = ar.async_get(hass).async_create("Küche")
    target = add_light(hass, "kitchen", area=kitchen.id)
    add_light(hass, "other")
    client = SimpleNamespace(async_converse=AsyncMock())
    entity = adaptive_entity(hass, client, llm_hass_api="assist")
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ) as prepare:
        first = await entity.async_process(user("Computer, Licht"))
        second = await entity.async_process(user("Küche"))
    assert first.continue_conversation
    assert second.response.speech["plain"]["speech"] == "Okay."
    assert not second.continue_conversation
    assert light_calls[0].data["entity_id"] == target
    client.async_converse.assert_not_awaited()
    prepare.assert_not_awaited()
    session = next(iter(entity._sessions._sessions.values()))
    assert target in str(session.history)
    assert session.pending_question is None and session.pending_light_action is None
    assert all(
        trace["bridge_turn_requests"] == 0 for trace in entity._diagnostics.export()
    )


async def test_old_bridge_fails_closed_before_any_tool(hass: HomeAssistant) -> None:
    """Missing policy acknowledgement cannot silently select legacy history."""

    async def old_bridge(payload: dict[str, Any], **callbacks: Any) -> None:
        await callbacks["async_handle_started"]({"context_status": "new"})
        pytest.fail("The missing acknowledgement must stop the request")

    client = SimpleNamespace(async_converse=AsyncMock(side_effect=old_bridge))
    entity = adaptive_entity(hass, client)
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        result = await entity.async_process(user("Eine Frage"))
    assert result.response.error_code is not None
    assert not next(iter(entity._sessions._sessions.values())).history
    client.async_converse.assert_awaited_once()


@pytest.mark.parametrize(
    ("text", "domain", "allowed"),
    [
        ("Ist die Lampe an?", "light", False),
        ("Schalte die Lampe an", "light", True),
        ("Schalte die Lampe nicht an", "light", False),
        ("Öffne die Haustür", "lock", False),
    ],
)
async def test_actual_tool_authorization_before_ha_dispatch(
    hass: HomeAssistant,
    text: str,
    domain: str,
    allowed: bool,
) -> None:
    """Read questions and shared-speaker requests cannot authorize sensitive writes."""
    api = SimpleNamespace(
        tools=[SimpleNamespace(name="HassTurnOn")],
        custom_serializer=None,
        async_call_tool=AsyncMock(return_value=tool_result({"success": True})),
    )

    async def prepare(log: conversation.ChatLog, *args: Any, **kwargs: Any) -> None:
        log.llm_api = cast("llm.APIInstance", api)

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        await callbacks["async_handle_tool"](
            BridgeToolCall("one", "HassTurnOn", {"domain": domain})
        )
        await callbacks["async_handle_delta"]("Okay.")

    entity = adaptive_entity(hass, SimpleNamespace(async_converse=converse))
    with (
        patch.object(conversation.ChatLog, "async_provide_llm_data", new=prepare),
        patch(
            "custom_components.codex_voice.conversation._serialize_tools",
            return_value=[],
        ),
    ):
        result = await entity.async_process(user(text))
    assert api.async_call_tool.await_count == int(allowed)
    assert (result.response.error_code is None) is allowed
    assert entity._diagnostics.export()[-1]["tool_calls_executed"] == int(allowed)


async def test_cancelled_adaptive_turn_clears_context_without_retry(
    hass: HomeAssistant,
) -> None:
    """Cancellation never preserves an unfinished action or guessed answer."""
    entered = asyncio.Event()

    async def hang(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        entered.set()
        await asyncio.Event().wait()

    client = SimpleNamespace(async_converse=AsyncMock(side_effect=hang))
    entity = adaptive_entity(hass, client)
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        task = asyncio.create_task(entity.async_process(user("Eine Frage")))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not next(iter(entity._sessions._sessions.values())).history
    assert entity._diagnostics.export()[-1]["error"] == "cancelled_outcome_unknown"
    client.async_converse.assert_awaited_once()


async def test_disabled_context_cache_does_not_inherit_history(
    hass: HomeAssistant,
) -> None:
    """Disabling continuity also disables adaptive RAM reuse."""

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"]("Eine Antwort.")

    client = SimpleNamespace(async_converse=AsyncMock(side_effect=converse))
    entity = adaptive_entity(hass, client, **{CONF_SESSION_CONTINUITY: False})
    with patch.object(
        conversation.ChatLog, "async_provide_llm_data", new_callable=AsyncMock
    ):
        await entity.async_process(user("Eine Frage"))
        await entity.async_process(user("Erklär das genauer"))
    client.async_converse.assert_awaited_once()
    assert not next(iter(entity._sessions._sessions.values())).history


async def test_real_prompt_preparation_drops_old_home_api_context(
    hass: HomeAssistant,
) -> None:
    """A new knowledge topic removes both old tools and old rendered HA context."""
    requests: list[dict[str, Any]] = []

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        requests.append(payload)
        await acknowledge(callbacks)
        await callbacks["async_handle_delta"]("Eine vollständige Antwort.")

    entity = adaptive_entity(
        hass, SimpleNamespace(async_converse=converse), llm_hass_api="assist"
    )
    api = cast(
        "llm.APIInstance",
        SimpleNamespace(
            api_prompt="SYNTHETIC_HOME_CONTEXT", tools=[], custom_serializer=None
        ),
    )
    with (
        patch.object(
            llm, "async_get_api", new_callable=AsyncMock, return_value=api
        ) as prepare_api,
        patch(
            "custom_components.codex_voice.conversation.async_try_local_intent",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        await entity.async_process(user("Wie viel Strom verbraucht mein NUC?"))
        await entity.async_process(user("Was ist Photosynthese?"))
    prepare_api.assert_awaited_once()
    assert "SYNTHETIC_HOME_CONTEXT" in requests[0]["instructions"]
    assert "SYNTHETIC_HOME_CONTEXT" not in requests[1]["instructions"]
    assert requests[1]["messages"] == [
        {"role": "user", "content": "Was ist Photosynthese?"}
    ]
    assert requests[1]["tools"] == []


async def test_refusal_on_open_action_question_revokes_tool_authority(
    hass: HomeAssistant,
) -> None:
    """Even a misbehaving model cannot turn a 'no' answer into a device action."""
    api = SimpleNamespace(
        tools=[SimpleNamespace(name="HassTurnOn")],
        custom_serializer=None,
        async_call_tool=AsyncMock(return_value=tool_result({"success": True})),
    )

    async def prepare(log: conversation.ChatLog, *args: Any, **kwargs: Any) -> None:
        log.llm_api = cast("llm.APIInstance", api)

    async def converse(payload: dict[str, Any], **callbacks: Any) -> None:
        await acknowledge(callbacks)
        if payload["text"] == "nein":
            await callbacks["async_handle_tool"](
                BridgeToolCall("one", "HassTurnOn", {"domain": "light"})
            )
            pytest.fail("A refusal must stop action dispatch")
        await callbacks["async_handle_delta"]("Soll ich die Lampe einschalten?")

    entity = adaptive_entity(hass, SimpleNamespace(async_converse=converse))
    with (
        patch.object(conversation.ChatLog, "async_provide_llm_data", new=prepare),
        patch(
            "custom_components.codex_voice.conversation._serialize_tools",
            return_value=[],
        ),
    ):
        await entity.async_process(user("Schalte die Lampe ein"))
        result = await entity.async_process(user("nein"))
    api.async_call_tool.assert_not_awaited()
    assert result.response.error_code is not None
    assert entity._diagnostics.export()[-1]["context_choice"] == "question_answer"
    assert entity._diagnostics.export()[-1]["error"] == "tool_authorization_required"
