"""Local light shortcuts and safe dispatch through existing HA intents."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hassil import RecognizeResult
from homeassistant.components import conversation
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import intent

from .const import DOMAIN
from .context_policy import normalized_text
from .tool_policy import ORDINARY_DOMAINS, light_action, select_tools

_LOCAL_ACTIONS = frozenset(
    {
        intent.INTENT_TURN_ON,
        intent.INTENT_TURN_OFF,
        intent.INTENT_TOGGLE,
        "HassLightSet",
    }
)
_LOCAL_READS = frozenset(
    {"HassGetState", "HassGetTemperature", "HassGetCurrentDate", "HassGetCurrentTime"}
)


@dataclass(frozen=True, slots=True)
class LocalResult:
    """One final local result or a bounded light-target clarification."""

    response: intent.IntentResponse
    pending_light_action: str | None = None


def _device_area(
    hass: HomeAssistant, user: conversation.ConversationInput
) -> str | None:
    device_id = user.device_id
    if user.satellite_id and (
        satellite := er.async_get(hass).async_get(user.satellite_id)
    ):
        if satellite.area_id:
            return satellite.area_id
        device_id = satellite.device_id or device_id
    if device_id and (device := dr.async_get(hass).async_get(device_id)):
        return device.area_id
    return None


def _light_target(
    hass: HomeAssistant,
    user: conversation.ConversationInput,
    target: str | None,
) -> dict[str, dict[str, Any]] | None:
    """Reuse HA exposure/name/area matching without a house-wide fallback."""
    base: dict[str, Any] = {"domains": {"light"}, "assistant": conversation.DOMAIN}
    area: str | None
    if target:
        named = intent.async_match_targets(
            hass,
            intent.MatchTargetsConstraints(**base, name=target, single_target=True),
        )
        if named.is_match and named.states:
            return {
                "domain": {"value": "light"},
                "name": {"value": named.states[0].entity_id},
            }
        area = target
    else:
        area = _device_area(hass, user)
    if area:
        match = intent.async_match_targets(
            hass, intent.MatchTargetsConstraints(**base, area_name=area)
        )
        if match.is_match and match.states:
            return {"domain": {"value": "light"}, "area": {"value": area}}
        return None
    match = intent.async_match_targets(
        hass, intent.MatchTargetsConstraints(**base, single_target=True)
    )
    if match.is_match and match.states:
        return {
            "domain": {"value": "light"},
            "name": {"value": match.states[0].entity_id},
        }
    return None


def _exclude_unsafe_intent(
    hass: HomeAssistant,
    result: RecognizeResult,
    *,
    writes_requested: bool,
) -> bool:
    """Filter writes by actual exposed targets, before HA can execute them."""
    if result.intent.name in _LOCAL_READS:
        return False
    if not writes_requested or result.intent.name not in _LOCAL_ACTIONS:
        return True
    values = {entity.name: entity.value for entity in result.entities_list}
    domains = values.get("domain")
    if isinstance(domains, str):
        domains = {domains}
    constraints = intent.MatchTargetsConstraints(
        name=None if values.get("name") == "all" else values.get("name"),
        area_name=values.get("area"),
        domains=domains,
        assistant=conversation.DOMAIN,
    )
    if not constraints.has_constraints:
        return True
    match = intent.async_match_targets(hass, constraints)
    return (
        not match.is_match
        or not match.states
        or any(state.domain not in ORDINARY_DOMAINS for state in match.states)
    )


def _shorten_success(response: intent.IntentResponse) -> None:
    if (
        response.response_type is intent.IntentResponseType.ACTION_DONE
        and not response.failed_results
    ):
        response.async_set_speech("Okay.")


async def async_try_local_intent(
    hass: HomeAssistant,
    user: conversation.ConversationInput,
    chat_log: conversation.ChatLog,
    *,
    pending_light_action: str | None = None,
    allow_native: bool = True,
) -> LocalResult | None:
    """Use existing HA intent handlers, preserving final errors without retries."""
    action = light_action(user.text)
    writes_requested = select_tools(
        user.text, inherited_action=bool(pending_light_action)
    ).writes_requested
    target: str | None = None
    if action is None and pending_light_action:
        action, target = pending_light_action, normalized_text(user.text)
    if action and writes_requested:
        slots = _light_target(hass, user, target)
        if slots is None:
            response = intent.IntentResponse(user.language)
            response.async_set_speech(
                "Welches Licht oder welchen Raum meinst du?"
                if user.language.startswith("de")
                else "Which light or room do you mean?"
            )
            return LocalResult(response, action)
        try:
            response = await intent.async_handle(
                hass,
                DOMAIN,
                action,
                slots,
                text_input=user.text,
                context=user.context,
                language=user.language,
                assistant=conversation.DOMAIN,
                device_id=user.device_id,
                satellite_id=user.satellite_id,
                conversation_agent_id=user.agent_id,
            )
        except intent.IntentError:
            response = intent.IntentResponse(user.language)
            response.async_set_error(
                intent.IntentResponseErrorCode.FAILED_TO_HANDLE,
                "Ich konnte die Lichtaktion nicht abschließen. Bitte prüfe den Zustand."
                if user.language.startswith("de")
                else "I could not complete the light action. Please check its state.",
            )
        _shorten_success(response)
        return LocalResult(response)
    if not allow_native:
        return None
    native_response = await conversation.async_handle_intents(
        hass,
        user,
        chat_log,
        intent_filter=lambda result: _exclude_unsafe_intent(
            hass, result, writes_requested=writes_requested
        ),
    )
    if native_response is None:
        return None
    _shorten_success(native_response)
    return LocalResult(native_response)
