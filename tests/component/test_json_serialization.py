"""Regression tests for normal Assist JSON payloads, including HA 2026.10."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import Enum
from pathlib import PurePosixPath
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock
from uuid import UUID

import orjson
import pytest
import voluptuous as vol
from aiohttp import web
from homeassistant.components import conversation
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_LLM_HASS_API
from homeassistant.core import Context, HomeAssistant
from homeassistant.helpers import llm
from homeassistant.setup import async_setup_component
from voluptuous_openapi import convert  # type: ignore[import-untyped]

from custom_components.codex_voice.api import (
    BridgeClient,
    BridgeProtocolError,
    _serialize_conversation_message,
)
from custom_components.codex_voice.conversation import CodexVoiceConversationEntity
from custom_components.codex_voice.llm_tools import (
    _PROBATIO,
    serialize_llm_tools,
    tool_result_data,
)


class Opaque:
    """An unsupported object's representation must never be invoked."""

    def __repr__(self) -> str:
        """Catch accidental default=str/repr/exception-value diagnostics."""
        pytest.fail("Private repr must never be used")


class StateLabel(Enum):
    """A supported enum preserves its value rather than its class label."""

    ON = "on"


@dataclass
class Reading:
    """Synthetic typed data returned by a Home Assistant tool."""

    state: StateLabel
    observed: datetime


@pytest.mark.parametrize(
    "tools",
    [[], [{"name": "synthetic", "parameters": {"type": "object", "properties": {}}}]],
)
def test_start_with_and_without_tools_normalizes_nested_ha_types(
    tools: list[dict[str, Any]],
) -> None:
    """Native dates, containers, enums, dataclasses and HA as_dict remain typed."""
    observed = datetime(2026, 10, 10, 12, 30, tzinfo=UTC)
    payload = {
        "type": "start",
        "tools": tools,
        "messages": [
            {
                "role": "tool_result",
                "result": {
                    "reading": Reading(StateLabel.ON, observed),
                    "day": date(2026, 10, 10),
                    "at": time(12, 30),
                    "set": frozenset({"light", "sensor"}),
                    "tuple": (1, False),
                    "path": PurePosixPath("/synthetic"),
                    "uuid": UUID("00000000-0000-0000-0000-000000000001"),
                    "context": Context(id="synthetic-context"),
                    "mapping": MappingProxyType({"value": 1.5}),
                    "fragment": orjson.Fragment('{"valid":true}'),
                },
            }
        ],
    }
    result = json.loads(_serialize_conversation_message(payload))
    data = result["messages"][0]["result"]
    assert result["tools"] == tools
    assert data["reading"] == {"state": "on", "observed": observed.isoformat()}
    assert data["day"] == "2026-10-10"
    assert data["at"] == "12:30:00"
    assert data["set"] == ["light", "sensor"]
    assert data["tuple"] == [1, False]
    assert data["mapping"] == {"value": 1.5}
    assert data["fragment"] == {"valid": True}
    assert data["context"]["id"] == "synthetic-context"


def test_diagnostics_log_only_redacted_path_and_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Neither private text, unknown dictionary keys nor argument values leak."""
    payload = {
        "text": "PRIVATE_TRANSCRIPT",
        "messages": [
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "arguments": {"PRIVATE_KEY": Opaque()},
                        "name": "PRIVATE_TOOL_NAME",
                    }
                ],
            }
        ],
        "access_token": "PRIVATE_TOKEN",
    }
    with (
        caplog.at_level(logging.WARNING, logger="custom_components.codex_voice.api"),
        pytest.raises(BridgeProtocolError),
    ):
        _serialize_conversation_message(payload)
    assert "$.messages[0].tool_calls[0].arguments[<key#0>]" in caplog.text
    assert "Opaque" in caplog.text
    assert "PRIVATE_" not in caplog.text


@pytest.mark.parametrize(
    "bad", [Opaque(), b"private audio", float("nan"), float("inf"), 2**65, "\ud800"]
)
def test_unsupported_nested_values_fail_closed(bad: Any) -> None:
    """Unsupported objects/bytes and non-finite numbers are never stringified."""
    with pytest.raises(BridgeProtocolError):
        _serialize_conversation_message({"messages": [{"result": {"nested": [bad]}}]})


def test_cycles_and_colliding_json_keys_fail_closed() -> None:
    """Recursion and ambiguous key conversion cannot bypass strict JSON."""
    cyclic: list[Any] = []
    cyclic.append(cyclic)
    for value in (cyclic, {1: "first", "1": "second"}):
        with pytest.raises(BridgeProtocolError):
            _serialize_conversation_message({"messages": value})


@pytest.mark.skipif(
    _PROBATIO is None, reason="Native Probatio tools start in HA 2026.10"
)
def test_reproduces_old_schema_sentinel_and_converts_native_schema(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The exact pre-fix offender is $.tools[0].parameters, Probatio's marker."""
    assert _PROBATIO is not None
    parameters = _PROBATIO.Schema({_PROBATIO.Required("name"): str})
    bad_parameters = convert(parameters, custom_serializer=llm.selector_serializer)
    assert bad_parameters is _PROBATIO.UNSUPPORTED
    with (
        caplog.at_level(logging.WARNING, logger="custom_components.codex_voice.api"),
        pytest.raises(BridgeProtocolError),
    ):
        _serialize_conversation_message({"tools": [{"parameters": bad_parameters}]})
    assert "path=$.tools[0].parameters" in caplog.text
    assert (
        f"type={type(bad_parameters).__module__}.{type(bad_parameters).__qualname__}"
        in caplog.text
    )
    api = cast(
        "llm.APIInstance",
        SimpleNamespace(
            tools=[
                SimpleNamespace(
                    name="HassTurnOn", description="Synthetic", parameters=parameters
                )
            ],
            custom_serializer=None,
        ),
    )
    fixed = serialize_llm_tools(api)
    assert fixed[0]["parameters"]["type"] == "object"
    assert fixed[0]["parameters"]["properties"]["name"]["type"] == "string"
    assert (
        json.loads(_serialize_conversation_message({"tools": fixed}))["tools"] == fixed
    )


def test_legacy_voluptuous_schemas_and_selectors_still_work() -> None:
    """The correct sentinel is translated when a legacy tool uses current HA."""
    parameters = vol.Schema({vol.Required("name"): str})
    api = cast(
        "llm.APIInstance",
        SimpleNamespace(
            tools=[
                SimpleNamespace(
                    name="HassTurnOn", description="Synthetic", parameters=parameters
                )
            ],
            custom_serializer=llm.selector_serializer,
        ),
    )
    assert (
        serialize_llm_tools(api)[0]["parameters"]["properties"]["name"]["type"]
        == "string"
    )


async def test_builtin_assist_tool_schemas_serialize(hass: HomeAssistant) -> None:
    """Serialize the actual selected Assist API's available HA intent tools."""
    assert await async_setup_component(hass, "homeassistant", {})
    assert await async_setup_component(hass, "conversation", {})
    hass.states.async_set("light.synthetic", "off")
    context = llm.LLMContext(
        platform="codex_voice",
        context=Context(),
        language="de-DE",
        assistant="conversation",
        device_id=None,
    )
    instance = await llm.async_get_api(hass, llm.LLM_API_ASSIST, context)
    tools = serialize_llm_tools(instance)
    assert any(tool["name"].endswith("HassTurnOn") for tool in tools)
    assert any(tool["name"].endswith("HassTurnOff") for tool in tools)
    decoded = json.loads(_serialize_conversation_message({"tools": tools}))
    assert decoded["tools"] == tools
    assert all(isinstance(tool["parameters"], dict) for tool in decoded["tools"])
    assert hass.states.get("light.synthetic").state == "off"


class SyntheticTool(llm.Tool):
    """A normal HA LLM tool with a strictly allowlisted mock light target."""

    name = "HassTurnOn"
    description = "Turn on only the synthetic unit-test light"
    integration = "codex_voice"

    def __init__(self) -> None:
        """Use the schema engine provided by the tested Home Assistant version."""
        engine = _PROBATIO or vol
        self.parameters = engine.Schema(
            {engine.Required("entity_id"): engine.In(["light.synthetic"])}
        )

    async def async_call(
        self,
        hass: HomeAssistant,
        tool_input: llm.ToolInput,
        llm_context: llm.LLMContext,
    ) -> Any:
        """Validate with HA's actual schema engine before the mock side effect."""
        arguments = self.parameters(tool_input.tool_args)
        hass.states.async_set(arguments["entity_id"], "on")
        data = {"success": True, "observed": datetime(2026, 10, 10, tzinfo=UTC)}
        return llm.ToolResult(data=data) if hasattr(llm, "ToolResult") else data


class SyntheticAPI(llm.API):
    """Selected normal LLM API, independent of the realtime tool broker."""

    async def async_get_api_instance(
        self, llm_context: llm.LLMContext
    ) -> llm.APIInstance:
        """Return an actual APIInstance and actual HA Tool for validation."""
        return llm.APIInstance(
            api=self,
            api_prompt="Synthetic light only",
            llm_context=llm_context,
            tools=[SyntheticTool()],
        )


@pytest.mark.skipif(
    not hasattr(llm, "ToolResult"), reason="Typed results start in HA 2026.10"
)
def test_typed_error_flag_cannot_become_success() -> None:
    """HA's new error flag takes precedence even when data has no error key."""
    result = tool_result_data(llm.ToolResult(data={"success": True}, error=True))
    assert result["error"] == "tool_failed"


async def test_actual_ha_api_rejects_unapproved_target(hass: HomeAssistant) -> None:
    """Serialization leaves the selected tool's schema validation intact."""
    hass.states.async_set("light.synthetic", "off")
    hass.states.async_set("switch.nuc", "on")
    api = SyntheticAPI(hass=hass, id="synthetic-validation", name="Synthetic")
    context = llm.LLMContext(
        platform="codex_voice",
        context=Context(),
        language="de-DE",
        assistant="conversation",
        device_id=None,
    )
    instance = await api.async_get_api_instance(context)
    invalid = _PROBATIO.Invalid if _PROBATIO is not None else vol.Invalid
    with pytest.raises(invalid):
        await instance.async_call_tool(
            llm.ToolInput(tool_name="HassTurnOn", tool_args={"entity_id": "switch.nuc"})
        )
    assert hass.states.get("light.synthetic").state == "off"
    assert hass.states.get("switch.nuc").state == "on"


async def test_normal_conversation_tool_execution_over_real_client(
    hass: HomeAssistant, aiohttp_client: Any, socket_enabled: None
) -> None:
    """Normal Assist schemas, ChatLog, validation and tool-result JSON all work."""
    received: dict[str, Any] = {}
    hass.states.async_set("light.synthetic", "off")
    hass.states.async_set("switch.nuc", "on")
    api = SyntheticAPI(hass=hass, id="synthetic", name="Synthetic normal Assist API")
    unregister = llm.async_register_api(hass, api)

    async def bridge(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        received["start"] = await ws.receive_json()
        await ws.send_json({"type": "started", "conversation_id": "synthetic"})
        await ws.send_json(
            {
                "type": "tool_call",
                "call_id": "one",
                "name": "HassTurnOn",
                "arguments": {"entity_id": "light.synthetic"},
            }
        )
        received["result"] = await ws.receive_json()
        await ws.send_json({"type": "delta", "delta": "Lampe ist an."})
        await ws.send_json({"type": "done", "status": "completed"})
        await ws.close()
        return ws

    app = web.Application()
    app.router.add_get("/v1/conversation", bridge)
    client = await aiohttp_client(app)
    runtime = BridgeClient(client.session, str(client.make_url("")), "synthetic-token")
    entry = cast(
        Any,
        SimpleNamespace(
            runtime_data=runtime, entry_id="entry-json", async_start_reauth=Mock()
        ),
    )
    profile = ConfigSubentry(
        data=MappingProxyType({CONF_LLM_HASS_API: ["synthetic"]}),
        subentry_type="conversation",
        title="JSON regression",
        unique_id=None,
    )
    entity = CodexVoiceConversationEntity(entry, profile)
    entity.hass, entity.entity_id = hass, "conversation.json_regression"
    user = conversation.ConversationInput(
        text="Synthetische Lampe an",
        context=Context(),
        conversation_id=None,
        device_id=None,
        satellite_id=None,
        language="de-DE",
        agent_id=entity.entity_id,
    )
    try:
        result = await entity.async_process(user)
    finally:
        unregister()
    assert result.response.speech["plain"]["speech"] == "Lampe ist an."
    assert hass.states.get("light.synthetic").state == "on"
    assert hass.states.get("switch.nuc").state == "on"
    assert received["result"]["result"]["success"] is True
    assert received["result"]["result"]["observed"] == "2026-10-10T00:00:00+00:00"
    assert received["start"]["tools"][0]["parameters"]["properties"]["entity_id"][
        "enum"
    ] == ["light.synthetic"]
