"""Tests for the Codex Voice config flow."""

from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant import config_entries
from homeassistant.components.homeassistant.const import DATA_EXPOSED_ENTITIES
from homeassistant.components.homeassistant.exposed_entities import ExposedEntities
from homeassistant.const import CONF_NAME, CONF_PROMPT
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import (  # type: ignore[import-untyped]
    MockConfigEntry,
)

from custom_components.codex_voice.api import BridgeAuthenticationError
from custom_components.codex_voice.const import (
    CONF_ACCESS_TOKEN,
    CONF_BRIDGE_URL,
    CONF_CONTEXT_MODE,
    CONF_REALTIME_AUTHORITY,
    CONF_REALTIME_LANGUAGE,
    CONF_REASONING_EFFORT,
    CONF_SERVICE_TIER,
    CONF_SESSION_METADATA,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    DEFAULT_CONVERSATION_SERVICE_TIER,
    DEFAULT_REALTIME_LANGUAGE,
    DOMAIN,
)
from custom_components.codex_voice.context_policy import (
    CONTEXT_ADAPTIVE,
    CONTEXT_LEGACY,
)


@pytest.fixture(autouse=True)
def mock_exposed_entities(hass: HomeAssistant) -> None:
    """Initialize the core exposed-entities interface used by conversation."""
    hass.data[DATA_EXPOSED_ENTITIES] = cast(
        "ExposedEntities",
        Mock(async_should_expose=Mock(return_value=False)),
    )


async def test_user_flow_creates_stable_provider_subentries(
    hass: HomeAssistant,
) -> None:
    """A validated bridge defaults to Conversation and TTS providers."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )
    assert result["type"] is FlowResultType.FORM

    with (
        patch(
            "custom_components.codex_voice.config_flow.BridgeClient.async_health",
            new_callable=AsyncMock,
            return_value={"status": "ok"},
        ),
        patch(
            "custom_components.codex_voice.async_setup_entry",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_BRIDGE_URL: "http://bridge.local:8099/",
                CONF_ACCESS_TOKEN: "bridge-token",
            },
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_BRIDGE_URL: "http://bridge.local:8099",
        CONF_ACCESS_TOKEN: "bridge-token",
    }
    assert [item["subentry_type"] for item in result["subentries"]] == [
        "conversation",
        "tts",
    ]
    assert result["subentries"][0]["data"][CONF_REASONING_EFFORT] == (
        DEFAULT_CONVERSATION_REASONING_EFFORT
    )
    assert result["subentries"][0]["data"][CONF_SERVICE_TIER] == (
        DEFAULT_CONVERSATION_SERVICE_TIER
    )
    assert result["subentries"][0]["data"][CONF_REALTIME_AUTHORITY] is False
    assert result["subentries"][0]["data"][CONF_CONTEXT_MODE] == CONTEXT_ADAPTIVE
    assert result["subentries"][0]["data"][CONF_SESSION_METADATA] is False
    assert (
        result["subentries"][0]["data"][CONF_REALTIME_LANGUAGE]
        == DEFAULT_REALTIME_LANGUAGE
    )


async def test_added_conversation_defaults_to_non_authoritative_mexican_spanish(
    hass: HomeAssistant,
) -> None:
    """Additional agents never silently take over realtime Home Assistant tools."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_BRIDGE_URL: "http://bridge.local:8787",
            CONF_ACCESS_TOKEN: "bridge-token",
        },
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, "conversation"),
        context={"source": config_entries.SOURCE_USER},
    )

    assert result["type"] is FlowResultType.FORM
    suggestions = {
        marker.schema: marker.description.get("suggested_value")
        for marker in result["data_schema"].schema
        if isinstance(marker.description, dict)
    }
    assert suggestions[CONF_REALTIME_AUTHORITY] is False
    assert suggestions[CONF_REALTIME_LANGUAGE] == "es-MX"


async def test_second_realtime_authority_is_rejected(
    hass: HomeAssistant,
) -> None:
    """The subentry flow prevents an ambiguous second authority."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_BRIDGE_URL: "http://bridge.local:8787",
            CONF_ACCESS_TOKEN: "bridge-token",
        },
        subentries_data=[
            {
                "data": {
                    CONF_REALTIME_AUTHORITY: True,
                    CONF_REALTIME_LANGUAGE: "es-MX",
                },
                "subentry_type": "conversation",
                "title": "Authority",
                "unique_id": None,
            }
        ],
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, "conversation"),
        context={"source": config_entries.SOURCE_USER},
    )
    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"],
        {
            CONF_NAME: "Second conversation",
            "model": "gpt-test",
            CONF_REASONING_EFFORT: "low",
            CONF_SERVICE_TIER: "standard",
            CONF_REALTIME_AUTHORITY: True,
            CONF_REALTIME_LANGUAGE: "es-MX",
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "realtime_authority_already_configured"}
    assert len(entry.subentries) == 1


async def test_experimental_stt_subentry_remains_manually_available(
    hass: HomeAssistant,
) -> None:
    """Existing users can explicitly add the diagnostic Codex STT provider."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_BRIDGE_URL: "http://bridge.local:8787",
            CONF_ACCESS_TOKEN: "bridge-token",
        },
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, "stt"),
        context={"source": config_entries.SOURCE_USER},
    )

    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.subentries.async_configure(
        result["flow_id"],
        {
            CONF_NAME: "Protocol diagnostic",
            CONF_PROMPT: "Known phrase",
        },
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "Protocol diagnostic"
    assert result["data"] == {CONF_PROMPT: "Known phrase"}
    subentry = next(iter(entry.subentries.values()))
    assert subentry.subentry_type == "stt"
    assert subentry.title == "Protocol diagnostic"


async def test_user_flow_reports_invalid_auth(hass: HomeAssistant) -> None:
    """An invalid bridge token remains on the form with an auth error."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )
    with patch(
        "custom_components.codex_voice.config_flow.BridgeClient.async_health",
        new_callable=AsyncMock,
        side_effect=BridgeAuthenticationError,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_BRIDGE_URL: "http://bridge.local:8099",
                CONF_ACCESS_TOKEN: "wrong-token",
            },
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}


async def test_legacy_conversation_reconfigure_suggests_safe_supported_values(
    hass: HomeAssistant,
) -> None:
    """A pre-tier profile opens with standard usage and a supported effort."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_BRIDGE_URL: "http://bridge.local:8787",
            CONF_ACCESS_TOKEN: "bridge-token",
        },
        subentries_data=[
            {
                "data": {
                    "model": "gpt-test",
                    CONF_REASONING_EFFORT: "none",
                },
                "subentry_type": "conversation",
                "title": "Legacy conversation",
                "unique_id": None,
            }
        ],
    )
    entry.add_to_hass(hass)
    subentry = next(iter(entry.subentries.values()))

    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, "conversation"),
        context={
            "source": config_entries.SOURCE_RECONFIGURE,
            "subentry_id": subentry.subentry_id,
        },
    )

    assert result["type"] is FlowResultType.FORM
    suggestions = {
        marker.schema: marker.description.get("suggested_value")
        for marker in result["data_schema"].schema
        if isinstance(marker.description, dict)
    }
    assert suggestions[CONF_REASONING_EFFORT] == (DEFAULT_CONVERSATION_REASONING_EFFORT)
    assert suggestions[CONF_SERVICE_TIER] == "standard"
    assert suggestions[CONF_CONTEXT_MODE] == CONTEXT_LEGACY
    assert CONF_CONTEXT_MODE not in subentry.data


async def test_duplicate_bridge_is_rejected(hass: HomeAssistant) -> None:
    """The normalized bridge URL is a unique config-entry id."""
    first = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )
    with (
        patch(
            "custom_components.codex_voice.config_flow.BridgeClient.async_health",
            new_callable=AsyncMock,
            return_value={"status": "ok"},
        ),
        patch(
            "custom_components.codex_voice.async_setup_entry",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        await hass.config_entries.flow.async_configure(
            first["flow_id"],
            {
                CONF_BRIDGE_URL: "http://bridge.local:8099",
                CONF_ACCESS_TOKEN: "bridge-token",
            },
        )
        second = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_USER},
        )
        second = await hass.config_entries.flow.async_configure(
            second["flow_id"],
            {
                CONF_BRIDGE_URL: "http://bridge.local:8099/",
                CONF_ACCESS_TOKEN: "another-token",
            },
        )

    assert second["type"] is FlowResultType.ABORT
    assert second["reason"] == "already_configured"


async def test_reconfigure_updates_bridge_unique_id(hass: HomeAssistant) -> None:
    """Changing bridge URL releases the old unique ID and claims the new one."""
    flow = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )
    with (
        patch(
            "custom_components.codex_voice.config_flow.BridgeClient.async_health",
            new_callable=AsyncMock,
            return_value={"status": "ok"},
        ),
        patch(
            "custom_components.codex_voice.async_setup_entry",
            new_callable=AsyncMock,
            return_value=True,
        ),
    ):
        await hass.config_entries.flow.async_configure(
            flow["flow_id"],
            {
                CONF_BRIDGE_URL: "http://old-bridge.local:8099",
                CONF_ACCESS_TOKEN: "old-token",
            },
        )
        entry = hass.config_entries.async_entries(DOMAIN)[0]
        reconfigure = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={
                "source": config_entries.SOURCE_RECONFIGURE,
                "entry_id": entry.entry_id,
            },
        )
        result = await hass.config_entries.flow.async_configure(
            reconfigure["flow_id"],
            {
                CONF_BRIDGE_URL: "http://new-bridge.local:8787/",
                CONF_ACCESS_TOKEN: "new-token",
            },
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.unique_id == "http://new-bridge.local:8787"
    assert entry.data == {
        CONF_BRIDGE_URL: "http://new-bridge.local:8787",
        CONF_ACCESS_TOKEN: "new-token",
    }


async def test_reconfigure_rejects_another_bridge_unique_id(
    hass: HomeAssistant,
) -> None:
    """Reconfiguration cannot claim another config entry's bridge URL."""
    first = MockConfigEntry(
        domain=DOMAIN,
        unique_id="http://first-bridge.local:8787",
        data={
            CONF_BRIDGE_URL: "http://first-bridge.local:8787",
            CONF_ACCESS_TOKEN: "first-token",
        },
    )
    first.add_to_hass(hass)
    MockConfigEntry(
        domain=DOMAIN,
        unique_id="http://second-bridge.local:8787",
        data={
            CONF_BRIDGE_URL: "http://second-bridge.local:8787",
            CONF_ACCESS_TOKEN: "second-token",
        },
    ).add_to_hass(hass)

    flow = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": config_entries.SOURCE_RECONFIGURE,
            "entry_id": first.entry_id,
        },
    )
    with patch(
        "custom_components.codex_voice.config_flow.BridgeClient.async_health",
        new_callable=AsyncMock,
        return_value={"status": "ok"},
    ):
        result = await hass.config_entries.flow.async_configure(
            flow["flow_id"],
            {
                CONF_BRIDGE_URL: "http://second-bridge.local:8787/",
                CONF_ACCESS_TOKEN: "replacement-token",
            },
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert first.unique_id == "http://first-bridge.local:8787"
    assert first.data[CONF_ACCESS_TOKEN] == "first-token"
