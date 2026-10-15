"""Config flow for Codex Voice."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, override

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryData,
    ConfigSubentryFlow,
    SubentryFlowResult,
)
from homeassistant.const import CONF_LLM_HASS_API, CONF_NAME, CONF_PROMPT
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import llm
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    BooleanSelector,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    TemplateSelector,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
from homeassistant.helpers.typing import VolDictType
from yarl import URL

from .api import (
    BridgeAuthenticationError,
    BridgeClient,
    BridgeConnectionError,
    normalize_bridge_url,
)
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_BRIDGE_URL,
    CONF_CONTEXT_MODE,
    CONF_INSTRUCTIONS,
    CONF_MODEL,
    CONF_REALTIME_AUTHORITY,
    CONF_REALTIME_LANGUAGE,
    CONF_REASONING_EFFORT,
    CONF_SERVICE_TIER,
    CONF_SESSION_CONTINUITY,
    CONF_SESSION_FALLBACK,
    CONF_SESSION_METADATA,
    CONF_SESSION_TTL,
    CONF_VOICE,
    DEFAULT_BRIDGE_URL,
    DEFAULT_CONVERSATION_MODEL,
    DEFAULT_CONVERSATION_NAME,
    DEFAULT_CONVERSATION_REASONING_EFFORT,
    DEFAULT_CONVERSATION_SERVICE_TIER,
    DEFAULT_LLM_HASS_API,
    DEFAULT_REALTIME_LANGUAGE,
    DEFAULT_STT_NAME,
    DEFAULT_TTS_NAME,
    DEFAULT_VOICE,
    DOMAIN,
    LEGACY_CONVERSATION_SERVICE_TIER,
    SUBENTRY_TYPE_CONVERSATION,
    SUBENTRY_TYPE_STT,
    SUBENTRY_TYPE_TTS,
    SUPPORTED_LANGUAGES,
    SUPPORTED_REASONING_EFFORTS,
    SUPPORTED_SERVICE_TIERS,
    SUPPORTED_VOICES,
)
from .context_policy import CONTEXT_ADAPTIVE, CONTEXT_LEGACY, CONTEXT_MODES

_LOGGER = logging.getLogger(__name__)

_CONNECTION_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_BRIDGE_URL, default=DEFAULT_BRIDGE_URL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.URL)
        ),
        vol.Required(CONF_ACCESS_TOKEN): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


def _default_subentries() -> list[ConfigSubentryData]:
    """Return the stable default Assist provider subentries.

    Home Assistant composes STT, Conversation, and TTS providers independently.
    Codex realtime input transcription is not a deterministic STT API, so new
    installations use a native Wyoming/Whisper entity for that pipeline stage.
    The experimental STT subentry remains available for existing users and
    explicit diagnostics.
    """
    return [
        {
            "subentry_type": SUBENTRY_TYPE_CONVERSATION,
            "title": DEFAULT_CONVERSATION_NAME,
            "unique_id": None,
            "data": {
                CONF_MODEL: DEFAULT_CONVERSATION_MODEL,
                CONF_CONTEXT_MODE: CONTEXT_ADAPTIVE,
                CONF_REASONING_EFFORT: DEFAULT_CONVERSATION_REASONING_EFFORT,
                CONF_SERVICE_TIER: DEFAULT_CONVERSATION_SERVICE_TIER,
                CONF_PROMPT: llm.DEFAULT_INSTRUCTIONS_PROMPT,
                CONF_LLM_HASS_API: DEFAULT_LLM_HASS_API,
                # Realtime control is a distinct, privileged execution path.
                # Keep it disabled until the user explicitly enables one
                # Conversation subentry in its options flow.
                CONF_REALTIME_AUTHORITY: False,
                CONF_REALTIME_LANGUAGE: DEFAULT_REALTIME_LANGUAGE,
                CONF_SESSION_CONTINUITY: True,
                CONF_SESSION_TTL: 15,
                CONF_SESSION_METADATA: False,
            },
        },
        {
            "subentry_type": SUBENTRY_TYPE_TTS,
            "title": DEFAULT_TTS_NAME,
            "unique_id": None,
            "data": {
                CONF_VOICE: DEFAULT_VOICE,
            },
        },
    ]


async def _async_validate_connection(
    hass: HomeAssistant,
    data: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and normalize bridge connection data."""
    bridge_url = normalize_bridge_url(str(data[CONF_BRIDGE_URL]))
    parsed_url = URL(bridge_url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.host:
        raise BridgeConnectionError("Bridge URL must be an HTTP(S) URL")

    client = BridgeClient(
        async_get_clientsession(hass),
        bridge_url,
        str(data[CONF_ACCESS_TOKEN]),
    )
    await client.async_health()
    return {
        CONF_BRIDGE_URL: bridge_url,
        CONF_ACCESS_TOKEN: str(data[CONF_ACCESS_TOKEN]),
    }


class CodexVoiceConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the Codex Voice config flow."""

    VERSION = 1
    MINOR_VERSION = 2

    @override
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Set up a Codex Voice bridge."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                data = await _async_validate_connection(self.hass, user_input)
            except BridgeAuthenticationError:
                errors["base"] = "invalid_auth"
            except BridgeConnectionError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error validating the Codex Voice bridge")
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(data[CONF_BRIDGE_URL])
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="Codex Voice",
                    data=data,
                    subentries=_default_subentries(),
                )

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(
                _CONNECTION_SCHEMA,
                user_input,
            ),
            errors=errors,
        )

    async def async_step_reconfigure(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Reconfigure the bridge connection."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                data = await _async_validate_connection(self.hass, user_input)
            except BridgeAuthenticationError:
                errors["base"] = "invalid_auth"
            except BridgeConnectionError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error validating the Codex Voice bridge")
                errors["base"] = "unknown"
            else:
                existing_entry = (
                    self.hass.config_entries.async_entry_for_domain_unique_id(
                        DOMAIN,
                        data[CONF_BRIDGE_URL],
                    )
                )
                if (
                    existing_entry is not None
                    and existing_entry.entry_id != entry.entry_id
                ):
                    return self.async_abort(reason="already_configured")
                return self.async_update_reload_and_abort(
                    entry,
                    unique_id=data[CONF_BRIDGE_URL],
                    data_updates=data,
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=self.add_suggested_values_to_schema(
                _CONNECTION_SCHEMA,
                user_input or dict(entry.data),
            ),
            errors=errors,
        )

    async def async_step_reauth(
        self,
        entry_data: Mapping[str, Any],
    ) -> ConfigFlowResult:
        """Start reauthentication."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Accept a replacement bridge access token."""
        entry = self._get_reauth_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            candidate = {
                CONF_BRIDGE_URL: entry.data[CONF_BRIDGE_URL],
                CONF_ACCESS_TOKEN: user_input[CONF_ACCESS_TOKEN],
            }
            try:
                data = await _async_validate_connection(self.hass, candidate)
            except BridgeAuthenticationError:
                errors["base"] = "invalid_auth"
            except BridgeConnectionError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error reauthenticating Codex Voice")
                errors["base"] = "unknown"
            else:
                return self.async_update_reload_and_abort(
                    entry,
                    data_updates=data,
                    reason="reauth_successful",
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_ACCESS_TOKEN): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    )
                }
            ),
            errors=errors,
        )

    @classmethod
    @callback
    @override
    def async_get_supported_subentry_types(
        cls,
        config_entry: ConfigEntry,
    ) -> dict[str, type[ConfigSubentryFlow]]:
        """Return supported provider profile subentries."""
        return {
            SUBENTRY_TYPE_CONVERSATION: CodexVoiceSubentryFlow,
            SUBENTRY_TYPE_STT: CodexVoiceSubentryFlow,
            SUBENTRY_TYPE_TTS: CodexVoiceSubentryFlow,
        }


class CodexVoiceSubentryFlow(ConfigSubentryFlow):
    """Create and reconfigure Conversation, STT, and TTS profiles."""

    _values: dict[str, Any]

    @property
    def _is_new(self) -> bool:
        """Return whether a new subentry is being created."""
        return self.source == "user"

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Start creating a profile."""
        self._values = self._defaults_for_type()
        return await self.async_step_init(user_input)

    async def async_step_reconfigure(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Start reconfiguring a profile."""
        self._values = dict(self._get_reconfigure_subentry().data)
        if self._subentry_type == SUBENTRY_TYPE_CONVERSATION:
            self._values.setdefault(CONF_CONTEXT_MODE, CONTEXT_LEGACY)
            # Preserve the usage behavior of profiles created before service
            # tiers were configurable. New profiles explicitly store priority.
            self._values.setdefault(
                CONF_SERVICE_TIER,
                LEGACY_CONVERSATION_SERVICE_TIER,
            )
            if self._values.get(CONF_REASONING_EFFORT) not in (
                SUPPORTED_REASONING_EFFORTS
            ):
                self._values[CONF_REASONING_EFFORT] = (
                    DEFAULT_CONVERSATION_REASONING_EFFORT
                )
            if self._values.get(CONF_SERVICE_TIER) not in SUPPORTED_SERVICE_TIERS:
                self._values[CONF_SERVICE_TIER] = LEGACY_CONVERSATION_SERVICE_TIER
        return await self.async_step_init(user_input)

    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Configure the selected profile type."""
        errors: dict[str, str] = {}
        if user_input is not None:
            submitted = dict(user_input)
            if (
                self._subentry_type == SUBENTRY_TYPE_CONVERSATION
                and submitted.get(CONF_REALTIME_AUTHORITY) is True
                and self._another_realtime_authority_exists()
            ):
                errors["base"] = "realtime_authority_already_configured"
                self._values = submitted
            elif self._is_new:
                title = submitted.pop(CONF_NAME)
                return self.async_create_entry(title=title, data=submitted)
            else:
                return self.async_update_and_abort(
                    self._get_entry(),
                    self._get_reconfigure_subentry(),
                    data=submitted,
                )

        schema = self._schema_for_type()
        return self.async_show_form(
            step_id="init",
            data_schema=self.add_suggested_values_to_schema(schema, self._values),
            errors=errors,
        )

    def _another_realtime_authority_exists(self) -> bool:
        """Return whether another Conversation profile owns realtime tools."""
        current_id = (
            None if self._is_new else self._get_reconfigure_subentry().subentry_id
        )
        return any(
            subentry.subentry_type == SUBENTRY_TYPE_CONVERSATION
            and subentry.subentry_id != current_id
            and subentry.data.get(CONF_REALTIME_AUTHORITY) is True
            for subentry in self._get_entry().subentries.values()
        )

    def _schema_for_type(self) -> vol.Schema:
        """Build the form schema for a profile type."""
        fields: VolDictType = {}
        if self._is_new:
            fields[vol.Required(CONF_NAME)] = str

        if self._subentry_type == SUBENTRY_TYPE_CONVERSATION:
            hass_apis = [
                SelectOptionDict(label=api.name, value=api.id)
                for api in llm.async_get_apis(self.hass)
            ]
            fields.update(
                {
                    vol.Required(CONF_MODEL): str,
                    vol.Optional(
                        CONF_CONTEXT_MODE,
                        default=self._values.get(CONF_CONTEXT_MODE, CONTEXT_LEGACY),
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=list(CONTEXT_MODES), translation_key="context_mode"
                        )
                    ),
                    vol.Optional(
                        CONF_SESSION_CONTINUITY,
                        default=self._values.get(CONF_SESSION_CONTINUITY, False),
                    ): bool,
                    vol.Optional(
                        CONF_SESSION_TTL, default=self._values.get(CONF_SESSION_TTL, 15)
                    ): vol.All(vol.Coerce(int), vol.Range(min=2, max=60)),
                    vol.Optional(
                        CONF_SESSION_FALLBACK,
                        default=self._values.get(CONF_SESSION_FALLBACK, ""),
                    ): vol.All(str, vol.Length(max=128)),
                    vol.Optional(
                        CONF_SESSION_METADATA,
                        default=self._values.get(CONF_SESSION_METADATA, True),
                    ): bool,
                    vol.Required(CONF_REASONING_EFFORT): SelectSelector(
                        SelectSelectorConfig(options=list(SUPPORTED_REASONING_EFFORTS))
                    ),
                    vol.Required(CONF_SERVICE_TIER): SelectSelector(
                        SelectSelectorConfig(options=list(SUPPORTED_SERVICE_TIERS))
                    ),
                    vol.Optional(CONF_PROMPT): TemplateSelector(),
                    vol.Optional(CONF_LLM_HASS_API): SelectSelector(
                        SelectSelectorConfig(options=hass_apis, multiple=True)
                    ),
                    vol.Required(CONF_REALTIME_AUTHORITY): BooleanSelector(),
                    vol.Required(CONF_REALTIME_LANGUAGE): SelectSelector(
                        SelectSelectorConfig(options=list(SUPPORTED_LANGUAGES))
                    ),
                }
            )
        elif self._subentry_type == SUBENTRY_TYPE_STT:
            fields.update(
                {
                    vol.Optional(CONF_PROMPT): str,
                }
            )
        else:
            fields.update(
                {
                    vol.Required(CONF_VOICE): SelectSelector(
                        SelectSelectorConfig(options=list(SUPPORTED_VOICES))
                    ),
                    vol.Optional(CONF_INSTRUCTIONS): str,
                }
            )
        return vol.Schema(fields)

    def _defaults_for_type(self) -> dict[str, Any]:
        """Return defaults for the selected profile type."""
        if self._subentry_type == SUBENTRY_TYPE_CONVERSATION:
            return {
                CONF_NAME: DEFAULT_CONVERSATION_NAME,
                CONF_MODEL: DEFAULT_CONVERSATION_MODEL,
                CONF_CONTEXT_MODE: CONTEXT_ADAPTIVE,
                CONF_REASONING_EFFORT: DEFAULT_CONVERSATION_REASONING_EFFORT,
                CONF_SERVICE_TIER: DEFAULT_CONVERSATION_SERVICE_TIER,
                CONF_PROMPT: llm.DEFAULT_INSTRUCTIONS_PROMPT,
                CONF_LLM_HASS_API: DEFAULT_LLM_HASS_API,
                CONF_REALTIME_AUTHORITY: False,
                CONF_REALTIME_LANGUAGE: DEFAULT_REALTIME_LANGUAGE,
                CONF_SESSION_CONTINUITY: True,
                CONF_SESSION_TTL: 15,
                CONF_SESSION_METADATA: False,
            }
        if self._subentry_type == SUBENTRY_TYPE_STT:
            return {CONF_NAME: DEFAULT_STT_NAME}
        return {
            CONF_NAME: DEFAULT_TTS_NAME,
            CONF_VOICE: DEFAULT_VOICE,
        }
