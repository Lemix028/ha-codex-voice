"""Constants for the Codex Voice integration."""

from datetime import timedelta
from typing import Final

from homeassistant.const import Platform

DOMAIN: Final = "codex_voice"
NAME: Final = "Codex Voice"
MANUFACTURER: Final = "Codex Voice community"

PLATFORMS: Final = (Platform.CONVERSATION, Platform.STT, Platform.TTS)

CONF_ACCESS_TOKEN: Final = "access_token"
CONF_BRIDGE_URL: Final = "bridge_url"
CONF_MODEL: Final = "model"
CONF_REASONING_EFFORT: Final = "reasoning_effort"
CONF_SERVICE_TIER: Final = "service_tier"
CONF_SESSION_CONTINUITY: Final = "session_continuity"
CONF_SESSION_TTL: Final = "session_idle_minutes"
CONF_SESSION_FALLBACK: Final = "session_fallback"
CONF_SESSION_METADATA: Final = "session_metadata"
CONF_VOICE: Final = "voice"
CONF_INSTRUCTIONS: Final = "instructions"
CONF_REALTIME_AUTHORITY: Final = "realtime_authority"
CONF_REALTIME_LANGUAGE: Final = "realtime_language"

SUBENTRY_TYPE_CONVERSATION: Final = "conversation"
SUBENTRY_TYPE_STT: Final = "stt"
SUBENTRY_TYPE_TTS: Final = "tts"

DEFAULT_BRIDGE_URL: Final = "http://localhost:8787"
DEFAULT_CONVERSATION_NAME: Final = "Codex Voice"
DEFAULT_STT_NAME: Final = "Experimental Codex Realtime STT"
DEFAULT_TTS_NAME: Final = "Codex Text-to-Speech"
DEFAULT_CONVERSATION_MODEL: Final = "gpt-5.6-sol"
DEFAULT_CONVERSATION_REASONING_EFFORT: Final = "low"
DEFAULT_CONVERSATION_SERVICE_TIER: Final = "priority"
LEGACY_CONVERSATION_SERVICE_TIER: Final = "standard"
DEFAULT_VOICE: Final = "cove"
DEFAULT_LLM_HASS_API: Final = ["assist"]
DEFAULT_REALTIME_LANGUAGE: Final = "es-MX"

REALTIME_TOOL_PROTOCOL_VERSION: Final = 1
REALTIME_TOOL_PATH: Final = "/v1/home-assistant/tools"
# Broker traffic is intentionally much smaller than normal conversation traffic.
# These bounds also cap memory retained while a reconnecting registration is built.
MAX_REALTIME_TOOL_MESSAGE_BYTES: Final = 256 * 1024
MAX_REALTIME_TOOL_REGISTRATION_BYTES: Final = 256 * 1024
MAX_REALTIME_TOOL_SCHEMA_BYTES: Final = 64 * 1024
MAX_REALTIME_TOOL_RESULT_BYTES: Final = 64 * 1024
MAX_REALTIME_TOOL_ARGUMENT_BYTES: Final = 64 * 1024
MAX_REALTIME_TOOLS: Final = 128

MIN_HA_VERSION: Final = "2026.8.0"
REQUEST_TIMEOUT: Final = 120.0
HEALTH_TIMEOUT: Final = 10.0
CONVERSATION_TIMEOUT: Final = 180.0
# JSON base64 adds roughly one third to the wire size. Keep the raw payload below
# the bridge's request-body ceiling as well as bounding Home Assistant memory use.
MAX_AUDIO_BYTES: Final = 16 * 1024 * 1024
MAX_SYNTHESIZED_AUDIO_BYTES: Final = 50 * 1024 * 1024
MAX_TOOL_CALLS: Final = 10

RECONNECT_INTERVAL: Final = timedelta(seconds=30)

# Realtime voices currently exposed by the Codex/OpenAI voice surface. The bridge
# remains authoritative and may reject a voice unavailable to the signed-in account.
SUPPORTED_VOICES: Final = (
    "arbor",
    "breeze",
    "cove",
    "ember",
    "juniper",
    "maple",
    "sol",
    "spruce",
    "vale",
)

SUPPORTED_REASONING_EFFORTS: Final = (
    "low",
    "medium",
    "high",
    "xhigh",
    "max",
    "ultra",
)

SUPPORTED_SERVICE_TIERS: Final = ("standard", "priority")

# The realtime speech models detect language automatically. Home Assistant still
# requires providers to advertise concrete BCP-47 language tags.
SUPPORTED_LANGUAGES: Final = (
    "af-ZA",
    "ar-SA",
    "az-AZ",
    "be-BY",
    "bg-BG",
    "bs-BA",
    "ca-ES",
    "cs-CZ",
    "cy-GB",
    "da-DK",
    "de-DE",
    "el-GR",
    "en-GB",
    "en-US",
    "es-ES",
    "es-MX",
    "et-EE",
    "fa-IR",
    "fi-FI",
    "fil-PH",
    "fr-FR",
    "gl-ES",
    "he-IL",
    "hi-IN",
    "hr-HR",
    "hu-HU",
    "hy-AM",
    "id-ID",
    "is-IS",
    "it-IT",
    "ja-JP",
    "kk-KZ",
    "kn-IN",
    "ko-KR",
    "lt-LT",
    "lv-LV",
    "mi-NZ",
    "mk-MK",
    "mr-IN",
    "ms-MY",
    "ne-NP",
    "nl-NL",
    "no-NO",
    "pl-PL",
    "pt-BR",
    "pt-PT",
    "ro-RO",
    "ru-RU",
    "sk-SK",
    "sl-SI",
    "sr-RS",
    "sv-SE",
    "sw-KE",
    "ta-IN",
    "th-TH",
    "tr-TR",
    "uk-UA",
    "ur-PK",
    "vi-VN",
    "zh-CN",
)
