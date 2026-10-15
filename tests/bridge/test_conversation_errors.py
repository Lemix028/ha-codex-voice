"""Regression coverage for typed, redacted quota errors."""

from __future__ import annotations

import pytest

from bridge.conversation_errors import bridge_error, provider_error, quota_exhausted
from bridge.errors import RpcError


@pytest.mark.parametrize(
    "payload",
    [
        {"error": {"message": "You've hit your usage limit. Try again at 7:41 PM"}},
        {"error": {"code": "usage_limit_reached", "message": "Limit reached"}},
        {"turn": {"status": "failed", "error": {"message": "Usage limit reached"}}},
        RpcError("turn/start", {"message": "You've hit your usage limit."}),
    ],
)
def test_quota_error_is_typed_and_redacted(payload: object) -> None:
    assert quota_exhausted(payload)
    assert provider_error(payload) == {
        "type": "error",
        "code": "quota_exhausted",
        "error": "Codex usage limit reached",
    }
    assert "7:41" not in str(provider_error(payload))


def test_unknown_provider_failure_does_not_leak_message() -> None:
    raw = {"error": {"message": "private/sensitive-provider-details"}}
    assert provider_error(raw) == {"type": "error", "error": "Codex request failed"}
    assert not quota_exhausted(raw)


def test_transient_rate_limiting_is_not_subscription_quota() -> None:
    assert not quota_exhausted({"error": {"code": "rate_limit_exceeded"}})


def test_internal_errors_remain_diagnostic() -> None:
    assert bridge_error(ValueError("invalid caller data")) == {
        "type": "error",
        "error": "invalid caller data",
    }


def test_unknown_rpc_provider_error_is_redacted() -> None:
    assert bridge_error(RpcError("turn/start", {"message": "secret debug payload"})) == {
        "type": "error",
        "error": "Codex request failed",
    }
