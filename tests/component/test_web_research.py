"""Validate the separate, disabled-by-default research capability."""

from typing import Any

import pytest

from custom_components.codex_voice.web_research import WebResearchPolicy


@pytest.mark.parametrize(
    ("policy", "host"),
    [
        (WebResearchPolicy(), "provider.example"),
        (
            WebResearchPolicy(provider_hosts=frozenset({"provider.example"})),
            "provider.example",
        ),
        (WebResearchPolicy(enabled=True), "provider.example"),
        (
            WebResearchPolicy(
                enabled=True, provider_hosts=frozenset({"provider.example"})
            ),
            "other.example",
        ),
    ],
)
def test_research_requires_opt_in_and_configured_host(
    policy: WebResearchPolicy,
    host: str,
) -> None:
    """An arbitrary or merely configured provider receives no authorization."""
    with pytest.raises(PermissionError):
        policy.validate_request(host, "synthetic question", calls=0)


@pytest.mark.parametrize(("max_calls", "calls"), [(1, 0), (2, 0), (2, 1)])
def test_authorized_request_within_budget(max_calls: int, calls: int) -> None:
    """Both supported budgets admit precisely the calls below the limit."""
    policy = WebResearchPolicy(True, frozenset({"provider.example"}), max_calls)
    policy.validate_request("provider.example", "x" * 512, calls=calls)


@pytest.mark.parametrize(
    ("max_calls", "calls", "query"),
    [
        (0, 0, "query"),
        (3, 0, "query"),
        (True, 0, "query"),
        (1.5, 0, "query"),
        (2, -1, "query"),
        (1, 1, "query"),
        (2, 2, "query"),
        (2, True, "query"),
        (2, 0.5, "query"),
        (2, 0, ""),
        (2, 0, " \n"),
        (2, 0, "x" * 513),
        (2, 0, " " * 512 + "x"),
        (2, 0, None),
    ],
)
def test_invalid_budget_or_query_is_rejected(
    max_calls: Any,
    calls: Any,
    query: Any,
) -> None:
    """Whitespace, invalid counts and unsupported input cannot bypass budgets."""
    policy = WebResearchPolicy(True, frozenset({"provider.example"}), max_calls)
    with pytest.raises(ValueError):
        policy.validate_request("provider.example", query, calls=calls)
