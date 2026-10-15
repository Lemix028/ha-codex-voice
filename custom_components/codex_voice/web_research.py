"""Prepared policy for a separate web provider; no network implementation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WebResearchPolicy:
    """Default-off capability independent of Codex sandbox/network rights."""

    enabled: bool = False
    provider_hosts: frozenset[str] = frozenset()
    max_calls: int = 2

    def validate_request(self, host: str, query: str, *, calls: int) -> None:
        """Reject an unconfigured provider or excess queries before any I/O."""
        if not self.enabled or host not in self.provider_hosts:
            raise PermissionError("Web research is not authorized")
        if (
            type(self.max_calls) is not int
            or not 1 <= self.max_calls <= 2
            or type(calls) is not int
            or not 0 <= calls < self.max_calls
            or not isinstance(query, str)
            or not query.strip()
            or len(query) > 512
        ):
            raise ValueError("Web research request exceeds its budget")
