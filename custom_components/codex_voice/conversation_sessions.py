"""Device-bound short-lived sessions; persistence contains metadata only."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import secrets
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from .json_serialization import normalize_json

MAX_SESSIONS = 128
MAX_CONTEXT_CHARS = 16_000
MAX_CONTEXT_MESSAGES = 24


class SessionBusyError(Exception):
    """The bounded session admission queue is full."""


@dataclass(slots=True)
class Session:
    """One isolated conversation and bounded RAM-only continuity context."""

    conversation_id: str = field(default_factory=lambda: uuid4().hex)
    last_used: float = field(default_factory=time.monotonic)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    admitted: int = 0
    history: list[dict[str, Any]] = field(default_factory=list, repr=False)
    previous_incoming: str | None = field(default=None, repr=False)
    history_complete: bool = True
    restored: bool = False
    local_action_unverified: bool = False
    ha_id: str | None = field(default=None, repr=False)
    ha_size: int = 0

    def reset(self, now: float) -> None:
        """Rotate continuity so old model threads cannot be reused."""
        self.conversation_id = uuid4().hex
        self.last_used = now
        self.history.clear()
        self.history_complete = True
        self.restored = False
        self.local_action_unverified = False
        self.ha_id = None
        self.ha_size = 0

    def remember_turn(self, messages: list[dict[str, Any]]) -> None:
        """Retain complete recent messages within an explicit context budget."""
        for message in normalize_json(messages, path="$.messages"):
            self.history.append(message)
        while (
            len(self.history) > MAX_CONTEXT_MESSAGES
            or len(json.dumps(self.history, ensure_ascii=False)) > MAX_CONTEXT_CHARS
        ):
            self.history.pop(0)
            self.history_complete = False


class SessionManager:
    """Own mapping and locks separately from HA's five-minute chat lifetime."""

    def __init__(
        self,
        *,
        ttl_minutes: int = 15,
        fallback: str = "",
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        """Initialize an isolated Conversation-profile manager."""
        if (
            isinstance(ttl_minutes, bool)
            or not isinstance(ttl_minutes, int)
            or not 2 <= ttl_minutes <= 60
        ):
            raise ValueError("Session idle lifetime must be 2–60 minutes")
        self.ttl = ttl_minutes * 60
        self.fallback = fallback.strip()
        self._clock = clock
        self._wall_clock = wall_clock
        self._salt = secrets.token_hex(32)
        self._sessions: dict[str, Session] = {}

    def identity(
        self,
        *,
        device_id: str | None,
        satellite_id: str | None,
        user_id: str | None,
        incoming_id: str | None,
    ) -> tuple[str, str]:
        """Never combine anonymous devices unless explicitly configured."""
        if device_id:
            source, value = "device", device_id
        elif satellite_id:
            source, value = "satellite", satellite_id
        elif self.fallback:
            source, value = "configured_fallback", self.fallback
        elif incoming_id:
            source, value = "incoming_id", incoming_id
        else:
            source, value = "isolated", uuid4().hex
        identity = json.dumps(
            [self._salt, source, value, user_id], separators=(",", ":")
        )
        return hashlib.sha256(identity.encode()).hexdigest(), source

    @asynccontextmanager
    async def lease(self, owner: str) -> AsyncIterator[Session]:
        """Serialize at most two admitted requests; never evict active owners."""
        now = self._clock()
        self._prune(now)
        session = self._sessions.get(owner)
        if session is None:
            if len(self._sessions) >= MAX_SESSIONS:
                raise SessionBusyError
            session = self._sessions[owner] = Session(last_used=now)
        if session.admitted >= 2:
            raise SessionBusyError
        session.admitted += 1
        acquired = False
        try:
            async with asyncio.timeout(180):
                await session.lock.acquire()
            acquired = True
            now = self._clock()
            if now - session.last_used >= self.ttl:
                session.reset(now)
            yield session
        finally:
            if acquired:
                session.last_used = self._clock()
                session.lock.release()
            session.admitted -= 1

    def _prune(self, now: float) -> None:
        for owner, session in list(self._sessions.items()):
            if not session.admitted and now - session.last_used >= self.ttl:
                del self._sessions[owner]

    def reset(self, session: Session) -> None:
        """End only the selected active conversation."""
        session.reset(self._clock())

    def expire(self) -> bool:
        """Remove inactive RAM context even when no subsequent request arrives."""
        before = len(self._sessions)
        self._prune(self._clock())
        return before != len(self._sessions)

    def touch(self, session: Session) -> None:
        """Checkpoint completion time before persisting metadata."""
        session.last_used = self._clock()

    def snapshot(self) -> dict[str, Any]:
        """Persist hashes, random session IDs and timestamps, never utterances."""
        self._prune(self._clock())
        return {
            "schema": 1,
            "salt": self._salt,
            "sessions": [
                {
                    "owner": owner,
                    "conversation_id": session.conversation_id,
                    "last_used": self._wall_clock()
                    - max(0, self._clock() - session.last_used),
                }
                for owner, session in self._sessions.items()
            ],
        }

    def restore(self, value: Mapping[str, Any]) -> None:
        """Reject incompatible/corrupt metadata rather than guessing owners."""
        if value.get("schema") != 1:
            raise ValueError("Unsupported session metadata schema")
        salt, records = value.get("salt"), value.get("sessions")
        if (
            not isinstance(salt, str)
            or len(salt) != 64
            or any(char not in "0123456789abcdef" for char in salt)
            or not isinstance(records, list)
            or len(records) > MAX_SESSIONS
        ):
            raise ValueError("Invalid session metadata")
        sessions: dict[str, Session] = {}
        now = self._wall_clock()
        for record in records:
            if not isinstance(record, dict):
                raise TypeError("Invalid session metadata")
            owner, conversation_id, used = (
                record.get("owner"),
                record.get("conversation_id"),
                record.get("last_used"),
            )
            if (
                not isinstance(owner, str)
                or len(owner) != 64
                or not isinstance(conversation_id, str)
                or len(conversation_id) != 32
                or isinstance(used, bool)
                or not isinstance(used, (float, int))
                or not math.isfinite(used)
                or owner in sessions
            ):
                raise ValueError("Invalid session metadata")
            if any(char not in "0123456789abcdef" for char in owner + conversation_id):
                raise ValueError("Invalid session metadata")
            if not 0 <= now - used < self.ttl:
                continue
            sessions[owner] = Session(
                conversation_id=conversation_id,
                last_used=self._clock() - (now - used),
                history_complete=False,
                restored=True,
            )
        self._salt, self._sessions = salt, sessions


def is_reset_command(text: str) -> bool:
    """Recognize explicit whole utterances without modifying other commands."""
    normalized = text.casefold().strip().rstrip(".!?")
    if normalized.startswith("computer,"):
        normalized = normalized[len("computer,") :].strip()
    return normalized in {
        "neues gespräch",
        "gespräch beenden",
        "new conversation",
        "end conversation",
    }
