"""Session isolation, restart, lifetime, admission and context-budget tests."""

from __future__ import annotations

import asyncio
import json

import pytest

from custom_components.codex_voice.conversation_sessions import (
    MAX_CONTEXT_CHARS,
    SessionBusyError,
    SessionManager,
    is_reset_command,
)


def owner(
    manager: SessionManager,
    device: str | None = "echo-a",
    incoming: str | None = None,
    user: str | None = None,
) -> str:
    """Resolve synthetic HA identities without real user data."""
    return manager.identity(
        device_id=device, satellite_id=None, user_id=user, incoming_id=incoming
    )[0]


async def test_changed_and_missing_ids_keep_device_context_and_ttl_resets() -> None:
    """T04/T05: device wins over changing IDs and idle expiry rotates context."""
    now = [1000.0]
    manager = SessionManager(ttl_minutes=2, clock=lambda: now[0])
    async with manager.lease(owner(manager, incoming="a")) as first:
        first.remember_turn(
            [{"role": "user", "content": "Wie viel Strom verbraucht mein NUC?"}]
        )
        first_id = first.conversation_id
    now[0] += 119
    async with manager.lease(owner(manager, incoming="b")) as second:
        assert second.conversation_id == first_id
        assert "NUC" in second.history[0]["content"]
    now[0] += 120
    async with manager.lease(owner(manager)) as expired:
        assert expired.conversation_id != first_id
        assert expired.history == []


async def test_devices_users_and_unknown_clients_are_isolated() -> None:
    """T07: no implicit common anonymous-device history."""
    manager = SessionManager()
    assert owner(manager, "echo-a") != owner(manager, "echo-b")
    assert owner(manager, "echo-a", user="user-a") != owner(
        manager, "echo-a", user="user-b"
    )
    assert owner(manager, None) != owner(manager, None)
    assert owner(manager, None, "a") != owner(manager, None, "b")
    fallback = SessionManager(fallback="dedicated echo only")
    assert owner(fallback, None, "a") == owner(fallback, None, "b")
    assert owner(fallback, "echo-a") != owner(fallback, "echo-b")


async def test_metadata_restore_contains_no_text_or_raw_device_identity() -> None:
    """T06/T13/T16: restore binding, explicitly lose RAM-only context."""
    now = [1000.0]
    manager = SessionManager(clock=lambda: now[0])
    async with manager.lease(owner(manager)) as first:
        first.remember_turn(
            [{"role": "user", "content": "synthetic private utterance"}]
        )
        first_id = first.conversation_id
    metadata = json.loads(json.dumps(manager.snapshot()))
    assert "synthetic private" not in str(metadata)
    assert "echo-a" not in str(metadata)
    restored = SessionManager(clock=lambda: now[0])
    restored.restore(metadata)
    async with restored.lease(owner(restored)) as second:
        assert second.conversation_id == first_id
        assert second.history == []
        assert second.restored
        assert not second.history_complete


async def test_same_session_serializes_and_rejects_unbounded_queue() -> None:
    """T14: waiters are bounded and cancellation releases admission."""
    manager = SessionManager()
    identity = owner(manager)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def wait() -> None:
        async with manager.lease(identity):
            entered.set()
            await release.wait()

    first = asyncio.create_task(wait())
    await entered.wait()
    second = asyncio.create_task(wait())
    await asyncio.sleep(0)
    with pytest.raises(SessionBusyError):
        async with manager.lease(identity):
            pytest.fail("Third request must not enter")
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    async with manager.lease(owner(manager, "echo-b")):
        pass
    release.set()
    await first
    async with manager.lease(identity) as available:
        assert available.admitted == 1


async def test_context_budget_and_explicit_reset() -> None:
    """Only bounded RAM context survives; reset rotates the model binding."""
    manager = SessionManager()
    async with manager.lease(owner(manager)) as session:
        for _ in range(30):
            session.remember_turn([{"role": "assistant", "content": "x" * 2000}])
        assert len(json.dumps(session.history)) <= MAX_CONTEXT_CHARS
        assert not session.history_complete
        first_id = session.conversation_id
        manager.reset(session)
        assert session.history == []
        assert session.conversation_id != first_id
    assert is_reset_command("Computer, neues Gespräch!")
    assert not is_reset_command("Bitte erzähle von einem neuen Gespräch")


@pytest.mark.parametrize("minutes", [1, 61, True])
def test_invalid_lifetime_is_rejected(minutes: int) -> None:
    """Invalid metadata/configuration cannot create unbounded sessions."""
    with pytest.raises(ValueError):
        SessionManager(ttl_minutes=minutes)


def test_unknown_schema_fails_closed() -> None:
    """No implicit migration or reinterpretation of persisted owners."""
    with pytest.raises(ValueError):
        SessionManager().restore({"schema": 2})
