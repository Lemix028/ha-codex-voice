"""Prepared, disabled-by-default personal knowledge repository, outside prompts.

This module is not a registered HA/model tool. Callers must obtain access from
an authenticated UI, never from model arguments or an Echo speaker guess.
Synchronous database methods must run in an executor if wired into HA later.
"""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Literal
from uuid import uuid4

MAX_IMPORT_ENTRIES = 100
MAX_ENTRY_CHARS = 4000
_STOP_WORDS = frozenset(
    {
        "die",
        "der",
        "das",
        "und",
        "ist",
        "ich",
        "was",
        "wie",
        "mein",
        "meine",
        "the",
        "and",
        "what",
        "my",
    }
)


class KnowledgeAccessDenied(Exception):
    """No active capability or no trustworthy private-data authorization."""


class KnowledgeUnavailable(Exception):
    """A content-free storage/schema failure."""


@dataclass(frozen=True, slots=True)
class KnowledgeAccess:
    """Application-owned authorization, never a model-supplied owner selector."""

    user_id: str | None = None
    authenticated_ui: bool = False
    device_id: str | None = None
    satellite_id: str | None = None


@dataclass(frozen=True, slots=True)
class KnowledgeEntry:
    """One explicitly curated fact and its provenance; never a system instruction."""

    id: str
    text: str
    source_kind: str
    source_ref: str
    created_at: str
    updated_at: str


def selected_export_messages(
    export: Sequence[Mapping[str, Any]], selected_ids: set[str]
) -> list[tuple[str, str]]:
    """Import only explicitly selected user text from a provided ChatGPT export.

    Assistant suggestions, system messages, audio and unselected chat history
    are excluded. This parser opens no file, account, network or OAuth state.
    """
    if (
        not selected_ids
        or len(selected_ids) > MAX_IMPORT_ENTRIES
        or any(not isinstance(id_, str) or not id_ for id_ in selected_ids)
    ):
        raise ValueError("Explicit bounded message selection required")
    found: dict[str, str] = {}
    for conversation in export:
        mapping = conversation.get("mapping")
        if not isinstance(mapping, Mapping):
            continue
        for node in mapping.values():
            message = node.get("message") if isinstance(node, Mapping) else None
            if not isinstance(message, Mapping):
                continue
            message_id = message.get("id")
            if not isinstance(message_id, str) or message_id not in selected_ids:
                continue
            author, content = message.get("author"), message.get("content")
            if (
                not isinstance(author, Mapping)
                or author.get("role") != "user"
                or not isinstance(content, Mapping)
                or content.get("content_type") != "text"
            ):
                raise ValueError("Selected message is not user text")
            parts = content.get("parts")
            if not isinstance(parts, list) or not all(
                isinstance(part, str) for part in parts
            ):
                raise ValueError("Selected message has unsupported content")
            text = "\n".join(parts)
            if not text.strip() or len(text) > MAX_ENTRY_CHARS:
                raise ValueError("Selected message exceeds the text budget")
            if message_id in found:
                raise ValueError("Ambiguous duplicate selected message")
            found[message_id] = text
    if found.keys() != selected_ids:
        raise ValueError("Some selected messages are missing")
    return sorted(found.items())


class KnowledgeRepository:
    """Versioned opt-in store with strict user separation and bounded retrieval."""

    def __init__(self, path: Path, *, enabled: bool = False) -> None:
        """Disabled construction creates no database or directories."""
        self.path, self.enabled = path, enabled
        self._lock = RLock()

    def _owner(self, access: KnowledgeAccess) -> str:
        if (
            not self.enabled
            or not access.authenticated_ui
            or not access.user_id
            or access.device_id is not None
            or access.satellite_id is not None
        ):
            raise KnowledgeAccessDenied
        return access.user_id

    @contextmanager
    def _database(self) -> Iterator[sqlite3.Connection]:
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                descriptor = os.open(
                    self.path,
                    os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                try:
                    if os.name == "posix" and os.fstat(descriptor).st_mode & 0o077:
                        raise KnowledgeUnavailable
                finally:
                    os.close(descriptor)
                with closing(sqlite3.connect(self.path)) as database:
                    database.row_factory = sqlite3.Row
                    version = database.execute("PRAGMA user_version").fetchone()[0]
                    if version not in (0, 1):
                        raise KnowledgeUnavailable
                    if version == 0:
                        database.execute(
                            "CREATE TABLE entries (id TEXT PRIMARY KEY, owner TEXT NOT NULL, text TEXT NOT NULL, source_kind TEXT NOT NULL, source_ref TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
                        )
                        database.execute("PRAGMA user_version=1")
                    columns = database.execute("PRAGMA table_info(entries)").fetchall()
                    if tuple(column[1] for column in columns) != (
                        "id",
                        "owner",
                        "text",
                        "source_kind",
                        "source_ref",
                        "created_at",
                        "updated_at",
                    ):
                        raise KnowledgeUnavailable
                    database.execute("PRAGMA secure_delete=ON")
                    database.execute("PRAGMA journal_mode=WAL")
                    database.commit()
                    yield database
        except sqlite3.Error, OSError:
            raise KnowledgeUnavailable from None

    def import_curated(
        self,
        access: KnowledgeAccess,
        values: Sequence[tuple[str, str]],
        *,
        consent: bool,
        source_kind: Literal["curated", "chatgpt_export"] = "curated",
    ) -> list[str]:
        """Write only after explicit consent; never import on conversation start."""
        owner = self._owner(access)
        if not consent or not 0 < len(values) <= MAX_IMPORT_ENTRIES:
            raise KnowledgeAccessDenied
        if source_kind not in {"curated", "chatgpt_export"}:
            raise ValueError("Invalid entry source kind")
        if any(
            not isinstance(text, str)
            or not text.strip()
            or len(text) > MAX_ENTRY_CHARS
            or not isinstance(source, str)
            or len(source) > 512
            for source, text in values
        ):
            raise ValueError("Invalid curated entry")
        now = datetime.now(UTC).isoformat()
        ids = [uuid4().hex for _ in values]
        with self._database() as database, database:
            database.executemany(
                "INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (id_, owner, text, source_kind, source, now, now)
                    for id_, (source, text) in zip(ids, values, strict=True)
                ],
            )
        return ids

    def import_export(
        self,
        access: KnowledgeAccess,
        export: Sequence[Mapping[str, Any]],
        selected_ids: set[str],
        *,
        consent: bool,
    ) -> list[str]:
        """Select explicit user messages; no account/history access is implied."""
        self._owner(access)
        if not consent:
            raise KnowledgeAccessDenied
        return self.import_curated(
            access,
            selected_export_messages(export, selected_ids),
            consent=True,
            source_kind="chatgpt_export",
        )

    def entries(self, access: KnowledgeAccess) -> list[KnowledgeEntry]:
        """Return owner-scoped provenance for the management UI, not for prompts."""
        owner = self._owner(access)
        with self._database() as database:
            rows = database.execute(
                "SELECT id, text, source_kind, source_ref, created_at, updated_at FROM entries WHERE owner=? ORDER BY updated_at DESC",
                (owner,),
            ).fetchall()
        if any(not all(isinstance(value, str) for value in row) for row in rows):
            raise KnowledgeUnavailable
        return [KnowledgeEntry(*row) for row in rows]

    def relevant(
        self, access: KnowledgeAccess, query: str, *, limit: int = 3
    ) -> list[KnowledgeEntry]:
        """Load relevant facts only, with a separate 1800-character context cap."""
        self._owner(access)
        if type(limit) is not int or not 1 <= limit <= 3:
            raise ValueError("Retrieval limit must be 1–3")
        words = set(re.findall(r"\w{3,}", query.casefold())) - _STOP_WORDS
        ranked = sorted(
            (
                (len(words & set(re.findall(r"\w{3,}", entry.text.casefold()))), entry)
                for entry in self.entries(access)
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        result: list[KnowledgeEntry] = []
        budget = 1800
        for score, entry in ranked:
            if score and len(result) < limit and len(entry.text) <= budget:
                result.append(entry)
                budget -= len(entry.text)
        return result

    def edit(self, access: KnowledgeAccess, id_: str, text: str) -> None:
        """Correct only the authenticated owner's entry and retain provenance."""
        owner = self._owner(access)
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_ENTRY_CHARS:
            raise ValueError("Invalid edited entry")
        with self._database() as database, database:
            if (
                database.execute(
                    "UPDATE entries SET text=?, updated_at=? WHERE id=? AND owner=?",
                    (text, datetime.now(UTC).isoformat(), id_, owner),
                ).rowcount
                != 1
            ):
                raise KnowledgeAccessDenied

    def delete_all(self, access: KnowledgeAccess) -> None:
        """Delete owner data and compact local pages/WAL; backups are separate."""
        owner = self._owner(access)
        with self._database() as database:
            database.execute("DELETE FROM entries WHERE owner=?", (owner,))
            database.commit()
            database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            database.execute("VACUUM")
            database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
