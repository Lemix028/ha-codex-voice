"""Exercise opt-in knowledge storage with synthetic, owner-separated data."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from custom_components.codex_voice.personal_knowledge import (
    MAX_ENTRY_CHARS,
    KnowledgeAccess,
    KnowledgeAccessDenied,
    KnowledgeRepository,
    KnowledgeUnavailable,
    selected_export_messages,
)

OWNER = KnowledgeAccess(user_id="user-a", authenticated_ui=True)
OTHER = KnowledgeAccess(user_id="user-b", authenticated_ui=True)


def export_message(
    id_: Any = "selected",
    *,
    role: str = "user",
    content_type: str = "text",
    parts: Any = None,
) -> dict[str, Any]:
    """Build a minimal export node without real chat content."""
    return {
        "message": {
            "id": id_,
            "author": {"role": role},
            "content": {
                "content_type": content_type,
                "parts": ["synthetic fact"] if parts is None else parts,
            },
        }
    }


@pytest.mark.parametrize(
    ("enabled", "access"),
    [
        (False, OWNER),
        (True, KnowledgeAccess()),
        (True, KnowledgeAccess(user_id="user-a")),
        (
            True,
            KnowledgeAccess(user_id="user-a", authenticated_ui=True, device_id="echo"),
        ),
        (
            True,
            KnowledgeAccess(
                user_id="user-a", authenticated_ui=True, satellite_id="echo"
            ),
        ),
    ],
)
def test_denied_access_creates_no_storage(
    tmp_path: Path,
    enabled: bool,
    access: KnowledgeAccess,
) -> None:
    """Disabled or shared-speaker access fails before any filesystem changes."""
    repository = KnowledgeRepository(
        tmp_path / "private" / "knowledge.db", enabled=enabled
    )
    for operation in (
        lambda: repository.entries(access),
        lambda: repository.relevant(access, "synthetic"),
        lambda: repository.import_curated(access, [("source", "fact")], consent=True),
        lambda: repository.import_export(access, [], {"selected"}, consent=True),
        lambda: repository.edit(access, "entry", "edited"),
        lambda: repository.delete_all(access),
    ):
        with pytest.raises(KnowledgeAccessDenied):
            operation()
    assert not repository.path.parent.exists()


def test_consent_is_required_before_storage(tmp_path: Path) -> None:
    """Neither import path can persist data without consent."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    with pytest.raises(KnowledgeAccessDenied):
        repository.import_curated(OWNER, [("source", "fact")], consent=False)
    with pytest.raises(KnowledgeAccessDenied):
        repository.import_export(OWNER, [], {"selected"}, consent=False)
    assert not repository.path.exists()


def test_owner_isolation_edit_provenance_and_deletion(tmp_path: Path) -> None:
    """Persistence and corrections never read, update or delete another owner."""
    path = tmp_path / "knowledge.db"
    repository = KnowledgeRepository(path, enabled=True)
    [id_] = repository.import_curated(
        OWNER, [("manual", "synthetic secret")], consent=True
    )
    repository.import_curated(OTHER, [("other", "other fact")], consent=True)
    repository = KnowledgeRepository(path, enabled=True)
    [entry] = repository.entries(OWNER)
    assert entry.id == id_
    assert entry.source_kind == "curated"
    assert entry.source_ref == "manual"
    assert entry.created_at == entry.updated_at
    with pytest.raises(KnowledgeAccessDenied):
        repository.edit(OTHER, id_, "unauthorized correction")
    repository.edit(OWNER, id_, "corrected secret")
    [corrected] = repository.entries(OWNER)
    assert corrected.text == "corrected secret"
    assert corrected.created_at == entry.created_at
    assert corrected.source_ref == entry.source_ref
    repository.delete_all(OWNER)
    assert repository.entries(OWNER) == []
    assert [entry.text for entry in repository.entries(OTHER)] == ["other fact"]
    assert b"synthetic secret" not in path.read_bytes()
    assert b"corrected secret" not in path.read_bytes()
    wal = Path(f"{path}-wal")
    assert not wal.exists() or wal.stat().st_size == 0
    if os.name == "posix":
        assert path.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize(
    "text", ["", " \n", "x" * (MAX_ENTRY_CHARS + 1), " " * MAX_ENTRY_CHARS + "x"]
)
def test_text_budget_applies_to_import_and_edit(tmp_path: Path, text: str) -> None:
    """Whitespace cannot bypass the stored text budget on either write path."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    with pytest.raises(ValueError):
        repository.import_curated(OWNER, [("source", text)], consent=True)
    assert not repository.path.exists()
    [id_] = repository.import_curated(OWNER, [("source", "original")], consent=True)
    with pytest.raises(ValueError):
        repository.edit(OWNER, id_, text)
    assert repository.entries(OWNER)[0].text == "original"


def test_invalid_batch_is_atomic(tmp_path: Path) -> None:
    """One invalid entry rejects the entire batch before creating storage."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    with pytest.raises(ValueError):
        repository.import_curated(
            OWNER, [("source", "valid"), ("source", "")], consent=True
        )
    with pytest.raises(ValueError):
        repository.import_curated(OWNER, [("x" * 513, "valid")], consent=True)
    with pytest.raises(ValueError):
        repository.import_curated(
            OWNER, [("source", "valid")], consent=True, source_kind="unknown"
        )  # type: ignore[arg-type]
    with pytest.raises(KnowledgeAccessDenied):
        repository.import_curated(OWNER, [("source", "valid")] * 101, consent=True)
    assert not repository.path.exists()


def test_maximum_entry_and_batch_are_supported(tmp_path: Path) -> None:
    """The documented upper bounds remain usable after stricter validation."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    ids = repository.import_curated(
        OWNER, [("s" * 512, "x" * MAX_ENTRY_CHARS)] * 100, consent=True
    )
    assert len(set(ids)) == 100
    assert len(repository.entries(OWNER)) == 100
    repository.edit(OWNER, ids[0], "y" * MAX_ENTRY_CHARS)
    assert any(
        entry.text == "y" * MAX_ENTRY_CHARS for entry in repository.entries(OWNER)
    )


def test_retrieval_caps_context_and_excludes_other_owners(tmp_path: Path) -> None:
    """Only matching owner facts fit the independent context and count budgets."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    repository.import_curated(
        OWNER, [("source", "synthetic " + "x" * 890)] * 4, consent=True
    )
    repository.import_curated(
        OTHER, [("source", "synthetic other private")], consent=True
    )
    entries = repository.relevant(OWNER, "synthetic")
    assert len(entries) == 2
    assert sum(len(entry.text) for entry in entries) <= 1800
    assert len(repository.relevant(OWNER, "synthetic", limit=1)) == 1
    assert repository.relevant(OWNER, "unrelated") == []
    assert repository.relevant(OWNER, "die und meine") == []


@pytest.mark.parametrize("limit", [0, 4, True, 1.5])
def test_retrieval_rejects_invalid_counts(tmp_path: Path, limit: Any) -> None:
    """Retrieval limits are integer counts, excluding booleans and fractions."""
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    with pytest.raises(ValueError):
        repository.relevant(OWNER, "synthetic", limit=limit)
    assert not repository.path.exists()


def test_export_selects_only_explicit_user_text(tmp_path: Path) -> None:
    """Unselected roles and malformed IDs do not enter the curated store."""
    export = [
        {
            "mapping": {
                "chosen": export_message(parts=["first", "second"]),
                "assistant": export_message("ignored", role="assistant"),
                "malformed": export_message([]),
                "empty": {},
            }
        }
    ]
    assert selected_export_messages(export, {"selected"}) == [
        ("selected", "first\nsecond")
    ]
    repository = KnowledgeRepository(tmp_path / "knowledge.db", enabled=True)
    repository.import_export(OWNER, export, {"selected"}, consent=True)
    [entry] = repository.entries(OWNER)
    assert entry.text == "first\nsecond"
    assert entry.source_kind == "chatgpt_export"
    assert entry.source_ref == "selected"


@pytest.mark.parametrize(
    ("nodes", "selection"),
    [
        ({"node": export_message(role="assistant")}, {"selected"}),
        ({"node": export_message(role="system")}, {"selected"}),
        ({"node": export_message(content_type="audio")}, {"selected"}),
        ({"node": export_message(parts=[{}])}, {"selected"}),
        ({"node": export_message(parts=[" " * MAX_ENTRY_CHARS + "x"])}, {"selected"}),
        ({"a": export_message(), "b": export_message()}, {"selected"}),
        ({}, {"missing"}),
        ({}, set()),
        ({}, {""}),
        ({}, {str(i) for i in range(101)}),
    ],
)
def test_invalid_export_selection_is_rejected(
    nodes: dict[str, Any],
    selection: set[str],
) -> None:
    """Unsupported, ambiguous, missing and unbounded selections fail closed."""
    with pytest.raises(ValueError):
        selected_export_messages([{"mapping": nodes}], selection)


@pytest.mark.parametrize("version", [0, 1, 2])
def test_incompatible_schema_is_not_repaired_silently(
    tmp_path: Path, version: int
) -> None:
    """Existing incompatible databases keep their schema and version unchanged."""
    path = tmp_path / "knowledge.db"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE entries (unexpected TEXT)")
        database.execute(f"PRAGMA user_version={version}")
    path.chmod(0o600)
    repository = KnowledgeRepository(path, enabled=True)
    with pytest.raises(KnowledgeUnavailable):
        repository.entries(OWNER)
    with sqlite3.connect(path) as database:
        assert database.execute("PRAGMA user_version").fetchone()[0] == version
        assert (
            database.execute("PRAGMA table_info(entries)").fetchone()[1] == "unexpected"
        )


def test_missing_versioned_table_fails_closed(tmp_path: Path) -> None:
    """A versioned but incomplete database must not be mistaken for a new store."""
    path = tmp_path / "knowledge.db"
    with sqlite3.connect(path) as database:
        database.execute("PRAGMA user_version=1")
    path.chmod(0o600)
    with pytest.raises(KnowledgeUnavailable):
        KnowledgeRepository(path, enabled=True).entries(OWNER)
    with sqlite3.connect(path) as database:
        assert (
            database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            == []
        )


def test_storage_failure_does_not_expose_content(tmp_path: Path) -> None:
    """Corrupt storage errors provide no private payload or database details."""
    path = tmp_path / "knowledge.db"
    path.write_bytes(b"synthetic private corrupt database")
    path.chmod(0o600)
    with pytest.raises(KnowledgeUnavailable) as error:
        KnowledgeRepository(path, enabled=True).entries(OWNER)
    assert str(error.value) == ""
    assert error.value.__suppress_context__


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes required")
def test_insecure_existing_store_is_rejected(tmp_path: Path) -> None:
    """An existing publicly readable private database is not opened."""
    path = tmp_path / "knowledge.db"
    path.touch(mode=0o644)
    path.chmod(0o644)
    with pytest.raises(KnowledgeUnavailable):
        KnowledgeRepository(path, enabled=True).entries(OWNER)
