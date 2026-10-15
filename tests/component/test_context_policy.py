"""Deterministic context decisions and bounded question metadata."""

import pytest

from custom_components.codex_voice.context_policy import (
    QUESTION_TTL,
    PendingQuestion,
    pending_question,
    select_context,
)


@pytest.mark.parametrize(
    "text",
    [
        "Und was kostet das?",
        "Erklär das genauer",
        "Mehr dazu.",
        "Warum ist das?",
        "Und wie viel?",
        "Mach es wieder aus",
        "Schalte sie an",
        "Computer, erklär das genauer!",
        "Tell me more",
        "Explain that in more detail",
        "And how much does it cost?",
    ],
)
@pytest.mark.parametrize("has_context", [True, False])
def test_follow_up_needs_actual_context(text: str, has_context: bool) -> None:
    """An orphaned reference is clarified locally instead of guessed."""
    decision = select_context(text, has_context=has_context, question=None, now=100)
    assert decision.kind == ("follow_up" if has_context else "clarify")
    assert decision.use_context is has_context


@pytest.mark.parametrize(
    "text",
    [
        "Was ist Photosynthese?",
        "Wie viel kostet ein Flug nach Paris?",
        "Wie ist das Wetter heute?",
        "Was ist das Internet?",
        "Erklär Quantenphysik",
        "Licht an",
        "Computer, Licht",
        "Wie funktioniert meine Heizung?",
        "Who was Ada Lovelace?",
        "How does a thermostat work?",
    ],
)
def test_new_topics_are_independent(text: str) -> None:
    """Available history does not make a new question a follow-up."""
    decision = select_context(text, has_context=True, question=None, now=100)
    assert decision.kind == "independent"
    assert not decision.use_context


@pytest.mark.parametrize(
    "text", ["weiter", "gespräch fortsetzen", "continue our conversation"]
)
@pytest.mark.parametrize("has_context", [True, False])
def test_explicit_continuation(text: str, has_context: bool) -> None:
    """Explicit requests need an available, owner-scoped context."""
    assert select_context(
        text, has_context=has_context, question=None, now=100
    ).kind == ("explicit" if has_context else "clarify")


@pytest.mark.parametrize(
    ("question", "answer", "kind"),
    [
        ("Soll ich das Licht ausschalten?", "ja", "yes_no"),
        ("Welchen Stromtarif hast du?", "30 Cent pro kWh", "number"),
        ("Welches Gerät meinst du?", "Lampe im Wohnzimmer", "entity"),
        ("Küche oder Wohnzimmer?", "Wohnzimmer", "choice"),
        ("Kitchen or bedroom?", "bedroom", "choice"),
        ("Would you like an example?", "yes please", "yes_no"),
    ],
)
def test_actual_recent_question_accepts_matching_answer(
    question: str,
    answer: str,
    kind: str,
) -> None:
    """Short answers reuse only a matching recent question, without a router."""
    pending = pending_question(question, now=100)
    assert pending is not None
    assert pending.kind == kind
    assert (
        select_context(answer, has_context=True, question=pending, now=101).kind
        == "question_answer"
    )
    assert not pending.accepts(answer, now=100 + QUESTION_TTL)
    assert not pending.accepts(answer, now=99)


@pytest.mark.parametrize(
    "text",
    [
        "Licht an",
        "Wie ist das Wetter heute?",
        "Berlin ist die Hauptstadt",
        "Schalte den Fernseher aus",
        "Neue Frage?",
        "room\nother room",
    ],
)
def test_entity_question_does_not_capture_new_commands_or_topics(text: str) -> None:
    """A pending device question is not a reason to attach unrelated context."""
    question = PendingQuestion("entity", 100)
    assert not question.accepts(text, 101)
    assert (
        select_context(text, has_context=True, question=question, now=101).kind
        != "question_answer"
    )


def test_no_question_metadata_from_complete_or_quoted_answer() -> None:
    """Mentioning a question in an answer does not open a new question slot."""
    assert pending_question("Wie viel? Die Antwort ist 42.", now=100) is None
    assert pending_question("Die Frage lautet: 'Welches Gerät?'", now=100) is None
    for question in (
        None,
        PendingQuestion("number", 100),
        PendingQuestion("yes_no", 0),
    ):
        assert (
            select_context("ja", has_context=True, question=question, now=200).kind
            == "clarify"
        )
