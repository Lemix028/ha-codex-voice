"""Deterministic, content-free decisions about explicitly needed short context."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

CONTEXT_ADAPTIVE = "single_turn_first"
CONTEXT_LEGACY = "legacy"
CONTEXT_MODES = (CONTEXT_ADAPTIVE, CONTEXT_LEGACY)
QUESTION_TTL = 120.0

_EXPLICIT = frozenset(
    {
        "weiter",
        "weiter bitte",
        "mach weiter",
        "fortsetzen",
        "gespräch fortsetzen",
        "setz das gespräch fort",
        "setze das gespräch fort",
        "continue",
        "continue our conversation",
    }
)
_FOLLOW_UP = re.compile(
    r"^(?:erklär(?:e)? (?:mir )?(?:das|es) (?:genauer|näher|nochmal|weiter)|"
    r"(?:mehr|weiteres) dazu|(?:und )?(?:warum|wieso)(?: ist| war| passiert)? (?:das|es)|"
    r"(?:und )?(?:was|wie viel|wieviel|wie|wann|wo).*(?:das|es|dazu|damit|davon)(?: (?:im|pro|bei|noch|denn|nun).*)?|"
    r"(?:und )?(?:mach(?:e)?|schalte) (?:es|sie|ihn|das) (?:wieder )?(?:aus|an|heller|dunkler)|"
    r"und (?:dann|sonst|wie viel|wieviel)|tell me more|explain (?:that|it) (?:further|in more detail)|"
    r"(?:and )?(?:what|how much|how|why).*(?:that|it|about that)(?: (?:cost|mean|work|called)(?: (?:per|in|with).*)?)?)$"
)
_YES_NO = frozenset({"ja", "nein", "ja bitte", "nein danke", "yes", "no", "yes please"})
_NEW_QUESTION = re.compile(
    r"^(?:was|wer|wie|wo|wann|warum|wieso|erklär(?:e)?|schalte|stell(?:e)?|mach(?:e)?|öffne|schließe|what|who|how|where|when|why|explain|turn|switch|set|open|close)\b"
)
_NUMBER = re.compile(
    r"^\d+(?:[.,]\d+)?(?:\s+(?:cent|euro|eur|€|pro|je|kwh|kilowattstunde|monat|monate|jahr|jahre|tage|day|days|month|months|year|years|per|at|a))*$"
)


def normalized_text(text: str) -> str:
    """Normalize command matching without ever using text in diagnostics."""
    value = " ".join(text.casefold().strip().split()).rstrip(".!?")
    return value.removeprefix("computer,").strip()


@dataclass(frozen=True, slots=True)
class PendingQuestion:
    """Short-lived answer shape; vocabulary never leaves the RAM context."""

    kind: Literal["yes_no", "number", "entity", "choice"]
    created: float
    choices: frozenset[str] = field(default_factory=frozenset, repr=False)

    def accepts(self, text: str, now: float) -> bool:
        """Require a bounded answer shape and a recent actual question."""
        if not 0 <= now - self.created < QUESTION_TTL:
            return False
        value = normalized_text(text)
        if self.kind == "yes_no":
            return value in _YES_NO
        if self.kind == "number":
            return bool(_NUMBER.fullmatch(value))
        if self.kind == "choice":
            return value in self.choices
        return (
            bool(value)
            and len(value.split()) <= 6
            and not _NEW_QUESTION.match(value)
            and not re.search(r"\b(?:ist|sind|war|waren|is|are|was|were)\b", value)
            and not re.fullmatch(
                r"(?:das |die )?(?:licht|lichter|lampe|lampen) (?:an|ein|aus|um)", value
            )
            and not re.search(r"[?!\n]", text)
        )


@dataclass(frozen=True, slots=True)
class ContextDecision:
    """Only fixed categories can enter the diagnostic ring."""

    kind: Literal["independent", "follow_up", "question_answer", "explicit", "clarify"]

    @property
    def use_context(self) -> bool:
        """Keep unrelated history out of every independent turn."""
        return self.kind in {"follow_up", "question_answer", "explicit"}


def select_context(
    text: str, *, has_context: bool, question: PendingQuestion | None, now: float
) -> ContextDecision:
    """Use no model call and never guess the subject of an orphaned reference."""
    value = normalized_text(text)
    if value in _EXPLICIT:
        return ContextDecision("explicit" if has_context else "clarify")
    if _FOLLOW_UP.fullmatch(value):
        return ContextDecision("follow_up" if has_context else "clarify")
    if has_context and question is not None and question.accepts(text, now):
        return ContextDecision("question_answer")
    if value in _YES_NO:
        return ContextDecision("clarify")
    return ContextDecision("independent")


def pending_question(answer: str, *, now: float) -> PendingQuestion | None:
    """Recognize a final question, rather than every question quoted in an answer."""
    if not answer.rstrip().endswith("?"):
        return None
    question = re.split(r"[.!?\n]", answer.rstrip()[:-1])[-1].strip().casefold()
    if " oder " in question or " or " in question:
        choices = re.findall(r"\b[\w-]+\b", question)
        separator = "oder" if "oder" in choices else "or"
        at = choices.index(separator)
        if at > 0 and at + 1 < len(choices):
            return PendingQuestion(
                "choice", now, frozenset({choices[at - 1], choices[at + 1]})
            )
    if re.match(
        r"(?:soll|möchtest|willst|hast|ist|sind|do you|would you|should i)\b", question
    ):
        return PendingQuestion("yes_no", now)
    if re.search(r"tarif|preis|budget|wie (?:viel|hoch)|how much", question):
        return PendingQuestion("number", now)
    if re.search(
        r"welch\w* (?:gerät|lampe|licht|raum|zimmer)|which (?:device|light|room)",
        question,
    ):
        return PendingQuestion("entity", now)
    return None
