"""Helpers for classifying airport operational notes."""

from __future__ import annotations

from typing import Mapping, Sequence

from .schemas import CategoryResult, CategoryStatus

_STATUS_PRIORITY: Mapping[CategoryStatus, int] = {"PASS": 0, "CAUTION": 1, "FAIL": 2}

_HAZARD_KEYWORDS: Sequence[tuple[str, CategoryStatus]] = (
    ("closure", "FAIL"),
    ("closed", "FAIL"),
    ("no ga", "FAIL"),
    ("curfew", "CAUTION"),
    ("construction", "CAUTION"),
    ("runway work", "CAUTION"),
    ("deice", "CAUTION"),
    ("cdf", "CAUTION"),
    ("slot", "CAUTION"),
    ("ppr", "CAUTION"),
)


def _combine_status(existing: CategoryStatus, candidate: CategoryStatus) -> CategoryStatus:
    return candidate if _STATUS_PRIORITY[candidate] > _STATUS_PRIORITY[existing] else existing


def _note_text(note: Mapping[str, object]) -> str:
    parts = []
    for key in ("title", "body"):
        value = note.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    if not parts:
        category = note.get("category") or note.get("type")
        if isinstance(category, str) and category.strip():
            parts.append(category.strip())
    return "; ".join(parts) if parts else "Operational note"


def summarize_operational_notes(icao: str, notes: Sequence[Mapping[str, object]]) -> CategoryResult:
    """Return a CategoryResult summarising operational notes for an airport."""

    if not notes:
        return CategoryResult(status="PASS", summary="No operational notes", issues=[])

    status: CategoryStatus = "PASS"
    issues = []
    for note in notes:
        text = _note_text(note)
        lower = text.lower()
        for keyword, keyword_status in _HAZARD_KEYWORDS:
            if keyword in lower:
                status = _combine_status(status, keyword_status)
                break
        issues.append(text)

    summary = "Operational notes available" if status == "PASS" else "Operational hazards detected"
    return CategoryResult(status=status, summary=summary, issues=issues)

