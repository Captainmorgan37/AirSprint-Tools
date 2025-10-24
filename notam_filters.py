"""Utility helpers for filtering NOTAM text."""
from __future__ import annotations

from typing import Iterable

# Keywords that typically indicate a NOTAM only affects taxiways.
_TAXIWAY_KEYWORDS: tuple[str, ...] = (
    "TWY",
    "TXY",
    "TAXIWAY",
    "TAXIWAYS",
    "TAXILANE",
    "TAXI LANE",
    "TAXIROUTE",
    "TAXI ROUTE",
    "TAXIING",
    "TAXI",
)

# Keywords that indicate a runway is mentioned in the NOTAM.
_RUNWAY_KEYWORDS: tuple[str, ...] = (
    "RWY",
    "RUNWAY",
    "RUNWAYS",
)


def _contains_any(text_upper: str, keywords: Iterable[str]) -> bool:
    """Return ``True`` when any keyword exists in ``text_upper``.

    ``text_upper`` should be an uppercase representation of the text being
    searched.  Keywords are also expected to be uppercase to keep comparisons
    case-insensitive without allocating additional strings.
    """

    return any(keyword in text_upper for keyword in keywords)


def is_taxiway_only_notam(notam_text: str | None) -> bool:
    """Return ``True`` when a NOTAM only contains taxiway information.

    A NOTAM is considered taxiway-only when at least one taxiway keyword is
    present and no runway keywords are found.  Empty or ``None`` values are
    treated as not taxiway-only so they remain visible by default.
    """

    if not notam_text:
        return False

    text_upper = notam_text.upper()

    has_taxiway_reference = _contains_any(text_upper, _TAXIWAY_KEYWORDS)
    if not has_taxiway_reference:
        return False

    has_runway_reference = _contains_any(text_upper, _RUNWAY_KEYWORDS)
    return not has_runway_reference


__all__ = ["is_taxiway_only_notam"]
