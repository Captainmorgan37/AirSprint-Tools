"""Utility helpers for filtering NOTAM text."""
from __future__ import annotations

from datetime import datetime, time, timedelta
import re
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

# Keywords that indicate runway surface condition (RSC) data is present.
_RSC_KEYWORDS: tuple[str, ...] = (
    "RSC",
)


_CLOSURE_RANGE_RE = re.compile(r"closed[^0-9]*?(\d{3,4})[-–](\d{3,4})", re.IGNORECASE)


def _parse_hhmm(raw: str) -> time | None:
    """Convert a numeric HHMM or HMM string into a :class:`datetime.time`.

    Returns ``None`` when the input cannot be parsed as a valid clock time.
    """

    digits = raw.strip()
    if not digits.isdigit() or len(digits) not in (3, 4):
        return None

    hour = int(digits[:-2])
    minute = int(digits[-2:])
    if not (0 <= hour < 24 and 0 <= minute < 60):
        return None

    return time(hour=hour, minute=minute)


def _build_closure_window(reference: datetime, start: time, end: time) -> tuple[datetime, datetime]:
    """Return the closure window spanning ``reference`` or the preceding day.

    The returned tuple contains the start and end datetimes for the closure
    window anchored to ``reference.date()``.  Overnight closures (where the
    closing time is earlier than the opening time) automatically roll the end
    into the following day.
    """

    start_dt = datetime.combine(reference.date(), start, tzinfo=reference.tzinfo)
    end_date = reference.date() if start <= end else reference.date() + timedelta(days=1)
    end_dt = datetime.combine(end_date, end, tzinfo=reference.tzinfo)
    return start_dt, end_dt


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
    present and no runway keywords are found. RSC (runway surface condition)
    NOTAMs are always retained even if taxiway keywords appear. Empty or
    ``None`` values are treated as not taxiway-only so they remain visible by
    default.
    """

    if not notam_text:
        return False

    text_upper = notam_text.upper()

    has_taxiway_reference = _contains_any(text_upper, _TAXIWAY_KEYWORDS)
    if not has_taxiway_reference:
        return False

    has_rsc_reference = _contains_any(text_upper, _RSC_KEYWORDS)
    if has_rsc_reference:
        return False

    has_runway_reference = _contains_any(text_upper, _RUNWAY_KEYWORDS)
    return not has_runway_reference


def evaluate_closure_notam(
    notam_text: str,
    planned_time_local: datetime,
    *,
    caution_buffer_minutes: int = 90,
) -> str:
    """Assess the impact of a closure NOTAM against a planned local time.

    The function expects NOTAM text containing a time range like
    ``"AIRPORT CLOSED 2000-0600 LOCAL"``.  The planned time must be a
    :class:`datetime.datetime` representing the local time of arrival or
    departure that should be evaluated.  Results are returned as one of the
    feasibility status strings: ``"FAIL"`` when inside the closure window,
    ``"CAUTION"`` when within ``caution_buffer_minutes`` of the closure start or
    end, and ``"INFO"`` otherwise.
    """

    match = _CLOSURE_RANGE_RE.search(notam_text)
    if not match:
        return "INFO"

    start_raw, end_raw = match.groups()
    start_time = _parse_hhmm(start_raw)
    end_time = _parse_hhmm(end_raw)
    if start_time is None or end_time is None:
        return "INFO"

    windows: list[tuple[datetime, datetime]] = []
    for offset_days in (0, -1):
        ref = planned_time_local + timedelta(days=offset_days)
        windows.append(_build_closure_window(ref, start_time, end_time))

    caution_delta = timedelta(minutes=caution_buffer_minutes)
    closest_edge: timedelta | None = None

    for window_start, window_end in windows:
        if window_start <= planned_time_local <= window_end:
            return "FAIL"

        distance = min(
            abs(window_start - planned_time_local),
            abs(window_end - planned_time_local),
        )
        if closest_edge is None or distance < closest_edge:
            closest_edge = distance

    if closest_edge is not None and closest_edge <= caution_delta:
        return "CAUTION"

    return "INFO"


__all__ = ["is_taxiway_only_notam", "evaluate_closure_notam"]
