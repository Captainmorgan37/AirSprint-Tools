"""Helpers for extracting and validating planning notes content."""

from __future__ import annotations

from datetime import date
import re
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from flight_leg_utils import safe_parse_dt

_MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_REQUEST_PATTERN = re.compile(r"request(?:ing|ed)\s+([A-Z0-9]{2,6})", re.IGNORECASE)


def extract_planning_note_text(payload: Any) -> Optional[str]:
    """Best-effort extraction of planning note text from varied payloads."""

    if isinstance(payload, str):
        text = payload.strip()
        return text or None
    if isinstance(payload, Mapping):
        for key in ("planningNotes", "planningNote", "note", "notes", "text"):
            value = payload.get(key)
            if isinstance(value, str):
                text = value.strip()
                if text:
                    return text
        data = payload.get("data")
        if isinstance(data, Iterable) and not isinstance(data, (str, bytes, bytearray)):
            for item in data:
                text = extract_planning_note_text(item)
                if text:
                    return text
        return None
    if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            text = extract_planning_note_text(item)
            if text:
                return text
    return None


def extract_requested_aircraft_from_note(note: str) -> Optional[str]:
    """Return the requested aircraft label embedded in a planning note, if any."""

    if not isinstance(note, str):
        return None
    match = _REQUEST_PATTERN.search(note)
    if match:
        return match.group(1).upper()
    return None


def _parse_note_date(token: str, *, default_year: Optional[int]) -> Optional[date]:
    match = re.match(r"^(\d{1,2})([A-Z]{3})(\d{2})?$", token)
    if not match:
        return None
    day = int(match.group(1))
    month = _MONTH_MAP.get(match.group(2).upper())
    if not month:
        return None
    year_component = match.group(3)
    year = default_year or date.today().year
    if year_component:
        year = 2000 + int(year_component)
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_route_points(route_text: str) -> List[str]:
    cleaned = re.sub(r"\[.*?\]", "", route_text)
    cleaned = re.sub(r"\(.*?\)", "", cleaned)
    cleaned = cleaned.replace("→", "-")
    parts = re.split(r"\s*-\s*", cleaned)
    points: List[str] = []
    for part in parts:
        code = part.strip().upper()
        if not code or not re.match(r"^[A-Z0-9]{3,4}$", code):
            continue
        points.append(code)
    return points


def parse_route_entries_from_note(
    note: str, *, default_year: Optional[int]
) -> List[Tuple[date, List[str]]]:
    """Extract date-tagged route sequences from a planning note."""

    if not isinstance(note, str):
        return []

    entries: List[Tuple[date, List[str]]] = []
    pattern = re.compile(r"^(\d{1,2}[A-Z]{3}(?:\d{2})?)\s+(.*)$")
    for raw_line in note.splitlines():
        line = raw_line.strip().strip("-=").strip()
        if not line:
            continue
        match = pattern.match(line)
        if not match:
            continue
        parsed_date = _parse_note_date(match.group(1), default_year=default_year)
        if not parsed_date:
            continue
        route_points = _extract_route_points(match.group(2))
        if route_points:
            entries.append((parsed_date, route_points))
    return entries


def find_route_mismatch(
    dep_icao: str, arr_icao: str, departure_time: Optional[str], planning_note: str
) -> Optional[str]:
    """Return an issue string when the planning note route differs from the flight."""

    if not planning_note:
        return None
    dep = (dep_icao or "").upper()
    arr = (arr_icao or "").upper()
    dep_dt = safe_parse_dt(departure_time) if departure_time else None
    default_year = dep_dt.year if dep_dt else None
    entries = parse_route_entries_from_note(planning_note, default_year=default_year)
    if not entries or dep_dt is None:
        return None

    matching: List[Tuple[date, List[str]]] = [
        (entry_date, route) for entry_date, route in entries if entry_date == dep_dt.date()
    ]
    if not matching:
        return None

    def _route_contains_segment(route: Sequence[str]) -> bool:
        for idx, code in enumerate(route[:-1]):
            if code == dep and route[idx + 1] == arr:
                return True
        return False

    for entry_date, route in matching:
        if _route_contains_segment(route):
            return None
        route_label = "-".join(route)
        return (
            f"Planning notes route for {entry_date.isoformat()} ({route_label}) "
            f"does not match booked {dep}→{arr}."
        )
    return None
