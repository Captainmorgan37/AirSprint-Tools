"""Utility helpers shared by feasibility checkers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

from flight_leg_utils import ARRIVAL_AIRPORT_COLUMNS, DEPARTURE_AIRPORT_COLUMNS, safe_parse_dt

STRING_KEYS = Sequence[str]


def extract_first_str(data: Mapping[str, Any], keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def extract_airport_code(data: Mapping[str, Any], *, arrival: bool) -> Optional[str]:
    columns = ARRIVAL_AIRPORT_COLUMNS if arrival else DEPARTURE_AIRPORT_COLUMNS
    code = extract_first_str(data, columns)
    if code:
        return code.upper()
    return None


def parse_minutes(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value != value:  # NaN
            return None
        minutes = int(round(float(value)))
        return minutes if minutes >= 0 else None
    text = str(value).strip()
    if not text:
        return None
    if ":" in text:
        try:
            parts = text.split(":")
            hours = int(parts[0])
            mins = int(parts[1]) if len(parts) > 1 else 0
            return hours * 60 + mins
        except ValueError:
            return None
    try:
        minutes = int(float(text))
        return minutes if minutes >= 0 else None
    except ValueError:
        return None


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return safe_parse_dt(str(value))
    except Exception:
        return None


def extract_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_country_for_airport(
    airport_code: Optional[str],
    lookup: Mapping[str, Mapping[str, Optional[str]]],
) -> Optional[str]:
    if not airport_code:
        return None
    record = lookup.get(airport_code.upper())
    if not record:
        return None
    country = record.get("country")
    if isinstance(country, str):
        text = country.strip()
        return text or None
    return None
