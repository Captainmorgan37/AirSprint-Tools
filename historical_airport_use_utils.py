"""Helpers for historical airport usage analytics."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

UTC = timezone.utc
_ATLANTIC_SUBDIVISIONS = {
    "NB",
    "NEW BRUNSWICK",
    "CA-NB",
    "NS",
    "NOVA SCOTIA",
    "CA-NS",
    "PE",
    "PEI",
    "PRINCE EDWARD ISLAND",
    "CA-PE",
    "CA-PEI",
    "NL",
    "NEWFOUNDLAND",
    "NEWFOUNDLAND AND LABRADOR",
    "LABRADOR",
    "CA-NL",
}


def _coerce_airport_code(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, Mapping):
        for key in ("icao", "iata", "code", "airport", "name"):
            nested = value.get(key)
            if nested not in (None, ""):
                return _coerce_airport_code(nested)
        return None
    text = str(value).strip().upper()
    return text or None


def extract_airport_code(leg: Mapping[str, Any], columns: Sequence[str]) -> Optional[str]:
    for key in columns:
        if key in leg:
            code = _coerce_airport_code(leg.get(key))
            if code:
                return code
    return None


def is_positioning_leg(leg: Mapping[str, Any]) -> bool:
    for key in ("flightType", "flight_type", "workflowCustomName", "workflow", "operation_type"):
        value = leg.get(key)
        if value is None:
            continue
        text = str(value).strip().upper()
        if not text:
            continue
        if text == "POS" or "POSITION" in text:
            return True
    return False


def is_atlantic_canada_airport(code: Optional[str], lookup: Mapping[str, Mapping[str, Any]]) -> bool:
    if not code:
        return False
    record = lookup.get(code.upper())
    if not isinstance(record, Mapping):
        return False
    country = str(record.get("country") or "").strip().upper()
    if country not in {"CA", "CANADA"}:
        return False
    subdivision = str(record.get("subd") or "").strip().upper()
    return subdivision in _ATLANTIC_SUBDIVISIONS


def leg_duration_hours(leg: Mapping[str, Any]) -> Optional[float]:
    dep = leg.get("dep_time")
    arr = leg.get("arrival_time")
    if dep in (None, "") or arr in (None, ""):
        return None
    try:
        dep_dt = datetime.fromisoformat(str(dep).replace("Z", "+00:00"))
        arr_dt = datetime.fromisoformat(str(arr).replace("Z", "+00:00"))
    except ValueError:
        return None

    if dep_dt.tzinfo is None:
        dep_dt = dep_dt.replace(tzinfo=UTC)
    else:
        dep_dt = dep_dt.astimezone(UTC)
    if arr_dt.tzinfo is None:
        arr_dt = arr_dt.replace(tzinfo=UTC)
    else:
        arr_dt = arr_dt.astimezone(UTC)

    delta_hours = (arr_dt - dep_dt).total_seconds() / 3600.0
    if delta_hours <= 0:
        return None
    return round(delta_hours, 2)

