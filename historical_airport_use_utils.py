"""Helpers for historical airport usage analytics."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

UTC = timezone.utc

ATLANTIC_CANADA_SUBDIVISIONS = {
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

CARIBBEAN_COUNTRY_CODES = {
    "AG",
    "AI",
    "AW",
    "BB",
    "BL",
    "BM",
    "BQ",
    "BS",
    "CU",
    "CW",
    "DM",
    "DO",
    "GD",
    "GP",
    "GY",
    "HT",
    "JM",
    "KN",
    "KY",
    "LC",
    "MF",
    "MQ",
    "MS",
    "PR",
    "SX",
    "TC",
    "TT",
    "VC",
    "VG",
    "VI",
}

EUROPE_COUNTRY_CODES = {
    "AL",
    "AD",
    "AT",
    "BA",
    "BE",
    "BG",
    "BY",
    "CH",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FO",
    "FR",
    "GB",
    "GG",
    "GI",
    "GR",
    "HR",
    "HU",
    "IE",
    "IM",
    "IS",
    "IT",
    "JE",
    "LI",
    "LT",
    "LU",
    "LV",
    "MC",
    "MD",
    "ME",
    "MK",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "RS",
    "SE",
    "SI",
    "SK",
    "SM",
    "UA",
    "VA",
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


def _normalise_country(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if text == "CANADA":
        return "CA"
    return text


def _normalise_subdivision(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    return text or None


def lookup_airport_record(code: Optional[str], lookup: Mapping[str, Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    if not code:
        return None
    record = lookup.get(code.upper())
    if not isinstance(record, Mapping):
        return None
    return record


def airport_country_code(code: Optional[str], lookup: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    record = lookup_airport_record(code, lookup)
    if not record:
        return None
    return _normalise_country(record.get("country"))


def airport_subdivision(code: Optional[str], lookup: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    record = lookup_airport_record(code, lookup)
    if not record:
        return None
    return _normalise_subdivision(record.get("subd"))


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


def airport_matches_focus(code: Optional[str], lookup: Mapping[str, Mapping[str, Any]], focus: str) -> bool:
    country = airport_country_code(code, lookup)
    subdivision = airport_subdivision(code, lookup)
    focus_key = focus.strip().lower()

    if focus_key == "atlantic_canada":
        return country == "CA" and subdivision in ATLANTIC_CANADA_SUBDIVISIONS
    if focus_key == "caribbean":
        return country in CARIBBEAN_COUNTRY_CODES
    if focus_key == "europe":
        return country in EUROPE_COUNTRY_CODES

    return False


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
