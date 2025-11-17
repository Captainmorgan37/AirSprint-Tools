"""Utility helpers shared by feasibility checkers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

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


def get_subdivision_for_airport(
    airport_code: Optional[str],
    lookup: Mapping[str, Mapping[str, Optional[str]]],
) -> Optional[str]:
    if not airport_code:
        return None
    record = lookup.get(airport_code.upper())
    if not record:
        return None
    subdivision = record.get("subd")
    if isinstance(subdivision, str):
        text = subdivision.strip()
        return text or None
    return None


REGULAR_CATEGORY = "REGULAR"
SSA_CATEGORY = "SSA"
OSA_CATEGORY = "OSA"
SSA_MIN_GROUND_MINUTES = 90
OSA_MIN_GROUND_MINUTES = 120

_CARIBBEAN_COUNTRY_CODES = {
    "AG",  # Antigua and Barbuda
    "AI",  # Anguilla
    "AW",  # Aruba
    "BB",  # Barbados
    "BL",  # Saint Barthelemy
    "BQ",  # Caribbean Netherlands
    "BS",  # Bahamas
    "CU",  # Cuba
    "CW",  # Curacao
    "DM",  # Dominica
    "DO",  # Dominican Republic
    "GD",  # Grenada
    "GP",  # Guadeloupe
    "HT",  # Haiti
    "JM",  # Jamaica
    "KN",  # Saint Kitts and Nevis
    "KY",  # Cayman Islands
    "LC",  # Saint Lucia
    "MF",  # Saint Martin (French)
    "MQ",  # Martinique
    "MS",  # Montserrat
    "SX",  # Sint Maarten (Dutch)
    "TC",  # Turks and Caicos
    "TT",  # Trinidad and Tobago
    "VC",  # Saint Vincent and the Grenadines
    "VG",  # British Virgin Islands
    "VI",  # U.S. Virgin Islands
}

_SSA_US_SUBDIVISIONS = {
    "ALASKA",
    "HAWAII",
    "PUERTO RICO",
    "VIRGIN ISLANDS",
}

_CONTIGUOUS_US_SUBDIVISIONS = {
    "ALABAMA",
    "ARIZONA",
    "ARKANSAS",
    "CALIFORNIA",
    "COLORADO",
    "CONNECTICUT",
    "DELAWARE",
    "DISTRICT OF COLUMBIA",
    "FLORIDA",
    "GEORGIA",
    "IDAHO",
    "ILLINOIS",
    "INDIANA",
    "IOWA",
    "KANSAS",
    "KENTUCKY",
    "LOUISIANA",
    "MAINE",
    "MARYLAND",
    "MASSACHUSETTS",
    "MICHIGAN",
    "MINNESOTA",
    "MISSISSIPPI",
    "MISSOURI",
    "MONTANA",
    "NEBRASKA",
    "NEVADA",
    "NEW HAMPSHIRE",
    "NEW JERSEY",
    "NEW MEXICO",
    "NEW YORK",
    "NORTH CAROLINA",
    "NORTH DAKOTA",
    "OHIO",
    "OKLAHOMA",
    "OREGON",
    "PENNSYLVANIA",
    "RHODE ISLAND",
    "SOUTH CAROLINA",
    "SOUTH DAKOTA",
    "TENNESSEE",
    "TEXAS",
    "UTAH",
    "VERMONT",
    "VIRGINIA",
    "WASHINGTON",
    "WEST VIRGINIA",
    "WISCONSIN",
    "WYOMING",
}


@dataclass(frozen=True)
class AirportCategoryResult:
    airport: Optional[str]
    category: str
    reasons: Tuple[str, ...]
    country: Optional[str]
    subdivision: Optional[str]


@dataclass(frozen=True)
class FlightCategoryResult:
    category: str
    reasons: Tuple[str, ...]


def _normalize(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    return text.upper() if text else None


def _format_region(value: Optional[str]) -> str:
    if not value:
        return "unknown region"
    return value.title()


def classify_airport_category(
    airport_code: Optional[str],
    lookup: Mapping[str, Mapping[str, Optional[str]]],
) -> AirportCategoryResult:
    code = (airport_code or "").strip().upper() or None
    country = _normalize(get_country_for_airport(airport_code, lookup))
    subdivision = _normalize(get_subdivision_for_airport(airport_code, lookup))

    display = code or "Unknown airport"

    if not code:
        return AirportCategoryResult(
            airport=None,
            category=OSA_CATEGORY,
            reasons=("Missing airport code; treating as OSA until resolved.",),
            country=country,
            subdivision=subdivision,
        )

    if not country:
        return AirportCategoryResult(
            airport=code,
            category=OSA_CATEGORY,
            reasons=(f"No country metadata for {display}; defaulting to OSA.",),
            country=country,
            subdivision=subdivision,
        )

    if country == "CA":
        if subdivision == "NUNAVUT":
            reason = f"{display} is in Nunavut; SSA handling applies."
            return AirportCategoryResult(code, SSA_CATEGORY, (reason,), country, subdivision)
        reason = f"{display} is in Canada (core service area)."
        return AirportCategoryResult(code, REGULAR_CATEGORY, (reason,), country, subdivision)

    if country == "US":
        if subdivision in _SSA_US_SUBDIVISIONS:
            reason = f"{display} is in {_format_region(subdivision)}; SSA handling applies."
            return AirportCategoryResult(code, SSA_CATEGORY, (reason,), country, subdivision)
        if subdivision in _CONTIGUOUS_US_SUBDIVISIONS:
            reason = f"{display} is in the contiguous United States."
            return AirportCategoryResult(code, REGULAR_CATEGORY, (reason,), country, subdivision)
        return AirportCategoryResult(
            code,
            OSA_CATEGORY,
            (f"{display} is in {_format_region(subdivision)}; treat as OSA.",),
            country,
            subdivision,
        )

    if country == "MX":
        reason = f"{display} is in Mexico; SSA handling applies."
        return AirportCategoryResult(code, SSA_CATEGORY, (reason,), country, subdivision)

    if country in _CARIBBEAN_COUNTRY_CODES:
        reason = f"{display} is in the Caribbean ({country})."
        return AirportCategoryResult(code, SSA_CATEGORY, (reason,), country, subdivision)

    reason = f"{display} is outside Canada and the contiguous United States."
    return AirportCategoryResult(code, OSA_CATEGORY, (reason,), country, subdivision)


def classify_flight_category(
    dep_airport: Optional[str],
    arr_airport: Optional[str],
    lookup: Mapping[str, Mapping[str, Optional[str]]],
) -> FlightCategoryResult:
    dep_info = classify_airport_category(dep_airport, lookup)
    arr_info = classify_airport_category(arr_airport, lookup)

    reasons: list[str] = []
    cross_border_core = (
        dep_info.category == REGULAR_CATEGORY
        and arr_info.category == REGULAR_CATEGORY
        and dep_info.country in {"CA", "US"}
        and arr_info.country in {"CA", "US"}
        and dep_info.country != arr_info.country
    )

    if dep_info.category == SSA_CATEGORY or arr_info.category == SSA_CATEGORY:
        if dep_info.category == SSA_CATEGORY:
            reasons.extend(dep_info.reasons)
        if arr_info.category == SSA_CATEGORY:
            reasons.extend(arr_info.reasons)
        if not reasons:
            reasons.append("At least one airport is in the SSA region.")
        return FlightCategoryResult(SSA_CATEGORY, tuple(reasons))

    if (
        dep_info.category == REGULAR_CATEGORY
        and arr_info.category == REGULAR_CATEGORY
        and dep_info.country == arr_info.country
        and dep_info.country in {"CA", "US"}
    ):
        if dep_info.country == "CA":
            reason = "Both airports are in Canada (excluding Nunavut)."
        else:
            reason = "Both airports are in the contiguous United States."
        return FlightCategoryResult(REGULAR_CATEGORY, (reason,))

    if dep_info.category == OSA_CATEGORY:
        reasons.extend(dep_info.reasons)
    if arr_info.category == OSA_CATEGORY:
        reasons.extend(arr_info.reasons)
    if not reasons:
        if cross_border_core:
            reasons.append("Cross-border Canada/U.S. sector falls outside the Regular definition.")
        else:
            reasons.append("Flight extends outside the Regular/SSA definitions.")
    return FlightCategoryResult(OSA_CATEGORY, tuple(reasons))
