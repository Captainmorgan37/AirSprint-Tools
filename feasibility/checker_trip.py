"""Trip planning checks covering Jeppesen / OSA / SSA rules."""

from __future__ import annotations

from typing import Any, List, Mapping, Optional

from flight_leg_utils import load_airport_metadata_lookup

from .common import classify_flight_category, extract_airport_code, get_country_for_airport, OSA_CATEGORY, SSA_CATEGORY
from .schemas import CategoryResult


def _is_high_risk_country(country: Optional[str]) -> bool:
    if not country:
        return False
    high_risk = {
        "RUSSIA",
        "CHINA",
        "SAUDI ARABIA",
        "IRAN",
        "IRAQ",
        "SYRIA",
        "CUBA",
    }
    return country.upper() in high_risk


def evaluate_trip(
    flight: Mapping[str, Any],
    *,
    airport_lookup: Optional[Mapping[str, Mapping[str, Optional[str]]]] = None,
) -> CategoryResult:
    lookup = airport_lookup or load_airport_metadata_lookup()

    dep = extract_airport_code(flight, arrival=False)
    arr = extract_airport_code(flight, arrival=True)

    dep_country = get_country_for_airport(dep, lookup)
    arr_country = get_country_for_airport(arr, lookup)

    issues: List[str] = []
    flags: List[str] = []

    classification = classify_flight_category(dep, arr, lookup)
    if classification.category in {SSA_CATEGORY, OSA_CATEGORY}:
        flags.append(f"{classification.category} sector; Jeppesen planning required.")
        issues.extend(classification.reasons)

    if dep_country and arr_country and dep_country != arr_country:
        flags.append("International sector; confirm Jeppesen support.")

    for country in (dep_country, arr_country):
        if _is_high_risk_country(country):
            flags.append(f"Operations in {country} trigger Jeppesen oversight.")

    if flags:
        summary = flags[0]
        issues.extend(flags)
        return CategoryResult(status="CAUTION", summary=summary, issues=issues)

    return CategoryResult(status="PASS", summary="Trip planning in compliance", issues=issues or ["No Jeppesen triggers detected."])
