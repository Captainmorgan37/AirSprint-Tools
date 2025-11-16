"""Airport, customs, and ground handling checks."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from deice_info_helper import has_deice_available
from flight_leg_utils import load_airport_metadata_lookup

from .common import extract_airport_code, get_country_for_airport
from .data_access import AirportCategoryRecord, CustomsRule, load_airport_categories, load_customs_rules
from .schemas import CategoryResult, CategoryStatus

_ALERT_PRIORITY: Dict[CategoryStatus, int] = {"PASS": 0, "CAUTION": 1, "FAIL": 2}


def _pick_summary(alerts: List[tuple[CategoryStatus, str]]) -> str:
    if not alerts:
        return "Airports verified"
    alerts_sorted = sorted(alerts, key=lambda item: _ALERT_PRIORITY[item[0]], reverse=True)
    return alerts_sorted[0][1]


def _international(dep_country: Optional[str], arr_country: Optional[str]) -> bool:
    return bool(dep_country and arr_country and dep_country != arr_country)


def evaluate_airport(
    flight: Mapping[str, Any],
    *,
    airport_lookup: Optional[Mapping[str, Mapping[str, Optional[str]]]] = None,
    airport_categories: Optional[Mapping[str, AirportCategoryRecord]] = None,
    customs_rules: Optional[Mapping[str, CustomsRule]] = None,
) -> CategoryResult:
    lookup = airport_lookup or load_airport_metadata_lookup()
    categories = airport_categories or load_airport_categories()
    customs = customs_rules or load_customs_rules()

    dep = extract_airport_code(flight, arrival=False)
    arr = extract_airport_code(flight, arrival=True)

    dep_country = get_country_for_airport(dep, lookup)
    arr_country = get_country_for_airport(arr, lookup)

    alerts: List[tuple[CategoryStatus, str]] = []
    issues: List[str] = []

    if not dep or not arr:
        alerts.append(("CAUTION", "Missing departure or arrival airport"))
        issues.append("Ensure both departure and arrival airports are populated in FL3XX.")
    else:
        issues.append(f"Route: {dep} → {arr}")

    category_record = categories.get(arr) if arr else None
    if category_record:
        if category_record.category in {"SSA", "OSA"}:
            minutes = category_record.min_ground_time_minutes or 90
            alerts.append(("CAUTION", f"{arr} classified as {category_record.category}"))
            issues.append(f"{arr} requires at least {minutes} minutes on the ground.")
            if category_record.notes:
                issues.append(category_record.notes)
    elif arr:
        issues.append(f"No category configured for {arr}; treating as STANDARD.")

    if _international(dep_country, arr_country):
        customs_rule = customs.get(arr) if arr else None
        if customs_rule is None:
            alerts.append(("CAUTION", "International arrival missing customs rule"))
            issues.append(f"No customs data found for {arr}; confirm lead times manually.")
        else:
            issues.append(
                f"Customs service type for {arr}: {customs_rule.service_type or 'Unknown'}."
            )
            if customs_rule.notes:
                issues.append(customs_rule.notes)

    if arr:
        deice = has_deice_available(icao=arr)
        if deice is False:
            alerts.append(("CAUTION", f"No deice available at {arr}"))
            issues.append(f"Arrange alternate deice support for {arr} or plan tech stop.")
        elif deice is None:
            issues.append(f"No deice intel for {arr}; monitor forecast if icing possible.")

    summary = _pick_summary(alerts)
    status = "PASS" if not alerts else max(alerts, key=lambda a: _ALERT_PRIORITY[a[0]])[0]
    if status == "PASS" and issues:
        summary = "Airports verified"

    return CategoryResult(status=status, summary=summary, issues=issues)
