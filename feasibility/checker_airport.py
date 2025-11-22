"""Airport, customs, and ground handling checks."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional

from flight_leg_utils import load_airport_metadata_lookup

from deice_info_helper import has_deice_available

from .airport_module import (
    AirportFeasibilityResult,
    build_leg_context_from_flight,
    evaluate_airport_feasibility_for_leg,
)
from .common import (
    OSA_CATEGORY,
    OSA_MIN_GROUND_MINUTES,
    SSA_CATEGORY,
    SSA_MIN_GROUND_MINUTES,
    classify_airport_category,
    extract_airport_code,
    get_country_for_airport,
)
from .data_access import CustomsRule, load_customs_rules
from .schemas import CategoryResult, CategoryStatus, combine_statuses

_ALERT_PRIORITY: Mapping[CategoryStatus, int] = {
    "PASS": 0,
    "INFO": 1,
    "CAUTION": 2,
    "FAIL": 3,
}


def _international(
    dep_country: Optional[str], arr_country: Optional[str]
) -> bool:  # pragma: no cover - exercised via evaluate_airport
    """Return ``True`` when the given countries form an international leg.

    ``None``/blank values are treated as unknown, so the leg is assumed to be
    domestic (``False``) until both ends have concrete values. Comparisons are
    case-insensitive to avoid mismatches caused by inconsistent casing in the
    source data.
    """

    if not dep_country or not arr_country:
        return False

    return dep_country.strip().upper() != arr_country.strip().upper()


def _international(
    dep_country: Optional[str], arr_country: Optional[str]
) -> bool:  # pragma: no cover - exercised via evaluate_airport
    """Return ``True`` when the given countries form an international leg.

    ``None``/blank values are treated as unknown, so the leg is assumed to be
    domestic (``False``) until both ends have concrete values. Comparisons are
    case-insensitive to avoid mismatches caused by inconsistent casing in the
    source data.
    """

    if not dep_country or not arr_country:
        return False

    return dep_country.strip().upper() != arr_country.strip().upper()


def _get_tz_from_lookup(
    lookup: Mapping[str, Mapping[str, Optional[str]]]
) -> Optional[Callable[[str], Optional[str]]]:  # pragma: no cover - lightweight helper
    if not lookup:
        return None

    def provider(icao: str) -> Optional[str]:
        record = lookup.get(icao.upper())
        if not isinstance(record, Mapping):
            return None
        tz = record.get("tz")
        if isinstance(tz, str) and tz.strip():
            return tz.strip()
        return None

    return provider


def _summarize_airport_feasibility(result: AirportFeasibilityResult) -> CategoryResult:
    issues: List[str] = []
    statuses: List[str] = []
    for label, category_result in result.iter_all_categories():
        statuses.append(category_result.status)
        if category_result.status == "PASS":
            continue
        issues.append(f"{label}: {category_result.summary}")
        for detail in category_result.issues:
            issues.append(f"- {detail}")

    status = combine_statuses(statuses)
    summary_map = {
        "PASS": "Airports verified",
        "CAUTION": "Airport cautions detected",
        "FAIL": "Airport blockers detected",
    }
    summary = summary_map.get(status, "Airport checks complete")
    return CategoryResult(status=status, summary=summary, issues=issues)


def _pick_summary(alerts: List[tuple[CategoryStatus, str]]) -> str:
    if not alerts:
        return "Airports verified"
    worst_alert = max(alerts, key=lambda alert: _ALERT_PRIORITY.get(alert[0], 0))
    return worst_alert[1]


def evaluate_airport(
    flight: Mapping[str, Any],
    *,
    airport_lookup: Optional[Mapping[str, Mapping[str, Optional[str]]]] = None,
    customs_rules: Optional[Mapping[str, CustomsRule]] = None,
) -> CategoryResult:
    lookup = airport_lookup or load_airport_metadata_lookup()
    customs = customs_rules or load_customs_rules()

    tz_provider = _get_tz_from_lookup(lookup)
    leg_context = build_leg_context_from_flight(flight, airport_metadata=lookup)
    module_result: Optional[AirportFeasibilityResult] = None
    if leg_context:
        try:
            module_result = evaluate_airport_feasibility_for_leg(
                leg_context,
                tz_provider=tz_provider,
                airport_metadata=lookup,
                customs_rules=customs,
            )
        except Exception:
            module_result = None
    if module_result:
        return _summarize_airport_feasibility(module_result)

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

    if arr:
        arrival_category = classify_airport_category(arr, lookup)
        if arrival_category.category == SSA_CATEGORY:
            minutes = SSA_MIN_GROUND_MINUTES
            alerts.append(("CAUTION", f"{arr} classified as {SSA_CATEGORY}"))
            issues.append(f"{arr} requires at least {minutes} minutes on the ground.")
            issues.extend(arrival_category.reasons)
        elif arrival_category.category == OSA_CATEGORY:
            minutes = OSA_MIN_GROUND_MINUTES
            alerts.append(("CAUTION", f"{arr} classified as {OSA_CATEGORY}"))
            issues.append(f"{arr} requires at least {minutes} minutes on the ground.")
            issues.extend(arrival_category.reasons)

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
