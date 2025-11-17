"""Airport, customs, and ground handling checks."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional

from flight_leg_utils import load_airport_metadata_lookup

from .airport_module import (
    AirportFeasibilityResult,
    build_leg_context_from_flight,
    evaluate_airport_feasibility_for_leg,
)
from .data_access import AirportCategoryRecord, CustomsRule, load_airport_categories, load_customs_rules
from .schemas import CategoryResult, combine_statuses


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

    leg = build_leg_context_from_flight(flight, airport_metadata=lookup)
    if not leg:
        return CategoryResult(
            status="CAUTION",
            summary="Missing airport data",
            issues=["Departure or arrival airport missing; unable to run feasibility."],
        )

    tz_provider = _get_tz_from_lookup(lookup)
    feasibility_result = evaluate_airport_feasibility_for_leg(
        leg,
        tz_provider=tz_provider,
        airport_metadata=lookup,
        airport_categories=categories,
        customs_rules=customs,
        now=datetime.now(timezone.utc),
    )
    return _summarize_airport_feasibility(feasibility_result)
