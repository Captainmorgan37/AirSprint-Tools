"""Simplified overflight permit evaluation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Set

from .common import parse_datetime
from .schemas import CategoryResult

_PERMIT_RULES: Dict[str, int] = {
    "CUBA": 72,
    "RUSSIA": 72,
    "CHINA": 96,
    "SAUDI ARABIA": 72,
    "MEXICO": 24,
}

_ROUTE_KEYS = (
    "routeCountries",
    "route_countries",
    "firCodes",
    "fir_codes",
    "overflightCountries",
)


def _normalize_country(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    return text.upper() or None


def _collect_route_countries(flight: Mapping[str, Any]) -> Set[str]:
    countries: Set[str] = set()
    for key in _ROUTE_KEYS:
        raw = flight.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            parts = [item.strip() for item in raw.replace(";", ",").split(",")]
        elif isinstance(raw, Iterable):
            parts = [str(item).strip() for item in raw]
        else:
            continue
        for part in parts:
            normalized = _normalize_country(part)
            if normalized:
                countries.add(normalized)
    return countries


def evaluate_overflight(
    flight: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    permit_rules: Optional[Mapping[str, int]] = None,
) -> CategoryResult:
    reference_time = now or datetime.now(timezone.utc)
    rules = permit_rules or _PERMIT_RULES
    departure_time = parse_datetime(flight.get("dep_time") or flight.get("departureTime"))

    if not departure_time:
        return CategoryResult(
            status="CAUTION",
            summary="Departure time unknown",
            issues=["Provide scheduled departure to evaluate permit lead times."],
        )

    hours_until_departure = (departure_time - reference_time).total_seconds() / 3600

    route_countries = _collect_route_countries(flight)
    if not route_countries:
        return CategoryResult(
            status="PASS",
            summary="No overflight permit triggers",
            issues=["Route countries not supplied; using departure/arrival permits only."],
        )

    alerts: List[str] = []
    issues: List[str] = []

    for country in sorted(route_countries):
        lead_hours = rules.get(country)
        if lead_hours is None:
            continue
        if hours_until_departure < lead_hours:
            alerts.append(f"{country} permit short by {lead_hours - hours_until_departure:.1f} hours")
        elif hours_until_departure < lead_hours + 12:
            issues.append(f"{country} permit lead time tight ({hours_until_departure:.1f}h vs {lead_hours}h)")
        else:
            issues.append(f"{country} permit lead time satisfied ({lead_hours}h requirement)")

    if alerts:
        summary = alerts[0]
        detail = alerts + issues
        return CategoryResult(status="FAIL", summary=summary, issues=detail)

    if issues:
        summary = issues[0]
        status = "CAUTION" if any("tight" in item for item in issues) else "PASS"
        return CategoryResult(status=status, summary=summary, issues=issues)

    return CategoryResult(status="PASS", summary="No overflight permit triggers", issues=["No permit countries detected."])
