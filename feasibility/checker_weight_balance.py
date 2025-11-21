"""Weight and balance feasibility checker using FL3XX pax and cargo data."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from .schemas import CategoryResult


MAX_PAX_CARGO = {
    "C25A": {"Summer": 1086, "Winter": 1034},
    "C25B": {"Summer": 1602, "Winter": 1550},
    "E545": {"Summer": 2116, "Winter": 2104},
}


STD_WEIGHTS = {
    "Summer": {"Male": 193, "Female": 159, "Child": 75, "Infant": 30},
    "Winter": {"Male": 199, "Female": 165, "Child": 75, "Infant": 30},
}


HIGH_RISK_KEYWORDS = ("SKI", "GOLF", "BIKE", "PET")


def determine_season(departure_time: Any) -> str:
    """Return ``Summer`` or ``Winter`` based on the month (UTC).

    Defaults to ``Winter`` when the date cannot be determined.
    """

    month: Optional[int] = None
    if isinstance(departure_time, (int, float)):
        try:
            if departure_time > 10**11:
                departure_time = departure_time / 1000.0
            month = datetime.utcfromtimestamp(float(departure_time)).month
        except Exception:
            month = None
    elif isinstance(departure_time, str):
        try:
            parsed = datetime.fromisoformat(departure_time.replace("Z", "+00:00"))
            month = parsed.month
        except ValueError:
            month = None
    if month is None and isinstance(departure_time, Mapping):
        for key in ("departureTime", "dep_time"):
            candidate = departure_time.get(key)
            if candidate:
                return determine_season(candidate)
    if month and 4 <= month <= 10:
        return "Summer"
    return "Winter"


def _coerce_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN guard
        return None
    return number


def _normalize_aircraft_type(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    text = str(name).strip()
    if not text:
        return None
    upper_text = text.upper()
    for key in MAX_PAX_CARGO:
        if key.upper() in upper_text:
            return key
    return text


def _normalize_gender_label(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = str(value).strip().lower()
    if cleaned.startswith("f"):
        return "Female"
    if cleaned.startswith("m"):
        return "Male"
    return None


def _extract_label(value: Any, *keys: str) -> Optional[str]:
    """Return a non-empty string from ``value`` or selected mapping keys."""

    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


def _pax_category(ticket: Mapping[str, Any]) -> str:
    pax_type_raw = (
        _extract_label(ticket.get("paxType"), "code", "type", "name", "label")
        or _extract_label(ticket.get("type"))
        or _extract_label(ticket.get("pax_type"))
        or "ADULT"
    )
    pax_type = pax_type_raw.upper()
    if "INFANT" in pax_type:
        return "Infant"
    if "CHILD" in pax_type:
        return "Child"

    pax_user = ticket.get("paxUser") if isinstance(ticket.get("paxUser"), Mapping) else {}
    gender_raw = (
        _extract_label(ticket.get("gender"))
        or _extract_label(pax_user.get("gender"))
        or _extract_label(pax_user.get("sex"))
    )
    gender_label = _normalize_gender_label(gender_raw) or _normalize_gender_label(pax_type)
    return gender_label or "Male"


def _standard_pax_weight(season: str, category: str) -> float:
    weights = STD_WEIGHTS.get(season, STD_WEIGHTS["Winter"])
    return float(weights.get(category, weights["Male"]))


def _iter_tickets(payload: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    pax = payload.get("pax") if isinstance(payload, Mapping) else None
    if isinstance(pax, Mapping) and isinstance(pax.get("tickets"), Iterable):
        for entry in pax.get("tickets", []) or []:
            if isinstance(entry, Mapping):
                yield entry
    tickets = payload.get("tickets") if isinstance(payload, Mapping) else None
    if isinstance(tickets, Iterable):
        for entry in tickets:
            if isinstance(entry, Mapping):
                yield entry


def _iter_cargo(payload: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    cargo = payload.get("cargo") if isinstance(payload, Mapping) else None
    if isinstance(cargo, Iterable):
        for entry in cargo:
            if isinstance(entry, Mapping):
                yield entry


def _extract_ticket_stats(ticket: Mapping[str, Any], season: str) -> tuple[float, str]:
    category = _pax_category(ticket)

    pax_user = ticket.get("paxUser") if isinstance(ticket.get("paxUser"), Mapping) else {}

    explicit_weight = _coerce_number(
        ticket.get("bodyWeight")
        or ticket.get("weight")
        or (pax_user.get("bodyWeight") if isinstance(pax_user, Mapping) else None)
    )
    base_weight = (
        explicit_weight
        if explicit_weight is not None
        else _standard_pax_weight(season, category)
    )

    luggage_weight = _coerce_number(ticket.get("luggageWeight") or ticket.get("luggage_weight")) or 0
    return base_weight + luggage_weight, category


def _detect_high_risk_items(cargo_entries: Iterable[Mapping[str, Any]]) -> bool:
    for entry in cargo_entries:
        note = str(entry.get("note") or "")
        if any(keyword in note.upper() for keyword in HIGH_RISK_KEYWORDS):
            return True
    return False


def evaluate_weight_balance(
    flight: Mapping[str, Any],
    *,
    pax_payload: Optional[Mapping[str, Any]],
    aircraft_type: Optional[str],
    season: str,
) -> CategoryResult:
    """Assess passenger + cargo payload feasibility for a flight."""

    issues = []
    normalised_type = _normalize_aircraft_type(aircraft_type)
    season_label = season if season in STD_WEIGHTS else "Winter"

    details: MutableMapping[str, Any] = {"season": season_label}

    if pax_payload is None:
        return CategoryResult(
            status="CAUTION",
            summary="No weight data available",
            issues=["Could not retrieve pax/cargo details."],
            details=dict(details),
        )

    tickets = list(_iter_tickets(pax_payload))
    pax_count = len(tickets)
    pax_breakdown: MutableMapping[str, int] = {"Male": 0, "Female": 0, "Child": 0, "Infant": 0}

    pax_weight = 0.0
    for ticket in tickets:
        ticket_weight, category = _extract_ticket_stats(ticket, season_label)
        pax_weight += ticket_weight
        pax_breakdown[category] = pax_breakdown.get(category, 0) + 1

    cargo_entries = list(_iter_cargo(pax_payload))
    cargo_weights = [
        _coerce_number(item.get("weightQty"))
        for item in cargo_entries
        if _coerce_number(item.get("weightQty")) is not None
    ]
    cargo_weight = sum(cargo_weights) if cargo_weights else None
    if cargo_weight is None:
        cargo_weight = 30 * pax_count

    total_payload = pax_weight + cargo_weight

    details.update(
        {
            "paxWeight": round(pax_weight, 2),
            "cargoWeight": round(cargo_weight, 2),
            "totalPayload": round(total_payload, 2),
            "paxCount": pax_count,
            "maxAllowed": None,
            "paxBreakdown": dict(pax_breakdown),
        }
    )

    if normalised_type not in MAX_PAX_CARGO:
        issues.append("Unsupported aircraft type for payload table.")
        return CategoryResult(
            status="CAUTION",
            summary=f"Missing payload limits for {normalised_type or 'aircraft'}",
            issues=issues,
            details=dict(details),
        )

    max_allowed = MAX_PAX_CARGO[normalised_type][season_label]
    details["maxAllowed"] = max_allowed

    payload_overage = total_payload - max_allowed
    high_risk = _detect_high_risk_items(cargo_entries)
    details["highRiskCargo"] = high_risk

    if payload_overage > 0:
        if payload_overage <= 50:
            status = "CAUTION"
        else:
            status = "FAIL"
        issues.append(f"Overweight by {round(payload_overage, 1)} lb")
    else:
        status = "PASS"

    if high_risk:
        status = "CAUTION" if status == "PASS" else status
        issues.append("High-risk cargo detected; verify fit")

    summary = "Within payload limits"
    if status == "FAIL":
        summary = f"Payload exceeds {normalised_type} {season_label} limit"
    elif status == "CAUTION" and payload_overage > 0:
        summary = f"Near payload limit for {normalised_type} {season_label}"
    elif status == "CAUTION" and high_risk:
        summary = "Payload ok but cargo needs review"
    else:
        summary = f"Within payload limits ({normalised_type} {season_label})"

    return CategoryResult(status=status, summary=summary, issues=issues, details=dict(details))

