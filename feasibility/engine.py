"""Feasibility engine orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, MutableMapping, Optional

from flight_leg_utils import load_airport_metadata_lookup
from fl3xx_api import fetch_flight_pax_details

from . import (
    checker_aircraft,
    checker_airport,
    checker_duty,
    checker_overflight,
    checker_trip,
    checker_weight_balance,
)
from .data_access import load_customs_rules
from .lookup import lookup_booking
from .schemas import CategoryResult, FeasibilityResult


def _build_notes(categories: Mapping[str, CategoryResult]) -> str:
    lines = []
    for name, result in categories.items():
        if result.status == "PASS":
            continue
        header = result.summary or f"{name.title()} status: {result.status}"
        lines.append(f"{name.title()}: {header}")
        for issue in result.issues:
            lines.append(f"- {issue}")
    if not lines:
        return "All categories passed feasibility checks."
    return "\n".join(lines)


def evaluate_flight(
    flight: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    airport_lookup: Optional[Mapping[str, Mapping[str, Optional[str]]]] = None,
    pax_payload: Optional[Mapping[str, Any]] = None,
) -> FeasibilityResult:
    reference_time = now or datetime.now(timezone.utc)
    airport_lookup = airport_lookup or load_airport_metadata_lookup()
    customs_rules = load_customs_rules()

    season = checker_weight_balance.determine_season(flight.get("departureTime") or flight)

    categories: Dict[str, CategoryResult] = {
        "aircraft": checker_aircraft.evaluate_aircraft(flight),
        "airport": checker_airport.evaluate_airport(
            flight,
            airport_lookup=airport_lookup,
            customs_rules=customs_rules,
        ),
        "duty": checker_duty.evaluate_duty(flight, now=reference_time),
        "trip": checker_trip.evaluate_trip(
            flight, airport_lookup=airport_lookup
        ),
        "overflight": checker_overflight.evaluate_overflight(flight, now=reference_time),
        "weightBalance": checker_weight_balance.evaluate_weight_balance(
            flight,
            pax_payload=pax_payload,
            aircraft_type=flight.get("aircraftType"),
            season=season,
        ),
    }

    booking_identifier = str(flight.get("bookingIdentifier") or flight.get("bookingCode") or "").strip()
    flight_id = str(flight.get("id") or flight.get("flightId") or "").strip() or None

    notes = _build_notes(categories)

    return FeasibilityResult.build(
        booking_identifier=booking_identifier or "UNKNOWN",
        flight_id=flight_id,
        categories=categories,
        notes_for_os=notes,
        flight=flight,
        timestamp=reference_time.isoformat().replace("+00:00", "Z"),
    )


def run_feasibility_for_booking(
    config: Any,
    booking_identifier: str,
    *,
    now: Optional[datetime] = None,
    cache: Optional[MutableMapping[str, Any]] = None,
    session: Any = None,
) -> FeasibilityResult:
    lookup_result = lookup_booking(
        config,
        booking_identifier,
        now=now,
        cache=cache,
        session=session,
    )
    pax_payload: Optional[Mapping[str, Any]] = None
    flight_id = lookup_result.flight.get("flightId") or lookup_result.flight.get("id")
    if flight_id:
        try:
            pax_payload = fetch_flight_pax_details(config, flight_id, session=session)
        except Exception:
            pax_payload = None
    return evaluate_flight(lookup_result.flight, now=now, pax_payload=pax_payload)
