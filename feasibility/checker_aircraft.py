"""Aircraft-level feasibility heuristics."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .common import extract_first_str, parse_minutes
from .schemas import CategoryResult

_AIRCRAFT_KEYS = (
    "aircraftType",
    "aircraft_type",
    "aircraft",
    "aircraftName",
)

_BLOCK_KEYS = (
    "plannedBlockTime",
    "planned_block_time",
    "blockTime",
    "block_time",
    "flightTime",
    "flight_time",
    "duration",
)

_ENDURANCE_LIMITS = {
    "CJ2": 210,
    "CJ3": 220,
    "CJ4": 230,
    "PC12": 240,
}

_DEFAULT_ENDURANCE = 240
_MARGIN_MINUTES = 20


def evaluate_aircraft(flight: Mapping[str, Any]) -> CategoryResult:
    aircraft_type = extract_first_str(flight, _AIRCRAFT_KEYS)
    block_minutes: Optional[int] = None
    for key in _BLOCK_KEYS:
        block_minutes = parse_minutes(flight.get(key))
        if block_minutes is not None:
            break

    if not aircraft_type:
        return CategoryResult(status="CAUTION", summary="Missing aircraft type", issues=["Cannot verify performance without aircraft type information."])

    limit = _ENDURANCE_LIMITS.get(aircraft_type.upper(), _DEFAULT_ENDURANCE)

    if block_minutes is None:
        return CategoryResult(
            status="CAUTION",
            summary=f"{aircraft_type} block time unknown",
            issues=["Provide planned block time to validate endurance margins."],
        )

    issues = [f"Planned block time: {block_minutes} minutes", f"Assumed endurance limit: {limit} minutes"]

    if block_minutes >= limit:
        return CategoryResult(status="FAIL", summary=f"{aircraft_type} exceeds endurance", issues=issues)

    if block_minutes >= max(limit - _MARGIN_MINUTES, int(limit * 0.9)):
        return CategoryResult(status="CAUTION", summary=f"{aircraft_type} near endurance limit", issues=issues)

    return CategoryResult(status="PASS", summary=f"{aircraft_type} within endurance", issues=issues)
