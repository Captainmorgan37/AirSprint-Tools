"""Aircraft-level feasibility heuristics."""

from __future__ import annotations

import re
from typing import Any, Mapping, NamedTuple, Optional, Sequence, Tuple

from .common import extract_first_str, extract_int, parse_minutes
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

_PAX_KEYS = (
    "pax",
    "paxNumber",
    "passengers",
    "pax_count",
    "passengerCount",
)


def _hm(hours: int, minutes: int) -> int:
    return hours * 60 + minutes


class PaxProfile(NamedTuple):
    limits: Tuple[Optional[int], ...]
    max_pax: int


def _profile(values: Sequence[Optional[int]]) -> PaxProfile:
    limits = tuple(values)
    max_supported = max(i for i, value in enumerate(limits) if value is not None)
    return PaxProfile(limits=limits, max_pax=max_supported)


_PASSENGER_PROFILES: Mapping[str, PaxProfile] = {
    "PRAETOR 500": _profile(
        (
            _hm(7, 15),
            _hm(7, 15),
            _hm(7, 15),
            _hm(7, 0),
            _hm(7, 0),
            _hm(6, 45),
            _hm(6, 35),
            _hm(6, 25),
            _hm(6, 15),
            _hm(6, 15),
        )
    ),
    "LEGACY 450": _profile(
        (
            _hm(6, 25),
            _hm(6, 10),
            _hm(5, 50),
            _hm(5, 50),
            _hm(5, 50),
            _hm(5, 45),
            _hm(5, 35),
            _hm(5, 25),
            _hm(5, 15),
            _hm(5, 5),
        )
    ),
    "CITATION CJ3+": _profile(
        (
            _hm(4, 40),
            _hm(4, 40),
            _hm(4, 30),
            _hm(4, 5),
            _hm(3, 45),
            _hm(3, 45),
            _hm(3, 30),
            _hm(3, 15),
            _hm(3, 15),
            None,
        )
    ),
    "CITATION CJ2+": _profile(
        (
            _hm(3, 45),
            _hm(3, 45),
            _hm(3, 25),
            _hm(3, 15),
            _hm(3, 0),
            _hm(2, 55),
            _hm(2, 40),
            _hm(2, 35),
            None,
            None,
        )
    ),
}


_AIRCRAFT_ALIASES = {
    # Praetor/Legacy fleet
    "PRAETOR 500": "PRAETOR 500",
    "EMBRAER PRAETOR 500": "PRAETOR 500",
    "EMB-550": "PRAETOR 500",
    "E550": "PRAETOR 500",
    "LEGACY 450": "LEGACY 450",
    "EMBRAER LEGACY 450": "LEGACY 450",
    "EMB-545": "LEGACY 450",
    "E545": "LEGACY 450",
    # CJ fleet
    "CITATION CJ3+": "CITATION CJ3+",
    "CITATION CJ3 PLUS": "CITATION CJ3+",
    "CJ3+": "CITATION CJ3+",
    "CJ3": "CITATION CJ3+",
    "CESSNA 525B": "CITATION CJ3+",
    "C25B": "CITATION CJ3+",
    "CITATION CJ2+": "CITATION CJ2+",
    "CITATION CJ2": "CITATION CJ2+",
    "CJ2+": "CITATION CJ2+",
    "CJ2": "CITATION CJ2+",
    "CESSNA 525A": "CITATION CJ2+",
    "C25A": "CITATION CJ2+",
    "CITATION CJ4": "CITATION CJ4",
    "CJ4": "CITATION CJ4",
    "CITATION CJ4 GEN2": "CITATION CJ4",
    "CESSNA 525C": "CITATION CJ4",
    "C25C": "CITATION CJ4",
    # PC-12 utility
    "PILATUS PC-12": "PILATUS PC-12",
    "PILATUS PC12": "PILATUS PC-12",
    "PC12": "PILATUS PC-12",
    "PC-12": "PILATUS PC-12",
}

_AIRCRAFT_KEYWORD_ALIASES = (
    ("PRAETOR", "PRAETOR 500"),
    ("LEGACY", "LEGACY 450", "E545"),
    ("CJ3", "CITATION CJ3+", "C25B"),
    ("CJ2", "CITATION CJ2+", "C25A"),
    ("CJ4", "CITATION CJ4"),
)

_ENDURANCE_LIMITS = {
    "CITATION CJ2+": 210,
    "CITATION CJ3+": 220,
    "CITATION CJ4": 230,
    "PILATUS PC-12": 240,
}

_DEFAULT_ENDURANCE = 240
_MARGIN_MINUTES = 20


def _canonical_aircraft(name: str) -> str:
    normalized = re.sub(r"[\s\-]+", " ", name.upper()).strip()
    direct = _AIRCRAFT_ALIASES.get(normalized)
    if direct:
        return direct
    for keyword, alias in _AIRCRAFT_KEYWORD_ALIASES:
        if keyword in normalized:
            return alias
    return normalized


def _extract_pax(flight: Mapping[str, Any]) -> Optional[int]:
    for key in _PAX_KEYS:
        value = extract_int(flight.get(key))
        if value is None:
            continue
        if value < 0:
            continue
        return value
    return None


def _evaluate_pax_profile(
    aircraft_type: str,
    pax: int,
    block_minutes: int,
    profile: PaxProfile,
) -> CategoryResult:
    limits = profile.limits
    max_pax = profile.max_pax
    issues = [
        f"Passengers: {pax} (max {max_pax})",
        f"Planned block time: {block_minutes} minutes",
    ]

    if pax > max_pax:
        issues.append("Requested pax exceeds certified maximum for this profile.")
        return CategoryResult(
            status="FAIL",
            summary=f"{aircraft_type} cannot accommodate {pax} pax",
            issues=issues,
        )

    limit = limits[pax] if pax < len(limits) else None
    if limit is None:
        issues.append("No endurance data available for this pax count.")
        return CategoryResult(
            status="FAIL",
            summary=f"{aircraft_type} lacks endurance data for {pax} pax",
            issues=issues,
        )

    diff = limit - block_minutes
    issues.append(f"Limit for {pax} pax: {limit} minutes")
    issues.append(f"Margin to limit: {diff} minutes")

    if diff >= 10:
        return CategoryResult(
            status="PASS",
            summary=f"{aircraft_type} ({pax} pax) within pax endurance (margin {diff} min)",
            issues=issues,
        )

    if diff >= -15:
        if diff >= 0:
            summary = f"{aircraft_type} ({pax} pax) near pax endurance limit (margin {diff} min)"
        else:
            summary = f"{aircraft_type} ({pax} pax) slightly exceeds pax endurance by {-diff} min"
        return CategoryResult(status="CAUTION", summary=summary, issues=issues)

    return CategoryResult(
        status="FAIL",
        summary=f"{aircraft_type} ({pax} pax) exceeds pax endurance by {-diff} min",
        issues=issues,
    )


def evaluate_aircraft(flight: Mapping[str, Any]) -> CategoryResult:
    aircraft_type = extract_first_str(flight, _AIRCRAFT_KEYS)
    block_minutes: Optional[int] = None
    for key in _BLOCK_KEYS:
        block_minutes = parse_minutes(flight.get(key))
        if block_minutes is not None:
            break

    if not aircraft_type:
        return CategoryResult(status="CAUTION", summary="Missing aircraft type", issues=["Cannot verify performance without aircraft type information."])

    pax = _extract_pax(flight)
    canonical = _canonical_aircraft(aircraft_type)
    profile = _PASSENGER_PROFILES.get(canonical)

    limit = _ENDURANCE_LIMITS.get(canonical, _DEFAULT_ENDURANCE)

    if block_minutes is None:
        return CategoryResult(
            status="CAUTION",
            summary=f"{aircraft_type} block time unknown",
            issues=["Provide planned block time to validate endurance margins."],
        )

    if profile is not None:
        if pax is None:
            return CategoryResult(
                status="CAUTION",
                summary=f"{aircraft_type} passenger count unknown",
                issues=["Provide passenger count to evaluate pax-based endurance limits."],
            )
        return _evaluate_pax_profile(aircraft_type, pax, block_minutes, profile)

    issues = [f"Planned block time: {block_minutes} minutes", f"Assumed endurance limit: {limit} minutes"]

    if block_minutes >= limit:
        return CategoryResult(status="FAIL", summary=f"{aircraft_type} exceeds endurance", issues=issues)

    if block_minutes >= max(limit - _MARGIN_MINUTES, int(limit * 0.9)):
        return CategoryResult(status="CAUTION", summary=f"{aircraft_type} near endurance limit", issues=issues)

    return CategoryResult(status="PASS", summary=f"{aircraft_type} within endurance", issues=issues)
