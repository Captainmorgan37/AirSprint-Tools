"""Reusable helpers for computing ferry (reposition) durations."""

from __future__ import annotations

import math
from typing import Dict, Iterable, List

from .neg_scheduler.contracts import Flight, Tail


def gcd_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance between two coordinates in NM."""

    R_nm = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return R_nm * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def norm_class(s: str) -> str:
    """Collapse fleet-class variants into shared compatibility buckets."""

    value = (s or "").upper()
    if value.startswith("CJ"):
        return "CJ"
    if value.startswith("LEG") or value.startswith("E"):
        return "LEG"
    if value == "GEN":
        return "GEN"
    return value


TAS: Dict[str, int] = {"CJ": 390, "LEG": 450, "GEN": 410}
ROUTE_FACTOR = 1.07
TAXI_MIN = 10
FUDGE: Dict[str, int] = {"CJ": 12, "LEG": 15, "GEN": 13}


def block_minutes_nm(nm: float, fleet_class: str) -> int:
    """Convert a distance in NM into block minutes for the given fleet class."""

    key = norm_class(fleet_class)
    tas = TAS.get(key, TAS["GEN"])
    fudge = FUDGE.get(key, FUDGE["GEN"])
    block = nm / tas * 60.0 + fudge + TAXI_MIN
    return int(math.ceil(block))


def repo_minutes_between(
    icao_from: str | None,
    icao_to: str | None,
    fleet_class: str,
    airports: Dict[str, Dict[str, object]],
) -> int:
    """Compute reposition minutes between two airports for a fleet class."""

    code_from = (icao_from or "").upper()
    code_to = (icao_to or "").upper()
    if not code_from or not code_to:
        return 999
    if code_from == code_to:
        return 0

    a = airports.get(code_from)
    b = airports.get(code_to)
    if not a or not b:
        return 999

    nm = gcd_nm(float(a["lat"]), float(a["lon"]), float(b["lat"]), float(b["lon"]))
    nm *= ROUTE_FACTOR
    return block_minutes_nm(nm, fleet_class)


def build_reposition_matrix(
    flights: Iterable[Flight],
    airports: Dict[str, Dict[str, object]],
) -> List[List[int]]:
    """Return reposition minutes for chaining every pair of flights."""

    flight_list = list(flights)
    count = len(flight_list)
    matrix: List[List[int]] = [[0] * count for _ in range(count)]

    for i, flight_i in enumerate(flight_list):
        for j, flight_j in enumerate(flight_list):
            if i == j:
                continue
            matrix[i][j] = repo_minutes_between(
                flight_i.dest, flight_j.origin, flight_i.fleet_class, airports
            )

    return matrix


def build_initial_reposition_matrix(
    tails: Iterable[Tail],
    flights: Iterable[Flight],
    airports: Dict[str, Dict[str, object]],
) -> List[List[int]]:
    """Return reposition minutes required before a tail's first assigned flight."""

    tail_list = list(tails)
    flight_list = list(flights)
    matrix: List[List[int]] = [[0] * len(flight_list) for _ in range(len(tail_list))]

    for tail_idx, tail in enumerate(tail_list):
        origin = (tail.last_position_airport or "").upper()
        if not origin:
            continue
        for flight_idx, flight in enumerate(flight_list):
            matrix[tail_idx][flight_idx] = repo_minutes_between(
                origin, flight.origin, tail.fleet_class, airports
            )

    return matrix
