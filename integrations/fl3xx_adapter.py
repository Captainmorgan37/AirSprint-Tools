"""Adapters for sourcing flights/tails from FL3XX or local demos."""

from __future__ import annotations

from datetime import date, datetime
from typing import Sequence, Tuple

from core.neg_scheduler.contracts import Flight, Tail


def to_minutes(value: datetime) -> int:
    """Convert a timezone-aware datetime to minutes from local midnight."""

    # Placeholder: assumes ``value`` is already localized to the operating day timezone.
    return value.hour * 60 + value.minute


def get_demo_data() -> Tuple[list[Flight], list[Tail]]:
    """Return a deterministic demo dataset for local prototyping."""

    flights = [
        Flight(
            id="F1",
            owner_id="O100",
            origin="CYBW",
            dest="CYVR",
            duration_min=150,
            fleet_class="CJ",
            earliest_etd_min=7 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=7 * 60 + 15,
        ),
        Flight(
            id="F2",
            owner_id="O220",
            origin="CYVR",
            dest="KSEA",
            duration_min=45,
            fleet_class="CJ",
            earliest_etd_min=9 * 60,
            latest_etd_min=10 * 60,
            preferred_etd_min=9 * 60 + 15,
        ),
        Flight(
            id="F3",
            owner_id="O330",
            origin="CYVR",
            dest="CYUL",
            duration_min=300,
            fleet_class="LEG",
            earliest_etd_min=9 * 60 + 30,
            latest_etd_min=12 * 60,
            preferred_etd_min=10 * 60,
        ),
        Flight(
            id="F4",
            owner_id="O220",
            origin="KSEA",
            dest="CYBW",
            duration_min=90,
            fleet_class="CJ",
            earliest_etd_min=10 * 60,
            latest_etd_min=12 * 60,
            preferred_etd_min=10 * 60 + 30,
        ),
        Flight(
            id="F5",
            owner_id="O555",
            origin="CYBW",
            dest="KDEN",
            duration_min=160,
            fleet_class="CJ",
            earliest_etd_min=8 * 60,
            latest_etd_min=9 * 60,
            preferred_etd_min=8 * 60 + 15,
        ),
    ]

    tails = [
        Tail(id="C-GCJ1", fleet_class="CJ", available_from_min=6 * 60, available_to_min=22 * 60),
        Tail(id="C-GCJ2", fleet_class="CJ", available_from_min=10 * 60 + 30, available_to_min=22 * 60),
        Tail(id="C-GLEG1", fleet_class="LEG", available_from_min=6 * 60, available_to_min=22 * 60),
    ]

    return flights, tails


def fetch_demo_from_fl3xx(day: date | None = None) -> Tuple[list[Flight], list[Tail]]:
    """Placeholder FL3XX fetch that mirrors the production contract."""

    # ``day`` is unused today but retained for interface compatibility.
    return get_demo_data()


def fetch_from_fl3xx(*, include_owners: Sequence[str] | None = None) -> Tuple[list[Flight], list[Tail]]:
    """Backward-compatible helper for existing callers."""

    return get_demo_data()
