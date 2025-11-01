"""Domain contracts for the negotiation-aware solver."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional


@dataclass(frozen=True, slots=True)
class Flight:
    """Representation of a flight leg to be scheduled."""

    id: str
    origin: str
    dest: str
    duration_min: int
    earliest_etd_min: int
    latest_etd_min: int
    preferred_etd_min: int
    fleet_class: str
    owner_id: str
    requested_start_utc: Optional[datetime] = None
    current_tail_id: Optional[str] = None
    allow_tail_swap: bool = False
    allow_outsource: bool = True
    shift_plus_cap: int = 90
    shift_minus_cap: int = 30
    shift_cost_per_min: int = 2
    intent: str = "PAX"
    must_cover: bool | None = None

    def __post_init__(self) -> None:
        if self.duration_min < 0:
            raise ValueError("duration_min must be non-negative")
        if self.earliest_etd_min < 0 or self.earliest_etd_min > 24 * 60:
            raise ValueError("earliest_etd_min must be within a day")
        if self.latest_etd_min < 0 or self.latest_etd_min > 24 * 60:
            raise ValueError("latest_etd_min must be within a day")
        if self.preferred_etd_min < 0 or self.preferred_etd_min > 24 * 60:
            raise ValueError("preferred_etd_min must be within a day")
        if self.latest_etd_min < self.earliest_etd_min:
            raise ValueError("latest_etd_min must be greater than or equal to earliest_etd_min")
        if self.shift_plus_cap < 0:
            raise ValueError("shift_plus_cap must be non-negative")
        if self.shift_minus_cap < 0:
            raise ValueError("shift_minus_cap must be non-negative")
        if self.shift_cost_per_min < 0:
            raise ValueError("shift_cost_per_min must be non-negative")

        intent = (self.intent or "PAX").upper()
        if intent not in {"PAX", "POS"}:
            intent = "POS"
        object.__setattr__(self, "intent", intent)

        if self.must_cover is None:
            object.__setattr__(self, "must_cover", intent == "PAX")


@dataclass(frozen=True, slots=True)
class Tail:
    """Representation of a tail, including availability bounds."""

    id: str
    fleet_class: str
    available_from_min: int = 0
    available_to_min: int = 24 * 60
    maintenance_due: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.available_from_min < 0 or self.available_from_min > 24 * 60:
            raise ValueError("available_from_min must be within a day")
        if self.available_to_min < 0 or self.available_to_min > 24 * 60:
            raise ValueError("available_to_min must be within a day")
        if self.available_to_min < self.available_from_min:
            raise ValueError("available_to_min must be greater than or equal to available_from_min")


def minutes_since_midnight(value: time | datetime) -> int:
    """Helper used by adapters to convert timestamps to minutes from midnight."""

    if isinstance(value, datetime):
        value = value.time()
    return value.hour * 60 + value.minute
