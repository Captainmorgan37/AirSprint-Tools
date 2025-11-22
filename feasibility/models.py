"""Typed models shared by the multi-leg feasibility engine."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, List, Mapping, Optional, Sequence, TypedDict

from .airport_module import LegContext
from .schemas import CategoryStatus


class FeasibilityRequest(TypedDict, total=False):
    """Payload required to run the quote-based feasibility engine."""

    quote_id: str | int
    quote: Mapping[str, Any]
    now_utc: datetime
    tz_provider: Callable[[str], Optional[str]]
    operational_notes_fetcher: Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]


class DayContext(TypedDict):
    """Normalized view of the entire quote as a duty day."""

    quote_id: Optional[str]
    bookingIdentifier: str
    aircraft_type: str
    aircraft_category: str
    legs: List[LegContext]
    sales_contact: Optional[str]
    createdDate: Optional[int]


class DutyFeasibilityResult(TypedDict):
    """Structured duty-day evaluation output."""

    status: CategoryStatus
    total_duty: Optional[int]
    duty_start_local: Optional[str]
    duty_end_local: Optional[str]
    turn_times: Sequence[int]
    split_duty_possible: bool
    reset_duty_possible: bool
    issues: List[str]
    summary: str


class FullFeasibilityResult(TypedDict):
    """Aggregate result emitted by ``run_feasibility_phase1``."""

    quote_id: Optional[str]
    bookingIdentifier: str
    aircraft_type: str
    aircraft_category: str
    flight_category: Optional[str]
    legs: Sequence[Mapping[str, Any]]
    duty: DutyFeasibilityResult
    overall_status: CategoryStatus
    issues: List[str]
    summary: str
