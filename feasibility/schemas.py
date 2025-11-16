"""Shared dataclasses and helpers for the feasibility engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Literal, Mapping, MutableMapping, Optional

CategoryStatus = Literal["PASS", "CAUTION", "FAIL"]

_STATUS_PRIORITY: Mapping[CategoryStatus, int] = {"PASS": 0, "CAUTION": 1, "FAIL": 2}


def combine_statuses(statuses: Iterable[CategoryStatus]) -> CategoryStatus:
    """Return the most severe status contained in ``statuses``."""

    worst: CategoryStatus = "PASS"
    worst_score = _STATUS_PRIORITY[worst]
    for status in statuses:
        score = _STATUS_PRIORITY.get(status, 0)
        if score > worst_score:
            worst = status  # type: ignore[assignment]
            worst_score = score
    return worst


@dataclass
class CategoryResult:
    """Normalized structure returned by each category checker."""

    status: CategoryStatus = "PASS"
    summary: str = ""
    issues: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {"status": self.status, "summary": self.summary, "issues": list(self.issues)}


@dataclass
class FeasibilityResult:
    """Aggregated feasibility output for a flight."""

    booking_identifier: str
    flight_id: Optional[str]
    overall_status: CategoryStatus
    categories: MutableMapping[str, CategoryResult]
    notes_for_os: str
    timestamp: str
    flight: Optional[Mapping[str, object]] = None

    def as_dict(self, *, include_flight: bool = True) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "bookingIdentifier": self.booking_identifier,
            "flightId": self.flight_id,
            "overallStatus": self.overall_status,
            "categories": {name: result.as_dict() for name, result in self.categories.items()},
            "notesForOS": self.notes_for_os,
            "timestamp": self.timestamp,
        }
        if include_flight and self.flight is not None:
            payload["flight"] = dict(self.flight)
        return payload

    @classmethod
    def build(
        cls,
        *,
        booking_identifier: str,
        flight_id: Optional[str],
        categories: Mapping[str, CategoryResult],
        notes_for_os: str,
        flight: Optional[Mapping[str, object]] = None,
        timestamp: Optional[str] = None,
    ) -> "FeasibilityResult":
        stamp = timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        overall = combine_statuses(result.status for result in categories.values())
        return cls(
            booking_identifier=booking_identifier,
            flight_id=flight_id,
            overall_status=overall,
            categories=dict(categories),
            notes_for_os=notes_for_os,
            timestamp=stamp,
            flight=dict(flight) if flight is not None else None,
        )
