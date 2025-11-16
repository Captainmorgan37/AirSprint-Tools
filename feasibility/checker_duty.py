"""Duty legality heuristics leveraging simplified FRMS assumptions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional

from .common import parse_datetime, parse_minutes
from .schemas import CategoryResult

_DUTY_KEYS = (
    "plannedDutyMinutes",
    "planned_duty_minutes",
    "dutyTime",
    "duty_time",
    "dutyDuration",
    "duty_duration",
)

_MAX_DUTY_MINUTES = 780  # 13 hours
_CAUTION_THRESHOLD = 720


def evaluate_duty(flight: Mapping[str, Any], *, now: Optional[datetime] = None) -> CategoryResult:
    duty_minutes: Optional[int] = None
    for key in _DUTY_KEYS:
        duty_minutes = parse_minutes(flight.get(key))
        if duty_minutes is not None:
            break

    if duty_minutes is None:
        return CategoryResult(
            status="CAUTION",
            summary="Duty inputs unavailable",
            issues=["Provide planned duty duration to validate FRMS margins."],
        )

    issues = [f"Planned duty: {duty_minutes} minutes"]

    if duty_minutes > _MAX_DUTY_MINUTES:
        return CategoryResult(status="FAIL", summary="Duty exceeds 13 hours", issues=issues)

    if duty_minutes > _CAUTION_THRESHOLD:
        return CategoryResult(status="CAUTION", summary="Duty within 1 hour of limit", issues=issues)

    # Highlight long duty following short rest if timestamps available.
    rest_start = parse_datetime(flight.get("previousDutyEnd"))
    dep_time = parse_datetime(flight.get("dep_time") or flight.get("departureTime"))
    if rest_start and dep_time:
        rest_hours = (dep_time - rest_start).total_seconds() / 3600
        issues.append(f"Rest before duty: {rest_hours:.1f} hours")
        if rest_hours < 10:
            return CategoryResult(
                status="CAUTION",
                summary="Rest < 10h before duty",
                issues=issues,
            )

    return CategoryResult(status="PASS", summary="Duty within limits", issues=issues)
