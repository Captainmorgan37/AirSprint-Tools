"""Generic multi-leg duty evaluation logic."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, List, Optional

import pytz

from flight_leg_utils import safe_parse_dt

from .airport_module import LegContext
from .models import DayContext, DutyFeasibilityResult
from .schemas import CategoryStatus

_SPLIT_DUTY_THRESHOLD_MINUTES = 6 * 60
_RESET_DUTY_THRESHOLD_MINUTES = 11 * 60 + 15
_STANDARD_DUTY_LIMIT_MINUTES = 14 * 60
_EXTENDED_DUTY_LIMIT_MINUTES = 17 * 60


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = safe_parse_dt(value)
    except Exception:
        return None
    return dt.astimezone(timezone.utc)


def _format_minutes(minutes: int) -> str:
    hours, remainder = divmod(max(0, minutes), 60)
    return f"{hours:02d}:{remainder:02d}"


def _format_local(dt: Optional[datetime], icao: Optional[str], tz_provider: Optional[Callable[[str], Optional[str]]]) -> Optional[str]:
    if dt is None:
        return None
    tz = timezone.utc
    if tz_provider and icao:
        try:
            tz_name = tz_provider(icao)
        except Exception:
            tz_name = None
        if tz_name:
            try:
                tz = pytz.timezone(tz_name)
            except Exception:
                tz = timezone.utc
    return dt.astimezone(tz).isoformat()


def _compute_turn_time(previous: LegContext, current: LegContext) -> Optional[int]:
    prev_arrival = _parse_timestamp(previous.get("arrival_date_utc"))
    next_departure = _parse_timestamp(current.get("departure_date_utc"))
    if prev_arrival is None or next_departure is None:
        return None
    delta = int((next_departure - prev_arrival).total_seconds() // 60)
    return max(delta, 0)


def evaluate_generic_duty_day(
    day: DayContext,
    *,
    tz_provider: Optional[Callable[[str], Optional[str]]] = None,
) -> DutyFeasibilityResult:
    legs = day.get("legs", [])
    issues: List[str] = []
    turn_times: List[int] = []

    if not legs:
        summary = "No legs provided"
        return DutyFeasibilityResult(
            status="PASS",
            total_duty=0,
            duty_start_local=None,
            duty_end_local=None,
            turn_times=tuple(),
            split_duty_possible=False,
            reset_duty_possible=False,
            issues=[summary],
            summary=summary,
        )

    first_leg = legs[0]
    last_leg = legs[-1]
    start_time = _parse_timestamp(first_leg.get("departure_date_utc"))
    end_time = _parse_timestamp(last_leg.get("arrival_date_utc"))

    total_duty: Optional[int] = None
    if start_time and end_time:
        total_duty = max(int((end_time - start_time).total_seconds() // 60), 0)
        issues.append(
            f"Duty window {start_time.isoformat().replace('+00:00', 'Z')} → {end_time.isoformat().replace('+00:00', 'Z')}"
        )
    else:
        issues.append("Unable to compute duty duration; missing timestamps on one or more legs.")

    for previous, current in zip(legs, legs[1:]):
        turn = _compute_turn_time(previous, current)
        if turn is None:
            issues.append(
                f"Missing timestamps for turn between {previous['arrival_icao']} and {current['departure_icao']}."
            )
            continue
        turn_times.append(turn)
        issues.append(
            f"Turn between {previous['arrival_icao']} and {current['departure_icao']}: {_format_minutes(turn)}"
        )

    split_possible = any(turn >= _SPLIT_DUTY_THRESHOLD_MINUTES for turn in turn_times)
    reset_possible = any(turn >= _RESET_DUTY_THRESHOLD_MINUTES for turn in turn_times)

    duty_start_local = _format_local(start_time, first_leg.get("departure_icao"), tz_provider)
    duty_end_local = _format_local(end_time, last_leg.get("arrival_icao"), tz_provider)

    if total_duty is None:
        status: CategoryStatus = "CAUTION"
        summary = "Duty duration unavailable"
    elif total_duty <= _STANDARD_DUTY_LIMIT_MINUTES:
        status = "PASS"
        summary = f"Total duty {_format_minutes(total_duty)} (PASS)"
    elif total_duty < _EXTENDED_DUTY_LIMIT_MINUTES:
        status = "CAUTION"
        summary = f"Total duty {_format_minutes(total_duty)} exceeds 14h standard"
        issues.append("Duty exceeds 14 hours and requires extension feasibility.")
    else:
        status = "FAIL"
        summary = f"Total duty {_format_minutes(total_duty)} exceeds 17h limit"
        issues.append("Duty exceeds 17-hour maximum.")

    if split_possible:
        issues.append("Split duty window available (≥6h ground).")
    if reset_possible:
        issues.append("Reset duty window available (≥11h15 ground).")

    return DutyFeasibilityResult(
        status=status,
        total_duty=total_duty,
        duty_start_local=duty_start_local,
        duty_end_local=duty_end_local,
        turn_times=tuple(turn_times),
        split_duty_possible=split_possible,
        reset_duty_possible=reset_possible,
        issues=issues,
        summary=summary,
    )
