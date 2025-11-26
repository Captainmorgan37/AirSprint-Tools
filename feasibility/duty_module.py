"""Generic multi-leg duty evaluation logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
_DUTY_START_BUFFER_MINUTES = 60
_DUTY_END_BUFFER_MINUTES = 15


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


def _worse_status(current: CategoryStatus, candidate: CategoryStatus) -> CategoryStatus:
    ranking = {"PASS": 0, "INFO": 1, "CAUTION": 2, "FAIL": 3}
    return candidate if ranking[candidate] > ranking[current] else current


def _describe_duty_segment(
    *,
    duration_minutes: int,
    label: str,
    issues: List[str],
) -> CategoryStatus:
    limit_label = _format_minutes(_STANDARD_DUTY_LIMIT_MINUTES)

    if duration_minutes <= _STANDARD_DUTY_LIMIT_MINUTES:
        issues.append(f"{label} {_format_minutes(duration_minutes)} within {limit_label} limit.")
        return "PASS"

    if duration_minutes < _EXTENDED_DUTY_LIMIT_MINUTES:
        issues.append(
            f"{label} {_format_minutes(duration_minutes)} exceeds {limit_label} limit (<=17:00 extension required)."
        )
        return "CAUTION"

    issues.append(f"{label} {_format_minutes(duration_minutes)} exceeds 17-hour maximum.")
    return "FAIL"


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
    scheduled_start = _parse_timestamp(first_leg.get("departure_date_utc"))
    scheduled_end = _parse_timestamp(last_leg.get("arrival_date_utc"))
    start_time = (
        scheduled_start - timedelta(minutes=_DUTY_START_BUFFER_MINUTES)
        if scheduled_start
        else None
    )
    end_time = (
        scheduled_end + timedelta(minutes=_DUTY_END_BUFFER_MINUTES)
        if scheduled_end
        else None
    )

    total_duty: Optional[int] = None
    if start_time and end_time:
        total_duty = max(int((end_time - start_time).total_seconds() // 60), 0)
        issues.append(
            f"Duty window {start_time.isoformat().replace('+00:00', 'Z')} → {end_time.isoformat().replace('+00:00', 'Z')}"
        )
        issues.append(
            "Duty start includes 60m pre-departure buffer; duty end includes 15m post-arrival buffer."
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

    qualifying_split_turns = [turn for turn in turn_times if turn >= _SPLIT_DUTY_THRESHOLD_MINUTES]
    split_possible = bool(qualifying_split_turns)
    reset_breaks = [
        (index, turn)
        for index, turn in enumerate(turn_times)
        if turn >= _RESET_DUTY_THRESHOLD_MINUTES
    ]
    reset_possible = bool(reset_breaks)

    split_extension_minutes = 0
    if reset_possible:
        issues.append("Reset duty window available (≥11h15 ground).")
    elif qualifying_split_turns:
        best_ground = max(qualifying_split_turns)
        split_extension_minutes = max(best_ground - 120, 0) // 2
        split_extension_minutes = min(
            split_extension_minutes,
            _EXTENDED_DUTY_LIMIT_MINUTES - _STANDARD_DUTY_LIMIT_MINUTES,
        )
        issues.append(
            "Split duty window available (≥6h ground)."
        )
        issues.append(
            f"Ground time {_format_minutes(best_ground)} allows {_format_minutes(split_extension_minutes)} duty extension."
        )

    duty_start_local = _format_local(start_time, first_leg.get("departure_icao"), tz_provider)
    duty_end_local = _format_local(end_time, last_leg.get("arrival_icao"), tz_provider)

    if total_duty is None:
        status: CategoryStatus = "CAUTION"
        summary = "Duty duration unavailable"
    elif reset_possible and reset_breaks:
        break_index, longest_ground = max(reset_breaks, key=lambda item: item[1])
        break_arrival = _parse_timestamp(legs[break_index].get("arrival_date_utc"))
        next_departure = _parse_timestamp(legs[break_index + 1].get("departure_date_utc"))

        first_segment_minutes: Optional[int] = None
        second_segment_minutes: Optional[int] = None

        if start_time and break_arrival:
            first_segment_end = break_arrival + timedelta(minutes=_DUTY_END_BUFFER_MINUTES)
            first_segment_minutes = max(int((first_segment_end - start_time).total_seconds() // 60), 0)
        else:
            issues.append("Unable to compute pre-break duty duration; missing timestamps.")

        if end_time and next_departure:
            second_segment_start = next_departure - timedelta(minutes=_DUTY_START_BUFFER_MINUTES)
            second_segment_minutes = max(int((end_time - second_segment_start).total_seconds() // 60), 0)
        else:
            issues.append("Unable to compute post-break duty duration; missing timestamps.")

        issues.append(
            f"Reset break {_format_minutes(longest_ground)} between {legs[break_index]['arrival_icao']} and {legs[break_index + 1]['departure_icao']}"
        )

        status = "PASS"
        summaries: List[str] = []

        if first_segment_minutes is not None:
            status = _worse_status(
                status,
                _describe_duty_segment(
                    duration_minutes=first_segment_minutes,
                    label="Duty before reset",
                    issues=issues,
                ),
            )
            summaries.append(f"pre-break {_format_minutes(first_segment_minutes)}")

        if second_segment_minutes is not None:
            status = _worse_status(
                status,
                _describe_duty_segment(
                    duration_minutes=second_segment_minutes,
                    label="Duty after reset",
                    issues=issues,
                ),
            )
            summaries.append(f"post-break {_format_minutes(second_segment_minutes)}")

        if first_segment_minutes is None and second_segment_minutes is None:
            status = "CAUTION"
            summary = "Duty duration unavailable"
        else:
            summary = "Reset duty day: " + "; ".join(summaries)
    else:
        allowed_limit = _STANDARD_DUTY_LIMIT_MINUTES + split_extension_minutes
        allowed_limit = min(allowed_limit, _EXTENDED_DUTY_LIMIT_MINUTES)
        limit_label = _format_minutes(allowed_limit)

        if total_duty <= allowed_limit:
            status = "PASS"
            summary = f"Total duty {_format_minutes(total_duty)} within {limit_label} limit"
        elif total_duty < _EXTENDED_DUTY_LIMIT_MINUTES:
            status = "CAUTION"
            summary = f"Total duty {_format_minutes(total_duty)} exceeds {limit_label} limit"
            issues.append(
                f"Duty {_format_minutes(total_duty)} exceeds the allowable {limit_label} duty window."
            )
        else:
            status = "FAIL"
            summary = f"Total duty {_format_minutes(total_duty)} exceeds 17h limit"
            issues.append("Duty exceeds 17-hour maximum.")

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
