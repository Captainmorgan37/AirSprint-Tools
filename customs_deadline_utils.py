"""Utilities for computing customs clearance timing windows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, List, Mapping, Optional, Tuple

from zoneinfo_compat import ZoneInfo

DAY_KEYS: Tuple[str, ...] = (
    "open_mon",
    "open_tue",
    "open_wed",
    "open_thu",
    "open_fri",
    "open_sat",
    "open_sun",
)

DEFAULT_BUSINESS_DAY_START = time(hour=9)
DEFAULT_BUSINESS_DAY_END = time(hour=17)


def parse_hours_value(value: str) -> Optional[Tuple[time, time]]:
    """Parse a textual hours range such as ``"09:00-17:00"`` into start/end times."""

    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    upper = normalized.upper()
    if upper in {"CLOSED", "CLOSE"}:
        return None
    if "24" in upper and "H" in upper:
        return time(hour=0, minute=0), time(hour=23, minute=59)
    match = re.search(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})", normalized)
    if not match:
        return None
    try:
        start_time = datetime.strptime(match.group(1), "%H:%M").time()
        end_time = datetime.strptime(match.group(2), "%H:%M").time()
    except ValueError:
        return None
    if end_time <= start_time:
        end_time = time(hour=23, minute=59)
    return start_time, end_time


def rule_hours_for_weekday(
    rule: Optional[Mapping[str, Any]], weekday: int
) -> Optional[Tuple[time, time]]:
    """Return operating hours for the given weekday from a customs rule mapping."""

    if not isinstance(rule, Mapping):
        return None
    if weekday < 0 or weekday >= len(DAY_KEYS):
        return None
    key = DAY_KEYS[weekday]
    if key not in rule:
        return None
    value = rule.get(key)
    if value is None:
        return None
    value_str = str(value).strip()
    if not value_str or value_str.upper() == "NAN":
        return None
    return parse_hours_value(value_str)


@dataclass(frozen=True)
class ClearanceWindow:
    """Represents a candidate clearance window for a customs arrival."""

    start: datetime
    end: datetime
    summary: str
    label: str


def _get_operating_hours_for_date(
    rule: Optional[Mapping[str, Any]], target_date: date
) -> Tuple[time, time, bool]:
    hours = rule_hours_for_weekday(rule, target_date.weekday())
    if hours is None:
        return DEFAULT_BUSINESS_DAY_START, DEFAULT_BUSINESS_DAY_END, False
    start_time, end_time = hours
    return start_time, end_time, True


def _format_port_window_desc(
    start_time: time,
    end_time: time,
    *,
    uses_rule: bool,
    default_label: str,
) -> str:
    label = "Port hours" if uses_rule else default_label
    return (
        f"{label} {start_time.strftime('%H:%M')}"
        f"-{end_time.strftime('%H:%M')} local."
    )


def build_followup_candidates(
    *,
    event_local: datetime,
    tzinfo: ZoneInfo,
    rule: Optional[Mapping[str, Any]],
    lead_deadline_local: Optional[datetime],
    lead_hours: Optional[float],
    departure_local: Optional[datetime],
) -> List[ClearanceWindow]:
    """Generate fallback clearance windows once the prior-day window has passed."""

    candidates: List[ClearanceWindow] = []

    lead_hours_text = None
    if lead_hours is not None:
        lead_hours_text = f"{lead_hours:g}"

    if (
        isinstance(lead_deadline_local, datetime)
        and lead_hours is not None
        and lead_hours >= 24
    ):
        lead_day = lead_deadline_local.date()
        lead_start_time, lead_end_time, lead_uses_rule = _get_operating_hours_for_date(
            rule, lead_day
        )
        lead_base_start = datetime.combine(lead_day, lead_start_time, tzinfo=tzinfo)
        lead_base_end = datetime.combine(lead_day, lead_end_time, tzinfo=tzinfo)
        lead_start = max(lead_base_start, lead_deadline_local)
        if lead_start > lead_base_end:
            lead_start = lead_base_end
        lead_end = lead_base_end
        if isinstance(departure_local, datetime):
            lead_end = min(lead_end, departure_local)
        if lead_end < lead_start:
            lead_end = lead_start
        lead_desc = [
            (
                f"Lead time requirement {lead_hours_text}h missed "
                f"({lead_deadline_local.strftime('%Y-%m-%d %H:%M %Z')})."
                if lead_hours_text is not None
                else (
                    "Lead time deadline "
                    f"{lead_deadline_local.strftime('%Y-%m-%d %H:%M %Z')} missed."
                )
            ),
            _format_port_window_desc(
                lead_start_time,
                lead_end_time,
                uses_rule=lead_uses_rule,
                default_label="Default clearance window",
            ),
        ]
        if lead_deadline_local < lead_base_start:
            lead_desc.append(
                "Deadline occurs before port opens; action required at opening."
            )
        if lead_deadline_local > lead_base_end:
            lead_desc.append(
                "Deadline occurs after port closes; action required before closing."
            )
        if (
            isinstance(departure_local, datetime)
            and departure_local.date() == lead_day
        ):
            lead_desc.append(
                "Flight departs "
                f"{departure_local.strftime('%Y-%m-%d %H:%M %Z')}"
            )
        lead_desc.append(
            "Extended clearance window "
            f"{lead_start.strftime('%Y-%m-%d %H:%M %Z')}"
            " → "
            f"{lead_end.strftime('%Y-%m-%d %H:%M %Z')}"
            "."
        )
        candidates.append(
            ClearanceWindow(
                start=lead_start,
                end=lead_end,
                summary=" ".join(lead_desc),
                label="Lead Deadline Day",
            )
        )

        next_day = lead_day + timedelta(days=1)
        if next_day < event_local.date():
            next_start_time, next_end_time, next_uses_rule = _get_operating_hours_for_date(
                rule, next_day
            )
            next_start = datetime.combine(next_day, next_start_time, tzinfo=tzinfo)
            next_end = datetime.combine(next_day, next_end_time, tzinfo=tzinfo)
            if isinstance(departure_local, datetime):
                next_end = min(next_end, departure_local)
            if next_end < next_start:
                next_end = next_start
            next_desc = [
                "Lead deadline day clearance window missed.",
                _format_port_window_desc(
                    next_start_time,
                    next_end_time,
                    uses_rule=next_uses_rule,
                    default_label="Default next-day window",
                ),
            ]
            if lead_hours_text is not None:
                next_desc.append(f"Lead requirement {lead_hours_text}h still outstanding.")
            if (
                isinstance(departure_local, datetime)
                and departure_local.date() == next_day
            ):
                next_desc.append(
                    "Flight departs "
                    f"{departure_local.strftime('%Y-%m-%d %H:%M %Z')}"
                )
            next_desc.append(
                "Next-day clearance window "
                f"{next_start.strftime('%Y-%m-%d %H:%M %Z')}"
                " → "
                f"{next_end.strftime('%Y-%m-%d %H:%M %Z')}"
                "."
            )
            candidates.append(
                ClearanceWindow(
                    start=next_start,
                    end=next_end,
                    summary=" ".join(next_desc),
                    label="Next Day",
                )
            )

    same_day = event_local.date()
    same_start_time, same_end_time, same_uses_rule = _get_operating_hours_for_date(
        rule, same_day
    )
    same_start = datetime.combine(same_day, same_start_time, tzinfo=tzinfo)
    same_end = datetime.combine(same_day, same_end_time, tzinfo=tzinfo)

    same_window_end = same_end
    if (
        isinstance(lead_deadline_local, datetime)
        and lead_deadline_local.date() == same_day
    ):
        same_window_end = min(same_window_end, lead_deadline_local)
    if isinstance(departure_local, datetime):
        same_window_end = min(same_window_end, departure_local)

    deadline_before_open = same_window_end < same_start
    if deadline_before_open:
        same_window_end = same_start

    same_desc = []
    if candidates:
        same_desc.append("Earlier clearance windows missed.")
    else:
        same_desc.append("Prior clearance window missed.")
    same_desc.append(
        _format_port_window_desc(
            same_start_time,
            same_end_time,
            uses_rule=same_uses_rule,
            default_label="Default same-day window",
        )
    )
    if isinstance(lead_deadline_local, datetime):
        if lead_hours_text is not None:
            same_desc.append(
                "Lead time ("
                f"{lead_hours_text}h) deadline {lead_deadline_local.strftime('%Y-%m-%d %H:%M %Z')}"
                "."
            )
        else:
            same_desc.append(
                "Lead time deadline "
                f"{lead_deadline_local.strftime('%Y-%m-%d %H:%M %Z')}"
                "."
            )
    if isinstance(departure_local, datetime):
        same_desc.append(
            "Flight departs "
            f"{departure_local.strftime('%Y-%m-%d %H:%M %Z')}"
            "."
        )
    if deadline_before_open:
        same_desc.append(
            "Lead/departure deadline occurs before port opens; action required at opening."
        )
    same_desc.append(
        "Same-day clearance window "
        f"{same_start.strftime('%Y-%m-%d %H:%M %Z')}"
        " → "
        f"{same_window_end.strftime('%Y-%m-%d %H:%M %Z')}"
        "."
    )

    candidates.append(
        ClearanceWindow(
            start=same_start,
            end=same_window_end,
            summary=" ".join(same_desc),
            label="Same Day",
        )
    )

    return candidates
