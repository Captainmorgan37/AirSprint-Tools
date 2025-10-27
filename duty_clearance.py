"""Helpers for building the crew duty clearance dashboard."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd

from fl3xx_api import (
    Fl3xxApiConfig,
    MOUNTAIN_TIME_ZONE,
    PreflightChecklistStatus,
    fetch_flight_crew,
    fetch_preflight,
    parse_preflight_payload,
)
from flight_leg_utils import get_todays_sorted_legs_by_tail
from zoneinfo_compat import ZoneInfo


def _epoch_to_dt_utc(epoch_val: Optional[int]) -> Optional[datetime]:
    """Convert a FL3XX epoch value (seconds, ms, µs, or ns) to UTC."""

    if epoch_val is None:
        return None

    numeric = float(epoch_val)

    # FL3XX sometimes returns timestamps with millisecond, microsecond, or
    # nanosecond precision encoded as integers. Keep dividing by 1000 until the
    # value falls into the expected "seconds since epoch" range. Guard the loop
    # to avoid infinite iteration on corrupted values.
    for _ in range(6):
        if abs(numeric) < 10**12:
            break
        numeric /= 1000.0

    return datetime.fromtimestamp(numeric, tz=timezone.utc)


def _build_preflight_signature(preflight_status: PreflightChecklistStatus) -> Optional[Tuple[Tuple[str, str], ...]]:
    """Return a stable crew signature based on the preflight check-ins."""

    entries: List[Tuple[str, str]] = []
    for check in preflight_status.crew_checkins:
        role = (check.pilot_role or "").strip().upper() or "UNK"
        identifier = (check.user_id or "").strip()
        if identifier:
            entries.append((role, identifier))
    if not entries:
        return None
    entries.sort()
    return tuple(entries)


def _coerce_zoneinfo(tz_name: Optional[str]) -> ZoneInfo:
    if not tz_name:
        return MOUNTAIN_TIME_ZONE
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return MOUNTAIN_TIME_ZONE


def _extract_departure_timezone(leg_info: Mapping[str, Any]) -> ZoneInfo:
    tz_name = leg_info.get("dep_tz")
    if isinstance(tz_name, str) and tz_name.strip():
        return _coerce_zoneinfo(tz_name.strip())
    return MOUNTAIN_TIME_ZONE


def _extract_departure_airport(leg_info: Mapping[str, Any]) -> Optional[str]:
    airport_value = leg_info.get("dep_airport")
    if isinstance(airport_value, str) and airport_value.strip():
        return airport_value.strip()
    return None


def _format_member_name(member: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for key in ("firstName", "middleName", "lastName"):
        value = member.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    if parts:
        return " ".join(parts)
    for fallback in ("logName", "email", "trigram", "personnelNumber"):
        value = member.get(fallback)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _select_crew_member(crew: Iterable[Mapping[str, Any]], role: str) -> Optional[Mapping[str, Any]]:
    for member in crew:
        role_value = member.get("role")
        if isinstance(role_value, str) and role_value.strip().upper() == role.upper():
            return member
    return None


def _format_crew_label(crew_payload: Iterable[Mapping[str, Any]]) -> str:
    pic_member = _select_crew_member(crew_payload, "CMD")
    sic_member = _select_crew_member(crew_payload, "FO")

    pic_name = _format_member_name(pic_member) if pic_member else ""
    sic_name = _format_member_name(sic_member) if sic_member else ""

    return f"CMD: {pic_name or '??'} / FO: {sic_name or '??'}"


def _serialise_checkins_for_debug(preflight_status: PreflightChecklistStatus) -> Optional[str]:
    if not preflight_status.crew_checkins:
        return None

    serialised: List[Dict[str, Any]] = []
    for entry in preflight_status.crew_checkins:
        serialised.append(
            {
                "pilotRole": entry.pilot_role,
                "userId": entry.user_id,
                "checkin": entry.checkin,
                "checkinActual": entry.checkin_actual,
                "checkinDefault": entry.checkin_default,
                "extraCheckins": list(entry.extra_checkins),
            }
        )

    return json.dumps(serialised, ensure_ascii=False, sort_keys=True)


def _get_report_time_local(
    preflight_status: PreflightChecklistStatus,
    duty_tz: ZoneInfo,
) -> Optional[datetime]:
    """Return the earliest planned report time for the duty in the duty timezone."""

    epochs: List[int] = []
    for check in preflight_status.crew_checkins:
        for candidate in (check.checkin_default, check.checkin_actual, check.checkin):
            if candidate is not None:
                epochs.append(candidate)
        for extra in getattr(check, "extra_checkins", ()):  # type: ignore[attr-defined]
            if extra is not None:
                epochs.append(extra)
    if not epochs:
        return None

    earliest = min(epochs)
    report_utc = _epoch_to_dt_utc(earliest)
    if report_utc is None:
        return None
    return report_utc.astimezone(duty_tz)


def _compute_confirm_by(
    report_local: datetime,
    first_leg_dep_local: datetime,
    has_early_flight: bool,
) -> datetime:
    """Compute the confirm-by deadline following the company rest logic."""

    ten_hours_before_report = report_local - timedelta(hours=10)

    previous_calendar_day = report_local.date() - timedelta(days=1)
    assumed_rest_start = datetime(
        previous_calendar_day.year,
        previous_calendar_day.month,
        previous_calendar_day.day,
        22,
        0,
        tzinfo=report_local.tzinfo,
    )

    candidates = [ten_hours_before_report, assumed_rest_start]

    if has_early_flight:
        ten_hours_before_etd = first_leg_dep_local - timedelta(hours=10)
        candidates.append(ten_hours_before_etd)

    return min(candidates)


def _fmt_timeleft(now_local: datetime, cutoff_local: datetime) -> Tuple[str, int]:
    """Return a human-friendly label and minutes remaining until the cutoff."""

    delta = cutoff_local - now_local
    minutes_left = math.floor(delta.total_seconds() / 60.0)

    hours = abs(minutes_left) // 60
    minutes = abs(minutes_left) % 60

    if minutes_left >= 0:
        label = f"{hours}h {minutes:02d}m left"
    else:
        label = f"OVERDUE by {hours}h {minutes:02d}m"

    return label, minutes_left


def compute_clearance_table(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    now: Optional[datetime] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build a table of crew duty clearance status for the provided duty date."""

    legs_by_tail = get_todays_sorted_legs_by_tail(config, target_date)
    now_utc = now.astimezone(timezone.utc) if isinstance(now, datetime) else datetime.now(timezone.utc)

    rows: List[Dict[str, Any]] = []
    troubleshooting: List[Dict[str, Any]] = []

    def record_issue(
        issue: str,
        *,
        tail: Optional[str] = None,
        flight_id: Optional[Any] = None,
        detail: Optional[str] = None,
        preflight_checkins: Optional[str] = None,
    ) -> None:
        entry: Dict[str, Any] = {
            "Tail": tail or "",
            "Flight ID": flight_id if flight_id is not None else "",
            "Issue": issue,
        }
        if detail:
            entry["Details"] = detail
        if preflight_checkins:
            entry["Preflight check-ins"] = preflight_checkins
        troubleshooting.append(entry)

    if not legs_by_tail:
        record_issue(
            "No duties returned for the selected date.",
            detail="Verify the FL3XX schedule or adjust the duty date.",
        )

    for tail, legs in legs_by_tail.items():
        last_signature: Optional[Tuple[Tuple[str, str], ...]] = None

        for leg_info in legs:
            flight_id = leg_info.get("flightId")
            if flight_id is None:
                record_issue(
                    "Missing flight identifier in FL3XX response.",
                    tail=tail,
                    detail=str({k: leg_info.get(k) for k in ("tail", "flightNumber") if k in leg_info}),
                )
                continue

            try:
                preflight_payload = fetch_preflight(config, flight_id)
            except Exception as exc:  # pragma: no cover - network failures
                record_issue(
                    "Failed to load preflight checklist.",
                    tail=tail,
                    flight_id=flight_id,
                    detail=str(exc),
                )
                continue

            try:
                preflight_status = parse_preflight_payload(preflight_payload)
            except Exception as exc:
                record_issue(
                    "Unable to parse preflight checklist payload.",
                    tail=tail,
                    flight_id=flight_id,
                    detail=str(exc),
                )
                continue

            checkins_debug = _serialise_checkins_for_debug(preflight_status)

            signature = _build_preflight_signature(preflight_status)
            if signature is None:
                signature = (("LEG", str(flight_id)),)

            if last_signature is not None and signature == last_signature:
                continue
            last_signature = signature

            try:
                crew_payload = fetch_flight_crew(config, flight_id)
            except Exception as exc:  # pragma: no cover - network failures
                record_issue(
                    "Failed to load crew roster from FL3XX.",
                    tail=tail,
                    flight_id=flight_id,
                    detail=str(exc),
                )
                continue

            crew_label = _format_crew_label(crew_payload)

            duty_tz = _extract_departure_timezone(leg_info)
            dep_dt_utc = leg_info.get("dep_dt_utc")
            if not isinstance(dep_dt_utc, datetime):
                record_issue(
                    "Missing departure time for first leg.",
                    tail=tail,
                    flight_id=flight_id,
                    preflight_checkins=checkins_debug,
                )
                continue

            first_leg_dep_local = dep_dt_utc.astimezone(duty_tz)
            has_early_flight = 2 <= first_leg_dep_local.hour < 8

            report_local = _get_report_time_local(preflight_status, duty_tz)
            if report_local is None:
                record_issue(
                    "Unable to determine crew report time from checklist.",
                    tail=tail,
                    flight_id=flight_id,
                    preflight_checkins=checkins_debug,
                )
                continue

            confirm_by_local = _compute_confirm_by(
                report_local,
                first_leg_dep_local,
                has_early_flight,
            )

            now_local = now_utc.astimezone(duty_tz)
            timeleft_label, minutes_left = _fmt_timeleft(now_local, confirm_by_local)

            if preflight_status.all_ok is True:
                status_text = "✅ CLEAR"
            elif preflight_status.all_ok is False:
                status_text = "⚠️ NOT CLEARED"
            else:
                status_text = "⏳ UNKNOWN"

            rows.append(
                {
                    "Tail": tail,
                    "Crew": crew_label,
                    "Report (local)": report_local.strftime("%Y-%m-%d %H:%M %Z"),
                    "First ETD (local)": first_leg_dep_local.strftime("%Y-%m-%d %H:%M %Z"),
                    "Status": status_text,
                    "Time left": timeleft_label,
                    "_minutes_left": minutes_left,
                    "_confirm_by_local": confirm_by_local,
                    "_report_local_dt": report_local,
                    "_first_dep_local_dt": first_leg_dep_local,
                    "_has_early_flight": has_early_flight,
                    "_departure_airport": _extract_departure_airport(leg_info),
                    "_duty_timezone": duty_tz.key if hasattr(duty_tz, "key") else str(duty_tz),
                }
            )

    raw_df = pd.DataFrame(rows)
    if not rows:
        display_df = pd.DataFrame()
    else:
        raw_df = raw_df.sort_values(by="_minutes_left")

        display_df = raw_df[[
            "Tail",
            "Crew",
            "Report (local)",
            "First ETD (local)",
            "Status",
            "Time left",
        ]].reset_index(drop=True)

    troubleshooting_df = pd.DataFrame(troubleshooting)

    return display_df, raw_df, troubleshooting_df


__all__ = [
    "compute_clearance_table",
]
