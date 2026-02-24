"""Helpers for aggregating CJ maintenance status from FL3XX aircraft schedules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set
from urllib.parse import urlsplit

import pandas as pd
import requests

from fl3xx_api import DEFAULT_FL3XX_BASE_URL, Fl3xxApiConfig
from flight_leg_utils import load_airport_tz_lookup
from flight_leg_utils import safe_parse_dt
from zoneinfo_compat import ZoneInfo
from hangar_logic import CJ_TAILS

UTC = timezone.utc
MAINTENANCE_TYPES: Sequence[str] = (
    "MAINTENANCE",
    "UNSCHEDULED_MAINTENANCE",
    "AOG",
)


def _covered_seconds(intervals: Sequence[tuple[datetime, datetime]]) -> float:
    if not intervals:
        return 0.0

    merged: List[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals, key=lambda interval: interval[0]):
        if end <= start:
            continue
        if not merged or start >= merged[-1][1]:
            merged.append((start, end))
            continue
        merged_start, merged_end = merged[-1]
        merged[-1] = (merged_start, max(merged_end, end))

    return sum((end - start).total_seconds() for start, end in merged)


@dataclass(frozen=True)
class MaintenanceEvent:
    tail: str
    task_id: str
    task_type: str
    start_utc: datetime
    end_utc: datetime
    notes: str
    airport_code: Optional[str] = None
    airport_tz: str = "UTC"


def list_cj_tails() -> List[str]:
    """Return CJ tails sorted and formatted for the FL3XX aircraft endpoint."""

    return [format_tail_for_fl3xx(tail) for tail in sorted(CJ_TAILS)]


def format_tail_for_fl3xx(tail: str) -> str:
    compact = str(tail).strip().upper().replace("-", "")
    if compact.startswith("C") and len(compact) > 1:
        return f"{compact[:1]}-{compact[1:]}"
    return compact


def _derive_api_root(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if not parsed.scheme or not parsed.netloc:
        parsed = urlsplit(DEFAULT_FL3XX_BASE_URL)
    return f"{parsed.scheme}://{parsed.netloc}"


def _schedule_url(config: Fl3xxApiConfig, tail: str) -> str:
    root = _derive_api_root(config.base_url)
    return f"{root}/api/external/aircraft/{tail}/schedule"


def _parse_utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    dt = safe_parse_dt(str(value))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def fetch_aircraft_schedule(
    config: Fl3xxApiConfig,
    tail: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _schedule_url(config, tail),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        payload = response.json()
    finally:
        if close_session:
            http.close()

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, Mapping)]
    if isinstance(payload, Mapping) and isinstance(payload.get("items"), list):
        return [row for row in payload["items"] if isinstance(row, Mapping)]
    return []


def extract_maintenance_events(tasks: Iterable[Mapping[str, Any]], tail: str) -> List[MaintenanceEvent]:
    events: List[MaintenanceEvent] = []
    airport_tz_lookup = load_airport_tz_lookup()

    def _extract_codes(value: Any) -> List[str]:
        if isinstance(value, Mapping):
            fields = [
                value.get("icao"),
                value.get("iata"),
                value.get("code"),
                value.get("airport"),
                value.get("id"),
                value.get("name"),
            ]
            text = " ".join(str(item) for item in fields if item not in (None, ""))
        else:
            text = str(value or "")
        return [token.upper() for token in re.findall(r"\b[A-Za-z0-9]{3,4}\b", text.upper())]

    def _event_airport_code(task: Mapping[str, Any]) -> Optional[str]:
        candidates = (
            "departureAirport",
            "departureAirportCode",
            "departureAirportIcao",
            "departureAirportIata",
            "airportFrom",
            "arrivalAirport",
            "arrivalAirportCode",
            "arrivalAirportIcao",
            "arrivalAirportIata",
            "airportTo",
        )
        for key in candidates:
            value = task.get(key)
            if value in (None, ""):
                continue
            codes = _extract_codes(value)
            if codes:
                return codes[0]
        return None

    for task in tasks:
        task_type_raw = task.get("taskType")
        task_type = str(task_type_raw).strip().upper() if task_type_raw is not None else ""
        if task_type not in MAINTENANCE_TYPES:
            continue

        start_utc = _parse_utc(task.get("departureDateUTC"))
        end_utc = _parse_utc(task.get("arrivalDateUTC"))
        if start_utc is None or end_utc is None:
            continue
        if end_utc < start_utc:
            start_utc, end_utc = end_utc, start_utc

        task_id = str(task.get("id") or "")
        notes = str(task.get("notes") or "")
        airport_code = _event_airport_code(task)
        airport_tz = airport_tz_lookup.get(airport_code, "UTC") if airport_code else "UTC"
        events.append(
            MaintenanceEvent(
                tail=tail,
                task_id=task_id,
                task_type=task_type,
                start_utc=start_utc,
                end_utc=end_utc,
                notes=notes,
                airport_code=airport_code,
                airport_tz=airport_tz,
            )
        )

    return events


def collect_cj_maintenance_events(
    config: Fl3xxApiConfig,
    *,
    tails: Optional[Sequence[str]] = None,
) -> tuple[list[MaintenanceEvent], list[str]]:
    selected_tails = list(tails) if tails else list_cj_tails()
    all_events: List[MaintenanceEvent] = []
    warnings: List[str] = []

    with requests.Session() as session:
        for tail in selected_tails:
            try:
                tasks = fetch_aircraft_schedule(config, tail, session=session)
                all_events.extend(extract_maintenance_events(tasks, tail))
            except Exception as exc:
                warnings.append(f"{tail}: {exc}")

    return all_events, warnings


def maintenance_daily_status(
    events: Sequence[MaintenanceEvent],
    start_date: date,
    end_date: date,
    *,
    fractional_day: bool = False,
) -> pd.DataFrame:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    rows: List[Dict[str, Any]] = []

    date_rows: Dict[date, Dict[str, Any]] = {
        day_ts.date(): {
            "date": day_ts.date(),
            "per_type_tails": {task_type: set() for task_type in MAINTENANCE_TYPES},
            "per_type_intervals": {task_type: {} for task_type in MAINTENANCE_TYPES},
            "per_tail_intervals": {},
        }
        for day_ts in all_dates
    }

    for event in events:
        try:
            zone = ZoneInfo(event.airport_tz)
        except Exception:
            zone = UTC

        local_start = event.start_utc.astimezone(zone)
        local_end = event.end_utc.astimezone(zone)

        if not fractional_day:
            current_day_start = datetime.combine(local_start.date(), time.min, tzinfo=zone)
            while current_day_start < local_end:
                day_row = date_rows.get(current_day_start.date())
                if day_row is not None:
                    day_row["per_type_tails"][event.task_type].add(event.tail)
                current_day_start += timedelta(days=1)
            continue

        current_day_start = datetime.combine(local_start.date(), time.min, tzinfo=zone)
        while current_day_start < local_end:
            next_day_start = current_day_start + timedelta(days=1)
            overlap_start = max(local_start, current_day_start)
            overlap_end = min(local_end, next_day_start)
            if overlap_end > overlap_start:
                day_row = date_rows.get(current_day_start.date())
                if day_row is not None:
                    day_row["per_type_intervals"][event.task_type].setdefault(event.tail, []).append(
                        (overlap_start, overlap_end)
                    )
                    day_row["per_tail_intervals"].setdefault(event.tail, []).append((overlap_start, overlap_end))
            current_day_start = next_day_start

    for day in [day_ts.date() for day_ts in all_dates]:
        day_row = date_rows[day]
        if not fractional_day:
            per_type = day_row["per_type_tails"]
            rows.append(
                {
                    "date": day,
                    "scheduled_maintenance": len(per_type["MAINTENANCE"]),
                    "unscheduled_maintenance": len(per_type["UNSCHEDULED_MAINTENANCE"]),
                    "aog": len(per_type["AOG"]),
                    "total_aircraft_down": len(set().union(*per_type.values())),
                }
            )
            continue

        seconds_in_day = 24 * 60 * 60
        per_type_intervals = day_row["per_type_intervals"]
        per_tail_intervals = day_row["per_tail_intervals"]
        rows.append(
            {
                "date": day,
                "scheduled_maintenance": (
                    sum(_covered_seconds(intervals) for intervals in per_type_intervals["MAINTENANCE"].values())
                    / seconds_in_day
                ),
                "unscheduled_maintenance": (
                    sum(
                        _covered_seconds(intervals)
                        for intervals in per_type_intervals["UNSCHEDULED_MAINTENANCE"].values()
                    )
                    / seconds_in_day
                ),
                "aog": sum(_covered_seconds(intervals) for intervals in per_type_intervals["AOG"].values())
                / seconds_in_day,
                "total_aircraft_down": (
                    sum(_covered_seconds(intervals) for intervals in per_tail_intervals.values()) / seconds_in_day
                ),
            }
        )

    return pd.DataFrame(rows)
