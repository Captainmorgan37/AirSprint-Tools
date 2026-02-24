"""Helpers for aggregating CJ maintenance status from FL3XX aircraft schedules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set
from urllib.parse import urlsplit

import pandas as pd
import requests

from fl3xx_api import DEFAULT_FL3XX_BASE_URL, Fl3xxApiConfig
from flight_leg_utils import safe_parse_dt
from hangar_logic import CJ_TAILS

UTC = timezone.utc
MAINTENANCE_TYPES: Sequence[str] = (
    "MAINTENANCE",
    "UNSCHEDULED_MAINTENANCE",
    "AOG",
)


@dataclass(frozen=True)
class MaintenanceEvent:
    tail: str
    task_id: str
    task_type: str
    start_utc: datetime
    end_utc: datetime
    notes: str


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
        events.append(
            MaintenanceEvent(
                tail=tail,
                task_id=task_id,
                task_type=task_type,
                start_utc=start_utc,
                end_utc=end_utc,
                notes=notes,
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
) -> pd.DataFrame:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    rows: List[Dict[str, Any]] = []

    for day_ts in all_dates:
        day_start = datetime.combine(day_ts.date(), time.min).replace(tzinfo=UTC)
        day_end = day_start + timedelta(days=1)
        per_type: Dict[str, Set[str]] = {task_type: set() for task_type in MAINTENANCE_TYPES}

        for event in events:
            overlaps = event.start_utc < day_end and event.end_utc >= day_start
            if overlaps:
                per_type[event.task_type].add(event.tail)

        rows.append(
            {
                "date": day_ts.date(),
                "scheduled_maintenance": len(per_type["MAINTENANCE"]),
                "unscheduled_maintenance": len(per_type["UNSCHEDULED_MAINTENANCE"]),
                "aog": len(per_type["AOG"]),
                "total_aircraft_down": len(set().union(*per_type.values())),
            }
        )

    return pd.DataFrame(rows)
