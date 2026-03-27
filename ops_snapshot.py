"""Shared operational data pull helpers for Gantt and crew tools."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import time
from typing import Any, Dict, List, Mapping, Optional

import requests

from cj_maintenance_status import fetch_aircraft_schedule
from flight_leg_utils import safe_parse_dt
from fl3xx_api import fetch_staff_roster
from gantt_roster_assignment import assign_roster_to_schedule_rows, roster_window_bounds


DEFAULT_LANE_DEFINITIONS: List[str] = [
    "Add EMB West",
    "Add EMB East",
    "C-GASL",
    "C-FASV",
    "C-FLAS",
    "C-FJAS",
    "C-FASF",
    "C-GASE",
    "C-GASK",
    "C-GXAS",
    "C-GBAS",
    "C-FSNY",
    "C-FSYX",
    "C-FSBR",
    "C-FSRX",
    "C-FSJR",
    "C-FASQ",
    "C-FSDO",
    "C-FASN",
    "Remove OCS",
    "Add CJ2+ West",
    "Add CJ2+ East",
    "C-FASP",
    "C-FASR",
    "C-FASW",
    "C-FIAS",
    "C-GASR",
    "C-GZAS",
    "Add CJ3+ West",
    "Add CJ3+ East",
    "C-FASY",
    "C-GASW",
    "C-GAAS",
    "C-FNAS",
    "C-GNAS",
    "C-GFFS",
    "C-FSFS",
    "C-GFSX",
    "C-FSFO",
    "C-FSNP",
    "C-FSQX",
    "C-FSFP",
    "C-FSEF",
    "C-FSDN",
    "C-GFSD",
    "C-FSUP",
    "C-FSRY",
    "C-GFSJ",
    "C-GIAS",
    "C-FSVP",
]

MAINTENANCE_TYPES = {"MAINTENANCE", "UNSCHEDULED_MAINTENANCE", "AOG"}
ROSTER_MIN_TIMEOUT_SECONDS = 60
ROSTER_PULL_MAX_ATTEMPTS = 3


def _roster_pull_config(config: Any) -> Any:
    timeout_value = getattr(config, "timeout", None)
    try:
        timeout_seconds = int(timeout_value)
    except (TypeError, ValueError):
        timeout_seconds = 30

    if timeout_seconds >= ROSTER_MIN_TIMEOUT_SECONDS:
        return config

    if hasattr(config, "__dataclass_fields__"):
        try:
            return replace(config, timeout=ROSTER_MIN_TIMEOUT_SECONDS)
        except Exception:
            pass

    return config


def _fetch_roster_rows_with_retry(config: Any, roster_window: tuple[datetime, datetime]) -> List[Mapping[str, Any]]:
    roster_config = _roster_pull_config(config)
    last_error: Optional[Exception] = None

    for attempt in range(1, ROSTER_PULL_MAX_ATTEMPTS + 1):
        try:
            with requests.Session() as roster_session:
                return list(
                    fetch_staff_roster(
                        roster_config,
                        from_time=roster_window[0],
                        to_time=roster_window[1],
                        filter_value="STAFF",
                        include_flights=True,
                        drop_empty_rows=True,
                        session=roster_session,
                    )
                )
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt >= ROSTER_PULL_MAX_ATTEMPTS:
                break
            time.sleep(float(attempt))

    if last_error is not None:
        raise RuntimeError(
            f"{last_error} (after {ROSTER_PULL_MAX_ATTEMPTS} attempts; roster timeout "
            f"raised to at least {ROSTER_MIN_TIMEOUT_SECONDS}s)"
        ) from last_error

    return []


def _to_utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        dt = safe_parse_dt(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _pick_dt(task: Mapping[str, Any], candidates: List[str]) -> Optional[datetime]:
    for key in candidates:
        if key in task:
            parsed = _to_utc(task.get(key))
            if parsed:
                return parsed
    return None


def _pick_airport(task: Mapping[str, Any], candidates: List[str]) -> str:
    for key in candidates:
        value = task.get(key)
        if value not in (None, ""):
            return str(value).strip().upper()
    return ""


def _extract_workflow(task: Mapping[str, Any]) -> str:
    values: List[str] = []

    workflow_custom_name = task.get("workflowCustomName")
    if workflow_custom_name not in (None, ""):
        values.append(str(workflow_custom_name))

    workflow = task.get("workflow")
    if isinstance(workflow, Mapping):
        values.extend(str(v) for v in workflow.values() if v not in (None, ""))
    elif isinstance(workflow, list):
        values.extend(str(item) for item in workflow if item not in (None, ""))
    elif workflow not in (None, ""):
        values.append(str(workflow))

    for key in ("workflowName", "workflowType", "workflowLabel"):
        value = task.get(key)
        if value not in (None, ""):
            values.append(str(value))

    return " | ".join(values)


def _classify(task: Mapping[str, Any], workflow_text: str) -> str:
    task_id = str(task.get("id") or "").strip().lower()
    task_type = str(task.get("taskType") or "").strip().upper()
    if task_id.startswith("task"):
        if task_type == "NOTE":
            return "Note"
        return "Maintenance"
    if task_id and not task_id.startswith("flight"):
        return "Maintenance"
    if task_type in MAINTENANCE_TYPES or "MAINT" in task_type:
        return "Maintenance"
    if "OCS" in workflow_text.upper():
        return "OCS Flight"
    return "Client Flight"


def _task_to_row(tail: str, lane: str, task: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    start = _pick_dt(
        task,
        [
            "departureDateUTC",
            "departureDateUtc",
            "scheduledOut",
            "offBlock",
            "departureDate",
            "startDateUTC",
            "startDate",
        ],
    )
    end = _pick_dt(
        task,
        [
            "arrivalDateUTC",
            "arrivalDateUtc",
            "scheduledIn",
            "onBlock",
            "arrivalDate",
            "endDateUTC",
            "endDate",
        ],
    )
    if not start or not end:
        return None
    if end < start:
        start, end = end, start
    if end == start:
        end = start + timedelta(minutes=15)

    workflow_text = _extract_workflow(task)
    category = _classify(task, workflow_text)
    return {
        "lane": lane,
        "tail": tail,
        "start_utc": start,
        "end_utc": end,
        "category": category,
        "task_type": str(task.get("taskType") or ""),
        "workflow": workflow_text,
        "notes": str(task.get("notes") or ""),
        "task_id": str(task.get("id") or ""),
        "departure_airport": _pick_airport(task, ["departureAirport", "fromAirport", "departureAirportIcao"]),
        "arrival_airport": _pick_airport(task, ["arrivalAirport", "toAirport", "arrivalAirportIcao"]),
    }


def pull_ops_snapshot(config: Any, lane_targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """Pull schedule + roster rows once and return reusable snapshot data."""

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    roster_rows: List[Mapping[str, Any]] = []

    targets = list(lane_targets or DEFAULT_LANE_DEFINITIONS)
    with requests.Session() as session:
        for lane in targets:
            try:
                schedule = fetch_aircraft_schedule(config, lane, session=session)
            except Exception as exc:
                warnings.append(f"{lane}: {exc}")
                continue

            for task in schedule:
                if not isinstance(task, Mapping):
                    continue
                row = _task_to_row(lane, lane, task)
                if row is not None:
                    rows.append(row)

    roster_window = roster_window_bounds()
    roster_meta = {
        "from": roster_window[0].strftime("%Y-%m-%dT%H:%M"),
        "to": roster_window[1].strftime("%Y-%m-%dT%H:%M"),
    }

    try:
        roster_rows = _fetch_roster_rows_with_retry(config, roster_window)
        rows = assign_roster_to_schedule_rows(rows, roster_rows)
    except Exception as exc:
        warnings.append(f"Roster pull failed: {exc}")

    return {
        "rows": rows,
        "warnings": warnings,
        "roster_meta": roster_meta,
        "roster_rows": roster_rows,
    }
