"""Temporary helper for pulling reserve-calendar PAX flight breakdowns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

from fl3xx_api import Fl3xxApiConfig, MOUNTAIN_TIME_ZONE, fetch_flights
from flight_leg_utils import filter_out_subcharter_rows, normalize_fl3xx_payload, safe_parse_dt
from reserve_calendar_checker import TARGET_DATES


@dataclass
class ReservePaxDayResult:
    date: date
    rows: List[Dict[str, Any]]
    diagnostics: Dict[str, Any]
    warnings: List[str]


@dataclass
class ReservePaxPullResult:
    year: int
    days: List[ReservePaxDayResult]
    warnings: List[str]


def select_reserve_dates_for_year(year: int) -> List[date]:
    return sorted(day for day in TARGET_DATES if day.year == year)


def _coerce_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_first_text(row: Mapping[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        text = _coerce_text(row.get(key))
        if text:
            return text
    return None


def _parse_mountain_dt(value: Any) -> Optional[datetime]:
    text = _coerce_text(value)
    if not text:
        return None
    try:
        parsed = safe_parse_dt(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(MOUNTAIN_TIME_ZONE)


def _extract_pax_number(row: Mapping[str, Any]) -> Optional[int]:
    for key in ("paxNumber", "pax", "passengerCount", "passengers"):
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            parsed = int(float(str(value)))
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return None


def _flight_is_in_window(row: Mapping[str, Any], target_date: date) -> bool:
    dep_dt = _parse_mountain_dt(row.get("dep_time"))
    if not dep_dt:
        return False
    if dep_dt.date() != target_date:
        return False
    dep_tod = dep_dt.time()
    return time(2, 0) <= dep_tod <= time(23, 59, 59)




def _is_ocs_workflow(workflow_name: Optional[str]) -> bool:
    if not workflow_name:
        return False
    return "ocs" in workflow_name.lower()

def _build_output_row(row: Mapping[str, Any], target_date: date) -> Dict[str, Any]:
    flight_ref = _extract_first_text(
        row,
        (
            "bookingIdentifier",
            "bookingReference",
            "bookingCode",
            "bookingId",
            "flightId",
            "id",
        ),
    ) or ""
    owner = _extract_first_text(
        row,
        (
            "accountName",
            "account",
            "owner",
            "ownerName",
            "customerName",
            "clientName",
        ),
    ) or ""
    workflow = _extract_first_text(
        row,
        (
            "workflowCustomName",
            "workflow_custom_name",
            "workflowName",
            "workflow",
        ),
    ) or ""
    pax_number = _extract_pax_number(row)

    dep_dt = _parse_mountain_dt(row.get("dep_time"))
    dep_time_mt = dep_dt.strftime("%H:%M") if dep_dt else ""

    return {
        "Date": target_date.isoformat(),
        "Dep Time (MT)": dep_time_mt,
        "Flight Ref": flight_ref,
        "Owner": owner,
        "PAX": pax_number if pax_number is not None else "",
        "Customs Workflow": workflow,
    }


def run_reserve_pax_pull(
    config: Fl3xxApiConfig,
    *,
    year: int,
    session: Optional[requests.Session] = None,
) -> ReservePaxPullResult:
    dates = select_reserve_dates_for_year(year)
    if not dates:
        return ReservePaxPullResult(year=year, days=[], warnings=[])

    http = session or requests.Session()
    close_session = session is None
    results: List[ReservePaxDayResult] = []
    warnings: List[str] = []

    try:
        for target_date in dates:
            day_warnings: List[str] = []
            try:
                flights, metadata = fetch_flights(
                    config,
                    from_date=target_date,
                    to_date=target_date + timedelta(days=1),
                    session=http,
                )
            except Exception as exc:  # pragma: no cover
                message = f"{target_date.isoformat()}: Unable to fetch flights: {exc}"
                day_warnings.append(message)
                warnings.append(message)
                results.append(
                    ReservePaxDayResult(
                        date=target_date,
                        rows=[],
                        diagnostics={"error": message},
                        warnings=day_warnings,
                    )
                )
                continue

            normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
            filtered_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)

            in_window_rows = [row for row in filtered_rows if _flight_is_in_window(row, target_date)]
            pax_rows = []
            skipped_ocs = 0
            for row in in_window_rows:
                workflow = _extract_first_text(
                    row,
                    (
                        "workflowCustomName",
                        "workflow_custom_name",
                        "workflowName",
                        "workflow",
                    ),
                )
                if _is_ocs_workflow(workflow):
                    skipped_ocs += 1
                    continue
                pax = _extract_pax_number(row)
                if pax is None or pax <= 0:
                    continue
                pax_rows.append(_build_output_row(row, target_date))

            diagnostics = {
                "fetched": len(flights),
                "normalization_stats": normalization_stats,
                "skipped_subcharter": skipped_subcharter,
                "in_window": len(in_window_rows),
                "pax_flights": len(pax_rows),
                "skipped_ocs": skipped_ocs,
                "fetch_metadata": metadata,
            }

            results.append(
                ReservePaxDayResult(
                    date=target_date,
                    rows=pax_rows,
                    diagnostics=diagnostics,
                    warnings=day_warnings,
                )
            )
    finally:
        if close_session:
            http.close()

    return ReservePaxPullResult(year=year, days=results, warnings=warnings)


__all__ = [
    "ReservePaxDayResult",
    "ReservePaxPullResult",
    "run_reserve_pax_pull",
    "select_reserve_dates_for_year",
]
