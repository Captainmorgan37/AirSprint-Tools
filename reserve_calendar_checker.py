"""Utilities for checking reserve calendar days for club workflows."""

from __future__ import annotations

from collections.abc import Iterable as IterableABC, Mapping as MappingABC
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    MOUNTAIN_TIME_ZONE,
    fetch_flights,
    fetch_flight_planning_note,
)
from flight_leg_utils import (
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
    safe_parse_dt,
)


_TARGET_DATE_VALUES: Sequence[date] = (
    date(2025, 12, 21),
    date(2025, 12, 26),
    date(2025, 12, 27),
    date(2025, 12, 28),
    date(2026, 1, 2),
    date(2026, 1, 3),
    date(2026, 1, 4),
    date(2026, 1, 17),
    date(2026, 2, 13),
    date(2026, 2, 14),
    date(2026, 2, 28),
    date(2026, 3, 7),
    date(2026, 3, 14),
    date(2026, 3, 21),
    date(2026, 4, 2),
    date(2026, 4, 3),
    date(2026, 4, 6),
    date(2026, 4, 7),
    date(2026, 4, 25),
    date(2026, 5, 18),
    date(2026, 10, 8),
    date(2026, 10, 9),
    date(2026, 10, 12),
    date(2026, 11, 7),
    date(2026, 11, 11),
    date(2026, 12, 19),
    date(2026, 12, 26),
    date(2026, 12, 27),
    date(2026, 12, 28),
)

TARGET_DATES: Sequence[date] = tuple(sorted(_TARGET_DATE_VALUES))


@dataclass
class ReserveDateCheck:
    """Result for a specific reserve calendar date."""

    date: date
    rows: List[Dict[str, Any]]
    diagnostics: Dict[str, Any]
    warnings: List[str]


@dataclass
class ReserveCheckResult:
    """Aggregated output for the reserve calendar checker."""

    dates: List[ReserveDateCheck]
    warnings: List[str]

    @property
    def has_matches(self) -> bool:
        return any(result.rows for result in self.dates)


def _coerce_to_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_nonempty(values: Iterable[Any]) -> Optional[str]:
    for value in values:
        text = _coerce_to_str(value)
        if text:
            return text
    return None


def _normalize_reference_datetime(reference: Optional[Any]) -> datetime:
    if reference is None:
        return datetime.now(MOUNTAIN_TIME_ZONE)
    if isinstance(reference, datetime):
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return reference.astimezone(MOUNTAIN_TIME_ZONE)
    if isinstance(reference, date):
        return datetime.combine(reference, time.min, tzinfo=MOUNTAIN_TIME_ZONE)
    raise TypeError("reference must be None, date, or datetime")


def select_upcoming_reserve_dates(
    reference: Optional[Any] = None,
    *,
    limit: int = 4,
) -> List[date]:
    """Return the next upcoming reserve dates relative to ``reference``."""

    if limit <= 0:
        return []

    normalized = _normalize_reference_datetime(reference)
    current_date = normalized.date()
    upcoming = [target for target in TARGET_DATES if target >= current_date]
    return upcoming[:limit]


def select_reserve_dates_in_range(start: date, end: date) -> List[date]:
    """Return reserve dates within an inclusive date range."""

    if start > end:
        start, end = end, start
    return [target for target in TARGET_DATES if start <= target <= end]


def _extract_flight_identifier(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "flightId",
        "flight_id",
        "flightID",
        "id",
        "uuid",
        "externalId",
        "external_id",
    ):
        value = row.get(key)
        text = _coerce_to_str(value)
        if text:
            return text
    return None


def _extract_workflow_label(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "workflowCustomName",
        "workflow_custom_name",
        "workflowCustomLabel",
        "workflow_custom_label",
        "workflowLabel",
        "workflow_label",
        "workflowName",
        "workflow_name",
        "workflow",
    ):
        value = row.get(key)
        if isinstance(value, MappingABC):
            nested = _first_nonempty(
                value.get(nested_key) for nested_key in ("customName", "customLabel", "label", "name", "title")
            )
            if nested:
                return nested
        text = _coerce_to_str(value)
        if text:
            return text
    return None


def _extract_planning_note_text(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        return _coerce_to_str(payload)
    if isinstance(payload, MappingABC):
        for key in ("planningNotes", "planningNote", "note", "notes", "text"):
            text = _coerce_to_str(payload.get(key))
            if text:
                return text
        data = payload.get("data")
        if isinstance(data, list):
            for item in data:
                text = _extract_planning_note_text(item)
                if text:
                    return text
        return None
    if isinstance(payload, IterableABC) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            text = _extract_planning_note_text(item)
            if text:
                return text
    return None


def _contains_club_keyword(note: str) -> bool:
    return "club" in note.lower()


def _workflow_has_as_available(workflow: Optional[str]) -> bool:
    if not workflow:
        return False
    return "as available" in workflow.lower()


def _parse_mountain_datetime(value: Any) -> Optional[datetime]:
    text = _coerce_to_str(value)
    if not text:
        return None
    try:
        parsed = safe_parse_dt(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(MOUNTAIN_TIME_ZONE)


def _format_route(row: Mapping[str, Any]) -> str:
    dep = _first_nonempty(
        row.get(key)
        for key in (
            "departure_airport",
            "dep_airport",
            "departureAirport",
            "departure_airport_code",
            "airportFrom",
            "fromAirport",
        )
    )
    arr = _first_nonempty(
        row.get(key)
        for key in (
            "arrival_airport",
            "arr_airport",
            "arrivalAirport",
            "arrival_airport_code",
            "airportTo",
            "toAirport",
        )
    )
    if dep or arr:
        return f"{dep or '???'} → {arr or '???'}"
    return ""


def _build_result_row(
    row: Mapping[str, Any],
    note: str,
    target_date: date,
) -> Dict[str, Any]:
    flight_id = _extract_flight_identifier(row) or ""
    workflow = _extract_workflow_label(row) or ""
    has_as_available = _workflow_has_as_available(workflow)

    tail = _first_nonempty((
        row.get("tail"),
        row.get("tailNumber"),
        row.get("tail_number"),
        row.get("aircraft"),
        row.get("aircraftRegistration"),
    )) or ""

    booking_identifier = _first_nonempty((
        row.get("bookingIdentifier"),
        row.get("booking_identifier"),
        row.get("bookingReference"),
        row.get("bookingCode"),
        row.get("bookingId"),
    )) or flight_id

    return {
        "Date": target_date.isoformat(),
        "Flight ID": booking_identifier,
        "tail": tail,
        "Route": _format_route(row),
        "workflow": workflow,
        "Planning Notes": note,
        "club_detected": True,
        "workflow_has_as_available": has_as_available,
        "status": (
            "✅ Workflow includes 'as available'"
            if has_as_available
            else "⚠️ Workflow missing 'as available'"
        ),
    }


def _filter_rows_for_target_date(rows: Iterable[Mapping[str, Any]], target_date: date) -> List[Mapping[str, Any]]:
    filtered: List[Mapping[str, Any]] = []
    for row in rows:
        mountain_dt = _parse_mountain_datetime(row.get("dep_time"))
        if mountain_dt and mountain_dt.date() == target_date:
            filtered.append(row)
    return filtered


def evaluate_flights_for_date(
    config: Fl3xxApiConfig,
    rows: Sequence[Mapping[str, Any]],
    target_date: date,
    *,
    session: Optional[requests.Session] = None,
    fetch_planning_note_fn=None,
) -> ReserveDateCheck:
    """Inspect flights for a single date and return flagged club flights."""

    if fetch_planning_note_fn is None:
        fetch_planning_note_fn = fetch_flight_planning_note

    diagnostics: Dict[str, Any] = {
        "total_flights": len(rows),
        "club_matches": 0,
        "missing_as_available": 0,
        "missing_flight_ids": 0,
        "missing_planning_notes": 0,
        "planning_note_errors": 0,
    }
    warnings: List[str] = []
    flagged: List[Dict[str, Any]] = []

    http = session or requests.Session()
    close_session = session is None
    try:
        for row in rows:
            flight_id = _extract_flight_identifier(row)
            if not flight_id:
                diagnostics["missing_flight_ids"] += 1
                continue
            try:
                payload = fetch_planning_note_fn(config, flight_id, session=http)
            except Exception as exc:  # pragma: no cover - defensive path
                diagnostics["planning_note_errors"] += 1
                warnings.append(
                    f"{target_date.isoformat()}: Unable to fetch planning note for flight {flight_id}: {exc}"
                )
                continue
            note = _extract_planning_note_text(payload)
            if not note:
                diagnostics["missing_planning_notes"] += 1
                continue
            if not _contains_club_keyword(note):
                continue
            diagnostics["club_matches"] += 1
            result_row = _build_result_row(row, note, target_date)
            if not result_row["workflow_has_as_available"]:
                diagnostics["missing_as_available"] += 1
            flagged.append(result_row)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return ReserveDateCheck(
        date=target_date,
        rows=flagged,
        diagnostics=diagnostics,
        warnings=warnings,
    )


def run_reserve_day_check(
    config: Fl3xxApiConfig,
    *,
    target_dates: Optional[Sequence[date]] = None,
    now: Optional[Any] = None,
    limit: int = 4,
    session: Optional[requests.Session] = None,
) -> ReserveCheckResult:
    """Fetch flights for upcoming reserve dates and flag club workflows."""

    if target_dates is not None:
        upcoming = sorted(target_dates)
    else:
        upcoming = select_upcoming_reserve_dates(now, limit=limit)

    if not upcoming:
        return ReserveCheckResult(dates=[], warnings=[])

    http = session or requests.Session()
    close_session = session is None
    results: List[ReserveDateCheck] = []
    warnings: List[str] = []

    try:
        for target_date in upcoming:
            try:
                flights, metadata = fetch_flights(
                    config,
                    from_date=target_date,
                    to_date=target_date + timedelta(days=1),
                    session=http,
                )
            except Exception as exc:  # pragma: no cover - defensive path
                message = f"{target_date.isoformat()}: Unable to fetch flights: {exc}"
                warnings.append(message)
                results.append(
                    ReserveDateCheck(
                        date=target_date,
                        rows=[],
                        diagnostics={"error": message},
                        warnings=[message],
                    )
                )
                continue

            normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
            filtered_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
            targeted_rows = _filter_rows_for_target_date(filtered_rows, target_date)

            date_result = evaluate_flights_for_date(
                config,
                targeted_rows,
                target_date,
                session=http,
            )
            date_result.diagnostics.update(
                {
                    "fetch_metadata": metadata,
                    "normalization_stats": normalization_stats,
                    "skipped_subcharter": skipped_subcharter,
                    "targeted_flights": len(targeted_rows),
                }
            )
            warnings.extend(date_result.warnings)
            results.append(date_result)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return ReserveCheckResult(dates=results, warnings=warnings)


__all__ = [
    "ReserveCheckResult",
    "ReserveDateCheck",
    "TARGET_DATES",
    "evaluate_flights_for_date",
    "run_reserve_day_check",
    "select_reserve_dates_in_range",
    "select_upcoming_reserve_dates",
]
