"""Business logic for executing the Operations Lead morning reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from fl3xx_api import compute_fetch_dates, fetch_flights
from flight_leg_utils import (
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
    safe_parse_dt,
)


class MorningReportError(RuntimeError):
    """Raised when the morning report workflow cannot be completed."""


@dataclass
class MorningReportResult:
    """Structured output for a single morning report."""

    code: str
    title: str
    header_label: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def match_count(self) -> int:
        return len(self.rows)

    @property
    def has_matches(self) -> bool:
        return self.match_count > 0

    def formatted_output(self) -> str:
        if self.has_matches:
            lines = ["Results Found:", self.header_label]
            lines.extend(row.get("line", "") for row in self.rows)
        else:
            lines = ["No Results Found"]
        return "\n".join(lines)


@dataclass
class MorningReportRun:
    """Combined output for a full button press run."""

    fetched_at: datetime
    from_date: date
    to_date: date
    reports: List[MorningReportResult]
    leg_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    normalization_stats: Dict[str, Any] = field(default_factory=dict)

    def report_map(self) -> Dict[str, MorningReportResult]:
        return {report.code: report for report in self.reports}


_APP_BOOKING_WORKFLOW = "APP BOOKING"
_APP_LINE_PREFIXES = (
    "APP ",
    "APP",
    "APP CJ2+",
    "APP CJ2+/CJ3+",
    "APP CJ3+",
    "APP E550",
)
_EXPECTED_EMPTY_LEG_ACCOUNT = "AIRSPRINT INC."


def run_morning_reports(
    api_settings: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> MorningReportRun:
    """Fetch FL3XX legs and execute the configured morning reports."""

    current_time = now or datetime.now(timezone.utc)
    config = build_fl3xx_api_config(dict(api_settings))
    default_from, default_to = compute_fetch_dates(current_time, inclusive_days=4)
    if from_date is None:
        from_date = default_from
    if to_date is None:
        to_date = default_to

    if to_date < from_date:
        raise MorningReportError("Report end date must not be before the start date")

    flights, fetch_metadata = fetch_flights(
        config,
        from_date=from_date,
        to_date=to_date,
        now=current_time,
    )

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    normalized_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)

    metadata = {
        **fetch_metadata,
        "skipped_subcharter": skipped_subcharter,
    }

    fetched_at_raw = metadata.get("fetched_at")
    fetched_at = safe_parse_dt(fetched_at_raw) if fetched_at_raw else current_time
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    reports = [
        _build_app_booking_report(normalized_rows),
        _build_app_line_assignment_report(normalized_rows),
        _build_empty_leg_report(normalized_rows),
    ]

    return MorningReportRun(
        fetched_at=fetched_at,
        from_date=from_date,
        to_date=to_date,
        reports=reports,
        leg_count=len(normalized_rows),
        metadata=metadata,
        normalization_stats=normalization_stats,
    )


def _build_app_booking_report(rows: Iterable[Mapping[str, Any]]) -> MorningReportResult:
    matches = [
        row
        for row in rows
        if _normalize_str(_extract_workflow(row)) == _APP_BOOKING_WORKFLOW
    ]
    formatted_rows = [_format_report_row(row) for row in _sort_rows(matches)]
    return MorningReportResult(
        code="16.1.1",
        title="App Booking Workflow Report",
        header_label="App Booking Workflow",
        rows=formatted_rows,
        metadata={"match_count": len(formatted_rows)},
    )


def _build_app_line_assignment_report(rows: Iterable[Mapping[str, Any]]) -> MorningReportResult:
    matches = [row for row in rows if _is_app_line_placeholder(row)]
    formatted_rows = [_format_report_row(row) for row in _sort_rows(matches)]
    return MorningReportResult(
        code="16.1.2",
        title="App Line Assignment Report",
        header_label="App Line Assignment",
        rows=formatted_rows,
        metadata={"match_count": len(formatted_rows)},
    )


def _build_empty_leg_report(rows: Iterable[Mapping[str, Any]]) -> MorningReportResult:
    matches = [row for row in rows if _is_empty_leg(row)]
    formatted_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for row in _sort_rows(matches):
        formatted = _format_report_row(row, include_tail=True)
        account_name_raw = formatted.get("account_name")
        account_name_normalized = _normalize_str(account_name_raw)
        account_match_value = (account_name_normalized or "").upper()
        account_ok = account_match_value == _EXPECTED_EMPTY_LEG_ACCOUNT
        formatted["account_expected"] = account_ok

        if account_ok:
            continue

        warning = (
            f"Leg {formatted.get('leg_id') or formatted.get('line')} "
            f"has unexpected account value: {account_name_normalized or '—'}"
        )
        formatted["line"] = f"{formatted['line']} ⚠️ Account mismatch"
        warnings.append(warning)
        formatted_rows.append(formatted)

    return MorningReportResult(
        code="16.1.3",
        title="Empty Leg Report",
        header_label="Empty Leg Report",
        rows=formatted_rows,
        warnings=warnings,
        metadata={
            "match_count": len(formatted_rows),
            "expected_account": _EXPECTED_EMPTY_LEG_ACCOUNT,
        },
    )


def _sort_rows(rows: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return sorted(rows, key=_row_sort_key)


def _row_sort_key(row: Mapping[str, Any]) -> datetime:
    dep_time = _normalize_str(row.get("dep_time"))
    if dep_time:
        dt = safe_parse_dt(dep_time)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.max.replace(tzinfo=timezone.utc)


def _format_report_row(
    row: Mapping[str, Any],
    *,
    include_tail: bool = False,
) -> Dict[str, Any]:
    dep_time_raw = _normalize_str(row.get("dep_time"))
    dep_dt = safe_parse_dt(dep_time_raw) if dep_time_raw else None
    if dep_dt and dep_dt.tzinfo is None:
        dep_dt = dep_dt.replace(tzinfo=timezone.utc)

    date_component = dep_dt.date().isoformat() if dep_dt else "Unknown Date"
    booking_reference = _extract_booking_reference(row)
    account_name = _extract_account_name(row)
    tail = _extract_tail(row) if include_tail else None

    parts = [
        date_component,
        booking_reference or "Unknown Booking",
        account_name or "Unknown Account",
    ]
    if include_tail:
        parts.append(tail or "Unknown Tail")

    formatted: Dict[str, Any] = {
        "line": "-".join(parts),
        "date": date_component,
        "departure_time": dep_dt.isoformat() if dep_dt else None,
        "booking_reference": booking_reference,
        "account_name": account_name,
        "tail": tail,
        "workflow": _extract_workflow(row),
        "flight_type": _extract_flight_type(row),
        "leg_id": _extract_leg_id(row),
        "departure_airport": _extract_airport(row, True),
        "arrival_airport": _extract_airport(row, False),
    }
    return formatted


def _normalize_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    return text or None


def _extract_workflow(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "workflowCustomName",
        "workflow_custom_name",
        "workflowName",
        "workflow",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _extract_booking_reference(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "bookingReference",
        "bookingCode",
        "bookingNumber",
        "bookingId",
        "booking_id",
        "bookingID",
        "bookingRef",
        "booking",
        "salesOrderNumber",
        "salesOrder",
        "reservationNumber",
        "reservationId",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _extract_account_name(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "accountName",
        "account",
        "owner",
        "ownerName",
        "customer",
        "customerName",
        "client",
        "clientName",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _extract_tail(row: Mapping[str, Any]) -> Optional[str]:
    tail_candidate = row.get("tail")
    value = _normalize_str(tail_candidate)
    if value:
        return value

    for key in (
        "registrationNumber",
        "registration",
        "aircraftRegistration",
        "aircraft",
    ):
        candidate = row.get(key)
        if isinstance(candidate, Mapping):
            nested = _normalize_str(
                candidate.get("registrationNumber")
                or candidate.get("registration")
                or candidate.get("tail")
            )
            if nested:
                return nested
        value = _normalize_str(candidate)
        if value:
            return value
    return None


def _extract_leg_id(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("leg_id", "legId", "id", "uuid", "externalId", "external_id"):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _extract_airport(row: Mapping[str, Any], departure: bool) -> Optional[str]:
    keys = (
        "departure_airport",
        "dep_airport",
        "departureAirport",
        "departure",
        "airportFrom",
        "fromAirport",
    ) if departure else (
        "arrival_airport",
        "arr_airport",
        "arrivalAirport",
        "arrival",
        "airportTo",
        "toAirport",
    )
    for key in keys:
        value = row.get(key)
        if isinstance(value, Mapping):
            nested = _normalize_str(
                value.get("icao")
                or value.get("iata")
                or value.get("code")
                or value.get("name")
            )
            if nested:
                return nested
        text = _normalize_str(value)
        if text:
            return text
    return None


def _extract_flight_type(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("flightType", "flight_type", "flighttype", "type"):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _is_app_line_placeholder(row: Mapping[str, Any]) -> bool:
    tail = _extract_tail(row)
    if not tail:
        return False
    upper_tail = tail.upper()
    return any(upper_tail.startswith(prefix) for prefix in _APP_LINE_PREFIXES)


def _is_empty_leg(row: Mapping[str, Any]) -> bool:
    flight_type = _extract_flight_type(row)
    return flight_type is not None and flight_type.upper() == "POS"


__all__ = [
    "MorningReportError",
    "MorningReportResult",
    "MorningReportRun",
    "run_morning_reports",
]

