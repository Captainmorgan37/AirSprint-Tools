"""Business logic for executing the Operations Lead morning reports."""

from __future__ import annotations

import csv
import inspect
import re
from collections.abc import Iterable as IterableABC
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    MOUNTAIN_TIME_ZONE,
    compute_fetch_dates,
    fetch_airport_services,
    fetch_flights,
    fetch_flight_notification,
    fetch_flight_services,
    fetch_leg_details,
    fetch_postflight,
)
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
        if not self.has_matches:
            lines = ["No Results Found"]
            return "\n".join(lines)

        if self.code == "16.1.10":
            return _format_upgraded_flights_block(self)
        if self.code == "16.1.6":
            return _format_cj3_on_cj2_block(self)
        if self.code == "16.1.7":
            return _format_priority_status_block(self)
        if self.code == "16.1.12":
            return _format_hub_duty_start_block(self)
        if self.code == "16.1.11":
            return _format_fbo_disconnect_block(self)

        lines = ["Results Found:", self.header_label]
        lines.extend(row.get("line", "") for row in self.rows)
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
    normalized_rows: List[Mapping[str, Any]] = field(default_factory=list)

    def report_map(self) -> Dict[str, MorningReportResult]:
        return {report.code: report for report in self.reports}


def _format_upgraded_flights_block(report: MorningReportResult) -> str:
    return _render_preferred_block(
        report.rows,
        header="UPGRADES",
        line_builder=_build_upgrade_line,
    )


def _format_cj3_on_cj2_block(report: MorningReportResult) -> str:
    block = _render_preferred_block(
        report.rows,
        header="CJ3 CLIENTS ON CJ2",
        line_builder=_build_cj3_line,
    )

    confirmation_note = _normalize_str(
        (report.metadata or {}).get("runway_confirmation_note")
    )

    def _is_confirmation_line(text: Optional[str]) -> bool:
        normalized = _normalize_str(text)
        if not normalized:
            return False
        if confirmation_note and normalized == confirmation_note:
            return True
        return normalized.lower().startswith("all runways confirmed as ")

    filtered_lines: List[str] = []
    for line in block.split("\n"):
        if _is_confirmation_line(line):
            if filtered_lines and filtered_lines[-1] == "":
                filtered_lines.pop()
            continue
        filtered_lines.append(line)

    while filtered_lines and filtered_lines[-1] == "":
        filtered_lines.pop()

    return "\n".join(filtered_lines)


def _format_priority_status_block(report: MorningReportResult) -> str:
    return _render_preferred_block(
        report.rows,
        header="PRIORITY CLIENTS",
        line_builder=_build_priority_line,
    )


def _format_hub_duty_start_block(report: MorningReportResult) -> str:
    return _render_preferred_block(
        report.rows,
        header="CYYZ/CYUL DUTY STARTS",
        line_builder=_build_hub_duty_start_line,
    )


def _format_fbo_disconnect_block(report: MorningReportResult) -> str:
    same_airport_rows: List[Mapping[str, Any]] = []
    handler_missing_rows: List[Mapping[str, Any]] = []

    for row in report.rows:
        scenario = _normalize_str(row.get("listing_scenario"))
        if scenario == "same_airport":
            same_airport_rows.append(row)
        else:
            handler_missing_rows.append(row)

    lines: List[str] = ["Results Found:", report.header_label]

    def _append_section(title: str, rows: List[Mapping[str, Any]]):
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(title)
        if rows:
            lines.extend(entry.get("line", "") for entry in rows)
        else:
            lines.append("No matches")

    if same_airport_rows:
        _append_section(
            "Scenario 2: handlers confirmed at the same airport", same_airport_rows
        )

    _append_section(
        "Scenario 1: at least one handler missing from the airport listing",
        handler_missing_rows,
    )

    return "\n".join(lines)


def _render_preferred_block(
    rows: Iterable[Mapping[str, Any]],
    *,
    header: str,
    line_builder: Callable[[Mapping[str, Any]], str],
) -> str:
    def _format_copyable_header(text: str) -> str:
        normalized = _normalize_str(text) or ""
        suffix = ":" if not normalized.endswith(":") else ""
        return f"{normalized}{suffix}"

    def _format_copyable_label(label: str) -> str:
        normalized = _normalize_str(label) or ""
        return normalized

    grouped_rows = _group_rows_by_display_date(rows)
    lines: List[str] = [_format_copyable_header(header)]

    if not grouped_rows:
        lines.append("")
        lines.append("No Results Found")
        return "\n".join(lines)

    lines.append("")
    for label, group in grouped_rows:
        formatted_label = _format_copyable_label(label)
        lines.append(formatted_label or label)
        lines.append("")
        for row in group:
            lines.append(line_builder(row))
        lines.append("")

    while lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines)


def _group_rows_by_display_date(
    rows: Iterable[Mapping[str, Any]]
) -> List[Tuple[str, List[Mapping[str, Any]]]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    ordered_labels: List[str] = []

    for row in rows:
        raw_date = row.get("date")
        parsed_date = _coerce_row_date(raw_date)
        label = _format_display_date_label(parsed_date, raw_date)

        if label not in grouped:
            grouped[label] = {
                "rows": [],
                "sort_key": parsed_date,
                "position": len(ordered_labels),
            }
            ordered_labels.append(label)

        entry = grouped[label]
        entry["rows"].append(row)
        if parsed_date is not None:
            current = entry.get("sort_key")
            if current is None or parsed_date < current:
                entry["sort_key"] = parsed_date

    def _sort_tuple(item: Tuple[str, Dict[str, Any]]):
        label, payload = item
        sort_key = payload.get("sort_key")
        position = payload.get("position", 0)
        return (sort_key or date.max, position, label)

    ordered = sorted(grouped.items(), key=_sort_tuple)
    return [(label, payload["rows"]) for label, payload in ordered]


def _mountain_date_from_datetime(dt: datetime) -> date:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    mountain_dt = dt.astimezone(MOUNTAIN_TIME_ZONE)
    return mountain_dt.date()


def _coerce_row_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return _mountain_date_from_datetime(value)
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            parsed = safe_parse_dt(text)
            if isinstance(parsed, datetime):
                return _mountain_date_from_datetime(parsed)
    return None


def _format_display_date_label(parsed: Optional[date], raw: Any) -> str:
    if parsed is not None:
        return parsed.strftime("%d%b%y").upper()
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if isinstance(raw, date):
        return raw.strftime("%d%b%y").upper()
    return "Unknown Date"


def _format_mountain_date_component(
    dep_dt: Optional[datetime],
    fallback: Any = None,
) -> Optional[str]:
    if dep_dt is not None:
        return _mountain_date_from_datetime(dep_dt).isoformat()
    if fallback is None:
        return None
    parsed_fallback = _coerce_row_date(fallback)
    if parsed_fallback is not None:
        return parsed_fallback.isoformat()
    if isinstance(fallback, str):
        text = fallback.strip()
        return text or None
    if isinstance(fallback, date):
        return fallback.isoformat()
    return None


def _build_upgrade_line(row: Mapping[str, Any]) -> str:
    tail_value = row.get("tail") or "Unknown Tail"
    tail = str(tail_value).strip() or "Unknown Tail"
    booking = (
        row.get("booking_reference")
        or row.get("booking_identifier")
        or row.get("quote_id")
        or "Unknown Booking"
    )
    account_value = row.get("account_name") or "Unknown Account"
    booking_display = str(booking).strip() or "Unknown Booking"
    account = str(account_value).strip() or "Unknown Account"
    return " - ".join([tail, booking_display, account])


def _build_cj3_line(row: Mapping[str, Any]) -> str:
    tail_value = row.get("tail") or "Unknown Tail"
    tail = str(tail_value).strip() or "Unknown Tail"
    booking = (
        row.get("booking_identifier")
        or row.get("flight_identifier")
        or row.get("booking_reference")
        or "Unknown Booking"
    )
    account_value = row.get("account_name") or "Unknown Account"
    pax_count = row.get("pax_count")
    pax_display = f"{pax_count} PAX" if pax_count is not None else "Unknown PAX"
    block_display = row.get("block_time_display")
    if not block_display:
        block_minutes = row.get("block_time_minutes")
        block_display = _format_block_minutes(block_minutes)
    block_display = (
        f"{block_display} FLIGHT TIME" if block_display and block_display != "Unknown" else "Unknown FLIGHT TIME"
    )
    base_line = " - ".join(
        [
            tail,
            str(booking).strip() or "Unknown Booking",
            str(account_value).strip() or "Unknown Account",
            pax_display,
            block_display,
        ]
    )

    threshold_ft = row.get("runway_alert_threshold_ft")
    runway_alerts = row.get("runway_alerts") or []
    if not runway_alerts:
        return base_line

    alert_lines: List[str] = []

    for alert in runway_alerts:
        role = _normalize_str(alert.get("role")) or "Airport"
        airport_display = (
            _normalize_str(alert.get("airport"))
            or _normalize_str(alert.get("airport_raw"))
            or "Unknown Airport"
        )
        length_value = alert.get("max_runway_length_ft")
        if isinstance(length_value, (int, float)):
            length_int = int(length_value)
            length_display = f"{length_int:,} FT"
        else:
            length_display = "Unknown length"

        if isinstance(threshold_ft, (int, float)):
            threshold_int = int(threshold_ft)
            alert_lines.append(
                f"    ALERT: {role} {airport_display} max runway {length_display} (< {threshold_int:,} FT)"
            )
        else:
            alert_lines.append(
                f"    ALERT: {role} {airport_display} max runway {length_display}"
            )

    return "\n".join([base_line, *alert_lines])


def _build_runway_confirmation_note(
    rows: Iterable[Mapping[str, Any]]
) -> Optional[str]:
    has_rows = False
    threshold_value: Optional[int] = None

    for row in rows:
        has_rows = True
        runway_alerts = row.get("runway_alerts") or []
        if runway_alerts:
            return None

        threshold_ft = row.get("runway_alert_threshold_ft")
        if not isinstance(threshold_ft, (int, float)):
            return None

        threshold_int = int(threshold_ft)
        if threshold_value is None:
            threshold_value = threshold_int
        elif threshold_value != threshold_int:
            threshold_value = max(threshold_value, threshold_int)

    if not has_rows or threshold_value is None:
        return None

    return f"All runways confirmed as {threshold_value:,}' or longer"


def _normalize_airport_ident(value: Any) -> Optional[str]:
    text = _normalize_str(value)
    if not text:
        return None
    text = text.upper()
    for token in re.split(r"[^A-Z0-9]+", text):
        if len(token) >= 3:
            return token
    return None


def _load_runway_length_cache() -> Dict[str, int]:
    global _RUNWAY_LENGTH_CACHE
    if _RUNWAY_LENGTH_CACHE is not None:
        return _RUNWAY_LENGTH_CACHE

    cache: Dict[str, int] = {}
    try:
        with _RUNWAY_DATA_PATH.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ident = _normalize_airport_ident(row.get("airport_ident"))
                if not ident:
                    continue

                length_text = _normalize_str(row.get("length_ft"))
                if not length_text:
                    continue
                try:
                    length_value = int(float(length_text))
                except (TypeError, ValueError):
                    continue
                if length_value <= 0:
                    continue

                current = cache.get(ident)
                if current is None or length_value > current:
                    cache[ident] = length_value

                if len(ident) == 4 and ident[0] in {"C", "K"}:
                    alias = ident[1:]
                    if len(alias) >= 3:
                        alias_current = cache.get(alias)
                        if alias_current is None or length_value > alias_current:
                            cache[alias] = length_value
    except FileNotFoundError:
        cache = {}

    _RUNWAY_LENGTH_CACHE = cache
    return cache


def _lookup_max_runway_length(airport: Optional[str]) -> Optional[int]:
    ident = _normalize_airport_ident(airport)
    if not ident:
        return None
    cache = _load_runway_length_cache()
    length = cache.get(ident)
    return int(length) if length is not None else None


def _build_runway_alerts(
    departure_airport: Optional[str],
    arrival_airport: Optional[str],
    *,
    lookup_fn: Callable[[Optional[str]], Optional[int]],
    threshold_ft: int,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    for role, airport in (("Departure", departure_airport), ("Arrival", arrival_airport)):
        normalized = _normalize_airport_ident(airport)
        length = lookup_fn(airport)
        if length is None:
            continue
        try:
            length_int = int(length)
        except (TypeError, ValueError):
            continue
        if length_int >= threshold_ft:
            continue
        alerts.append(
            {
                "role": role,
                "airport": normalized or (_normalize_str(airport) or "Unknown"),
                "airport_raw": airport,
                "max_runway_length_ft": length_int,
            }
        )
    return alerts


def _build_priority_line(row: Mapping[str, Any]) -> str:
    tail_value = row.get("tail") or "Unknown Tail"
    tail = str(tail_value).strip() or "Unknown Tail"
    booking_value = (
        row.get("booking_reference") or row.get("booking_identifier") or "Unknown Booking"
    )
    account_value = row.get("account_name") or "Unknown Account"
    has_issue = bool(row.get("has_issue"))
    status_label = "NOT ACCOMMODATED" if has_issue else "ACCOMMODATED"
    status_detail = row.get("status")
    if has_issue and status_detail:
        status_display = f"{status_label} - {status_detail}"
    else:
        status_display = status_label
    return " - ".join(
        [
            tail,
            str(booking_value).strip() or "Unknown Booking",
            str(account_value).strip() or "Unknown Account",
            status_display,
        ]
    )


def _build_hub_duty_start_line(row: Mapping[str, Any]) -> str:
    tail_value = row.get("tail") or "Unknown Tail"
    tail = str(tail_value).strip() or "Unknown Tail"
    departure_airport = row.get("departure_airport") or "Unknown Departure"
    has_issue = bool(row.get("has_issue"))
    status_label = "NOT ACCOMMODATED" if has_issue else "ACCOMMODATED"
    status_detail = row.get("status")
    status_display = (
        f"{status_label} - {status_detail}" if has_issue and status_detail else status_label
    )
    return " - ".join(
        [
            tail,
            str(departure_airport).strip() or "Unknown Departure",
            status_display,
        ]
    )
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
_OCS_ACCOUNT_NAME = "AIRSPRINT INC."

_LEGACY_AIRCRAFT_CATEGORIES = {"E550", "E545"}
_LEGACY_AIRCRAFT_TOKENS = ("E550", "E545", "LEGACY", "PRAETOR", "EMB", "EMBRAER")

_RUNWAY_ALERT_THRESHOLD_FT = 4900

_RUNWAY_DATA_PATH = Path(__file__).with_name("runways.csv")
_RUNWAY_LENGTH_CACHE: Optional[Dict[str, int]] = None

_PRIORITY_CHECKIN_THRESHOLD_MINUTES = 90
_PRIORITY_DUTY_REST_THRESHOLD_MINUTES = 9 * 60

_PLACEHOLDER_TAILS = {
    "ADD CJ2+ EAST",
    "ADD CJ2+ WEST",
    "ADD CJ3+ EAST",
    "ADD CJ3+ WEST",
    "ADD EMB EAST",
    "ADD EMB WEST",
    "ADD LINE",
    "REMOVE LINE",
    "REMOVE OCS",
    "REMOVE OCS 2",
}


@dataclass
class _DutyState:
    duty_id: int
    earliest_departure: datetime
    last_completion_dt: Optional[datetime] = None
    last_arrival_dt: Optional[datetime] = None
    last_arrival_row: Optional[Mapping[str, Any]] = None


@dataclass
class _DutyAssignment:
    tail_key: str
    duty_id: int
    earliest_departure: datetime
    previous_arrival_dt: Optional[datetime]
    previous_arrival_row: Optional[Mapping[str, Any]]


def _calculate_duty_assignments(
    sorted_rows: Sequence[Mapping[str, Any]] | List[Mapping[str, Any]]
) -> List[Optional[_DutyAssignment]]:
    duty_states: Dict[str, _DutyState] = {}
    duty_assignments: List[Optional[_DutyAssignment]] = [None] * len(sorted_rows)

    for index, row in enumerate(sorted_rows):
        tail = _extract_tail(row)
        dep_dt = _extract_departure_dt(row)
        arr_dt = _extract_arrival_dt(row)

        if not tail or dep_dt is None:
            if tail and arr_dt is not None:
                tail_key = tail.upper() if isinstance(tail, str) else str(tail).upper()
                state = duty_states.get(tail_key)
                if state is not None:
                    state.last_completion_dt = arr_dt
                    state.last_arrival_dt = arr_dt
                    state.last_arrival_row = row
            continue

        tail_key = tail.upper() if isinstance(tail, str) else str(tail).upper()
        state = duty_states.get(tail_key)
        previous_arrival_dt = state.last_arrival_dt if state else None
        previous_arrival_row = state.last_arrival_row if state else None
        rest_minutes: Optional[float] = None
        if state and state.last_completion_dt is not None:
            rest_minutes = (dep_dt - state.last_completion_dt).total_seconds() / 60.0

        new_duty = state is None
        if not new_duty:
            if rest_minutes is None:
                new_duty = False
            elif rest_minutes < 0:
                new_duty = True
            elif rest_minutes >= _PRIORITY_DUTY_REST_THRESHOLD_MINUTES:
                new_duty = True
            else:
                new_duty = False

        if new_duty:
            duty_id = 0 if state is None else state.duty_id + 1
            state = _DutyState(duty_id=duty_id, earliest_departure=dep_dt)
            duty_states[tail_key] = state
            previous_arrival_dt = None
            previous_arrival_row = None
        else:
            duty_id = state.duty_id
            if dep_dt < state.earliest_departure:
                state.earliest_departure = dep_dt

        duty_assignments[index] = _DutyAssignment(
            tail_key=tail_key,
            duty_id=duty_id,
            earliest_departure=state.earliest_departure,
            previous_arrival_dt=previous_arrival_dt,
            previous_arrival_row=previous_arrival_row,
        )

        completion_dt = arr_dt or dep_dt
        state.last_completion_dt = completion_dt
        if arr_dt is not None:
            state.last_arrival_dt = arr_dt
            state.last_arrival_row = row

    return duty_assignments


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
        _build_ocs_pax_report(normalized_rows, config),
        _build_owner_continuous_flight_validation_report(normalized_rows),
        _build_cj3_owners_on_cj2_report(normalized_rows, config),
        _build_priority_status_report(normalized_rows, config),
        _build_hub_duty_start_report(normalized_rows, config),
        _build_upgrade_workflow_validation_report(normalized_rows, config),
        _build_upgrade_flights_report(normalized_rows, config),
        _build_fbo_disconnect_report(normalized_rows, config),
    ]

    metadata["report_codes"] = [report.code for report in reports]

    return MorningReportRun(
        fetched_at=fetched_at,
        from_date=from_date,
        to_date=to_date,
        reports=reports,
        leg_count=len(normalized_rows),
        metadata=metadata,
        normalization_stats=normalization_stats,
        normalized_rows=normalized_rows,
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


def _build_ocs_pax_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
) -> MorningReportResult:
    matches = [row for row in rows if _is_ocs_pax_leg(row)]
    formatted_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    notification_cache: Dict[str, Optional[Any]] = {}
    session: Optional[requests.Session] = None

    try:
        for row in _sort_rows(matches):
            formatted = _format_report_row(row)
            pax_count = _extract_pax_count(row)
            flight_identifier = _extract_flight_identifier(row, formatted)
            note_text: Optional[str] = None

            if flight_identifier:
                if session is None:
                    session = requests.Session()
                if flight_identifier not in notification_cache:
                    try:
                        payload = fetch_flight_notification(
                            config, flight_identifier, session=session
                        )
                    except Exception as exc:  # pragma: no cover - defensive path
                        warnings.append(
                            "Failed to fetch OCS notification for "
                            f"flight {flight_identifier}: {exc}"
                        )
                        notification_cache[flight_identifier] = None
                    else:
                        notification_cache[flight_identifier] = payload

                payload = notification_cache.get(flight_identifier)
                if payload is not None:
                    note_text = _extract_notification_note(payload)
            else:
                warnings.append(
                    "Skipping notification fetch for leg "
                    f"{formatted.get('leg_id') or formatted.get('line')} due to missing flight identifier"
                )

            display_note = _format_notification_text(note_text)
            pax_display = str(pax_count) if pax_count is not None else "Unknown Pax"

            base_parts = [
                formatted.get("date") or "Unknown Date",
                formatted.get("booking_reference")
                or formatted.get("bookingIdentifier")
                or formatted.get("leg_id")
                or "Unknown Flight",
                formatted.get("account_name") or "Unknown Account",
            ]
            line = "-".join(base_parts + [pax_display, display_note])

            formatted.update(
                {
                    "line": line,
                    "pax_count": pax_count,
                    "ocs_note": display_note,
                    "ocs_note_raw": note_text,
                    "flight_id": flight_identifier,
                }
            )
            formatted_rows.append(formatted)
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    return MorningReportResult(
        code="16.1.4",
        title="OCS Pax Flights Report",
        header_label="OCS Pax Flights",
        rows=formatted_rows,
        warnings=warnings,
        metadata={
            "match_count": len(formatted_rows),
            "notification_requests": len(notification_cache),
        },
    )


def _build_owner_continuous_flight_validation_report(
    rows: Iterable[Mapping[str, Any]]
) -> MorningReportResult:
    relevant_rows: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        flight_type = _extract_flight_type(row)
        if flight_type is None or flight_type.upper() != "PAX":
            continue

        if _is_ocs_pax_leg(row):
            continue

        if _is_app_line_placeholder(row):
            continue

        account_name = _extract_account_name(row)
        if not account_name:
            continue

        tail = _extract_tail(row)
        if not tail:
            continue

        dep_dt = _extract_departure_dt(row)
        arr_dt = _extract_arrival_dt(row)
        if dep_dt is None or arr_dt is None:
            continue

        record = {
            "account_name": account_name,
            "tail": tail,
            "tail_upper": tail.upper(),
            "departure_time": dep_dt,
            "arrival_time": arr_dt,
            "leg_id": _extract_leg_id(row),
            "departure_airport": _extract_airport(row, True),
            "arrival_airport": _extract_airport(row, False),
        }

        relevant_rows.setdefault(account_name, []).append(record)

    discrepancies: List[Dict[str, Any]] = []

    for account, legs in sorted(relevant_rows.items(), key=lambda item: item[0].upper()):
        ordered = sorted(legs, key=lambda entry: entry["departure_time"])

        for current, nxt in zip(ordered, ordered[1:]):
            if current["tail_upper"] == nxt["tail_upper"]:
                continue

            arr_dt = current["arrival_time"]
            next_dep_dt = nxt["departure_time"]

            if arr_dt is None or next_dep_dt is None:
                continue

            gap = next_dep_dt - arr_dt
            if gap.total_seconds() < 0:
                continue

            if gap < timedelta(hours=3):
                gap_minutes = int(gap.total_seconds() // 60)
                line = (
                    f"{account} | {current['tail']} → {nxt['tail']} | "
                    f"Arr {arr_dt.isoformat()} → Dep {next_dep_dt.isoformat()} | "
                    f"Gap {gap_minutes} min"
                )
                discrepancies.append(
                    {
                        "line": line,
                        "account_name": account,
                        "previous_tail": current["tail"],
                        "next_tail": nxt["tail"],
                        "previous_leg_id": current.get("leg_id"),
                        "next_leg_id": nxt.get("leg_id"),
                        "previous_arrival_time": arr_dt.isoformat(),
                        "next_departure_time": next_dep_dt.isoformat(),
                        "gap_minutes": gap_minutes,
                        "previous_arrival_airport": current.get("arrival_airport"),
                        "next_departure_airport": nxt.get("departure_airport"),
                    }
                )

    discrepancies.sort(key=lambda row: (row["account_name"].upper(), row["next_departure_time"]))

    return MorningReportResult(
        code="16.1.5",
        title="Owner Continuous Flight Validation",
        header_label="Owner Continuous Flight Validation",
        rows=discrepancies,
        metadata={
            "match_count": len(discrepancies),
            "flagged_accounts": sorted({row["account_name"] for row in discrepancies}),
        },
    )


def _build_cj3_owners_on_cj2_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_leg_details_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_leg_details,
    runway_lookup_fn: Optional[Callable[[Optional[str]], Optional[int]]] = None,
    runway_threshold_ft: int = _RUNWAY_ALERT_THRESHOLD_FT,
) -> MorningReportResult:
    matches: List[Dict[str, Any]] = []
    warnings: List[str] = []
    detail_cache: Dict[str, Optional[Any]] = {}
    session: Optional[requests.Session] = None

    total_flagged = 0
    inspected = 0

    lookup_fn = runway_lookup_fn or _lookup_max_runway_length

    try:
        for row in _sort_rows(rows):
            flight_type = _extract_flight_type(row)
            if flight_type is None or flight_type.upper() != "PAX":
                continue

            if _is_ocs_pax_leg(row):
                continue

            tail = _extract_tail(row)
            if not tail:
                continue
            upper_tail = tail.upper()
            if upper_tail.startswith("ADD") or upper_tail.startswith("REMOVE"):
                continue
            if _is_app_line_placeholder(row):
                continue

            aircraft_category = _extract_aircraft_category(row)
            if not aircraft_category or aircraft_category.upper() != "C25A":
                continue

            quote_id = _extract_quote_identifier(row)
            if not quote_id:
                warnings.append(
                    "Skipping CJ3-on-CJ2 check for leg "
                    f"{_extract_leg_id(row) or 'unknown'} due to missing quote identifier"
                )
                continue

            account_name = _extract_account_name(row)
            if not account_name:
                continue

            inspected += 1

            if session is None:
                session = requests.Session()

            if quote_id not in detail_cache:
                try:
                    payload = fetch_leg_details_fn(config, quote_id, session=session)
                except Exception as exc:  # pragma: no cover - defensive path
                    warnings.append(
                        f"Failed to fetch leg details for quote {quote_id}: {exc}"
                    )
                    detail_cache[quote_id] = None
                else:
                    detail_cache[quote_id] = payload

            payload = detail_cache.get(quote_id)
            detail = _select_leg_detail(payload, row)
            note_text = _extract_planning_note(detail)

            if not note_text:
                continue

            if not _planning_note_requests_non_cj2(note_text):
                continue

            total_flagged += 1

            pax_count = _extract_detail_pax(detail, row)
            block_minutes = _extract_block_minutes(detail, row)

            violation = False
            violation_reasons: List[str] = []
            if pax_count is None:
                violation = True
                warnings.append(
                    f"Missing passenger count for quote {quote_id}; flagging for review"
                )
                violation_reasons.append("Missing passenger count")
            elif pax_count >= 5:
                violation = True
                violation_reasons.append("Passenger count at or above limit")

            if block_minutes is None:
                violation = True
                warnings.append(
                    f"Missing block time for quote {quote_id}; flagging for review"
                )
                violation_reasons.append("Missing block time")
            elif block_minutes >= 180:
                violation = True
                violation_reasons.append("Block time at or above limit")

            threshold_status = (
                "Threshold exceeded" if violation else "Within thresholds"
            )

            dep_dt = _extract_departure_dt(row)
            if dep_dt is None:
                dep_dt = _extract_detail_departure_dt(detail)

            date_component = _format_mountain_date_component(dep_dt) or "Unknown Date"

            formatted_stub = {"leg_id": _extract_leg_id(row)}
            booking_identifier = _extract_booking_reference(row)
            if not booking_identifier and detail:
                booking_identifier = _extract_booking_reference(detail)

            flight_identifier = _extract_flight_identifier(row, formatted_stub)
            display_identifier = (
                booking_identifier
                or flight_identifier
                or formatted_stub["leg_id"]
            )

            pax_display = str(pax_count) if pax_count is not None else "Unknown"
            block_display = _format_block_minutes(block_minutes)

            dep_airport_raw = _extract_airport(row, True)
            arr_airport_raw = _extract_airport(row, False)
            if not dep_airport_raw and detail:
                dep_airport_raw = _extract_airport(detail, True)
            if not arr_airport_raw and detail:
                arr_airport_raw = _extract_airport(detail, False)

            dep_airport = _normalize_airport_ident(dep_airport_raw)
            arr_airport = _normalize_airport_ident(arr_airport_raw)

            runway_alerts = _build_runway_alerts(
                dep_airport_raw,
                arr_airport_raw,
                lookup_fn=lookup_fn,
                threshold_ft=runway_threshold_ft,
            )

            line = "-".join(
                [
                    date_component,
                    tail,
                    display_identifier or "Unknown Flight",
                    account_name,
                    pax_display,
                    block_display,
                    threshold_status,
                ]
            )

            matches.append(
                {
                    "line": line,
                    "date": date_component,
                    "tail": tail,
                    "flight_identifier": flight_identifier,
                    "booking_identifier": booking_identifier,
                    "account_name": account_name,
                    "pax_count": pax_count,
                    "block_time_minutes": block_minutes,
                    "block_time_display": block_display,
                    "planning_note": note_text,
                    "quote_id": quote_id,
                    "threshold_status": threshold_status,
                    "threshold_breached": violation,
                    "threshold_reasons": violation_reasons,
                    "departure_airport": dep_airport,
                    "departure_airport_raw": dep_airport_raw,
                    "arrival_airport": arr_airport,
                    "arrival_airport_raw": arr_airport_raw,
                    "runway_alerts": runway_alerts,
                    "runway_alert_threshold_ft": runway_threshold_ft,
                }
            )
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    metadata: Dict[str, Any] = {
        "match_count": len(matches),
        "flagged_candidates": total_flagged,
        "inspected_legs": inspected,
    }

    confirmation_note = _build_runway_confirmation_note(matches)
    if confirmation_note:
        metadata["runway_confirmation_note"] = confirmation_note

    return MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=matches,
        warnings=warnings,
        metadata=metadata,
    )


def _build_priority_status_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_postflight_fn: Callable[..., Any] = fetch_postflight,
    threshold_minutes: int = _PRIORITY_CHECKIN_THRESHOLD_MINUTES,
) -> MorningReportResult:
    sorted_rows = _sort_rows(rows)
    duty_assignments = _calculate_duty_assignments(sorted_rows)

    priority_candidates: List[Dict[str, Any]] = []
    for index, row in enumerate(sorted_rows):
        formatted = _format_report_row(row, include_tail=True)
        tail = formatted.get("tail")
        dep_dt = _extract_departure_dt(row)
        dep_date = _mountain_date_from_datetime(dep_dt) if dep_dt else None
        assignment = duty_assignments[index]

        tail_key: Optional[str] = None
        previous_arrival_dt: Optional[datetime] = None
        previous_arrival_row: Optional[Mapping[str, Any]] = None
        is_first_departure = False

        if assignment is not None:
            tail_key = assignment.tail_key
            previous_arrival_dt = assignment.previous_arrival_dt
            previous_arrival_row = assignment.previous_arrival_row
            if dep_dt is not None:
                earliest = assignment.earliest_departure
                delta = abs((dep_dt - earliest).total_seconds())
                if delta < 1.0:
                    is_first_departure = True
        else:
            tail_key = tail.upper() if tail else None

        is_priority, priority_label = _row_priority_info(row)
        if is_priority:
            priority_candidates.append(
                {
                    "row": row,
                    "formatted": formatted,
                    "tail": tail,
                    "dep_dt": dep_dt,
                    "dep_date": dep_date,
                    "priority_label": priority_label,
                    "is_first_departure": is_first_departure,
                    "previous_arrival_dt": previous_arrival_dt,
                    "previous_arrival_row": previous_arrival_row,
                }
            )

    if not priority_candidates:
        return MorningReportResult(
            code="16.1.7",
            title="Priority Status Report",
            header_label="Priority Duty-Start Validation",
            rows=[],
            metadata={
                "total_priority_flights": 0,
                "validation_required": 0,
                "validated_without_issue": 0,
                "issues_found": 0,
                "threshold_minutes": threshold_minutes,
            },
        )

    credentials_available = bool(config.api_token or config.auth_header)
    warning_messages: List[str] = []
    if not credentials_available:
        warning_messages.append(
            "FL3XX credentials are missing; cannot retrieve crew check-in timestamps for priority departures."
        )

    matches: List[Dict[str, Any]] = []
    issues_found = 0
    validation_required = 0
    validated_without_issue = 0

    shared_session: Optional[requests.Session] = None
    postflight_cache: Dict[Any, Any] = {}
    postflight_request_counts = {"unique": 0, "cached": 0}

    try:
        signature = inspect.signature(fetch_postflight_fn)
    except (TypeError, ValueError):
        accepts_session_kw = True
    else:
        accepts_session_kw = any(
            param.kind is inspect.Parameter.VAR_KEYWORD or name == "session"
            for name, param in signature.parameters.items()
        )

    try:
        for candidate in priority_candidates:
            formatted = candidate["formatted"]
            tail = candidate["tail"]
            dep_dt: Optional[datetime] = candidate["dep_dt"]
            dep_date = candidate["dep_date"]
            is_first_departure = candidate["is_first_departure"]
            previous_arrival_dt: Optional[datetime] = candidate.get("previous_arrival_dt")
            previous_arrival_row = candidate.get("previous_arrival_row")
            label = candidate["priority_label"] or formatted.get("workflow")
            if not label or "priority" not in label.lower():
                label = "Priority"

            status = ""
            issue = False
            minutes_before: Optional[float] = None
            turn_minutes: Optional[float] = None
            earliest_checkin: Optional[datetime] = None
            latest_checkin: Optional[datetime] = None
            checkin_count: Optional[int] = None
            checkin_times_display: Optional[str] = None

            current_booking = formatted.get("booking_reference")
            previous_booking = (
                _extract_booking_reference(previous_arrival_row)
                if previous_arrival_row
                else None
            )
            previous_was_priority = False
            if previous_arrival_row is not None:
                previous_was_priority, _ = _row_priority_info(previous_arrival_row)

            skip_turn_validation = (
                not is_first_departure
                and previous_was_priority
                and bool(current_booking)
                and current_booking == previous_booking
            )

            flight_identifier = _extract_flight_identifier(candidate["row"], formatted)
            needs_validation = bool(tail and dep_dt) and not skip_turn_validation
            if needs_validation:
                validation_required += 1

            if dep_dt is None:
                status = "Missing departure time; cannot validate check-in window"
                issue = True
            elif not tail:
                status = "Missing tail number; cannot validate check-in window"
                issue = True
            elif skip_turn_validation:
                # Continuation legs that share the same booking with a prior priority
                # segment should be omitted from the duty-start validation report.
                # These legs do not require turn validation and can cause noise if
                # they appear in the output, so we simply exclude them while still
                # counting them toward the overall priority totals.
                continue
            elif not is_first_departure:
                if previous_arrival_dt is None:
                    status = "Missing previous arrival time; cannot validate turn interval"
                    issue = True
                else:
                    turn_minutes = (dep_dt - previous_arrival_dt).total_seconds() / 60.0
                    if turn_minutes < 0:
                        status = (
                            "Inconsistent timing data; previous arrival occurs after departure"
                        )
                        issue = True
                    elif turn_minutes >= threshold_minutes:
                        status = (
                            f"Turn time meets threshold ({turn_minutes:.1f} min gap before departure)"
                        )
                        if needs_validation:
                            validated_without_issue += 1
                    else:
                        status = (
                            f"Turn time only {turn_minutes:.1f} min before departure "
                            f"(requires {threshold_minutes} min)"
                        )
                        issue = True
            elif not credentials_available:
                status = "Missing FL3XX credentials; cannot retrieve check-in timestamps"
                issue = True
            elif not flight_identifier:
                status = "Missing flight identifier; cannot retrieve check-in timestamps"
                issue = True
            else:
                try:
                    if flight_identifier in postflight_cache:
                        payload = postflight_cache[flight_identifier]
                        postflight_request_counts["cached"] += 1
                    else:
                        call_kwargs = {}
                        if accepts_session_kw:
                            if shared_session is None:
                                shared_session = requests.Session()
                            call_kwargs["session"] = shared_session
                        payload = fetch_postflight_fn(
                            config, flight_identifier, **call_kwargs
                        )
                        postflight_cache[flight_identifier] = payload
                        postflight_request_counts["unique"] += 1
                except Exception as exc:  # pragma: no cover - defensive path
                    status = f"Unable to retrieve check-in data ({exc})"
                    warning_messages.append(
                        f"{tail or 'Unknown tail'} on {dep_date or 'unknown date'}: {exc}"
                    )
                    issue = True
                else:
                    values = _extract_checkin_values(payload)
                    target_tz = dep_dt.tzinfo or timezone.utc
                    checkins = [
                        dt
                        for value in values
                        if (dt := _checkin_to_datetime(value, target_tz)) is not None
                    ]
                    if not checkins:
                        status = "No crew check-in timestamps available"
                        issue = True
                    else:
                        checkins.sort()
                        earliest_checkin = checkins[0]
                        latest_checkin = checkins[-1]
                        minutes_before = (
                            dep_dt - earliest_checkin
                        ).total_seconds() / 60.0
                        checkin_count = len(checkins)
                        checkin_times_display = ", ".join(
                            dt.strftime("%H:%M") for dt in checkins
                        )
                        if minutes_before >= threshold_minutes:
                            status = (
                                f"Meets threshold ({minutes_before:.1f} min before departure)"
                            )
                            if needs_validation:
                                validated_without_issue += 1
                        else:
                            status = (
                                f"Check-in only {minutes_before:.1f} min before departure "
                                f"(requires {threshold_minutes} min)"
                            )
                            issue = True

            if issue:
                issues_found += 1

            line = f"{formatted['line']} | {label} | {status}"
            previous_leg_id = (
                _extract_leg_id(previous_arrival_row) if previous_arrival_row else None
            )
            previous_leg_departure_dt = (
                _extract_departure_dt(previous_arrival_row)
                if previous_arrival_row
                else None
            )

            row_data = {
                "line": line,
                "date": formatted.get("date"),
                "tail": tail,
                "priority_label": label,
                "status": status,
                "is_first_departure": is_first_departure,
                "needs_validation": needs_validation,
                "has_issue": issue,
                "minutes_before_departure": round(minutes_before, 1)
                if minutes_before is not None
                else None,
                "turn_gap_minutes": round(turn_minutes, 1)
                if turn_minutes is not None
                else None,
                "checkin_count": checkin_count,
                "checkin_times": checkin_times_display,
                "earliest_checkin": earliest_checkin.isoformat()
                if earliest_checkin
                else None,
                "latest_checkin": latest_checkin.isoformat() if latest_checkin else None,
                "previous_arrival_time": previous_arrival_dt.isoformat()
                if previous_arrival_dt
                else None,
                "departure_time": dep_dt.isoformat() if dep_dt else None,
                "booking_reference": formatted.get("booking_reference"),
                "account_name": formatted.get("account_name"),
                "flight_identifier": flight_identifier,
                "leg_id": formatted.get("leg_id"),
                "previous_leg_id": previous_leg_id,
                "previous_leg_departure_time": previous_leg_departure_dt.isoformat()
                if previous_leg_departure_dt
                else None,
                "_sort_key": dep_dt or datetime.max.replace(tzinfo=timezone.utc),
            }
            matches.append(row_data)
    finally:
        if shared_session is not None:
            try:
                shared_session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    matches.sort(key=lambda row: row["_sort_key"])
    for row in matches:
        row.pop("_sort_key", None)

    metadata = {
        "total_priority_flights": len(priority_candidates),
        "validation_required": validation_required,
        "validated_without_issue": validated_without_issue,
        "issues_found": issues_found,
        "threshold_minutes": threshold_minutes,
    }

    if any(postflight_request_counts.values()):
        metadata["postflight_requests"] = postflight_request_counts

    return MorningReportResult(
        code="16.1.7",
        title="Priority Status Report",
        header_label="Priority Duty-Start Validation",
        rows=matches,
        warnings=list(dict.fromkeys(warning_messages)),
        metadata=metadata,
    )


def _build_hub_duty_start_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_postflight_fn: Callable[..., Any] = fetch_postflight,
    threshold_minutes: int = _PRIORITY_CHECKIN_THRESHOLD_MINUTES,
    target_airports: Iterable[str] = ("CYYZ", "CYUL"),
) -> MorningReportResult:
    filtered_rows = [
        row for row in rows if not _is_placeholder_tail(_extract_tail(row))
    ]

    sorted_rows = _sort_rows(filtered_rows)
    duty_assignments = _calculate_duty_assignments(sorted_rows)

    credentials_available = bool(config.api_token or config.auth_header)
    warning_messages: List[str] = []
    matches: List[Dict[str, Any]] = []

    issues_found = 0
    validation_required = 0
    validated_without_issue = 0

    shared_session: Optional[requests.Session] = None
    postflight_cache: Dict[Any, Any] = {}
    postflight_request_counts = {"unique": 0, "cached": 0}

    normalized_airports = {airport.upper() for airport in target_airports}

    try:
        signature = inspect.signature(fetch_postflight_fn)
    except (TypeError, ValueError):
        accepts_session_kw = True
    else:
        accepts_session_kw = any(
            param.kind is inspect.Parameter.VAR_KEYWORD or name == "session"
            for name, param in signature.parameters.items()
        )

    try:
        for index, row in enumerate(sorted_rows):
            formatted = _format_report_row(row, include_tail=True)
            dep_dt = _extract_departure_dt(row)
            dep_airport = _normalize_airport_ident(_extract_airport(row, True))
            tail = formatted.get("tail")
            flight_identifier = _extract_flight_identifier(row, formatted)

            assignment = duty_assignments[index]
            is_first_departure = False

            if assignment is not None and dep_dt is not None:
                earliest = assignment.earliest_departure
                delta = abs((dep_dt - earliest).total_seconds())
                if delta < 1.0:
                    is_first_departure = True

            if not is_first_departure:
                continue

            if dep_airport is None or dep_airport not in normalized_airports:
                continue

            dep_date = _mountain_date_from_datetime(dep_dt) if dep_dt else None

            status = ""
            issue = False
            minutes_before: Optional[float] = None
            earliest_checkin: Optional[datetime] = None
            latest_checkin: Optional[datetime] = None
            checkin_count: Optional[int] = None
            checkin_times_display: Optional[str] = None

            needs_validation = bool(tail and dep_dt)
            if needs_validation:
                validation_required += 1

            if dep_dt is None:
                status = "Missing departure time; cannot validate check-in window"
                issue = True
            elif not tail:
                status = "Missing tail number; cannot validate check-in window"
                issue = True
            elif not credentials_available:
                status = "Missing FL3XX credentials; cannot retrieve check-in timestamps"
                issue = True
            elif not flight_identifier:
                status = "Missing flight identifier; cannot retrieve check-in timestamps"
                issue = True
            else:
                try:
                    if flight_identifier in postflight_cache:
                        payload = postflight_cache[flight_identifier]
                        postflight_request_counts["cached"] += 1
                    else:
                        call_kwargs = {}
                        if accepts_session_kw:
                            if shared_session is None:
                                shared_session = requests.Session()
                            call_kwargs["session"] = shared_session
                        payload = fetch_postflight_fn(config, flight_identifier, **call_kwargs)
                        postflight_cache[flight_identifier] = payload
                        postflight_request_counts["unique"] += 1
                except Exception as exc:  # pragma: no cover - defensive path
                    status = f"Unable to retrieve check-in data ({exc})"
                    warning_messages.append(
                        f"{tail or 'Unknown tail'} on {dep_date or 'unknown date'}: {exc}"
                    )
                    issue = True
                else:
                    values = _extract_checkin_values(payload)
                    target_tz = dep_dt.tzinfo or timezone.utc
                    checkins = [
                        dt
                        for value in values
                        if (dt := _checkin_to_datetime(value, target_tz)) is not None
                    ]
                    if not checkins:
                        status = "No crew check-in timestamps available"
                        issue = True
                    else:
                        checkins.sort()
                        earliest_checkin = checkins[0]
                        latest_checkin = checkins[-1]
                        minutes_before = (dep_dt - earliest_checkin).total_seconds() / 60.0
                        checkin_count = len(checkins)
                        checkin_times_display = ", ".join(
                            dt.strftime("%H:%M") for dt in checkins
                        )
                        if minutes_before >= threshold_minutes:
                            status = (
                                f"Meets threshold ({minutes_before:.1f} min before departure)"
                            )
                            if needs_validation:
                                validated_without_issue += 1
                        else:
                            status = (
                                f"Check-in only {minutes_before:.1f} min before departure "
                                f"(requires {threshold_minutes} min)"
                            )
                            issue = True

            if issue:
                issues_found += 1

            row_data = {
                "line": f"{formatted['line']} | {dep_airport or 'Unknown'} | {status}",
                "date": formatted.get("date"),
                "tail": tail,
                "status": status,
                "has_issue": issue,
                "needs_validation": needs_validation,
                "minutes_before_departure": round(minutes_before, 1)
                if minutes_before is not None
                else None,
                "checkin_count": checkin_count,
                "checkin_times": checkin_times_display,
                "earliest_checkin": earliest_checkin.isoformat()
                if earliest_checkin
                else None,
                "latest_checkin": latest_checkin.isoformat() if latest_checkin else None,
                "departure_airport": dep_airport,
                "departure_time": dep_dt.isoformat() if dep_dt else None,
                "booking_reference": formatted.get("booking_reference"),
                "account_name": formatted.get("account_name"),
                "flight_identifier": flight_identifier,
                "leg_id": formatted.get("leg_id"),
                "_sort_key": dep_dt or datetime.max.replace(tzinfo=timezone.utc),
            }
            matches.append(row_data)
    finally:
        if shared_session is not None:
            try:
                shared_session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    matches.sort(key=lambda row: row["_sort_key"])
    for row in matches:
        row.pop("_sort_key", None)

    metadata = {
        "validation_required": validation_required,
        "validated_without_issue": validated_without_issue,
        "issues_found": issues_found,
        "threshold_minutes": threshold_minutes,
        "target_airports": sorted(normalized_airports),
        "match_count": len(matches),
    }

    if any(postflight_request_counts.values()):
        metadata["postflight_requests"] = postflight_request_counts

    return MorningReportResult(
        code="16.1.12",
        title="CYYZ/CYUL Duty Start Report",
        header_label="CYYZ/CYUL Duty-Start Validation",
        rows=matches,
        warnings=list(dict.fromkeys(warning_messages)),
        metadata=metadata,
    )


def _build_upgrade_workflow_validation_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_leg_details_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_leg_details,
) -> MorningReportResult:
    matches: List[Dict[str, Any]] = []
    warnings: List[str] = []
    detail_cache: Dict[str, Optional[Any]] = {}
    session: Optional[requests.Session] = None

    inspected = 0
    flagged = 0

    try:
        for row in _sort_rows(rows):
            aircraft_category = _extract_aircraft_category(row)
            if not _is_legacy_aircraft_category(aircraft_category):
                continue

            booking_reference = _extract_booking_reference(row)
            if not booking_reference:
                warnings.append(
                    "Skipping upgrade workflow validation due to missing booking reference "
                    f"for leg {_extract_leg_id(row) or 'unknown'}"
                )
                continue

            inspected += 1

            if session is None:
                session = requests.Session()

            if booking_reference not in detail_cache:
                try:
                    payload = fetch_leg_details_fn(
                        config, booking_reference, session=session
                    )
                except Exception as exc:  # pragma: no cover - defensive path
                    warnings.append(
                        f"Failed to fetch leg details for booking {booking_reference}: {exc}"
                    )
                    detail_cache[booking_reference] = None
                else:
                    detail_cache[booking_reference] = payload

            detail = _select_leg_detail(detail_cache.get(booking_reference), row)
            planning_note = _extract_planning_note(detail)
            request_label = _planning_note_cj_request_label(planning_note)

            if not request_label:
                continue

            flagged += 1

            formatted = _format_report_row(row, include_tail=True)
            dep_dt = _extract_departure_dt(row) or _extract_detail_departure_dt(detail)
            date_component = _format_mountain_date_component(
                dep_dt, formatted.get("date")
            )

            tail = formatted.get("tail")
            booking_id = formatted.get("booking_reference") or booking_reference
            account_name = formatted.get("account_name")
            workflow = formatted.get("workflow") or _extract_workflow(row)
            assigned_type = _extract_assigned_aircraft_type(row) or _extract_assigned_aircraft_type(detail or {})
            owner_class = _extract_owner_class(row) or _extract_owner_class(detail or {})

            workflow_matches_upgrade = (
                workflow is not None and "UPGRADE" in workflow.upper()
            )

            line_parts = [
                date_component or "Unknown Date",
                tail or "Unknown Tail",
                booking_id or "Unknown Booking",
                account_name or "Unknown Account",
                request_label,
            ]

            matches.append(
                {
                    "line": "-".join(line_parts),
                    "date": date_component,
                    "tail": tail,
                    "booking_reference": booking_id,
                    "account_name": account_name,
                    "workflow": workflow,
                    "workflow_matches_upgrade": workflow_matches_upgrade,
                    "aircraft_category": aircraft_category,
                    "assigned_aircraft_type": assigned_type,
                    "owner_class": owner_class,
                    "planning_note": planning_note,
                    "request_label": request_label,
                    "leg_id": _extract_leg_id(row),
                    "flight_identifier": _extract_flight_identifier(row, formatted),
                }
            )
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    metadata = {
        "match_count": len(matches),
        "inspected_legs": inspected,
        "flagged_requests": flagged,
    }

    return MorningReportResult(
        code="16.1.9",
        title="Upgrade Workflow Validation Report",
        header_label="Legacy Upgrade Workflow Validation",
        rows=matches,
        warnings=list(dict.fromkeys(warnings)),
        metadata=metadata,
    )


def _build_upgrade_flights_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_leg_details_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_leg_details,
) -> MorningReportResult:
    matches: List[Dict[str, Any]] = []
    warnings: List[str] = []
    detail_cache: Dict[str, Optional[Any]] = {}
    session: Optional[requests.Session] = None

    inspected = 0
    details_fetched = 0

    try:
        for row in _sort_rows(rows):
            workflow = _extract_workflow(row)
            if not workflow or "upgrade" not in workflow.lower():
                continue

            inspected += 1

            formatted = _format_report_row(row, include_tail=True)
            dep_dt = _extract_departure_dt(row)
            date_component = _format_mountain_date_component(
                dep_dt,
                formatted.get("date"),
            )

            tail = formatted.get("tail")
            booking_reference = (
                formatted.get("booking_reference")
                or _extract_booking_reference(row)
            )
            quote_id = _extract_quote_identifier(row)
            assigned_type = _extract_assigned_aircraft_type(row)
            requested_type = _extract_requested_aircraft_type(row)
            aircraft_category = _extract_aircraft_category(row)
            planning_note: Optional[str] = None
            account_name: Optional[str] = formatted.get("account_name")

            detail: Optional[Mapping[str, Any]] = None

            if quote_id:
                if session is None:
                    session = requests.Session()

                if quote_id not in detail_cache:
                    try:
                        payload = fetch_leg_details_fn(
                            config, quote_id, session=session
                        )
                    except Exception as exc:  # pragma: no cover - defensive path
                        warnings.append(
                            f"Failed to fetch leg details for quote {quote_id}: {exc}"
                        )
                        detail_cache[quote_id] = None
                    else:
                        detail_cache[quote_id] = payload
                        if payload is not None:
                            details_fetched += 1

                detail = _select_leg_detail(detail_cache.get(quote_id), row)

                if detail:
                    if booking_reference is None:
                        booking_reference = _extract_booking_reference(detail)
                    if assigned_type is None:
                        assigned_type = _extract_assigned_aircraft_type(detail)
                    if requested_type is None:
                        requested_type = _extract_requested_aircraft_type(detail)
                    if account_name is None:
                        account_name = _extract_account_name(detail)
                    if aircraft_category is None:
                        aircraft_category = _extract_aircraft_category(detail)
                    planning_note = _extract_planning_note(detail)

            else:
                warnings.append(
                    f"Missing quote identifier for upgrade workflow leg "
                    f"{_extract_leg_id(row) or 'unknown'}"
                )

            identifier = booking_reference or quote_id or formatted.get("leg_id")

            transition_label: Optional[str] = None
            if requested_type or assigned_type:
                transition_label = (
                    f"{requested_type or 'Unknown Request'} -> "
                    f"{assigned_type or 'Unknown Assignment'}"
                )

            is_cj_assigned = _is_cj_aircraft_type(
                assigned_type or aircraft_category
            )
            line_parts = [
                date_component or "Unknown Date",
                tail or "Unknown Tail",
                identifier or "Unknown Booking",
                account_name or "Unknown Account",
                workflow,
            ]
            if transition_label:
                line_parts.append(transition_label)

            planning_note = planning_note or _extract_planning_note(row)
            upgrade_reason_note = _planning_note_upgrade_reason(planning_note)
            billing_instruction_note = _planning_note_billing_instruction(
                planning_note
            )
            matches.append(
                {
                    "line": "-".join(line_parts),
                    "date": date_component,
                    "tail": tail,
                    "booking_reference": booking_reference,
                    "quote_id": quote_id,
                    "workflow": workflow,
                    "upgrade_reason_note": upgrade_reason_note,
                    "billing_instruction_note": billing_instruction_note,
                    "planning_note": planning_note,
                    "account_name": account_name,
                    "assigned_aircraft_type": assigned_type,
                    "requested_aircraft_type": requested_type,
                    "aircraft_category": aircraft_category,
                    "is_cj_assigned": is_cj_assigned,
                    "leg_id": formatted.get("leg_id"),
                }
            )
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    metadata = {
        "match_count": len(matches),
        "inspected_legs": inspected,
        "details_fetched": details_fetched,
    }

    return MorningReportResult(
        code="16.1.10",
        title="Upgraded Flights Report",
        header_label="Upgrade Workflow Flights",
        rows=matches,
        warnings=list(dict.fromkeys(warnings)),
        metadata=metadata,
    )


def _build_fbo_disconnect_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_services_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_flight_services,
    fetch_airport_services_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_airport_services,
) -> MorningReportResult:
    sorted_rows = _sort_rows(rows)
    last_arrival_by_tail: Dict[str, Dict[str, Any]] = {}
    services_cache: Dict[str, Optional[Any]] = {}
    airport_services_cache: Dict[str, Optional[Set[str]]] = {}
    matches: List[Dict[str, Any]] = []
    warnings: List[str] = []
    inspected = 0
    comparisons = 0

    session: Optional[requests.Session] = None

    try:
        for row in sorted_rows:
            tail = _extract_tail(row)
            if not tail:
                continue

            inspected += 1
            tail_key = tail.upper()
            formatted = _format_report_row(row, include_tail=True)

            dep_airport = _extract_airport(row, True)
            arr_airport = _extract_airport(row, False)
            dep_airport_key = dep_airport.upper() if dep_airport else None
            arr_airport_key = arr_airport.upper() if arr_airport else None

            leg_id = _extract_leg_id(row)
            flight_identifier: Optional[str] = None
            for key in ("flightId", "flight_id", "flightID", "flightid"):
                value = _normalize_str(row.get(key))
                if value:
                    flight_identifier = value
                    break

            departure_handler_raw: Optional[str] = None
            arrival_handler_raw: Optional[str] = None

            if flight_identifier:
                if flight_identifier not in services_cache:
                    if session is None:
                        session = requests.Session()
                    try:
                        services_cache[flight_identifier] = fetch_services_fn(
                            config, flight_identifier, session=session
                        )
                    except Exception as exc:  # pragma: no cover - defensive path
                        warnings.append(
                            f"Failed to fetch services for flight {flight_identifier}: {exc}"
                        )
                        services_cache[flight_identifier] = None

                payload = services_cache.get(flight_identifier)
                departure_handler_raw = _extract_handler_company(payload, True)
                arrival_handler_raw = _extract_handler_company(payload, False)
            else:
                warnings.append(
                    "Skipping services fetch due to missing flight identifier"
                    f" for leg {leg_id or 'unknown'}"
                )

            arrival_entry = last_arrival_by_tail.get(tail_key)

            if (
                arrival_entry
                and dep_airport_key
                and dep_airport_key == arrival_entry.get("airport")
            ):
                comparisons += 1
                previous_handler_norm = arrival_entry.get("handler_normalized")
                departure_handler_norm = _normalize_handler_name(departure_handler_raw)

                if previous_handler_norm != departure_handler_norm:
                    arrival_listed: Optional[bool] = None
                    departure_listed: Optional[bool] = None
                    handler_listing_status = "unknown"

                    if dep_airport_key and fetch_airport_services_fn:
                        airport_handlers = airport_services_cache.get(dep_airport_key)
                        if dep_airport_key not in airport_services_cache:
                            if session is None:
                                session = requests.Session()
                            try:
                                airport_payload = fetch_airport_services_fn(
                                    config, dep_airport_key, session=session
                                )
                            except Exception as exc:  # pragma: no cover - defensive path
                                warnings.append(
                                    f"Failed to fetch airport services for {dep_airport_key}: {exc}"
                                )
                                airport_services_cache[dep_airport_key] = None
                                airport_handlers = None
                            else:
                                airport_handlers = _extract_ground_handler_names(
                                    airport_payload
                                )
                                airport_services_cache[dep_airport_key] = airport_handlers
                        if airport_handlers is not None:
                            if previous_handler_norm:
                                arrival_listed = (
                                    previous_handler_norm in airport_handlers
                                )
                            if departure_handler_norm:
                                departure_listed = (
                                    departure_handler_norm in airport_handlers
                                )
                            handler_listing_status = _classify_handler_listing(
                                arrival_listed, departure_listed
                            )

                    previous_handler_display = arrival_entry.get("handler") or "Unknown"
                    departure_handler_display = departure_handler_raw or "Unknown"
                    airport_display = dep_airport_key

                    issue_line = (
                        f"{formatted['line']} - {airport_display} handler mismatch "
                        f"(arrival: {previous_handler_display}; "
                        f"departure: {departure_handler_display})"
                    )

                    listing_note = _format_handler_listing_note(
                        airport_display,
                        arrival_listed,
                        departure_listed,
                        handler_listing_status,
                    )
                    if listing_note:
                        issue_line = f"{issue_line} {listing_note}"

                    matches.append(
                        {
                            **formatted,
                            "line": issue_line,
                            "issue_airport": airport_display,
                            "previous_leg_id": arrival_entry.get("leg_id"),
                            "previous_line": arrival_entry.get("formatted_line"),
                            "arrival_handler": arrival_entry.get("handler"),
                            "departure_handler": departure_handler_raw,
                            "arrival_handler_listed": arrival_listed,
                            "departure_handler_listed": departure_listed,
                            "handler_listing_status": handler_listing_status,
                            "listing_scenario": _determine_listing_scenario(
                                handler_listing_status
                            ),
                        }
                    )

            if arr_airport_key:
                last_arrival_by_tail[tail_key] = {
                    "airport": arr_airport_key,
                    "handler": arrival_handler_raw,
                    "handler_normalized": _normalize_handler_name(arrival_handler_raw),
                    "leg_id": leg_id or flight_identifier,
                    "formatted_line": formatted["line"],
                }
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    metadata = {
        "match_count": len(matches),
        "inspected_legs": inspected,
        "comparisons_evaluated": comparisons,
    }

    return MorningReportResult(
        code="16.1.11",
        title="FBO Disconnect Report",
        header_label="FBO Disconnect Checks",
        rows=matches,
        warnings=list(dict.fromkeys(warnings)),
        metadata=metadata,
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

    date_component = _format_mountain_date_component(dep_dt) or "Unknown Date"
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
        "bookingIdentifier": booking_reference,
        "account_name": account_name,
        "tail": tail,
        "workflow": _extract_workflow(row),
        "flight_type": _extract_flight_type(row),
        "leg_id": _extract_leg_id(row),
        "departure_airport": _extract_airport(row, True),
        "arrival_airport": _extract_airport(row, False),
    }
    return formatted


def _extract_departure_dt(row: Mapping[str, Any]) -> Optional[datetime]:
    """Best-effort extraction of a departure timestamp from a leg payload."""

    candidate_paths = [
        ("dep_time",),
        ("blockOffEstUTC",),
        ("blockOffEstUtc",),
        ("blockOffEstLocal",),
        ("blockOffEstimatedUTC",),
        ("blockOffEstimatedUtc",),
        ("blockOffEstimateUTC",),
        ("blockOffEstimateUtc",),
        ("blockOffUtc",),
        ("blockOffTimeUtc",),
        ("blockOffTime",),
        ("offBlockEstUTC",),
        ("offBlockEstUtc",),
        ("offBlockEstLocal",),
        ("offBlockTimeUtc",),
        ("departure", "estimatedUtc"),
        ("departure", "estimatedTime"),
        ("departure", "scheduledUtc"),
        ("departure", "scheduledTime"),
        ("departure", "actualUtc"),
        ("departure", "actualTime"),
        ("times", "departure", "estimatedUtc"),
        ("times", "departure", "estimatedTime"),
        ("times", "departure", "scheduledUtc"),
        ("times", "departure", "scheduledTime"),
        ("times", "departure", "actualUtc"),
        ("times", "departure", "actualTime"),
        ("times", "offBlock", "estimatedUtc"),
        ("times", "offBlock", "estimatedTime"),
        ("times", "offBlock", "scheduledUtc"),
        ("times", "offBlock", "scheduledTime"),
        ("times", "offBlock", "actualUtc"),
        ("times", "offBlock", "actualTime"),
        ("times", "offBlock.estimated",),
        ("times", "offBlock.estimatedUtc",),
        ("times", "offBlock.estimatedTime",),
        ("times", "offBlock.scheduled",),
        ("times", "offBlock.scheduledUtc",),
        ("times", "offBlock.scheduledTime",),
    ]

    for path in candidate_paths:
        value = _extract_nested_value(row, path)
        if value is None:
            continue
        dep_dt = _coerce_datetime_value(value)
        if dep_dt is not None:
            return dep_dt
    return None


def _extract_arrival_dt(row: Mapping[str, Any]) -> Optional[datetime]:
    """Best-effort extraction of an arrival timestamp from a leg payload."""

    candidate_paths = [
        ("blockOnEstUTC",),
        ("blockOnEstUtc",),
        ("blockOnEstLocal",),
        ("blockOnEstimatedUTC",),
        ("blockOnEstimatedUtc",),
        ("blockOnEstimateUTC",),
        ("blockOnEstimateUtc",),
        ("arr_time",),
        ("arrivalTimeUtc",),
        ("arrival_time_utc",),
        ("arrivalTime",),
        ("arrival_time",),
        ("arrivalActualUtc",),
        ("arrivalScheduledUtc",),
        ("arrivalActualTime",),
        ("arrivalScheduledTime",),
        ("arrivalActual",),
        ("arrivalScheduled",),
        ("scheduledIn",),
        ("actualIn",),
        ("inActual",),
        ("inScheduled",),
        ("onBlock",),
        ("onBlockActual",),
        ("onBlockScheduled",),
        ("blockOnTimeUtc",),
        ("onBlockTimeUtc",),
        ("arrivalOnBlockUtc",),
        ("blockOnUtc",),
        ("arrivalUtc",),
        ("arrOnBlock",),
        ("arrivalOnBlock",),
        ("blockOnTime",),
        ("onBlockTime",),
        ("arrival", "actualUtc"),
        ("arrival", "scheduledUtc"),
        ("arrival", "actualTime"),
        ("arrival", "scheduledTime"),
        ("arrival", "actual"),
        ("arrival", "scheduled"),
        ("arrival.actual",),
        ("arrival.actualUtc",),
        ("arrival.actualTime",),
        ("arrival.scheduled",),
        ("arrival.scheduledUtc",),
        ("arrival.scheduledTime",),
        ("times", "arrival", "actualUtc"),
        ("times", "arrival", "scheduledUtc"),
        ("times", "arrival", "actualTime"),
        ("times", "arrival", "scheduledTime"),
        ("times", "arrival", "actual"),
        ("times", "arrival", "scheduled"),
        ("times", "arrival.actual",),
        ("times", "arrival.actualUtc",),
        ("times", "arrival.actualTime",),
        ("times", "arrival.scheduled",),
        ("times", "arrival.scheduledUtc",),
        ("times", "arrival.scheduledTime",),
        ("times", "onBlock", "actualUtc"),
        ("times", "onBlock", "scheduledUtc"),
        ("times", "onBlock", "actualTime"),
        ("times", "onBlock", "scheduledTime"),
        ("times", "onBlock", "actual"),
        ("times", "onBlock", "scheduled"),
        ("times", "onBlock.actual",),
        ("times", "onBlock.actualUtc",),
        ("times", "onBlock.actualTime",),
        ("times", "onBlock.scheduled",),
        ("times", "onBlock.scheduledUtc",),
        ("times", "onBlock.scheduledTime",),
    ]

    for path in candidate_paths:
        value = _extract_nested_value(row, path)
        if value is None:
            continue
        arr_dt = _coerce_datetime_value(value)
        if arr_dt is not None:
            return arr_dt
    return None


def _extract_nested_value(container: Mapping[str, Any], path: Tuple[str, ...]) -> Any:
    """Retrieve a nested value from ``container`` following ``path`` segments."""

    value: Any = container
    for index, segment in enumerate(path):
        if not isinstance(value, Mapping):
            return None
        if segment in value:
            value = value[segment]
            continue
        if "." in segment:
            direct = value.get(segment)
            if direct is not None:
                value = direct
                continue
            sub_segments = tuple(part for part in segment.split(".") if part)
            if not sub_segments:
                return None
            remaining_path = sub_segments + path[index + 1 :]
            return _extract_nested_value(value, remaining_path)
        return None
    return value


def _coerce_datetime_value(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        arr_dt = value
    else:
        text = _normalize_str(value)
        if not text:
            return None
        try:
            arr_dt = safe_parse_dt(text)
        except Exception:
            return None
    if arr_dt.tzinfo is None:
        arr_dt = arr_dt.replace(tzinfo=timezone.utc)
    else:
        arr_dt = arr_dt.astimezone(timezone.utc)
    return arr_dt


def _normalize_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    return text or None


def _normalize_handler_name(value: Any) -> Optional[str]:
    text = _normalize_str(value)
    if text is None:
        return None
    collapsed = re.sub(r"\s+", " ", text)
    return collapsed.upper()


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _extract_workflow(row: Mapping[str, Any]) -> Optional[str]:
    """Return a human-readable workflow label from a leg payload."""

    def _coerce(value: Any) -> Optional[str]:
        if isinstance(value, Mapping):
            for nested_key in (
                "customName",
                "customLabel",
                "label",
                "name",
                "title",
            ):
                nested_value = _normalize_str(value.get(nested_key))
                if nested_value:
                    return nested_value
            return None
        return _normalize_str(value)

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
        label = _coerce(value)
        if label:
            return label
    return None


def _row_priority_info(row: Mapping[str, Any]) -> Tuple[bool, Optional[str]]:
    label_candidates = [
        _normalize_str(row.get("priority_label")),
        _normalize_str(row.get("priorityLabel")),
        _normalize_str(row.get("priorityDetail")),
        _normalize_str(row.get("priority_details")),
        _normalize_str(row.get("priorityDescription")),
        _normalize_str(row.get("priority_note")),
    ]
    workflow = _extract_workflow(row)
    if workflow:
        label_candidates.append(workflow)

    for label in label_candidates:
        if label and "priority" in label.lower():
            return True, label

    flag_keys = (
        "priority",
        "priorityFlight",
        "priority_flag",
        "priorityFlightFlag",
        "isPriority",
    )
    for key in flag_keys:
        if _coerce_bool(row.get(key)):
            fallback_label = next((label for label in label_candidates if label), None)
            if fallback_label and "priority" not in fallback_label.lower():
                fallback_label = f"Priority - {fallback_label}"
            return True, fallback_label or "Priority"

    fallback = next((label for label in label_candidates if label), None)
    return False, fallback


def _extract_booking_reference(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "bookingReference",
        "bookingCode",
        "bookingNumber",
        "bookingId",
        "booking_id",
        "bookingID",
        "bookingRef",
        "bookingIdentifier",
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


def _is_placeholder_tail(value: Any) -> bool:
    normalized = _normalize_str(value)
    if not normalized:
        return False
    return normalized.upper() in _PLACEHOLDER_TAILS


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


def _is_ocs_pax_leg(row: Mapping[str, Any]) -> bool:
    flight_type = _extract_flight_type(row)
    if flight_type is None or flight_type.upper() != "PAX":
        return False
    account_name = _extract_account_name(row)
    if not account_name:
        return False
    return account_name.upper() == _OCS_ACCOUNT_NAME


def _extract_pax_count(row: Mapping[str, Any]) -> Optional[int]:
    for key in (
        "paxNumber",
        "pax_count",
        "pax",
        "passengerCount",
        "passengers",
        "passenger_count",
    ):
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        text = _normalize_str(value)
        if not text:
            continue
        try:
            return int(text)
        except ValueError:
            match = re.search(r"\d+", text)
            if match:
                try:
                    return int(match.group())
                except ValueError:
                    continue
    return None


def _extract_checkin_values(payload: Any) -> List[Any]:
    values: List[Any] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, Mapping):
            for key, value in obj.items():
                if isinstance(key, str) and key.lower() == "checkin":
                    values.append(value)
                _walk(value)
        elif isinstance(obj, IterableABC) and not isinstance(obj, (str, bytes, bytearray)):
            for item in obj:
                _walk(item)

    _walk(payload)
    return values


def _checkin_to_datetime(value: Any, target_tz: timezone) -> Optional[datetime]:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    try:
        dt_utc = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    if target_tz is None:
        return dt_utc
    try:
        return dt_utc.astimezone(target_tz)
    except Exception:
        return dt_utc


def _extract_flight_identifier(
    row: Mapping[str, Any], formatted: Mapping[str, Any]
) -> Optional[str]:
    for key in ("flightId", "flight_id", "flightID", "flightid"):
        value = _normalize_str(row.get(key))
        if value:
            return value
    fallback = _normalize_str(formatted.get("leg_id"))
    if fallback:
        return fallback
    return None


def _extract_notification_note(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, Mapping):
        for key in ("note", "notificationNote", "notification", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for key in ("items", "notifications", "data", "results"):
            nested = payload.get(key)
            note = _extract_notification_note(nested)
            if note:
                return note
        return None
    if isinstance(payload, IterableABC) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            note = _extract_notification_note(item)
            if note:
                return note
    if isinstance(payload, str) and payload.strip():
        return payload.strip()
    return None


def _extract_handler_company(payload: Any, departure: bool) -> Optional[str]:
    target_key = "departureHandler" if departure else "arrivalHandler"

    def _coerce_handler_value(handler_value: Any) -> Optional[str]:
        if isinstance(handler_value, Mapping):
            for field in ("company", "handler", "name", "fbo", "provider"):
                text = _normalize_str(handler_value.get(field))
                if text:
                    return text

            for nested_key in (
                "airportService",
                "airport_service",
                "service",
                "handler",
                "provider",
                "company",
                "details",
            ):
                if nested_key in handler_value:
                    nested_result = _coerce_handler_value(handler_value[nested_key])
                    if nested_result:
                        return nested_result

            for value in handler_value.values():
                nested_result = _coerce_handler_value(value)
                if nested_result:
                    return nested_result
            return None

        if isinstance(handler_value, IterableABC) and not isinstance(
            handler_value, (str, bytes, bytearray)
        ):
            for item in handler_value:
                nested_result = _coerce_handler_value(item)
                if nested_result:
                    return nested_result
            return None

        return _normalize_str(handler_value)

    def _search(obj: Any) -> Optional[str]:
        if isinstance(obj, Mapping):
            if target_key in obj:
                handler_value = obj[target_key]
                extracted = _coerce_handler_value(handler_value)
                if extracted:
                    return extracted
            for value in obj.values():
                result = _search(value)
                if result:
                    return result
        elif isinstance(obj, IterableABC) and not isinstance(obj, (str, bytes, bytearray)):
            for item in obj:
                result = _search(item)
                if result:
                    return result
        return None

    return _search(payload)


def _extract_ground_handler_names(payload: Any) -> Set[str]:
    handlers: Set[str] = set()

    if payload is None:
        return handlers

    def _iter_services(obj: Any) -> Iterable[Any]:
        if isinstance(obj, Mapping):
            items = obj.get("items")
            if isinstance(items, IterableABC) and not isinstance(
                items, (str, bytes, bytearray)
            ):
                for entry in items:
                    yield entry
            else:
                yield obj
        elif isinstance(obj, IterableABC) and not isinstance(
            obj, (str, bytes, bytearray)
        ):
            for entry in obj:
                yield entry
        else:
            return

    for service in _iter_services(payload):
        if not isinstance(service, Mapping):
            continue
        type_info = service.get("type")
        type_id: Optional[int] = None
        type_name: Optional[str] = None
        type_display: Optional[str] = None
        if isinstance(type_info, Mapping):
            try:
                type_id = int(type_info.get("id")) if type_info.get("id") is not None else None
            except (TypeError, ValueError):
                type_id = None
            type_name = _normalize_str(type_info.get("name"))
            type_display = _normalize_str(type_info.get("displayName"))
        else:
            type_name = _normalize_str(type_info)

        if not _is_ground_handler_type(type_id, type_name, type_display):
            continue

        company_name: Optional[str] = None
        for candidate_key in ("company", "name", "handler", "provider", "title"):
            candidate_value = service.get(candidate_key)
            normalized = _normalize_handler_name(candidate_value)
            if normalized:
                company_name = normalized
                break
        if company_name:
            handlers.add(company_name)

    return handlers


def _is_ground_handler_type(
    type_id: Optional[int],
    type_name: Optional[str],
    type_display: Optional[str],
) -> bool:
    if type_id == 2:
        return True

    for text in (type_name, type_display):
        if not text:
            continue
        upper_text = text.upper()
        if "FBO" in upper_text or "GROUND" in upper_text:
            return True
    return False


def _classify_handler_listing(
    arrival_listed: Optional[bool], departure_listed: Optional[bool]
) -> str:
    if arrival_listed is True and departure_listed is True:
        return "both_listed"
    if arrival_listed is False or departure_listed is False:
        return "missing_handler"
    if arrival_listed is None and departure_listed is None:
        return "unknown"
    return "partial_listing"


def _format_handler_listing_note(
    airport: Optional[str],
    arrival_listed: Optional[bool],
    departure_listed: Optional[bool],
    status: str,
) -> Optional[str]:
    airport_component = f" at {airport}" if airport else ""
    if status == "unknown":
        return f"[listing check unavailable{airport_component}]"

    if arrival_listed is True and departure_listed is True:
        return f"[listing check: both handlers listed{airport_component}]"

    details: List[str] = []
    if arrival_listed is not None:
        arrival_text = "listed" if arrival_listed else "missing"
        details.append(f"arrival {arrival_text}")
    if departure_listed is not None:
        departure_text = "listed" if departure_listed else "missing"
        details.append(f"departure {departure_text}")

    if not details:
        return None

    detail_text = ", ".join(details)
    return f"[listing check: {detail_text}{airport_component}]"


def _determine_listing_scenario(status: Optional[str]) -> str:
    if status == "both_listed":
        return "same_airport"
    return "handler_missing"


def _extract_aircraft_category(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "aircraftCategory",
        "aircraft_category",
        "aircraftType",
        "aircraftClass",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    aircraft = row.get("aircraft")
    if isinstance(aircraft, Mapping):
        nested = _normalize_str(aircraft.get("category") or aircraft.get("type"))
        if nested:
            return nested
    return None


def _is_legacy_aircraft_category(category: Optional[str]) -> bool:
    if not category:
        return False
    normalized = category.upper()
    if normalized in _LEGACY_AIRCRAFT_CATEGORIES:
        return True
    return any(token in normalized for token in _LEGACY_AIRCRAFT_TOKENS)


def _extract_assigned_aircraft_type(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "assignedAircraftType",
        "assigned_aircraft_type",
        "requestedAircraftType",
        "aircraftTypeAssigned",
        "aircraftTypeName",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    aircraft = row.get("aircraft")
    if isinstance(aircraft, Mapping):
        for nested_key in ("assignedType", "requestedType", "typeName", "name"):
            value = _normalize_str(aircraft.get(nested_key))
            if value:
                return value
    return None


def _extract_requested_aircraft_type(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "requestedAircraftType",
        "requested_aircraft_type",
        "requestedType",
        "requestedAircraft",
        "requestedEquipment",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    aircraft = row.get("aircraft")
    if isinstance(aircraft, Mapping):
        for nested_key in (
            "requestedType",
            "requestedAircraftType",
            "requestedEquipment",
            "requested",
        ):
            value = _normalize_str(aircraft.get(nested_key))
            if value:
                return value
    request = row.get("request")
    if isinstance(request, Mapping):
        for nested_key in (
            "aircraftType",
            "aircraft",
            "requestedType",
        ):
            value = _normalize_str(request.get(nested_key))
            if value:
                return value
    return None


def _is_cj_aircraft_type(value: Optional[str]) -> bool:
    if not value:
        return False
    normalized = _normalize_str(value)
    if not normalized:
        return False
    upper = normalized.upper()
    if upper.startswith("CJ"):
        return True
    if upper in {"C25A", "C25B"}:
        return True
    return bool(re.search(r"\bCJ\d?\+?\b", upper))


def _extract_owner_class(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "ownerClass",
        "owner_class",
        "ownerClassification",
        "owner_classification",
        "ownerType",
        "ownerTypeName",
        "ownerClassName",
        "aircraftOwnerClass",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    owner = row.get("owner")
    if isinstance(owner, Mapping):
        for nested_key in ("class", "classification", "type", "name"):
            value = _normalize_str(owner.get(nested_key))
            if value:
                return value
    return None


def _extract_quote_identifier(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("quoteId", "quote_id", "quoteID", "quote", "quoteNumber"):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _extract_basic_flight_identifier(row: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "flightIdentifier",
        "flight_identifier",
        "flightId",
        "flight_id",
        "flightID",
        "flightid",
    ):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _select_leg_detail(
    payload: Any, row: Optional[Mapping[str, Any]] = None
) -> Optional[Mapping[str, Any]]:
    candidates: List[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        candidates.append(payload)
    elif isinstance(payload, IterableABC) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            if isinstance(item, Mapping):
                candidates.append(item)

    if not candidates:
        return None

    if row is None:
        return candidates[0]

    row_leg_id = _extract_leg_id(row)
    row_flight_id = _extract_basic_flight_identifier(row)
    row_dep_dt = _extract_departure_dt(row)
    row_dep_airport = _normalize_airport_ident(_extract_airport(row, True))
    row_arr_airport = _normalize_airport_ident(_extract_airport(row, False))

    def _to_utc(dt_value: Optional[datetime]) -> Optional[datetime]:
        if dt_value is None:
            return None
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc)

    row_dep_dt_utc = _to_utc(row_dep_dt)

    if row_leg_id:
        for detail in candidates:
            detail_leg_id = _extract_leg_id(detail)
            if detail_leg_id and detail_leg_id == row_leg_id:
                return detail

    if row_flight_id:
        for detail in candidates:
            detail_flight_id = _extract_basic_flight_identifier(detail)
            if detail_flight_id and detail_flight_id == row_flight_id:
                return detail

    best_detail: Optional[Mapping[str, Any]] = None
    best_score = -1

    for detail in candidates:
        score = 0

        detail_dep_dt = _extract_detail_departure_dt(detail) or _extract_departure_dt(detail)
        detail_dep_dt_utc = _to_utc(detail_dep_dt)
        if row_dep_dt_utc and detail_dep_dt_utc:
            delta = abs((detail_dep_dt_utc - row_dep_dt_utc).total_seconds())
            if delta <= 300:
                score += 3
            elif delta <= 3600:
                score += 1

        if row_dep_airport:
            detail_dep_airport = _normalize_airport_ident(_extract_airport(detail, True))
            if detail_dep_airport and detail_dep_airport == row_dep_airport:
                score += 1

        if row_arr_airport:
            detail_arr_airport = _normalize_airport_ident(_extract_airport(detail, False))
            if detail_arr_airport and detail_arr_airport == row_arr_airport:
                score += 1

        if score > best_score:
            best_detail = detail
            best_score = score

    if best_detail is not None and best_score > 0:
        return best_detail

    return candidates[0]


def _extract_planning_note(detail: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not detail:
        return None
    for key in ("planningNotes", "planningNote", "planning_notes", "notes"):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _split_planning_note_segments(note: str) -> List[str]:
    segments = re.split(r"(?:[\r\n]+|(?<=[.!?])\s+)", note)
    cleaned: List[str] = []
    for segment in segments:
        if not segment:
            continue
        trimmed = segment.strip(" -")
        if trimmed:
            cleaned.append(trimmed)
    return cleaned


def _planning_note_upgrade_reason(note: Optional[str]) -> Optional[str]:
    if not note:
        return None
    for segment in _split_planning_note_segments(note):
        lowered = segment.lower()
        if "upgrade" not in lowered:
            continue
        if "due" not in lowered and "because" not in lowered:
            continue
        return segment
    return None


def _planning_note_billing_instruction(note: Optional[str]) -> Optional[str]:
    if not note:
        return None
    for segment in _split_planning_note_segments(note):
        lowered = segment.lower()
        if "bill" not in lowered:
            continue
        if "hour" not in lowered:
            continue
        if "cj" not in lowered:
            continue
        if not re.search(r"\d+(?:\.\d+)?", segment):
            continue
        return segment
    return None


def _extract_booking_note(detail: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not detail:
        return None
    for key in (
        "bookingNote",
        "bookingNotes",
        "booking_note",
        "bookingnote",
        "booking",
    ):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


_NON_CJ2_REQUEST_KEYWORDS = (
    "CJ3",
    "CJ3+",
    "CJ3 PLUS",
    "CJ3+/CJ2+",
    "CJ3+/CJ3",
    "EMB",
    "E550",
    "EMB-550",
    "EMB550",
    "L450",
    "LEGACY 450",
)

_REQUEST_ARTICLE_PATTERN = r"(?:\s+(?:A|AN|THE)\b)?"


def _build_request_keyword_pattern(keyword: str) -> re.Pattern[str]:
    return re.compile(
        rf"\bREQUESTING\b{_REQUEST_ARTICLE_PATTERN}[^A-Z0-9]*{re.escape(keyword)}(?=\b|[^A-Z0-9]|$)",
        re.IGNORECASE,
    )


_NON_CJ2_REQUEST_PATTERNS = tuple(
    _build_request_keyword_pattern(keyword) for keyword in _NON_CJ2_REQUEST_KEYWORDS
)

_CJ_REQUEST_PATTERNS = (
    re.compile(
        rf"\bREQUESTING\b{_REQUEST_ARTICLE_PATTERN}[^A-Z0-9]*(CJ\s+FLEET|CJ(?:[-\s]?\d)?\+?)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\bREQUESTING\b{_REQUEST_ARTICLE_PATTERN}[^A-Z0-9]*(CITATION(?:\s+[A-Z0-9]+)?)",
        re.IGNORECASE,
    ),
)


def _planning_note_requests_non_cj2(note: Optional[str]) -> bool:
    if not note:
        return False
    text = note.upper()
    if "REQUESTING" not in text:
        return False
    if "REQUESTING CJ2" in text and not any(
        keyword in text for keyword in _NON_CJ2_REQUEST_KEYWORDS
    ):
        return False
    for pattern in _NON_CJ2_REQUEST_PATTERNS:
        if pattern.search(note):
            return True
    return False


def _planning_note_cj_request_label(note: Optional[str]) -> Optional[str]:
    if not note:
        return None

    for pattern in _CJ_REQUEST_PATTERNS:
        match = pattern.search(note)
        if not match:
            continue
        group = match.group(1)
        if not isinstance(group, str):
            continue
        cleaned = re.sub(r"[^A-Z0-9]+", "", group.upper())
        if cleaned.startswith("CJ"):
            if "FLEET" in cleaned:
                return "CJ Fleet"
            if len(cleaned) > 2:
                return cleaned
            return "CJ"
        if cleaned.startswith("CITATION"):
            suffix = cleaned[len("CITATION") :]
            return "Citation" + (f" {suffix}" if suffix else "")
    return None


def _extract_detail_pax(
    detail: Optional[Mapping[str, Any]],
    fallback_row: Mapping[str, Any],
) -> Optional[int]:
    for container in (detail, fallback_row):
        if not isinstance(container, Mapping):
            continue
        for key in ("pax", "paxNumber", "passengers", "pax_count", "passengerCount"):
            value = container.get(key)
            if value is None or isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
            text = _normalize_str(value)
            if not text:
                continue
            try:
                return int(text)
            except ValueError:
                match = re.search(r"\d+", text)
                if match:
                    try:
                        return int(match.group())
                    except ValueError:
                        continue
    return None


def _extract_block_minutes(
    detail: Optional[Mapping[str, Any]],
    fallback_row: Mapping[str, Any],
) -> Optional[int]:
    for container in (detail, fallback_row):
        if not isinstance(container, Mapping):
            continue
        for key in (
            "blockTime",
            "block_time",
            "blockMinutes",
            "block_minutes",
            "blockTimeMinutes",
        ):
            value = container.get(key)
            if value is None:
                continue
            if isinstance(value, (int, float)):
                try:
                    minutes = int(value)
                except (TypeError, ValueError):
                    continue
                if minutes >= 0:
                    return minutes
            text = _normalize_str(value)
            if not text:
                continue
            minutes: Optional[int] = None
            try:
                minutes = int(float(text))
            except ValueError:
                match = re.search(r"\d+", text)
                if match:
                    try:
                        minutes = int(match.group())
                    except ValueError:
                        minutes = None
            if minutes is not None and minutes >= 0:
                return minutes
    return None


def _extract_detail_departure_dt(detail: Optional[Mapping[str, Any]]) -> Optional[datetime]:
    if not detail:
        return None
    for key in (
        "departureDate",
        "departureDateUTC",
        "departure_date",
        "departure_time",
    ):
        value = detail.get(key)
        if not value:
            continue
        text = _normalize_str(value)
        if not text:
            continue
        try:
            dt_value = safe_parse_dt(text)
        except Exception:
            continue
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        else:
            dt_value = dt_value.astimezone(timezone.utc)
        return dt_value
    return None


def _format_block_minutes(minutes: Optional[int]) -> str:
    if minutes is None or minutes < 0:
        return "Unknown"
    hours, remainder = divmod(minutes, 60)
    return f"{hours:02d}:{remainder:02d}"


def _format_notification_text(note: Optional[str]) -> str:
    if not note:
        return "No OCS notes found"
    normalized = note.replace("\r", "\n").strip()
    if not normalized:
        return "No OCS notes found"
    parts = [segment.strip() for segment in normalized.split("\n") if segment.strip()]
    if not parts:
        return "No OCS notes found"
    return " | ".join(parts)


__all__ = [
    "MorningReportError",
    "MorningReportResult",
    "MorningReportRun",
    "run_morning_reports",
]
