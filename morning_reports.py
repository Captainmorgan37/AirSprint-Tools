"""Business logic for executing the Operations Lead morning reports."""

from __future__ import annotations

from collections.abc import Iterable as IterableABC
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    compute_fetch_dates,
    fetch_flights,
    fetch_flight_notification,
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
_OCS_ACCOUNT_NAME = "AIRSPRINT INC."

_PRIORITY_CHECKIN_THRESHOLD_MINUTES = 90


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
) -> MorningReportResult:
    matches: List[Dict[str, Any]] = []
    warnings: List[str] = []
    detail_cache: Dict[str, Optional[Any]] = {}
    session: Optional[requests.Session] = None

    total_flagged = 0
    inspected = 0

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
            detail = _select_leg_detail(payload)
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
            elif pax_count > 5:
                violation = True
                violation_reasons.append("Passenger count above limit")

            if block_minutes is None:
                violation = True
                warnings.append(
                    f"Missing block time for quote {quote_id}; flagging for review"
                )
                violation_reasons.append("Missing block time")
            elif block_minutes > 180:
                violation = True
                violation_reasons.append("Block time above limit")

            threshold_status = (
                "Threshold exceeded" if violation else "Within thresholds"
            )

            dep_dt = _extract_departure_dt(row)
            if dep_dt is None:
                dep_dt = _extract_detail_departure_dt(detail)

            date_component = dep_dt.date().isoformat() if dep_dt else "Unknown Date"

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
                }
            )
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    return MorningReportResult(
        code="16.1.6",
        title="CJ3 Owners on CJ2 Report",
        header_label="CJ3 Owners on CJ2",
        rows=matches,
        warnings=warnings,
        metadata={
            "match_count": len(matches),
            "flagged_candidates": total_flagged,
            "inspected_legs": inspected,
        },
    )


def _build_priority_status_report(
    rows: Iterable[Mapping[str, Any]],
    config: Fl3xxApiConfig,
    *,
    fetch_postflight_fn: Callable[[Fl3xxApiConfig, Any], Any] = fetch_postflight,
    threshold_minutes: int = _PRIORITY_CHECKIN_THRESHOLD_MINUTES,
) -> MorningReportResult:
    sorted_rows = _sort_rows(rows)

    earliest_departure: Dict[Tuple[str, date], datetime] = {}
    for row in sorted_rows:
        tail = _extract_tail(row)
        dep_dt = _extract_departure_dt(row)
        if not tail or dep_dt is None:
            continue
        key = (tail.upper(), dep_dt.date())
        current = earliest_departure.get(key)
        if current is None or dep_dt < current:
            earliest_departure[key] = dep_dt

    priority_candidates: List[Dict[str, Any]] = []
    last_arrival_by_tail: Dict[str, Dict[str, Any]] = {}
    for row in sorted_rows:
        formatted = _format_report_row(row, include_tail=True)
        tail = formatted.get("tail")
        dep_dt = _extract_departure_dt(row)
        dep_date = dep_dt.date() if dep_dt else None
        tail_key = tail.upper() if tail else None
        previous_arrival_entry = last_arrival_by_tail.get(tail_key) if tail_key else None
        previous_arrival_dt: Optional[datetime] = None
        previous_arrival_row: Optional[Mapping[str, Any]] = None
        if previous_arrival_entry:
            previous_arrival_dt = previous_arrival_entry.get("arrival_dt")
            previous_arrival_row = previous_arrival_entry.get("row")

        is_first_departure = False
        if tail_key and dep_date and dep_dt is not None:
            key = (tail_key, dep_date)
            earliest = earliest_departure.get(key)
            if earliest is not None:
                delta = abs((dep_dt - earliest).total_seconds())
                if delta < 1.0:
                    is_first_departure = True

        is_priority, priority_label = _row_priority_info(row)
        if is_priority:
            if is_first_departure:
                previous_arrival_dt = None
                previous_arrival_row = None
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

        arrival_dt = _extract_arrival_dt(row)
        if tail_key and arrival_dt is not None:
            last_arrival_by_tail[tail_key] = {"arrival_dt": arrival_dt, "row": row}

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
            status = "Continuation of same booking; turn validation not required"
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
                payload = fetch_postflight_fn(config, flight_identifier)
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

    return MorningReportResult(
        code="16.1.7",
        title="Priority Status Report",
        header_label="Priority Duty-Start Validation",
        rows=matches,
        warnings=list(dict.fromkeys(warning_messages)),
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
        "bookingIdentifier",
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


def _extract_quote_identifier(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("quoteId", "quote_id", "quoteID", "quote", "quoteNumber"):
        value = _normalize_str(row.get(key))
        if value:
            return value
    return None


def _select_leg_detail(payload: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        return payload
    if isinstance(payload, IterableABC) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            if isinstance(item, Mapping):
                return item
    return None


def _extract_planning_note(detail: Optional[Mapping[str, Any]]) -> Optional[str]:
    if not detail:
        return None
    for key in ("planningNotes", "planningNote", "planning_notes", "notes"):
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
    for keyword in _NON_CJ2_REQUEST_KEYWORDS:
        if f"REQUESTING {keyword}" in text:
            return True
    return False


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

