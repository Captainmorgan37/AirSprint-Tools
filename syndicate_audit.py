"""Utilities for auditing syndicate/partner bookings against daily flights."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests

from fl3xx_api import Fl3xxApiConfig, MOUNTAIN_TIME_ZONE, fetch_flights, fetch_preflight
from flight_leg_utils import filter_out_subcharter_rows, normalize_fl3xx_payload, safe_parse_dt


_NOTE_KEYS = (
    "bookingNotes",
    "bookingNote",
    "booking_note",
    "bookingnote",
)


@dataclass
class SyndicateMatch:
    note_type: str
    partner_name: str
    note_line: str
    tail_type: Optional[str]


@dataclass
class SyndicateAuditEntry:
    owner_account: str
    partner_account: str
    partner_present: bool
    partner_match: Optional[str]
    booking_reference: str
    aircraft_type: str
    workflow: str
    tail: str
    route: str
    note_type: str
    note_line: str
    syndicate_tail_type: str


@dataclass
class SyndicateAuditResult:
    date: date
    entries: List[SyndicateAuditEntry]
    diagnostics: Dict[str, Any]
    warnings: List[str]


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


def _iter_mapping_candidates(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        yield payload
        for value in payload.values():
            yield from _iter_mapping_candidates(value)
    elif isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            yield from _iter_mapping_candidates(item)


def _extract_booking_notes(payload: Any) -> Optional[str]:
    for candidate in _iter_mapping_candidates(payload):
        for key in _NOTE_KEYS:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _normalize_name(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(cleaned.split())


def _is_na_name(value: str) -> bool:
    normalized = _normalize_name(value)
    return normalized in {"na", "n a", "none", "no partner"}


def _split_partner_names(value: str) -> List[str]:
    parts = re.split(r"[;,]", value)
    names: List[str] = []
    for part in parts:
        text = part.strip()
        if text:
            names.append(text)
    return names


def _extract_syndicate_matches(notes: str) -> List[SyndicateMatch]:
    matches: List[SyndicateMatch] = []
    last_tail_type: Optional[str] = None
    for raw_line in notes.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "-":
            continue
        if "syndicate" not in line.lower() and "partner" not in line.lower():
            last_tail_type = line
            continue
        match = re.search(r"(?P<label>syndicate|partner)\s*[:\-–]\s*(?P<name>.+)", line, flags=re.IGNORECASE)
        if not match:
            continue
        label = match.group("label").strip().title()
        raw_name = match.group("name").strip()
        if not raw_name:
            continue
        raw_name = raw_name.split("[", 1)[0].strip()
        raw_name = raw_name.split("(", 1)[0].strip()
        if not raw_name:
            continue
        for name in _split_partner_names(raw_name):
            if not name or _is_na_name(name):
                continue
            matches.append(
                SyndicateMatch(
                    note_type=label,
                    partner_name=name,
                    note_line=line,
                    tail_type=last_tail_type,
                )
            )
    return matches


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


def _extract_account_name(row: Mapping[str, Any]) -> Optional[str]:
    return _first_nonempty(
        (
            row.get("accountName"),
            row.get("account"),
            row.get("account_name"),
            row.get("owner"),
            row.get("ownerName"),
            row.get("customer"),
            row.get("customerName"),
        )
    )


def _extract_booking_reference(row: Mapping[str, Any]) -> Optional[str]:
    return _first_nonempty(
        (
            row.get("bookingIdentifier"),
            row.get("booking_reference"),
            row.get("bookingReference"),
            row.get("bookingCode"),
            row.get("bookingId"),
        )
    )


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
        if isinstance(value, Mapping):
            nested = _first_nonempty(
                value.get(nested_key)
                for nested_key in ("customName", "customLabel", "label", "name", "title")
            )
            if nested:
                return nested
        text = _coerce_to_str(value)
        if text:
            return text
    return None


def _extract_aircraft_type(row: Mapping[str, Any]) -> Optional[str]:
    return _first_nonempty(
        (
            row.get("assignedAircraftType"),
            row.get("aircraftType"),
            row.get("aircraft_type"),
            row.get("aircraftCategory"),
            row.get("aircraft_category"),
            row.get("aircraft"),
        )
    )


_TAIL_TYPE_PATTERNS = [
    r"\bCJ2\+?\b",
    r"\bCJ3\+?\b",
    r"\bP500\b",
    r"\bPraetor\s*500\b",
    r"\bL450\b",
    r"\bLegacy\s*450\b",
    r"\bLegacy\b",
    r"\bEmbraer\b",
]
_TAIL_TYPE_REGEX = re.compile("|".join(_TAIL_TYPE_PATTERNS), re.IGNORECASE)


def _extract_tail_type_label(value: Optional[str]) -> str:
    if not value:
        return ""
    match = _TAIL_TYPE_REGEX.search(value)
    if not match:
        return ""
    label = match.group(0).strip()
    normalized = " ".join(label.split())
    if normalized.lower() == "legacy":
        return "Legacy"
    return normalized


def _extract_tail(row: Mapping[str, Any]) -> str:
    return _first_nonempty(
        (
            row.get("tail"),
            row.get("tailNumber"),
            row.get("tail_number"),
            row.get("aircraft"),
            row.get("aircraftRegistration"),
        )
    ) or ""


def _format_route(row: Mapping[str, Any]) -> str:
    dep = _first_nonempty(
        (
            row.get("departure_airport"),
            row.get("dep_airport"),
            row.get("departureAirport"),
            row.get("departure_airport_code"),
            row.get("airportFrom"),
            row.get("fromAirport"),
        )
    )
    arr = _first_nonempty(
        (
            row.get("arrival_airport"),
            row.get("arr_airport"),
            row.get("arrivalAirport"),
            row.get("arrival_airport_code"),
            row.get("airportTo"),
            row.get("toAirport"),
        )
    )
    if dep or arr:
        return f"{dep or '???'} → {arr or '???'}"
    return ""


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


def _filter_rows_for_target_date(rows: Iterable[Mapping[str, Any]], target_date: date) -> List[Mapping[str, Any]]:
    filtered: List[Mapping[str, Any]] = []
    for row in rows:
        mountain_dt = _parse_mountain_datetime(row.get("dep_time"))
        if mountain_dt and mountain_dt.date() == target_date:
            filtered.append(row)
    return filtered


def _is_pax_flight(row: Mapping[str, Any]) -> bool:
    flight_type = _first_nonempty((row.get("flightType"), row.get("flight_type"), row.get("type")))
    if not flight_type:
        return False
    normalized = flight_type.strip().upper()
    return normalized == "PAX" or "PAX" in normalized or "PASSENGER" in normalized


def run_syndicate_audit(
    config: Fl3xxApiConfig,
    *,
    target_date: date,
    session: Optional[requests.Session] = None,
) -> SyndicateAuditResult:
    diagnostics: Dict[str, Any] = {
        "total_flights": 0,
        "targeted_flights": 0,
        "pax_flights": 0,
        "unique_accounts": 0,
        "missing_accounts": 0,
        "missing_flight_ids": 0,
        "preflight_requests": 0,
        "preflight_errors": 0,
        "missing_booking_notes": 0,
        "syndicate_matches": 0,
    }
    warnings: List[str] = []
    entries: List[SyndicateAuditEntry] = []

    http = session or requests.Session()
    close_session = session is None
    try:
        flights, metadata = fetch_flights(
            config,
            from_date=target_date,
            to_date=target_date + timedelta(days=1),
            session=http,
        )
        diagnostics["fetch_metadata"] = metadata
        diagnostics["total_flights"] = len(flights)

        normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
        filtered_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
        targeted_rows = _filter_rows_for_target_date(filtered_rows, target_date)
        diagnostics["normalization_stats"] = normalization_stats
        diagnostics["skipped_subcharter"] = skipped_subcharter
        diagnostics["targeted_flights"] = len(targeted_rows)

        pax_rows = [row for row in targeted_rows if _is_pax_flight(row)]
        diagnostics["pax_flights"] = len(pax_rows)

        account_rows: Dict[str, Mapping[str, Any]] = {}
        account_display: Dict[str, str] = {}
        for row in pax_rows:
            account_name = _extract_account_name(row)
            if not account_name:
                diagnostics["missing_accounts"] += 1
                continue
            normalized_account = _normalize_name(account_name)
            if not normalized_account:
                diagnostics["missing_accounts"] += 1
                continue
            if normalized_account not in account_rows:
                account_rows[normalized_account] = row
                account_display[normalized_account] = account_name

        diagnostics["unique_accounts"] = len(account_rows)

        for normalized_account, row in account_rows.items():
            flight_id = _extract_flight_identifier(row)
            if not flight_id:
                diagnostics["missing_flight_ids"] += 1
                continue
            diagnostics["preflight_requests"] += 1
            try:
                payload = fetch_preflight(config, flight_id, session=http)
            except Exception as exc:  # pragma: no cover - defensive path
                diagnostics["preflight_errors"] += 1
                warnings.append(f"{target_date.isoformat()}: Unable to fetch preflight for flight {flight_id}: {exc}")
                continue

            booking_notes = _extract_booking_notes(payload)
            if not booking_notes:
                diagnostics["missing_booking_notes"] += 1
                continue

            matches = _extract_syndicate_matches(booking_notes)
            if not matches:
                continue

            booking_reference = _extract_booking_reference(row) or flight_id
            aircraft_type = _extract_aircraft_type(row) or ""
            workflow = _extract_workflow_label(row) or ""
            for match in matches:
                diagnostics["syndicate_matches"] += 1
                partner_normalized = _normalize_name(match.partner_name)
                partner_present = partner_normalized in account_display if partner_normalized else False
                partner_match = account_display.get(partner_normalized) if partner_present else None

                syndicate_tail_type = _extract_tail_type_label(match.tail_type or match.note_line)
                entry = SyndicateAuditEntry(
                    owner_account=account_display.get(normalized_account, normalized_account),
                    partner_account=match.partner_name,
                    partner_present=partner_present,
                    partner_match=partner_match,
                    booking_reference=booking_reference,
                    aircraft_type=aircraft_type,
                    workflow=workflow,
                    tail=_extract_tail(row),
                    route=_format_route(row),
                    note_type=match.note_type,
                    note_line=match.note_line,
                    syndicate_tail_type=syndicate_tail_type,
                )
                entries.append(entry)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return SyndicateAuditResult(
        date=target_date,
        entries=entries,
        diagnostics=diagnostics,
        warnings=warnings,
    )


__all__ = [
    "SyndicateAuditEntry",
    "SyndicateAuditResult",
    "run_syndicate_audit",
]
