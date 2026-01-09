"""Utilities for auditing syndicate/partner bookings against daily flights."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import difflib
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    MOUNTAIN_TIME_ZONE,
    fetch_flights,
    fetch_leg_details,
    fetch_preflight,
)
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


@dataclass
class SyndicateQuoteMatch:
    partner_account: str
    partner_present: bool
    partner_match: Optional[str]
    note_type: str
    note_line: str
    syndicate_tail_type: str


@dataclass
class SyndicateQuoteFlight:
    account: str
    booking_reference: str
    flight_id: str
    aircraft_type: str
    workflow: str
    tail: str
    route: str
    dep_time: Optional[datetime]


@dataclass
class SyndicateQuoteAuditResult:
    quote_id: str
    flight_id: Optional[str]
    flight_date: Optional[date]
    owner_account: Optional[str]
    matches: List[SyndicateQuoteMatch]
    partner_flights: List[SyndicateQuoteFlight]
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


def _find_fuzzy_account_match(
    partner_normalized: str, account_display: Mapping[str, str]
) -> Optional[str]:
    if not partner_normalized:
        return None
    if partner_normalized in account_display:
        return partner_normalized

    partner_tokens = set(partner_normalized.split())
    best_token_match: Optional[str] = None
    best_token_count = 0
    for normalized_account in account_display:
        account_tokens = set(normalized_account.split())
        if partner_tokens and partner_tokens.issubset(account_tokens):
            if len(partner_tokens) >= 2 or len(partner_normalized) >= 8:
                token_count = len(partner_tokens)
                if token_count > best_token_count:
                    best_token_count = token_count
                    best_token_match = normalized_account
    if best_token_match:
        return best_token_match

    best_ratio = 0.0
    best_ratio_match: Optional[str] = None
    for normalized_account in account_display:
        ratio = difflib.SequenceMatcher(None, partner_normalized, normalized_account).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_ratio_match = normalized_account
    if best_ratio >= 0.9:
        return best_ratio_match
    return None


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


def _parse_mountain_datetime_value(value: Any) -> Optional[datetime]:
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
        return parsed.astimezone(MOUNTAIN_TIME_ZONE)
    return _parse_mountain_datetime(value)


_PREFLIGHT_DATE_KEYS = (
    "dep_time",
    "depTime",
    "dep_time_utc",
    "depTimeUtc",
    "departureTime",
    "departure_time",
    "departure_time_utc",
    "departureDate",
    "dep_date",
    "depDate",
    "flightDate",
    "std",
    "stdUtc",
    "date",
)


def _extract_preflight_date(payload: Any) -> Optional[date]:
    for candidate in _iter_mapping_candidates(payload):
        for key in _PREFLIGHT_DATE_KEYS:
            if key not in candidate:
                continue
            parsed = _parse_mountain_datetime_value(candidate.get(key))
            if parsed:
                return parsed.date()
        for container_key in ("dep", "departure", "flightDetails", "detailsDeparture"):
            nested = candidate.get(container_key)
            if isinstance(nested, Mapping):
                for key in _PREFLIGHT_DATE_KEYS:
                    if key not in nested:
                        continue
                    parsed = _parse_mountain_datetime_value(nested.get(key))
                    if parsed:
                        return parsed.date()
    return None


def _extract_leg_flight_id(payload: Any) -> Optional[str]:
    for candidate in _iter_mapping_candidates(payload):
        flight_id = _extract_flight_identifier(candidate)
        if flight_id:
            return flight_id
    return None


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
                matched_account = _find_fuzzy_account_match(partner_normalized, account_display)
                partner_present = matched_account is not None
                partner_match = account_display.get(matched_account) if matched_account else None

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


def run_syndicate_quote_audit(
    config: Fl3xxApiConfig,
    *,
    quote_id: str,
    session: Optional[requests.Session] = None,
) -> SyndicateQuoteAuditResult:
    diagnostics: Dict[str, Any] = {
        "preflight_requests": 0,
        "preflight_errors": 0,
        "leg_requests": 0,
        "flight_date_found": False,
        "syndicate_matches": 0,
        "partner_flights": 0,
    }
    warnings: List[str] = []
    matches: List[SyndicateQuoteMatch] = []
    partner_flights: List[SyndicateQuoteFlight] = []
    flight_id: Optional[str] = None
    flight_date: Optional[date] = None
    owner_account: Optional[str] = None

    http = session or requests.Session()
    close_session = session is None
    try:
        diagnostics["leg_requests"] += 1
        leg_payload = fetch_leg_details(config, quote_id, session=http)
        flight_id = _extract_leg_flight_id(leg_payload)
        if not flight_id:
            warnings.append(f"Quote {quote_id}: no flightId found in leg payload.")
            return SyndicateQuoteAuditResult(
                quote_id=quote_id,
                flight_id=None,
                flight_date=None,
                owner_account=None,
                matches=[],
                partner_flights=[],
                diagnostics=diagnostics,
                warnings=warnings,
            )

        diagnostics["preflight_requests"] += 1
        try:
            preflight_payload = fetch_preflight(config, flight_id, session=http)
        except Exception as exc:
            diagnostics["preflight_errors"] += 1
            warnings.append(f"Flight {flight_id}: unable to fetch preflight: {exc}")
            return SyndicateQuoteAuditResult(
                quote_id=quote_id,
                flight_id=flight_id,
                flight_date=None,
                owner_account=None,
                matches=[],
                partner_flights=[],
                diagnostics=diagnostics,
                warnings=warnings,
            )

        booking_notes = _extract_booking_notes(preflight_payload) or ""
        flight_date = _extract_preflight_date(preflight_payload)
        diagnostics["flight_date_found"] = flight_date is not None

        syndicate_matches = _extract_syndicate_matches(booking_notes) if booking_notes else []
        diagnostics["syndicate_matches"] = len(syndicate_matches)
        for match in syndicate_matches:
            syndicate_tail_type = _extract_tail_type_label(match.tail_type or match.note_line)
            matches.append(
                SyndicateQuoteMatch(
                    partner_account=match.partner_name,
                    partner_present=False,
                    partner_match=None,
                    note_type=match.note_type,
                    note_line=match.note_line,
                    syndicate_tail_type=syndicate_tail_type,
                )
            )

        if not flight_date:
            warnings.append(f"Flight {flight_id}: unable to determine flight date from preflight payload.")
            return SyndicateQuoteAuditResult(
                quote_id=quote_id,
                flight_id=flight_id,
                flight_date=None,
                owner_account=None,
                matches=matches,
                partner_flights=[],
                diagnostics=diagnostics,
                warnings=warnings,
            )

        flights, metadata = fetch_flights(
            config,
            from_date=flight_date,
            to_date=flight_date + timedelta(days=1),
            session=http,
        )
        diagnostics["fetch_metadata"] = metadata

        normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
        filtered_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
        targeted_rows = _filter_rows_for_target_date(filtered_rows, flight_date)
        diagnostics["normalization_stats"] = normalization_stats
        diagnostics["skipped_subcharter"] = skipped_subcharter

        pax_rows = [row for row in targeted_rows if _is_pax_flight(row)]
        account_rows: Dict[str, List[Mapping[str, Any]]] = {}
        account_display: Dict[str, str] = {}
        for row in pax_rows:
            account_name = _extract_account_name(row)
            if not account_name:
                continue
            normalized_account = _normalize_name(account_name)
            if not normalized_account:
                continue
            account_rows.setdefault(normalized_account, []).append(row)
            account_display.setdefault(normalized_account, account_name)

        owner_row = next(
            (row for row in pax_rows if _extract_flight_identifier(row) == str(flight_id)),
            None,
        )
        if owner_row:
            owner_account = _extract_account_name(owner_row)

        if matches:
            for entry in matches:
                partner_normalized = _normalize_name(entry.partner_account)
                matched_account = _find_fuzzy_account_match(partner_normalized, account_display)
                entry.partner_present = matched_account is not None
                entry.partner_match = account_display.get(matched_account) if matched_account else None

                if matched_account and matched_account in account_rows:
                    for row in account_rows[matched_account]:
                        flight_identifier = _extract_flight_identifier(row) or ""
                        partner_flights.append(
                            SyndicateQuoteFlight(
                                account=account_display.get(matched_account, matched_account),
                                booking_reference=_extract_booking_reference(row) or flight_identifier,
                                flight_id=flight_identifier,
                                aircraft_type=_extract_aircraft_type(row) or "",
                                workflow=_extract_workflow_label(row) or "",
                                tail=_extract_tail(row),
                                route=_format_route(row),
                                dep_time=_parse_mountain_datetime(row.get("dep_time")),
                            )
                        )
            diagnostics["partner_flights"] = len(partner_flights)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return SyndicateQuoteAuditResult(
        quote_id=quote_id,
        flight_id=flight_id,
        flight_date=flight_date,
        owner_account=owner_account,
        matches=matches,
        partner_flights=partner_flights,
        diagnostics=diagnostics,
        warnings=warnings,
    )


__all__ = [
    "SyndicateAuditEntry",
    "SyndicateAuditResult",
    "SyndicateQuoteAuditResult",
    "SyndicateQuoteMatch",
    "SyndicateQuoteFlight",
    "run_syndicate_audit",
    "run_syndicate_quote_audit",
]
