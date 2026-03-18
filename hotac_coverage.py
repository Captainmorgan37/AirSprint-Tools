"""HOTAC coverage monitoring helpers for the Hotel Check dashboard."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd
import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    fetch_crew_member,
    fetch_flight_crew,
    fetch_flight_services,
    fetch_flights,
    fetch_staff_roster,
)
from flight_leg_utils import safe_parse_dt
from zoneinfo_compat import ZoneInfo

UTC = timezone.utc
CANCELLED_STATUSES = {"CNL", "CANCELED", "CANCELLED"}
PILOT_ROLES = {"CMD", "FO", "PIC", "SIC", "CAPTAIN", "COPILOT"}

CrewFetcher = Callable[[Fl3xxApiConfig, Any], List[Dict[str, Any]]]
ServicesFetcher = Callable[[Fl3xxApiConfig, Any], Any]
CrewMemberFetcher = Callable[[Fl3xxApiConfig, Any], Any]
RosterFetcher = Callable[[Fl3xxApiConfig, datetime, datetime], List[Dict[str, Any]]]


def _normalize_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _canonical_id(value: Any) -> Optional[str]:
    """Return a comparison-friendly ID representation.

    FL3XX payloads can expose person identifiers in different formats
    (e.g., `"395655"`, `395655`, or values with leading labels). We keep
    exact matching first, then fall back to a digits-only comparison key.
    """

    normalized = _normalize_id(value)
    if not normalized:
        return None
    digits_only = "".join(ch for ch in normalized if ch.isdigit())
    return digits_only or normalized


def _extract_arrival_hotac_records(services_payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], str]:
    """Return candidate arrival HOTAC records and the source path used."""

    candidate_paths: Sequence[Tuple[str, ...]] = (
        ("arrivalHotac",),
        ("arrivalHOTAC",),
        ("arrival_hotac",),
        ("arrival", "hotac"),
        ("arrival", "hotacs"),
        ("arr", "hotac"),
        ("arr", "hotacs"),
        ("flightDetails", "arr", "hotac"),
        ("flightDetails", "arr", "hotacs"),
        ("hotac",),
        ("hotacs",),
    )

    for path in candidate_paths:
        cursor: Any = services_payload
        for segment in path:
            if not isinstance(cursor, Mapping):
                cursor = None
                break
            cursor = cursor.get(segment)

        if isinstance(cursor, list):
            return [item for item in cursor if isinstance(item, Mapping)], ".".join(path)

    return [], "none"


def _extract_person_identifiers(record: Mapping[str, Any]) -> Dict[str, Optional[str]]:
    person = record.get("person") if isinstance(record.get("person"), Mapping) else {}
    user = record.get("user") if isinstance(record.get("user"), Mapping) else {}
    crew = record.get("crew") if isinstance(record.get("crew"), Mapping) else {}
    pilot = record.get("pilot") if isinstance(record.get("pilot"), Mapping) else {}

    return {
        "id": _normalize_id(
            person.get("id")
            or person.get("userId")
            or person.get("personId")
            or person.get("crewId")
            or user.get("id")
            or user.get("userId")
            or crew.get("id")
            or crew.get("userId")
            or pilot.get("id")
            or pilot.get("userId")
            or record.get("userId")
            or record.get("personId")
            or record.get("crewId")
            or record.get("id")
        ),
        "personnel": _normalize_id(
            record.get("personnelNumber")
            or person.get("personnelNumber")
            or user.get("personnelNumber")
            or crew.get("personnelNumber")
            or pilot.get("personnelNumber")
        ),
        "trigram": _normalize_id(
            record.get("trigram")
            or person.get("trigram")
            or user.get("trigram")
            or crew.get("trigram")
            or pilot.get("trigram")
        ),
        "role": _normalize_status(
            record.get("pilotRole")
            or record.get("role")
            or record.get("crewPosition")
            or person.get("pilotRole")
            or person.get("role")
        ),
    }


def _normalize_status(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _extract_timestamp(payload: Mapping[str, Any], keys: Sequence[str]) -> Optional[datetime]:
    for key in keys:
        value = _extract_nested_value(payload, key)
        if value in (None, ""):
            continue
        try:
            return safe_parse_dt(str(value)).astimezone(UTC)
        except Exception:
            continue
    return None


def _extract_nested_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload.get(key)

    segments = key.split(".")
    cursor: Any = payload
    for segment in segments:
        if not isinstance(cursor, Mapping):
            return None
        cursor = cursor.get(segment)
    return cursor


def _extract_airport(payload: Mapping[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue

        if isinstance(value, Mapping):
            for nested in ("icao", "iata", "code", "airport"):
                nested_value = value.get(nested)
                if isinstance(nested_value, str) and nested_value.strip():
                    return nested_value.strip().upper()
            continue

        if isinstance(value, str) and value.strip():
            return value.strip().upper()

    return None


def _extract_tail(flight: Mapping[str, Any]) -> str:
    for key in (
        "tail",
        "tailNumber",
        "aircraftReg",
        "aircraftRegistration",
        "registration",
        "registrationNumber",
        "flightDetails.tail",
        "flightDetails.tailNumber",
    ):
        value = _extract_nested_value(flight, key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()

    aircraft = flight.get("aircraft")
    if isinstance(aircraft, Mapping):
        for key in ("tail", "registration", "reg"):
            value = aircraft.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()

    return ""


_PLACEHOLDER_PREFIXES = {"ADD", "REMOVE"}


def _is_add_remove_line(flight: Mapping[str, Any]) -> bool:
    for key in ("tail", "tailNumber", "flightNumber", "flightNo", "flightNumberCompany", "number", "tripNumber"):
        value = _extract_nested_value(flight, key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        first_word = text.split()[0].upper()
        if first_word in _PLACEHOLDER_PREFIXES:
            return True
    return False


def _extract_flight_number(flight: Mapping[str, Any]) -> str:
    def _is_missing_segment(segment: str) -> bool:
        return segment.strip().casefold() in {"", "null", "none", "nan"}

    booking_identifier = _extract_nested_value(flight, "bookingIdentifier")
    if booking_identifier not in (None, ""):
        booking_text = str(booking_identifier).strip()
        if booking_text:
            return booking_text

    for key in (
        "flightNumber",
        "flightNo",
        "flightNumberCompany",
        "number",
        "tripNumber",
        "flightDetails.flightNumber",
        "flightDetails.flightNo",
    ):
        value = _extract_nested_value(flight, key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if not text:
            continue
        parts = [part.strip() for part in text.split("-")]
        if any(_is_missing_segment(part) for part in parts) or _is_missing_segment(text):
            continue
        return text
    return ""


def _is_pilot_member(member: Mapping[str, Any]) -> bool:
    pilot_flag = member.get("pilot")
    if isinstance(pilot_flag, bool):
        return pilot_flag

    role = str(member.get("role") or "").strip().upper()
    if role in PILOT_ROLES:
        return True

    seat = str(member.get("seat") or "").strip().upper()
    if seat in PILOT_ROLES:
        return True

    return False


def _normalize_pilot_member(member: Mapping[str, Any]) -> Dict[str, Any]:
    role = str(member.get("role") or member.get("seat") or "").strip().upper() or None
    person_block = member.get("person") if isinstance(member.get("person"), Mapping) else {}
    person_id = _normalize_id(
        member.get("id")
        or member.get("pilotId")
        or member.get("userId")
        or member.get("personId")
        or member.get("crewId")
        or person_block.get("id")
        or person_block.get("userId")
        or person_block.get("personId")
    )

    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    if not first_name and not last_name:
        full_name = str(member.get("name") or member.get("logName") or "").strip()
    else:
        full_name = " ".join(part for part in (first_name, last_name) if part).strip()

    if not full_name:
        full_name = str(member.get("email") or member.get("trigram") or "Unknown pilot").strip() or "Unknown pilot"

    home_base_airport = _extract_home_airport_icao(member)
    if not home_base_airport and person_block:
        home_base_airport = _extract_home_airport_icao(person_block)

    return {
        "person_id": person_id,
        "name": full_name,
        "first_name": first_name or None,
        "last_name": last_name or None,
        "personnel": str(member.get("personnelNumber") or "").strip() or None,
        "trigram": str(member.get("trigram") or "").strip() or None,
        "role": role,
        "home_base_airport": home_base_airport,
    }


def _extract_pilot_members_from_flight_payload(flight: Mapping[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Mapping[str, Any]] = []
    for key in ("crew", "crewMembers", "assignedCrew", "staff"):
        maybe_list = flight.get(key)
        if isinstance(maybe_list, list):
            for member in maybe_list:
                if isinstance(member, Mapping):
                    candidates.append(member)

    pilots = [_normalize_pilot_member(member) for member in candidates if _is_pilot_member(member)]
    return _dedupe_pilots(pilots)


def _dedupe_pilots(pilots: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pilot in pilots:
        name = str(pilot.get("name") or "Unknown pilot").strip() or "Unknown pilot"
        person_id = _normalize_id(pilot.get("person_id")) or ""
        key = (person_id, name)
        deduped[key] = {
            "person_id": person_id or None,
            "name": name,
            "first_name": pilot.get("first_name"),
            "last_name": pilot.get("last_name"),
            "personnel": pilot.get("personnel"),
            "trigram": pilot.get("trigram"),
            "role": pilot.get("role"),
            "home_base_airport": pilot.get("home_base_airport"),
        }
    return list(deduped.values())




def _extract_hotac_company(record: Mapping[str, Any]) -> Optional[str]:
    service = record.get("hotacService") if isinstance(record.get("hotacService"), Mapping) else {}

    for source in (record, service):
        for key in ("company", "hotelCompany", "hotel_company", "provider", "hotelProvider"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None

def _status_from_hotac_records(records: Sequence[Mapping[str, Any]]) -> Tuple[str, Optional[str], str]:
    if not records:
        return "Missing", None, "No matching arrival HOTAC record for pilot"

    has_ok = False
    cancelled_only = True
    company: Optional[str] = None
    itinerary_missing = False
    unrecognized_details: List[str] = []

    for record in records:
        status = _normalize_status(record.get("status"))
        company_value = _extract_hotac_company(record)
        if status not in CANCELLED_STATUSES and company_value and (status == "OK" or not company):
            company = company_value

        if status == "OK":
            has_ok = True
            cancelled_only = False

            documents = record.get("documents")
            if not (isinstance(documents, list) and len(documents) > 0):
                itinerary_missing = True

        elif status in CANCELLED_STATUSES:
            continue
        else:
            cancelled_only = False
            detail_note = _extract_hotac_unrecognized_note(record)
            if detail_note:
                unrecognized_details.append(f"{status} - {detail_note}")
            elif status:
                unrecognized_details.append(status)

    if has_ok:
        if itinerary_missing:
            return "Booked", company, "HOTAC OK but itinerary/documents missing"
        return "Booked", company, "HOTAC OK"

    if cancelled_only:
        return "Cancelled-only", company, "All matching HOTAC records are cancelled"

    if unrecognized_details:
        return "Unsure - unconfirmed status", company, "; ".join(unrecognized_details)
    return "Unsure - unconfirmed status", company, "Unrecognized HOTAC statuses for pilot"


def _extract_hotac_unrecognized_note(record: Mapping[str, Any]) -> Optional[str]:
    service = record.get("hotacService") if isinstance(record.get("hotacService"), Mapping) else {}

    for source in (record, service):
        for key in ("note", "notes", "remark", "remarks", "comment", "comments"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _extract_hotac_unrecognized_note(record: Mapping[str, Any]) -> Optional[str]:
    service = record.get("hotacService") if isinstance(record.get("hotacService"), Mapping) else {}

    for source in (record, service):
        for key in ("note", "notes", "remark", "remarks", "comment", "comments"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _rank_status(status: str) -> int:
    order = {
        "Missing": 0,
        "Unsure - crew based at CYUL and may be staying at home": 1,
        "Unsure - unconfirmed status": 2,
        "Cancelled-only": 3,
        "Unknown": 4,
        "Home base": 5,
        "Booked": 6,
    }
    return order.get(status, 9)


def _is_canadian_airport(airport_code: str) -> bool:
    code = airport_code.strip().upper()
    if len(code) == 4:
        return code.startswith("C")
    if len(code) == 3:
        return code.startswith("Y")
    return False


def _extract_home_airport_icao(crew_payload: Any) -> Optional[str]:
    if not isinstance(crew_payload, Mapping):
        return None

    home_airport = crew_payload.get("homeAirport")
    if isinstance(home_airport, Mapping):
        icao = home_airport.get("icao")
        if isinstance(icao, str) and icao.strip():
            return icao.strip().upper()

    for key in ("homeAirportIcao", "homeBaseIcao"):
        value = crew_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()

    return None



def _extract_roster_home_base_airports(
    roster_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, str]:
    home_airports_by_personnel: Dict[str, str] = {}
    for row in roster_rows:
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        personnel = _normalize_id(user.get("personnelNumber"))
        if not personnel:
            continue

        home_airport = _extract_home_airport_icao(user)
        if not home_airport:
            home_airport = _extract_home_airport_icao(row)
        if home_airport:
            home_airports_by_personnel[personnel] = home_airport

    return home_airports_by_personnel


def _extract_roster_positioning_events(
    roster_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    events_by_personnel: Dict[str, List[Dict[str, Any]]] = {}
    for row in roster_rows:
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        personnel = _normalize_id(user.get("personnelNumber"))
        entries = row.get("entries") if isinstance(row.get("entries"), list) else []
        if not personnel:
            continue
        events: List[Dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            if _normalize_status(entry.get("type")) != "P":
                continue
            from_airport = _extract_airport(entry, ["fromAirport"])
            to_airport = _extract_airport(entry, ["toAirport"])
            from_ms = entry.get("from")
            to_ms = entry.get("to")
            from_utc = datetime.fromtimestamp(from_ms / 1000, tz=UTC) if isinstance(from_ms, (int, float)) else None
            to_utc = datetime.fromtimestamp(to_ms / 1000, tz=UTC) if isinstance(to_ms, (int, float)) else None
            notes = str(entry.get("notes") or "").strip()
            events.append(
                {
                    "from_airport": from_airport or "",
                    "to_airport": to_airport or "",
                    "from_utc": from_utc,
                    "to_utc": to_utc,
                    "notes": notes,
                    "ends_duty_period": bool(entry.get("endsDutyPeriod")),
                }
            )
        if events:
            events_by_personnel[personnel] = sorted(
                events,
                key=lambda event: event.get("from_utc") or datetime.min.replace(tzinfo=UTC),
            )
    return events_by_personnel


def _events_overlap(
    first_start: Optional[datetime],
    first_end: Optional[datetime],
    second_start: Optional[datetime],
    second_end: Optional[datetime],
) -> bool:
    if not all(isinstance(value, datetime) for value in (first_start, first_end, second_start, second_end)):
        return False
    return max(first_start, second_start) < min(first_end, second_end)



def _roster_row_has_overlapping_available_day(row: Mapping[str, Any]) -> bool:
    entries = row.get("entries") if isinstance(row.get("entries"), list) else []
    positioning_entries = [
        entry for entry in entries if isinstance(entry, Mapping) and _normalize_status(entry.get("type")) == "P"
    ]
    available_entries = [
        entry for entry in entries if isinstance(entry, Mapping) and _normalize_status(entry.get("type")) == "A"
    ]
    for positioning_entry in positioning_entries:
        positioning_from_ms = positioning_entry.get("from")
        positioning_to_ms = positioning_entry.get("to")
        positioning_from_utc = (
            datetime.fromtimestamp(positioning_from_ms / 1000, tz=UTC)
            if isinstance(positioning_from_ms, (int, float))
            else None
        )
        positioning_to_utc = (
            datetime.fromtimestamp(positioning_to_ms / 1000, tz=UTC)
            if isinstance(positioning_to_ms, (int, float))
            else None
        )
        for available_entry in available_entries:
            available_from_ms = available_entry.get("from")
            available_to_ms = available_entry.get("to")
            available_from_utc = (
                datetime.fromtimestamp(available_from_ms / 1000, tz=UTC)
                if isinstance(available_from_ms, (int, float))
                else None
            )
            available_to_utc = (
                datetime.fromtimestamp(available_to_ms / 1000, tz=UTC)
                if isinstance(available_to_ms, (int, float))
                else None
            )
            if _events_overlap(positioning_from_utc, positioning_to_utc, available_from_utc, available_to_utc):
                return True
    return False


def _extract_roster_positioning_only_pilots(
    roster_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    pilots_by_personnel: Dict[str, Dict[str, Any]] = {}
    for row in roster_rows:
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        personnel = _normalize_id(user.get("personnelNumber"))
        if not personnel:
            continue

        flights = row.get("flights")
        has_scheduled_flights = isinstance(flights, list) and len(flights) > 0
        if has_scheduled_flights:
            continue

        entries = row.get("entries") if isinstance(row.get("entries"), list) else []
        has_positioning_entry = any(
            isinstance(entry, Mapping) and _normalize_status(entry.get("type")) == "P" for entry in entries
        )
        if not has_positioning_entry:
            continue

        first_name = str(user.get("firstName") or "").strip()
        last_name = str(user.get("lastName") or "").strip()
        name = " ".join(part for part in (first_name, last_name) if part).strip()
        if not name:
            name = str(user.get("logName") or user.get("name") or user.get("email") or personnel).strip()

        pilots_by_personnel[personnel] = {
            "person_id": _normalize_id(
                user.get("id")
                or user.get("userId")
                or user.get("personId")
                or user.get("internalId")
                or user.get("externalReference")
            ),
            "crew_lookup_id": _normalize_id(user.get("internalId"))
            or _normalize_id(user.get("id") or user.get("userId") or user.get("personId")),
            "personnel": personnel,
            "trigram": _normalize_id(user.get("trigram") or user.get("acronym")),
            "name": name or "Unknown pilot",
            "first_name": first_name or None,
            "last_name": last_name or None,
            "role": _normalize_status(
                user.get("pilotRole") or user.get("role") or user.get("rank") or user.get("function")
            )
            or None,
            "home_base_airport": _extract_home_airport_icao(user) or _extract_home_airport_icao(row),
        }

    return pilots_by_personnel


def _find_positioning_event_for_leg(
    events: Sequence[Mapping[str, Any]],
    end_airport: str,
    arrival_utc: Optional[datetime],
) -> Optional[Mapping[str, Any]]:
    airport = end_airport.strip().upper()
    if not airport:
        return None

    matching = [event for event in events if str(event.get("from_airport") or "").strip().upper() == airport]
    if not matching:
        return None

    def _prefer_duty_end(candidates: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
        duty_ending = [event for event in candidates if bool(event.get("ends_duty_period"))]
        if duty_ending:
            return duty_ending[0]
        return candidates[0] if candidates else None

    if isinstance(arrival_utc, datetime):
        after_arrival = [
            event
            for event in matching
            if isinstance(event.get("from_utc"), datetime) and event["from_utc"] >= arrival_utc
        ]
        preferred = _prefer_duty_end(after_arrival)
        if preferred is not None:
            return preferred

    preferred = _prefer_duty_end(matching)
    if preferred is not None:
        return preferred
    return matching[0]


_POSITIONING_HOTEL_KEYWORDS = (
    "hotel",
    "hilton",
    "doubletree",
    "ramada",
    "hampton",
    "wyndham",
    "inn",
    "suite",
    "suites",
    "lodge",
)


def _extract_hotel_from_positioning_notes(notes: str) -> str:
    normalized = notes.replace("\\n", "\n")
    keyword_pattern = re.compile(
        r"\b(?:" + "|".join(re.escape(keyword) for keyword in _POSITIONING_HOTEL_KEYWORDS) + r")\b",
        re.IGNORECASE,
    )
    for line in normalized.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        if cleaned.casefold().startswith("hotel:"):
            return cleaned.split(":", 1)[1].strip()
        if keyword_pattern.search(cleaned):
            return cleaned
    return ""


def compute_hotac_coverage(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    flights: Optional[Iterable[Mapping[str, Any]]] = None,
    crew_fetcher: Optional[CrewFetcher] = None,
    services_fetcher: Optional[ServicesFetcher] = None,
    crew_member_fetcher: Optional[CrewMemberFetcher] = None,
    roster_fetcher: Optional[RosterFetcher] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return HOTAC coverage display, raw, and troubleshooting DataFrames."""

    if flights is None:
        flights_payload, _metadata = fetch_flights(
            config,
            from_date=target_date,
            to_date=target_date + timedelta(days=1),
        )
        fetched_flights = list(flights_payload)
    else:
        fetched_flights = list(flights)

    fetch_crew = crew_fetcher or fetch_flight_crew
    fetch_services = services_fetcher or fetch_flight_services
    fetch_crew_member_details = crew_member_fetcher or fetch_crew_member
    fetch_roster = roster_fetcher or (
        lambda conf, from_time, to_time: fetch_staff_roster(conf, from_time=from_time, to_time=to_time)
    )

    roster_window_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8)
    roster_window_end = roster_window_start + timedelta(days=1)
    roster_events_by_personnel: Dict[str, List[Dict[str, Any]]] = {}
    roster_home_base_by_personnel: Dict[str, str] = {}
    roster_positioning_only_pilots: Dict[str, Dict[str, Any]] = {}
    roster_positioning_only_with_a_day: Set[str] = set()
    troubleshooting_rows: List[Dict[str, Any]] = []
    should_fetch_roster = roster_fetcher is not None or bool(config.api_token or config.auth_header)
    if should_fetch_roster:
        try:
            roster_rows = fetch_roster(config, roster_window_start, roster_window_end)
            roster_events_by_personnel = _extract_roster_positioning_events(roster_rows)
            roster_home_base_by_personnel = _extract_roster_home_base_airports(roster_rows)
            roster_positioning_only_pilots = _extract_roster_positioning_only_pilots(roster_rows)
            roster_positioning_only_with_a_day = {
                _normalize_id((row.get("user") if isinstance(row.get("user"), Mapping) else {}).get("personnelNumber"))
                for row in roster_rows
                if isinstance(row, Mapping) and _roster_row_has_overlapping_available_day(row)
            }
        except Exception as exc:
            troubleshooting_rows.append(
                {
                    "Flight ID": "",
                    "Tail": "",
                    "Issue": "Unable to fetch staff roster",
                    "Details": str(exc),
                }
            )

    pilot_last_leg: Dict[str, Dict[str, Any]] = {}

    for index, flight in enumerate(fetched_flights):
        if _is_add_remove_line(flight):
            continue

        flight_id = flight.get("flightId") or flight.get("id")
        tail = _extract_tail(flight)

        dep_utc = _extract_timestamp(
            flight,
            [
                "departureTimeUtc",
                "dep_time",
                "depTime",
                "departureUtc",
                "scheduledOutUtc",
                "scheduledOut",
                "departureTime",
                "detailsDeparture.scheduledOut",
                "detailsDeparture.scheduledOutUtc",
                "flightDetails.dep.scheduledOut",
                "flightDetails.dep.scheduledOutUtc",
            ],
        )
        arr_utc = _extract_timestamp(
            flight,
            [
                "arrivalTimeUtc",
                "arrival_time",
                "arrTime",
                "arrivalUtc",
                "scheduledInUtc",
                "scheduledIn",
                "arrivalTime",
                "detailsArrival.scheduledIn",
                "detailsArrival.scheduledInUtc",
                "flightDetails.arr.scheduledIn",
                "flightDetails.arr.scheduledInUtc",
            ],
        )

        crew_payload: List[Dict[str, Any]] = []
        if flight_id is not None:
            try:
                crew_payload = fetch_crew(config, flight_id) or []
            except Exception as exc:
                troubleshooting_rows.append(
                    {
                        "Flight ID": flight_id,
                        "Tail": tail,
                        "Issue": "Unable to fetch crew roster",
                        "Details": str(exc),
                    }
                )

        pilots = _dedupe_pilots(_normalize_pilot_member(member) for member in crew_payload if _is_pilot_member(member))
        if not pilots:
            pilots = _extract_pilot_members_from_flight_payload(flight)

        if not pilots:
            troubleshooting_rows.append(
                {
                    "Flight ID": flight_id or "",
                    "Tail": tail,
                    "Issue": "No pilot crew found for flight",
                    "Details": "Crew endpoint and flight payload both returned no pilot assignments.",
                }
            )
            continue

        end_airport = _extract_airport(flight, ["arrivalAirport", "arr_airport", "airportTo", "toAirport"])
        flight_number = _extract_flight_number(flight)

        for pilot in pilots:
            pilot_key = _normalize_id(pilot.get("person_id")) or f"name::{pilot.get('name', '')}"
            sort_key = (
                arr_utc or datetime.min.replace(tzinfo=UTC),
                dep_utc or datetime.min.replace(tzinfo=UTC),
                index,
            )

            current = pilot_last_leg.get(pilot_key)
            if current is None or sort_key >= current["_sort_key"]:
                pilot_last_leg[pilot_key] = {
                    "pilot": pilot,
                    "flight_id": flight_id,
                    "flight_number": flight_number,
                    "tail": tail,
                    "end_airport": end_airport or "",
                    "dep_utc": dep_utc,
                    "arr_utc": arr_utc,
                    "_sort_key": sort_key,
                }

    for personnel, pilot in roster_positioning_only_pilots.items():
        if personnel not in roster_positioning_only_with_a_day:
            continue
        positioning_events = roster_events_by_personnel.get(personnel, [])
        if not positioning_events:
            continue

        duty_ending_events = [event for event in positioning_events if bool(event.get("ends_duty_period"))]
        final_event = duty_ending_events[-1] if duty_ending_events else positioning_events[-1]
        final_from = str(final_event.get("from_airport") or "").strip().upper()
        final_departure = final_event.get("from_utc")
        sort_key = (
            final_departure if isinstance(final_departure, datetime) else datetime.min.replace(tzinfo=UTC),
            final_departure if isinstance(final_departure, datetime) else datetime.min.replace(tzinfo=UTC),
            len(fetched_flights),
        )

        candidate_leg = {
            "pilot": pilot,
            "flight_id": None,
            "flight_number": "",
            "tail": "",
            "end_airport": final_from,
            "dep_utc": final_departure,
            "arr_utc": final_departure,
            "ends_duty_period": bool(final_event.get("ends_duty_period")),
            "_sort_key": sort_key,
        }

        pilot_key = _normalize_id(pilot.get("person_id")) or f"personnel::{personnel}"
        matching_existing_key = None
        for existing_key, existing_leg in pilot_last_leg.items():
            existing_personnel = _normalize_id(existing_leg.get("pilot", {}).get("personnel"))
            if existing_personnel and existing_personnel == personnel:
                matching_existing_key = existing_key
                break

        if matching_existing_key is not None:
            existing_leg = pilot_last_leg[matching_existing_key]
            existing_dep = existing_leg.get("dep_utc")
            candidate_dep = candidate_leg.get("dep_utc")
            should_prefer_candidate = candidate_leg["_sort_key"] >= existing_leg.get(
                "_sort_key", (datetime.min.replace(tzinfo=UTC),) * 3
            )
            if (
                not should_prefer_candidate
                and bool(candidate_leg.get("ends_duty_period"))
                and isinstance(candidate_dep, datetime)
                and isinstance(existing_dep, datetime)
                and existing_dep > candidate_dep
            ):
                should_prefer_candidate = True

            if should_prefer_candidate:
                pilot_last_leg[matching_existing_key] = candidate_leg
            continue

        existing_leg_for_key = pilot_last_leg.get(pilot_key)
        if existing_leg_for_key is not None:
            existing_dep = existing_leg_for_key.get("dep_utc")
            candidate_dep = candidate_leg.get("dep_utc")
            should_prefer_candidate = candidate_leg["_sort_key"] >= existing_leg_for_key.get(
                "_sort_key", (datetime.min.replace(tzinfo=UTC),) * 3
            )
            if (
                not should_prefer_candidate
                and bool(candidate_leg.get("ends_duty_period"))
                and isinstance(candidate_dep, datetime)
                and isinstance(existing_dep, datetime)
                and existing_dep > candidate_dep
            ):
                should_prefer_candidate = True

            if should_prefer_candidate:
                pilot_last_leg[pilot_key] = candidate_leg
            continue

        pilot_last_leg[pilot_key] = candidate_leg

    rows: List[Dict[str, Any]] = []

    for leg in pilot_last_leg.values():
        flight_id = leg.get("flight_id")
        pilot = leg.get("pilot", {})
        profile_home_base_airport = str(pilot.get("home_base_airport") or "").strip().upper()
        if not profile_home_base_airport:
            pilot_personnel = _normalize_id(pilot.get("personnel"))
            profile_home_base_airport = str(roster_home_base_by_personnel.get(pilot_personnel or "") or "").strip().upper()
        positioning_route = ""

        status = "Unknown"
        company = ""
        notes = "Unable to evaluate HOTAC"

        if flight_id is None:
            status = "Missing"
            notes = "No scheduled flight; evaluating roster positioning note only"

            end_airport = str(leg.get("end_airport") or "").strip().upper()
            pilot_personnel = _normalize_id(pilot.get("personnel"))
            positioning_events = roster_events_by_personnel.get(pilot_personnel or "", [])
            positioning_event = _find_positioning_event_for_leg(
                positioning_events,
                end_airport,
                leg.get("arr_utc"),
            )
            reposition_to = ""
            if positioning_event:
                reposition_from = str(positioning_event.get("from_airport") or "").strip().upper()
                reposition_to = str(positioning_event.get("to_airport") or "").strip().upper()
                if reposition_from and reposition_to:
                    positioning_route = f"{reposition_from}-{reposition_to}"
                elif reposition_to:
                    positioning_route = f"{end_airport}-{reposition_to}"

                should_lookup_roster_only_home_base = (
                    reposition_to
                    and (not profile_home_base_airport or profile_home_base_airport != reposition_to)
                    and (_is_canadian_airport(end_airport) or _is_canadian_airport(reposition_to))
                )
                if should_lookup_roster_only_home_base:
                    lookup_ids: List[str] = []
                    pilot_crew_lookup_id = _normalize_id(pilot.get("crew_lookup_id"))
                    pilot_person_id = _normalize_id(pilot.get("person_id"))
                    pilot_personnel = _normalize_id(pilot.get("personnel"))
                    for candidate in (pilot_crew_lookup_id, pilot_person_id, pilot_personnel):
                        if candidate and candidate not in lookup_ids:
                            lookup_ids.append(candidate)

                    for lookup_id in lookup_ids:
                        try:
                            crew_member_payload = fetch_crew_member_details(config, lookup_id)
                            looked_up_home_airport = _extract_home_airport_icao(crew_member_payload)
                            if looked_up_home_airport:
                                profile_home_base_airport = looked_up_home_airport
                                break
                        except Exception as exc:
                            troubleshooting_rows.append(
                                {
                                    "Flight ID": "",
                                    "Tail": leg.get("tail") or "",
                                    "Issue": "Unable to fetch pilot home airport",
                                    "Details": f"lookup_id={lookup_id}: {exc}",
                                }
                            )

                hotel_note = _extract_hotel_from_positioning_notes(str(positioning_event.get("notes") or ""))
                if profile_home_base_airport and reposition_to and reposition_to == profile_home_base_airport:
                    status = "Home base"
                    notes = f"Positioned to home base ({reposition_to})"
                elif hotel_note:
                    status = "Booked"
                    notes = f"Positioning hotel note: {hotel_note}"
                elif reposition_to:
                    notes = f"Positioned {end_airport} → {reposition_to}; hotel required at {reposition_to}"
                else:
                    notes = "Positioning event found without destination airport"
            else:
                notes = "No matching roster positioning event found"
        else:
            try:
                services_payload = fetch_services(config, flight_id)
                if not isinstance(services_payload, Mapping):
                    raise ValueError("Malformed services payload")

                arrival_hotac, hotac_source = _extract_arrival_hotac_records(services_payload)

                pilot_person_id = _normalize_id(pilot.get("person_id"))
                pilot_person_id_key = _canonical_id(pilot_person_id)
                pilot_personnel = _normalize_id(pilot.get("personnel"))
                pilot_trigram = _normalize_id(pilot.get("trigram"))
                pilot_role = _normalize_status(pilot.get("role"))
                pilot_first = _normalize_id(pilot.get("first_name"))
                pilot_last = _normalize_id(pilot.get("last_name"))
                pilot_name = _normalize_id(pilot.get("name"))
                matching_records: List[Mapping[str, Any]] = []

                for item in arrival_hotac:
                    identifiers = _extract_person_identifiers(item)
                    item_person_id = identifiers.get("id")
                    item_person_id_key = _canonical_id(item_person_id)
                    item_personnel = _normalize_id(identifiers.get("personnel"))
                    item_trigram = _normalize_id(identifiers.get("trigram"))
                    item_role = _normalize_status(identifiers.get("role"))
                    person = item.get("person") if isinstance(item.get("person"), Mapping) else {}
                    item_first = _normalize_id(person.get("firstName"))
                    item_last = _normalize_id(person.get("lastName"))
                    item_name = _normalize_id(
                        " ".join(part for part in (item_first, item_last) if part) or person.get("name")
                    )

                    id_match = pilot_person_id and (
                        item_person_id == pilot_person_id
                        or (
                            pilot_person_id_key
                            and item_person_id_key
                            and item_person_id_key == pilot_person_id_key
                        )
                    )
                    personnel_match = bool(
                        pilot_personnel and item_personnel and pilot_personnel == item_personnel
                    )
                    trigram_match = bool(
                        pilot_trigram and item_trigram and pilot_trigram.upper() == item_trigram.upper()
                    )
                    role_only_match = bool(
                        pilot_role
                        and item_role
                        and pilot_role == item_role
                        and not item_person_id
                        and not item_personnel
                        and not item_trigram
                    )
                    name_match = bool(
                        (pilot_first and item_first and pilot_first.casefold() == item_first.casefold())
                        and (pilot_last and item_last and pilot_last.casefold() == item_last.casefold())
                    ) or bool(
                        pilot_name and item_name and pilot_name.casefold() == item_name.casefold()
                    )

                    if id_match or personnel_match or trigram_match or role_only_match or name_match:
                        matching_records.append(item)

                status, company_value, notes = _status_from_hotac_records(matching_records)
                company = company_value or ""
                if status == "Missing":
                    notes = (
                        f"No matched HOTAC in {hotac_source} "
                        f"(arrival HOTAC records={len(arrival_hotac)}; pilot_id={pilot_person_id or 'n/a'})"
                    )
                    end_airport = str(leg.get("end_airport") or "").strip().upper()

                    pilot_personnel = _normalize_id(pilot.get("personnel"))
                    positioning_events = roster_events_by_personnel.get(pilot_personnel or "", [])
                    positioning_event = _find_positioning_event_for_leg(
                        positioning_events,
                        end_airport,
                        leg.get("arr_utc"),
                    )
                    reposition_to = ""
                    if positioning_event:
                        reposition_from = str(positioning_event.get("from_airport") or "").strip().upper()
                        reposition_to = str(positioning_event.get("to_airport") or "").strip().upper()
                        if reposition_from and reposition_to:
                            positioning_route = f"{reposition_from}-{reposition_to}"
                        elif reposition_to:
                            positioning_route = f"{end_airport}-{reposition_to}"

                    if profile_home_base_airport and end_airport:
                        if profile_home_base_airport == end_airport:
                            status = "Home base"
                            notes = f"Pilot ending at home base ({profile_home_base_airport})"
                        elif end_airport == "CYHU" and profile_home_base_airport == "CYUL":
                            status = "Unsure - crew based at CYUL and may be staying at home"
                            notes = "Crew ended at CYHU and is CYUL based; may be staying at home"

                    should_lookup_home_base = (
                        status == "Missing"
                        and pilot_person_id
                        and end_airport
                        and (
                            _is_canadian_airport(end_airport)
                            or (reposition_to and _is_canadian_airport(reposition_to))
                        )
                    )
                    if should_lookup_home_base:
                        try:
                            crew_member_payload = fetch_crew_member_details(config, pilot_person_id)
                            home_airport_icao = _extract_home_airport_icao(crew_member_payload)
                            profile_home_base_airport = home_airport_icao or ""
                            if home_airport_icao and home_airport_icao == end_airport:
                                status = "Home base"
                                notes = f"Pilot ending at home base ({home_airport_icao})"
                            elif end_airport == "CYHU" and home_airport_icao == "CYUL":
                                status = "Unsure - crew based at CYUL and may be staying at home"
                                notes = "Crew ended at CYHU and is CYUL based; may be staying at home"
                        except Exception as exc:
                            troubleshooting_rows.append(
                                {
                                    "Flight ID": flight_id,
                                    "Tail": leg.get("tail") or "",
                                    "Issue": "Unable to fetch pilot home airport",
                                    "Details": str(exc),
                            }
                        )

                    if positioning_event and reposition_to:
                        notes = f"Positioning note found: {end_airport} → {reposition_to}"
                        if profile_home_base_airport and reposition_to == profile_home_base_airport:
                            status = "Home base"
                            notes = f"Positioned to home base ({reposition_to})"
                        else:
                            hotel_note = _extract_hotel_from_positioning_notes(str(positioning_event.get("notes") or ""))
                            if hotel_note:
                                status = "Booked"
                                notes = f"Positioning hotel note: {hotel_note}"
                            else:
                                notes = f"Positioned {end_airport} → {reposition_to}; hotel required at {reposition_to}"

            except requests.HTTPError as exc:
                status = "Unknown"
                notes = f"Services API error: {exc}"
            except Exception as exc:
                status = "Unknown"
                notes = f"Services parse error: {exc}"

        end_airport = str(leg.get("end_airport") or "")
        rows.append(
            {
                "Pilot": pilot.get("name") or "Unknown pilot",
                "Personnel/Trigram": pilot.get("personnel") or pilot.get("trigram") or "",
                "Tail": leg.get("tail") or "",
                "Flight": leg.get("flight_number") or "",
                "Flight ID": leg.get("flight_id") or "",
                "End airport": end_airport,
                "Positioning route": positioning_route,
                "Profile home base": profile_home_base_airport,
                "HOTAC status": status,
                "Hotel company": company,
                "Notes": notes,
            }
        )

    raw_df = pd.DataFrame(rows)
    if raw_df.empty:
        display_df = raw_df.copy()
    else:
        display_df = raw_df.sort_values(
            by=["HOTAC status", "Tail", "Pilot"],
            key=lambda series: series.map(_rank_status) if series.name == "HOTAC status" else series,
        ).reset_index(drop=True)

    troubleshooting_df = pd.DataFrame(troubleshooting_rows)
    return display_df, raw_df, troubleshooting_df


__all__ = ["compute_hotac_coverage", "_status_from_hotac_records"]
