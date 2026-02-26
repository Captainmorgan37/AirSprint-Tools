"""HOTAC coverage monitoring helpers for the Hotel Check dashboard."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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

    return {
        "person_id": person_id,
        "name": full_name,
        "first_name": first_name or None,
        "last_name": last_name or None,
        "personnel": str(member.get("personnelNumber") or "").strip() or None,
        "trigram": str(member.get("trigram") or "").strip() or None,
        "role": role,
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
        }
    return list(deduped.values())


def _status_from_hotac_records(records: Sequence[Mapping[str, Any]]) -> Tuple[str, Optional[str], str]:
    if not records:
        return "Missing", None, "No matching arrival HOTAC record for pilot"

    has_ok = False
    cancelled_only = True
    company: Optional[str] = None
    itinerary_missing = False

    for record in records:
        status = _normalize_status(record.get("status"))
        service = record.get("hotacService") if isinstance(record.get("hotacService"), Mapping) else {}

        if status == "OK":
            has_ok = True
            cancelled_only = False
            company_value = service.get("company") if isinstance(service, Mapping) else None
            if isinstance(company_value, str) and company_value.strip():
                company = company_value.strip()

            documents = record.get("documents")
            if not (isinstance(documents, list) and len(documents) > 0):
                itinerary_missing = True

        elif status in CANCELLED_STATUSES:
            continue
        else:
            cancelled_only = False

    if has_ok:
        if itinerary_missing:
            return "Booked", company, "HOTAC OK but itinerary/documents missing"
        return "Booked", company, "HOTAC OK"

    if cancelled_only:
        return "Cancelled-only", company, "All matching HOTAC records are cancelled"

    return "Unknown", company, "Unrecognized HOTAC statuses for pilot"


def _rank_status(status: str) -> int:
    order = {"Missing": 0, "Cancelled-only": 1, "Unknown": 2, "Home base": 3, "Booked": 4}
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
                }
            )
        if events:
            events_by_personnel[personnel] = sorted(
                events,
                key=lambda event: event.get("from_utc") or datetime.min.replace(tzinfo=UTC),
            )
    return events_by_personnel


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

    if isinstance(arrival_utc, datetime):
        after_arrival = [
            event
            for event in matching
            if isinstance(event.get("from_utc"), datetime) and event["from_utc"] >= arrival_utc
        ]
        if after_arrival:
            return after_arrival[0]
    return matching[0]


def _extract_hotel_from_positioning_notes(notes: str) -> str:
    normalized = notes.replace("\\n", "\n")
    for line in normalized.splitlines():
        cleaned = line.strip()
        if cleaned.casefold().startswith("hotel:"):
            return cleaned.split(":", 1)[1].strip()
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

    roster_window_start = datetime.combine(target_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12)
    roster_window_end = roster_window_start + timedelta(days=1)
    roster_events_by_personnel: Dict[str, List[Dict[str, Any]]] = {}
    troubleshooting_rows: List[Dict[str, Any]] = []
    should_fetch_roster = roster_fetcher is not None or bool(config.api_token or config.auth_header)
    if should_fetch_roster:
        try:
            roster_rows = fetch_roster(config, roster_window_start, roster_window_end)
            roster_events_by_personnel = _extract_roster_positioning_events(roster_rows)
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

    rows: List[Dict[str, Any]] = []

    for leg in pilot_last_leg.values():
        flight_id = leg.get("flight_id")
        pilot = leg.get("pilot", {})
        profile_home_base_airport = ""
        positioning_route = ""

        status = "Unknown"
        company = ""
        notes = "Unable to evaluate HOTAC"

        if flight_id is None:
            notes = "Missing flight ID for final leg"
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
                    if pilot_person_id and end_airport and _is_canadian_airport(end_airport):
                        try:
                            crew_member_payload = fetch_crew_member_details(config, pilot_person_id)
                            home_airport_icao = _extract_home_airport_icao(crew_member_payload)
                            profile_home_base_airport = home_airport_icao or ""
                            if home_airport_icao and home_airport_icao == end_airport:
                                status = "Home base"
                                notes = f"Pilot ending at home base ({home_airport_icao})"
                        except Exception as exc:
                            troubleshooting_rows.append(
                                {
                                    "Flight ID": flight_id,
                                    "Tail": leg.get("tail") or "",
                                    "Issue": "Unable to fetch pilot home airport",
                                    "Details": str(exc),
                            }
                        )

                    pilot_personnel = _normalize_id(pilot.get("personnel"))
                    positioning_events = roster_events_by_personnel.get(pilot_personnel or "", [])
                    positioning_event = _find_positioning_event_for_leg(
                        positioning_events,
                        end_airport,
                        leg.get("arr_utc"),
                    )
                    if positioning_event:
                        reposition_from = str(positioning_event.get("from_airport") or "").strip().upper()
                        reposition_to = str(positioning_event.get("to_airport") or "").strip().upper()
                        if reposition_from and reposition_to:
                            positioning_route = f"{reposition_from}-{reposition_to}"
                        elif reposition_to:
                            positioning_route = f"{end_airport}-{reposition_to}"
                        if reposition_to:
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
