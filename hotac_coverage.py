"""HOTAC coverage monitoring helpers for the Hotel Check dashboard."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import requests

from fl3xx_api import (
    Fl3xxApiConfig,
    fetch_flight_crew,
    fetch_flight_services,
    fetch_flights,
)
from flight_leg_utils import load_airport_tz_lookup, safe_parse_dt
from zoneinfo_compat import ZoneInfo

UTC = timezone.utc
CANCELLED_STATUSES = {"CNL", "CANCELED", "CANCELLED"}
PILOT_ROLES = {"CMD", "FO", "PIC", "SIC", "CAPTAIN", "COPILOT"}

CrewFetcher = Callable[[Fl3xxApiConfig, Any], List[Dict[str, Any]]]
ServicesFetcher = Callable[[Fl3xxApiConfig, Any], Any]


def _normalize_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_status(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _extract_timestamp(payload: Mapping[str, Any], keys: Sequence[str]) -> Optional[datetime]:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return safe_parse_dt(str(value)).astimezone(UTC)
        except Exception:
            continue
    return None


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
    for key in ("tail", "aircraftReg", "registration", "tailNumber"):
        value = flight.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()

    aircraft = flight.get("aircraft")
    if isinstance(aircraft, Mapping):
        for key in ("tail", "registration", "reg"):
            value = aircraft.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()

    return ""


def _extract_flight_number(flight: Mapping[str, Any]) -> str:
    for key in ("flightNumber", "flightNo", "number", "tripNumber"):
        value = flight.get(key)
        if value not in (None, ""):
            return str(value)
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
    person_id = _normalize_id(member.get("id") or member.get("userId") or member.get("personId"))

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
    order = {"Missing": 0, "Cancelled-only": 1, "Unknown": 2, "Booked": 3}
    return order.get(status, 9)


def _local_time_label(dt_utc: Optional[datetime], airport_code: str, tz_lookup: Mapping[str, str]) -> str:
    if not isinstance(dt_utc, datetime):
        return ""

    tz_name = tz_lookup.get(airport_code.upper()) if airport_code else None
    if tz_name:
        try:
            return dt_utc.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            pass
    return dt_utc.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def compute_hotac_coverage(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    flights: Optional[Iterable[Mapping[str, Any]]] = None,
    crew_fetcher: Optional[CrewFetcher] = None,
    services_fetcher: Optional[ServicesFetcher] = None,
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
    tz_lookup = load_airport_tz_lookup()

    pilot_last_leg: Dict[str, Dict[str, Any]] = {}
    troubleshooting_rows: List[Dict[str, Any]] = []

    for index, flight in enumerate(fetched_flights):
        flight_id = flight.get("flightId") or flight.get("id")
        tail = _extract_tail(flight)

        dep_utc = _extract_timestamp(
            flight,
            ["departureTimeUtc", "dep_time", "depTime", "departureUtc", "scheduledOutUtc"],
        )
        arr_utc = _extract_timestamp(
            flight,
            ["arrivalTimeUtc", "arrival_time", "arrTime", "arrivalUtc", "scheduledInUtc"],
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

                arrival_hotac = services_payload.get("arrivalHotac")
                if not isinstance(arrival_hotac, list):
                    arrival_hotac = []

                pilot_person_id = _normalize_id(pilot.get("person_id"))
                matching_records: List[Mapping[str, Any]] = []

                for item in arrival_hotac:
                    if not isinstance(item, Mapping):
                        continue
                    person = item.get("person")
                    item_person_id = None
                    if isinstance(person, Mapping):
                        item_person_id = _normalize_id(person.get("id"))

                    if pilot_person_id and item_person_id == pilot_person_id:
                        matching_records.append(item)

                status, company_value, notes = _status_from_hotac_records(matching_records)
                company = company_value or ""

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
                "End ETD (local)": _local_time_label(leg.get("dep_utc"), end_airport, tz_lookup),
                "End ETA (local)": _local_time_label(leg.get("arr_utc"), end_airport, tz_lookup),
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
            by=["HOTAC status", "Pilot"],
            key=lambda series: series.map(_rank_status) if series.name == "HOTAC status" else series,
        ).reset_index(drop=True)

    troubleshooting_df = pd.DataFrame(troubleshooting_rows)
    return display_df, raw_df, troubleshooting_df


__all__ = ["compute_hotac_coverage", "_status_from_hotac_records"]
