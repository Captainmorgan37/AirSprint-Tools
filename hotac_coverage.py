"""HOTAC coverage monitoring helpers for the Hotel Check dashboard."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import requests

from fl3xx_api import Fl3xxApiConfig, fetch_flight_services, fetch_flights
from flight_leg_utils import load_airport_tz_lookup, safe_parse_dt
from zoneinfo_compat import ZoneInfo

UTC = timezone.utc
CANCELLED_STATUSES = {"CNL", "CANCELED", "CANCELLED"}


CrewEntry = Dict[str, Any]


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
            for nested in ("icao", "iata", "code", "airport", "name"):
                nested_val = value.get(nested)
                if isinstance(nested_val, str) and nested_val.strip():
                    return nested_val.strip().upper()
            continue
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


def _extract_tail(flight: Mapping[str, Any]) -> Optional[str]:
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
    return None


def _extract_flight_number(flight: Mapping[str, Any]) -> Optional[str]:
    for key in ("flightNumber", "flightNo", "number", "tripNumber"):
        value = flight.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _extract_crew_members(flight: Mapping[str, Any]) -> List[CrewEntry]:
    candidates: List[Any] = []
    for key in ("crew", "crewMembers", "assignedCrew", "staff"):
        block = flight.get(key)
        if isinstance(block, list):
            candidates.extend(block)

    extracted: List[CrewEntry] = []
    for member in candidates:
        if not isinstance(member, Mapping):
            continue

        pilot_flag = member.get("pilot")
        role = str(member.get("role") or "").strip().upper()
        if pilot_flag is False:
            continue
        if pilot_flag is None and role and role not in {"CMD", "FO", "PIC", "SIC", "CAPTAIN", "COPILOT"}:
            continue

        person = member.get("person") if isinstance(member.get("person"), Mapping) else {}

        person_id = _normalize_id(
            person.get("id") if isinstance(person, Mapping) else None
        ) or _normalize_id(member.get("id")) or _normalize_id(member.get("userId"))

        first_name = ""
        last_name = ""
        if isinstance(person, Mapping):
            first_name = str(person.get("firstName") or "").strip()
            last_name = str(person.get("lastName") or "").strip()

        if not first_name:
            first_name = str(member.get("firstName") or "").strip()
        if not last_name:
            last_name = str(member.get("lastName") or "").strip()

        name_parts = [part for part in (first_name, last_name) if part]
        name = " ".join(name_parts).strip()
        if not name:
            for key in ("name", "logName", "email", "trigram"):
                value = member.get(key)
                if isinstance(value, str) and value.strip():
                    name = value.strip()
                    break

        extracted.append(
            {
                "person_id": person_id,
                "name": name or "Unknown pilot",
                "trigram": str(member.get("trigram") or "").strip() or None,
                "personnel": str(member.get("personnelNumber") or "").strip() or None,
                "role": role or None,
            }
        )

    unique: Dict[Tuple[Optional[str], str], CrewEntry] = {}
    for pilot in extracted:
        key = (pilot.get("person_id"), pilot.get("name", ""))
        unique[key] = pilot
    return list(unique.values())


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
            company_val = service.get("company") if isinstance(service, Mapping) else None
            if isinstance(company_val, str) and company_val.strip():
                company = company_val.strip()

            docs = record.get("documents")
            docs_attached = isinstance(docs, list) and len(docs) > 0
            if not docs_attached:
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
    order = {
        "Missing": 0,
        "Cancelled-only": 1,
        "Unknown": 2,
        "Booked": 3,
    }
    return order.get(status, 9)


def _local_time_label(dt_utc: Optional[datetime], airport: Optional[str], tz_lookup: Mapping[str, str]) -> str:
    if not isinstance(dt_utc, datetime):
        return ""
    tz_name = tz_lookup.get((airport or "").upper()) if airport else None
    if tz_name:
        try:
            local_dt = dt_utc.astimezone(ZoneInfo(tz_name))
            return local_dt.strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            pass
    return dt_utc.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def compute_hotac_coverage(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    flights: Optional[Iterable[Mapping[str, Any]]] = None,
    services_fetcher: Optional[Callable[[Fl3xxApiConfig, Any], Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute HOTAC coverage rows and troubleshooting diagnostics for a date."""

    fetched_flights: List[Mapping[str, Any]]
    if flights is None:
        fetched_flights, _ = fetch_flights(
            config,
            from_date=target_date,
            to_date=target_date + timedelta(days=1),
        )
    else:
        fetched_flights = list(flights)

    fetch_services = services_fetcher or fetch_flight_services
    tz_lookup = load_airport_tz_lookup()

    pilot_legs: Dict[str, Dict[str, Any]] = {}
    troubleshooting_rows: List[Dict[str, Any]] = []

    for index, flight in enumerate(fetched_flights):
        flight_id = flight.get("flightId") or flight.get("id")
        dep_utc = _extract_timestamp(
            flight,
            [
                "departureTimeUtc",
                "dep_time",
                "depTime",
                "departureUtc",
                "scheduledOutUtc",
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
            ],
        )

        crew = _extract_crew_members(flight)
        if not crew:
            troubleshooting_rows.append(
                {
                    "Flight ID": flight_id or "",
                    "Tail": _extract_tail(flight) or "",
                    "Issue": "No pilot crew found on flight payload",
                }
            )
            continue

        for pilot in crew:
            person_key = _normalize_id(pilot.get("person_id")) or f"name::{pilot.get('name')}"
            current = pilot_legs.get(person_key)
            comparison = (
                arr_utc or datetime.min.replace(tzinfo=UTC),
                dep_utc or datetime.min.replace(tzinfo=UTC),
                index,
            )
            if current is None or comparison >= current["_sort_key"]:
                pilot_legs[person_key] = {
                    "pilot": pilot,
                    "flight": flight,
                    "flight_id": flight_id,
                    "tail": _extract_tail(flight),
                    "flight_number": _extract_flight_number(flight),
                    "end_airport": _extract_airport(
                        flight,
                        ["arrivalAirport", "arr_airport", "airportTo", "toAirport"],
                    ),
                    "dep_utc": dep_utc,
                    "arr_utc": arr_utc,
                    "_sort_key": comparison,
                }

    rows: List[Dict[str, Any]] = []

    for entry in pilot_legs.values():
        flight_id = entry.get("flight_id")
        pilot = entry.get("pilot", {})

        status = "Unknown"
        company = None
        notes = "Unable to evaluate HOTAC"
        try:
            services_payload = fetch_services(config, flight_id)
            if not isinstance(services_payload, Mapping):
                raise ValueError("Malformed services payload")

            arrival_hotac = services_payload.get("arrivalHotac")
            if not isinstance(arrival_hotac, list):
                arrival_hotac = []

            person_id = _normalize_id(pilot.get("person_id"))
            matching: List[Mapping[str, Any]] = []
            for item in arrival_hotac:
                if not isinstance(item, Mapping):
                    continue
                person = item.get("person") if isinstance(item.get("person"), Mapping) else {}
                item_person_id = _normalize_id(person.get("id") if isinstance(person, Mapping) else None)
                if person_id and item_person_id == person_id:
                    matching.append(item)

            status, company, notes = _status_from_hotac_records(matching)

        except requests.HTTPError as exc:
            status = "Unknown"
            notes = f"Services API error: {exc}"
        except Exception as exc:
            status = "Unknown"
            notes = f"Services parse error: {exc}"

        rows.append(
            {
                "Pilot": pilot.get("name") or "Unknown pilot",
                "Personnel/Trigram": pilot.get("personnel") or pilot.get("trigram") or "",
                "Tail": entry.get("tail") or "",
                "Flight": entry.get("flight_number") or "",
                "Flight ID": entry.get("flight_id") or "",
                "End airport": entry.get("end_airport") or "",
                "End ETD (local)": _local_time_label(entry.get("dep_utc"), entry.get("end_airport"), tz_lookup),
                "End ETA (local)": _local_time_label(entry.get("arr_utc"), entry.get("end_airport"), tz_lookup),
                "HOTAC status": status,
                "Hotel company": company or "",
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
