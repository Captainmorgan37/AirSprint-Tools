"""Shared helpers for normalising FL3XX flight legs and airport metadata."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd
import pytz

from fl3xx_api import (
    DEFAULT_FL3XX_BASE_URL,
    DutySnapshot,
    DutySnapshotPilot,
    Fl3xxApiConfig,
    MOUNTAIN_TIME_ZONE,
    enrich_flights_with_crew,
    fetch_flights,
    fetch_postflight,
    parse_postflight_payload,
)

UTC = timezone.utc
AIRPORT_TZ_FILENAME = "Airport TZ.txt"
DEPARTURE_AIRPORT_COLUMNS: Sequence[str] = (
    "departure_airport",
    "dep_airport",
    "departureAirport",
    "departure_airport_code",
    "airportFrom",
    "fromAirport",
)
ARRIVAL_AIRPORT_COLUMNS: Sequence[str] = (
    "arrival_airport",
    "arr_airport",
    "arrivalAirport",
    "arrival_airport_code",
    "airportTo",
    "toAirport",
)


class FlightDataError(RuntimeError):
    """Raised when flight data cannot be fetched or normalised."""


def safe_parse_dt(dt_str: str) -> datetime:
    """Parse ISO-like datetime strings, defaulting to UTC when naive."""

    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=pytz.UTC)
        return dt
    except Exception:
        dt = pd.to_datetime(dt_str, utc=True).to_pydatetime()
        return dt


def format_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def compute_departure_window_bounds(
    target_date: date,
    *,
    start_time: time,
    end_time: time,
) -> Tuple[datetime, datetime]:
    start = datetime.combine(target_date, start_time)
    end_date = target_date + timedelta(days=1)
    end = datetime.combine(end_date, end_time)
    return start, end


def filter_rows_by_departure_window(
    rows: List[Dict[str, Any]],
    start_utc: datetime,
    end_utc: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    stats = {
        "total": len(rows),
        "within_window": 0,
        "before_window": 0,
        "after_window": 0,
    }
    if not rows:
        return [], stats

    filtered: List[Dict[str, Any]] = []

    for row in rows:
        dep_raw = row.get("dep_time")
        if dep_raw is None:
            stats["before_window"] += 1
            continue
        dt = safe_parse_dt(str(dep_raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        if dt < start_utc:
            stats["before_window"] += 1
            continue
        if dt > end_utc:
            stats["after_window"] += 1
            continue
        filtered.append(row)
        stats["within_window"] += 1

    return filtered, stats


def compute_mountain_day_window_utc(target_date: date) -> Tuple[datetime, datetime]:
    """Return the UTC bounds covering a local Mountain Time calendar day."""

    start_local = datetime.combine(target_date, time(0, 0)).replace(tzinfo=MOUNTAIN_TIME_ZONE)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)
    return start_utc, end_utc


def _airport_tz_path(filename: str = AIRPORT_TZ_FILENAME) -> Path:
    this_file = Path(__file__).resolve()

    for directory in this_file.parents:
        candidate = directory / filename
        if candidate.exists():
            return candidate

    cwd_candidate = Path.cwd() / filename
    if cwd_candidate.exists():
        return cwd_candidate

    return this_file.with_name(filename)


@lru_cache(maxsize=1)
def load_airport_metadata_lookup() -> Dict[str, Dict[str, Optional[str]]]:
    path = _airport_tz_path()
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    lookup: Dict[str, Dict[str, Optional[str]]] = {}
    for _, row in df.iterrows():
        tz: Optional[str] = None
        tz_value = row.get("tz")
        if isinstance(tz_value, str) and tz_value.strip():
            tz = tz_value.strip()

        country: Optional[str] = None
        country_value = row.get("country")
        if isinstance(country_value, str) and country_value.strip():
            country = country_value.strip()

        if tz is None and country is None:
            continue
        for key in ("icao", "iata", "lid"):
            code_value = row.get(key)
            if isinstance(code_value, str) and code_value.strip():
                lookup[code_value.strip().upper()] = {
                    "tz": tz,
                    "country": country,
                }
    return lookup


@lru_cache(maxsize=1)
def load_airport_tz_lookup() -> Dict[str, str]:
    metadata = load_airport_metadata_lookup()
    tz_lookup: Dict[str, str] = {}
    for code, record in metadata.items():
        if isinstance(record, Mapping):
            tz_value = record.get("tz")
            if isinstance(tz_value, str) and tz_value:
                tz_lookup[code] = tz_value
    return tz_lookup


def _extract_codes(value: Any) -> List[str]:
    if not isinstance(value, str):
        return []
    cleaned = value.strip()
    if not cleaned:
        return []
    upper = cleaned.upper()
    if upper.replace(" ", "").isalnum() and len(upper.strip()) in {3, 4}:
        return [upper]
    return [token.upper() for token in re.findall(r"\b[A-Za-z0-9]{3,4}\b", upper)]


def _airport_country_from_row(
    row: Mapping[str, Any],
    columns: Sequence[str],
    lookup: Dict[str, Dict[str, Optional[str]]],
) -> Optional[str]:
    for column in columns:
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        for code in _extract_codes(str(value)):
            record = lookup.get(code)
            if not record:
                continue
            country = record.get("country") if isinstance(record, Mapping) else None
            if isinstance(country, str) and country.strip():
                return country.strip()
    return None


def is_customs_leg(
    row: Mapping[str, Any],
    lookup: Optional[Dict[str, Dict[str, Optional[str]]]] = None,
) -> bool:
    if lookup is None:
        lookup = load_airport_metadata_lookup()
    if not lookup:
        return False
    dep_country = _airport_country_from_row(row, DEPARTURE_AIRPORT_COLUMNS, lookup)
    arr_country = _airport_country_from_row(row, ARRIVAL_AIRPORT_COLUMNS, lookup)
    if dep_country and arr_country:
        return dep_country != arr_country
    return False


def apply_airport_timezones(df: pd.DataFrame) -> Tuple[pd.DataFrame, Set[str], bool]:
    if df.empty:
        return df, set(), False
    if "dep_tz" not in df.columns:
        df["dep_tz"] = None

    lookup = load_airport_tz_lookup()
    lookup_used = bool(lookup)

    missing: Set[str] = set()

    def _needs_timezone(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, float) and pd.isna(val):
            return True
        if isinstance(val, str) and not val.strip():
            return True
        return False

    for idx, row in df.iterrows():
        if not _needs_timezone(row.get("dep_tz")):
            continue
        airport_value: Optional[str] = None
        for col in DEPARTURE_AIRPORT_COLUMNS:
            if col in df.columns and not pd.isna(row.get(col)):
                airport_value = str(row[col])
                if airport_value:
                    break
        if not airport_value:
            continue
        codes = _extract_codes(airport_value)
        tz_guess = None
        if lookup_used:
            tz_guess = next((lookup.get(code) for code in codes if code in lookup), None)
        if tz_guess:
            df.at[idx, "dep_tz"] = tz_guess
        else:
            missing.add(airport_value)

    return df, missing, lookup_used


def normalize_fl3xx_payload(payload: Any) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    def _iterable_items(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "items", "flights", "legs"):
                nested = data.get(key)
                if isinstance(nested, list):
                    return nested
        return []

    items = _iterable_items(payload)
    if not items and isinstance(payload, dict):
        items = [payload]
    elif not items and isinstance(payload, list):
        items = payload

    normalized: List[Dict[str, Any]] = []
    stats = {
        "flights_processed": len(items),
        "candidate_legs": 0,
        "legs_normalized": 0,
        "skipped_missing_tail": 0,
        "skipped_missing_dep_time": 0,
    }
    for flight in items:
        legs: List[Dict[str, Any]] = []
        if isinstance(flight, dict):
            legs_data = flight.get("legs")
            if isinstance(legs_data, list) and legs_data:
                legs = legs_data
            else:
                legs = [flight]
        elif isinstance(flight, list):
            legs = [leg for leg in flight if isinstance(leg, dict)]
        else:
            continue

        flight_tail = flight if isinstance(flight, dict) else {}

        for leg in legs:
            stats["candidate_legs"] += 1
            tail = _extract_first(
                leg,
                "tail",
                "tailNumber",
                "tail_number",
                "aircraft",
                "aircraftRegistration",
                "registrationNumber",
                "registration",
            )
            if not tail and isinstance(flight_tail, dict):
                tail = _extract_first(
                    flight_tail,
                    "tail",
                    "tailNumber",
                    "tail_number",
                    "aircraft",
                    "aircraftRegistration",
                    "registrationNumber",
                    "registration",
                )
            if isinstance(tail, dict):
                tail = _extract_first(
                    tail,
                    "registrationNumber",
                    "registration",
                    "tailNumber",
                    "tail",
                    "name",
                )
            if not tail:
                stats["skipped_missing_tail"] += 1
                continue

            leg_id = _extract_first(
                leg,
                "id",
                "legId",
                "leg_id",
                "uuid",
                "externalId",
                "external_id",
                "legNumber",
                "number",
            )
            if leg_id is None and isinstance(flight_tail, dict):
                leg_id = _extract_first(
                    flight_tail,
                    "id",
                    "legId",
                    "leg_id",
                    "uuid",
                    "externalId",
                    "external_id",
                    "legNumber",
                    "number",
                )

            dep_time = _extract_first(
                leg,
                "departureTimeUtc",
                "departure_time_utc",
                "departureTime",
                "departure_time",
                "offBlockTimeUtc",
                "scheduledTimeUtc",
                "scheduled_departure_utc",
                "blockOffEstUTC",
                "blockOffUtc",
                "scheduledOffBlockUtc",
                "blockOffTimeUtc",
                "blockOffActualUTC",
                "scheduledDepartureTime",
                "scheduledDeparture",
                "startTime",
                "departUtc",
                "dep_time",
                "offBlockTime",
            )
            if not dep_time and isinstance(flight_tail, dict):
                dep_time = _extract_first(
                    flight_tail,
                    "departureTimeUtc",
                    "departure_time_utc",
                    "departureTime",
                    "departure_time",
                    "scheduledDepartureTime",
                    "scheduledDeparture",
                )
            if not dep_time:
                stats["skipped_missing_dep_time"] += 1
                continue

            dep_tz = _extract_first(
                leg,
                "departureTimezone",
                "departureTimeZone",
                "departure_timezone",
                "departure_tz",
                "departureTimeZoneName",
                "blockOffTimeZone",
                "offBlockTimeZone",
                "dep_tz",
            )
            if not dep_tz and isinstance(leg.get("departure"), Mapping):
                dep_tz = _extract_first(leg["departure"], "timezone", "timeZone")
            if not dep_tz and isinstance(flight_tail, dict):
                departure = flight_tail.get("departure")
                if isinstance(departure, Mapping):
                    dep_tz = _extract_first(departure, "timezone", "timeZone")

            arr_time = _extract_first(
                leg,
                "arrivalTimeUtc",
                "arrival_time_utc",
                "arrivalTime",
                "arrival_time",
                "arrivalUtc",
                "arrivalUTC",
                "arrivalOnBlockUtc",
                "arrivalOnBlock",
                "arrivalActualUtc",
                "arrivalScheduledUtc",
                "arrivalActualTime",
                "arrivalScheduledTime",
                "arrivalActual",
                "arrivalScheduled",
                "scheduledIn",
                "actualIn",
                "onBlockTimeUtc",
                "onBlockUtc",
                "onBlockTime",
                "blockOnTimeUtc",
                "blockOnUtc",
                "blockOnTime",
                "blockOnEstUTC",
                "blockOnEstUtc",
                "blockOnEstimatedUTC",
                "blockOnEstimatedUtc",
                "blockOnEstimateUTC",
                "blockOnEstimateUtc",
            )
            if not arr_time and isinstance(leg.get("arrival"), Mapping):
                arr_time = _extract_first(
                    leg["arrival"],
                    "actualUtc",
                    "scheduledUtc",
                    "actualTime",
                    "scheduledTime",
                    "actual",
                    "scheduled",
                )
            if not arr_time and isinstance(leg.get("times"), Mapping):
                times = leg["times"]
                if isinstance(times.get("arrival"), Mapping):
                    arr_time = _extract_first(
                        times["arrival"],
                        "actualUtc",
                        "scheduledUtc",
                        "actualTime",
                        "scheduledTime",
                        "actual",
                        "scheduled",
                    )
                if arr_time is None:
                    arr_time = _extract_first(
                        times,
                        "arrival",
                        "arrivalUtc",
                        "arrivalActual",
                        "arrivalScheduled",
                        "arrivalActualUtc",
                        "arrivalScheduledUtc",
                    )
            if not arr_time and isinstance(flight_tail, dict):
                arr_time = _extract_first(
                    flight_tail,
                    "arrivalTimeUtc",
                    "arrival_time_utc",
                    "arrivalTime",
                    "arrival_time",
                    "arrivalUtc",
                    "arrivalUTC",
                    "arrivalOnBlockUtc",
                    "arrivalOnBlock",
                    "arrivalActualUtc",
                    "arrivalScheduledUtc",
                    "arrivalActualTime",
                    "arrivalScheduledTime",
                    "arrivalActual",
                    "arrivalScheduled",
                    "scheduledIn",
                    "actualIn",
                    "onBlockTimeUtc",
                    "onBlockUtc",
                    "onBlockTime",
                    "blockOnTimeUtc",
                    "blockOnUtc",
                    "blockOnTime",
                    "blockOnEstUTC",
                    "blockOnEstUtc",
                    "blockOnEstimatedUTC",
                    "blockOnEstimatedUtc",
                    "blockOnEstimateUTC",
                    "blockOnEstimateUtc",
                )
                if (
                    arr_time is None
                    and isinstance(flight_tail.get("arrival"), Mapping)
                ):
                    arr_time = _extract_first(
                        flight_tail["arrival"],
                        "actualUtc",
                        "scheduledUtc",
                        "actualTime",
                        "scheduledTime",
                        "actual",
                        "scheduled",
                    )
            def _coerce_name(container: Mapping[str, Any], *keys: str) -> Optional[str]:
                value = _extract_first(container, *keys)
                if value is None:
                    return None
                text = str(value).strip()
                return text or None

            pic_name = _coerce_name(
                leg,
                "picName",
                "pic",
                "pic_name",
                "captainName",
                "captain",
            )
            if not pic_name and isinstance(flight_tail, dict):
                pic_name = _coerce_name(
                    flight_tail,
                    "picName",
                    "pic",
                    "pic_name",
                    "captainName",
                    "captain",
                )

            sic_name = _coerce_name(
                leg,
                "sicName",
                "sic",
                "foName",
                "firstOfficer",
                "first_officer",
            )
            if not sic_name and isinstance(flight_tail, dict):
                sic_name = _coerce_name(
                    flight_tail,
                    "sicName",
                    "sic",
                    "foName",
                    "firstOfficer",
                    "first_officer",
                )

            workflow_custom_name = _extract_first(
                leg,
                "workflowCustomName",
                "workflow_custom_name",
                "workflowName",
                "workflow",
            )
            if not workflow_custom_name and isinstance(flight_tail, dict):
                workflow_custom_name = _extract_first(
                    flight_tail,
                    "workflowCustomName",
                    "workflow_custom_name",
                    "workflowName",
                    "workflow",
                )

            aircraft_category = _extract_first(
                leg,
                "aircraftCategory",
                "aircraft_category",
                "aircraftType",
                "aircraftClass",
            )
            if not aircraft_category and isinstance(leg.get("aircraft"), Mapping):
                aircraft_category = _extract_first(
                    leg["aircraft"],
                    "category",
                    "type",
                    "aircraftType",
                    "aircraftClass",
                )
            if (
                not aircraft_category
                and isinstance(flight_tail, dict)
                and isinstance(flight_tail.get("aircraft"), Mapping)
            ):
                aircraft_category = _extract_first(
                    flight_tail["aircraft"],
                    "category",
                    "type",
                    "aircraftType",
                    "aircraftClass",
                )

            assigned_aircraft_type = _extract_first(
                leg,
                "assignedAircraftType",
                "assigned_aircraft_type",
                "requestedAircraftType",
                "aircraftTypeAssigned",
                "aircraftTypeName",
            )
            if not assigned_aircraft_type and isinstance(leg.get("aircraft"), Mapping):
                assigned_aircraft_type = _extract_first(
                    leg["aircraft"],
                    "assignedType",
                    "requestedType",
                    "typeName",
                )
            if (
                not assigned_aircraft_type
                and isinstance(flight_tail, dict)
                and isinstance(flight_tail.get("aircraft"), Mapping)
            ):
                assigned_aircraft_type = _extract_first(
                    flight_tail["aircraft"],
                    "assignedType",
                    "requestedType",
                    "typeName",
                )
            if not assigned_aircraft_type and isinstance(flight_tail, dict):
                assigned_aircraft_type = _extract_first(
                    flight_tail,
                    "assignedAircraftType",
                    "assigned_aircraft_type",
                    "requestedAircraftType",
                    "aircraftTypeAssigned",
                    "aircraftTypeName",
                )

            owner_class = _extract_first(
                leg,
                "ownerClass",
                "owner_class",
                "ownerClassification",
                "owner_classification",
                "ownerType",
                "ownerTypeName",
                "ownerClassName",
                "aircraftOwnerClass",
            )
            if not owner_class and isinstance(leg.get("owner"), Mapping):
                owner_class = _extract_first(
                    leg["owner"],
                    "class",
                    "classification",
                    "type",
                    "name",
                )
            if (
                not owner_class
                and isinstance(flight_tail, dict)
                and isinstance(flight_tail.get("owner"), Mapping)
            ):
                owner_class = _extract_first(
                    flight_tail["owner"],
                    "class",
                    "classification",
                    "type",
                    "name",
                )
            if not owner_class and isinstance(flight_tail, dict):
                owner_class = _extract_first(
                    flight_tail,
                    "ownerClass",
                    "owner_class",
                    "ownerClassification",
                    "owner_classification",
                    "ownerType",
                    "ownerTypeName",
                    "ownerClassName",
                    "aircraftOwnerClass",
                )

            dep_airport = _extract_first(
                leg,
                "departureAirport",
                "departureAirportCode",
                "departureAirportIcao",
                "departureAirportIata",
                "departureAirportName",
                "departure_airport",
                "dep_airport",
                "departure",
                "airportFrom",
                "fromAirport",
            )
            if isinstance(dep_airport, Mapping):
                dep_airport = _extract_first(
                    dep_airport,
                    "icao",
                    "iata",
                    "code",
                    "name",
                    "airport",
                )

            arr_airport = _extract_first(
                leg,
                "arrivalAirport",
                "arrivalAirportCode",
                "arrivalAirportIcao",
                "arrivalAirportIata",
                "arrivalAirportName",
                "arrival_airport",
                "arr_airport",
                "arrival",
                "airportTo",
                "toAirport",
            )
            if isinstance(arr_airport, Mapping):
                arr_airport = _extract_first(
                    arr_airport,
                    "icao",
                    "iata",
                    "code",
                    "name",
                    "airport",
                )

            booking_identifier = _extract_first(
                leg,
                "bookingIdentifier",
                "booking_identifier",
                "bookingidentifier",
            )
            if booking_identifier is None and isinstance(flight_tail, dict):
                booking_identifier = _extract_first(
                    flight_tail,
                    "bookingIdentifier",
                    "booking_identifier",
                    "bookingidentifier",
                )

            if isinstance(booking_identifier, Mapping):
                booking_identifier = _extract_first(
                    booking_identifier,
                    "identifier",
                    "code",
                    "reference",
                    "number",
                    "id",
                )

            booking_id = _extract_first(
                leg,
                "bookingReference",
                "bookingId",
                "bookingID",
                "booking_id",
                "booking",
            )
            if booking_id is None and isinstance(flight_tail, dict):
                booking_id = _extract_first(
                    flight_tail,
                    "bookingReference",
                    "bookingId",
                    "bookingID",
                    "booking_id",
                    "booking",
                )

            booking_code = _extract_first(
                leg,
                "bookingCode",
                "booking_code",
                "bookingNumber",
                "booking_number",
                "bookingRef",
                "bookingReferenceNumber",
            )
            if booking_code is None and isinstance(flight_tail, dict):
                booking_code = _extract_first(
                    flight_tail,
                    "bookingCode",
                    "booking_code",
                    "bookingNumber",
                    "booking_number",
                    "bookingRef",
                    "bookingReferenceNumber",
                )

            account_name = _extract_first(
                leg,
                "accountName",
                "account",
                "account_name",
                "owner",
                "ownerName",
                "customer",
                "customerName",
                "client",
                "clientName",
            )
            if account_name is None and isinstance(flight_tail, dict):
                account_name = _extract_first(
                    flight_tail,
                    "accountName",
                    "account",
                    "account_name",
                    "owner",
                    "ownerName",
                    "customer",
                    "customerName",
                    "client",
                    "clientName",
                )

            flight_type = _extract_first(
                leg,
                "flightType",
                "flight_type",
                "flighttype",
                "type",
            )
            if flight_type is None and isinstance(flight_tail, dict):
                flight_type = _extract_first(
                    flight_tail,
                    "flightType",
                    "flight_type",
                    "flighttype",
                    "type",
                )

            pax_number = _extract_first(
                leg,
                "paxNumber",
                "pax_count",
                "pax",
                "passengerCount",
                "passengers",
                "passenger_count",
            )
            if pax_number is None and isinstance(flight_tail, dict):
                pax_number = _extract_first(
                    flight_tail,
                    "paxNumber",
                    "pax_count",
                    "pax",
                    "passengerCount",
                    "passengers",
                    "passenger_count",
                )

            flight_identifier = None
            if isinstance(flight_tail, dict):
                flight_identifier = _extract_first(
                    flight_tail,
                    "flightId",
                    "flight_id",
                    "id",
                    "uuid",
                    "externalId",
                    "external_id",
                )

            normalized_leg: Dict[str, Any] = {**leg}
            normalized_leg.update(
                {
                    "tail": str(tail),
                    "leg_id": str(leg_id) if leg_id is not None else str(len(normalized) + 1),
                    "dep_time": dep_time,
                    "dep_tz": dep_tz,
                }
            )
            if arr_time:
                normalized_leg.setdefault("arrival_time", arr_time)
            if dep_airport:
                normalized_leg.setdefault("departure_airport", str(dep_airport))
            if arr_airport:
                normalized_leg.setdefault("arrival_airport", str(arr_airport))
            if pic_name:
                normalized_leg.setdefault("picName", pic_name)
            if sic_name:
                normalized_leg.setdefault("sicName", sic_name)
            if workflow_custom_name:
                normalized_leg.setdefault("workflowCustomName", str(workflow_custom_name))
            if aircraft_category:
                normalized_leg.setdefault("aircraftCategory", str(aircraft_category))
            if assigned_aircraft_type:
                normalized_leg.setdefault("assignedAircraftType", str(assigned_aircraft_type))
            if owner_class:
                normalized_leg.setdefault("ownerClass", str(owner_class))
            if booking_identifier:
                normalized_leg.setdefault("bookingIdentifier", str(booking_identifier))
            if booking_id:
                normalized_leg.setdefault("bookingId", str(booking_id))
            if booking_code:
                normalized_leg.setdefault("bookingCode", str(booking_code))
            booking_reference_candidate = booking_code or booking_id or booking_identifier
            if booking_reference_candidate:
                normalized_leg.setdefault(
                    "bookingReference",
                    str(booking_reference_candidate),
                )
            if account_name:
                normalized_leg.setdefault("accountName", str(account_name))
                normalized_leg.setdefault("account", str(account_name))
            if flight_type:
                normalized_leg.setdefault("flightType", str(flight_type))
            if flight_identifier:
                normalized_leg.setdefault("flightId", str(flight_identifier))

            if pax_number is not None:
                try:
                    pax_value = int(float(str(pax_number)))
                except (TypeError, ValueError):
                    pax_value = None
                if pax_value is not None:
                    normalized_leg.setdefault("paxNumber", pax_value)

            if isinstance(leg.get("crewMembers"), list):
                normalized_leg["crewMembers"] = leg["crewMembers"]
            elif isinstance(flight_tail, dict) and isinstance(flight_tail.get("crewMembers"), list):
                normalized_leg["crewMembers"] = flight_tail["crewMembers"]

            normalized.append(normalized_leg)
            stats["legs_normalized"] += 1

    return normalized, stats


def _extract_first(obj: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


_SUBCHARTER_PATTERN = re.compile(r"subcharter", re.IGNORECASE)


def _workflow_indicates_subcharter(value: Any) -> bool:
    if value is None:
        return False
    return bool(_SUBCHARTER_PATTERN.search(str(value)))


def filter_out_subcharter_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    skipped = 0
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        workflow_value: Optional[Any] = None
        if isinstance(row, Mapping):
            for key in (
                "workflowCustomName",
                "workflow_custom_name",
                "workflowName",
                "workflow",
            ):
                if key in row and row[key] not in (None, ""):
                    workflow_value = row[key]
                    break
        if _workflow_indicates_subcharter(workflow_value):
            skipped += 1
            continue
        filtered.append(row)
    return filtered, skipped


def build_fl3xx_api_config(settings: Optional[Dict[str, Any]]) -> Fl3xxApiConfig:
    if not settings:
        raise FlightDataError(
            "FL3XX API secrets are not configured; add credentials to `.streamlit/secrets.toml`."
        )

    def _coerce_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return default

    def _coerce_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    extra_headers = settings.get("extra_headers")
    if isinstance(extra_headers, dict):
        sanitized_headers = {str(k): str(v) for k, v in extra_headers.items()}
    else:
        sanitized_headers = {}

    extra_params = settings.get("extra_params")
    if isinstance(extra_params, dict):
        sanitized_params = {str(k): str(v) for k, v in extra_params.items()}
    else:
        sanitized_params = {}

    return Fl3xxApiConfig(
        base_url=str(settings.get("base_url") or DEFAULT_FL3XX_BASE_URL),
        api_token=str(settings.get("api_token")) if settings.get("api_token") else None,
        auth_header=str(settings.get("auth_header")) if settings.get("auth_header") else None,
        auth_header_name=str(settings.get("auth_header_name") or "Authorization"),
        api_token_scheme=str(settings.get("api_token_scheme")) if settings.get("api_token_scheme") else None,
        extra_headers=sanitized_headers,
        verify_ssl=_coerce_bool(settings.get("verify_ssl"), True),
        timeout=_coerce_int(settings.get("timeout"), 30),
        extra_params=sanitized_params,
    )


def fetch_legs_dataframe(
    config: Fl3xxApiConfig,
    *,
    from_date: date,
    to_date: date,
    departure_window: Optional[Tuple[datetime, datetime]] = None,
    fetch_crew: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any], Optional[Dict[str, Any]]]:
    flights, metadata = fetch_flights(
        config,
        from_date=from_date,
        to_date=to_date,
    )

    crew_summary: Optional[Dict[str, Any]] = None
    if fetch_crew:
        crew_summary = enrich_flights_with_crew(config, flights)
        metadata = {**metadata, "crew_summary": crew_summary}

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    normalized_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    normalization_stats["skipped_subcharter"] = skipped_subcharter

    if departure_window:
        rows, window_stats = filter_rows_by_departure_window(
            normalized_rows, departure_window[0], departure_window[1]
        )
        window_meta = {
            "start": format_utc(departure_window[0]),
            "end": format_utc(departure_window[1]),
        }
    else:
        rows = normalized_rows
        window_stats = {
            "total": len(rows),
            "within_window": len(rows),
            "before_window": 0,
            "after_window": 0,
        }
        window_meta = None

    metadata = {
        **metadata,
        "normalization_stats": normalization_stats,
        "departure_window_counts": window_stats,
    }
    if window_meta:
        metadata["departure_window_utc"] = window_meta
    if skipped_subcharter:
        metadata["skipped_subcharter_legs"] = skipped_subcharter

    if not rows:
        return pd.DataFrame(), metadata, crew_summary

    df = pd.DataFrame(rows)
    df, missing_tz_airports, tz_lookup_used = apply_airport_timezones(df)

    metadata = {
        **metadata,
        "timezone_lookup_used": tz_lookup_used,
    }
    if missing_tz_airports:
        metadata["missing_dep_tz_airports"] = sorted(missing_tz_airports)

    return df, metadata, crew_summary


def _format_minutes(total_min: Optional[int]) -> Optional[str]:
    if total_min is None:
        return None
    if total_min < 0:
        return None
    hours, minutes = divmod(total_min, 60)
    return f"{hours}:{minutes:02d}"


def get_todays_sorted_legs_by_tail(
    config: Fl3xxApiConfig,
    target_date: date,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return today's legs grouped by tail and ordered by departure time."""

    start_utc, end_utc = compute_mountain_day_window_utc(target_date)
    df, _metadata, _crew = fetch_legs_dataframe(
        config,
        from_date=target_date,
        to_date=target_date + timedelta(days=1),
        departure_window=(start_utc, end_utc),
        fetch_crew=False,
    )

    legs_by_tail: Dict[str, List[Dict[str, Any]]] = {}
    if df.empty:
        return legs_by_tail

    for _, row in df.iterrows():
        tail = str(row.get("tail") or "").strip()
        if not tail:
            continue

        flight_id_raw = row.get("flightId")
        if flight_id_raw is None or (isinstance(flight_id_raw, float) and pd.isna(flight_id_raw)):
            continue
        if isinstance(flight_id_raw, float):
            flight_id = int(flight_id_raw)
        else:
            flight_id = flight_id_raw

        dep_raw = row.get("dep_time")
        if dep_raw is None or (isinstance(dep_raw, float) and pd.isna(dep_raw)):
            continue
        dep_dt = safe_parse_dt(str(dep_raw)).astimezone(UTC)

        legs_by_tail.setdefault(tail, []).append(
            {
                "tail": tail,
                "flightId": flight_id,
                "dep_dt_utc": dep_dt,
            }
        )

    for tail, legs in legs_by_tail.items():
        legs.sort(key=lambda leg: leg["dep_dt_utc"])

    return legs_by_tail


def _build_crew_signature(pilots: List[DutySnapshotPilot]) -> Optional[Tuple[Tuple[str, str], ...]]:
    entries: List[Tuple[str, str]] = []
    for pilot in pilots:
        name = (pilot.name or "").strip()
        seat = pilot.seat or "PIC"
        if name:
            entries.append((seat, name))
    if not entries:
        return None
    entries.sort()
    return tuple(entries)


def build_duty_snapshots_for_today(
    config: Fl3xxApiConfig,
    target_date: date,
) -> List[DutySnapshot]:
    """Collect duty snapshots for each distinct crew duty start on the target date."""

    legs_by_tail = get_todays_sorted_legs_by_tail(config, target_date)
    snapshots: List[DutySnapshot] = []

    for tail, legs in legs_by_tail.items():
        last_signature: Optional[Tuple[Tuple[str, str], ...]] = None

        for leg_info in legs:
            flight_id = leg_info["flightId"]
            raw_postflight = fetch_postflight(config, flight_id)
            snapshot = parse_postflight_payload(raw_postflight)
            if not snapshot.tail:
                snapshot.tail = tail

            signature = _build_crew_signature(snapshot.pilots)
            if signature is None:
                signature = (("LEG", str(flight_id)),)

            if last_signature is not None and signature == last_signature:
                continue

            snapshots.append(snapshot)
            last_signature = signature

    return snapshots


def summarize_frms_watch_items(snapshots: List[DutySnapshot]) -> Dict[str, List[str]]:
    """Return formatted report lines for the FRMS watch items."""

    long_duty_lines: List[str] = []
    split_duty_lines: List[str] = []
    tight_turn_lines: List[str] = []

    for snapshot in snapshots:
        tail = snapshot.tail or "UNKNOWN"

        long_hits: List[Tuple[int, str, str]] = []
        split_hits: List[Tuple[str, str, str]] = []
        tight_hits: List[Tuple[int, str, str]] = []

        for pilot in snapshot.pilots:
            seat = pilot.seat or "PIC"

            if pilot.fdp_actual_min is not None and pilot.fdp_max_min:
                if pilot.fdp_max_min > 0:
                    ratio = pilot.fdp_actual_min / pilot.fdp_max_min
                    if ratio >= 0.90:
                        fdp_str = pilot.fdp_actual_str or _format_minutes(pilot.fdp_actual_min)
                        if fdp_str:
                            long_hits.append((pilot.fdp_actual_min, fdp_str, seat))

            if pilot.split_duty:
                fdp_str = pilot.fdp_actual_str or _format_minutes(pilot.fdp_actual_min)
                if fdp_str:
                    split_hits.append((fdp_str, pilot.split_break_str or "", seat))

            if pilot.rest_after_min is not None and pilot.rest_after_min < 660:
                rest_str = pilot.rest_after_str or _format_minutes(pilot.rest_after_min)
                if rest_str:
                    tight_hits.append((pilot.rest_after_min, rest_str, seat))

        if long_hits:
            seats = "/".join(sorted({seat for _, _, seat in long_hits}))
            worst = max(long_hits, key=lambda entry: entry[0])
            long_duty_lines.append(f"{tail} – {worst[1]} ({seats})")

        if split_hits:
            seats = "/".join(sorted({seat for _, _, seat in split_hits}))
            fdp_str, break_str, _ = split_hits[0]
            if break_str:
                split_duty_lines.append(f"{tail} – {fdp_str} duty – {break_str} break ({seats} split)")
            else:
                split_duty_lines.append(f"{tail} – {fdp_str} duty ({seats} split)")

        if tight_hits:
            seats = "/".join(sorted({seat for _, _, seat in tight_hits}))
            worst = min(tight_hits, key=lambda entry: entry[0])
            tight_turn_lines.append(
                f"{tail} – {worst[1]} rest before next duty ({seats})"
            )

    return {
        "long_duty": long_duty_lines,
        "split_duty": split_duty_lines,
        "tight_turn": tight_turn_lines,
    }


def generate_frms_report_for_today(
    config: Fl3xxApiConfig,
    today_local_date: date,
) -> Dict[str, List[str]]:
    """Convenience helper to fetch duty snapshots and format report sections."""

    snapshots = build_duty_snapshots_for_today(config, today_local_date)
    return summarize_frms_watch_items(snapshots)
__all__ = [
    "AIRPORT_TZ_FILENAME",
    "ARRIVAL_AIRPORT_COLUMNS",
    "DEPARTURE_AIRPORT_COLUMNS",
    "UTC",
    "FlightDataError",
    "apply_airport_timezones",
    "build_fl3xx_api_config",
    "build_duty_snapshots_for_today",
    "compute_departure_window_bounds",
    "compute_mountain_day_window_utc",
    "fetch_legs_dataframe",
    "filter_out_subcharter_rows",
    "filter_rows_by_departure_window",
    "format_utc",
    "generate_frms_report_for_today",
    "get_todays_sorted_legs_by_tail",
    "is_customs_leg",
    "load_airport_metadata_lookup",
    "load_airport_tz_lookup",
    "normalize_fl3xx_payload",
    "summarize_frms_watch_items",
    "safe_parse_dt",
]
