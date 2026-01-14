from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import requests
import streamlit as st

from fl3xx_api import Fl3xxApiConfig, fetch_flights
from flight_leg_utils import normalize_fl3xx_payload
from Home import configure_page, get_secret, password_gate, render_sidebar


FORE_FLIGHT_BASE_URL = "https://public-api.foreflight.com/public/api/Flights/flights"
DEFAULT_MISMATCH_THRESHOLD_MIN = 20


@dataclass
class FlightRecord:
    source: str
    tail: str
    departure_airport: str
    arrival_airport: str
    departure_time: Optional[datetime]
    arrival_time: Optional[datetime]
    duration_minutes: Optional[int]
    flight_id: Optional[str]
    booking_identifier: Optional[str]


configure_page(page_title="ForeFlight Test")
password_gate()
render_sidebar()

st.title("🛰️ ForeFlight Test")


def _normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_tail(value: Any) -> Optional[str]:
    text = _normalize_text(value)
    if not text:
        return None
    return text.replace("-", "").upper()


def _normalize_airport(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key in ("icao", "icaoCode", "icao_code", "identifier", "ident", "code", "id"):
            if key in value and value[key]:
                return _normalize_text(value[key])
    return _normalize_text(value)


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = _normalize_text(value)
        if not text:
            return None
        dt = pd.to_datetime(text, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _duration_minutes(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if not start or not end:
        return None
    delta = end - start
    return int(round(delta.total_seconds() / 60))


def _extract_first(container: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in container and container[key] not in (None, ""):
            return container[key]
    return None


def _build_fl3xx_config(token: Optional[str] = None) -> Fl3xxApiConfig:
    secrets_section = get_secret("fl3xx_api", {})
    base_url = secrets_section.get("base_url") or Fl3xxApiConfig().base_url
    auth_header_name = secrets_section.get("auth_header_name") or "Authorization"
    auth_header = secrets_section.get("auth_header")
    api_token_scheme = secrets_section.get("api_token_scheme")

    extra_headers = {}
    if isinstance(secrets_section.get("extra_headers"), Mapping):
        extra_headers = {str(k): str(v) for k, v in secrets_section["extra_headers"].items()}

    extra_params = {}
    if isinstance(secrets_section.get("extra_params"), Mapping):
        extra_params = {str(k): str(v) for k, v in secrets_section["extra_params"].items()}

    timeout_value = secrets_section.get("timeout")
    timeout = None
    if timeout_value is not None:
        try:
            timeout = int(timeout_value)
        except (TypeError, ValueError):
            timeout = None

    config_kwargs = {
        "base_url": base_url,
        "api_token": token or secrets_section.get("api_token"),
        "auth_header": auth_header,
        "auth_header_name": auth_header_name,
        "api_token_scheme": api_token_scheme,
        "extra_headers": extra_headers,
        "extra_params": extra_params,
        "verify_ssl": True,
    }
    if timeout is not None:
        config_kwargs["timeout"] = timeout

    return Fl3xxApiConfig(**config_kwargs)


def _extract_foreflight_flights(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        flights = payload.get("flights")
        if isinstance(flights, list):
            return [flight for flight in flights if isinstance(flight, Mapping)]
    if isinstance(payload, list):
        return [flight for flight in payload if isinstance(flight, Mapping)]
    return []


def _foreflight_record(flight: Mapping[str, Any]) -> Optional[FlightRecord]:
    tail = _normalize_tail(
        _extract_first(
            flight,
            "aircraftRegistration",
            "registration",
            "tail",
            "tailNumber",
            "aircraft",
        )
    )
    if not tail and isinstance(flight.get("aircraft"), Mapping):
        tail = _normalize_tail(_extract_first(flight["aircraft"], "registration", "tailNumber"))

    departure_airport = _normalize_airport(
        _extract_first(
            flight,
            "departure",
            "departureAirport",
            "origin",
            "departureAirportCode",
            "departureAirportIcao",
        )
    )
    arrival_airport = _normalize_airport(
        _extract_first(
            flight,
            "destination",
            "destinationAirport",
            "arrival",
            "arrivalAirport",
            "destinationAirportCode",
            "destinationAirportIcao",
        )
    )

    departure_time = _parse_datetime(
        _extract_first(
            flight,
            "departureTimeUtc",
            "departureTime",
            "scheduledDepartureTime",
            "scheduledDeparture",
            "departure",
        )
    )
    arrival_time = _parse_datetime(
        _extract_first(
            flight,
            "arrivalTimeUtc",
            "arrivalTime",
            "scheduledArrivalTime",
            "scheduledArrival",
            "arrival",
        )
    )

    if not tail or not departure_airport or not arrival_airport:
        return None

    booking_identifier = _normalize_text(_extract_first(flight, "bookingIdentifier", "booking_identifier"))
    if not booking_identifier:
        tags = flight.get("tags")
        if isinstance(tags, list):
            booking_identifier = next((_normalize_text(tag) for tag in tags if _normalize_text(tag)), None)

    return FlightRecord(
        source="ForeFlight",
        tail=tail,
        departure_airport=departure_airport,
        arrival_airport=arrival_airport,
        departure_time=departure_time,
        arrival_time=arrival_time,
        duration_minutes=_duration_minutes(departure_time, arrival_time),
        flight_id=_normalize_text(_extract_first(flight, "flightId", "flight_id", "id")),
        booking_identifier=booking_identifier,
    )


def _fl3xx_record(leg: Mapping[str, Any]) -> Optional[FlightRecord]:
    tail = _normalize_tail(_extract_first(leg, "tail", "aircraftRegistration", "registration"))
    departure_airport = _normalize_airport(
        _extract_first(
            leg,
            "departure_airport",
            "departureAirport",
            "departure",
            "origin",
            "from",
        )
    )
    arrival_airport = _normalize_airport(
        _extract_first(
            leg,
            "arrival_airport",
            "arrivalAirport",
            "destination",
            "arrival",
            "to",
        )
    )

    departure_time = _parse_datetime(
        _extract_first(
            leg,
            "dep_time",
            "departureTimeUtc",
            "departureTime",
            "departure_time",
            "scheduledDeparture",
        )
    )
    arrival_time = _parse_datetime(
        _extract_first(
            leg,
            "arrival_time",
            "arrivalTimeUtc",
            "arrivalTime",
            "arrival_time",
            "scheduledArrival",
        )
    )

    if not tail or not departure_airport or not arrival_airport:
        return None

    return FlightRecord(
        source="FL3XX",
        tail=tail,
        departure_airport=departure_airport,
        arrival_airport=arrival_airport,
        departure_time=departure_time,
        arrival_time=arrival_time,
        duration_minutes=_duration_minutes(departure_time, arrival_time),
        flight_id=_normalize_text(_extract_first(leg, "leg_id", "flightId", "id")),
        booking_identifier=_normalize_text(_extract_first(leg, "bookingIdentifier", "booking_identifier")),
    )


def _build_records(records: Iterable[Optional[FlightRecord]]) -> list[FlightRecord]:
    return [record for record in records if record is not None]


def _record_key(record: FlightRecord) -> tuple[str, str, str]:
    return (record.tail, record.departure_airport, record.arrival_airport)


def _match_records(
    foreflight_records: list[FlightRecord],
    fl3xx_records: list[FlightRecord],
) -> tuple[list[tuple[FlightRecord, FlightRecord]], list[FlightRecord], list[FlightRecord]]:
    fl3xx_by_booking: dict[str, list[FlightRecord]] = {}
    for record in fl3xx_records:
        if record.booking_identifier:
            fl3xx_by_booking.setdefault(record.booking_identifier, []).append(record)

    fl3xx_by_key: dict[tuple[str, str, str], list[FlightRecord]] = {}
    for record in fl3xx_records:
        fl3xx_by_key.setdefault(_record_key(record), []).append(record)

    matches: list[tuple[FlightRecord, FlightRecord]] = []
    unmatched_foreflight: list[FlightRecord] = []

    for ff_record in foreflight_records:
        candidates: Optional[list[FlightRecord]] = None
        matched_using_booking = False
        if ff_record.booking_identifier:
            candidates = fl3xx_by_booking.get(ff_record.booking_identifier)
            matched_using_booking = candidates is not None
        if not candidates:
            key = _record_key(ff_record)
            candidates = fl3xx_by_key.get(key)
        if not candidates:
            unmatched_foreflight.append(ff_record)
            continue
        if ff_record.departure_time:
            def _distance_minutes(candidate: FlightRecord) -> float:
                if not candidate.departure_time:
                    return float("inf")
                delta = candidate.departure_time - ff_record.departure_time
                return abs(delta.total_seconds())
            candidates.sort(key=_distance_minutes)
        matched = candidates.pop(0)
        matches.append((ff_record, matched))
        if matched.booking_identifier:
            booking_candidates = fl3xx_by_booking.get(matched.booking_identifier)
            if booking_candidates and matched in booking_candidates:
                booking_candidates.remove(matched)
                if not booking_candidates:
                    fl3xx_by_booking.pop(matched.booking_identifier, None)
        matched_key = _record_key(matched)
        key_candidates = fl3xx_by_key.get(matched_key)
        if key_candidates and matched in key_candidates:
            key_candidates.remove(matched)
            if not key_candidates:
                fl3xx_by_key.pop(matched_key, None)
        if matched_using_booking and ff_record.booking_identifier and not candidates:
            fl3xx_by_booking.pop(ff_record.booking_identifier, None)

    remaining_by_key = [record for records in fl3xx_by_key.values() for record in records]
    remaining_by_booking = [
        record for records in fl3xx_by_booking.values() for record in records
    ]
    unmatched_fl3xx = list({id(record): record for record in remaining_by_key + remaining_by_booking}.values())
    return matches, unmatched_foreflight, unmatched_fl3xx


def _format_duration(value: Optional[int]) -> str:
    if value is None:
        return "—"
    return f"{value} min"


def _format_timestamp(value: Optional[datetime]) -> str:
    if not value:
        return "—"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


st.markdown(
    """
    Compare ForeFlight flights against FL3XX for a selected date range.
    Matches are based on **booking identifier tags**, falling back to **tail + departure + arrival**.
    """
)

col1, col2 = st.columns(2)

today = date.today()
with col1:
    date_range = st.date_input(
        "Date range",
        value=(today, today + timedelta(days=1)),
    )
with col2:
    mismatch_threshold = st.number_input(
        "Mismatch threshold (minutes)",
        min_value=5,
        max_value=180,
        value=DEFAULT_MISMATCH_THRESHOLD_MIN,
        step=5,
    )

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = today
    end_date = today + timedelta(days=1)

fetch = st.button("Fetch flights")

if fetch:
    foreflight_token = get_secret("foreflight_api", {}).get("api_token")
    fl3xx_token = get_secret("fl3xx_api", {}).get("api_token")

    if not foreflight_token:
        st.error("ForeFlight API token is missing. Add it to Streamlit secrets under [foreflight_api].")
        st.stop()
    if not fl3xx_token:
        st.error("FL3XX API token is missing. Add it to Streamlit secrets under [fl3xx_api].")
        st.stop()

    with st.spinner("Fetching ForeFlight flights..."):
        from_date = start_date
        to_date = end_date + timedelta(days=1)
        params = {
            "fromDate": f"{from_date.isoformat()}Z",
            "toDate": f"{to_date.isoformat()}Z",
        }
        headers = {
            "x-api-key": foreflight_token,
            "Accept": "application/json",
        }
        response = requests.get(FORE_FLIGHT_BASE_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        foreflight_payload = response.json()

    with st.spinner("Fetching FL3XX flights..."):
        fl3xx_config = _build_fl3xx_config(fl3xx_token)
        fl3xx_flights, _meta = fetch_flights(fl3xx_config, from_date=from_date, to_date=to_date)
        fl3xx_normalized, _stats = normalize_fl3xx_payload(fl3xx_flights)

    foreflight_records = _build_records(_foreflight_record(flight) for flight in _extract_foreflight_flights(foreflight_payload))
    fl3xx_records = _build_records(_fl3xx_record(leg) for leg in fl3xx_normalized)

    matches, unmatched_foreflight, unmatched_fl3xx = _match_records(foreflight_records, fl3xx_records)

    mismatch_rows = []
    for ff_record, fl3xx_record in matches:
        if ff_record.duration_minutes is None or fl3xx_record.duration_minutes is None:
            continue
        diff = ff_record.duration_minutes - fl3xx_record.duration_minutes
        if abs(diff) >= mismatch_threshold:
            mismatch_rows.append(
                {
                    "Tail": ff_record.tail,
                    "Route": f"{ff_record.departure_airport} → {ff_record.arrival_airport}",
                    "ForeFlight Duration": _format_duration(ff_record.duration_minutes),
                    "FL3XX Duration": _format_duration(fl3xx_record.duration_minutes),
                    "Diff (min)": diff,
                    "ForeFlight Departure": _format_timestamp(ff_record.departure_time),
                    "FL3XX Departure": _format_timestamp(fl3xx_record.departure_time),
                    "Booking Identifier": ff_record.booking_identifier or fl3xx_record.booking_identifier or "—",
                }
            )

    st.subheader("Results")
    st.caption(
        f"Matched {len(matches)} flights • {len(unmatched_foreflight)} ForeFlight-only • {len(unmatched_fl3xx)} FL3XX-only"
    )

    if mismatch_rows:
        st.markdown("### ⏱️ Duration mismatches")
        st.dataframe(pd.DataFrame(mismatch_rows), use_container_width=True)
    else:
        st.success("No duration mismatches found for the current threshold.")

    if unmatched_foreflight or unmatched_fl3xx:
        st.markdown("### 🧩 Unmatched flights")
        unmatched_rows = []
        for record in unmatched_foreflight:
            unmatched_rows.append(
                {
                    "Source": record.source,
                    "Tail": record.tail,
                    "Route": f"{record.departure_airport} → {record.arrival_airport}",
                    "Departure": _format_timestamp(record.departure_time),
                    "Arrival": _format_timestamp(record.arrival_time),
                    "Booking Identifier": record.booking_identifier or "—",
                }
            )
        for record in unmatched_fl3xx:
            unmatched_rows.append(
                {
                    "Source": record.source,
                    "Tail": record.tail,
                    "Route": f"{record.departure_airport} → {record.arrival_airport}",
                    "Departure": _format_timestamp(record.departure_time),
                    "Arrival": _format_timestamp(record.arrival_time),
                    "Booking Identifier": record.booking_identifier or "—",
                }
            )
        st.dataframe(pd.DataFrame(unmatched_rows), use_container_width=True)
    else:
        st.info("Every flight matched between ForeFlight and FL3XX for the selected range.")
