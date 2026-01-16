from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import requests
import streamlit as st

from fl3xx_api import Fl3xxApiConfig, fetch_flights
from flight_leg_utils import normalize_fl3xx_payload
from Home import configure_page, get_secret, password_gate, render_sidebar


FORE_FLIGHT_BASE_URL = "https://public-api.foreflight.com/public/api/Flights/flights"
FORE_FLIGHT_PERFORMANCE_URL = "https://public-api.foreflight.com/public/api/Flights/{flight_id}/performance"
TAG_PATTERN = re.compile(r"^[A-Z]{5}\d?$")
TARGET_LANDING_FUEL_LBS = {
    "CJ": 1200,
    "Embraer": 3000,
}


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


configure_page(page_title="Fuel Planning Assistant")
password_gate()
render_sidebar()

st.title("⛽ Fuel Planning Assistant (WIP)")
st.caption(
    "Pulls flights from FL3XX and ForeFlight, matches them by booking tag or tail/route, "
    "and surfaces ForeFlight performance data for fuel planning."
)


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


def _extract_booking_identifier_from_tags(tags: Any) -> Optional[str]:
    if not isinstance(tags, list):
        return None
    for tag in tags:
        normalized = _normalize_text(tag)
        if not normalized:
            continue
        candidate = normalized.upper()
        if TAG_PATTERN.match(candidate):
            return candidate
    return None


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
        booking_identifier = _extract_booking_identifier_from_tags(flight.get("tags"))

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


def _format_timestamp(value: Optional[datetime]) -> str:
    if not value:
        return "—"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def _fetch_performance(
    flight_id: str,
    *,
    token: str,
) -> Optional[Mapping[str, Any]]:
    url = FORE_FLIGHT_PERFORMANCE_URL.format(flight_id=flight_id)
    headers = {
        "x-api-key": token,
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=30)
    if not response.ok:
        return None
    return response.json()


def _extract_performance_fields(payload: Mapping[str, Any]) -> dict[str, Optional[float]]:
    performance = payload.get("performance", {}) if isinstance(payload, Mapping) else {}
    fuel = performance.get("fuel", {}) if isinstance(performance, Mapping) else {}
    weights = performance.get("weights", {}) if isinstance(performance, Mapping) else {}
    return {
        "fuel_to_destination": fuel.get("fuelToDestination"),
        "taxi_fuel": fuel.get("taxiFuel"),
        "flight_fuel": fuel.get("flightFuel"),
        "landing_fuel": fuel.get("landingFuel"),
        "total_fuel": fuel.get("totalFuel"),
        "max_total_fuel": fuel.get("maxTotalFuel"),
        "ramp_weight": weights.get("rampWeight"),
        "max_ramp_weight": weights.get("maxRampWeight"),
        "takeoff_weight": weights.get("takeOffWeight"),
        "max_takeoff_weight": weights.get("maxTakeOffWeight"),
        "landing_weight": weights.get("landingWeight"),
        "max_landing_weight": weights.get("maxLandingWeight"),
        "zero_fuel_weight": weights.get("zeroFuelWeight"),
        "max_zero_fuel_weight": weights.get("maxZeroFuelWeight"),
    }


col1, col2, col3 = st.columns(3)

with col1:
    selected_date = st.date_input("Flight date", value=date.today())
with col2:
    tail_input = st.text_input("Tail (registration)", placeholder="CFJAS").strip().upper()
with col3:
    aircraft_type = st.selectbox("Aircraft type", ["CJ", "Embraer"], index=0)

target_landing_fuel = st.number_input(
    "Target landing fuel (lb)",
    value=float(TARGET_LANDING_FUEL_LBS[aircraft_type]),
    min_value=0.0,
    step=100.0,
)

fetch = st.button("Fetch flights & performance")

if "fuel_planning_rows" not in st.session_state:
    st.session_state["fuel_planning_rows"] = []
if "fuel_planning_missing_performance" not in st.session_state:
    st.session_state["fuel_planning_missing_performance"] = []
if "fuel_planning_summary" not in st.session_state:
    st.session_state["fuel_planning_summary"] = None

if fetch:
    foreflight_token = get_secret("foreflight_api", {}).get("api_token")
    fl3xx_token = get_secret("fl3xx_api", {}).get("api_token")

    if not tail_input:
        st.error("Enter a tail number to continue.")
        st.stop()
    if not foreflight_token:
        st.error("ForeFlight API token is missing. Add it to Streamlit secrets under [foreflight_api].")
        st.stop()
    if not fl3xx_token:
        st.error("FL3XX API token is missing. Add it to Streamlit secrets under [fl3xx_api].")
        st.stop()

    from_date = selected_date
    to_date = selected_date + timedelta(days=1)

    with st.spinner("Fetching ForeFlight flights..."):
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

    foreflight_records = [record for record in foreflight_records if record.tail == tail_input]
    fl3xx_records = [record for record in fl3xx_records if record.tail == tail_input]

    matches, unmatched_foreflight, unmatched_fl3xx = _match_records(foreflight_records, fl3xx_records)

    summary = {
        "matched": len(matches),
        "unmatched_foreflight": len(unmatched_foreflight),
        "unmatched_fl3xx": len(unmatched_fl3xx),
    }
    st.session_state["fuel_planning_summary"] = summary

    if not matches:
        st.warning("No matched flights found for that tail/date.")
        st.session_state["fuel_planning_rows"] = []
        st.session_state["fuel_planning_missing_performance"] = []
        st.stop()

    rows: list[dict[str, Any]] = []
    missing_performance: list[str] = []

    with st.spinner("Fetching ForeFlight performance data..."):
        for ff_record, _fl3xx_record in matches:
            if not ff_record.flight_id:
                missing_performance.append(f"{ff_record.departure_airport} → {ff_record.arrival_airport}")
                continue
            payload = _fetch_performance(ff_record.flight_id, token=foreflight_token)
            if not payload:
                missing_performance.append(f"{ff_record.departure_airport} → {ff_record.arrival_airport}")
                continue
            perf = _extract_performance_fields(payload)
            fuel_to_destination = perf.get("fuel_to_destination")
            required_departure_fuel = None
            if fuel_to_destination is not None:
                required_departure_fuel = fuel_to_destination + target_landing_fuel

            rows.append(
                {
                    "Departure": ff_record.departure_airport,
                    "Arrival": ff_record.arrival_airport,
                    "Dep Time (UTC)": _format_timestamp(ff_record.departure_time),
                    "Arr Time (UTC)": _format_timestamp(ff_record.arrival_time),
                    "Fuel To Dest (lb)": fuel_to_destination,
                    "Taxi Fuel (lb)": perf.get("taxi_fuel"),
                    "Landing Fuel (lb)": perf.get("landing_fuel"),
                    "Total Fuel (lb)": perf.get("total_fuel"),
                    "Max Total Fuel (lb)": perf.get("max_total_fuel"),
                    "Required Dep Fuel (lb)": required_departure_fuel,
                    "Ramp Weight (lb)": perf.get("ramp_weight"),
                    "Max Ramp (lb)": perf.get("max_ramp_weight"),
                    "Takeoff Weight (lb)": perf.get("takeoff_weight"),
                    "Max Takeoff (lb)": perf.get("max_takeoff_weight"),
                    "Landing Weight (lb)": perf.get("landing_weight"),
                    "Max Landing (lb)": perf.get("max_landing_weight"),
                    "Zero Fuel Weight (lb)": perf.get("zero_fuel_weight"),
                    "Max Zero Fuel (lb)": perf.get("max_zero_fuel_weight"),
                    "Fuel Price ($/unit)": None,
                    "Ramp Fee ($)": None,
                    "Waiver Fuel (unit)": None,
                }
            )

    st.session_state["fuel_planning_rows"] = rows
    st.session_state["fuel_planning_missing_performance"] = missing_performance

summary = st.session_state.get("fuel_planning_summary")
rows = st.session_state.get("fuel_planning_rows", [])

if summary and rows:
    st.subheader("Matched legs")
    st.caption(
        f"{summary['matched']} matched • {summary['unmatched_foreflight']} ForeFlight-only • "
        f"{summary['unmatched_fl3xx']} FL3XX-only"
    )

missing_performance = st.session_state.get("fuel_planning_missing_performance", [])
if rows:
    if missing_performance:
        st.warning(
            "Missing performance data for: "
            + ", ".join(missing_performance)
        )

    st.markdown("### Fuelerlinx inputs")
    st.caption("Enter values per departure airport. Units should align with the ForeFlight fuel unit (lb).")

    data_editor = st.data_editor(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
        key="fuelerlinx_inputs_editor",
        column_config={
            "Fuel Price ($/unit)": st.column_config.NumberColumn(min_value=0.0, step=0.01),
            "Ramp Fee ($)": st.column_config.NumberColumn(min_value=0.0, step=1.0),
            "Waiver Fuel (unit)": st.column_config.NumberColumn(min_value=0.0, step=10.0),
        },
        disabled=[
            "Departure",
            "Arrival",
            "Dep Time (UTC)",
            "Arr Time (UTC)",
            "Fuel To Dest (lb)",
            "Taxi Fuel (lb)",
            "Landing Fuel (lb)",
            "Total Fuel (lb)",
            "Max Total Fuel (lb)",
            "Required Dep Fuel (lb)",
            "Ramp Weight (lb)",
            "Max Ramp (lb)",
            "Takeoff Weight (lb)",
            "Max Takeoff (lb)",
            "Landing Weight (lb)",
            "Max Landing (lb)",
            "Zero Fuel Weight (lb)",
            "Max Zero Fuel (lb)",
        ],
    )

    st.session_state["fuel_planning_rows"] = data_editor.to_dict(orient="records")

    st.markdown("### Next steps")
    st.info(
        "Fuel optimization logic will use the inputs above to recommend where to waive ramp fees, "
        "where to tanker, and how to meet landing-fuel targets. The current view confirms the "
        "required performance data is available."
    )
