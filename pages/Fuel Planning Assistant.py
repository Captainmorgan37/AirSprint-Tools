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


def _map_tail_type(value: Any) -> Optional[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    upper = normalized.upper()
    if upper in {"C25A", "C25B"}:
        return "CJ"
    return "Embraer"


def _infer_aircraft_type(
    legs: Iterable[Mapping[str, Any]],
    tail: Optional[str],
) -> Optional[str]:
    normalized_tail = _normalize_tail(tail)
    if not normalized_tail:
        return None
    for leg in legs:
        if _normalize_tail(leg.get("tail")) != normalized_tail:
            continue
        for key in ("aircraftCategory", "assignedAircraftType", "aircraftType", "aircraftClass"):
            inferred = _map_tail_type(leg.get(key))
            if inferred:
                return inferred
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


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _calculate_required_departure_fuel(
    performance: Mapping[str, Any],
    target_landing_fuel: float,
) -> Optional[float]:
    flight_fuel = _as_float(performance.get("flight_fuel"))
    fuel_to_destination = _as_float(performance.get("fuel_to_destination"))
    taxi_fuel = _as_float(performance.get("taxi_fuel")) or 0.0
    burn_fuel = flight_fuel if flight_fuel is not None else fuel_to_destination
    if burn_fuel is None:
        return None
    return burn_fuel + taxi_fuel + target_landing_fuel


def _build_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    recommendations: list[str] = []
    notes: list[str] = []

    rows = df.to_dict(orient="records")

    for index, row in enumerate(rows):
        required_departure_fuel = _as_float(row.get("Required Dep Fuel (lb)"))
        max_total_fuel = _as_float(row.get("Max Total Fuel (lb)"))
        fuel_price = _as_float(row.get("Fuel Price ($/unit)"))
        ramp_fee = _as_float(row.get("Ramp Fee ($)"))
        waiver_fuel = _as_float(row.get("Waiver Fuel (unit)"))

        decision = "Review manually"
        detail: list[str] = []

        if required_departure_fuel is None:
            detail.append("Missing required departure fuel.")
        else:
            decision = f"Take at least {required_departure_fuel:,.0f} lb"
            detail.append("Meets target landing fuel requirement.")

            if waiver_fuel and waiver_fuel > 0:
                if max_total_fuel is not None and waiver_fuel > max_total_fuel:
                    detail.append("Waiver threshold exceeds max fuel; cannot waive.")
                elif required_departure_fuel >= waiver_fuel:
                    decision = "Waiver already met"
                    detail.append("Required fuel already meets waiver threshold.")
                elif fuel_price is not None and ramp_fee is not None:
                    extra_needed = waiver_fuel - required_departure_fuel
                    extra_cost = extra_needed * fuel_price
                    if ramp_fee > extra_cost:
                        decision = "Buy waiver only"
                        detail.append(
                            f"Extra {extra_needed:,.0f} lb to waive saves about "
                            f"${ramp_fee - extra_cost:,.0f}."
                        )
                    else:
                        decision = "Take minimum and pay ramp fee"
                        detail.append(
                            f"Ramp fee (${ramp_fee:,.0f}) is cheaper than extra fuel "
                            f"(${extra_cost:,.0f})."
                        )
                else:
                    detail.append("Add fuel price and ramp fee to compare waiver savings.")

        next_row = rows[index + 1] if index + 1 < len(rows) else None
        next_price = _as_float(next_row.get("Fuel Price ($/unit)")) if next_row else None
        if (
            fuel_price is not None
            and next_price is not None
            and required_departure_fuel is not None
            and max_total_fuel is not None
        ):
            extra_capacity = max_total_fuel - required_departure_fuel
            if extra_capacity > 0 and fuel_price < next_price:
                decision = f"{decision} • Tankering recommended"
                detail.append(
                    f"Fuel is ${next_price - fuel_price:,.2f}/unit cheaper than next leg."
                )

        recommendations.append(decision)
        notes.append(" ".join(detail))

    result = df.copy()
    result["Recommendation"] = recommendations
    result["Decision Notes"] = notes
    return result


def _sort_by_departure_time(df: pd.DataFrame) -> pd.DataFrame:
    if "Dep Time (UTC)" not in df.columns:
        return df
    sorted_df = df.copy()
    sorted_df["__dep_time_sort"] = pd.to_datetime(
        sorted_df["Dep Time (UTC)"], utc=True, errors="coerce"
    )
    sorted_df = sorted_df.sort_values("__dep_time_sort", na_position="last")
    sorted_df = sorted_df.drop(columns=["__dep_time_sort"]).reset_index(drop=True)
    return sorted_df


def _ensure_dataframe(value: Any, fallback: pd.DataFrame) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, Mapping):
        return pd.DataFrame([value])
    return fallback


def _is_editor_state(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    return {"edited_rows", "added_rows", "deleted_rows"}.issubset(value.keys())


if "fuel_planning_aircraft_type" not in st.session_state:
    st.session_state["fuel_planning_aircraft_type"] = "CJ"
if "fuel_planning_target_landing_fuel" not in st.session_state:
    st.session_state["fuel_planning_target_landing_fuel"] = float(
        TARGET_LANDING_FUEL_LBS[st.session_state["fuel_planning_aircraft_type"]]
    )
if "fuel_planning_last_aircraft_type" not in st.session_state:
    st.session_state["fuel_planning_last_aircraft_type"] = st.session_state["fuel_planning_aircraft_type"]

col1, col2, col3 = st.columns(3)

with col1:
    selected_date = st.date_input("Flight date", value=date.today())
with col2:
    tail_input = st.text_input("Tail (registration)", placeholder="CFJAS").strip().upper()
with col3:
    aircraft_type = st.selectbox(
        "Aircraft type",
        ["CJ", "Embraer"],
        key="fuel_planning_aircraft_type",
    )

if aircraft_type != st.session_state.get("fuel_planning_last_aircraft_type"):
    st.session_state["fuel_planning_target_landing_fuel"] = float(
        TARGET_LANDING_FUEL_LBS[aircraft_type]
    )
    st.session_state["fuel_planning_last_aircraft_type"] = aircraft_type

target_landing_fuel = st.number_input(
    "Target landing fuel (lb)",
    value=float(st.session_state["fuel_planning_target_landing_fuel"]),
    min_value=0.0,
    step=100.0,
    key="fuel_planning_target_landing_fuel",
)
st.caption("Required departure fuel uses taxi + flight burn + this target landing fuel.")

fetch = st.button("Fetch flights & performance")

if "fuel_planning_df" not in st.session_state:
    st.session_state["fuel_planning_df"] = pd.DataFrame()
if "fuel_planning_missing_performance" not in st.session_state:
    st.session_state["fuel_planning_missing_performance"] = []
if "fuel_planning_summary" not in st.session_state:
    st.session_state["fuel_planning_summary"] = None
if "fuel_planning_recommendations" not in st.session_state:
    st.session_state["fuel_planning_recommendations"] = pd.DataFrame()
if "fuel_planning_last_df" not in st.session_state:
    st.session_state["fuel_planning_last_df"] = pd.DataFrame()
if "fuel_planning_editor_df" not in st.session_state:
    st.session_state["fuel_planning_editor_df"] = pd.DataFrame()

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

    inferred_type = _infer_aircraft_type(fl3xx_normalized, tail_input)
    if inferred_type:
        st.session_state["fuel_planning_aircraft_type"] = inferred_type
        st.session_state["fuel_planning_last_aircraft_type"] = inferred_type
        st.session_state["fuel_planning_target_landing_fuel"] = float(
            TARGET_LANDING_FUEL_LBS[inferred_type]
        )

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
        st.session_state["fuel_planning_df"] = pd.DataFrame()
        st.session_state["fuel_planning_missing_performance"] = []
        st.session_state["fuel_planning_recommendations"] = pd.DataFrame()
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
            required_departure_fuel = _calculate_required_departure_fuel(
                perf,
                float(st.session_state["fuel_planning_target_landing_fuel"]),
            )

            rows.append(
                {
                    "Departure": ff_record.departure_airport,
                    "Arrival": ff_record.arrival_airport,
                    "Dep Time (UTC)": _format_timestamp(ff_record.departure_time),
                    "Arr Time (UTC)": _format_timestamp(ff_record.arrival_time),
                    "Fuel To Dest (lb)": fuel_to_destination,
                    "Taxi Fuel (lb)": perf.get("taxi_fuel"),
                    "Max Total Fuel (lb)": perf.get("max_total_fuel"),
                    "Required Dep Fuel (lb)": required_departure_fuel,
                    "Fuel Price ($/unit)": None,
                    "Ramp Fee ($)": None,
                    "Waiver Fuel (unit)": None,
                }
            )

    st.session_state["fuel_planning_df"] = _sort_by_departure_time(pd.DataFrame(rows))
    st.session_state["fuel_planning_missing_performance"] = missing_performance
    st.session_state["fuel_planning_recommendations"] = pd.DataFrame()
    st.session_state["fuel_planning_last_df"] = st.session_state["fuel_planning_df"].copy()
    st.session_state["fuel_planning_editor_df"] = st.session_state["fuel_planning_df"].copy()

summary = st.session_state.get("fuel_planning_summary")
fuel_df = st.session_state.get("fuel_planning_df")
if _is_editor_state(fuel_df):
    fuel_df = st.session_state.get("fuel_planning_last_df", pd.DataFrame())
fuel_df = _ensure_dataframe(
    fuel_df,
    st.session_state.get("fuel_planning_last_df", pd.DataFrame()),
)

if summary is not None and fuel_df is not None and not fuel_df.empty:
    st.subheader("Matched legs")
    st.caption(
        f"{summary['matched']} matched • {summary['unmatched_foreflight']} ForeFlight-only • "
        f"{summary['unmatched_fl3xx']} FL3XX-only"
    )

missing_performance = st.session_state.get("fuel_planning_missing_performance", [])
if fuel_df is not None and not fuel_df.empty:
    if missing_performance:
        st.warning(
            "Missing performance data for: "
            + ", ".join(missing_performance)
        )

    st.markdown("### Fuelerlinx inputs")
    st.caption("Enter values per departure airport. Units should align with the ForeFlight fuel unit (lb).")

    editor_source = _ensure_dataframe(
        st.session_state.get("fuel_planning_editor_df"),
        fuel_df,
    )
    st.dataframe(editor_source, use_container_width=True, hide_index=True)

    updated_df = editor_source.copy()
    with st.form("fuelerlinx_inputs"):
        for index, row in editor_source.iterrows():
            st.markdown(f"**{row.get('Departure', 'Unknown')} → {row.get('Arrival', '')}**")
            col1, col2, col3 = st.columns(3)
            fuel_price_value = row.get("Fuel Price ($/unit)")
            ramp_fee_value = row.get("Ramp Fee ($)")
            waiver_fuel_value = row.get("Waiver Fuel (unit)")

            with col1:
                updated_df.at[index, "Fuel Price ($/unit)"] = st.number_input(
                    "Fuel Price ($/unit)",
                    min_value=0.0,
                    step=0.01,
                    value=float(fuel_price_value) if pd.notna(fuel_price_value) else 0.0,
                    key=f"fuelerlinx_price_{index}",
                )
            with col2:
                updated_df.at[index, "Ramp Fee ($)"] = st.number_input(
                    "Ramp Fee ($)",
                    min_value=0.0,
                    step=1.0,
                    value=float(ramp_fee_value) if pd.notna(ramp_fee_value) else 0.0,
                    key=f"fuelerlinx_ramp_{index}",
                )
            with col3:
                updated_df.at[index, "Waiver Fuel (unit)"] = st.number_input(
                    "Waiver Fuel (unit)",
                    min_value=0.0,
                    step=10.0,
                    value=float(waiver_fuel_value) if pd.notna(waiver_fuel_value) else 0.0,
                    key=f"fuelerlinx_waiver_{index}",
                )
        submitted = st.form_submit_button("Save Fuelerlinx inputs")

    if submitted:
        st.session_state["fuel_planning_df"] = updated_df.copy()
        st.session_state["fuel_planning_last_df"] = updated_df.copy()
        st.session_state["fuel_planning_editor_df"] = updated_df.copy()

    st.markdown("### Decision logic (MVP)")
    st.caption(
        "Generate instructional recommendations that satisfy target landing fuel and compare "
        "ramp-fee waivers vs fuel price savings."
    )
    if st.button("Generate recommendations"):
        st.session_state["fuel_planning_recommendations"] = _build_recommendations(
            st.session_state.get("fuel_planning_editor_df", editor_source)
        )

recommendations_df = st.session_state.get("fuel_planning_recommendations")
if recommendations_df is not None and not recommendations_df.empty:
    st.markdown("### Recommendations")
    st.dataframe(recommendations_df, use_container_width=True, hide_index=True)
