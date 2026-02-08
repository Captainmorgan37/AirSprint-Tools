from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import requests
import streamlit as st

from Home import configure_page, get_secret, password_gate, render_sidebar


FORE_FLIGHT_BASE_URL = "https://public-api.foreflight.com/public/api/Flights/flights"
ROUTE_TERMS = ("ROVMA", "KUGTC")


@dataclass
class RouteMatch:
    flight_id: str
    tail: str
    departure_airport: str
    arrival_airport: str
    departure_time: Optional[datetime]
    route: str
    matched_terms: list[str]


configure_page(page_title="Route Watcher")
password_gate()
render_sidebar()

st.title("🧭 Route Watcher")
st.caption("Scan ForeFlight flight routes for watchlist terms and surface matching flights.")


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


def _format_timestamp(value: Optional[datetime]) -> str:
    if not value:
        return "—"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _extract_first(container: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in container and container[key] not in (None, ""):
            return container[key]
    return None


def _extract_foreflight_flights(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        flights = payload.get("flights")
        if isinstance(flights, list):
            return [flight for flight in flights if isinstance(flight, Mapping)]
    if isinstance(payload, list):
        return [flight for flight in payload if isinstance(flight, Mapping)]
    return []


def _extract_route_terms(route: str, terms: Iterable[str]) -> list[str]:
    upper_route = route.upper()
    matches = [term for term in terms if term.upper() in upper_route]
    return sorted(set(matches))


def _build_match_record(flight: Mapping[str, Any], terms: Iterable[str]) -> Optional[RouteMatch]:
    route_raw = _normalize_text(flight.get("route"))
    if not route_raw:
        return None

    matched_terms = _extract_route_terms(route_raw, terms)
    if not matched_terms:
        return None

    flight_id = _normalize_text(_extract_first(flight, "flightId", "flight_id", "id")) or "—"
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
            "arrivalAirportCode",
            "arrivalAirportIcao",
        )
    )

    departure_time = _parse_datetime(
        _extract_first(
            flight,
            "departureTimeUtc",
            "departureTime",
            "scheduledDepartureTime",
            "scheduledDepartureTimeUtc",
            "departureTimeZulu",
        )
    )

    return RouteMatch(
        flight_id=flight_id,
        tail=tail or "—",
        departure_airport=departure_airport or "—",
        arrival_airport=arrival_airport or "—",
        departure_time=departure_time,
        route=route_raw,
        matched_terms=matched_terms,
    )


today = date.today()
date_range = st.date_input(
    "Date range",
    value=(today, today + timedelta(days=1)),
)

st.subheader("Watchlist terms")
st.write(", ".join(ROUTE_TERMS))

fetch = st.button("Scan routes")

if fetch:
    foreflight_token = get_secret("foreflight_api", {}).get("api_token")

    if not foreflight_token:
        st.error("ForeFlight API token is missing. Add it to Streamlit secrets under [foreflight_api].")
        st.stop()

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = today
        end_date = today + timedelta(days=1)

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

    flights = _extract_foreflight_flights(foreflight_payload)
    matches = [
        record
        for flight in flights
        for record in [_build_match_record(flight, ROUTE_TERMS)]
        if record is not None
    ]

    st.subheader("Results")
    st.caption(f"Scanned {len(flights)} flights between {start_date} and {end_date}.")

    if matches:
        rows = [
            {
                "Flight ID": match.flight_id,
                "Tail": match.tail,
                "Departure": match.departure_airport,
                "Arrival": match.arrival_airport,
                "Departure Time (UTC)": _format_timestamp(match.departure_time),
                "Route": match.route,
                "Matched Terms": ", ".join(match.matched_terms),
            }
            for match in matches
        ]
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.success("No route matches found for the selected dates.")
