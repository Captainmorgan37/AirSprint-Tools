from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt
from typing import Any

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from feasibility.checker_aircraft import (
    get_endurance_limit_minutes,
    get_supported_endurance_aircraft_types,
)


DEFAULT_MAX_FLIGHT_TIME_BY_PAX_HOURS = (
    (1, 4, 4.5),
    (5, 8, 4.0),
    (9, 12, 3.5),
    (13, 50, 3.0),
)
MIN_RUNWAY_LENGTH_FT = 5000


@dataclass(frozen=True)
class Airport:
    icao: str
    iata: str | None
    lid: str | None
    name: str
    city: str | None
    country: str | None
    lat: float
    lon: float
    max_runway_length_ft: float | None
    customs_available: bool


configure_page(page_title="Fuel Stop Advisor")
password_gate()
render_sidebar()

st.title("⛽ Fuel Stop Advisor")
st.caption(
    "Estimate whether a direct leg exceeds the maximum flight-time limits and suggest fuel/customs stops "
    "with ≥ 5,000' runways based on the airport TZ and runway datasets."
)


@st.cache_data(show_spinner=False)
def _load_airports() -> pd.DataFrame:
    airports = pd.read_csv("Airport TZ.txt")
    airports["icao"] = airports["icao"].astype(str).str.upper()
    airports["iata"] = airports["iata"].astype(str).str.upper()
    airports["lid"] = airports["lid"].astype(str).str.upper()
    return airports


@st.cache_data(show_spinner=False)
def _load_runways() -> pd.DataFrame:
    runways = pd.read_csv("runways.csv")
    runways["length_ft"] = pd.to_numeric(runways["length_ft"], errors="coerce")
    runways = runways.dropna(subset=["length_ft"])
    return runways.groupby("airport_ident", as_index=False)["length_ft"].max()


@st.cache_data(show_spinner=False)
def _load_customs() -> pd.DataFrame:
    customs = pd.read_csv("customs_rules.csv")
    customs["airport_icao"] = customs["airport_icao"].astype(str).str.upper()
    return customs


@st.cache_data(show_spinner=False)
def _build_airport_catalog() -> pd.DataFrame:
    airports = _load_airports()
    runways = _load_runways()
    customs = _load_customs()

    airports = airports.merge(
        runways,
        how="left",
        left_on="icao",
        right_on="airport_ident",
    )
    airports["customs_available"] = airports["icao"].isin(customs["airport_icao"])
    airports = airports.rename(columns={"length_ft": "max_runway_length_ft"})
    return airports


AIRPORTS = _build_airport_catalog()


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "NAN":
        return None
    return text


def _clean_optional_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _lookup_airport(code: str) -> Airport | None:
    normalized = code.strip().upper()
    if not normalized:
        return None
    for column in ("icao", "iata", "lid"):
        matches = AIRPORTS.loc[AIRPORTS[column] == normalized]
        if not matches.empty:
            row = matches.iloc[0]
            return Airport(
                icao=row["icao"],
                iata=_clean_optional(row.get("iata")),
                lid=_clean_optional(row.get("lid")),
                name=row.get("name") or "Unknown",
                city=_clean_optional(row.get("city")),
                country=_clean_optional(row.get("country")),
                lat=float(row.get("lat")),
                lon=float(row.get("lon")),
                max_runway_length_ft=_clean_optional_number(row.get("max_runway_length_ft")),
                customs_available=bool(row.get("customs_available")),
            )
    return None


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_nm = 3440.065
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return 2 * earth_radius_nm * asin(sqrt(a))


def _max_flight_time_for_pax_default(pax: int) -> float:
    for start, end, max_hours in DEFAULT_MAX_FLIGHT_TIME_BY_PAX_HOURS:
        if start <= pax <= end:
            return max_hours
    return DEFAULT_MAX_FLIGHT_TIME_BY_PAX_HOURS[-1][2]


st.subheader("Trip inputs")

col1, col2, col3 = st.columns(3)
with col1:
    departure_code = st.text_input("Departure airport code", value="", placeholder="ICAO/IATA")
with col2:
    arrival_code = st.text_input("Arrival airport code", value="", placeholder="ICAO/IATA")
with col3:
    pax_count = st.number_input("Passengers", min_value=1, max_value=50, value=6)

EXCLUDED_AIRCRAFT_TYPES = {"CITATION CJ4", "PILATUS PC-12"}
SUPPORTED_AIRCRAFT_TYPES = tuple(
    aircraft
    for aircraft in get_supported_endurance_aircraft_types()
    if aircraft not in EXCLUDED_AIRCRAFT_TYPES
)

aircraft_type = st.selectbox(
    "Aircraft type",
    options=SUPPORTED_AIRCRAFT_TYPES,
    index=0,
    help="Uses feasibility aircraft+pax endurance limits for stop-needed decisions.",
)

col4, col5, col6 = st.columns(3)
with col4:
    planned_time_hours = st.number_input(
        "Planned direct flight time (hours)",
        min_value=0.1,
        max_value=20.0,
        value=3.5,
        step=0.1,
    )
with col5:
    detour_ratio = st.slider(
        "Max detour ratio for stops",
        min_value=1.05,
        max_value=1.5,
        value=1.2,
        step=0.05,
    )
with col6:
    override_max = st.checkbox("Override max flight time", value=False)

pax_count_int = int(pax_count)
endurance_limit_minutes = get_endurance_limit_minutes(aircraft_type, pax_count_int)

if endurance_limit_minutes is not None:
    max_flight_time_hours = endurance_limit_minutes / 60.0
else:
    max_flight_time_hours = _max_flight_time_for_pax_default(pax_count_int)

if override_max:
    max_flight_time_hours = st.number_input(
        "Max flight time (hours)",
        min_value=0.5,
        max_value=20.0,
        value=float(max_flight_time_hours),
        step=0.1,
    )
else:
    if endurance_limit_minutes is not None:
        st.caption(
            "Max flight time derived from feasibility aircraft+pax limits: "
            f"**{max_flight_time_hours:.2f} hr** ({endurance_limit_minutes} min)."
        )
    else:
        st.warning(
            f"No feasibility endurance value is defined for {aircraft_type} at {pax_count_int} pax; using default pax bands."
        )
        st.caption(
            "Max flight time derived from default pax bands: "
            f"**{max_flight_time_hours:.1f} hr**."
        )


if departure_code and arrival_code:
    departure = _lookup_airport(departure_code)
    arrival = _lookup_airport(arrival_code)

    if not departure or not arrival:
        st.error("One or both airport codes were not found in the airport TZ dataset.")
        st.stop()

    direct_distance_nm = _haversine_nm(
        departure.lat,
        departure.lon,
        arrival.lat,
        arrival.lon,
    )
    speed_kts = direct_distance_nm / planned_time_hours
    max_leg_distance_nm = max_flight_time_hours * speed_kts

    is_international = bool(departure.country and arrival.country and departure.country != arrival.country)
    require_customs = st.checkbox(
        "Require customs-capable stop(s)",
        value=is_international,
        help="Enabled by default when the departure and arrival countries differ.",
    )

    st.subheader("Direct leg check")
    summary_cols = st.columns(4)
    summary_cols[0].metric("Direct distance (nm)", f"{direct_distance_nm:.0f}")
    summary_cols[1].metric("Planned speed (kts)", f"{speed_kts:.0f}")
    summary_cols[2].metric("Max leg distance (nm)", f"{max_leg_distance_nm:.0f}")
    summary_cols[3].metric("Max flight time (hr)", f"{max_flight_time_hours:.1f}")

    if planned_time_hours <= max_flight_time_hours:
        st.success("The direct leg is within the maximum flight-time limit. No fuel stop required.")
        st.stop()

    st.warning(
        "Direct leg exceeds the maximum flight-time limit. Suggested fuel/customs stops are listed below."
    )

    candidates = AIRPORTS.copy()
    candidates = candidates.loc[
        (candidates["max_runway_length_ft"] >= MIN_RUNWAY_LENGTH_FT)
        & (candidates["icao"] != departure.icao)
        & (candidates["icao"] != arrival.icao)
    ].copy()

    if require_customs:
        candidates = candidates.loc[candidates["customs_available"]]

    def compute_leg_distances(lat: float, lon: float) -> tuple[float, float]:
        return (
            _haversine_nm(departure.lat, departure.lon, lat, lon),
            _haversine_nm(lat, lon, arrival.lat, arrival.lon),
        )

    distances = candidates.apply(
        lambda row: compute_leg_distances(row["lat"], row["lon"]), axis=1, result_type="expand"
    )
    candidates["leg1_nm"] = distances[0]
    candidates["leg2_nm"] = distances[1]
    candidates["total_nm"] = candidates["leg1_nm"] + candidates["leg2_nm"]

    one_stop = candidates.loc[
        (candidates["leg1_nm"] <= max_leg_distance_nm)
        & (candidates["leg2_nm"] <= max_leg_distance_nm)
        & (candidates["total_nm"] <= direct_distance_nm * detour_ratio)
    ].copy()

    one_stop = one_stop.sort_values("total_nm").head(15)

    st.subheader("One-stop options")
    if one_stop.empty:
        st.info("No one-stop options met the runway/customs/detour criteria.")
    else:
        one_stop_display = one_stop[
            [
                "icao",
                "name",
                "city",
                "country",
                "max_runway_length_ft",
                "customs_available",
                "leg1_nm",
                "leg2_nm",
                "total_nm",
            ]
        ].copy()
        one_stop_display["max_runway_length_ft"] = one_stop_display["max_runway_length_ft"].round(0)
        one_stop_display["leg1_nm"] = one_stop_display["leg1_nm"].round(0)
        one_stop_display["leg2_nm"] = one_stop_display["leg2_nm"].round(0)
        one_stop_display["total_nm"] = one_stop_display["total_nm"].round(0)
        st.dataframe(one_stop_display, use_container_width=True, hide_index=True)

    st.subheader("Two-stop options")
    if candidates.empty:
        st.info("No candidates meet the runway/customs criteria for multi-stop routing.")
    else:
        near_departure = candidates.loc[candidates["leg1_nm"] <= max_leg_distance_nm].copy()
        near_departure = near_departure.sort_values("leg1_nm").head(25)
        near_arrival = candidates.loc[candidates["leg2_nm"] <= max_leg_distance_nm].copy()
        near_arrival = near_arrival.sort_values("leg2_nm").head(25)

        two_stop_rows = []
        for _, first in near_departure.iterrows():
            for _, second in near_arrival.iterrows():
                if first["icao"] == second["icao"]:
                    continue
                leg2_nm = _haversine_nm(
                    float(first["lat"]),
                    float(first["lon"]),
                    float(second["lat"]),
                    float(second["lon"]),
                )
                if leg2_nm > max_leg_distance_nm:
                    continue
                total_nm = float(first["leg1_nm"]) + leg2_nm + float(second["leg2_nm"])
                if total_nm > direct_distance_nm * detour_ratio:
                    continue
                two_stop_rows.append(
                    {
                        "stop_1": first["icao"],
                        "stop_2": second["icao"],
                        "stop_1_name": first["name"],
                        "stop_2_name": second["name"],
                        "stop_1_country": first["country"],
                        "stop_2_country": second["country"],
                        "stop_1_customs": bool(first["customs_available"]),
                        "stop_2_customs": bool(second["customs_available"]),
                        "leg1_nm": float(first["leg1_nm"]),
                        "leg2_nm": leg2_nm,
                        "leg3_nm": float(second["leg2_nm"]),
                        "total_nm": total_nm,
                    }
                )

        if not two_stop_rows:
            st.info("No two-stop options met the runway/customs/detour criteria.")
        else:
            two_stop_df = pd.DataFrame(two_stop_rows)
            two_stop_df = two_stop_df.sort_values("total_nm").head(15)
            for column in ("leg1_nm", "leg2_nm", "leg3_nm", "total_nm"):
                two_stop_df[column] = two_stop_df[column].round(0)
            st.dataframe(two_stop_df, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(
    "Runway length filter uses the max length in `runways.csv`. Update the max flight time mapping in this page "
    "to match your published limits. If aircraft type is supplied, feasibility endurance tables are used first."
)
