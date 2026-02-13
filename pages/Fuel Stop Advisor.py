from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import asin, cos, radians, sin, sqrt
import os
from typing import Any

import pandas as pd
import pydeck as pdk
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
PREFERRED_RUNWAY_LENGTH_FT = 7500
MAX_RECOMMENDED_OPTIONS = 3
MAX_EVENNESS_DEGRADATION_RATIO = 1.2
MAX_EVENNESS_DEGRADATION_NM = 75
MAX_CRUISE_SPEED_KTS_BY_AIRCRAFT = {
    "CITATION CJ2+": 418,
    "CITATION CJ3+": 416,
    "CITATION CJ4": 451,
    "LEGACY 450": 462,
    "PILATUS PC-12": 290,
    "PRAETOR 500": 466,
}

_MAPBOX_TOKEN = st.secrets.get("mapbox_token")  # type: ignore[attr-defined]
if isinstance(_MAPBOX_TOKEN, str) and _MAPBOX_TOKEN.strip():
    os.environ["MAPBOX_API_KEY"] = _MAPBOX_TOKEN.strip()


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


def _hours_to_hhmm(hours: float) -> str:
    total_minutes = max(1, int(round(hours * 60)))
    hh, mm = divmod(total_minutes, 60)
    return f"{hh:02d}:{mm:02d}"


def _parse_hhmm_to_hours(value: str) -> float | None:
    text = value.strip()
    try:
        parsed = datetime.strptime(text, "%H:%M")
    except ValueError:
        return None
    return (parsed.hour * 60 + parsed.minute) / 60.0


def _split_top_options(df: pd.DataFrame, score_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, df

    sorted_df = df.sort_values(score_column, ascending=True).reset_index(drop=True)
    best_score = float(sorted_df.iloc[0][score_column])
    close_match_mask = sorted_df[score_column] <= best_score * 1.15
    top_count = int(min(MAX_RECOMMENDED_OPTIONS, max(1, close_match_mask.sum())))
    return sorted_df.head(top_count).copy(), sorted_df.iloc[top_count:].copy()


def _runway_penalty(length_ft: pd.Series) -> pd.Series:
    shortfall = (PREFERRED_RUNWAY_LENGTH_FT - length_ft).clip(lower=0, upper=PREFERRED_RUNWAY_LENGTH_FT - MIN_RUNWAY_LENGTH_FT)
    return shortfall / (PREFERRED_RUNWAY_LENGTH_FT - MIN_RUNWAY_LENGTH_FT)


def _evenness_threshold(best_evenness_nm: float) -> float:
    return max(
        best_evenness_nm * MAX_EVENNESS_DEGRADATION_RATIO,
        best_evenness_nm + MAX_EVENNESS_DEGRADATION_NM,
    )


def _render_route_map(
    departure: Airport,
    arrival: Airport,
    one_stop: pd.DataFrame,
    two_stop: pd.DataFrame | None,
) -> None:
    line_data = [
        {
            "from_lon": departure.lon,
            "from_lat": departure.lat,
            "to_lon": arrival.lon,
            "to_lat": arrival.lat,
            "color": [37, 99, 235],
            "label": f"Direct: {departure.icao} → {arrival.icao}",
        }
    ]
    point_rows = [
        {
            "lon": departure.lon,
            "lat": departure.lat,
            "label": f"Departure: {departure.icao}",
            "kind": "main",
            "color": [16, 185, 129],
        },
        {
            "lon": arrival.lon,
            "lat": arrival.lat,
            "label": f"Arrival: {arrival.icao}",
            "kind": "main",
            "color": [249, 115, 22],
        },
    ]

    if not one_stop.empty:
        for _, stop in one_stop.iterrows():
            line_data.extend(
                [
                    {
                        "from_lon": departure.lon,
                        "from_lat": departure.lat,
                        "to_lon": float(stop["lon"]),
                        "to_lat": float(stop["lat"]),
                        "color": [220, 38, 38],
                        "label": f"Suggested fuel leg: {departure.icao} → {stop['icao']}",
                    },
                    {
                        "from_lon": float(stop["lon"]),
                        "from_lat": float(stop["lat"]),
                        "to_lon": arrival.lon,
                        "to_lat": arrival.lat,
                        "color": [220, 38, 38],
                        "label": f"Suggested fuel leg: {stop['icao']} → {arrival.icao}",
                    },
                ]
            )
            point_rows.append(
                {
                    "lon": float(stop["lon"]),
                    "lat": float(stop["lat"]),
                    "label": f"Fuel stop: {stop['icao']}",
                    "kind": "fuel",
                    "color": [220, 38, 38],
                }
            )
    elif two_stop is not None and not two_stop.empty:
        first_option = two_stop.iloc[0]
        stop_1 = _lookup_airport(str(first_option["stop_1"]))
        stop_2 = _lookup_airport(str(first_option["stop_2"]))
        if stop_1 and stop_2:
            route_points = [departure, stop_1, stop_2, arrival]
            for idx in range(len(route_points) - 1):
                source = route_points[idx]
                target = route_points[idx + 1]
                line_data.append(
                    {
                        "from_lon": source.lon,
                        "from_lat": source.lat,
                        "to_lon": target.lon,
                        "to_lat": target.lat,
                        "color": [220, 38, 38],
                        "label": f"Suggested fuel leg: {source.icao} → {target.icao}",
                    }
                )
            point_rows.extend(
                [
                    {
                        "lon": stop_1.lon,
                        "lat": stop_1.lat,
                        "label": f"Fuel stop: {stop_1.icao}",
                        "kind": "fuel",
                        "color": [220, 38, 38],
                    },
                    {
                        "lon": stop_2.lon,
                        "lat": stop_2.lat,
                        "label": f"Fuel stop: {stop_2.icao}",
                        "kind": "fuel",
                        "color": [220, 38, 38],
                    },
                ]
            )

    if not line_data:
        return

    unique_points = list({(row["lon"], row["lat"], row["label"]): row for row in point_rows}.values())
    avg_lat = sum(point["lat"] for point in unique_points) / len(unique_points)
    avg_lon = sum(point["lon"] for point in unique_points) / len(unique_points)

    lines_layer = pdk.Layer(
        "LineLayer",
        data=line_data,
        get_source_position="[from_lon, from_lat]",
        get_target_position="[to_lon, to_lat]",
        get_color="color",
        get_width=4,
        pickable=True,
    )
    points_layer = pdk.Layer(
        "ScatterplotLayer",
        data=unique_points,
        get_position="[lon, lat]",
        get_color="color",
        get_radius=6000,
        pickable=True,
    )
    labels_layer = pdk.Layer(
        "TextLayer",
        data=unique_points,
        get_position="[lon, lat]",
        get_text="label",
        get_size=14,
        get_color=[255, 255, 255],
        get_alignment_baseline="bottom",
        get_pixel_offset=[0, -12],
    )

    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/dark-v10",
            initial_view_state=pdk.ViewState(latitude=avg_lat, longitude=avg_lon, zoom=3.4),
            layers=[lines_layer, points_layer, labels_layer],
            tooltip={"text": "{label}"},
        ),
        use_container_width=True,
    )


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
    planned_time_hhmm = st.text_input(
        "Planned direct flight time (HH:MM)",
        value="",
        help=(
            "Optional. Use 24-hour HH:MM duration format (for example 03:30 or 04:15). "
            "If left blank, an estimated time is computed from great-circle distance and aircraft max cruise speed."
        ),
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
    override_max_hhmm = st.text_input(
        "Max flight time (HH:MM)",
        value=_hours_to_hhmm(max_flight_time_hours),
        help="Use 24-hour HH:MM duration format.",
    )
    parsed_max = _parse_hhmm_to_hours(override_max_hhmm)
    if parsed_max is None or not (0.5 <= parsed_max <= 20.0):
        st.error("Max flight time must be in HH:MM format between 00:30 and 20:00.")
        st.stop()
    max_flight_time_hours = parsed_max
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
    planned_time_hours: float | None
    used_auto_planned_time = False
    planned_time_hhmm_clean = planned_time_hhmm.strip()
    if planned_time_hhmm_clean:
        planned_time_hours = _parse_hhmm_to_hours(planned_time_hhmm_clean)
        if planned_time_hours is None or not (0.1 <= planned_time_hours <= 20.0):
            st.error("Planned direct flight time must be in HH:MM format between 00:06 and 20:00.")
            st.stop()
    else:
        cruise_speed_kts = MAX_CRUISE_SPEED_KTS_BY_AIRCRAFT.get(aircraft_type)
        if cruise_speed_kts is None:
            st.error(
                "No cruise speed is configured for this aircraft type. Enter a planned direct flight time to continue."
            )
            st.stop()
        planned_time_hours = direct_distance_nm / cruise_speed_kts
        used_auto_planned_time = True

    if used_auto_planned_time:
        st.caption(
            "Estimated direct flight time from max cruise speed "
            f"(**{cruise_speed_kts} kts**): **{_hours_to_hhmm(planned_time_hours)}**. "
            "Enter a planned direct flight time above to override this estimate."
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

    if not is_international and departure.country and arrival.country:
        candidates = candidates.loc[candidates["country"] == departure.country]

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

    one_stop["evenness_score"] = (one_stop["leg1_nm"] - one_stop["leg2_nm"]).abs()
    one_stop["detour_nm"] = one_stop["total_nm"] - direct_distance_nm
    one_stop["runway_penalty"] = _runway_penalty(one_stop["max_runway_length_ft"])
    if not one_stop.empty:
        best_evenness = float(one_stop["evenness_score"].min())
        evenness_threshold = _evenness_threshold(best_evenness)
        one_stop["evenness_over_threshold"] = (
            one_stop["evenness_score"] - evenness_threshold
        ).clip(lower=0)
        one_stop = one_stop.sort_values(
            ["evenness_over_threshold", "runway_penalty", "evenness_score", "detour_nm"],
            ascending=[True, True, True, True],
        )

    top_one_stop, additional_one_stop = _split_top_options(one_stop, "evenness_over_threshold")

    st.subheader("One-stop options (recommended)")
    if top_one_stop.empty:
        st.info("No one-stop options met the runway/customs/detour criteria.")
    else:
        one_stop_display = top_one_stop[
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
                "evenness_score",
                "runway_penalty",
            ]
        ].copy()
        one_stop_display["max_runway_length_ft"] = one_stop_display["max_runway_length_ft"].round(0)
        one_stop_display["leg1_nm"] = one_stop_display["leg1_nm"].round(0)
        one_stop_display["leg2_nm"] = one_stop_display["leg2_nm"].round(0)
        one_stop_display["total_nm"] = one_stop_display["total_nm"].round(0)
        one_stop_display["evenness_score"] = one_stop_display["evenness_score"].round(0)
        one_stop_display["runway_penalty"] = one_stop_display["runway_penalty"].round(2)
        st.dataframe(one_stop_display, use_container_width=True, hide_index=True)

        if not additional_one_stop.empty:
            with st.expander("Additional one-stop options"):
                additional_one_stop_display = additional_one_stop[
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
                        "evenness_score",
                        "runway_penalty",
                    ]
                ].copy()
                additional_one_stop_display["max_runway_length_ft"] = additional_one_stop_display[
                    "max_runway_length_ft"
                ].round(0)
                additional_one_stop_display["leg1_nm"] = additional_one_stop_display["leg1_nm"].round(0)
                additional_one_stop_display["leg2_nm"] = additional_one_stop_display["leg2_nm"].round(0)
                additional_one_stop_display["total_nm"] = additional_one_stop_display["total_nm"].round(0)
                additional_one_stop_display["evenness_score"] = additional_one_stop_display[
                    "evenness_score"
                ].round(0)
                additional_one_stop_display["runway_penalty"] = additional_one_stop_display[
                    "runway_penalty"
                ].round(2)
                st.dataframe(additional_one_stop_display, use_container_width=True, hide_index=True)

    top_two_stop = pd.DataFrame()

    if one_stop.empty:
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
                two_stop_df["max_leg_gap_nm"] = two_stop_df[["leg1_nm", "leg2_nm", "leg3_nm"]].max(axis=1) - two_stop_df[
                    ["leg1_nm", "leg2_nm", "leg3_nm"]
                ].min(axis=1)
                two_stop_df["detour_nm"] = two_stop_df["total_nm"] - direct_distance_nm
                runway_by_icao = candidates.set_index("icao")["max_runway_length_ft"]
                two_stop_df["min_runway_length_ft"] = two_stop_df[["stop_1", "stop_2"]].apply(
                    lambda row: min(float(runway_by_icao[row["stop_1"]]), float(runway_by_icao[row["stop_2"]])),
                    axis=1,
                )
                two_stop_df["runway_penalty"] = _runway_penalty(two_stop_df["min_runway_length_ft"])
                best_gap = float(two_stop_df["max_leg_gap_nm"].min())
                gap_threshold = _evenness_threshold(best_gap)
                two_stop_df["gap_over_threshold"] = (two_stop_df["max_leg_gap_nm"] - gap_threshold).clip(lower=0)
                two_stop_df = two_stop_df.sort_values(
                    ["gap_over_threshold", "runway_penalty", "max_leg_gap_nm", "detour_nm"],
                    ascending=[True, True, True, True],
                )
                top_two_stop, additional_two_stop = _split_top_options(two_stop_df, "gap_over_threshold")

                st.subheader("Two-stop options (recommended)")
                for column in ("leg1_nm", "leg2_nm", "leg3_nm", "total_nm", "max_leg_gap_nm", "min_runway_length_ft"):
                    top_two_stop[column] = top_two_stop[column].round(0)
                top_two_stop["runway_penalty"] = top_two_stop["runway_penalty"].round(2)
                st.dataframe(top_two_stop, use_container_width=True, hide_index=True)

                if not additional_two_stop.empty:
                    with st.expander("Additional two-stop options"):
                        for column in ("leg1_nm", "leg2_nm", "leg3_nm", "total_nm", "max_leg_gap_nm", "min_runway_length_ft"):
                            additional_two_stop[column] = additional_two_stop[column].round(0)
                        additional_two_stop["runway_penalty"] = additional_two_stop["runway_penalty"].round(2)
                        st.dataframe(additional_two_stop, use_container_width=True, hide_index=True)
    else:
        st.caption("Two-stop options are hidden because one-stop routing is available.")

    st.subheader("Suggested stop map")
    _render_route_map(departure, arrival, top_one_stop, top_two_stop)

    stop_locations = []
    for _, stop in top_one_stop.iterrows():
        stop_locations.append(
            {
                "type": "One-stop",
                "icao": stop["icao"],
                "name": stop["name"],
                "city": stop["city"],
                "country": stop["country"],
                "lat": float(stop["lat"]),
                "lon": float(stop["lon"]),
            }
        )
    if not stop_locations and not top_two_stop.empty:
        first_two_stop = top_two_stop.iloc[0]
        for stop_code in (str(first_two_stop["stop_1"]), str(first_two_stop["stop_2"])):
            stop_airport = _lookup_airport(stop_code)
            if not stop_airport:
                continue
            stop_locations.append(
                {
                    "type": "Two-stop",
                    "icao": stop_airport.icao,
                    "name": stop_airport.name,
                    "city": stop_airport.city,
                    "country": stop_airport.country,
                    "lat": stop_airport.lat,
                    "lon": stop_airport.lon,
                }
            )

    if stop_locations:
        st.subheader("Suggested fuel stop locations")
        stop_locations_df = pd.DataFrame(stop_locations)
        stop_locations_df["lat"] = stop_locations_df["lat"].round(4)
        stop_locations_df["lon"] = stop_locations_df["lon"].round(4)
        st.dataframe(stop_locations_df, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption(
    "Runway length filter uses the max length in `runways.csv`. Update the max flight time mapping in this page "
    "to match your published limits. If aircraft type is supplied, feasibility endurance tables are used first."
)
