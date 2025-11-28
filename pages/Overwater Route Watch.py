from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import math
import os

import pandas as pd
import pydeck as pdk
import streamlit as st

from fl3xx_api import MOUNTAIN_TIME_ZONE
from flight_leg_utils import (
    ARRIVAL_AIRPORT_COLUMNS,
    DEPARTURE_AIRPORT_COLUMNS,
    FlightDataError,
    build_fl3xx_api_config,
    fetch_legs_dataframe,
    load_airport_metadata_lookup,
    safe_parse_dt,
)
from Home import configure_page, get_secret, password_gate, render_sidebar

_MAPBOX_TOKEN = st.secrets.get("mapbox_token")  # type: ignore[attr-defined]
if isinstance(_MAPBOX_TOKEN, str) and _MAPBOX_TOKEN.strip():
    os.environ["MAPBOX_API_KEY"] = _MAPBOX_TOKEN.strip()

TARGET_TAILS: Tuple[str, ...] = ("C-GASE", "C-FSBR")
TAIL_KEYS = {"".join(tail.upper().split("-")) for tail in TARGET_TAILS}
LAND_BUFFER_NM = 200
SAMPLES_PER_LEG = 18


configure_page(page_title="Overwater Route Watch")
password_gate()
render_sidebar()

st.title("🌊 Overwater Route Watch")
st.caption(
    "Automatically pulls the next five days of legs for focus tails and highlights any legs that stray more than"
    f" {LAND_BUFFER_NM} NM from land (based on proximity to known airports)."
)


@st.cache_data(ttl=3600)
def _airport_coordinates() -> List[Tuple[float, float]]:
    lookup = load_airport_metadata_lookup()
    coords: List[Tuple[float, float]] = []
    for record in lookup.values():
        if not isinstance(record, Dict):
            continue
        lat = record.get("lat")
        lon = record.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            coords.append((float(lat), float(lon)))
    return coords


def _normalize_tail(value: Any) -> str:
    if value is None:
        return ""
    return "".join(str(value).upper().split("-"))


def _resolve_airport_code(row: pd.Series, columns: Iterable[str]) -> Optional[str]:
    for column in columns:
        if column not in row:
            continue
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        code = str(value).strip().upper()
        if code:
            return code
    return None


def _get_airport_latlon(code: str, lookup: Dict[str, Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    if not code:
        return None
    record = lookup.get(code.upper())
    if not isinstance(record, Dict):
        return None
    lat = record.get("lat")
    lon = record.get("lon")
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        return float(lat), float(lon)
    return None


def _haversine_nm(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    sin_dlat = math.sin(dlat / 2.0)
    sin_dlon = math.sin(dlon / 2.0)
    earth_radius_m = 6371000
    distance_m = 2 * earth_radius_m * math.asin(
        math.sqrt(sin_dlat**2 + math.cos(lat1) * math.cos(lat2) * sin_dlon**2)
    )
    return distance_m / 1852


def _sample_line(
    start: Tuple[float, float], end: Tuple[float, float], samples: int = SAMPLES_PER_LEG
) -> List[Tuple[float, float]]:
    if samples < 2:
        return [start, end]
    lat1, lon1 = start
    lat2, lon2 = end
    return [
        (lat1 + (lat2 - lat1) * fraction, lon1 + (lon2 - lon1) * fraction)
        for fraction in (i / (samples - 1) for i in range(samples))
    ]


def _distance_from_land_nm(point: Tuple[float, float], candidates: List[Tuple[float, float]]) -> float:
    if not candidates:
        return float("nan")
    min_distance = float("inf")
    for candidate in candidates:
        dist = _haversine_nm(point, candidate)
        if dist < min_distance:
            min_distance = dist
    return min_distance


def _evaluate_leg_buffer(
    start: Tuple[float, float],
    end: Tuple[float, float],
    airport_coords: List[Tuple[float, float]],
) -> Tuple[float, float]:
    track_points = _sample_line(start, end)
    leg_distance = _haversine_nm(start, end)
    furthest_from_land = max(_distance_from_land_nm(point, airport_coords) for point in track_points)
    return leg_distance, furthest_from_land


def _format_departure(dep_raw: Any) -> str:
    try:
        dep_dt = safe_parse_dt(str(dep_raw))
        dep_local = dep_dt.astimezone(MOUNTAIN_TIME_ZONE)
        return dep_local.strftime("%a %b %d · %H:%M MT")
    except Exception:
        return str(dep_raw) if dep_raw is not None else "—"


@st.cache_data(ttl=300)
def _fetch_tail_legs(from_date: date, to_date: date) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    api_settings = get_secret("fl3xx_api")
    if not api_settings:
        raise FlightDataError("FL3XX API secrets are missing. Configure the `fl3xx_api` section in Streamlit secrets.")

    config = build_fl3xx_api_config(dict(api_settings))
    df, metadata, _crew = fetch_legs_dataframe(
        config,
        from_date=from_date,
        to_date=to_date,
        fetch_crew=False,
    )
    return df, metadata


def _prepare_tail_data(df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    if df.empty or "tail" not in df.columns:
        return {}

    airport_lookup = load_airport_metadata_lookup()
    airport_coords = _airport_coordinates()

    grouped: Dict[str, List[Dict[str, Any]]] = {tail: [] for tail in TARGET_TAILS}
    for _, row in df.iterrows():
        tail_value = row.get("tail")
        if _normalize_tail(tail_value) not in TAIL_KEYS:
            continue

        dep_code = _resolve_airport_code(row, DEPARTURE_AIRPORT_COLUMNS)
        arr_code = _resolve_airport_code(row, ARRIVAL_AIRPORT_COLUMNS)
        dep_coords = _get_airport_latlon(dep_code or "", airport_lookup)
        arr_coords = _get_airport_latlon(arr_code or "", airport_lookup)

        leg_distance_nm: Optional[float] = None
        max_buffer_nm: Optional[float] = None
        if dep_coords and arr_coords:
            leg_distance_nm, max_buffer_nm = _evaluate_leg_buffer(dep_coords, arr_coords, airport_coords)

        leg_info = {
            "tail": str(tail_value),
            "dep": dep_code or "?",
            "arr": arr_code or "?",
            "departure": _format_departure(row.get("dep_time")),
            "leg_distance_nm": leg_distance_nm,
            "furthest_from_land_nm": max_buffer_nm,
            "overwater_risk": (max_buffer_nm or 0) > LAND_BUFFER_NM if max_buffer_nm is not None else None,
            "dep_coords": dep_coords,
            "arr_coords": arr_coords,
        }
        grouped.setdefault(str(tail_value), []).append(leg_info)

    for legs in grouped.values():
        legs.sort(key=lambda leg: leg.get("departure", ""))

    return grouped


def _render_tail_section(tail: str, legs: List[Dict[str, Any]]) -> None:
    st.subheader(tail)
    if not legs:
        st.info("No scheduled legs found in the selected window.")
        return

    def _display_value(value: Optional[float]) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return "—"
        return f"{value:.0f}"

    display_rows = [
        {
            "Route": f"{leg['dep']} → {leg['arr']}",
            "Departure": leg["departure"],
            "Leg NM": _display_value(leg.get("leg_distance_nm")),
            f"Max > {LAND_BUFFER_NM} NM?": "⚠️ Check" if leg.get("overwater_risk") else "✅ Within buffer",
        }
        for leg in legs
    ]
    st.dataframe(pd.DataFrame(display_rows), use_container_width=True, hide_index=True)

    line_data: List[Dict[str, Any]] = []
    point_data: List[Dict[str, Any]] = []
    for leg in legs:
        if not leg.get("dep_coords") or not leg.get("arr_coords"):
            continue
        color = [220, 38, 38] if leg.get("overwater_risk") else [37, 99, 235]
        dep_lat, dep_lon = leg["dep_coords"]
        arr_lat, arr_lon = leg["arr_coords"]
        line_data.append(
            {
                "from_lon": dep_lon,
                "from_lat": dep_lat,
                "to_lon": arr_lon,
                "to_lat": arr_lat,
                "color": color,
                "tooltip": f"{leg['dep']} → {leg['arr']} ({leg['departure']})",
            }
        )
        point_data.extend(
            [
                {"lon": dep_lon, "lat": dep_lat, "label": leg["dep"]},
                {"lon": arr_lon, "lat": arr_lat, "label": leg["arr"]},
            ]
        )

    if not line_data:
        st.warning("Unable to map routes because airport coordinates are missing.")
        return

    avg_lat = sum(item["from_lat"] for item in line_data) / len(line_data)
    avg_lon = sum(item["from_lon"] for item in line_data) / len(line_data)

    layer_lines = pdk.Layer(
        "LineLayer",
        data=line_data,
        get_source_position="[from_lon, from_lat]",
        get_target_position="[to_lon, to_lat]",
        get_color="color",
        get_width=4,
        pickable=True,
    )
    layer_points = pdk.Layer(
        "ScatterplotLayer",
        data=point_data,
        get_position="[lon, lat]",
        get_color=[15, 118, 110, 180],
        get_radius=4500,
        pickable=True,
    )

    deck = pdk.Deck(
        map_style="mapbox://styles/mapbox/dark-v10",
        initial_view_state=pdk.ViewState(latitude=avg_lat, longitude=avg_lon, zoom=3.5),
        layers=[layer_lines, layer_points],
        tooltip={"text": "{tooltip}"},
    )
    st.pydeck_chart(deck)


now_mt = datetime.now(tz=MOUNTAIN_TIME_ZONE)
start_date = now_mt.date()
end_date = start_date + timedelta(days=5)

st.markdown(
    f"Checking routes for {', '.join(TARGET_TAILS)} from **{start_date}** through **{end_date - timedelta(days=1)}**."
)

try:
    legs_df, fetch_metadata = _fetch_tail_legs(start_date, end_date)
except FlightDataError as exc:  # pragma: no cover - user feedback path
    st.error(str(exc))
    st.stop()

with st.expander("Raw fetch metadata"):
    st.json(fetch_metadata)

leg_sets = _prepare_tail_data(legs_df)
for tail in TARGET_TAILS:
    _render_tail_section(tail, leg_sets.get(tail, []))
