from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError

from feasibility.operational_notes import fetch_airport_notes
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from Home import configure_page, password_gate, render_sidebar
from airport_proximity import GeocodingError, nearest_airports, geocode_address_mapbox, suggest_addresses_mapbox

configure_page(page_title="Nearby Airport Finder")
password_gate()
render_sidebar()

st.title("🌍 Nearby Airport Finder")
st.caption("Find nearest airports from an address with runway/category filters.")


@st.cache_data(show_spinner=False)
def _load_fl3xx_settings() -> dict[str, object]:
    try:
        secrets_section = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        secrets_section = None

    if isinstance(secrets_section, Mapping):
        return {str(key): secrets_section[key] for key in secrets_section}
    return {}


def _build_fl3xx_config():
    cached = st.session_state.get("airport_proximity_fl3xx_config")
    if cached is not None:
        return cached

    settings = _load_fl3xx_settings()
    if not settings:
        return None
    try:
        config = build_fl3xx_api_config(settings)
    except FlightDataError:
        return None

    st.session_state["airport_proximity_fl3xx_config"] = config
    return config


def _format_note_text(note: object) -> str:
    if isinstance(note, str):
        return note.strip()
    if isinstance(note, Mapping):
        for key in ("note", "body", "title", "category", "type"):
            value = note.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return str(note)


def _render_fl3xx_notes(icao: str, notes: list[dict[str, object]]) -> None:
    if not notes:
        st.info(f"No FL3XX operational notes found for {icao}.")
        return

    rows: list[dict[str, object]] = []
    alert_flags: list[bool] = []
    for note in notes:
        rows.append({"Note": _format_note_text(note)})
        alert_flags.append(bool(note.get("alert")))

    notes_df = pd.DataFrame(rows)

    def _highlight_alert(row: pd.Series) -> list[str]:
        if alert_flags[row.name]:
            return [
                "background-color: rgba(220, 38, 38, 0.18); color: #ef4444; font-weight: 600;"
            ]
        return [""]

    st.caption(f"FL3XX operational notes for {icao}")
    st.dataframe(notes_df.style.apply(_highlight_alert, axis=1), use_container_width=True, hide_index=True)


def _serialize_results(results: Sequence[Any]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for airport in results:
        serialized.append(
            {
                "icao": airport.icao,
                "name": airport.name,
                "city": airport.city,
                "distance_nm": airport.distance_nm,
                "max_runway_length_ft": airport.max_runway_length_ft,
                "airport_category": airport.airport_category,
                "latitude": airport.latitude,
                "longitude": airport.longitude,
            }
        )
    return serialized


def _render_results(airport_rows: list[dict[str, Any]], *, lat: float | None = None, lon: float | None = None) -> None:
    if lat is not None and lon is not None:
        st.success(f"Geocoded to lat/lon: {lat:.6f}, {lon:.6f}")

    table = pd.DataFrame(
        {
            "ICAO": [r.get("icao") for r in airport_rows],
            "Airport": [r.get("name") for r in airport_rows],
            "City": [r.get("city") for r in airport_rows],
            "Distance (nm)": [round(float(r.get("distance_nm") or 0), 1) for r in airport_rows],
            "Max Runway (ft)": [r.get("max_runway_length_ft") for r in airport_rows],
            "Category": [r.get("airport_category") for r in airport_rows],
            "Latitude": [r.get("latitude") for r in airport_rows],
            "Longitude": [r.get("longitude") for r in airport_rows],
        }
    )
    st.dataframe(table, use_container_width=True, hide_index=True)
    st.download_button(
        "Download CSV",
        data=table.to_csv(index=False).encode("utf-8"),
        file_name="airport_proximity_results.csv",
        mime="text/csv",
    )

    airports_with_category = [r for r in airport_rows if r.get("airport_category")]
    if not airports_with_category:
        st.caption("No returned airports have an airport category, so FL3XX note buttons are hidden.")
        return

    st.markdown("### FL3XX Airport Notes")
    st.caption("Click an airport to fetch its FL3XX operational notes. Alert notes are highlighted.")

    notes_cache = st.session_state.setdefault("airport_proximity_notes_cache", {})
    notes_error_cache = st.session_state.setdefault("airport_proximity_notes_error_cache", {})

    config = _build_fl3xx_config()
    if config is None:
        st.warning("FL3XX API credentials are unavailable, so airport note lookups are disabled.")

    for airport in airports_with_category:
        airport_icao = str(airport.get("icao") or "").upper()
        airport_name = airport.get("name") or "Airport"
        airport_category = airport.get("airport_category")

        button_col, detail_col = st.columns([1, 3], vertical_alignment="center")
        with button_col:
            fetch_clicked = st.button(
                f"Fetch notes for {airport_icao}",
                key=f"fetch-notes-{airport_icao}",
                disabled=config is None,
                use_container_width=True,
            )
        with detail_col:
            st.markdown(f"**{airport_icao}** · {airport_name} · Category `{airport_category}`")

        if fetch_clicked and config is not None:
            try:
                notes_cache[airport_icao] = [dict(note) for note in fetch_airport_notes(config, airport_icao)]
                notes_error_cache.pop(airport_icao, None)
            except Exception as exc:  # pragma: no cover
                notes_cache.pop(airport_icao, None)
                notes_error_cache[airport_icao] = str(exc)

        error_message = notes_error_cache.get(airport_icao)
        if isinstance(error_message, str) and error_message:
            st.error(f"Could not load FL3XX notes for {airport_icao}: {error_message}")

        cached_notes = notes_cache.get(airport_icao)
        if isinstance(cached_notes, list):
            _render_fl3xx_notes(airport_icao, cached_notes)


try:
    mapbox_token = st.secrets.get("mapbox_token")  # type: ignore[attr-defined]
except StreamlitSecretNotFoundError:
    mapbox_token = None

if not isinstance(mapbox_token, str) or not mapbox_token.strip():
    st.error("Mapbox token is missing in Streamlit secrets (`mapbox_token`).")
    st.stop()

address = st.text_input("Address", placeholder="1600 Amphitheatre Parkway, Mountain View, CA")
selected_address = address

if len(address.strip()) >= 3:
    try:
        suggestions = suggest_addresses_mapbox(address, token=mapbox_token, limit=6)
    except Exception as exc:  # pragma: no cover
        st.caption(f"Address suggestions unavailable: {exc}")
    else:
        if suggestions:
            suggestion_options = [s.label for s in suggestions]
            selected_address = st.selectbox(
                "Suggested matches",
                options=suggestion_options,
                index=0,
                help="Pick a suggestion or keep your typed address.",
            )

with st.form("airport-proximity-form"):
    col1, col2, col3 = st.columns(3)
    with col1:
        max_results = st.number_input("Max results", min_value=1, max_value=25, value=5, step=1)
    with col2:
        min_runway_ft = st.number_input(
            "Minimum runway length (ft)", min_value=0, max_value=25000, value=4500, step=100
        )
    with col3:
        allowed_categories = st.multiselect(
            "Allowed categories",
            options=["A", "B", "C", "NC", "P"],
            default=["A", "B", "C"],
            help="Leave empty to allow all categories.",
        )
    submitted = st.form_submit_button("Find nearest airports")

if submitted:
    try:
        lat, lon = geocode_address_mapbox(selected_address, token=mapbox_token)
        results = nearest_airports(
            lat,
            lon,
            limit=int(max_results),
            min_runway_ft=int(min_runway_ft) if min_runway_ft else None,
            allowed_categories=allowed_categories or None,
        )
    except GeocodingError as exc:
        st.error(str(exc))
    except Exception as exc:  # pragma: no cover
        st.error(f"Mapbox geocoding failed: {exc}")
    else:
        if not results:
            st.info("No airports matched the selected runway/category filters.")
            st.session_state.pop("airport_proximity_last_results", None)
            st.session_state.pop("airport_proximity_last_geocode", None)
        else:
            st.session_state["airport_proximity_last_results"] = _serialize_results(results)
            st.session_state["airport_proximity_last_geocode"] = {"lat": lat, "lon": lon}

stored_results = st.session_state.get("airport_proximity_last_results")
stored_geocode = st.session_state.get("airport_proximity_last_geocode")
if isinstance(stored_results, list) and stored_results:
    lat = None
    lon = None
    if isinstance(stored_geocode, Mapping):
        lat_value = stored_geocode.get("lat")
        lon_value = stored_geocode.get("lon")
        if isinstance(lat_value, (int, float)) and isinstance(lon_value, (int, float)):
            lat = float(lat_value)
            lon = float(lon_value)
    _render_results(stored_results, lat=lat, lon=lon)
