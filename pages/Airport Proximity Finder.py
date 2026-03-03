from __future__ import annotations

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError

from Home import configure_page, password_gate, render_sidebar
from airport_proximity import GeocodingError, geocode_address_mapbox, nearest_airports

configure_page(page_title="Nearby Airport Finder")
password_gate()
render_sidebar()

st.title("🌍 Nearby Airport Finder")
st.caption("Find nearest airports from an address with runway/category filters.")

try:
    mapbox_token = st.secrets.get("mapbox_token")  # type: ignore[attr-defined]
except StreamlitSecretNotFoundError:
    mapbox_token = None

if not isinstance(mapbox_token, str) or not mapbox_token.strip():
    st.error("Mapbox token is missing in Streamlit secrets (`mapbox_token`).")
    st.stop()

with st.form("airport-proximity-form"):
    address = st.text_input("Address", placeholder="1600 Amphitheatre Parkway, Mountain View, CA")
    col1, col2, col3 = st.columns(3)
    with col1:
        max_results = st.number_input("Max results", min_value=1, max_value=25, value=5, step=1)
    with col2:
        min_runway_ft = st.number_input("Minimum runway length (ft)", min_value=0, max_value=25000, value=4500, step=100)
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
        lat, lon = geocode_address_mapbox(address, token=mapbox_token)
    except GeocodingError as exc:
        st.error(str(exc))
        st.stop()
    except Exception as exc:  # pragma: no cover
        st.error(f"Mapbox geocoding failed: {exc}")
        st.stop()

    st.success(f"Geocoded to lat/lon: {lat:.6f}, {lon:.6f}")
    results = nearest_airports(
        lat,
        lon,
        limit=int(max_results),
        min_runway_ft=int(min_runway_ft) if min_runway_ft else None,
        allowed_categories=allowed_categories or None,
    )

    if not results:
        st.info("No airports matched the selected runway/category filters.")
        st.stop()

    table = pd.DataFrame(
        {
            "ICAO": [r.icao for r in results],
            "Airport": [r.name for r in results],
            "City": [r.city for r in results],
            "Distance (nm)": [round(r.distance_nm, 1) for r in results],
            "Max Runway (ft)": [r.max_runway_length_ft for r in results],
            "Category": [r.airport_category for r in results],
            "Latitude": [r.latitude for r in results],
            "Longitude": [r.longitude for r in results],
        }
    )
    st.dataframe(table, use_container_width=True, hide_index=True)
    st.download_button(
        "Download CSV",
        data=table.to_csv(index=False).encode("utf-8"),
        file_name="airport_proximity_results.csv",
        mime="text/csv",
    )
