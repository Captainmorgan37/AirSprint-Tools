from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from crew_presence import crew_at_airport, results_to_rows
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from ops_snapshot import pull_ops_snapshot


configure_page(page_title="Crew Availability Finder")
password_gate()
render_sidebar()

st.title("🧑‍✈️ Crew Availability Finder")
st.write(
    "Search who is likely available at a specific airport/time/fleet using the roster window already used by Gantt."
)

if "gantt_rows" not in st.session_state:
    st.session_state["gantt_rows"] = None
    st.session_state["gantt_warnings"] = []
    st.session_state["gantt_roster_meta"] = {}
    st.session_state["gantt_roster_rows"] = []

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error("Missing FL3XX API credentials in `.streamlit/secrets.toml` (`[fl3xx_api]`).")
    st.stop()

try:
    config = build_fl3xx_api_config(dict(api_settings))
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

refresh_col, info_col = st.columns([1, 3])
with refresh_col:
    if st.button("Pull / Refresh Shared Data", type="primary"):
        with st.spinner("Pulling schedule + roster snapshot..."):
            snapshot = pull_ops_snapshot(config)
        st.session_state["gantt_rows"] = snapshot["rows"]
        st.session_state["gantt_warnings"] = snapshot["warnings"]
        st.session_state["gantt_roster_meta"] = snapshot["roster_meta"]
        st.session_state["gantt_roster_rows"] = snapshot["roster_rows"]

with info_col:
    roster_meta = st.session_state.get("gantt_roster_meta", {})
    if roster_meta:
        st.caption(
            f"Shared roster window (UTC): {roster_meta.get('from', '')} to {roster_meta.get('to', '')}. "
            "This is reused across Gantt + Crew pages."
        )

warnings = st.session_state.get("gantt_warnings", [])
if warnings:
    with st.expander("Data pull warnings"):
        for warning in warnings:
            st.caption(f"• {warning}")

roster_rows = st.session_state.get("gantt_roster_rows", [])
if not roster_rows:
    st.info("No shared roster data in memory yet. Press **Pull / Refresh Shared Data**.")
    st.stop()

st.subheader("Availability query")
col1, col2, col3 = st.columns([1.2, 1, 1])

now_utc = datetime.now(UTC).replace(second=0, microsecond=0)
with col1:
    query_time = st.datetime_input("Time (UTC)", value=now_utc)
with col2:
    query_airport = st.text_input("Airport", value="CYYZ").strip().upper()
with col3:
    query_fleet = st.selectbox("Fleet", options=["CJ2", "CJ3", "EMB", "Any"], index=0)

if not query_airport:
    st.warning("Enter an airport ICAO code.")
    st.stop()

fleet_filter = "" if query_fleet == "Any" else query_fleet
results = crew_at_airport(
    roster_rows,
    at_time=query_time if isinstance(query_time, datetime) else datetime.combine(query_time, datetime.min.time(), tzinfo=UTC),
    airport=query_airport,
    fleet=fleet_filter,
)

rows: List[Dict[str, Any]] = results_to_rows(results)

st.markdown(f"**Matches:** {len(rows)}")
if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)
else:
    st.info("No crew matched the selected airport/time/fleet query.")

with st.expander("Raw shared roster rows"):
    st.write(f"Rows in memory: {len(roster_rows)}")
    st.dataframe(pd.DataFrame(roster_rows), width="stretch")
