from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import json

import pandas as pd
import streamlit as st

from fl3xx_api import fetch_staff_roster
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from roster_pull import build_crew_snapshots, filter_active_roster_rows, parse_roster_payload


st.set_page_config(page_title="Roster Pull Explorer", layout="wide")
st.title("Roster Pull Explorer")
st.caption(
    "Load a staff roster pull and inspect who is where, who is active, and who appears available. "
    "Rows with both empty flights and empty entries are discarded."
)

DEFAULT_PATH = "doc/Roster_API_Pull.txt"

with st.sidebar:
    st.header("Roster source")
    source = st.radio("Choose source", ["Live FL3XX API", "Repo file", "Upload .txt/.json", "Paste JSON"], index=0)

raw_text = ""

if source == "Live FL3XX API":
    default_start = datetime.now(UTC).date() - timedelta(days=3)
    default_end = datetime.now(UTC).date() + timedelta(days=3)
    start_date = st.date_input("From date (UTC)", value=default_start)
    end_date = st.date_input("To date (UTC)", value=default_end)
    include_flights = st.checkbox("Include flights", value=True)

    if start_date > end_date:
        st.error("From date cannot be after To date.")
        st.stop()

    from_time = datetime.combine(start_date, datetime.min.time(), tzinfo=UTC)
    to_time = datetime.combine(end_date, datetime.max.time().replace(second=59, microsecond=0), tzinfo=UTC)

    try:
        api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        api_settings = None

    if not isinstance(api_settings, dict):
        st.error("Missing FL3XX API credentials in Streamlit secrets under [fl3xx_api].")
        st.stop()

    try:
        config = build_fl3xx_api_config(dict(api_settings))
    except FlightDataError as exc:
        st.error(str(exc))
        st.stop()

    with st.spinner("Fetching live roster from FL3XX..."):
        try:
            rows = fetch_staff_roster(
                config,
                from_time=from_time,
                to_time=to_time,
                filter_value="STAFF",
                include_flights=include_flights,
                drop_empty_rows=False,
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to fetch roster from FL3XX: {exc}")
            st.stop()
elif source == "Repo file":
    file_path = st.text_input("Path", value=DEFAULT_PATH)
    path = Path(file_path)
    if path.exists() and path.is_file():
        raw_text = path.read_text(encoding="utf-8")
        st.success(f"Loaded {file_path}")
    else:
        st.warning(f"File not found at {file_path}. Upload or paste instead.")
elif source == "Upload .txt/.json":
    uploaded = st.file_uploader("Roster pull file", type=["txt", "json"])
    if uploaded is not None:
        raw_text = uploaded.getvalue().decode("utf-8", errors="replace")
else:
    raw_text = st.text_area("Paste roster JSON", height=240)

if source != "Live FL3XX API":
    if not raw_text.strip():
        st.stop()

    try:
        rows = parse_roster_payload(raw_text)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

active_rows = filter_active_roster_rows(rows)

col1, col2, col3 = st.columns(3)
col1.metric("Rows in pull", len(rows))
col2.metric("Rows after filtering", len(active_rows))
col3.metric("Discarded empty rows", len(rows) - len(active_rows))

with st.expander("Preview raw row (filtered)"):
    if active_rows:
        st.json(active_rows[0], expanded=False)

st.subheader("Crew state query")
query_cols = st.columns([1, 1, 1, 1])
airport_filter = query_cols[0].text_input("Airport (ICAO/IATA)", value="")
aircraft_filter = query_cols[1].text_input("Aircraft contains", value="")
only_available = query_cols[2].checkbox("Only available", value=True)
at_time = query_cols[3].datetime_input("At time (UTC)", value=datetime.now(UTC))

if isinstance(at_time, datetime):
    query_time = at_time.astimezone(UTC) if at_time.tzinfo else at_time.replace(tzinfo=UTC)
else:
    query_time = datetime.combine(at_time, datetime.min.time(), tzinfo=UTC)

snapshots = build_crew_snapshots(active_rows, at_time=query_time)

records = [
    {
        "personnel_number": snap.personnel_number,
        "name": snap.name,
        "trigram": snap.trigram,
        "available": snap.available,
        "event_type": snap.active_event_type,
        "event_label": snap.active_event_label,
        "current_airport": snap.current_airport,
        "next_airport": snap.next_airport,
        "event_aircraft": snap.event_aircraft,
        "event_start_utc": snap.event_start_utc,
        "event_end_utc": snap.event_end_utc,
    }
    for snap in snapshots
]

frame = pd.DataFrame.from_records(records)
if frame.empty:
    st.info("No crew snapshots could be built from this pull.")
    st.stop()

if airport_filter.strip():
    filter_value = airport_filter.strip().upper()
    frame = frame[frame["current_airport"].fillna("").str.upper().str.contains(filter_value)]

if aircraft_filter.strip():
    filter_value = aircraft_filter.strip().upper()
    frame = frame[frame["event_aircraft"].fillna("").str.upper().str.contains(filter_value)]

if only_available:
    frame = frame[frame["available"]]

st.write(f"Matched crew members: **{len(frame)}**")
st.dataframe(frame.sort_values(["current_airport", "name"]).reset_index(drop=True), use_container_width=True)

st.download_button(
    "Download matched results as CSV",
    data=frame.to_csv(index=False).encode("utf-8"),
    file_name="roster_query_results.csv",
    mime="text/csv",
)

with st.expander("Debug: Parsed rows JSON"):
    st.code(json.dumps(active_rows[:3], ensure_ascii=False, indent=2)[:12000])
