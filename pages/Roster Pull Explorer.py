from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pandas as pd
import streamlit as st

from roster_pull import build_crew_snapshots, filter_active_roster_rows, parse_roster_payload


st.set_page_config(page_title="Roster Pull Explorer", layout="wide")
st.title("Roster Pull Explorer")
st.caption(
    "Load a 5-day staff roster pull and inspect who is where, who is active, and who appears available. "
    "Rows with both empty flights and empty entries are discarded."
)

DEFAULT_PATH = "doc/Roster_API_Pull.txt"

with st.sidebar:
    st.header("Roster source")
    source = st.radio("Choose source", ["Repo file", "Upload .txt/.json", "Paste JSON"], index=0)

raw_text = ""

if source == "Repo file":
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
