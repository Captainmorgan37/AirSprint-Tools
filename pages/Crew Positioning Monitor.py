from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pandas as pd
import streamlit as st

from crew_positioning import build_positioning_statuses
from roster_pull import filter_active_roster_rows, parse_roster_payload


st.set_page_config(page_title="Crew Positioning Monitor", layout="wide")
st.title("Crew Positioning Monitor")
st.caption(
    "Prototype: derive crew positioning actions from FL3XX roster pulls so TC can book from a single queue."
)

DEFAULT_PATH = "docs/Roster_API_Pull.txt"

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

query_cols = st.columns([1, 1, 1])
at_time = query_cols[0].datetime_input("Reference time (UTC)", value=datetime.now(UTC))
only_actionable = query_cols[1].checkbox("Only actionable statuses", value=True)
status_filter = query_cols[2].multiselect(
    "Status filter",
    [
        "ACTION_REQUIRED",
        "RETURN_HOME_REQUIRED",
        "POSITIONING_BOOKED",
        "RETURN_HOME_BOOKED",
        "AT_REQUIRED",
        "NO_ACTION",
    ],
    default=[],
)

if isinstance(at_time, datetime):
    reference_time = at_time.astimezone(UTC) if at_time.tzinfo else at_time.replace(tzinfo=UTC)
else:
    reference_time = datetime.combine(at_time, datetime.min.time(), tzinfo=UTC)

statuses = build_positioning_statuses(active_rows, at_time=reference_time)
records = [
    {
        "personnel_number": item.personnel_number,
        "name": item.name,
        "trigram": item.trigram,
        "home_base_airport": item.home_base_airport,
        "current_airport": item.current_airport,
        "next_required_airport": item.next_required_airport,
        "next_required_utc": item.next_required_utc,
        "booked_positioning_route": item.booked_positioning_route,
        "booked_positioning_utc": item.booked_positioning_utc,
        "status": item.status,
        "recommendation": item.recommendation,
        "reason": item.reason,
    }
    for item in statuses
]

frame = pd.DataFrame.from_records(records)
if frame.empty:
    st.info("No positionable crew rows could be derived from this roster pull.")
    st.stop()

if only_actionable:
    frame = frame[frame["status"].isin(["ACTION_REQUIRED", "RETURN_HOME_REQUIRED"])]

if status_filter:
    frame = frame[frame["status"].isin(status_filter)]

metric_cols = st.columns(4)
metric_cols[0].metric("Rows in pull", len(rows))
metric_cols[1].metric("Rows after activity filter", len(active_rows))
metric_cols[2].metric("Crew statuses", len(statuses))
metric_cols[3].metric(
    "Action required",
    int(frame["status"].isin(["ACTION_REQUIRED", "RETURN_HOME_REQUIRED"]).sum()) if not frame.empty else 0,
)

st.write(f"Queue rows: **{len(frame)}**")
st.dataframe(
    frame.sort_values(["status", "next_required_utc", "name"], na_position="last").reset_index(drop=True),
    use_container_width=True,
)

st.download_button(
    "Download queue as CSV",
    data=frame.to_csv(index=False).encode("utf-8"),
    file_name="crew_positioning_queue.csv",
    mime="text/csv",
)

with st.expander("Debug: parsed roster sample"):
    st.code(json.dumps(active_rows[:2], ensure_ascii=False, indent=2)[:12000])
