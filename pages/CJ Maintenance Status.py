from datetime import date, timedelta

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from cj_maintenance_status import (
    collect_cj_maintenance_events,
    maintenance_daily_status,
)
from flight_leg_utils import FlightDataError, build_fl3xx_api_config

configure_page(page_title="CJ Maintenance Status")
password_gate()
render_sidebar()

st.title("🛠️ CJ Maintenance Status")
st.write(
    """
    Pulls each CJ aircraft schedule from FL3XX and counts, per day, how many aircraft are down for:
    - **MAINTENANCE** (scheduled)
    - **UNSCHEDULED_MAINTENANCE**
    - **AOG**

    Use the date controls to review a single day, a custom period, or a full calendar-style table.
    """
)

default_end = date.today() + timedelta(days=30)
default_start = date.today() - timedelta(days=14)

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start date", value=default_start)
with col2:
    end_date = st.date_input("End date", value=default_end)

if end_date < start_date:
    st.error("End date must be on or after start date.")
    st.stop()

run_check = st.button("Run CJ Maintenance Pull", type="primary")
if not run_check:
    st.info("Choose your dates, then run the pull.")
    st.stop()

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

with st.spinner("Pulling aircraft schedule data for all CJs..."):
    events, warnings = collect_cj_maintenance_events(config)

if warnings:
    st.warning("Some aircraft could not be fetched:")
    for warning in warnings:
        st.caption(f"• {warning}")

daily_df = maintenance_daily_status(events, start_date=start_date, end_date=end_date)

if daily_df.empty:
    st.info("No maintenance activity found in the selected period.")
    st.stop()

summary = {
    "Scheduled maint days": int(daily_df["scheduled_maintenance"].sum()),
    "Unscheduled maint days": int(daily_df["unscheduled_maintenance"].sum()),
    "AOG days": int(daily_df["aog"].sum()),
    "Total aircraft-down days": int(daily_df["total_aircraft_down"].sum()),
}

metric_cols = st.columns(4)
for idx, (label, value) in enumerate(summary.items()):
    metric_cols[idx].metric(label, value)

chart_df = daily_df.set_index("date")[["scheduled_maintenance", "unscheduled_maintenance", "aog"]]
st.subheader("Daily aircraft down count")
st.line_chart(chart_df)

st.subheader("Calendar view")
st.dataframe(daily_df, width="stretch")

selected_day = st.date_input(
    "Inspect a single day",
    value=start_date,
    min_value=start_date,
    max_value=end_date,
    key="single_day_selector",
)

single_row = daily_df.loc[daily_df["date"] == selected_day]
if not single_row.empty:
    row = single_row.iloc[0]
    st.markdown(
        f"**{selected_day.isoformat()}** → Scheduled: `{int(row['scheduled_maintenance'])}`, "
        f"Unscheduled: `{int(row['unscheduled_maintenance'])}`, AOG: `{int(row['aog'])}`, "
        f"Total down: `{int(row['total_aircraft_down'])}`"
    )

with st.expander("Raw maintenance events"):
    event_rows = [
        {
            "tail": event.tail,
            "task_id": event.task_id,
            "task_type": event.task_type,
            "start_utc": event.start_utc.isoformat(),
            "end_utc": event.end_utc.isoformat(),
            "notes": event.notes,
        }
        for event in sorted(events, key=lambda item: (item.start_utc, item.tail, item.task_type))
    ]
    st.dataframe(pd.DataFrame(event_rows), width="stretch")
