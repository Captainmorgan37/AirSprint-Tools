from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import MOUNTAIN_TIME_ZONE
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from hotac_coverage import compute_hotac_coverage

configure_page(page_title="HOTAC Coverage Monitor")
password_gate()
render_sidebar()

st.title("🏨 HOTAC Coverage Monitor")
st.write(
    "Track end-of-day arrival hotel coverage for pilots. Action-required rows are sorted first."
)


def _load_fl3xx_settings() -> Optional[Dict[str, Any]]:
    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None

    try:
        section = secrets["fl3xx_api"]
    except Exception:
        return None

    if isinstance(section, Mapping):
        return dict(section)

    items_getter = getattr(section, "items", None)
    if callable(items_getter):
        return dict(items_getter())

    return None


now_mt = datetime.now(tz=MOUNTAIN_TIME_ZONE)
allowed_dates = [now_mt.date(), now_mt.date() + timedelta(days=1)]

with st.form("hotel-check-form"):
    target_date = st.selectbox(
        "Duty date",
        options=allowed_dates,
        index=0,
        format_func=lambda d: f"{d.isoformat()} ({'today' if d == allowed_dates[0] else 'tomorrow'})",
    )
    submitted = st.form_submit_button("Run Hotel Check", type="primary")

if submitted:
    st.session_state["hotac_coverage_target_date"] = target_date

stored_target_date = st.session_state.get("hotac_coverage_target_date")
if isinstance(stored_target_date, date):
    target_date = stored_target_date

if not submitted and "hotac_coverage_results" not in st.session_state:
    st.info("Choose today or tomorrow, then run Hotel Check.")
    st.stop()

fl3xx_settings = _load_fl3xx_settings()
if not fl3xx_settings:
    st.error("Missing `fl3xx_api` credentials in Streamlit secrets.")
    st.stop()

try:
    config = build_fl3xx_api_config(fl3xx_settings)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

if submitted or "hotac_coverage_results" not in st.session_state:
    with st.spinner("Fetching flights and HOTAC services from FL3XX…"):
        try:
            display_df, raw_df, troubleshooting_df = compute_hotac_coverage(config, target_date)
        except Exception as exc:
            st.error(f"Unable to compute HOTAC coverage: {exc}")
            st.stop()
    st.session_state["hotac_coverage_results"] = {
        "display_df": display_df,
        "raw_df": raw_df,
        "troubleshooting_df": troubleshooting_df,
    }
else:
    cached = st.session_state.get("hotac_coverage_results", {})
    display_df = cached.get("display_df", pd.DataFrame())
    raw_df = cached.get("raw_df", pd.DataFrame())
    troubleshooting_df = cached.get("troubleshooting_df", pd.DataFrame())

if raw_df.empty:
    st.warning("No pilot end-of-day HOTAC rows were generated for this date.")
    if not troubleshooting_df.empty:
        with st.expander("Troubleshooting"):
            st.dataframe(troubleshooting_df, width="stretch", hide_index=True)
    st.stop()

status_counts = raw_df["HOTAC status"].value_counts(dropna=False)
metric_cols = st.columns(5)
metric_cols[0].metric("Pilots ending day", int(len(raw_df)))
metric_cols[1].metric("Booked", int(status_counts.get("Booked", 0)))
metric_cols[2].metric("Missing", int(status_counts.get("Missing", 0)))
metric_cols[3].metric("Cancelled-only", int(status_counts.get("Cancelled-only", 0)))
metric_cols[4].metric("Unknown", int(status_counts.get("Unknown", 0)))

airport_options = sorted({value for value in raw_df["End airport"].dropna().astype(str) if value})
status_options = ["Missing", "Cancelled-only", "Unknown", "Booked"]
tail_options = sorted({value for value in raw_df["Tail"].dropna().astype(str) if value})

filter_cols = st.columns(3)
selected_airports = filter_cols[0].multiselect("Airport filter", airport_options)
selected_statuses = filter_cols[1].multiselect("Status filter", status_options, default=status_options)
selected_tails = filter_cols[2].multiselect("Tail filter", tail_options)

filtered_df = display_df.copy()
if selected_airports:
    filtered_df = filtered_df[filtered_df["End airport"].isin(selected_airports)]
if selected_statuses:
    filtered_df = filtered_df[filtered_df["HOTAC status"].isin(selected_statuses)]
if selected_tails:
    filtered_df = filtered_df[filtered_df["Tail"].isin(selected_tails)]

st.dataframe(filtered_df, width="stretch", hide_index=True)

with st.expander("Troubleshooting details"):
    if troubleshooting_df.empty:
        st.caption("No troubleshooting issues were captured.")
    else:
        st.dataframe(troubleshooting_df, width="stretch", hide_index=True)

csv_bytes = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download filtered CSV",
    data=csv_bytes,
    file_name=f"hotel-check-{target_date.isoformat()}.csv",
    mime="text/csv",
)
