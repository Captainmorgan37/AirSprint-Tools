from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from collections.abc import Mapping
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from duty_clearance import compute_clearance_table
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from fl3xx_api import MOUNTAIN_TIME_ZONE
from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Crew Confirmation Monitor")
password_gate()
render_sidebar()

st.title("🧑‍✈️ Crew Confirmation Monitor")

st.write(
    """
    Track which flight crews still need to confirm before rest begins. Pick the duty date you
    want to monitor—typically tomorrow—and fetch the latest readiness status from FL3XX.
    """
)


def _load_fl3xx_settings() -> Optional[Dict[str, Any]]:
    """Return FL3XX settings from Streamlit secrets when available."""

    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None

    # Streamlit exposes secrets as a mapping-like object. Indexing raises a
    # KeyError if the section is missing, so guard access carefully.
    try:
        section = secrets["fl3xx_api"]
    except Exception:
        return None

    if isinstance(section, Mapping):
        return dict(section)

    if isinstance(section, dict):  # pragma: no cover - defensive fallback
        return dict(section)

    # Section proxies sometimes provide an .items() iterator even if they are
    # not formal Mapping subclasses. Handle that gracefully.
    items_getter = getattr(section, "items", None)
    if callable(items_getter):  # pragma: no cover - defensive fallback
        return dict(items_getter())

    return None


def _format_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return ""


now_mt = datetime.now(tz=MOUNTAIN_TIME_ZONE)
default_target_date = now_mt.date() + timedelta(days=1)

with st.form("duty-clearance-form"):
    target_date: date = st.date_input(
        "Duty date",
        value=default_target_date,
        help="Duty start date to evaluate. The table will include each crew whose first report time falls on this date.",
    )
    submitted = st.form_submit_button("Fetch duty clearance", type="primary")

if submitted:
    st.session_state["crew_confirmation_last_target_date"] = target_date

stored_target_date = st.session_state.get("crew_confirmation_last_target_date")
if isinstance(stored_target_date, date):
    target_date = stored_target_date

if not submitted and "crew_confirmation_last_results" not in st.session_state:
    st.info("Select a duty date and run the report to load the latest crew readiness status.")
    st.stop()

if not isinstance(target_date, date):
    st.error("Invalid duty date selected.")
    st.stop()

fl3xx_settings = _load_fl3xx_settings()
if not fl3xx_settings:
    st.error(
        "FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml` under the `fl3xx_api` section and reload the app."
    )
    st.stop()

try:
    config = build_fl3xx_api_config(fl3xx_settings)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

if submitted or "crew_confirmation_last_results" not in st.session_state:
    with st.spinner("Fetching duty clearance data from FL3XX…"):
        try:
            display_df, raw_df, troubleshooting_df = compute_clearance_table(
                config,
                target_date,
            )
        except Exception as exc:
            st.error(f"Unable to load duty clearance data: {exc}")
            st.stop()

    st.session_state["crew_confirmation_last_results"] = {
        "display_df": display_df,
        "raw_df": raw_df,
        "troubleshooting_df": troubleshooting_df,
    }
else:
    stored_results = st.session_state.get("crew_confirmation_last_results", {})
    display_df = stored_results.get("display_df", pd.DataFrame())
    raw_df = stored_results.get("raw_df", pd.DataFrame())
    troubleshooting_df = stored_results.get("troubleshooting_df", pd.DataFrame())

if raw_df.empty:
    st.success(
        "No active duties found for the selected date. Everyone is either clear or no crews are scheduled."
    )
    if "troubleshooting_df" in locals() and not troubleshooting_df.empty:
        st.warning(
            "We requested data from FL3XX but some flights were skipped because required details were missing. Review the troubleshooting table below for next steps."
        )
        st.dataframe(troubleshooting_df, width="stretch", hide_index=True)
        st.caption(
            "Troubleshooting tips show which flights were filtered out—for example missing report times or preflight data. Fix the issue in FL3XX and re-run the report."
        )
    st.stop()

presentation_df = raw_df.copy()

if "_confirm_by_mt" in presentation_df.columns:
    presentation_df["Clear by (MT)"] = presentation_df["_confirm_by_mt"].apply(
        lambda value: value.strftime("%Y-%m-%d %H:%M %Z") if isinstance(value, datetime) else ""
    )

positioning_mask = presentation_df.get("_is_positioning", False)
if not isinstance(positioning_mask, pd.Series):
    positioning_mask = pd.Series([False] * len(presentation_df), index=presentation_df.index)

flight_df = presentation_df.loc[~positioning_mask].reset_index(drop=True)
positioning_df = presentation_df.loc[positioning_mask].reset_index(drop=True)

not_confirmed = int((flight_df["Status"] == "⚠️ Not Confirmed").sum())
unknown = int((flight_df["Status"] == "⏳ UNKNOWN").sum())
total_crews = int(len(flight_df))

metrics_row = st.columns(3)
metrics_row[0].metric("Crews monitored", total_crews)
metrics_row[1].metric("Not confirmed", not_confirmed)
metrics_row[2].metric("Status unknown", unknown)

st.caption(
    "Confirm-by deadlines are computed per crew using their local duty timezone. Time left updates each time the report is run."
)

not_confirmed_columns = [
    "Tail",
    "Crew",
    "Clear by (MT)",
    "Report (local)",
    "First ETD (local)",
    "Status",
    "Time left",
]

confirmed_columns = [
    "Tail",
    "Crew",
    "Clear by (MT)",
    "Report (local)",
    "First ETD (local)",
    "Status",
]

if not positioning_df.empty:
    positioning_df["Report lead (min)"] = positioning_df.get("_minutes_before_departure")

not_confirmed_mask = flight_df["Status"] != "✅ Confirmed"
not_confirmed_df = flight_df.loc[not_confirmed_mask, not_confirmed_columns].reset_index(drop=True)
confirmed_df = flight_df.loc[~not_confirmed_mask, confirmed_columns].reset_index(drop=True)

if not positioning_df.empty:
    st.info(
        "Positioning confirmations are separated below when the report time is more than 120 minutes before first departure."
    )

st.subheader("Crews requiring confirmation")
if not not_confirmed_df.empty:
    minutes_left_series = flight_df.loc[not_confirmed_mask, "_minutes_left"].reset_index(drop=True)

    def _highlight_time_left(row: pd.Series) -> list[str]:
        minutes_left = minutes_left_series.iloc[row.name]
        if pd.isna(minutes_left):
            return [""] * len(row)
        if minutes_left < 120:
            return ["background-color: #7f1d1d; color: #ffffff"] * len(row)
        if minutes_left < 300:
            return ["background-color: #ffd54f; color: #1a1200"] * len(row)
        return [""] * len(row)

    styled_not_confirmed = not_confirmed_df.style.apply(_highlight_time_left, axis=1)
    st.dataframe(styled_not_confirmed, width="stretch", hide_index=True)
else:
    st.success("All crews are confirmed.")

st.subheader("Confirmed crews")
if not confirmed_df.empty:
    st.dataframe(confirmed_df, width="stretch", hide_index=True)
else:
    st.info("No crews are currently marked as confirmed.")

if not positioning_df.empty:
    st.subheader("Positioning confirmations")
    positioning_columns = [
        "Tail",
        "Crew",
        "Report (local)",
        "First ETD (local)",
        "Report lead (min)",
        "Status",
    ]
    st.dataframe(positioning_df[positioning_columns], width="stretch", hide_index=True)

if not troubleshooting_df.empty:
    with st.expander("Troubleshooting details"):
        st.dataframe(troubleshooting_df, width="stretch", hide_index=True)
        st.caption(
            "Entries in this table were skipped because required information was unavailable. Resolve the issue in FL3XX—such as adding crew check-ins or departure times—and rerun the report."
        )

with st.expander("Download data or inspect raw fields"):
    download_df = raw_df.copy()
    for column in ["_confirm_by_local", "_confirm_by_mt", "_report_local_dt", "_first_dep_local_dt"]:
        if column in download_df.columns:
            download_df[column] = download_df[column].apply(_format_datetime)
    download_df["_generated_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    csv_bytes = download_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"duty-clearance-{target_date.isoformat()}.csv",
        mime="text/csv",
    )

    st.dataframe(download_df, width="stretch")

st.caption(
    "Need to refresh after making changes in FL3XX? Re-run the report to pull the latest check-in and clearance status."
)
