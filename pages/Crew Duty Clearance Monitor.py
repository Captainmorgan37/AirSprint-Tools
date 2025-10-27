from __future__ import annotations

from datetime import date, datetime, timedelta
from collections.abc import Mapping
from typing import Any, Dict, Optional
import streamlit as st

from duty_clearance import compute_clearance_table
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from fl3xx_api import MOUNTAIN_TIME_ZONE
from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Crew Duty Clearance Monitor")
password_gate()
render_sidebar()

st.title("🧑‍✈️ Crew Duty Clearance Monitor")

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

if not submitted:
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

with st.spinner("Fetching duty clearance data from FL3XX…"):
    try:
        display_df, raw_df = compute_clearance_table(config, target_date)
    except Exception as exc:
        st.error(f"Unable to load duty clearance data: {exc}")
        st.stop()

if display_df.empty:
    st.success(
        "No active duties found for the selected date. Everyone is either clear or no crews are scheduled."
    )
    st.stop()

raw_minutes = raw_df.get("_minutes_left")
overdue_count = int((raw_minutes < 0).sum()) if raw_minutes is not None else 0
not_cleared = int((raw_df["Status"] == "⚠️ NOT CLEARED").sum())
unknown = int((raw_df["Status"] == "⏳ UNKNOWN").sum())
total_crews = int(len(raw_df))

metrics_row = st.columns(4)
metrics_row[0].metric("Crews monitored", total_crews)
metrics_row[1].metric("Overdue", overdue_count)
metrics_row[2].metric("Not cleared", not_cleared)
metrics_row[3].metric("Status unknown", unknown)

st.caption(
    "Confirm-by deadlines are computed per crew using their local duty timezone. Time left updates each time the report is run."
)

st.dataframe(display_df, use_container_width=True, hide_index=True)

with st.expander("Download data or inspect raw fields"):
    download_df = raw_df.copy()
    for column in ["_confirm_by_local", "_report_local_dt", "_first_dep_local_dt"]:
        if column in download_df.columns:
            download_df[column] = download_df[column].apply(_format_datetime)
    download_df["_generated_utc"] = datetime.utcnow().isoformat() + "Z"

    csv_bytes = download_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"duty-clearance-{target_date.isoformat()}.csv",
        mime="text/csv",
    )

    st.dataframe(download_df, use_container_width=True)

st.caption(
    "Need to refresh after making changes in FL3XX? Re-run the report to pull the latest check-in and clearance status."
)
