import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from reserve_calendar_pax_pull import run_reserve_pax_pull, select_reserve_dates_for_year

configure_page(page_title="Reserve Calendar PAX Pull (TEMP)")
password_gate()
render_sidebar()

st.title("🛫 Reserve Calendar PAX Pull (TEMP)")
st.caption(
    "Temporary utility to pull PAX-flight breakdowns on reserve days for a selected year "
    "between 02:00 and 23:59 Mountain Time."
)

year = st.number_input("Reserve year", min_value=2023, max_value=2035, value=2025, step=1)
reserve_dates = select_reserve_dates_for_year(int(year))

if reserve_dates:
    st.markdown(
        "**Reserve dates in scope:** "
        + ", ".join(item.strftime("%Y-%m-%d") for item in reserve_dates)
    )
else:
    st.warning(f"No reserve calendar dates were found for {year}.")

run_pull = st.button("Run PAX Pull", type="primary", disabled=not reserve_dates)
if run_pull:
    st.session_state["reserve_pax_pull_run"] = int(year)

selected_year = st.session_state.get("reserve_pax_pull_run")
if selected_year is None:
    st.info("Pick a year and click **Run PAX Pull**.")
    st.stop()

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error(
        "FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml` under `fl3xx_api`."
    )
    st.stop()

try:
    config = build_fl3xx_api_config(dict(api_settings))
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

with st.spinner("Fetching reserve-day flights from FL3XX..."):
    result = run_reserve_pax_pull(config, year=int(selected_year))

if not result.days:
    st.warning(f"No reserve days were evaluated for {selected_year}.")
    st.stop()

all_rows = []
for day in result.days:
    date_label = day.date.strftime("%Y-%m-%d")
    st.subheader(date_label)
    st.caption(
        f"Fetched: {day.diagnostics.get('fetched', 0)} | "
        f"In window (02:00-23:59 MT): {day.diagnostics.get('in_window', 0)} | "
        f"PAX flights: {day.diagnostics.get('pax_flights', 0)}"
    )

    if day.warnings:
        for warning in day.warnings:
            st.warning(warning)

    if not day.rows:
        st.info("No PAX flights found for this reserve day.")
        continue

    df = pd.DataFrame(day.rows)
    all_rows.extend(day.rows)
    st.dataframe(df, width="stretch", hide_index=True)

if all_rows:
    st.markdown("---")
    st.subheader("Combined export")
    combined_df = pd.DataFrame(all_rows)
    st.dataframe(combined_df, width="stretch", hide_index=True)
    csv_data = combined_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_data,
        file_name=f"reserve_pax_pull_{selected_year}.csv",
        mime="text/csv",
    )

if result.warnings:
    st.markdown("---")
    st.warning("Completed with warnings; review messages above.")
