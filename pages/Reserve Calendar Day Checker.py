import pandas as pd
import streamlit as st

from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from reserve_calendar_checker import (
    TARGET_DATES,
    run_reserve_day_check,
    select_reserve_dates_in_range,
    select_upcoming_reserve_dates,
)
from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Reserve Calendar Day Checker")
password_gate()
render_sidebar()

st.title("📆 Reserve Calendar Day Checker")

st.write(
    """
    This tool scans the configured reserve calendar days, fetches the associated FL3XX flights,
    and highlights any flights whose planning notes mention **club** but whose workflow is missing
    the phrase **"as available"**.
    """
)

if not TARGET_DATES:
    st.error("No reserve calendar dates are configured.")
    st.stop()

date_mode = st.radio(
    "Reserve day selection",
    options=("Upcoming count", "Date range"),
    horizontal=True,
    help="Choose whether to check the next set of reserve days or all reserve days within a date range.",
)

if date_mode == "Upcoming count":
    limit_max = min(10, len(TARGET_DATES))
    default_limit = min(4, limit_max)
    limit = st.slider(
        "Number of upcoming reserve days to check",
        min_value=1,
        max_value=limit_max,
        value=default_limit,
        help="The checker evaluates the next set of configured reserve days in chronological order.",
    )
    upcoming_dates = select_upcoming_reserve_dates(limit=limit)
else:
    min_date = min(TARGET_DATES)
    max_date = max(TARGET_DATES)
    start_date = st.date_input(
        "Start date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
    )
    end_date = st.date_input(
        "End date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )
    upcoming_dates = select_reserve_dates_in_range(start_date, end_date)

if not upcoming_dates:
    if date_mode == "Upcoming count":
        st.warning(
            "All configured reserve dates are in the past. Update the schedule to continue monitoring."
        )
    else:
        st.warning("No reserve days are configured within the selected date range.")
    st.stop()

st.markdown(
    "**Selected reserve days:** "
    + ", ".join(date_obj.strftime("%Y-%m-%d") for date_obj in upcoming_dates)
)

st.caption(
    "Departure times are normalised to America/Edmonton (Mountain Time) when evaluating each day."
)

run_check = st.button("Run Reserve Day Check", type="primary")

if run_check:
    st.session_state["reserve_day_check_should_run"] = True

if not st.session_state.get("reserve_day_check_should_run"):
    st.info("Select the reserve days you want to inspect, then press the button above.")
    st.stop()

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error(
        "FL3XX API credentials are missing. Please add them to `.streamlit/secrets.toml` under the `fl3xx_api` section."
    )
    st.stop()

try:
    config = build_fl3xx_api_config(dict(api_settings))
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

if run_check or "reserve_day_check_result" not in st.session_state:
    with st.spinner("Fetching flights and planning notes from FL3XX..."):
        result = run_reserve_day_check(config, target_dates=upcoming_dates)
    st.session_state["reserve_day_check_result"] = result
else:
    result = st.session_state["reserve_day_check_result"]

if not result.dates:
    st.info("No reserve days were evaluated.")
    st.stop()

for date_result in result.dates:
    label = date_result.date.strftime("%Y-%m-%d")
    st.subheader(f"{label}")

    diagnostics = date_result.diagnostics
    summary_parts = [
        f"Flights inspected: {diagnostics.get('total_flights', 0)}",
        f"Club matches: {diagnostics.get('club_matches', 0)}",
    ]
    missing_count = diagnostics.get("missing_as_available")
    if isinstance(missing_count, int):
        summary_parts.append(f"Missing 'as available': {missing_count}")
    targeted = diagnostics.get("targeted_flights")
    if isinstance(targeted, int):
        summary_parts.append(f"Within date window: {targeted}")
    st.caption(" | ".join(summary_parts))

    if date_result.warnings:
        for warning in date_result.warnings:
            st.warning(warning)

    if not date_result.rows:
        st.success("No club flights requiring workflow updates were found for this day.")
        continue

    df = pd.DataFrame(date_result.rows)
    highlight_columns = {"club_detected", "workflow_has_as_available"}
    highlight_indices = set()

    if highlight_columns.issubset(df.columns):
        mask = df["club_detected"] & ~df["workflow_has_as_available"]
        highlight_indices = set(df.index[mask].tolist())

    display_df = df.drop(columns=[col for col in highlight_columns if col in df.columns])

    def _highlight_row(row: pd.Series) -> list[str]:
        if row.name in highlight_indices:
            return ["background-color: rgba(255, 0, 0, 0.15);"] * len(row)
        return [""] * len(row)

    styler = display_df.style.apply(_highlight_row, axis=1)
    try:
        styler = styler.hide(axis="index")
    except AttributeError:
        pass

    st.dataframe(styler, width="stretch")

if result.warnings:
    st.markdown("---")
    st.warning(
        "The check completed with warnings. Review the messages above for affected flights or dates."
    )
