from __future__ import annotations

from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any, Dict, Mapping, Optional

import pandas as pd
import streamlit as st

from Home import configure_page, get_secret, password_gate, render_sidebar
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from oca_reports import (
    MaxFlightTimeAlert,
    ZfwFlightCheck,
    evaluate_flights_for_max_time,
    evaluate_flights_for_zfw_check,
    format_duration_label,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None  # type: ignore[assignment]


_STATE_KEY = "oca_reports_state"
_MOUNTAIN_TZ_NAME = "America/Edmonton"


def _default_start_date() -> date:
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(_MOUNTAIN_TZ_NAME)
            return datetime.now(tz).date()
        except Exception:
            pass
    return date.today()


def _store_state(state: Dict[str, Any]) -> None:
    st.session_state[_STATE_KEY] = state


def _load_state() -> Dict[str, Any]:
    stored = st.session_state.get(_STATE_KEY)
    if isinstance(stored, dict):
        return stored
    return {}


def _normalise_settings(raw: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(raw, Mapping):
        return dict(raw)
    return None


def _normalise_state(state: Mapping[str, Any]) -> Dict[str, Any]:
    if "max_time" in state or "zfw" in state:
        return dict(state)

    # Backwards compatibility for states stored before the ZFW workflow existed.
    if "alerts" in state or "metadata" in state or "diagnostics" in state:
        legacy: Dict[str, Any] = {
            "start_date": state.get("start_date"),
            "end_date": state.get("end_date"),
            "days": state.get("days"),
            "max_time": {
                "alerts": state.get("alerts", []),
                "metadata": state.get("metadata", {}),
                "diagnostics": state.get("diagnostics", {}),
            },
            "zfw": {},
        }
        if state.get("error"):
            legacy["error"] = state["error"]
        return legacy

    return dict(state)


def _render_max_time_results(data: Mapping[str, Any], start_label: Optional[str], end_label: Optional[str]) -> None:
    st.subheader("⏱️ Max Flight Time Monitoring")

    error = data.get("error")
    if error:
        st.error(error)
        return

    alerts = data.get("alerts", [])
    metadata = data.get("metadata", {})
    diagnostics = data.get("diagnostics", {})

    if start_label and end_label:
        if alerts:
            st.success(
                f"Identified {len(alerts)} flight(s) exceeding the configured block time limits "
                f"between {start_label} and {end_label}."
            )
        else:
            st.info(
                f"No flights exceeded the configured thresholds between {start_label} and {end_label}."
            )

    if alerts or start_label:
        summary_cols = st.columns(4)
        summary_cols[0].metric("Flights flagged", diagnostics.get("flagged_flights", len(alerts)))
        summary_cols[1].metric("PAX flights evaluated", diagnostics.get("pax_flights", 0))
        summary_cols[2].metric("Flights missing duration", diagnostics.get("missing_duration", 0))
        summary_cols[3].metric("Notes fetched", diagnostics.get("notes_requested", 0))

        note_cols = st.columns(2)
        note_cols[0].metric(
            "Notes with FPL confirmation", diagnostics.get("booking_note_confirmations", 0)
        )
        note_cols[1].metric("Existing booking notes", diagnostics.get("notes_found", 0))

    if diagnostics.get("note_errors"):
        st.warning(
            "Some booking notes could not be retrieved. Review the diagnostics below for details."
        )

    df = pd.DataFrame(alerts)
    if not df.empty:
        df = df.copy()
        df["Duration"] = df["duration_minutes"].map(format_duration_label)
        df["Limit"] = df["threshold_minutes"].map(format_duration_label)
        df["Over by"] = df["overage_minutes"].map(format_duration_label)
        df["Booking note present"] = df["booking_note_present"].map(lambda v: "Yes" if v else "No")
        df["FPL run confirmed"] = df["booking_note_confirms_fpl"].map(lambda v: "Yes" if v else "No")

        columns = [
            "departure_utc",
            "arrival_utc",
            "registration",
            "airport_from",
            "airport_to",
            "flight_reference",
            "aircraft_category",
            "pax_count",
            "Duration",
            "Limit",
            "Over by",
            "Booking note present",
            "FPL run confirmed",
            "booking_note",
        ]
        available_columns = [col for col in columns if col in df.columns]
        display_df = df[available_columns]

        st.dataframe(display_df, use_container_width=True)

        csv_buffer = StringIO()
        export_columns = [
            "flight_id",
            "quote_id",
            "flight_reference",
            "departure_utc",
            "arrival_utc",
            "registration",
            "airport_from",
            "airport_to",
            "aircraft_category",
            "pax_count",
            "duration_minutes",
            "threshold_minutes",
            "overage_minutes",
            "booking_note_present",
            "booking_note_confirms_fpl",
            "booking_note",
        ]
        export_df = df[[col for col in export_columns if col in df.columns]]
        export_df.to_csv(csv_buffer, index=False)
        st.download_button(
            "Download flagged flights as CSV",
            csv_buffer.getvalue(),
            file_name="oca_max_flight_time_alerts.csv",
            mime="text/csv",
        )

    with st.expander("FL3XX request metadata", expanded=False):
        st.json(metadata)

    with st.expander("Diagnostics", expanded=False):
        st.json(diagnostics)


def _render_zfw_results(data: Mapping[str, Any], start_label: Optional[str], end_label: Optional[str]) -> None:
    st.subheader("⚖️ Zero Fuel Weight Checks")

    error = data.get("error")
    if error:
        st.error(error)
        return

    items = data.get("items", [])
    metadata = data.get("metadata", {})
    diagnostics = data.get("diagnostics", {})

    if start_label and end_label:
        if items:
            st.success(
                f"Identified {len(items)} PAX flight(s) meeting the ZFW review thresholds "
                f"between {start_label} and {end_label}."
            )
        else:
            st.info(
                f"No PAX flights met the ZFW review thresholds between {start_label} and {end_label}."
            )

    if items or start_label:
        summary_cols = st.columns(4)
        summary_cols[0].metric("Flights flagged", diagnostics.get("flagged_flights", len(items)))
        summary_cols[1].metric("PAX flights evaluated", diagnostics.get("pax_flights", 0))
        summary_cols[2].metric(
            "Flights meeting threshold", diagnostics.get("threshold_applicable", 0)
        )
        summary_cols[3].metric("Missing pax count", diagnostics.get("missing_pax_count", 0))

        note_cols = st.columns(3)
        note_cols[0].metric("Notes fetched", diagnostics.get("notes_requested", 0))
        note_cols[1].metric("Existing booking notes", diagnostics.get("notes_found", 0))
        note_cols[2].metric("ZFW confirmations logged", diagnostics.get("zfw_confirmations", 0))

    if diagnostics.get("note_errors"):
        st.warning(
            "Some booking notes could not be retrieved. Review the diagnostics below for details."
        )

    df = pd.DataFrame(items)
    if not df.empty:
        df = df.copy()
        df["Booking note present"] = df["booking_note_present"].map(lambda v: "Yes" if v else "No")
        df["ZFW confirmed"] = df["booking_note_confirms_zfw"].map(lambda v: "Yes" if v else "No")

        columns = [
            "departure_utc",
            "arrival_utc",
            "registration",
            "airport_from",
            "airport_to",
            "flight_reference",
            "aircraft_category",
            "pax_count",
            "pax_threshold",
            "Booking note present",
            "ZFW confirmed",
            "booking_note",
        ]
        available_columns = [col for col in columns if col in df.columns]
        display_df = df[available_columns]

        st.dataframe(display_df, use_container_width=True)

        csv_buffer = StringIO()
        export_columns = [
            "flight_id",
            "quote_id",
            "flight_reference",
            "departure_utc",
            "arrival_utc",
            "registration",
            "airport_from",
            "airport_to",
            "aircraft_category",
            "pax_count",
            "pax_threshold",
            "booking_note_present",
            "booking_note_confirms_zfw",
            "booking_note",
        ]
        export_df = df[[col for col in export_columns if col in df.columns]]
        export_df.to_csv(csv_buffer, index=False)
        st.download_button(
            "Download ZFW review flights as CSV",
            csv_buffer.getvalue(),
            file_name="oca_zfw_check_flights.csv",
            mime="text/csv",
        )

    with st.expander("FL3XX request metadata", expanded=False):
        st.json(metadata)

    with st.expander("Diagnostics", expanded=False):
        st.json(diagnostics)


def _render_results(state: Dict[str, Any]) -> None:
    normalised = _normalise_state(state)

    error = normalised.get("error")
    if error:
        st.error(error)
        return

    start_label = normalised.get("start_date")
    end_label = normalised.get("end_date")

    max_time_data = normalised.get("max_time") or {}
    zfw_data = normalised.get("zfw") or {}

    if not any((max_time_data, zfw_data, start_label, end_label)):
        return

    if max_time_data:
        _render_max_time_results(max_time_data, start_label, end_label)

    if zfw_data:
        _render_zfw_results(zfw_data, start_label, end_label)


configure_page(page_title="OCA Reports")
password_gate()
render_sidebar()

st.title("🛫 OCA Reports")
st.caption("Generate OCA-specific monitoring reports based on FL3XX data.")

state = _load_state()
_render_results(state)

api_settings_raw = get_secret("fl3xx_api", {})
api_settings = _normalise_settings(api_settings_raw)

with st.form("oca_reports_form"):
    start_date = st.date_input(
        "Report start date",
        value=_default_start_date(),
        help="The monitoring window begins on this date in America/Edmonton.",
    )
    day_count = st.number_input(
        "Days to monitor",
        min_value=1,
        max_value=7,
        value=3,
        help="Include this many calendar days in the flight scan.",
    )
    submitted = st.form_submit_button("Run OCA Reports")

if submitted:
    if api_settings is None:
        st.error(
            "FL3XX API credentials are missing. Configure the `fl3xx_api` section in `.streamlit/secrets.toml`."
        )
    else:
        try:
            config = build_fl3xx_api_config(dict(api_settings))
        except FlightDataError as exc:
            st.error(str(exc))
        else:
            to_date_exclusive = start_date + timedelta(days=int(day_count))
            inclusive_end = to_date_exclusive - timedelta(days=1)

            max_time_state: Dict[str, Any] = {}
            zfw_state: Dict[str, Any] = {}

            try:
                with st.spinner("Evaluating max flight time limits..."):
                    alerts, metadata, diagnostics = evaluate_flights_for_max_time(
                        config,
                        from_date=start_date,
                        to_date=to_date_exclusive,
                    )
            except FlightDataError as exc:
                max_time_state["error"] = str(exc)
                st.error(str(exc))
            except Exception as exc:  # pragma: no cover - defensive UI path
                max_time_state["error"] = str(exc)
                st.error(str(exc))
            else:
                max_time_state = {
                    "alerts": [
                        alert.as_dict() if isinstance(alert, MaxFlightTimeAlert) else dict(alert)
                        for alert in alerts
                    ],
                    "metadata": metadata,
                    "diagnostics": diagnostics,
                }

            try:
                with st.spinner("Evaluating ZFW review thresholds..."):
                    zfw_items, zfw_metadata, zfw_diagnostics = evaluate_flights_for_zfw_check(
                        config,
                        from_date=start_date,
                        to_date=to_date_exclusive,
                    )
            except FlightDataError as exc:
                zfw_state["error"] = str(exc)
                st.error(str(exc))
            except Exception as exc:  # pragma: no cover - defensive UI path
                zfw_state["error"] = str(exc)
                st.error(str(exc))
            else:
                zfw_state = {
                    "items": [
                        item.as_dict() if isinstance(item, ZfwFlightCheck) else dict(item)
                        for item in zfw_items
                    ],
                    "metadata": zfw_metadata,
                    "diagnostics": zfw_diagnostics,
                }

            _store_state(
                {
                    "start_date": start_date.isoformat(),
                    "end_date": inclusive_end.isoformat(),
                    "days": int(day_count),
                    "max_time": max_time_state,
                    "zfw": zfw_state,
                }
            )
            st.rerun()
