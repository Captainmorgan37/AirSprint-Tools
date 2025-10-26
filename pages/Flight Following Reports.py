from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable, Tuple

import streamlit as st

from Home import configure_page, get_secret, password_gate, render_sidebar
from flight_following_reports import (
    build_flight_following_report,
    collect_duty_start_snapshots,
    summarize_long_duty_days,
    summarize_split_duty_days,
    summarize_tight_turnarounds,
)
from flight_leg_utils import FlightDataError, build_fl3xx_api_config

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None  # type: ignore[assignment]


_PAGE_STATE_KEY = "_flight_following_report_state"
_MOUNTAIN_TZ_NAME = "America/Edmonton"


def _default_target_date() -> date:
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(_MOUNTAIN_TZ_NAME)
            return datetime.now(tz).date()
        except Exception:
            pass
    return date.today()


def _store_state(state: Dict[str, Any]) -> None:
    st.session_state[_PAGE_STATE_KEY] = state


def _load_state() -> Dict[str, Any] | None:
    state = st.session_state.get(_PAGE_STATE_KEY)
    if isinstance(state, dict):
        return state
    return None


def _display_sections(sections: Iterable[Tuple[str, Iterable[str]]]) -> None:
    for title, lines in sections:
        st.subheader(title)
        normalized = [line for line in lines if isinstance(line, str) and line.strip()]
        if not normalized:
            st.caption("None")
            continue
        for line in normalized:
            st.markdown(f"• {line}")


def _render_metadata(collection_summary: Dict[str, Any]) -> None:
    with st.expander("Flight data diagnostics"):
        st.json(collection_summary)


def _summarize_collection(collection) -> Dict[str, Any]:
    tails = sorted(collection.grouped_flights.keys()) if collection.grouped_flights else []
    metadata = {str(k): v for k, v in (collection.flights_metadata or {}).items()}
    return {
        "target_date": collection.target_date.isoformat(),
        "window_start_utc": collection.start_utc.isoformat(),
        "window_end_utc": collection.end_utc.isoformat(),
        "duty_start_snapshots": len(collection.snapshots),
        "tails_processed": len(tails),
        "tails": tails,
        "flights_metadata": metadata,
    }


def _display_report(state: Dict[str, Any]) -> None:
    report_text = state.get("report_text", "")
    st.success(f"Report generated for {state.get('target_date', 'the selected date')}")

    summary = state.get("collection_summary", {})
    col1, col2 = st.columns(2)
    col1.metric("Duty starts captured", summary.get("duty_start_snapshots", 0))
    col2.metric("Tails processed", summary.get("tails_processed", 0))

    st.text_area("Report text", report_text, height=260)
    st.download_button(
        "Download report text",
        data=report_text,
        file_name=f"flight_following_report_{state.get('target_date', 'day')}.txt",
        mime="text/plain",
    )

    _display_sections(state.get("sections", []))
    _render_metadata(summary)


configure_page(page_title="Flight Following Reports")
password_gate()
render_sidebar()

st.title("Flight Following Reports")
st.caption(
    "Generate daily duty watchlists for Dispatch using FL3XX flight and postflight data."
)

fl3xx_settings_raw = get_secret("fl3xx_api", {})
fl3xx_settings = dict(fl3xx_settings_raw) if isinstance(fl3xx_settings_raw, dict) else {}

with st.form("flight_following_reports_form"):
    target_date = st.date_input(
        "Duty date",
        value=_default_target_date(),
        help="Flights are fetched for midnight–23:59 local time in America/Edmonton.",
    )
    submitted = st.form_submit_button("Generate Flight Following Report")

if submitted:
    if not fl3xx_settings:
        st.error(
            "FL3XX API credentials are missing. Configure the `fl3xx_api` section in `.streamlit/secrets.toml`."
        )
    else:
        try:
            config = build_fl3xx_api_config(fl3xx_settings)
        except FlightDataError as exc:
            st.error(str(exc))
        else:
            with st.spinner("Collecting flights and duty snapshots…"):
                collection = collect_duty_start_snapshots(config, target_date)
                report = build_flight_following_report(
                    collection,
                    section_builders=(
                        ("Long Duty Days", summarize_long_duty_days),
                        ("Split Duty Days", summarize_split_duty_days),
                        ("Tight Turnarounds (<11h Before Next Duty)", summarize_tight_turnarounds),
                    ),
                )

            state = {
                "target_date": target_date.isoformat(),
                "generated_at": report.generated_at.isoformat(),
                "report_text": report.text_payload(),
                "sections": [(section.title, section.normalized_lines()) for section in report.sections],
                "collection_summary": _summarize_collection(collection),
            }
            _store_state(state)
            _display_report(state)
else:
    stored_state = _load_state()
    if stored_state:
        _display_report(stored_state)
    elif not fl3xx_settings:
        st.warning(
            "Add your FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to enable live fetching."
        )
    else:
        st.info("Select a date and click the button to generate the report.")
