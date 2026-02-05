from __future__ import annotations

from datetime import date, datetime, time, timedelta
from functools import partial
from typing import Any, Dict, Iterable, Mapping, Tuple

import streamlit as st

from Home import configure_page, get_secret, password_gate, render_sidebar
from flight_following_reports import (
    build_flight_following_report,
    build_rest_before_index,
    collect_duty_start_snapshots,
    compute_short_turn_summary_for_collection,
    summarize_collection_for_display,
    summarize_cyyz_night_operations,
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
_SHORT_TURN_THRESHOLD_MIN = 45


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
    for index, (title, lines) in enumerate(sections):
        st.subheader(title)
        normalized = [line for line in lines if isinstance(line, str) and line.strip()]
        if not normalized:
            st.caption("None")
            continue
        if len(normalized) == 1 and "\n" in normalized[0]:
            text = normalized[0]
            line_count = text.count("\n") + 1
            st.text_area(
                f"{title} details",
                text,
                height=min(600, max(160, 24 * line_count)),
                key=f"section_text_{index}",
            )
            continue
        for line in normalized:
            st.markdown(f"• {line}")


def _render_metadata(collection_summary: Dict[str, Any]) -> None:
    with st.expander("Flight data diagnostics"):
        st.json(collection_summary)


def _summarize_collection(collection) -> Dict[str, Any]:
    return summarize_collection_for_display(collection)


def _display_report(state: Dict[str, Any]) -> None:
    report_text = state.get("report_text", "")
    st.success(f"Report generated for {state.get('target_date', 'the selected date')}")

    summary = state.get("collection_summary", {})
    col1, col2, col3 = st.columns(3)
    col1.metric("Duty starts captured", summary.get("duty_start_snapshots", 0))
    col2.metric("Tails processed", summary.get("tails_processed", 0))
    short_turns_info = summary.get("short_turns", {}) if isinstance(summary, Mapping) else {}
    col3.metric(
        "Short turns (<45 min)",
        short_turns_info.get("turns_detected", state.get("short_turn_count", 0)),
    )

    st.text_area("Report text", report_text, height=260)
    st.download_button(
        "Download report text",
        data=report_text,
        file_name=f"flight_following_report_{state.get('target_date', 'day')}.txt",
        mime="text/plain",
    )

    with st.expander("Duty insights", expanded=False):
        _display_sections(state.get("sections", []))
    _render_metadata(summary)


def _display_single_report(state: Dict[str, Any]) -> None:
    sections = state.get("sections", [])
    if not sections:
        st.info("Generate a report to view individual sections.")
        return

    labeled_sections = []
    for title, lines in sections:
        display_title = title or "CYYZ Night Operations"
        labeled_sections.append((display_title, lines))

    titles = [title for title, _lines in labeled_sections]
    default_index = 0
    for index, title in enumerate(titles):
        if "Tight Turnarounds" in title:
            default_index = index
            break

    selected_title = st.selectbox(
        "Select a report section",
        titles,
        index=default_index,
    )
    selected_lines = next(
        lines for title, lines in labeled_sections if title == selected_title
    )
    _display_sections([(selected_title, selected_lines)])


configure_page(page_title="Flight Following Reports")
password_gate()
render_sidebar()

st.title("Flight Following Reports")
st.caption(
    "Generate daily duty watchlists for Dispatch using FL3XX flight and postflight data."
)

fl3xx_settings_raw = get_secret("fl3xx_api", {})
if isinstance(fl3xx_settings_raw, Mapping):
    fl3xx_settings = dict(fl3xx_settings_raw)
else:
    fl3xx_settings = {}

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
            with st.spinner("Collecting flights, postflights, and rest data…"):
                collection = collect_duty_start_snapshots(
                    config,
                    target_date,
                    min_departure_time_local=time(3, 0),
                )
                try:
                    next_day_collection = collect_duty_start_snapshots(
                        config, target_date + timedelta(days=1)
                    )
                except Exception:  # pragma: no cover - network/runtime issues
                    next_day_collection = None
                try:
                    following_day_collection = collect_duty_start_snapshots(
                        config, target_date + timedelta(days=2)
                    )
                except Exception:  # pragma: no cover - network/runtime issues
                    following_day_collection = None

            if next_day_collection is None:
                next_day_rest_index = None
                st.warning(
                    "Next-day rest data could not be retrieved. Positioning notes will be omitted.",
                )
            else:
                next_day_rest_index = build_rest_before_index(next_day_collection)
            if next_day_collection is not None and following_day_collection is None:
                following_day_rest_index = None
                st.warning(
                    "Following-day rest data could not be retrieved. Tomorrow positioning notes will be omitted.",
                )
            elif following_day_collection is not None:
                following_day_rest_index = build_rest_before_index(following_day_collection)
            else:
                following_day_rest_index = None
            tight_turns_builder = partial(
                summarize_tight_turnarounds,
                next_day_rest_index=next_day_rest_index,
            )

            def tomorrow_tight_turns(_collection_input):
                if next_day_collection is None:
                    return []
                return summarize_tight_turnarounds(
                    next_day_collection,
                    next_day_rest_index=following_day_rest_index,
                )
            report_metadata: Dict[str, Any] = {}
            short_turn_capture: Dict[str, Any] = {
                "text": "",
                "count": 0,
                "metadata": {},
            }

            def short_turn_section(collection_input):
                summary_text, count, metadata = compute_short_turn_summary_for_collection(
                    collection_input,
                    threshold_min=_SHORT_TURN_THRESHOLD_MIN,
                    priority_threshold_min=_SHORT_TURN_THRESHOLD_MIN,
                    local_tz_name=_MOUNTAIN_TZ_NAME,
                )
                short_turn_capture["text"] = summary_text
                short_turn_capture["count"] = count
                short_turn_capture["metadata"] = metadata
                report_metadata["short_turns"] = metadata
                return [summary_text]

            report = build_flight_following_report(
                collection,
                section_builders=(
                    ("Long Duty Days", summarize_long_duty_days),
                    ("Split Duty Days", summarize_split_duty_days),
                    (
                        "Tonights Tight Turnarounds (<12h Before Next Duty)",
                        tight_turns_builder,
                    ),
                    (
                        "Tomorrows Tight Turnarounds (<12h Before Next Duty)",
                        tomorrow_tight_turns,
                    ),
                    ("Short Turns (<45 min)", short_turn_section),
                    ("", summarize_cyyz_night_operations),
                ),
                metadata=report_metadata,
            )

            collection_summary = _summarize_collection(collection)
            short_turn_metadata = dict(short_turn_capture.get("metadata") or {})
            if "turns_detected" not in short_turn_metadata:
                short_turn_metadata["turns_detected"] = short_turn_capture.get("count", 0)
            short_turn_metadata["summary_text"] = short_turn_capture.get("text", "")
            collection_summary["short_turns"] = short_turn_metadata

            state = {
                "target_date": target_date.isoformat(),
                "generated_at": report.generated_at.isoformat(),
                "report_text": report.text_payload(),
                "sections": [(section.title, section.normalized_lines()) for section in report.sections],
                "collection_summary": collection_summary,
                "short_turn_summary_text": short_turn_capture.get("text", ""),
                "short_turn_count": short_turn_capture.get("count", 0),
            }
            _store_state(state)
            combined_tab, single_tab = st.tabs(["Combined view", "Single report"])
            with combined_tab:
                _display_report(state)
            with single_tab:
                _display_single_report(state)
else:
    stored_state = _load_state()
    if stored_state:
        combined_tab, single_tab = st.tabs(["Combined view", "Single report"])
        with combined_tab:
            _display_report(stored_state)
        with single_tab:
            _display_single_report(stored_state)
    elif not fl3xx_settings:
        st.warning(
            "Add your FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to enable live fetching."
        )
    else:
        st.info("Select a date and click the button to generate the report.")
