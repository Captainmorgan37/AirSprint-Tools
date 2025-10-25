from datetime import date, datetime, timedelta, timezone
from typing import Mapping, Optional

import streamlit as st

from fl3xx_api import compute_fetch_dates
from Home import configure_page, password_gate, render_sidebar
from morning_reports import MorningReportResult, MorningReportRun, run_morning_reports

configure_page(page_title="FBO Disconnect Report")
password_gate()
render_sidebar()


st.title("🛫 FBO Disconnect Report")


_DEF_FROM_KEY = "fbo_report_from_date"
_DEF_TO_KEY = "fbo_report_to_date"
_RUN_KEY = "fbo_report_run"
_ERROR_KEY = "fbo_report_error"
_WARNING_KEY = "fbo_report_warning"


_DEF_REPORT_CODE = "16.1.11"
_DEF_REPORT_TITLE = "FBO Disconnect Report"
_DEF_REPORT_HEADER = "FBO Disconnect Checks"


def _initialise_state() -> None:
    st.session_state.setdefault(_RUN_KEY, None)
    st.session_state.setdefault(_ERROR_KEY, None)
    st.session_state.setdefault(_WARNING_KEY, None)
    if _DEF_FROM_KEY not in st.session_state or _DEF_TO_KEY not in st.session_state:
        default_from, default_to_exclusive = compute_fetch_dates(
            datetime.now(timezone.utc), inclusive_days=4
        )
        st.session_state.setdefault(_DEF_FROM_KEY, default_from)
        st.session_state.setdefault(
            _DEF_TO_KEY, default_to_exclusive - timedelta(days=1)
        )


def _get_selected_dates() -> tuple[date, date]:
    from_date = st.session_state.get(_DEF_FROM_KEY)
    to_date = st.session_state.get(_DEF_TO_KEY)
    if not isinstance(from_date, date) or not isinstance(to_date, date):
        today = datetime.now(timezone.utc).date()
        return today, today
    return from_date, to_date


def _get_api_settings() -> Optional[Mapping[str, str]]:
    try:
        settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        return None
    if not settings:
        return None
    if isinstance(settings, Mapping):
        return dict(settings)
    return None


def _handle_fetch(
    api_settings: Mapping[str, str],
    *,
    from_date: date,
    to_date_inclusive: date,
) -> None:
    to_date_exclusive = to_date_inclusive + timedelta(days=1)
    st.session_state[_WARNING_KEY] = None

    try:
        with st.spinner("Fetching flights from FL3XX..."):
            run = run_morning_reports(
                api_settings,
                from_date=from_date,
                to_date=to_date_exclusive,
            )
    except TypeError as exc:
        message = str(exc)
        if "unexpected keyword argument" in message and (
            "from_date" in message or "to_date" in message
        ):
            try:
                with st.spinner("Fetching flights from FL3XX..."):
                    run = run_morning_reports(api_settings)
            except Exception as fallback_exc:  # pragma: no cover - defensive UI path
                st.session_state[_RUN_KEY] = None
                st.session_state[_ERROR_KEY] = str(fallback_exc)
                return

            st.session_state[_WARNING_KEY] = (
                "The installed morning report runner does not yet support custom date ranges. "
                "Fetched the default window instead."
            )
            st.session_state[_RUN_KEY] = run
            st.session_state[_ERROR_KEY] = None
            return

        st.session_state[_RUN_KEY] = None
        st.session_state[_ERROR_KEY] = message
        return
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state[_RUN_KEY] = None
        st.session_state[_ERROR_KEY] = str(exc)
        return

    st.session_state[_RUN_KEY] = run
    st.session_state[_ERROR_KEY] = None


def _render_report_output(report: MorningReportResult) -> None:
    st.code(report.formatted_output(), language="text")

    if report.warnings:
        for warning in report.warnings:
            st.warning(warning)

    if report.rows:
        st.markdown("#### Matching legs")
        st.dataframe(report.rows, use_container_width=True)
    else:
        st.info("No matching legs found for this report.")


def _render_results() -> None:
    error_message = st.session_state.get(_ERROR_KEY)
    run: Optional[MorningReportRun] = st.session_state.get(_RUN_KEY)
    warning_message = st.session_state.get(_WARNING_KEY)

    if error_message:
        st.error(error_message)

    if not run:
        if not error_message:
            st.info("Press **Fetch FBO Disconnect Report** to run the report against live data.")
        return

    selected_range = f"{run.from_date.isoformat()} → {(run.to_date - timedelta(days=1)).isoformat()}"

    st.success(
        "Report fetched"
        + (
            f" · {run.fetched_at.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%MZ')}"
            if isinstance(run.fetched_at, datetime)
            else ""
        )
        + f" · {run.leg_count} legs analysed"
        + f" · Dates: {selected_range}"
    )

    if warning_message:
        st.warning(warning_message)

    report = run.report_map().get(_DEF_REPORT_CODE)
    if report is None:
        placeholder = MorningReportResult(
            code=_DEF_REPORT_CODE,
            title=_DEF_REPORT_TITLE,
            header_label=_DEF_REPORT_HEADER,
            rows=[],
            metadata={"match_count": 0, "placeholder": True},
        )
        st.warning(
            "The installed morning report runner did not return the FBO Disconnect Report. "
            "Showing a placeholder block instead."
        )
        report = placeholder

    st.markdown(f"### {report.title}")
    _render_report_output(report)

    metadata_payload = {
        "from_date": run.metadata.get("from_date"),
        "to_date": run.metadata.get("to_date"),
        "request_url": run.metadata.get("request_url"),
        "request_params": run.metadata.get("request_params"),
        "hash": run.metadata.get("hash"),
        "skipped_subcharter": run.metadata.get("skipped_subcharter"),
        "normalization_stats": run.normalization_stats,
    }

    with st.expander("Fetch metadata", expanded=False):
        st.json(metadata_payload)


def main() -> None:
    _initialise_state()

    st.markdown(
        """
        Press **Fetch FBO Disconnect Report** to run the disconnect checks using the latest
        FL3XX flight data. Review any matching legs and warnings directly below.
        """
    )

    selected_from, selected_to = _get_selected_dates()
    date_input = st.date_input(
        "Report date range",
        value=(selected_from, selected_to),
        help="Choose the inclusive date range to analyse.",
    )

    if isinstance(date_input, tuple) and len(date_input) == 2:
        selected_from, selected_to = date_input
    elif isinstance(date_input, date):
        selected_from = date_input
        selected_to = date_input

    st.session_state[_DEF_FROM_KEY] = selected_from
    st.session_state[_DEF_TO_KEY] = selected_to

    api_settings = _get_api_settings()
    if api_settings is None:
        st.warning(
            "FL3XX API credentials are not configured. Add them to "
            "`.streamlit/secrets.toml` under the `[fl3xx_api]` section to enable live fetches."
        )

    if st.button(
        "Fetch FBO Disconnect Report",
        help=(
            "Fetch FL3XX legs and execute the FBO Disconnect report for the selected date range."
        ),
        use_container_width=False,
    ):
        if selected_to < selected_from:
            st.session_state[_RUN_KEY] = None
            st.session_state[_ERROR_KEY] = (
                "The report end date must be on or after the start date."
            )
        elif api_settings is None:
            st.session_state[_RUN_KEY] = None
            st.session_state[_ERROR_KEY] = (
                "FL3XX API secrets are not configured; provide credentials before fetching."
            )
        else:
            _handle_fetch(
                api_settings,
                from_date=selected_from,
                to_date_inclusive=selected_to,
            )

    _render_results()


if __name__ == "__main__":
    main()
