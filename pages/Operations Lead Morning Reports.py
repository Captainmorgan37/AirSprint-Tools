import streamlit as st
from datetime import datetime, timezone
from typing import Mapping, Optional
from morning_reports import MorningReportResult, MorningReportRun, run_morning_reports


st.set_page_config(page_title="Operations Lead Morning Reports", layout="wide")
st.title("📋 Operations Lead Morning Reports")


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def _initialise_state():
    st.session_state.setdefault("ol_reports_run", None)
    st.session_state.setdefault("ol_reports_error", None)


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


def _handle_fetch(api_settings: Mapping[str, str]) -> None:
    try:
        with st.spinner("Fetching flights from FL3XX..."):
            run = run_morning_reports(api_settings)
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state["ol_reports_run"] = None
        st.session_state["ol_reports_error"] = str(exc)
    else:
        st.session_state["ol_reports_run"] = run
        st.session_state["ol_reports_error"] = None


def _render_report_output(report: MorningReportResult):
    st.code(report.formatted_output(), language="text")
    if report.warnings:
        for warning in report.warnings:
            st.warning(warning)

    if report.rows:
        st.markdown("#### Matching legs")
        st.dataframe(report.rows, use_container_width=True)
    else:
        st.info("No matching legs found for this report.")


def _render_results():
    error_message = st.session_state.get("ol_reports_error")
    run: Optional[MorningReportRun] = st.session_state.get("ol_reports_run")

    if error_message:
        st.error(error_message)

    if not run:
        if not error_message:
            st.info("Press **Fetch Morning Reports** to run the reports against live data.")
        return

    st.success(
        "Morning reports fetched"
        + (
            f" · {_format_timestamp(run.fetched_at)}"
            if isinstance(run.fetched_at, datetime)
            else ""
        )
        + f" · {run.leg_count} legs analysed"
    )

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

    report_tabs = st.tabs([report.title for report in run.reports])
    for tab, report in zip(report_tabs, run.reports):
        with tab:
            st.markdown(f"### {report.title}")
            _render_report_output(report)


def main():
    _initialise_state()

    st.markdown(
        """
        Press **Fetch Morning Reports** to run the App Booking, App Line Assignment,
        and Empty Leg checks using the latest FL3XX flight data. Review any
        matching legs and warnings directly in the report tabs below.
        """
    )

    api_settings = _get_api_settings()
    if api_settings is None:
        st.warning(
            "FL3XX API credentials are not configured. Add them to "
            "`.streamlit/secrets.toml` under the `[fl3xx_api]` section to enable live fetches."
        )

    if st.button(
        "Fetch Morning Reports",
        help="Fetch FL3XX legs and execute the App Booking, App Line Assignment, and Empty Leg reports.",
        use_container_width=False,
    ):
        if api_settings is None:
            st.session_state["ol_reports_run"] = None
            st.session_state["ol_reports_error"] = (
                "FL3XX API secrets are not configured; provide credentials before fetching."
            )
        else:
            _handle_fetch(api_settings)

    _render_results()


if __name__ == "__main__":
    main()
