from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from fl3xx_api import compute_fetch_dates
from Home import configure_page, password_gate, render_sidebar
from morning_reports import MorningReportResult, MorningReportRun, run_morning_reports

configure_page(page_title="Operations Lead Morning Reports")
password_gate()
render_sidebar()


_EXPECTED_REPORTS: Sequence[Tuple[str, str, str]] = (
    ("16.1.1", "App Booking Workflow Report", "App Booking Workflow"),
    ("16.1.2", "App Line Assignment Report", "App Line Assignment"),
    ("16.1.3", "Empty Leg Report", "Empty Legs"),
    ("16.1.4", "OCS Pax Flights Report", "OCS Pax Flights"),
    (
        "16.1.5",
        "Owner Continuous Flight Validation Report",
        "Owner Continuous Flight Validation",
    ),
    ("16.1.6", "CJ3 Owners on CJ2 Report", "CJ3 Owners on CJ2"),
    ("16.1.7", "Priority Status Report", "Priority Duty-Start Validation"),
    (
        "16.1.9",
        "Upgrade Workflow Validation Report",
        "Legacy Upgrade Workflow Validation",
    ),
    (
        "16.1.10",
        "Upgraded Flights Report",
        "Upgrade Workflow Flights",
    ),
)


_PREFERRED_FORMAT_CODES = {"16.1.6", "16.1.7", "16.1.10"}


def _build_expected_reports(
    reports: Iterable[MorningReportResult],
) -> Tuple[List[MorningReportResult], List[str]]:
    report_map = {report.code: report for report in reports}
    ordered_reports: List[MorningReportResult] = []
    missing_codes: List[str] = []

    for code, title, header_label in _EXPECTED_REPORTS:
        existing = report_map.pop(code, None)
        if existing is not None:
            ordered_reports.append(existing)
            continue

        missing_codes.append(code)
        ordered_reports.append(
            MorningReportResult(
                code=code,
                title=title,
                header_label=header_label,
                rows=[],
                metadata={"match_count": 0, "placeholder": True},
            )
        )

    if report_map:
        ordered_reports.extend(sorted(report_map.values(), key=lambda r: r.code))

    return ordered_reports, missing_codes


st.title("📋 Operations Lead Morning Reports")


def _highlight_cj3_thresholds(row: pd.Series) -> List[str]:
    highlight = bool(row.get("threshold_breached"))
    color = "color: red" if highlight else ""
    return [color] * len(row)


def _build_cj3_row_display(rows: Iterable[Mapping[str, object]]):
    df = pd.DataFrame(rows)
    if df.empty or "threshold_breached" not in df.columns:
        return df
    return df.style.apply(_highlight_cj3_thresholds, axis=1)


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def _initialise_state():
    st.session_state.setdefault("ol_reports_run", None)
    st.session_state.setdefault("ol_reports_error", None)
    st.session_state.setdefault("ol_reports_warning", None)
    if "ol_reports_from_date" not in st.session_state or "ol_reports_to_date" not in st.session_state:
        default_from, default_to_exclusive = compute_fetch_dates(datetime.now(timezone.utc), inclusive_days=4)
        st.session_state.setdefault("ol_reports_from_date", default_from)
        st.session_state.setdefault(
            "ol_reports_to_date", default_to_exclusive - timedelta(days=1)
        )


def _get_selected_dates() -> Tuple[date, date]:
    from_date = st.session_state.get("ol_reports_from_date")
    to_date = st.session_state.get("ol_reports_to_date")
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
    st.session_state["ol_reports_warning"] = None

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
                st.session_state["ol_reports_run"] = None
                st.session_state["ol_reports_error"] = str(fallback_exc)
                return

            st.session_state["ol_reports_warning"] = (
                "The installed morning report runner does not yet support custom date ranges. "
                "Fetched the default window instead."
            )
            st.session_state["ol_reports_run"] = run
            st.session_state["ol_reports_error"] = None
            return

        st.session_state["ol_reports_run"] = None
        st.session_state["ol_reports_error"] = message
        return
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state["ol_reports_run"] = None
        st.session_state["ol_reports_error"] = str(exc)
        return

    st.session_state["ol_reports_run"] = run
    st.session_state["ol_reports_error"] = None


def _render_report_output(report: MorningReportResult):
    if report.code in _PREFERRED_FORMAT_CODES:
        st.caption("Copy-and-paste friendly summary block:")
    st.code(report.formatted_output(), language="text")
    confirmation_note = report.metadata.get("runway_confirmation_note")
    if confirmation_note:
        st.info(confirmation_note)
    if report.code in _PREFERRED_FORMAT_CODES and report.has_matches:
        st.caption("Scroll down for detailed rows if you need additional context.")
    if report.warnings:
        for warning in report.warnings:
            st.warning(warning)

    if report.metadata:
        with st.expander("Report metadata", expanded=False):
            st.json(report.metadata)

    if report.rows:
        st.markdown("#### Matching legs")
        if report.code == "16.1.6":
            st.dataframe(
                _build_cj3_row_display(report.rows),
                use_container_width=True,
            )
        else:
            st.dataframe(report.rows, use_container_width=True)
    else:
        st.info("No matching legs found for this report.")


def _render_results():
    error_message = st.session_state.get("ol_reports_error")
    run: Optional[MorningReportRun] = st.session_state.get("ol_reports_run")
    warning_message = st.session_state.get("ol_reports_warning")

    if error_message:
        st.error(error_message)

    if not run:
        if not error_message:
            st.info("Press **Fetch Morning Reports** to run the reports against live data.")
        return

    selected_range = f"{run.from_date.isoformat()} → {(run.to_date - timedelta(days=1)).isoformat()}"

    st.success(
        "Morning reports fetched"
        + (
            f" · {_format_timestamp(run.fetched_at)}"
            if isinstance(run.fetched_at, datetime)
            else ""
        )
        + f" · {run.leg_count} legs analysed"
        + f" · Dates: {selected_range}"
    )

    display_reports, missing_reports = _build_expected_reports(run.reports)

    st.caption(
        "Reports included: "
        + ", ".join(f"{report.code} – {report.title}" for report in display_reports)
    )

    if warning_message:
        st.warning(warning_message)

    if missing_reports:
        formatted_missing = ", ".join(sorted(missing_reports))
        st.warning(
            "Some expected reports were not returned by the installed morning report "
            "runner. Displaying placeholder tabs instead. Missing report codes: "
            f"{formatted_missing}."
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

    report_tabs = st.tabs([report.title for report in display_reports])
    for tab, report in zip(report_tabs, display_reports):
        with tab:
            st.markdown(f"### {report.title}")
            _render_report_output(report)


def main():
    _initialise_state()

    st.markdown(
        """
        Press **Fetch Morning Reports** to run the App Booking, App Line Assignment,
        Empty Leg, OCS Pax Flights, Owner Continuous Flight Validation, CJ3 Owners on
        CJ2, Priority Status, Upgrade Workflow Validation, and Upgraded Flights checks
        using the latest FL3XX flight data. Review any matching legs
        and warnings directly in the report tabs below.
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

    st.session_state["ol_reports_from_date"] = selected_from
    st.session_state["ol_reports_to_date"] = selected_to

    api_settings = _get_api_settings()
    if api_settings is None:
        st.warning(
            "FL3XX API credentials are not configured. Add them to "
            "`.streamlit/secrets.toml` under the `[fl3xx_api]` section to enable live fetches."
        )

    if st.button(
        "Fetch Morning Reports",
        help=(
            "Fetch FL3XX legs and execute the App Booking, App Line Assignment, Empty Leg, "
            "OCS Pax Flights, Owner Continuous Flight Validation, CJ3 Owners on CJ2, Priority Status, "
            "Upgrade Workflow Validation, and Upgraded Flights reports."
        ),
        use_container_width=False,
    ):
        if selected_to < selected_from:
            st.session_state["ol_reports_run"] = None
            st.session_state["ol_reports_error"] = (
                "The report end date must be on or after the start date."
            )
        elif api_settings is None:
            st.session_state["ol_reports_run"] = None
            st.session_state["ol_reports_error"] = (
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
