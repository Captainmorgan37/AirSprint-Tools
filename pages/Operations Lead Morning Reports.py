import streamlit as st
from pathlib import Path
from datetime import datetime, timezone

from morning_report_plan import list_reports


PLAN_PATH = Path("docs/FL3XX-report-automation-plan.md")
REPORT_CODES = ["16.1.1", "16.1.2", "16.1.3"]


st.set_page_config(page_title="Operations Lead Morning Reports", layout="wide")
st.title("📋 Operations Lead Morning Reports")


@st.cache_data(show_spinner=False)
def _load_plan_sections():
    return list_reports(PLAN_PATH, include=REPORT_CODES)


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def _initialise_state():
    if "ol_reports" not in st.session_state:
        st.session_state["ol_reports"] = None


def _render_report_plan(report):
    st.subheader(report.short_name())

    with st.expander("Current Capability", expanded=False):
        st.markdown(report.current_capability)

    with st.expander("Gaps / Required Inputs", expanded=False):
        st.markdown(report.gaps or "—")

    with st.expander("Next Steps", expanded=True):
        st.markdown(report.next_steps)
        if report.sample_outputs:
            st.caption("Sample outputs from the plan:")
            for idx, block in enumerate(report.sample_outputs):
                st.code(block, language="text")


def _render_results():
    result = st.session_state.get("ol_reports")
    if not result:
        st.info("Press **Fetch Morning Reports** to load the latest plan context.")
        return

    fetched_at = result.get("fetched_at")
    st.success(
        "Morning report scaffolding initialised"
        + (f" · fetched {_format_timestamp(fetched_at)}" if fetched_at else "")
    )

    tabs = st.tabs([report.short_name() for report in result["reports"]])
    for tab, report in zip(tabs, result["reports"]):
        with tab:
            _render_report_plan(report)


def _handle_fetch(plan_reports):
    st.session_state["ol_reports"] = {
        "fetched_at": datetime.now(timezone.utc),
        "reports": plan_reports,
    }


def main():
    _initialise_state()

    st.markdown(
        """
        This page scaffolds the first three FL3XX morning reports described in the
        internal automation plan. It reads the latest instructions from the shared
        markdown file so that engineering work can begin without duplicating
        requirements.
        """
    )

    if not PLAN_PATH.exists():
        st.error(
            "The plan document could not be found. Confirm that "
            f"`{PLAN_PATH}` is present in the repository."
        )
        return

    plan_reports = _load_plan_sections()

    st.button(
        "Fetch Morning Reports",
        help="Load the latest instructions for the App Booking, App Line Assignment, and Empty Leg reports.",
        on_click=_handle_fetch,
        kwargs={"plan_reports": plan_reports},
    )

    _render_results()


if __name__ == "__main__":
    main()
