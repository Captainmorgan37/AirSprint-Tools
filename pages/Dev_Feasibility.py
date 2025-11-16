from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import streamlit as st

from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from feasibility import FeasibilityResult, run_feasibility_for_booking
from feasibility.lookup import BookingLookupError
from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Feasibility Engine (Dev)")
password_gate()
render_sidebar()

st.title("🧮 DM Feasibility Engine")

st.write(
    """
    Run a DM-ready feasibility scan for any FL3XX booking identifier. The engine fetches the
    target flight, evaluates aircraft performance, airport readiness, crew duty, trip planning,
    and overflight permit risks, then outputs a standardized summary you can paste into OS notes.
    """
)


@st.cache_data(show_spinner=False)
def _load_fl3xx_settings() -> Dict[str, Any]:
    try:
        secrets_section = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        secrets_section = None
    if isinstance(secrets_section, Mapping):
        return {str(key): secrets_section[key] for key in secrets_section}
    if isinstance(secrets_section, dict):
        return dict(secrets_section)
    return {}


def _run_feasibility(booking_identifier: str) -> Optional[FeasibilityResult]:
    if not booking_identifier:
        st.warning("Enter a booking identifier to continue.")
        return None

    settings = _load_fl3xx_settings()
    try:
        config = build_fl3xx_api_config(dict(settings))
    except FlightDataError as exc:
        st.error(str(exc))
        return None

    cache = st.session_state.setdefault("feasibility_lookup_cache", {})
    with st.spinner("Fetching flight and running feasibility checks…"):
        try:
            result = run_feasibility_for_booking(config, booking_identifier, cache=cache)
        except BookingLookupError as exc:
            st.warning(str(exc))
            return None
        except Exception as exc:  # pragma: no cover - safety net for Streamlit UI
            st.exception(exc)
            return None
    return result


with st.form("feasibility-form", clear_on_submit=False):
    booking_input = st.text_input("Booking Identifier", placeholder="e.g. ILARD").strip().upper()
    submitted = st.form_submit_button("Run Feasibility")

if submitted:
    result = _run_feasibility(booking_input)
    if result:
        st.session_state["feasibility_last_result"] = result

stored_result = st.session_state.get("feasibility_last_result")

STATUS_EMOJI = {"PASS": "✅", "CAUTION": "⚠️", "FAIL": "❌"}


def _render_category(name: str, category) -> None:
    emoji = STATUS_EMOJI.get(category.status, "")
    header = f"{emoji} {name.title()} – {category.summary or category.status}"
    with st.expander(header, expanded=category.status != "PASS"):
        st.write(f"Status: **{category.status}**")
        if category.issues:
            st.markdown("\n".join(f"- {issue}" for issue in category.issues))
        else:
            st.write("No issues recorded.")


if stored_result and isinstance(stored_result, FeasibilityResult):
    overall_emoji = STATUS_EMOJI.get(stored_result.overall_status, "")
    st.subheader(f"{overall_emoji} Overall Status: {stored_result.overall_status}")
    st.caption(f"Generated at {stored_result.timestamp}")

    for name, category in stored_result.categories.items():
        _render_category(name, category)

    st.markdown("### Notes for OS")
    st.code(stored_result.notes_for_os or "No notes", language="markdown")

    with st.expander("Raw result JSON"):
        st.json(stored_result.as_dict(include_flight=False))

    if stored_result.flight:
        with st.expander("Source flight payload"):
            st.json(stored_result.flight)
else:
    st.info("Submit a booking identifier to generate a feasibility report.")
