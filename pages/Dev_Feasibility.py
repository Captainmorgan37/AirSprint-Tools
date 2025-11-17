from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import streamlit as st

from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from feasibility import FeasibilityResult, evaluate_flight, run_feasibility_for_booking
from feasibility.lookup import BookingLookupError
from feasibility.quote_lookup import (
    QuoteLookupError,
    fetch_quote_leg_options,
)
from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Feasibility Engine (Dev)")
password_gate()
render_sidebar()

st.title("🧮 DM Feasibility Engine")

st.write(
    """
    Run a DM-ready feasibility scan for pre-booking quote legs or confirmed bookings. Use the
    **Quote ID** tab when evaluating requests that have not yet become bookings, and the
    **Booking Identifier** tab for accepted trips. The engine evaluates aircraft performance,
    airport readiness, crew duty, trip planning, and overflight permit risks, then outputs a
    standardized summary you can paste into OS notes.
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


def _load_quote_options(quote_id: str) -> list[Dict[str, Any]]:
    if not quote_id:
        st.warning("Enter a Quote ID to continue.")
        return []

    settings = _load_fl3xx_settings()
    try:
        config = build_fl3xx_api_config(dict(settings))
    except FlightDataError as exc:
        st.error(str(exc))
        return []

    with st.spinner("Fetching quote and legs from FL3XX…"):
        try:
            options, payload = fetch_quote_leg_options(config, quote_id)
        except QuoteLookupError as exc:
            st.warning(str(exc))
            return []
        except Exception as exc:  # pragma: no cover - defensive UI guard
            st.exception(exc)
            return []

    st.session_state["feasibility_quote_payload"] = payload
    st.success(f"Loaded {len(options)} leg(s) for quote {quote_id}.")
    return options


def _run_quote_leg(flight: Mapping[str, Any]) -> Optional[FeasibilityResult]:
    with st.spinner("Running feasibility checks for selected quote leg…"):
        try:
            return evaluate_flight(flight)
        except Exception as exc:  # pragma: no cover - Streamlit safety net
            st.exception(exc)
            return None


quote_tab, booking_tab = st.tabs(["Quote ID", "Booking Identifier"])

with quote_tab:
    st.subheader("Search via Quote ID")
    st.caption("Use this to evaluate feasibility before a booking exists.")

    with st.form("quote-form", clear_on_submit=False):
        quote_input = st.text_input("Quote ID", placeholder="e.g. 3621613").strip()
        quote_submitted = st.form_submit_button("Load Quote")

    if quote_submitted:
        options = _load_quote_options(quote_input)
        if options:
            st.session_state["feasibility_quote_options"] = options
            st.session_state["feasibility_quote_leg_index"] = 0

    quote_options = st.session_state.get("feasibility_quote_options", [])
    quote_payload = st.session_state.get("feasibility_quote_payload")

    if quote_options:
        option_indices = list(range(len(quote_options)))
        default_index = min(
            st.session_state.get("feasibility_quote_leg_index", 0),
            len(option_indices) - 1,
        )
        selected_index = st.selectbox(
            "Select quote leg",
            option_indices,
            format_func=lambda idx: quote_options[idx]["label"],
            index=default_index,
            key="feasibility_quote_leg_index",
        )
        selected_option = quote_options[selected_index]
        leg_info = selected_option.get("leg", {})
        pax = leg_info.get("pax") or "n/a"
        block = leg_info.get("blockTime") or leg_info.get("flightTime") or "n/a"
        st.caption(f"PAX: {pax} · Block: {block} minutes")

        if st.button("Run Feasibility for Selected Quote Leg", key="run-quote"):
            result = _run_quote_leg(selected_option["flight"])
            if result:
                st.session_state["feasibility_last_result"] = result
    else:
        st.info("Load a quote to view available legs for feasibility analysis.")

    if quote_payload:
        with st.expander("Loaded quote payload"):
            st.json(quote_payload)

with booking_tab:
    st.subheader("Search via Booking Identifier")
    with st.form("booking-form", clear_on_submit=False):
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
    st.info("Select a quote leg or submit a booking identifier to generate a feasibility report.")
