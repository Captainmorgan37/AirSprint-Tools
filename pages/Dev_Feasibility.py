from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, cast

import streamlit as st

from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from feasibility import (
    FeasibilityResult,
    run_feasibility_for_booking,
    run_feasibility_phase1,
)
from feasibility.operational_notes import build_operational_notes_fetcher
from feasibility.lookup import BookingLookupError
from feasibility.quote_lookup import (
    QuoteLookupError,
    fetch_quote_leg_options,
)
from Home import configure_page, password_gate, render_sidebar
from feasibility.models import FullFeasibilityResult

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

STATUS_EMOJI = {"PASS": "✅", "CAUTION": "⚠️", "FAIL": "❌"}
SECTION_ORDER = [
    "suitability",
    "deice",
    "customs",
    "slot_ppr",
    "osa_ssa",
    "overflight",
    "operational_notes",
]
SECTION_LABELS = {
    "suitability": "Suitability",
    "deice": "Deice",
    "customs": "Customs",
    "slot_ppr": "Slot / PPR",
    "osa_ssa": "OSA / SSA",
    "overflight": "Overflight",
    "operational_notes": "Other Operational Notes",
}
KEY_ISSUE_SECTIONS = {"customs", "deice", "overflight"}


def status_icon(status: str) -> str:
    return STATUS_EMOJI.get(status, "❔")


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


def _build_operational_notes_fetcher() -> Optional[
    Callable[[str, Optional[str]], Sequence[Mapping[str, Any]]]
]:
    config = st.session_state.get("feasibility_fl3xx_config")
    if config is None:
        settings = _load_fl3xx_settings()
        if not settings:
            return None
        try:
            config = build_fl3xx_api_config(dict(settings))
        except FlightDataError:
            return None
        st.session_state["feasibility_fl3xx_config"] = config
    try:
        return build_operational_notes_fetcher(config)
    except Exception:
        return None


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
    st.session_state["feasibility_fl3xx_config"] = config

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
    st.session_state["feasibility_fl3xx_config"] = config

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


def _run_full_quote_day(quote: Mapping[str, Any]) -> Optional[FullFeasibilityResult]:
    request_payload: Dict[str, Any] = {"quote": quote}
    fetcher = _build_operational_notes_fetcher()
    if fetcher:
        request_payload["operational_notes_fetcher"] = fetcher
    with st.spinner("Running feasibility checks for entire quote day…"):
        try:
            return run_feasibility_phase1(request_payload)
        except Exception as exc:  # pragma: no cover - UI safeguard
            st.exception(exc)
            return None


def _format_minutes(total_minutes: Optional[int]) -> str:
    if total_minutes is None:
        return "n/a"
    hours, minutes = divmod(int(total_minutes), 60)
    return f"{hours:d}h {minutes:02d}m"


def _format_note_text(note: Any) -> str:
    if isinstance(note, Mapping):
        for key in ("note", "body", "title", "category", "type"):
            value = note.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return str(note)
    if note is None:
        return ""
    return str(note)


def _render_category_block(label: str, category: Mapping[str, Any]) -> None:
    status = str(category.get("status", "PASS"))
    summary = category.get("summary") or status
    st.markdown(f"**{label}:** {status_icon(status)} {summary}")
    issues = [str(issue) for issue in category.get("issues", []) if issue]
    if issues:
        with st.expander(f"{label} details", expanded=status != "PASS"):
            for issue in issues:
                st.markdown(f"- {issue}")


def _render_leg_side(label: str, side: Mapping[str, Any]) -> None:
    icao = side.get("icao", "???") if isinstance(side, Mapping) else "???"
    st.markdown(f"**{label} {icao}**")
    for key in SECTION_ORDER:
        display = SECTION_LABELS.get(key, key.title())
        category = side.get(key) if isinstance(side, Mapping) else None
        if isinstance(category, Mapping):
            _render_category_block(display, category)
    raw_notes = side.get("raw_operational_notes") if isinstance(side, Mapping) else None
    if raw_notes:
        with st.expander(f"{label} {icao} raw notes"):
            for entry in raw_notes:
                text = _format_note_text(entry)
                if text:
                    st.markdown(f"- {text}")


def _collect_key_issues(result: Mapping[str, Any]) -> List[str]:
    issues: List[str] = []
    duty = result.get("duty") if isinstance(result, Mapping) else None
    if isinstance(duty, Mapping):
        duty_status = duty.get("status", "PASS")
        if duty_status in {"CAUTION", "FAIL"}:
            summary = duty.get("summary") or f"Duty {duty_status.title()}"
            issues.append(f"Duty: {summary}")

    legs = result.get("legs") if isinstance(result, Mapping) else None
    if isinstance(legs, Sequence):
        for index, leg in enumerate(legs, start=1):
            if not isinstance(leg, Mapping):
                continue
            for side_name in ("departure", "arrival"):
                side = leg.get(side_name)
                if not isinstance(side, Mapping):
                    continue
                icao = side.get("icao", "???")
                for key in SECTION_ORDER:
                    category = side.get(key)
                    if not isinstance(category, Mapping):
                        continue
                    status = category.get("status", "PASS")
                    if status == "PASS":
                        continue
                    display = SECTION_LABELS.get(key, key.title())
                    summary = category.get("summary") or status
                    label = f"{side_name.title()} {icao} {display}"
                    if status == "FAIL" or (status == "CAUTION" and key in KEY_ISSUE_SECTIONS):
                        issues.append(f"{label}: {summary}")
    return issues


def _render_full_quote_result(result: FullFeasibilityResult) -> None:
    legs = result.get("legs", [])
    duty = result.get("duty", {})
    overall_status = result.get("overall_status", "PASS")
    emoji = STATUS_EMOJI.get(overall_status, "")

    st.markdown("---")
    st.subheader(f"{emoji} Full Quote Day Status: {overall_status}")
    st.caption(
        f"{result.get('bookingIdentifier', 'Unknown Quote')} • {len(legs)} leg(s) • {result.get('aircraft_type', 'Unknown Aircraft')}"
    )

    summary = result.get("summary")
    if summary:
        formatted = summary.strip().replace("\n", "  \n")
        st.markdown(formatted)

    key_issues = _collect_key_issues(result)
    st.subheader("Key Issues")
    if key_issues:
        for issue in key_issues:
            st.markdown(f"- {issue}")
    else:
        st.caption("No customs, deice, duty, or permit cautions detected.")

    with st.expander("Duty Day Evaluation", expanded=duty.get("status") != "PASS"):
        status = duty.get("status", "PASS")
        col1, col2, col3 = st.columns(3)
        col1.metric("Duty Status", f"{status_icon(status)} {status}")
        col2.metric("Total Duty", _format_minutes(duty.get("total_duty")))
        col3.metric("Turn Segments", len(duty.get("turn_times", [])))
        st.write(f"- Start: {duty.get('duty_start_local') or 'Unknown'}")
        st.write(f"- End: {duty.get('duty_end_local') or 'Unknown'}")
        if duty.get("split_duty_possible"):
            st.write("- Split duty window available (≥ 6h ground).")
        if duty.get("reset_duty_possible"):
            st.write("- Reset possible (≥ 11h15 ground).")
        if duty.get("issues"):
            st.write("- Issues:")
            for entry in duty.get("issues", []):
                st.write(f"  • {entry}")

    for index, leg in enumerate(legs, start=1):
        departure = leg.get("departure", {}) if isinstance(leg, Mapping) else {}
        arrival = leg.get("arrival", {}) if isinstance(leg, Mapping) else {}
        dep_code = departure.get("icao", "???")
        arr_code = arrival.get("icao", "???")
        header = f"Leg {index}: {dep_code} → {arr_code}"
        with st.expander(header, expanded=False):
            _render_leg_side("Departure", departure)
            _render_leg_side("Arrival", arrival)

    with st.expander("Raw full quote result"):
        st.json(result)

quote_tab, booking_tab = st.tabs(["Quote ID", "Booking Identifier"])

with quote_tab:
    st.subheader("Search via Quote ID")
    st.caption(
        "Use this to evaluate feasibility before a booking exists. The dev engine always runs"
        " every leg in the quote so you consistently get duty-day coverage; expand the legs"
        " in the results below for per-segment breakdowns."
    )

    with st.form("quote-form", clear_on_submit=False):
        quote_input = st.text_input("Quote ID", placeholder="e.g. 3621613").strip()
        quote_submitted = st.form_submit_button("Load Quote")

    if quote_submitted:
        options = _load_quote_options(quote_input)
        if options:
            st.session_state["feasibility_quote_options"] = options

    quote_options = st.session_state.get("feasibility_quote_options", [])
    quote_payload = st.session_state.get("feasibility_quote_payload")

    if quote_options:
        st.markdown("**Loaded Legs**")
        for option in quote_options:
            leg_info = option.get("leg", {}) if isinstance(option, Mapping) else {}
            label = option.get("label", "Leg") if isinstance(option, Mapping) else "Leg"
            pax = leg_info.get("pax") or "n/a"
            block = leg_info.get("blockTime") or leg_info.get("flightTime") or "n/a"
            st.caption(f"{label}: PAX {pax} · Block {block} minutes")
    else:
        st.info("Load a quote to view available legs for feasibility analysis.")

    quote_loaded = isinstance(quote_payload, Mapping)
    with st.expander("Loaded quote payload"):
        if quote_loaded:
            st.json(quote_payload)
        else:
            st.caption("Load a quote to view the payload and enable multi-leg checks.")

    st.markdown("#### Evaluate Full Quote Day")
    if not quote_loaded:
        st.info("Load a quote to enable multi-leg feasibility checks.")

    run_full_quote = st.button(
        "Run Feasibility for Quote (All Legs)",
        key="run-full-quote",
        type="primary",
        disabled=not quote_loaded,
    )

    if run_full_quote and quote_loaded:
        full_day_result = _run_full_quote_day(quote_payload)
        if full_day_result:
            st.session_state["feasibility_last_full_quote_result"] = full_day_result

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
full_quote_result = st.session_state.get("feasibility_last_full_quote_result")

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
    st.info("Load a quote or submit a booking identifier to generate a feasibility report.")

if isinstance(full_quote_result, Mapping):
    _render_full_quote_result(cast(FullFeasibilityResult, full_quote_result))
