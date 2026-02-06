from __future__ import annotations

from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any, Dict, Mapping, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from Home import configure_page, get_secret, password_gate, render_sidebar
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from oca_reports import (
    HighPaxWeightAlert,
    MaxFlightTimeAlert,
    MelHoldItem,
    RunwayLengthCheck,
    ZfwFlightCheck,
    evaluate_flights_for_max_time,
    evaluate_flights_for_high_pax_weight,
    evaluate_flights_for_runway_length,
    evaluate_flights_for_zfw_check,
    evaluate_mel_hold_items,
    format_duration_label,
    _format_pax_breakdown,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None  # type: ignore[assignment]


_STATE_KEY = "oca_reports_state"
_MOUNTAIN_TZ_NAME = "America/Edmonton"
TAIL_DISPLAY_ORDER = (
    "C-GASL",
    "C-FASV",
    "C-FLAS",
    "C-FJAS",
    "C-FASF",
    "C-GASE",
    "C-GASK",
    "C-GXAS",
    "C-GBAS",
    "C-FSNY",
    "C-FSYX",
    "C-FSBR",
    "C-FSRX",
    "C-FSJR",
    "C-FASQ",
    "C-FSDO",
    "C-FASP",
    "C-FASR",
    "C-FASW",
    "C-FIAS",
    "C-GASR",
    "C-GZAS",
    "C-FASY",
    "C-GASW",
    "C-GAAS",
    "C-FNAS",
    "C-GNAS",
    "C-GFFS",
    "C-FSFS",
    "C-GFSX",
    "C-FSFO",
    "C-FSNP",
    "C-FSQX",
    "C-FSFP",
    "C-FSEF",
    "C-FSDN",
    "C-GFSD",
    "C-FSUP",
    "C-FSRY",
    "C-GFSJ",
    "C-GIAS",
    "C-FSVP",
    "ADD EMB WEST",
    "ADD EMB EAST",
    "ADD CJ2+ WEST",
    "ADD CJ2+ EAST",
    "ADD CJ3+ WEST",
    "ADD CJ3+ EAST",
)
TAIL_INDEX = {tail: idx for idx, tail in enumerate(TAIL_DISPLAY_ORDER)}


def _default_start_date() -> date:
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(_MOUNTAIN_TZ_NAME)
            return datetime.now(tz).date()
        except Exception:
            pass
    return date.today()


def _tail_order_key(tail: str) -> tuple[int, str]:
    return (TAIL_INDEX.get(tail, len(TAIL_DISPLAY_ORDER)), tail)



@st.cache_data(ttl=3600)
def _load_tail_list() -> list[str]:
    try:
        tail_df = pd.read_csv("tails.csv")
    except FileNotFoundError:
        return []
    if tail_df.empty:
        return []
    column = "Tail" if "Tail" in tail_df.columns else tail_df.columns[0]
    tails = [
        str(value).strip().upper()
        for value in tail_df[column].dropna()
        if str(value).strip()
    ]
    ordered = sorted(set(tails), key=_tail_order_key)
    return ordered


def _format_tail_for_api(value: str) -> str:
    text = "".join(str(value).upper().split("-"))
    if not text:
        return ""
    if "-" in str(value):
        return str(value).upper()
    if len(text) > 1:
        return f"{text[0]}-{text[1:]}"
    return text


def _condense_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return " ".join(text.split())


def _build_mel_report_text(items: pd.DataFrame) -> str:
    if items.empty:
        return ""
    report_date = _default_start_date().strftime("%d%b%y").upper()
    lines = [f"Current MELs - {report_date}", ""]
    ordered_items = items.sort_values(
        by=["tail"], key=lambda series: series.map(_tail_order_key)
    )
    for _, row in ordered_items.iterrows():
        tail = _format_tail_for_api(row.get("tail", ""))
        description = _condense_text(row.get("description"))
        limitation = _condense_text(row.get("limitations_description"))
        if not limitation:
            limitation = _condense_text(row.get("limitations"))
        if limitation:
            lines.append(f"{tail} - {description} - {limitation}")
        else:
            lines.append(f"{tail} - {description}")
        lines.append("")
    return "\n".join(lines).strip()


def _parse_iso_date(value: Any) -> Optional[date]:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _store_state(state: Dict[str, Any]) -> None:
    existing = _load_state()
    merged = dict(existing)
    merged.update(state)
    st.session_state[_STATE_KEY] = merged


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
    if "max_time" in state or "zfw" in state or "high_pax_weight" in state:
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
            "high_pax_weight": {},
        }
        if state.get("error"):
            legacy["error"] = state["error"]
        return legacy

    return dict(state)


def _enable_booking_reference_copy_support() -> None:
    components.html(
        """
        <script>
        const w = window.parent;
        if (w.__bookingReferenceCopyBound) {
            return;
        }
        w.__bookingReferenceCopyBound = true;

        const doc = w.document;

        if (!doc.getElementById("booking-reference-copy-style")) {
            const style = doc.createElement("style");
            style.id = "booking-reference-copy-style";
            style.textContent = `
                [data-booking-reference-copy] {
                    cursor: copy;
                    position: relative;
                }

                [data-booking-reference-copy]:focus {
                    outline: 2px solid var(--primary-color, #2c8ef4);
                    outline-offset: -2px;
                }

                [data-booking-reference-copy].booking-reference-copied::after {
                    content: "Copied";
                    position: absolute;
                    top: 50%;
                    right: 0.5rem;
                    transform: translateY(-50%);
                    font-size: 0.7rem;
                    color: var(--text-color, #444);
                    background: rgba(255, 255, 255, 0.85);
                    padding: 0.1rem 0.3rem;
                    border-radius: 0.25rem;
                }
            `;
            doc.head.appendChild(style);
        }

        function selectCell(cell) {
            const range = doc.createRange();
            range.selectNodeContents(cell);
            const selection = w.getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
        }

        function flagCells(tableRoot) {
            const headers = Array.from(tableRoot.querySelectorAll('[role="columnheader"]'));
            const header = headers.find((node) => node.innerText.trim().toLowerCase() === "flight_reference");
            if (!header) {
                return;
            }

            const colIndex = header.getAttribute("aria-colindex");
            if (!colIndex) {
                return;
            }

            const cells = tableRoot.querySelectorAll(`[role="gridcell"][aria-colindex="${colIndex}"]`);
            cells.forEach((cell) => {
                if (cell.dataset.bookingReferenceCopy === "true") {
                    return;
                }
                cell.dataset.bookingReferenceCopy = "true";
                cell.setAttribute("data-booking-reference-copy", "");
                cell.setAttribute("tabindex", "0");

                cell.addEventListener("click", () => {
                    selectCell(cell);
                    const value = cell.innerText.trim();
                    if (value && w.navigator?.clipboard) {
                        w.navigator.clipboard.writeText(value).catch(() => {});
                        cell.classList.add("booking-reference-copied");
                        setTimeout(() => {
                            cell.classList.remove("booking-reference-copied");
                        }, 1200);
                    }
                });

                cell.addEventListener("keydown", (event) => {
                    if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        cell.click();
                    }
                });
            });
        }

        function processExistingTables() {
            const tables = doc.querySelectorAll('div[data-testid="stDataFrame"]');
            tables.forEach(flagCells);
        }

        processExistingTables();

        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                    if (!node || node.nodeType !== 1) {
                        return;
                    }
                    if (node.matches && node.matches('div[data-testid="stDataFrame"]')) {
                        flagCells(node);
                    }
                    const nestedTables = node.querySelectorAll ? node.querySelectorAll('div[data-testid="stDataFrame"]') : [];
                    nestedTables.forEach(flagCells);
                });
            });
        });

        observer.observe(doc.body, { childList: true, subtree: true });
        </script>
        """,
        height=0,
    )


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

        st.dataframe(display_df, width="stretch")

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

        st.dataframe(display_df, width="stretch")

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


def _render_high_pax_weight_results(
    data: Mapping[str, Any], start_label: Optional[str], end_label: Optional[str]
) -> None:
    st.subheader("🧳 High Pax Weight Report")

    error = data.get("error")
    if error:
        st.error(error)
        return

    items = data.get("items", [])
    metadata = data.get("metadata", {})
    diagnostics = data.get("diagnostics", {})

    if start_label and end_label:
        if items:
            st.warning(
                f"Identified {len(items)} PAX flight(s) exceeding the passenger weight thresholds "
                f"between {start_label} and {end_label}."
            )
        else:
            st.info(
                f"No PAX flights exceeded the passenger weight thresholds between {start_label} and {end_label}."
            )

    if items or start_label:
        summary_cols = st.columns(4)
        summary_cols[0].metric("Flights flagged", diagnostics.get("flagged_flights", len(items)))
        summary_cols[1].metric("PAX flights evaluated", diagnostics.get("pax_flights", 0))
        summary_cols[2].metric("Flights with duration over threshold", diagnostics.get("duration_applicable", 0))
        summary_cols[3].metric("Payload pulls", diagnostics.get("payloads_requested", 0))

        detail_cols = st.columns(3)
        detail_cols[0].metric("Missing durations", diagnostics.get("missing_duration", 0))
        detail_cols[1].metric("Payload errors", diagnostics.get("payload_errors", 0))
        detail_cols[2].metric("Flights missing pax weights", diagnostics.get("missing_pax_weights", 0))

    df = pd.DataFrame(items)
    if not df.empty:
        df = df.copy()
        df["Duration"] = df["duration_minutes"].map(format_duration_label)
        df["Total Weight (lbs)"] = df["pax_weight_lbs"].map(lambda v: f"{float(v):,.0f}")
        df["Weight threshold (lbs)"] = df["pax_weight_threshold_lbs"].map(lambda v: f"{int(v):,}")
        df["Pax breakdown"] = df["pax_breakdown"].map(_format_pax_breakdown)
        df["Cargo weight (lbs)"] = df["cargo_weight_lbs"].map(lambda v: f"{float(v):,.0f}")
        df["Animal weight (lbs)"] = df["animal_weight_lbs"].map(lambda v: f"{float(v):,.0f}")
        df["Departure Date/Time (UTC)"] = df["departure_utc"]
        df["Tail #"] = df["registration"]
        df["Departure Airport"] = df["airport_from"]
        df["Arrival Airport"] = df["airport_to"]
        df["Flight #"] = df["flight_reference"]
        df["Aircraft Type"] = df["aircraft_category"]
        df["# of Pax"] = df["pax_count"]

        columns = [
            "Departure Date/Time (UTC)",
            "Tail #",
            "Departure Airport",
            "Arrival Airport",
            "Flight #",
            "Aircraft Type",
            "# of Pax",
            "Duration",
            "Total Weight (lbs)",
            "Weight threshold (lbs)",
            "Pax breakdown",
            "Cargo weight (lbs)",
            "Animal weight (lbs)",
        ]
        available_columns = [col for col in columns if col in df.columns]
        display_df = df[available_columns]

        st.dataframe(display_df, width="stretch")

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
            "missing_pax_weights",
            "pax_weight_lbs",
            "pax_weight_threshold_lbs",
            "pax_breakdown",
            "cargo_weight_lbs",
            "animal_weight_lbs",
            "duration_minutes",
            "duration_threshold_minutes",
        ]
        export_df = df[[col for col in export_columns if col in df.columns]]
        export_df.to_csv(csv_buffer, index=False)
        st.download_button(
            "Download high pax weight flights as CSV",
            csv_buffer.getvalue(),
            file_name="oca_high_pax_weight_report.csv",
            mime="text/csv",
        )

    with st.expander("FL3XX request metadata", expanded=False):
        st.json(metadata)

    with st.expander("Diagnostics", expanded=False):
        st.json(diagnostics)


def _render_primary_results(state: Dict[str, Any]) -> None:
    normalised = _normalise_state(state)

    error = normalised.get("error")
    if error:
        st.error(error)
        return

    start_label = normalised.get("start_date")
    end_label = normalised.get("end_date")

    max_time_data = normalised.get("max_time") or {}
    zfw_data = normalised.get("zfw") or {}
    high_pax_weight_data = normalised.get("high_pax_weight") or {}

    if not any((max_time_data, zfw_data, high_pax_weight_data, start_label, end_label)):
        return

    if max_time_data:
        _render_max_time_results(max_time_data, start_label, end_label)

    if zfw_data:
        _render_zfw_results(zfw_data, start_label, end_label)

    if high_pax_weight_data:
        _render_high_pax_weight_results(high_pax_weight_data, start_label, end_label)


def _render_runway_results(state: Mapping[str, Any]) -> None:
    runway_state = state.get("runway") if isinstance(state, Mapping) else None
    start_label = state.get("runway_start_date") if isinstance(state, Mapping) else None
    end_label = state.get("runway_end_date") if isinstance(state, Mapping) else None

    if not runway_state and not start_label and not end_label:
        return

    st.subheader("🛬 Short Runway Report")

    if not isinstance(runway_state, Mapping):
        return

    error = runway_state.get("error")
    if error:
        st.error(error)
        return

    items = runway_state.get("items", [])
    metadata = runway_state.get("metadata", {})
    diagnostics = runway_state.get("diagnostics", {})
    threshold_ft = runway_state.get("threshold_ft")
    try:
        threshold_value = int(threshold_ft)
    except (TypeError, ValueError):
        threshold_value = 5000

    if start_label and end_label:
        start_display = str(start_label)
        end_display = str(end_label)
        if items:
            st.warning(
                f"Identified {len(items)} flight(s) operating from airports below the {threshold_value:,} ft threshold "
                f"between {start_display} and {end_display}."
            )
        else:
            st.info(
                f"No airports below the {threshold_value:,} ft threshold were found between {start_display} and {end_display}."
            )

    if items:
        summary_cols = st.columns(3)
        summary_cols[0].metric("Flights flagged", diagnostics.get("flagged_flights", len(items)))
        summary_cols[1].metric("Total flights evaluated", diagnostics.get("total_flights", 0))
        summary_cols[2].metric("Missing runway data", diagnostics.get("missing_departure_length", 0) + diagnostics.get("missing_arrival_length", 0))

    df = pd.DataFrame(items)
    if not df.empty:
        df = df.copy()

        def _format_length(value: Any) -> str:
            if value is None or value == "":
                return "—"
            try:
                return f"{int(value):,}"
            except (TypeError, ValueError):
                return str(value)

        df["Departure runway (ft)"] = df["departure_runway_length_ft"].map(_format_length)
        df["Departure status"] = df["departure_below_threshold"].map(
            lambda flag: "⚠️ Below threshold" if flag else ""
        )
        df["Arrival runway (ft)"] = df["arrival_runway_length_ft"].map(_format_length)
        df["Arrival status"] = df["arrival_below_threshold"].map(
            lambda flag: "⚠️ Below threshold" if flag else ""
        )

        columns = [
            "departure_utc",
            "arrival_utc",
            "registration",
            "airport_from",
            "Departure runway (ft)",
            "Departure status",
            "airport_to",
            "Arrival runway (ft)",
            "Arrival status",
            "flight_reference",
            "booking_reference",
            "flight_number",
        ]
        display_columns = [col for col in columns if col in df.columns]
        display_df = df[display_columns]

        st.dataframe(display_df, width="stretch")

        csv_buffer = StringIO()
        export_columns = [
            "flight_id",
            "quote_id",
            "flight_reference",
            "booking_reference",
            "departure_utc",
            "arrival_utc",
            "airport_from",
            "departure_runway_length_ft",
            "departure_below_threshold",
            "airport_to",
            "arrival_runway_length_ft",
            "arrival_below_threshold",
            "registration",
            "flight_number",
            "runway_threshold_ft",
        ]
        export_df = df[[col for col in export_columns if col in df.columns]]
        export_df.to_csv(csv_buffer, index=False)
        st.download_button(
            f"Download short runway flights (< {threshold_value:,} ft)",
            csv_buffer.getvalue(),
            file_name="oca_short_runway_flights.csv",
            mime="text/csv",
        )

    with st.expander("FL3XX request metadata", expanded=False):
        st.json(metadata)

    with st.expander("Diagnostics", expanded=False):
        st.json(diagnostics)


def _render_mel_results(state: Mapping[str, Any]) -> None:
    mel_state = state.get("mel")
    if not isinstance(mel_state, Mapping):
        st.info("Run the MEL report to view hold items.")
        return

    if mel_state.get("error"):
        st.error(mel_state.get("error"))
        return

    items = mel_state.get("items", [])
    if not isinstance(items, list):
        st.info("No MEL hold items available.")
        return

    df = pd.DataFrame(items)
    if df.empty:
        st.info("No MEL hold items were returned for the selected window.")
        return

    df = df.fillna("")
    df["has_description"] = df.get("has_description", False)
    df["has_limitation"] = df.get("has_limitation", False)
    df["has_client_impact"] = df.get("has_client_impact", False)
    df["tail"] = df["tail"].apply(_format_tail_for_api)

    primary_df = df[df["has_description"] & df["has_limitation"]].copy()
    secondary_df = df[~(df["has_description"] & df["has_limitation"])].copy()
    primary_df = primary_df.sort_values(by=["tail"], key=lambda series: series.map(_tail_order_key))
    secondary_df = secondary_df.sort_values(by=["tail"], key=lambda series: series.map(_tail_order_key))

    st.subheader("MELs with descriptions and limitations")
    st.caption("These hold items include both a description and a limitation.")

    report_text = _build_mel_report_text(primary_df)
    if report_text:
        st.text_area("Copyable MEL report text", report_text, height=240)
        st.download_button(
            "Download MEL report text",
            data=report_text,
            file_name="mel_report.txt",
            mime="text/plain",
        )
    else:
        st.info("No MEL hold items with both a description and limitation were found.")

    if not primary_df.empty:
        primary_display = primary_df[
            ["tail", "description", "limitations", "limitations_description"]
        ].rename(
            columns={
                "tail": "Tail",
                "description": "Description",
                "limitations": "Limitations",
                "limitations_description": "Limitations Description",
            }
        )

        st.dataframe(primary_display, width="stretch")

    st.subheader("MELs with partial details")
    st.caption("Hold items that only include a description or a limitation.")
    if secondary_df.empty:
        st.info("No MEL hold items with partial details were found.")
    else:
        secondary_display = secondary_df[
            ["tail", "description", "limitations", "limitations_description"]
        ].rename(
            columns={
                "tail": "Tail",
                "description": "Description",
                "limitations": "Limitations",
                "limitations_description": "Limitations Description",
            }
        )
        st.dataframe(secondary_display, width="stretch")

    metadata = mel_state.get("metadata", {})
    diagnostics = mel_state.get("diagnostics", {})

    with st.expander("FL3XX request metadata", expanded=False):
        st.json(metadata)

    with st.expander("Diagnostics", expanded=False):
        st.json(diagnostics)


configure_page(page_title="OCA Reports")
password_gate()
render_sidebar()
_enable_booking_reference_copy_support()

st.title("🛫 OCA Reports")
st.caption("Generate OCA-specific monitoring reports based on FL3XX data.")

raw_state = _load_state()
state = _normalise_state(raw_state)

api_settings_raw = get_secret("fl3xx_api", {})
api_settings = _normalise_settings(api_settings_raw)

primary_tab, runway_tab, mel_tab = st.tabs(
    ["Max Flight Time & ZFW", "Short Runway Report", "MEL Report"]
)

with primary_tab:
    _render_primary_results(state)

    default_start = _parse_iso_date(state.get("start_date")) or _default_start_date()
    default_days_raw = state.get("days")
    try:
        default_days = int(default_days_raw)
    except (TypeError, ValueError):
        default_days = 3

    with st.form("oca_reports_form"):
        start_date = st.date_input(
            "Report start date",
            value=default_start,
            help="The monitoring window begins on this date in America/Edmonton.",
        )
        day_count = st.number_input(
            "Days to monitor",
            min_value=1,
            max_value=7,
            value=default_days,
            step=1,
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
                high_pax_weight_state: Dict[str, Any] = {}

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

                try:
                    with st.spinner("Evaluating high pax weight thresholds..."):
                        high_pax_items, high_pax_metadata, high_pax_diagnostics = (
                            evaluate_flights_for_high_pax_weight(
                                config,
                                from_date=start_date,
                                to_date=to_date_exclusive,
                            )
                        )
                except FlightDataError as exc:
                    high_pax_weight_state["error"] = str(exc)
                    st.error(str(exc))
                except Exception as exc:  # pragma: no cover - defensive UI path
                    high_pax_weight_state["error"] = str(exc)
                    st.error(str(exc))
                else:
                    high_pax_weight_state = {
                        "items": [
                            item.as_dict()
                            if isinstance(item, HighPaxWeightAlert)
                            else dict(item)
                            for item in high_pax_items
                        ],
                        "metadata": high_pax_metadata,
                        "diagnostics": high_pax_diagnostics,
                    }

                _store_state(
                    {
                        "start_date": start_date.isoformat(),
                        "end_date": inclusive_end.isoformat(),
                        "days": int(day_count),
                        "max_time": max_time_state,
                        "zfw": zfw_state,
                        "high_pax_weight": high_pax_weight_state,
                    }
                )
                st.rerun()

with runway_tab:
    _render_runway_results(state)

    default_runway_start = _parse_iso_date(state.get("runway_start_date")) or _default_start_date()
    default_runway_days_raw = state.get("runway_days")
    try:
        default_runway_days = int(default_runway_days_raw)
    except (TypeError, ValueError):
        default_runway_days = 2

    with st.form("oca_runway_form"):
        runway_start_date = st.date_input(
            "Runway report start date",
            value=default_runway_start,
            help="The short runway scan begins on this date in America/Edmonton.",
        )
        runway_day_count = st.number_input(
            "Days to scan",
            min_value=1,
            max_value=7,
            value=default_runway_days,
            step=1,
            help="Include this many calendar days in the short runway scan.",
        )
        runway_submitted = st.form_submit_button("Run Short Runway Report")

    if runway_submitted:
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
                runway_to_date = runway_start_date + timedelta(days=int(runway_day_count))
                runway_inclusive_end = runway_to_date - timedelta(days=1)

                runway_state: Dict[str, Any] = {}

                try:
                    with st.spinner("Evaluating runway lengths..."):
                        runway_items, runway_metadata, runway_diagnostics = evaluate_flights_for_runway_length(
                            config,
                            from_date=runway_start_date,
                            to_date=runway_to_date,
                        )
                except FlightDataError as exc:
                    runway_state["error"] = str(exc)
                    st.error(str(exc))
                except Exception as exc:  # pragma: no cover - defensive UI path
                    runway_state["error"] = str(exc)
                    st.error(str(exc))
                else:
                    runway_state = {
                        "items": [
                            item.as_dict() if isinstance(item, RunwayLengthCheck) else dict(item)
                            for item in runway_items
                        ],
                        "metadata": runway_metadata,
                        "diagnostics": runway_diagnostics,
                        "threshold_ft": 5000,
                    }

                _store_state(
                    {
                        "runway_start_date": runway_start_date.isoformat(),
                        "runway_end_date": runway_inclusive_end.isoformat(),
                        "runway_days": int(runway_day_count),
                        "runway": runway_state,
                    }
                )
                st.rerun()

with mel_tab:
    _render_mel_results(state)

    mel_today = _default_start_date()
    mel_default_start = mel_today - timedelta(days=365)

    with st.form("oca_mel_form"):
        mel_start_date = st.date_input(
            "MEL report start date",
            value=mel_default_start,
            help="Defaults to one year before today.",
        )
        mel_end_date = st.date_input(
            "MEL report end date",
            value=mel_today,
            help="Defaults to today.",
        )
        mel_submitted = st.form_submit_button("Run MEL Report")

    if mel_submitted:
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
                tails = _load_tail_list()
                if not tails:
                    st.error("No tails were found in tails.csv.")
                else:
                    mel_state: Dict[str, Any] = {}
                    try:
                        with st.spinner("Fetching MEL hold items..."):
                            mel_items, mel_metadata, mel_diagnostics = evaluate_mel_hold_items(
                                config,
                                tails=[_format_tail_for_api(tail) for tail in tails],
                                from_date=mel_start_date,
                                to_date=mel_end_date,
                            )
                    except FlightDataError as exc:
                        mel_state["error"] = str(exc)
                        st.error(str(exc))
                    except Exception as exc:  # pragma: no cover - defensive UI path
                        mel_state["error"] = str(exc)
                        st.error(str(exc))
                    else:
                        mel_state = {
                            "items": [
                                item.as_dict() if isinstance(item, MelHoldItem) else dict(item)
                                for item in mel_items
                            ],
                            "metadata": mel_metadata,
                            "diagnostics": mel_diagnostics,
                            "start_date": mel_start_date.isoformat(),
                            "end_date": mel_end_date.isoformat(),
                        }

                    _store_state({"mel": mel_state})
                    st.rerun()
