from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Iterable, Mapping

import streamlit as st
from openpyxl import load_workbook

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import compute_fetch_dates, fetch_flights
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    normalize_fl3xx_payload,
    safe_parse_dt,
)

configure_page(page_title="CARICOM Helper")
password_gate()
render_sidebar()

st.title("🌴 CARICOM Helper")

st.write(
    """
    Generate a CARICOM eAPIS workbook for an upcoming booking. Start by entering a
    **Booking Identifier** to pull the next 72 hours of flights from FL3XX. Any
    fields we cannot yet populate are left blank so the exported Excel template
    remains upload-ready while we build out the full data feed.
    """
)

CARICOM_COUNTRIES = {
    "Antigua and Barbuda",
    "Bahamas",
    "Barbados",
    "Belize",
    "Dominica",
    "Grenada",
    "Guyana",
    "Haiti",
    "Jamaica",
    "Montserrat",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Suriname",
    "Trinidad and Tobago",
}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_upcoming_flights(settings: Dict[str, Any]) -> list[dict[str, Any]]:
    config = build_fl3xx_api_config(settings)
    from_date, to_date = compute_fetch_dates(inclusive_days=2)
    flights, _ = fetch_flights(config, from_date=from_date, to_date=to_date)
    return flights


def _normalise_legs(payload: Iterable[Any]) -> list[dict[str, Any]]:
    legs, _ = normalize_fl3xx_payload(list(payload))
    return legs


def _matches_booking(leg: Mapping[str, Any], booking_identifier: str) -> bool:
    target = booking_identifier.strip().lower()
    if not target:
        return False

    for key in ("bookingIdentifier", "bookingReference", "bookingCode", "bookingId"):
        value = leg.get(key)
        if isinstance(value, str) and value.strip().lower() == target:
            return True
    return False


def _format_date_time(value: Any) -> tuple[str, str]:
    if value is None:
        return "", ""

    try:
        dt = safe_parse_dt(str(value))
    except Exception:
        return str(value), ""

    return dt.strftime("%Y/%m/%d"), dt.strftime("%H:%M")


def _build_workbook(leg: dict[str, Any]) -> BytesIO:
    workbook = load_workbook("docs/caricomformFlightsV6.xlsx")
    general_ws = workbook["General Information"]

    dep_date, dep_time = _format_date_time(leg.get("dep_time"))
    arr_date, arr_time = _format_date_time(leg.get("arrival_time"))

    general_ws["B13"] = leg.get("flightId") or leg.get("bookingReference") or ""
    general_ws["E13"] = leg.get("tail") or leg.get("aircraftName") or ""
    general_ws["H13"] = leg.get("paxNumber") or ""

    crew_members = leg.get("crewMembers")
    general_ws["K13"] = len(crew_members) if isinstance(crew_members, list) else ""

    general_ws["B18"] = dep_date
    general_ws["E18"] = dep_time
    general_ws["H18"] = leg.get("departure_airport") or ""

    general_ws["L18"] = arr_date
    general_ws["O18"] = arr_time
    general_ws["R18"] = leg.get("arrival_airport") or ""

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return buffer


booking_identifier = st.text_input("Booking Identifier", placeholder="e.g. ASP-123456")
fetch_button = st.button("Fetch Flight & Prepare Workbook", type="primary")

if not fetch_button:
    st.info("Enter a booking identifier and click **Fetch** to begin.")
    st.stop()

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error(
        "FL3XX API credentials are missing. Add them under `fl3xx_api` in `.streamlit/secrets.toml`."
    )
    st.stop()

try:
    with st.spinner("Fetching upcoming flights from FL3XX…"):
        raw_flights = _fetch_upcoming_flights(dict(api_settings))
        legs = _normalise_legs(raw_flights)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except Exception as exc:  # pragma: no cover - runtime fetch failures
    st.error(f"Flight retrieval failed: {exc}")
    st.stop()

if not legs:
    st.warning("No flights returned for the upcoming 72-hour window.")
    st.stop()

matched_legs = [leg for leg in legs if _matches_booking(leg, booking_identifier)]

if not matched_legs:
    st.warning("No matching booking was found in the next 72 hours.")
    st.stop()

sorted_legs = sorted(
    matched_legs,
    key=lambda leg: leg.get("dep_time") or "",
)
selected_leg = sorted_legs[0]

dep_airport = selected_leg.get("departure_airport")
arr_airport = selected_leg.get("arrival_airport")
crew_members = selected_leg.get("crewMembers") if isinstance(selected_leg.get("crewMembers"), list) else []
crew_count = len(crew_members)
caricom_route = [code for code in (dep_airport, arr_airport) if code]

st.subheader("Flight summary")
st.json(
    {
        "Booking": booking_identifier,
        "Tail": selected_leg.get("tail"),
        "Departure": dep_airport,
        "Arrival": arr_airport,
        "Passenger Count": selected_leg.get("paxNumber"),
        "Crew Count": crew_count,
        "Route Airports": caricom_route,
    },
    expanded=False,
)

st.info(
    "This initial version leaves crew and passenger rosters blank in the export. "
    "We will fill these fields as supporting APIs become available."
)

workbook_bytes = _build_workbook(selected_leg)
file_label = f"CARICOM_{booking_identifier or 'booking'}.xlsx"

st.download_button(
    "Download CARICOM Excel",
    data=workbook_bytes,
    file_name=file_label,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.caption(
    "Flights are fetched for today through two days ahead. If CARICOM applicability needs validation, "
    "use the routing summary above while we extend the automated coverage check."
)
