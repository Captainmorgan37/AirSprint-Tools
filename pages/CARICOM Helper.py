from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

import csv
from datetime import datetime, timezone

import streamlit as st
from openpyxl import load_workbook

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import (
    PreflightCrewMember,
    compute_fetch_dates,
    extract_crew_from_preflight,
    fetch_flights,
    fetch_preflight,
)
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


def _format_epoch_date(value: Any) -> str:
    try:
        epoch = int(value)
    except (TypeError, ValueError):
        return ""

    try:
        dt = datetime.fromtimestamp(epoch / 1000, tz=timezone.utc)
    except Exception:
        return ""

    return dt.strftime("%Y/%m/%d")


def _format_gender(value: Any) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip().upper()
    if cleaned.startswith("M"):
        return "M"
    if cleaned.startswith("F"):
        return "F"
    return cleaned


def _crew_rows(ws) -> list[int]:
    rows: list[int] = []
    for row in ws.iter_rows(min_col=2, max_col=2, min_row=19):
        cell = row[0]
        if isinstance(cell.value, int):
            rows.append(cell.row)
    return rows


def _populate_crew_sheet(
    workbook, crew_roster: Sequence[PreflightCrewMember], dep_airport: str, arr_airport: str
) -> None:
    crew_ws = workbook["Crew List"]
    dep_iata = _icao_to_iata(dep_airport)
    arr_iata = _icao_to_iata(arr_airport)
    target_rows = _crew_rows(crew_ws)

    fields = (
        ("last_name", 3),
        ("first_name", 6),
        ("middle_name", 9),
        ("nationality_iso3", 11),
        ("gender", 13),
        ("birth_date", 14),
        ("document_number", 16),
        ("document_issue_country_iso3", 18),
        ("document_expiration", 20),
    )

    for member, row in zip(crew_roster, target_rows):
        for field, column in fields:
            value = getattr(member, field)
            if field in {"birth_date", "document_expiration"}:
                value = _format_epoch_date(value)
            elif field == "gender":
                value = _format_gender(value)
            crew_ws.cell(row=row, column=column).value = value or ""

        crew_ws.cell(row=row, column=22).value = dep_iata
        crew_ws.cell(row=row, column=23).value = arr_iata
        crew_ws.cell(row=row, column=24).value = arr_iata

    empty_columns = (3, 6, 9, 11, 13, 14, 16, 18, 20, 22, 23, 24)
    for row in target_rows[len(crew_roster) :]:
        for column in empty_columns:
            crew_ws.cell(row=row, column=column).value = ""


def _build_workbook(
    leg: dict[str, Any],
    crew_roster: Sequence[PreflightCrewMember],
    dep_airport: str,
    arr_airport: str,
    crew_count: int,
) -> BytesIO:
    workbook = load_workbook("docs/caricomformFlightsV6.xlsx")
    general_ws = workbook["General Information"]

    dep_date, dep_time = _format_date_time(leg.get("dep_time"))
    arr_date, arr_time = _format_date_time(leg.get("arrival_time"))

    general_ws["B13"] = leg.get("aircraft_callsign") or leg.get("callsign") or _tail_to_callsign(
        leg.get("tail") or leg.get("aircraftName")
    )
    general_ws["E13"] = leg.get("tail") or leg.get("aircraftName") or ""
    general_ws["H13"] = leg.get("paxNumber") or ""
    general_ws["K13"] = len(crew_roster) or crew_count or ""

    general_ws["B18"] = dep_date
    general_ws["E18"] = dep_time
    general_ws["H18"] = _icao_to_iata(dep_airport)

    general_ws["L18"] = arr_date
    general_ws["O18"] = arr_time
    general_ws["R18"] = _icao_to_iata(arr_airport)

    general_ws["O13"] = ""
    general_ws["S13"] = "4032161699"
    general_ws["V13"] = "dispatch@airsprint.com"

    _populate_flight_details(workbook, leg, dep_airport, arr_airport)
    _populate_crew_sheet(workbook, crew_roster, dep_airport, arr_airport)

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return buffer


def _tail_to_callsign(tail: Any) -> str:
    if tail is None:
        return ""

    tail_sanitized = str(tail).replace("-", "").upper()
    tail_map = {

        "CGASL": "ASP816",
        "CFASV": "ASP812",
        "CFLAS": "ASP820",
        "CFJAS": "ASP822",
        "CFASF": "ASP827",
        "CGASE": "ASP846",
        "CGASK": "ASP839",
        "CGXAS": "ASP826",
        "CGBAS": "ASP875",
        "CFSNY": "ASP858",
        "CFSYX": "ASP844",
        "CFSBR": "ASP814",
        "CFSRX": "ASP864",
        "CFSJR": "ASP877",
        "CFASQ": "ASP821",
        "CFSDO": "ASP836",
        "CFASP": "ASP519",
        "CFASR": "ASP524",
        "CFASW": "ASP503",
        "CFIAS": "ASP511",
        "CGASR": "ASP510",
        "CGZAS": "ASP508",
        "CFASY": "ASP489",
        "CGASW": "ASP554",
        "CGAAS": "ASP567",
        "CFNAS": "ASP473",
        "CGNAS": "ASP642",
        "CGFFS": "ASP595",
        "CFSFS": "ASP654",
        "CGFSX": "ASP609",
        "CFSFO": "ASP668",
        "CFSNP": "ASP675",
        "CFSQX": "ASP556",
        "CFSFP": "ASP686",
        "CFSEF": "ASP574",
        "CFSDN": "ASP548",
        "CGFSD": "ASP655",
        "CFSUP": "ASP653",
        "CFSRY": "ASP565",
        "CGFSJ": "ASP501",
        "CGIAS": "ASP531",
    }

    return tail_map.get(tail_sanitized, "")


def _combine_date_time(date_str: str, time_str: str) -> str:
    return " ".join(part for part in (date_str, time_str) if part)


def _populate_flight_details(workbook, leg: Mapping[str, Any], dep_airport: str, arr_airport: str) -> None:
    dep_date, dep_time = _format_date_time(leg.get("dep_time"))
    arr_date, arr_time = _format_date_time(leg.get("arrival_time"))

    dep_dt = _combine_date_time(dep_date, dep_time)
    arr_dt = _combine_date_time(arr_date, arr_time)

    flight_id = leg.get("id") or leg.get("flightId") or leg.get("flight_id") or ""
    aircraft_name = leg.get("tail") or leg.get("aircraftName") or ""

    for sheet_name in ("Crew List", "Passenger List"):
        ws = workbook[sheet_name]
        ws["D11"] = flight_id
        ws["G11"] = aircraft_name
        ws["L11"] = dep_dt
        ws["O11"] = _icao_to_iata(dep_airport)
        ws["R11"] = arr_dt
        ws["U11"] = _icao_to_iata(arr_airport)


@st.cache_data(show_spinner=False)
def _airport_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    path = Path("Airport TZ.txt")
    if not path.exists():
        return lookup

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            icao = (row.get("icao") or row.get("ICAO") or "").strip().upper()
            iata = (row.get("iata") or row.get("IATA") or "").strip().upper()
            if icao and iata:
                lookup[icao] = iata
    return lookup


def _icao_to_iata(code: Any) -> str:
    if code is None:
        return ""

    icao = str(code).strip().upper()
    lookup = _airport_lookup()
    return lookup.get(icao, icao)


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

try:
    config = build_fl3xx_api_config(dict(api_settings))
    flight_id = selected_leg.get("id") or selected_leg.get("flightId") or selected_leg.get("flight_id")
    crew_roster: list[PreflightCrewMember] = []
    if flight_id:
        with st.spinner("Loading preflight crew roster…"):
            preflight_payload = fetch_preflight(config, flight_id)
        crew_roster = extract_crew_from_preflight(preflight_payload)
        if crew_roster:
            crew_count = len(crew_roster)
    else:
        st.warning("Flight ID missing on selected leg; crew roster will be left blank.")
except Exception as exc:  # pragma: no cover - runtime fetch failures
    crew_roster = []
    st.warning(f"Unable to load crew roster from preflight: {exc}")

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
    "Crew details are now pulled from the preflight endpoint. Passenger roster fields "
    "remain blank until the pax_details feed is connected."
)

workbook_bytes = _build_workbook(selected_leg, crew_roster, dep_airport, arr_airport, crew_count)
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
