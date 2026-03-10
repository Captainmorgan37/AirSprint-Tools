from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
import json

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar


configure_page(page_title="Roster Pull Explorer")
password_gate()
render_sidebar()

CREW_IDENTIFIER_ENTRY_TYPES = {"A"}
DISPLAY_ENTRY_TYPES = {"A", "OFF", "P", "SIC"}
SCHEDULE_PRIORITY = {"A": 1, "SIC": 2, "P": 3, "OFF": 4}


PEOPLE_COLUMNS = [
    "internal_id",
    "name",
    "trigram",
    "personnel_number",
    "email",
    "status",
    "role",
    "entry_types",
    "has_A",
    "has_SIC",
    "has_P",
    "flight_count",
]

ENTRIES_COLUMNS = [
    "internal_id",
    "name",
    "entry_id",
    "entry_type",
    "start_utc",
    "end_utc",
    "counts_as_duty_time",
    "begins_duty_period",
    "ends_duty_period",
    "from_airport",
    "to_airport",
    "notes",
]

FLIGHTS_COLUMNS = [
    "internal_id",
    "name",
    "flight_id",
    "quote_id",
    "booking_identifier",
    "flight_type",
    "aircraft_category",
    "registration",
    "from_airport",
    "to_airport",
    "etd_utc",
    "eta_utc",
    "block_off_est_utc",
    "block_on_est_utc",
    "workflow",
    "workflow_name",
    "account_name",
    "pax_number",
    "flight_status",
    "post_flight_closed",
    "crew_role",
    "pilot_takeoff",
    "pilot_landing",
    "takeoffs",
    "landings",
]


def safe_get(data: object, *keys: str, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def ms_to_iso_utc(value: object) -> str | None:
    if value in (None, ""):
        return None
    try:
        timestamp_seconds = int(value) / 1000
        return datetime.fromtimestamp(timestamp_seconds, tz=UTC).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def build_full_name(user: dict) -> str:
    first = str(user.get("firstName") or "").strip()
    last = str(user.get("lastName") or "").strip()
    full = f"{first} {last}".strip()
    return full or str(user.get("name") or "").strip() or "Unknown"


def _matched_crew_member(user_internal_id: object, flight: dict) -> dict | None:
    crew_list = flight.get("crew") if isinstance(flight, dict) else None
    if not isinstance(crew_list, list):
        return None
    for crew_member in crew_list:
        if isinstance(crew_member, dict) and crew_member.get("pilotId") == user_internal_id:
            return crew_member
    return None


def infer_role_from_flights(user_internal_id: object, flights: list[dict]) -> str | None:
    roles: list[str] = []
    for flight in flights:
        if not isinstance(flight, dict):
            continue
        matched = _matched_crew_member(user_internal_id, flight)
        if not matched:
            continue
        role = matched.get("role")
        if role:
            roles.append(str(role))

    if not roles:
        return None
    return Counter(roles).most_common(1)[0][0]


def has_a_entry(entries: list[dict]) -> bool:
    for entry in entries:
        if isinstance(entry, dict) and entry.get("type") in CREW_IDENTIFIER_ENTRY_TYPES:
            return True
    return False


def appears_as_flight_crew(user_internal_id: object, flights: list[dict]) -> bool:
    for flight in flights:
        if isinstance(flight, dict) and _matched_crew_member(user_internal_id, flight):
            return True
    return False


def is_flying_crewmember(person: dict) -> bool:
    user = person.get("user") if isinstance(person, dict) else {}
    entries = person.get("entries") if isinstance(person, dict) else []
    flights = person.get("flights") if isinstance(person, dict) else []

    entries = entries if isinstance(entries, list) else []
    flights = flights if isinstance(flights, list) else []
    user_internal_id = user.get("internalId") if isinstance(user, dict) else None

    return has_a_entry(entries) or appears_as_flight_crew(user_internal_id, flights)


@st.cache_data(show_spinner=False)
def parse_fl3xx_roster(raw_text: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    payload = json.loads(raw_text)
    staff = payload.get("staff") if isinstance(payload, dict) else []
    staff = staff if isinstance(staff, list) else []

    people_rows: list[dict] = []
    entry_rows: list[dict] = []
    flight_rows: list[dict] = []

    for person in staff:
        if not isinstance(person, dict) or not is_flying_crewmember(person):
            continue

        user = person.get("user") if isinstance(person.get("user"), dict) else {}
        entries = person.get("entries") if isinstance(person.get("entries"), list) else []
        flights = person.get("flights") if isinstance(person.get("flights"), list) else []

        internal_id = user.get("internalId")
        name = build_full_name(user)

        entry_types = sorted({str(e.get("type")) for e in entries if isinstance(e, dict) and e.get("type")})
        has_a = "A" in entry_types
        has_sic = "SIC" in entry_types
        has_p = "P" in entry_types

        matched_flights: list[tuple[dict, dict]] = []
        for flight in flights:
            if not isinstance(flight, dict):
                continue
            matched_crew = _matched_crew_member(internal_id, flight)
            if matched_crew:
                matched_flights.append((flight, matched_crew))

        people_rows.append(
            {
                "internal_id": internal_id,
                "name": name,
                "trigram": user.get("trigram"),
                "personnel_number": user.get("personnelNumber"),
                "email": user.get("email"),
                "status": user.get("status"),
                "role": infer_role_from_flights(internal_id, flights),
                "entry_types": ", ".join(entry_types),
                "has_A": has_a,
                "has_SIC": has_sic,
                "has_P": has_p,
                "flight_count": len(matched_flights),
            }
        )

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_type = entry.get("type")
            if entry_type not in DISPLAY_ENTRY_TYPES:
                continue

            entry_rows.append(
                {
                    "internal_id": internal_id,
                    "name": name,
                    "entry_id": entry.get("id"),
                    "entry_type": entry_type,
                    "start_utc": ms_to_iso_utc(entry.get("from")),
                    "end_utc": ms_to_iso_utc(entry.get("to")),
                    "counts_as_duty_time": entry.get("countsAsDutyTime"),
                    "begins_duty_period": entry.get("beginsDutyPeriod"),
                    "ends_duty_period": entry.get("endsDutyPeriod"),
                    "from_airport": safe_get(entry, "fromAirport", "icao"),
                    "to_airport": safe_get(entry, "toAirport", "icao"),
                    "notes": entry.get("notes"),
                }
            )

        for flight, matched_crew in matched_flights:
            flight_rows.append(
                {
                    "internal_id": internal_id,
                    "name": name,
                    "flight_id": flight.get("id"),
                    "quote_id": flight.get("quoteId"),
                    "booking_identifier": flight.get("bookingIdentifier"),
                    "flight_type": flight.get("type"),
                    "aircraft_category": safe_get(flight, "aircraft", "category"),
                    "registration": safe_get(flight, "aircraft", "registration"),
                    "from_airport": safe_get(flight, "fromAirport", "icao"),
                    "to_airport": safe_get(flight, "toAirport", "icao"),
                    "etd_utc": ms_to_iso_utc(flight.get("etd")),
                    "eta_utc": ms_to_iso_utc(flight.get("eta")),
                    "block_off_est_utc": ms_to_iso_utc(flight.get("blockOffEst")),
                    "block_on_est_utc": ms_to_iso_utc(flight.get("blockOnEst")),
                    "workflow": safe_get(flight, "workflow", "code"),
                    "workflow_name": safe_get(flight, "workflow", "name"),
                    "account_name": safe_get(flight, "account", "name"),
                    "pax_number": flight.get("paxNumber"),
                    "flight_status": flight.get("status"),
                    "post_flight_closed": flight.get("postFlightClosed"),
                    "crew_role": matched_crew.get("role"),
                    "pilot_takeoff": matched_crew.get("pilotTakeoff"),
                    "pilot_landing": matched_crew.get("pilotLanding"),
                    "takeoffs": matched_crew.get("takeoffs"),
                    "landings": matched_crew.get("landings"),
                }
            )

    people_df = pd.DataFrame(people_rows, columns=PEOPLE_COLUMNS)
    entries_df = pd.DataFrame(entry_rows, columns=ENTRIES_COLUMNS)
    flights_df = pd.DataFrame(flight_rows, columns=FLIGHTS_COLUMNS)

    if not people_df.empty:
        people_df = people_df.sort_values(["role", "name"], na_position="last").reset_index(drop=True)
    if not entries_df.empty:
        entries_df = entries_df.sort_values(["name", "start_utc"], na_position="last").reset_index(drop=True)
    if not flights_df.empty:
        flights_df = flights_df.sort_values(["name", "etd_utc"], na_position="last").reset_index(drop=True)

    return people_df, entries_df, flights_df


def build_daily_schedule(entries_df: pd.DataFrame) -> pd.DataFrame:
    if entries_df.empty:
        return pd.DataFrame()

    schedule = entries_df.copy()
    schedule["date"] = pd.to_datetime(schedule["start_utc"], errors="coerce", utc=True).dt.date
    schedule = schedule.dropna(subset=["date"])

    if schedule.empty:
        return pd.DataFrame()

    schedule["priority"] = schedule["entry_type"].map(SCHEDULE_PRIORITY).fillna(999)
    schedule = (
        schedule.sort_values(["name", "date", "priority"])  # keep top priority per day
        .drop_duplicates(subset=["name", "date"], keep="first")
    )

    matrix = (
        schedule.pivot(index="name", columns="date", values="entry_type")
        .fillna("")
        .sort_index()
        .sort_index(axis=1)
    )

    return matrix.reset_index()


st.title("Roster Pull Explorer")
st.header("Fl3xx Crew Roster Normalizer")
st.caption("Upload a raw FL3XX roster payload and normalize it into crew-focused views.")

uploaded_file = st.file_uploader("Upload roster export (.json or .txt)", type=["json", "txt"])

if uploaded_file is None:
    st.info("Upload a roster file to begin.")
    st.stop()

try:
    raw_text = uploaded_file.getvalue().decode("utf-8")
except UnicodeDecodeError:
    st.error("Could not decode the uploaded file as UTF-8.")
    st.stop()

try:
    people_df, entries_df, flights_df = parse_fl3xx_roster(raw_text)
except json.JSONDecodeError:
    st.error("Invalid JSON in uploaded file. Please upload a valid FL3XX roster payload.")
    st.stop()
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not parse roster payload: {exc}")
    st.stop()

schedule_df = build_daily_schedule(entries_df)

metric_cols = st.columns(3)
metric_cols[0].metric("Included crewmembers", len(people_df))
metric_cols[1].metric("Kept entries", len(entries_df))
metric_cols[2].metric("Kept flights", len(flights_df))

st.subheader("Crew Summary")
st.dataframe(people_df, use_container_width=True)
st.download_button(
    "Download crew summary CSV",
    data=people_df.to_csv(index=False).encode("utf-8"),
    file_name="fl3xx_crew_summary.csv",
    mime="text/csv",
)

st.subheader("Daily Schedule")
st.dataframe(schedule_df, use_container_width=True)
st.download_button(
    "Download daily schedule CSV",
    data=schedule_df.to_csv(index=False).encode("utf-8"),
    file_name="fl3xx_daily_schedule.csv",
    mime="text/csv",
)

st.subheader("Crew Entries")
st.dataframe(entries_df, use_container_width=True)

st.subheader("Crew Flights")
st.dataframe(flights_df, use_container_width=True)
st.download_button(
    "Download crew flights CSV",
    data=flights_df.to_csv(index=False).encode("utf-8"),
    file_name="fl3xx_crew_flights.csv",
    mime="text/csv",
)
