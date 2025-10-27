
# Use the same config you already build in Crew Duty Clearance Monitor.py
config = Fl3xxApiConfig(
[fl3xx_api]
# base_url is optional—the client will default to the FL3XX flights endpoint.
base_url = "https://app.fl3xx.us/api/external/flight/flights"
api_token = "ts__ddFWrYpN-N3Hvt9Rp08LJ5nk9ODl"
auth_header_name = "X-Auth-Token"


import streamlit as st
from datetime import datetime, timedelta, date
from zoneinfo_compat import ZoneInfo

from fl3xx_api import (
    Fl3xxApiConfig,
    fetch_preflight,
    parse_preflight_payload,
)
from flight_leg_utils import (
    get_todays_sorted_legs_by_tail,
)
from duty_clearance import (
    _get_report_time_local,
)

# --- Page setup ---
st.set_page_config(page_title="DEBUG PREFLIGHT / CHECKINS", layout="wide")
st.title("DEBUG: Preflight / Checkins / Legs by Tail")

# --- Build config from secrets the same way your main page does ---
# adjust the secrets paths if yours differ
fl3xx_secrets = st.secrets["fl3xx"]

config = Fl3xxApiConfig(
    base_url=fl3xx_secrets["https://app.fl3xx.us/api/external/flight/flights"],
    api_token=fl3xx_secrets["ts__ddFWrYpN-N3Hvt9Rp08LJ5nk9ODl"],
    auth_header_name=fl3xx_secrets["X-Auth-Token"],
)

# --- Pick a target date (default = tomorrow in America/Edmonton) ---
MOUNTAIN_TZ = ZoneInfo("America/Edmonton")
now_mt = datetime.now(tz=MOUNTAIN_TZ)
default_target_date = (now_mt.date() + timedelta(days=1))

target_date = st.date_input(
    "Target duty date to inspect",
    value=default_target_date,
    help="Usually tomorrow. This is the date whose crews should appear on the clearance dashboard.",
)

st.write("Selected target_date:", target_date)

# --- 1) Show LEGS BY TAIL so we can see what flights we're even considering ---
st.header("Step 1: Legs by Tail")
legs_by_tail = get_todays_sorted_legs_by_tail(config, target_date)

if not legs_by_tail:
    st.warning(
        "get_todays_sorted_legs_by_tail() returned no legs. "
        "That means: either no flights, or no tails assigned, or parsing filtered them all out."
    )
else:
    for tail, legs in legs_by_tail.items():
        st.subheader(f"Tail {tail}")
        # just show first 2 legs for brevity
        st.write(legs[:2])

# --- Get a flightId to debug ---
st.markdown("---")
st.header("Step 2: Pick a flight to inspect preflight data")

# try to auto-suggest a flight ID from the first tail
some_flight_id = None
for _tail, _legs in legs_by_tail.items():
    if _legs:
        # _legs entries should have "flightId"
        fid = _legs[0].get("flightId")
        if fid:
            some_flight_id = fid
            break

flight_id_to_debug = st.text_input(
    "Flight ID to debug",
    value=str(some_flight_id) if some_flight_id else "",
    help="This should be a FL3XX flightId for one of the flights on the selected date.",
)

do_run = st.button("Fetch & Inspect Preflight for this Flight ID")

if do_run:
    if not flight_id_to_debug.strip().isdigit():
        st.error("Please enter a numeric flightId.")
    else:
        flight_id_int = int(flight_id_to_debug.strip())

        # --- 2) Pull raw preflight payload from FL3XX ---
        st.subheader("Raw preflight payload")
        preflight_payload = fetch_preflight(config, flight_id_int)
        st.write(preflight_payload)

        # --- 3) Parse it using our existing parser ---
        st.subheader("Parsed preflight status object")
        parsed_status = parse_preflight_payload(preflight_payload)
        st.write(parsed_status)

        # --- 4) Dump the crew_checkins that parsed_status thinks it found ---
        st.subheader("Crew checkins from parsed_status")
        if not parsed_status.crew_checkins:
            st.warning(
                "No crew_checkins parsed. "
                "If the raw preflight payload clearly has checkin times / user IDs, "
                "then our parse_preflight_payload() isn't looking in the right place."
            )
        else:
            for check in parsed_status.crew_checkins:
                st.write({
                    "user_id": check.user_id,
                    "pilot_role": check.pilot_role,
                    "checkin": check.checkin,
                    "checkin_actual": check.checkin_actual,
                    "checkin_default": check.checkin_default,
                })

        # --- 5) Try to compute report_local from parsed_status
        st.subheader("Derived report_local using _get_report_time_local()")
        # We need a timezone. We'll guess from the first leg of this tail+date if available;
        # fallback to Mountain.
        duty_tz = MOUNTAIN_TZ
        # try to infer dep_tz that matches this exact flight_id
        for _tail, _legs in legs_by_tail.items():
            for leg in _legs:
                if leg.get("flightId") == flight_id_int:
                    # get dep_tz if present
                    dep_tz_name = leg.get("dep_tz")
                    if dep_tz_name:
                        try:
                            duty_tz = ZoneInfo(dep_tz_name)
                        except Exception:
                            duty_tz = MOUNTAIN_TZ
                    break

        report_local = _get_report_time_local(parsed_status, duty_tz)
        st.write("duty_tz:", duty_tz)
        st.write("report_local:", report_local)

        if report_local is None:
            st.error(
                "report_local came back None.\n"
                "That means _get_report_time_local() could not find usable epoch timestamps.\n"
                "We'll need to adjust parse_preflight_payload() or timestamp conversion."
            )
        else:
            st.success(
                "We successfully derived a report_local, which means the dashboard "
                "should NOT have filtered this crew out once we wire in this value."
            )
