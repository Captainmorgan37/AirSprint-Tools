import streamlit as st
from fl3xx_api import fetch_preflight, parse_preflight_payload, Fl3xxApiConfig
from duty_clearance import _get_report_time_local  # if not exported, you can copy logic inline
from zoneinfo_compat import ZoneInfo

# Use the same config you already build in Crew Duty Clearance Monitor.py
config = Fl3xxApiConfig(
[fl3xx_api]
# base_url is optional—the client will default to the FL3XX flights endpoint.
base_url = "https://app.fl3xx.us/api/external/flight/flights"
api_token = "ts__ddFWrYpN-N3Hvt9Rp08LJ5nk9ODl"
auth_header_name = "X-Auth-Token"
)

flight_id_to_debug = 1023229 

preflight_payload = fetch_preflight(config, flight_id_to_debug)
st.write("RAW PREFLIGHT PAYLOAD:")
st.write(preflight_payload)

parsed = parse_preflight_payload(preflight_payload)
st.write("PARSED PREFLIGHT STATUS OBJECT:")
st.write(parsed)

st.write("CREW CHECKINS FROM PARSED:")
for c in parsed.crew_checkins:
    st.write({
        "user_id": c.user_id,
        "pilot_role": c.pilot_role,
        "checkin": c.checkin,
        "checkin_actual": c.checkin_actual,
        "checkin_default": c.checkin_default,
    })
