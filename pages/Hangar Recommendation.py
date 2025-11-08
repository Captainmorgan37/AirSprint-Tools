from fl3xx_api import Fl3xxApiConfig, fetch_flights
from taf_utils import get_taf_reports
import streamlit as st
from datetime import datetime, timedelta, timezone
from zoneinfo_compat import ZoneInfo

MOUNTAIN_TZ = ZoneInfo("America/Edmonton")

st.title("🏠 Hangar Recommendation Tool")

# 1️⃣ Fetch flights for today + tomorrow
config = build_fl3xx_api_config(st.secrets["fl3xx"])
flights, meta = fetch_flights(config, from_date=today, to_date=tomorrow_plus_one)

# Normalize flights → Pandas DF
df = normalize_fl3xx_payload({"items": flights})[0]

# 2️⃣ For each tail, find overnight stop
overnights = []
for tail, group in df.groupby("aircraftRegistration"):
    group_sorted = group.sort_values("arrival_time_utc")
    last_arr = group_sorted[group_sorted["arrival_time_utc"].dt.date == today].tail(1)
    first_dep = group_sorted[group_sorted["departure_time_utc"].dt.date == tomorrow].head(1)
    if not last_arr.empty and not first_dep.empty:
        overnights.append({
            "tail": tail,
            "icao": last_arr["arrival_airport"].iloc[0],
            "arr_utc": last_arr["arrival_time_utc"].iloc[0],
            "dep_utc": first_dep["departure_time_utc"].iloc[0]
        })

# 3️⃣ Pull TAF for each overnight airport
taf_reports = get_taf_reports(tuple(set(o["icao"] for o in overnights)))

# 4️⃣ Evaluate conditions per your hangar rules
def evaluate_hangar_need(taf_report):
    temp_min, dew, wind, wx = extract_temp_dew_wind(taf_report)
    reasons = []
    if temp_min <= -20: reasons.append("Below -20°C")
    if temp_min <= -10 and fleet == "CJ" and first_leg_client_occupied: ...
    if temp_min <= -15 and fleet in ("Legacy","Praetor"): ...
    if temp_min < 0 and dew - temp_min <= 5 and wind < 10: reasons.append("Frost risk")
    if any(code in wx for code in ("FZRA", "FZDZ", "FZFG")): reasons.append("Freezing precipitation")

    return reasons

# 5️⃣ Display cards in Streamlit
for o in overnights:
    taf = taf_reports.get(o["icao"])
    reasons = evaluate_hangar_need(taf)
    st.subheader(f"{o['tail']} – {o['icao']}")
    if reasons:
        st.markdown("✅ **Hangar Recommended**")
        st.write("\n".join(f"- {r}" for r in reasons))
    else:
        st.markdown("☀️ **No hangar needed tonight**")
