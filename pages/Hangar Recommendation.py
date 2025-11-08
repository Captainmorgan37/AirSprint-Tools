import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from zoneinfo_compat import ZoneInfo

from fl3xx_api import fetch_flights
from flight_leg_utils import build_fl3xx_api_config, normalize_fl3xx_payload, filter_out_subcharter_rows
from taf_utils import get_taf_reports

# ============================================================
# Page Configuration
# ============================================================
st.set_page_config(page_title="Hangar Recommendation", layout="wide")
MOUNTAIN_TZ = ZoneInfo("America/Edmonton")

st.title("🏠 Hangar Recommendation Tool")

# ============================================================
# Utility Functions
# ============================================================

def _default_date_range(now: datetime | None = None) -> tuple[date, date]:
    now_local = (now or datetime.now(tz=MOUNTAIN_TZ)).astimezone(MOUNTAIN_TZ)
    start_date = now_local.date()
    end_date = start_date + timedelta(days=1)
    return start_date, end_date


def _ensure_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _format_local(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.astimezone(MOUNTAIN_TZ).strftime("%a %b %d · %H:%M MT")


def _parse_temp_from_taf(taf_segments):
    temps = []
    for seg in taf_segments:
        details = seg.get("details", [])
        for label, val in details:
            if "Temp" in label or label.startswith("Temperature"):
                try:
                    temps.append(float(val))
                except Exception:
                    continue
    return min(temps) if temps else None


def _parse_weather_codes(taf_segments):
    wx_codes = []
    for seg in taf_segments:
        details = seg.get("details", [])
        for label, val in details:
            if label == "Weather":
                wx_codes.extend(val.split(", "))
    return wx_codes


# ============================================================
# Data Loading
# ============================================================

start_date, end_date = _default_date_range()
settings = st.secrets.get("fl3xx", {})
config = build_fl3xx_api_config(settings)

with st.spinner("Fetching flight data..."):
    flights, meta = fetch_flights(config, from_date=start_date, to_date=end_date + timedelta(days=1))
    normalized, _ = normalize_fl3xx_payload({"items": flights})
    filtered, _ = filter_out_subcharter_rows(normalized)
    df = pd.DataFrame(filtered)

if df.empty:
    st.warning("No flights found for today or tomorrow.")
    st.stop()

# ============================================================
# Identify Overnight Stays
# ============================================================

overnight_rows = []
for tail, group in df.groupby("aircraftRegistration"):
    group = group.sort_values("arrival_time_utc")
    today_arr = group[group["arrival_time_utc"].dt.date == start_date]
    tomorrow_dep = group[group["departure_time_utc"].dt.date == end_date]
    if not today_arr.empty and not tomorrow_dep.empty:
        last_arr = today_arr.tail(1).iloc[0]
        first_dep = tomorrow_dep.head(1).iloc[0]
        overnight_rows.append({
            "tail": tail,
            "arrival_airport": last_arr["arrival_airport"],
            "arr_utc": _ensure_utc(last_arr["arrival_time_utc"]),
            "dep_utc": _ensure_utc(first_dep["departure_time_utc"])
        })

if not overnight_rows:
    st.info("No overnight pairs found between today and tomorrow.")
    st.stop()

icao_list = tuple({row["arrival_airport"] for row in overnight_rows})
with st.spinner("Loading TAF forecasts..."):
    taf_reports = get_taf_reports(icao_list)

# ============================================================
# Hangar Logic
# ============================================================

def evaluate_hangar_need(taf_data: list[dict]) -> list[str]:
    reasons = []
    if not taf_data:
        return ["No TAF data available"]

    segments = taf_data[0].get("forecast", [])
    temp_min = _parse_temp_from_taf(segments)
    wx_codes = _parse_weather_codes(segments)

    # Frost risk logic
    if temp_min is not None and temp_min < 0:
        reasons.append("Temperature below freezing — frost risk")
    if temp_min is not None and temp_min <= -20:
        reasons.append("Below -20°C — hangar required")

    # Freezing precipitation
    if any(code.startswith("FZ") for code in wx_codes):
        reasons.append("Freezing precipitation in forecast")

    # Hail or severe storm risk indicators
    if any(code in wx_codes for code in ["TS", "GR", "GS"]):
        reasons.append("Thunderstorms or hail risk")

    return reasons


# ============================================================
# Display Results
# ============================================================

for entry in overnight_rows:
    tail = entry["tail"]
    airport = entry["arrival_airport"]
    taf_data = taf_reports.get(airport, [])
    reasons = evaluate_hangar_need(taf_data)

    st.markdown(f"### ✈️ {tail} – {airport}")
    st.write(f"Arrives: {_format_local(entry['arr_utc'])}  →  Departs: {_format_local(entry['dep_utc'])}")

    if any(r for r in reasons if "required" in r or "freezing" in r or "risk" in r):
        st.markdown("<span style='color:#22c55e;font-weight:600'>✅ Hangar Recommended</span>", unsafe_allow_html=True)
    else:
        st.markdown("<span style='color:#60a5fa;font-weight:600'>☀️ No Hangar Needed</span>", unsafe_allow_html=True)

    for r in reasons:
        st.markdown(f"• {r}")

    st.markdown("<hr style='opacity:0.3'>", unsafe_allow_html=True)

st.success("Evaluation complete.")
