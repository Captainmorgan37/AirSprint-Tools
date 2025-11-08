import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from zoneinfo_compat import ZoneInfo

from fl3xx_api import fetch_flights
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
)
from Home import configure_page, get_secret, password_gate, render_sidebar
from taf_utils import get_taf_reports

# ============================================================
# Page Configuration
# ============================================================
configure_page(page_title="Hangar Recommendation")
password_gate()
render_sidebar()
MOUNTAIN_TZ = ZoneInfo("America/Edmonton")
TAIL_DISPLAY_ORDER: tuple[str, ...] = (
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
    "ADD EMB WEST",
    "ADD EMB EAST",
    "ADD CJ2+ WEST",
    "ADD CJ2+ EAST",
    "ADD CJ3+ WEST",
    "ADD CJ3+ EAST",
)
TAIL_INDEX = {tail: idx for idx, tail in enumerate(TAIL_DISPLAY_ORDER)}

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
fl3xx_settings = get_secret("fl3xx_api")

try:
    config = build_fl3xx_api_config(fl3xx_settings)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except Exception as exc:  # pragma: no cover - defensive
    st.error(f"Error loading FL3XX credentials: {exc}")
    st.stop()

with st.spinner("Fetching flight data..."):
    flights, meta = fetch_flights(config, from_date=start_date, to_date=end_date + timedelta(days=1))
    normalized, _ = normalize_fl3xx_payload({"items": flights})
    filtered, _ = filter_out_subcharter_rows(normalized)
df = pd.DataFrame(filtered)

if df.empty:
    st.warning("No flights found for today or tomorrow.")
    st.stop()


def _pick_column(dataframe: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in dataframe.columns:
            return column
    return None


tail_column = _pick_column(
    df,
    (
        "tail",
        "aircraftRegistration",
        "aircraft",
        "registrationNumber",
        "registration",
    ),
)
arrive_column = _pick_column(
    df,
    (
        "arrival_time_utc",
        "arrivalTimeUtc",
        "arrivalUtc",
        "arrival_time",
        "arrivalTime",
    ),
)
depart_column = _pick_column(
    df,
    (
        "departure_time_utc",
        "departureTimeUtc",
        "blockOffTimeUtc",
        "dep_time",
        "departureTime",
    ),
)
airport_column = _pick_column(
    df,
    (
        "arrival_airport",
        "arrivalAirport",
        "airportTo",
    ),
)

missing_columns = [
    name
    for name, column in (
        ("tail registration", tail_column),
        ("arrival time", arrive_column),
        ("departure time", depart_column),
        ("arrival airport", airport_column),
    )
    if column is None
]

if missing_columns:
    st.error(
        "Unable to determine required flight details: "
        + ", ".join(missing_columns)
        + "."
    )
    st.stop()

for column in (arrive_column, depart_column):
    df[column] = pd.to_datetime(df[column], utc=True, errors="coerce")

df["arrival_local"] = df[arrive_column].dt.tz_convert(MOUNTAIN_TZ)
df["departure_local"] = df[depart_column].dt.tz_convert(MOUNTAIN_TZ)
df["arrival_local_date"] = df["arrival_local"].dt.date
df["departure_local_date"] = df["departure_local"].dt.date

# ============================================================
# Identify Overnight Stays
# ============================================================

overnight_rows = []
for tail, group in df.groupby(tail_column):
    tail = str(tail or "").strip()
    if not tail:
        continue
    group = group.sort_values(arrive_column)
    today_arr = group[group["arrival_local_date"] == start_date]
    tomorrow_dep = group[group["departure_local_date"] == end_date]
    if not today_arr.empty and not tomorrow_dep.empty:
        last_arr = today_arr.tail(1).iloc[0]
        first_dep = tomorrow_dep.head(1).iloc[0]
        overnight_rows.append({
            "tail": tail,
            "arrival_airport": last_arr[airport_column],
            "arr_utc": _ensure_utc(last_arr[arrive_column]),
            "dep_utc": _ensure_utc(first_dep[depart_column]),
            "arr_local": last_arr["arrival_local"],
            "dep_local": first_dep["departure_local"],
        })

if not overnight_rows:
    st.info("No overnight pairs found between today and tomorrow.")
    st.stop()

def _tail_sort_key(tail: str) -> tuple[int, str]:
    return (TAIL_INDEX.get(tail, len(TAIL_INDEX)), tail)

overnight_rows.sort(key=lambda row: _tail_sort_key(row["tail"]))

icao_list = tuple({row["arrival_airport"] for row in overnight_rows})
with st.spinner("Loading TAF forecasts..."):
    taf_reports = get_taf_reports(icao_list)

# ============================================================
# Hangar Logic
# ============================================================

def evaluate_hangar_need(taf_data: list[dict]) -> dict[str, list[str] | bool | None]:
    assessment: dict[str, list[str] | bool | None] = {
        "needs_hangar": False,
        "triggers": [],
        "notes": [],
        "min_temp": None,
    }

    if not taf_data:
        assessment["notes"] = [
            "No TAF data available — unable to evaluate local weather risks.",
        ]
        return assessment

    segments = taf_data[0].get("forecast", [])
    temp_min = _parse_temp_from_taf(segments)
    wx_codes = _parse_weather_codes(segments)
    assessment["min_temp"] = temp_min

    if temp_min is None:
        assessment["notes"].append("Forecast minimum temperature unavailable in TAF.")
    else:
        assessment["notes"].append(f"Forecast minimum temperature: {temp_min:.0f}°C")
        if temp_min <= -20:
            assessment["triggers"].append("Temperature at or below -20°C — hangar required")
        elif temp_min < 0:
            assessment["triggers"].append("Temperature below freezing — frost risk")

    if wx_codes:
        assessment["notes"].append(
            "Weather codes in primary forecast window: " + ", ".join(sorted(set(wx_codes)))
        )
    else:
        assessment["notes"].append("No significant weather codes in the primary TAF segment.")

    if any(code.startswith("FZ") for code in wx_codes):
        assessment["triggers"].append("Freezing precipitation expected (FZ prefix codes present)")

    if any(code in wx_codes for code in ["TS", "GR", "GS"]):
        assessment["triggers"].append("Thunderstorm or hail risk indicated in TAF")

    assessment["needs_hangar"] = bool(assessment["triggers"])
    if not assessment["triggers"]:
        assessment["notes"].append("No hangar-triggering conditions detected in current forecast.")

    return assessment


# ============================================================
# Display Results
# ============================================================

for entry in overnight_rows:
    tail = entry["tail"]
    airport = entry["arrival_airport"]
    taf_data = taf_reports.get(airport, [])
    assessment = evaluate_hangar_need(taf_data)
    triggers = list(assessment.get("triggers", []))
    notes = list(assessment.get("notes", []))

    st.markdown(f"### ✈️ {tail} – {airport}")
    st.write(
        "Arrives: "
        f"{_format_local(entry['arr_utc'])}  →  "
        f"Departs: {_format_local(entry['dep_utc'])}"
    )

    if bool(assessment.get("needs_hangar")):
        st.markdown(
            "<span style='color:#22c55e;font-weight:600'>✅ Hangar Recommended</span>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<span style='color:#60a5fa;font-weight:600'>☀️ No Hangar Needed</span>",
            unsafe_allow_html=True,
        )

    if triggers:
        st.markdown("**Triggers:**")
        for item in triggers:
            st.markdown(f"• {item}")

    if notes:
        st.markdown("**Forecast Details:**")
        for item in notes:
            st.markdown(f"• {item}")

    st.markdown("<hr style='opacity:0.3'>", unsafe_allow_html=True)

st.success("Evaluation complete.")
