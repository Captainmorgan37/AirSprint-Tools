from Home import password_gate
password_gate()
import streamlit as st
import pandas as pd
import requests
import json
import re
import html
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union


from fl3xx_client import fetch_flights, compute_fetch_dates
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    normalize_fl3xx_payload,
)

# ----- CONFIG -----
FAA_CLIENT_ID = st.secrets["FAA_CLIENT_ID"]
FAA_CLIENT_SECRET = st.secrets["FAA_CLIENT_SECRET"]
KEYWORDS = ["CLOSED", "CLSD"]  # Add any more keywords here
HIDE_KEYWORDS = ["crane", "RUSSIAN", "CONGO", "OBST RIG", "CANCELLED", "CANCELED", 
                 "SAFETY AREA NOT STD", "GRASS CUTTING", "OBST TOWER", "SFC MARKINGS NOT STD"]

CATEGORY_COLORS = {
    "Runway": "#ff4d4d",
    "PPR": "#ffcc00",
    "Airspace/Navigation": "#4da6ff",
    "Airport Services": "#ffa64d",
    "Other": "#ccc"
}

st.set_page_config(page_title="CFPS/FAA NOTAM Viewer", layout="wide")
st.title("CFPS & FAA NOTAM Viewer")

# ----- RUNWAYS DATA -----
@st.cache_data
def load_runway_data():
    df = pd.read_csv("runways.csv")
    return df

runways_df = load_runway_data()

# ----- FUNCTIONS -----
def format_iso_timestamp(value):
    if value in (None, "", []):
        return "N/A", None

    def _format_dt(dt_obj):
        dt_naive = dt_obj.replace(tzinfo=None)
        return dt_naive.strftime("%b %d %Y, %H:%MZ"), dt_naive

    # Handle numeric timestamps (seconds or milliseconds since epoch)
    if isinstance(value, (int, float)):
        try:
            seconds = float(value)
            if seconds > 1e12:  # Likely milliseconds
                seconds /= 1000.0
            dt = datetime.utcfromtimestamp(seconds)
            return _format_dt(dt)
        except (OverflowError, ValueError):
            return str(value), None

    value_str = str(value).strip()
    if not value_str:
        return "N/A", None

    # Handle purely numeric strings (e.g., epoch seconds/milliseconds)
    if value_str.isdigit():
        try:
            seconds = int(value_str)
            if len(value_str) > 10:
                seconds /= 1000.0
            dt = datetime.utcfromtimestamp(seconds)
            return _format_dt(dt)
        except (OverflowError, ValueError):
            pass

    try:
        dt = datetime.fromisoformat(value_str.replace("Z", "+00:00"))
        return _format_dt(dt)
    except ValueError:
        return value_str, None


def _first_non_empty(data_dict: dict, *keys):
    for key in keys:
        if not key:
            continue
        if key in data_dict:
            value = data_dict.get(key)
            if value not in (None, "", []):
                return value
    return None


def _simplify_detail_value(value):
    """Return a display-friendly version of a detail value."""

    if isinstance(value, dict):
        # Prefer representative string fields over raw JSON output.
        preferred_keys = ("repr", "text", "raw", "string")
        numeric_keys = ("value", "visibility", "minValue", "maxValue")

        for key in preferred_keys:
            if key in value:
                simplified = _simplify_detail_value(value[key])
                if simplified not in (None, "", []):
                    return simplified

        for key in numeric_keys:
            if key in value:
                simplified = _simplify_detail_value(value[key])
                if simplified not in (None, "", []):
                    return simplified

        return json.dumps(value)

    if isinstance(value, (list, tuple)):
        simplified_items = []
        for item in value:
            simplified = _simplify_detail_value(item)
            if simplified in (None, "", []):
                continue
            simplified_items.append(str(simplified))
        if not simplified_items:
            return None
        return ", ".join(simplified_items)

    return value


def build_detail_list(data_dict, field_map):
    if not isinstance(data_dict, dict):
        return []
    details = []
    for key_spec, label in field_map:
        if isinstance(key_spec, (tuple, list)):
            value = None
            for key in key_spec:
                value = data_dict.get(key)
                if value not in (None, "", []):
                    break
            else:
                continue
        else:
            if key_spec not in data_dict:
                continue
            value = data_dict.get(key_spec)
            if value in (None, "", []):
                continue

        simplified_value = _simplify_detail_value(value)
        if simplified_value in (None, "", []):
            continue
        details.append((label, simplified_value))
    return details


METAR_DETAIL_FIELDS = [
    (("temp", "temperature", "temperature_c"), "Temperature (°C)"),
    (("dewpoint", "dew_point", "dewpoint_c"), "Dewpoint (°C)"),
    (("windDir", "wind_direction", "wind_direction_degrees"), "Wind Dir (°)"),
    (("windSpeed", "wind_speed", "wind_speed_kt"), "Wind Speed (kt)"),
    (("windGust", "wind_gust", "wind_gust_kt"), "Wind Gust (kt)"),
    (("visibility", "visibility_statute", "visibility_sm", "visibility_mi"), "Visibility"),
    (("altimeter", "altimeter_in_hg", "altim_in_hg"), "Altimeter (inHg)"),
    (("ceiling", "ceiling_ft_agl"), "Ceiling (ft)"),
    (("wxString", "weather", "wx", "wx_string"), "Weather"),
]

METAR_SUMMARY_LABELS = {
    "Temperature (°C)",
    "Dewpoint (°C)",
    "Wind Dir (°)",
    "Wind Speed (kt)",
    "Wind Gust (kt)",
    "Visibility",
    "Altimeter (inHg)",
    "Ceiling (ft)",
    "Weather",
}


_METAR_WIND_REGEX = re.compile(
    r"\b(?P<dir>\d{3}|VRB)(?P<speed>\d{2,3})(?:G(?P<gust>\d{2,3}))?KT\b"
)
_METAR_VIS_REGEX = re.compile(
    r"\b(?P<prefix>P|M)?(?:"
    r"(?P<whole>\d+)\s(?P<fraction>\d+/\d+)"
    r"|(?P<only_fraction>\d+/\d+)"
    r"|(?P<only_number>\d+)"
    r")SM\b"
)
_METAR_TEMP_DEW_REGEX = re.compile(r"\b(?P<temp>M?\d{2})/(?P<dew>M?\d{2})\b")


def _format_numeric(value, decimals: int | None = None) -> str | None:
    if value in (None, "", [], "M"):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None

    if decimals is None:
        if abs(num - round(num)) < 1e-6:
            return str(int(round(num)))
        return f"{num:.1f}"
    return f"{num:.{decimals}f}"


def _format_wind_direction(value) -> str | None:
    if value in (None, "", [], "M"):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        if text.upper() == "VRB":
            return "variable"
        return text
    return f"{int(round(num))}°"


def _format_cloud_layers(metar_data: dict) -> str | None:
    layer_sources = (
        "cloudLayers",
        "clouds",
        "cloudList",
        "skyCondition",
    )
    for key in layer_sources:
        layers = metar_data.get(key)
        if not isinstance(layers, list):
            continue
        layer_parts = []
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            cover = _first_non_empty(layer, "cover", "coverage", "amount")
            base_val = _first_non_empty(
                layer,
                "base",
                "base_feet",
                "baseFeet",
                "base_feet_agl",
                "base_agl",
                "baseHeightFt",
                "baseHeight",
                "base_ft_agl",
                "height",
                "altitude",
            )
            if isinstance(base_val, dict):
                base_val = _first_non_empty(base_val, "ft", "feet", "value")
            base_text = _format_numeric(base_val)
            if cover and base_text:
                layer_parts.append(f"{cover} {base_text} ft")
            elif cover:
                layer_parts.append(str(cover))
            elif base_text:
                layer_parts.append(f"{base_text} ft")
        if layer_parts:
            return ", ".join(layer_parts)
    return None


def _format_weather(metar_data: dict) -> str | None:
    weather = _first_non_empty(
        metar_data,
        "wxString",
        "weather",
        "wx",
        "wx_string",
    )
    if weather is None:
        weather = metar_data.get("presentWeather")
    if isinstance(weather, list):
        parts = []
        for item in weather:
            if isinstance(item, dict):
                descriptor = " ".join(
                    str(item.get(key)).strip()
                    for key in ("intensity", "descriptor", "phenomena", "value")
                    if item.get(key)
                ).strip()
                if descriptor:
                    parts.append(descriptor)
            elif item not in (None, ""):
                parts.append(str(item))
        weather = ", ".join(parts)
    if isinstance(weather, str):
        weather = weather.strip()
    return weather or None


def _parse_fraction(value: str) -> float | None:
    try:
        numerator, denominator = value.split("/", 1)
        return float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError):
        return None


def _parse_visibility_from_match(match: re.Match) -> float | str | None:
    prefix = match.group("prefix") or ""
    value_text = None
    if match.group("whole") and match.group("fraction"):
        value_text = f"{match.group('whole')} {match.group('fraction')}"
        fraction_value = _parse_fraction(match.group("fraction"))
        if fraction_value is None:
            numeric_value = None
        else:
            numeric_value = float(match.group("whole")) + fraction_value
    elif match.group("only_fraction"):
        value_text = match.group("only_fraction")
        numeric_value = _parse_fraction(value_text)
    else:
        value_text = match.group("only_number")
        numeric_value = float(value_text) if value_text is not None else None

    if value_text is None:
        return None

    if prefix:
        return f"{prefix}{value_text}".strip()
    if numeric_value is not None:
        return numeric_value
    return value_text


def _try_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_visibility_value(value) -> float | None:
    if value in (None, "", [], "M"):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, dict):
        for key in ("value", "visibility", "minValue", "maxValue"):
            if key in value:
                nested_val = _parse_visibility_value(value[key])
                if nested_val is not None:
                    return nested_val
        for key in ("repr", "text", "raw", "string"):
            if key in value:
                nested_val = _parse_visibility_value(value[key])
                if nested_val is not None:
                    return nested_val
        return None

    if isinstance(value, (list, tuple)):
        for item in value:
            nested_val = _parse_visibility_value(item)
            if nested_val is not None:
                return nested_val
        return None

    text = str(value).strip().upper()
    if not text:
        return None

    if text.startswith("P") or text.startswith("M"):
        text = text[1:]

    if text.endswith("SM"):
        text = text[:-2]
    text = text.replace("SM", "")
    text = text.strip().strip("+")
    if not text:
        return None

    parts = text.split()
    if len(parts) == 2:
        whole_val = _try_float(parts[0]) or 0.0
        frac_val = _parse_fraction(parts[1])
        if frac_val is None:
            return _try_float(text)
        return whole_val + frac_val

    if "/" in text:
        frac_val = _parse_fraction(text)
        if frac_val is not None:
            return frac_val

    return _try_float(text)


def _should_highlight_visibility(value) -> bool:
    if value in (None, ""):
        return False

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return False
        upper_value = stripped.upper()
        if "/" in upper_value and "SM" not in upper_value:
            return False

    vis_value = _parse_visibility_value(value)
    if vis_value is None:
        return False
    return vis_value <= 2.0


def _should_highlight_weather(value) -> bool:
    if value in (None, ""):
        return False
    if not isinstance(value, str):
        value = str(value)
    return "TS" in value.upper()


def _wrap_highlight(text: str) -> str:
    return f"<span style='color:#c41230;font-weight:bold'>{text}</span>"


def _format_detail_entry(label: str, value) -> str:
    value_text = "—" if value in (None, "") else str(value)
    label_lower = label.lower()
    highlight = False
    if "visibility" in label_lower and _should_highlight_visibility(value):
        highlight = True
    if "weather" in label_lower and _should_highlight_weather(value_text):
        highlight = True
    if highlight:
        value_text = _wrap_highlight(value_text)
    return f"<strong>{label}:</strong> {value_text}"


def _format_inline_detail(label: str, value) -> str:
    value_text = "—" if value in (None, "") else str(value)
    label_lower = label.lower()
    highlight = False
    if "visibility" in label_lower and _should_highlight_visibility(value):
        highlight = True
    if "weather" in label_lower and _should_highlight_weather(value_text):
        highlight = True
    text = f"{label}: {value_text}"
    return _wrap_highlight(text) if highlight else text


def _parse_signed_temperature(value: str) -> int | None:
    if not value:
        return None
    sign = -1 if value.startswith("M") else 1
    digits = value.lstrip("M")
    if not digits.isdigit():
        return None
    return sign * int(digits)


def parse_metar_raw(raw_text: str) -> dict:
    if not isinstance(raw_text, str) or not raw_text:
        return {}

    parsed: dict[str, object] = {}

    wind_match = _METAR_WIND_REGEX.search(raw_text)
    if wind_match:
        wind_dir = wind_match.group("dir")
        wind_speed = wind_match.group("speed")
        wind_gust = wind_match.group("gust")
        if wind_dir:
            parsed["windDir"] = wind_dir
        if wind_speed:
            try:
                parsed["windSpeed"] = int(wind_speed)
            except ValueError:
                parsed["windSpeed"] = wind_speed
        if wind_gust:
            try:
                parsed["windGust"] = int(wind_gust)
            except ValueError:
                parsed["windGust"] = wind_gust

    vis_match = _METAR_VIS_REGEX.search(raw_text)
    if vis_match:
        visibility_value = _parse_visibility_from_match(vis_match)
        if visibility_value is not None:
            parsed["visibility"] = visibility_value

    temp_match = _METAR_TEMP_DEW_REGEX.search(raw_text)
    if temp_match:
        temp_value = _parse_signed_temperature(temp_match.group("temp"))
        dew_value = _parse_signed_temperature(temp_match.group("dew"))
        if temp_value is not None:
            parsed["temp"] = temp_value
        if dew_value is not None:
            parsed["dewpoint"] = dew_value

    return parsed


def build_metar_summary(report_entry: dict) -> list[str]:
    metar_data = report_entry.get("metar_data") or {}
    if not isinstance(metar_data, dict):
        metar_data = {}

    raw_text = report_entry.get("raw", "")
    fallback_data = parse_metar_raw(raw_text)
    if fallback_data:
        combined_data = dict(fallback_data)
        combined_data.update(metar_data)
        metar_data = combined_data

    summary_lines: list[str] = []

    issue_display = report_entry.get("issue_time_display")
    if issue_display and issue_display != "N/A":
        summary_lines.append(f"Issued {issue_display}")

    wind_speed = _format_numeric(
        _first_non_empty(metar_data, "windSpeed", "wind_speed", "wind_speed_kt")
    )
    if wind_speed:
        wind_dir_value = _first_non_empty(
            metar_data, "windDir", "wind_direction", "wind_direction_degrees"
        )
        wind_dir = _format_wind_direction(wind_dir_value)
        gust = _format_numeric(
            _first_non_empty(metar_data, "windGust", "wind_gust", "wind_gust_kt")
        )
        if wind_dir:
            wind_line = f"Wind {wind_dir} at {wind_speed} kt"
        else:
            wind_line = f"Wind {wind_speed} kt"
        if gust:
            wind_line += f" (gusting {gust} kt)"
        summary_lines.append(wind_line)
    else:
        wind_char = _first_non_empty(
            metar_data, "windDir", "wind_direction", "wind_direction_degrees"
        )
        if isinstance(wind_char, str) and wind_char.strip().upper() == "CALM":
            summary_lines.append("Wind calm")
        else:
            wind_dir = _format_wind_direction(wind_char)
            if wind_dir:
                summary_lines.append(f"Wind {wind_dir}")

    temp = _format_numeric(
        _first_non_empty(metar_data, "temp", "temperature", "temperature_c")
    )
    dewpoint = _format_numeric(
        _first_non_empty(metar_data, "dewpoint", "dew_point", "dewpoint_c")
    )
    if temp or dewpoint:
        temp_parts = []
        if temp:
            temp_parts.append(f"Temp {temp}°C")
        if dewpoint:
            temp_parts.append(f"Dew point {dewpoint}°C")
        summary_lines.append(" / ".join(temp_parts))

    visibility = _first_non_empty(
        metar_data,
        "visibility",
        "visibility_statute",
        "visibility_sm",
        "visibility_mi",
    )
    vis_text = _format_numeric(visibility)
    if visibility and not vis_text:
        vis_text = str(visibility).strip()
    if vis_text:
        vis_line = None
        try:
            vis_numeric = float(str(visibility).strip("+"))
        except (TypeError, ValueError):
            vis_numeric = None
        if vis_numeric is not None:
            vis_line = f"Visibility {vis_text} sm"
        else:
            vis_line = f"Visibility {vis_text}"
        if _should_highlight_visibility(visibility):
            vis_line = _wrap_highlight(vis_line)
        summary_lines.append(vis_line)

    altimeter = _format_numeric(
        _first_non_empty(metar_data, "altimeter", "altimeter_in_hg", "altim_in_hg"),
        decimals=2,
    )
    if altimeter:
        summary_lines.append(f"Altimeter {altimeter} inHg")

    ceiling = _format_numeric(
        _first_non_empty(metar_data, "ceiling", "ceiling_ft_agl")
    )
    if ceiling:
        summary_lines.append(f"Ceiling {ceiling} ft")

    weather = _format_weather(metar_data)
    if weather:
        weather_line = f"Weather {weather}"
        if _should_highlight_weather(weather):
            weather_line = _wrap_highlight(weather_line)
        summary_lines.append(weather_line)

    clouds = _format_cloud_layers(metar_data)
    if clouds:
        summary_lines.append(f"Clouds {clouds}")

    return summary_lines

TAF_FORECAST_FIELDS = [
    (("changeIndicator", "change_indicator"), "Change"),
    (
        (
            "probability",
            "probabilityPercent",
            "probability_percent",
            "probability_pct",
        ),
        "Probability",
    ),
    (
        (
            "windDir",
            "windDirDegrees",
            "wind_direction",
            "wind_direction_degrees",
        ),
        "Wind Dir (°)",
    ),
    (
        (
            "windSpeed",
            "windSpeedKt",
            "windSpeedKT",
            "wind_speed",
            "wind_speed_kt",
        ),
        "Wind Speed (kt)",
    ),
    (
        (
            "windGust",
            "windGustKt",
            "windGustKT",
            "wind_gust",
            "wind_gust_kt",
        ),
        "Wind Gust (kt)",
    ),
    (
        (
            "visibility",
            "visibilitySM",
            "visibility_sm",
            "visibility_statute",
            "visibility_statute_mi",
            "visibility_mi",
        ),
        "Visibility",
    ),
    (
        (
            "vertVisibility",
            "vert_visibility",
            "verticalVisibility",
            "vertical_visibility",
            "vertical_visibility_ft",
        ),
        "Vertical Vis (ft)",
    ),
]

TAF_CHANGE_REGEX = re.compile(r"^(FM\d{6}|TEMPO|BECMG|PROB\d{2}|RMK|AMD|COR)$")

def highlight_keywords(notam_text: str):
    for kw in KEYWORDS:
        notam_text = notam_text.replace(
            kw, f"<span style='color:red;font-weight:bold'>{kw}</span>"
        )
    return notam_text

def parse_cfps_times(notam_text):
    start_match = re.search(r'\bB\)\s*(\d{10}|PERM)', notam_text)
    end_match = re.search(r'\bC\)\s*(\d{10}|PERM)', notam_text)

    def format_time(t):
        if not t:
            return 'N/A', None
        if t == 'PERM':
            return 'PERM', None
        dt = datetime.strptime(t, "%y%m%d%H%M")
        return dt.strftime("%b %d %Y, %H:%M"), dt

    start, start_dt = format_time(start_match.group(1)) if start_match else ('N/A', None)
    end, end_dt = format_time(end_match.group(1)) if end_match else ('N/A', None)
    return start, end, start_dt, end_dt

def categorize_notam(notam_text):
    text_upper = notam_text.upper()
    # Explicit PPR check (whole word only)
    if re.search(r"\bPPR\b", text_upper):
        return "PPR"
    elif any(rwy_kw in text_upper for rwy_kw in ["RWY", "RUNWAY"]):
        return "Runway"
    elif any(air_kw in text_upper for air_kw in ["SID", "STAR", "APPROACH", "AIRSPACE", "NAVIGATION", "FDC"]):
        return "Airspace/Navigation"
    elif any(ser_kw in text_upper for ser_kw in ["TOWER", "APRON", "GROUND", "SERVICE"]):
        return "Airport Services"
    else:
        return "Other"

def get_cfps_notams(icao: str):
    url = "https://plan.navcanada.ca/weather/api/alpha/"
    params = {
        "site": icao,
        "alpha": ["notam"],
        "notam_choice": "default",
        "_": "1756244240291"
    }
    query_params = []
    for key, value in params.items():
        if isinstance(value, list):
            for v in value:
                query_params.append((key, v))
        else:
            query_params.append((key, value))

    response = requests.get(url, params=query_params)
    response.raise_for_status()
    data = response.json()
    notams = []

    for n in data.get("data", []):
        if n.get("type") == "notam":
            text = n["text"]
            try:
                notam_json = json.loads(text)
                notam_text = notam_json.get("raw", text)
            except:
                notam_text = text

            if any(hide_kw.lower() in notam_text.lower() for hide_kw in HIDE_KEYWORDS):
                continue

            effective_start, effective_end, start_dt, end_dt = parse_cfps_times(notam_text)
            sort_key = start_dt if start_dt else datetime.min

            notams.append({
                "text": notam_text,
                "effectiveStart": effective_start,
                "effectiveEnd": effective_end,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "sortKey": sort_key,
                "category": categorize_notam(notam_text)
            })

    notams.sort(key=lambda x: x["sortKey"], reverse=True)
    return notams

def get_faa_notams(icao: str):
    url = "https://external-api.faa.gov/notamapi/v1/notams"
    headers = {
        "client_id": FAA_CLIENT_ID,
        "client_secret": FAA_CLIENT_SECRET
    }
    params = {
        "icaoLocation": icao.upper(),
        "responseFormat": "geoJson",
        "pageSize": 200
    }

    all_items = []
    page_cursor = None

    while True:
        if page_cursor:
            params["pageCursor"] = page_cursor

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        all_items.extend(items)
        page_cursor = data.get("nextPageCursor")
        if not page_cursor:
            break

    notams = []

    for feature in all_items:
        props = feature.get("properties", {})
        core = props.get("coreNOTAMData", {})
        notam_data = core.get("notam", {})

        notam_text = notam_data.get("text", "")
        translations = core.get("notamTranslation", [])
        simple_text = None
        for t in translations:
            if t.get("type") == "LOCAL_FORMAT":
                simple_text = t.get("simpleText")
        text_to_use = simple_text if simple_text else notam_text

        # Skip ICAO-format NOTAMs (keep only LOCAL_FORMAT / domestic)
        if not simple_text:
            continue

        if any(hide_kw.lower() in text_to_use.lower() for hide_kw in HIDE_KEYWORDS):
            continue

        effective = notam_data.get("effectiveStart", None)
        expiry = notam_data.get("effectiveEnd", None)

        start_dt = end_dt = None
        if effective == "PERM":
            effective_display = "PERM"
        elif effective:
            start_dt = datetime.fromisoformat(effective.replace("Z", ""))
            effective_display = start_dt.strftime("%b %d %Y, %H:%M")
        else:
            effective_display = "N/A"

        if expiry == "PERM":
            expiry_display = "PERM"
        elif expiry:
            end_dt = datetime.fromisoformat(expiry.replace("Z", ""))
            expiry_display = end_dt.strftime("%b %d %Y, %H:%M")
        else:
            expiry_display = "N/A"

        notams.append({
            "text": text_to_use,
            "effectiveStart": effective_display,
            "effectiveEnd": expiry_display,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "sortKey": start_dt if start_dt else datetime.min,
            "category": categorize_notam(text_to_use)
        })

    notams.sort(key=lambda x: x["sortKey"], reverse=True)
    notams = deduplicate_notams(notams)
    return notams


def _normalize_aviationweather_features(data):
    """Yield dictionaries that represent METAR/TAF reports from varied responses."""

    def _yield_from_candidate(candidate):
        if isinstance(candidate, dict):
            props = candidate.get("properties")
            if isinstance(props, dict):
                yield props
            else:
                yield candidate

    def _walk(obj):
        if isinstance(obj, dict):
            # GeoJSON style list under "features"
            features = obj.get("features")
            if isinstance(features, list):
                for feature in features:
                    yield from _walk(feature)
                return
            if isinstance(features, dict):
                for feature in features.values():
                    yield from _walk(feature)
                return

            # Some responses provide a "data" key with nested lists/dicts
            data_field = obj.get("data")
            if isinstance(data_field, list):
                for item in data_field:
                    yield from _walk(item)
                return
            if isinstance(data_field, dict):
                for item in data_field.values():
                    yield from _walk(item)
                return

            # Fall back to treating the current dict as the candidate itself
            yield from _yield_from_candidate(obj)
            return

        if isinstance(obj, list):
            for item in obj:
                yield from _walk(item)
            return

        # Ignore other data types (strings, numbers, etc.)
        return

    yield from _walk(data)


@st.cache_data(ttl=300)
def get_metar_reports(icao_codes: tuple[str, ...]):
    if not icao_codes:
        return {}

    url = "https://aviationweather.gov/api/data/metar"
    params = {
        "ids": ",".join(sorted(set(code.upper() for code in icao_codes))),
        "format": "json",
        "mostRecent": "true",
        "mostRecentForEachStation": "true",
        "hours": 3,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 400:
            fallback_params = {
                "ids": params["ids"],
                "format": "json",
                "hours": 3,
            }
            response = requests.get(url, params=fallback_params, timeout=10)
            response.raise_for_status()
        else:
            raise

    data = response.json()

    reports = {}
    for props in _normalize_aviationweather_features(data):
        station = (
            props.get("station")
            or props.get("stationId")
            or props.get("icaoId")
            or props.get("icao_id")
            or ""
        ).upper()
        issue_display, issue_dt = format_iso_timestamp(
            props.get("issueTime")
            or props.get("issue_time")
            or props.get("obsTime")
            or props.get("obs_time")
            or props.get("reportTime")
        )
        raw_text = (
            props.get("rawMETAR")
            or props.get("rawOb")
            or props.get("rawText")
            or props.get("raw_text")
            or props.get("raw")
            or ""
        )
        flight_category = props.get("flightCategory") or props.get("flight_category")
        metar_data = (
            props.get("data")
            or props.get("metarData")
            or props.get("report")
            or props
        )
        if not isinstance(metar_data, dict):
            metar_data = {}
        details = build_detail_list(metar_data, METAR_DETAIL_FIELDS)

        if not station:
            continue

        report_entry = {
            "station": station,
            "raw": raw_text,
            "issue_time_display": issue_display,
            "issue_time": issue_dt,
            "flight_category": flight_category,
            "details": details,
            "metar_data": metar_data,
        }

        existing_entries = reports.get(station, [])
        if not existing_entries:
            reports[station] = [report_entry]
        else:
            existing = existing_entries[0]
            existing_dt = existing.get("issue_time") or datetime.min
            new_dt = report_entry.get("issue_time") or datetime.min
            if new_dt >= existing_dt:
                reports[station] = [report_entry]

    return reports


@st.cache_data(ttl=300)
def get_taf_reports(icao_codes: tuple[str, ...]):
    if not icao_codes:
        return {}

    url = "https://aviationweather.gov/api/data/taf"
    params = {
        "ids": ",".join(sorted(set(code.upper() for code in icao_codes))),
        "format": "json",
        "mostRecent": "true",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 400:
            fallback_params = {
                "ids": params["ids"],
                "format": "json",
            }
            response = requests.get(url, params=fallback_params, timeout=10)
            response.raise_for_status()
        else:
            raise

    data = response.json()

    taf_reports = {}

    for props in _normalize_aviationweather_features(data):
        station = (
            props.get("station")
            or props.get("stationId")
            or props.get("icaoId")
            or props.get("icao_id")
            or ""
        ).upper()
        if not station:
            continue

        issue_display, issue_dt = format_iso_timestamp(
            props.get("issueTime")
            or props.get("issue_time")
            or props.get("obsTime")
            or props.get("obs_time")
            or props.get("bulletinTime")
        )
        valid_from_display, valid_from_dt = format_iso_timestamp(
            props.get("validTimeFrom") or props.get("valid_time_from")
        )
        valid_to_display, valid_to_dt = format_iso_timestamp(
            props.get("validTimeTo") or props.get("valid_time_to")
        )
        raw_text = (
            props.get("rawTAF")
            or props.get("rawText")
            or props.get("raw")
            or props.get("raw_text")
            or ""
        )

        forecast_source = (
            props.get("forecast")
            or props.get("forecastList")
            or props.get("periods")
            or []
        )
        if isinstance(forecast_source, dict):
            nested = []
            for key in ("data", "period", "periods", "forecast"):
                value = forecast_source.get(key)
                if isinstance(value, list):
                    nested.extend(value)
            if not nested:
                nested.extend(
                    v for v in forecast_source.values() if isinstance(v, dict)
                )
            forecast_source = nested
        if not isinstance(forecast_source, list):
            forecast_source = []

        forecast_periods = []
        for fc in forecast_source:
            if not isinstance(fc, dict):
                continue
            fc_from_display, fc_from_dt = format_iso_timestamp(
                fc.get("fcstTimeFrom")
                or fc.get("timeFrom")
                or fc.get("time_from")
            )
            fc_to_display, fc_to_dt = format_iso_timestamp(
                fc.get("fcstTimeTo")
                or fc.get("timeTo")
                or fc.get("time_to")
            )
            fc_details = build_detail_list(fc, TAF_FORECAST_FIELDS)

            wx = fc.get("wxString") or fc.get("weather")
            if not wx:
                wx = fc.get("wx_string")
            if wx:
                if isinstance(wx, list):
                    wx = ", ".join(str(v) for v in wx if v not in (None, ""))
                fc_details.append(("Weather", wx))

            clouds = (
                fc.get("clouds")
                or fc.get("cloudList")
                or fc.get("skyCondition")
                or fc.get("sky_condition")
            )
            if isinstance(clouds, list):
                cloud_parts = []
                for cloud in clouds:
                    if not isinstance(cloud, dict):
                        continue
                    cover = (
                        cloud.get("cover")
                        or cloud.get("cloudCover")
                        or cloud.get("cloud_cover")
                        or cloud.get("skyCover")
                    )
                    base = (
                        cloud.get("base")
                        or cloud.get("base_feet")
                        or cloud.get("cloudBaseFT")
                        or cloud.get("cloudBaseFt")
                        or cloud.get("baseFeetAGL")
                        or cloud.get("base_feet_agl")
                    )
                    if cover and base:
                        cloud_parts.append(f"{cover} {base}ft")
                    elif cover:
                        cloud_parts.append(str(cover))
                if cloud_parts:
                    fc_details.append(("Clouds", ", ".join(cloud_parts)))

            forecast_periods.append({
                "from_display": fc_from_display,
                "from_time": fc_from_dt,
                "to_display": fc_to_display,
                "to_time": fc_to_dt,
                "details": fc_details,
            })

        taf_reports.setdefault(station, []).append({
            "station": station,
            "raw": raw_text,
            "issue_time_display": issue_display,
            "issue_time": issue_dt,
            "valid_from_display": valid_from_display,
            "valid_from": valid_from_dt,
            "valid_to_display": valid_to_display,
            "valid_to": valid_to_dt,
            "forecast": forecast_periods,
        })

    return taf_reports


def _split_taf_into_lines(raw_taf: str) -> list[list[str]]:
    if not raw_taf:
        return []

    tokens = raw_taf.split()
    if not tokens:
        return []

    lines: list[list[str]] = []
    current_line: list[str] = []

    for token in tokens:
        if current_line and TAF_CHANGE_REGEX.match(token):
            first_token = current_line[0]
            if not (re.match(r"^PROB\d{2}$", first_token) and token == "TEMPO"):
                lines.append(current_line)
                current_line = []
        current_line.append(token)

    if current_line:
        lines.append(current_line)

    return lines


def format_taf_for_display(raw_taf: str) -> str:
    lines = _split_taf_into_lines(raw_taf)
    if not lines:
        return raw_taf or ""
    return "\n".join(" ".join(line) for line in lines)


def format_taf_for_display_html(raw_taf: str) -> str:
    lines = _split_taf_into_lines(raw_taf)
    if not lines:
        escaped = html.escape(raw_taf or "")
        return (
            "<div style='font-family:monospace;white-space:pre-wrap;'>"
            f"{escaped}"
            "</div>"
        )

    html_lines: list[str] = []
    for line in lines:
        tokens_html = []
        for token in line:
            token_str = token or ""
            highlight = False
            if _should_highlight_visibility(token_str):
                highlight = True
            if _should_highlight_weather(token_str):
                highlight = True
            escaped_token = html.escape(token_str)
            if highlight:
                tokens_html.append(_wrap_highlight(escaped_token))
            else:
                tokens_html.append(escaped_token)
        html_lines.append(" ".join(tokens_html))

    joined = "<br>".join(html_lines)
    return (
        "<div style='font-family:monospace;white-space:pre-wrap;'>"
        f"{joined}"
        "</div>"
    )


def format_notam_card(notam):
    highlighted_text = highlight_keywords(notam["text"])
    category_color = CATEGORY_COLORS.get(notam["category"], "#ccc")

    if notam["start_dt"] and notam["end_dt"]:
        delta = notam["end_dt"] - notam["start_dt"]
        hours, remainder = divmod(delta.total_seconds(), 3600)
        minutes = remainder // 60
        duration_str = f"{int(hours)}h{int(minutes):02d}m"
    else:
        duration_str = "N/A"

    now = datetime.utcnow()
    if notam["end_dt"]:
        remaining_delta = notam["end_dt"] - now
        if remaining_delta.total_seconds() > 0:
            rem_hours, rem_remainder = divmod(remaining_delta.total_seconds(), 3600)
            rem_minutes = rem_remainder // 60
            remaining_str = f"(in {int(rem_hours)}h{int(rem_minutes):02d}m)"
        else:
            remaining_str = "(expired)"
    else:
        remaining_str = ""

    # Highlight PPR category more prominently
    border_style = f"3px solid {category_color}" if notam["category"] in ["Runway", "PPR"] else "1px solid #ccc"

    card_html = f"""
    <div style='border:{border_style}; padding:10px; margin-bottom:8px; background-color:#111; color:#eee; border-radius:5px;'>
        <p style='margin:0; font-family:monospace;'><strong style="color:{category_color}">[{notam['category']}]</strong></p>
        <p style='margin:0; font-family:monospace; white-space:pre-wrap;'>{highlighted_text}</p>
        <table style='margin-top:5px; font-size:0.9em; color:#aaa; width:100%;'>
            <tr><td><strong>Effective:</strong></td><td>{notam['effectiveStart']}</td><td>{remaining_str}</td></tr>
            <tr><td><strong>Expires:</strong></td><td>{notam['effectiveEnd']}</td></tr>
            <tr><td><strong>Duration:</strong></td><td>{duration_str}</td></tr>
        </table>
    </div>
    """
    return card_html

def normalize_for_dedup(raw_text: str) -> str:
    text = raw_text.lstrip("!").strip()
    text = re.sub(r"\b\d{2}/\d{3}\b", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def deduplicate_notams(notams):
    grouped = {}
    for n in notams:
        norm_text = normalize_for_dedup(n["text"])
        key = (norm_text, n["effectiveStart"], n["effectiveEnd"])
        if key not in grouped:
            grouped[key] = n
        else:
            existing = grouped[key]
            if len(n["text"]) > len(existing["text"]):
                grouped[key] = n
    return list(grouped.values())

def is_runway_closed(notam_text, runway_name):
    text_upper = notam_text.upper()
    runway_upper = runway_name.upper()
    direct_rwy_pattern = rf"RWY\s+{re.escape(runway_upper)}\b.*(?:{'|'.join(KEYWORDS)})"
    twy_context_pattern = rf"TWY\s+[A-Z0-9]+.*RWY\s+{re.escape(runway_upper)}"
    if re.search(direct_rwy_pattern, text_upper):
        if not re.search(twy_context_pattern, text_upper):
            return True
        if "AVBL AS TWY" in text_upper:
            return True
    return False

def normalize_surface(surface):
    s = str(surface).upper()
    if any(a in s for a in ["ASP", "ASPH", "ASPHALT"]):
        return "Asphalt", True
    elif any(c in s for c in ["CON", "CONC", "CONCRETE"]):
        return "Concrete", True
    else:
        return s.title(), False

def get_runway_status(icao: str, airport_notams: list):
    airport_runways = runways_df[runways_df['airport_ident'] == icao.upper()]
    status_list = []
    for _, row in airport_runways.iterrows():
        full_rwy_name = row['le_ident'] + '/' + row['he_ident'] if pd.notna(row['he_ident']) else row['le_ident']
        closed = False
        for n in airport_notams:
            if is_runway_closed(n["text"], full_rwy_name):
                closed = True
                break

        surface_normalized, usable = normalize_surface(row.get('surface', 'Unknown'))

        status_list.append({
            "runway": full_rwy_name,
            "length_ft": row['length_ft'],
            "surface": surface_normalized,
            "usable": usable,
            "status": "closed" if closed else "open"
        })

    return status_list

def sort_notams_for_display(notams):
    def sort_key(n):
        if n["category"] == "Runway":
            return (0, n["sortKey"])
        elif n["category"] == "PPR":
            return (1, n["sortKey"])
        elif n["category"] == "Airspace/Navigation":
            return (2, n["sortKey"])
        elif n["category"] == "Airport Services":
            return (3, n["sortKey"])
        else:
            return (4, n["sortKey"])
    return sorted(notams, key=sort_key)

# ----- FL3XX HELPERS -----


def _normalise_date_range(selection: Union[Sequence[Optional[date]], Optional[date]]) -> Tuple[date, date]:
    """Return a valid inclusive date range from a Streamlit date_input selection."""

    if isinstance(selection, (tuple, list)):
        if len(selection) >= 2:
            start, end = selection[0], selection[1]
        elif len(selection) == 1:
            start = selection[0]
            end = selection[0]
        else:
            start = None
            end = None
    else:
        start = selection
        end = selection

    # Streamlit returns ``None`` while a user is still selecting the end date of a
    # range. Default ``None`` values to the selected start date so we always
    # operate on valid ``date`` objects.
    if start is None and end is None:
        today = date.today()
        start = today
        end = today
    if start is None:
        start = end
    if end is None:
        end = start

    if end < start:
        start, end = end, start
    return start, end


def _extract_airport_codes(rows: List[Dict[str, Any]]) -> List[str]:
    """Collect unique ICAO codes from normalised FL3XX leg rows."""

    candidates: List[str] = []
    seen: Set[str] = set()
    airport_fields = (
        "departure_airport",
        "arrival_airport",
        "departure_airport_code",
        "arrival_airport_code",
    )

    for row in rows:
        for field in airport_fields:
            if field not in row:
                continue
            value = row.get(field)
            if value in (None, ""):
                continue
            code = str(value).strip().upper()
            if len(code) != 4 or not code.isalnum():
                continue
            if code in seen:
                continue
            seen.add(code)
            candidates.append(code)

    return candidates


@st.cache_data(show_spinner=True, ttl=300)
def load_fl3xx_airports(
    _settings: Optional[Dict[str, Any]],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[List[str], Dict[str, Any], Dict[str, Any]]:
    """Fetch FL3XX flights and return unique airport codes with metadata."""

    config = build_fl3xx_api_config(_settings)
    flights, metadata = fetch_flights(
        config,
        from_date=from_date,
        to_date=to_date,
    )

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    airports = _extract_airport_codes(normalized_rows)

    metadata = {**metadata, "flights_returned": len(flights)}
    return airports, metadata, normalization_stats

# ----- USER INPUT -----
st.sidebar.header("Airport Source")
airport_source = st.sidebar.radio(
    "Select how to populate airport codes:",
    ("Manual entry / Upload", "FL3XX Flight Schedule"),
)

icao_list: List[str] = []
fl3xx_metadata: Optional[Dict[str, Any]] = None
fl3xx_normalization: Optional[Dict[str, Any]] = None

if airport_source == "Manual entry / Upload":
    icao_input = st.text_input(
        "Enter ICAO code(s) separated by commas (e.g., CYYC, KTEB):",
    ).upper().strip()

    uploaded_file = st.file_uploader(
        "Or upload an Excel/CSV with ICAO codes (columns: 'ICAO', 'From (ICAO)', 'To (ICAO)')",
        type=["xlsx", "csv"],
    )

    if icao_input:
        icao_list.extend([code.strip() for code in icao_input.split(",") if code.strip()])

    if uploaded_file:
        try:
            df = (
                pd.read_csv(uploaded_file)
                if uploaded_file.name.endswith(".csv")
                else pd.read_excel(uploaded_file)
            )
            found_codes = []
            for col in ["ICAO", "From (ICAO)", "To (ICAO)"]:
                if col in df.columns:
                    found_codes.extend(df[col].dropna().astype(str).str.upper().tolist())
            if found_codes:
                icao_list.extend(list(dict.fromkeys(found_codes)))
            else:
                st.error("Uploaded file must have a valid ICAO column")
        except Exception as e:
            st.error(f"Error reading file: {e}")
else:
    st.subheader("Load airports from FL3XX schedule")
    default_from, default_to_exclusive = compute_fetch_dates()
    default_to_inclusive = default_to_exclusive - timedelta(days=1)

    date_selection = st.date_input(
        "Select FL3XX departure date range (UTC)",
        value=(default_from, default_to_inclusive),
        help="Flights departing within this inclusive date window will be scanned for airports.",
    )

    range_start, range_end = _normalise_date_range(date_selection)
    fetch_to_date = range_end + timedelta(days=1)

    if fetch_to_date <= range_start:
        fetch_to_date = range_start + timedelta(days=1)

    fl3xx_settings = st.secrets.get("fl3xx_api")
    if not fl3xx_settings:
        st.warning(
            "Add your FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to enable schedule fetching.",
        )
    else:
        try:
            airports, fl3xx_metadata, fl3xx_normalization = load_fl3xx_airports(
                fl3xx_settings,
                from_date=range_start,
                to_date=fetch_to_date,
            )
            icao_list.extend(airports)
        except FlightDataError as exc:
            st.error(str(exc))
        except requests.HTTPError as exc:
            st.error(f"FL3XX API request failed: {exc}")
        except Exception as exc:  # pragma: no cover - defensive fallback
            st.error(f"Unexpected error fetching FL3XX flights: {exc}")

    if fl3xx_metadata:
        fetched_from = fl3xx_metadata.get("from_date", range_start.isoformat())
        fetched_to = range_end.isoformat()
        flights_returned = fl3xx_metadata.get("flights_returned", 0)
        airport_count = len(dict.fromkeys(icao_list))
        st.info(
            f"Loaded {airport_count} airport(s) from {flights_returned} FL3XX flight(s) between {fetched_from} and {fetched_to}.",
        )
        with st.expander("FL3XX fetch metadata", expanded=False):
            st.json({"metadata": fl3xx_metadata, "normalization": fl3xx_normalization or {}})

icao_list = list(dict.fromkeys([code.strip().upper() for code in icao_list if code]))


def _fetch_notams_for_airports(codes: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Tuple[str, str]]]:
    """Retrieve NOTAMs for the provided ICAO codes grouped by data source."""

    cfps_list: List[Dict[str, Any]] = []
    faa_list: List[Dict[str, Any]] = []
    errors: List[Tuple[str, str]] = []

    for icao in codes:
        try:
            if icao.startswith("C"):
                cfps_list.append({"ICAO": icao, "notams": get_cfps_notams(icao)})
            else:
                faa_list.append({"ICAO": icao, "notams": get_faa_notams(icao)})
        except Exception as exc:  # pragma: no cover - defensive fallback
            errors.append((icao, str(exc)))

    return cfps_list, faa_list, errors


SESSION_KEY_RESULTS = "notam_results"

stored_results = st.session_state.get(SESSION_KEY_RESULTS)
current_codes = tuple(icao_list)

if stored_results and stored_results.get("codes") != current_codes:
    stored_results = None
    st.session_state[SESSION_KEY_RESULTS] = None

# ----- TABS -----
tab1, tab2 = st.tabs(["CFPS/FAA Viewer", "METAR/TAF"])

# ---------------- Tab 1: CFPS/FAA Viewer ----------------
with tab1:
    fetch_button = st.button(
        "Fetch NOTAMs",
        type="primary",
        disabled=not icao_list,
        help="Fetch NOTAMs for the airport list above. Data is retained until the list changes.",
    )

    if fetch_button and not icao_list:
        st.warning("Enter at least one ICAO code to fetch NOTAMs.")

    if fetch_button and icao_list:
        with st.spinner(f"Fetching NOTAMs for {len(icao_list)} airport(s)..."):
            cfps_list, faa_list, errors = _fetch_notams_for_airports(icao_list)
        stored_results = {
            "codes": current_codes,
            "cfps": cfps_list,
            "faa": faa_list,
            "errors": errors,
        }
        st.session_state[SESSION_KEY_RESULTS] = stored_results

    if stored_results and stored_results.get("codes") == current_codes:
        cfps_list = stored_results.get("cfps", [])
        faa_list = stored_results.get("faa", [])
        errors = stored_results.get("errors", [])

        if errors:
            for icao, message in errors:
                st.warning(f"Failed to fetch data for {icao}: {message}")

        # Filter input
        filter_input = st.text_input("Filter NOTAMs by keywords (comma-separated):").strip().lower()
        filter_terms = [t.strip() for t in filter_input.split(",") if t.strip()]

        def matches_filter(text: str):
            if not filter_terms:
                return True
            return any(term in text.lower() for term in filter_terms)

        def highlight_search_terms(notam_text: str):
            highlighted = notam_text
            for term in filter_terms:
                highlighted = re.sub(
                    f"({re.escape(term)})",
                    r"<span style='background-color:rgba(255, 255, 0, 0.3); font-weight:bold'>\1</span>",
                    highlighted,
                    flags=re.IGNORECASE,
                )
            return highlighted

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Canadian Airports (CFPS)")
            for airport in cfps_list:
                # Apply filter to NOTAMs before rendering
                filtered_notams = [n for n in sort_notams_for_display(airport["notams"]) if matches_filter(n["text"])]
                if not filtered_notams:
                    continue  # Skip airport if no NOTAMs match

                with st.expander(airport["ICAO"], expanded=False):
                    # Only show runway status if there are filtered NOTAMs
                    runways_status = get_runway_status(airport["ICAO"], filtered_notams)
                    if runways_status:
                        runway_table_html = "<table style='border-collapse: collapse; width:100%; color:#eee;'>"
                        runway_table_html += "<tr><th>Runway</th><th>Length (ft)</th><th>Surface</th><th>Status</th></tr>"
                        for r in runways_status:
                            color = "#f00" if r["status"] == "closed" else "#0f0"
                            surface_color = "#f00" if not r["usable"] else "#0f0"
                            runway_table_html += f"<tr><td>{r['runway']}</td><td>{r['length_ft']}</td><td style='color:{surface_color}'>{r['surface']}</td><td style='color:{color}'>{r['status']}</td></tr>"
                        runway_table_html += "</table>"
                        st.markdown(runway_table_html, unsafe_allow_html=True)
        
                    for notam in filtered_notams:
                        notam_copy = notam.copy()
                        notam_copy["text"] = highlight_search_terms(notam_copy["text"])
                        st.markdown(format_notam_card(notam_copy), unsafe_allow_html=True)
        with col2:
            st.subheader("US Airports (FAA)")
            for airport in faa_list:
                # Apply filter to NOTAMs before rendering
                filtered_notams = [n for n in sort_notams_for_display(airport["notams"]) if matches_filter(n["text"])]
                if not filtered_notams:
                    continue  # Skip airport if no NOTAMs match
        
                with st.expander(airport["ICAO"], expanded=False):
                    # Only show runway status if there are filtered NOTAMs
                    runways_status = get_runway_status(airport["ICAO"], filtered_notams)
                    if runways_status:
                        runway_table_html = "<table style='border-collapse: collapse; width:100%; color:#eee;'>"
                        runway_table_html += "<tr><th>Runway</th><th>Length (ft)</th><th>Surface</th><th>Status</th></tr>"
                        for r in runways_status:
                            color = "#f00" if r["status"] == "closed" else "#0f0"
                            surface_color = "#f00" if not r["usable"] else "#0f0"
                            runway_table_html += f"<tr><td>{r['runway']}</td><td>{r['length_ft']}</td><td style='color:{surface_color}'>{r['surface']}</td><td style='color:{color}'>{r['status']}</td></tr>"
                        runway_table_html += "</table>"
                        st.markdown(runway_table_html, unsafe_allow_html=True)
        
                    for notam in filtered_notams:
                        notam_copy = notam.copy()
                        notam_copy["text"] = highlight_search_terms(notam_copy["text"])
                        st.markdown(format_notam_card(notam_copy), unsafe_allow_html=True)
    elif icao_list:
        st.info("Click 'Fetch NOTAMs' to retrieve data for the selected airports.")
    else:
        st.info("Add ICAO codes above to enable NOTAM fetching.")



# ---------------- Tab 2: METAR/TAF ----------------
with tab2:
    st.header("METAR & TAF Data")

    if not icao_list:
        st.info("Enter at least one ICAO code above to retrieve METAR/TAF data.")
    else:
        unique_codes = sorted(set(icao_list))
        st.write(f"Fetching METAR/TAF data for {len(unique_codes)} station(s)...")

        try:
            metar_reports = get_metar_reports(tuple(unique_codes))
        except Exception as e:
            metar_reports = {}
            st.warning(f"Failed to retrieve METAR data: {e}")

        try:
            taf_reports = get_taf_reports(tuple(unique_codes))
        except Exception as e:
            taf_reports = {}
            st.warning(f"Failed to retrieve TAF data: {e}")

        if not any(metar_reports.get(code) or taf_reports.get(code) for code in unique_codes):
            st.info("No METAR/TAF data returned for the provided stations.")

        for code in unique_codes:
            metars = metar_reports.get(code, [])
            tafs = taf_reports.get(code, [])

            with st.expander(code, expanded=False):
                if metars:
                    st.subheader("Latest METAR")
                    latest_metar = max(
                        metars,
                        key=lambda m: m.get("issue_time") or datetime.min,
                    )

                    header_parts = ["**METAR**"]
                    if latest_metar.get("flight_category"):
                        header_parts.append(
                            f"Flight Category: `{latest_metar['flight_category']}`"
                        )
                    if (
                        latest_metar.get("issue_time_display")
                        and latest_metar["issue_time_display"] != "N/A"
                    ):
                        header_parts.append(
                            f"Issued {latest_metar['issue_time_display']}"
                        )
                    st.markdown(" · ".join(header_parts))
                    st.code(latest_metar.get("raw", ""), language="text")

                    summary_lines = build_metar_summary(latest_metar)
                    if summary_lines:
                        st.markdown(
                            "\n".join(f"- {line}" for line in summary_lines),
                            unsafe_allow_html=True,
                        )

                    remaining_details = [
                        (label, value)
                        for label, value in latest_metar.get("details", [])
                        if label not in METAR_SUMMARY_LABELS
                    ]
                    if remaining_details:
                        detail_html = "<br>".join(
                            _format_detail_entry(label, value)
                            for label, value in remaining_details
                        )
                        st.markdown(detail_html, unsafe_allow_html=True)

                    st.markdown("---")
                else:
                    st.write("No METAR data returned for this station.")

                if tafs:
                    st.subheader("Latest TAF")
                    for taf in sorted(tafs, key=lambda t: t.get("issue_time") or datetime.min, reverse=True):
                        header_parts = ["**TAF**"]
                        if taf.get("issue_time_display") and taf["issue_time_display"] != "N/A":
                            header_parts.append(f"Issued {taf['issue_time_display']}")
                        validity_parts = []
                        if taf.get("valid_from_display") and taf["valid_from_display"] != "N/A":
                            validity_parts.append(taf["valid_from_display"])
                        if taf.get("valid_to_display") and taf["valid_to_display"] != "N/A":
                            validity_parts.append(taf["valid_to_display"])
                        if validity_parts:
                            header_parts.append("Valid " + " → ".join(validity_parts))

                        st.markdown(" · ".join(header_parts))
                        raw_taf = taf.get("raw", "")
                        formatted_taf_html = format_taf_for_display_html(raw_taf)
                        st.markdown(formatted_taf_html, unsafe_allow_html=True)

                        forecast_rows = []
                        for fc in taf.get("forecast", []):
                            detail_entries = [
                                _format_inline_detail(label, value)
                                for label, value in fc.get("details", [])
                            ]
                            details_text = "; ".join(detail_entries) if detail_entries else "—"
                            forecast_rows.append({
                                "From": fc.get("from_display", "N/A"),
                                "To": fc.get("to_display", "N/A"),
                                "Details": details_text,
                            })

                        if forecast_rows:
                            forecast_df = pd.DataFrame(forecast_rows)
                            st.markdown(
                                forecast_df.to_html(escape=False, index=False),
                                unsafe_allow_html=True,
                            )

                        st.markdown("---")
                else:
                    st.write("No TAF data returned for this station.")

                if not metars and not tafs:
                    st.info("No METAR or TAF information was available for this station.")
