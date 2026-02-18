import csv
import hashlib
import html
import json
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests
import streamlit as st

from arrival_deice_utils import resolve_deice_status
from fl3xx_client import fetch_flights
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
    safe_parse_dt,
)
from Home import configure_page, password_gate, render_sidebar
from taf_utils import get_taf_reports
from zoneinfo_compat import ZoneInfo
from arrival_weather_utils import (
    _CEILING_CODE_REGEX,
    _combine_highlight_levels,
    _determine_highlight_level,
    _format_clouds_value,
    _get_ceiling_highlight,
    _get_visibility_highlight,
    _has_freezing_precip,
    _has_wintry_precip,
    _build_weather_value_html,
    _parse_ceiling_value,
    _parse_fraction,
    _parse_visibility_value,
    _should_highlight_weather,
    _try_float,
    _wrap_highlight_html,
)

configure_page(page_title="Arrival Weather Outlook")
password_gate()
render_sidebar()


st.title("🛬 Arrival Weather Outlook")


def _load_mountain_tz() -> ZoneInfo:
    for name in ("America/Edmonton", "US/Mountain"):
        try:
            return ZoneInfo(name)
        except Exception:
            continue
    return ZoneInfo("UTC")


MOUNTAIN_TZ = _load_mountain_tz()
TAIL_DISPLAY_ORDER: Sequence[str] = (
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
FAR_FUTURE = datetime.max.replace(tzinfo=timezone.utc)

TAILWIND_DIRECTION_RANGES: Dict[str, Tuple[int, int]] = {
    "CYRV": (30, 210),
    "KSUN": (40, 220),
    "KASE": (240, 60),
}

ARRIVAL_TIME_KEYS: Sequence[str] = (
    "arrival_time",
    "arrival_time_utc",
    "arrivalUtc",
    "arrivalUTC",
    "arrivalOnBlockUtc",
    "arrivalActualUtc",
    "arrivalScheduledUtc",
    "blockOnTimeUtc",
    "blockOnUtc",
    "realDateIN",
    "realDateON",
    "arr_time",
)
ACTUAL_ARRIVAL_TIME_KEYS: Sequence[str] = (
    "arrivalActualUtc",
    "arrivalActualTime",
    "arrivalActual",
    "realDateIN",
    "realDateON",
    "onBlockTimeUtc",
    "onBlockUtc",
    "onBlockTime",
    "onBlockActual",
    "blockOnTimeUtc",
    "blockOnUtc",
    "blockOnTime",
    "blockOnActualUTC",
    "blockOnActualUtc",
    "blockOnActual",
)
DEPARTURE_TIME_KEYS: Sequence[str] = (
    "dep_time",
    "departureTimeUtc",
    "departure_time_utc",
    "blockOffTimeUtc",
    "blockOffUtc",
    "scheduledDepartureTime",
    "scheduledDeparture",
)


st.markdown(
    """
    <style>
    .flight-row {display:flex; flex-wrap:wrap; gap:0.75rem; margin-bottom:1.5rem;}
    .flight-card {border-radius:12px; padding:0.9rem 1.1rem; min-width:240px; max-width:360px;
                  box-shadow:0 8px 18px rgba(15, 23, 42, 0.35); border:1px solid rgba(148, 163, 184, 0.4);
                  background:rgba(17, 24, 39, 0.85); transition:background 0.2s ease, border-color 0.2s ease;}
    .flight-card--today {background:rgba(37, 99, 235, 0.22); border-color:rgba(147, 197, 253, 0.65);}
    .flight-card--future {background:rgba(15, 23, 42, 0.88);}
    .flight-card--past {background:rgba(22, 101, 52, 0.78); border-color:rgba(34, 197, 94, 0.82);
                        box-shadow:0 0 0 2px rgba(34, 197, 94, 0.45), 0 12px 24px rgba(22, 101, 52, 0.35);}
    .flight-card--arrival-elapsed {background:rgba(202, 138, 4, 0.78); border-color:rgba(250, 204, 21, 0.82);
                                   box-shadow:0 0 0 2px rgba(250, 204, 21, 0.45), 0 12px 24px rgba(161, 98, 7, 0.35);}
    .flight-card__header {display:flex; justify-content:space-between; align-items:flex-start; gap:0.75rem;}
    .flight-card h4 {margin:0 0 0.35rem 0; font-size:1.05rem; color:#f8fafc;}
    .flight-card__runway {font-size:0.75rem; color:#e2e8f0; background:rgba(30, 64, 175, 0.35);
                          border-radius:0.6rem; padding:0.35rem 0.6rem; white-space:normal;
                          font-weight:600; letter-spacing:0.03em; display:flex; flex-direction:column;
                          gap:0.15rem; min-width:fit-content;}
    .flight-card__runway-text {font-size:0.75rem; line-height:1.2;}
    .flight-card__deice {font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;}
    .flight-card__deice--full {color:#bbf7d0;}
    .flight-card__deice--partial {color:#fde68a;}
    .flight-card__deice--none {color:#fecaca;}
    .flight-card__deice--unknown {color:#fef9c3;}
    .flight-card .times {font-family:"Source Code Pro", Menlo, Consolas, monospace; font-size:0.9rem;
                         margin-bottom:0.45rem; line-height:1.35; color:#cbd5f5;}
    .flight-card .past-flag {display:inline-block; padding:0.25rem 0.55rem; margin-bottom:0.5rem;
                             border-radius:999px; font-size:0.75rem; font-weight:700; letter-spacing:0.04em;
                             background:rgba(34, 197, 94, 0.22); color:#bbf7d0; border:1px solid rgba(34, 197, 94, 0.45);
                             text-transform:uppercase;}
    .flight-card .arrival-elapsed-flag {display:inline-block; padding:0.25rem 0.55rem; margin-bottom:0.5rem;
                                        border-radius:999px; font-size:0.75rem; font-weight:700; letter-spacing:0.04em;
                                        background:rgba(250, 204, 21, 0.25); color:#fef9c3; border:1px solid rgba(250, 204, 21, 0.55);
                                        text-transform:uppercase;}
    .flight-card .badge-strip {display:flex; flex-wrap:wrap; gap:0.35rem; margin-bottom:0.35rem;}
    .flight-card .badge {background:rgba(59,130,246,0.18); color:#93c5fd; padding:0.1rem 0.45rem;
                         border-radius:999px; font-size:0.75rem; letter-spacing:0.02em; text-transform:uppercase;}
    .flight-card .taf {font-size:0.85rem; line-height:1.45; color:#e2e8f0;}
    .flight-card .taf ul {padding-left:1.05rem; margin:0.35rem 0;}
    .flight-card .taf li {margin-bottom:0.2rem;}
    .flight-card details {margin-top:0.45rem;}
    .flight-card details summary {cursor:pointer; color:#38bdf8;}
    .flight-card details pre {background:rgba(15,23,42,0.75); padding:0.5rem; border-radius:8px;
                              overflow:auto; color:#cbd5f5;}
    .flight-card .taf-missing {color:#fca5a5; font-style:italic;}
    .flight-card .taf-warning {margin-top:0.35rem; color:#facc15; font-weight:600;}
    .taf-fallback-banner {margin-bottom:0.5rem; padding:0.35rem 0.55rem; border-radius:0.4rem;
                          background:rgba(251, 191, 36, 0.18); border:1px solid rgba(251, 191, 36, 0.45);
                          color:#facc15; font-size:0.8rem; font-weight:600; letter-spacing:0.03em;
                          text-transform:uppercase; display:inline-block;}
    .taf-highlight {font-weight:600;}
    .taf-highlight--red {color:#c41230;}
    .taf-highlight--yellow {color:#b8860b;}
    .taf-highlight--blue {color:#38bdf8;}
    .tail-header {font-size:1.2rem; margin:0.5rem 0 0.4rem 0; padding-left:0.1rem; color:#e0f2fe;}
    .section-divider {border-bottom:1px solid rgba(148,163,184,0.25); margin:0.75rem 0 1.1rem 0;}
    .flight-card__rsc {display:inline-block;}
    .flight-card__rsc summary {cursor:pointer; display:inline-flex; align-items:center; padding:0.18rem 0.55rem;
                               border-radius:999px; font-size:0.7rem; font-weight:700; letter-spacing:0.04em;
                               text-transform:uppercase; list-style:none;}
    .flight-card__rsc summary::-webkit-details-marker {display:none;}
    .flight-card__rsc--green summary {background:rgba(34,197,94,0.22); color:#bbf7d0;
                                      border:1px solid rgba(34,197,94,0.55);}
    .flight-card__rsc--yellow summary {background:rgba(250,204,21,0.2); color:#fef9c3;
                                       border:1px solid rgba(250,204,21,0.5);}
    .flight-card__rsc--red summary {background:rgba(248,113,113,0.22); color:#fecaca;
                                    border:1px solid rgba(248,113,113,0.55);}
    .flight-card__rsc--critical summary {background:rgba(190,18,60,0.3); color:#ffe4e6;
                                         border:1px solid rgba(251,113,133,0.75);
                                         box-shadow:0 0 0 2px rgba(251,113,133,0.45),
                                                    0 6px 18px rgba(190,18,60,0.35);}
    .flight-card__rsc--neutral summary {background:rgba(148,163,184,0.18); color:#e2e8f0;
                                        border:1px solid rgba(148,163,184,0.35);}
    .flight-card__rsc-body {margin-top:0.4rem; font-size:0.78rem; color:#e2e8f0;}
    .flight-card__rsc-body ul {margin:0.2rem 0 0.4rem 1.05rem;}
    .flight-card__rsc-note {font-style:italic; color:#facc15;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_longest_runways() -> Dict[str, int]:
    runway_map: Dict[str, int] = {}
    try:
        runways_path = Path(__file__).resolve().parents[1] / "runways.csv"
        with runways_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ident = (row.get("airport_ident") or "").strip().upper()
                if not ident:
                    continue
                length_text = (row.get("length_ft") or "").strip()
                try:
                    length_val = int(float(length_text))
                except ValueError:
                    continue
                if length_val <= 0:
                    continue
                current = runway_map.get(ident)
                if current is None or length_val > current:
                    runway_map[ident] = length_val
    except FileNotFoundError:
        return {}
    return runway_map


def _default_date_range(now: Optional[datetime] = None) -> Tuple[date, date]:
    now_local = (now or datetime.now(tz=MOUNTAIN_TZ)).astimezone(MOUNTAIN_TZ)
    start_date = now_local.date()
    end_date = start_date + timedelta(days=1)
    return start_date, end_date


def _normalise_date_range(selection: Any) -> Tuple[date, date]:
    if isinstance(selection, (list, tuple)) and selection:
        start = selection[0]
        end = selection[-1]
    else:
        start = selection
        end = selection
    if start is None or end is None:
        today = datetime.now(tz=MOUNTAIN_TZ).date()
        start = end = today
    if end < start:
        start, end = end, start
    return start, end


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", []):
        return None
    try:
        parsed = safe_parse_dt(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _extract_datetime(row: Dict[str, Any], keys: Sequence[str]) -> Optional[datetime]:
    for key in keys:
        if key not in row:
            continue
        parsed = _parse_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _to_local(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.astimezone(MOUNTAIN_TZ)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_local(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    return dt.astimezone(MOUNTAIN_TZ).strftime("%a %b %d · %H:%M MT")


def _format_utc(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    return dt.astimezone(timezone.utc).strftime("%H:%MZ")


def _format_duration_short(delta: timedelta) -> str:
    total_minutes = int(max(delta.total_seconds(), 0) // 60)
    days, remainder_minutes = divmod(total_minutes, 60 * 24)
    hours, minutes = divmod(remainder_minutes, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def _coerce_code(value: Any) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


def _get_weather_highlight(value: Optional[str], deice_status: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return None
    highlight = "red" if _should_highlight_weather(value) else None
    if not deice_status:
        deice_status = "full"
    if _has_freezing_precip(value):
        if highlight == "red":
            return "red"
        return "blue"
    if _has_wintry_precip(value):
        if deice_status == "partial":
            return "red"
        if deice_status in ("none", "unknown"):
            if highlight == "red":
                return "red"
            return "blue"
    return highlight


def _tail_order_key(tail: str) -> Tuple[int, str]:
    return (TAIL_INDEX.get(tail, len(TAIL_DISPLAY_ORDER)), tail)


def _settings_digest(settings: Mapping[str, Any]) -> str:
    def _normalise(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(k): _normalise(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_normalise(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    normalized = {str(k): _normalise(v) for k, v in settings.items()}
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def load_flight_rows(
    settings_digest: str,
    settings: Dict[str, Any],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    # ``settings_digest`` participates in the cache key to ensure that changes to
    # FL3XX credentials invalidate the cached data, while ``hash_funcs`` above
    # avoids hashing the secrets themselves.
    _ = settings_digest
    config = build_fl3xx_api_config(settings)
    flights, metadata = fetch_flights(config, from_date=from_date, to_date=to_date)
    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    filtered_rows, subcharter_skipped = filter_out_subcharter_rows(normalized_rows)
    metadata = {
        **metadata,
        "flights_returned": len(flights),
        "legs_after_filter": len(filtered_rows),
        "subcharters_filtered": subcharter_skipped,
    }
    return filtered_rows, metadata, normalization_stats


@st.cache_data(show_spinner=True, ttl=600)
def load_taf_reports(codes: Tuple[str, ...]) -> Dict[str, List[Dict[str, Any]]]:
    if not codes:
        return {}
    return get_taf_reports(codes)

def _select_forecast_period(
    report_list: Sequence[Dict[str, Any]],
    arrival_dt: Optional[datetime],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not report_list:
        return None, None
    sorted_reports = sorted(
        report_list,
        key=lambda item: item.get("issue_time") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    arrival_dt = _ensure_utc(arrival_dt)

    if arrival_dt is not None:
        for report in sorted_reports:
            valid_from = _ensure_utc(report.get("valid_from"))
            valid_to = _ensure_utc(report.get("valid_to"))
            if (
                valid_from is not None
                and valid_to is not None
                and valid_from <= arrival_dt < valid_to
            ):
                period = _match_period(report.get("forecast", []), arrival_dt)
                if period:
                    return report, period

    latest = sorted_reports[0]
    fallback_period = _match_period(latest.get("forecast", []), arrival_dt)
    return latest, fallback_period


def _match_period(
    periods: Iterable[Dict[str, Any]], arrival_dt: Optional[datetime]
) -> Optional[Dict[str, Any]]:
    period_list = [period for period in periods if isinstance(period, dict)]
    if not period_list:
        return None

    sorted_periods = sorted(
        period_list,
        key=lambda period: period.get("from_time")
        or datetime.min.replace(tzinfo=timezone.utc),
    )

    arrival_dt = _ensure_utc(arrival_dt)

    if arrival_dt is None:
        return sorted_periods[-1]

    for period in sorted_periods:
        start = _ensure_utc(period.get("from_time"))
        end = _ensure_utc(period.get("to_time"))
        if start and end and start <= arrival_dt < end:
            return period
        if start and not end and arrival_dt >= start:
            return period
        if not start and end and arrival_dt < end:
            return period

    prior_periods = [
        period
        for period in sorted_periods
        if (_ensure_utc(period.get("from_time")) or datetime.min.replace(tzinfo=timezone.utc))
        <= arrival_dt
    ]
    if prior_periods:
        return prior_periods[-1]

    return sorted_periods[0]


def _parse_wind_direction(direction_text: Optional[str]) -> Optional[int]:
    if direction_text in (None, ""):
        return None
    try:
        cleaned = str(direction_text).strip()
    except Exception:
        return None
    if not cleaned.isdigit():
        return None
    try:
        value = int(cleaned)
    except ValueError:
        return None
    if 0 <= value <= 360:
        return value
    return None


def _is_tailwind_direction(
    airport_code: Optional[str], wind_dir_text: Optional[str]
) -> bool:
    if not airport_code:
        return False
    airport = str(airport_code).strip().upper()
    if not airport or airport not in TAILWIND_DIRECTION_RANGES:
        return False
    wind_dir = _parse_wind_direction(wind_dir_text)
    if wind_dir is None:
        return False
    start, end = TAILWIND_DIRECTION_RANGES[airport]
    if start <= end:
        return start <= wind_dir <= end
    return wind_dir >= start or wind_dir <= end


_RSC_LINE_REGEX = re.compile(
    r"(?:^|\n)\s*(?:E\)\s*)?(RSC\s+\d{2}.*?\.)",
    re.IGNORECASE | re.DOTALL,
)
_RSC_VALUE_REGEX = re.compile(
    r"RSC\s+\d{2}[LRC]?\s+(\d)\s*(?:/|\s)\s*(\d)\s*(?:/|\s)\s*(\d)",
    re.IGNORECASE,
)
_FICON_LINE_REGEX = re.compile(
    r"(?:^|\n)\s*(?:E\)\s*)?([^\n]*\b(?:RWY|RUNWAY)\b[^\n]*\bFICON\b[^\n]*(?:\.|$))",
    re.IGNORECASE,
)
_FICON_VALUE_REGEX = re.compile(
    r"FICON\s+(\d)\s*(?:/|\s)\s*(\d)\s*(?:/|\s)\s*(\d)",
    re.IGNORECASE,
)


def _strip_french_translation(notam_text: str) -> str:
    if not notam_text:
        return notam_text

    lines = notam_text.splitlines()
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if re.match(r"^FR\s*:", stripped, re.IGNORECASE):
            return "\n".join(lines[:idx]).rstrip()
    return notam_text


def _extract_rsc_lines(notam_text: str) -> List[str]:
    matches = []
    for match in _RSC_LINE_REGEX.finditer(notam_text):
        cleaned = " ".join(match.group(1).split())
        if cleaned:
            matches.append(cleaned)
    return matches


def _summarize_rsc(lines: Sequence[str]) -> Tuple[str, List[str], str]:
    if not lines:
        return "yellow", [], "No RSC NOTAMs found."

    has_value = False
    has_low_digit = False
    for line in lines:
        match = _RSC_VALUE_REGEX.search(line)
        if not match:
            continue
        has_value = True
        digits = [int(value) for value in match.groups()]
        if any(value < 6 for value in digits):
            has_low_digit = True
            break

    if has_low_digit:
        return "red", list(lines), "RSC below 6/6/6."
    if has_value:
        return "green", list(lines), "All RSC values are 6/6/6."
    return "yellow", list(lines), "RSC NOTAM detected, but format was unexpected."


def _rsc_has_critical_digits(lines: Sequence[str]) -> bool:
    for line in lines:
        match = _RSC_VALUE_REGEX.search(line)
        if not match:
            continue
        digits = [int(value) for value in match.groups()]
        if any(value in {0, 1} for value in digits):
            return True
    return False


def _extract_ficon_lines(notam_text: str) -> List[str]:
    matches = []
    for match in _FICON_LINE_REGEX.finditer(notam_text):
        cleaned = " ".join(match.group(1).split())
        if not cleaned:
            continue
        if re.search(r"\b(TAXIWAY|TWY|APRON|RAMP)\b", cleaned, re.IGNORECASE):
            continue
        matches.append(cleaned)
    return matches


def _summarize_ficon(lines: Sequence[str]) -> Tuple[str, List[str], str]:
    if not lines:
        return "neutral", [], "No FICON NOTAMs found."

    has_value = False
    has_low_digit = False
    for line in lines:
        match = _FICON_VALUE_REGEX.search(line)
        if not match:
            continue
        has_value = True
        digits = [int(value) for value in match.groups()]
        if any(value < 5 for value in digits):
            has_low_digit = True
            break

    if has_low_digit:
        return "red", list(lines), "FICON below 5/5/5."
    if has_value:
        return "green", list(lines), "All FICON values are 5/5/5."
    return "yellow", list(lines), "FICON NOTAM detected, but format was unexpected."


def _ficon_has_critical_digits(lines: Sequence[str]) -> bool:
    for line in lines:
        match = _FICON_VALUE_REGEX.search(line)
        if not match:
            continue
        digits = [int(value) for value in match.groups()]
        if any(value in {0, 1} for value in digits):
            return True
    return False


def _arrival_within_rsc_window(
    arrival_dt: Optional[datetime],
    now_utc: datetime,
    *,
    window_hours: int = 5,
) -> bool:
    arrival_utc = _ensure_utc(arrival_dt)
    if arrival_utc is None:
        return False
    return now_utc <= arrival_utc <= now_utc + timedelta(hours=window_hours)


@st.cache_data(show_spinner=False, ttl=300)
def load_cfps_notams(codes: Tuple[str, ...]) -> Tuple[Dict[str, List[str]], List[str]]:
    results: Dict[str, List[str]] = {}
    errors: List[str] = []
    for icao in codes:
        if not icao or not icao.startswith("C"):
            continue
        try:
            url = "https://plan.navcanada.ca/weather/api/alpha/"
            params = {
                "site": icao,
                "alpha": ["notam"],
                "notam_choice": "default",
            }
            query_params = []
            for key, value in params.items():
                if isinstance(value, list):
                    for entry in value:
                        query_params.append((key, entry))
                else:
                    query_params.append((key, value))

            response = requests.get(url, params=query_params, timeout=20)
            response.raise_for_status()
            data = response.json()
            notams: List[str] = []
            for entry in data.get("data", []):
                if entry.get("type") != "notam":
                    continue
                text = entry.get("text") or ""
                try:
                    notam_json = json.loads(text)
                    notam_text = notam_json.get("raw", text)
                except json.JSONDecodeError:
                    notam_text = text
                notam_text = _strip_french_translation(notam_text)
                if notam_text:
                    notams.append(notam_text)
            results[icao] = notams
        except requests.RequestException as exc:
            results[icao] = []
            errors.append(f"{icao}: {exc}")
    return results, errors


@st.cache_data(show_spinner=False, ttl=300)
def load_faa_notams(
    codes: Tuple[str, ...],
    client_id: str,
    client_secret: str,
) -> Tuple[Dict[str, List[str]], List[str]]:
    results: Dict[str, List[str]] = {}
    errors: List[str] = []
    for icao in codes:
        if not icao or not icao.startswith("K"):
            continue
        try:
            url = "https://external-api.faa.gov/notamapi/v1/notams"
            headers = {
                "client_id": client_id,
                "client_secret": client_secret,
            }
            params = {
                "icaoLocation": icao.upper(),
                "responseFormat": "geoJson",
                "pageSize": 200,
            }
            items: List[Dict[str, Any]] = []
            page_cursor = None
            while True:
                if page_cursor:
                    params["pageCursor"] = page_cursor
                response = requests.get(url, headers=headers, params=params, timeout=20)
                response.raise_for_status()
                data = response.json()
                page_items = data.get("items", [])
                if isinstance(page_items, list):
                    items.extend(page_items)
                page_cursor = data.get("nextPageCursor")
                if not page_cursor:
                    break

            notams: List[str] = []
            for feature in items:
                props = feature.get("properties", {})
                core = props.get("coreNOTAMData", {})
                notam_data = core.get("notam", {})
                translations = core.get("notamTranslation", [])
                simple_text = None
                for translation in translations:
                    if translation.get("type") == "LOCAL_FORMAT":
                        simple_text = translation.get("simpleText")
                if simple_text:
                    normalized_simple = simple_text.strip().upper()
                    if normalized_simple == "NOT AVAILABLE" or normalized_simple.endswith(" NOT AVAILABLE"):
                        continue
                text_to_use = simple_text or notam_data.get("text", "")
                if not simple_text:
                    continue
                if text_to_use:
                    notams.append(text_to_use)
            results[icao] = notams
        except requests.RequestException as exc:
            results[icao] = []
            errors.append(f"{icao}: {exc}")
    return results, errors


def _summarise_period(
    period: Dict[str, Any],
    arrival_dt: Optional[datetime],
    airport_code: Optional[str],
    *,
    deice_status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    details_map = {label: value for label, value in period.get("details", [])}

    def _coerce(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        if isinstance(value, (list, tuple, set)):
            parts: List[str] = []
            for item in value:
                text = _coerce(item)
                if text:
                    parts.append(text)
            return " ".join(parts) if parts else None
        text = str(value).strip()
        return text or None

    summary: List[Dict[str, Any]] = []

    # prevailing conditions
    wind_dir = _coerce(details_map.get("Wind Dir (°)"))
    wind_speed = _coerce(details_map.get("Wind Speed (kt)"))
    wind_gust = _coerce(details_map.get("Wind Gust (kt)"))
    wind_parts: List[str] = []
    if wind_dir:
        wind_parts.append(wind_dir)
    if wind_speed:
        wind_parts.append(f"{wind_speed}kt")
    if wind_gust:
        wind_parts.append(f"G{wind_gust}")
    if wind_parts:
        wind_text = " ".join(wind_parts)
        tailwind = _is_tailwind_direction(airport_code, wind_dir)
        entry: Dict[str, Any] = {"label": "Wind", "value": wind_text}
        if tailwind:
            entry["value"] = f"{wind_text} — Tailwind (TAF)"
            entry["highlight"] = "red"
        summary.append(entry)

    for detail_key, label in (
        ("Visibility", "Visibility"),
        ("Weather", "Weather"),
        ("Clouds", "Clouds"),
    ):
        value = _coerce(details_map.get(detail_key))
        if value:
            entry: Dict[str, Any] = {"label": label, "value": value}
            if label == "Weather":
                weather_highlight = _get_weather_highlight(value, deice_status)
                if weather_highlight:
                    entry["highlight"] = weather_highlight
                if weather_highlight == "blue":
                    inline_html = _build_weather_value_html(value, deice_status)
                    if inline_html:
                        entry["value_html"] = inline_html
            summary.append(entry)

    # --- NEW: include only relevant TEMPO / PROB windows ---
    tempo_blocks = period.get("tempo", [])

    # normalize arrival to UTC so comparisons are sane
    arr_utc = _ensure_utc(arrival_dt)

    for tempo in tempo_blocks:
        tb_start = tempo.get("start")
        tb_end = tempo.get("end")

        # both of these may already be tz-aware UTC from the parser,
        # but normalize anyway for safety
        tb_start_utc = _ensure_utc(tb_start)
        tb_end_utc = _ensure_utc(tb_end)

        # Decide if this tempo/probability window is relevant.
        # We'll show it if:
        #   - we don't know arrival_dt (arr_utc is None), OR
        #   - arrival is inside [tb_start, tb_end)
        overlaps_arrival = False
        if arr_utc is None:
            overlaps_arrival = True
        else:
            # handle open-endeds gracefully
            start_ok = (tb_start_utc is None) or (arr_utc >= tb_start_utc)
            end_ok = (tb_end_utc is None) or (arr_utc < tb_end_utc)
            overlaps_arrival = start_ok and end_ok

        if not overlaps_arrival:
            # skip this tempo block, it's nowhere near our arrival time
            continue

        # Build human-readable text for the tempo window
        if isinstance(tb_start, datetime):
            tb_start_txt = _format_local(tb_start)
        else:
            tb_start_txt = "—"
        if isinstance(tb_end, datetime):
            tb_end_txt = _format_local(tb_end)
        else:
            tb_end_txt = "—"
        window_txt = (
            f"{tb_start_txt} – {tb_end_txt}"
            if tb_start_txt != "—" or tb_end_txt != "—"
            else "temporary window"
        )

        prob_prefix = tempo.get("prob") or "TEMPO"
        source_label = str(prob_prefix)

        # flatten tempo details similar to prevailing
        tempo_detail_map = {label: value for label, value in tempo.get("details", [])}
        tempo_bits: List[str] = []
        tempo_bits_html: List[str] = []
        tempo_bits_have_html = False

        def _append_tempo_bit(text: str, html_override: Optional[str] = None) -> None:
            nonlocal tempo_bits_have_html
            tempo_bits.append(text)
            if html_override is not None:
                tempo_bits_have_html = True
                tempo_bits_html.append(html_override)
            else:
                tempo_bits_html.append(html.escape(text))

        tempo_wind_dir = _coerce(tempo_detail_map.get("Wind Dir (°)"))
        tempo_wind_speed = _coerce(tempo_detail_map.get("Wind Speed (kt)"))
        tempo_wind_gust = _coerce(tempo_detail_map.get("Wind Gust (kt)"))
        tempo_wind_parts: List[str] = []
        if tempo_wind_dir:
            tempo_wind_parts.append(tempo_wind_dir)
        if tempo_wind_speed:
            tempo_wind_parts.append(f"{tempo_wind_speed}kt")
        if tempo_wind_gust:
            tempo_wind_parts.append(f"G{tempo_wind_gust}")
        if tempo_wind_parts:
            _append_tempo_bit("Wind " + " ".join(tempo_wind_parts))
        vis_t = _coerce(tempo_detail_map.get("Visibility"))
        wx_t = _coerce(tempo_detail_map.get("Weather"))
        clouds_t = _coerce(tempo_detail_map.get("Clouds"))
        if vis_t:
            _append_tempo_bit(f"Vis {vis_t}")
        weather_highlight = None
        if wx_t:
            weather_highlight = _get_weather_highlight(wx_t, deice_status)
            html_override = None
            if weather_highlight == "blue":
                html_override = _build_weather_value_html(wx_t, deice_status)
            _append_tempo_bit(wx_t, html_override)
        if clouds_t:
            _append_tempo_bit(clouds_t)

        tempo_highlight = _combine_highlight_levels(
            (
                _get_visibility_highlight(vis_t),
                weather_highlight,
                _get_ceiling_highlight(clouds_t) if clouds_t else None,
            )
        )

        tempo_tailwind = _is_tailwind_direction(airport_code, tempo_wind_dir)
        if tempo_tailwind:
            _append_tempo_bit(f"Tailwind ({source_label})")

        if tempo_bits:
            entry_value = "; ".join(tempo_bits)
            tempo_entry: Dict[str, Any] = {
                "label": f"{source_label} {window_txt}",
                "value": entry_value,
            }
            if tempo_bits_have_html:
                tempo_entry["value_html"] = "; ".join(tempo_bits_html)
            if tempo_tailwind:
                tempo_entry["highlight"] = "red"
            elif tempo_highlight:
                tempo_entry["highlight"] = tempo_highlight
            summary.append(tempo_entry)

    return summary


def _format_period_window(period: Dict[str, Any]) -> str:
    start = _ensure_utc(period.get("from_time"))
    end = _ensure_utc(period.get("to_time"))
    start_text = _format_local(start) if isinstance(start, datetime) else "—"
    end_text = _format_local(end) if isinstance(end, datetime) else "—"
    if start_text == "—" and end_text == "—":
        return "Timing unavailable"
    if end_text == "—":
        return f"From {start_text}"
    if start_text == "—":
        return f"Until {end_text}"
    return f"{start_text} – {end_text}"


def _format_detail_entry(
    entry: Mapping[str, Any]
) -> Optional[Tuple[str, str, Optional[str]]]:
    label = entry.get("label")
    if label is None:
        return None

    value = entry.get("value")
    value_html = entry.get("value_html")
    explicit_highlight = entry.get("highlight")
    label_lower = str(label).lower()

    display_highlight = explicit_highlight
    detection_highlight = explicit_highlight

    if value_html is not None:
        value_text = value_html
        detection_highlight = explicit_highlight or _determine_highlight_level(label, value)
        display_highlight = detection_highlight
        if display_highlight == "blue":
            display_highlight = None
    elif "cloud" in label_lower:
        value_text = _format_clouds_value(value)
        detection_highlight = explicit_highlight or _get_ceiling_highlight(value)
        display_highlight = detection_highlight
    else:
        value_text = html.escape(str(value)) if value is not None else ""
        detection_highlight = explicit_highlight or _determine_highlight_level(label, value)
        display_highlight = detection_highlight

    if display_highlight:
        value_text = _wrap_highlight_html(value_text, display_highlight)

    return str(label), value_text, detection_highlight


def _collect_highlight_entries(
    summary_items: Iterable[Mapping[str, Any]]
) -> Tuple[List[Tuple[str, str]], List[Dict[str, Any]]]:
    details: List[Tuple[str, str]] = []
    highlights: List[Dict[str, Any]] = []
    for entry in summary_items:
        formatted = _format_detail_entry(entry)
        if formatted is None:
            continue
        label, value_text, detection_highlight = formatted
        details.append((label, value_text))
        if detection_highlight:
            highlights.append(
                {
                    "label": label,
                    "value": value_text,
                    "highlight": detection_highlight,
                }
            )
    return details, highlights


def _build_taf_html(
    report: Optional[Dict[str, Any]],
    period: Optional[Dict[str, Any]],
    arrival_dt: Optional[datetime],
    airport_code: Optional[str],
    deice_status: Optional[str],
    *,
    prior_period: Optional[Dict[str, Any]] = None,
    prior_arrival_dt: Optional[datetime] = None,
) -> str:
    if report is None:
        return "<div class='taf taf-missing'>No TAF segment matched the arrival window.</div>"

    fallback_banner = ""
    if report.get("is_fallback"):
        fallback_station = str(report.get("station") or "").strip().upper()
        distance_val = _try_float(report.get("fallback_distance_nm"))
        distance_text = ""
        if distance_val is not None:
            distance_text = f" {int(round(distance_val))} nm away"
        station_text = html.escape(fallback_station) if fallback_station else "Unknown"
        banner_text = f"USING NEARBY TAF {station_text}{html.escape(distance_text)}"
        fallback_banner = f"<div class='taf-fallback-banner'>{banner_text}</div>"

    if period is None:
        raw_taf = html.escape(str(report.get("raw") or ""))
        issue_display = html.escape(str(report.get("issue_time_display") or ""))
        parts = [
            "<div class='taf'>",
            fallback_banner,
            "<div class='taf-missing'>No structured TAF segment matched the arrival window.</div>",
        ]
        if issue_display:
            parts.append(
                "<div style='font-size:0.75rem;color:#94a3b8;margin-top:0.3rem;'>"
                f"Issued {issue_display}"
                "</div>"
            )
        if raw_taf:
            parts.append("<details><summary>Raw TAF</summary><pre>")
            parts.append(raw_taf)
            parts.append("</pre></details>")
        parts.append("</div>")
        return "".join(parts)

    window_text = _format_period_window(period)
    arrival_dt = _ensure_utc(arrival_dt)
    warning_html = ""
    period_end = _ensure_utc(period.get("to_time"))
    if (
        arrival_dt is not None
        and period_end is not None
        and arrival_dt - period_end >= timedelta(hours=3)
    ):
        diff = arrival_dt - period_end
        total_minutes = int(diff.total_seconds() // 60)
        hours, minutes = divmod(total_minutes, 60)
        diff_parts: List[str] = []
        if hours:
            diff_parts.append(f"{hours}h")
        if minutes:
            diff_parts.append(f"{minutes}m")
        if not diff_parts:
            diff_parts.append("0m")
        diff_text = " ".join(diff_parts)
        end_local_text = _format_local(period_end)
        arrival_local_text = _format_local(arrival_dt)
        warning_html = (
            "<div class='taf-warning'>⚠️ Forecast window ends at "
            f"{html.escape(end_local_text)} ({html.escape(diff_text)} before arrival"
            f" at {html.escape(arrival_local_text)}).</div>"
        )
    summary_items = _summarise_period(
        period,
        arrival_dt,
        airport_code,
        deice_status=deice_status,
    )
    details, highlights = _collect_highlight_entries(summary_items)

    prior_highlights: List[Dict[str, Any]] = []
    if prior_period is not None and (arrival_dt is not None or prior_arrival_dt is not None):
        prior_summary = _summarise_period(
            prior_period,
            prior_arrival_dt or arrival_dt,
            airport_code,
            deice_status=deice_status,
        )
        _, prior_highlights = _collect_highlight_entries(prior_summary)

    lines: List[str] = []
    if fallback_banner:
        lines.append(fallback_banner)
    lines.append(f"<div><strong>Forecast window:</strong> {html.escape(window_text)}</div>")
    if warning_html:
        lines.append(warning_html)
    details_html = ""
    if details:
        detail_entries = [
            f"<li><strong>{html.escape(label)}:</strong> {value}</li>" for label, value in details
        ]
        details_html = "<ul>" + "".join(detail_entries) + "</ul>"

    prior_only_highlights: List[str] = []
    if prior_highlights:
        current_keys = {item["label"].lower() for item in highlights}
        for entry in prior_highlights:
            label = entry.get("label")
            if not label:
                continue
            if label.lower() in current_keys:
                continue
            prior_only_highlights.append(
                f"<li><strong>{html.escape(label)}:</strong> {entry['value']}</li>"
            )

    if prior_only_highlights:
        prior_html = "<ul>" + "".join(prior_only_highlights) + "</ul>"
        lines.append(
            "<div class='taf-warning'>⚠️ Highlights within 1 hour before arrival:"
            f" {prior_html}</div>"
        )
    issue_display = report.get("issue_time_display") or ""
    issue_html = (
        f"<div style='font-size:0.75rem;color:#94a3b8;margin-top:0.3rem;'>"
        f"Issued {html.escape(issue_display)}"
        "</div>"
        if issue_display
        else ""
    )
    raw_taf = report.get("raw") or ""
    raw_html = ""
    if raw_taf:
        raw_html = (
            "<details><summary>Raw TAF</summary><pre>"
            f"{html.escape(raw_taf)}"
            "</pre></details>"
        )
    return "".join(["<div class='taf'>", *lines, details_html, issue_html, raw_html, "</div>"])


def _build_flight_card(flight: Dict[str, Any], taf_html: str) -> str:
    route = f"{flight['departure_airport'] or '???'} → {flight['arrival_airport'] or '???'}"
    dep_line = f"Dep: {_format_local(flight['dep_dt_local'])} ({_format_utc(flight['dep_dt_utc'])})"
    arr_line = f"Arr: {_format_local(flight['arr_dt_local'])} ({_format_utc(flight['arr_dt_utc'])})"
    card_classes = ["flight-card"]
    actual_arrival_utc = _ensure_utc(flight.get("arr_actual_dt_utc"))
    arrival_utc = _ensure_utc(flight.get("arr_dt_utc"))
    arrival_state: Optional[str] = None
    past_flag_html = ""
    if actual_arrival_utc is not None:
        now_utc = datetime.now(timezone.utc)
        diff = now_utc - actual_arrival_utc
        arrival_state = "past"
        if diff >= timedelta(seconds=0):
            elapsed_text = _format_duration_short(diff)
            past_flag_html = (
                "<div class='past-flag'>"
                f"Landed {html.escape(elapsed_text)} ago"
                "</div>"
            )
        else:
            past_flag_html = "<div class='past-flag'>Landed</div>"
    elif arrival_utc is not None:
        now_utc = datetime.now(timezone.utc)
        if arrival_utc <= now_utc:
            elapsed = now_utc - arrival_utc
            arrival_state = "elapsed"
            elapsed_text = _format_duration_short(elapsed)
            past_flag_html = (
                "<div class='arrival-elapsed-flag'>"
                f"Estimated arrival time elapsed {html.escape(elapsed_text)} ago"
                "</div>"
            )
        else:
            remaining = arrival_utc - now_utc
            if remaining <= timedelta(minutes=30):
                arrival_state = "elapsed"
                remaining_text = _format_duration_short(remaining)
                past_flag_html = (
                    "<div class='arrival-elapsed-flag'>"
                    f"Estimated arrival in {html.escape(remaining_text)}"
                    "</div>"
                )
    if arrival_state == "past":
        card_classes.append("flight-card--past")
    elif arrival_state == "elapsed":
        card_classes.append("flight-card--arrival-elapsed")
    elif flight.get("is_today"):
        card_classes.append("flight-card--today")
    else:
        card_classes.append("flight-card--future")
    badges: List[str] = []
    if flight.get("flight_type"):
        badges.append(html.escape(str(flight["flight_type"])))
    if flight.get("account_name"):
        badges.append(html.escape(str(flight["account_name"])))
    if flight.get("pax") not in (None, ""):
        badges.append(f"PAX {html.escape(str(flight['pax']))}")

    rsc_status = flight.get("rsc_status")
    rsc_summary = flight.get("rsc_summary")
    rsc_lines = flight.get("rsc_lines") or []
    rsc_note = flight.get("rsc_note") or "No RSC details available."
    rsc_html = ""
    if rsc_status:
        if rsc_lines:
            rsc_body = (
                "<ul>"
                + "".join(f"<li>{html.escape(line)}</li>" for line in rsc_lines)
                + "</ul>"
            )
        else:
            rsc_body = f"<div class='flight-card__rsc-note'>{html.escape(rsc_note)}</div>"
        summary_text = f"RSC {rsc_summary}" if rsc_summary else "RSC"
        rsc_html = (
            "<details class='flight-card__rsc flight-card__rsc--"
            f"{html.escape(rsc_status)}'>"
            f"<summary>{html.escape(summary_text)}</summary>"
            f"<div class='flight-card__rsc-body'>{rsc_body}</div>"
            "</details>"
        )

    ficon_status = flight.get("ficon_status")
    ficon_summary = flight.get("ficon_summary")
    ficon_lines = flight.get("ficon_lines") or []
    ficon_note = flight.get("ficon_note") or "No FICON details available."
    ficon_html = ""
    if ficon_status:
        if ficon_lines:
            ficon_body = (
                "<ul>"
                + "".join(f"<li>{html.escape(line)}</li>" for line in ficon_lines)
                + "</ul>"
            )
        else:
            ficon_body = f"<div class='flight-card__rsc-note'>{html.escape(ficon_note)}</div>"
        summary_text = f"FICON {ficon_summary}" if ficon_summary else "FICON"
        ficon_html = (
            "<details class='flight-card__rsc flight-card__rsc--"
            f"{html.escape(ficon_status)}'>"
            f"<summary>{html.escape(summary_text)}</summary>"
            f"<div class='flight-card__rsc-body'>{ficon_body}</div>"
            "</details>"
        )

    badge_html = ""
    badge_items: List[str] = [f"<span class='badge'>{badge}</span>" for badge in badges]
    if rsc_html:
        badge_items.append(rsc_html)
    if ficon_html:
        badge_items.append(ficon_html)
    if badge_items:
        badge_html = "<div class='badge-strip'>" + "".join(badge_items) + "</div>"

    runway_html = ""
    longest_runway = flight.get("longest_runway_ft")
    deice_label = flight.get("deice_status_label")
    deice_code = flight.get("deice_status_code") or "unknown"
    if longest_runway or deice_label:
        runway_lines: List[str] = []
        if longest_runway:
            runway_lines.append(
                "<div class='flight-card__runway-text'>"
                f"Longest RWY {longest_runway:,} ft"
                "</div>"
            )
        if deice_label:
            runway_lines.append(
                "<div class='flight-card__deice flight-card__deice--"
                f"{html.escape(deice_code)}'>{html.escape(deice_label)}</div>"
            )
        runway_html = "<div class='flight-card__runway'>" + "".join(runway_lines) + "</div>"

    header_html = (
        "<div class='flight-card__header'>"
        f"<h4>{html.escape(route)}</h4>"
        f"{runway_html}"
        "</div>"
    )

    return (
        f"<div class='{' '.join(card_classes)}'>"
        f"{header_html}"
        f"{badge_html}"
        f"{past_flag_html}"
        f"<div class='times'>{html.escape(dep_line)}<br>{html.escape(arr_line)}</div>"
        f"{taf_html}"
        "</div>"
    )

with st.sidebar:
    st.header("Filters")
    default_start, default_end = _default_date_range()
    date_selection = st.date_input(
        "Arrival window (Mountain)",
        value=(default_start, default_end),
        help="Flights with arrivals inside this local date window will be shown.",
    )
    tail_selector_placeholder = st.empty()
    show_metadata = st.checkbox("Show FL3XX fetch metadata", value=False)


window_start_date, window_end_date = _normalise_date_range(date_selection)
fetch_to_date = window_end_date + timedelta(days=1)

fl3xx_settings_raw = st.secrets.get("fl3xx_api")
if not fl3xx_settings_raw:
    st.warning("Add FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to fetch flights.")
    st.stop()

try:
    fl3xx_settings = dict(fl3xx_settings_raw)
except (TypeError, ValueError):
    st.error("FL3XX API secrets must be provided as key/value pairs.")
    st.stop()

settings_digest = _settings_digest(fl3xx_settings)

try:
    flight_rows, metadata, normalization_stats = load_flight_rows(
        settings_digest,
        fl3xx_settings,
        from_date=window_start_date,
        to_date=fetch_to_date,
    )
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except requests.HTTPError as exc:
    st.error(f"FL3XX API request failed: {exc}")
    st.stop()

window_start_local = datetime.combine(window_start_date, time.min, tzinfo=MOUNTAIN_TZ)
window_end_local = datetime.combine(window_end_date + timedelta(days=1), time.min, tzinfo=MOUNTAIN_TZ)

runway_lengths = load_longest_runways()

processed_flights: List[Dict[str, Any]] = []
today_local_date = datetime.now(tz=MOUNTAIN_TZ).date()
for row in flight_rows:
    tail = _coerce_code(row.get("tail"))
    if not tail:
        continue
    arr_dt_utc = _extract_datetime(row, ARRIVAL_TIME_KEYS)
    arr_actual_dt_utc = _extract_datetime(row, ACTUAL_ARRIVAL_TIME_KEYS)
    dep_dt_utc = _extract_datetime(row, DEPARTURE_TIME_KEYS)
    arr_dt_local = _to_local(arr_dt_utc)
    dep_dt_local = _to_local(dep_dt_utc)
    candidate_dt = arr_dt_local or dep_dt_local
    if candidate_dt is not None:
        if candidate_dt < window_start_local or candidate_dt >= window_end_local:
            continue
    candidate_date = candidate_dt.date() if candidate_dt else None
    arrival_airport = _coerce_code(row.get("arrival_airport") or row.get("arrivalAirport") or row.get("airportTo"))
    departure_airport = _coerce_code(row.get("departure_airport") or row.get("departureAirport") or row.get("airportFrom"))
    deice_status = resolve_deice_status(arrival_airport)

    processed_flights.append(
        {
            "tail": tail,
            "arrival_airport": arrival_airport,
            "departure_airport": departure_airport,
            "arr_dt_utc": arr_dt_utc,
            "arr_actual_dt_utc": arr_actual_dt_utc,
            "dep_dt_utc": dep_dt_utc,
            "arr_dt_local": arr_dt_local,
            "dep_dt_local": dep_dt_local,
            "flight_type": row.get("flightType") or row.get("flight_type"),
            "account_name": row.get("accountName") or row.get("account"),
            "pax": row.get("paxNumber") or row.get("pax_count") or row.get("pax"),
            "raw": row,
            "local_service_date": candidate_date,
            "is_today": candidate_date == today_local_date,
            "longest_runway_ft": runway_lengths.get(arrival_airport or ""),
            "deice_status_code": deice_status.get("code"),
            "deice_status_label": deice_status.get("label"),
        }
    )

if not processed_flights:
    st.info("No flights found inside the selected arrival window.")
    if show_metadata:
        with st.expander("FL3XX fetch metadata"):
            st.json({"metadata": metadata, "normalization": normalization_stats})
    st.stop()

arrival_airports = sorted({f["arrival_airport"] for f in processed_flights if f["arrival_airport"]})
try:
    taf_reports = load_taf_reports(tuple(arrival_airports))
except requests.HTTPError as exc:
    st.warning(f"Failed to retrieve TAF data: {exc}")
    taf_reports = {}
except Exception as exc:
    st.warning(f"Unexpected error retrieving TAF data: {exc}")
    taf_reports = {}

now_utc = datetime.now(timezone.utc)
RSC_STANDARD_WINDOW_HOURS = 5
RSC_LOOKAHEAD_HOURS = 24
FAA_CLIENT_ID = st.secrets.get("FAA_CLIENT_ID")
FAA_CLIENT_SECRET = st.secrets.get("FAA_CLIENT_SECRET")
rsc_airports = sorted(
    {
        flight["arrival_airport"]
        for flight in processed_flights
        if flight.get("arrival_airport")
        and str(flight["arrival_airport"]).startswith("C")
        and _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_LOOKAHEAD_HOURS,
        )
    }
)

rsc_notams: Dict[str, List[str]] = {}
if rsc_airports:
    rsc_notams, rsc_errors = load_cfps_notams(tuple(rsc_airports))
    if rsc_errors:
        for message in rsc_errors:
            st.warning(f"RSC NOTAM fetch failed: {message}")

ficon_airports = sorted(
    {
        flight["arrival_airport"]
        for flight in processed_flights
        if flight.get("arrival_airport")
        and str(flight["arrival_airport"]).startswith("K")
        and _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_LOOKAHEAD_HOURS,
        )
    }
)

ficon_notams: Dict[str, List[str]] = {}
if ficon_airports:
    if not FAA_CLIENT_ID or not FAA_CLIENT_SECRET:
        st.warning("FAA NOTAM credentials missing; FICON status unavailable.")
    else:
        ficon_notams, ficon_errors = load_faa_notams(
            tuple(ficon_airports),
            FAA_CLIENT_ID,
            FAA_CLIENT_SECRET,
        )
        if ficon_errors:
            for message in ficon_errors:
                st.warning(f"FICON NOTAM fetch failed: {message}")

for flight in processed_flights:
    station_reports = taf_reports.get(flight["arrival_airport"], []) if flight["arrival_airport"] else []
    report, period = _select_forecast_period(station_reports, flight["arr_dt_utc"])
    flight["taf_report"] = report
    flight["taf_period"] = period
    prior_period: Optional[Dict[str, Any]] = None
    if flight.get("arr_dt_utc"):
        prior_dt = flight["arr_dt_utc"] - timedelta(hours=1)
        _, prior_period = _select_forecast_period(station_reports, prior_dt)
    flight["taf_prior_period"] = prior_period

    arrival_airport = flight.get("arrival_airport")
    if arrival_airport and str(arrival_airport).startswith("C"):
        arrival_actual = _ensure_utc(flight.get("arr_actual_dt_utc"))
        within_standard_window = _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_STANDARD_WINDOW_HOURS,
        )
        within_lookahead_window = _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_LOOKAHEAD_HOURS,
        )
        if arrival_actual is not None and arrival_actual <= now_utc:
            status = "neutral"
            lines = []
            note = "Arrival already landed."
        elif within_standard_window or within_lookahead_window:
            notam_texts = rsc_notams.get(arrival_airport, [])
            rsc_lines: List[str] = []
            seen = set()
            for text in notam_texts:
                for line in _extract_rsc_lines(text):
                    if line in seen:
                        continue
                    seen.add(line)
                    rsc_lines.append(line)
            status, lines, note = _summarize_rsc(rsc_lines)
            if not within_standard_window:
                if _rsc_has_critical_digits(rsc_lines):
                    status = "critical"
                    note = "RSC includes 0/1 values."
                else:
                    status = "neutral"
                    note = "RSC lookahead only highlights 0/1 values."
        else:
            status = "neutral"
            lines = []
            note = "RSC check runs within 24 hours of arrival."
        if status == "green":
            summary = "6/6/6"
        elif status == "critical":
            summary = "0/1"
        elif status == "red":
            summary = "<6"
        else:
            summary = ""
        flight["rsc_status"] = status
        flight["rsc_lines"] = lines
        flight["rsc_note"] = note
        flight["rsc_summary"] = summary
    elif arrival_airport and str(arrival_airport).startswith("K"):
        arrival_actual = _ensure_utc(flight.get("arr_actual_dt_utc"))
        within_standard_window = _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_STANDARD_WINDOW_HOURS,
        )
        within_lookahead_window = _arrival_within_rsc_window(
            flight.get("arr_dt_utc"),
            now_utc,
            window_hours=RSC_LOOKAHEAD_HOURS,
        )
        if arrival_actual is not None and arrival_actual <= now_utc:
            status = "neutral"
            lines = []
            note = "Arrival already landed."
        elif within_standard_window or within_lookahead_window:
            notam_texts = ficon_notams.get(arrival_airport, [])
            ficon_lines: List[str] = []
            seen = set()
            for text in notam_texts:
                for line in _extract_ficon_lines(text):
                    if line in seen:
                        continue
                    seen.add(line)
                    ficon_lines.append(line)
            status, lines, note = _summarize_ficon(ficon_lines)
            if not within_standard_window:
                if _ficon_has_critical_digits(ficon_lines):
                    status = "critical"
                    note = "FICON includes 0/1 values."
                else:
                    status = "neutral"
                    note = "FICON lookahead only highlights 0/1 values."
        else:
            status = "neutral"
            lines = []
            note = "FICON check runs within 24 hours of arrival."
        if status == "green":
            summary = "5/5/5"
        elif status == "critical":
            summary = "0/1"
        elif status == "red":
            summary = "<5"
        else:
            summary = ""
        flight["ficon_status"] = status
        flight["ficon_lines"] = lines
        flight["ficon_note"] = note
        flight["ficon_summary"] = summary

processed_flights.sort(
    key=lambda item: (
        _tail_order_key(item["tail"]),
        item["arr_dt_utc"] or item["dep_dt_utc"] or FAR_FUTURE,
    )
)

unique_tails: List[str] = []
seen_tails = set()
for flight in processed_flights:
    if flight["tail"] in seen_tails:
        continue
    seen_tails.add(flight["tail"])
    unique_tails.append(flight["tail"])

if not unique_tails:
    unique_tails = [flight["tail"] for flight in processed_flights]

if unique_tails:
    tail_options = ["All tails", *unique_tails]
    selected_tail = tail_selector_placeholder.selectbox("Tail", tail_options, index=0)
else:
    selected_tail = tail_selector_placeholder.selectbox("Tail", ["All tails"], index=0, disabled=True)

display_flights = processed_flights
if selected_tail != "All tails":
    display_flights = [flight for flight in processed_flights if flight["tail"] == selected_tail]

summary_text = (
    f"Displaying {len(display_flights)} flight(s) across {len({f['tail'] for f in display_flights})} tail(s) "
    f"and {len(arrival_airports)} arrival airport(s)."
)
st.caption(summary_text)

flights_by_tail: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
for flight in display_flights:
    flights_by_tail[flight["tail"]].append(flight)

for tail in sorted(flights_by_tail.keys(), key=_tail_order_key):
    tail_flights = flights_by_tail[tail]
    if not tail_flights:
        continue
    st.markdown(f"<div class='tail-header'>{tail}</div>", unsafe_allow_html=True)
    cards = []
    for flight in tail_flights:
        taf_html = _build_taf_html(
            flight.get("taf_report"),
            flight.get("taf_period"),
            flight.get("arr_dt_utc"),
            flight.get("arrival_airport"),
            flight.get("deice_status_code"),
            prior_period=flight.get("taf_prior_period"),
            prior_arrival_dt=(
                flight.get("arr_dt_utc") - timedelta(hours=1)
                if flight.get("arr_dt_utc")
                else None
            ),
        )
        cards.append(_build_flight_card(flight, taf_html))
    st.markdown(f"<div class='flight-row'>{''.join(cards)}</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

if show_metadata:
    with st.expander("FL3XX fetch metadata"):
        st.json({"metadata": metadata, "normalization": normalization_stats})
