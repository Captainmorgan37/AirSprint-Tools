import hashlib
import html
import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests
import streamlit as st

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
    "ADD EMB WEST",
    "ADD EMB EAST",
    "ADD CJ2+ WEST",
    "ADD CJ2+ EAST",
    "ADD CJ3+ WEST",
    "ADD CJ3+ EAST",
)
TAIL_INDEX = {tail: idx for idx, tail in enumerate(TAIL_DISPLAY_ORDER)}
FAR_FUTURE = datetime.max.replace(tzinfo=timezone.utc)

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
    "arr_time",
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
    .flight-card h4 {margin:0 0 0.35rem 0; font-size:1.05rem; color:#f8fafc;}
    .flight-card .times {font-family:"Source Code Pro", Menlo, Consolas, monospace; font-size:0.9rem;
                         margin-bottom:0.45rem; line-height:1.35; color:#cbd5f5;}
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
    .tail-header {font-size:1.2rem; margin:0.5rem 0 0.4rem 0; padding-left:0.1rem; color:#e0f2fe;}
    .section-divider {border-bottom:1px solid rgba(148,163,184,0.25); margin:0.75rem 0 1.1rem 0;}
    </style>
    """,
    unsafe_allow_html=True,
)

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


def _coerce_code(value: Any) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


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


def _summarise_period(period: Dict[str, Any]) -> List[Tuple[str, str]]:
    details_map = {label: value for label, value in period.get("details", [])}

    def _coerce(value: Any) -> Optional[str]:
        if value in (None, "", []):
            return None
        return str(value)

    summary: List[Tuple[str, str]] = []

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
        summary.append(("Wind", " ".join(wind_parts)))

    for detail_key, label in (
        ("Visibility", "Visibility"),
        ("Weather", "Weather"),
        ("Clouds", "Clouds"),
        ("Probability (%)", "Probability"),
        ("Icing", "Icing"),
        ("Turbulence", "Turbulence"),
    ):
        value = _coerce(details_map.get(detail_key))
        if value:
            summary.append((label, value))

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


def _build_taf_html(
    report: Optional[Dict[str, Any]],
    period: Optional[Dict[str, Any]],
    arrival_dt: Optional[datetime] = None,
) -> str:
    if report is None:
        return "<div class='taf taf-missing'>No TAF segment matched the arrival window.</div>"

    if period is None:
        raw_taf = html.escape(str(report.get("raw") or ""))
        issue_display = html.escape(str(report.get("issue_time_display") or ""))
        parts = [
            "<div class='taf'>",
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
    summary_items = _summarise_period(period)

    lines = [f"<div><strong>Forecast window:</strong> {html.escape(window_text)}</div>"]
    if warning_html:
        lines.append(warning_html)
    details_html = ""
    if summary_items:
        detail_entries = "".join(
            f"<li><strong>{html.escape(label)}:</strong> {html.escape(str(value))}</li>"
            for label, value in summary_items
        )
        details_html = f"<ul>{detail_entries}</ul>"
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
    if flight.get("is_today"):
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

    badge_html = ""
    if badges:
        badge_html = "<div class='badge-strip'>" + "".join(
            f"<span class='badge'>{badge}</span>" for badge in badges
        ) + "</div>"

    return (
        f"<div class='{' '.join(card_classes)}'>"
        f"<h4>{html.escape(route)}</h4>"
        f"{badge_html}"
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

processed_flights: List[Dict[str, Any]] = []
today_local_date = datetime.now(tz=MOUNTAIN_TZ).date()
for row in flight_rows:
    tail = _coerce_code(row.get("tail"))
    if not tail:
        continue
    arr_dt_utc = _extract_datetime(row, ARRIVAL_TIME_KEYS)
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
    processed_flights.append(
        {
            "tail": tail,
            "arrival_airport": arrival_airport,
            "departure_airport": departure_airport,
            "arr_dt_utc": arr_dt_utc,
            "dep_dt_utc": dep_dt_utc,
            "arr_dt_local": arr_dt_local,
            "dep_dt_local": dep_dt_local,
            "flight_type": row.get("flightType") or row.get("flight_type"),
            "account_name": row.get("accountName") or row.get("account"),
            "pax": row.get("paxNumber") or row.get("pax_count") or row.get("pax"),
            "raw": row,
            "local_service_date": candidate_date,
            "is_today": candidate_date == today_local_date,
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

for flight in processed_flights:
    station_reports = taf_reports.get(flight["arrival_airport"], []) if flight["arrival_airport"] else []
    report, period = _select_forecast_period(station_reports, flight["arr_dt_utc"])
    flight["taf_report"] = report
    flight["taf_period"] = period

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
        )
        cards.append(_build_flight_card(flight, taf_html))
    st.markdown(f"<div class='flight-row'>{''.join(cards)}</div>", unsafe_allow_html=True)
    st.markdown("<div class='section-divider'></div>", unsafe_allow_html=True)

if show_metadata:
    with st.expander("FL3XX fetch metadata"):
        st.json({"metadata": metadata, "normalization": normalization_stats})
