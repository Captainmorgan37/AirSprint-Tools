"""Streamlit app to highlight flights with missing crew qualifications."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import requests
import streamlit as st

from fl3xx_api import (
    MOUNTAIN_TIME_ZONE,
    extract_missing_qualifications_from_preflight,
    fetch_flights,
    fetch_preflight,
)
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
    safe_parse_dt,
)
from Home import configure_page, password_gate, render_sidebar


configure_page(page_title="Crew Qualification Monitor")
password_gate()
render_sidebar()

st.title("🛫 Crew Qualification Monitor")
st.write(
    """Use this tool to review upcoming flights for missing crew qualifications.
    Select a date window (defaults to today plus the next three days) and the app
    will fetch matching flights from FL3XX, skip add-line placeholders and
    subcharters, and highlight any crew members with outstanding qualification
    requirements."""
)


def _settings_digest(settings: Mapping[str, Any]) -> str:
    def _normalise(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(k): _normalise(v) for k, v in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [_normalise(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    normalised = {str(k): _normalise(v) for k, v in settings.items()}
    encoded = json.dumps(normalised, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalise_date_range(selection: Any, default_start: date, default_end: date) -> Tuple[date, date]:
    if isinstance(selection, tuple):
        if len(selection) == 2:
            start, end = selection
        elif len(selection) == 1:
            start = end = selection[0]
        else:
            start, end = default_start, default_end
    else:
        start = end = selection if isinstance(selection, date) else default_start

    if start > end:
        start, end = end, start

    return start, end


_PLACEHOLDER_PREFIXES = {"ADD", "REMOVE"}


def _is_add_line_leg(leg: Mapping[str, Any]) -> bool:
    tail_value = leg.get("tail")
    if tail_value is None:
        return False
    tail_text = str(tail_value).strip()
    if not tail_text:
        return False
    first_word = tail_text.split()[0].upper()
    return first_word in _PLACEHOLDER_PREFIXES


def _filter_out_add_lines(rows: Iterable[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    filtered: List[Dict[str, Any]] = []
    skipped = 0
    for row in rows:
        if _is_add_line_leg(row):
            skipped += 1
            continue
        filtered.append(dict(row))
    return filtered, skipped


def _normalise_identifier(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


def _parse_leg_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    dt = safe_parse_dt(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MOUNTAIN_TIME_ZONE)


def _format_local(dt: Optional[datetime]) -> str:
    if not dt:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def _coerce_code(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip().upper()


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def load_filtered_legs(
    settings_digest: str,
    settings: Dict[str, Any],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    _ = settings_digest  # participate in the cache key without storing secrets

    config = build_fl3xx_api_config(settings)
    flights, metadata = fetch_flights(config, from_date=from_date, to_date=to_date)

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    non_subcharter_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    filtered_rows, skipped_add = _filter_out_add_lines(non_subcharter_rows)

    metadata = {
        **metadata,
        "flights_returned": len(flights),
        "legs_after_filter": len(filtered_rows),
        "skipped_subcharter": skipped_subcharter,
        "skipped_add_lines": skipped_add,
    }

    normalization_stats = {**normalization_stats, "skipped_add_lines": skipped_add}
    return filtered_rows, metadata, normalization_stats


def _select_primary_leg(legs: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not legs:
        return {}

    def _sort_key(leg: Mapping[str, Any]) -> Tuple[int, datetime]:
        dep_dt = _parse_leg_time(leg.get("dep_time"))
        if dep_dt is None:
            return (1, datetime.max.replace(tzinfo=timezone.utc))
        return (0, dep_dt)

    return min(legs, key=_sort_key)


with st.sidebar:
    st.header("Flight selection")
    today_local = datetime.now(tz=MOUNTAIN_TIME_ZONE).date()
    default_end = today_local + timedelta(days=3)
    date_selection = st.date_input(
        "Departure window (Mountain)",
        value=(today_local, default_end),
        help="Flights departing within this inclusive local date range will be inspected.",
    )
    start_date, end_date = _normalise_date_range(date_selection, today_local, default_end)
    fetch_to_date = end_date + timedelta(days=1)
    show_metadata = st.checkbox("Show fetch details", value=False)


fl3xx_settings_raw = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
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
    legs, fetch_metadata, normalization_stats = load_filtered_legs(
        settings_digest,
        fl3xx_settings,
        from_date=start_date,
        to_date=fetch_to_date,
    )
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except requests.HTTPError as exc:
    st.error(f"FL3XX API request failed: {exc}")
    st.stop()

if show_metadata:
    with st.expander("FL3XX fetch metadata", expanded=False):
        st.json(fetch_metadata)
    with st.expander("Normalization stats", expanded=False):
        st.json(normalization_stats)

if not legs:
    st.info("No matching flights were found for the selected window after filtering add lines and subcharters.")
    st.stop()

legs_by_flight: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
missing_flight_id = 0
for leg in legs:
    flight_id = _normalise_identifier(leg.get("flightId") or leg.get("flight_id") or leg.get("id"))
    if not flight_id:
        missing_flight_id += 1
        continue
    legs_by_flight[flight_id].append(leg)

if missing_flight_id:
    st.warning(
        f"Skipped {missing_flight_id} leg{'s' if missing_flight_id != 1 else ''} without a flight identifier; "
        "preflight checks require a flight ID."
    )

config = build_fl3xx_api_config(fl3xx_settings)

total_flights = len(legs_by_flight)
if total_flights == 0:
    st.info("No flights with valid identifiers were available for qualification checks.")
    st.stop()

status_placeholder = st.empty()
progress_bar = st.progress(0)

missing_rows: List[Dict[str, Any]] = []
errors: List[Dict[str, str]] = []

session: Optional[requests.Session] = None
try:
    for index, (flight_id, flight_legs) in enumerate(legs_by_flight.items(), start=1):
        status_placeholder.info(f"Fetching preflight {index}/{total_flights} (Flight {flight_id})")
        progress_bar.progress(index / total_flights)

        if session is None:
            session = requests.Session()

        try:
            payload = fetch_preflight(config, flight_id, session=session)
        except requests.HTTPError as exc:
            errors.append({"flight_id": flight_id, "error": str(exc)})
            continue
        except Exception as exc:  # pragma: no cover - defensive path
            errors.append({"flight_id": flight_id, "error": str(exc)})
            continue

        alerts = extract_missing_qualifications_from_preflight(payload)
        if not alerts:
            continue

        primary_leg = _select_primary_leg(flight_legs)
        dep_local = _parse_leg_time(primary_leg.get("dep_time"))
        arr_local = _parse_leg_time(primary_leg.get("arrival_time"))
        dep_airport = _coerce_code(
            primary_leg.get("departure_airport")
            or primary_leg.get("departureAirport")
            or primary_leg.get("airportFrom")
        )
        arr_airport = _coerce_code(
            primary_leg.get("arrival_airport")
            or primary_leg.get("arrivalAirport")
            or primary_leg.get("airportTo")
        )
        tail = str(primary_leg.get("tail") or "").strip()
        leg_id = primary_leg.get("leg_id") or primary_leg.get("legId") or ""

        for alert in alerts:
            missing_rows.append(
                {
                    "Flight ID": flight_id,
                    "Leg": leg_id,
                    "Tail": tail or "—",
                    "Route": f"{dep_airport or 'UNK'} → {arr_airport or 'UNK'}",
                    "Departure": _format_local(dep_local),
                    "Arrival": _format_local(arr_local),
                    "Seat": alert.seat,
                    "Crew member": alert.pilot_name or "Unknown",
                    "Missing qualification": alert.qualification_name,
                }
            )
finally:
    status_placeholder.empty()
    progress_bar.empty()
    if session is not None:
        session.close()

if errors:
    with st.expander("Preflight request errors", expanded=False):
        st.json(errors)

if not missing_rows:
    st.success("No missing crew qualifications detected for the selected flights.")
else:
    issues_df = pd.DataFrame(missing_rows)
    st.warning(
        f"Detected {len(missing_rows)} missing qualification entry{'ies' if len(missing_rows) != 1 else ''} "
        f"across {len({row['Flight ID'] for row in missing_rows})} flight{'s' if len({row['Flight ID'] for row in missing_rows}) != 1 else ''}."
    )
    st.dataframe(issues_df, use_container_width=True, hide_index=True)
