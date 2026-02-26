"""Streamlit app to highlight flights with missing crew qualifications."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

from fl3xx_api import (
    MOUNTAIN_TIME_ZONE,
    extract_conflicts_from_preflight,
    extract_missing_qualifications_from_preflight,
    fetch_flights,
    fetch_preflight,
)
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    is_canadian_country,
    load_airport_metadata_lookup,
    normalize_fl3xx_payload,
    safe_parse_dt,
    load_airport_tz_lookup,
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


_AIRPORT_TZ_LOOKUP = load_airport_tz_lookup()
_AIRPORT_METADATA_LOOKUP = load_airport_metadata_lookup()


def _lookup_airport_country(airport_code: str) -> Optional[str]:
    if not airport_code:
        return None
    record = _AIRPORT_METADATA_LOOKUP.get(airport_code)
    if not isinstance(record, Mapping):
        return None
    country = record.get("country")
    if isinstance(country, str) and country.strip():
        return country.strip()
    return None


def _should_include_missing_qualification(
    qualification_name: str,
    *,
    departure_airport: str,
    arrival_airport: str,
) -> bool:
    normalized = qualification_name.strip().upper()
    if normalized.startswith("AQ-"):
        return False
    if "CANPASS" in normalized:
        dep_country = _lookup_airport_country(departure_airport)
        arr_country = _lookup_airport_country(arrival_airport)
        if not dep_country or not arr_country:
            return False
        return is_canadian_country(arr_country) and not is_canadian_country(dep_country)
    return True


def _resolve_airport_timezone(airport_code: Optional[str], fallback: timezone) -> timezone:
    if airport_code:
        tz_name = _AIRPORT_TZ_LOOKUP.get(airport_code)
        if tz_name:
            try:
                return ZoneInfo(tz_name)
            except Exception:  # pragma: no cover - fallback to default if zone data missing
                pass
    return fallback


def _parse_leg_time(value: Any, airport_code: Optional[str] = None) -> Optional[datetime]:
    if value in (None, ""):
        return None
    dt = safe_parse_dt(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    target_tz = _resolve_airport_timezone(airport_code, dt.tzinfo or MOUNTAIN_TIME_ZONE)
    return dt.astimezone(target_tz)


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


@st.cache_data(show_spinner=False, ttl=300, hash_funcs={dict: lambda _: "0"})
def analyze_preflight_results(
    analysis_digest: str,
    settings: Dict[str, Any],
    legs_by_flight_items: Sequence[Tuple[str, List[Dict[str, Any]]]],
) -> Dict[str, List[Dict[str, Any]]]:
    _ = analysis_digest

    config = build_fl3xx_api_config(settings)
    total_flights = len(legs_by_flight_items)

    missing_rows: List[Dict[str, Any]] = []
    conflict_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    session: Optional[requests.Session] = None
    try:
        for flight_id, flight_legs in legs_by_flight_items:
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
            conflicts = extract_conflicts_from_preflight(payload)
            if not alerts and not conflicts:
                continue

            primary_leg = _select_primary_leg(flight_legs)
            booking_identifier = _normalise_identifier(
                primary_leg.get("bookingIdentifier") or primary_leg.get("booking_identifier")
            )
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
            dep_local = _parse_leg_time(primary_leg.get("dep_time"), dep_airport)
            arr_local = _parse_leg_time(primary_leg.get("arrival_time"), arr_airport)
            tail = str(primary_leg.get("tail") or "").strip()
            leg_id = primary_leg.get("leg_id") or primary_leg.get("legId") or ""

            display_identifier = booking_identifier or flight_id

            for alert in alerts:
                if not _should_include_missing_qualification(
                    alert.qualification_name,
                    departure_airport=dep_airport or "",
                    arrival_airport=arr_airport or "",
                ):
                    continue
                missing_rows.append(
                    {
                        "Booking Identifier": display_identifier,
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

            for conflict in conflicts:
                conflict_rows.append(
                    {
                        "Booking Identifier": display_identifier,
                        "Leg": leg_id,
                        "Tail": tail or "—",
                        "Route": f"{dep_airport or 'UNK'} → {arr_airport or 'UNK'}",
                        "Departure": _format_local(dep_local),
                        "Arrival": _format_local(arr_local),
                        "Seat": conflict.seat or "—",
                        "Type": conflict.category,
                        "Status": conflict.status,
                        "Conflict": conflict.description,
                    }
                )
    finally:
        if session is not None:
            session.close()

    return {
        "missing_rows": missing_rows,
        "conflict_rows": conflict_rows,
        "errors": errors,
        "total_flights": total_flights,
    }


_FETCH_RESULTS_KEY = "crew_qualification_fetch_results"
with st.sidebar:
    st.header("Flight selection")
    today_local = datetime.now(tz=MOUNTAIN_TIME_ZONE).date()
    default_end = today_local + timedelta(days=3)

    with st.form("crew_qualification_fetch"):
        date_selection = st.date_input(
            "Departure window (Mountain)",
            value=(today_local, default_end),
            help="Flights departing within this inclusive local date range will be inspected.",
        )
        submit_fetch = st.form_submit_button("Fetch flights", width="stretch")

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

fetch_results = st.session_state.get(_FETCH_RESULTS_KEY)

if submit_fetch:
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

    fetch_results = {
        "legs": legs,
        "fetch_metadata": fetch_metadata,
        "normalization_stats": normalization_stats,
    }
    st.session_state[_FETCH_RESULTS_KEY] = fetch_results
    st.session_state.pop(_ANALYSIS_RESULTS_KEY, None)

if not fetch_results:
    st.info('Select a departure window and press "Fetch flights" to retrieve data from FL3XX.')
    st.stop()

legs = fetch_results["legs"]
fetch_metadata = fetch_results["fetch_metadata"]
normalization_stats = fetch_results["normalization_stats"]

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

total_flights = len(legs_by_flight)
if total_flights == 0:
    st.info("No flights with valid identifiers were available for qualification checks.")
    st.stop()

analysis_signature = {
    "settings_digest": settings_digest,
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "flight_ids": sorted(legs_by_flight.keys()),
}
analysis_digest = _settings_digest(analysis_signature)
legs_by_flight_items = sorted((flight_id, flight_legs) for flight_id, flight_legs in legs_by_flight.items())

status_placeholder = st.empty()
progress_bar = st.progress(0)
status_placeholder.info(f"Fetching preflights for {total_flights} flight{'s' if total_flights != 1 else ''}…")
progress_bar.progress(0.5)
analysis_results = analyze_preflight_results(
    analysis_digest,
    fl3xx_settings,
    legs_by_flight_items,
)
progress_bar.progress(1.0)
status_placeholder.empty()
progress_bar.empty()

missing_rows = list(analysis_results.get("missing_rows", []))
conflict_rows = list(analysis_results.get("conflict_rows", []))
errors = list(analysis_results.get("errors", []))
if errors:
    with st.expander("Preflight request errors", expanded=False):
        st.json(errors)

if not missing_rows:
    st.success("No missing crew qualifications detected for the selected flights.")
else:
    issues_df = pd.DataFrame(missing_rows)
    st.warning(
        f"Detected {len(missing_rows)} missing qualification entry{'ies' if len(missing_rows) != 1 else ''} "
        f"across {len({row['Booking Identifier'] for row in missing_rows})} booking{'s' if len({row['Booking Identifier'] for row in missing_rows}) != 1 else ''}."
    )
    st.dataframe(issues_df, width="stretch", hide_index=True)

if conflict_rows:
    st.error(
        f"Detected {len(conflict_rows)} preflight conflict entry{'ies' if len(conflict_rows) != 1 else ''} "
        f"across {len({row['Booking Identifier'] for row in conflict_rows})} booking{'s' if len({row['Booking Identifier'] for row in conflict_rows}) != 1 else ''}."
    )
    conflict_df = pd.DataFrame(conflict_rows)
    st.dataframe(conflict_df, width="stretch", hide_index=True)
else:
    st.success("No preflight conflicts detected for the selected flights.")
