from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import requests
import streamlit as st

from fl3xx_api import MOUNTAIN_TIME_ZONE, fetch_flights
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    filter_rows_by_departure_window,
    normalize_fl3xx_payload,
    safe_parse_dt,
)
from Home import configure_page, password_gate, render_sidebar


UTC = timezone.utc
_TAILS_PATH = Path(__file__).resolve().parent.parent / "tails.csv"
_ADD_PREFIXES = {"ADD", "REMOVE"}
_FETCH_RESULTS_KEY = "schedule_app_fetch_results"


def _normalise_tail(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return text.replace("-", "")


def _is_add_line(tail_text: Optional[str]) -> bool:
    if not tail_text:
        return False
    first_token = tail_text.split()[0]
    return first_token in _ADD_PREFIXES


def _first(values: Mapping[str, Any], *keys: str) -> Optional[Any]:
    for key in keys:
        value = values.get(key)
        if value not in (None, ""):
            return value
    return None


def _parse_utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    dt = safe_parse_dt(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt


def _format_dt(dt: Optional[datetime], *, tz: timezone) -> str:
    if not dt:
        return "—"
    return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M %Z")


def _settings_digest(settings: Mapping[str, Any]) -> str:
    def _serialise(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(k): _serialise(v) for k, v in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [_serialise(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    payload = {str(k): _serialise(v) for k, v in settings.items()}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def fetch_schedule_rows(
    settings_digest: str,
    settings: Dict[str, Any],
    *,
    start_utc: datetime,
    end_utc: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], Dict[str, int]]:
    _ = settings_digest  # participate in the cache key without storing secrets

    config = build_fl3xx_api_config(settings)
    from_date = start_utc.date()
    to_date_exclusive = (end_utc + timedelta(days=1)).date()

    flights, metadata = fetch_flights(config, from_date=from_date, to_date=to_date_exclusive)
    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    non_subcharter_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    window_rows, window_stats = filter_rows_by_departure_window(non_subcharter_rows, start_utc, end_utc)

    metadata = {
        **metadata,
        "flights_returned": len(flights),
        "legs_after_subcharter": len(non_subcharter_rows),
        "legs_within_window": len(window_rows),
        "skipped_subcharter": skipped_subcharter,
    }
    normalization_stats = {**normalization_stats, "skipped_subcharter": skipped_subcharter}

    return window_rows, metadata, normalization_stats, window_stats


def _load_tail_set(path: Path) -> Iterable[str]:
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path)
    except Exception:
        return []
    if "Tail" not in df.columns:
        return []
    tails = df["Tail"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)
    return {tail for tail in tails if tail}


def _classify_rows(
    rows: Iterable[Mapping[str, Any]],
    tail_codes: Iterable[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    real_tail_set = {str(code).strip().upper() for code in tail_codes if str(code).strip()}

    scheduled: List[Dict[str, Any]] = []
    unscheduled: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []

    for row in rows:
        tail_raw = row.get("tail")
        tail_normalised = _normalise_tail(tail_raw)
        is_add = _is_add_line(tail_normalised)

        entry = dict(row)
        entry["tail_normalized"] = tail_normalised
        entry["is_add_line"] = is_add

        if tail_normalised and tail_normalised in real_tail_set:
            scheduled.append(entry)
        elif is_add:
            unscheduled.append(entry)
        else:
            other.append(entry)

    return scheduled, unscheduled, other


def _build_display_table(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    display_rows: List[Dict[str, Any]] = []

    for row in rows:
        dep_dt = _parse_utc(row.get("dep_time"))
        arr_dt = _parse_utc(
            _first(
                row,
                "arrival_time",
                "arrivalTimeUtc",
                "arrivalScheduledUtc",
                "arrivalActualUtc",
                "arrivalUtc",
                "onBlockUtc",
            )
        )

        display_rows.append(
            {
                "Tail": row.get("tail"),
                "Tail (normalized)": row.get("tail_normalized"),
                "Account": row.get("accountName") or row.get("account"),
                "Workflow": row.get("workflowCustomName") or row.get("workflow"),
                "Flight Type": row.get("flightType"),
                "Booking": row.get("bookingReference") or row.get("bookingIdentifier"),
                "From": row.get("departure_airport"),
                "To": row.get("arrival_airport"),
                "Departure (UTC)": _format_dt(dep_dt, tz=UTC),
                "Departure (Mountain)": _format_dt(dep_dt, tz=MOUNTAIN_TIME_ZONE),
                "Arrival (UTC)": _format_dt(arr_dt, tz=UTC),
                "Arrival (Mountain)": _format_dt(arr_dt, tz=MOUNTAIN_TIME_ZONE),
                "PAX": row.get("paxNumber"),
                "Leg ID": row.get("leg_id"),
                "Flight ID": row.get("flightId"),
            }
        )

    if not display_rows:
        return pd.DataFrame()

    df = pd.DataFrame(display_rows)
    if "Departure (UTC)" in df.columns:
        df = df.sort_values(by="Departure (UTC)")
    return df.reset_index(drop=True)


configure_page(page_title="Schedule Snapshot")
password_gate()
render_sidebar()

st.title("🗓️ Schedule Snapshot (FL3XX)")
st.caption(
    "Select a target date to load all FL3XX flights departing between 08:00Z on that day"
    " and 08:00Z the following day. Real-tail flights are counted as scheduled and add-line"
    " placeholders are tracked separately as unscheduled demand."
)

with st.sidebar:
    st.header("Flight window")
    today_mountain = datetime.now(tz=MOUNTAIN_TIME_ZONE).date()
    selected_date = st.date_input(
        "Target date (08:00Z window)",
        value=today_mountain,
        help="The app will fetch flights departing between 08:00Z on this date and 08:00Z the next day.",
    )
    fetch_clicked = st.button("Fetch schedule", use_container_width=True)
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

if isinstance(selected_date, date):
    start_utc = datetime.combine(selected_date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8)
else:
    start_utc = datetime.now(tz=UTC).replace(hour=8, minute=0, second=0, microsecond=0)
end_utc = start_utc + timedelta(days=1)

if fetch_clicked:
    try:
        rows, metadata, normalization_stats, window_stats = fetch_schedule_rows(
            settings_digest,
            fl3xx_settings,
            start_utc=start_utc,
            end_utc=end_utc,
        )
    except FlightDataError as exc:
        st.error(str(exc))
        st.stop()
    except requests.HTTPError as exc:
        st.error(f"FL3XX API request failed: {exc}")
        st.stop()
    except Exception as exc:  # pragma: no cover - defensive fallback
        st.error(f"Unexpected error while fetching flights: {exc}")
        st.stop()

    st.session_state[_FETCH_RESULTS_KEY] = {
        "rows": rows,
        "metadata": metadata,
        "normalization": normalization_stats,
        "window_stats": window_stats,
        "start_utc": start_utc,
        "end_utc": end_utc,
    }

results = st.session_state.get(_FETCH_RESULTS_KEY)
if not results:
    st.info('Select a target date and click "Fetch schedule" to load flights from FL3XX.')
    st.stop()

rows: List[Dict[str, Any]] = results["rows"]
metadata = results["metadata"]
normalization_stats = results["normalization"]
window_stats = results["window_stats"]
start_utc = results["start_utc"]
end_utc = results["end_utc"]

if show_metadata:
    with st.expander("FL3XX fetch metadata", expanded=False):
        st.json(metadata)
    with st.expander("Normalization stats", expanded=False):
        st.json(normalization_stats)
    with st.expander("Window filter stats", expanded=False):
        st.json(window_stats)

if not rows:
    st.info("No flights matched the selected 08:00Z departure window after filtering subcharters.")
    st.stop()

tail_codes = _load_tail_set(_TAILS_PATH)
scheduled_rows, unscheduled_rows, other_rows = _classify_rows(rows, tail_codes)

col1, col2, col3 = st.columns(3)
col1.metric("Scheduled (real tails)", len(scheduled_rows))
col2.metric("Unscheduled (add lines)", len(unscheduled_rows))
col3.metric("Other", len(other_rows))

st.markdown(
    f"**Window:** {start_utc.strftime('%Y-%m-%d %H:%M %Z')} → {end_utc.strftime('%Y-%m-%d %H:%M %Z')}"
)

if not tail_codes:
    st.warning("`tails.csv` was not found or is empty. Real-tail detection may be incomplete.")

scheduled_df = _build_display_table(scheduled_rows)
unscheduled_df = _build_display_table(unscheduled_rows)
other_df = _build_display_table(other_rows)

if not scheduled_df.empty:
    st.subheader("Scheduled flights")
    st.dataframe(scheduled_df, use_container_width=True)
else:
    st.info("No real-tail flights were scheduled in this window.")

st.subheader("Unscheduled demand (add lines)")
if not unscheduled_df.empty:
    st.dataframe(unscheduled_df, use_container_width=True)
else:
    st.write("— no add-line flights —")

if other_df.empty:
    st.caption("No additional flights outside of tail/add-line categories were found.")
else:
    st.subheader("Other flights")
    st.dataframe(other_df, use_container_width=True)

with st.expander("Raw rows", expanded=False):
    st.json(rows)
