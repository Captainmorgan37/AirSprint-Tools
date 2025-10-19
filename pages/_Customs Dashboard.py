import io
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytz
import streamlit as st
from zoneinfo import ZoneInfo

from flight_leg_utils import (
    AIRPORT_TZ_FILENAME,
    ARRIVAL_AIRPORT_COLUMNS,
    DEPARTURE_AIRPORT_COLUMNS,
    FlightDataError,
    build_fl3xx_api_config,
    fetch_legs_dataframe,
    is_customs_leg,
    load_airport_metadata_lookup,
    safe_parse_dt,
)
from fl3xx_api import fetch_flight_migration


DEFAULT_BUSINESS_DAY_START = time(hour=9)
DEFAULT_BUSINESS_DAY_END = time(hour=17)


st.set_page_config(page_title="Customs Dashboard", layout="wide")
st.title("🛃 Customs Dashboard")

st.caption(
    "Pull upcoming legs from FL3XX, identify customs segments, and track migration statuses in one view."
)


def _to_local(dt: datetime, tz_name: Optional[str]) -> datetime:
    if tz_name:
        try:
            return dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt.astimezone(ZoneInfo("UTC"))


@st.cache_data(show_spinner=False)
def _airport_lookup_cache() -> Dict[str, Dict[str, Optional[str]]]:
    return load_airport_metadata_lookup()


@st.cache_data(show_spinner=False)
def _sanitize_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Create a hashable copy of FL3XX settings for caching."""

    sanitized: Dict[str, Any] = {}
    for key, value in settings.items():
        if isinstance(value, Mapping):
            sanitized[key] = {str(k): v for k, v in value.items()}
        else:
            sanitized[key] = value
    return sanitized


def _detect_airport_value(row: Mapping[str, Any], columns: Tuple[str, ...]) -> str:
    for column in columns:
        value = row.get(column)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return ""


def _extract_migration_fields(payload: Optional[Mapping[str, Any]], key: str) -> Tuple[str, str, str, int, str]:
    section = payload.get(key) if isinstance(payload, Mapping) else None
    if not isinstance(section, Mapping):
        return "NR", "", "", 0, ""
    status = str(section.get("status") or "NR").upper()
    by = str(section.get("by") or "")
    notes = str(section.get("notes") or "")
    documents = section.get("documents")
    if isinstance(documents, list):
        names: List[str] = []
        for doc in documents:
            if isinstance(doc, Mapping):
                name = doc.get("customName") or doc.get("originalName") or doc.get("name")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
        doc_count = len(names)
        doc_summary = ", ".join(names)
    else:
        doc_count = 0
        doc_summary = ""
    return status, by, notes, doc_count, doc_summary


def _format_local_time(row: Mapping[str, Any]) -> Tuple[str, str]:
    dep_time_raw = row.get("dep_time") or row.get("departureTime")
    if not dep_time_raw:
        return "", ""
    dt = safe_parse_dt(str(dep_time_raw))
    tz_name = row.get("dep_tz") or row.get("departureTimeZone")
    if tz_name:
        try:
            dt_local = dt.astimezone(ZoneInfo(str(tz_name)))
        except Exception:
            dt_local = _to_local(dt, None)
    else:
        dt_local = _to_local(dt, None)
    return (
        dt.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        dt_local.strftime("%Y-%m-%d %H:%M %Z"),
    )


def _extract_airport_timezone(
    airport_code: str, lookup: Dict[str, Dict[str, Optional[str]]]
) -> Optional[str]:
    if not airport_code:
        return None
    record = lookup.get(airport_code)
    if not isinstance(record, Mapping):
        return None
    tz_value = record.get("tz")
    if isinstance(tz_value, str) and tz_value.strip():
        return tz_value.strip()
    return None


def _candidate_timezone_from_row(row: Mapping[str, Any]) -> Optional[str]:
    timezone_keys = (
        "arrivalTimeZone",
        "arrival_tz",
        "arr_tz",
        "dep_tz",
        "departureTimeZone",
    )
    for key in timezone_keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _previous_business_day(reference: date) -> date:
    candidate = reference - timedelta(days=1)
    while candidate.weekday() >= 5:  # 5=Saturday, 6=Sunday
        candidate -= timedelta(days=1)
    return candidate


def _compute_clearance_window(
    row: Mapping[str, Any],
    dep_airport: str,
    arr_airport: str,
    lookup: Dict[str, Dict[str, Optional[str]]],
) -> Tuple[str, str, str, str]:
    event_candidates = (
        "arrivalTime",
        "arrival_time",
        "arr_time",
        "eta",
        "sta",
        "scheduledArrival",
        "dep_time",
        "departureTime",
    )
    event_dt: Optional[datetime] = None
    for key in event_candidates:
        raw_value = row.get(key)
        if not raw_value:
            continue
        try:
            event_dt = safe_parse_dt(str(raw_value))
            break
        except Exception:
            continue

    if event_dt is None:
        return "", "", "", ""

    tz_name = _extract_airport_timezone(arr_airport, lookup) or _candidate_timezone_from_row(row)
    if not tz_name:
        tz_name = _extract_airport_timezone(dep_airport, lookup)

    if tz_name:
        try:
            event_local = event_dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            event_local = _to_local(event_dt, None)
    else:
        event_local = _to_local(event_dt, None)

    target_date = _previous_business_day(event_local.date())
    tzinfo = event_local.tzinfo or ZoneInfo("UTC")

    start_local = datetime.combine(target_date, DEFAULT_BUSINESS_DAY_START).replace(tzinfo=tzinfo)
    end_local = datetime.combine(target_date, DEFAULT_BUSINESS_DAY_END).replace(tzinfo=tzinfo)

    start_local_str = start_local.strftime("%Y-%m-%d %H:%M %Z")
    end_local_str = end_local.strftime("%Y-%m-%d %H:%M %Z")
    end_utc_str = end_local.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M UTC")
    goal_summary = (
        f"Complete during prior business day ({start_local.strftime('%b %d')} {DEFAULT_BUSINESS_DAY_START.strftime('%H:%M')}"
        f"-{DEFAULT_BUSINESS_DAY_END.strftime('%H:%M')} local)."
    )

    return start_local_str, end_local_str, end_utc_str, goal_summary


st.sidebar.header("Configuration")

fl3xx_cfg: Dict[str, Any] = {}
try:
    if "fl3xx_api" in st.secrets:
        cfg = st.secrets["fl3xx_api"]
        if isinstance(cfg, Mapping):
            fl3xx_cfg = {str(k): cfg[k] for k in cfg}
        elif isinstance(cfg, dict):
            fl3xx_cfg = dict(cfg)
except Exception:
    fl3xx_cfg = {}

has_live_credentials = bool(fl3xx_cfg.get("api_token") or fl3xx_cfg.get("auth_header"))
if has_live_credentials:
    st.sidebar.success("Using FL3XX credentials from Streamlit secrets.")
else:
    st.sidebar.info(
        "Add your FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to enable live fetching."
    )

start_date = st.sidebar.date_input("Start date", value=date.today())
additional_days = st.sidebar.slider(
    "Additional days to include",
    min_value=0,
    max_value=7,
    value=4,
    help="Fetch flights for the selected start date plus this many additional days.",
)
end_date = start_date + timedelta(days=additional_days)

st.sidebar.write(
    f"Fetching window: **{start_date.isoformat()}** → **{end_date.isoformat()}**"
)

clearance_file = st.sidebar.file_uploader(
    "Optional: Upload clearance requirements (CSV or Excel)",
    type=["csv", "xls", "xlsx"],
    help="Provide airport-specific clearance lead times or notes to display alongside each leg.",
)

clearance_requirements: Dict[str, str] = {}
if clearance_file is not None:
    try:
        if clearance_file.name.lower().endswith(".csv"):
            clearance_df = pd.read_csv(clearance_file)
        else:
            clearance_df = pd.read_excel(clearance_file)
    except Exception as exc:
        st.sidebar.error(f"Unable to read clearance reference: {exc}")
    else:
        if clearance_df.empty:
            st.sidebar.warning("Uploaded clearance reference is empty.")
        else:
            code_candidates = [
                col
                for col in clearance_df.columns
                if any(token in str(col).lower() for token in ("airport", "icao", "port"))
            ]
            if not code_candidates:
                code_candidates = [clearance_df.columns[0]]
            value_candidates = [col for col in clearance_df.columns if col not in code_candidates]
            if not value_candidates:
                value_candidates = [clearance_df.columns[-1]]
            code_col = code_candidates[0]
            value_col = value_candidates[0]
            for _, rec in clearance_df.iterrows():
                code = str(rec.get(code_col) or "").strip().upper()
                if not code:
                    continue
                requirement = str(rec.get(value_col) or "").strip()
                clearance_requirements[code] = requirement
            with st.sidebar.expander("Preview clearance reference", expanded=False):
                st.dataframe(clearance_df)
                st.caption(
                    f"Using `{code_col}` for airport code and `{value_col}` for requirement."
                )

fetch_button = st.button("Load customs legs", type="primary")

if not fetch_button:
    st.info("Select parameters and click **Load customs legs** to fetch customs flights.")
    st.stop()

with st.spinner("Fetching FL3XX flights..."):
    try:
        config = build_fl3xx_api_config(_sanitize_settings(fl3xx_cfg))
    except FlightDataError as exc:
        st.error(str(exc))
        st.stop()
    except Exception as exc:  # pragma: no cover - defensive
        st.error(f"Error preparing FL3XX API configuration: {exc}")
        st.stop()

    try:
        legs_df, metadata, _ = fetch_legs_dataframe(
            config,
            from_date=start_date,
            to_date=end_date,
            departure_window=None,
            fetch_crew=False,
        )
    except Exception as exc:
        st.error(f"Error fetching data from FL3XX API: {exc}")
        st.stop()

if legs_df.empty:
    st.success("No flights returned for the selected window.")
    st.stop()

missing_tz_airports = metadata.get("missing_dep_tz_airports", [])
tz_lookup_used = metadata.get("timezone_lookup_used", False)
if missing_tz_airports:
    sample = ", ".join(missing_tz_airports)
    if len(sample) > 200:
        sample = sample[:197] + "..."
    message = (
        "Added timezone from airport lookup where possible. Update `%s` to cover: %s"
        % (AIRPORT_TZ_FILENAME, sample)
    )
    if tz_lookup_used:
        st.info(message)
    else:
        st.warning(
            "Unable to infer departure timezones automatically because `%s` was not found. "
            "Sample airports without tz: %s"
            % (AIRPORT_TZ_FILENAME, sample)
        )

lookup = _airport_lookup_cache()


def _is_customs_row(row: Mapping[str, Any]) -> bool:
    try:
        return is_customs_leg(row, lookup)
    except Exception:
        return False


customs_mask = legs_df.apply(lambda r: _is_customs_row(r.to_dict()), axis=1)
customs_df = legs_df.loc[customs_mask].copy()

if customs_df.empty:
    st.success("No customs legs found in the selected window.")
    st.stop()

customs_df["dep_time"] = customs_df["dep_time"].astype(str)
customs_df = customs_df.sort_values("dep_time").reset_index(drop=True)

migration_cache: Dict[Any, Optional[Dict[str, Any]]] = {}
errors: List[str] = []
rows: List[Dict[str, Any]] = []

for _, leg in customs_df.iterrows():
    row = leg.to_dict()
    tail = str(row.get("tail") or "")
    dep_airport = _detect_airport_value(row, DEPARTURE_AIRPORT_COLUMNS)
    arr_airport = _detect_airport_value(row, ARRIVAL_AIRPORT_COLUMNS)
    dep_utc, dep_local = _format_local_time(row)

    flight_id = (
        row.get("flightId")
        or row.get("flight_id")
        or row.get("flightID")
        or row.get("id")
        or row.get("flight")
    )

    migration_payload: Optional[Dict[str, Any]] = None
    if flight_id:
        if flight_id not in migration_cache:
            try:
                migration_cache[flight_id] = fetch_flight_migration(config, flight_id)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"Flight {flight_id}: {exc}")
                migration_cache[flight_id] = None
        migration_payload = migration_cache.get(flight_id)
    else:
        errors.append(f"Missing flight ID for tail {tail} departing {dep_utc}.")

    dep_status, dep_by, dep_notes, dep_docs, dep_doc_names = _extract_migration_fields(
        migration_payload, "departureMigration"
    )
    arr_status, arr_by, arr_notes, arr_docs, arr_doc_names = _extract_migration_fields(
        migration_payload, "arrivalMigration"
    )

    clearance_note = ""
    if clearance_requirements and arr_airport:
        clearance_note = clearance_requirements.get(arr_airport.upper(), "")

    (
        clearance_start_local,
        clearance_end_local,
        clearance_end_utc,
        clearance_goal,
    ) = _compute_clearance_window(row, dep_airport, arr_airport, lookup)

    rows.append(
        {
            "Tail": tail,
            "Departure": dep_airport,
            "Arrival": arr_airport,
            "Departure UTC": dep_utc,
            "Departure Local": dep_local,
            "Departure Status": dep_status,
            "Departure By": dep_by,
            "Departure Notes": dep_notes,
            "Departure Documents": dep_docs,
            "Arrival Status": arr_status,
            "Arrival By": arr_by,
            "Arrival Notes": arr_notes,
            "Arrival Documents": arr_docs,
            "Arrival Doc Names": arr_doc_names,
            "Departure Doc Names": dep_doc_names,
            "Clearance Requirement": clearance_note,
            "Clearance Target Start (Local)": clearance_start_local,
            "Clearance Target End (Local)": clearance_end_local,
            "Clearance Target End (UTC)": clearance_end_utc,
            "Clearance Goal": clearance_goal,
        }
    )

result_df = pd.DataFrame(rows)

status_counts = (
    result_df["Arrival Status"].value_counts().rename_axis("Arrival Status").reset_index(name="Legs")
)
status_counts_dep = (
    result_df["Departure Status"].value_counts().rename_axis("Departure Status").reset_index(name="Legs")
)

col1, col2 = st.columns(2)
with col1:
    st.metric("Customs legs", result_df.shape[0])
with col2:
    st.metric(
        "Pending departures",
        int((result_df["Departure Status"] != "OK").sum()),
    )

summary_tab, table_tab = st.tabs(["Status summary", "Detailed view"])
with summary_tab:
    st.subheader("Departure status distribution")
    st.dataframe(status_counts_dep, use_container_width=True)
    st.subheader("Arrival status distribution")
    st.dataframe(status_counts, use_container_width=True)

with table_tab:
    st.dataframe(result_df.drop(columns=["Departure Doc Names", "Arrival Doc Names"]), use_container_width=True)
    st.caption(
        "Clearance goals assume completion during the prior business day between %s and %s local time."
        % (
            DEFAULT_BUSINESS_DAY_START.strftime("%H:%M"),
            DEFAULT_BUSINESS_DAY_END.strftime("%H:%M"),
        )
    )
    if not result_df.empty:
        csv_buffer = io.StringIO()
        result_df.to_csv(csv_buffer, index=False)
        st.download_button(
            "Download CSV",
            data=csv_buffer.getvalue().encode("utf-8"),
            file_name="customs_dashboard.csv",
            mime="text/csv",
        )

if clearance_requirements:
    st.info(
        "Clearance requirements applied for arrival airports. Update `%s` to expand timezone coverage."
        % AIRPORT_TZ_FILENAME
    )

if errors:
    with st.expander("Warnings", expanded=True):
        for err in errors:
            st.warning(err)

st.caption(
    "Statuses sourced from FL3XX flight migration endpoint. Upload updated clearance references as needed."
)

