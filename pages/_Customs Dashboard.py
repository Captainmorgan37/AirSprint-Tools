import io
import re
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from pathlib import Path
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

CUSTOMS_RULES_PATH = Path(__file__).resolve().parent.parent / "customs_rules.csv"


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


def _extract_codes(value: str) -> List[str]:
    cleaned = value.strip().upper()
    if not cleaned:
        return []
    if cleaned.replace(" ", "").isalnum() and len(cleaned) in {3, 4}:
        return [cleaned]
    return [token.upper() for token in re.findall(r"\b[A-Za-z0-9]{3,4}\b", cleaned)]


def _airport_country_from_value(value: str, lookup: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    for code in _extract_codes(value):
        record = lookup.get(code)
        if not isinstance(record, Mapping):
            continue
        country = record.get("country")
        if isinstance(country, str) and country.strip():
            return country.strip().upper()
    return None


def _arrival_country_from_row(row: Mapping[str, Any], lookup: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    for column in ARRIVAL_AIRPORT_COLUMNS:
        value = row.get(column)
        if isinstance(value, str) and value.strip():
            country = _airport_country_from_value(value, lookup)
            if country:
                return country
    return None


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


@st.cache_data(show_spinner=False)
def _load_customs_rules(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        raise
    if df.empty:
        return df
    df.columns = [str(col).strip() for col in df.columns]
    return df


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
    return None


def _format_hours_summary(rule: Mapping[str, Any]) -> str:
    days = (
        ("Mon", "open_mon"),
        ("Tue", "open_tue"),
        ("Wed", "open_wed"),
        ("Thu", "open_thu"),
        ("Fri", "open_fri"),
        ("Sat", "open_sat"),
        ("Sun", "open_sun"),
    )
    segments: List[str] = []
    for label, key in days:
        value = rule.get(key)
        if value is None:
            continue
        value_str = str(value).strip()
        if not value_str or value_str.upper() == "NAN":
            continue
        segments.append(f"{label}: {value_str}")
    return "; ".join(segments)


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
    "Optional: Upload customs rules (CSV or Excel)",
    type=["csv", "xls", "xlsx"],
    help="Provide airport-specific customs lead times or notes to display alongside each leg.",
)

customs_rules_df = pd.DataFrame()
customs_rules_note = ""

if CUSTOMS_RULES_PATH.exists():
    try:
        customs_rules_df = _load_customs_rules(str(CUSTOMS_RULES_PATH))
    except Exception as exc:  # pragma: no cover - defensive
        st.sidebar.error(f"Error loading bundled customs rules: {exc}")
    else:
        if customs_rules_df.empty:
            st.sidebar.warning(
                f"Bundled customs rules file `{CUSTOMS_RULES_PATH.name}` is empty."
            )
        else:
            customs_rules_note = f"Loaded default rules from `{CUSTOMS_RULES_PATH.name}`."
else:
    st.sidebar.info(
        "Add a `customs_rules.csv` file to load default customs lead times and operating hours."
    )

if clearance_file is not None:
    try:
        if clearance_file.name.lower().endswith(".csv"):
            uploaded_df = pd.read_csv(clearance_file)
        else:
            uploaded_df = pd.read_excel(clearance_file)
    except Exception as exc:
        st.sidebar.error(f"Unable to read uploaded customs rules: {exc}")
    else:
        if uploaded_df.empty:
            st.sidebar.warning("Uploaded customs rules sheet is empty.")
        else:
            customs_rules_df = uploaded_df.copy()
            customs_rules_note = f"Using uploaded rules file `{clearance_file.name}`."
            st.sidebar.success("Customs rules overridden by uploaded sheet.")
            with st.sidebar.expander("Preview uploaded rules", expanded=False):
                st.dataframe(uploaded_df)

if customs_rules_note:
    st.sidebar.caption(customs_rules_note)

clearance_requirements: Dict[str, str] = {}
rules_lookup: Dict[str, Mapping[str, Any]] = {}

if not customs_rules_df.empty:
    normalized_country_col = None
    if "country" in customs_rules_df.columns:
        normalized_country_col = "country"
    elif "Country" in customs_rules_df.columns:
        normalized_country_col = "Country"

    for _, rec in customs_rules_df.iterrows():
        code = str(rec.get("airport_icao") or rec.get("Airport_icao") or rec.get("airport") or "").strip().upper()
        if not code:
            continue
        rules_lookup[code] = rec.to_dict()
        if "notes" in rec and isinstance(rec["notes"], str):
            clearance_requirements[code] = rec["notes"].strip()

    us_rules = customs_rules_df
    if normalized_country_col is not None:
        us_mask = customs_rules_df[normalized_country_col].astype(str).str.upper() == "US"
        us_rules = customs_rules_df.loc[us_mask]
    st.sidebar.success(f"Customs rules loaded: {len(us_rules)} US ports, {len(customs_rules_df)} total records.")
    with st.sidebar.expander("US customs rules preview", expanded=False):
        preview_cols = [
            col
            for col in (
                "airport_icao",
                "lead_time_arrival_hours",
                "lead_time_departure_hours",
                "open_mon",
                "open_tue",
                "open_wed",
                "open_thu",
                "open_fri",
                "after_hours_available",
                "notes",
            )
            if col in us_rules.columns
        ]
        st.dataframe(us_rules[preview_cols])


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


def _is_us_arrival(row: Mapping[str, Any]) -> bool:
    country = _arrival_country_from_row(row, lookup)
    return country == "US"


us_arrival_mask = customs_df.apply(lambda r: _is_us_arrival(r.to_dict()), axis=1)
customs_df = customs_df.loc[us_arrival_mask].copy()

if customs_df.empty:
    st.success("No US customs arrivals found in the selected window.")
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

    rule = rules_lookup.get(arr_airport.upper()) if arr_airport else None
    lead_time_arrival = ""
    lead_time_departure = ""
    hours_summary = ""
    after_hours = ""
    contacts = ""
    rule_notes = ""
    rule_source = ""
    if isinstance(rule, Mapping):
        lead_time_arrival = str(rule.get("lead_time_arrival_hours") or "").strip()
        lead_time_departure = str(rule.get("lead_time_departure_hours") or "").strip()
        hours_summary = _format_hours_summary(rule)
        after_hours_bool = _normalize_bool(rule.get("after_hours_available"))
        if after_hours_bool is None and "after_hours_available" in rule:
            after_hours = str(rule.get("after_hours_available") or "").strip()
        elif after_hours_bool is not None:
            after_hours = "Yes" if after_hours_bool else "No"
        contacts = str(rule.get("contacts") or "").strip()
        rule_notes = str(rule.get("notes") or "").strip()
        rule_source = str(rule.get("source") or "").strip()

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

    arr_status, arr_by, arr_notes, arr_docs, arr_doc_names = _extract_migration_fields(
        migration_payload, "arrivalMigration"
    )

    clearance_note = ""
    if clearance_requirements and arr_airport:
        clearance_note = clearance_requirements.get(arr_airport.upper(), "")
    if not clearance_note and rule_notes:
        clearance_note = rule_notes

    (
        clearance_start_local,
        clearance_end_local,
        _,
        clearance_goal,
    ) = _compute_clearance_window(row, dep_airport, arr_airport, lookup)

    rows.append(
        {
            "Tail": tail,
            "Departure": dep_airport,
            "Arrival": arr_airport,
            "Departure Local": dep_local,
            "Arrival Status": arr_status,
            "Arrival By": arr_by,
            "Arrival Notes": arr_notes,
            "Arrival Documents": arr_docs,
            "Arrival Doc Names": arr_doc_names,
            "Clearance Requirement": clearance_note,
            "Clearance Target Start (Local)": clearance_start_local,
            "Clearance Target End (Local)": clearance_end_local,
            "Clearance Goal": clearance_goal,
            "Rule Lead Time Arrival (hrs)": lead_time_arrival,
            "Rule Lead Time Departure (hrs)": lead_time_departure,
            "Rule Operating Hours": hours_summary,
            "Rule After Hours Available": after_hours,
            "Rule Contacts": contacts,
            "Rule Source": rule_source,
        }
    )

result_df = pd.DataFrame(rows)

status_counts = (
    result_df["Arrival Status"].value_counts().rename_axis("Arrival Status").reset_index(name="Legs")
)

col1, col2 = st.columns(2)
with col1:
    st.metric("Customs legs", result_df.shape[0])
with col2:
    st.metric(
        "Pending arrivals",
        int((result_df["Arrival Status"] != "OK").sum()),
    )

summary_tab, table_tab = st.tabs(["Status summary", "Detailed view"])
with summary_tab:
    st.subheader("Arrival status distribution")
    st.dataframe(status_counts, use_container_width=True)

with table_tab:
    drop_cols = [col for col in ("Arrival Doc Names",) if col in result_df.columns]
    st.dataframe(result_df.drop(columns=drop_cols), use_container_width=True)
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

