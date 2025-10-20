import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo
import pandas as pd
import streamlit as st

from fl3xx_api import Fl3xxApiConfig, fetch_flights, fetch_postflight

# ----------------------------
# App Config
# ----------------------------
st.set_page_config(page_title="Short Turns Highlighter", layout="wide")
st.title("✈️ Short Turn/Priority Status Viewer")


def _purge_autorefresh_session_state() -> None:
    """Remove stale session-state keys left behind by the old autorefresh widget."""

    stale_keys = [key for key in st.session_state if "autorefresh" in key.lower()]
    for key in stale_keys:
        st.session_state.pop(key, None)


_purge_autorefresh_session_state()

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Edmonton"))
DEFAULT_TURN_THRESHOLD_MIN = int(os.getenv("TURN_THRESHOLD_MIN", "45"))
PRIORITY_TURN_THRESHOLD_MIN = int(os.getenv("PRIORITY_TURN_THRESHOLD_MIN", "90"))

# ----------------------------
# Helper: Normalize / Parse datetimes
# ----------------------------
def parse_dt(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x)
    # Try multiple formats
    for fmt in (None, "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return pd.to_datetime(x, format=fmt, utc=True).tz_convert(LOCAL_TZ) if fmt else pd.to_datetime(x, utc=True).tz_convert(LOCAL_TZ)
        except Exception:
            continue
    # Last resort
    try:
        return pd.to_datetime(x, utc=True).tz_convert(LOCAL_TZ)
    except Exception:
        return pd.NaT

# ----------------------------
# Data Model we need
# ----------------------------
# Minimal normalized columns required for the turn calculation:
#   tail: str (aircraft reg)
#   station: str (ICAO where the turn happens; typically ARR airport of leg N and DEP airport of leg N+1)
#   arr_onblock: datetime (actual or scheduled on-block for arriving leg)
#   dep_offblock_next: datetime (actual or scheduled off-block for next departing leg from same station)
#   arr_leg_id / dep_leg_id: identifiers (optional but helpful)
#
# The app provides two input paths:
#   1) Direct FL3XX API fetch (configure below in fetch_fl3xx_legs)
#   2) Upload CSV/JSON already exported by your FF Dashboard or FL3XX

# ----------------------------
# FL3XX Fetch (skeleton — adapt endpoint & mapping to your account)
# ----------------------------
def _coerce_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _get_nested(mapping, path):
    if isinstance(path, str):
        parts = path.split(".")
    else:
        parts = list(path)
    current = mapping
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _detect_priority(value):
    if value is None:
        return False, None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return False, None
    if "priority" in text.lower():
        return True, text
    return False, None


def _first_stripped(*values):
    for value in values:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
        elif value is not None:
            candidate = str(value).strip()
            if candidate:
                return candidate
    return None


def _coerce_datetime(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return parse_dt(value)
    if isinstance(value, str):
        return parse_dt(value)
    if isinstance(value, dict):
        for key in (
            "actual",
            "actualTime",
            "actualUTC",
            "actualUtc",
            "actualDateTime",
            "scheduled",
            "scheduledTime",
            "scheduledUTC",
            "scheduledUtc",
            "scheduledDateTime",
            "offBlock",
            "offBlockActual",
            "offBlockScheduled",
            "out",
            "outActual",
            "outScheduled",
            "in",
            "inActual",
            "inScheduled",
        ):
            if key in value:
                dt = _coerce_datetime(value[key])
                if dt is not pd.NaT:
                    return dt
        for sub_value in value.values():
            dt = _coerce_datetime(sub_value)
            if dt is not pd.NaT:
                return dt
        return pd.NaT
    if isinstance(value, (list, tuple, set)):
        for item in value:
            dt = _coerce_datetime(item)
            if dt is not pd.NaT:
                return dt
    return pd.NaT


def _extract_field(payload: dict, options):
    for option in options:
        value = _get_nested(payload, option)
        if value is None:
            continue
        result = _first_stripped(value)
        if result:
            return result
    return None


def _is_placeholder_tail(tail: str) -> bool:
    """Return ``True`` when the provided tail number is a placeholder."""

    if not tail:
        return False
    first_word = tail.split()[0]
    return first_word in {"ADD", "REMOVE"}


def _normalise_flights(flights):
    """Return a dataframe of legs and diagnostics about skipped flights."""

    stats = {
        "raw_count": 0,
        "normalised": 0,
        "skipped_non_mapping": 0,
        "skipped_missing_tail": 0,
        "skipped_placeholder_tail": 0,
        "skipped_missing_airports": 0,
        "skipped_missing_dep_airport": 0,
        "skipped_missing_arr_airport": 0,
        "skipped_missing_times": 0,
    }

    dep_time_keys = [
        "scheduledOut",
        "actualOut",
        "outActual",
        "outScheduled",
        "offBlock",
        "offBlockActual",
        "offBlockScheduled",
        "blockOffEstUTC",
        "blockOffEstLocal",
        "blocksoffestimated",
        "departureScheduledTime",
        "departureActualTime",
        "departureTimeScheduled",
        "departureTimeActual",
        "departureScheduledUtc",
        "departureActualUtc",
        "departure.scheduled",
        "departure.actual",
        "departure.scheduledTime",
        "departure.actualTime",
        "departure.scheduledUtc",
        "departure.actualUtc",
        "times.departure.scheduled",
        "times.departure.actual",
        "times.offBlock.scheduled",
        "times.offBlock.actual",
    ]

    arr_time_keys = [
        "scheduledIn",
        "actualIn",
        "inActual",
        "inScheduled",
        "onBlock",
        "onBlockActual",
        "onBlockScheduled",
        "blockOnEstUTC",
        "blockOnEstLocal",
        "blocksonestimated",
        "arrivalScheduledTime",
        "arrivalActualTime",
        "arrivalTimeScheduled",
        "arrivalTimeActual",
        "arrivalScheduledUtc",
        "arrivalActualUtc",
        "arrival.scheduled",
        "arrival.actual",
        "arrival.scheduledTime",
        "arrival.actualTime",
        "arrival.scheduledUtc",
        "arrival.actualUtc",
        "times.arrival.scheduled",
        "times.arrival.actual",
        "times.onBlock.scheduled",
        "times.onBlock.actual",
    ]

    tail_keys = [
        "aircraftRegistration",
        "aircraft.registration",
        "aircraft.reg",
        "aircraft.registrationNumber",
        "registrationNumber",
        "aircraft.tailNumber",
        "aircraft.name",
        "tailNumber",
        "tail",
        "registration",
    ]

    dep_airport_keys = [
        "departureAirportIcao",
        "departureAirport.icao",
        "departure.airportIcao",
        "departure.airport.icao",
        "departureAirport",
        "departure.icao",
        "departure.airport",
        "departureStation",
        "airportFrom",
        "realAirportFrom",
    ]

    arr_airport_keys = [
        "arrivalAirportIcao",
        "arrivalAirport.icao",
        "arrival.airportIcao",
        "arrival.airport.icao",
        "arrivalAirport",
        "arrival.icao",
        "arrival.airport",
        "arrivalStation",
        "airportTo",
        "realAirportTo",
    ]

    leg_id_keys = [
        "bookingIdentifier",
        "booking.identifier",
        "id",
        "uuid",
        "scheduleId",
    ]

    flight_id_keys = [
        "flightId",
        "flight.id",
        "legId",
        "leg_id",
        "scheduleId",
        "id",
        "uuid",
    ]

    booking_code_keys = [
        "bookingCode",
        "booking.code",
        "booking.bookingCode",
        "booking.codeName",
        "bookingCodeName",
        "bookingReference",
        "booking.reference",
    ]

    priority_label_keys = [
        "workflowCustomName",
        "workflow_custom_name",
        "workflowName",
        "workflow",
        "tags",
        "labels",
        "notes",
    ]

    priority_flag_keys = [
        "priority",
        "isPriority",
        "priorityFlight",
        "priority_flag",
        "hasPriority",
    ]

    rows = []
    for flight in flights:
        stats["raw_count"] += 1
        if not isinstance(flight, dict):
            stats["skipped_non_mapping"] += 1
            continue
        tail = _extract_field(flight, tail_keys)
        dep_ap = _extract_field(flight, dep_airport_keys)
        arr_ap = _extract_field(flight, arr_airport_keys)
        dep_time = _extract_datetime(flight, dep_time_keys)
        arr_time = _extract_datetime(flight, arr_time_keys)
        leg_id = _extract_field(flight, leg_id_keys)
        flight_id = _extract_field(flight, flight_id_keys)
        if not flight_id:
            flight_id = leg_id
        booking_code = _extract_field(flight, booking_code_keys)
        priority_label = _extract_field(flight, priority_label_keys)
        is_priority, priority_text = _detect_priority(priority_label)
        if not is_priority:
            for flag_key in priority_flag_keys:
                value = _get_nested(flight, flag_key)
                if value is None:
                    continue
                if _coerce_bool(value):
                    is_priority = True
                    if not priority_text:
                        priority_text = priority_label or "Priority"
                    break

        if tail:
            tail = tail.upper()
            if _is_placeholder_tail(tail):
                stats["skipped_placeholder_tail"] += 1
                continue
        if dep_ap:
            dep_ap = dep_ap.upper()
        if arr_ap:
            arr_ap = arr_ap.upper()

        if not tail:
            stats["skipped_missing_tail"] += 1
            continue

        missing_airport = False
        if not dep_ap:
            stats["skipped_missing_dep_airport"] += 1
            missing_airport = True
        if not arr_ap:
            stats["skipped_missing_arr_airport"] += 1
            missing_airport = True

        if missing_airport:
            stats["skipped_missing_airports"] += 1
            continue

        if dep_time is pd.NaT:
            dep_time = None
        if arr_time is pd.NaT:
            arr_time = None

        if dep_time is None and arr_time is None:
            stats["skipped_missing_times"] += 1
            continue

        rows.append(
            {
                "tail": tail,
                "dep_airport": dep_ap,
                "arr_airport": arr_ap,
                "dep_offblock": dep_time,
                "arr_onblock": arr_time,
                "leg_id": leg_id,
                "flight_id": flight_id,
                "booking_code": booking_code,
                "is_priority": is_priority,
                "priority_label": priority_text,
            }
        )

        stats["normalised"] += 1

    return pd.DataFrame(rows), stats


def _extract_datetime(payload: dict, options):
    for option in options:
        value = _get_nested(payload, option)
        if value is None:
            continue
        dt = _coerce_datetime(value)
        if dt is not pd.NaT:
            return dt
    return None


def _build_fl3xx_config(token: str) -> Fl3xxApiConfig:
    secrets_section = st.secrets.get("fl3xx_api", {})

    base_url = secrets_section.get("base_url") or os.getenv("FL3XX_BASE_URL") or Fl3xxApiConfig().base_url

    auth_header_name = secrets_section.get("auth_header_name") or os.getenv("FL3XX_AUTH_HEADER", "Authorization")

    auth_header = secrets_section.get("auth_header") or os.getenv("FL3XX_AUTH_HEADER_VALUE")

    api_token_scheme = secrets_section.get("api_token_scheme") or os.getenv("FL3XX_TOKEN_SCHEME")

    extra_headers = {}
    if "extra_headers" in secrets_section and isinstance(secrets_section["extra_headers"], dict):
        extra_headers = dict(secrets_section["extra_headers"])

    extra_params = {}
    if "extra_params" in secrets_section and isinstance(secrets_section["extra_params"], dict):
        extra_params = dict(secrets_section["extra_params"])

    verify_ssl = secrets_section.get("verify_ssl")
    if verify_ssl is None:
        verify_ssl = os.getenv("FL3XX_VERIFY_SSL")
    verify_ssl = True if verify_ssl is None else _coerce_bool(verify_ssl)

    timeout = secrets_section.get("timeout") or os.getenv("FL3XX_TIMEOUT")
    if timeout is not None:
        try:
            timeout = int(timeout)
        except (TypeError, ValueError):
            timeout = None

    config_kwargs = {
        "base_url": base_url,
        "api_token": token or secrets_section.get("api_token"),
        "auth_header": auth_header,
        "auth_header_name": auth_header_name,
        "api_token_scheme": api_token_scheme,
        "extra_headers": extra_headers,
        "extra_params": extra_params,
        "verify_ssl": verify_ssl,
    }

    if timeout is not None:
        config_kwargs["timeout"] = timeout

    return Fl3xxApiConfig(**config_kwargs)


@st.cache_data(show_spinner=True, ttl=180)
def fetch_fl3xx_legs(token: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    """Fetch FL3XX flights and normalise them into the dataframe the app expects."""

    config = _build_fl3xx_config(token)

    if not (config.api_token or config.auth_header):
        st.error("No FL3XX API token found. Provide a token or configure it in Streamlit secrets.")
        return pd.DataFrame()

    from_date = start_utc.date()
    to_date = end_utc.date()

    try:
        flights, metadata = fetch_flights(config, from_date=from_date, to_date=to_date)
    except Exception as exc:
        st.error(f"FL3XX fetch failed: {exc}")
        return pd.DataFrame()

    legs_df, normalise_stats = _normalise_flights(flights)

    st.session_state["fl3xx_last_metadata"] = {
        "count": len(flights),
        **metadata,
        "normalisation": normalise_stats,
    }

    return legs_df

# ----------------------------
# Upload parser (CSV/JSON) — expects the columns listed above, but will try to infer
# ----------------------------
def load_uploaded(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith(".json"):
        raw = pd.read_json(file)
    else:
        raw = pd.read_csv(file)

    # Try to normalize column names
    cols = {c.lower(): c for c in raw.columns}
    def pick(*options):
        for opt in options:
            if opt in cols:
                return cols[opt]
        return None

    tail_col = pick("tail", "aircraftregistration", "aircraft", "reg")
    dep_ap_col = pick("dep_airport", "departureairporticao", "depicao", "departure")
    arr_ap_col = pick("arr_airport", "arrivalairporticao", "arricao", "arrival")
    dep_off_col = pick("dep_offblock", "scheduledout", "outtime", "offblock")
    arr_on_col = pick("arr_onblock", "scheduledin", "intime", "onblock")
    leg_id_col = pick("leg_id", "bookingidentifier", "id", "legid", "uuid")
    flight_id_col = pick("flight_id", "flightid", "scheduleid", "legid", "uuid", "id")
    booking_code_col = pick(
        "booking_code",
        "bookingcode",
        "booking code",
        "bookingcodename",
        "booking code name",
        "bookingreference",
        "booking_reference",
        "bookingcode_name",
        "bookingcodeidentifier",
        "booking_reference_code",
        "bookingref",
    )
    priority_label_col = pick(
        "priority_label",
        "prioritydetail",
        "priority_details",
        "workflowcustomname",
        "workflow_name",
    )
    priority_flag_col = pick("is_priority", "priority", "priorityflag", "priority_flight")

    df = pd.DataFrame({
        "tail": raw[tail_col] if tail_col else None,
        "dep_airport": raw[dep_ap_col] if dep_ap_col else None,
        "arr_airport": raw[arr_ap_col] if arr_ap_col else None,
        "dep_offblock": raw[dep_off_col].apply(parse_dt) if dep_off_col else pd.NaT,
        "arr_onblock": raw[arr_on_col].apply(parse_dt) if arr_on_col else pd.NaT,
        "leg_id": raw[leg_id_col] if leg_id_col else None,
    })

    if flight_id_col:
        df["flight_id"] = raw[flight_id_col]
    else:
        df["flight_id"] = df.get("leg_id")

    if booking_code_col:
        df["booking_code"] = raw[booking_code_col]
    else:
        df["booking_code"] = None

    priority_bool = pd.Series([False] * len(raw), index=raw.index, dtype="bool")
    priority_label = pd.Series([None] * len(raw), index=raw.index, dtype="object")

    if priority_label_col:
        detections = raw[priority_label_col].apply(_detect_priority)
        priority_bool = detections.apply(lambda pair: pair[0])
        priority_label = detections.apply(lambda pair: pair[1])

    if priority_flag_col:
        flag_series = raw[priority_flag_col].apply(_coerce_bool)
        priority_bool = priority_bool | flag_series
        missing_label_mask = flag_series & priority_label.isna()
        priority_label.loc[missing_label_mask] = "Priority"

    df["is_priority"] = priority_bool.values
    df["priority_label"] = priority_label.values

    return df.dropna(subset=["tail"]) if "tail" in df else df

# ----------------------------
# Core: Compute Turns
# ----------------------------
def compute_short_turns(
    legs: pd.DataFrame,
    threshold_min: int,
    priority_threshold_min: int = PRIORITY_TURN_THRESHOLD_MIN,
) -> pd.DataFrame:
    if legs.empty:
        return pd.DataFrame(columns=[
            "tail",
            "station",
            "arr_leg_id",
            "arr_onblock",
            "dep_leg_id",
            "dep_offblock",
            "turn_min",
            "required_threshold_min",
            "priority_flag",
            "arr_priority_label",
            "dep_priority_label",
        ])

    # Ensure dtypes
    legs = legs.copy()
    legs["dep_offblock"] = legs["dep_offblock"].apply(parse_dt)
    legs["arr_onblock"] = legs["arr_onblock"].apply(parse_dt)
    if "flight_id" not in legs.columns:
        legs["flight_id"] = legs.get("leg_id")
    if "booking_code" not in legs.columns:
        legs["booking_code"] = None
    if "is_priority" not in legs.columns:
        legs["is_priority"] = False
    if "priority_label" not in legs.columns:
        legs["priority_label"] = None

    # We'll compute turns per tail per station: find next departure from the ARR station after ARR onblock
    # Prepare two views: arrivals and departures
    arrs = legs.dropna(subset=["arr_airport", "arr_onblock"]).copy()
    arrs.rename(columns={"arr_airport": "station", "arr_onblock": "arr_onblock"}, inplace=True)

    deps = legs.dropna(subset=["dep_airport", "dep_offblock"]).copy()
    deps.rename(columns={"dep_airport": "station", "dep_offblock": "dep_offblock"}, inplace=True)

    arrs = arrs[
        [
            "tail",
            "station",
            "arr_onblock",
            "leg_id",
            "flight_id",
            "booking_code",
            "is_priority",
            "priority_label",
        ]
    ].rename(
        columns={
            "leg_id": "arr_leg_id",
            "flight_id": "arr_flight_id",
            "booking_code": "arr_booking_code",
            "is_priority": "arr_is_priority",
            "priority_label": "arr_priority_label",
        }
    )
    deps = deps[
        [
            "tail",
            "station",
            "dep_offblock",
            "leg_id",
            "flight_id",
            "booking_code",
            "is_priority",
            "priority_label",
        ]
    ].rename(
        columns={
            "leg_id": "dep_leg_id",
            "flight_id": "dep_flight_id",
            "booking_code": "dep_booking_code",
            "is_priority": "dep_is_priority",
            "priority_label": "dep_priority_label",
        }
    )

    # Sort for asof merge (next departure after arrival)
    arrs = arrs.sort_values(["tail", "station", "arr_onblock"]).reset_index(drop=True)
    deps = deps.sort_values(["tail", "station", "dep_offblock"]).reset_index(drop=True)

    # Merge by tail & station; for each arrival, find the FIRST departure strictly after arrival
    short_turn_rows = []
    # We'll iterate per (tail, station) to keep memory small
    for (tail, station), arr_grp in arrs.groupby(["tail", "station"], sort=False):
        dep_grp = deps[(deps["tail"] == tail) & (deps["station"] == station)]
        if dep_grp.empty:
            continue
        dep_records = dep_grp.to_dict("records")
        for _, r in arr_grp.iterrows():
            arr_t = r["arr_onblock"]
            # find first dep time > arr_t
            next_dep = None
            for dep_row in dep_records:
                dep_t = dep_row.get("dep_offblock")
                if pd.notna(arr_t) and pd.notna(dep_t) and dep_t > arr_t:
                    next_dep = dep_row
                    break
            if next_dep is None:
                continue
            dep_t = next_dep.get("dep_offblock")
            dep_id = next_dep.get("dep_leg_id")
            turn_min = (dep_t - arr_t).total_seconds() / 60.0
            arr_priority = bool(r.get("arr_is_priority"))
            dep_priority = bool(next_dep.get("dep_is_priority"))
            priority_flag = dep_priority
            arr_code = r.get("arr_booking_code")
            dep_code = next_dep.get("dep_booking_code")
            same_booking_code = False
            if arr_code and dep_code:
                arr_code_str = str(arr_code).strip().upper()
                dep_code_str = str(dep_code).strip().upper()
                same_booking_code = bool(arr_code_str and arr_code_str == dep_code_str)

            required_threshold = threshold_min
            if priority_flag and not same_booking_code:
                required_threshold = max(threshold_min, priority_threshold_min)
            if turn_min < required_threshold:
                short_turn_rows.append({
                    "tail": tail,
                    "station": station,
                    "arr_leg_id": r.get("arr_leg_id"),
                    "arr_flight_id": r.get("arr_flight_id"),
                    "arr_onblock": arr_t,
                    "dep_leg_id": dep_id,
                    "dep_flight_id": next_dep.get("dep_flight_id"),
                    "dep_offblock": dep_t,
                    "turn_min": round(turn_min, 1),
                    "required_threshold_min": required_threshold,
                    "priority_flag": priority_flag,
                    "arr_priority_label": r.get("arr_priority_label"),
                    "dep_priority_label": next_dep.get("dep_priority_label"),
                    "arr_booking_code": arr_code,
                    "dep_booking_code": dep_code,
                    "same_booking_code": same_booking_code,
                })

    out = pd.DataFrame(short_turn_rows)
    if not out.empty:
        out = out.sort_values(["turn_min", "tail", "station"]).reset_index(drop=True)
    return out


def _extract_checkin_values(payload: Any) -> list[Any]:
    """Return all values stored under a ``checkin`` key in the payload."""

    values: list[Any] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, Mapping):
            for key, value in obj.items():
                if isinstance(key, str) and key.lower() == "checkin":
                    values.append(value)
                _walk(value)
        elif isinstance(obj, (list, tuple, set)):
            for item in obj:
                _walk(item)

    _walk(payload)
    return values


def _checkin_to_datetime(value: Any, target_tz: ZoneInfo) -> Optional[datetime]:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    try:
        dt_utc = datetime.fromtimestamp(seconds, tz=ZoneInfo("UTC"))
    except (OverflowError, OSError, ValueError):
        return None
    try:
        return dt_utc.astimezone(target_tz)
    except Exception:
        return dt_utc


def compute_priority_checkin_warnings(
    legs: pd.DataFrame,
    token: Optional[str],
    priority_threshold_min: int = PRIORITY_TURN_THRESHOLD_MIN,
) -> tuple[pd.DataFrame, list[str], int]:
    """Return priority check-in warnings, errors, and the number of evaluated flights."""

    if legs.empty:
        return pd.DataFrame(), [], 0

    legs = legs.copy()
    legs["dep_offblock"] = legs["dep_offblock"].apply(parse_dt)
    legs = legs.dropna(subset=["dep_offblock", "tail"])
    if legs.empty:
        return pd.DataFrame(), [], 0

    if "flight_id" not in legs.columns:
        legs["flight_id"] = legs.get("leg_id")
    if "is_priority" not in legs.columns:
        legs["is_priority"] = False
    if "priority_label" not in legs.columns:
        legs["priority_label"] = None

    legs["dep_date"] = legs["dep_offblock"].dt.date
    legs = legs.sort_values("dep_offblock").reset_index(drop=True)

    first_indices = (
        legs.groupby(["tail", "dep_date"], sort=False)["dep_offblock"].idxmin()
    )
    if first_indices.empty:
        return pd.DataFrame(), [], 0

    first_rows = legs.loc[first_indices].copy()
    priority_first = first_rows[first_rows["is_priority"]]
    evaluated_total = len(priority_first)
    if priority_first.empty:
        return pd.DataFrame(), [], evaluated_total

    config = _build_fl3xx_config(token)
    if not (config.api_token or config.auth_header):
        return pd.DataFrame(), [
            "FL3XX credentials are missing; cannot retrieve postflight check-in data.",
        ], evaluated_total

    warnings: list[dict[str, Any]] = []
    errors: list[str] = []

    for _, row in priority_first.iterrows():
        flight_id = row.get("flight_id") or row.get("leg_id")
        if not flight_id:
            errors.append(
                f"Missing flight identifier for tail {row['tail']} on {row['dep_date']}"
            )
            continue
        try:
            payload = fetch_postflight(config, flight_id)
        except Exception as exc:  # pragma: no cover - defensive path
            errors.append(f"Flight {flight_id}: {exc}")
            continue

        checkin_values = _extract_checkin_values(payload)
        if not checkin_values:
            errors.append(f"Flight {flight_id}: no check-in timestamps found")
            continue

        dep_time = row["dep_offblock"]
        target_tz = dep_time.tzinfo or LOCAL_TZ
        checkin_datetimes = [
            dt
            for value in checkin_values
            if (dt := _checkin_to_datetime(value, target_tz)) is not None
        ]

        if not checkin_datetimes:
            errors.append(f"Flight {flight_id}: unable to parse check-in timestamps")
            continue

        earliest = min(checkin_datetimes)
        latest = max(checkin_datetimes)
        minutes_before = (dep_time - earliest).total_seconds() / 60.0
        if minutes_before < priority_threshold_min:
            warnings.append(
                {
                    "tail": row["tail"],
                    "dep_date": row["dep_date"],
                    "dep_airport": row.get("dep_airport"),
                    "priority_label": row.get("priority_label"),
                    "dep_leg_id": row.get("leg_id"),
                    "flight_id": flight_id,
                    "departure_time": dep_time,
                    "earliest_checkin": earliest,
                    "latest_checkin": latest,
                    "checkin_count": len(checkin_datetimes),
                    "minutes_before_departure": round(minutes_before, 1),
                    "required_threshold_min": priority_threshold_min,
                    "checkin_times": ", ".join(
                        dt.strftime("%H:%M") for dt in sorted(checkin_datetimes)
                    ),
                }
            )

    warnings_df = pd.DataFrame(warnings)
    if not warnings_df.empty:
        warnings_df = warnings_df.sort_values("departure_time").reset_index(drop=True)

    return warnings_df, errors, evaluated_total

# ----------------------------
# UI — Sidebar Controls
# ----------------------------
st.sidebar.header("Data Source")
source = st.sidebar.radio("Choose source", ["FL3XX API", "Upload CSV/JSON"], index=0)
threshold = st.sidebar.number_input("Short-turn threshold (minutes)", min_value=5, max_value=240, value=DEFAULT_TURN_THRESHOLD_MIN, step=5)

# Date selector defaults: highlight the current day through the next few days
local_today = datetime.now(LOCAL_TZ).date()
default_start = local_today
default_end = local_today + timedelta(days=3)
selected_dates = st.sidebar.date_input(
    "Date range (local)",
    value=(default_start, default_end),
)

if isinstance(selected_dates, list):
    selected_dates = tuple(selected_dates)

if not selected_dates:
    start_date, end_date = default_start, default_end
elif isinstance(selected_dates, tuple):
    if len(selected_dates) == 2:
        start_date, end_date = selected_dates
    elif len(selected_dates) == 1:
        start_date = end_date = selected_dates[0]
    else:
        start_date = end_date = default_start
else:
    start_date = end_date = selected_dates

if start_date > end_date:
    start_date, end_date = end_date, start_date

start_local = datetime.combine(start_date, datetime.min.time(), tzinfo=LOCAL_TZ)
end_local = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=LOCAL_TZ)
start_utc = start_local.astimezone(ZoneInfo("UTC"))
end_utc = end_local.astimezone(ZoneInfo("UTC"))
window_label = f"{start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}"

# ----------------------------
# Load Data
# ----------------------------
legs_df = pd.DataFrame()
token: Optional[str] = None

if source != "FL3XX API":
    st.session_state.pop("fl3xx_last_metadata", None)

if source == "FL3XX API":
    default_token = ""
    if "fl3xx_api" in st.secrets:
        default_token = st.secrets["fl3xx_api"].get("api_token", "")
    if not default_token:
        default_token = st.secrets.get("FL3XX_TOKEN", "")

    token = default_token
    if not token:
        st.sidebar.warning(
            "Configure an FL3XX API token in Streamlit secrets to enable fetching."
        )

    fetch_btn = st.sidebar.button(
        "Fetch from FL3XX", type="primary", disabled=not bool(token)
    )
    if fetch_btn:
        legs_df = fetch_fl3xx_legs(token, start_utc, end_utc)
        if legs_df.empty:
            message = (
                "No legs returned. Check your endpoint/mapping and date range "
                f"({window_label})."
            )
            metadata = st.session_state.get("fl3xx_last_metadata", {})
            stats = metadata.get("normalisation", {})
            raw_count = stats.get("raw_count")
            normalised = stats.get("normalised")

            if raw_count:
                parts = [
                    f"The API returned {raw_count} flight{'s' if raw_count != 1 else ''}"
                ]
                if normalised is not None:
                    parts.append(
                        f"but {normalised} could be converted into legs"
                    )

                reasons = []

                def _format_reason(key, description):
                    count = stats.get(key, 0)
                    if not count:
                        return None
                    suffix = "s" if count != 1 else ""
                    return f"{count} flight{suffix} {description}"

                for key, desc in (
                    ("skipped_missing_tail", "were missing tail numbers"),
                    ("skipped_missing_dep_airport", "were missing departure airports"),
                    ("skipped_missing_arr_airport", "were missing arrival airports"),
                    (
                        "skipped_missing_times",
                        "were missing both departure and arrival times",
                    ),
                    ("skipped_non_mapping", "had an unexpected format"),
                ):
                    reason = _format_reason(key, desc)
                    if reason:
                        reasons.append(reason)

                details = ". ".join(parts)
                if reasons:
                    details += ". Flights were skipped because " + ", ".join(reasons)

                message = f"{message} {details}."

            st.warning(message)
else:
    up = st.sidebar.file_uploader("Upload CSV or JSON", type=["csv", "json"])
    if up is not None:
        legs_df = load_uploaded(up)

# ----------------------------
# Compute & Display Short Turns
# ----------------------------
if not legs_df.empty:
    with st.expander("Raw legs (normalized)", expanded=False):
        st.dataframe(legs_df, use_container_width=True, hide_index=True)

    short_df = compute_short_turns(legs_df, threshold)

    st.subheader(
        f"Short turns (≥{threshold} min standard / ≥{PRIORITY_TURN_THRESHOLD_MIN} min priority) for {window_label} ({LOCAL_TZ.key})"
    )

    if short_df.empty:
        st.success(f"No short turns found in the selected window ({window_label}). 🎉")
    else:
        # Nice column formatting
        col_config = {
            "arr_leg_id": st.column_config.TextColumn(
                "Arrival Booking",
                help="Booking identifier for the arrival leg",
            ),
            "dep_leg_id": st.column_config.TextColumn(
                "Departure Booking",
                help="Booking identifier for the departure leg",
            ),
            "arr_onblock": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
            "dep_offblock": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
            "turn_min": st.column_config.NumberColumn(
                "Turn (min)",
                help="Minutes between ARR on-block and next DEP off-block at the same station",
                step=0.1,
            ),
        }
        if "priority_flag" in short_df.columns:
            col_config["priority_flag"] = st.column_config.CheckboxColumn(
                "Priority",
                help="Turn involves at least one priority flight",
                disabled=True,
            )
        if "required_threshold_min" in short_df.columns:
            col_config["required_threshold_min"] = st.column_config.NumberColumn(
                "Required Min",
                help="Minimum minutes required for this turn",
                step=5,
            )
        if "arr_priority_label" in short_df.columns:
            col_config["arr_priority_label"] = st.column_config.TextColumn(
                "Arrival Priority Detail",
                help="Priority metadata tied to the arrival leg",
            )
        if "dep_priority_label" in short_df.columns:
            col_config["dep_priority_label"] = st.column_config.TextColumn(
                "Departure Priority Detail",
                help="Priority metadata tied to the departure leg",
            )
        if "same_booking_code" in short_df.columns:
            col_config["same_booking_code"] = st.column_config.CheckboxColumn(
                "Same Booking",
                help="Arrival and departure legs share the same booking code",
                disabled=True,
            )

        desired_order = [
            "tail",
            "station",
            "arr_leg_id",
            "dep_leg_id",
            "same_booking_code",
            "turn_min",
            "required_threshold_min",
            "priority_flag",
            "arr_priority_label",
            "dep_priority_label",
            "arr_onblock",
            "dep_offblock",
        ]
        display_short_df = short_df.drop(
            columns=["arr_booking_code", "dep_booking_code"], errors="ignore"
        )
        if "arr_onblock" in display_short_df.columns:
            display_short_df = display_short_df.sort_values(
                "arr_onblock", ascending=True, kind="mergesort"
            )
        column_order = [col for col in desired_order if col in display_short_df.columns]

        if "priority_flag" in display_short_df.columns:
            priority_mask = (
                display_short_df["priority_flag"].fillna(False).astype(bool)
            )
        else:
            priority_mask = pd.Series(
                False, index=display_short_df.index, dtype="bool"
            )

        regular_short_df = display_short_df[~priority_mask]
        priority_short_df = display_short_df[priority_mask]

        def _render_short_turn_table(df: pd.DataFrame, title: str, empty_message: str) -> None:
            st.markdown(f"#### {title}")
            if df.empty:
                st.info(empty_message)
                return
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config=col_config,
                column_order=column_order if column_order else None,
            )

        _render_short_turn_table(
            regular_short_df,
            "Regular short turns",
            "No standard short turns were found for the selected window.",
        )
        if not regular_short_df.empty:
            def _normalise_timestamp(value: Any) -> Optional[pd.Timestamp]:
                if pd.isna(value):
                    return None
                if isinstance(value, pd.Timestamp):
                    ts = value
                elif isinstance(value, datetime):
                    ts = pd.Timestamp(value)
                elif isinstance(value, str):
                    ts = parse_dt(value)
                else:
                    return None
                if ts is pd.NaT or pd.isna(ts):
                    return None
                if ts.tzinfo is None:
                    try:
                        ts = ts.tz_localize(LOCAL_TZ)
                    except (TypeError, ValueError):
                        return None
                else:
                    ts = ts.tz_convert(LOCAL_TZ)
                return ts

            def _extract_turn_timestamp(row: pd.Series) -> Optional[pd.Timestamp]:
                for key in ("arr_onblock", "dep_offblock"):
                    ts = _normalise_timestamp(row.get(key))
                    if ts is not None:
                        return ts
                return None

            def _stringify(value: Any, fallback: str) -> str:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return fallback
                text = str(value).strip()
                return text or fallback

            def _format_turn_minutes(value: Any) -> str:
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return "Unknown turn time"
                try:
                    minutes = float(value)
                except (TypeError, ValueError):
                    return str(value)
                rounded = round(minutes)
                if abs(minutes - rounded) < 0.05:
                    return f"{int(rounded)} min"
                return f"{minutes:.1f} min"

            summary_df = regular_short_df.copy()
            summary_df["__turn_ts"] = summary_df.apply(_extract_turn_timestamp, axis=1)
            summary_df["__turn_date_label"] = summary_df["__turn_ts"].apply(
                lambda ts: ts.strftime("%Y-%m-%d") if ts is not None else "Unknown date"
            )
            summary_df = summary_df.sort_values(
                ["__turn_ts", "tail", "station", "arr_leg_id", "dep_leg_id"],
                kind="mergesort",
            )

            lines: list[str] = ["SHORT TURNS:"]
            for date_label, group in summary_df.groupby("__turn_date_label", sort=False):
                lines.append("")
                lines.append(date_label)
                for _, row in group.iterrows():
                    tail = _stringify(row.get("tail"), "Unknown tail")
                    arr_leg = _stringify(row.get("arr_leg_id"), "?")
                    dep_leg = _stringify(row.get("dep_leg_id"), "?")
                    station = _stringify(row.get("station"), "Unknown station")
                    turn_text = _format_turn_minutes(row.get("turn_min"))
                    lines.append(f"{tail} - {arr_leg}/{dep_leg} - {station} - {turn_text}")

            summary_text = "\n".join(lines)
            line_count = summary_text.count("\n") + 1
            st.text_area(
                "Copy regular short turns summary",
                summary_text,
                height=min(600, max(160, 24 * line_count)),
                help="Copy and paste this text block wherever you need to share the regular short turns.",
            )
        _render_short_turn_table(
            priority_short_df,
            "Priority short turns",
            "No priority short turns were found for the selected window.",
        )

        # Download
        csv = short_df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            file_name=f"short_turns_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    if source == "FL3XX API":
        priority_warnings, priority_errors, evaluated_total = compute_priority_checkin_warnings(
            legs_df, token
        )
    else:
        priority_warnings = pd.DataFrame()
        priority_errors = []
        evaluated_total = 0

    if priority_errors:
        st.warning(
            "\n".join(["Priority check-in issues:"] + [f"• {msg}" for msg in priority_errors])
        )

    if evaluated_total:
        st.subheader("Priority duty-start validation")
        if priority_warnings.empty:
            st.success(
                "All first priority departures meet the required 90-minute crew check-in window."
            )
        else:
            warning_col_config = {
                "tail": st.column_config.TextColumn("Tail"),
                "dep_date": st.column_config.TextColumn(
                    "Departure Date",
                    help="Date of the priority departure",
                ),
                "dep_airport": st.column_config.TextColumn(
                    "Departure Airport",
                    help="Airport code for the departure",
                ),
                "dep_leg_id": st.column_config.TextColumn(
                    "Departure Booking",
                    help="Booking identifier for the departure leg",
                ),
                "priority_label": st.column_config.TextColumn(
                    "Priority Detail",
                    help="Priority metadata associated with the departure",
                ),
                "departure_time": st.column_config.DatetimeColumn(
                    format="YYYY-MM-DD HH:mm",
                    help="Scheduled/actual departure time for the priority leg",
                ),
                "earliest_checkin": st.column_config.DatetimeColumn(
                    format="YYYY-MM-DD HH:mm",
                    help="Earliest crew check-in returned by FL3XX",
                ),
                "latest_checkin": st.column_config.DatetimeColumn(
                    format="YYYY-MM-DD HH:mm",
                    help="Latest crew check-in returned by FL3XX",
                ),
                "minutes_before_departure": st.column_config.NumberColumn(
                    "Minutes Before Departure",
                    help="Actual gap between earliest check-in and departure",
                    step=0.1,
                ),
                "required_threshold_min": st.column_config.NumberColumn(
                    "Required Min",
                    help="Minimum minutes required before departure",
                    step=5,
                ),
                "checkin_count": st.column_config.NumberColumn(
                    "Check-ins",
                    help="Number of crew check-in entries returned",
                ),
                "checkin_times": st.column_config.TextColumn(
                    "Check-in Times",
                    help="Crew check-in timestamps returned by FL3XX",
                ),
            }

            priority_order = [
                "tail",
                "dep_date",
                "dep_airport",
                "dep_leg_id",
                "priority_label",
                "departure_time",
                "earliest_checkin",
                "latest_checkin",
                "minutes_before_departure",
                "required_threshold_min",
                "checkin_count",
                "checkin_times",
            ]
            display_priority_warnings = priority_warnings.drop(
                columns=["flight_id"], errors="ignore"
            )
            if "departure_time" in display_priority_warnings.columns:
                display_priority_warnings = display_priority_warnings.sort_values(
                    "departure_time", ascending=True, kind="mergesort"
                )
            st.dataframe(
                display_priority_warnings,
                use_container_width=True,
                hide_index=True,
                column_config=warning_col_config,
                column_order=[
                    col
                    for col in priority_order
                    if col in display_priority_warnings.columns
                ],
            )
    elif source == "FL3XX API" and not priority_errors:
        st.info(
            "No priority first departures were found in the selected window, so no duty-start validation was required."
        )
else:
    st.info("Select a data source and load legs to see short turns.")

if source == "FL3XX API" and "fl3xx_last_metadata" in st.session_state:
    with st.expander("FL3XX fetch metadata", expanded=False):
        st.json(st.session_state["fl3xx_last_metadata"])
