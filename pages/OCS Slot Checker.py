import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime, date, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple

from fl3xx_api import Fl3xxApiConfig, fetch_flights, compute_flights_digest
from flight_leg_utils import filter_out_subcharter_rows, normalize_fl3xx_payload, safe_parse_dt

st.set_page_config(page_title="OCS vs Fl3xx Slot Compliance", layout="wide")
st.title("🛫 OCS vs Fl3xx Slot Compliance")

st.markdown("""
Fetch **Fl3xx flights via the API** or upload **Fl3xx CSV(s)**, then upload **OCS CSV(s)** (CYYZ GIR free-text or structured export).
This tool normalizes both formats and compares them against Fl3xx with airport-specific time windows.

**Results**
- ✔ Matched
- ⚠ Missing (no usable slot)
- ⚠ Misaligned (wrong tail or outside time window)
- ⚠ Stale Slots (slot not used by any Fl3xx leg)
""")

# ---------------- Config ----------------
WINDOWS_MIN = {"CYYC": 30, "CYVR": 30, "CYYZ": 30, "CYUL": 15}
FL3XX_FETCH_CHUNK_DAYS = 5
MONTHS = {m: i for i, m in enumerate(
    ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], 1)}

# Pretty month names for displaying Date tuples like (13, 9)
MONTH_ABBR = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

def with_datestr(df: pd.DataFrame, date_col="Date"):
    """Add a human-friendly DateStr column next to the Date tuple."""
    if df is None or df.empty or date_col not in df.columns:
        return df
    out = df.copy()

    def fmt(d):
        try:
            day, month = d
            abbr = MONTH_ABBR.get(int(month), str(month))
            return f"{int(day):02d}-{abbr}"
        except Exception:
            return d

    out["DateStr"] = out[date_col].apply(fmt)

    # place DateStr right after Date
    cols = list(out.columns)
    if "DateStr" in cols and date_col in cols:
        cols.insert(cols.index(date_col) + 1, cols.pop(cols.index("DateStr")))
        out = out[cols]
    return out


SLOT_AIRPORTS = set(WINDOWS_MIN.keys())


# ---------------- FL3XX API helpers ----------------
def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _build_fl3xx_config(token: Optional[str] = None) -> Fl3xxApiConfig:
    secrets_section = st.secrets.get("fl3xx_api", {})

    base_url = (
        secrets_section.get("base_url")
        or os.getenv("FL3XX_BASE_URL")
        or Fl3xxApiConfig().base_url
    )
    auth_header_name = (
        secrets_section.get("auth_header_name")
        or os.getenv("FL3XX_AUTH_HEADER")
        or "Authorization"
    )
    auth_header = secrets_section.get("auth_header") or os.getenv("FL3XX_AUTH_HEADER_VALUE")
    api_token_scheme = secrets_section.get("api_token_scheme") or os.getenv("FL3XX_TOKEN_SCHEME")

    extra_headers = {}
    if isinstance(secrets_section.get("extra_headers"), Mapping):
        extra_headers = {
            str(k): str(v)
            for k, v in secrets_section["extra_headers"].items()
        }

    extra_params = {}
    if isinstance(secrets_section.get("extra_params"), Mapping):
        extra_params = {
            str(k): str(v)
            for k, v in secrets_section["extra_params"].items()
        }

    verify_ssl_value = secrets_section.get("verify_ssl")
    if verify_ssl_value is None:
        verify_ssl_value = os.getenv("FL3XX_VERIFY_SSL")
    verify_ssl = _coerce_bool(verify_ssl_value) if verify_ssl_value is not None else True

    timeout_value = secrets_section.get("timeout") or os.getenv("FL3XX_TIMEOUT")
    timeout = None
    if timeout_value is not None:
        try:
            timeout = int(timeout_value)
        except (TypeError, ValueError):
            timeout = None

    config_kwargs = {
        "base_url": base_url,
        "api_token": token or secrets_section.get("api_token") or os.getenv("FL3XX_API_TOKEN"),
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


def _normalize_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    return text or None


def _extract_nested_value(container: Mapping[str, Any], path: Tuple[str, ...]) -> Any:
    value: Any = container
    for index, segment in enumerate(path):
        if not isinstance(value, Mapping):
            return None
        if segment in value:
            value = value[segment]
            continue
        if "." in segment:
            direct = value.get(segment)
            if direct is not None:
                value = direct
                continue
            sub_segments = tuple(part for part in segment.split(".") if part)
            if not sub_segments:
                return None
            remaining_path = sub_segments + path[index + 1 :]
            return _extract_nested_value(value, remaining_path)
        return None
    return value


def _coerce_datetime_value(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = _normalize_str(value)
        if not text:
            return None
        try:
            dt = safe_parse_dt(text)
        except Exception:
            try:
                dt = pd.to_datetime(text, utc=True).to_pydatetime()
            except Exception:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _extract_departure_dt(row: Mapping[str, Any]) -> Optional[datetime]:
    candidate_paths: Sequence[Tuple[str, ...]] = [
        ("dep_time",),
        ("departureTimeUtc",),
        ("departure_time_utc",),
        ("departureTime",),
        ("departure_time",),
        ("offBlockEstUTC",),
        ("offBlockEstUtc",),
        ("offBlockEstLocal",),
        ("offBlockEstimatedUTC",),
        ("offBlockEstimatedUtc",),
        ("offBlockEstimateUTC",),
        ("offBlockEstimateUtc",),
        ("scheduledOut",),
        ("offBlock", "estimatedUtc"),
        ("offBlock", "scheduledUtc"),
        ("offBlock", "actualUtc"),
        ("times", "offBlock", "estimatedUtc"),
        ("times", "offBlock", "scheduledUtc"),
        ("times", "offBlock", "actualUtc"),
        ("times", "departure", "estimatedUtc"),
        ("times", "departure", "scheduledUtc"),
        ("times", "departure", "actualUtc"),
        ("departure", "estimatedUtc"),
        ("departure", "scheduledUtc"),
        ("departure", "actualUtc"),
    ]
    for path in candidate_paths:
        value = _extract_nested_value(row, path)
        if value is None:
            continue
        dt = _coerce_datetime_value(value)
        if dt is not None:
            return dt
    return None


def _extract_arrival_dt(row: Mapping[str, Any]) -> Optional[datetime]:
    candidate_paths: Sequence[Tuple[str, ...]] = [
        ("arrivalOnBlockUtc",),
        ("arrivalOnBlock",),
        ("arrOnBlock",),
        ("blockOnEstUTC",),
        ("blockOnEstUtc",),
        ("blockOnEstLocal",),
        ("blockOnEstimatedUTC",),
        ("blockOnEstimatedUtc",),
        ("blockOnEstimateUTC",),
        ("blockOnEstimateUtc",),
        ("scheduledIn",),
        ("onBlock",),
        ("onBlockActual",),
        ("onBlockScheduled",),
        ("times", "onBlock", "estimatedUtc"),
        ("times", "onBlock", "scheduledUtc"),
        ("times", "onBlock", "actualUtc"),
        ("times", "arrival", "estimatedUtc"),
        ("times", "arrival", "scheduledUtc"),
        ("times", "arrival", "actualUtc"),
        ("arrival", "estimatedUtc"),
        ("arrival", "scheduledUtc"),
        ("arrival", "actualUtc"),
    ]
    for path in candidate_paths:
        value = _extract_nested_value(row, path)
        if value is None:
            continue
        dt = _coerce_datetime_value(value)
        if dt is not None:
            return dt
    return None


def _normalize_airport(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        for key in ("icao", "iata", "code", "name", "airport"):
            nested = value.get(key)
            result = _normalize_airport(nested)
            if result:
                return result
        return None
    text = _normalize_str(value)
    if not text:
        return None
    upper = text.upper()
    match = re.search(r"[A-Z]{4}", upper)
    if match:
        return match.group(0)
    match = re.search(r"[A-Z]{3}", upper)
    if match:
        return match.group(0)
    return upper


def _extract_airport(row: Mapping[str, Any], keys: Iterable[str]) -> Optional[str]:
    for key in keys:
        value = row.get(key)
        result = _normalize_airport(value)
        if result:
            return result
        if isinstance(value, Mapping):
            continue
        if value is None and isinstance(row.get("times"), Mapping):
            nested = row.get("times", {})
            nested_value = nested.get(key)
            result = _normalize_airport(nested_value)
            if result:
                return result
    return None


def _first_non_empty(row: Mapping[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        value = row.get(key)
        text = _normalize_str(value)
        if text:
            return text
    return None


def _to_naive_utc(dt: Optional[datetime]) -> pd.Timestamp:
    if dt is None:
        return pd.NaT
    ts = pd.to_datetime(dt, utc=True)
    if pd.isna(ts):
        return pd.NaT
    return ts.tz_localize(None)


def _normalise_fl3xx_rows(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    records = []
    for row in rows:
        tail = _normalize_str(row.get("tail"))
        if tail:
            tail = tail.replace("-", "").upper()
        booking = _first_non_empty(
            row,
            (
                "bookingIdentifier",
                "bookingidentifier",
                "booking_identifier",
                "bookingCode",
                "booking_code",
                "bookingReference",
                "bookingRef",
                "booking_reference",
                "bookingReferenceNumber",
                "bookingNumber",
                "booking_number",
                "bookingId",
                "bookingID",
                "booking_id",
                "salesOrderNumber",
            ),
        )
        if not booking:
            booking_value = row.get("booking")
            if isinstance(booking_value, Mapping):
                booking = _first_non_empty(
                    booking_value,
                    (
                        "identifier",
                        "code",
                        "reference",
                        "number",
                        "id",
                    ),
                )
            else:
                booking = _normalize_str(booking_value)
        from_icao = _extract_airport(
            row,
            (
                "departure_airport",
                "dep_airport",
                "departureAirport",
                "airportFrom",
                "fromAirport",
                "departure",
            ),
        )
        to_icao = _extract_airport(
            row,
            (
                "arrival_airport",
                "arr_airport",
                "arrivalAirport",
                "airportTo",
                "toAirport",
                "arrival",
            ),
        )
        offblock = _to_naive_utc(_extract_departure_dt(row))
        onblock = _to_naive_utc(_extract_arrival_dt(row))
        aircraft_type = _first_non_empty(
            row,
            (
                "assignedAircraftType",
                "aircraftType",
                "aircraftCategory",
                "aircraftClass",
            ),
        )
        workflow = _first_non_empty(
            row,
            (
                "workflowCustomName",
                "workflowName",
                "workflow",
            ),
        )

        records.append(
            {
                "Booking": booking,
                "From (ICAO)": from_icao,
                "To (ICAO)": to_icao,
                "Tail": tail or "",
                "OffBlock": offblock,
                "OnBlock": onblock,
                "Aircraft Type": aircraft_type,
                "Workflow": workflow,
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["Tail"] = df["Tail"].fillna("").astype(str).str.replace("-", "", regex=False).str.upper()
    return df


def _iter_date_chunks(start: date, end: date, chunk_days: int) -> Iterable[Tuple[date, date]]:
    """Yield (start, end) pairs covering the range in chunk_days spans."""

    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")

    if end < start:
        start, end = end, start

    if end <= start:
        yield start, end
        return

    cursor = start
    delta = timedelta(days=chunk_days)
    while cursor < end:
        chunk_end = min(cursor + delta, end)
        yield cursor, chunk_end
        cursor = chunk_end


@st.cache_data(show_spinner=True, ttl=180)
def fetch_fl3xx_dataframe(start: date, end: date, token: Optional[str] = None) -> Tuple[pd.DataFrame, dict]:
    config = _build_fl3xx_config(token)

    if not (config.api_token or config.auth_header):
        raise RuntimeError(
            "No FL3XX credentials configured. Update Streamlit secrets or environment variables."
        )

    if end < start:
        start, end = end, start

    all_flights = []
    seen_ids = set()
    duplicate_collisions = 0
    chunk_summaries = []
    raw_flight_count = 0
    last_metadata: Optional[dict] = None

    for chunk_start, chunk_end in _iter_date_chunks(start, end, FL3XX_FETCH_CHUNK_DAYS):
        flights, metadata = fetch_flights(
            config, from_date=chunk_start, to_date=chunk_end
        )
        last_metadata = metadata
        raw_flight_count += len(flights)

        chunk_summary = {
            **metadata,
            "chunk_range": {
                "from": chunk_start.isoformat(),
                "to": chunk_end.isoformat(),
            },
            "flight_count": len(flights),
        }
        chunk_summaries.append(chunk_summary)

        for flight in flights:
            flight_id = flight.get("id") if isinstance(flight, Mapping) else None
            if flight_id is not None:
                if flight_id in seen_ids:
                    duplicate_collisions += 1
                    continue
                seen_ids.add(flight_id)
            all_flights.append(flight)

    normalized_rows, stats = normalize_fl3xx_payload({"items": all_flights})
    normalized_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    stats["skipped_subcharter"] = skipped_subcharter

    df = _normalise_fl3xx_rows(normalized_rows)

    fetched_at = last_metadata.get("fetched_at") if last_metadata else None
    time_zone = last_metadata.get("time_zone") if last_metadata else None
    value = last_metadata.get("value") if last_metadata else None
    request_params = {
        "from": start.isoformat(),
        "to": end.isoformat(),
    }
    if time_zone:
        request_params["timeZone"] = time_zone
    if value:
        request_params["value"] = value
    request_params.update(config.extra_params)

    metadata = {
        "from_date": start.isoformat(),
        "to_date": end.isoformat(),
        "time_zone": time_zone,
        "value": value,
        "fetched_at": fetched_at,
        "hash": compute_flights_digest(all_flights),
        "request_url": config.base_url,
        "request_params": request_params,
        "chunk_size_days": FL3XX_FETCH_CHUNK_DAYS,
        "chunk_count": len(chunk_summaries),
        "chunk_summaries": chunk_summaries,
        "raw_flight_count": raw_flight_count,
        "duplicate_collisions": duplicate_collisions,
        "normalization": stats,
        "flight_count": len(all_flights),
    }

    return df, metadata

# ---------------- Helpers ----------------
def _cyvr_future_exempt(ap: str, sched_dt: pd.Timestamp, threshold_days: int = 4) -> bool:
    """Return True if this should NOT be flagged as Missing:
       CYVR legs that are threshold_days or more days in the future."""
    if ap != "CYVR" or pd.isna(sched_dt):
        return False
    today = pd.Timestamp.utcnow().date()
    days_out = (sched_dt.date() - today).days
    return days_out >= threshold_days

def show_table(df: pd.DataFrame, title: str, key: str):
    st.subheader(title)
    if df is None or df.empty:
        st.write("— no rows —")
        return
    st.dataframe(df, use_container_width=True)
    st.download_button(
        f"Download {title} CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{key}.csv",
        mime="text/csv",
        key=f"dl_{key}"
    )

def _tail_future_exempt(sched_dt: pd.Timestamp, threshold_days: int = 3) -> bool:
    """Hide tail-mismatch items when the leg is >= threshold_days (3+) days in the future."""
    if pd.isna(sched_dt):
        return False
    today = pd.Timestamp.utcnow().date()
    days_out = (sched_dt.date() - today).days
    return days_out >= threshold_days


# ---------------- Tail filtering ----------------
def load_tails(path="tails.csv"):
    if not os.path.exists(path):
        print(f"No tail list found at {path}, skipping filter")
        return []
    try:
        df = pd.read_csv(path)
        return df["Tail"].astype(str).str.replace("-", "").str.upper().tolist()
    except Exception as e:
        print(f"Error reading tail list: {e}")
        return []

TAILS = load_tails()
if TAILS:
    st.sidebar.success(f"Loaded {len(TAILS)} company tails from tails.csv")
else:
    st.sidebar.warning("No tails.csv found — showing all OCS slots")


def _default_date_range() -> Tuple[date, date]:
    today = datetime.utcnow().date()
    return today, today + timedelta(days=1)


st.sidebar.markdown("---")
st.sidebar.header("FL3XX Flights Source")

default_start, default_end = _default_date_range()
date_selection = st.sidebar.date_input(
    "Flight window (UTC)",
    value=(default_start, default_end),
)

if isinstance(date_selection, (tuple, list)) and len(date_selection) == 2:
    fl3xx_from, fl3xx_to = date_selection
else:
    fl3xx_from = fl3xx_to = date_selection

secrets_section = st.secrets.get("fl3xx_api", {})
env_token = os.getenv("FL3XX_API_TOKEN")
env_auth_header = os.getenv("FL3XX_AUTH_HEADER_VALUE")

has_token = bool(secrets_section.get("api_token") or env_token)
has_auth_header = bool(secrets_section.get("auth_header") or env_auth_header)

credentials_configured = has_token or has_auth_header

if credentials_configured:
    st.sidebar.info("Using configured Streamlit secrets for FL3XX API access.")
else:
    st.sidebar.error(
        "No FL3XX API credentials found. Configure the fl3xx_api section in Streamlit secrets."
    )

fetch_clicked = st.sidebar.button(
    "Fetch flights from FL3XX",
    disabled=not credentials_configured,
)

if fetch_clicked:
    try:
        with st.spinner("Fetching flights from FL3XX..."):
            api_df, api_meta = fetch_fl3xx_dataframe(
                fl3xx_from,
                fl3xx_to,
            )
    except Exception as exc:
        st.sidebar.error(f"FL3XX fetch failed: {exc}")
    else:
        st.session_state["fl3xx_api_df"] = api_df
        st.session_state["fl3xx_api_meta"] = api_meta
        if api_df.empty:
            st.sidebar.warning("FL3XX API returned no legs for the selected window.")
        else:
            st.sidebar.success(
                f"Fetched {len(api_df)} FL3XX legs covering {api_meta.get('flight_count', 0)} flights."
            )

if "fl3xx_api_meta" in st.session_state:
    meta = st.session_state.get("fl3xx_api_meta", {})
    with st.sidebar.expander("Latest FL3XX fetch details", expanded=False):
        st.json(meta)

# ---------------- Utils ----------------
def _read_csv_reset(file, **kwargs):
    file.seek(0)
    try:
        return pd.read_csv(file, **kwargs)
    except UnicodeDecodeError:
        file.seek(0)
        return pd.read_csv(file, encoding="latin-1", **kwargs)

def _hhmm_str(x):
    """Convert various input formats into zero-padded HHMM string."""
    if pd.isna(x):
        return None
    s = str(x).strip()

    # Handle floats like 15.0, 930.0
    if re.match(r"^\d+\.0$", s):
        s = s[:-2]

    # Handle cases like 9:30 or 09:30
    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            hh, mm = parts
            return hh.zfill(2) + mm.zfill(2)

    # Remove non-digits
    s = re.sub(r"\D", "", s)

    # Accept 1–4 digit numbers
    if len(s) == 1:   # "5" → "0005"
        s = s.zfill(4)
    elif len(s) == 2: # "15" → "0015"
        s = s.zfill(4)
    elif len(s) == 3: # "930" → "0930"
        s = s.zfill(4)
    elif len(s) == 4: # "1530" → "1530"
        pass
    else:
        return None

    return s

# ---------------- OCS Parsing ----------------
def parse_gir_file(file):
    df = _read_csv_reset(file)
    col = df.columns[0]
    parsed = []

    for line in df[col].astype(str).tolist():
        # normalize whitespace
        line = line.replace("\u00A0", " ")
        line = re.sub(r"\s+", " ", line.strip())
        parts = line.split(" ")
        if len(parts) < 5:
            continue

        try:
            # 1) Find date token like 13SEP anywhere
            date_idx = next((i for i, p in enumerate(parts) if re.match(r"^\d{2}[A-Z]{3}$", p)), None)
            if date_idx is None:
                continue
            date_str = parts[date_idx]
            day = int(date_str[:2])
            month = MONTHS.get(date_str[2:5].upper())
            if not month:
                continue

            # 2) Find ICAO + time (supports: CYUL0320, 0320CYUL, or split "CYUL","0320" / "0320","CYUL")
            link_icao, slot_time = None, None

            # search tokens after date for ICAO/time
            for i in range(date_idx + 1, len(parts)):
                tok = parts[i]

                m1 = re.match(r"^([A-Z]{4})(\d{3,4})$", tok)   # CYUL0320
                m2 = re.match(r"^(\d{3,4})([A-Z]{4})$", tok)   # 0320CYUL
                if m1:
                    link_icao, slot_time = m1.groups()
                    break
                if m2:
                    slot_time, link_icao = m2.groups()
                    break

                # split across two tokens?
                if i + 1 < len(parts):
                    nxt = parts[i + 1]
                    if re.match(r"^[A-Z]{4}$", tok) and re.match(r"^\d{3,4}$", nxt):
                        link_icao, slot_time = tok, nxt
                        break
                    if re.match(r"^\d{3,4}$", tok) and re.match(r"^[A-Z]{4}$", nxt):
                        slot_time, link_icao = tok, nxt
                        break

            if not slot_time or not link_icao:
                continue

            slot_time = _hhmm_str(slot_time)
            if not slot_time:
                continue

            # 3) Tail token like RE.CFASY
            try:
                tail_token = next(p for p in parts if p.startswith("RE."))
            except StopIteration:
                continue
            tail = tail_token.replace("RE.", "").upper()

            # 4) Slot token like IDA.CYYZAGNN953500/  or IDD.CYYZDGNN027800/
            try:
                slot_token = next(p for p in parts if p.startswith("ID"))
            except StopIteration:
                continue

            mslot = re.match(r"ID[AD]\.(?P<apt>[A-Z]{4})(?P<mov>[AD])(?P<ref>[A-Z0-9]+)/?", slot_token)
            if not mslot:
                continue
            gd = mslot.groupdict()
            
            # Build full slot reference to match structured format (e.g., CYULAGN0396000)
            slot_ref_full = f"{gd['apt']}{gd['mov']}{gd['ref']}"
            
            parsed.append({
                "SlotAirport": gd["apt"],
                "Date": (day, month),
                "Movement": "ARR" if gd["mov"] == "A" else "DEP",
                "SlotTimeHHMM": slot_time,
                "Tail": tail,
                "SlotRef": slot_ref_full  # <-- use full slot id
})
            
        except Exception:
            # keep robust; skip only the bad line
            continue

    print(f"Parsed {len(parsed)} GIR rows out of {len(df)}")
    return pd.DataFrame(parsed, columns=["SlotAirport","Date","Movement","SlotTimeHHMM","Tail","SlotRef"])


def parse_structured_file(file):
    df = _read_csv_reset(file)
    df.columns = [re.sub(r"[^A-Za-z0-9]", "", c).upper() for c in df.columns]

    rows = []
    for _, r in df.iterrows():
        ap = r.get("AP")
        date_val = str(r.get("DATE")).strip()
        if pd.isna(ap) or not date_val:
            continue

        token = date_val.split()[0] if " " in date_val else date_val
        if not re.match(r"\d{2}[A-Z]{3}", token):
            continue
        day = int(token[:2])
        month = MONTHS.get(token[2:5].upper())
        if not month:
            continue

        tail = str(r.get("ACREG", "")).replace("-", "").upper()

        # Arrival
        atime = _hhmm_str(r.get("ATIME"))
        aslot = r.get("ASLOTID")
        if atime and pd.notna(aslot):
            rows.append({
                "SlotAirport": str(ap).upper(),
                "Date": (day, month),
                "Movement": "ARR",
                "SlotTimeHHMM": atime,
                "Tail": tail,
                "SlotRef": str(aslot)
            })

        # Departure
        dtime = _hhmm_str(r.get("DTIME"))
        dslot = r.get("DSLOTID")
        if dtime and pd.notna(dslot):
            rows.append({
                "SlotAirport": str(ap).upper(),
                "Date": (day, month),
                "Movement": "DEP",
                "SlotTimeHHMM": dtime,
                "Tail": tail,
                "SlotRef": str(dslot)
            })

    print(f"Parsed {len(rows)} structured rows out of {len(df)}")
    return pd.DataFrame(rows, columns=["SlotAirport","Date","Movement","SlotTimeHHMM","Tail","SlotRef"])

def parse_ocs_file(file):
    head = _read_csv_reset(file, nrows=5)
    cols_norm = [c.strip().upper() for c in head.columns]
    file.seek(0)
    if "GIR" in cols_norm:
        return parse_gir_file(file)
    hallmark = {"A/P","A/C REG","ATIME","DTIME","ASLOTID","DSLOTID"}
    if hallmark.intersection(set(cols_norm)):
        return parse_structured_file(file)
    return parse_structured_file(file)

# ---------------- Fl3xx Parsing ----------------
def parse_fl3xx_file(file):
    df = _read_csv_reset(file)
    if "Aircraft" in df.columns:
        df["Tail"] = df["Aircraft"].astype(str).str.replace("-", "", regex=False).str.upper()
    else:
        df["Tail"] = ""
    def parse_dt(col):
        if col in df.columns:
            return pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        return pd.Series([pd.NaT]*len(df))
    df["OnBlock"] = parse_dt("On-Block (Est)")
    dep_try = ["Off-Block (Est)","Out-Block (Est)","STD (UTC)","Scheduled Departure (UTC)","Departure Time"]
    dep_series = None
    for c in dep_try:
        if c in df.columns:
            dep_series = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
            break
    df["OffBlock"] = dep_series
    keep = ["Booking","From (ICAO)","To (ICAO)","Tail","OnBlock","OffBlock","Aircraft Type","Workflow"]
    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()

# ---------------- Comparison ----------------
def compare(fl3xx_df: pd.DataFrame, ocs_df: pd.DataFrame):
    # Split misaligned into tail vs time
    results = {"Matched": [], "Missing": [], "MisalignedTail": [], "MisalignedTime": []}

    # Build Fl3xx legs at slot airports
    legs = []
    for _, r in fl3xx_df.iterrows():
        tail = str(r.get("Tail", "")).upper()
        to_ap = r.get("To (ICAO)")
        if isinstance(to_ap, str) and to_ap in SLOT_AIRPORTS and pd.notna(r.get("OnBlock")):
            legs.append({"Flight": r.get("Booking"), "Tail": tail, "Airport": to_ap,
                         "Movement": "ARR", "SchedDT": r.get("OnBlock")})
        from_ap = r.get("From (ICAO)")
        if isinstance(from_ap, str) and from_ap in SLOT_AIRPORTS and pd.notna(r.get("OffBlock")):
            legs.append({"Flight": r.get("Booking"), "Tail": tail, "Airport": from_ap,
                         "Movement": "DEP", "SchedDT": r.get("OffBlock")})

    # De-duplicate legs so the same flight isn't processed twice
    if legs:
        legs_df = pd.DataFrame(legs)
        legs_df["SchedDT"] = pd.to_datetime(legs_df["SchedDT"]).dt.floor("min")
        legs_df["LegKey"] = (
            legs_df["Flight"].astype(str) + "|" +
            legs_df["Tail"].astype(str) + "|" +
            legs_df["Airport"].astype(str) + "|" +
            legs_df["Movement"].astype(str) + "|" +
            legs_df["SchedDT"].astype(str)
        )
        legs_df = legs_df.drop_duplicates(subset="LegKey").drop(columns="LegKey")
        legs = legs_df.to_dict("records")

    def minutes_diff(a, b):
        return abs(int((a - b).total_seconds() // 60))

    # Prevent reusing a slot once it is matched to a leg
    allocated_slot_refs = set()
    # Optional: avoid suggesting the same slot for multiple legs
    suggested_slot_refs = set()

    for leg in legs:
        ap, move, tail, sched_dt = leg["Airport"], leg["Movement"], leg["Tail"], leg["SchedDT"]

        # Start with slots for same airport & movement that are NOT already allocated
        cand = ocs_df[
            (ocs_df["SlotAirport"] == ap) &
            (ocs_df["Movement"] == move) &
            (~ocs_df["SlotRef"].astype(str).isin(allocated_slot_refs))
        ].copy()

        if cand.empty:
            if _cyvr_future_exempt(ap, sched_dt):  # your helper
                continue
            results["Missing"].append({**leg, "Reason": "No slot for airport/movement"})
            continue

        # Build each slot's absolute datetime using the slot's own (day, month)
        def slot_dt_for_row(row):
            d, m = row["Date"]
            hhmm = row["SlotTimeHHMM"]; hh = int(hhmm[:2]); mm = int(hhmm[2:])
            return datetime(sched_dt.year, m, d, hh, mm)

        cand["_SlotDT"] = cand.apply(slot_dt_for_row, axis=1)

        # Keep slots on the same day or ±1 day of the leg (cross-midnight tolerance)
        cand = cand[cand["_SlotDT"].apply(lambda d: abs((d.date() - sched_dt.date()).days) <= 1)]
        if cand.empty:
            if _cyvr_future_exempt(ap, sched_dt):
                continue
            results["Missing"].append({**leg, "Reason": "No slot (±1 day)"})
            continue

        window = WINDOWS_MIN.get(ap, 30)

        # Compute best same-tail and best any-tail deltas
        same_tail = cand[cand["Tail"] == tail]
        same_tail_best, same_tail_row = None, None
        if not same_tail.empty:
            deltas_same = same_tail["_SlotDT"].apply(lambda dt: minutes_diff(sched_dt, dt))
            same_idx = deltas_same.idxmin()
            same_tail_best = int(deltas_same.loc[same_idx])
            same_tail_row  = same_tail.loc[same_idx]

        deltas_any = cand["_SlotDT"].apply(lambda dt: minutes_diff(sched_dt, dt))
        any_idx = deltas_any.idxmin()
        any_best = int(deltas_any.loc[any_idx])
        any_row  = cand.loc[any_idx]

        # Decision order:
        # 1) same-tail within window → Matched (allocate; cannot be reused)
        if same_tail_best is not None and same_tail_best <= window:
            results["Matched"].append({
                **leg,
                "SlotTime": same_tail_row["SlotTimeHHMM"],
                "DeltaMin": same_tail_best,
                "SlotRef":  same_tail_row["SlotRef"]
            })
            allocated_slot_refs.add(str(same_tail_row["SlotRef"]))
            continue

        # 2) any-tail within window → Tail mismatch (do NOT allocate; wrong tail booked)
        if any_best <= window:
            if not _tail_future_exempt(sched_dt, threshold_days=3):  # your helper
                if str(any_row["SlotRef"]) not in suggested_slot_refs:
                    results["MisalignedTail"].append({
                        **leg,
                        "NearestSlotTime": any_row["SlotTimeHHMM"],
                        "DeltaMin": any_best,
                        "WindowMin": window,
                        "SlotTail": any_row["Tail"],
                        "SlotRef":  any_row["SlotRef"]
                    })
                    suggested_slot_refs.add(str(any_row["SlotRef"]))
            continue

        # 3) same-tail exists but out of window → Time mismatch (do NOT allocate)
        if same_tail_best is not None:
            results["MisalignedTime"].append({
                **leg,
                "NearestSlotTime": same_tail_row["SlotTimeHHMM"],
                "DeltaMin": same_tail_best,
                "WindowMin": window,
                "SlotRef":  same_tail_row["SlotRef"]
            })
            continue

        # 4) otherwise → Missing (respect CYVR exemption)
        if not _cyvr_future_exempt(ap, sched_dt):
            results["Missing"].append({**leg, "Reason": "No matching tail/time within window"})

    # --- Slot-side evaluation (Stale)
    # A slot is NOT stale if:
    #   (a) there is ANY leg with same airport/movement/TAIL within ±1 day of the slot's date, or
    #   (b) we've already used it (Matched) or suggested it (Tail mismatch), or
    #   (c) it's a far-future wrong-tail case (we suppress tail mismatches 5+ days out).
    def has_leg_for_slot(slot_row):
        ap   = slot_row["SlotAirport"]
        mv   = slot_row["Movement"]
        tail = slot_row["Tail"]
        day, month = slot_row["Date"]

        for lg in legs:
            if lg["Airport"] != ap or lg["Movement"] != mv or lg["Tail"] != tail:
                continue
            # build slot date using leg's year (slots have no year)
            slot_date = datetime(lg["SchedDT"].year, month, day).date()
            if abs((slot_date - lg["SchedDT"].date()).days) <= 1:
                return True
        return False

    def _far_future_wrong_tail(slot_row):
        ap   = slot_row["SlotAirport"]
        mv   = slot_row["Movement"]
        tail = slot_row["Tail"]
        day, month = slot_row["Date"]
        for lg in legs:
            if lg["Airport"] != ap or lg["Movement"] != mv:
                continue
            # slot date in the leg's year
            slot_date = datetime(lg["SchedDT"].year, month, day).date()
            # same day ±1 indicates this slot relates to that leg's operation
            if abs((slot_date - lg["SchedDT"].date()).days) <= 1:
                # wrong tail & we intentionally suppress tail mismatches far in the future
                if lg["Tail"] != tail and _tail_future_exempt(lg["SchedDT"], threshold_days=3):
                    return True
        return False

    # Slots already used (Matched) or suggested (Tail mismatch) should not be stale
    used_or_suggested = set()
    used_or_suggested.update(allocated_slot_refs)
    used_or_suggested.update(suggested_slot_refs)

    stale_df = ocs_df[
        (~ocs_df["SlotRef"].astype(str).isin(used_or_suggested)) &
        (~ocs_df.apply(has_leg_for_slot, axis=1)) &
        (~ocs_df.apply(_far_future_wrong_tail, axis=1))
    ].copy()




    return results, stale_df


# ---------------- UI ----------------
fl3xx_files = st.file_uploader("Upload Fl3xx CSV(s)", type="csv", accept_multiple_files=True)
ocs_files = st.file_uploader("Upload OCS CSV(s)", type="csv", accept_multiple_files=True)

fl3xx_frames = []
if fl3xx_files:
    fl3xx_frames.extend(parse_fl3xx_file(f) for f in fl3xx_files)

api_dataframe = st.session_state.get("fl3xx_api_df")
if isinstance(api_dataframe, pd.DataFrame) and not api_dataframe.empty:
    fl3xx_frames.append(api_dataframe)

if fl3xx_frames and ocs_files:
    fl3xx_df = pd.concat(fl3xx_frames, ignore_index=True)
    ocs_list = [parse_ocs_file(f) for f in ocs_files]
    ocs_list = [df for df in ocs_list if not df.empty]
    ocs_df = pd.concat(ocs_list, ignore_index=True) if ocs_list else pd.DataFrame(columns=["SlotAirport","Date","Movement","SlotTimeHHMM","Tail","SlotRef"])

    # Filter OCS slots by tails.csv
    if TAILS:
        before = len(ocs_df)
        ocs_df = ocs_df[ocs_df["Tail"].isin(TAILS)]
        st.info(f"Filtered OCS slots: {before} → {len(ocs_df)} using company tail list")

    # NEW: ensure no duplicate SlotRef remain
    ocs_df = ocs_df.drop_duplicates(subset=["SlotRef"]).reset_index(drop=True)

    st.success(f"Loaded {len(fl3xx_df)} flights and {len(ocs_df)} slots.")

    with st.expander("🔎 Preview parsed OCS (normalized)"):
        st.dataframe(ocs_df.head(20))
    with st.expander("🔎 Preview parsed Fl3xx"):
        st.dataframe(fl3xx_df.head(20))

    results, stale = compare(fl3xx_df, ocs_df)
    
    matched_df       = pd.DataFrame(results["Matched"])
    missing_df       = pd.DataFrame(results["Missing"])
    mis_tail_df      = pd.DataFrame(results["MisalignedTail"])
    mis_time_df      = pd.DataFrame(results["MisalignedTime"])
    stale_df         = with_datestr(stale)  # if you added the pretty date helper
    
    show_table(matched_df,  "✔ Matched",          "matched")
    show_table(missing_df,  "⚠ Missing",          "missing")
    show_table(mis_tail_df, "⚠ Tail mismatch",    "misaligned_tail")
    show_table(mis_time_df, "⚠ Time mismatch",    "misaligned_time")
    show_table(stale_df,    "⚠ Stale Slots",      "stale")


elif not ocs_files:
    st.info("Upload OCS CSV files to compare against FL3XX data.")
else:
    st.info("Provide FL3XX data via the API fetch or CSV uploads to begin.")
