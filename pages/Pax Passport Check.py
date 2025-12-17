from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import (
    PassengerDetail,
    extract_passengers_from_pax_details,
    fetch_flights,
    fetch_flight_pax_details,
)
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    is_customs_leg,
    load_airport_metadata_lookup,
    normalize_fl3xx_payload,
    safe_parse_dt,
)

configure_page(page_title="Pax Passport Check")
password_gate()
render_sidebar()

st.title("🛂 Pax Passport Check")

st.write(
    """
    Scan upcoming customs legs for passengers whose passport expiration dates look risky.
    The tool batches flight searches into 3-day windows, then pulls passenger passport
    details for each pax leg that crosses an international border. Any passports expiring
    **before 20 Jul 2026** or **after 1 Jan 2036** are flagged for review.
    """
)

EXPIRY_SOON_CUTOFF = date(2026, 7, 20)
EXPIRY_FAR_CUTOFF = date(2036, 1, 1)
CHUNK_DAYS = 3


def _settings_digest(settings: Mapping[str, Any]) -> str:
    normalized = json.dumps(settings, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _chunk_ranges(start: date, end_inclusive: date, *, span_days: int = CHUNK_DAYS) -> Iterable[Tuple[date, date]]:
    end_exclusive = end_inclusive + timedelta(days=1)
    current = start
    while current < end_exclusive:
        chunk_end = min(current + timedelta(days=span_days), end_exclusive)
        yield current, chunk_end
        current = chunk_end


def _format_passenger_name(pax: PassengerDetail) -> str:
    parts = [
        part
        for part in (pax.first_name, pax.middle_name, pax.last_name)
        if isinstance(part, str) and part.strip()
    ]
    return " ".join(parts) if parts else "Unknown"


def _passport_expiry_info(expiration_ms: Optional[int]) -> Tuple[Optional[date], Optional[str]]:
    if expiration_ms is None:
        return None, "Missing passport expiration"

    try:
        expiry_date = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc).date()
    except Exception:
        return None, "Unreadable passport expiration"

    if expiry_date < EXPIRY_SOON_CUTOFF:
        return expiry_date, "Expiring before 20 Jul 2026"
    if expiry_date > EXPIRY_FAR_CUTOFF:
        return expiry_date, "Expires after 1 Jan 2036"
    return expiry_date, None


def _is_pax_leg(leg: Mapping[str, Any]) -> bool:
    pax_number = leg.get("paxNumber")
    if isinstance(pax_number, int) and pax_number > 0:
        return True
    if pax_number is not None:
        try:
            return int(pax_number) > 0
        except (TypeError, ValueError):
            pass
    pax_refs = leg.get("paxReferences")
    if isinstance(pax_refs, Sequence):
        return any(isinstance(entry, Mapping) for entry in pax_refs)
    return False


@st.cache_data(show_spinner=True, ttl=300, hash_funcs={dict: lambda _: "0"})
def _load_legs(
    settings_digest: str,
    settings: Dict[str, Any],
    *,
    from_date: date,
    to_date: date,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    _ = settings_digest  # participate in cache key without hashing secrets
    config = build_fl3xx_api_config(settings)
    flights: list[dict[str, Any]] = []
    chunk_meta: list[dict[str, Any]] = []

    for chunk_start, chunk_end in _chunk_ranges(from_date, to_date):
        chunk_flights, meta = fetch_flights(config, from_date=chunk_start, to_date=chunk_end)
        flights.extend(chunk_flights)
        chunk_meta.append(meta)

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    filtered_rows, subcharter_skipped = filter_out_subcharter_rows(normalized_rows)

    metadata = {
        "total_flights": len(flights),
        "legs_after_filter": len(filtered_rows),
        "subcharters_filtered": subcharter_skipped,
        "chunks": chunk_meta,
        "normalization": normalization_stats,
    }
    return filtered_rows, metadata


@st.cache_data(show_spinner=False, ttl=300, hash_funcs={dict: lambda _: "0"})
def _load_passengers(
    settings_digest: str,
    settings: Dict[str, Any],
    flight_id: str,
) -> List[PassengerDetail]:
    _ = settings_digest
    config = build_fl3xx_api_config(settings)
    pax_payload = fetch_flight_pax_details(config, flight_id)
    return extract_passengers_from_pax_details(pax_payload)


def _extract_dep_time(leg: Mapping[str, Any]) -> Optional[datetime]:
    dep_raw = leg.get("dep_time") or leg.get("departureDate") or leg.get("departureDateUTC")
    if not dep_raw:
        return None
    try:
        dep_dt = safe_parse_dt(str(dep_raw))
    except Exception:
        return None
    if dep_dt.tzinfo is None:
        return dep_dt.replace(tzinfo=timezone.utc)
    return dep_dt.astimezone(timezone.utc)


def _build_flight_label(leg: Mapping[str, Any]) -> str:
    dep = str(leg.get("departure_airport") or "?").upper()
    arr = str(leg.get("arrival_airport") or "?").upper()
    return f"{dep} → {arr}"


def _collect_flagged_passports(
    legs: Iterable[Mapping[str, Any]],
    *,
    settings_digest: str,
    settings: Dict[str, Any],
    airport_lookup: Mapping[str, Mapping[str, Optional[Any]]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    flagged: list[dict[str, Any]] = []
    errors: list[str] = []

    for leg in legs:
        if not (is_customs_leg(leg, airport_lookup) and _is_pax_leg(leg)):
            continue

        flight_id = leg.get("flightId") or leg.get("flight_id") or leg.get("id")
        if not flight_id:
            errors.append(f"Missing flight ID for {leg.get('tail', 'Unknown tail')} { _build_flight_label(leg)}")
            continue

        try:
            passengers = _load_passengers(settings_digest, settings, str(flight_id))
        except Exception as exc:  # pragma: no cover - runtime fetch failures
            errors.append(f"Unable to load passengers for flight {flight_id}: {exc}")
            continue

        dep_dt = _extract_dep_time(leg)
        dep_time_label = dep_dt.isoformat().replace("+00:00", "Z") if dep_dt else "Unknown"

        for pax in passengers:
            expiry_date, flag_reason = _passport_expiry_info(pax.document_expiration)
            if flag_reason is None:
                continue

            flagged.append(
                {
                    "Passenger": _format_passenger_name(pax),
                    "Passport Expiry": expiry_date.isoformat() if expiry_date else "—",
                    "Flag": flag_reason,
                    "Tail": leg.get("tail"),
                    "Flight": _build_flight_label(leg),
                    "Departure (UTC)": dep_time_label,
                    "Booking": leg.get("bookingIdentifier") or leg.get("bookingReference"),
                    "Flight ID": str(flight_id),
                    "Nationality": pax.nationality_iso3,
                }
            )

    return flagged, errors


api_settings = st.secrets.get("fl3xx_api", {})
if not api_settings:
    st.error("FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml`.")
    st.stop()

settings_digest = _settings_digest(api_settings)

start_default = date.today()
end_default = start_default + timedelta(days=7)

with st.form("passport_scan"):
    date_range = st.date_input("Date range", value=(start_default, end_default))
    submitted = st.form_submit_button("Run passport scan")

if not submitted:
    st.info("Choose a date range and run the scan to check passport expirations.")
    st.stop()

if isinstance(date_range, Sequence) and len(date_range) == 2:
    start_date, end_date = date_range
elif isinstance(date_range, date):
    start_date, end_date = date_range, date_range + timedelta(days=7)
else:
    st.error("Please choose a valid start and end date.")
    st.stop()

if start_date > end_date:
    st.error("The start date must be on or before the end date.")
    st.stop()

try:
    with st.spinner("Fetching flights…"):
        legs, fetch_metadata = _load_legs(
            settings_digest,
            dict(api_settings),
            from_date=start_date,
            to_date=end_date,
        )
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except Exception as exc:  # pragma: no cover - runtime fetch failures
    st.error(f"Unable to load flights: {exc}")
    st.stop()

airport_lookup = load_airport_metadata_lookup()

with st.spinner("Evaluating passport expirations…"):
    flagged_passports, fetch_errors = _collect_flagged_passports(
        legs,
        settings_digest=settings_digest,
        settings=dict(api_settings),
        airport_lookup=airport_lookup,
    )

summary_cols = st.columns(3)
summary_cols[0].metric("Legs fetched", fetch_metadata.get("legs_after_filter", 0))
summary_cols[1].metric("Customs pax legs scanned", len(
    [leg for leg in legs if is_customs_leg(leg, airport_lookup) and _is_pax_leg(leg)]
))
summary_cols[2].metric("Flagged passengers", len(flagged_passports))

if fetch_errors:
    st.warning("\n".join(fetch_errors))

if not flagged_passports:
    st.success("No passports matched the alert thresholds in the selected window.")
else:
    st.subheader("Flagged passengers")
    st.dataframe(flagged_passports, use_container_width=True, hide_index=True)

st.caption(
    f"Flights were retrieved in {CHUNK_DAYS}-day chunks to cover the full date range without API limits."
)
