from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import streamlit as st
from dateutil.relativedelta import relativedelta

from Home import configure_page, password_gate, render_sidebar
from fl3xx_api import (
    PassengerDetail,
    extract_passengers_from_pax_details,
    fetch_flights,
    fetch_flight_pax_details,
    backfill_missing_passenger_passports,
)
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    is_customs_leg,
    leg_countries,
    load_airport_metadata_lookup,
    normalize_country_code,
    normalize_fl3xx_payload,
    safe_parse_dt,
)

configure_page(page_title="Pax Passport Check")
password_gate()
render_sidebar()

st.title("🛂 Pax Passport Check")

st.write(
    """
    Scan upcoming international arrivals into the US for passengers who need attention before
    customs clearance. The tool batches flight searches into 3-day windows, then pulls passenger
    passport details and APIS destination addresses. Any passports expiring before your selected
    cutoff or **after 1 Jan 2036** are flagged for review, along with passengers missing a US
    address.
    """
)

DEFAULT_EXPIRY_SOON_CUTOFF = date(2026, 7, 20)
DEFAULT_EXPIRY_WINDOW_DAYS = 60
EXPIRY_FAR_CUTOFF = date(2036, 1, 1)
CHUNK_DAYS = 3
EXPIRY_MODE_DATE = "Date cutoff"
EXPIRY_MODE_DAYS = "Days before expiry from flight"
DATE_RANGE_KEY = "passport_check_date_range"
DATE_PRESET_KEY = "passport_check_date_preset"
CUSTOM_DATE_PRESET = "Custom range"
WEEKEND_PRESET = "Upcoming Saturday through Monday"
US_COUNTRY_CODES = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}


def _upcoming_weekend_range(start: date) -> Tuple[date, date]:
    saturday = start + timedelta(days=(5 - start.weekday()) % 7)
    monday = saturday + timedelta(days=2)
    return saturday, monday


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


def _needs_passport_backfill(pax: PassengerDetail) -> bool:
    return not (
        pax.document_number
        and pax.document_issue_country_iso3
        and pax.document_expiration is not None
    )


def _format_display_date(value: date) -> str:
    return value.strftime("%d %b %Y")


def _future_date_presets(start: date) -> Dict[str, Tuple[date, date]]:
    return {
        WEEKEND_PRESET: _upcoming_weekend_range(start),
        "Next week": (start, start + timedelta(days=7)),
        "Next month": (start, start + relativedelta(months=1)),
        "Next 2 months": (start, start + relativedelta(months=2)),
        "Next 6 months": (start, start + relativedelta(months=6)),
        "Next year": (start, start + relativedelta(years=1)),
        "Next 2 years": (start, start + relativedelta(years=2)),
    }


def _passport_expiry_info(
    expiration_ms: Optional[int],
    *,
    expiry_soon_cutoff: date,
    expiry_soon_label: str,
    expiry_far_cutoff: date = EXPIRY_FAR_CUTOFF,
) -> Tuple[Optional[date], Optional[str], Optional[str]]:
    if expiration_ms is None:
        return None, "Missing passport expiration", "missing"

    try:
        expiry_date = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc).date()
    except Exception:
        return None, "Unreadable passport expiration", "missing"

    if expiry_date < expiry_soon_cutoff:
        return expiry_date, expiry_soon_label, "expiring"
    if expiry_date > expiry_far_cutoff:
        return expiry_date, "Expires after 1 Jan 2036", "missing"
    return expiry_date, None, None


def _missing_passport_fields(pax: PassengerDetail) -> List[str]:
    missing: list[str] = []
    if not pax.document_number:
        missing.append("number")
    if not pax.document_issue_country_iso3:
        missing.append("issuing country")
    if pax.document_expiration is None:
        missing.append("expiration")
    return missing


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


def _arrives_in_us(leg: Mapping[str, Any], airport_lookup: Mapping[str, Mapping[str, Optional[Any]]]) -> bool:
    _, arr_country = leg_countries(leg, airport_lookup)
    normalized = normalize_country_code(arr_country)
    return bool(normalized and normalized in US_COUNTRY_CODES)


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
    passengers = extract_passengers_from_pax_details(pax_payload)

    if any(_needs_passport_backfill(pax) for pax in passengers):
        passengers = backfill_missing_passenger_passports(config, passengers)

    return passengers


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
    expiry_mode: str,
    expiry_soon_cutoff: Optional[date],
    expiry_window_days: Optional[int],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    expiring: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    missing_address: list[dict[str, Any]] = []
    errors: list[str] = []

    for leg in legs:
        if not (
            is_customs_leg(leg, airport_lookup)
            and _arrives_in_us(leg, airport_lookup)
            and _is_pax_leg(leg)
        ):
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
        cutoff_date: Optional[date]
        cutoff_label: str
        if expiry_mode == EXPIRY_MODE_DAYS:
            if dep_dt is None:
                errors.append(
                    "Missing departure time for "
                    f"{leg.get('tail', 'Unknown tail')} { _build_flight_label(leg)}; "
                    "unable to apply day-based expiry rule."
                )
                continue
            days = expiry_window_days or 0
            cutoff_date = dep_dt.date() + timedelta(days=days)
            cutoff_label = (
                f"Expiring within {days} days of flight "
                f"(before {_format_display_date(cutoff_date)})"
            )
        else:
            if expiry_soon_cutoff is None:
                errors.append("Missing date cutoff for passport expiry rule.")
                continue
            cutoff_date = expiry_soon_cutoff
            cutoff_label = f"Expiring before {_format_display_date(cutoff_date)}"

        for pax in passengers:
            if pax.has_us_address is False:
                missing_address.append(
                    {
                        "Passenger": _format_passenger_name(pax),
                        "Flag": "Missing US destination address",
                        "Tail": leg.get("tail"),
                        "Flight": _build_flight_label(leg),
                        "Departure (UTC)": dep_time_label,
                        "Booking": leg.get("bookingIdentifier") or leg.get("bookingReference"),
                        "Flight ID": str(flight_id),
                        "Nationality": pax.nationality_iso3,
                    }
                )

            missing_fields = _missing_passport_fields(pax)
            if missing_fields:
                missing.append(
                    {
                        "Passenger": _format_passenger_name(pax),
                        "Passport Expiry": "—",
                        "Flag": f"Missing passport {', '.join(missing_fields)}",
                        "Tail": leg.get("tail"),
                        "Flight": _build_flight_label(leg),
                        "Departure (UTC)": dep_time_label,
                        "Booking": leg.get("bookingIdentifier") or leg.get("bookingReference"),
                        "Flight ID": str(flight_id),
                        "Nationality": pax.nationality_iso3,
                    }
                )
                continue

            expiry_date, flag_reason, flag_category = _passport_expiry_info(
                pax.document_expiration,
                expiry_soon_cutoff=cutoff_date,
                expiry_soon_label=cutoff_label,
            )
            if flag_reason is None:
                continue

            target = expiring if flag_category == "expiring" else missing
            target.append(
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

    return expiring, missing, missing_address, errors


api_settings = st.secrets.get("fl3xx_api", {})
if not api_settings:
    st.error("FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml`.")
    st.stop()

settings_digest = _settings_digest(api_settings)

start_default = date.today()
future_presets = _future_date_presets(start_default)
end_default = start_default + timedelta(days=7)

expiry_mode = st.radio(
    "Passport expiry rule",
    (EXPIRY_MODE_DATE, EXPIRY_MODE_DAYS),
    help="Choose whether to flag based on a fixed cutoff date or days from the flight.",
)

with st.form("passport_scan"):
    preset_labels = [CUSTOM_DATE_PRESET, *future_presets.keys()]
    selected_preset = st.selectbox(
        "Choose a date range",
        preset_labels,
        index=1 if WEEKEND_PRESET in preset_labels else 0,
        key=DATE_PRESET_KEY,
    )
    if selected_preset != CUSTOM_DATE_PRESET:
        st.session_state[DATE_RANGE_KEY] = future_presets[selected_preset]
    elif DATE_RANGE_KEY not in st.session_state:
        st.session_state[DATE_RANGE_KEY] = future_presets.get(WEEKEND_PRESET, (start_default, end_default))

    date_range = st.date_input(
        "Date range",
        key=DATE_RANGE_KEY,
        min_value=start_default,
    )
    expiry_soon_cutoff: Optional[date] = None
    expiry_window_days: Optional[int] = None
    if expiry_mode == EXPIRY_MODE_DATE:
        expiry_soon_cutoff = st.date_input(
            "Flag passports expiring before",
            value=DEFAULT_EXPIRY_SOON_CUTOFF,
            help="Passengers whose passports expire before this date will be flagged.",
        )
    else:
        expiry_window_days = int(
            st.number_input(
                "Flag passports expiring within (days) of the flight",
                min_value=1,
                max_value=3650,
                value=DEFAULT_EXPIRY_WINDOW_DAYS,
                step=1,
                help="Passengers whose passports expire within this many days of the flight date will be flagged.",
            )
        )
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

if expiry_mode == EXPIRY_MODE_DATE:
    if not isinstance(expiry_soon_cutoff, date):
        st.error("Please choose a valid passport expiry cutoff date.")
        st.stop()
else:
    if not isinstance(expiry_window_days, int) or expiry_window_days < 1:
        st.error("Please choose a valid passport expiry window in days.")
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
    expiring_passports, missing_passports, missing_addresses, fetch_errors = _collect_flagged_passports(
        legs,
        settings_digest=settings_digest,
        settings=dict(api_settings),
        airport_lookup=airport_lookup,
        expiry_mode=expiry_mode,
        expiry_soon_cutoff=expiry_soon_cutoff,
        expiry_window_days=expiry_window_days,
    )

summary_cols = st.columns(4)
summary_cols[0].metric("Legs fetched", fetch_metadata.get("legs_after_filter", 0))
summary_cols[1].metric(
    "US-arrival customs pax legs scanned",
    len(
        [
            leg
            for leg in legs
            if is_customs_leg(leg, airport_lookup)
            and _arrives_in_us(leg, airport_lookup)
            and _is_pax_leg(leg)
        ]
    ),
)
summary_cols[2].metric("Expiring before cutoff", len(expiring_passports))
summary_cols[3].metric("Missing passport details", len(missing_passports))

if fetch_errors:
    st.warning("\n".join(fetch_errors))

if not expiring_passports and not missing_passports and not missing_addresses:
    if expiry_mode == EXPIRY_MODE_DAYS:
        cutoff_label = f"within {expiry_window_days} days of the flight"
    else:
        cutoff_label = f"before {_format_display_date(expiry_soon_cutoff)}"
    st.success(
        f"No passports expiring {cutoff_label} or after 1 Jan 2036, "
        "and no missing US addresses, matched the alert thresholds in the selected window."
    )
else:
    if expiring_passports:
        st.subheader("Passports expiring before cutoff")
        st.dataframe(expiring_passports, use_container_width=True, hide_index=True)
    else:
        st.info("No passports were confirmed to be expiring before the selected cutoff.")

    if missing_passports:
        st.subheader("Missing passport details")
        st.dataframe(missing_passports, use_container_width=True, hide_index=True)

    if missing_addresses:
        st.subheader("Missing US destination addresses")
        st.dataframe(missing_addresses, use_container_width=True, hide_index=True)

if expiry_mode == EXPIRY_MODE_DAYS:
    expiry_caption = f"Passports expiring within {expiry_window_days} days of the flight are flagged."
else:
    expiry_caption = (
        f"Passports expiring before {_format_display_date(expiry_soon_cutoff)} are flagged."
    )
st.caption(
    f"Flights were retrieved in {CHUNK_DAYS}-day chunks to cover the full date range without API limits. "
    f"{expiry_caption} Passports expiring after 1 Jan 2036 are flagged. "
    "US arrivals without destination addresses are flagged."
)
