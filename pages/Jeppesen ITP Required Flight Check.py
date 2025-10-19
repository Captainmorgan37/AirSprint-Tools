from __future__ import annotations

import re
from dataclasses import replace
from datetime import date, datetime, timedelta
from math import inf
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from flight_leg_utils import (
    AIRPORT_TZ_FILENAME,
    ARRIVAL_AIRPORT_COLUMNS,
    DEPARTURE_AIRPORT_COLUMNS,
    FlightDataError,
    build_fl3xx_api_config,
    fetch_legs_dataframe,
    load_airport_metadata_lookup,
    safe_parse_dt,
)


st.set_page_config(page_title="Jeppesen ITP Required Flight Check", layout="wide")
st.title("🛫 Jeppesen ITP Required Flight Check")

st.write(
    """
    Scan scheduled flights and highlight any legs operating outside Canada, the United States,
    Mexico, or approved Caribbean countries. Use the generated summary to request Jeppesen ITP
    permits where needed.
    """
)


def _load_fl3xx_settings() -> Dict[str, Any]:
    settings: Dict[str, Any] = {}
    try:
        secrets_section = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        secrets_section = None
    if isinstance(secrets_section, Mapping):
        settings = {str(key): secrets_section[key] for key in secrets_section}
    elif isinstance(secrets_section, dict):
        settings = dict(secrets_section)
    return settings


# The Caribbean list intentionally includes common variants that appear in airport metadata.
_CARIBBEAN_COUNTRIES = {
    "anguilla",
    "antigua and barbuda",
    "aruba",
    "bahamas",
    "bahamas, the",
    "barbados",
    "bermuda",
    "bonaire",
    "british virgin islands",
    "cayman islands",
    "cuba",
    "curacao",
    "dominica",
    "dominican republic",
    "french guiana",
    "grenada",
    "guadeloupe",
    "haiti",
    "jamaica",
    "martinique",
    "montserrat",
    "puerto rico",
    "saba",
    "saint barthelemy",
    "saint kitts and nevis",
    "saint lucia",
    "saint martin",
    "saint vincent and the grenadines",
    "sint eustatius",
    "sint maarten",
    "st. barthelemy",
    "st. kitts and nevis",
    "st. lucia",
    "st. martin",
    "st. vincent and the grenadines",
    "trinidad and tobago",
    "turks and caicos islands",
    "u.s. virgin islands",
    "united states virgin islands",
    "virgin islands",
}

_ALLOWED_COUNTRIES = {
    "canada",
    "mexico",
    "united states",
    "united states of america",
    "usa",
}
_ALLOWED_COUNTRIES.update(_CARIBBEAN_COUNTRIES)


def _normalize_country_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    text = str(name).strip().lower()
    return text or None


_CODE_PATTERN = re.compile(r"\b[A-Za-z0-9]{3,4}\b")


def _extract_codes(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    upper = text.upper()
    if len(upper.replace(" ", "")) in {3, 4} and upper.replace(" ", "").isalnum():
        return [upper.replace(" ", "")]
    return [match.upper() for match in _CODE_PATTERN.findall(upper)]


def _detect_country(
    row: Mapping[str, Any],
    columns: Sequence[str],
    lookup: Mapping[str, Mapping[str, Optional[str]]],
    missing_codes: set[str],
) -> Tuple[Optional[str], Optional[str]]:
    for column in columns:
        if column not in row:
            continue
        value = row[column]
        for code in _extract_codes(value):
            record = lookup.get(code)
            if record:
                country = record.get("country")
                normalized = _normalize_country_name(country)
                if normalized:
                    return country, code
            else:
                missing_codes.add(code)
    return None, None


def _coerce_text(row: Mapping[str, Any], keys: Sequence[str], default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            return text
    return default


BOOKING_KEYS = (
    "bookingIdentifier",
    "booking_identifier",
    "bookingCode",
    "booking_code",
    "bookingNumber",
    "booking_number",
    "bookingReference",
    "booking_reference",
    "bookingId",
    "booking_id",
)

ACCOUNT_KEYS = (
    "accountName",
    "account_name",
    "account",
    "customer",
    "customerName",
    "customer_name",
    "owner",
    "ownerName",
    "owner_name",
    "client",
    "clientName",
    "client_name",
)


with st.sidebar:
    st.header("Date range")
    today = date.today()
    default_end = today + timedelta(days=30)
    start_date, end_date = st.date_input(
        "Select departure window",
        value=(today, default_end),
    )
    st.caption("The report scans inclusive dates. End date defaults to 30 days after today.")

    generate_report = st.button("Generate report", use_container_width=True)


if not isinstance(start_date, date) or not isinstance(end_date, date):
    st.error("Please select both a start and end date.")
    st.stop()

if start_date > end_date:
    st.error("Start date must be on or before the end date.")
    st.stop()


fl3xx_settings = _load_fl3xx_settings()
has_credentials = bool(
    fl3xx_settings.get("api_token")
    or fl3xx_settings.get("auth_header")
    or fl3xx_settings.get("auth_header_name")
)

if not has_credentials:
    st.info(
        "Add your FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to enable live data fetching."
    )

if not generate_report:
    st.stop()

try:
    config = build_fl3xx_api_config(fl3xx_settings)
    config = replace(
        config,
        extra_params={**config.extra_params, "timeZone": "UTC"},
    )
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()
except Exception as exc:
    st.error(f"Error preparing FL3XX API configuration: {exc}")
    st.stop()

with st.spinner("Fetching flights from FL3XX..."):
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
    st.success("No flights found in the selected window.")
    st.stop()

lookup = load_airport_metadata_lookup()
if not lookup:
    st.warning(
        "Airport metadata file `%s` was not found or could not be parsed. Countries cannot be determined."
        % AIRPORT_TZ_FILENAME
    )
    st.stop()

missing_airports: set[str] = set()
report_entries: List[Tuple[float, int, str]] = []


def _preferred_airport_display(
    row: Mapping[str, Any], columns: Sequence[str], fallback: str = "Unknown"
) -> str:
    for column in columns:
        if column not in row:
            continue
        value = row[column]
        for code in _extract_codes(value):
            return code
    display = _coerce_text(row, columns, default=fallback)
    return display or fallback

for idx, (_, leg) in enumerate(legs_df.iterrows()):
    row = leg.to_dict()
    dep_country, dep_code = _detect_country(row, DEPARTURE_AIRPORT_COLUMNS, lookup, missing_airports)
    arr_country, arr_code = _detect_country(row, ARRIVAL_AIRPORT_COLUMNS, lookup, missing_airports)

    triggered_countries: List[str] = []

    dep_normalized = _normalize_country_name(dep_country)
    if dep_normalized and dep_normalized not in _ALLOWED_COUNTRIES:
        triggered_countries.append(dep_country or dep_normalized.title())

    arr_normalized = _normalize_country_name(arr_country)
    if arr_normalized and arr_normalized not in _ALLOWED_COUNTRIES:
        triggered_countries.append(arr_country or arr_normalized.title())

    if not triggered_countries:
        continue

    dep_code_display = dep_code or _preferred_airport_display(
        row, ("departure_airport", "departureAirport", "airportFrom")
    )
    arr_code_display = arr_code or _preferred_airport_display(
        row, ("arrival_airport", "arrivalAirport", "airportTo")
    )

    booking_identifier = _coerce_text(row, BOOKING_KEYS, default="(No booking)")
    account_name = _coerce_text(row, ACCOUNT_KEYS, default="(No account)")
    dep_time_raw = row.get("dep_time")
    dep_dt: Optional[datetime]
    if dep_time_raw is None or (isinstance(dep_time_raw, float) and pd.isna(dep_time_raw)):
        dep_dt = None
    else:
        try:
            dep_dt = safe_parse_dt(str(dep_time_raw))
        except Exception:
            dep_dt = None
    dep_date = dep_dt.strftime("%d%b%y").upper() if dep_dt else "UNKNOWN"
    try:
        sort_key = dep_dt.timestamp() if dep_dt else inf
    except Exception:
        sort_key = inf

    country_display = ", ".join(dict.fromkeys(triggered_countries))
    report_entries.append(
        (
            sort_key,
            idx,
            f"{dep_date} - {booking_identifier} - {account_name} - {dep_code_display} to {arr_code_display} ({country_display})",
        )
    )

report_entries.sort(key=lambda item: (item[0], item[1]))
report_rows = [entry[2] for entry in report_entries]

if not report_rows:
    st.success("No Jeppesen ITP-required flights detected for the selected window.")
else:
    header = "Jeppesen ITP Required Flights"
    date_range_line = f"{start_date.strftime('%d%b%y').upper()} to {end_date.strftime('%d%b%y').upper()}"
    output_lines = [header, date_range_line, ""] + report_rows
    report_text = "\n".join(output_lines)
    st.subheader("Report preview")
    st.code(report_text, language="text")
    st.download_button(
        "Download report", report_text, file_name="jeppesen_itp_required_flights.txt", mime="text/plain"
    )

if missing_airports:
    sorted_missing = sorted(missing_airports)
    sample = ", ".join(sorted_missing[:20])
    if len(sorted_missing) > 20:
        sample += ", ..."
    st.warning(
        "Unable to determine country for the following airport codes: %s. Update `%s` if necessary."
        % (sample, AIRPORT_TZ_FILENAME)
    )

metadata_expander = st.expander("FL3XX metadata", expanded=False)
with metadata_expander:
    st.json(metadata)
