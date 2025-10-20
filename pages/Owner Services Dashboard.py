import pandas as pd
import requests
import streamlit as st

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional

from fl3xx_api import Fl3xxApiConfig, compute_fetch_dates, fetch_flight_services, fetch_flights
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    normalize_fl3xx_payload,
    safe_parse_dt,
)
from owner_services import (
    OwnerServicesSummary,
    format_owner_service_entries,
)


ROW_STATE_STYLES: Dict[str, Dict[str, str]] = {
    "complete": {
        "background": "rgba(129, 199, 132, 0.45)",
        "border": "#2e7d32",
        "text": "#0b2e13",
    },
    "attention": {
        "background": "rgba(255, 213, 79, 0.40)",
        "border": "#f9a825",
        "text": "#3b2f04",
    },
}


st.set_page_config(page_title="Owner Services Dashboard", layout="wide")
st.title("🧾 Owner Catering & Transport Dashboard")


_FROM_KEY = "owner_services_from"
_TO_KEY = "owner_services_to"
_RESULTS_KEY = "owner_services_results"
_ERROR_KEY = "owner_services_error"


def _initialise_state() -> None:
    st.session_state.setdefault(_RESULTS_KEY, None)
    st.session_state.setdefault(_ERROR_KEY, None)
    if _FROM_KEY not in st.session_state or _TO_KEY not in st.session_state:
        default_from, default_to_exclusive = compute_fetch_dates(
            datetime.now(timezone.utc), inclusive_days=4
        )
        st.session_state[_FROM_KEY] = default_from
        st.session_state[_TO_KEY] = default_to_exclusive - timedelta(days=1)


def _get_selected_dates() -> tuple[date, date]:
    from_date = st.session_state.get(_FROM_KEY)
    to_date = st.session_state.get(_TO_KEY)
    if isinstance(from_date, date) and isinstance(to_date, date):
        return from_date, to_date
    today = datetime.now(timezone.utc).date()
    return today, today + timedelta(days=4)


def _get_api_settings() -> Optional[Mapping[str, Any]]:
    try:
        settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
    except Exception:
        return None
    if not settings or not isinstance(settings, Mapping):
        return None
    return dict(settings)


def _format_datetime(raw_value: Any) -> tuple[str, Optional[str]]:
    if raw_value in (None, ""):
        return "—", None
    if isinstance(raw_value, datetime):
        parsed = raw_value
    else:
        try:
            parsed = safe_parse_dt(str(raw_value))
        except Exception:
            return str(raw_value), None
    label = parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    return label, parsed.isoformat()


def _extract_airport_code(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, Mapping):
        for key in ("icao", "iata", "name", "code"):
            candidate = value.get(key)
            text = str(candidate).strip() if candidate not in (None, "") else ""
            if text:
                return text
    return None


def _coerce_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _extract_booking_identifier(leg: Mapping[str, Any]) -> Optional[str]:
    booking_keys = (
        "bookingIdentifier",
        "booking_identifier",
        "bookingReference",
        "booking_reference",
        "bookingCode",
        "booking_code",
        "bookingNumber",
        "booking_number",
        "bookingId",
        "booking_id",
        "bookingID",
        "bookingRef",
        "booking",
        "salesOrderNumber",
        "salesOrder",
        "reservationNumber",
        "reservationId",
    )

    for key in booking_keys:
        text = _coerce_text(leg.get(key))
        if text:
            return text

    nested_candidates = []
    for nested_key in ("booking", "reservation", "salesOrder"):
        nested_value = leg.get(nested_key)
        if isinstance(nested_value, Mapping):
            nested_candidates.append(nested_value)

    for nested in nested_candidates:
        for key in ("identifier", "reference", "code", "number", "id"):
            text = _coerce_text(nested.get(key))
            if text:
                return text

    return None


def _extract_pax_number(leg: Mapping[str, Any]) -> Optional[int]:
    pax_keys = (
        "paxNumber",
        "pax_count",
        "pax",
        "passengerCount",
        "passengers",
        "passenger_count",
    )
    for key in pax_keys:
        value = leg.get(key)
        if value in (None, ""):
            continue
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            continue
    return None


def _build_display_row(
    leg: Mapping[str, Any],
    summary: OwnerServicesSummary,
    *,
    services_available: bool,
) -> Dict[str, Any]:
    departure_label, sort_key = _format_datetime(
        leg.get("dep_time") or leg.get("departureTimeUtc")
    )
    arrival_label, _ = _format_datetime(
        leg.get("arrivalTimeUtc") or leg.get("arrival_time")
    )

    tail = str(leg.get("tail") or "Unknown")
    booking_identifier = _extract_booking_identifier(leg)
    dep_ap = _extract_airport_code(
        leg.get("departure_airport") or leg.get("departureAirport")
    )
    arr_ap = _extract_airport_code(
        leg.get("arrival_airport") or leg.get("arrivalAirport")
    )
    route = f"{dep_ap or '?'} → {arr_ap or '?'}"
    pax_number = _extract_pax_number(leg)

    owner = leg.get("accountName") or leg.get("account") or ""
    owner_text = str(owner) if owner not in (None, "") else "—"

    if not services_available:
        status_label = "Services unavailable"
        row_state = "attention"
    elif summary.needs_attention:
        status_label = "Needs attention"
        row_state = "attention"
    elif summary.has_owner_services:
        status_label = "Complete"
        row_state = "complete"
    else:
        status_label = "No owner services"
        row_state = "attention"

    return {
        "_sort_key": sort_key or departure_label,
        "Departure (UTC)": departure_label,
        "Arrival (UTC)": arrival_label,
        "Tail": tail,
        "Booking Identifier": booking_identifier or "—",
        "Route": route,
        "Pax": pax_number if pax_number is not None else "—",
        "Owner": owner_text,
        "Owner Services Status": status_label,
        "Departure Catering": (
            format_owner_service_entries(summary.departure_catering)
            if services_available
            else "—"
        ),
        "Arrival Catering": (
            format_owner_service_entries(summary.arrival_catering)
            if services_available
            else "—"
        ),
        "Departure Transport": (
            format_owner_service_entries(summary.departure_ground_transport)
            if services_available
            else "—"
        ),
        "Arrival Transport": (
            format_owner_service_entries(summary.arrival_ground_transport)
            if services_available
            else "—"
        ),
        "_row_state": row_state,
    }


def _build_dashboard_rows(
    rows: List[Mapping[str, Any]],
    config: Fl3xxApiConfig,
) -> tuple[List[Dict[str, Any]], List[str], Dict[str, Any]]:
    display_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    stats = {
        "legs_considered": len(rows),
        "missing_flight_ids": 0,
        "service_requests": 0,
        "service_failures": 0,
        "legs_without_services_data": 0,
        "legs_with_owner_services": 0,
        "legs_needing_attention": 0,
        "owner_service_entries": 0,
    }

    services_cache: Dict[str, Optional[Any]] = {}
    missing_seen: set[tuple[str, str]] = set()

    session = requests.Session()
    try:
        for leg in rows:
            flight_id = leg.get("flightId") or leg.get("flight_id")
            if not flight_id:
                stats["missing_flight_ids"] += 1
                key = (str(leg.get("tail") or "Unknown"), str(leg.get("leg_id") or ""))
                if key not in missing_seen:
                    missing_seen.add(key)
                    warnings.append(
                        f"Leg {key[1] or 'unknown'} ({key[0]}) is missing a flight identifier; "
                        "skipping services lookup."
                    )
                continue

            flight_key = str(flight_id)
            if flight_key not in services_cache:
                stats["service_requests"] += 1
                try:
                    payload = fetch_flight_services(config, flight_key, session=session)
                except Exception as exc:
                    stats["service_failures"] += 1
                    services_cache[flight_key] = None
                    warnings.append(
                        f"Failed to fetch services for flight {flight_key}: {exc}"
                    )
                else:
                    services_cache[flight_key] = payload

            payload = services_cache.get(flight_key)
            services_available = payload is not None
            if not services_available:
                stats["legs_without_services_data"] += 1

            summary = OwnerServicesSummary.from_payload(payload)
            if summary.has_owner_services:
                stats["legs_with_owner_services"] += 1
                stats["owner_service_entries"] += len(summary.all_entries())
            if summary.needs_attention:
                stats["legs_needing_attention"] += 1

            row = _build_display_row(
                leg, summary, services_available=services_available
            )
            if row.get("Owner Services Status") == "No owner services":
                continue

            display_rows.append(row)
    finally:
        session.close()

    display_rows.sort(key=lambda row: row.get("_sort_key") or "")
    for row in display_rows:
        row.pop("_sort_key", None)

    return display_rows, warnings, stats


def _handle_fetch(
    settings: Mapping[str, Any],
    *,
    from_date: date,
    to_date_inclusive: date,
) -> None:
    try:
        config = build_fl3xx_api_config(settings)
    except FlightDataError as exc:
        st.session_state[_RESULTS_KEY] = None
        st.session_state[_ERROR_KEY] = str(exc)
        return

    to_date_exclusive = to_date_inclusive + timedelta(days=1)

    try:
        with st.spinner("Fetching flights from FL3XX..."):
            flights, metadata = fetch_flights(
                config,
                from_date=from_date,
                to_date=to_date_exclusive,
            )
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state[_RESULTS_KEY] = None
        st.session_state[_ERROR_KEY] = f"Failed to fetch flights: {exc}"
        return

    try:
        normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state[_RESULTS_KEY] = None
        st.session_state[_ERROR_KEY] = f"Failed to normalise flight data: {exc}"
        return

    normalized_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)

    display_rows: List[Dict[str, Any]]
    warnings: List[str]
    service_stats: Dict[str, Any]

    with st.spinner("Fetching services for owner checks..."):
        display_rows, warnings, service_stats = _build_dashboard_rows(normalized_rows, config)

    metadata_payload = {
        **metadata,
        "normalized_leg_count": len(normalized_rows),
        "normalization_stats": normalization_stats,
        "skipped_subcharter": skipped_subcharter,
        "service_stats": service_stats,
        "selected_from": from_date.isoformat(),
        "selected_to": to_date_inclusive.isoformat(),
    }

    st.session_state[_RESULTS_KEY] = {
        "rows": display_rows,
        "warnings": warnings,
        "metadata": metadata_payload,
        "stats": service_stats,
    }
    st.session_state[_ERROR_KEY] = None


def _render_results() -> None:
    error_message = st.session_state.get(_ERROR_KEY)
    results = st.session_state.get(_RESULTS_KEY)

    if error_message:
        st.error(error_message)

    if not results:
        if not error_message:
            st.info(
                "Press **Fetch Owner Services** to view catering and transport statuses "
                "for owner flights in the selected range."
            )
        return

    metadata: Mapping[str, Any] = results.get("metadata", {})
    stats: Mapping[str, Any] = results.get("stats", {})
    warnings: List[str] = list(results.get("warnings", []))
    rows: List[Mapping[str, Any]] = list(results.get("rows", []))

    selected_range = f"{metadata.get('selected_from', '—')} → {metadata.get('selected_to', '—')}"
    fetched_at = metadata.get("fetched_at")
    if isinstance(fetched_at, str):
        fetched_label = fetched_at
    else:
        fetched_label = ""

    st.success(
        "Owner services fetched"
        + (f" · {fetched_label}" if fetched_label else "")
        + f" · Legs analysed: {stats.get('legs_considered', len(rows))}"
        + f" · Dates: {selected_range}"
    )

    cols = st.columns(4)
    cols[0].metric(
        "Legs needing attention",
        int(stats.get("legs_needing_attention", 0)),
    )
    cols[1].metric(
        "Legs with owner services",
        int(stats.get("legs_with_owner_services", 0)),
    )
    service_requests = int(stats.get("service_requests", 0))
    service_failures = int(stats.get("service_failures", 0))
    cols[2].metric(
        "Service fetch failures",
        service_failures,
        delta=f"{service_requests - service_failures}/{service_requests} successes"
        if service_requests
        else None,
    )
    cols[3].metric(
        "Missing flight IDs",
        int(stats.get("missing_flight_ids", 0)),
    )

    for warning in warnings:
        st.warning(warning)

    df = pd.DataFrame(rows)
    if df.empty:
        st.info("No flights found for the selected range.")
    else:
        column_order = [
            "Departure (UTC)",
            "Arrival (UTC)",
            "Tail",
            "Booking Identifier",
            "Route",
            "Pax",
            "Owner",
            "Owner Services Status",
            "Departure Catering",
            "Arrival Catering",
            "Departure Transport",
            "Arrival Transport",
        ]
        has_state_column = "_row_state" in df.columns
        state_series = df["_row_state"] if has_state_column else None
        df_display = df[[col for col in column_order if col in df.columns]]

        if has_state_column:
            def _apply_row_highlight(row: pd.Series) -> List[str]:
                state = state_series.loc[row.name]
                styles = ROW_STATE_STYLES.get(str(state))
                if not isinstance(styles, Mapping):
                    return [""] * len(row)

                css_parts: List[str] = []
                background = styles.get("background")
                border = styles.get("border")
                text_color = styles.get("text")
                if background:
                    css_parts.append(f"background-color: {background}")
                if border:
                    css_parts.append(f"border-left: 4px solid {border}")
                if text_color:
                    css_parts.append(f"color: {text_color}")
                else:
                    css_parts.append("color: inherit")

                css_parts.append("font-weight: 600")
                css = "; ".join(css_parts)
                return [css] * len(row)

            styler = df_display.style.apply(_apply_row_highlight, axis=1)
            st.dataframe(styler, use_container_width=True)
        else:
            st.dataframe(df_display, use_container_width=True)

        csv_bytes = (
            df.drop(columns=["_row_state"], errors="ignore").to_csv(index=False).encode("utf-8")
        )
        st.download_button(
            "Download results as CSV",
            data=csv_bytes,
            file_name="owner_services_dashboard.csv",
            mime="text/csv",
        )

    with st.expander("Fetch metadata", expanded=False):
        st.json(metadata)


def main() -> None:
    _initialise_state()

    st.markdown(
        """
        Review the current status of owner catering and ground transport requests for upcoming
        flights. Configure the inclusive date range below (defaulting to the next four days)
        and press **Fetch Owner Services** to retrieve the latest data from FL3XX.
        """
    )

    selected_from, selected_to = _get_selected_dates()
    date_input = st.date_input(
        "Dashboard date range",
        value=(selected_from, selected_to),
        help="Choose the inclusive departure date range to analyse.",
    )

    if isinstance(date_input, tuple) and len(date_input) == 2:
        selected_from, selected_to = date_input
    elif isinstance(date_input, date):
        selected_from = date_input
        selected_to = date_input

    st.session_state[_FROM_KEY] = selected_from
    st.session_state[_TO_KEY] = selected_to

    api_settings = _get_api_settings()
    if api_settings is None:
        st.warning(
            "FL3XX API credentials are not configured. Add them to "
            "`.streamlit/secrets.toml` under the `[fl3xx_api]` section to enable live fetches."
        )

    if st.button(
        "Fetch Owner Services",
        help="Fetch FL3XX flights and owner services for the selected date range.",
        use_container_width=False,
    ):
        if selected_to < selected_from:
            st.session_state[_RESULTS_KEY] = None
            st.session_state[_ERROR_KEY] = (
                "The dashboard end date must be on or after the start date."
            )
        elif api_settings is None:
            st.session_state[_RESULTS_KEY] = None
            st.session_state[_ERROR_KEY] = (
                "FL3XX API secrets are not configured; provide credentials before fetching."
            )
        else:
            _handle_fetch(
                api_settings,
                from_date=selected_from,
                to_date_inclusive=selected_to,
            )

    _render_results()


if __name__ == "__main__":
    main()

