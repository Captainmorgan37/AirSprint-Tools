import re

import pandas as pd
import requests
import streamlit as st

from functools import lru_cache
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from fl3xx_api import (
    Fl3xxApiConfig,
    compute_fetch_dates,
    fetch_flight_services,
    fetch_flights,
    fetch_leg_details,
)
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


_NOTES_FROM_KEY = "owner_services_sensitive_from"
_NOTES_TO_KEY = "owner_services_sensitive_to"
_NOTES_RESULTS_KEY = "owner_services_sensitive_results"
_NOTES_ERROR_KEY = "owner_services_sensitive_error"

_DEFAULT_SENSITIVE_KEYWORDS: tuple[str, ...] = (
    "gun",
    "guns",
    "rifle",
    "rifles",
    "emergency",
    "operation",
    "funeral",
    "dog",
    "dogs",
    "cat",
    "cats",
    "commercial",
    "connecting",
    "esta",    
    "visa",
    "turbulence",
)

_SENSITIVE_TERMS_STATE_KEY = "owner_services_sensitive_terms"
_SENSITIVE_ADD_INPUT_KEY = "owner_services_sensitive_add_term"
_SENSITIVE_REMOVE_SELECT_KEY = "owner_services_sensitive_remove_terms"


_ACCOUNT_KEYS: tuple[str, ...] = (
    "accountName",
    "account",
    "account_name",
    "owner",
    "ownerName",
    "customer",
    "customerName",
    "client",
    "clientName",
)


def _initialise_state() -> None:
    st.session_state.setdefault(_RESULTS_KEY, None)
    st.session_state.setdefault(_ERROR_KEY, None)
    if _FROM_KEY not in st.session_state or _TO_KEY not in st.session_state:
        default_from, default_to_exclusive = compute_fetch_dates(
            datetime.now(timezone.utc), inclusive_days=4
        )
        st.session_state[_FROM_KEY] = default_from
        st.session_state[_TO_KEY] = default_to_exclusive - timedelta(days=1)


def _initialise_sensitive_state() -> None:
    st.session_state.setdefault(_NOTES_RESULTS_KEY, None)
    st.session_state.setdefault(_NOTES_ERROR_KEY, None)
    if _NOTES_FROM_KEY not in st.session_state or _NOTES_TO_KEY not in st.session_state:
        default_from, default_to_exclusive = compute_fetch_dates(
            datetime.now(timezone.utc), inclusive_days=4
        )
        st.session_state[_NOTES_FROM_KEY] = default_from
        st.session_state[_NOTES_TO_KEY] = default_to_exclusive - timedelta(days=1)

    _ensure_sensitive_keywords_state()


def _normalise_keyword(term: Any) -> Optional[str]:
    if term in (None, ""):
        return None
    text = str(term).strip().lower()
    return text or None


def _ensure_sensitive_keywords_state() -> List[str]:
    stored = st.session_state.get(_SENSITIVE_TERMS_STATE_KEY)
    if not isinstance(stored, list):
        stored = list(_DEFAULT_SENSITIVE_KEYWORDS)
    cleaned: List[str] = []
    seen: set[str] = set()
    for term in stored:
        normalised = _normalise_keyword(term)
        if not normalised or normalised in seen:
            continue
        cleaned.append(normalised)
        seen.add(normalised)
    if cleaned != stored:
        st.session_state[_SENSITIVE_TERMS_STATE_KEY] = cleaned
    else:
        st.session_state.setdefault(_SENSITIVE_TERMS_STATE_KEY, cleaned)
    return cleaned


def _get_sensitive_keywords() -> tuple[str, ...]:
    keywords = _ensure_sensitive_keywords_state()
    return tuple(keywords)


@lru_cache(maxsize=32)
def _compile_keyword_pattern(keywords: tuple[str, ...]) -> re.Pattern[str]:
    if not keywords:
        return re.compile(r"$^")
    return re.compile(
        r"\b(" + "|".join(re.escape(term) for term in keywords) + r")\b",
        re.IGNORECASE,
    )


def _get_keyword_pattern() -> re.Pattern[str]:
    keywords = _get_sensitive_keywords()
    return _compile_keyword_pattern(keywords)


def _add_sensitive_keyword(term: str) -> bool:
    normalised = _normalise_keyword(term)
    if not normalised:
        st.warning("Enter a keyword before adding it to the monitor.")
        return False

    keywords = list(_get_sensitive_keywords())
    if normalised in keywords:
        st.info(f"`{normalised}` is already being monitored.")
        return False

    keywords.append(normalised)
    st.session_state[_SENSITIVE_TERMS_STATE_KEY] = keywords
    _compile_keyword_pattern.cache_clear()
    st.success(f"Added `{normalised}` to the monitored keyword list.")
    return True


def _remove_sensitive_keywords(terms: Sequence[str]) -> bool:
    removal_targets = {
        value for value in (_normalise_keyword(term) for term in terms) if value
    }
    if not removal_targets:
        st.warning("Select at least one keyword to remove.")
        return False

    keywords = list(_get_sensitive_keywords())
    new_keywords = [term for term in keywords if term not in removal_targets]
    if len(new_keywords) == len(keywords):
        st.info("The selected keywords are not currently monitored.")
        return False

    st.session_state[_SENSITIVE_TERMS_STATE_KEY] = new_keywords
    _compile_keyword_pattern.cache_clear()
    removed_display = ", ".join(f"`{term}`" for term in sorted(removal_targets))
    st.success(f"Stopped monitoring {removed_display}.")
    return True


def _get_selected_dates() -> tuple[date, date]:
    from_date = st.session_state.get(_FROM_KEY)
    to_date = st.session_state.get(_TO_KEY)
    if isinstance(from_date, date) and isinstance(to_date, date):
        return from_date, to_date
    today = datetime.now(timezone.utc).date()
    return today, today + timedelta(days=4)


def _get_sensitive_selected_dates() -> tuple[date, date]:
    from_date = st.session_state.get(_NOTES_FROM_KEY)
    to_date = st.session_state.get(_NOTES_TO_KEY)
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


def _extract_account_label(row: Mapping[str, Any]) -> str:
    for key in _ACCOUNT_KEYS:
        value = row.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, Mapping):
            nested_value = value.get("name") or value.get("accountName") or value.get("account")
            if nested_value in (None, ""):
                continue
            value = nested_value
        text = str(value).strip()
        if text:
            return text
    return "—"


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


def _extract_quote_identifier(leg: Mapping[str, Any]) -> Optional[str]:
    for key in ("quoteId", "quote_id", "quoteID", "quote", "quoteNumber"):
        value = leg.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
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


def _select_leg_detail(payload: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        return payload
    if isinstance(payload, (list, tuple)):
        for item in payload:
            if isinstance(item, Mapping):
                return item
    return None


def _extract_leg_notes(payload: Any) -> Optional[str]:
    detail = _select_leg_detail(payload)
    if not detail:
        return None
    for key in ("notes", "planningNotes", "planningNote", "planning_notes"):
        value = detail.get(key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return None


def _coerce_note_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _iter_note_mappings(value: Any) -> List[Mapping[str, Any]]:
    mappings: List[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        mappings.append(value)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            if isinstance(item, Mapping):
                mappings.append(item)
            else:
                text = _coerce_note_text(item)
                if text:
                    mappings.append({"note": text})
    else:
        text = _coerce_note_text(value)
        if text:
            mappings.append({"note": text})
    return mappings


def _extract_service_notes(payload: Any) -> List[tuple[str, str]]:
    notes: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    if not isinstance(payload, Mapping):
        return notes

    raw_notes = payload.get("notes")
    for item in _iter_note_mappings(raw_notes):
        text: Optional[str] = None
        for key in ("note", "notes", "text", "details", "description", "content"):
            text = _coerce_note_text(item.get(key))
            if text:
                break

        if not text:
            continue

        label_text: Optional[str] = None
        for key in ("type", "title", "subject", "category", "label", "name"):
            label_text = _coerce_note_text(item.get(key))
            if label_text:
                break

        label = (
            f"Owner service note – {label_text}" if label_text else "Owner service note"
        )
        candidate = (label, text)
        if candidate in seen:
            continue
        seen.add(candidate)
        notes.append(candidate)

    return notes


def _highlight_keywords(note_text: str) -> tuple[str, List[str]]:
    normalized = note_text.replace("\r\n", "\n").replace("\r", "\n")
    pattern = _get_keyword_pattern()
    matches = sorted({match.group(0).lower() for match in pattern.finditer(normalized)})
    return normalized, [match.upper() for match in matches]


def _build_sensitive_notes_rows(
    rows: List[Mapping[str, Any]],
    config: Fl3xxApiConfig,
) -> tuple[List[Dict[str, Any]], List[str], Dict[str, Any]]:
    display_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    stats = {
        "legs_considered": len(rows),
        "missing_quote_ids": 0,
        "detail_requests": 0,
        "detail_failures": 0,
        "legs_with_detail": 0,
        "legs_with_leg_notes": 0,
        "legs_with_notes": 0,
        "legs_flagged": 0,
        "missing_flight_ids": 0,
        "service_requests": 0,
        "service_failures": 0,
        "legs_with_service_data": 0,
        "legs_with_service_notes": 0,
    }

    detail_cache: Dict[str, Optional[Any]] = {}
    services_cache: Dict[str, Optional[Any]] = {}
    missing_flight_seen: set[tuple[str, str]] = set()

    session = requests.Session()
    try:
        for leg in rows:
            note_blocks: List[tuple[str, str]] = []

            quote_id = _extract_quote_identifier(leg)
            payload: Optional[Any] = None
            if not quote_id:
                stats["missing_quote_ids"] += 1
            else:
                if quote_id not in detail_cache:
                    stats["detail_requests"] += 1
                    try:
                        payload = fetch_leg_details(config, quote_id, session=session)
                    except Exception as exc:
                        stats["detail_failures"] += 1
                        detail_cache[quote_id] = None
                        warnings.append(
                            f"Failed to fetch leg details for quote {quote_id}: {exc}"
                        )
                    else:
                        detail_cache[quote_id] = payload
                payload = detail_cache.get(quote_id)
                if payload is not None:
                    stats["legs_with_detail"] += 1
                    note_text = _extract_leg_notes(payload)
                    if note_text:
                        stats["legs_with_leg_notes"] += 1
                        note_blocks.append(("Leg notes", note_text))

            flight_id = leg.get("flightId") or leg.get("flight_id")
            services_payload: Optional[Any] = None
            if not flight_id:
                stats["missing_flight_ids"] += 1
                leg_identifier = (
                    str(leg.get("leg_id") or leg.get("legId") or leg.get("id") or "")
                )
                warning_key = (str(leg.get("tail") or "Unknown"), leg_identifier)
                if warning_key not in missing_flight_seen:
                    missing_flight_seen.add(warning_key)
                    warnings.append(
                        f"Leg {leg_identifier or 'unknown'} ({warning_key[0]}) is missing a flight "
                        "identifier; skipping owner services note lookup."
                    )
            else:
                flight_key = str(flight_id)
                if flight_key not in services_cache:
                    stats["service_requests"] += 1
                    try:
                        services_payload = fetch_flight_services(
                            config, flight_key, session=session
                        )
                    except Exception as exc:
                        stats["service_failures"] += 1
                        services_cache[flight_key] = None
                        warnings.append(
                            f"Failed to fetch services for flight {flight_key}: {exc}"
                        )
                    else:
                        services_cache[flight_key] = services_payload
                services_payload = services_cache.get(flight_key)
                if services_payload is not None:
                    stats["legs_with_service_data"] += 1
                    service_notes = _extract_service_notes(services_payload)
                    if service_notes:
                        stats["legs_with_service_notes"] += 1
                        note_blocks.extend(service_notes)

            if not note_blocks:
                continue

            stats["legs_with_notes"] += 1

            rendered_blocks = []
            aggregated_matches: set[str] = set()
            for label, text in note_blocks:
                rendered_text, matched_keywords = _highlight_keywords(text)
                aggregated_matches.update(matched_keywords)
                rendered_blocks.append(
                    {
                        "label": label,
                        "highlighted": rendered_text,
                        "raw": text,
                    }
                )

            if not aggregated_matches:
                continue

            stats["legs_flagged"] += 1

            departure_label, sort_key = _format_datetime(
                leg.get("dep_time") or leg.get("departureTimeUtc")
            )

            tail = str(leg.get("tail") or "Unknown")
            booking_identifier = _extract_booking_identifier(leg) or "—"
            dep_ap = _extract_airport_code(
                leg.get("departure_airport") or leg.get("departureAirport")
            )
            arr_ap = _extract_airport_code(
                leg.get("arrival_airport") or leg.get("arrivalAirport")
            )
            route = f"{dep_ap or '?'} → {arr_ap or '?'}"
            account_label = _extract_account_label(leg)

            if len(rendered_blocks) == 1 and rendered_blocks[0]["label"] == "Leg notes":
                notes_display = rendered_blocks[0]["highlighted"]
                raw_notes = rendered_blocks[0]["raw"]
            else:
                sections = []
                raw_sections = []
                for block in rendered_blocks:
                    sections.append(f"{block['label']}:\n{block['highlighted']}")
                    raw_sections.append(f"{block['label']}: {block['raw']}")
                notes_display = "\n\n".join(sections)
                raw_notes = "\n\n---\n\n".join(raw_sections)

            display_rows.append(
                {
                    "_sort_key": sort_key or departure_label,
                    "Departure (UTC)": departure_label,
                    "Tail": tail,
                    "Booking Identifier": booking_identifier,
                    "Account Name": account_label,
                    "Route": route,
                    "Matched Keywords": ", ".join(sorted(aggregated_matches)),
                    "Notes": notes_display,
                    "_notes_raw": raw_notes,
                }
            )
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


def _handle_sensitive_notes_fetch(
    settings: Mapping[str, Any],
    *,
    from_date: date,
    to_date_inclusive: date,
) -> None:
    try:
        config = build_fl3xx_api_config(settings)
    except FlightDataError as exc:
        st.session_state[_NOTES_RESULTS_KEY] = None
        st.session_state[_NOTES_ERROR_KEY] = str(exc)
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
        st.session_state[_NOTES_RESULTS_KEY] = None
        st.session_state[_NOTES_ERROR_KEY] = f"Failed to fetch flights: {exc}"
        return

    try:
        normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights})
    except Exception as exc:  # pragma: no cover - defensive UI path
        st.session_state[_NOTES_RESULTS_KEY] = None
        st.session_state[_NOTES_ERROR_KEY] = f"Failed to normalise flight data: {exc}"
        return

    normalized_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)

    with st.spinner("Inspecting leg notes for keywords..."):
        display_rows, warnings, detail_stats = _build_sensitive_notes_rows(
            normalized_rows, config
        )

    metadata_payload = {
        **metadata,
        "normalized_leg_count": len(normalized_rows),
        "normalization_stats": normalization_stats,
        "skipped_subcharter": skipped_subcharter,
        "detail_stats": detail_stats,
        "selected_from": from_date.isoformat(),
        "selected_to": to_date_inclusive.isoformat(),
    }

    st.session_state[_NOTES_RESULTS_KEY] = {
        "rows": display_rows,
        "warnings": warnings,
        "metadata": metadata_payload,
        "stats": detail_stats,
    }
    st.session_state[_NOTES_ERROR_KEY] = None


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


def _render_owner_services_tab(api_settings: Optional[Mapping[str, Any]]) -> None:
    st.markdown(
        """
        Review the current status of owner catering and ground transport requests for upcoming
        flights. Configure the inclusive date range below (defaulting to the next four days)
        and press **Fetch Owner Services** to retrieve the latest data from FL3XX.
        """
    )

    selected_from, selected_to = _get_selected_dates()
    date_input = st.date_input(
        "Owner services date range",
        value=(selected_from, selected_to),
        help="Choose the inclusive departure date range to analyse.",
        key="owner_services_date_input",
    )

    if isinstance(date_input, tuple) and len(date_input) == 2:
        selected_from, selected_to = date_input
    elif isinstance(date_input, date):
        selected_from = date_input
        selected_to = date_input

    st.session_state[_FROM_KEY] = selected_from
    st.session_state[_TO_KEY] = selected_to

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


def _render_sensitive_notes_results() -> None:
    error_message = st.session_state.get(_NOTES_ERROR_KEY)
    results = st.session_state.get(_NOTES_RESULTS_KEY)

    if error_message:
        st.error(error_message)

    if not results:
        if not error_message:
            st.info(
                "Press **Fetch Sensitive Notes** to search FL3XX leg notes for the configured keywords."
            )
        return

    metadata: Mapping[str, Any] = results.get("metadata", {})
    stats: Mapping[str, Any] = results.get("stats", {})
    warnings: List[str] = list(results.get("warnings", []))
    rows: List[Mapping[str, Any]] = list(results.get("rows", []))

    selected_range = f"{metadata.get('selected_from', '—')} → {metadata.get('selected_to', '—')}"
    fetched_at = metadata.get("fetched_at")
    fetched_label = fetched_at if isinstance(fetched_at, str) else ""

    st.success(
        "Sensitive notes fetched"
        + (f" · {fetched_label}" if fetched_label else "")
        + f" · Legs analysed: {stats.get('legs_considered', len(rows))}"
        + f" · Dates: {selected_range}"
    )

    cols = st.columns(4)
    cols[0].metric(
        "Legs flagged",
        int(stats.get("legs_flagged", 0)),
    )
    cols[1].metric(
        "Legs with notes",
        int(stats.get("legs_with_notes", 0)),
    )
    cols[2].metric(
        "Detail fetch failures",
        int(stats.get("detail_failures", 0)),
    )
    cols[3].metric(
        "Missing quote IDs",
        int(stats.get("missing_quote_ids", 0)),
    )

    for warning in warnings:
        st.warning(warning)

    if not rows:
        st.info("No leg notes matched the configured keywords in the selected range.")
    else:
        _render_sensitive_notes_table(rows)

        export_rows = []
        for row in rows:
            export_rows.append(
                {
                    "Departure (UTC)": row.get("Departure (UTC)", ""),
                    "Tail": row.get("Tail", ""),
                    "Booking Identifier": row.get("Booking Identifier", ""),
                    "Account Name": row.get("Account Name", ""),
                    "Route": row.get("Route", ""),
                    "Matched Keywords": row.get("Matched Keywords", ""),
                    "Notes": row.get("_notes_raw", ""),
                }
            )

        df_export = pd.DataFrame(export_rows)
        csv_bytes = df_export.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download flagged notes as CSV",
            data=csv_bytes,
            file_name="owner_services_sensitive_notes.csv",
            mime="text/csv",
        )

    with st.expander("Fetch metadata", expanded=False):
        st.json(metadata)


def _render_sensitive_notes_table(rows: List[Mapping[str, Any]]) -> None:
    df = pd.DataFrame(rows)

    column_order = [
        "Departure (UTC)",
        "Tail",
        "Booking Identifier",
        "Account Name",
        "Route",
        "Matched Keywords",
        "Notes",
    ]
    present_columns = [column for column in column_order if column in df.columns]
    if not present_columns:
        return

    df_display = df[present_columns].copy()

    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)

    for column in df_display.columns:
        df_display[column] = df_display[column].map(_stringify)

    styler = (
        df_display.style.hide(axis="index")
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": "text-align: left; font-weight: 600;",
                },
                {
                    "selector": "td",
                    "props": "text-align: left; vertical-align: top; white-space: pre-wrap; word-break: break-word;",
                },
            ]
        )
    )

    st.dataframe(styler, use_container_width=True)


def _render_sensitive_keyword_manager() -> None:
    st.markdown("---")
    st.markdown("#### Monitored sensitive keywords")

    keywords = list(_get_sensitive_keywords())
    if keywords:
        st.caption(
            "The Sensitive Notes Monitor currently scans leg notes for these terms."
        )
        st.write(" ".join(f"`{term}`" for term in keywords))
    else:
        st.info(
            "No keywords are currently configured. Add a term below to begin monitoring "
            "for sensitive notes."
        )

    with st.form("owner_services_sensitive_add_form", clear_on_submit=True):
        new_keyword = st.text_input(
            "Add keyword to monitor",
            key=_SENSITIVE_ADD_INPUT_KEY,
            placeholder="e.g. firearms",
        )
        add_submitted = st.form_submit_button("Add keyword")
        if add_submitted:
            _add_sensitive_keyword(new_keyword)

    with st.form("owner_services_sensitive_remove_form"):
        current_keywords = list(_get_sensitive_keywords())
        remove_selection = st.multiselect(
            "Select keywords to stop monitoring",
            options=current_keywords,
            format_func=lambda term: term.upper(),
            key=_SENSITIVE_REMOVE_SELECT_KEY,
        )
        remove_submitted = st.form_submit_button("Remove selected keywords")
        if remove_submitted:
            if _remove_sensitive_keywords(remove_selection):
                st.session_state[_SENSITIVE_REMOVE_SELECT_KEY] = []


def _render_sensitive_notes_tab(api_settings: Optional[Mapping[str, Any]]) -> None:
    st.markdown(
        """
        Search FL3XX leg notes for sensitive keywords that may require additional attention.
        Select the inclusive departure date range below and press **Fetch Sensitive Notes**
        to scan each matching leg's detailed notes.
        """
    )

    selected_from, selected_to = _get_sensitive_selected_dates()
    date_input = st.date_input(
        "Sensitive notes date range",
        value=(selected_from, selected_to),
        help="Choose the inclusive departure date range to inspect for keyword matches.",
        key="sensitive_notes_date_input",
    )

    if isinstance(date_input, tuple) and len(date_input) == 2:
        selected_from, selected_to = date_input
    elif isinstance(date_input, date):
        selected_from = date_input
        selected_to = date_input

    st.session_state[_NOTES_FROM_KEY] = selected_from
    st.session_state[_NOTES_TO_KEY] = selected_to

    if api_settings is None:
        st.warning(
            "FL3XX API credentials are not configured. Add them to "
            "`.streamlit/secrets.toml` under the `[fl3xx_api]` section to enable live fetches."
        )

    if st.button(
        "Fetch Sensitive Notes",
        help="Fetch FL3XX flights and inspect leg notes for the selected date range.",
        use_container_width=False,
    ):
        if selected_to < selected_from:
            st.session_state[_NOTES_RESULTS_KEY] = None
            st.session_state[_NOTES_ERROR_KEY] = (
                "The notes search end date must be on or after the start date."
            )
        elif api_settings is None:
            st.session_state[_NOTES_RESULTS_KEY] = None
            st.session_state[_NOTES_ERROR_KEY] = (
                "FL3XX API secrets are not configured; provide credentials before fetching."
            )
        else:
            _handle_sensitive_notes_fetch(
                api_settings,
                from_date=selected_from,
                to_date_inclusive=selected_to,
            )

    _render_sensitive_notes_results()
    _render_sensitive_keyword_manager()


def main() -> None:
    _initialise_state()
    _initialise_sensitive_state()

    api_settings = _get_api_settings()

    owner_tab, notes_tab = st.tabs(
        ["Owner Services Dashboard", "Sensitive Notes Monitor"]
    )

    with owner_tab:
        _render_owner_services_tab(api_settings)

    with notes_tab:
        _render_sensitive_notes_tab(api_settings)


if __name__ == "__main__":
    main()

