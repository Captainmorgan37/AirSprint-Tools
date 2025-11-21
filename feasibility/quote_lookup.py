"""Quote lookup helpers for the feasibility engine."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from fl3xx_api import Fl3xxApiConfig, fetch_quote_details


class QuoteLookupError(RuntimeError):
    """Raised when a quote cannot be resolved to one or more legs."""


def _coerce_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    text = str(value).strip()
    return text or None


def _coerce_upper(value: Any) -> Optional[str]:
    text = _coerce_str(value)
    if text:
        return text.upper()
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_datetime_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 10**11:  # assume milliseconds
            seconds = seconds / 1000.0
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    text = _coerce_str(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_label_time(value: Any) -> Optional[str]:
    iso_value = _format_datetime_value(value)
    if not iso_value:
        return None
    try:
        dt = datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
    except ValueError:
        return iso_value
    return dt.strftime("%Y-%m-%d %H:%MZ")


def _resolve_airport_code(leg: Mapping[str, Any], prefix: str) -> Optional[str]:
    direct = _coerce_upper(leg.get(f"{prefix}Airport"))
    if direct:
        return direct
    obj = leg.get(f"{prefix}AirportObj")
    if isinstance(obj, Mapping):
        for key in ("icao", "iata", "lid", "code"):
            candidate = _coerce_upper(obj.get(key))
            if candidate:
                return candidate
    return None


def _apply_airport_code(flight: MutableMapping[str, Any], leg: Mapping[str, Any], prefix: str) -> None:
    code = _resolve_airport_code(leg, prefix)
    if not code:
        return
    if prefix == "departure":
        keys = ("departureAirport", "dep_airport", "airportFrom", "fromAirport")
    else:
        keys = ("arrivalAirport", "arr_airport", "airportTo", "toAirport")
    for key in keys:
        flight.setdefault(key, code)


def _apply_aircraft_metadata(flight: MutableMapping[str, Any], quote: Mapping[str, Any]) -> None:
    aircraft = quote.get("aircraftObj")
    if not isinstance(aircraft, Mapping):
        return
    for target, source_keys in (
        ("tailNumber", ("tailNumber", "tail", "tail_number")),
        ("aircraftType", ("type", "typeName", "model")),
        ("aircraftName", ("aircraftName", "model")),
    ):
        for key in source_keys:
            value = _coerce_str(aircraft.get(key))
            if value:
                flight.setdefault(target, value)
                break
    seats = _coerce_int(aircraft.get("numberOfSeats"))
    if seats is not None:
        flight.setdefault("numberOfSeats", seats)
    category = _coerce_str(aircraft.get("category"))
    if category:
        flight.setdefault("aircraftCategory", category)
    owners_required = aircraft.get("ownersApprovalRequired")
    if owners_required is not None:
        flight.setdefault("ownersApprovalRequired", owners_required)
    equipment = aircraft.get("equipment")
    if isinstance(equipment, Mapping):
        flight.setdefault("equipment", dict(equipment))
    flight.setdefault("aircraftObj", dict(aircraft))


def _coalesce_str(*values: Any) -> Optional[str]:
    for value in values:
        text = _coerce_str(value)
        if text:
            return text
    return None


def _normalize_quote_leg(
    quote: Mapping[str, Any],
    leg: Mapping[str, Any],
    *,
    quote_id: Optional[str],
    index: int,
) -> Dict[str, Any]:
    flight: Dict[str, Any] = {}
    for key, value in leg.items():
        if isinstance(value, Mapping):
            flight[key] = dict(value)
        elif isinstance(value, list):
            flight[key] = list(value)
        else:
            flight[key] = value

    flight.setdefault("source", "quote")
    flight.setdefault("quoteLegIndex", index)

    resolved_quote_id = _coalesce_str(quote_id, quote.get("id"), quote.get("quoteId"), quote.get("quoteNumber"))
    if resolved_quote_id:
        flight.setdefault("quoteId", resolved_quote_id)

    booking_identifier = _coalesce_str(
        leg.get("bookingIdentifier"),
        quote.get("bookingIdentifier"),
        quote.get("bookingCode"),
        quote.get("bookingid"),
    )
    if booking_identifier:
        flight.setdefault("bookingIdentifier", booking_identifier)

    quote_number = _coerce_str(quote.get("quoteNumber"))
    if quote_number:
        flight.setdefault("quoteNumber", quote_number)

    workflow = _coalesce_str(leg.get("workflow"), quote.get("workflow"))
    if workflow:
        flight.setdefault("workflow", workflow)
    custom_workflow = _coalesce_str(leg.get("workflowCustomName"), quote.get("workflowCustomName"))
    if custom_workflow:
        flight.setdefault("workflowCustomName", custom_workflow)

    dep_time = _format_datetime_value(leg.get("departureDateUTC") or leg.get("departureDate"))
    if dep_time:
        flight.setdefault("dep_time", dep_time)
        flight.setdefault("departureTime", dep_time)
    arr_time = _format_datetime_value(leg.get("arrivalDateUTC") or leg.get("arrivalDate"))
    if arr_time:
        flight.setdefault("arrivalTime", arr_time)

    block_minutes = leg.get("blockTime")
    if block_minutes is None:
        block_minutes = leg.get("flightTime")
    if block_minutes is not None:
        flight.setdefault("blockTime", block_minutes)
        flight.setdefault("plannedBlockTime", block_minutes)
    if leg.get("flightTime") is not None:
        flight.setdefault("flightTime", leg.get("flightTime"))

    pax = _coerce_int(leg.get("pax"))
    if pax is not None:
        flight.setdefault("pax", pax)

    route_countries = leg.get("routeCountries") or quote.get("routeCountries")
    if route_countries:
        flight.setdefault("routeCountries", route_countries)
    fir_codes = leg.get("firCodes") or quote.get("firCodes")
    if fir_codes:
        flight.setdefault("firCodes", fir_codes)

    comment = _coerce_str(quote.get("comment"))
    if comment:
        flight.setdefault("quoteComment", comment)

    _apply_aircraft_metadata(flight, quote)
    _apply_airport_code(flight, leg, "departure")
    _apply_airport_code(flight, leg, "arrival")

    leg_id = _coalesce_str(leg.get("id"), leg.get("legId"))
    if leg_id:
        flight.setdefault("flightId", leg_id)
        flight.setdefault("id", leg_id)
    elif resolved_quote_id:
        synthetic_id = f"{resolved_quote_id}-LEG-{index + 1}"
        flight.setdefault("flightId", synthetic_id)
        flight.setdefault("id", synthetic_id)

    return flight


def _format_leg_label(leg: Mapping[str, Any], index: int) -> str:
    dep = _resolve_airport_code(leg, "departure") or "???"
    arr = _resolve_airport_code(leg, "arrival") or "???"
    label = f"Leg {index + 1}: {dep} → {arr}"
    dep_time = _format_label_time(leg.get("departureDateUTC") or leg.get("departureDate"))
    if dep_time:
        label = f"{label} ({dep_time})"
    return label


def fetch_quote_payload(
    config: Fl3xxApiConfig,
    quote_id: str,
    *,
    session: Optional[Any] = None,
) -> Mapping[str, Any]:
    if not quote_id or not str(quote_id).strip():
        raise QuoteLookupError("Quote ID is required.")
    payload = fetch_quote_details(config, quote_id, session=session)
    if not isinstance(payload, Mapping):
        raise QuoteLookupError("Quote API returned an unexpected payload structure.")
    return payload


def build_quote_leg_options(
    quote_payload: Mapping[str, Any],
    *,
    quote_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    legs_raw = quote_payload.get("legs")
    if not isinstance(legs_raw, Sequence):
        raise QuoteLookupError("Quote does not contain any legs.")

    options: List[Dict[str, Any]] = []
    for index, leg in enumerate(legs_raw):
        if not isinstance(leg, Mapping):
            continue
        status = _coerce_upper(leg.get("status"))
        if status in {"CANCELED", "CANCELLED"}:
            continue
        flight = _normalize_quote_leg(quote_payload, leg, quote_id=quote_id, index=index)
        identifier = _coalesce_str(leg.get("id"), leg.get("legId")) or f"LEG-{index + 1}"
        label = _format_leg_label(leg, index)
        options.append(
            {
                "identifier": identifier,
                "label": label,
                "flight": flight,
                "leg": dict(leg),
                "index": index,
            }
        )

    if not options:
        raise QuoteLookupError("Quote does not contain any usable leg data.")
    return options


def fetch_quote_leg_options(
    config: Fl3xxApiConfig,
    quote_id: str,
    *,
    session: Optional[Any] = None,
) -> Tuple[List[Dict[str, Any]], Mapping[str, Any]]:
    payload = fetch_quote_payload(config, quote_id, session=session)
    return build_quote_leg_options(payload, quote_id=quote_id), payload
