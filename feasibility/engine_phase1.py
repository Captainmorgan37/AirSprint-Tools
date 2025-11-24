"""Multi-leg quote feasibility engine."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Callable, List, Mapping, Optional, Sequence

from flight_leg_utils import load_airport_metadata_lookup, safe_parse_dt

from .airport_module import (
    AirportFeasibilityResult,
    AirportMetadataLookup,
    LegContext,
    build_leg_context_from_flight,
    evaluate_airport_feasibility_for_leg,
)
from .common import OSA_CATEGORY, SSA_CATEGORY, classify_airport_category
from .duty_module import evaluate_generic_duty_day
from .models import DayContext, FeasibilityRequest, FullFeasibilityResult
from .planning_notes import extract_requested_aircraft_from_note, find_route_mismatch
from .quote_lookup import build_quote_leg_options
from .schemas import CategoryStatus, combine_statuses


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _build_default_tz_provider() -> Callable[[str], Optional[str]]:
    lookup = load_airport_metadata_lookup()

    def provider(icao: str) -> Optional[str]:
        record = lookup.get(icao.upper()) if isinstance(icao, str) else None
        if isinstance(record, Mapping):
            tz = record.get("tz")
            if isinstance(tz, str) and tz.strip():
                return tz.strip()
        return None

    return provider


def _build_leg_contexts(
    quote: Mapping[str, Any],
    airport_metadata: AirportMetadataLookup,
    *,
    pax_details_fetcher: Optional[Callable[[str], Mapping[str, Any]]] = None,
) -> List[LegContext]:
    quote_id = quote.get("id") or quote.get("quoteId") or quote.get("quoteNumber")
    options = build_quote_leg_options(quote, quote_id=str(quote_id) if quote_id is not None else None)
    aircraft = quote.get("aircraftObj")
    quote_aircraft_type = _coerce_str(aircraft.get("type") or aircraft.get("model")) if isinstance(aircraft, Mapping) else ""
    quote_aircraft_category = _coerce_str(aircraft.get("category")) if isinstance(aircraft, Mapping) else ""
    legs: List[LegContext] = []
    for option in options:
        flight = option.get("flight")
        if not isinstance(flight, Mapping):
            continue
        context = build_leg_context_from_flight(flight, airport_metadata=airport_metadata)
        if context:
            if pax_details_fetcher:
                flight_id = _coerce_str(flight.get("flightId") or flight.get("id"))
                if flight_id:
                    try:
                        pax_payload = pax_details_fetcher(flight_id)
                        context["pax_payload_source"] = "api"
                        context.pop("pax_payload_error", None)
                    except Exception as exc:
                        pax_payload = None
                        context["pax_payload_source"] = "api_error"
                        context["pax_payload_error"] = str(exc)
                    if isinstance(pax_payload, Mapping):
                        context["pax_payload"] = pax_payload
            if not context.get("aircraft_type") and quote_aircraft_type:
                context["aircraft_type"] = quote_aircraft_type
            if not context.get("aircraft_category") and quote_aircraft_category:
                context["aircraft_category"] = quote_aircraft_category
            legs.append(context)
    legs.sort(key=lambda leg: leg.get("departure_date_utc") or "")
    return legs


def _build_day_context(
    quote: Mapping[str, Any],
    legs: Sequence[LegContext],
) -> DayContext:
    aircraft = quote.get("aircraftObj")
    aircraft_type = ""
    aircraft_category = ""
    if isinstance(aircraft, Mapping):
        aircraft_type = _coerce_str(aircraft.get("type") or aircraft.get("model"))
        aircraft_category = _coerce_str(aircraft.get("category"))

    sales_contact = None
    sales = quote.get("salesPerson")
    if isinstance(sales, Mapping):
        first = _coerce_str(sales.get("firstName"))
        last = _coerce_str(sales.get("lastName"))
        name = " ".join(part for part in (first, last) if part)
        sales_contact = name or None

    booking_identifier = (
        _coerce_str(quote.get("bookingIdentifier"))
        or _coerce_str(quote.get("bookingCode"))
        or _coerce_str(quote.get("bookingid"))
    )

    day: DayContext = {
        "quote_id": _coerce_str(quote.get("bookingid") or quote.get("quoteId") or quote.get("id")) or None,
        "bookingIdentifier": booking_identifier or "UNKNOWN",
        "aircraft_type": aircraft_type or "Unknown Aircraft",
        "aircraft_category": aircraft_category or "",
        "legs": list(legs),
        "sales_contact": sales_contact,
        "createdDate": quote.get("createdDate"),
    }
    return day


def _leg_status(result: AirportFeasibilityResult) -> CategoryStatus:
    statuses = [category.status for _label, category in result.iter_all_categories()]
    return combine_statuses(statuses)


def _collect_issues(
    day: DayContext,
    leg_results: Sequence[AirportFeasibilityResult],
    duty_result: Mapping[str, Any],
) -> List[str]:
    issues: List[str] = []
    for entry in duty_result.get("issues", []):
        issues.append(f"Duty: {entry}")
    for index, (leg, result) in enumerate(zip(day.get("legs", []), leg_results), start=1):
        leg_label = f"Leg {index} {leg['departure_icao']}→{leg['arrival_icao']}"
        for label, category in result.iter_all_categories():
            if category.status == "PASS":
                continue
            summary = category.summary or f"{label} {category.status}"
            issues.append(f"{leg_label}: {label} — {summary}")
    return issues


def _sequence_label(day: DayContext) -> str:
    legs = day.get("legs", [])
    if not legs:
        return ""
    path: List[str] = []
    for leg in legs:
        if not path:
            path.append(leg["departure_icao"])
        path.append(leg["arrival_icao"])
    return "→".join(path)


def _format_date_range(day: DayContext) -> str:
    legs = day.get("legs", [])
    if not legs:
        return ""
    start_dt = safe_parse_dt(legs[0].get("departure_date_utc")) if legs[0].get("departure_date_utc") else None
    end_dt = safe_parse_dt(legs[-1].get("arrival_date_utc")) if legs[-1].get("arrival_date_utc") else None
    if start_dt and end_dt:
        start = start_dt.strftime("%d %b %Y")
        end = end_dt.strftime("%d %b %Y")
        if start == end:
            return start
        return f"{start} – {end}"
    return ""


def _determine_flight_category(
    legs: Sequence[LegContext], airport_metadata: AirportMetadataLookup
) -> Optional[str]:
    if not legs:
        return None

    any_osa = False
    any_ssa = False
    all_us = True
    seen_airport = False

    for leg in legs:
        for key in ("departure_icao", "arrival_icao"):
            icao = leg.get(key)
            if not isinstance(icao, str):
                all_us = False
                continue
            seen_airport = True
            classification = classify_airport_category(icao, airport_metadata)
            if classification.category == OSA_CATEGORY:
                any_osa = True
            elif classification.category == SSA_CATEGORY:
                any_ssa = True
            if classification.country != "US":
                all_us = False

    if any_osa:
        return OSA_CATEGORY
    if any_ssa:
        return SSA_CATEGORY
    if seen_airport and all_us:
        return "US point-to-point"
    return None


def _build_summary(
    day: DayContext,
    leg_results: Sequence[AirportFeasibilityResult],
    duty_result: Mapping[str, Any],
) -> str:
    legs = day.get("legs", [])
    lines: List[str] = []
    lines.append(f"Quote {day.get('bookingIdentifier')} ({day.get('aircraft_type')})")
    sequence = _sequence_label(day)
    date_range = _format_date_range(day)
    descriptor = f"{len(legs)}-leg sequence {sequence}" if legs else "No legs"
    if date_range:
        descriptor = f"{descriptor} on {date_range}"
    lines.append(descriptor)
    lines.append("")
    lines.append("Duty Day:")
    lines.append(f"- {duty_result.get('summary', 'Duty evaluation unavailable')}")
    if duty_result.get("split_duty_possible"):
        lines.append("- Split duty possible (≥6h ground).")
    if duty_result.get("reset_duty_possible"):
        lines.append("- Reset duty possible (≥11h15 ground).")
    lines.append("")
    for index, (leg, result) in enumerate(zip(legs, leg_results), start=1):
        lines.append(f"Leg {index} ({leg['departure_icao']}→{leg['arrival_icao']}):")
        status = _leg_status(result)
        non_pass_entries = [entry for entry in result.iter_all_categories() if entry[1].status != "PASS"]
        if not non_pass_entries:
            lines.append("- All checks PASS.")
        else:
            for label, category in non_pass_entries:
                detail = category.summary or category.status
                lines.append(f"- {label}: {detail} ({category.status})")
        lines.append("")
    return "\n".join(line for line in lines if line is not None)


def _determine_overall_status(
    leg_results: Sequence[AirportFeasibilityResult],
    duty_result: Mapping[str, Any],
) -> CategoryStatus:
    statuses: List[CategoryStatus] = [duty_result.get("status", "PASS")]
    for result in leg_results:
        statuses.append(_leg_status(result))

    # Full-day quotes should only surface PASS, CAUTION, or FAIL. If all
    # categories are informational, treat the day as a PASS.
    normalized = ["PASS" if status == "INFO" else status for status in statuses]

    return combine_statuses(normalized)


def _collect_planning_note_issues(day: DayContext) -> List[str]:
    issues: List[str] = []
    for index, leg in enumerate(day.get("legs", []), start=1):
        note = leg.get("planning_notes")
        if not note:
            continue
        mismatch = find_route_mismatch(
            leg.get("departure_icao", ""),
            leg.get("arrival_icao", ""),
            leg.get("departure_date_utc"),
            note,
        )
        if mismatch:
            issues.append(
                f"Leg {index} {leg.get('departure_icao')}→{leg.get('arrival_icao')}: {mismatch}"
            )
    return issues


def _normalize_aircraft_label(label: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", label or "").upper()


def _labels_match(requested: str, actual: str) -> bool:
    req_norm = _normalize_aircraft_label(requested)
    act_norm = _normalize_aircraft_label(actual)
    if not req_norm or not act_norm:
        return False
    return req_norm in act_norm or act_norm in req_norm


def _extract_requested_aircraft_type(quote: Mapping[str, Any]) -> Optional[str]:
    for key in (
        "requestedAircraftType",
        "requested_aircraft_type",
        "requestedType",
        "requestedAircraft",
        "requestedEquipment",
    ):
        value = _coerce_str(quote.get(key))
        if value:
            return value

    aircraft = quote.get("aircraftObj")
    if isinstance(aircraft, Mapping):
        for nested_key in (
            "requestedType",
            "requestedAircraftType",
            "requestedEquipment",
            "requested",
        ):
            value = _coerce_str(aircraft.get(nested_key))
            if value:
                return value

    planning_sources: List[str] = []
    for key in ("planningNotes", "planningNote", "notes"):
        text = _coerce_str(quote.get(key))
        if text:
            planning_sources.append(text)

    legs = quote.get("legs")
    if isinstance(legs, Sequence):
        for leg in legs:
            if not isinstance(leg, Mapping):
                continue
            text = _coerce_str(leg.get("planningNotes") or leg.get("planningNote"))
            if text:
                planning_sources.append(text)

    for note in planning_sources:
        requested = extract_requested_aircraft_from_note(note)
        if requested:
            return requested
    return None


def _build_requested_type_issue(quote: Mapping[str, Any], aircraft_type: str) -> Optional[str]:
    requested = _extract_requested_aircraft_type(quote)
    if not requested or not aircraft_type:
        return None
    if _labels_match(requested, aircraft_type):
        return None
    return (
        f"Requested aircraft type '{requested}' does not match quoted aircraft '{aircraft_type}'."
    )


def run_feasibility_phase1(request: FeasibilityRequest) -> FullFeasibilityResult:
    quote = request.get("quote")
    if not isinstance(quote, Mapping):
        raise ValueError("request must include a 'quote' mapping")

    airport_metadata = load_airport_metadata_lookup()
    pax_details_fetcher = request.get("pax_details_fetcher")
    legs = _build_leg_contexts(
        quote,
        airport_metadata,
        pax_details_fetcher=pax_details_fetcher if callable(pax_details_fetcher) else None,
    )
    if not legs:
        raise ValueError("Quote does not contain any valid legs.")

    day = _build_day_context(quote, legs)

    tz_provider = request.get("tz_provider") or _build_default_tz_provider()
    operational_notes_fetcher = request.get("operational_notes_fetcher")

    leg_results: List[AirportFeasibilityResult] = []
    for leg in day["legs"]:
        leg_results.append(
            evaluate_airport_feasibility_for_leg(
                leg,
                tz_provider=tz_provider,
                airport_metadata=airport_metadata,
                operational_notes_fetcher=operational_notes_fetcher,
            )
        )

    duty_result = evaluate_generic_duty_day(day, tz_provider=tz_provider)
    flight_category = _determine_flight_category(day["legs"], airport_metadata)
    overall_status = _determine_overall_status(leg_results, duty_result)
    issues = _collect_issues(day, leg_results, duty_result)
    validation_checks = _collect_planning_note_issues(day)
    requested_issue = _build_requested_type_issue(quote, day.get("aircraft_type", ""))
    if requested_issue:
        validation_checks.append(requested_issue)
    issues.extend(validation_checks)
    summary = _build_summary(day, leg_results, duty_result)

    return FullFeasibilityResult(
        quote_id=day.get("quote_id"),
        bookingIdentifier=day["bookingIdentifier"],
        aircraft_type=day["aircraft_type"],
        aircraft_category=day["aircraft_category"],
        flight_category=flight_category,
        legs=[result.as_dict() for result in leg_results],
        duty=duty_result,
        overall_status=overall_status,
        validation_checks=validation_checks,
        issues=issues,
        summary=summary,
    )
