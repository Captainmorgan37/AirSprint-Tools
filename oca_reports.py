"""Utilities for building OCA-focused operational reports."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests

from fl3xx_api import Fl3xxApiConfig, fetch_flights, fetch_leg_details
from flight_leg_utils import FlightDataError, format_utc, safe_parse_dt

_RUNWAY_DATA_PATH = Path(__file__).with_name("runways.csv")
_RUNWAY_LENGTH_CACHE: Optional[Dict[str, int]] = None

_NOTE_KEYS: Tuple[str, ...] = (
    "bookingNote",
    "bookingNotes",
    "booking_note",
    "bookingnote",
    "booking",
    "notes",
)


def _minutes(hours: int, minutes: int) -> int:
    return hours * 60 + minutes


_CATEGORY_THRESHOLDS: Dict[str, Dict[int, int]] = {
    "C25B": {
        1: _minutes(4, 40),
        2: _minutes(4, 40),
        3: _minutes(4, 20),
        4: _minutes(4, 5),
        5: _minutes(3, 45),
        6: _minutes(3, 30),
        7: _minutes(3, 15),
    },
    "C25A": {
        1: _minutes(3, 45),
        2: _minutes(3, 45),
        3: _minutes(3, 25),
        4: _minutes(3, 10),
        5: _minutes(2, 55),
        6: _minutes(2, 35),
        7: _minutes(2, 0),
    },
    "E545": {
        1: _minutes(6, 25),
        2: _minutes(6, 20),
        3: _minutes(6, 10),
        4: _minutes(6, 0),
        5: _minutes(5, 50),
        6: _minutes(5, 45),
        7: _minutes(5, 35),
        8: _minutes(5, 25),
        9: _minutes(5, 15),
    },
}

_CATEGORY_ALIASES = {
    "E550": "E545",
}

_ZFW_PAX_THRESHOLDS: Dict[str, int] = {
    "C25A": 5,
    "C25B": 6,
    "E545": 9,
}


def _normalise_airport_ident(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return text


def _load_runway_length_cache() -> Dict[str, int]:
    global _RUNWAY_LENGTH_CACHE
    if _RUNWAY_LENGTH_CACHE is not None:
        return _RUNWAY_LENGTH_CACHE

    cache: Dict[str, int] = {}
    try:
        with _RUNWAY_DATA_PATH.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ident = _normalise_airport_ident(row.get("airport_ident"))
                if not ident:
                    continue

                length_text = row.get("length_ft")
                if length_text is None:
                    continue
                try:
                    length_value = int(float(str(length_text).strip()))
                except (TypeError, ValueError):
                    continue
                if length_value <= 0:
                    continue

                current = cache.get(ident)
                if current is None or length_value > current:
                    cache[ident] = length_value

                if len(ident) == 4 and ident[0] in {"C", "K"}:
                    alias = ident[1:]
                    if len(alias) >= 3:
                        alias_current = cache.get(alias)
                        if alias_current is None or length_value > alias_current:
                            cache[alias] = length_value
    except FileNotFoundError:
        cache = {}

    _RUNWAY_LENGTH_CACHE = cache
    return cache


def _lookup_max_runway_length(airport: Optional[str]) -> Optional[int]:
    ident = _normalise_airport_ident(airport)
    if not ident:
        return None
    cache = _load_runway_length_cache()
    length = cache.get(ident)
    return int(length) if length is not None else None


@dataclass(frozen=True)
class MaxFlightTimeAlert:
    """Data describing a flight that exceeds the configured time limits."""

    flight_id: Any
    quote_id: Any
    flight_reference: Optional[str]
    booking_reference: Optional[str]
    aircraft_category: Optional[str]
    pax_count: Optional[int]
    departure_utc: Optional[str]
    arrival_utc: Optional[str]
    duration_minutes: int
    threshold_minutes: int
    overage_minutes: int
    airport_from: Optional[str]
    airport_to: Optional[str]
    registration: Optional[str]
    flight_number: Optional[str]
    booking_note: Optional[str]
    booking_note_present: bool
    booking_note_confirms_fpl: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "flight_id": self.flight_id,
            "quote_id": self.quote_id,
            "flight_reference": self.flight_reference,
            "booking_reference": self.booking_reference,
            "aircraft_category": self.aircraft_category,
            "pax_count": self.pax_count,
            "departure_utc": self.departure_utc,
            "arrival_utc": self.arrival_utc,
            "duration_minutes": self.duration_minutes,
            "threshold_minutes": self.threshold_minutes,
            "overage_minutes": self.overage_minutes,
            "airport_from": self.airport_from,
            "airport_to": self.airport_to,
            "registration": self.registration,
            "flight_number": self.flight_number,
            "booking_note": self.booking_note,
            "booking_note_present": self.booking_note_present,
            "booking_note_confirms_fpl": self.booking_note_confirms_fpl,
        }


@dataclass(frozen=True)
class ZfwFlightCheck:
    """Data describing flights that require a ZFW confirmation."""

    flight_id: Any
    quote_id: Any
    flight_reference: Optional[str]
    booking_reference: Optional[str]
    aircraft_category: Optional[str]
    pax_count: Optional[int]
    pax_threshold: Optional[int]
    departure_utc: Optional[str]
    arrival_utc: Optional[str]
    airport_from: Optional[str]
    airport_to: Optional[str]
    registration: Optional[str]
    flight_number: Optional[str]
    booking_note: Optional[str]
    booking_note_present: bool
    booking_note_confirms_zfw: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "flight_id": self.flight_id,
            "quote_id": self.quote_id,
            "flight_reference": self.flight_reference,
            "booking_reference": self.booking_reference,
            "aircraft_category": self.aircraft_category,
            "pax_count": self.pax_count,
            "pax_threshold": self.pax_threshold,
            "departure_utc": self.departure_utc,
            "arrival_utc": self.arrival_utc,
            "airport_from": self.airport_from,
            "airport_to": self.airport_to,
            "registration": self.registration,
            "flight_number": self.flight_number,
            "booking_note": self.booking_note,
            "booking_note_present": self.booking_note_present,
            "booking_note_confirms_zfw": self.booking_note_confirms_zfw,
        }


@dataclass(frozen=True)
class RunwayLengthCheck:
    """Data describing flights operating from airports below a runway threshold."""

    flight_id: Any
    quote_id: Any
    flight_reference: Optional[str]
    booking_reference: Optional[str]
    departure_utc: Optional[str]
    arrival_utc: Optional[str]
    airport_from: Optional[str]
    airport_to: Optional[str]
    registration: Optional[str]
    flight_number: Optional[str]
    departure_runway_length_ft: Optional[int]
    arrival_runway_length_ft: Optional[int]
    departure_below_threshold: bool
    arrival_below_threshold: bool
    runway_threshold_ft: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "flight_id": self.flight_id,
            "quote_id": self.quote_id,
            "flight_reference": self.flight_reference,
            "booking_reference": self.booking_reference,
            "departure_utc": self.departure_utc,
            "arrival_utc": self.arrival_utc,
            "airport_from": self.airport_from,
            "airport_to": self.airport_to,
            "registration": self.registration,
            "flight_number": self.flight_number,
            "departure_runway_length_ft": self.departure_runway_length_ft,
            "arrival_runway_length_ft": self.arrival_runway_length_ft,
            "departure_below_threshold": self.departure_below_threshold,
            "arrival_below_threshold": self.arrival_below_threshold,
            "runway_threshold_ft": self.runway_threshold_ft,
        }


def _normalise_category(category: Optional[str]) -> Optional[str]:
    if not isinstance(category, str):
        return None
    upper = category.strip().upper()
    if not upper:
        return None
    return _CATEGORY_ALIASES.get(upper, upper)


def _lookup_threshold_minutes(category: Optional[str], pax_count: Optional[int]) -> Optional[int]:
    if pax_count is None:
        return None
    if not isinstance(pax_count, int):
        try:
            pax_count = int(pax_count)
        except (TypeError, ValueError):
            return None
    if pax_count <= 0:
        return None
    normalized_category = _normalise_category(category)
    if normalized_category is None:
        return None
    thresholds = _CATEGORY_THRESHOLDS.get(normalized_category)
    if not thresholds:
        return None
    return thresholds.get(pax_count)


def _extract_datetime_value(row: Mapping[str, Any], keys: Iterable[str]) -> Optional[datetime]:
    for key in keys:
        value = row.get(key)
        if not value:
            continue
        try:
            return safe_parse_dt(str(value))
        except Exception:
            continue
    return None


def _extract_datetime_text(row: Mapping[str, Any], keys: Iterable[str]) -> Optional[str]:
    dt = _extract_datetime_value(row, keys)
    if dt is None:
        return None
    return format_utc(dt)


def _compute_duration_minutes(row: Mapping[str, Any]) -> Optional[int]:
    departure_keys = (
        "blocksoffestimated",
        "blockOffEstUTC",
        "realDateOFF",
        "realDateOUT",
        "etd",
    )
    arrival_keys = (
        "blocksonestimated",
        "blockOnEstUTC",
        "realDateON",
        "realDateIN",
        "eta",
    )

    dep_dt = _extract_datetime_value(row, departure_keys)
    arr_dt = _extract_datetime_value(row, arrival_keys)
    if dep_dt is None or arr_dt is None:
        return None

    if arr_dt <= dep_dt:
        return None

    delta = arr_dt - dep_dt
    return int(delta.total_seconds() // 60)


def _iter_mapping_candidates(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        yield payload
        for value in payload.values():
            yield from _iter_mapping_candidates(value)
    elif isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            yield from _iter_mapping_candidates(item)


def _extract_booking_identifier(payload: Any) -> Optional[str]:
    for candidate in _iter_mapping_candidates(payload):
        value = candidate.get("bookingIdentifier")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_booking_reference(payload: Any) -> Optional[str]:
    for candidate in _iter_mapping_candidates(payload):
        for key in (
            "bookingReference",
            "booking_reference",
            "booking",
        ):
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    identifier = _extract_booking_identifier(payload)
    if identifier:
        return identifier
    return None


def _extract_flight_reference(payload: Any) -> Optional[str]:
    identifier = _extract_booking_identifier(payload)
    if identifier:
        return identifier

    for candidate in _iter_mapping_candidates(payload):
        for key in (
            "flightReference",
            "flight_reference",
            "flightReferenceCode",
            "flight_reference_code",
            "flightreference",
        ):
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    # Fallback to the booking reference when a dedicated flight reference is
    # not present in the payload. This preserves backwards compatibility with
    # historical data where the booking reference doubled as the reference code.
    return _extract_booking_reference(payload)


def _extract_pax_count(row: Mapping[str, Any]) -> Optional[int]:
    pax_value = row.get("paxNumber")
    if isinstance(pax_value, int):
        return pax_value
    if pax_value is not None:
        try:
            pax_int = int(pax_value)
        except (TypeError, ValueError):
            pax_int = None
        else:
            return pax_int

    pax_refs = row.get("paxReferences")
    if isinstance(pax_refs, Iterable) and not isinstance(pax_refs, (str, bytes, bytearray)):
        count = sum(1 for item in pax_refs if isinstance(item, Mapping))
        if count:
            return count
    return None


def _extract_booking_note(payload: Any) -> Optional[str]:
    for candidate in _iter_mapping_candidates(payload):
        for key in _NOTE_KEYS:
            if key in candidate:
                value = candidate[key]
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def format_duration_label(minutes: Optional[int]) -> str:
    if minutes is None:
        return "—"
    sign = ""
    value = minutes
    if value < 0:
        sign = "-"
        value = abs(value)
    hours, remainder = divmod(value, 60)
    return f"{sign}{hours:d}h {remainder:02d}m"


def evaluate_flights_for_max_time(
    config: Fl3xxApiConfig,
    *,
    from_date: date,
    to_date: date,
    fetch_flights_fn=fetch_flights,
    fetch_leg_details_fn=fetch_leg_details,
) -> Tuple[List[MaxFlightTimeAlert], Dict[str, Any], Dict[str, Any]]:
    """Return flights that exceed the allowable block time window."""

    if to_date <= from_date:
        raise FlightDataError("The end date must be after the start date for the OCA report window.")

    flights, metadata = fetch_flights_fn(config, from_date=from_date, to_date=to_date)

    diagnostics: Dict[str, Any] = {
        "total_flights": len(flights),
        "pax_flights": 0,
        "threshold_applicable": 0,
        "missing_duration": 0,
        "flagged_flights": 0,
        "notes_requested": 0,
        "notes_found": 0,
        "booking_note_confirmations": 0,
        "note_errors": 0,
        "note_error_messages": [],
    }

    alerts: List[MaxFlightTimeAlert] = []

    session: Optional[requests.Session] = None

    try:
        for row in flights:
            flight_type = str(row.get("flightType") or "").upper()
            if flight_type != "PAX":
                continue
            diagnostics["pax_flights"] += 1

            pax_count = _extract_pax_count(row)
            threshold = _lookup_threshold_minutes(row.get("aircraftCategory"), pax_count)
            if threshold is None:
                continue
            diagnostics["threshold_applicable"] += 1

            duration_minutes = _compute_duration_minutes(row)
            if duration_minutes is None:
                diagnostics["missing_duration"] += 1
                continue

            if duration_minutes <= threshold:
                continue

            overage = duration_minutes - threshold

            alert = MaxFlightTimeAlert(
                flight_id=row.get("flightId"),
                quote_id=row.get("quoteId"),
                flight_reference=_extract_flight_reference(row),
                booking_reference=_extract_booking_reference(row),
                aircraft_category=row.get("aircraftCategory"),
                pax_count=pax_count,
                departure_utc=_extract_datetime_text(
                    row,
                    ("blocksoffestimated", "blockOffEstUTC", "realDateOFF", "realDateOUT", "etd"),
                ),
                arrival_utc=_extract_datetime_text(
                    row,
                    ("blocksonestimated", "blockOnEstUTC", "realDateON", "realDateIN", "eta"),
                ),
                duration_minutes=duration_minutes,
                threshold_minutes=threshold,
                overage_minutes=overage,
                airport_from=row.get("airportFrom"),
                airport_to=row.get("airportTo"),
                registration=row.get("registrationNumber"),
                flight_number=row.get("flightNumberCompany") or row.get("flightNumber"),
                booking_note=None,
                booking_note_present=False,
                booking_note_confirms_fpl=False,
            )

            quote_id = alert.quote_id
            if quote_id:
                if session is None:
                    session = requests.Session()
                try:
                    payload = fetch_leg_details_fn(config, quote_id, session=session)
                except Exception as exc:  # pragma: no cover - network/runtime issues
                    diagnostics["note_errors"] += 1
                    diagnostics["note_error_messages"].append(str(exc))
                else:
                    diagnostics["notes_requested"] += 1
                    updates: Dict[str, Any] = {}
                    payload_reference = _extract_flight_reference(payload)
                    if (
                        payload_reference
                        and payload_reference != alert.flight_reference
                    ):
                        updates["flight_reference"] = payload_reference
                    payload_booking_reference = _extract_booking_reference(payload)
                    if payload_booking_reference and not alert.booking_reference:
                        updates["booking_reference"] = payload_booking_reference
                    if updates:
                        alert = MaxFlightTimeAlert(
                            **{
                                **alert.as_dict(),
                                **updates,
                            }
                        )
                    note = _extract_booking_note(payload)
                    if note:
                        diagnostics["notes_found"] += 1
                        confirmation = "FPL RUN BY" in note.upper()
                        if confirmation:
                            diagnostics["booking_note_confirmations"] += 1
                        alert = MaxFlightTimeAlert(
                            **{
                                **alert.as_dict(),
                                "booking_note": note,
                                "booking_note_present": True,
                                "booking_note_confirms_fpl": confirmation,
                            }
                        )

            alerts.append(alert)
            diagnostics["flagged_flights"] += 1
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    alerts.sort(key=lambda a: (a.departure_utc or "", a.flight_id or 0))

    return alerts, metadata, diagnostics


def evaluate_flights_for_zfw_check(
    config: Fl3xxApiConfig,
    *,
    from_date: date,
    to_date: date,
    fetch_flights_fn=fetch_flights,
    fetch_leg_details_fn=fetch_leg_details,
) -> Tuple[List[ZfwFlightCheck], Dict[str, Any], Dict[str, Any]]:
    """Return PAX flights that require a Zero Fuel Weight note review."""

    if to_date <= from_date:
        raise FlightDataError("The end date must be after the start date for the OCA report window.")

    flights, metadata = fetch_flights_fn(config, from_date=from_date, to_date=to_date)

    diagnostics: Dict[str, Any] = {
        "total_flights": len(flights),
        "pax_flights": 0,
        "threshold_applicable": 0,
        "flagged_flights": 0,
        "missing_pax_count": 0,
        "notes_requested": 0,
        "notes_found": 0,
        "zfw_confirmations": 0,
        "note_errors": 0,
        "note_error_messages": [],
    }

    items: List[ZfwFlightCheck] = []

    session: Optional[requests.Session] = None

    try:
        for row in flights:
            flight_type = str(row.get("flightType") or "").upper()
            if flight_type != "PAX":
                continue
            diagnostics["pax_flights"] += 1

            pax_count = _extract_pax_count(row)
            if pax_count is None:
                diagnostics["missing_pax_count"] += 1
                continue

            normalized_category = _normalise_category(row.get("aircraftCategory"))
            if normalized_category is None:
                continue

            threshold = _ZFW_PAX_THRESHOLDS.get(normalized_category)
            if threshold is None:
                continue
            diagnostics["threshold_applicable"] += 1

            if pax_count < threshold:
                continue

            item = ZfwFlightCheck(
                flight_id=row.get("flightId"),
                quote_id=row.get("quoteId"),
                flight_reference=_extract_flight_reference(row),
                booking_reference=_extract_booking_reference(row),
                aircraft_category=row.get("aircraftCategory"),
                pax_count=pax_count,
                pax_threshold=threshold,
                departure_utc=_extract_datetime_text(
                    row,
                    ("blocksoffestimated", "blockOffEstUTC", "realDateOFF", "realDateOUT", "etd"),
                ),
                arrival_utc=_extract_datetime_text(
                    row,
                    ("blocksonestimated", "blockOnEstUTC", "realDateON", "realDateIN", "eta"),
                ),
                airport_from=row.get("airportFrom"),
                airport_to=row.get("airportTo"),
                registration=row.get("registrationNumber"),
                flight_number=row.get("flightNumberCompany") or row.get("flightNumber"),
                booking_note=None,
                booking_note_present=False,
                booking_note_confirms_zfw=False,
            )

            quote_id = item.quote_id
            if quote_id:
                if session is None:
                    session = requests.Session()
                try:
                    payload = fetch_leg_details_fn(config, quote_id, session=session)
                except Exception as exc:  # pragma: no cover - network/runtime issues
                    diagnostics["note_errors"] += 1
                    diagnostics["note_error_messages"].append(str(exc))
                else:
                    diagnostics["notes_requested"] += 1
                    updates: Dict[str, Any] = {}
                    payload_reference = _extract_flight_reference(payload)
                    if (
                        payload_reference
                        and payload_reference != item.flight_reference
                    ):
                        updates["flight_reference"] = payload_reference
                    payload_booking_reference = _extract_booking_reference(payload)
                    if payload_booking_reference and not item.booking_reference:
                        updates["booking_reference"] = payload_booking_reference
                    if updates:
                        item = ZfwFlightCheck(
                            **{
                                **item.as_dict(),
                                **updates,
                            }
                        )
                    note = _extract_booking_note(payload)
                    if note:
                        diagnostics["notes_found"] += 1
                        note_upper = note.upper()
                        confirmation = "ZFW" in note_upper and "OK WITH CURRENT PAX/BAGGAGE" in note_upper
                        if confirmation:
                            diagnostics["zfw_confirmations"] += 1
                        item = ZfwFlightCheck(
                            **{
                                **item.as_dict(),
                                "booking_note": note,
                                "booking_note_present": True,
                                "booking_note_confirms_zfw": confirmation,
                            }
                        )

            items.append(item)
            diagnostics["flagged_flights"] += 1
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    items.sort(key=lambda a: (a.departure_utc or "", a.flight_id or 0))

    return items, metadata, diagnostics


def evaluate_flights_for_runway_length(
    config: Fl3xxApiConfig,
    *,
    from_date: date,
    to_date: date,
    runway_threshold_ft: int = 5000,
    fetch_flights_fn=fetch_flights,
) -> Tuple[List[RunwayLengthCheck], Dict[str, Any], Dict[str, Any]]:
    """Return flights departing or arriving at airports below a runway length threshold."""

    if to_date <= from_date:
        raise FlightDataError("The end date must be after the start date for the OCA report window.")
    if runway_threshold_ft <= 0:
        raise FlightDataError("The runway length threshold must be a positive value.")

    flights, metadata = fetch_flights_fn(config, from_date=from_date, to_date=to_date)

    diagnostics: Dict[str, Any] = {
        "total_flights": len(flights),
        "flagged_flights": 0,
        "missing_departure_length": 0,
        "missing_arrival_length": 0,
    }

    items: List[RunwayLengthCheck] = []

    for row in flights:
        departure_airport = row.get("airportFrom")
        arrival_airport = row.get("airportTo")

        departure_length = _lookup_max_runway_length(departure_airport)
        arrival_length = _lookup_max_runway_length(arrival_airport)

        if departure_length is None:
            diagnostics["missing_departure_length"] += 1
        if arrival_length is None:
            diagnostics["missing_arrival_length"] += 1

        departure_short = departure_length is not None and departure_length < runway_threshold_ft
        arrival_short = arrival_length is not None and arrival_length < runway_threshold_ft

        if not (departure_short or arrival_short):
            continue

        item = RunwayLengthCheck(
            flight_id=row.get("flightId"),
            quote_id=row.get("quoteId"),
            flight_reference=_extract_flight_reference(row),
            booking_reference=_extract_booking_reference(row),
            departure_utc=_extract_datetime_text(
                row,
                ("blocksoffestimated", "blockOffEstUTC", "realDateOFF", "realDateOUT", "etd"),
            ),
            arrival_utc=_extract_datetime_text(
                row,
                ("blocksonestimated", "blockOnEstUTC", "realDateON", "realDateIN", "eta"),
            ),
            airport_from=departure_airport,
            airport_to=arrival_airport,
            registration=row.get("registrationNumber"),
            flight_number=row.get("flightNumberCompany") or row.get("flightNumber"),
            departure_runway_length_ft=departure_length,
            arrival_runway_length_ft=arrival_length,
            departure_below_threshold=departure_short,
            arrival_below_threshold=arrival_short,
            runway_threshold_ft=int(runway_threshold_ft),
        )

        items.append(item)
        diagnostics["flagged_flights"] += 1

    items.sort(key=lambda a: (a.departure_utc or "", a.flight_id or 0))

    return items, metadata, diagnostics


__all__ = [
    "MaxFlightTimeAlert",
    "ZfwFlightCheck",
    "RunwayLengthCheck",
    "evaluate_flights_for_max_time",
    "evaluate_flights_for_zfw_check",
    "evaluate_flights_for_runway_length",
    "format_duration_label",
]

