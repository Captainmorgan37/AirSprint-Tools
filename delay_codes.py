"""Utilities for evaluating delay codes in FL3XX postflight data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests

from fl3xx_api import Fl3xxApiConfig, fetch_flights, fetch_postflight
from flight_leg_utils import safe_parse_dt


@dataclass(frozen=True)
class DelayCodeRecord:
    flight_id: Optional[int]
    quote_id: Optional[int]
    booking_reference: Optional[str]
    account_name: Optional[str]
    flight_reference: Optional[str]
    tail_number: Optional[str]
    airport_from: Optional[str]
    airport_to: Optional[str]
    block_off_est: Optional[datetime]
    block_on_est: Optional[datetime]
    real_out: Optional[datetime]
    real_off: Optional[datetime]
    real_on: Optional[datetime]
    real_in: Optional[datetime]
    off_block_delay_min: Optional[int]
    on_block_delay_min: Optional[int]
    off_block_reasons: List[str]
    takeoff_reasons: List[str]
    landing_reasons: List[str]
    on_block_reasons: List[str]
    off_block_reason_status: str
    on_block_reason_status: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "Booking Reference": self.booking_reference,
            "Account": self.account_name,
            "Tail": self.tail_number,
            "From": self.airport_from,
            "To": self.airport_to,
            "Takeoff delay (min)": _format_delay(self.off_block_delay_min),
            "Block-on delay (min)": _format_delay(self.on_block_delay_min),
            "Off-block reasons": _format_reasons(self.off_block_reasons),
            "Takeoff reasons": _format_reasons(self.takeoff_reasons),
            "Landing reasons": _format_reasons(self.landing_reasons),
            "On-block reasons": _format_reasons(self.on_block_reasons),
            "Off-block reason status": self.off_block_reason_status,
            "On-block reason status": self.on_block_reason_status,
        }


def collect_delay_code_records(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    delay_threshold_min: int = 15,
    fetch_flights_fn=fetch_flights,
    fetch_postflight_fn=fetch_postflight,
) -> Tuple[List[DelayCodeRecord], Dict[str, Any], Dict[str, Any]]:
    """Return flights with delay codes for a given day."""

    from_date = target_date
    to_date = target_date + timedelta(days=1)

    flights, metadata = fetch_flights_fn(config, from_date=from_date, to_date=to_date)

    diagnostics: Dict[str, Any] = {
        "total_flights": len(flights),
        "started_flights": 0,
        "flagged_flights": 0,
        "postflight_errors": 0,
        "postflight_error_messages": [],
    }

    records: List[DelayCodeRecord] = []
    session: Optional[requests.Session] = None

    try:
        for flight in flights:
            if not _has_real_activity(flight):
                continue
            diagnostics["started_flights"] += 1

            block_off_est = _extract_datetime_value(
                flight,
                (
                    "blocksoffestimated",
                    "blockOffEstUTC",
                ),
            )
            block_on_est = _extract_datetime_value(
                flight,
                (
                    "blocksonestimated",
                    "blockOnEstUTC",
                ),
            )
            real_out = _extract_datetime_value(flight, ("realDateOUT",))
            real_off = _extract_datetime_value(flight, ("realDateOFF",))
            real_on = _extract_datetime_value(flight, ("realDateON",))
            real_in = _extract_datetime_value(flight, ("realDateIN",))

            off_block_delay_min = _compute_max_delay_minutes(
                block_off_est,
                [real_out, real_off],
            )
            on_block_delay_min = _compute_max_delay_minutes(
                block_on_est,
                [real_on, real_in],
            )

            has_delay = _delay_meets_threshold(off_block_delay_min, delay_threshold_min) or _delay_meets_threshold(
                on_block_delay_min,
                delay_threshold_min,
            )
            if not has_delay:
                continue

            diagnostics["flagged_flights"] += 1

            flight_id = flight.get("flightId")
            postflight_payload: Any = None
            if flight_id is not None:
                if session is None:
                    session = requests.Session()
                try:
                    postflight_payload = fetch_postflight_fn(config, flight_id, session=session)
                except Exception as exc:  # pragma: no cover - network/runtime issues
                    diagnostics["postflight_errors"] += 1
                    diagnostics["postflight_error_messages"].append(str(exc))

            reasons = _extract_delay_reasons(postflight_payload)

            off_block_reason_status = _reason_status(
                off_block_delay_min,
                delay_threshold_min,
                reasons["off_block"] + reasons["takeoff"],
            )
            on_block_reason_status = _reason_status(
                on_block_delay_min,
                delay_threshold_min,
                reasons["landing"] + reasons["on_block"],
            )

            record = DelayCodeRecord(
                flight_id=_coerce_int(flight.get("flightId")),
                quote_id=_coerce_int(flight.get("quoteId")),
                booking_reference=_coerce_str(
                    flight.get("bookingIdentifier")
                    or flight.get("bookingReference")
                ),
                account_name=_coerce_str(
                    flight.get("accountName")
                    or flight.get("account")
                    or flight.get("account_name")
                    or flight.get("owner")
                    or flight.get("ownerName")
                    or flight.get("customer")
                    or flight.get("customerName")
                    or flight.get("client")
                    or flight.get("clientName")
                ),
                flight_reference=_coerce_str(flight.get("flightNumberCompany") or flight.get("flightNumber")),
                tail_number=_coerce_str(
                    flight.get("registrationNumber")
                    or flight.get("tail")
                    or flight.get("tailNumber")
                ),
                airport_from=_coerce_str(flight.get("airportFrom")),
                airport_to=_coerce_str(flight.get("airportTo")),
                block_off_est=block_off_est,
                block_on_est=block_on_est,
                real_out=real_out,
                real_off=real_off,
                real_on=real_on,
                real_in=real_in,
                off_block_delay_min=off_block_delay_min,
                on_block_delay_min=on_block_delay_min,
                off_block_reasons=reasons["off_block"],
                takeoff_reasons=reasons["takeoff"],
                landing_reasons=reasons["landing"],
                on_block_reasons=reasons["on_block"],
                off_block_reason_status=off_block_reason_status,
                on_block_reason_status=on_block_reason_status,
            )
            records.append(record)
    finally:
        if session is not None:
            try:
                session.close()
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass

    records.sort(key=lambda item: (item.block_off_est or datetime.min))
    return records, metadata, diagnostics


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


def _has_real_activity(flight: Mapping[str, Any]) -> bool:
    for key in ("realDateOUT", "realDateOFF", "realDateON", "realDateIN"):
        value = flight.get(key)
        if isinstance(value, str) and value.strip():
            return True
        if value:
            return True
    return False


def _compute_max_delay_minutes(
    estimate: Optional[datetime],
    actuals: Sequence[Optional[datetime]],
) -> Optional[int]:
    if estimate is None:
        return None
    deltas: List[int] = []
    for actual in actuals:
        if actual is None:
            continue
        delta = actual - estimate
        deltas.append(int(delta.total_seconds() // 60))
    if not deltas:
        return None
    return max(deltas)


def _delay_meets_threshold(delay_minutes: Optional[int], threshold: int) -> bool:
    return delay_minutes is not None and delay_minutes >= threshold


def _extract_delay_reasons(payload: Any) -> Dict[str, List[str]]:
    if payload is None:
        return {
            "off_block": [],
            "takeoff": [],
            "landing": [],
            "on_block": [],
        }

    return {
        "off_block": _extract_reason_values(payload, ("delayOffBlockReason", "delayOffBlockReasons")),
        "takeoff": _extract_reason_values(payload, ("delayTakeOffReason", "delayTakeOffReasons")),
        "landing": _extract_reason_values(payload, ("delayLandingReason", "delayLandingReasons")),
        "on_block": _extract_reason_values(payload, ("delayOnBlockReason", "delayOnBlockReasons")),
    }


def _iter_mapping_candidates(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        yield payload
        for value in payload.values():
            yield from _iter_mapping_candidates(value)
    elif isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            yield from _iter_mapping_candidates(item)


def _extract_reason_values(payload: Any, keys: Iterable[str]) -> List[str]:
    reasons: List[str] = []
    for candidate in _iter_mapping_candidates(payload):
        for key in keys:
            value = candidate.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned:
                    reasons.append(cleaned)
                continue
            if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
                for entry in value:
                    if isinstance(entry, str):
                        cleaned = entry.strip()
                        if cleaned:
                            reasons.append(cleaned)
                    elif entry is not None:
                        reasons.append(str(entry))
    return _dedupe_keep_order(reasons)


def _dedupe_keep_order(items: Sequence[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _format_reasons(reasons: Sequence[str]) -> str:
    if not reasons:
        return "—"
    return ", ".join(reasons)


def _format_delay(delay_minutes: Optional[int]) -> str:
    if delay_minutes is None:
        return "—"
    return str(delay_minutes)


def _reason_status(delay_minutes: Optional[int], threshold: int, reasons: Sequence[str]) -> str:
    if delay_minutes is None or delay_minutes < threshold:
        return "—"
    return "Provided" if reasons else "Missing"


def _coerce_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
