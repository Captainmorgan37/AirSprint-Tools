"""Smart booking identifier lookup helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from fl3xx_api import Fl3xxApiConfig, fetch_flights

BOOKING_KEYS: Tuple[str, ...] = (
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


class BookingLookupError(RuntimeError):
    """Raised when a booking identifier cannot be resolved."""


@dataclass
class LookupResult:
    """Represents a successful lookup along with metadata."""

    flight: Mapping[str, Any]
    tier: str
    range_start: date
    range_end: date


def _normalize_booking(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _cache_key(start: date, end: date) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def _store_cache(
    cache: Optional[MutableMapping[str, List[Dict[str, Any]]]],
    start: date,
    end: date,
    flights: List[Dict[str, Any]],
) -> None:
    if cache is not None:
        cache[_cache_key(start, end)] = flights


def _fetch_range(
    config: Fl3xxApiConfig,
    start: date,
    end: date,
    *,
    cache: Optional[MutableMapping[str, List[Dict[str, Any]]]] = None,
    session: Any = None,
) -> List[Dict[str, Any]]:
    if cache is not None:
        cached = cache.get(_cache_key(start, end))
        if cached is not None:
            return cached

    flights, _metadata = fetch_flights(config, from_date=start, to_date=end, session=session)
    _store_cache(cache, start, end, flights)
    return flights


def _match_flight(flights: Iterable[Mapping[str, Any]], booking_identifier: str) -> Optional[Mapping[str, Any]]:
    normalized = _normalize_booking(booking_identifier)
    if not normalized:
        return None
    for flight in flights:
        for key in BOOKING_KEYS:
            value = flight.get(key) if isinstance(flight, Mapping) else None
            if _normalize_booking(value) == normalized:
                return flight
    return None


def _iter_future_slabs(start: date, end: date) -> Iterable[Tuple[date, date]]:
    cursor = start
    while cursor <= end:
        offset = (cursor - start).days
        if offset < 30:
            span = 5
        elif offset < 180:
            span = 10
        else:
            span = 21
        slab_end = min(end, cursor + timedelta(days=span))
        yield cursor, slab_end
        cursor = slab_end + timedelta(days=1)


def lookup_booking(
    config: Fl3xxApiConfig,
    booking_identifier: str,
    *,
    now: Optional[datetime] = None,
    cache: Optional[MutableMapping[str, List[Dict[str, Any]]]] = None,
    session: Any = None,
) -> LookupResult:
    """Resolve ``booking_identifier`` to a FL3XX flight payload."""

    if not booking_identifier or not booking_identifier.strip():
        raise BookingLookupError("Booking identifier is required.")

    reference_time = now or datetime.now(timezone.utc)
    today = reference_time.date()

    search_plan: List[Tuple[str, Tuple[date, date]]] = [
        ("tier1", (today, today + timedelta(days=4))),
        ("tier2", (today - timedelta(days=2), today)),
    ]

    future_start = today + timedelta(days=4)
    future_end = today + timedelta(days=365)
    for slab_start, slab_end in _iter_future_slabs(future_start, future_end):
        search_plan.append(("tier3", (slab_start, slab_end)))

    normalized = _normalize_booking(booking_identifier)

    for tier, (start, end) in search_plan:
        flights = _fetch_range(config, start, end, cache=cache, session=session)
        match = _match_flight(flights, normalized or "")
        if match:
            return LookupResult(flight=match, tier=tier, range_start=start, range_end=end)

    raise BookingLookupError(f"Booking identifier '{booking_identifier}' was not found in FL3XX.")
