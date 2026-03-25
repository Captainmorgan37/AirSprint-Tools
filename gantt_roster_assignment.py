"""Roster-to-flight assignment helpers used by the Gantt view."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


def roster_window_bounds(now: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """Return default roster pull bounds: -10 days to +5 days (UTC)."""

    reference = now.astimezone(UTC) if isinstance(now, datetime) and now.tzinfo else (now or datetime.now(UTC))
    start_day = (reference - timedelta(days=10)).date()
    end_day = (reference + timedelta(days=5)).date()
    return (
        datetime.combine(start_day, time(hour=0, minute=0), tzinfo=UTC),
        datetime.combine(end_day, time(hour=23, minute=59), tzinfo=UTC),
    )


def _to_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    return None


def _normalize_tail(value: Any) -> Optional[str]:
    text = "".join(char for char in str(value or "").upper() if char.isalnum())
    if not text:
        return None
    if text.startswith("C") and len(text) > 1:
        return f"{text[0]}-{text[1:]}"
    return text


def _airport(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    return text or None


def _pick(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _flight_key(tail: Optional[str], dep: Optional[str], arr: Optional[str], start: Optional[datetime]) -> Optional[Tuple[str, str, str, int]]:
    if not tail or not dep or not arr or not isinstance(start, datetime):
        return None
    minute_bucket = int(start.timestamp() // 60)
    return (tail, dep, arr, minute_bucket)


def _event_route(event: Mapping[str, Any]) -> Optional[str]:
    dep = _airport(_pick(event, ("fromAirport", "departureAirport", "from")))
    arr = _airport(_pick(event, ("toAirport", "arrivalAirport", "to")))
    if dep and arr:
        return f"{dep}-{arr}"
    return None


def _latest_positioning_before(
    entries: Iterable[Mapping[str, Any]],
    *,
    at_time: datetime,
) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    for entry in entries:
        event_type = str(_pick(entry, ("type", "eventType")) or "").upper()
        if event_type not in {"P", "POSITIONING"}:
            continue
        end = _to_utc(_pick(entry, ("to", "end", "arrivalTime", "in")))
        if not isinstance(end, datetime) or end > at_time:
            continue
        if at_time - end > timedelta(hours=48):
            continue
        if best is None or end > best["end"]:
            best = {"end": end, "route": _event_route(entry)}
    return best


def assign_roster_to_schedule_rows(
    schedule_rows: List[Dict[str, Any]],
    roster_rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach crew and positioning details to schedule rows when flight tuples match."""

    index: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}

    for row in roster_rows:
        if not isinstance(row, MutableMapping):
            continue
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        first = str(user.get("firstName") or "").strip()
        last = str(user.get("lastName") or "").strip()
        display = " ".join(part for part in (first, last) if part).strip()
        if not display:
            display = str(user.get("logName") or user.get("name") or user.get("email") or user.get("personnelNumber") or "Unknown")

        entries = [item for item in (row.get("entries") if isinstance(row.get("entries"), list) else []) if isinstance(item, Mapping)]
        flights = [item for item in (row.get("flights") if isinstance(row.get("flights"), list) else []) if isinstance(item, Mapping)]

        for flight in flights:
            start = _to_utc(_pick(flight, ("departureTime", "from", "start", "out")))
            dep = _airport(_pick(flight, ("fromAirport", "departureAirport")))
            arr = _airport(_pick(flight, ("toAirport", "arrivalAirport")))
            tail = _normalize_tail(
                _pick(
                    flight,
                    (
                        "aircraftRegistration",
                        "aircraftReg",
                        "aircraftTail",
                        "aircraft",
                        "tail",
                        "registration",
                    ),
                )
            )
            key = _flight_key(tail, dep, arr, start)
            if key is None:
                continue

            positioning = _latest_positioning_before(entries, at_time=start)
            bucket = index.setdefault(key, {"crew": set(), "positioning": []})
            bucket["crew"].add(display)
            if positioning and positioning.get("route"):
                route = str(positioning["route"])
                marker = f"{display}: {route} ({positioning['end'].strftime('%Y-%m-%d %H:%MZ')})"
                if marker not in bucket["positioning"]:
                    bucket["positioning"].append(marker)

    enriched: List[Dict[str, Any]] = []
    for row in schedule_rows:
        if row.get("category") not in {"Client Flight", "OCS Flight"}:
            row["crew"] = ""
            row["positioning"] = ""
            enriched.append(row)
            continue

        start = row.get("start_utc") if isinstance(row.get("start_utc"), datetime) else None
        dep = _airport(_pick(row, ("departure_airport",)))
        arr = _airport(_pick(row, ("arrival_airport",)))
        key = _flight_key(_normalize_tail(row.get("tail")), dep, arr, start)

        bucket = index.get(key or ("", "", "", -1), {"crew": set(), "positioning": []})
        crew = sorted(bucket.get("crew") or [])
        row["crew"] = " | ".join(crew)
        row["positioning"] = " | ".join(bucket.get("positioning") or [])
        enriched.append(row)

    return enriched
