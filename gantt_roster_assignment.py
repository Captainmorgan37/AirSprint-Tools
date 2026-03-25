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


def _flight_key(
    tail: Optional[str],
    dep: Optional[str],
    arr: Optional[str],
    start: Optional[datetime],
) -> Optional[Tuple[str, str, str, int]]:
    if not tail or not dep or not arr or not isinstance(start, datetime):
        return None
    minute_bucket = int(start.timestamp() // 60)
    return (tail, dep, arr, minute_bucket)


def _event_route(event: Mapping[str, Any]) -> Optional[str]:
    dep = _airport(_pick(event, ("fromAirport", "departureAirport", "airportFrom", "from")))
    arr = _airport(_pick(event, ("toAirport", "arrivalAirport", "airportTo", "to")))
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


def _crew_display(person: Mapping[str, Any]) -> str:
    first = str(person.get("firstName") or "").strip()
    last = str(person.get("lastName") or "").strip()
    full_name = " ".join(part for part in (first, last) if part).strip()
    if full_name:
        role = str(person.get("role") or "").strip().upper()
        return f"{full_name} ({role})" if role else full_name
    return str(
        person.get("logName")
        or person.get("name")
        or person.get("email")
        or person.get("personnelNumber")
        or "Unknown"
    )


def _task_id_from_flight(flight: Mapping[str, Any]) -> Optional[str]:
    raw = _pick(flight, ("flightId", "flightID", "id"))
    if raw in (None, ""):
        return None
    return f"flight_{raw}".lower()


def _flight_payloads_from_row(row: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    payloads: List[Mapping[str, Any]] = []

    if isinstance(row, Mapping) and any(key in row for key in ("flightId", "registrationNumber", "airportFrom", "airportTo")):
        payloads.append(row)

    flights = row.get("flights")
    if isinstance(flights, list):
        payloads.extend(item for item in flights if isinstance(item, Mapping))

    return payloads


def assign_roster_to_schedule_rows(
    schedule_rows: List[Dict[str, Any]],
    roster_rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach crew, positioning, and roster flight details to schedule rows."""

    index_by_tuple: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}
    index_by_task_id: Dict[str, Dict[str, Any]] = {}

    for row in roster_rows:
        if not isinstance(row, MutableMapping):
            continue

        entries = [
            item for item in (row.get("entries") if isinstance(row.get("entries"), list) else []) if isinstance(item, Mapping)
        ]

        default_crew: List[str] = []
        user = row.get("user") if isinstance(row.get("user"), Mapping) else None
        if isinstance(user, Mapping):
            default_crew = [_crew_display(user)]

        for flight in _flight_payloads_from_row(row):
            start = _to_utc(
                _pick(
                    flight,
                    (
                        "departureTime",
                        "blockOffEstUTC",
                        "blocksoffestimated",
                        "etd",
                        "realDateOUT",
                        "from",
                        "start",
                        "out",
                    ),
                )
            )
            dep = _airport(_pick(flight, ("fromAirport", "departureAirport", "airportFrom", "realAirportFrom")))
            arr = _airport(_pick(flight, ("toAirport", "arrivalAirport", "airportTo", "realAirportTo")))
            tail = _normalize_tail(
                _pick(
                    flight,
                    (
                        "registrationNumber",
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
            task_id = _task_id_from_flight(flight)

            if key is None and not task_id:
                continue

            crew_payload = flight.get("crew") if isinstance(flight.get("crew"), list) else []
            crew_names = [
                _crew_display(member)
                for member in crew_payload
                if isinstance(member, Mapping)
            ]
            if not crew_names:
                crew_names = default_crew

            positioning_markers: List[str] = []
            if isinstance(start, datetime):
                for crew_name in crew_names:
                    positioning = _latest_positioning_before(entries, at_time=start)
                    if positioning and positioning.get("route"):
                        marker = (
                            f"{crew_name}: {positioning['route']} "
                            f"({positioning['end'].strftime('%Y-%m-%d %H:%MZ')})"
                        )
                        if marker not in positioning_markers:
                            positioning_markers.append(marker)

            bucket: Dict[str, Any] = {
                "crew": set(crew_names),
                "positioning": list(positioning_markers),
                "roster_flight_id": str(_pick(flight, ("flightId", "id")) or ""),
                "booking_reference": str(_pick(flight, ("bookingReference", "bookingIdentifier")) or ""),
                "flight_status": str(_pick(flight, ("flightStatus", "status")) or ""),
                "workflow_name": str(_pick(flight, ("workflowCustomName", "workflow")) or ""),
                "pax_number": _pick(flight, ("paxNumber",)),
            }

            if key is not None:
                existing = index_by_tuple.setdefault(key, {"crew": set(), "positioning": []})
                existing["crew"].update(bucket["crew"])
                for marker in bucket["positioning"]:
                    if marker not in existing["positioning"]:
                        existing["positioning"].append(marker)
                for meta_field in (
                    "roster_flight_id",
                    "booking_reference",
                    "flight_status",
                    "workflow_name",
                    "pax_number",
                ):
                    if not existing.get(meta_field) and bucket.get(meta_field) not in (None, ""):
                        existing[meta_field] = bucket.get(meta_field)

            if task_id:
                existing = index_by_task_id.setdefault(task_id, {"crew": set(), "positioning": []})
                existing["crew"].update(bucket["crew"])
                for marker in bucket["positioning"]:
                    if marker not in existing["positioning"]:
                        existing["positioning"].append(marker)
                for meta_field in (
                    "roster_flight_id",
                    "booking_reference",
                    "flight_status",
                    "workflow_name",
                    "pax_number",
                ):
                    if not existing.get(meta_field) and bucket.get(meta_field) not in (None, ""):
                        existing[meta_field] = bucket.get(meta_field)

    enriched: List[Dict[str, Any]] = []
    for row in schedule_rows:
        if row.get("category") not in {"Client Flight", "OCS Flight"}:
            row["crew"] = ""
            row["positioning"] = ""
            row["roster_flight_id"] = ""
            row["booking_reference"] = ""
            row["flight_status"] = ""
            row["workflow_name"] = ""
            row["pax_number"] = ""
            enriched.append(row)
            continue

        task_id = str(row.get("task_id") or "").strip().lower()
        start = row.get("start_utc") if isinstance(row.get("start_utc"), datetime) else None
        dep = _airport(_pick(row, ("departure_airport",)))
        arr = _airport(_pick(row, ("arrival_airport",)))
        key = _flight_key(_normalize_tail(row.get("tail")), dep, arr, start)

        bucket = index_by_task_id.get(task_id)
        if bucket is None and key is not None:
            bucket = index_by_tuple.get(key)
        if bucket is None:
            bucket = {"crew": set(), "positioning": []}

        row["crew"] = " | ".join(sorted(bucket.get("crew") or []))
        row["positioning"] = " | ".join(bucket.get("positioning") or [])
        row["roster_flight_id"] = str(bucket.get("roster_flight_id") or "")
        row["booking_reference"] = str(bucket.get("booking_reference") or "")
        row["flight_status"] = str(bucket.get("flight_status") or "")
        row["workflow_name"] = str(bucket.get("workflow_name") or "")
        row["pax_number"] = bucket.get("pax_number") if bucket.get("pax_number") is not None else ""
        enriched.append(row)

    return enriched
