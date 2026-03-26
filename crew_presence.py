"""Crew location inference helpers for availability searches."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass
class CrewPresenceResult:
    crew_name: str
    role: str
    airport: str
    status: str
    fleet_match: bool
    source: str
    event_time_utc: Optional[datetime]


def _to_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    return None


def _pick(mapping: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _airport(value: Any) -> str:
    return str(value or "").strip().upper()


def _contains_fleet_hint(value: Any, fleet: str) -> bool:
    target = fleet.casefold().strip()
    if not target:
        return True
    if isinstance(value, str):
        text = value.casefold()
        return target in text
    if isinstance(value, list):
        return any(_contains_fleet_hint(item, fleet) for item in value)
    if isinstance(value, Mapping):
        return any(_contains_fleet_hint(item, fleet) for item in value.values())
    return False


def _fleet_match(user: Mapping[str, Any], flights: Iterable[Mapping[str, Any]], fleet: str) -> bool:
    if not fleet:
        return True
    hints = [
        user.get("fleet"),
        user.get("fleets"),
        user.get("qualifications"),
        user.get("aircraftTypes"),
        user.get("aircraftType"),
    ]
    if any(_contains_fleet_hint(item, fleet) for item in hints):
        return True

    # Fallback: infer from registrations on flights the crew is attached to.
    fleet_hint = fleet.casefold().replace("+", "")
    for flight in flights:
        reg = str(_pick(flight, ("registrationNumber", "aircraftRegistration", "aircraftReg", "tail", "registration")) or "")
        reg_compact = reg.upper().replace("-", "")
        if fleet_hint == "cj2" and reg_compact.startswith("CFA"):
            return True
        if fleet_hint == "cj3" and (reg_compact.startswith("CFN") or reg_compact.startswith("CGN")):
            return True
        if fleet_hint in reg.casefold():
            return True
    return False


def _is_a_day_entry(entry: Mapping[str, Any]) -> bool:
    entry_type = str(_pick(entry, ("type", "eventType", "code", "name")) or "").upper()
    return entry_type in {"A", "ADAY", "A DAY", "OFF", "HOME"}


def _in_flight_at_time(flight: Mapping[str, Any], at_time: datetime) -> bool:
    start = _to_utc(_pick(flight, ("departureTime", "blockOffEstUTC", "etd", "from", "start", "out")))
    end = _to_utc(_pick(flight, ("arrivalTime", "blockOnEstUTC", "eta", "to", "end", "in")))
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return False
    return start <= at_time <= end


def _latest_arrival(flights: Iterable[Mapping[str, Any]], at_time: datetime) -> tuple[str, Optional[datetime], str]:
    best_airport = ""
    best_time: Optional[datetime] = None
    source = ""
    for flight in flights:
        end = _to_utc(_pick(flight, ("arrivalTime", "blockOnEstUTC", "eta", "to", "end", "in")))
        if not isinstance(end, datetime) or end > at_time:
            continue
        arr = _airport(_pick(flight, ("toAirport", "arrivalAirport", "airportTo", "realAirportTo")))
        if not arr:
            continue
        if best_time is None or end > best_time:
            route = f"{_airport(_pick(flight, ('fromAirport', 'departureAirport', 'airportFrom')))}-{arr}".strip("-")
            best_airport = arr
            best_time = end
            source = f"Latest arrived flight ({route})"
    return best_airport, best_time, source


def _latest_positioning(entries: Iterable[Mapping[str, Any]], at_time: datetime) -> tuple[str, Optional[datetime], str]:
    best_airport = ""
    best_time: Optional[datetime] = None
    for entry in entries:
        event_type = str(_pick(entry, ("type", "eventType", "code")) or "").upper()
        if event_type not in {"P", "POSITIONING"}:
            continue
        end = _to_utc(_pick(entry, ("to", "end", "arrivalTime", "in")))
        if not isinstance(end, datetime) or end > at_time:
            continue
        arr = _airport(_pick(entry, ("toAirport", "arrivalAirport", "airportTo", "to")))
        if not arr:
            continue
        if best_time is None or end > best_time:
            best_airport = arr
            best_time = end
    return best_airport, best_time, "Latest positioning"


def crew_at_airport(
    roster_rows: Iterable[Mapping[str, Any]],
    *,
    at_time: datetime,
    airport: str,
    fleet: str,
) -> List[CrewPresenceResult]:
    target_airport = airport.strip().upper()
    at_time_utc = at_time.astimezone(UTC) if at_time.tzinfo else at_time.replace(tzinfo=UTC)

    results: List[CrewPresenceResult] = []
    for row in roster_rows:
        if not isinstance(row, Mapping):
            continue
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        flights = [item for item in (row.get("flights") or []) if isinstance(item, Mapping)]
        entries = [item for item in (row.get("entries") or []) if isinstance(item, Mapping)]

        name = " ".join(
            part for part in [str(user.get("firstName") or "").strip(), str(user.get("lastName") or "").strip()] if part
        ).strip() or str(user.get("logName") or user.get("name") or user.get("email") or "Unknown")
        role = str(user.get("role") or user.get("position") or "")

        if any(_in_flight_at_time(flight, at_time_utc) for flight in flights):
            continue

        fleet_ok = _fleet_match(user, flights, fleet)
        if not fleet_ok:
            continue

        flight_airport, flight_time, flight_source = _latest_arrival(flights, at_time_utc)
        pos_airport, pos_time, _ = _latest_positioning(entries, at_time_utc)

        chosen_airport = ""
        chosen_time: Optional[datetime] = None
        source = ""
        status = "Unknown"

        if flight_time and (not pos_time or flight_time >= pos_time):
            chosen_airport = flight_airport
            chosen_time = flight_time
            source = flight_source
            status = "Arrived by flight"
        elif pos_time:
            chosen_airport = pos_airport
            chosen_time = pos_time
            source = "Latest positioning"
            status = "Positioned"

        if not chosen_airport:
            # A-day/home fallback.
            if any(_is_a_day_entry(entry) for entry in entries):
                chosen_airport = _airport(
                    _pick(user, ("baseAirport", "homeBase", "airport", "base", "station"))
                )
                chosen_time = at_time_utc
                source = "A-day/home-base fallback"
                status = "Home/A-day"

        if not chosen_airport or chosen_airport != target_airport:
            continue

        results.append(
            CrewPresenceResult(
                crew_name=name,
                role=role,
                airport=chosen_airport,
                status=status,
                fleet_match=fleet_ok,
                source=source,
                event_time_utc=chosen_time,
            )
        )

    results.sort(key=lambda item: (item.status != "Arrived by flight", item.crew_name.casefold()))
    return results


def results_to_rows(results: Iterable[CrewPresenceResult]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for result in results:
        rows.append(
            {
                "Crew": result.crew_name,
                "Role": result.role,
                "Airport": result.airport,
                "Status": result.status,
                "Why": result.source,
                "Last Event (UTC)": result.event_time_utc.strftime("%Y-%m-%d %H:%M") if result.event_time_utc else "",
            }
        )
    return rows
