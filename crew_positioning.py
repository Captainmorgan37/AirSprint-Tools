"""Crew positioning intent analysis derived from FL3XX roster pulls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional


_DUTY_TYPES = {"F", "FLIGHT", "DUTY"}
_POSITIONING_TYPES = {"P", "POSITIONING"}


@dataclass(frozen=True)
class CrewPositioningStatus:
    personnel_number: str
    name: str
    trigram: str
    home_base_airport: Optional[str]
    current_airport: Optional[str]
    next_required_airport: Optional[str]
    next_required_utc: Optional[datetime]
    booked_positioning_route: Optional[str]
    booked_positioning_utc: Optional[datetime]
    status: str
    recommendation: str
    reason: str


def _normalize_airport(value: Any) -> Optional[str]:
    code = str(value or "").strip().upper()
    return code or None


def _to_datetime(value: Any) -> Optional[datetime]:
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


def _first_non_empty(user: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = user.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_home_base(row: Mapping[str, Any], user: Mapping[str, Any]) -> Optional[str]:
    for payload in (user, row):
        home_airport = payload.get("homeAirport") if isinstance(payload, Mapping) else None
        if isinstance(home_airport, Mapping):
            airport = _normalize_airport(home_airport.get("icao"))
            if airport:
                return airport
        for key in ("homeAirportIcao", "homeBaseIcao"):
            airport = _normalize_airport(payload.get(key)) if isinstance(payload, Mapping) else None
            if airport:
                return airport
    return None


def _extract_events(row: Mapping[str, Any]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for source in ("entries", "flights"):
        items = row.get(source)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            event_type = str(item.get("type") or item.get("eventType") or "").strip().upper() or None
            start = _to_datetime(item.get("from") or item.get("departureTime") or item.get("start") or item.get("out"))
            end = _to_datetime(item.get("to") or item.get("arrivalTime") or item.get("end") or item.get("in"))
            from_airport = _normalize_airport(item.get("fromAirport") or item.get("departureAirport"))
            to_airport = _normalize_airport(item.get("toAirport") or item.get("arrivalAirport"))
            notes = str(item.get("notes") or "").strip()
            events.append(
                {
                    "source": "flight" if source == "flights" else "entry",
                    "type": event_type,
                    "start": start,
                    "end": end,
                    "from_airport": from_airport,
                    "to_airport": to_airport,
                    "notes": notes,
                }
            )
    events.sort(key=lambda event: event.get("start") or datetime.min.replace(tzinfo=UTC))
    return events


def _is_duty_event(event: Mapping[str, Any]) -> bool:
    source = event.get("source")
    event_type = str(event.get("type") or "").upper()
    return source == "flight" or event_type in _DUTY_TYPES


def _is_positioning_event(event: Mapping[str, Any]) -> bool:
    event_type = str(event.get("type") or "").upper()
    return event_type in _POSITIONING_TYPES


def build_positioning_statuses(rows: Iterable[Mapping[str, Any]], *, at_time: datetime) -> List[CrewPositioningStatus]:
    reference = at_time.astimezone(UTC) if at_time.tzinfo else at_time.replace(tzinfo=UTC)
    statuses: List[CrewPositioningStatus] = []

    for row in rows:
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        events = _extract_events(row)
        if not events:
            continue

        name = " ".join(part for part in (_first_non_empty(user, "firstName"), _first_non_empty(user, "lastName")) if part).strip()
        if not name:
            name = _first_non_empty(user, "logName", "name", "email", "personnelNumber")

        personnel = _first_non_empty(user, "personnelNumber", "id")
        trigram = _first_non_empty(user, "trigram")
        home_base = _extract_home_base(row, user)

        active = None
        latest = None
        next_duty = None
        future_positionings: List[Dict[str, Any]] = []

        for event in events:
            start = event.get("start")
            end = event.get("end")
            if isinstance(start, datetime) and start <= reference:
                latest = event
            if isinstance(start, datetime) and isinstance(end, datetime) and start <= reference <= end:
                active = event

            if isinstance(start, datetime) and start > reference:
                if _is_duty_event(event) and next_duty is None:
                    next_duty = event
                if _is_positioning_event(event):
                    future_positionings.append(event)

        chosen = active or latest
        current_airport = None
        if isinstance(chosen, Mapping):
            current_airport = chosen.get("to_airport") or chosen.get("from_airport")
            if active and chosen.get("from_airport"):
                current_airport = chosen.get("from_airport")

        next_required_airport = None
        next_required_utc = None
        if isinstance(next_duty, Mapping):
            next_required_airport = next_duty.get("from_airport") or next_duty.get("to_airport")
            next_required_utc = next_duty.get("start")

        matching_positioning = None
        if next_required_airport:
            for event in future_positionings:
                if (
                    event.get("to_airport") == next_required_airport
                    and (
                        not isinstance(next_required_utc, datetime)
                        or not isinstance(event.get("start"), datetime)
                        or event["start"] <= next_required_utc
                    )
                ):
                    matching_positioning = event
                    break

        if matching_positioning is None and home_base and not next_required_airport and current_airport and current_airport != home_base:
            for event in future_positionings:
                if event.get("to_airport") == home_base:
                    matching_positioning = event
                    break

        booked_route = None
        booked_time = None
        if matching_positioning:
            from_airport = str(matching_positioning.get("from_airport") or "").strip().upper()
            to_airport = str(matching_positioning.get("to_airport") or "").strip().upper()
            if from_airport and to_airport:
                booked_route = f"{from_airport}-{to_airport}"
            booked_time = matching_positioning.get("start")

        needs_duty_positioning = bool(
            next_required_airport
            and current_airport
            and next_required_airport != current_airport
            and matching_positioning is None
        )

        needs_home_return = bool(
            home_base
            and current_airport
            and not next_required_airport
            and current_airport != home_base
            and matching_positioning is None
        )

        status = "NO_ACTION"
        recommendation = "No action"
        reason = "Crew is correctly positioned"

        if needs_duty_positioning:
            status = "ACTION_REQUIRED"
            recommendation = "Book positioning to next duty"
            reason = "Crew not at next required airport"
        elif next_required_airport and current_airport and next_required_airport == current_airport:
            status = "AT_REQUIRED"
            recommendation = "No action"
            reason = "Crew already at next required airport"
        elif next_required_airport and matching_positioning is not None:
            status = "POSITIONING_BOOKED"
            recommendation = "Verify booked positioning timing"
            reason = "Positioning event found to next duty"
        elif needs_home_return:
            status = "RETURN_HOME_REQUIRED"
            recommendation = "Book return-to-home positioning"
            reason = "No remaining duty and crew away from home base"
        elif home_base and matching_positioning is not None:
            status = "RETURN_HOME_BOOKED"
            recommendation = "Verify return-home positioning"
            reason = "Positioning event found to home base"

        statuses.append(
            CrewPositioningStatus(
                personnel_number=personnel,
                name=name,
                trigram=trigram,
                home_base_airport=home_base,
                current_airport=current_airport,
                next_required_airport=next_required_airport,
                next_required_utc=next_required_utc,
                booked_positioning_route=booked_route,
                booked_positioning_utc=booked_time,
                status=status,
                recommendation=recommendation,
                reason=reason,
            )
        )

    statuses.sort(key=lambda item: (item.status, item.current_airport or "", item.name))
    return statuses
