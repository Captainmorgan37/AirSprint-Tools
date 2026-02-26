"""Helpers for parsing and exploring FL3XX staff roster pulls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional


@dataclass(frozen=True)
class CrewSnapshot:
    personnel_number: str
    name: str
    trigram: str
    active_event_type: Optional[str]
    active_event_label: Optional[str]
    current_airport: Optional[str]
    next_airport: Optional[str]
    event_start_utc: Optional[datetime]
    event_end_utc: Optional[datetime]
    event_aircraft: Optional[str]
    available: bool


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        # FL3XX payloads are generally epoch milliseconds.
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


def parse_roster_payload(raw_text: str) -> List[Dict[str, Any]]:
    """Parse roster export text into a list of roster rows."""

    text = raw_text.strip()
    if not text:
        return []

    candidates = [text]
    if text.startswith('"') and text.endswith('"'):
        candidates.append(text[1:-1])

    last_error: Optional[Exception] = None
    for candidate in candidates:
        for decoder in (
            lambda s: s,
            lambda s: bytes(s, "utf-8").decode("unicode_escape"),
        ):
            payload_text = decoder(candidate).strip()
            try:
                payload = json.loads(payload_text)
            except Exception as exc:  # noqa: BLE001 - preserve parse attempts
                last_error = exc
                continue

            if isinstance(payload, list):
                return [row for row in payload if isinstance(row, Mapping)]
            if isinstance(payload, Mapping):
                for key in ("items", "data", "results", "rows", "roster", "staff"):
                    nested = payload.get(key)
                    if isinstance(nested, list):
                        return [row for row in nested if isinstance(row, Mapping)]
                if any(key in payload for key in ("user", "entries", "flights")):
                    return [dict(payload)]

    if last_error is not None:
        raise ValueError(f"Unable to parse roster payload as JSON: {last_error}")
    raise ValueError("Unable to parse roster payload as JSON")


def filter_active_roster_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Drop rows where both ``entries`` and ``flights`` are empty lists."""

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        entries = row.get("entries")
        flights = row.get("flights")
        has_entries = isinstance(entries, list) and len(entries) > 0
        has_flights = isinstance(flights, list) and len(flights) > 0
        if has_entries or has_flights:
            filtered.append(dict(row))
    return filtered


def _user_value(user: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = user.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_event(entry: Mapping[str, Any], source: str) -> Dict[str, Any]:
    start = _to_datetime(entry.get("from") or entry.get("departureTime") or entry.get("start") or entry.get("out"))
    end = _to_datetime(entry.get("to") or entry.get("arrivalTime") or entry.get("end") or entry.get("in"))
    return {
        "source": source,
        "type": str(entry.get("type") or entry.get("eventType") or "").strip().upper() or None,
        "label": str(entry.get("name") or entry.get("notes") or entry.get("status") or "").strip() or None,
        "start": start,
        "end": end,
        "from_airport": str(entry.get("fromAirport") or entry.get("departureAirport") or "").strip().upper() or None,
        "to_airport": str(entry.get("toAirport") or entry.get("arrivalAirport") or "").strip().upper() or None,
        "aircraft": str(entry.get("aircraftType") or entry.get("aircraft") or "").strip().upper() or None,
    }


def _build_events(row: Mapping[str, Any]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    entries = row.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, Mapping):
                events.append(_extract_event(entry, "entry"))

    flights = row.get("flights")
    if isinstance(flights, list):
        for flight in flights:
            if isinstance(flight, Mapping):
                events.append(_extract_event(flight, "flight"))

    events.sort(key=lambda event: event.get("start") or datetime.min.replace(tzinfo=UTC))
    return events


def build_crew_snapshots(rows: Iterable[Mapping[str, Any]], at_time: datetime) -> List[CrewSnapshot]:
    """Build current-state snapshots for each crew member at ``at_time``."""

    reference = at_time.astimezone(UTC) if at_time.tzinfo else at_time.replace(tzinfo=UTC)
    snapshots: List[CrewSnapshot] = []

    for row in rows:
        user = row.get("user") if isinstance(row.get("user"), Mapping) else {}
        events = _build_events(row)
        if not events:
            continue

        name = " ".join(
            part
            for part in (
                _user_value(user, "firstName"),
                _user_value(user, "lastName"),
            )
            if part
        ).strip()
        if not name:
            name = _user_value(user, "logName", "name", "email", "personnelNumber")

        personnel = _user_value(user, "personnelNumber", "id")
        trigram = _user_value(user, "trigram")

        active: Optional[Dict[str, Any]] = None
        latest: Optional[Dict[str, Any]] = None
        next_event: Optional[Dict[str, Any]] = None
        for event in events:
            start = event.get("start")
            end = event.get("end")
            if isinstance(start, datetime) and start <= reference:
                latest = event
            if isinstance(start, datetime) and start > reference and next_event is None:
                next_event = event
            if isinstance(start, datetime) and isinstance(end, datetime) and start <= reference <= end:
                active = event

        chosen = active or latest or next_event
        if chosen is None:
            continue

        event_type = str(chosen.get("type") or "").upper() if chosen.get("type") else None
        unavailable_types = {"A", "A-DAY", "ADAY", "P", "POSITIONING", "F", "FLIGHT", "DUTY"}
        available = active is None and (event_type not in unavailable_types)

        current_airport = chosen.get("to_airport") or chosen.get("from_airport")
        if active and chosen.get("from_airport"):
            current_airport = chosen.get("from_airport")

        snapshots.append(
            CrewSnapshot(
                personnel_number=personnel,
                name=name,
                trigram=trigram,
                active_event_type=event_type,
                active_event_label=chosen.get("label"),
                current_airport=current_airport,
                next_airport=next_event.get("to_airport") if isinstance(next_event, Mapping) else None,
                event_start_utc=chosen.get("start"),
                event_end_utc=chosen.get("end"),
                event_aircraft=chosen.get("aircraft"),
                available=available,
            )
        )

    snapshots.sort(key=lambda snapshot: (snapshot.current_airport or "", snapshot.name))
    return snapshots
