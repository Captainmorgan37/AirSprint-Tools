"""Flight Following duty snapshot collection utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from fl3xx_api import Fl3xxApiConfig, fetch_flights, fetch_postflight
from flight_leg_utils import compute_mountain_day_window_utc, safe_parse_dt

UTC = timezone.utc


@dataclass
class DutyStartPilotSnapshot:
    """Detailed duty information for a single pilot at the start of a duty."""

    seat: str
    name: str
    person_id: Optional[str] = None
    crew_member_id: Optional[str] = None
    personnel_number: Optional[str] = None
    log_name: Optional[str] = None
    email: Optional[str] = None
    trigram: Optional[str] = None
    full_duty_state: Dict[str, Any] = field(default_factory=dict)
    explainer_map: Dict[str, Any] = field(default_factory=dict)
    rest_payload: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    fdp_actual_min: Optional[int] = None
    fdp_max_min: Optional[int] = None
    fdp_actual_str: Optional[str] = None
    split_duty: bool = False
    split_break_str: Optional[str] = None
    rest_after_min: Optional[int] = None
    rest_after_str: Optional[str] = None


@dataclass
class DutyStartSnapshot:
    """Snapshot representing the beginning of a duty sequence for a tail."""

    tail: str
    flight_id: Any
    block_off_est_utc: Optional[datetime]
    pilots: List[DutyStartPilotSnapshot] = field(default_factory=list)
    flight_payload: Dict[str, Any] = field(default_factory=dict)
    postflight_payload: Any = None

    def crew_signature(self) -> Tuple[Tuple[str, str], ...]:
        """Return a stable crew signature derived from PIC/SIC identifiers."""

        entries: List[Tuple[str, str]] = []
        for pilot in self.pilots:
            identifier = _select_identifier(pilot)
            seat = (pilot.seat or "PIC").upper()
            if identifier:
                entries.append((seat, identifier))
        if not entries:
            entries.append(("LEG", str(self.flight_id)))
        entries.sort()
        return tuple(entries)


@dataclass
class DutyStartCollection:
    """Combined output for a day's worth of duty start snapshots."""

    target_date: date
    start_utc: datetime
    end_utc: datetime
    snapshots: List[DutyStartSnapshot] = field(default_factory=list)
    flights_metadata: Dict[str, Any] = field(default_factory=dict)
    grouped_flights: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)


def collect_duty_start_snapshots(
    config: Fl3xxApiConfig,
    target_date: date,
    *,
    flights: Optional[Iterable[Dict[str, Any]]] = None,
    postflight_fetcher: Optional[Callable[[Fl3xxApiConfig, Any], Any]] = None,
) -> DutyStartCollection:
    """Return duty snapshots for each crew duty start on the target date."""

    if isinstance(target_date, datetime):
        target_date = target_date.astimezone(UTC).date()

    start_utc, end_utc = compute_mountain_day_window_utc(target_date)

    flights_metadata: Dict[str, Any] = {}
    if flights is None:
        fetched_flights, flights_metadata = fetch_flights(
            config,
            from_date=target_date,
            to_date=target_date + timedelta(days=1),
        )
    else:
        fetched_flights = list(flights)

    grouped = _group_flights_by_tail(fetched_flights, start_utc, end_utc)

    fetcher = postflight_fetcher or fetch_postflight
    snapshots: List[DutyStartSnapshot] = []

    for tail, tail_flights in grouped.items():
        last_signature: Optional[Tuple[Tuple[str, str], ...]] = None

        for flight_info in tail_flights:
            flight_id = flight_info.get("flight_id")
            if flight_id is None:
                continue

            postflight_payload = fetcher(config, flight_id)
            snapshot = _build_snapshot_from_postflight(
                postflight_payload,
                tail=tail,
                flight_payload=flight_info.get("flight_payload", {}),
                flight_id=flight_id,
                block_off_est_utc=flight_info.get("block_off_est_utc"),
            )

            signature = snapshot.crew_signature()
            if last_signature is not None and signature == last_signature:
                continue

            snapshots.append(snapshot)
            last_signature = signature

    return DutyStartCollection(
        target_date=target_date,
        start_utc=start_utc,
        end_utc=end_utc,
        snapshots=snapshots,
        flights_metadata=flights_metadata,
        grouped_flights=grouped,
    )


def _group_flights_by_tail(
    flights: Iterable[Dict[str, Any]],
    start_utc: datetime,
    end_utc: datetime,
) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for flight in flights:
        if not isinstance(flight, MutableMapping):
            continue

        tail = _clean_str(
            flight.get("registrationNumber")
            or flight.get("tailNumber")
            or flight.get("aircraftRegistration")
            or flight.get("aircraft")
        )
        if not tail:
            continue

        block_off_raw = _extract_first(
            flight,
            "blockOffEstUTC",
            "blockOffUtc",
            "blockOffActualUTC",
            "scheduledOffBlockUtc",
            "blockOffTimeUtc",
            "departureTimeUtc",
            "scheduledDepartureUtc",
            "scheduledDepartureTime",
            "scheduledDeparture",
        )
        if not block_off_raw:
            continue
        try:
            block_off_dt = safe_parse_dt(str(block_off_raw))
        except Exception:
            continue
        if block_off_dt.tzinfo is None:
            block_off_dt = block_off_dt.replace(tzinfo=UTC)
        else:
            block_off_dt = block_off_dt.astimezone(UTC)
        if block_off_dt < start_utc or block_off_dt >= end_utc:
            continue

        flight_id = _extract_first(
            flight,
            "flightId",
            "id",
            "flight_id",
            "legId",
            "quoteId",
            "uuid",
            "externalId",
        )
        if flight_id is None:
            continue

        entry = {
            "flight_id": flight_id,
            "block_off_est_utc": block_off_dt,
            "flight_payload": dict(flight),
        }
        grouped.setdefault(tail, []).append(entry)

    for tail, tail_flights in grouped.items():
        tail_flights.sort(key=lambda item: item["block_off_est_utc"] or datetime.max.replace(tzinfo=UTC))

    return grouped


def _build_snapshot_from_postflight(
    postflight_payload: Any,
    *,
    tail: str,
    flight_payload: Optional[Mapping[str, Any]],
    flight_id: Any,
    block_off_est_utc: Optional[datetime],
) -> DutyStartSnapshot:
    resolved_tail = tail
    if isinstance(postflight_payload, MutableMapping):
        maybe_tail = _clean_str(
            postflight_payload.get("tailNumber")
            or postflight_payload.get("registrationNumber")
            or postflight_payload.get("tail")
        )
        if maybe_tail:
            resolved_tail = maybe_tail

    pilots = _parse_pilot_blocks(postflight_payload)
    snapshot = DutyStartSnapshot(
        tail=resolved_tail,
        flight_id=flight_id,
        block_off_est_utc=block_off_est_utc,
        pilots=pilots,
        flight_payload=dict(flight_payload or {}),
        postflight_payload=postflight_payload,
    )
    return snapshot


def _parse_pilot_blocks(postflight_payload: Any) -> List[DutyStartPilotSnapshot]:
    if isinstance(postflight_payload, MutableMapping):
        dtls2 = postflight_payload.get("dtls2")
    else:
        dtls2 = None

    if not isinstance(dtls2, list):
        return []

    pilots: List[DutyStartPilotSnapshot] = []
    for pilot_block in dtls2:
        if not isinstance(pilot_block, MutableMapping):
            continue

        seat = _normalise_seat(pilot_block.get("pilotRole") or pilot_block.get("role"))
        name = _derive_name(pilot_block)

        full_duty_state = _clone_mapping(pilot_block.get("fullDutyState"))
        explainer_map = _clone_mapping(full_duty_state.get("explainerMap")) if full_duty_state else {}
        rest_payload = _clone_mapping(pilot_block.get("restAfterDuty"))

        fdp_info = _clone_mapping(full_duty_state.get("fdp")) if full_duty_state else {}
        fdp_actual_min = _coerce_minutes(fdp_info.get("actual")) if fdp_info else None
        fdp_max_min = _coerce_minutes(fdp_info.get("max")) if fdp_info else None

        fdp_actual_str = _extract_fdp_actual_str(explainer_map)
        split_break_str = _extract_break_str(explainer_map)

        split_duty = False
        if pilot_block.get("splitDutyStart") is True or pilot_block.get("splitDutyType"):
            split_duty = True
        if isinstance(full_duty_state, dict):
            if full_duty_state.get("splitDutyStart") is True or full_duty_state.get("splitDutyType"):
                split_duty = True

        rest_after_min = _extract_rest_minutes(rest_payload, full_duty_state)
        rest_after_str = _minutes_to_hhmm(rest_after_min)

        pilot_snapshot = DutyStartPilotSnapshot(
            seat=seat,
            name=name,
            person_id=_clean_str(
                pilot_block.get("personId")
                or pilot_block.get("personnelId")
                or pilot_block.get("crewPersonId")
            ),
            crew_member_id=_clean_str(pilot_block.get("crewMemberId") or pilot_block.get("crewId")),
            personnel_number=_clean_str(pilot_block.get("personnelNumber")),
            log_name=_clean_str(pilot_block.get("logName")),
            email=_clean_str(pilot_block.get("email")),
            trigram=_clean_str(pilot_block.get("trigram")),
            full_duty_state=full_duty_state,
            explainer_map=explainer_map,
            rest_payload=rest_payload,
            raw_payload=dict(pilot_block),
            fdp_actual_min=fdp_actual_min,
            fdp_max_min=fdp_max_min,
            fdp_actual_str=fdp_actual_str,
            split_duty=split_duty,
            split_break_str=split_break_str,
            rest_after_min=rest_after_min,
            rest_after_str=rest_after_str,
        )
        pilots.append(pilot_snapshot)

    return pilots


def _select_identifier(pilot: DutyStartPilotSnapshot) -> Optional[str]:
    for attribute in (
        pilot.person_id,
        pilot.crew_member_id,
        pilot.personnel_number,
        pilot.log_name,
        pilot.email,
        pilot.trigram,
        pilot.name,
    ):
        if attribute:
            candidate = attribute.strip()
            if candidate:
                return candidate
    return None


def _normalise_seat(role: Any) -> str:
    role_str = _clean_str(role).upper()
    if role_str in {"CMD", "PIC", "CAPT", "CAPTAIN"}:
        return "PIC"
    if role_str in {"FO", "SIC", "FIRST OFFICER"}:
        return "SIC"
    return role_str or "PIC"


def _derive_name(pilot_block: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for key in ("firstName", "middleName", "lastName"):
        value = _clean_str(pilot_block.get(key))
        if value:
            parts.append(value)
    if parts:
        return " ".join(parts)
    for fallback in ("logName", "email", "personnelNumber", "trigram"):
        value = _clean_str(pilot_block.get(fallback))
        if value:
            return value
    return ""


def _clean_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _clone_mapping(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, Mapping):
        return dict(obj)
    return {}


def _coerce_minutes(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)
    try:
        if value is not None:
            parsed = int(value)
            return parsed
    except (TypeError, ValueError):
        pass
    return None


def _minutes_to_hhmm(total_min: Optional[int]) -> Optional[str]:
    if total_min is None:
        return None
    if total_min < 0:
        return None
    hours, minutes = divmod(total_min, 60)
    return f"{hours}:{minutes:02d}"


def _extract_rest_minutes(
    rest_payload: Optional[Mapping[str, Any]],
    full_duty_state: Optional[Mapping[str, Any]],
) -> Optional[int]:
    if isinstance(rest_payload, Mapping):
        actual = rest_payload.get("actual")
        minutes = _coerce_minutes(actual)
        if minutes is not None:
            return minutes
    if isinstance(full_duty_state, Mapping):
        rest_block = full_duty_state.get("restAfterDuty")
        if isinstance(rest_block, Mapping):
            actual = rest_block.get("actual")
            minutes = _coerce_minutes(actual)
            if minutes is not None:
                return minutes
    return None


def _parse_actual_fdp_details(
    explainer_map: Mapping[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """Return the duty length and break duration strings from ACTUAL_FDP."""

    if not isinstance(explainer_map, Mapping):
        return None, None
    actual = explainer_map.get("ACTUAL_FDP")
    if not isinstance(actual, Mapping):
        return None, None

    duty_str: Optional[str] = None
    header = actual.get("header")
    if isinstance(header, str):
        header = header.strip()
        if "=" in header:
            header = header.split("=", 1)[1].strip()
        if header:
            duty_str = header

    break_str: Optional[str] = None
    text_lines = actual.get("text")
    if isinstance(text_lines, Iterable):
        for line in text_lines:
            if not isinstance(line, str):
                continue
            lower_line = line.strip().lower()
            if not lower_line.startswith("break"):
                continue
            parts = line.split("=", 1)
            candidate = parts[1].strip() if len(parts) == 2 else line.strip()
            if candidate:
                break_str = candidate
                break

    return duty_str, break_str


def _extract_break_str(explainer_map: Mapping[str, Any]) -> Optional[str]:
    _, break_str = _parse_actual_fdp_details(explainer_map)
    return break_str


def _extract_fdp_actual_str(explainer_map: Mapping[str, Any]) -> Optional[str]:
    duty_str, _ = _parse_actual_fdp_details(explainer_map)
    return duty_str


def _extract_first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def summarize_long_duty_days(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
) -> List[str]:
    """Return formatted lines for the Split Duty section."""

    if isinstance(collection, DutyStartCollection):
        snapshots: Iterable[DutyStartSnapshot] = collection.snapshots
    else:
        snapshots = collection

    lines: List[str] = []

    for snapshot in snapshots:
        tail = snapshot.tail or "UNKNOWN"
        split_pilots = [pilot for pilot in snapshot.pilots if pilot.split_duty]
        if not split_pilots:
            continue

        seats = sorted({(pilot.seat or "PIC").upper() for pilot in split_pilots if pilot.seat})
        seats_display = "/".join(seats) if seats else "CREW"

        duty_display: Optional[str] = None
        break_display: Optional[str] = None

        for pilot in split_pilots:
            duty_candidate, break_candidate = _parse_actual_fdp_details(pilot.explainer_map)
            if duty_candidate and not duty_display:
                duty_display = duty_candidate
            if break_candidate and not break_display:
                break_display = break_candidate
            if duty_display and break_display:
                break

        if duty_display is None:
            for pilot in split_pilots:
                if pilot.fdp_actual_str:
                    duty_display = pilot.fdp_actual_str
                    break
                fallback = _minutes_to_hhmm(pilot.fdp_actual_min)
                if fallback:
                    duty_display = fallback
                    break

        if break_display is None:
            for pilot in split_pilots:
                if pilot.split_break_str:
                    break_display = pilot.split_break_str
                    break

        duty_display = duty_display or "Unknown"
        break_display = break_display or "Unknown"

        line = f"{tail} – Duty {duty_display} Break {break_display} ({seats_display})"
        lines.append(line)

    return lines


__all__ = [
    "DutyStartPilotSnapshot",
    "DutyStartSnapshot",
    "DutyStartCollection",
    "collect_duty_start_snapshots",
    "summarize_long_duty_days",
]
