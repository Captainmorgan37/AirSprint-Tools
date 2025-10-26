"""Flight Following duty snapshot collection utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)

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
    rest_after_payload: Dict[str, Any] = field(default_factory=dict)
    rest_before_payload: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    fdp_actual_min: Optional[int] = None
    fdp_max_min: Optional[int] = None
    fdp_actual_str: Optional[str] = None
    split_duty: bool = False
    split_break_str: Optional[str] = None
    rest_after_actual_min: Optional[int] = None
    rest_after_required_min: Optional[int] = None
    rest_after_actual_str: Optional[str] = None
    rest_before_actual_min: Optional[int] = None
    rest_before_required_min: Optional[int] = None
    rest_before_actual_str: Optional[str] = None


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
    ingestion_diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FlightFollowingReportSection:
    """Structured representation of a single Flight Following section."""

    title: str
    lines: List[str] = field(default_factory=list)

    def normalized_lines(self) -> List[str]:
        """Return sanitized lines with whitespace removed."""

        normalized: List[str] = []
        for line in self.lines:
            if line is None:
                continue
            if not isinstance(line, str):
                line = str(line)
            trimmed = line.strip()
            if trimmed:
                normalized.append(trimmed)
        return normalized

    @property
    def text(self) -> str:
        """Return the section body text, falling back to ``"None"``."""

        normalized = self.normalized_lines()
        if not normalized:
            return "None"
        return "\n".join(normalized)

    def render(self) -> str:
        """Return the formatted section with its header."""

        header = (self.title or "").strip()
        if not header:
            return self.text
        return f"{header}\n{self.text}"


@dataclass
class FlightFollowingReport:
    """Aggregated Flight Following report with text payload rendering."""

    target_date: date
    generated_at: datetime
    sections: List[FlightFollowingReportSection] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def text_payload(self) -> str:
        """Return a Markdown/plain text payload combining all sections."""

        generated_at_utc = self.generated_at.astimezone(UTC)
        header_lines = [
            f"Flight Following Duty Report – {self.target_date.isoformat()}",
            f"Generated at {generated_at_utc.strftime('%Y-%m-%d %H:%M %Z')}",
        ]

        lines: List[str] = header_lines + [""]

        for section in self.sections:
            rendered = section.render()
            if rendered:
                lines.append(rendered)
                lines.append("")

        while lines and lines[-1] == "":
            lines.pop()

        return "\n".join(lines)

    def message_payload(self) -> Dict[str, Any]:
        """Return a message-friendly payload for Flight Following consumers."""

        generated_at_utc = self.generated_at.astimezone(UTC)
        sections_payload = [
            {
                "title": section.title,
                "lines": section.normalized_lines() or ["None"],
                "text": section.text,
            }
            for section in self.sections
        ]

        payload: Dict[str, Any] = {
            "target_date": self.target_date.isoformat(),
            "generated_at": generated_at_utc.isoformat(),
            "sections": sections_payload,
            "text": self.text_payload(),
            "metadata": dict(self.metadata),
        }
        return payload


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

    grouped, ingestion_diagnostics = _group_flights_by_tail(
        fetched_flights, start_utc, end_utc
    )

    fetcher = postflight_fetcher or fetch_postflight
    snapshots: List[DutyStartSnapshot] = []

    for tail, tail_flights in grouped.items():
        last_signature: Optional[Tuple[Tuple[str, str], ...]] = None
        last_snapshot: Optional[DutyStartSnapshot] = None

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
                if last_snapshot is not None:
                    _merge_split_duty_information(last_snapshot, snapshot)
                continue

            snapshots.append(snapshot)
            last_signature = signature
            last_snapshot = snapshot

    return DutyStartCollection(
        target_date=target_date,
        start_utc=start_utc,
        end_utc=end_utc,
        snapshots=snapshots,
        flights_metadata=flights_metadata,
        grouped_flights=grouped,
        ingestion_diagnostics=ingestion_diagnostics,
    )


def summarize_collection_for_display(collection: DutyStartCollection) -> Dict[str, Any]:
    """Return a structured summary highlighting duty and crew diagnostics."""

    tails = sorted(collection.grouped_flights.keys()) if collection.grouped_flights else []
    flights_metadata = {
        str(key): value for key, value in (collection.flights_metadata or {}).items()
    }

    summary: Dict[str, Any] = {
        "target_date": collection.target_date.isoformat(),
        "window_start_utc": collection.start_utc.isoformat(),
        "window_end_utc": collection.end_utc.isoformat(),
        "duty_start_snapshots": len(collection.snapshots),
        "tails_processed": len(tails),
        "tails": tails,
        "flights_metadata": flights_metadata,
    }

    ingestion = collection.ingestion_diagnostics or {}
    if ingestion:
        summary["ingestion_diagnostics"] = _normalize_ingestion_diagnostics(ingestion)
    else:
        summary["ingestion_diagnostics"] = {}

    summary["crew_summary"] = _summarize_snapshots_for_metadata(collection.snapshots)

    return summary


def build_rest_before_index(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
) -> Dict[str, Dict[str, Any]]:
    """Return a mapping of pilot identifiers to next-day rest-before metrics."""

    if isinstance(collection, DutyStartCollection):
        snapshots: Iterable[DutyStartSnapshot] = collection.snapshots
    else:
        snapshots = collection

    index: Dict[str, Dict[str, Any]] = {}

    for snapshot in snapshots:
        for pilot in snapshot.pilots:
            identifier = _select_identifier(pilot)
            if not identifier:
                continue

            entry = index.setdefault(identifier, {"pilot": pilot})

            if entry.get("rest_before_actual_min") is None:
                rest_actual = pilot.rest_before_actual_min
                if rest_actual is None and pilot.rest_before_payload:
                    rest_actual = _coerce_minutes(
                        pilot.rest_before_payload.get("actual")
                    )
                if rest_actual is not None:
                    entry["rest_before_actual_min"] = rest_actual

            if entry.get("rest_before_required_min") is None:
                rest_required = pilot.rest_before_required_min
                if rest_required is None and pilot.rest_before_payload:
                    rest_required = _coerce_minutes(
                        pilot.rest_before_payload.get("min")
                    )
                if rest_required is not None:
                    entry["rest_before_required_min"] = rest_required

    return index


def _group_flights_by_tail(
    flights: Iterable[Dict[str, Any]],
    start_utc: datetime,
    end_utc: datetime,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    diagnostics = _init_ingestion_diagnostics()

    for flight in flights:
        diagnostics["total_flights"] += 1

        if not isinstance(flight, MutableMapping):
            _record_skip(diagnostics, "non_mapping", flight)
            continue

        tail = _clean_str(
            flight.get("registrationNumber")
            or flight.get("tailNumber")
            or flight.get("aircraftRegistration")
            or flight.get("aircraft")
        )
        if not tail:
            _record_skip(diagnostics, "missing_tail", flight)
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
            _record_skip(diagnostics, "missing_block_off", flight)
            continue
        try:
            block_off_dt = safe_parse_dt(str(block_off_raw))
        except Exception:
            _record_skip(diagnostics, "invalid_block_off", flight, extra={"value": block_off_raw})
            continue
        if block_off_dt.tzinfo is None:
            block_off_dt = block_off_dt.replace(tzinfo=UTC)
        else:
            block_off_dt = block_off_dt.astimezone(UTC)
        if block_off_dt < start_utc or block_off_dt >= end_utc:
            _record_skip(
                diagnostics,
                "outside_window",
                flight,
                extra={
                    "block_off_est_utc": block_off_dt.isoformat(),
                },
            )
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
            _record_skip(diagnostics, "missing_flight_id", flight)
            continue

        entry = {
            "flight_id": flight_id,
            "block_off_est_utc": block_off_dt,
            "flight_payload": dict(flight),
        }
        grouped.setdefault(tail, []).append(entry)
        diagnostics["accepted_flights"] += 1
        diagnostics.setdefault("tails", {}).setdefault(tail, 0)
        diagnostics["tails"][tail] += 1

    for tail, tail_flights in grouped.items():
        tail_flights.sort(key=lambda item: item["block_off_est_utc"] or datetime.max.replace(tzinfo=UTC))

    return grouped, diagnostics


def _init_ingestion_diagnostics() -> Dict[str, Any]:
    return {
        "total_flights": 0,
        "accepted_flights": 0,
        "tails": {},
        "skipped": {},
    }


def _record_skip(
    diagnostics: Dict[str, Any],
    reason: str,
    flight: Any,
    *,
    extra: Optional[Dict[str, Any]] = None,
    sample_limit: int = 5,
) -> None:
    skipped = diagnostics.setdefault("skipped", {})
    bucket = skipped.setdefault(reason, {"count": 0, "samples": []})
    bucket["count"] += 1
    samples: List[Any] = bucket["samples"]
    if len(samples) >= sample_limit:
        return
    samples.append(_summarize_flight_sample(flight, extra=extra))


def _summarize_flight_sample(
    flight: Any,
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    sample: Dict[str, Any] = {}
    if isinstance(flight, MutableMapping):
        keys_to_capture = (
            "flightId",
            "id",
            "legId",
            "registrationNumber",
            "tailNumber",
            "blockOffEstUTC",
            "blockOffUtc",
            "blockOffActualUTC",
        )
        for key in keys_to_capture:
            if key in flight:
                sample[key] = flight.get(key)
    else:
        sample["repr"] = repr(flight)

    if extra:
        sample.update(extra)

    return sample


def _normalize_ingestion_diagnostics(data: Mapping[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            normalized[key] = _normalize_ingestion_diagnostics(value)
        elif isinstance(value, list):
            normalized[key] = [
                _normalize_ingestion_diagnostics(item)
                if isinstance(item, Mapping)
                else item
                for item in value
            ]
        else:
            normalized[key] = value
    return normalized


def _summarize_snapshots_for_metadata(
    snapshots: Iterable[DutyStartSnapshot],
) -> Dict[str, Any]:
    total_snapshots = 0
    signature_map: Dict[Tuple[Tuple[str, str], ...], Dict[str, Any]] = {}
    pilot_map: Dict[str, Dict[str, Any]] = {}

    for snapshot in snapshots:
        total_snapshots += 1

        signature = snapshot.crew_signature()
        signature_entry = signature_map.setdefault(
            signature,
            {
                "signature": _format_signature_display(signature),
                "count": 0,
                "tails": set(),
                "sample_duties": [],
            },
        )
        signature_entry["count"] += 1
        signature_entry["tails"].add(snapshot.tail or "UNKNOWN")
        if len(signature_entry["sample_duties"]) < 3:
            signature_entry["sample_duties"].append(
                {
                    "tail": snapshot.tail or "UNKNOWN",
                    "flight_id": snapshot.flight_id,
                    "crew": _snapshot_crew_display(snapshot),
                }
            )

        for pilot in snapshot.pilots:
            identifier_value = _select_identifier(pilot)
            primary_identifier = identifier_value or pilot.name or "UNKNOWN"
            pilot_entry = pilot_map.setdefault(
                primary_identifier,
                {
                    "identifier": identifier_value,
                    "name": pilot.name or primary_identifier,
                    "count": 0,
                    "seats": set(),
                },
            )
            pilot_entry["count"] += 1
            pilot_entry["seats"].add(_normalise_seat(pilot.seat))

    crews = []
    for entry in signature_map.values():
        crews.append(
            {
                "signature": entry["signature"],
                "count": entry["count"],
                "tails": sorted(entry["tails"]),
                "sample_duties": entry["sample_duties"],
            }
        )
    crews.sort(key=lambda item: (-item["count"], item["signature"]))

    pilots = []
    for entry in pilot_map.values():
        seats = sorted(entry["seats"], key=_seat_sort_key)
        pilots.append(
            {
                "identifier": entry["identifier"],
                "name": entry["name"],
                "count": entry["count"],
                "seats": seats,
            }
        )
    pilots.sort(
        key=lambda item: (
            -item["count"],
            item["name"] or "",
            item["identifier"] or "",
        )
    )

    return {
        "total_snapshots": total_snapshots,
        "unique_crews": len(signature_map),
        "unique_pilots": len(pilot_map),
        "crews": crews,
        "pilots": pilots,
    }


def _format_signature_display(signature: Tuple[Tuple[str, str], ...]) -> str:
    if not signature:
        return "Unknown crew"
    parts = []
    for seat, identifier in signature:
        seat_display = (seat or "PIC").upper()
        parts.append(f"{seat_display}: {identifier}")
    return " + ".join(parts)


def _snapshot_crew_display(snapshot: DutyStartSnapshot) -> List[Dict[str, Any]]:
    crew: List[Dict[str, Any]] = []
    for pilot in snapshot.pilots:
        crew.append(
            {
                "seat": _normalise_seat(pilot.seat),
                "name": pilot.name,
                "identifier": _select_identifier(pilot),
            }
        )
    return crew


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
    if not isinstance(postflight_payload, MutableMapping):
        return []

    time_block = postflight_payload.get("time")
    if not isinstance(time_block, Mapping):
        time_block = {}

    dtls2 = time_block.get("dtls2")
    if not isinstance(dtls2, list):
        dtls2 = postflight_payload.get("dtls2")
    if not isinstance(dtls2, list):
        dtls2 = []

    pilots: List[DutyStartPilotSnapshot] = []
    for pilot_block in dtls2:
        snapshot = _pilot_snapshot_from_block(pilot_block)
        if snapshot:
            pilots.append(snapshot)

    if not pilots and time_block:
        for key, seat in (("cmd", "PIC"), ("fo", "SIC")):
            snapshot = _pilot_snapshot_from_block(time_block.get(key), default_seat=seat)
            if snapshot:
                pilots.append(snapshot)

    if not pilots:
        deice_block = postflight_payload.get("deice")
        if isinstance(deice_block, Mapping):
            crew_list = deice_block.get("crew")
            if isinstance(crew_list, list):
                for index, member in enumerate(crew_list):
                    default_seat = "PIC" if index == 0 else "SIC"
                    seat_hint = member.get("jobTitle") or member.get("role")
                    snapshot = _pilot_snapshot_from_block(
                        member,
                        default_seat=_normalise_seat(seat_hint or default_seat),
                    )
                    if snapshot:
                        pilots.append(snapshot)

    return pilots


def _merge_split_duty_information(
    target_snapshot: Optional[DutyStartSnapshot],
    source_snapshot: Optional[DutyStartSnapshot],
) -> None:
    """Propagate split duty metadata from ``source_snapshot`` to ``target_snapshot``."""

    if not target_snapshot or not source_snapshot:
        return

    source_pilots = list(source_snapshot.pilots or [])
    if not source_pilots:
        return

    for pilot in target_snapshot.pilots or []:
        match = _find_matching_pilot(pilot, source_pilots)
        if not match:
            continue

        if match.split_duty and not pilot.split_duty:
            pilot.split_duty = True

        if match.split_break_str and match.split_break_str != pilot.split_break_str:
            pilot.split_break_str = match.split_break_str

        if match.rest_after_actual_min and not pilot.rest_after_actual_min:
            pilot.rest_after_actual_min = match.rest_after_actual_min
            pilot.rest_after_actual_str = match.rest_after_actual_str

        if match.rest_after_required_min and not pilot.rest_after_required_min:
            pilot.rest_after_required_min = match.rest_after_required_min

        if match.rest_before_actual_min and not pilot.rest_before_actual_min:
            pilot.rest_before_actual_min = match.rest_before_actual_min
            pilot.rest_before_actual_str = match.rest_before_actual_str

        if match.rest_before_required_min and not pilot.rest_before_required_min:
            pilot.rest_before_required_min = match.rest_before_required_min


def _find_matching_pilot(
    pilot: DutyStartPilotSnapshot,
    candidates: Sequence[DutyStartPilotSnapshot],
) -> Optional[DutyStartPilotSnapshot]:
    identifier = _select_identifier(pilot)
    if identifier:
        for candidate in candidates:
            if _select_identifier(candidate) == identifier:
                return candidate

    seat = _normalise_seat(pilot.seat)
    name = (pilot.name or "").strip().lower()

    for candidate in candidates:
        candidate_seat = _normalise_seat(candidate.seat)
        candidate_name = (candidate.name or "").strip().lower()
        if candidate_seat == seat and candidate_name == name:
            return candidate

    for candidate in candidates:
        if _normalise_seat(candidate.seat) == seat:
            return candidate

    return None


def _pilot_snapshot_from_block(
    pilot_block: Any,
    *,
    default_seat: str = "PIC",
) -> Optional[DutyStartPilotSnapshot]:
    if not isinstance(pilot_block, Mapping):
        return None

    seat = _normalise_seat(pilot_block.get("pilotRole") or pilot_block.get("role") or default_seat)
    name = _derive_name(pilot_block)

    full_duty_state = _clone_mapping(pilot_block.get("fullDutyState"))
    explainer_map = _clone_mapping(full_duty_state.get("explainerMap")) if full_duty_state else {}
    if not explainer_map:
        explainer_map = _clone_mapping(pilot_block.get("explainerMap"))
    rest_after_payload = _clone_mapping(pilot_block.get("restAfterDuty"))
    rest_before_payload = _clone_mapping(pilot_block.get("restBeforeDuty"))

    fdp_info = _clone_mapping(full_duty_state.get("fdp")) if full_duty_state else {}
    if not fdp_info:
        fdp_info = _clone_mapping(pilot_block.get("fdp"))
    fdp_actual_min = _coerce_minutes(fdp_info.get("actual")) if fdp_info else None
    fdp_max_min = _coerce_minutes(fdp_info.get("max")) if fdp_info else None

    fdp_actual_str = _extract_fdp_actual_str(explainer_map)
    split_break_str = _extract_break_str(explainer_map)

    split_duty = False
    for candidate in (pilot_block, full_duty_state):
        if isinstance(candidate, Mapping):
            if _is_truthy(candidate.get("splitDutyStart")) or candidate.get("splitDutyType"):
                split_duty = True
                break

    rest_after_actual_min, rest_after_required_min = _extract_rest_components(
        rest_after_payload, full_duty_state, "restAfterDuty"
    )
    rest_after_str = _minutes_to_hhmm(rest_after_actual_min)

    rest_before_actual_min, rest_before_required_min = _extract_rest_components(
        rest_before_payload, full_duty_state, "restBeforeDuty"
    )
    rest_before_str = _minutes_to_hhmm(rest_before_actual_min)

    person_identifier = _clean_str(
        _extract_first(
            pilot_block,
            "personId",
            "personnelId",
            "crewPersonId",
            "userId",
            "id",
        )
    )

    pilot_snapshot = DutyStartPilotSnapshot(
        seat=seat,
        name=name,
        person_id=person_identifier,
        crew_member_id=_clean_str(_extract_first(pilot_block, "crewMemberId", "crewId")),
        personnel_number=_clean_str(pilot_block.get("personnelNumber")),
        log_name=_clean_str(pilot_block.get("logName")),
        email=_clean_str(pilot_block.get("email")),
        trigram=_clean_str(pilot_block.get("trigram")),
        full_duty_state=full_duty_state,
        explainer_map=explainer_map,
        rest_after_payload=rest_after_payload,
        rest_before_payload=rest_before_payload,
        raw_payload=dict(pilot_block),
        fdp_actual_min=fdp_actual_min,
        fdp_max_min=fdp_max_min,
        fdp_actual_str=fdp_actual_str,
        split_duty=split_duty,
        split_break_str=split_break_str,
        rest_after_actual_min=rest_after_actual_min,
        rest_after_required_min=rest_after_required_min,
        rest_after_actual_str=rest_after_str,
        rest_before_actual_min=rest_before_actual_min,
        rest_before_required_min=rest_before_required_min,
        rest_before_actual_str=rest_before_str,
    )
    return pilot_snapshot


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


_ISO8601_DURATION_RE = re.compile(
    r"^P(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)$",
    re.IGNORECASE,
)


def _coerce_minutes(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None

        try:
            return int(stripped)
        except ValueError:
            pass

        iso_match = _ISO8601_DURATION_RE.fullmatch(stripped)
        if iso_match:
            hours = iso_match.group("hours")
            minutes = iso_match.group("minutes")
            seconds = iso_match.group("seconds")
            total = 0
            if hours:
                total += int(hours) * 60
            if minutes:
                total += int(minutes)
            if seconds:
                total += int(seconds) // 60
            return total if total or hours or minutes or seconds else None

        normalized = stripped.lower()
        replacements = {
            "hours": "h",
            "hour": "h",
            "hrs": "h",
            "hr": "h",
            "minutes": "m",
            "minute": "m",
            "mins": "m",
            "min": "m",
        }
        for original, replacement in replacements.items():
            normalized = normalized.replace(original, replacement)
        normalized = normalized.replace(" ", "")

        if ":" in normalized:
            parts = normalized.split(":")
            if 2 <= len(parts) <= 3 and all(part.isdigit() for part in parts):
                hours = int(parts[0])
                minutes = int(parts[1]) if len(parts) >= 2 else 0
                seconds = int(parts[2]) if len(parts) == 3 else 0
                return hours * 60 + minutes + seconds // 60

        if "h" in normalized:
            hours_part, _, remainder = normalized.partition("h")
            if hours_part.isdigit():
                minutes_part = remainder
                if minutes_part.endswith("m"):
                    minutes_part = minutes_part[:-1]
                if minutes_part == "" or minutes_part.isdigit():
                    minutes = int(minutes_part) if minutes_part else 0
                    return int(hours_part) * 60 + minutes

        if normalized.endswith("m") and normalized[:-1].isdigit():
            return int(normalized[:-1])

    try:
        if value is not None:
            return int(value)
    except (TypeError, ValueError):
        pass
    return None


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"", "0", "false", "f", "no", "n", "off"}:
            return False
        if lowered in {"1", "true", "t", "yes", "y", "on"}:
            return True
        return False
    return False


def _minutes_to_hhmm(total_min: Optional[int]) -> Optional[str]:
    if total_min is None:
        return None
    if total_min < 0:
        return None
    hours, minutes = divmod(total_min, 60)
    return f"{hours}:{minutes:02d}"


def _extract_rest_components(
    rest_payload: Optional[Mapping[str, Any]],
    full_duty_state: Optional[Mapping[str, Any]],
    duty_key: str,
) -> Tuple[Optional[int], Optional[int]]:
    actual: Optional[int] = None
    minimum: Optional[int] = None

    if isinstance(rest_payload, Mapping):
        actual = _coerce_minutes(rest_payload.get("actual"))
        minimum = _coerce_minutes(rest_payload.get("min"))

    if isinstance(full_duty_state, Mapping):
        rest_block = full_duty_state.get(duty_key)
        if isinstance(rest_block, Mapping):
            if actual is None:
                actual = _coerce_minutes(rest_block.get("actual"))
            if minimum is None:
                minimum = _coerce_minutes(rest_block.get("min"))

    return actual, minimum


def _extract_rest_minutes(
    rest_payload: Optional[Mapping[str, Any]],
    full_duty_state: Optional[Mapping[str, Any]],
) -> Optional[int]:
    actual, _ = _extract_rest_components(rest_payload, full_duty_state, "restAfterDuty")
    return actual


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


def _format_seat_display(seats: Iterable[str]) -> str:
    ordered = []
    seen = set()
    for seat in sorted({(seat or "PIC").upper() for seat in seats if seat}, key=_seat_sort_key):
        if seat not in seen:
            ordered.append(seat)
            seen.add(seat)
    if not ordered:
        return "Crew"
    return "+".join(ordered)


def summarize_split_duty_days(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
) -> List[str]:
    """Return formatted lines for the Split Duty Days section."""

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

        seats_display = _format_seat_display(pilot.seat for pilot in split_pilots)

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

        line = f"{tail} – {duty_display} duty – {break_display} break ({seats_display} split)"
        lines.append(line)

    return lines


_LONG_DUTY_THRESHOLD_RATIO = 0.90


def summarize_long_duty_days(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
) -> List[str]:
    """Return formatted lines for the Long Duty Days section."""

    if isinstance(collection, DutyStartCollection):
        snapshots: Iterable[DutyStartSnapshot] = collection.snapshots
    else:
        snapshots = collection

    tail_entries: Dict[str, List[Tuple[str, float, str]]] = {}

    for snapshot in snapshots:
        tail = snapshot.tail or "UNKNOWN"

        for pilot in snapshot.pilots:
            actual = pilot.fdp_actual_min
            max_allowed = pilot.fdp_max_min
            if actual is None or max_allowed in (None, 0):
                continue

            utilisation = actual / max_allowed
            if utilisation < _LONG_DUTY_THRESHOLD_RATIO:
                continue

            seat = (pilot.seat or "PIC").upper()
            display = pilot.fdp_actual_str or _minutes_to_hhmm(actual)
            if not display:
                display = f"{actual} min"

            tail_entries.setdefault(tail, []).append((seat, utilisation, display))

    lines: List[str] = []
    for tail in sorted(tail_entries):
        entries = tail_entries[tail]
        if not entries:
            continue

        ordered_entries = sorted(entries, key=lambda item: (_seat_sort_key(item[0]), -item[1]))
        seats = [entry[0] for entry in ordered_entries]
        displays = [entry[2] for entry in ordered_entries]

        seat_display = _format_seat_display(seats)
        if not seat_display:
            seat_display = "Crew"

        unique_displays = []
        seen_displays = set()
        for display in displays:
            if display not in seen_displays:
                unique_displays.append(display)
                seen_displays.add(display)

        duty_display = unique_displays[0] if len(unique_displays) == 1 else "/".join(unique_displays)

        lines.append(f"{tail} – {duty_display} ({seat_display})")

    return lines


_REST_THRESHOLD_MINUTES = 11 * 60
_REST_MATCH_TOLERANCE_MINUTES = 5


def _requires_non_flight_note(
    pilot: DutyStartPilotSnapshot,
    rest_minutes: int,
    next_day_rest_index: Mapping[str, Dict[str, Any]],
    *,
    tolerance: int,
) -> bool:
    identifier = _select_identifier(pilot)
    if not identifier:
        return True

    entry = next_day_rest_index.get(identifier)
    if not isinstance(entry, Mapping):
        return True

    next_actual = entry.get("rest_before_actual_min")
    if next_actual is None:
        return True

    after_actual = pilot.rest_after_actual_min if pilot.rest_after_actual_min is not None else rest_minutes
    if after_actual is None:
        return True

    if abs(int(next_actual) - int(after_actual)) <= max(tolerance, 0):
        return False

    return True


def summarize_tight_turnarounds(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
    *,
    next_day_rest_index: Optional[Mapping[str, Dict[str, Any]]] = None,
    rest_match_tolerance_min: int = _REST_MATCH_TOLERANCE_MINUTES,
) -> List[str]:
    """Return formatted lines listing crew whose rest falls below the threshold."""

    if isinstance(collection, DutyStartCollection):
        snapshots: Iterable[DutyStartSnapshot] = collection.snapshots
    else:
        snapshots = collection

    tail_map: Dict[str, Dict[str, Tuple[int, str, bool]]] = {}

    for snapshot in snapshots:
        tail = snapshot.tail or "UNKNOWN"
        seat_map = tail_map.setdefault(tail, {})

        for pilot in snapshot.pilots:
            rest_minutes = pilot.rest_after_actual_min
            if rest_minutes is None and pilot.rest_after_payload:
                rest_minutes = _coerce_minutes(pilot.rest_after_payload.get("actual"))
            if rest_minutes is None:
                continue
            if rest_minutes >= _REST_THRESHOLD_MINUTES:
                continue

            rest_display = (
                pilot.rest_after_actual_str or _minutes_to_hhmm(rest_minutes)
            )
            if not rest_display:
                continue

            seat = (pilot.seat or "PIC").upper()
            note_required = False
            if next_day_rest_index is not None:
                note_required = _requires_non_flight_note(
                    pilot,
                    rest_minutes,
                    next_day_rest_index,
                    tolerance=rest_match_tolerance_min,
                )

            existing = seat_map.get(seat)
            candidate = (rest_minutes, rest_display, note_required)
            if existing is None:
                seat_map[seat] = candidate
            else:
                existing_minutes, existing_display, existing_note = existing
                if rest_minutes < existing_minutes:
                    seat_map[seat] = candidate
                elif (
                    rest_minutes == existing_minutes
                    and note_required
                    and not existing_note
                ):
                    seat_map[seat] = candidate

    lines: List[str] = []
    for tail in sorted(tail_map):
        seat_map = tail_map[tail]
        if not seat_map:
            continue

        sorted_entries = sorted(
            seat_map.items(),
            key=lambda item: _seat_sort_key(item[0]),
        )

        seats = [seat for seat, _ in sorted_entries]
        rest_values = [value[1] for _, value in sorted_entries]
        note_flags = [value[2] for _, value in sorted_entries]
        if not seats:
            continue

        seat_display = _format_seat_display(seats)
        rest_display = rest_values[0] if len(set(rest_values)) == 1 else "/".join(rest_values)
        note_suffix = ""
        if any(note_flags):
            note_suffix = " – non-flight duties?"
        lines.append(
            f"{tail} – {rest_display} rest before next duty ({seat_display}){note_suffix}"
        )

    return lines


def summarize_insufficient_rest(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
) -> List[str]:
    """Compatibility wrapper for the previous function name."""

    return summarize_tight_turnarounds(collection)


def _seat_sort_key(seat: str) -> Tuple[int, str]:
    seat_upper = (seat or "").upper()
    if seat_upper == "PIC":
        return (0, seat_upper)
    if seat_upper == "SIC":
        return (1, seat_upper)
    return (2, seat_upper)


SectionBuilder = Callable[[Iterable[DutyStartSnapshot] | DutyStartCollection], Iterable[str]]


def build_flight_following_report(
    collection: Iterable[DutyStartSnapshot] | DutyStartCollection,
    *,
    generated_at: Optional[datetime] = None,
    target_date: Optional[date] = None,
    section_builders: Optional[Sequence[Tuple[str, SectionBuilder]]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> FlightFollowingReport:
    """Construct a :class:`FlightFollowingReport` from duty start snapshots."""

    if isinstance(collection, DutyStartCollection):
        resolved_target_date = collection.target_date
    else:
        if target_date is None:
            raise ValueError(
                "target_date must be provided when collection is not a DutyStartCollection"
            )
        resolved_target_date = target_date

    if generated_at is None:
        generated_at = datetime.now(tz=UTC)
    elif generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)
    else:
        generated_at = generated_at.astimezone(UTC)

    if section_builders is None:
        section_builders = (
            ("Long Duty Days", summarize_long_duty_days),
            ("Split Duty Days", summarize_split_duty_days),
            ("Tight Turnarounds (<11h Before Next Duty)", summarize_tight_turnarounds),
        )

    sections: List[FlightFollowingReportSection] = []
    for title, builder in section_builders:
        raw_lines = builder(collection)
        lines = [line for line in raw_lines if isinstance(line, str)] if raw_lines else []
        sections.append(FlightFollowingReportSection(title=title, lines=lines))

    report_metadata = dict(metadata or {})

    return FlightFollowingReport(
        target_date=resolved_target_date,
        generated_at=generated_at,
        sections=sections,
        metadata=report_metadata,
    )


__all__ = [
    "DutyStartPilotSnapshot",
    "DutyStartSnapshot",
    "DutyStartCollection",
    "FlightFollowingReportSection",
    "FlightFollowingReport",
    "collect_duty_start_snapshots",
    "summarize_collection_for_display",
    "build_rest_before_index",
    "summarize_split_duty_days",
    "summarize_long_duty_days",
    "summarize_tight_turnarounds",
    "summarize_insufficient_rest",
    "build_flight_following_report",
]
