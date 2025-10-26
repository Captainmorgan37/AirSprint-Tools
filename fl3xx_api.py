"""Utilities for interacting with the FL3XX external flight API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple, Literal

import requests
from zoneinfo_compat import ZoneInfo


DEFAULT_FL3XX_BASE_URL = "https://app.fl3xx.us/api/external/flight/flights"
MOUNTAIN_TIME_ZONE_NAME = "America/Edmonton"
MOUNTAIN_TIME_ZONE = ZoneInfo(MOUNTAIN_TIME_ZONE_NAME)


@dataclass(frozen=True)
class Fl3xxApiConfig:
    """Configuration for issuing requests to the FL3XX API."""

    base_url: str = DEFAULT_FL3XX_BASE_URL
    api_token: Optional[str] = None
    auth_header: Optional[str] = None
    auth_header_name: str = "Authorization"
    api_token_scheme: Optional[str] = None
    extra_headers: Dict[str, str] = field(default_factory=dict)
    verify_ssl: bool = True
    timeout: int = 30
    extra_params: Dict[str, str] = field(default_factory=dict)

    def build_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        header_name = self.auth_header_name or "Authorization"
        if self.auth_header:
            headers[header_name] = self.auth_header
        elif self.api_token:
            token = str(self.api_token)
            scheme = self.api_token_scheme
            if scheme is None:
                scheme = "Bearer" if header_name.lower() == "authorization" else ""
            else:
                scheme = scheme.strip()
            headers[header_name] = f"{scheme} {token}".strip() if scheme else token
        headers.update(self.extra_headers)
        return headers


def compute_fetch_dates(
    now: Optional[datetime] = None,
    *,
    inclusive_days: int = 1,
) -> Tuple[date, date]:
    """Return the default (exclusive) date range that should be requested."""

    if inclusive_days < 0:
        raise ValueError("inclusive_days must be non-negative")

    current = now or datetime.now(timezone.utc)
    mountain_time = current.astimezone(MOUNTAIN_TIME_ZONE)
    start = mountain_time.date()
    end = start + timedelta(days=inclusive_days + 1)
    return start, end


def _normalise_payload(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, MutableMapping):
        if "items" in data and isinstance(data["items"], Iterable):
            items = list(data["items"])
            if all(isinstance(item, MutableMapping) for item in items):
                return items  # type: ignore[return-value]
        raise ValueError("Unsupported FL3XX API payload structure: mapping without 'items' list")
    raise ValueError("Unsupported FL3XX API payload structure")


def compute_flights_digest(flights: Iterable[Any]) -> str:
    """Return a stable SHA256 digest for the provided flight payload."""

    digest_input = json.dumps(list(flights), sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(digest_input).hexdigest()


def fetch_flights(
    config: Fl3xxApiConfig,
    *,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: Optional[requests.Session] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Retrieve flights from the FL3XX API and return them with metadata."""

    reference_time = now or datetime.now(timezone.utc)
    if from_date is None or to_date is None:
        default_from, default_to = compute_fetch_dates(reference_time)
        if from_date is None:
            from_date = default_from
        if to_date is None:
            to_date = default_to

    params_sequence: List[Tuple[str, str]] = [
        ("from", from_date.isoformat()),
        ("timeZone", MOUNTAIN_TIME_ZONE_NAME),
        ("to", to_date.isoformat()),
        ("value", "ALL"),
    ]

    sequence_index: Dict[str, int] = {name: idx for idx, (name, _) in enumerate(params_sequence)}
    for key, value in config.extra_params.items():
        if key in sequence_index:
            params_sequence[sequence_index[key]] = (key, value)
        else:
            sequence_index[key] = len(params_sequence)
            params_sequence.append((key, value))

    params: Dict[str, str] = dict(params_sequence)

    headers = config.build_headers()

    http = session or requests.Session()
    response = http.get(
        config.base_url,
        params=params_sequence,
        headers=headers,
        timeout=config.timeout,
        verify=config.verify_ssl,
    )
    response.raise_for_status()
    payload = response.json()
    flights = _normalise_payload(payload)

    digest = compute_flights_digest(flights)
    fetched_at = reference_time.isoformat().replace("+00:00", "Z")

    metadata = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "time_zone": params["timeZone"],
        "value": params["value"],
        "fetched_at": fetched_at,
        "hash": digest,
        "request_url": config.base_url,
        "request_params": params,
    }
    return flights, metadata


def _build_flight_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/crew"


def _build_migration_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/migration"


def _build_postflight_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/postflight"


def _build_notification_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/notification"


def _build_leg_endpoint(base_url: str, quote_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    if base.lower().endswith("/flight"):
        base = base[: -len("/flight")]
    return f"{base}/leg/{quote_id}"


def _build_services_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/services"


def _build_planning_note_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/planningnote"


def _normalise_crew_payload(payload: Any) -> List[Dict[str, Any]]:
    """Return a list of crew member dictionaries from various payload layouts."""

    if payload is None:
        return []

    def _coerce_members(obj: Any) -> Optional[List[Dict[str, Any]]]:
        if obj is None:
            return []
        if isinstance(obj, MutableMapping):
            return [value for value in obj.values() if isinstance(value, MutableMapping)]
        if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes, bytearray)):
            return [item for item in obj if isinstance(item, MutableMapping)]
        return None

    if isinstance(payload, MutableMapping):
        for key in ("crewMembers", "items", "crew", "data", "results", "crews"):
            if key in payload:
                members = _coerce_members(payload[key])
                if members is not None:
                    return members

        if any(
            key in payload
            for key in ("role", "firstName", "lastName", "logName", "email", "trigram", "personnelNumber")
        ):
            return [payload]

        if not payload:
            return []

        raise ValueError("Unsupported FL3XX crew payload structure")

    members = _coerce_members(payload)
    if members is not None:
        return members

    raise ValueError("Unsupported FL3XX crew payload structure")


def fetch_flight_crew(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Return the crew payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_flight_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        payload = response.json()
        return _normalise_crew_payload(payload)
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def fetch_postflight(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the postflight payload (including crew check-in times) for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_postflight_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


@dataclass
class DutySnapshotPilot:
    """Duty information for a single pilot on a duty period."""

    seat: Literal["PIC", "SIC"]
    name: str
    fdp_actual_min: Optional[int]
    fdp_max_min: Optional[int]
    fdp_actual_str: Optional[str]
    split_duty: bool
    split_break_str: Optional[str]
    rest_after_min: Optional[int]
    rest_after_str: Optional[str]


@dataclass
class DutySnapshot:
    """Summary of duty information for a specific tail/flight."""

    tail: str
    pilots: List[DutySnapshotPilot]


def _minutes_to_hhmm(total_min: Optional[int]) -> Optional[str]:
    if total_min is None:
        return None
    if total_min < 0:
        return None
    hours, minutes = divmod(total_min, 60)
    return f"{hours}:{minutes:02d}"


def _extract_break_str(explainer_map: Any) -> Optional[str]:
    """Return the break duration string from the ACTUAL_FDP explainer map."""

    if not isinstance(explainer_map, dict):
        return None
    actual = explainer_map.get("ACTUAL_FDP", {})
    text_lines = actual.get("text")
    if not isinstance(text_lines, list):
        return None
    for line in text_lines:
        if not isinstance(line, str):
            continue
        if line.strip().lower().startswith("break"):
            parts = line.split("=")
            if len(parts) >= 2:
                return parts[-1].strip()
    return None


def _extract_fdp_actual_str(explainer_map: Any) -> Optional[str]:
    """Return the formatted FDP string from the ACTUAL_FDP header."""

    if not isinstance(explainer_map, dict):
        return None
    header = explainer_map.get("ACTUAL_FDP", {}).get("header")
    if not isinstance(header, str):
        return None
    if "=" in header:
        return header.split("=", 1)[1].strip()
    header = header.strip()
    return header or None


def parse_postflight_payload(postflight_payload: Any) -> DutySnapshot:
    """Normalise a postflight payload into duty data for reporting."""

    tail = ""
    if isinstance(postflight_payload, dict):
        maybe_tail = postflight_payload.get("tailNumber") or postflight_payload.get("registrationNumber")
        if isinstance(maybe_tail, str):
            tail = maybe_tail.strip()

    pilots: List[DutySnapshotPilot] = []
    dtls2 = postflight_payload.get("dtls2", []) if isinstance(postflight_payload, dict) else []
    if not isinstance(dtls2, list):
        dtls2 = []

    for pilot_block in dtls2:
        if not isinstance(pilot_block, dict):
            continue

        role_raw = pilot_block.get("pilotRole") or pilot_block.get("role") or ""
        role_normalised = str(role_raw).strip().upper()
        if role_normalised in {"CMD", "PIC", "CAPT", "CAPTAIN"}:
            seat: Literal["PIC", "SIC"] = "PIC"
        elif role_normalised in {"FO", "SIC", "FIRST OFFICER"}:
            seat = "SIC"
        else:
            seat = "PIC"

        first = pilot_block.get("firstName") or ""
        last = pilot_block.get("lastName") or ""
        name = f"{first} {last}".strip()
        if not name:
            for fallback in ("logName", "email", "personnelNumber"):
                fallback_value = pilot_block.get(fallback)
                if isinstance(fallback_value, str) and fallback_value.strip():
                    name = fallback_value.strip()
                    break

        full_duty_state = pilot_block.get("fullDutyState", {})
        fdp_info = full_duty_state.get("fdp", {}) if isinstance(full_duty_state, dict) else {}
        fdp_actual_min = fdp_info.get("actual") if isinstance(fdp_info, dict) else None
        fdp_max_min = fdp_info.get("max") if isinstance(fdp_info, dict) else None
        if isinstance(fdp_actual_min, (int, float)):
            fdp_actual_min = int(fdp_actual_min)
        else:
            fdp_actual_min = None
        if isinstance(fdp_max_min, (int, float)):
            fdp_max_min = int(fdp_max_min)
        else:
            fdp_max_min = None

        explainer_map = full_duty_state.get("explainerMap", {}) if isinstance(full_duty_state, dict) else {}
        fdp_actual_str = _extract_fdp_actual_str(explainer_map)
        split_break_str = _extract_break_str(explainer_map)

        split_duty = False
        if pilot_block.get("splitDutyStart") is True or pilot_block.get("splitDutyType"):
            split_duty = True
        if isinstance(full_duty_state, dict) and (
            full_duty_state.get("splitDutyStart") is True or full_duty_state.get("splitDutyType")
        ):
            split_duty = True

        rest_after_min = None
        rest_block = pilot_block.get("restAfterDuty")
        if isinstance(rest_block, dict):
            actual = rest_block.get("actual")
            if isinstance(actual, (int, float)):
                rest_after_min = int(actual)
        if rest_after_min is None and isinstance(full_duty_state, dict):
            rest_block = full_duty_state.get("restAfterDuty")
            if isinstance(rest_block, dict):
                actual = rest_block.get("actual")
                if isinstance(actual, (int, float)):
                    rest_after_min = int(actual)

        rest_after_str = _minutes_to_hhmm(rest_after_min)

        pilots.append(
            DutySnapshotPilot(
                seat=seat,
                name=name,
                fdp_actual_min=fdp_actual_min,
                fdp_max_min=fdp_max_min,
                fdp_actual_str=fdp_actual_str,
                split_duty=split_duty,
                split_break_str=split_break_str,
                rest_after_min=rest_after_min,
                rest_after_str=rest_after_str,
            )
        )

    return DutySnapshot(tail=tail, pilots=pilots)


def fetch_flight_services(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the services payload (including handlers) for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_services_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def fetch_flight_planning_note(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the planning note payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_planning_note_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def fetch_flight_migration(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Return the customs migration payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_migration_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, MutableMapping):
            return dict(payload)
        raise ValueError("Unsupported FL3XX migration payload structure")
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def fetch_flight_notification(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the notification payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_notification_endpoint(config.base_url, flight_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def fetch_leg_details(
    config: Fl3xxApiConfig,
    quote_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the leg payload (including planning notes) for a specific quote."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_leg_endpoint(config.base_url, quote_id),
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass


def _select_crew_member(crew: Iterable[Dict[str, Any]], role: str) -> Optional[Dict[str, Any]]:
    for member in crew:
        if not isinstance(member, MutableMapping):
            continue
        member_role = str(member.get("role") or "").upper()
        if member_role == role.upper():
            return member
    return None


def _format_crew_name(member: Optional[Dict[str, Any]]) -> str:
    if not member:
        return ""
    parts = []
    for key in ("firstName", "middleName", "lastName"):
        value = member.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value:
            parts.append(str(value))
    if parts:
        return " ".join(parts)
    for fallback_key in ("logName", "email", "trigram", "personnelNumber"):
        fallback = member.get(fallback_key)
        if isinstance(fallback, str):
            fallback = fallback.strip()
        if fallback:
            return str(fallback)
    return ""


def enrich_flights_with_crew(
    config: Fl3xxApiConfig,
    flights: Iterable[Dict[str, Any]],
    *,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Populate crew information (PIC/SIC names) onto the provided flights."""

    summary = {"fetched": 0, "errors": [], "updated": False}
    mutable_flights = [flight for flight in flights if isinstance(flight, MutableMapping)]
    if not mutable_flights:
        return summary

    http = session or requests.Session()
    close_session = session is None
    try:
        for flight in mutable_flights:
            flight_id = flight.get("flightId") or flight.get("id")
            if not flight_id:
                continue
            if not force and flight.get("picName") and flight.get("sicName"):
                continue
            try:
                crew_payload = fetch_flight_crew(config, flight_id, session=http)
            except Exception as exc:  # pragma: no cover - defensive path
                summary["errors"].append({"flight_id": flight_id, "error": str(exc)})
                continue

            summary["fetched"] += 1
            flight["crewMembers"] = crew_payload
            pic_member = _select_crew_member(crew_payload, "CMD")
            sic_member = _select_crew_member(crew_payload, "FO")
            pic_name = _format_crew_name(pic_member)
            sic_name = _format_crew_name(sic_member)
            if pic_name:
                if flight.get("picName") != pic_name:
                    summary["updated"] = True
                flight["picName"] = pic_name
            if sic_name:
                if flight.get("sicName") != sic_name:
                    summary["updated"] = True
                flight["sicName"] = sic_name
    finally:
        if close_session:
            try:
                http.close()
            except AttributeError:
                pass

    return summary


__all__ = [
    "Fl3xxApiConfig",
    "DEFAULT_FL3XX_BASE_URL",
    "MOUNTAIN_TIME_ZONE",
    "compute_fetch_dates",
    "compute_flights_digest",
    "fetch_flights",
    "fetch_flight_crew",
    "fetch_postflight",
    "fetch_flight_services",
    "fetch_flight_planning_note",
    "fetch_flight_migration",
    "fetch_flight_notification",
    "enrich_flights_with_crew",
    "DutySnapshot",
    "DutySnapshotPilot",
    "parse_postflight_payload",
]
