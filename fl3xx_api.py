"""Utilities for interacting with the FL3XX external flight API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Literal

import pandas as pd
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


def _build_preflight_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/preflight"


def _build_pax_details_endpoint(base_url: str, flight_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    return f"{base}/{flight_id}/pax_details"


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


def _build_quote_endpoint(base_url: str, quote_id: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    if base.lower().endswith("/flight"):
        base = base[: -len("/flight")]
    return f"{base}/quote/{quote_id}"


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


def _build_airport_services_endpoint(base_url: str, airport_code: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    if base.lower().endswith("/flight"):
        base = base[: -len("/flight")]
    airport_ident = str(airport_code).strip().upper()
    return f"{base}/airports/{airport_ident}/services"


def _build_operational_notes_endpoint(base_url: str, airport_code: Any) -> str:
    base = base_url.rstrip("/")
    if base.lower().endswith("/flights"):
        base = base[: -len("/flights")]
    if base.lower().endswith("/flight"):
        base = base[: -len("/flight")]
    airport_ident = str(airport_code).strip().upper()
    return f"{base}/airports/{airport_ident}/operationalNotes"


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


def fetch_preflight(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the preflight payload for a specific flight."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_preflight_endpoint(config.base_url, flight_id),
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


def fetch_flight_pax_details(
    config: Fl3xxApiConfig,
    flight_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the pax_details payload for a specific flight.

    This issues a GET to ``{base_url}/{flight_id}/pax_details`` (with the
    default base pointing at the external ``/flight/flights`` host) and is
    the source used by feasibility to read passenger genders and types.
    """

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_pax_details_endpoint(config.base_url, flight_id),
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


@dataclass(frozen=True)
class MissingQualificationAlert:
    """Details about a missing crew qualification for a specific seat."""

    seat: str
    pilot_name: str
    pilot_id: Optional[str]
    qualification_name: str


@dataclass(frozen=True)
class PreflightConflictAlert:
    """Details about a preflight conflict surfaced by FL3XX."""

    seat: Optional[str]
    category: str
    status: str
    description: str


@dataclass(frozen=True)
class PreflightCrewCheckin:
    """Normalised check-in information for a crew member."""

    user_id: Optional[str] = None
    pilot_role: Optional[str] = None
    checkin: Optional[int] = None
    checkin_actual: Optional[int] = None
    checkin_default: Optional[int] = None
    extra_checkins: Tuple[int, ...] = ()


@dataclass(frozen=True)
class PreflightChecklistStatus:
    """Normalised crew preflight checklist readiness indicators."""

    crew_briefing: Optional[str] = None
    crew_assign: Optional[str] = None
    crew_checkins: Tuple[PreflightCrewCheckin, ...] = ()

    def _normalise_flag(self, value: Optional[str]) -> Optional[bool]:
        if value is None:
            return None
        normalised = value.strip().upper()
        if not normalised:
            return None
        return normalised == "OK"

    @property
    def crew_briefing_ok(self) -> Optional[bool]:
        return self._normalise_flag(self.crew_briefing)

    @property
    def crew_assign_ok(self) -> Optional[bool]:
        return self._normalise_flag(self.crew_assign)

    @property
    def all_ok(self) -> Optional[bool]:
        flags = (self.crew_briefing_ok, self.crew_assign_ok)
        if any(flag is False for flag in flags):
            return False
        if all(flag is True for flag in flags):
            return True
        if any(flag is None for flag in flags):
            return None
        return None

    @property
    def has_data(self) -> bool:
        if any(value is not None for value in (self.crew_briefing, self.crew_assign)):
            return True
        return any(checkin.checkin is not None for checkin in self.crew_checkins)


def _extract_preflight_status_value(value: Any) -> Optional[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _format_pilot_name(user_block: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for key in ("firstName", "middleName", "lastName"):
        value = user_block.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value:
            parts.append(str(value))
    if parts:
        return " ".join(parts)

    for fallback_key in (
        "nickname",
        "logName",
        "emailAddress",
        "email",
        "trigram",
        "personnelNumber",
    ):
        fallback = user_block.get(fallback_key)
        if isinstance(fallback, str):
            fallback = fallback.strip()
        if fallback:
            return str(fallback)

    return ""


def _normalise_optional_epoch(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            numeric = float(cleaned)
        except ValueError:
            return None
        return int(numeric)
    return None


def _normalise_datetime_candidate(value: Any) -> Optional[int]:
    """Return an epoch value for strings that encode datetimes."""

    epoch = _normalise_optional_epoch(value)
    if epoch is not None:
        return epoch

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None

        try:
            parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = pd.to_datetime(cleaned, utc=True).to_pydatetime()
            except Exception:
                return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())

    return None


def extract_missing_qualifications_from_preflight(
    preflight_payload: Any,
) -> List[MissingQualificationAlert]:
    """Return any missing crew qualifications for the assigned PIC/SIC."""

    results: List[MissingQualificationAlert] = []

    if not isinstance(preflight_payload, Mapping):
        return results

    crew_assign = preflight_payload.get("crewAssign")
    if not isinstance(crew_assign, Mapping):
        return results

    for seat_key, seat_label in (("commander", "PIC"), ("firstOfficer", "SIC")):
        crew_block = crew_assign.get(seat_key)
        if not isinstance(crew_block, Mapping):
            continue

        user_block = crew_block.get("user")
        if not isinstance(user_block, Mapping):
            user_block = {}

        pilot_id_raw = user_block.get("id")
        pilot_id = str(pilot_id_raw) if pilot_id_raw is not None else None
        pilot_name = _format_pilot_name(user_block)

        warnings_block = crew_block.get("warnings")
        if not isinstance(warnings_block, Mapping):
            continue

        messages = warnings_block.get("messages")
        if not isinstance(messages, list):
            continue

        for message in messages:
            if not isinstance(message, Mapping):
                continue

            msg_type = str(message.get("type") or "").strip().upper()
            msg_status = str(message.get("status") or "").strip().upper()
            qual_name = message.get("name")

            if (
                msg_type in {"QUALIFICATION", "RECENCY"}
                and msg_status in {"MISSING", "EXPIRED"}
                and isinstance(qual_name, str)
            ):
                cleaned_name = qual_name.strip()
                if cleaned_name:
                    results.append(
                        MissingQualificationAlert(
                            seat=seat_label,
                            pilot_name=pilot_name,
                            pilot_id=pilot_id,
                            qualification_name=cleaned_name,
                        )
                    )

    return results


def _extract_conflict_description(message: Mapping[str, Any]) -> str:
    for key in ("name", "message", "description", "details"):
        value = message.get(key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return json.dumps(message, default=str)


def extract_conflicts_from_preflight(preflight_payload: Any) -> List[PreflightConflictAlert]:
    """Return any preflight conflicts surfaced in the crew assignment payload."""

    results: List[PreflightConflictAlert] = []

    if not isinstance(preflight_payload, Mapping):
        return results

    crew_assign = preflight_payload.get("crewAssign")
    if not isinstance(crew_assign, Mapping):
        return results

    def _extract_from_messages(messages: Any, seat: Optional[str]) -> None:
        if not isinstance(messages, list):
            return
        for message in messages:
            if not isinstance(message, Mapping):
                continue
            msg_type = str(message.get("type") or "").strip().upper()
            msg_status = str(message.get("status") or "").strip().upper()
            if msg_type != "FLIGHT" or msg_status != "CONFLICT":
                continue
            description = _extract_conflict_description(message)
            results.append(
                PreflightConflictAlert(
                    seat=seat,
                    category=msg_type,
                    status=msg_status,
                    description=description,
                )
            )

    general_warnings = crew_assign.get("warnings")
    if isinstance(general_warnings, Mapping):
        _extract_from_messages(general_warnings.get("messages"), None)

    for seat_key, seat_label in (("commander", "PIC"), ("firstOfficer", "SIC")):
        crew_block = crew_assign.get(seat_key)
        if not isinstance(crew_block, Mapping):
            continue
        warnings_block = crew_block.get("warnings")
        if not isinstance(warnings_block, Mapping):
            continue
        _extract_from_messages(warnings_block.get("messages"), seat_label)

    return results


def _parse_preflight_checkin(entry: Mapping[str, Any]) -> PreflightCrewCheckin:
    user_id = entry.get("userId")
    user_id_str: Optional[str]
    if user_id is None:
        user_id_str = None
    elif isinstance(user_id, str):
        user_id_str = user_id.strip() or None
    else:
        user_id_str = str(user_id)

    pilot_role_value = entry.get("pilotRole")
    pilot_role: Optional[str]
    if isinstance(pilot_role_value, str):
        pilot_role = pilot_role_value.strip() or None
    elif pilot_role_value is None:
        pilot_role = None
    else:
        pilot_role = str(pilot_role_value)

    checkin = _normalise_optional_epoch(entry.get("checkin"))
    checkin_actual = _normalise_optional_epoch(entry.get("checkinActual"))
    checkin_default = _normalise_optional_epoch(entry.get("checkinDefault"))

    extra_epochs: List[int] = []
    for key, value in entry.items():
        if not isinstance(key, str):
            continue
        normalised_key = key.strip().lower()
        if normalised_key in {"checkin", "checkinactual", "checkindefault"}:
            continue
        if "checkin" in normalised_key or (
            "report" in normalised_key
            and any(token in normalised_key for token in ("time", "utc", "local"))
        ):
            candidate = _normalise_datetime_candidate(value)
            if candidate is not None:
                extra_epochs.append(candidate)

    unique_extra = tuple(sorted(set(extra_epochs)))

    return PreflightCrewCheckin(
        user_id=user_id_str,
        pilot_role=pilot_role,
        checkin=checkin,
        checkin_actual=checkin_actual,
        checkin_default=checkin_default,
        extra_checkins=unique_extra,
    )


def parse_preflight_payload(preflight_payload: Any) -> PreflightChecklistStatus:
    """Return crew readiness flags and check-in data from a preflight payload."""

    crew_briefing: Optional[str] = None
    crew_assign: Optional[str] = None
    dtls2_list: List[Mapping[str, Any]] = []

    if isinstance(preflight_payload, Mapping):
        # Newer AirSprint payloads expose crew status blocks at the root.
        maybe_brief = preflight_payload.get("crewBrief")
        if isinstance(maybe_brief, Mapping):
            crew_briefing = _extract_preflight_status_value(maybe_brief.get("status"))
        else:
            crew_briefing = _extract_preflight_status_value(maybe_brief)

        maybe_assign = preflight_payload.get("crewAssign")
        if isinstance(maybe_assign, Mapping):
            crew_assign = _extract_preflight_status_value(maybe_assign.get("status"))
        else:
            crew_assign = _extract_preflight_status_value(maybe_assign)

        duty_time_lim = preflight_payload.get("dutyTimeLim")
        if isinstance(duty_time_lim, Mapping):
            dtls2 = duty_time_lim.get("dtls2")
            if isinstance(dtls2, list):
                dtls2_list = [entry for entry in dtls2 if isinstance(entry, Mapping)]

        # Fall back to the original structure that surfaced crew info under "crw".
        if crew_briefing is None or crew_assign is None:
            maybe_crew = preflight_payload.get("crw")
            if isinstance(maybe_crew, Mapping):
                if crew_briefing is None:
                    crew_briefing = _extract_preflight_status_value(
                        maybe_crew.get("crewBriefing")
                    )
                if crew_assign is None:
                    crew_assign = _extract_preflight_status_value(maybe_crew.get("crewAssign"))

        # Original payloads also surfaced dtls2 either at the root or under "time".
        if not dtls2_list:
            maybe_dtls2 = preflight_payload.get("dtls2")
            if isinstance(maybe_dtls2, list):
                dtls2_list = [entry for entry in maybe_dtls2 if isinstance(entry, Mapping)]
            else:
                time_block = preflight_payload.get("time")
                if isinstance(time_block, Mapping):
                    nested_dtls2 = time_block.get("dtls2")
                    if isinstance(nested_dtls2, list):
                        dtls2_list = [
                            entry for entry in nested_dtls2 if isinstance(entry, Mapping)
                        ]

    crew_checkins = tuple(_parse_preflight_checkin(entry) for entry in dtls2_list)

    status = PreflightChecklistStatus(
        crew_briefing=crew_briefing,
        crew_assign=crew_assign,
        crew_checkins=crew_checkins,
    )

    return status


@dataclass
class DutySnapshotPilot:
    """Duty information for a single pilot on a duty period."""

    seat: Literal["PIC", "SIC"]
    name: str
    pilot_id: Optional[str] = None
    fdp_actual_min: Optional[int] = None
    fdp_max_min: Optional[int] = None
    fdp_actual_str: Optional[str] = None
    split_duty: bool = False
    split_break_str: Optional[str] = None
    rest_after_min: Optional[int] = None
    rest_after_str: Optional[str] = None


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


def _normalise_pilot_seat(role: Any, default: Literal["PIC", "SIC"] = "PIC") -> Literal["PIC", "SIC"]:
    role_str = str(role or "").strip().upper()
    if role_str in {"CMD", "PIC", "CAPT", "CAPTAIN"}:
        return "PIC"
    if role_str in {"FO", "SIC", "FIRST OFFICER"}:
        return "SIC"
    return default


def _duty_snapshot_pilot_from_block(
    pilot_block: Mapping[str, Any],
    *,
    default_seat: Literal["PIC", "SIC"] = "PIC",
) -> DutySnapshotPilot:
    seat = _normalise_pilot_seat(pilot_block.get("pilotRole") or pilot_block.get("role"), default_seat)

    pilot_id_value = pilot_block.get("userId") or pilot_block.get("id") or pilot_block.get("crewMemberId")
    pilot_id: Optional[str]
    if isinstance(pilot_id_value, (str, int)):
        pilot_id = str(pilot_id_value)
    else:
        pilot_id = None

    first = pilot_block.get("firstName") or ""
    last = pilot_block.get("lastName") or ""
    name = f"{first} {last}".strip()
    if not name:
        for fallback in ("nickname", "logName", "email", "personnelNumber", "trigram"):
            fallback_value = pilot_block.get(fallback)
            if isinstance(fallback_value, str) and fallback_value.strip():
                name = fallback_value.strip()
                break

    full_duty_state_raw = pilot_block.get("fullDutyState")
    full_duty_state: Dict[str, Any] = dict(full_duty_state_raw) if isinstance(full_duty_state_raw, Mapping) else {}

    fdp_info_raw: Mapping[str, Any] = {}
    if full_duty_state:
        maybe_fdp = full_duty_state.get("fdp")
        if isinstance(maybe_fdp, Mapping):
            fdp_info_raw = maybe_fdp
    if not fdp_info_raw:
        maybe_direct_fdp = pilot_block.get("fdp")
        if isinstance(maybe_direct_fdp, Mapping):
            fdp_info_raw = maybe_direct_fdp

    fdp_actual_min = fdp_info_raw.get("actual") if isinstance(fdp_info_raw, Mapping) else None
    fdp_max_min = fdp_info_raw.get("max") if isinstance(fdp_info_raw, Mapping) else None
    if isinstance(fdp_actual_min, (int, float)):
        fdp_actual_min = int(fdp_actual_min)
    else:
        fdp_actual_min = None
    if isinstance(fdp_max_min, (int, float)):
        fdp_max_min = int(fdp_max_min)
    else:
        fdp_max_min = None

    explainer_source: Mapping[str, Any] = {}
    if full_duty_state:
        maybe_explainer = full_duty_state.get("explainerMap")
        if isinstance(maybe_explainer, Mapping):
            explainer_source = maybe_explainer
    if not explainer_source:
        maybe_direct_explainer = pilot_block.get("explainerMap")
        if isinstance(maybe_direct_explainer, Mapping):
            explainer_source = maybe_direct_explainer
    explainer_map = dict(explainer_source) if isinstance(explainer_source, Mapping) else {}

    fdp_actual_str = _extract_fdp_actual_str(explainer_map)
    split_break_str = _extract_break_str(explainer_map)

    split_duty = False
    for candidate in (pilot_block, full_duty_state):
        if isinstance(candidate, Mapping):
            if candidate.get("splitDutyType"):
                split_duty = True
                break
            split_value = candidate.get("splitDutyStart")
            if isinstance(split_value, bool) and split_value:
                split_duty = True
                break

    rest_payload_raw = pilot_block.get("restAfterDuty")
    rest_payload = dict(rest_payload_raw) if isinstance(rest_payload_raw, Mapping) else {}
    rest_after_min = None
    if rest_payload:
        actual = rest_payload.get("actual")
        if isinstance(actual, (int, float)):
            rest_after_min = int(actual)
    if rest_after_min is None and isinstance(full_duty_state, Mapping):
        rest_block = full_duty_state.get("restAfterDuty")
        if isinstance(rest_block, Mapping):
            actual = rest_block.get("actual")
            if isinstance(actual, (int, float)):
                rest_after_min = int(actual)

    rest_after_str = _minutes_to_hhmm(rest_after_min)

    return DutySnapshotPilot(
        seat=seat,
        name=name,
        pilot_id=pilot_id,
        fdp_actual_min=fdp_actual_min,
        fdp_max_min=fdp_max_min,
        fdp_actual_str=fdp_actual_str,
        split_duty=split_duty,
        split_break_str=split_break_str,
        rest_after_min=rest_after_min,
        rest_after_str=rest_after_str,
    )


def _fallback_pilots_from_time_block(time_block: Mapping[str, Any]) -> List[DutySnapshotPilot]:
    pilots: List[DutySnapshotPilot] = []
    for key, default_seat in (("cmd", "PIC"), ("fo", "SIC")):
        pilot_block = time_block.get(key)
        if isinstance(pilot_block, Mapping):
            pilots.append(_duty_snapshot_pilot_from_block(pilot_block, default_seat=default_seat))
    return [pilot for pilot in pilots if pilot is not None]


def _fallback_pilots_from_deice(postflight_payload: Mapping[str, Any]) -> List[DutySnapshotPilot]:
    deice_block = postflight_payload.get("deice")
    if not isinstance(deice_block, Mapping):
        return []
    crew_list = deice_block.get("crew")
    if not isinstance(crew_list, list):
        return []

    pilots: List[DutySnapshotPilot] = []
    for index, member in enumerate(crew_list):
        if not isinstance(member, Mapping):
            continue
        job_title = member.get("jobTitle")
        default_seat: Literal["PIC", "SIC"] = "PIC" if index == 0 else "SIC"
        seat = _normalise_pilot_seat(job_title, default_seat)
        pilots.append(_duty_snapshot_pilot_from_block(member, default_seat=seat))
    return [pilot for pilot in pilots if pilot is not None]


def parse_postflight_payload(postflight_payload: Any) -> DutySnapshot:
    """Normalise a postflight payload into duty data for reporting."""

    tail = ""
    if isinstance(postflight_payload, dict):
        maybe_tail = postflight_payload.get("tailNumber") or postflight_payload.get("registrationNumber")
        if isinstance(maybe_tail, str):
            tail = maybe_tail.strip()

    pilots: List[DutySnapshotPilot] = []
    time_block: Dict[str, Any] = {}
    if isinstance(postflight_payload, dict):
        maybe_time_block = postflight_payload.get("time")
        if isinstance(maybe_time_block, dict):
            time_block = maybe_time_block

    dtls2 = time_block.get("dtls2", []) if time_block else []
    if not isinstance(dtls2, list):
        dtls2 = postflight_payload.get("dtls2") if isinstance(postflight_payload, Mapping) else []
        if not isinstance(dtls2, list):
            dtls2 = []

    for pilot_block in dtls2:
        if isinstance(pilot_block, Mapping):
            pilots.append(_duty_snapshot_pilot_from_block(pilot_block))

    if not pilots and time_block:
        pilots.extend(_fallback_pilots_from_time_block(time_block))

    if not pilots and isinstance(postflight_payload, Mapping):
        pilots.extend(_fallback_pilots_from_deice(postflight_payload))

    pilots = [pilot for pilot in pilots if isinstance(pilot, DutySnapshotPilot)]

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


def fetch_airport_services(
    config: Fl3xxApiConfig,
    airport_code: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the services payload for a specific airport (FBO listings, etc.)."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_airport_services_endpoint(config.base_url, airport_code),
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


def fetch_operational_notes(
    config: Fl3xxApiConfig,
    airport_code: Any,
    *,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: Optional[requests.Session] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Return airport operational notes within the requested date range."""

    reference_time = now or datetime.now(timezone.utc)
    start = from_date or reference_time.date()
    end = to_date or (start + timedelta(days=1))
    if end <= start:
        end = start + timedelta(days=1)

    params = {"from": start.isoformat(), "to": end.isoformat()}

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_operational_notes_endpoint(config.base_url, airport_code),
            params=params,
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        payload = response.json()
        notes: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            notes = [dict(entry) for entry in payload if isinstance(entry, Mapping)]
        elif isinstance(payload, Mapping):
            notes = [dict(payload)] if payload else []
        elif payload is None:
            notes = []
        else:
            raise ValueError("Unsupported FL3XX operational notes payload structure")
        return notes
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


def fetch_quote_details(
    config: Fl3xxApiConfig,
    quote_id: Any,
    *,
    session: Optional[requests.Session] = None,
) -> Any:
    """Return the quote payload for a specific quote identifier."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            _build_quote_endpoint(config.base_url, quote_id),
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
    "fetch_preflight",
    "fetch_flight_pax_details",
    "fetch_flight_services",
    "fetch_operational_notes",
    "fetch_flight_planning_note",
    "fetch_flight_migration",
    "fetch_flight_notification",
    "enrich_flights_with_crew",
    "DutySnapshot",
    "DutySnapshotPilot",
    "MissingQualificationAlert",
    "PreflightConflictAlert",
    "PreflightCrewCheckin",
    "PreflightChecklistStatus",
    "parse_postflight_payload",
    "parse_preflight_payload",
    "extract_missing_qualifications_from_preflight",
    "extract_conflicts_from_preflight",
]
