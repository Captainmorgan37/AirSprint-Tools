"""Utilities for identifying and formatting short aircraft turns."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

try:  # pragma: no cover - Python <3.9 fallback
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None  # type: ignore[assignment]


_LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "America/Edmonton")
_DEFAULT_TURN_THRESHOLD_MIN = int(os.getenv("TURN_THRESHOLD_MIN", "45"))
_PRIORITY_TURN_THRESHOLD_MIN = int(os.getenv("PRIORITY_TURN_THRESHOLD_MIN", "90"))
DEFAULT_TURN_THRESHOLD_MIN = _DEFAULT_TURN_THRESHOLD_MIN
PRIORITY_TURN_THRESHOLD_MIN = _PRIORITY_TURN_THRESHOLD_MIN

_ACCOUNT_NAME_KEYS: Tuple[str, ...] = (
    "accountName",
    "account",
    "account_name",
    "owner",
    "ownerName",
    "customer",
    "customerName",
    "client",
    "clientName",
    "detail.accountName",
    "detail.account",
    "detail.account_name",
)

_ACCOUNT_NESTED_NAME_KEYS: Tuple[str, ...] = (
    "name",
    "accountName",
    "account",
    "account_name",
    "owner",
    "ownerName",
    "customer",
    "customerName",
    "client",
    "clientName",
    "displayName",
    "label",
)


def _resolve_local_tz(local_tz: Optional[ZoneInfo] = None) -> timezone:
    if local_tz is not None:
        return local_tz
    if ZoneInfo is not None:
        try:
            return ZoneInfo(_LOCAL_TZ_NAME)
        except Exception:  # pragma: no cover - environment specific
            pass
    return timezone.utc


def parse_dt(value: Any, *, local_tz: Optional[ZoneInfo] = None) -> pd.Timestamp:
    """Return a timezone-aware :class:`~pandas.Timestamp` for the provided value."""

    if pd.isna(value) or value == "":
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        ts = value
    elif isinstance(value, datetime):
        ts = pd.Timestamp(value)
    else:
        try:
            ts = pd.to_datetime(value, utc=True)
        except Exception:
            return pd.NaT
    if ts is pd.NaT:
        return pd.NaT
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    target_tz = _resolve_local_tz(local_tz)
    try:
        return ts.tz_convert(target_tz)
    except Exception:  # pragma: no cover - conversion issues
        return ts


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _get_nested(mapping: Mapping[str, Any], path: Sequence[str] | str) -> Any:
    parts: Sequence[str]
    if isinstance(path, str):
        parts = path.split(".")
    else:
        parts = list(path)
    current: Any = mapping
    for part in parts:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _detect_priority(value: Any) -> Tuple[bool, Optional[str]]:
    if value is None:
        return False, None
    text = str(value).strip()
    if not text:
        return False, None
    if "priority" in text.lower():
        return True, text
    return False, None


def _first_stripped(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
        elif value is not None:
            candidate = str(value).strip()
            if candidate:
                return candidate
    return None


def _extract_field(payload: Mapping[str, Any], keys: Sequence[str]) -> Optional[Any]:
    for key in keys:
        value = _get_nested(payload, key)
        if value not in (None, ""):
            return value
    return None


def _is_placeholder_tail(tail: str) -> bool:
    if not tail:
        return False
    first_word = tail.split()[0]
    return first_word in {"ADD", "REMOVE"}


def _extract_datetime(payload: Mapping[str, Any], options: Sequence[str], *, local_tz: Optional[ZoneInfo]) -> Optional[pd.Timestamp]:
    for option in options:
        value = _get_nested(payload, option)
        if value is None:
            continue
        dt = parse_dt(value, local_tz=local_tz)
        if dt is not pd.NaT:
            return dt
    return None


def _coerce_account_value(value: Any) -> Optional[str]:
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    if isinstance(value, Mapping):
        for nested_key in _ACCOUNT_NESTED_NAME_KEYS:
            nested_value = value.get(nested_key)
            candidate = _coerce_account_value(nested_value)
            if candidate:
                return candidate
        return None
    if value is not None:
        candidate = str(value).strip()
        return candidate or None
    return None


def _extract_account_name(payload: Mapping[str, Any]) -> Optional[str]:
    for key in _ACCOUNT_NAME_KEYS:
        value = _get_nested(payload, key)
        candidate = _coerce_account_value(value)
        if candidate:
            return candidate
    return None


def normalise_flights_for_short_turns(
    flights: Iterable[Mapping[str, Any]],
    *,
    local_tz: Optional[ZoneInfo] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Return a normalised dataframe suitable for short-turn computation."""

    stats = {
        "raw_count": 0,
        "normalised": 0,
        "skipped_non_mapping": 0,
        "skipped_missing_tail": 0,
        "skipped_placeholder_tail": 0,
        "skipped_missing_airports": 0,
        "skipped_missing_dep_airport": 0,
        "skipped_missing_arr_airport": 0,
        "skipped_missing_times": 0,
    }

    dep_time_keys = [
        "scheduledOut",
        "actualOut",
        "outActual",
        "outScheduled",
        "offBlock",
        "offBlockActual",
        "offBlockScheduled",
        "blockOffEstUTC",
        "blockOffEstLocal",
        "blocksoffestimated",
        "departureScheduledTime",
        "departureActualTime",
        "departureTimeScheduled",
        "departureTimeActual",
        "departureScheduledUtc",
        "departureActualUtc",
        "departure.scheduled",
        "departure.actual",
        "departure.scheduledTime",
        "departure.actualTime",
        "departure.scheduledUtc",
        "departure.actualUtc",
        "times.departure.scheduled",
        "times.departure.actual",
        "times.offBlock.scheduled",
        "times.offBlock.actual",
    ]

    arr_time_keys = [
        "scheduledIn",
        "actualIn",
        "inActual",
        "inScheduled",
        "onBlock",
        "onBlockActual",
        "onBlockScheduled",
        "blockOnEstUTC",
        "blockOnEstLocal",
        "blocksonestimated",
        "arrivalScheduledTime",
        "arrivalActualTime",
        "arrivalTimeScheduled",
        "arrivalTimeActual",
        "arrivalScheduledUtc",
        "arrivalActualUtc",
        "arrival.scheduled",
        "arrival.actual",
        "arrival.scheduledTime",
        "arrival.actualTime",
        "arrival.scheduledUtc",
        "arrival.actualUtc",
        "times.arrival.scheduled",
        "times.arrival.actual",
        "times.onBlock.scheduled",
        "times.onBlock.actual",
    ]

    tail_keys = [
        "aircraftRegistration",
        "aircraft.registration",
        "aircraft.reg",
        "aircraft.registrationNumber",
        "registrationNumber",
        "aircraft.tailNumber",
        "aircraft.name",
        "tailNumber",
        "tail",
        "registration",
    ]

    dep_airport_keys = [
        "departureAirportIcao",
        "departureAirport.icao",
        "departure.airportIcao",
        "departure.airport.icao",
        "departureAirport",
        "departure.icao",
        "departure.airport",
        "departureStation",
        "airportFrom",
        "realAirportFrom",
    ]

    arr_airport_keys = [
        "arrivalAirportIcao",
        "arrivalAirport.icao",
        "arrival.airportIcao",
        "arrival.airport.icao",
        "arrivalAirport",
        "arrival.icao",
        "arrival.airport",
        "arrivalStation",
        "airportTo",
        "realAirportTo",
    ]

    leg_id_keys = [
        "bookingIdentifier",
        "booking.identifier",
        "id",
        "uuid",
        "scheduleId",
    ]

    flight_id_keys = [
        "flightId",
        "flight.id",
        "legId",
        "leg_id",
        "scheduleId",
        "id",
        "uuid",
    ]

    booking_code_keys = [
        "bookingCode",
        "booking.code",
        "booking.bookingCode",
        "booking.codeName",
        "bookingCodeName",
        "bookingReference",
        "booking.reference",
    ]

    priority_label_keys = [
        "workflowCustomName",
        "workflow_custom_name",
        "workflowName",
        "workflow",
        "tags",
        "labels",
        "notes",
    ]

    priority_flag_keys = [
        "priority",
        "isPriority",
        "priorityFlight",
        "priority_flag",
        "hasPriority",
    ]

    rows: List[Dict[str, Any]] = []
    local_zone = _resolve_local_tz(local_tz)

    for flight in flights:
        stats["raw_count"] += 1
        if not isinstance(flight, Mapping):
            stats["skipped_non_mapping"] += 1
            continue

        tail = _extract_field(flight, tail_keys)
        dep_ap = _extract_field(flight, dep_airport_keys)
        arr_ap = _extract_field(flight, arr_airport_keys)
        dep_time = _extract_datetime(flight, dep_time_keys, local_tz=local_zone)
        arr_time = _extract_datetime(flight, arr_time_keys, local_tz=local_zone)
        leg_id = _extract_field(flight, leg_id_keys)
        flight_id = _extract_field(flight, flight_id_keys)
        if not flight_id:
            flight_id = leg_id
        booking_code = _extract_field(flight, booking_code_keys)
        account_name = _extract_account_name(flight)
        priority_label = _extract_field(flight, priority_label_keys)
        is_priority, priority_text = _detect_priority(priority_label)
        if not is_priority:
            for flag_key in priority_flag_keys:
                flag_value = _get_nested(flight, flag_key)
                if flag_value is None:
                    continue
                if _coerce_bool(flag_value):
                    is_priority = True
                    if not priority_text:
                        priority_text = _first_stripped(priority_label, "Priority")
                    break

        if isinstance(tail, str):
            tail = tail.upper()
            if _is_placeholder_tail(tail):
                stats["skipped_placeholder_tail"] += 1
                continue
        elif tail is not None:
            tail = str(tail).upper()
            if _is_placeholder_tail(tail):
                stats["skipped_placeholder_tail"] += 1
                continue

        if dep_ap:
            dep_ap = str(dep_ap).upper()
        if arr_ap:
            arr_ap = str(arr_ap).upper()

        if not tail:
            stats["skipped_missing_tail"] += 1
            continue

        missing_airport = False
        if not dep_ap:
            stats["skipped_missing_dep_airport"] += 1
            missing_airport = True
        if not arr_ap:
            stats["skipped_missing_arr_airport"] += 1
            missing_airport = True
        if missing_airport:
            stats["skipped_missing_airports"] += 1
            continue

        if dep_time is None and arr_time is None:
            stats["skipped_missing_times"] += 1
            continue

        rows.append(
            {
                "tail": tail,
                "dep_airport": dep_ap,
                "arr_airport": arr_ap,
                "dep_offblock": dep_time,
                "arr_onblock": arr_time,
                "leg_id": leg_id,
                "flight_id": flight_id,
                "booking_code": booking_code,
                "account_name": account_name,
                "is_priority": is_priority,
                "priority_label": priority_text,
            }
        )
        stats["normalised"] += 1

    if not rows:
        return pd.DataFrame(), stats

    df = pd.DataFrame(rows)
    return df, stats


def compute_short_turns(
    legs: pd.DataFrame,
    threshold_min: int,
    priority_threshold_min: int = _PRIORITY_TURN_THRESHOLD_MIN,
) -> pd.DataFrame:
    if legs.empty:
        return pd.DataFrame(
            columns=[
                "tail",
                "station",
                "arr_leg_id",
                "arr_onblock",
                "dep_leg_id",
                "dep_offblock",
                "turn_min",
                "required_threshold_min",
                "priority_flag",
                "arr_priority_label",
                "dep_priority_label",
                "arr_booking_code",
                "dep_booking_code",
                "same_booking_code",
                "arr_account_name",
                "dep_account_name",
            ]
        )

    legs = legs.copy()
    legs["dep_offblock"] = legs["dep_offblock"].apply(parse_dt)
    legs["arr_onblock"] = legs["arr_onblock"].apply(parse_dt)
    if "flight_id" not in legs.columns:
        legs["flight_id"] = legs.get("leg_id")
    if "booking_code" not in legs.columns:
        legs["booking_code"] = None
    if "is_priority" not in legs.columns:
        legs["is_priority"] = False
    if "priority_label" not in legs.columns:
        legs["priority_label"] = None

    arrs = legs.dropna(subset=["arr_airport", "arr_onblock"]).copy()
    arrs.rename(columns={"arr_airport": "station"}, inplace=True)

    deps = legs.dropna(subset=["dep_airport", "dep_offblock"]).copy()
    deps.rename(columns={"dep_airport": "station"}, inplace=True)

    arrs = arrs[
        [
            "tail",
            "station",
            "arr_onblock",
            "leg_id",
            "flight_id",
            "booking_code",
            "account_name",
            "is_priority",
            "priority_label",
        ]
    ].rename(
        columns={
            "leg_id": "arr_leg_id",
            "flight_id": "arr_flight_id",
            "booking_code": "arr_booking_code",
            "account_name": "arr_account_name",
            "is_priority": "arr_is_priority",
            "priority_label": "arr_priority_label",
        }
    )

    deps = deps[
        [
            "tail",
            "station",
            "dep_offblock",
            "leg_id",
            "flight_id",
            "booking_code",
            "account_name",
            "is_priority",
            "priority_label",
        ]
    ].rename(
        columns={
            "leg_id": "dep_leg_id",
            "flight_id": "dep_flight_id",
            "booking_code": "dep_booking_code",
            "account_name": "dep_account_name",
            "is_priority": "dep_is_priority",
            "priority_label": "dep_priority_label",
        }
    )

    arrs = arrs.sort_values(["tail", "station", "arr_onblock"]).reset_index(drop=True)
    deps = deps.sort_values(["tail", "station", "dep_offblock"]).reset_index(drop=True)

    short_turn_rows: List[Dict[str, Any]] = []

    for (tail, station), arr_grp in arrs.groupby(["tail", "station"], sort=False):
        dep_grp = deps[(deps["tail"] == tail) & (deps["station"] == station)]
        if dep_grp.empty:
            continue
        dep_records = dep_grp.to_dict("records")
        for _, arr_row in arr_grp.iterrows():
            arr_time = arr_row["arr_onblock"]
            next_dep: Optional[Dict[str, Any]] = None
            for dep_row in dep_records:
                dep_time = dep_row.get("dep_offblock")
                if pd.notna(arr_time) and pd.notna(dep_time) and dep_time > arr_time:
                    next_dep = dep_row
                    break
            if next_dep is None:
                continue
            dep_time = next_dep.get("dep_offblock")
            turn_min = (dep_time - arr_time).total_seconds() / 60.0

            arr_priority = bool(arr_row.get("arr_is_priority"))
            dep_priority = bool(next_dep.get("dep_is_priority"))
            priority_flag = dep_priority

            arr_code = arr_row.get("arr_booking_code")
            dep_code = next_dep.get("dep_booking_code")
            same_booking_code = False
            if arr_code and dep_code:
                arr_code_str = str(arr_code).strip().upper()
                dep_code_str = str(dep_code).strip().upper()
                same_booking_code = bool(arr_code_str and arr_code_str == dep_code_str)

            required_threshold = threshold_min
            if priority_flag and not same_booking_code:
                required_threshold = max(threshold_min, priority_threshold_min)

            if turn_min < required_threshold:
                short_turn_rows.append(
                    {
                        "tail": tail,
                        "station": station,
                        "arr_leg_id": arr_row.get("arr_leg_id"),
                        "arr_flight_id": arr_row.get("arr_flight_id"),
                        "arr_onblock": arr_time,
                        "dep_leg_id": next_dep.get("dep_leg_id"),
                        "dep_flight_id": next_dep.get("dep_flight_id"),
                        "dep_offblock": dep_time,
                        "turn_min": round(turn_min, 1),
                        "required_threshold_min": required_threshold,
                        "priority_flag": priority_flag,
                        "arr_priority_label": arr_row.get("arr_priority_label"),
                        "dep_priority_label": next_dep.get("dep_priority_label"),
                        "arr_booking_code": arr_code,
                        "dep_booking_code": dep_code,
                        "same_booking_code": same_booking_code,
                        "arr_is_priority": arr_priority,
                        "dep_is_priority": dep_priority,
                        "arr_account_name": arr_row.get("arr_account_name"),
                        "dep_account_name": next_dep.get("dep_account_name"),
                    }
                )

    short_df = pd.DataFrame(short_turn_rows)
    if not short_df.empty:
        short_df = short_df.sort_values(["turn_min", "tail", "station"]).reset_index(drop=True)
    return short_df


def build_short_turn_summary_text(
    short_turn_df: pd.DataFrame,
    *,
    local_tz: Optional[ZoneInfo] = None,
) -> str:
    """Return a formatted multi-line short-turn summary text block."""

    lines: List[str] = ["Short turns:"]
    if short_turn_df.empty:
        lines.append("None")
        return "\n".join(lines)

    display_df = short_turn_df.copy()
    display_df["arr_onblock"] = display_df["arr_onblock"].apply(
        lambda value: value if isinstance(value, pd.Timestamp) else parse_dt(value, local_tz=local_tz)
    )
    display_df["dep_offblock"] = display_df["dep_offblock"].apply(
        lambda value: value if isinstance(value, pd.Timestamp) else parse_dt(value, local_tz=local_tz)
    )

    tz = _resolve_local_tz(local_tz)

    def _extract_turn_timestamp(row: pd.Series) -> Optional[pd.Timestamp]:
        for key in ("arr_onblock", "dep_offblock"):
            ts = row.get(key)
            if isinstance(ts, pd.Timestamp) and ts is not pd.NaT:
                try:
                    return ts.tz_convert(tz)
                except Exception:
                    return ts
        return None

    def _stringify(value: Any, fallback: str) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return fallback
        text = str(value).strip()
        return text or fallback

    def _format_turn_minutes(value: Any) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "Unknown turn time"
        try:
            minutes = float(value)
        except (TypeError, ValueError):
            return str(value)
        rounded = round(minutes)
        if abs(minutes - rounded) < 0.05:
            rounded_int = int(rounded)
            unit = "min" if rounded_int == 1 else "mins"
            return f"{rounded_int} {unit}"
        return f"{minutes:.1f} mins"

    display_df["__turn_ts"] = display_df.apply(_extract_turn_timestamp, axis=1)
    display_df = display_df.sort_values(
        ["__turn_ts", "turn_min", "tail", "station", "arr_leg_id", "dep_leg_id"],
        kind="mergesort",
    )

    def _format_account(value: Any) -> str:
        candidate = _coerce_account_value(value)
        return candidate or "Unknown Account"

    for _, row in display_df.iterrows():
        tail = _stringify(row.get("tail"), "Unknown tail")
        station = _stringify(row.get("station"), "Unknown station")
        turn_text = _format_turn_minutes(row.get("turn_min"))
        account = _format_account(row.get("dep_account_name") or row.get("arr_account_name"))
        lines.append(f"{tail} – {turn_text} - {station} - {account}")

    return "\n".join(lines)


def summarize_short_turns(
    flights: Iterable[Mapping[str, Any]] | None,
    *,
    threshold_min: int = _DEFAULT_TURN_THRESHOLD_MIN,
    priority_threshold_min: int = _PRIORITY_TURN_THRESHOLD_MIN,
    local_tz: Optional[ZoneInfo] = None,
) -> Tuple[str, pd.DataFrame, Dict[str, Any]]:
    """Return the summary text, dataframe, and metadata for short turns."""

    flights_list = list(flights or [])
    legs_df, normalization_stats = normalise_flights_for_short_turns(flights_list, local_tz=local_tz)
    short_df = compute_short_turns(legs_df, threshold_min, priority_threshold_min)
    summary_text = build_short_turn_summary_text(short_df, local_tz=local_tz)
    metadata = {
        "legs_considered": len(legs_df),
        "normalization": normalization_stats,
        "turns_detected": 0 if short_df.empty else int(len(short_df)),
    }
    return summary_text, short_df, metadata


__all__ = [
    "parse_dt",
    "normalise_flights_for_short_turns",
    "compute_short_turns",
    "build_short_turn_summary_text",
    "summarize_short_turns",
    "DEFAULT_TURN_THRESHOLD_MIN",
    "PRIORITY_TURN_THRESHOLD_MIN",
]
