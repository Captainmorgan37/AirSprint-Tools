"""Adapters for sourcing flights/tails from FL3XX or local demos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence, Tuple

import pandas as pd

from core.neg_scheduler import LeverPolicy
from core.neg_scheduler.contracts import Flight, Tail
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    filter_out_subcharter_rows,
    filter_rows_by_departure_window,
    normalize_fl3xx_payload,
    safe_parse_dt,
)
from fl3xx_api import fetch_flights


UTC = timezone.utc
ADD_LINE_PREFIXES = {"ADD", "REMOVE"}
TAILS_PATH = Path(__file__).resolve().parent.parent / "tails.csv"
WINDOW_START_UTC = time(8, 0, tzinfo=UTC)


@dataclass(frozen=True)
class NegotiationData:
    """Structured payload returned when sourcing FL3XX data."""

    flights: list[Flight]
    tails: list[Tail]
    scheduled_rows: list[dict[str, object]]
    unscheduled_rows: list[dict[str, object]]
    metadata: dict[str, object]


def _normalise_tail(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return text.replace("-", "")


def _is_add_line(tail_text: str | None) -> bool:
    if not tail_text:
        return False
    first_token = tail_text.split()[0]
    return first_token in ADD_LINE_PREFIXES


def _load_tail_registry() -> set[str]:
    if not TAILS_PATH.exists():
        return set()
    try:
        df = pd.read_csv(TAILS_PATH)
    except Exception:
        return set()
    if "Tail" not in df.columns:
        return set()
    tails = (
        df["Tail"].astype(str).str.strip().str.upper().str.replace("-", "", regex=False)
    )
    return {tail for tail in tails if tail}


def _classify_rows(
    rows: Iterable[Mapping[str, object]],
    tail_registry: Iterable[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    real_tails = {str(code).strip().upper() for code in tail_registry if str(code).strip()}

    scheduled: list[dict[str, object]] = []
    unscheduled: list[dict[str, object]] = []
    other: list[dict[str, object]] = []

    for row in rows:
        row_dict = dict(row)
        tail_normalised = _normalise_tail(row_dict.get("tail"))
        row_dict["tail_normalized"] = tail_normalised
        is_add = _is_add_line(tail_normalised)
        row_dict["is_add_line"] = is_add
        row_dict["fleet_class"] = _derive_fleet_class(row_dict)

        if tail_normalised and tail_normalised in real_tails:
            scheduled.append(row_dict)
        elif is_add:
            unscheduled.append(row_dict)
        else:
            other.append(row_dict)

    return scheduled, unscheduled, other


def _resolve_settings(settings: Mapping[str, object] | None) -> Mapping[str, object]:
    if settings is not None:
        return settings
    try:
        from Home import get_secret
    except Exception as exc:  # pragma: no cover - streamlit import runtime
        raise FlightDataError("FL3XX API credentials must be supplied via settings or secrets.") from exc

    resolved = get_secret("fl3xx_api")
    if not resolved:
        raise FlightDataError(
            "FL3XX API secrets are not configured; add credentials to `.streamlit/secrets.toml`."
        )
    if not isinstance(resolved, MutableMapping):
        raise FlightDataError("FL3XX API secrets must be a mapping of configuration values.")
    return resolved


def _window_bounds(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, WINDOW_START_UTC)
    end = start + timedelta(days=1)
    return start, end


def _coerce_minutes(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            minutes = int(round(float(value)))
        except (TypeError, ValueError):
            return None
        return minutes if minutes > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if ":" in text:
            parts = text.split(":")
            if len(parts) == 2 and all(part.isdigit() for part in parts):
                hours, minutes = parts
                return int(hours) * 60 + int(minutes)
        try:
            minutes = int(float(text))
        except ValueError:
            return None
        return minutes if minutes > 0 else None
    return None


def _extract_shift_cap(row: Mapping[str, object], keys: Sequence[str]) -> int:
    for key in keys:
        minutes = _coerce_minutes(row.get(key))
        if minutes is not None:
            return minutes
    return 0


FLEET_PATTERN_MAP: list[tuple[str, tuple[str, ...]]] = [
    ("LEG", ("LEGACY", "PRAETOR", "EMB", "EMBRAER")),
    ("CJ2", ("CJ2", "CJ-2", "CJII", "CJ 2", "525A")),
    ("CJ3", ("CJ3", "CJ-3", "CJIII", "CJ 3", "525B", "525C")),
    ("CJ", ("CITATION", "CJ")),
    ("PC12", ("PC12", "PC-12", "PC 12")),
    ("CHALLENGER", ("CHALLENGER", "CL30", "CL-30", "CL 30", "CL350", "CL300")),
    ("HAWKER", ("HAWKER", "H25", "HS125")),
]


def _normalise_fleet_hint(value: object) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    return text.upper()


def _derive_fleet_class(row: Mapping[str, object]) -> str:
    candidates: list[str] = []

    simple_keys = (
        "assignedAircraftType",
        "aircraftCategory",
        "aircraftType",
        "aircraftClass",
        "aircraftModel",
        "aircraftName",
        "aircraftTypeName",
        "ownerClass",
        "ownerClassification",
        "tail",
    )

    for key in simple_keys:
        hint = _normalise_fleet_hint(row.get(key))
        if hint:
            candidates.append(hint)

    nested_sources = (
        row.get("aircraft"),
        row.get("owner"),
    )
    nested_keys = (
        "category",
        "type",
        "typeName",
        "model",
        "name",
        "assignedType",
        "requestedType",
        "aircraftType",
        "aircraftClass",
    )
    for source in nested_sources:
        if not isinstance(source, Mapping):
            continue
        for key in nested_keys:
            hint = _normalise_fleet_hint(source.get(key))
            if hint:
                candidates.append(hint)

    for text in candidates:
        for fleet_class, patterns in FLEET_PATTERN_MAP:
            for pattern in patterns:
                if pattern in text:
                    return fleet_class

    return "GEN"


def _estimate_duration(row: Mapping[str, object], dep_dt: datetime) -> int:
    arrival_raw = row.get("arrival_time")
    if arrival_raw:
        try:
            arr_dt = safe_parse_dt(str(arrival_raw))
        except Exception:
            arr_dt = None
        else:
            if arr_dt.tzinfo is None:
                arr_dt = arr_dt.replace(tzinfo=UTC)
            else:
                arr_dt = arr_dt.astimezone(UTC)
            delta = arr_dt - dep_dt
            minutes = int(delta.total_seconds() // 60)
            if minutes > 0:
                return minutes

    for key in (
        "blockTime",
        "block_time",
        "flightTime",
        "flight_time",
        "scheduledBlockTime",
        "estimatedBlockTime",
        "scheduledBlockMinutes",
        "duration",
        "durationMinutes",
    ):
        minutes = _coerce_minutes(row.get(key))
        if minutes:
            return minutes

    return 90


def _minutes_from_window(reference: datetime, window_start: datetime) -> int:
    offset = reference - window_start
    return int(offset.total_seconds() // 60)


def _flight_identifier(row: Mapping[str, object]) -> str:
    for key in ("leg_id", "flightId", "bookingReference", "bookingId", "id"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return f"LEG-{hash(frozenset(row.items())) & 0xFFFF:04X}"


def _build_flight(
    row: Mapping[str, object],
    *,
    window_start: datetime,
    fixed_tail: bool,
    policy: LeverPolicy,
) -> Flight:
    dep_raw = row.get("dep_time")
    if dep_raw in (None, ""):
        raise FlightDataError("Encountered leg without departure time after normalization.")
    dep_dt = safe_parse_dt(str(dep_raw))
    if dep_dt.tzinfo is None:
        dep_dt = dep_dt.replace(tzinfo=UTC)
    else:
        dep_dt = dep_dt.astimezone(UTC)

    start_min = max(0, min(24 * 60, _minutes_from_window(dep_dt, window_start)))
    duration_min = max(15, min(23 * 60, _estimate_duration(row, dep_dt)))

    fleet_class = row.get("fleet_class") or _derive_fleet_class(row)
    tail_normalised = _normalise_tail(row.get("tail"))
    owner = row.get("accountName") or row.get("account") or row.get("owner") or "Unknown"

    if fixed_tail and not tail_normalised:
        raise FlightDataError("Scheduled leg missing a recognized tail assignment.")

    base_shift_plus = _extract_shift_cap(
        row,
        (
            "shift_plus_cap",
            "shiftPlusCap",
            "shiftPlusMinutes",
            "shiftPlus",
            "shift_plus",
        ),
    )
    base_shift_minus = _extract_shift_cap(
        row,
        (
            "shift_minus_cap",
            "shiftMinusCap",
            "shiftMinusMinutes",
            "shiftMinus",
            "shift_minus",
        ),
    )

    earliest = latest = preferred = start_min
    shift_plus_cap = base_shift_plus or 0
    shift_minus_cap = base_shift_minus or 0
    shift_cost = policy.cost_per_min_shift + 1
    allow_swap = allow_outsource = False

    if not fixed_tail:
        earliest = 0
        latest = 24 * 60
        preferred = start_min
        if shift_plus_cap <= 0:
            shift_plus_cap = 24 * 60
        if shift_minus_cap <= 0:
            shift_minus_cap = 24 * 60
        shift_cost = policy.cost_per_min_shift
        allow_swap = True
        allow_outsource = True

    if fixed_tail:
        shift_plus_cap = max(15, shift_plus_cap or 0)
        shift_minus_cap = max(10, shift_minus_cap or 0)
    else:
        shift_plus_cap = max(90, shift_plus_cap or 0)
        shift_minus_cap = max(30, shift_minus_cap or 0)

    return Flight(
        id=_flight_identifier(row),
        origin=str(row.get("departure_airport") or row.get("dep_airport") or "UNK"),
        dest=str(row.get("arrival_airport") or row.get("arr_airport") or "UNK"),
        duration_min=duration_min,
        earliest_etd_min=earliest,
        latest_etd_min=latest,
        preferred_etd_min=preferred,
        fleet_class=fleet_class,
        owner_id=str(owner),
        requested_start_utc=dep_dt,
        current_tail_id=tail_normalised if fixed_tail else None,
        allow_tail_swap=allow_swap,
        allow_outsource=allow_outsource,
        shift_plus_cap=shift_plus_cap,
        shift_minus_cap=shift_minus_cap,
        shift_cost_per_min=shift_cost,
    )


def fetch_negotiation_data(
    target_date: date,
    *,
    settings: Mapping[str, object] | None = None,
    policy: LeverPolicy | None = None,
) -> NegotiationData:
    """Pull FL3XX flights for the negotiation solver window."""

    resolved_settings = _resolve_settings(settings)
    config = build_fl3xx_api_config(dict(resolved_settings))

    window_start, window_end = _window_bounds(target_date)
    # ``fetch_flights`` expects the ``to`` parameter to be exclusive of the final day.
    to_date_exclusive = (window_end + timedelta(days=1)).date()

    flights_payload, raw_metadata = fetch_flights(
        config,
        from_date=window_start.date(),
        to_date=to_date_exclusive,
    )

    normalized_rows, normalization_stats = normalize_fl3xx_payload({"items": flights_payload})
    filtered_rows, skipped_subcharter = filter_out_subcharter_rows(normalized_rows)
    window_rows, window_stats = filter_rows_by_departure_window(
        filtered_rows, window_start, window_end
    )

    tail_registry = _load_tail_registry()
    scheduled_rows, unscheduled_rows, other_rows = _classify_rows(window_rows, tail_registry)

    flights: list[Flight] = []
    skipped_scheduled: list[str] = []
    skipped_unscheduled: list[str] = []

    policy_obj = policy or LeverPolicy()

    for row in scheduled_rows:
        try:
            flights.append(
                _build_flight(
                    row,
                    window_start=window_start,
                    fixed_tail=True,
                    policy=policy_obj,
                )
            )
        except FlightDataError as exc:
            identifier = _flight_identifier(row)
            skipped_scheduled.append(f"{identifier}: {exc}")

    for row in unscheduled_rows:
        try:
            flights.append(
                _build_flight(
                    row,
                    window_start=window_start,
                    fixed_tail=False,
                    policy=policy_obj,
                )
            )
        except FlightDataError as exc:
            identifier = _flight_identifier(row)
            skipped_unscheduled.append(f"{identifier}: {exc}")

    tail_classes: dict[str, str] = {}
    if tail_registry:
        for tail in tail_registry:
            tail_classes.setdefault(tail, "GEN")

    for row in scheduled_rows:
        tail_id = row.get("tail_normalized")
        if not isinstance(tail_id, str) or not tail_id:
            continue
        fleet_class = row.get("fleet_class") or _derive_fleet_class(row)
        current = tail_classes.get(tail_id)
        if current in (None, "GEN") or fleet_class != "GEN":
            tail_classes[tail_id] = fleet_class

    if not tail_classes:
        # Ensure we have at least one placeholder tail so the solver can run.
        tail_classes["GEN-TAIL"] = "GEN"

    tails = [
        Tail(
            id=tail_id,
            fleet_class=fleet_class,
            available_from_min=0,
            available_to_min=24 * 60,
        )
        for tail_id, fleet_class in sorted(tail_classes.items())
    ]

    metadata: dict[str, object] = {
        "window_start_utc": window_start.isoformat().replace("+00:00", "Z"),
        "window_end_utc": window_end.isoformat().replace("+00:00", "Z"),
        "flights_returned": len(flights_payload),
        "legs_after_subcharter": len(filtered_rows),
        "legs_in_window": len(window_rows),
        "scheduled_count": len(scheduled_rows),
        "unscheduled_count": len(unscheduled_rows),
        "other_count": len(other_rows),
        "skipped_subcharter": skipped_subcharter,
        "skipped_scheduled": skipped_scheduled,
        "skipped_unscheduled": skipped_unscheduled,
        "normalization_stats": normalization_stats,
        "window_counts": window_stats,
    }
    metadata.update({f"raw_{k}": v for k, v in raw_metadata.items()})

    return NegotiationData(
        flights=flights,
        tails=tails,
        scheduled_rows=scheduled_rows,
        unscheduled_rows=unscheduled_rows,
        metadata=metadata,
    )


def get_demo_data() -> Tuple[list[Flight], list[Tail]]:
    """Return a deterministic demo dataset for local prototyping."""

    flights = [
        Flight(
            id="F1",
            owner_id="O100",
            origin="CYBW",
            dest="CYVR",
            duration_min=150,
            fleet_class="CJ",
            earliest_etd_min=7 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=7 * 60 + 15,
        ),
        Flight(
            id="F2",
            owner_id="O220",
            origin="CYVR",
            dest="KSEA",
            duration_min=45,
            fleet_class="CJ",
            earliest_etd_min=9 * 60,
            latest_etd_min=10 * 60,
            preferred_etd_min=9 * 60 + 15,
        ),
        Flight(
            id="F3",
            owner_id="O330",
            origin="CYVR",
            dest="CYUL",
            duration_min=300,
            fleet_class="LEG",
            earliest_etd_min=9 * 60 + 30,
            latest_etd_min=12 * 60,
            preferred_etd_min=10 * 60,
        ),
        Flight(
            id="F4",
            owner_id="O220",
            origin="KSEA",
            dest="CYBW",
            duration_min=90,
            fleet_class="CJ",
            earliest_etd_min=10 * 60,
            latest_etd_min=12 * 60,
            preferred_etd_min=10 * 60 + 30,
        ),
        Flight(
            id="F5",
            owner_id="O555",
            origin="CYBW",
            dest="KDEN",
            duration_min=160,
            fleet_class="CJ",
            earliest_etd_min=8 * 60,
            latest_etd_min=9 * 60,
            preferred_etd_min=8 * 60 + 15,
        ),
    ]

    tails = [
        Tail(id="C-GCJ1", fleet_class="CJ", available_from_min=6 * 60, available_to_min=22 * 60),
        Tail(id="C-GCJ2", fleet_class="CJ", available_from_min=10 * 60 + 30, available_to_min=22 * 60),
        Tail(id="C-GLEG1", fleet_class="LEG", available_from_min=6 * 60, available_to_min=22 * 60),
    ]

    return flights, tails


def fetch_demo_from_fl3xx(day: date | None = None) -> Tuple[list[Flight], list[Tail]]:
    """Placeholder FL3XX fetch that mirrors the production contract."""

    # ``day`` is unused today but retained for interface compatibility.
    return get_demo_data()


def fetch_from_fl3xx(
    day: date,
    *,
    settings: Mapping[str, object] | None = None,
    include_owners: Sequence[str] | None = None,
    policy: LeverPolicy | None = None,
) -> Tuple[list[Flight], list[Tail]]:
    """Compatibility helper returning only the flight/tail lists."""

    _ = include_owners
    data = fetch_negotiation_data(day, settings=settings, policy=policy)
    return data.flights, data.tails
