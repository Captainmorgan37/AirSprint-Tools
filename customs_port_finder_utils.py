"""Helpers for finding nearest customs-capable airports by origin airport code."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Mapping, Optional, Sequence

import pandas as pd

from airport_proximity import haversine_nm
from flight_leg_utils import load_airport_metadata_lookup

PROJECT_ROOT = Path(__file__).resolve().parent
CUSTOMS_RULES_PATH = PROJECT_ROOT / "customs_rules.csv"


@dataclass(frozen=True)
class CustomsPortCandidate:
    airport_code: str
    name: Optional[str]
    city: Optional[str]
    country: str
    distance_nm: float
    service_type: Optional[str]
    agency: Optional[str]
    lead_time_arrival_hours: Optional[float]
    lead_time_departure_hours: Optional[float]
    after_hours_available: Optional[bool]
    open_mon: Optional[str]
    open_tue: Optional[str]
    open_wed: Optional[str]
    open_thu: Optional[str]
    open_fri: Optional[str]
    open_sat: Optional[str]
    open_sun: Optional[str]
    notes: Optional[str]


@lru_cache(maxsize=1)
def load_customs_rules() -> pd.DataFrame:
    if not CUSTOMS_RULES_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(CUSTOMS_RULES_PATH)
    except Exception:
        return pd.DataFrame()


def _normalize_country(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper()


def _as_bool(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1"}:
            return True
        if normalized in {"false", "no", "n", "0"}:
            return False
    return None


def _to_text(value: object) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _to_float(value: object) -> Optional[float]:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(value)
    except Exception:
        return None


def resolve_airport_code(
    airport_code: str,
    *,
    metadata_lookup: Optional[Mapping[str, Mapping[str, object]]] = None,
) -> Optional[dict[str, object]]:
    metadata = metadata_lookup if metadata_lookup is not None else load_airport_metadata_lookup()
    code = str(airport_code or "").strip().upper()
    if not code:
        return None

    record = metadata.get(code)
    if not isinstance(record, Mapping):
        return None

    lat = record.get("lat")
    lon = record.get("lon")
    if not isinstance(lat, (float, int)) or not isinstance(lon, (float, int)):
        return None

    return {
        "code": code,
        "icao": str(record.get("icao") or "").strip().upper() or code,
        "name": _to_text(record.get("name")),
        "city": _to_text(record.get("city")),
        "country": _normalize_country(record.get("country")),
        "lat": float(lat),
        "lon": float(lon),
    }


def nearest_customs_ports(
    origin_airport_code: str,
    *,
    limit: int = 5,
    customs_df: Optional[pd.DataFrame] = None,
    metadata_lookup: Optional[Mapping[str, Mapping[str, object]]] = None,
) -> tuple[Optional[dict[str, object]], list[CustomsPortCandidate]]:
    metadata = metadata_lookup if metadata_lookup is not None else load_airport_metadata_lookup()
    origin = resolve_airport_code(origin_airport_code, metadata_lookup=metadata)
    if origin is None:
        return None, []

    origin_country = _normalize_country(origin.get("country"))
    if not origin_country:
        return origin, []

    df = customs_df if customs_df is not None else load_customs_rules()
    if df.empty:
        return origin, []

    working = df.copy()
    if "airport_icao" not in working.columns or "country" not in working.columns:
        return origin, []

    working["airport_icao"] = working["airport_icao"].astype(str).str.strip().str.upper()
    working["country"] = working["country"].map(_normalize_country)
    working = working[(working["airport_icao"] != "") & (working["country"] == origin_country)]

    candidates: list[CustomsPortCandidate] = []
    for _, row in working.iterrows():
        target_code = str(row.get("airport_icao") or "").strip().upper()
        if not target_code:
            continue

        target = resolve_airport_code(target_code, metadata_lookup=metadata)
        if target is None:
            continue

        if target.get("icao") == origin.get("icao"):
            continue

        distance_nm = haversine_nm(
            float(origin["lat"]),
            float(origin["lon"]),
            float(target["lat"]),
            float(target["lon"]),
        )

        candidates.append(
            CustomsPortCandidate(
                airport_code=str(target.get("icao") or target_code),
                name=_to_text(target.get("name")),
                city=_to_text(target.get("city")),
                country=origin_country,
                distance_nm=distance_nm,
                service_type=_to_text(row.get("service_type")),
                agency=_to_text(row.get("agency")),
                lead_time_arrival_hours=_to_float(row.get("lead_time_arrival_hours")),
                lead_time_departure_hours=_to_float(row.get("lead_time_departure_hours")),
                after_hours_available=_as_bool(row.get("after_hours_available")),
                open_mon=_to_text(row.get("open_mon")),
                open_tue=_to_text(row.get("open_tue")),
                open_wed=_to_text(row.get("open_wed")),
                open_thu=_to_text(row.get("open_thu")),
                open_fri=_to_text(row.get("open_fri")),
                open_sat=_to_text(row.get("open_sat")),
                open_sun=_to_text(row.get("open_sun")),
                notes=_to_text(row.get("notes")),
            )
        )

    deduped: dict[str, CustomsPortCandidate] = {}
    for candidate in sorted(candidates, key=lambda item: (item.distance_nm, item.airport_code)):
        deduped.setdefault(candidate.airport_code, candidate)

    max_results = max(1, int(limit))
    return origin, list(deduped.values())[:max_results]


def candidates_to_dataframe(candidates: Sequence[CustomsPortCandidate]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Airport": [c.airport_code for c in candidates],
            "Name": [c.name for c in candidates],
            "City": [c.city for c in candidates],
            "Distance (nm)": [round(c.distance_nm, 1) for c in candidates],
            "Agency": [c.agency for c in candidates],
            "Service": [c.service_type for c in candidates],
            "Lead Time Arr (hrs)": [c.lead_time_arrival_hours for c in candidates],
            "Lead Time Dep (hrs)": [c.lead_time_departure_hours for c in candidates],
            "After Hours": [c.after_hours_available for c in candidates],
            "Mon": [c.open_mon for c in candidates],
            "Tue": [c.open_tue for c in candidates],
            "Wed": [c.open_wed for c in candidates],
            "Thu": [c.open_thu for c in candidates],
            "Fri": [c.open_fri for c in candidates],
            "Sat": [c.open_sat for c in candidates],
            "Sun": [c.open_sun for c in candidates],
            "Notes": [c.notes for c in candidates],
        }
    )
