"""Address geocoding and nearest-airport lookup helpers for OS workflows."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import requests

from flight_leg_utils import load_airport_metadata_lookup
from feasibility.data_access import load_fl3xx_airport_categories

PROJECT_ROOT = Path(__file__).resolve().parent
RUNWAYS_PATH = PROJECT_ROOT / "runways.csv"


class GeocodingError(RuntimeError):
    """Raised when an address cannot be geocoded."""


@dataclass(frozen=True)
class AirportCandidate:
    icao: str
    iata: Optional[str]
    name: Optional[str]
    city: Optional[str]
    latitude: float
    longitude: float
    distance_nm: float
    max_runway_length_ft: Optional[int]
    airport_category: Optional[str]


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance between two points in nautical miles."""

    earth_radius_nm = 3440.065
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)

    a = sin(d_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(d_lon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return earth_radius_nm * c


@lru_cache(maxsize=256)
def geocode_address_mapbox(
    address: str,
    *,
    token: str,
    limit: int = 1,
    timeout_seconds: float = 10.0,
) -> Tuple[float, float]:
    """Geocode an address using Mapbox Search API and return lat/lon."""

    cleaned = str(address or "").strip()
    if not cleaned:
        raise GeocodingError("Address is required.")
    if not token or not token.strip():
        raise GeocodingError("Mapbox token is missing.")

    url = "https://api.mapbox.com/geocoding/v5/mapbox.places/{}.json".format(requests.utils.quote(cleaned))
    response = requests.get(
        url,
        params={"access_token": token.strip(), "limit": max(1, int(limit))},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json() if response.content else {}
    features = payload.get("features") if isinstance(payload, Mapping) else None
    if not isinstance(features, list) or not features:
        raise GeocodingError(f"No geocoding match found for: {cleaned}")

    center = features[0].get("center") if isinstance(features[0], Mapping) else None
    if not isinstance(center, list) or len(center) < 2:
        raise GeocodingError(f"No coordinate center found for: {cleaned}")

    lon = float(center[0])
    lat = float(center[1])
    return lat, lon


@lru_cache(maxsize=1)
def _load_runway_lengths() -> Mapping[str, int]:
    if not RUNWAYS_PATH.exists():
        return {}
    df = pd.read_csv(RUNWAYS_PATH, usecols=["airport_ident", "length_ft"])
    df["airport_ident"] = df["airport_ident"].astype(str).str.strip().str.upper()
    df["length_ft"] = pd.to_numeric(df["length_ft"], errors="coerce")
    grouped = (
        df.dropna(subset=["airport_ident", "length_ft"])
        .groupby("airport_ident", as_index=False)["length_ft"]
        .max()
    )
    grouped["length_ft"] = grouped["length_ft"].round(0).astype(int)
    return dict(zip(grouped["airport_ident"], grouped["length_ft"]))


@lru_cache(maxsize=1)
def _load_airport_records() -> Tuple[Tuple[str, Optional[str], Optional[str], float, float], ...]:
    metadata = load_airport_metadata_lookup()
    records: List[Tuple[str, Optional[str], Optional[str], float, float]] = []
    for code, entry in metadata.items():
        icao = str(code or "").strip().upper()
        if len(icao) != 4:
            continue
        if not isinstance(entry, Mapping):
            continue
        lat = entry.get("lat")
        lon = entry.get("lon")
        if not isinstance(lat, (float, int)) or not isinstance(lon, (float, int)):
            continue
        records.append(
            (
                icao,
                str(entry.get("name")).strip() if entry.get("name") else None,
                str(entry.get("city")).strip() if entry.get("city") else None,
                float(lat),
                float(lon),
            )
        )
    return tuple(records)


def nearest_airports(
    latitude: float,
    longitude: float,
    *,
    limit: int = 5,
    min_runway_ft: Optional[int] = None,
    allowed_categories: Optional[Sequence[str]] = None,
    airport_records: Optional[Iterable[Tuple[str, Optional[str], Optional[str], float, float]]] = None,
) -> List[AirportCandidate]:
    """Return nearest airports after runway/category filtering."""

    runway_lookup = _load_runway_lengths()
    category_lookup = load_fl3xx_airport_categories()
    normalized_categories = (
        {c.strip().upper() for c in allowed_categories if str(c).strip()}
        if allowed_categories
        else None
    )

    rows = airport_records if airport_records is not None else _load_airport_records()
    results: List[AirportCandidate] = []

    for icao, name, city, lat, lon in rows:
        runway = runway_lookup.get(icao)
        if min_runway_ft is not None and (runway is None or runway < min_runway_ft):
            continue

        category = category_lookup.get(icao).category if icao in category_lookup else None
        normalized_category = category.strip().upper() if isinstance(category, str) and category.strip() else None
        if normalized_categories is not None and normalized_category not in normalized_categories:
            continue

        results.append(
            AirportCandidate(
                icao=icao,
                iata=None,
                name=name,
                city=city,
                latitude=float(lat),
                longitude=float(lon),
                distance_nm=haversine_nm(latitude, longitude, float(lat), float(lon)),
                max_runway_length_ft=runway,
                airport_category=normalized_category,
            )
        )

    results.sort(
        key=lambda candidate: (
            candidate.distance_nm,
            -(candidate.max_runway_length_ft or 0),
            candidate.icao,
        )
    )
    return results[: max(1, int(limit))]
