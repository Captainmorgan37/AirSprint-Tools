"""Lightweight data loaders for feasibility checkers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DATA_DIR = _PROJECT_ROOT / "data"


@dataclass(frozen=True)
class AirportCategoryRecord:
    icao: str
    category: str
    min_ground_time_minutes: Optional[int]
    notes: Optional[str] = None


@lru_cache(maxsize=1)
def load_airport_categories(path: Optional[Path] = None) -> Dict[str, AirportCategoryRecord]:
    csv_path = Path(path) if path else _DATA_DIR / "airport_categories.csv"
    if not csv_path.exists():
        return {}
    lookup: Dict[str, AirportCategoryRecord] = {}
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            icao = (row.get("icao") or "").strip().upper()
            if not icao:
                continue
            category = (row.get("category") or "STANDARD").strip().upper()
            min_ground = row.get("min_ground_time_minutes")
            try:
                min_ground_value = int(min_ground) if min_ground else None
            except ValueError:
                min_ground_value = None
            notes = (row.get("notes") or "").strip() or None
            lookup[icao] = AirportCategoryRecord(
                icao=icao, category=category, min_ground_time_minutes=min_ground_value, notes=notes
            )
    return lookup


@dataclass(frozen=True)
class CustomsRule:
    airport: str
    service_type: Optional[str]
    notes: Optional[str]


@lru_cache(maxsize=1)
def load_customs_rules(path: Optional[Path] = None) -> Dict[str, CustomsRule]:
    csv_path = Path(path) if path else _PROJECT_ROOT / "customs_rules.csv"
    if not csv_path.exists():
        return {}
    lookup: Dict[str, CustomsRule] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            icao = (row.get("airport_icao") or "").strip().upper()
            if not icao:
                continue
            service_type = (row.get("service_type") or "").strip() or None
            notes = (row.get("notes") or "").strip() or None
            lookup[icao] = CustomsRule(airport=icao, service_type=service_type, notes=notes)
    return lookup
