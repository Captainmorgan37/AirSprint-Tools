"""Lightweight data loaders for feasibility checkers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CustomsRule:
    airport: str
    service_type: Optional[str]
    notes: Optional[str]


@dataclass(frozen=True)
class AirportCategoryRecord:
    icao: str
    category: Optional[str]
    notes: Optional[str]


@dataclass(frozen=True)
class Fl3xxAirportCategory:
    icao: str
    category: Optional[str]


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


@lru_cache(maxsize=1)
def load_airport_categories(path: Optional[Path] = None) -> Dict[str, AirportCategoryRecord]:
    csv_path = Path(path) if path else _PROJECT_ROOT / "data" / "airport_categories.csv"
    if not csv_path.exists():
        return {}
    lookup: Dict[str, AirportCategoryRecord] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            icao = (row.get("airport_ident") or row.get("icao") or "").strip().upper()
            if not icao:
                continue
            category = (row.get("category") or row.get("airport_category") or "").strip() or None
            notes = (row.get("notes") or row.get("remarks") or "").strip() or None
            lookup[icao] = AirportCategoryRecord(icao=icao, category=category, notes=notes)
    return lookup


@lru_cache(maxsize=1)
def load_fl3xx_airport_categories(path: Optional[Path] = None) -> Dict[str, Fl3xxAirportCategory]:
    csv_path = Path(path) if path else _PROJECT_ROOT / "data" / "Airports_Fl3xx_Categories.csv"
    if not csv_path.exists():
        return {}

    def _normalize_category(raw: str) -> Optional[str]:
        if not raw:
            return None
        cleaned = raw.strip().upper()
        if cleaned.startswith("AIRSPRINT INC.:"):
            cleaned = cleaned.split(":", 1)[-1].strip()
        if cleaned in {"A", "B", "C", "NC", "P"}:
            return cleaned
        return cleaned or None

    lookup: Dict[str, Fl3xxAirportCategory] = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            icao = (row.get("ICAO") or "").strip().upper()
            if not icao:
                continue
            category = _normalize_category(row.get("Category") or "")
            lookup[icao] = Fl3xxAirportCategory(icao=icao, category=category)

    return lookup
