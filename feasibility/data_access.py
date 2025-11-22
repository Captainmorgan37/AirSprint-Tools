"""Lightweight data loaders for feasibility checkers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

from flight_leg_utils import load_airport_metadata_lookup

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

    metadata_lookup = load_airport_metadata_lookup()
    if metadata_lookup:
        for code, metadata in metadata_lookup.items():
            icao = str(code or "").strip().upper()
            if not icao or len(icao) != 4 or icao in lookup:
                continue
            category_record = _derive_category_from_country_metadata(icao=icao, metadata=metadata)
            if category_record:
                lookup[icao] = category_record
    return lookup


def _derive_category_from_country_metadata(icao: str, metadata: Dict[str, Optional[str]]) -> Optional[AirportCategoryRecord]:
    country_raw = str(metadata.get("country") or "").strip()
    subdivision_raw = str(metadata.get("subd") or "").strip()
    if not country_raw:
        return None

    country = country_raw.upper()
    subdivision = subdivision_raw.upper()

    caribbean_countries = {
        "ANTIGUA AND BARBUDA",
        "ARUBA",
        "BAHAMAS",
        "BARBADOS",
        "BELIZE",
        "BERMUDA",
        "BONAIRE",
        "BRITISH VIRGIN ISLANDS",
        "CAYMAN ISLANDS",
        "CUBA",
        "CURACAO",
        "DOMINICA",
        "DOMINICAN REPUBLIC",
        "GRENADA",
        "GUADELOUPE",
        "HAITI",
        "JAMAICA",
        "MARTINIQUE",
        "MONTSERRAT",
        "PUERTO RICO",
        "SABA",
        "SAINT BARTHELEMY",
        "SAINT KITTS AND NEVIS",
        "SAINT LUCIA",
        "SAINT MARTIN",
        "SAINT VINCENT AND THE GRENADINES",
        "SINT EUSTATIUS",
        "SINT MAARTEN",
        "TRINIDAD AND TOBAGO",
        "TURKS AND CAICOS ISLANDS",
        "UNITED STATES VIRGIN ISLANDS",
    }

    if country in {"US", "USA", "UNITED STATES"}:
        if subdivision == "ALASKA":
            return AirportCategoryRecord(
                icao=icao, category="SSA", notes="Alaska airport treated as SSA; SSA handling only."
            )
        if subdivision == "HAWAII":
            return AirportCategoryRecord(
                icao=icao,
                category="OSA",
                notes="Outside service area; final feasibility must be determined in Fl3xx.",
            )
        return AirportCategoryRecord(
            icao=icao,
            category="STANDARD",
            notes="Primary service area airport with no special handling required.",
        )

    if country in {"CANADA", "CA"}:
        if subdivision == "NUNAVUT":
            return AirportCategoryRecord(
                icao=icao, category="SSA", notes="Nunavut airport treated as SSA; SSA handling only."
            )
        return AirportCategoryRecord(
            icao=icao,
            category="STANDARD",
            notes="Primary service area airport with no special handling required.",
        )

    if country == "MEXICO" or country == "MX" or country in caribbean_countries:
        return AirportCategoryRecord(icao=icao, category="SSA", notes="SSA region airport; SSA handling only.")

    return AirportCategoryRecord(
        icao=icao,
        category="OSA",
        notes="Outside service area; final feasibility must be determined in Fl3xx.",
    )


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
