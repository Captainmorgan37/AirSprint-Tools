"""Helper utilities for loading and querying airport de-ice availability."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional

__all__ = ["DeiceRecord", "get_deice_record", "has_deice_available"]

_DEFAULT_DEICE_PATH = Path(__file__).resolve().with_name("Airport Deice info.csv")


@dataclass(frozen=True)
class DeiceRecord:
    """Represents the de-ice status for a single airport."""

    icao: Optional[str]
    iata: Optional[str]
    faa: Optional[str]
    name: Optional[str]
    has_deice: Optional[bool]
    deice_info: Optional[str]
    raw_deice_flag: Optional[str]
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @property
    def status_label(self) -> str:
        """Return a short human readable status string."""

        if self.has_deice is True:
            if self.deice_info:
                return "Deice available"
            return "Deice available (no details)"
        if self.has_deice is False:
            return "No deice available"
        return "No deice information"

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Expose a dictionary version that mirrors the CSV columns."""

        return {
            "ICAO": self.icao,
            "IATA": self.iata,
            "FAA": self.faa,
            "Name": self.name,
            "Latitude": self.latitude,
            "Longitude": self.longitude,
            "Deice Info": self.deice_info,
            "Deice Not Available": self.raw_deice_flag,
        }


def _normalize_identifier(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _clean_multiline_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines: List[str] = [line.rstrip() for line in text.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    if not lines:
        return None
    return "\n".join(lines)


def _parse_deice_flag(value: Optional[str]) -> Optional[bool]:
    normalized = _normalize_identifier(value)
    if normalized == "NO":
        return True
    if normalized == "YES":
        return False
    return None


def _parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=4)
def _load_deice_records(csv_path: Path) -> Dict[str, DeiceRecord]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Could not locate deice data at {path}")

    lookup: Dict[str, DeiceRecord] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            deice_flag_raw = (row.get("Deice Not Available") or "").strip() or None
            record = DeiceRecord(
                icao=_normalize_identifier(row.get("ICAO")),
                iata=_normalize_identifier(row.get("IATA")),
                faa=_normalize_identifier(row.get("FAA")),
                name=row.get("Name") or None,
                latitude=_parse_float(row.get("Latitude")),
                longitude=_parse_float(row.get("Longitude")),
                has_deice=_parse_deice_flag(deice_flag_raw),
                deice_info=_clean_multiline_text(row.get("Deice Info")),
                raw_deice_flag=deice_flag_raw,
            )
            for identifier in (record.icao, record.iata, record.faa):
                if identifier and identifier not in lookup:
                    lookup[identifier] = record
    return lookup


def get_deice_record(
    *,
    icao: Optional[str] = None,
    iata: Optional[str] = None,
    faa: Optional[str] = None,
    identifiers: Optional[Iterable[str]] = None,
    csv_path: Optional[Path] = None,
) -> Optional[DeiceRecord]:
    """Return the first matching :class:`DeiceRecord` using the priority rules."""

    path = Path(csv_path) if csv_path else _DEFAULT_DEICE_PATH
    lookup = _load_deice_records(path)

    normalized_identifiers: List[str] = []
    for code in (icao, iata, faa):
        normalized = _normalize_identifier(code)
        if normalized:
            normalized_identifiers.append(normalized)

    if identifiers is not None:
        for raw in identifiers:
            normalized = _normalize_identifier(raw)
            if normalized:
                normalized_identifiers.append(normalized)

    for identifier in normalized_identifiers:
        record = lookup.get(identifier)
        if record is not None:
            return record
    return None


def has_deice_available(**kwargs) -> Optional[bool]:
    """Convenience wrapper that returns only the availability flag."""

    record = get_deice_record(**kwargs)
    if record is None:
        return None
    return record.has_deice
