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
