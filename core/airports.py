"""Utilities for loading airport metadata used by scheduling models."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


def load_airports(path: str | Path) -> Dict[str, Dict[str, object]]:
    """Load airport latitude/longitude and time zone metadata.

    Parameters
    ----------
    path:
        Path to the CSV/TXT file containing airport records. The file is
        expected to expose at least ``icao``, ``lat`` and ``lon`` columns and an
        optional ``tz`` column.

    Returns
    -------
    dict
        Mapping of uppercase ICAO code → metadata dictionary containing the
        numeric ``lat`` and ``lon`` values and the original ``tz`` string if
        present.
    """

    csv_path = Path(path)
    df = pd.read_csv(csv_path, dtype=str)

    for col in ("lat", "lon"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["icao"] = df["icao"].str.upper().str.strip()
    df = df.dropna(subset=["icao", "lat", "lon"])

    airports: Dict[str, Dict[str, object]] = {}
    for _, row in df.iterrows():
        airports[row["icao"]] = {
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "tz": (row.get("tz") or "") if isinstance(row.get("tz"), str) else "",
        }

    return airports
