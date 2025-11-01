"""Tests for airport reference loading utilities."""

from __future__ import annotations

from pathlib import Path
import sys
from textwrap import dedent

sys.path.append(str(Path(__file__).resolve().parents[1]))

from core.airports import load_airports


def test_load_airports_includes_iata_codes(tmp_path) -> None:
    csv_path = tmp_path / "airports.csv"
    csv_path.write_text(
        dedent(
            """
            icao,iata,lat,lon,tz
            07FA,OCA,25.32432,-80.27573,America/New_York
            ZZZZ,OCA,40.00000,-70.00000,America/New_York
            ABCD,,12.30000,45.60000,Etc/UTC
            """
        ).strip()
    )

    airports = load_airports(csv_path)

    assert "07FA" in airports
    assert "OCA" in airports
    assert airports["OCA"] == airports["07FA"]
    # Ensure the duplicate IATA entry does not overwrite the first mapping.
    assert airports["ZZZZ"] != airports["OCA"]
