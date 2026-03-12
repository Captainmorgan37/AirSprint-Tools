from __future__ import annotations

import pandas as pd

from customs_port_finder_utils import nearest_customs_ports


def test_nearest_customs_ports_same_country_and_sorted() -> None:
    metadata = {
        "KAAA": {"icao": "KAAA", "country": "US", "lat": 40.0, "lon": -75.0, "name": "Origin", "city": "A"},
        "KBBB": {"icao": "KBBB", "country": "US", "lat": 40.1, "lon": -75.0, "name": "Near 1", "city": "B"},
        "KCCC": {"icao": "KCCC", "country": "US", "lat": 40.5, "lon": -75.0, "name": "Near 2", "city": "C"},
        "CYYY": {"icao": "CYYY", "country": "CA", "lat": 40.05, "lon": -75.0, "name": "Canada", "city": "D"},
    }
    customs_df = pd.DataFrame(
        [
            {"airport_icao": "KAAA", "country": "US"},
            {"airport_icao": "KCCC", "country": "US"},
            {"airport_icao": "KBBB", "country": "US"},
            {"airport_icao": "CYYY", "country": "CA"},
        ]
    )

    origin, results = nearest_customs_ports(
        "KAAA",
        limit=5,
        customs_df=customs_df,
        metadata_lookup=metadata,
    )

    assert origin is not None
    assert [item.airport_code for item in results] == ["KBBB", "KCCC"]


def test_nearest_customs_ports_resolves_iata_input() -> None:
    metadata = {
        "KTEB": {"icao": "KTEB", "country": "US", "lat": 40.85, "lon": -74.06, "name": "Teterboro", "city": "Teterboro"},
        "TEB": {"icao": "KTEB", "country": "US", "lat": 40.85, "lon": -74.06, "name": "Teterboro", "city": "Teterboro"},
        "KJFK": {"icao": "KJFK", "country": "US", "lat": 40.64, "lon": -73.78, "name": "JFK", "city": "New York"},
    }
    customs_df = pd.DataFrame([
        {"airport_icao": "KTEB", "country": "US"},
        {"airport_icao": "KJFK", "country": "US"},
    ])

    origin, results = nearest_customs_ports(
        "TEB",
        customs_df=customs_df,
        metadata_lookup=metadata,
    )

    assert origin is not None
    assert origin["icao"] == "KTEB"
    assert [item.airport_code for item in results] == ["KJFK"]
