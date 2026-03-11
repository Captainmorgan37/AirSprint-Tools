"""Tests for historical airport usage helper functions."""

from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from historical_airport_use_utils import (
    extract_airport_code,
    is_atlantic_canada_airport,
    is_positioning_leg,
    leg_duration_hours,
)


def test_extract_airport_code_reads_nested_mapping() -> None:
    leg = {"departure": {"icao": "cyhz"}}
    assert extract_airport_code(leg, ("departure",)) == "CYHZ"


def test_positioning_leg_detection_supports_pos_and_position_words() -> None:
    assert is_positioning_leg({"flightType": "POS"})
    assert is_positioning_leg({"workflowCustomName": "Positioning Ferry"})
    assert not is_positioning_leg({"flightType": "Owner"})


def test_atlantic_canada_detection_checks_country_and_subdivision() -> None:
    lookup = {
        "CYHZ": {"country": "CA", "subd": "NS"},
        "CYYZ": {"country": "CA", "subd": "ON"},
        "KBOS": {"country": "US", "subd": "MA"},
    }
    assert is_atlantic_canada_airport("CYHZ", lookup)
    assert not is_atlantic_canada_airport("CYYZ", lookup)
    assert not is_atlantic_canada_airport("KBOS", lookup)


def test_leg_duration_hours_returns_positive_utc_duration() -> None:
    leg = {
        "dep_time": "2024-01-01T10:00:00Z",
        "arrival_time": "2024-01-01T12:30:00Z",
    }
    assert leg_duration_hours(leg) == 2.5
