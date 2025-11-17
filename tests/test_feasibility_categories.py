"""Tests covering the flight category helper logic used by feasibility checkers."""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.common import (
    OSA_CATEGORY,
    REGULAR_CATEGORY,
    SSA_CATEGORY,
    classify_airport_category,
    classify_flight_category,
)
from flight_leg_utils import load_airport_metadata_lookup


LOOKUP = load_airport_metadata_lookup()


def test_nunavut_airport_classified_as_ssa() -> None:
    result = classify_airport_category("CYFB", LOOKUP)
    assert result.category == SSA_CATEGORY
    assert any("Nunavut" in reason for reason in result.reasons)


def test_canada_domestic_leg_is_regular() -> None:
    result = classify_flight_category("CYYZ", "CYVR", LOOKUP)
    assert result.category == REGULAR_CATEGORY


def test_cross_border_core_leg_is_osa() -> None:
    result = classify_flight_category("CYYZ", "KBOS", LOOKUP)
    assert result.category == OSA_CATEGORY
    assert any("Cross-border" in reason for reason in result.reasons)


def test_mexico_leg_is_ssa() -> None:
    result = classify_flight_category("CYYZ", "MMMX", LOOKUP)
    assert result.category == SSA_CATEGORY


def test_europe_leg_is_osa() -> None:
    result = classify_flight_category("CYYZ", "EGLL", LOOKUP)
    assert result.category == OSA_CATEGORY
