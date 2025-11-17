"""Unit tests for the aircraft feasibility checker."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from feasibility import checker_aircraft


def _build_flight(**overrides):
    flight = {
        "aircraftType": "Praetor 500",
        "plannedBlockTime": "6:40",
        "pax": 4,
    }
    flight.update(overrides)
    return flight


def test_praetor_pass_when_margin_is_large():
    result = checker_aircraft.evaluate_aircraft(_build_flight(plannedBlockTime="6:40", pax=4))
    assert result.status == "PASS"
    assert "within pax endurance" in result.summary


def test_praetor_caution_when_close_to_limit():
    result = checker_aircraft.evaluate_aircraft(_build_flight(plannedBlockTime="6:55", pax=4))
    assert result.status == "CAUTION"
    assert "near pax endurance" in result.summary


def test_praetor_fail_when_over_limit():
    result = checker_aircraft.evaluate_aircraft(_build_flight(plannedBlockTime="7:30", pax=4))
    assert result.status == "FAIL"
    assert "exceeds pax endurance" in result.summary


def test_cj2_rejects_too_many_passengers():
    result = checker_aircraft.evaluate_aircraft(
        {
            "aircraftType": "Citation CJ2+",
            "plannedBlockTime": "1:00",
            "pax": 8,
        }
    )
    assert result.status == "FAIL"
    assert "cannot accommodate" in result.summary


def test_missing_pax_triggers_caution_for_supported_profile():
    result = checker_aircraft.evaluate_aircraft(
        {
            "aircraftType": "Citation CJ3+",
            "plannedBlockTime": "3:30",
        }
    )
    assert result.status == "CAUTION"
    assert "passenger count unknown" in result.summary


def test_icao_category_aliases_use_passenger_profiles():
    result = checker_aircraft.evaluate_aircraft(
        {
            "aircraftType": "C25B",
            "plannedBlockTime": "3:30",
            "pax": 5,
        }
    )
    assert result.status == "PASS"
    assert "within pax endurance" in result.summary
    assert any("Limit for 5 pax" in issue for issue in result.issues)


def test_generic_limit_lookup_uses_canonical_alias():
    result = checker_aircraft.evaluate_aircraft(
        {
            "aircraftType": "CJ4",
            "plannedBlockTime": "3:55",
            "pax": 4,
        }
    )
    assert result.status == "FAIL"
    assert "exceeds endurance" in result.summary
