from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict, Optional, cast

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.engine_phase1 import run_feasibility_phase1
from feasibility.duty_module import evaluate_generic_duty_day
from feasibility.models import DayContext


def _build_simple_quote() -> Dict[str, Any]:
    return {
        "bookingIdentifier": "PIURB",
        "bookingid": 987,
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "salesPerson": {"firstName": "Alex", "lastName": "Smith"},
        "legs": [
            {
                "id": "LEG-1",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYEG",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T16:15:00Z",
                "pax": 4,
                "blockTime": 75,
            },
            {
                "id": "LEG-2",
                "departureAirport": "CYEG",
                "arrivalAirport": "CYYC",
                "departureDateUTC": "2025-11-19T17:00:00Z",
                "arrivalDateUTC": "2025-11-19T18:15:00Z",
                "pax": 4,
                "blockTime": 75,
            },
        ],
    }


def _tz_provider(icao: str) -> Optional[str]:
    return {
        "CYYC": "America/Edmonton",
        "CYEG": "America/Edmonton",
    }.get(icao)


def test_phase1_engine_runs_for_entire_quote() -> None:
    quote = _build_simple_quote()
    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert result["bookingIdentifier"] == "PIURB"
    assert len(result["legs"]) == 2
    assert result["duty"]["total_duty"] == 270
    assert result["duty"]["status"] == "PASS"
    assert all(leg["weightBalance"]["status"] == "PASS" for leg in result["legs"])


def test_aircraft_endurance_is_evaluated_for_each_leg() -> None:
    quote = {
        "bookingIdentifier": "ACFT1",
        "aircraftObj": {"type": "Citation CJ3+", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-1",
                "departureAirport": "CYYC",
                "arrivalAirport": "KDEN",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T19:00:00Z",
                "pax": 8,
                "blockTime": 240,
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    leg = result["legs"][0]
    assert leg["aircraft"]["status"] == "FAIL"
    assert "exceeds pax endurance" in leg["aircraft"]["summary"]
    assert leg["weightBalance"]["status"] == "FAIL"
    assert "Overweight" in "".join(leg["weightBalance"].get("issues", []))
    assert result["overall_status"] == "FAIL"
    assert any("Aircraft" in issue for issue in result["issues"])


def test_duty_module_flags_extended_day_and_split_window() -> None:
    legs = [
        {
            "leg_id": "1",
            "departure_icao": "CYYC",
            "arrival_icao": "CYEG",
            "departure_date_utc": "2025-01-01T10:00:00Z",
            "arrival_date_utc": "2025-01-01T12:00:00Z",
        },
        {
            "leg_id": "2",
            "departure_icao": "CYEG",
            "arrival_icao": "CYYC",
            "departure_date_utc": "2025-01-01T18:00:00Z",
            "arrival_date_utc": "2025-01-02T02:00:00Z",
        },
    ]
    day = cast(
        DayContext,
        {
            "quote_id": "Q1",
            "bookingIdentifier": "ABC",
            "aircraft_type": "Test",
            "aircraft_category": "",
            "legs": legs,
            "sales_contact": None,
            "createdDate": None,
        },
    )

    result = evaluate_generic_duty_day(day)

    assert result["total_duty"] == 1035
    assert result["status"] == "FAIL"
    assert result["split_duty_possible"] is True
    assert result["reset_duty_possible"] is False


def test_split_duty_extension_extends_allowable_window() -> None:
    legs = [
        {
            "leg_id": "1",
            "departure_icao": "CYYC",
            "arrival_icao": "CYEG",
            "departure_date_utc": "2025-01-01T08:00:00Z",
            "arrival_date_utc": "2025-01-01T10:00:00Z",
        },
        {
            "leg_id": "2",
            "departure_icao": "CYEG",
            "arrival_icao": "CYYC",
            "departure_date_utc": "2025-01-01T18:00:00Z",
            "arrival_date_utc": "2025-01-01T23:00:00Z",
        },
    ]
    day = cast(
        DayContext,
        {
            "quote_id": "Q2",
            "bookingIdentifier": "DEF",
            "aircraft_type": "Test",
            "aircraft_category": "",
            "legs": legs,
            "sales_contact": None,
            "createdDate": None,
        },
    )

    result = evaluate_generic_duty_day(day)

    assert result["total_duty"] == 975
    assert result["status"] == "PASS"
    assert result["split_duty_possible"] is True
    assert any("allows" in issue for issue in result["issues"])
