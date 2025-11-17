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
        "aircraftObj": {"type": "Legacy 450", "category": "SUPER_MIDSIZE_JET"},
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
    assert result["duty"]["total_duty"] == 195
    assert result["duty"]["status"] == "PASS"


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

    assert result["total_duty"] == 960
    assert result["status"] == "CAUTION"
    assert result["split_duty_possible"] is True
    assert result["reset_duty_possible"] is False
