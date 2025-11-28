from __future__ import annotations

from pathlib import Path
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, cast

sys.path.append(str(Path(__file__).resolve().parents[1]))

from feasibility.engine_phase1 import run_feasibility_phase1
from feasibility import checker_weight_balance
from feasibility.duty_module import evaluate_generic_duty_day
from feasibility.models import DayContext


def _build_simple_quote() -> Dict[str, Any]:
    return {
        "bookingIdentifier": "PIURB",
        "bookingid": 987,
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "salesPerson": {"firstName": "Alex", "lastName": "Smith"},
        "workflow": "PRIVATE",
        "workflowCustomName": "FEX Guaranteed",
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
    assert result["workflow"] == "PRIVATE"
    assert result["workflow_custom_name"] == "FEX Guaranteed"
    assert len(result["legs"]) == 2
    assert result["duty"]["total_duty"] == 270
    assert result["duty"]["status"] == "PASS"
    assert all(leg["weightBalance"]["status"] == "PASS" for leg in result["legs"])


def test_phase1_engine_uses_pax_details_fetcher() -> None:
    quote = {
        "bookingIdentifier": "PAX1",
        "legs": [
            {
                "id": "LEG-PAX",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T17:00:00Z",
                "pax": 0,
                "blockTime": 120,
            }
        ],
    }

    fetched_ids: list[str] = []

    def _fetcher(flight_id: str) -> Dict[str, Any]:
        fetched_ids.append(flight_id)
        return {
            "pax": {
                "tickets": [
                    {"paxType": "ADULT", "paxUser": {"gender": "Male"}},
                    {"paxType": "ADULT", "paxUser": {"gender": "Female"}},
                ]
            }
        }

    result = run_feasibility_phase1(
        {"quote": quote, "tz_provider": _tz_provider, "pax_details_fetcher": _fetcher}
    )

    assert fetched_ids == ["LEG-PAX"]
    leg = result["legs"][0]
    details = leg["weightBalance"]["details"]
    assert details["paxCount"] == 2
    assert details["paxBreakdown"]["Male"] == 1
    assert details["paxBreakdown"]["Female"] == 1


def test_phase1_engine_prefers_flight_id_when_fetching_pax_details() -> None:
    quote = {
        "bookingIdentifier": "NESTED-ID",
        "legs": [
            {
                "id": "LEG-WRAPPER",
                "flight": {"id": "FLIGHT-123"},
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T17:00:00Z",
                "pax": 1,
                "blockTime": 120,
            }
        ],
    }

    fetched_ids: list[str] = []

    def _fetcher(flight_id: str) -> Dict[str, Any]:
        fetched_ids.append(flight_id)
        return {
            "pax": {
                "tickets": [
                    {"paxType": "ADULT", "paxUser": {"gender": "Male"}},
                ]
            }
        }

    run_feasibility_phase1(
        {"quote": quote, "tz_provider": _tz_provider, "pax_details_fetcher": _fetcher}
    )


def _workflow_quote(workflow_custom: str, planning_note: str) -> Dict[str, Any]:
    return {
        "bookingIdentifier": "WFLOW",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "workflow": "PRIVATE",
        "workflowCustomName": workflow_custom,
        "legs": [
            {
                "id": "LEG-WORKFLOW",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYEG",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T16:15:00Z",
                "pax": 2,
                "blockTime": 75,
                "planningNotes": planning_note,
            }
        ],
    }


def test_workflow_validation_matches_guaranteed_request() -> None:
    quote = _workflow_quote("Club Guaranteed", "CLUB CJ3 OWNER REQUESTING CJ3")

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any(
        "Workflow 'Club Guaranteed' aligns with planning notes (Guaranteed)" in entry
        for entry in result["validation_checks"]
    )
    assert not any("workflow" in issue.lower() for issue in result["issues"])


def test_workflow_validation_flags_mismatch() -> None:
    quote = _workflow_quote("Club Guaranteed", "INFINITY CJ2 OWNER REQUESTING CJ3")

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any(
        "Workflow 'Club Guaranteed' is Guaranteed but planning notes indicate Interchange" in entry
        for entry in result["validation_checks"]
    )
    assert any("planning notes indicate Interchange" in issue for issue in result["issues"])


def test_workflow_validation_allows_owner_prefix_typos() -> None:
    quote = _workflow_quote(
        "FEX Interchange", "05DEC KPSP - CYEG\n-\n24Club CJ3 owner requesting interchange to EMB"
    )

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any(
        "Workflow 'FEX Interchange' aligns with planning notes (Interchange)" in entry
        for entry in result["validation_checks"]
    )
    assert not any("workflow" in issue.lower() for issue in result["issues"])


def test_as_available_workflow_validates_without_notes() -> None:
    quote = _workflow_quote("FEX As Available", "")

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert "Workflow 'FEX As Available' validated as As Available." in result["validation_checks"]


def test_phase1_engine_uses_flight_info_id_for_pax_details() -> None:
    quote = {
        "bookingIdentifier": "FLIGHT-INFO-ID",
        "legs": [
            {
                "id": "LEG-WRAPPER",
                "flightInfo": {"flightId": "FLIGHT-INFO-999"},
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T17:00:00Z",
                "pax": 1,
                "blockTime": 120,
            }
        ],
    }

    fetched_ids: list[str] = []

    def _fetcher(flight_id: str) -> Dict[str, Any]:
        fetched_ids.append(flight_id)
        return {"pax": {"tickets": [{"paxType": "ADULT"}]}}

    run_feasibility_phase1(
        {"quote": quote, "tz_provider": _tz_provider, "pax_details_fetcher": _fetcher}
    )

    assert fetched_ids == ["FLIGHT-INFO-999"]


def test_phase1_engine_surfaces_pax_details_error() -> None:
    quote = {
        "bookingIdentifier": "PAX-ERR",
        "legs": [
            {
                "id": "LEG-ERR",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2025-11-19T15:00:00Z",
                "arrivalDateUTC": "2025-11-19T17:00:00Z",
                "pax": 2,
                "blockTime": 120,
            }
        ],
    }

    def _fetcher(_: str) -> Dict[str, Any]:
        raise RuntimeError("HTTP 401 Unauthorized: token missing")

    result = run_feasibility_phase1(
        {"quote": quote, "tz_provider": _tz_provider, "pax_details_fetcher": _fetcher}
    )

    leg = result["legs"][0]
    details = leg["weightBalance"]["details"]
    assert details["payloadSource"] == "api_error"
    assert "Unauthorized" in details.get("payloadError", "")


def test_planning_notes_route_mismatch_flags_issue() -> None:
    quote = {
        "bookingIdentifier": "ROUTE-MISMATCH",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-ROUTE",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
                "planningNotes": "22DEC CYKF-KSRQ [ONE-WAY]",
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any("Planning notes route" in issue for issue in result["issues"])
    assert any(
        "Planning notes route" in entry for entry in result.get("validation_checks", [])
    )


def test_planning_notes_route_date_mismatch_flags_issue() -> None:
    quote = {
        "bookingIdentifier": "ROUTE-DATE-MISMATCH",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-ROUTE",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
                "planningNotes": "23DEC CYYC - CYVR",
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any("route date" in issue for issue in result["issues"])
    assert any(
        "route date" in entry for entry in result.get("validation_checks", [])
    )


def test_planning_notes_route_match_no_issue() -> None:
    quote = {
        "bookingIdentifier": "ROUTE-MATCH",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-ROUTE",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
                "planningNotes": "22DEC CYYC - CYVR",
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert not any("Planning notes route" in issue for issue in result["issues"])
    assert any(
        "Planning notes route" in entry and "matches booked" in entry
        for entry in result.get("validation_checks", [])
    )


def test_planning_notes_route_confirmation_surfaces_alongside_other_checks() -> None:
    quote = {
        "bookingIdentifier": "ROUTE-CONFIRM-WITH-OTHER",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "requestedAircraftType": "EMB",
        "legs": [
            {
                "id": "LEG-ROUTE",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
                "planningNotes": "22DEC CYYC - CYVR",
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any(
        "Planning notes route" in entry and "matches booked" in entry
        for entry in result.get("validation_checks", [])
    )
    assert any(
        "Requested aircraft type" in entry
        for entry in result.get("validation_checks", [])
    )


def test_requested_aircraft_type_mismatch_flags_issue() -> None:
    quote = {
        "bookingIdentifier": "REQ-MISMATCH",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "requestedAircraftType": "EMB",
        "legs": [
            {
                "id": "LEG-REQ",
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert any("Requested aircraft type" in issue for issue in result["issues"])
    assert any(
        "Requested aircraft type" in entry
        for entry in result.get("validation_checks", [])
    )


def test_requested_aircraft_synonyms_match_citation_jets() -> None:
    quote = {
        "bookingIdentifier": "REQ-CJ2-EQUIV",
        "aircraftObj": {"type": "CJ2", "category": "LIGHT_JET"},
        "requestedAircraftType": "C25A",
        "legs": [
            {
                "id": "LEG-REQ", 
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert not any(
        "Requested aircraft type" in entry
        for entry in result.get("validation_checks", [])
    )


def test_requested_aircraft_synonyms_match_emb_family() -> None:
    quote = {
        "bookingIdentifier": "REQ-EMB-EQUIV",
        "aircraftObj": {"type": "E550", "category": "LIGHT_JET"},
        "requestedAircraftType": "P500",
        "legs": [
            {
                "id": "LEG-REQ", 
                "departureAirport": "CYYC",
                "arrivalAirport": "CYVR",
                "departureDateUTC": "2026-12-22T15:00:00Z",
                "arrivalDateUTC": "2026-12-22T17:00:00Z",
                "blockTime": 120,
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": _tz_provider})

    assert not any(
        "Requested aircraft type" in entry
        for entry in result.get("validation_checks", [])
    )


def test_flight_category_highlights_osa_routes() -> None:
    quote = {
        "bookingIdentifier": "OSA-TEST",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-OSA-1",
                "departureAirport": "CYYZ",
                "arrivalAirport": "MMMX",
                "departureDateUTC": "2025-11-19T12:00:00Z",
                "arrivalDateUTC": "2025-11-19T16:00:00Z",
                "blockTime": 240,
            },
            {
                "id": "LEG-OSA-2",
                "departureAirport": "MMMX",
                "arrivalAirport": "EGLL",
                "departureDateUTC": "2025-11-20T10:00:00Z",
                "arrivalDateUTC": "2025-11-20T18:00:00Z",
                "blockTime": 480,
            },
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": lambda _icao: None})

    assert result["flight_category"] == "OSA"


def test_flight_category_detects_us_point_to_point() -> None:
    quote = {
        "bookingIdentifier": "US-DOMESTIC",
        "aircraftObj": {"type": "CJ3", "category": "LIGHT_JET"},
        "legs": [
            {
                "id": "LEG-US",
                "departureAirport": "KBOS",
                "arrivalAirport": "KDEN",
                "departureDateUTC": "2025-12-01T12:00:00Z",
                "arrivalDateUTC": "2025-12-01T16:30:00Z",
                "blockTime": 270,
            }
        ],
    }

    result = run_feasibility_phase1({"quote": quote, "tz_provider": lambda _icao: None})

    assert result["flight_category"] == "US point-to-point"


def test_weight_balance_reads_nested_pax_and_cargo_payload() -> None:
    payload = {
        "paxDetails": {
            "tickets": [
                {"paxType": "ADULT", "paxUser": {"gender": "Female"}},
            ]
        },
        "cargoItems": [
            {"weightQty": 260, "note": "PET DOG 6lb"},
        ],
    }

    result = checker_weight_balance.evaluate_weight_balance(
        {"aircraft_type": "C25A"},
        pax_payload=payload,
        aircraft_type="C25A",
        season="Winter",
        payload_source="api",
    )

    details = result.details
    assert details["payloadSource"] == "api"
    assert details["paxCount"] == 1
    assert details["paxBreakdown"]["Female"] == 1
    assert details["cargoWeight"] == 260


def test_weight_balance_reads_deeply_nested_payloads() -> None:
    payload = {
        "payload": {
            "paxPayload": {
                "tickets": [
                    {"paxUser": {"gender": "Female"}},
                    {"paxUser": {"gender": "Male"}},
                ],
                "cargoItems": [
                    {"weightQty": 60},
                    {"weightQty": 200, "note": "PET"},
                ],
            }
        }
    }

    result = checker_weight_balance.evaluate_weight_balance(
        {"aircraft_type": "C25A"},
        pax_payload=payload,
        aircraft_type="C25A",
        season="Winter",
        payload_source="api",
    )

    details = result.details
    assert details["paxCount"] == 2
    assert details["paxBreakdown"]["Male"] == 1
    assert details["paxBreakdown"]["Female"] == 1
    assert details["cargoWeight"] == 260
    assert details["highRiskCargo"] is True


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
    assert "pax" in leg["aircraft"]["summary"].lower()
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
            "workflow": "",
            "workflow_custom_name": "",
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
            "workflow": "",
            "workflow_custom_name": "",
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


def test_reset_duty_day_allows_new_segments() -> None:
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
            "departure_date_utc": "2025-01-01T22:00:00Z",
            "arrival_date_utc": "2025-01-02T02:00:00Z",
        },
    ]
    day = cast(
        DayContext,
        {
            "quote_id": "Q3",
            "bookingIdentifier": "GHI",
            "aircraft_type": "Test",
            "aircraft_category": "",
            "workflow": "",
            "workflow_custom_name": "",
            "legs": legs,
            "sales_contact": None,
            "createdDate": None,
        },
    )

    result = evaluate_generic_duty_day(day)

    assert result["total_duty"] == 1155
    assert result["status"] == "PASS"
    assert result["reset_duty_possible"] is True
    assert "Reset duty day" in result["summary"]
