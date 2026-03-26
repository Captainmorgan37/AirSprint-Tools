from datetime import UTC, datetime

from gantt_roster_assignment import assign_roster_to_schedule_rows, roster_window_bounds


def test_roster_window_bounds_uses_minus_10_plus_5_days() -> None:
    now = datetime(2026, 3, 25, 12, 0, tzinfo=UTC)

    start, end = roster_window_bounds(now)

    assert start.isoformat() == "2026-03-15T00:00:00+00:00"
    assert end.isoformat() == "2026-03-30T23:59:00+00:00"


def test_assign_roster_to_schedule_rows_matches_on_task_id_and_flight_crew() -> None:
    schedule_rows = [
        {
            "tail": "C-GFFS",
            "task_id": "flight_1132575",
            "category": "Client Flight",
            "departure_airport": "CYYJ",
            "arrival_airport": "KPSP",
            "start_utc": datetime(2026, 3, 22, 19, 0, tzinfo=UTC),
            "end_utc": datetime(2026, 3, 22, 21, 56, tzinfo=UTC),
        }
    ]
    roster_rows = [
        {
            "flightId": 1132575,
            "bookingReference": "3989395",
            "flightStatus": "On Block",
            "workflowCustomName": "FEX Guaranteed",
            "paxNumber": 2,
            "registrationNumber": "C-GFFS",
            "blockOffEstUTC": "2026-03-22T19:00:00.000Z",
            "airportFrom": "CYYJ",
            "airportTo": "KPSP",
            "crew": [
                {"firstName": "Michael", "lastName": "Carpenter", "role": "CMD"},
                {"firstName": "Kirk", "lastName": "Wakefield", "role": "FO"},
            ],
        }
    ]

    enriched = assign_roster_to_schedule_rows(schedule_rows, roster_rows)

    assert "Michael Carpenter (CMD)" in enriched[0]["crew"]
    assert "Kirk Wakefield (FO)" in enriched[0]["crew"]
    assert enriched[0]["roster_flight_id"] == "1132575"
    assert enriched[0]["booking_reference"] == "3989395"
    assert enriched[0]["flight_status"] == "On Block"
    assert enriched[0]["workflow_name"] == "FEX Guaranteed"
    assert enriched[0]["pax_number"] == 2


def test_assign_roster_to_schedule_rows_adds_positioning_from_entries() -> None:
    schedule_rows = [
        {
            "tail": "C-FASP",
            "task_id": "flight_100",
            "category": "Client Flight",
            "departure_airport": "CYYZ",
            "arrival_airport": "CYUL",
            "start_utc": datetime(2026, 3, 20, 14, 0, tzinfo=UTC),
            "end_utc": datetime(2026, 3, 20, 15, 0, tzinfo=UTC),
        }
    ]
    roster_rows = [
        {
            "user": {"firstName": "Alex", "lastName": "Pilot"},
            "entries": [
                {
                    "type": "POSITIONING",
                    "fromAirport": "CYVR",
                    "toAirport": "CYYZ",
                    "to": "2026-03-20T11:45:00Z",
                }
            ],
            "flights": [
                {
                    "flightId": 100,
                    "departureTime": "2026-03-20T14:00:00Z",
                    "fromAirport": "CYYZ",
                    "toAirport": "CYUL",
                    "aircraftRegistration": "CFASP",
                }
            ],
        }
    ]

    enriched = assign_roster_to_schedule_rows(schedule_rows, roster_rows)

    assert enriched[0]["crew"] == "Alex Pilot"
    assert "Alex Pilot: CYVR-CYYZ" in enriched[0]["positioning"]


def test_assign_roster_to_schedule_rows_leaves_non_flights_blank() -> None:
    schedule_rows = [
        {
            "tail": "C-FASP",
            "task_id": "task_maintenance_1",
            "category": "Maintenance",
            "departure_airport": "",
            "arrival_airport": "",
            "start_utc": datetime(2026, 3, 20, 14, 0, tzinfo=UTC),
            "end_utc": datetime(2026, 3, 20, 15, 0, tzinfo=UTC),
        }
    ]

    enriched = assign_roster_to_schedule_rows(schedule_rows, [])

    assert enriched[0]["crew"] == ""
    assert enriched[0]["positioning"] == ""
    assert enriched[0]["roster_flight_id"] == ""
