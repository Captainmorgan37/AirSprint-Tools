from datetime import UTC, datetime

from gantt_roster_assignment import assign_roster_to_schedule_rows, roster_window_bounds


def test_roster_window_bounds_uses_minus_10_plus_5_days() -> None:
    now = datetime(2026, 3, 25, 12, 0, tzinfo=UTC)

    start, end = roster_window_bounds(now)

    assert start.isoformat() == "2026-03-15T00:00:00+00:00"
    assert end.isoformat() == "2026-03-30T23:59:00+00:00"


def test_assign_roster_to_schedule_rows_adds_crew_and_positioning() -> None:
    schedule_rows = [
        {
            "tail": "C-FASP",
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
