from datetime import UTC, datetime

from crew_presence import crew_at_airport


def test_crew_at_airport_prefers_latest_arrival_and_matches_fleet() -> None:
    roster_rows = [
        {
            "user": {
                "firstName": "Alex",
                "lastName": "Pilot",
                "role": "CMD",
                "fleet": "CJ2",
                "baseAirport": "CYYZ",
            },
            "flights": [
                {
                    "fromAirport": "CYYC",
                    "toAirport": "CYYZ",
                    "departureTime": "2026-03-25T12:00:00Z",
                    "arrivalTime": "2026-03-25T16:00:00Z",
                    "registrationNumber": "C-FASP",
                }
            ],
            "entries": [],
        }
    ]

    results = crew_at_airport(
        roster_rows,
        at_time=datetime(2026, 3, 25, 18, 0, tzinfo=UTC),
        airport="CYYZ",
        fleet="CJ2",
    )

    assert len(results) == 1
    assert results[0].crew_name == "Alex Pilot"
    assert results[0].airport == "CYYZ"
    assert results[0].status == "Arrived by flight"


def test_crew_at_airport_uses_home_base_for_a_day() -> None:
    roster_rows = [
        {
            "user": {
                "firstName": "Sam",
                "lastName": "Reserve",
                "role": "FO",
                "fleet": "CJ2",
                "baseAirport": "CYYZ",
            },
            "flights": [],
            "entries": [
                {
                    "eventType": "A",
                    "start": "2026-03-25T00:00:00Z",
                    "end": "2026-03-25T23:59:00Z",
                }
            ],
        }
    ]

    results = crew_at_airport(
        roster_rows,
        at_time=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        airport="CYYZ",
        fleet="CJ2",
    )

    assert len(results) == 1
    assert results[0].status == "Home/A-day"
