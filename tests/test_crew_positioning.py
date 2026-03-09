from datetime import UTC, datetime

from crew_positioning import build_positioning_statuses


def test_build_positioning_statuses_flags_action_required_for_next_duty() -> None:
    rows = [
        {
            "user": {"personnelNumber": "101", "firstName": "Alex", "lastName": "Pilot", "homeBaseIcao": "CYYZ"},
            "entries": [
                {"type": "A", "fromAirport": "CYVR", "toAirport": "CYVR", "from": 1704067200000, "to": 1704070800000}
            ],
            "flights": [
                {"type": "F", "fromAirport": "CYYZ", "toAirport": "CYUL", "departureTime": 1704074400000, "arrivalTime": 1704078000000}
            ],
        }
    ]

    statuses = build_positioning_statuses(rows, at_time=datetime(2024, 1, 1, 1, 30, tzinfo=UTC))

    assert len(statuses) == 1
    assert statuses[0].status == "ACTION_REQUIRED"
    assert statuses[0].next_required_airport == "CYYZ"


def test_build_positioning_statuses_marks_positioning_booked() -> None:
    rows = [
        {
            "user": {"personnelNumber": "201", "firstName": "Sam", "lastName": "Crew"},
                "entries": [
                    {"type": "A", "fromAirport": "CYVR", "toAirport": "CYVR", "from": 1704067200000, "to": 1704070800000},
                    {"type": "P", "fromAirport": "CYVR", "toAirport": "CYYZ", "from": 1704072600000, "to": 1704074400000},
                ],
            "flights": [
                {"type": "F", "fromAirport": "CYYZ", "toAirport": "CYUL", "departureTime": 1704074400000, "arrivalTime": 1704078000000}
            ],
        }
    ]

    statuses = build_positioning_statuses(rows, at_time=datetime(2024, 1, 1, 1, 10, tzinfo=UTC))

    assert statuses[0].status == "POSITIONING_BOOKED"
    assert statuses[0].booked_positioning_route == "CYVR-CYYZ"


def test_build_positioning_statuses_marks_return_home_required() -> None:
    rows = [
        {
            "user": {"personnelNumber": "301", "firstName": "Jo", "lastName": "Pilot", "homeBaseIcao": "CYYZ"},
            "entries": [
                {"type": "A", "fromAirport": "CYVR", "toAirport": "CYVR", "from": 1704067200000, "to": 1704070800000}
            ],
            "flights": [],
        }
    ]

    statuses = build_positioning_statuses(rows, at_time=datetime(2024, 1, 1, 2, 0, tzinfo=UTC))

    assert statuses[0].status == "RETURN_HOME_REQUIRED"


def test_build_positioning_statuses_marks_return_home_booked() -> None:
    rows = [
        {
            "user": {"personnelNumber": "401", "firstName": "Pat", "lastName": "Pilot", "homeBaseIcao": "CYYZ"},
                "entries": [
                    {"type": "A", "fromAirport": "CYVR", "toAirport": "CYVR", "from": 1704067200000, "to": 1704070800000},
                    {"type": "P", "fromAirport": "CYVR", "toAirport": "CYYZ", "from": 1704072600000, "to": 1704074400000},
                ],
                "flights": [],
            }
        ]

    statuses = build_positioning_statuses(rows, at_time=datetime(2024, 1, 1, 1, 10, tzinfo=UTC))

    assert statuses[0].status == "RETURN_HOME_BOOKED"
    assert statuses[0].booked_positioning_route == "CYVR-CYYZ"
