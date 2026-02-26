from datetime import UTC, datetime

from roster_pull import build_crew_snapshots, filter_active_roster_rows, parse_roster_payload


def test_parse_roster_payload_supports_escaped_json_string() -> None:
    text = '"[{\\"user\\":{\\"personnelNumber\\":\\"101\\"},\\"entries\\":[],\\"flights\\":[]}]"'

    rows = parse_roster_payload(text)

    assert len(rows) == 1
    assert rows[0]["user"]["personnelNumber"] == "101"


def test_filter_active_roster_rows_drops_empty_activity_rows() -> None:
    rows = [
        {"user": {"personnelNumber": "100"}, "entries": [], "flights": []},
        {"user": {"personnelNumber": "200"}, "entries": [{"type": "A"}], "flights": []},
        {"user": {"personnelNumber": "300"}, "entries": [], "flights": [{"type": "F"}]},
    ]

    filtered = filter_active_roster_rows(rows)

    assert [row["user"]["personnelNumber"] for row in filtered] == ["200", "300"]


def test_build_crew_snapshots_reports_available_and_location() -> None:
    rows = [
        {
            "user": {"personnelNumber": "200", "firstName": "A", "lastName": "Pilot", "trigram": "APL"},
            "entries": [
                {
                    "type": "A",
                    "fromAirport": "CYYZ",
                    "toAirport": "CYYZ",
                    "from": 1704067200000,
                    "to": 1704070800000,
                }
            ],
            "flights": [
                {
                    "type": "F",
                    "fromAirport": "CYYZ",
                    "toAirport": "CYUL",
                    "departureTime": 1704074400000,
                    "arrivalTime": 1704078000000,
                    "aircraftType": "E545",
                }
            ],
        }
    ]

    at_time = datetime(2024, 1, 1, 1, 30, tzinfo=UTC)
    snapshots = build_crew_snapshots(rows, at_time)

    assert len(snapshots) == 1
    assert snapshots[0].personnel_number == "200"
    assert snapshots[0].current_airport == "CYYZ"
    assert snapshots[0].available is False
