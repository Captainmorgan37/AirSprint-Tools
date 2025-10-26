from __future__ import annotations

from datetime import date, datetime, timezone

from flight_following_reports import (
    DutyStartCollection,
    summarize_cyyz_night_operations,
)


def _collection_with_flights(flights_by_tail: dict[str, list[dict]]) -> DutyStartCollection:
    return DutyStartCollection(
        target_date=date(2024, 5, 1),
        start_utc=datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc),
        end_utc=datetime(2024, 5, 2, 0, 0, tzinfo=timezone.utc),
        snapshots=[],
        grouped_flights=flights_by_tail,
    )


def test_summarize_cyyz_night_operations_highlights_arrivals_and_departures():
    collection = _collection_with_flights(
        {
            "CFASP": [
                {
                    "flight_id": "1",
                    "flight_payload": {
                        "tailNumber": "CFASP",
                        "arrivalAirport": "CYYZ",
                        "arrivalActualUtc": "2024-05-02T03:15:00+00:00",
                        "accountName": "OCS",
                    },
                }
            ],
            "CGZAS": [
                {
                    "flight_id": "2",
                    "flight_payload": {
                        "tailNumber": "CGZAS",
                        "departureAirport": "CYYZ",
                        "blockOffEstUTC": "2024-05-02T04:13:00+00:00",
                        "account": {"name": "OCS"},
                    },
                }
            ],
            "CGQRS": [
                {
                    "flight_id": "3",
                    "flight_payload": {
                        "tailNumber": "CGQRS",
                        "arrivalAirport": "CYYZ",
                        "arrivalActualUtc": "2024-05-01T18:00:00+00:00",
                        "accountName": "Daytime Arrival",
                    },
                }
            ],
        }
    )

    lines = summarize_cyyz_night_operations(collection)

    assert lines == [
        "CYYZ Late Arrivals:\nCFASP – 2315 – OCS\n\nCYYZ Late Departures:\nCGZAS – 0013 – OCS",
    ]


def test_summarize_cyyz_night_operations_handles_empty_matches():
    collection = _collection_with_flights(
        {
            "CGAAA": [
                {
                    "flight_id": "4",
                    "flight_payload": {
                        "tailNumber": "CGAAA",
                        "departureAirport": "CYYZ",
                        "blockOffEstUTC": "2024-05-01T16:00:00+00:00",
                        "accountName": "Daytime Departure",
                    },
                }
            ],
            "CFBBB": [
                {
                    "flight_id": "5",
                    "flight_payload": {
                        "tailNumber": "CFBBB",
                        "arrivalAirport": "CYVR",
                        "arrivalActualUtc": "2024-05-02T05:00:00+00:00",
                        "account": {"name": "Other Arrival"},
                    },
                }
            ],
        }
    )

    lines = summarize_cyyz_night_operations(collection)

    assert lines == [
        "CYYZ Late Arrivals:\nNone currently scheduled\n\nCYYZ Late Departures:\nNone currently scheduled",
    ]
