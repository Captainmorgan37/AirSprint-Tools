from __future__ import annotations

import pathlib
import sys
from datetime import date

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import Fl3xxApiConfig
from hotac_coverage import _status_from_hotac_records, compute_hotac_coverage


def test_status_mapping_prefers_ok_and_flags_missing_documents() -> None:
    status, company, notes = _status_from_hotac_records(
        [
            {
                "status": "CNL",
                "hotacService": {"company": "Old Hotel"},
                "documents": [{"id": 10}],
            },
            {
                "status": "OK",
                "hotacService": {"company": "Main Hotel"},
                "documents": [],
            },
        ]
    )

    assert status == "Booked"
    assert company == "Main Hotel"
    assert "itinerary" in notes


def test_status_mapping_cancelled_only_when_no_active_records() -> None:
    status, company, notes = _status_from_hotac_records(
        [
            {"status": "CNL", "hotacService": {"company": "Hotel A"}},
            {"status": "CANCELLED", "hotacService": {"company": "Hotel B"}},
        ]
    )

    assert status == "Cancelled-only"
    assert company is None
    assert "cancelled" in notes.lower()


def test_compute_hotac_coverage_uses_crew_fetcher_and_last_leg_per_pilot() -> None:
    flights = [
        {
            "flightId": 1,
            "tail": "C-GAAA",
            "flightNumber": "AS100",
            "departureTimeUtc": "2026-01-01T14:00:00Z",
            "arrivalTimeUtc": "2026-01-01T16:00:00Z",
            "arrivalAirport": "CYYC",
        },
        {
            "flightId": 2,
            "tail": "C-GAAA",
            "flightNumber": "AS101",
            "departureTimeUtc": "2026-01-01T18:00:00Z",
            "arrivalTimeUtc": "2026-01-01T20:00:00Z",
            "arrivalAirport": "CYVR",
        },
        {
            "flightId": 3,
            "tail": "C-GBBB",
            "flightNumber": "AS200",
            "departureTimeUtc": "2026-01-01T17:00:00Z",
            "arrivalTimeUtc": "2026-01-01T19:00:00Z",
            "arrivalAirport": "CYYC",
        },
    ]

    def fake_crew(_config, flight_id):
        if flight_id in {1, 2}:
            return [{"role": "CMD", "id": "100", "firstName": "Pat", "lastName": "One"}]
        if flight_id == 3:
            return [{"role": "FO", "id": "200", "firstName": "Sam", "lastName": "Two"}]
        return []

    def fake_services(_config, flight_id):
        if flight_id == 2:
            return {
                "arrivalHotac": [
                    {
                        "status": "OK",
                        "person": {"id": "100"},
                        "hotacService": {"company": "River Hotel"},
                        "documents": [{"id": 1}],
                    }
                ]
            }
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        return {"homeAirport": {"icao": "CYVR"}}

    display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 1, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
    )

    assert len(raw_df) == 2
    assert troubleshooting_df.empty

    pat_row = raw_df.loc[raw_df["Pilot"] == "Pat One"].iloc[0]
    assert pat_row["Flight ID"] == 2
    assert pat_row["HOTAC status"] == "Booked"

    sam_row = raw_df.loc[raw_df["Pilot"] == "Sam Two"].iloc[0]
    assert sam_row["HOTAC status"] == "Missing"

    assert display_df.iloc[0]["HOTAC status"] == "Missing"


def test_compute_hotac_coverage_matches_hotac_person_using_alternate_id_fields() -> None:
    flights = [
        {
            "flightId": 44,
            "tail": "C-GALT",
            "flightNumber": "AS404",
            "departureTimeUtc": "2026-02-01T18:00:00Z",
            "arrivalTimeUtc": "2026-02-01T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "crewId": "crew-395655", "firstName": "Alex", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {
            "arrivalHotac": [
                {
                    "status": "OK",
                    "person": {"userId": 395655},
                    "hotacService": {"company": "River Hotel"},
                    "documents": [{"id": 1}],
                }
            ]
        }

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Booked"
    assert row["Hotel company"] == "River Hotel"


def test_compute_hotac_coverage_reads_alternate_hotac_collection_path() -> None:
    flights = [
        {
            "flightId": 55,
            "tail": "C-GALT",
            "flightNumber": "AS505",
            "departureTimeUtc": "2026-02-01T18:00:00Z",
            "arrivalTimeUtc": "2026-02-01T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "FO", "personnelNumber": "678", "firstName": "Sam", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {
            "flightDetails": {
                "arr": {
                    "hotacs": [
                        {
                            "status": "OK",
                            "person": {"personnelNumber": "678", "pilotRole": "FO"},
                            "hotacService": {"company": "Harbour Hotel"},
                            "documents": [{"id": 1}],
                        }
                    ]
                }
            }
        }

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Booked"
    assert row["Hotel company"] == "Harbour Hotel"


def test_compute_hotac_coverage_missing_notes_include_source_context() -> None:
    flights = [
        {
            "flightId": 66,
            "tail": "C-GALT",
            "flightNumber": "AS606",
            "departureTimeUtc": "2026-02-01T18:00:00Z",
            "arrivalTimeUtc": "2026-02-01T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "101", "firstName": "Casey", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Missing"
    assert "arrivalHotac" in row["Notes"]


def test_compute_hotac_coverage_matches_real_fl3xx_shape_with_pilot_id() -> None:
    flights = [
        {
            "flightId": 1111486,
            "tail": "C-FSNP",
            "flightNumber": "IPDOG",
            "departureTimeUtc": "2026-02-26T17:00:00Z",
            "arrivalTimeUtc": "2026-02-26T19:00:00Z",
            "arrivalAirport": "CYUL",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [
            {
                "pilotId": 938698,
                "firstName": "Francois",
                "lastName": "Doyon",
                "trigram": "FXD",
                "personnelNumber": "1436",
                "role": "FO",
            },
            {
                "pilotId": 545362,
                "firstName": "Alexandre",
                "lastName": "Carriere",
                "trigram": "ACA",
                "personnelNumber": "999",
                "role": "CMD",
            },
        ]

    def fake_services(_config, _flight_id):
        return {
            "arrivalHotac": [
                {
                    "id": 24461535,
                    "status": "OK",
                    "person": {
                        "id": 545362,
                        "firstName": "Alexandre",
                        "lastName": "Carriere",
                        "personnelNumber": "999",
                    },
                    "hotacService": {"company": "FAIRFIELD INN MONTREAL AIRPORT"},
                    "documents": [{"id": 1}],
                },
                {
                    "id": 24461536,
                    "status": "OK",
                    "person": {
                        "id": 938698,
                        "firstName": "Francois",
                        "lastName": "Doyon",
                        "personnelNumber": "1436",
                    },
                    "hotacService": {"company": "FAIRFIELD INN MONTREAL AIRPORT"},
                    "documents": [{"id": 2}],
                },
            ]
        }

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    assert len(raw_df) == 2
    assert set(raw_df["HOTAC status"].tolist()) == {"Booked"}


def test_compute_hotac_coverage_extracts_tail_flight_and_times_from_nested_leg_details() -> None:
    flights = [
        {
            "flightId": 77,
            "tailNumber": "C-FXYZ",
            "flightNumberCompany": "AS777",
            "detailsDeparture": {"scheduledOut": "2026-03-01T10:15:00Z"},
            "detailsArrival": {"scheduledIn": "2026-03-01T12:45:00Z"},
            "arrivalAirport": "CYUL",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "77", "firstName": "Terry", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {
            "arrivalHotac": [
                {
                    "status": "OK",
                    "person": {"id": "77"},
                    "hotacService": {"company": "Airport Hotel"},
                    "documents": [{"id": 1}],
                }
            ]
        }

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    row = raw_df.iloc[0]
    assert row["Tail"] == "C-FXYZ"
    assert row["Flight"] == "AS777"
    assert row["Positioning route"] == ""


def test_compute_hotac_coverage_skips_add_remove_line_flights() -> None:
    flights = [
        {
            "flightId": 701,
            "tail": "ADD LINE",
            "flightNumber": "ADD-209",
            "departureTimeUtc": "2026-03-01T10:00:00Z",
            "arrivalTimeUtc": "2026-03-01T12:00:00Z",
            "arrivalAirport": "CYVR",
        },
        {
            "flightId": 702,
            "tail": "C-GREAL",
            "flightNumber": "AS702",
            "departureTimeUtc": "2026-03-01T13:00:00Z",
            "arrivalTimeUtc": "2026-03-01T15:00:00Z",
            "arrivalAirport": "CYVR",
        },
    ]

    def fake_crew(_config, flight_id):
        assert flight_id == 702
        return [{"role": "CMD", "id": "702", "firstName": "Riley", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    assert len(raw_df) == 1
    assert raw_df.iloc[0]["Flight ID"] == 702


def test_compute_hotac_coverage_uses_booking_identifier_when_flight_number_is_null_like() -> None:
    flights = [
        {
            "flightId": 703,
            "tail": "C-GASL",
            "flightNumber": "209-null",
            "bookingIdentifier": "IPDOG",
            "departureTimeUtc": "2026-03-01T10:00:00Z",
            "arrivalTimeUtc": "2026-03-01T12:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "703", "firstName": "Alex", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    row = raw_df.iloc[0]
    assert row["Flight"] == "IPDOG"



def test_compute_hotac_coverage_marks_home_base_for_missing_canadian_hotac() -> None:
    flights = [
        {
            "flightId": 90,
            "tail": "C-GHOME",
            "flightNumber": "AS900",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "CYUL",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "pilotId": 545362, "firstName": "Alexandre", "lastName": "Carriere"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, crew_id):
        assert str(crew_id) == "545362"
        return {"homeAirport": {"icao": "CYUL"}}

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Home base"
    assert row["Profile home base"] == "CYUL"
    assert "home base" in row["Notes"].lower()
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_skips_home_base_lookup_for_non_canadian_missing_hotac() -> None:
    flights = [
        {
            "flightId": 91,
            "tail": "C-GINTL",
            "flightNumber": "AS901",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "KJFK",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "777", "firstName": "Pat", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        raise AssertionError("crew member lookup should not happen for non-Canadian arrivals")

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Missing"
    assert row["Profile home base"] == ""
    assert troubleshooting_df.empty

def test_compute_hotac_coverage_uses_positioning_roster_hotel_note() -> None:
    flights = [
        {
            "flightId": 120,
            "tail": "C-GPOS",
            "flightNumber": "AS120",
            "departureTimeUtc": "2026-02-26T18:00:00Z",
            "arrivalTimeUtc": "2026-02-26T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "395519", "personnelNumber": "248", "firstName": "Craig", "lastName": "Berntzen"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        return {"homeAirport": {"icao": "CYYC"}}

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {"personnelNumber": "248"},
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYZ"},
                        "notes": "Flight: ...\nHotel: Doubletree Toronto Airport/ CONF#54590540",
                    }
                ],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Booked"
    assert row["Positioning route"] == "CYVR-CYYZ"
    assert "Positioning hotel note" in row["Notes"]


def test_compute_hotac_coverage_uses_positioning_roster_to_home_base() -> None:
    flights = [
        {
            "flightId": 121,
            "tail": "C-GPOS",
            "flightNumber": "AS121",
            "departureTimeUtc": "2026-02-26T18:00:00Z",
            "arrivalTimeUtc": "2026-02-26T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "395519", "personnelNumber": "248", "firstName": "Craig", "lastName": "Berntzen"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        return {"homeAirport": {"icao": "CYYC"}}

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {"personnelNumber": "248"},
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYC"},
                        "notes": "Flight: ...",
                    }
                ],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Home base"
    assert row["Positioning route"] == "CYVR-CYYC"
    assert "Positioned to home base" in row["Notes"]


def test_compute_hotac_coverage_uses_utc_1200_roster_window() -> None:
    flights = [
        {
            "flightId": 130,
            "tail": "C-GUTC",
            "flightNumber": "AS130",
            "departureTimeUtc": "2026-02-26T18:00:00Z",
            "arrivalTimeUtc": "2026-02-26T20:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    captured = {}

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "100", "personnelNumber": "100", "firstName": "UTC", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_roster(_config, from_time, to_time):
        captured["from"] = from_time.isoformat()
        captured["to"] = to_time.isoformat()
        return []

    _display_df, _raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        roster_fetcher=fake_roster,
    )

    assert captured["from"] == "2026-02-26T12:00:00+00:00"
    assert captured["to"] == "2026-02-27T12:00:00+00:00"
