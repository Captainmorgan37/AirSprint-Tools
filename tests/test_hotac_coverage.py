from __future__ import annotations

import pathlib
import sys
from datetime import date

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import Fl3xxApiConfig
from hotac_coverage import (
    _extract_hotel_from_positioning_notes,
    _status_from_hotac_records,
    compute_hotac_coverage,
)


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


def test_status_mapping_unknown_includes_unrecognized_status_and_note() -> None:
    status, company, notes = _status_from_hotac_records(
        [
            {
                "status": "REQ",
                "notes": "NAME CHANGE FROM TFH TO DIJ REQUESTED WITH ELITE // SEE EMAIL 2042805",
            }
        ]
    )

    assert status == "Unsure - unconfirmed status"
    assert company is None
    assert notes == "REQ - NAME CHANGE FROM TFH TO DIJ REQUESTED WITH ELITE // SEE EMAIL 2042805"


def test_status_mapping_unknown_includes_company_when_present() -> None:
    status, company, notes = _status_from_hotac_records(
        [
            {
                "status": "REQ",
                "hotacService": {"company": "Elite Hotels"},
                "notes": "Awaiting confirmation",
            }
        ]
    )

    assert status == "Unsure - unconfirmed status"
    assert company == "Elite Hotels"
    assert notes == "REQ - Awaiting confirmation"


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


def test_compute_hotac_coverage_sorts_status_then_tail() -> None:
    flights = [
        {
            "flightId": 10,
            "tail": "C-GZZZ",
            "flightNumber": "AS910",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "CYVR",
        },
        {
            "flightId": 20,
            "tail": "C-GAAA",
            "flightNumber": "AS920",
            "departureTimeUtc": "2026-03-01T18:10:00Z",
            "arrivalTimeUtc": "2026-03-01T20:10:00Z",
            "arrivalAirport": "CYVR",
        },
    ]

    def fake_crew(_config, flight_id):
        if flight_id == 10:
            return [{"role": "CMD", "id": "10", "firstName": "Zulu", "lastName": "Pilot"}]
        return [{"role": "CMD", "id": "20", "firstName": "Alpha", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
    )

    assert len(raw_df) == 2
    assert list(display_df["Tail"]) == ["C-GAAA", "C-GZZZ"]

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



def test_compute_hotac_coverage_marks_cyhu_cyul_missing_hotac_as_unsure() -> None:
    flights = [
        {
            "flightId": 92,
            "tail": "C-GHUU",
            "flightNumber": "AS902",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "CYHU",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "pilotId": 545363, "firstName": "Jordan", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, crew_id):
        assert str(crew_id) == "545363"
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
    assert row["HOTAC status"] == "Unsure - crew based at CYUL and may be staying at home"
    assert row["Profile home base"] == "CYUL"
    assert "may be staying at home" in row["Notes"].lower()
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_uses_crew_payload_home_base_without_lookup() -> None:
    flights = [
        {
            "flightId": 93,
            "tail": "C-GYYC",
            "flightNumber": "AS903",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "CYYC",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [
            {
                "role": "CMD",
                "firstName": "Taylor",
                "lastName": "Pilot",
                "homeAirport": {"icao": "CYYC"},
            }
        ]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        raise AssertionError("crew member lookup should not happen when crew payload has home base")

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
    assert row["Profile home base"] == "CYYC"
    assert "home base" in row["Notes"].lower()
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_uses_roster_user_home_base_without_crew_member_lookup() -> None:
    flights = [
        {
            "flightId": 94,
            "tail": "C-GASK",
            "flightNumber": "ZAIBK",
            "departureTimeUtc": "2026-03-03T16:00:00Z",
            "arrivalTimeUtc": "2026-03-03T20:00:00Z",
            "arrivalAirport": "KMIA",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [
            {
                "role": "CMD",
                "personnelNumber": "777",
                "firstName": "Andreas",
                "lastName": "Bertoni",
            }
        ]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        raise AssertionError("crew member lookup should not happen when roster user has home base")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "personnelNumber": "777",
                    "homeAirport": {"icao": "CYYC"},
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772558400000,
                        "to": 1772580000000,
                        "fromAirport": {"icao": "KMIA"},
                        "toAirport": {"icao": "CYYC"},
                        "notes": "Positioning",
                    }
                ],
            }
        ]

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 3),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["Profile home base"] == "CYYC"
    assert row["Positioning route"] == "KMIA-CYYC"
    assert row["HOTAC status"] == "Home base"
    assert "Positioned to home base" in row["Notes"]
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_looks_up_home_base_for_non_canadian_end_when_positioning_to_canada() -> None:
    flights = [
        {
            "flightId": 95,
            "tail": "C-GASK",
            "flightNumber": "ZAIBK",
            "departureTimeUtc": "2026-03-03T16:00:00Z",
            "arrivalTimeUtc": "2026-03-03T20:00:00Z",
            "arrivalAirport": "KMIA",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [
            {
                "role": "CMD",
                "pilotId": "545364",
                "personnelNumber": "778",
                "firstName": "Michael",
                "lastName": "Pelletier",
            }
        ]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, crew_id):
        assert str(crew_id) == "545364"
        return {"homeAirport": {"icao": "CYYC"}}

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {"personnelNumber": "778"},
                "entries": [
                    {
                        "type": "P",
                        "from": 1772558400000,
                        "to": 1772580000000,
                        "fromAirport": {"icao": "KMIA"},
                        "toAirport": {"icao": "CYYC"},
                        "notes": "Positioning",
                    }
                ],
            }
        ]

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 3),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["Profile home base"] == "CYYC"
    assert row["Positioning route"] == "KMIA-CYYC"
    assert row["HOTAC status"] == "Home base"
    assert "Positioned to home base" in row["Notes"]
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_skips_home_base_lookup_for_non_canadian_missing_hotac_without_canadian_positioning() -> None:
    flights = [
        {
            "flightId": 96,
            "tail": "C-GINTL",
            "flightNumber": "AS901",
            "departureTimeUtc": "2026-03-01T18:00:00Z",
            "arrivalTimeUtc": "2026-03-01T20:00:00Z",
            "arrivalAirport": "KJFK",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "pilotId": "546000", "personnelNumber": "779", "firstName": "Pat", "lastName": "Pilot"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_crew_member(_config, _crew_id):
        raise AssertionError("crew member lookup should not happen without Canadian end/positioning")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {"personnelNumber": "779"},
                "entries": [
                    {
                        "type": "P",
                        "from": 1772558400000,
                        "to": 1772580000000,
                        "fromAirport": {"icao": "KJFK"},
                        "toAirport": {"icao": "KLAX"},
                        "notes": "Positioning",
                    }
                ],
            }
        ]

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 1),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["HOTAC status"] == "Missing"
    assert row["Profile home base"] == ""
    assert row["Positioning route"] == "KJFK-KLAX"
    assert "hotel required at KLAX" in row["Notes"]
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


def test_extract_hotel_from_positioning_notes_matches_known_hotel_brands_without_hotel_label() -> None:
    note = (
        "CONF# MEAXDC // WS 168 // DEP 1615LT ARR 1717LT\n"
        "HAMPTON INN SUITE - 3916 84TH AVE LEDUC AB T9E 7G1 CA  CONF# 94553989"
    )

    assert (
        _extract_hotel_from_positioning_notes(note)
        == "HAMPTON INN SUITE - 3916 84TH AVE LEDUC AB T9E 7G1 CA  CONF# 94553989"
    )


def test_extract_hotel_from_positioning_notes_does_not_treat_airline_line_as_hotel() -> None:
    note = "CONF# MEAXDC // WS 168 // DEP 1615LT ARR 1717LT"

    assert _extract_hotel_from_positioning_notes(note) == ""


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
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    },
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


def test_compute_hotac_coverage_uses_utc_0800_roster_window() -> None:
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

    assert captured["from"] == "2026-02-26T08:00:00+00:00"
    assert captured["to"] == "2026-02-27T08:00:00+00:00"


def test_compute_hotac_coverage_adds_positioning_only_roster_pilot_with_hotel_note() -> None:
    flights = []

    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "600",
                    "personnelNumber": "600",
                    "firstName": "Row",
                    "lastName": "Only",
                    "pilot": True,
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYZ"},
                        "notes": "Flight: ...\nHotel: Doubletree Toronto Airport/ CONF#54590540",
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    },
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=flights,
        services_fetcher=fake_services,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["Pilot"] == "Row Only"
    assert row["Flight ID"] == ""
    assert row["End airport"] == "CYVR"
    assert row["Positioning route"] == "CYVR-CYYZ"
    assert row["HOTAC status"] == "Booked"
    assert "Positioning hotel note" in row["Notes"]






def test_compute_hotac_coverage_roster_only_positioning_to_home_base_marks_home_base() -> None:
    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "700",
                    "personnelNumber": "700",
                    "firstName": "Home",
                    "lastName": "Bound",
                    "homeAirport": {"icao": "CYYC"},
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYC"},
                        "notes": "Hotel: Should not matter",
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    }
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        services_fetcher=fake_services,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["Pilot"] == "Home Bound"
    assert row["Positioning route"] == "CYVR-CYYC"
    assert row["HOTAC status"] == "Home base"
    assert "Positioned to home base" in row["Notes"]


def test_compute_hotac_coverage_roster_only_home_base_uses_crew_member_lookup() -> None:
    calls = {"count": 0}

    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "710",
                    "personnelNumber": "710",
                    "firstName": "Lookup",
                    "lastName": "Pilot",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYUL"},
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    }
                ],
                "flights": [],
            }
        ]

    def fake_crew_member(_config, crew_id):
        calls["count"] += 1
        assert crew_id == "710"
        return {"homeAirport": {"icao": "CYUL"}}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert calls["count"] == 1
    assert row["Pilot"] == "Lookup Pilot"
    assert row["Profile home base"] == "CYUL"
    assert row["Positioning route"] == "CYVR-CYUL"
    assert row["HOTAC status"] == "Home base"
    assert "Positioned to home base" in row["Notes"]




def test_compute_hotac_coverage_roster_only_home_base_lookup_falls_back_to_personnel() -> None:
    looked_up_ids = []

    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "personnelNumber": "9001",
                    "firstName": "Fallback",
                    "lastName": "Personnel",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYUL"},
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    }
                ],
                "flights": [],
            }
        ]

    def fake_crew_member(_config, crew_id):
        looked_up_ids.append(crew_id)
        assert crew_id == "9001"
        return {"homeAirport": {"icao": "CYUL"}}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert looked_up_ids == ["9001"]
    assert row["HOTAC status"] == "Home base"
    assert row["Profile home base"] == "CYUL"


def test_compute_hotac_coverage_roster_only_home_base_lookup_uses_internal_id() -> None:
    looked_up_ids = []

    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "internalId": 833149,
                    "personnelNumber": "1340",
                    "firstName": "Ryan",
                    "lastName": "Neumann",
                    "acronym": "RSN",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1773146700000,
                        "to": 1773159420000,
                        "fromAirport": {"icao": "CYUL"},
                        "toAirport": {"icao": "CYYZ"},
                        "endsDutyPeriod": True,
                    },
                    {
                        "type": "A",
                        "from": 1773146700000,
                        "to": 1773159420000,
                    }
                ],
                "flights": [],
            }
        ]

    def fake_crew_member(_config, crew_id):
        looked_up_ids.append(str(crew_id))
        if str(crew_id) == "833149":
            return {"homeAirport": {"icao": "CYYZ"}}
        raise AssertionError("lookup should use internalId before personnel")

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 10),
        flights=[],
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert looked_up_ids == ["833149"]
    assert row["Pilot"] == "Ryan Neumann"
    assert row["Personnel/Trigram"] == "1340"
    assert row["Positioning route"] == "CYUL-CYYZ"
    assert row["HOTAC status"] == "Home base"
    assert troubleshooting_df.empty


def test_compute_hotac_coverage_roster_only_home_base_lookup_prefers_internal_id_over_id() -> None:
    looked_up_ids = []

    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "legacy-id",
                    "internalId": 833149,
                    "personnelNumber": "1340",
                    "firstName": "Ryan",
                    "lastName": "Neumann",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1773146700000,
                        "to": 1773159420000,
                        "fromAirport": {"icao": "CYUL"},
                        "toAirport": {"icao": "CYYZ"},
                        "endsDutyPeriod": True,
                    },
                    {
                        "type": "A",
                        "from": 1773146700000,
                        "to": 1773159420000,
                    }
                ],
                "flights": [],
            }
        ]

    def fake_crew_member(_config, crew_id):
        looked_up_ids.append(str(crew_id))
        if str(crew_id) == "833149":
            return {"homeAirport": {"icao": "CYYZ"}}
        raise AssertionError("lookup should prefer internalId")

    _display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 10),
        flights=[],
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert looked_up_ids == ["833149"]
    assert row["HOTAC status"] == "Home base"
    assert troubleshooting_df.empty
def test_compute_hotac_coverage_roster_only_prefers_duty_ending_positioning_event() -> None:
    def fake_services(_config, _flight_id):
        raise AssertionError("services fetch should not happen for roster-only pilot rows")

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "720",
                    "personnelNumber": "720",
                    "firstName": "Duty",
                    "lastName": "End",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYEG"},
                        "endsDutyPeriod": False,
                    },
                    {
                        "type": "P",
                        "from": 1772161200000,
                        "to": 1772172000000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYUL"},
                        "endsDutyPeriod": True,
                    },
                    {
                        "type": "A",
                        "from": 1772161200000,
                        "to": 1772172000000,
                    },
                ],
                "flights": [],
            }
        ]

    def fake_crew_member(_config, crew_id):
        assert crew_id == "720"
        return {"homeAirport": {"icao": "CYUL"}}

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        services_fetcher=fake_services,
        crew_member_fetcher=fake_crew_member,
        roster_fetcher=fake_roster,
    )

    row = raw_df.iloc[0]
    assert row["Positioning route"] == "CYVR-CYUL"
    assert row["End airport"] == "CYVR"
    assert row["HOTAC status"] == "Home base"


def test_compute_hotac_coverage_replaces_earlier_scheduled_leg_with_later_roster_positioning() -> None:
    flights = [
        {
            "flightId": 150,
            "tail": "C-GSCH",
            "flightNumber": "AS150",
            "departureTimeUtc": "2026-03-10T06:00:00Z",
            "arrivalTimeUtc": "2026-03-10T08:00:00Z",
            "arrivalAirport": "CYUL",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "legacy-id", "personnelNumber": "1340", "firstName": "Ryan", "lastName": "Neumann"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "internalId": 833149,
                    "personnelNumber": "1340",
                    "firstName": "Ryan",
                    "lastName": "Neumann",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1773146700000,
                        "to": 1773159420000,
                        "fromAirport": {"icao": "CYUL"},
                        "toAirport": {"icao": "CYYZ"},
                        "endsDutyPeriod": True,
                        "notes": "Hotel: Hilton Garden Inn",
                    },
                    {
                        "type": "A",
                        "from": 1773146700000,
                        "to": 1773159420000,
                    }
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 10),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        roster_fetcher=fake_roster,
    )

    assert len(raw_df) == 1
    row = raw_df.iloc[0]
    assert row["Pilot"] == "Ryan Neumann"
    assert row["Flight ID"] == ""
    assert row["Positioning route"] == "CYUL-CYYZ"
    assert row["HOTAC status"] == "Booked"


def test_compute_hotac_coverage_prefers_roster_duty_end_over_later_next_day_scheduled_leg() -> None:
    flights = [
        {
            "flightId": 160,
            "tail": "C-GNXT",
            "flightNumber": "AS160",
            "departureTimeUtc": "2026-03-11T14:00:00Z",
            "arrivalTimeUtc": "2026-03-11T16:00:00Z",
            "arrivalAirport": "CYVR",
        }
    ]

    def fake_crew(_config, _flight_id):
        return [{"role": "CMD", "id": "legacy-id", "personnelNumber": "1340", "firstName": "Ryan", "lastName": "Neumann"}]

    def fake_services(_config, _flight_id):
        return {"arrivalHotac": []}

    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "internalId": 833149,
                    "personnelNumber": "1340",
                    "firstName": "Ryan",
                    "lastName": "Neumann",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1773146700000,
                        "to": 1773159420000,
                        "fromAirport": {"icao": "CYUL"},
                        "toAirport": {"icao": "CYYZ"},
                        "endsDutyPeriod": True,
                        "notes": "Hotel: HILTON GARDEN INN YYZ",
                    },
                    {
                        "type": "A",
                        "from": 1773146700000,
                        "to": 1773159420000,
                    }
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 3, 10),
        flights=flights,
        crew_fetcher=fake_crew,
        services_fetcher=fake_services,
        roster_fetcher=fake_roster,
    )

    assert len(raw_df) == 1
    row = raw_df.iloc[0]
    assert row["Pilot"] == "Ryan Neumann"
    assert row["Flight ID"] == ""
    assert row["Positioning route"] == "CYUL-CYYZ"
    assert row["HOTAC status"] == "Booked"
def test_compute_hotac_coverage_includes_positioning_only_row_when_role_not_explicit() -> None:
    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "801",
                    "personnelNumber": "801",
                    "firstName": "Unknown",
                    "lastName": "Role",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYZ"},
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    },
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        roster_fetcher=fake_roster,
    )

    assert len(raw_df) == 1
    assert raw_df.iloc[0]["Pilot"] == "Unknown Role"
def test_compute_hotac_coverage_includes_positioning_only_rows_based_on_roster_activity() -> None:
    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "800",
                    "personnelNumber": "800",
                    "firstName": "Cabin",
                    "lastName": "Crew",
                    "pilot": False,
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYVR"},
                        "toAirport": {"icao": "CYYZ"},
                    },
                    {
                        "type": "A",
                        "from": 1772139600000,
                        "to": 1772150400000,
                    },
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        roster_fetcher=fake_roster,
    )

    assert len(raw_df) == 1
    row = raw_df.iloc[0]
    assert row["Pilot"] == "Cabin Crew"
    assert row["Positioning route"] == "CYVR-CYYZ"


def test_compute_hotac_coverage_excludes_positioning_only_rows_without_overlapping_a_day() -> None:
    def fake_roster(_config, _from_time, _to_time):
        return [
            {
                "user": {
                    "id": "802",
                    "personnelNumber": "802",
                    "firstName": "Tyler",
                    "lastName": "Derko",
                },
                "entries": [
                    {
                        "type": "P",
                        "from": 1772139600000,
                        "to": 1772150400000,
                        "fromAirport": {"icao": "CYYC"},
                        "toAirport": {"icao": "YYZ"},
                    },
                    {
                        "type": "OFF",
                        "from": 1772154000000,
                        "to": 1772161200000,
                    },
                ],
                "flights": [],
            }
        ]

    _display_df, raw_df, _troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 2, 26),
        flights=[],
        roster_fetcher=fake_roster,
    )

    assert raw_df.empty
