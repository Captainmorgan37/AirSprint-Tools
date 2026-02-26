from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from datetime import date

from fl3xx_api import Fl3xxApiConfig
from hotac_coverage import _status_from_hotac_records, compute_hotac_coverage


def test_status_mapping_prefers_ok_and_flags_missing_documents():
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


def test_status_mapping_cancelled_only_when_no_active_records():
    status, company, notes = _status_from_hotac_records(
        [
            {"status": "CNL", "hotacService": {"company": "Hotel A"}},
            {"status": "CANCELLED", "hotacService": {"company": "Hotel B"}},
        ]
    )

    assert status == "Cancelled-only"
    assert "cancelled" in notes.lower()


def test_compute_hotac_coverage_uses_last_leg_per_pilot_and_sorts_triage_first():
    flights = [
        {
            "flightId": 1,
            "tail": "C-GAAA",
            "flightNumber": "AS100",
            "departureTimeUtc": "2026-01-01T14:00:00Z",
            "arrivalTimeUtc": "2026-01-01T16:00:00Z",
            "arrivalAirport": "CYYC",
            "crew": [
                {"pilot": True, "person": {"id": 100, "firstName": "Pat", "lastName": "One"}},
            ],
        },
        {
            "flightId": 2,
            "tail": "C-GAAA",
            "flightNumber": "AS101",
            "departureTimeUtc": "2026-01-01T18:00:00Z",
            "arrivalTimeUtc": "2026-01-01T20:00:00Z",
            "arrivalAirport": "CYVR",
            "crew": [
                {"pilot": True, "person": {"id": 100, "firstName": "Pat", "lastName": "One"}},
            ],
        },
        {
            "flightId": 3,
            "tail": "C-GBBB",
            "flightNumber": "AS200",
            "departureTimeUtc": "2026-01-01T17:00:00Z",
            "arrivalTimeUtc": "2026-01-01T19:00:00Z",
            "arrivalAirport": "CYYC",
            "crew": [
                {"pilot": True, "person": {"id": 200, "firstName": "Sam", "lastName": "Two"}},
            ],
        },
    ]

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
        if flight_id == 3:
            return {"arrivalHotac": []}
        return {"arrivalHotac": []}

    display_df, raw_df, troubleshooting_df = compute_hotac_coverage(
        Fl3xxApiConfig(),
        date(2026, 1, 1),
        flights=flights,
        services_fetcher=fake_services,
    )

    assert len(raw_df) == 2
    assert troubleshooting_df.empty

    pat_row = raw_df.loc[raw_df["Pilot"] == "Pat One"].iloc[0]
    assert pat_row["Flight ID"] == 2
    assert pat_row["HOTAC status"] == "Booked"

    sam_row = raw_df.loc[raw_df["Pilot"] == "Sam Two"].iloc[0]
    assert sam_row["HOTAC status"] == "Missing"

    assert display_df.iloc[0]["HOTAC status"] == "Missing"
