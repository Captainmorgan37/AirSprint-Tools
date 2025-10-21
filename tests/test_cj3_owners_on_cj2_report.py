import datetime as dt
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import Fl3xxApiConfig
from morning_reports import (
    MorningReportResult,
    _build_cj3_owners_on_cj2_report,
)


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _leg(
    *,
    account: str,
    tail: str,
    dep: dt.datetime,
    leg_id: str,
    quote_id: str,
    flight_id: str,
    booking_identifier: str,
    aircraft_category: str = "C25A",
):
    return {
        "accountName": account,
        "tail": tail,
        "dep_time": iso(dep),
        "leg_id": leg_id,
        "quoteId": quote_id,
        "flightId": flight_id,
        "bookingIdentifier": booking_identifier,
        "flightType": "PAX",
        "aircraftCategory": aircraft_category,
    }


def _stub_fetch(payload_map):
    def _fetch(config, quote_id, session=None):
        return payload_map[quote_id]

    return _fetch


def test_flags_legs_exceeding_thresholds():
    dep = dt.datetime(2024, 5, 10, 12, 0)
    row = _leg(
        account="Owner One",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L1",
        quote_id="Q1",
        flight_id="F100",
        booking_identifier="B100",
    )

    payload_map = {
        "Q1": [
            {
                "planningNotes": "CJ3 owner requesting CJ3",
                "pax": 4,
                "blockTime": 205,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert isinstance(result, MorningReportResult)
    assert result.metadata["match_count"] == 1
    assert result.metadata["flagged_candidates"] == 1
    assert result.metadata["inspected_legs"] == 1
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["pax_count"] == 4
    assert entry["block_time_minutes"] == 205
    assert (
        entry["line"]
        == "2024-05-10-C-GCJ2-B100-Owner One-4-03:25-Threshold exceeded"
    )
    assert entry["threshold_status"] == "Threshold exceeded"
    assert entry["threshold_breached"] is True
    assert entry["threshold_reasons"] == ["Block time at or above limit"]


def test_skips_within_threshold_requests():
    dep = dt.datetime(2024, 6, 1, 9, 0)
    row = _leg(
        account="Owner Two",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L2",
        quote_id="Q2",
        flight_id="F200",
        booking_identifier="B200",
    )

    payload_map = {
        "Q2": [
            {
                "planningNotes": "Owner requesting a CJ3",
                "pax": 4,
                "blockTime": 150,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert result.metadata["flagged_candidates"] == 1
    assert result.metadata["match_count"] == 1
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert (
        entry["line"]
        == "2024-06-01-C-GCJ2-B200-Owner Two-4-02:30-Within thresholds"
    )
    assert entry["threshold_status"] == "Within thresholds"
    assert entry["threshold_breached"] is False
    assert entry["threshold_reasons"] == []


def test_flags_when_threshold_values_met():
    dep = dt.datetime(2024, 6, 2, 12, 0)
    row = _leg(
        account="Owner Three",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L3",
        quote_id="Q3",
        flight_id="F300",
        booking_identifier="B300",
    )

    payload_map = {
        "Q3": [
            {
                "planningNotes": "Owner requesting CJ3",
                "pax": 5,
                "blockTime": 180,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert result.metadata["flagged_candidates"] == 1
    assert result.metadata["match_count"] == 1
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["threshold_breached"] is True
    assert entry["threshold_status"] == "Threshold exceeded"
    assert entry["threshold_reasons"] == [
        "Passenger count at or above limit",
        "Block time at or above limit",
    ]


def test_ignores_cj2_requests():
    dep = dt.datetime(2024, 7, 1, 15, 0)
    row = _leg(
        account="Owner Three",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L3",
        quote_id="Q3",
        flight_id="F300",
        booking_identifier="B300",
    )

    payload_map = {
        "Q3": [
            {
                "planningNotes": "Owner requesting CJ2",
                "pax": 6,
                "blockTime": 240,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert result.metadata["flagged_candidates"] == 0
    assert result.rows == []


def test_missing_quote_id_generates_warning():
    dep = dt.datetime(2024, 8, 1, 8, 0)
    row = {
        "accountName": "Owner Four",
        "tail": "C-GCJ2",
        "dep_time": iso(dep),
        "leg_id": "L4",
        "flightType": "PAX",
        "aircraftCategory": "C25A",
    }

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch({}),
    )

    assert result.rows == []
    assert result.metadata["match_count"] == 0
    assert any("missing quote" in warning.lower() for warning in result.warnings)


def test_includes_runway_alerts_below_threshold():
    dep = dt.datetime(2024, 9, 1, 14, 30)
    row = _leg(
        account="Owner Runway",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L5",
        quote_id="Q5",
        flight_id="F500",
        booking_identifier="B500",
    )
    row["airportFrom"] = "CYBW"
    row["airportTo"] = "CYYC"

    payload_map = {
        "Q5": [
            {
                "planningNotes": "Owner requesting CJ3",
                "pax": 4,
                "blockTime": 170,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    def _runway_lookup(code):
        lookup = {
            "CYBW": 4880,
            "CYYC": 12500,
        }
        if code:
            return lookup.get(code.upper())
        return None

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
        runway_lookup_fn=_runway_lookup,
    )

    assert result.rows
    entry = result.rows[0]
    assert entry["departure_airport"] == "CYBW"
    assert entry["arrival_airport"] == "CYYC"
    alerts = entry["runway_alerts"]
    assert alerts == [
        {
            "role": "Departure",
            "airport": "CYBW",
            "airport_raw": "CYBW",
            "max_runway_length_ft": 4880,
        }
    ]
    assert entry["runway_alert_threshold_ft"] == 4900


def test_sets_runway_confirmation_note_when_all_runways_clear():
    dep = dt.datetime(2024, 9, 2, 9, 45)
    row = _leg(
        account="Owner Runway Clear",
        tail="C-GCJ2",
        dep=dep,
        leg_id="L6",
        quote_id="Q6",
        flight_id="F600",
        booking_identifier="B600",
    )
    row["airportFrom"] = "CYVR"
    row["airportTo"] = "CYEG"

    payload_map = {
        "Q6": [
            {
                "planningNotes": "Owner requesting CJ3",
                "pax": 2,
                "blockTime": 120,
                "departureDateUTC": iso(dep),
            }
        ]
    }

    def _runway_lookup(code):
        lookup = {
            "CYVR": 11500,
            "CYEG": 11000,
        }
        if code:
            return lookup.get(code.upper())
        return None

    result = _build_cj3_owners_on_cj2_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
        runway_lookup_fn=_runway_lookup,
    )

    assert result.rows
    entry = result.rows[0]
    assert entry["runway_alerts"] == []
    assert result.metadata["runway_confirmation_note"] == "All runways confirmed as 4,900' or longer"
