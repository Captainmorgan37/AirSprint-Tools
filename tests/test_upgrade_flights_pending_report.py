"""Tests for the upgraded flights (pending) report."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

from fl3xx_api import Fl3xxApiConfig
from morning_reports import MorningReportResult, _build_upgrade_flights_report


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _leg(
    *,
    dep: dt.datetime,
    workflow: str,
    quote_id: Optional[str],
    booking: Optional[str],
):
    payload: Dict[str, Any] = {
        "dep_time": iso(dep),
        "tail": "C-GLXY",
        "leg_id": "LEG-1",
        "flightType": "PAX",
        "workflowCustomName": workflow,
        "assignedAircraftType": "Legacy 450",
    }
    if quote_id is not None:
        payload["quoteId"] = quote_id
    if booking is not None:
        payload["bookingReference"] = booking
    return payload


def _stub_fetch(payload_map):
    def _fetch(config, quote_id, session=None):
        return payload_map[quote_id]

    return _fetch


def test_handles_nested_workflow_structures():
    dep = dt.datetime(2024, 8, 15, 8, 30)
    row = _leg(
        dep=dep,
        workflow="",  # replaced below with nested workflow payload
        quote_id="Q42",
        booking="BOOK-42",
    )
    row.pop("workflowCustomName")
    row["workflow"] = {
        "label": "Owner Upgrade Pending",
        "status": "open",
    }

    payload_map = {
        "Q42": {
            "bookingNote": "Needs review of billable hours",
            "requestedAircraftType": "Praetor 500",
            "assignedAircraftType": "Legacy 450",
            "departureDateUTC": iso(dep),
        }
    }

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert result.metadata == {
        "match_count": 1,
        "inspected_legs": 1,
        "details_fetched": 1,
    }
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["workflow"] == "Owner Upgrade Pending"
    assert entry["booking_note"] == "Needs review of billable hours"


def test_includes_booking_note_and_requested_type():
    dep = dt.datetime(2024, 8, 10, 12, 0)
    row = _leg(
        dep=dep,
        workflow="Owner Upgrade Request",
        quote_id="Q1",
        booking="BOOK-1",
    )

    payload_map = {
        "Q1": {
            "bookingNote": "Upgrade approved for billable hours",
            "requestedAircraftType": "CJ3",
            "assignedAircraftType": "Legacy 450",
            "departureDateUTC": iso(dep),
        }
    }

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert isinstance(result, MorningReportResult)
    assert result.metadata == {
        "match_count": 1,
        "inspected_legs": 1,
        "details_fetched": 1,
    }
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["booking_reference"] == "BOOK-1"
    assert entry["requested_aircraft_type"] == "CJ3"
    assert entry["assigned_aircraft_type"] == "Legacy 450"
    assert entry["booking_note"] == "Upgrade approved for billable hours"


def test_missing_quote_id_includes_warning_and_row():
    dep = dt.datetime(2024, 9, 5, 9, 0)
    row = _leg(
        dep=dep,
        workflow="Upgrade Workflow",
        quote_id=None,
        booking="BOOK-2",
    )

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch({}),
    )

    assert result.metadata == {
        "match_count": 1,
        "inspected_legs": 1,
        "details_fetched": 0,
    }
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["booking_reference"] == "BOOK-2"
    assert entry["quote_id"] is None
    assert entry["booking_note"] is None
    assert any("missing quote" in warning.lower() for warning in result.warnings)


def test_non_upgrade_workflows_are_ignored():
    dep = dt.datetime(2024, 8, 20, 14, 0)
    row = _leg(
        dep=dep,
        workflow="Standard Workflow",
        quote_id="Q3",
        booking="BOOK-3",
    )

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch({"Q3": {"bookingNote": "N/A"}}),
    )

    assert result.metadata == {
        "match_count": 0,
        "inspected_legs": 0,
        "details_fetched": 0,
    }
    assert result.rows == []
