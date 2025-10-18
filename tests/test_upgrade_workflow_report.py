import datetime as dt
from typing import Optional

from fl3xx_api import Fl3xxApiConfig
from morning_reports import (
    MorningReportResult,
    _build_upgrade_workflow_validation_report,
)


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _leg(
    *,
    dep: dt.datetime,
    booking_reference: Optional[str],
    aircraft_category: str = "E550",
):
    payload = {
        "dep_time": iso(dep),
        "tail": "C-GLXY",
        "leg_id": "LEG-1",
        "flightType": "PAX",
        "aircraftCategory": aircraft_category,
        "workflowCustomName": "Legacy Upgrade",
        "assignedAircraftType": "Legacy 450",
        "ownerClass": "Legacy Owner",
        "accountName": "Owner Example",
    }
    if booking_reference is not None:
        payload["bookingReference"] = booking_reference
    return payload


def _stub_fetch(payload_map):
    def _fetch(config, booking_reference, session=None):
        return payload_map[booking_reference]

    return _fetch


def test_flags_legacy_with_cj_request():
    dep = dt.datetime(2024, 7, 30, 15, 30)
    row = _leg(dep=dep, booking_reference="BOOK-1")

    payload_map = {
        "BOOK-1": {
            "planningNotes": "8HR INFINITY CJ3 OWNER REQUESTING CJ3",
            "departureDateUTC": iso(dep),
        }
    }

    result = _build_upgrade_workflow_validation_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert isinstance(result, MorningReportResult)
    assert result.metadata == {
        "match_count": 1,
        "inspected_legs": 1,
        "flagged_requests": 1,
    }
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["booking_reference"] == "BOOK-1"
    assert entry["request_label"] == "CJ3"
    assert entry["workflow_matches_upgrade"] is True
    assert entry["assigned_aircraft_type"] == "Legacy 450"
    assert entry["owner_class"] == "Legacy Owner"


def test_skips_when_note_not_requesting_cj():
    dep = dt.datetime(2024, 8, 5, 10, 0)
    row = _leg(dep=dep, booking_reference="BOOK-2")

    payload_map = {
        "BOOK-2": {
            "planningNotes": "Owner requesting Embraer upgrade",
            "departureDateUTC": iso(dep),
        }
    }

    result = _build_upgrade_workflow_validation_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert result.metadata["match_count"] == 0
    assert result.metadata["inspected_legs"] == 1
    assert result.metadata["flagged_requests"] == 0
    assert result.rows == []


def test_warns_when_booking_reference_missing():
    dep = dt.datetime(2024, 9, 1, 12, 0)
    row = _leg(dep=dep, booking_reference=None, aircraft_category="E545")

    result = _build_upgrade_workflow_validation_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch({}),
    )

    assert result.metadata["match_count"] == 0
    assert result.metadata["inspected_legs"] == 0
    assert result.metadata["flagged_requests"] == 0
    assert any("missing booking" in warning.lower() for warning in result.warnings)
