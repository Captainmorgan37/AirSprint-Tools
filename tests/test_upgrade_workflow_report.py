import datetime as dt
from typing import Optional

import morning_reports as mr
from fl3xx_api import Fl3xxApiConfig
from morning_reports import (
    MorningReportResult,
    run_morning_reports,
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


def test_upgrade_report_runs_with_morning_reports(monkeypatch):
    departure = dt.datetime(2024, 7, 30, 15, 30, tzinfo=dt.timezone.utc)

    flight_row = {
        "dep_time": iso(departure),
        "tail": "C-GLXY",
        "leg_id": "LEG-1",
        "flightType": "PAX",
        "aircraftCategory": "E550",
        "workflowCustomName": "Legacy Upgrade",
        "assignedAircraftType": "Legacy 450",
        "ownerClass": "Legacy Owner",
        "accountName": "Owner Example",
        "bookingReference": "BOOK-1",
    }

    flights_payload = [flight_row]
    fetch_metadata = {"fetched_at": iso(departure)}

    def fake_build_config(settings):
        assert settings.get("api_token") == "token"
        return Fl3xxApiConfig(api_token="token")

    def fake_fetch_flights(config, from_date, to_date, now):
        assert config.api_token == "token"
        return flights_payload, fetch_metadata

    def fake_normalize(payload):
        assert payload == {"items": flights_payload}
        return flights_payload, {"normalized": len(flights_payload)}

    def fake_filter(rows):
        assert rows == flights_payload
        return rows, 0

    def fake_fetch_notification(config, flight_identifier, session=None):
        return {}

    def fake_fetch_postflight(config, flight_identifier):
        return {}

    def fake_fetch_leg_details(config, booking_reference, session=None):
        assert booking_reference == "BOOK-1"
        return {
            "planningNotes": "Owner requesting CJ3 upgrade",
            "departureDateUTC": iso(departure),
        }

    monkeypatch.setattr("morning_reports.build_fl3xx_api_config", fake_build_config)
    monkeypatch.setattr("morning_reports.fetch_flights", fake_fetch_flights)
    monkeypatch.setattr("morning_reports.normalize_fl3xx_payload", fake_normalize)
    monkeypatch.setattr("morning_reports.filter_out_subcharter_rows", fake_filter)
    monkeypatch.setattr("morning_reports.fetch_flight_notification", fake_fetch_notification)
    monkeypatch.setattr("morning_reports.fetch_postflight", fake_fetch_postflight)

    original_builder = mr._build_upgrade_workflow_validation_report

    def patched_builder(rows, config, *, fetch_leg_details_fn=fake_fetch_leg_details):
        return original_builder(
            rows,
            config,
            fetch_leg_details_fn=fake_fetch_leg_details,
        )

    monkeypatch.setattr(
        "morning_reports._build_upgrade_workflow_validation_report",
        patched_builder,
    )

    run = run_morning_reports(
        {"api_token": "token"},
        now=departure,
        from_date=departure.date(),
        to_date=departure.date(),
    )

    upgrade_report = next(
        report for report in run.reports if report.code == "16.1.9"
    )

    assert upgrade_report.metadata == {
        "match_count": 1,
        "inspected_legs": 1,
        "flagged_requests": 1,
    }
    assert len(upgrade_report.rows) == 1
    assert upgrade_report.rows[0]["booking_reference"] == "BOOK-1"
    assert upgrade_report.rows[0]["request_label"] == "CJ3"
