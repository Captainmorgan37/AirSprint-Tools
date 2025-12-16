"""Tests for the upgraded flights (pending) report."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

from fl3xx_api import Fl3xxApiConfig
from morning_reports import (
    MorningReportResult,
    _build_upgrade_flights_report,
    run_morning_reports,
)


def iso(ts: dt.datetime) -> str:
    return ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _leg(
    *,
    dep: dt.datetime,
    workflow: str,
    quote_id: Optional[str],
    booking: Optional[str],
    account: Optional[str] = "Acme Corp",
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
    if account is not None:
        payload["accountName"] = account
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
            "planningNotes": "Confirm owner notes align with upgrade workflow",
        }
    }

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    metadata = result.metadata
    assert metadata["match_count"] == 1
    assert metadata["inspected_legs"] == 1
    assert metadata["details_fetched"] == 1
    assert metadata["missing_quote_ids"] == 0
    assert metadata["missing_booking_references"] == 0
    assert metadata["detail_fetch_failures"] == 0
    assert metadata["workflow_summary"] == [
        {"workflow": "Owner Upgrade Pending", "count": 1}
    ]
    assert metadata["upgrade_workflow_summary"] == [
        {"workflow": "Owner Upgrade Pending", "count": 1}
    ]
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["workflow"] == "Owner Upgrade Pending"
    assert entry["planning_note"] == "Confirm owner notes align with upgrade workflow"
    assert entry["upgrade_reason_note"] is None
    assert entry["billing_instruction_note"] is None
    assert entry["account_name"] == "Acme Corp"
    assert "-Acme Corp-" in entry["line"]


def test_highlights_planning_note_details():
    dep = dt.datetime(2024, 8, 10, 12, 0)
    row = _leg(
        dep=dep,
        workflow="Owner Upgrade Request",
        quote_id="Q1",
        booking="BOOK-1",
    )

    payload_map = {
        "Q1": {
            "requestedAircraftType": "CJ3",
            "assignedAircraftType": "Legacy 450",
            "departureDateUTC": iso(dep),
            "planningNotes": (
                "Comp upgraded due to busy CJ schedule. "
                "Bill at 1.5 CJ2 hours please."
            ),
        }
    }

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert isinstance(result, MorningReportResult)
    metadata = result.metadata
    assert metadata["match_count"] == 1
    assert metadata["inspected_legs"] == 1
    assert metadata["details_fetched"] == 1
    assert metadata["missing_quote_ids"] == 0
    assert metadata["missing_booking_references"] == 0
    assert metadata["detail_fetch_failures"] == 0
    assert metadata["workflow_summary"] == [
        {"workflow": "Owner Upgrade Request", "count": 1}
    ]
    assert metadata["upgrade_workflow_summary"] == [
        {"workflow": "Owner Upgrade Request", "count": 1}
    ]
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["booking_reference"] == "BOOK-1"
    assert entry["planning_note"] == (
        "Comp upgraded due to busy CJ schedule. Bill at 1.5 CJ2 hours please."
    )
    assert (
        entry["upgrade_reason_note"]
        == "Comp upgraded due to busy CJ schedule."
    )
    assert (
        entry["billing_instruction_note"]
        == "Bill at 1.5 CJ2 hours please."
    )
    assert entry["account_name"] == "Acme Corp"
    assert entry["line"].startswith("2024-08-10-C-GLXY-BOOK-1-Acme Corp-")


def test_departure_date_normalized_to_mountain_time():
    dep = dt.datetime(2024, 8, 20, 3, 30, tzinfo=dt.timezone.utc)
    row = _leg(
        dep=dep,
        workflow="Owner Upgrade Request",
        quote_id="Q-MTN",
        booking="BOOK-MTN",
    )

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch({}),
    )

    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["date"] == "2024-08-19"
    assert entry["line"].startswith("2024-08-19-C-GLXY-BOOK-MTN-Acme Corp-")


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

    metadata = result.metadata
    assert metadata["match_count"] == 1
    assert metadata["inspected_legs"] == 1
    assert metadata["details_fetched"] == 0
    assert metadata["missing_quote_ids"] == 1
    assert metadata["missing_booking_references"] == 0
    assert metadata["detail_fetch_failures"] == 0
    assert metadata["workflow_summary"] == [
        {"workflow": "Upgrade Workflow", "count": 1}
    ]
    assert metadata["upgrade_workflow_summary"] == [
        {"workflow": "Upgrade Workflow", "count": 1}
    ]
    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["booking_reference"] == "BOOK-2"
    assert entry["quote_id"] is None
    assert entry["planning_note"] is None
    assert entry["upgrade_reason_note"] is None
    assert entry["billing_instruction_note"] is None
    assert entry["account_name"] == "Acme Corp"
    assert any("missing quote" in warning.lower() for warning in result.warnings)


def test_falls_back_to_detail_account_when_missing_on_leg():
    dep = dt.datetime(2024, 11, 1, 15, 0)
    row = _leg(
        dep=dep,
        workflow="Owner Upgrade Request",
        quote_id="Q-ACCOUNT",
        booking="BOOK-ACCOUNT",
        account=None,
    )

    payload_map = {
        "Q-ACCOUNT": {
            "accountName": "Detail Account",
            "assignedAircraftType": "Legacy 450",
            "requestedAircraftType": "Praetor 500",
        }
    }

    result = _build_upgrade_flights_report(
        [row],
        Fl3xxApiConfig(),
        fetch_leg_details_fn=_stub_fetch(payload_map),
    )

    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["account_name"] == "Detail Account"
    assert "-Detail Account-" in entry["line"]


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

    metadata = result.metadata
    assert metadata["match_count"] == 0
    assert metadata["inspected_legs"] == 0
    assert metadata["details_fetched"] == 0
    assert metadata["missing_quote_ids"] == 0
    assert metadata["missing_booking_references"] == 0
    assert metadata["detail_fetch_failures"] == 0
    assert metadata["workflow_summary"] == [
        {"workflow": "Standard Workflow", "count": 1}
    ]
    assert metadata["upgrade_workflow_summary"] == []
    assert result.rows == []


def test_full_run_includes_upgraded_flights_report(monkeypatch):
    dep = dt.datetime(2024, 9, 10, 12, 0)
    workflow_row = _leg(
        dep=dep,
        workflow="Owner Upgrade Request",
        quote_id="Q42",
        booking="BOOK-42",
    )

    flights_payload = [workflow_row]
    fetch_metadata = {"fetched_at": iso(dep)}

    monkeypatch.setattr(
        "morning_reports.build_fl3xx_api_config",
        lambda settings: Fl3xxApiConfig(api_token="token"),
    )
    monkeypatch.setattr(
        "morning_reports.fetch_flights",
        lambda config, from_date, to_date, now: (flights_payload, fetch_metadata),
    )
    monkeypatch.setattr(
        "morning_reports.normalize_fl3xx_payload",
        lambda payload: (flights_payload, {"normalised": 1}),
    )
    monkeypatch.setattr(
        "morning_reports.filter_out_subcharter_rows", lambda rows: (rows, 0)
    )
    monkeypatch.setattr("morning_reports.fetch_postflight", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        "morning_reports.fetch_leg_details",
        _stub_fetch(
            {
                "Q42": {
                    "bookingNote": "Upgrade approved",
                    "requestedAircraftType": "Praetor 500",
                    "assignedAircraftType": "Legacy 450",
                }
            }
        ),
    )

    run = run_morning_reports(
        {"api_token": "token"},
        now=dep,
        from_date=dep.date(),
        to_date=dep.date(),
    )

    report_codes = [report.code for report in run.reports]
    assert "16.1.10" in report_codes
    assert run.metadata.get("report_codes") == report_codes
