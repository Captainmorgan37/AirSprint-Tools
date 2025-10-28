"""Tests for pilot parsing helpers in :mod:`flight_following_reports`."""

from __future__ import annotations

import pathlib
import sys
from datetime import date, datetime, timezone

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from flight_following_reports import (
    FlightFollowingReport,
    _merge_split_duty_information,
    _parse_pilot_blocks,
    build_rest_before_index,
    DutyStartCollection,
    DutyStartPilotSnapshot,
    DutyStartSnapshot,
    summarize_split_duty_days,
    summarize_tight_turnarounds,
)


def test_parse_pilot_blocks_reads_time_dtls2() -> None:
    payload = {
        "time": {
            "dtls2": [
                {
                    "pilotRole": "CMD",
                    "firstName": "Taylor",
                    "lastName": "Captain",
                    "userId": "P1",
                    "fullDutyState": {
                        "fdp": {"actual": 600, "max": 720},
                        "explainerMap": {
                            "ACTUAL_FDP": {
                                "header": "Actual FDP = 10h",
                                "text": ["Break = 01:00"],
                            }
                        },
                    },
                },
                {
                    "pilotRole": "FO",
                    "firstName": "Jordan",
                    "lastName": "Copilot",
                    "userId": "P2",
                },
            ]
        }
    }

    pilots = _parse_pilot_blocks(payload)

    assert len(pilots) == 2
    assert all(isinstance(pilot, DutyStartPilotSnapshot) for pilot in pilots)
    identifiers = {pilot.person_id for pilot in pilots}
    assert identifiers == {"P1", "P2"}
    assert any(pilot.fdp_actual_min == 600 for pilot in pilots)


def test_parse_pilot_blocks_falls_back_to_time_members() -> None:
    payload = {
        "time": {
            "cmd": {
                "pilotRole": "CMD",
                "firstName": "Alex",
                "lastName": "Cmd",
                "userId": 101,
            },
            "fo": {
                "pilotRole": "FO",
                "firstName": "Sky",
                "lastName": "Fo",
                "userId": 202,
            },
        }
    }

    pilots = _parse_pilot_blocks(payload)

    assert len(pilots) == 2
    identifiers = {pilot.person_id for pilot in pilots}
    assert identifiers == {"101", "202"}


def test_merge_split_duty_information_updates_existing_snapshot() -> None:
    target_snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id="F1",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Taylor Captain",
                person_id="P1",
                split_duty=False,
                split_break_str=None,
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Jordan Copilot",
                person_id="P2",
                split_duty=False,
            ),
        ],
    )

    source_snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id="F2",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Taylor Captain",
                person_id="P1",
                split_duty=True,
                split_break_str="Break = 01:00",
                rest_after_actual_min=600,
                rest_after_actual_str="10:00",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Jordan Copilot",
                person_id="P2",
                split_duty=True,
            ),
        ],
    )

    _merge_split_duty_information(target_snapshot, source_snapshot)

    pilot_lookup = {pilot.person_id: pilot for pilot in target_snapshot.pilots}

    updated_pic = pilot_lookup["P1"]
    assert updated_pic.split_duty is True
    assert updated_pic.split_break_str == "Break = 01:00"
    assert updated_pic.rest_after_actual_min == 600
    assert updated_pic.rest_after_actual_str == "10:00"

    updated_sic = pilot_lookup["P2"]
    assert updated_sic.split_duty is True


def _build_collection_with_snapshot(snapshot: DutyStartSnapshot) -> DutyStartCollection:
    return DutyStartCollection(
        target_date=datetime(2024, 1, 1, tzinfo=timezone.utc).date(),
        start_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_utc=datetime(2024, 1, 2, tzinfo=timezone.utc),
        snapshots=[snapshot],
    )


def test_summarize_tight_turnarounds_flags_non_flight_when_no_next_day_match() -> None:
    today_snapshot = DutyStartSnapshot(
        tail="C-GASK",
        flight_id="F1",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Taylor Captain",
                person_id="P1",
                rest_after_actual_min=520,
                rest_after_actual_str="8:40",
                rest_after_required_min=600,
            )
        ],
    )
    collection_today = _build_collection_with_snapshot(today_snapshot)

    tomorrow_snapshot = DutyStartSnapshot(
        tail="C-GASK",
        flight_id="F2",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jordan Copilot",
                person_id="P2",
                rest_before_actual_min=700,
                rest_before_required_min=600,
            )
        ],
    )
    next_index = build_rest_before_index([tomorrow_snapshot])

    lines = summarize_tight_turnarounds(
        collection_today, next_day_rest_index=next_index
    )

    assert lines == []


def test_summarize_tight_turnarounds_no_note_when_rest_matches() -> None:
    today_snapshot = DutyStartSnapshot(
        tail="C-GKPR",
        flight_id="F3",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Jordan Copilot",
                person_id="P2",
                rest_after_actual_min=600,
                rest_after_actual_str="10:00",
                rest_after_required_min=600,
            )
        ],
    )
    collection_today = _build_collection_with_snapshot(today_snapshot)

    tomorrow_snapshot = DutyStartSnapshot(
        tail="C-GKPR",
        flight_id="F4",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Jordan Copilot",
                person_id="P2",
                rest_before_actual_min=600,
                rest_before_required_min=600,
            )
        ],
    )
    next_index = build_rest_before_index([tomorrow_snapshot])

    lines = summarize_tight_turnarounds(
        collection_today, next_day_rest_index=next_index
    )

    assert lines
    assert all("non-flight duties" not in line.lower() for line in lines)


def test_summarize_split_duty_days_adds_ground_time_offset() -> None:
    snapshot = DutyStartSnapshot(
        tail="C-FSDN",
        flight_id="F5",
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Taylor Captain",
                person_id="P1",
                split_duty=True,
                explainer_map={
                    "ACTUAL_FDP": {
                        "header": "Actual FDP = 14h02",
                        "text": ["Break = 4h12"],
                    }
                },
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Jordan Copilot",
                person_id="P2",
                split_duty=True,
                explainer_map={},
            ),
        ],
    )

    lines = summarize_split_duty_days([snapshot])

    assert lines == ["C-FSDN - 14H02 duty - 6H12 ground time (PIC+SIC split)"]


def test_flight_following_report_text_payload_header() -> None:
    report = FlightFollowingReport(
        target_date=date(2025, 10, 26),
        generated_at=datetime(2025, 10, 26, 20, 39, tzinfo=timezone.utc),
        sections=[],
    )

    payload = report.text_payload().splitlines()

    assert payload == ["High Risk Flight Report - 26OCT25"]
