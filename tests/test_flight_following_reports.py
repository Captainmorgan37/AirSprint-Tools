"""Tests for pilot parsing helpers in :mod:`flight_following_reports`."""

from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from flight_following_reports import (
    _merge_split_duty_information,
    _parse_pilot_blocks,
    DutyStartPilotSnapshot,
    DutyStartSnapshot,
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
                rest_after_min=600,
                rest_after_str="10:00",
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
    assert updated_pic.rest_after_min == 600
    assert updated_pic.rest_after_str == "10:00"

    updated_sic = pilot_lookup["P2"]
    assert updated_sic.split_duty is True
