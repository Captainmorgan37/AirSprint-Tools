"""Tests for pilot parsing helpers in :mod:`flight_following_reports`."""

from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from flight_following_reports import _parse_pilot_blocks, DutyStartPilotSnapshot


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
