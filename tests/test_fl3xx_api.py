"""Tests for helpers in :mod:`fl3xx_api`."""

from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import DutySnapshot, DutySnapshotPilot, parse_postflight_payload


def test_parse_postflight_payload_reads_time_dtls2() -> None:
    payload = {
        "tailNumber": "C-GASK",
        "time": {
            "dtls2": [
                {
                    "pilotRole": "CMD",
                    "firstName": "Kyle",
                    "lastName": "Roxburgh",
                    "userId": 395627,
                    "fullDutyState": {
                        "fdp": {"actual": 633, "max": 840},
                        "explainerMap": {
                            "ACTUAL_FDP": {
                                "header": "Actual FDP = 10h33",
                                "text": ["Break = 01:15"],
                            }
                        },
                        "restAfterDuty": {"actual": 600},
                    },
                },
                {
                    "pilotRole": "FO",
                    "firstName": "Ryan",
                    "lastName": "Kawa",
                    "userId": 765708,
                    "restAfterDuty": {"actual": 540},
                },
            ]
        },
    }

    snapshot = parse_postflight_payload(payload)

    assert isinstance(snapshot, DutySnapshot)
    assert snapshot.tail == "C-GASK"
    assert len(snapshot.pilots) == 2

    pic = snapshot.pilots[0]
    assert isinstance(pic, DutySnapshotPilot)
    assert pic.pilot_id == "395627"
    assert pic.fdp_actual_min == 633
    assert pic.fdp_max_min == 840
    assert pic.fdp_actual_str == "10h33"
    assert pic.split_break_str == "01:15"
    assert pic.rest_after_min == 600
    assert pic.rest_after_str == "10:00"

    fo = snapshot.pilots[1]
    assert fo.pilot_id == "765708"
    assert fo.rest_after_min == 540
    assert fo.rest_after_str == "9:00"


def test_parse_postflight_payload_falls_back_to_time_commanders() -> None:
    payload = {
        "tailNumber": "C-GFSJ",
        "time": {
            "cmd": {
                "pilotRole": "CMD",
                "firstName": "Alex",
                "lastName": "Pic",
                "userId": 111,
                "restAfterDuty": {"actual": 600},
            },
            "fo": {
                "pilotRole": "FO",
                "firstName": "Sam",
                "lastName": "Fo",
                "userId": 222,
            },
        },
    }

    snapshot = parse_postflight_payload(payload)

    assert snapshot.tail == "C-GFSJ"
    assert len(snapshot.pilots) == 2
    identifiers = {pilot.pilot_id for pilot in snapshot.pilots}
    assert identifiers == {"111", "222"}
    assert any(pilot.rest_after_min == 600 for pilot in snapshot.pilots)
