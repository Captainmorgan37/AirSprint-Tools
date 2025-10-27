"""Tests for helpers in :mod:`fl3xx_api`."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import (
    DutySnapshot,
    DutySnapshotPilot,
    PreflightChecklistStatus,
    PreflightCrewCheckin,
    parse_postflight_payload,
    parse_preflight_payload,
)


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


def test_parse_preflight_payload_extracts_status_flags() -> None:
    payload = {
        "crw": {
            "crewBriefing": "OK",
            "crewAssign": "REQ",
        }
    }

    status = parse_preflight_payload(payload)

    assert isinstance(status, PreflightChecklistStatus)
    assert status.crew_briefing == "OK"
    assert status.crew_assign == "REQ"
    assert status.crew_briefing_ok is True
    assert status.crew_assign_ok is False
    assert status.all_ok is False
    assert status.has_data is True


def test_preflight_status_all_ok_requires_both_flags() -> None:
    payload = {"crw": {"crewBriefing": "OK"}}

    status = parse_preflight_payload(payload)

    assert status.crew_briefing_ok is True
    assert status.crew_assign is None
    assert status.all_ok is None
    assert status.has_data is True


def test_parse_preflight_payload_collects_checkins() -> None:
    payload = {
        "dtls2": [
            {
                "userId": 395555,
                "pilotRole": "CMD",
                "checkin": 1791036000.000000000,
                "checkinActual": "1791036000.000000000",
                "checkinDefault": 1791036000.000000000,
            },
            {
                "userId": "395567",
                "pilotRole": "FO",
                "checkin": "1791036000.0",
            },
            "not-a-mapping",
        ]
    }

    status = parse_preflight_payload(payload)

    assert status.crew_checkins
    assert len(status.crew_checkins) == 2

    first = status.crew_checkins[0]
    assert isinstance(first, PreflightCrewCheckin)
    assert first.user_id == "395555"
    assert first.pilot_role == "CMD"
    assert first.checkin == 1791036000
    assert first.checkin_actual == 1791036000
    assert first.checkin_default == 1791036000
    assert first.extra_checkins == ()

    second = status.crew_checkins[1]
    assert second.user_id == "395567"
    assert second.pilot_role == "FO"
    assert second.checkin == 1791036000
    assert second.checkin_actual is None
    assert status.has_data is True


def test_parse_preflight_payload_collects_additional_datetime_fields() -> None:
    payload = {
        "dtls2": [
            {
                "userId": 123,
                "pilotRole": "CMD",
                "crewReportTimeUtc": "2024-05-02T12:15:00Z",
                "checkInLocal": "2024-05-02 06:15:00-06:00",
                "checkInLocalDuplicate": "2024-05-02T12:15:00+00:00",
            }
        ]
    }

    status = parse_preflight_payload(payload)
    assert status.crew_checkins

    checkin = status.crew_checkins[0]
    assert checkin.extra_checkins

    expected_epoch = int(datetime(2024, 5, 2, 12, 15, tzinfo=timezone.utc).timestamp())
    assert checkin.extra_checkins == (expected_epoch,)
