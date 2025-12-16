"""Tests for helpers in :mod:`fl3xx_api`."""

from __future__ import annotations

import json
import pathlib
from datetime import datetime, timezone
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import (
    DutySnapshot,
    DutySnapshotPilot,
    MissingQualificationAlert,
    Fl3xxApiConfig,
    PreflightChecklistStatus,
    PreflightCrewCheckin,
    PreflightCrewMember,
    PreflightConflictAlert,
    PassengerDetail,
    backfill_missing_crew_passports,
    extract_conflicts_from_preflight,
    extract_crew_from_preflight,
    extract_passengers_from_pax_details,
    extract_missing_qualifications_from_preflight,
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
        "crewBrief": {"status": "OK"},
        "crewAssign": {"status": "REQ"},
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


def test_parse_preflight_payload_falls_back_to_legacy_structure() -> None:
    payload = {"crw": {"crewBriefing": "REQ", "crewAssign": "OK"}}

    status = parse_preflight_payload(payload)

    assert status.crew_briefing == "REQ"
    assert status.crew_assign == "OK"


def test_parse_preflight_payload_collects_checkins() -> None:
    payload = {
        "dutyTimeLim": {
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


def test_extract_passengers_from_pax_details_reads_tickets() -> None:
    payload = json.loads(pathlib.Path("docs/pax details API pull.txt").read_text())

    passengers = extract_passengers_from_pax_details(payload)

    assert len(passengers) == 6

    first = passengers[0]
    assert isinstance(first, PassengerDetail)
    assert first.last_name == "Aune"
    assert first.first_name == "Jonathan"
    assert first.middle_name == "Patrick Nelse"
    assert first.gender == "M"
    assert first.nationality_iso3 == "CAN"
    assert first.birth_date == 164246400000
    assert first.document_number == "P171050ED"
    assert first.document_issue_country_iso3 == "CAN"
    assert first.document_expiration == 2057356800000


def test_parse_preflight_payload_collects_additional_datetime_fields() -> None:
    payload = {
        "dutyTimeLim": {
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
    }

    status = parse_preflight_payload(payload)
    assert status.crew_checkins

    checkin = status.crew_checkins[0]
    assert checkin.extra_checkins

    expected_epoch = int(datetime(2024, 5, 2, 12, 15, tzinfo=timezone.utc).timestamp())
    assert checkin.extra_checkins == (expected_epoch,)


def test_extract_crew_from_preflight_returns_roster() -> None:
    payload = {
        "crewAssign": {
            "status": "OK",
            "commander": {
                "user": {
                    "id": 395655,
                    "firstName": "Steward",
                    "middleName": "John",
                    "lastName": "Van Male",
                    "gender": "MALE",
                    "birthDate": -69638400000,
                },
                "idCard": {
                    "number": "AL678654",
                    "issueCountry": {"iso3": "CAN"},
                    "expirationDate": 1944950400000,
                },
            },
            "firstOfficer": {
                "user": {
                    "id": "999888",
                    "firstName": "Robert",
                    "middleName": "Lewis Cameron",
                    "lastName": "Foster",
                    "gender": "male",
                    "birthDate": 419817600000,
                },
                "idCard": {
                    "number": "AB698200",
                    "issueCountry": {"iso3": "CAN"},
                    "expirationDate": 1822435200000,
                },
            },
        }
    }

    crew = extract_crew_from_preflight(payload)

    assert [member.seat for member in crew] == ["PIC", "SIC"]
    assert crew[0] == PreflightCrewMember(
        seat="PIC",
        user_id="395655",
        first_name="Steward",
        middle_name="John",
        last_name="Van Male",
        gender="M",
        nationality_iso3="CAN",
        birth_date=-69638400000,
        document_number="AL678654",
        document_issue_country_iso3="CAN",
        document_expiration=1944950400000,
    )

    fo = crew[1]
    assert fo.gender == "M"
    assert fo.document_number == "AB698200"
    assert fo.birth_date == 419817600000


def test_extract_crew_from_preflight_handles_missing_blocks() -> None:
    assert extract_crew_from_preflight({}) == []
    assert extract_crew_from_preflight({"crewAssign": {"status": "OK", "commander": "not-a-mapping"}}) == []


def test_backfill_missing_crew_passports_fetches_missing_data() -> None:
    roster = [
        PreflightCrewMember(seat="PIC", user_id="123"),
        PreflightCrewMember(
            seat="SIC",
            user_id="456",
            document_number="ALREADY",
            document_issue_country_iso3="CAN",
            document_expiration=1704067200,
        ),
    ]

    passport_payload = {
        "idCards": [
            {
                "type": "PASSPORT",
                "number": "A8251256",
                "main": True,
                "issueCountry": "EC",
                "expirationDate": "2032-09-07",
            }
        ]
    }

    calls: list[str] = []

    def fake_fetch(config, crew_id, session=None):  # type: ignore[override]
        calls.append(str(crew_id))
        return passport_payload

    updated = backfill_missing_crew_passports(
        Fl3xxApiConfig(), roster, fetch_member_fn=fake_fetch
    )

    assert calls == ["123"]

    expected_expiration = int(
        datetime.fromisoformat("2032-09-07").replace(tzinfo=timezone.utc).timestamp()
    )

    assert updated[0].document_number == "A8251256"
    assert updated[0].document_issue_country_iso3 == "EC"
    assert updated[0].document_expiration == expected_expiration

    assert updated[1].document_number == "ALREADY"
    assert updated[1].document_issue_country_iso3 == "CAN"
    assert updated[1].document_expiration == 1704067200


def test_extract_missing_qualifications_from_preflight_returns_alerts() -> None:
    payload = {
        "crewAssign": {
            "commander": {
                "user": {
                    "id": 111,
                    "firstName": "Alex",
                    "lastName": "Commander",
                },
                "warnings": {
                    "messages": [
                        {"type": "qualification", "status": "missing", "name": "CANPASS"},
                        {
                            "type": "qualification",
                            "status": "expired",
                            "name": "TP Performance Airspace: RNP 10",
                        },
                        {
                            "type": "RECENCY",
                            "status": "EXPIRED",
                            "name": "(6m) 5 Night TO/Ldg",
                        },
                        {"type": "duty", "status": "setup", "name": "Duty_0"},
                    ]
                },
            },
            "firstOfficer": {
                "user": {"id": "222", "nickname": "Sam"},
                "warnings": {
                    "messages": [
                        {"type": "QUALIFICATION", "status": "MISSING", "name": "RNP AR"},
                        {"type": "QUALIFICATION", "status": "OK", "name": "RVSM"},
                        "not-a-mapping",
                    ]
                },
            },
        }
    }

    alerts = extract_missing_qualifications_from_preflight(payload)

    assert len(alerts) == 4
    assert alerts[0] == MissingQualificationAlert(
        seat="PIC",
        pilot_name="Alex Commander",
        pilot_id="111",
        qualification_name="CANPASS",
    )
    assert alerts[1] == MissingQualificationAlert(
        seat="PIC",
        pilot_name="Alex Commander",
        pilot_id="111",
        qualification_name="TP Performance Airspace: RNP 10",
    )
    assert alerts[2] == MissingQualificationAlert(
        seat="PIC",
        pilot_name="Alex Commander",
        pilot_id="111",
        qualification_name="(6m) 5 Night TO/Ldg",
    )
    assert alerts[3] == MissingQualificationAlert(
        seat="SIC",
        pilot_name="Sam",
        pilot_id="222",
        qualification_name="RNP AR",
    )


def test_extract_missing_qualifications_handles_missing_blocks() -> None:
    alerts = extract_missing_qualifications_from_preflight({})
    assert alerts == []


def test_extract_conflicts_from_preflight_returns_conflicts() -> None:
    payload = {
        "crewAssign": {
            "warnings": {
                "messages": [
                    {
                        "type": "flight",
                        "status": "conflict",
                        "name": "Location disconnect: CYUL ≠ CYEG",
                    }
                ]
            },
            "commander": {
                "warnings": {
                    "messages": [
                        {
                            "type": "FLIGHT",
                            "status": "CONFLICT",
                            "description": "Duty overlap",
                        }
                    ]
                }
            },
        }
    }

    conflicts = extract_conflicts_from_preflight(payload)

    assert conflicts == [
        PreflightConflictAlert(
            seat=None,
            category="FLIGHT",
            status="CONFLICT",
            description="Location disconnect: CYUL ≠ CYEG",
        ),
        PreflightConflictAlert(
            seat="PIC",
            category="FLIGHT",
            status="CONFLICT",
            description="Duty overlap",
        ),
    ]


def test_extract_conflicts_from_preflight_handles_missing_blocks() -> None:
    assert extract_conflicts_from_preflight({}) == []
