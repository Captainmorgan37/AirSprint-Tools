from datetime import date, datetime, timezone
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import Fl3xxApiConfig
from flight_following_reports import (
    DutyStartCollection,
    DutyStartPilotSnapshot,
    DutyStartSnapshot,
    collect_duty_start_snapshots,
    summarize_long_duty_days,
)


UTC = timezone.utc


def _make_postflight(
    pic_name: str,
    sic_name: str,
    *,
    pic_id: str,
    sic_id: str,
    rest_actual: int = 480,
    tail_number: str = "C-FAKE",
):
    return {
        "tailNumber": tail_number,
        "dtls2": [
            {
                "pilotRole": "PIC",
                "firstName": pic_name.split()[0],
                "lastName": pic_name.split()[-1],
                "personId": pic_id,
                "fullDutyState": {
                    "fdp": {"actual": 300, "max": 600},
                    "explainerMap": {
                        "ACTUAL_FDP": {
                            "header": "FDP = 5:00",
                            "text": ["Break = 1:00"],
                        }
                    },
                    "restAfterDuty": {"actual": rest_actual},
                },
            },
            {
                "pilotRole": "SIC",
                "firstName": sic_name.split()[0],
                "lastName": sic_name.split()[-1],
                "personId": sic_id,
                "fullDutyState": {
                    "fdp": {"actual": 310, "max": 600},
                    "explainerMap": {
                        "ACTUAL_FDP": {
                            "header": "FDP = 5:10",
                            "text": ["Break = 1:00"],
                        }
                    },
                    "restAfterDuty": {"actual": rest_actual},
                },
            },
        ],
    }


def test_collect_duty_start_snapshots_groups_by_tail_and_tracks_crew_changes():
    target_date = date(2024, 1, 1)
    flights = [
        {
            "id": 1,
            "registrationNumber": "C-FAKE",
            "blockOffEstUTC": "2024-01-01T08:00:00Z",
        },
        {
            "id": 2,
            "registrationNumber": "C-FAKE",
            "blockOffEstUTC": "2024-01-01T12:00:00Z",
        },
        {
            "id": 3,
            "registrationNumber": "C-FAKE",
            "blockOffEstUTC": "2024-01-01T16:00:00Z",
        },
        {
            "id": 4,
            "registrationNumber": "C-OTHER",
            "blockOffEstUTC": "2024-01-01T10:00:00Z",
        },
    ]

    postflight_payloads = {
        1: _make_postflight("Jane Doe", "John Smith", pic_id="P1", sic_id="S1"),
        2: _make_postflight("Jane Doe", "John Smith", pic_id="P1", sic_id="S1"),
        3: _make_postflight("Jane Doe", "Alex Shaw", pic_id="P1", sic_id="S2", rest_actual=420),
        4: _make_postflight(
            "Riley Blue",
            "Mason Gray",
            pic_id="P3",
            sic_id="S3",
            tail_number="C-OTHER",
        ),
    }

    def fake_postflight_fetcher(_config, flight_id):
        return postflight_payloads[flight_id]

    config = Fl3xxApiConfig(api_token="dummy")

    collection = collect_duty_start_snapshots(
        config,
        target_date,
        flights=flights,
        postflight_fetcher=fake_postflight_fetcher,
    )

    assert collection.start_utc.tzinfo == UTC
    assert collection.end_utc.tzinfo == UTC
    assert len(collection.snapshots) == 3

    first_snapshot = collection.snapshots[0]
    assert first_snapshot.tail == "C-FAKE"
    assert first_snapshot.flight_id == 1
    assert first_snapshot.block_off_est_utc == datetime(2024, 1, 1, 8, 0, tzinfo=UTC)
    assert len(first_snapshot.pilots) == 2
    pic = first_snapshot.pilots[0]
    assert pic.person_id == "P1"
    assert pic.fdp_actual_min == 300
    assert pic.fdp_max_min == 600
    assert pic.fdp_actual_str == "5:00"
    assert pic.split_break_str == "1:00"
    assert pic.rest_after_min == 480
    assert pic.rest_after_str == "8:00"
    assert pic.full_duty_state["fdp"]["actual"] == 300
    assert "ACTUAL_FDP" in pic.explainer_map

    third_snapshot = collection.snapshots[1]
    assert third_snapshot.flight_id == 3
    assert third_snapshot.pilots[1].person_id == "S2"
    assert third_snapshot.pilots[1].rest_after_min == 420

    other_tail_snapshot = collection.snapshots[2]
    assert other_tail_snapshot.tail == "C-OTHER"
    assert other_tail_snapshot.flight_id == 4
    assert other_tail_snapshot.pilots[0].name == "Riley Blue"
    assert other_tail_snapshot.crew_signature() == (("PIC", "P3"), ("SIC", "S3"))


def test_summarize_long_duty_days_lists_split_duty_details():
    snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id=1,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jane Doe",
                split_duty=True,
                explainer_map={
                    "ACTUAL_FDP": {"header": "FDP = 9:00", "text": ["Break = 3:00"]}
                },
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="John Smith",
                split_duty=True,
                explainer_map={
                    "ACTUAL_FDP": {"header": "FDP = 9:00", "text": ["Break = 3:00"]}
                },
            ),
        ],
    )

    lines = summarize_long_duty_days([snapshot])

    assert lines == ["C-FAKE – Duty 9:00 Break 3:00 (PIC/SIC)"]


def test_summarize_long_duty_days_uses_fallback_break_and_filters_roles():
    snapshot = DutyStartSnapshot(
        tail="C-BOTH",
        flight_id=99,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Riley Blue",
                split_duty=True,
                explainer_map={"ACTUAL_FDP": {"header": "FDP = 8:30", "text": []}},
                split_break_str="2:00",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Mason Gray",
                split_duty=False,
                explainer_map={"ACTUAL_FDP": {"header": "FDP = 7:45", "text": ["Break = 1:30"]}},
            ),
        ],
    )

    collection = DutyStartCollection(
        target_date=date(2024, 1, 1),
        start_utc=datetime(2024, 1, 1, 6, 0, tzinfo=UTC),
        end_utc=datetime(2024, 1, 2, 6, 0, tzinfo=UTC),
        snapshots=[snapshot],
    )

    lines = summarize_long_duty_days(collection)

    assert lines == ["C-BOTH – Duty 8:30 Break 2:00 (PIC)"]


def test_summarize_long_duty_days_skips_snapshots_without_split_duty():
    snapshot = DutyStartSnapshot(
        tail="C-NORMAL",
        flight_id=55,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jordan Sky",
                split_duty=False,
                explainer_map={"ACTUAL_FDP": {"header": "FDP = 7:00", "text": []}},
            )
        ],
    )

    assert summarize_long_duty_days([snapshot]) == []
