from datetime import date, datetime, timezone
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import pytest

from fl3xx_api import Fl3xxApiConfig
from flight_following_reports import (
    DutyStartCollection,
    DutyStartPilotSnapshot,
    DutyStartSnapshot,
    _coerce_minutes,
    build_flight_following_report,
    collect_duty_start_snapshots,
    summarize_collection_for_display,
    summarize_insufficient_rest,
    summarize_long_duty_days,
    summarize_split_duty_days,
    summarize_tight_turnarounds,
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
    assert collection.ingestion_diagnostics["total_flights"] == len(flights)
    assert collection.ingestion_diagnostics["accepted_flights"] == 4
    assert collection.ingestion_diagnostics["tails"] == {"C-FAKE": 3, "C-OTHER": 1}

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


def test_collect_duty_start_snapshots_records_ingestion_diagnostics_for_skips():
    target_date = date(2024, 3, 3)
    flights = [
        {"id": 1, "blockOffEstUTC": "2024-03-03T08:00:00Z"},
        {"id": 2, "registrationNumber": "C-LOG"},
        {
            "id": 3,
            "registrationNumber": "C-LOG",
            "blockOffEstUTC": "not-a-date",
        },
        {
            "registrationNumber": "C-LOG",
            "blockOffEstUTC": "2024-03-03T09:00:00Z",
        },
        {
            "id": 5,
            "registrationNumber": "C-LOG",
            "blockOffEstUTC": "2024-03-03T10:00:00Z",
        },
    ]

    postflight_payloads = {
        5: _make_postflight(
            "Jordan Blue",
            "Casey Red",
            pic_id="PLOG",
            sic_id="SLOG",
            tail_number="C-LOG",
        )
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

    diagnostics = collection.ingestion_diagnostics
    assert diagnostics["total_flights"] == len(flights)
    assert diagnostics["accepted_flights"] == 1
    assert diagnostics["tails"] == {"C-LOG": 1}

    skipped = diagnostics["skipped"]
    assert skipped["missing_tail"]["count"] == 1
    assert skipped["missing_block_off"]["count"] == 1
    assert skipped["invalid_block_off"]["count"] == 1
    assert skipped["missing_flight_id"]["count"] == 1


def test_collect_duty_start_snapshots_detects_string_split_duty_flags():
    target_date = date(2024, 2, 2)
    flights = [
        {
            "id": 11,
            "registrationNumber": "C-SPLT",
            "blockOffEstUTC": "2024-02-02T08:00:00Z",
        }
    ]

    postflight_payloads = {
        11: {
            "tailNumber": "C-SPLT",
            "dtls2": [
                {
                    "pilotRole": "PIC",
                    "firstName": "Alex",
                    "lastName": "Morgan",
                    "personId": "P11",
                    "splitDutyStart": "true",
                    "fullDutyState": {
                        "fdp": {"actual": 300, "max": 600},
                        "splitDutyStart": "TRUE",
                        "explainerMap": {},
                    },
                },
                {
                    "pilotRole": "SIC",
                    "firstName": "Riley",
                    "lastName": "Stone",
                    "personId": "S11",
                    "splitDutyStart": "false",
                    "fullDutyState": {
                        "fdp": {"actual": 300, "max": 600},
                        "explainerMap": {},
                    },
                },
            ],
        }
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

    assert len(collection.snapshots) == 1
    snapshot = collection.snapshots[0]
    assert any(pilot.split_duty for pilot in snapshot.pilots)
    assert snapshot.pilots[1].split_duty is False


@pytest.mark.parametrize(
    "value, expected",
    [
        ("15H31", 15 * 60 + 31),
        ("15h31m", 15 * 60 + 31),
        ("15:31", 15 * 60 + 31),
        ("15:31:30", 15 * 60 + 31),
        ("PT15H31M", 15 * 60 + 31),
        ("90m", 90),
        (" 45 ", 45),
        ("", None),
        (None, None),
    ],
)
def test_coerce_minutes_handles_duration_strings(value, expected):
    assert _coerce_minutes(value) == expected


def test_long_duty_summary_includes_string_encoded_durations():
    snapshot = DutyStartSnapshot(
        tail="C-LONG",
        flight_id=42,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jordan Pilot",
                fdp_actual_min=_coerce_minutes("15H31"),
                fdp_max_min=_coerce_minutes("PT16H0M"),
                fdp_actual_str="15:31",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Sky Copilot",
                fdp_actual_min=_coerce_minutes("12:00"),
                fdp_max_min=_coerce_minutes("14:00"),
                fdp_actual_str="12:00",
            ),
        ],
    )

    lines = summarize_long_duty_days([snapshot])

    assert lines == ["C-LONG – 15:31 (PIC)"]


def test_summarize_long_duty_days_highlight_utilisation():
    snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id=1,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jane Doe",
                fdp_actual_min=540,
                fdp_max_min=600,
                fdp_actual_str="9:00",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="John Smith",
                fdp_actual_min=400,
                fdp_max_min=600,
            ),
        ],
    )

    lines = summarize_long_duty_days([snapshot])

    assert lines == ["C-FAKE – 9:00 (PIC)"]


def test_summarize_long_duty_days_combines_multiple_seats():
    snapshot = DutyStartSnapshot(
        tail="C-BOTH",
        flight_id=99,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Riley Blue",
                fdp_actual_min=540,
                fdp_max_min=600,
                fdp_actual_str="9:00",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="Mason Gray",
                fdp_actual_min=550,
                fdp_max_min=600,
                fdp_actual_str="9:10",
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

    assert lines == ["C-BOTH – 9:00/9:10 (PIC+SIC)"]


def test_summarize_long_duty_days_skips_when_below_threshold():
    snapshot = DutyStartSnapshot(
        tail="C-NORMAL",
        flight_id=55,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jordan Sky",
                fdp_actual_min=300,
                fdp_max_min=600,
            )
        ],
    )

    assert summarize_long_duty_days([snapshot]) == []


def test_summarize_split_duty_days_formats_details():
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
                    "ACTUAL_FDP": {"header": "Actual FDP = 9:00", "text": ["Break = 3:00"]}
                },
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="John Smith",
                split_duty=True,
                explainer_map={
                    "ACTUAL_FDP": {"header": "Actual FDP = 9:00", "text": ["Break = 3:00"]}
                },
            ),
        ],
    )

    lines = summarize_split_duty_days([snapshot])

    assert lines == ["C-FAKE – 9:00 duty – 3:00 break (PIC+SIC split)"]


def test_summarize_split_duty_days_skips_when_not_flagged():
    snapshot = DutyStartSnapshot(
        tail="C-NORMAL",
        flight_id=55,
        block_off_est_utc=None,
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jordan Sky",
                split_duty=False,
            )
        ],
    )

    assert summarize_split_duty_days([snapshot]) == []


def test_summarize_tight_turnarounds_merges_seats_and_tails():
    snapshots = [
        DutyStartSnapshot(
            tail="C-FAKE",
            flight_id=1,
            block_off_est_utc=None,
            pilots=[
                DutyStartPilotSnapshot(
                    seat="PIC",
                    name="Jane Doe",
                    rest_after_min=600,
                    rest_after_str="10:00",
                ),
                DutyStartPilotSnapshot(
                    seat="SIC",
                    name="John Smith",
                    rest_after_min=630,
                    rest_after_str="10:30",
                ),
            ],
        ),
        DutyStartSnapshot(
            tail="C-FAKE",
            flight_id=2,
            block_off_est_utc=None,
            pilots=[
                DutyStartPilotSnapshot(
                    seat="SIC",
                    name="Alex Shaw",
                    rest_after_min=620,
                    rest_after_str="10:20",
                ),
            ],
        ),
        DutyStartSnapshot(
            tail="C-OTHER",
            flight_id=3,
            block_off_est_utc=None,
            pilots=[
                DutyStartPilotSnapshot(
                    seat="PIC",
                    name="Riley Blue",
                    rest_after_min=650,
                    rest_after_str="10:50",
                ),
                DutyStartPilotSnapshot(
                seat="SIC",
                name="Mason Gray",
                rest_after_min=640,
                rest_after_str="10:40",
            ),
        ],
    ),
    ]

    lines = summarize_tight_turnarounds(snapshots)

    assert lines == [
        "C-FAKE – 10:00/10:20 rest before next duty (PIC+SIC)",
        "C-OTHER – 10:50/10:40 rest before next duty (PIC+SIC)",
    ]


def test_summarize_collection_for_display_surfaces_diagnostics_and_crew_details():
    snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id="LEG-1",
        block_off_est_utc=datetime(2025, 10, 27, 13, 0, tzinfo=UTC),
        pilots=[
            DutyStartPilotSnapshot(seat="PIC", name="Jane Doe", person_id="P1"),
            DutyStartPilotSnapshot(seat="SIC", name="John Smith", crew_member_id="S2"),
        ],
    )

    collection = DutyStartCollection(
        target_date=date(2025, 10, 27),
        start_utc=datetime(2025, 10, 27, 6, 0, tzinfo=UTC),
        end_utc=datetime(2025, 10, 28, 6, 0, tzinfo=UTC),
        snapshots=[snapshot],
        flights_metadata={"source": "test"},
        grouped_flights={
            "C-FAKE": [
                {
                    "flight_id": "LEG-1",
                    "block_off_est_utc": snapshot.block_off_est_utc,
                    "flight_payload": {},
                }
            ]
        },
        ingestion_diagnostics={
            "total_flights": 2,
            "accepted_flights": 1,
            "tails": {"C-FAKE": 1},
            "skipped": {"outside_window": {"count": 1, "samples": []}},
        },
    )

    summary = summarize_collection_for_display(collection)

    assert summary["duty_start_snapshots"] == 1
    assert summary["ingestion_diagnostics"]["tails"] == {"C-FAKE": 1}

    crew_summary = summary["crew_summary"]
    assert crew_summary["total_snapshots"] == 1
    assert crew_summary["unique_crews"] == 1

    crew_entry = crew_summary["crews"][0]
    assert crew_entry["signature"] == "PIC: P1 + SIC: S2"
    assert crew_entry["tails"] == ["C-FAKE"]
    assert crew_entry["sample_duties"][0]["crew"][0]["seat"] == "PIC"

    pilot_names = {pilot["name"] for pilot in crew_summary["pilots"]}
    assert {"Jane Doe", "John Smith"} <= pilot_names

    pic_entry = next(p for p in crew_summary["pilots"] if p["name"] == "Jane Doe")
    assert pic_entry["seats"] == ["PIC"]


def test_summarize_insufficient_rest_alias():
    snapshots = [
        DutyStartSnapshot(
            tail="C-ALIAS",
            flight_id=1,
            block_off_est_utc=None,
            pilots=[
                DutyStartPilotSnapshot(
                    seat="PIC",
                    name="Jane Doe",
                    rest_after_min=600,
                    rest_after_str="10:00",
                ),
            ],
        )
    ]

    assert summarize_insufficient_rest(snapshots) == summarize_tight_turnarounds(snapshots)


def test_build_flight_following_report_compiles_payload_and_text():
    snapshot = DutyStartSnapshot(
        tail="C-FAKE",
        flight_id=1,
        block_off_est_utc=datetime(2024, 1, 1, 8, 0, tzinfo=UTC),
        pilots=[
            DutyStartPilotSnapshot(
                seat="PIC",
                name="Jane Doe",
                split_duty=True,
                explainer_map={"ACTUAL_FDP": {"header": "FDP = 9:00", "text": ["Break = 3:00"]}},
                fdp_actual_min=540,
                fdp_max_min=600,
                fdp_actual_str="9:00",
                rest_after_min=700,
                rest_after_str="11:40",
            ),
            DutyStartPilotSnapshot(
                seat="SIC",
                name="John Smith",
                split_duty=True,
                explainer_map={"ACTUAL_FDP": {"header": "FDP = 9:00", "text": ["Break = 3:00"]}},
                fdp_actual_min=540,
                fdp_max_min=600,
                fdp_actual_str="9:00",
                rest_after_min=700,
                rest_after_str="11:40",
            ),
        ],
    )

    collection = DutyStartCollection(
        target_date=date(2024, 1, 1),
        start_utc=datetime(2024, 1, 1, 6, 0, tzinfo=UTC),
        end_utc=datetime(2024, 1, 2, 6, 0, tzinfo=UTC),
        snapshots=[snapshot],
    )

    report = build_flight_following_report(
        collection,
        generated_at=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
    )

    text_payload = report.text_payload()
    assert "Flight Following Duty Report – 2024-01-01" in text_payload
    assert "Long Duty Days" in text_payload
    assert "Split Duty Days" in text_payload
    assert "Tight Turnarounds (<11h Before Next Duty)" in text_payload
    assert "C-FAKE – 9:00 (PIC+SIC)" in text_payload
    assert "C-FAKE – 9:00 duty – 3:00 break (PIC+SIC split)" in text_payload
    assert text_payload.endswith("None")

    message_payload = report.message_payload()
    assert message_payload["target_date"] == "2024-01-01"
    assert message_payload["text"] == text_payload
    assert message_payload["sections"][0]["lines"] == [
        "C-FAKE – 9:00 (PIC+SIC)"
    ]
    assert message_payload["sections"][1]["lines"] == [
        "C-FAKE – 9:00 duty – 3:00 break (PIC+SIC split)"
    ]
    assert message_payload["sections"][2]["lines"] == ["None"]


def test_build_flight_following_report_handles_custom_sections_and_dates():
    snapshots = [
        DutyStartSnapshot(
            tail="C-TEST",
            flight_id=5,
            block_off_est_utc=None,
            pilots=[
                DutyStartPilotSnapshot(
                    seat="PIC",
                    name="Jordan Sky",
                )
            ],
        )
    ]

    def empty_section(_collection):
        return []

    report = build_flight_following_report(
        snapshots,
        generated_at=datetime(2024, 2, 2, 5, 0),
        target_date=date(2024, 2, 2),
        section_builders=[("Empty", empty_section)],
    )

    assert report.sections[0].title == "Empty"
    assert report.sections[0].text == "None"
    assert "None" in report.text_payload()


def test_build_flight_following_report_requires_target_date_for_iterables():
    with pytest.raises(ValueError):
        build_flight_following_report([], generated_at=datetime(2024, 1, 1, 1, 0))
