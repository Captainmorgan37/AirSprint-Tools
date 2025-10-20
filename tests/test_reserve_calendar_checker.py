import datetime as dt

import pytest

from fl3xx_api import Fl3xxApiConfig, MOUNTAIN_TIME_ZONE
from reserve_calendar_checker import (
    TARGET_DATES,
    ReserveCheckResult,
    evaluate_flights_for_date,
    run_reserve_day_check,
    select_upcoming_reserve_dates,
)


@pytest.mark.parametrize(
    "reference, expected",
    [
        (
            dt.datetime(2025, 11, 5, 12, tzinfo=MOUNTAIN_TIME_ZONE),
            [dt.date(2025, 11, 6), dt.date(2025, 11, 7), dt.date(2025, 11, 11), dt.date(2025, 12, 21)],
        ),
        (
            dt.datetime(2026, 12, 29, 0, tzinfo=MOUNTAIN_TIME_ZONE),
            [],
        ),
    ],
)
def test_select_upcoming_reserve_dates(reference, expected):
    assert select_upcoming_reserve_dates(reference=reference, limit=4) == expected


def test_evaluate_flights_for_date_flags_missing_as_available():
    row = {
        "flightId": "F-1",
        "tail": "C-TEST",
        "dep_time": "2025-11-06T15:30:00Z",
        "departure_airport": "CYYZ",
        "arrival_airport": "CYUL",
        "workflowCustomName": "Reserve Club",
    }

    def stub_fetch_planning(config, flight_id, session=None):
        assert flight_id == "F-1"
        return {"note": "Club member request"}

    result = evaluate_flights_for_date(
        Fl3xxApiConfig(),
        [row],
        dt.date(2025, 11, 6),
        fetch_planning_note_fn=stub_fetch_planning,
    )

    assert len(result.rows) == 1
    entry = result.rows[0]
    assert entry["planning_note"] == "Club member request"
    assert entry["status"].startswith("⚠️")
    assert result.diagnostics["club_matches"] == 1
    assert result.diagnostics["missing_as_available"] == 1


def test_evaluate_flights_for_date_skips_non_club_notes():
    row = {
        "flightId": "F-2",
        "tail": "C-TEST",
        "dep_time": "2025-11-06T18:00:00Z",
        "departure_airport": "CYYZ",
        "arrival_airport": "CYUL",
        "workflowCustomName": "Reserve Club",
    }

    def stub_fetch_planning(config, flight_id, session=None):
        return {"note": "Regular positioning"}

    result = evaluate_flights_for_date(
        Fl3xxApiConfig(),
        [row],
        dt.date(2025, 11, 6),
        fetch_planning_note_fn=stub_fetch_planning,
    )

    assert result.rows == []
    assert result.diagnostics["club_matches"] == 0


def test_run_reserve_day_check_fetches_flagged_rows(monkeypatch):
    target_date = dt.date(2025, 11, 6)
    flights_payload = [
        {
            "flightId": "F-3",
            "tail": "C-GLXY",
            "dep_time": "2025-11-06T12:00:00Z",
            "departure_airport": "CYYZ",
            "arrival_airport": "CYUL",
            "workflowCustomName": "Reserve Club",
        }
    ]

    def stub_fetch_flights(config, from_date, to_date, session=None):
        assert from_date == target_date
        assert to_date == target_date + dt.timedelta(days=1)
        return flights_payload, {"from_date": from_date.isoformat(), "to_date": to_date.isoformat()}

    def stub_fetch_planning(config, flight_id, session=None):
        return {"note": "Club rotation"}

    monkeypatch.setattr("reserve_calendar_checker.fetch_flights", stub_fetch_flights)
    monkeypatch.setattr("reserve_calendar_checker.fetch_flight_planning_note", stub_fetch_planning)

    result = run_reserve_day_check(
        Fl3xxApiConfig(),
        target_dates=[target_date],
    )

    assert isinstance(result, ReserveCheckResult)
    assert len(result.dates) == 1
    date_result = result.dates[0]
    assert len(date_result.rows) == 1
    assert date_result.rows[0]["planning_note"] == "Club rotation"
    assert date_result.diagnostics["club_matches"] == 1
    assert date_result.diagnostics["targeted_flights"] == 1
