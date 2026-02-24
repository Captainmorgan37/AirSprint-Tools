from datetime import date

from cj_maintenance_status import (
    extract_maintenance_events,
    format_tail_for_fl3xx,
    maintenance_daily_status,
)


def test_extract_maintenance_events_filters_supported_types_only():
    tasks = [
        {
            "id": "task_1",
            "taskType": "MAINTENANCE",
            "departureDateUTC": "2026-02-10T00:00",
            "arrivalDateUTC": "2026-02-12T00:00",
            "notes": "Scheduled",
        },
        {
            "id": "task_2",
            "taskType": "NOTE",
            "departureDateUTC": "2026-02-11T00:00",
            "arrivalDateUTC": "2026-02-11T03:00",
        },
    ]

    events = extract_maintenance_events(tasks, "C-FSEF")

    assert len(events) == 1
    assert events[0].task_id == "task_1"
    assert events[0].task_type == "MAINTENANCE"


def test_extract_maintenance_events_uses_airport_timezone_when_available():
    tasks = [
        {
            "id": "task_1",
            "taskType": "MAINTENANCE",
            "departureDateUTC": "2026-02-10T00:00",
            "arrivalDateUTC": "2026-02-12T00:00",
            "departureAirport": "CYYC",
        }
    ]

    events = extract_maintenance_events(tasks, "C-FSEF")

    assert len(events) == 1
    assert events[0].airport_code == "CYYC"
    assert events[0].airport_tz == "America/Edmonton"


def test_maintenance_daily_status_counts_unique_tails_per_type():
    tasks_a = [
        {
            "id": "a1",
            "taskType": "MAINTENANCE",
            "departureDateUTC": "2026-02-10T00:00",
            "arrivalDateUTC": "2026-02-11T12:00",
        },
        {
            "id": "a2",
            "taskType": "AOG",
            "departureDateUTC": "2026-02-11T13:00",
            "arrivalDateUTC": "2026-02-12T02:00",
        },
    ]
    tasks_b = [
        {
            "id": "b1",
            "taskType": "UNSCHEDULED_MAINTENANCE",
            "departureDateUTC": "2026-02-11T05:00",
            "arrivalDateUTC": "2026-02-11T15:00",
        }
    ]

    events = extract_maintenance_events(tasks_a, "C-FSEF") + extract_maintenance_events(tasks_b, "C-FASP")
    df = maintenance_daily_status(events, start_date=date(2026, 2, 10), end_date=date(2026, 2, 12))

    feb11 = df.loc[df["date"] == date(2026, 2, 11)].iloc[0]
    assert int(feb11["scheduled_maintenance"]) == 1
    assert int(feb11["unscheduled_maintenance"]) == 1
    assert int(feb11["aog"]) == 1
    assert int(feb11["total_aircraft_down"]) == 2


def test_maintenance_daily_status_uses_local_airport_calendar_day():
    tasks = [
        {
            "id": "task_1",
            "taskType": "MAINTENANCE",
            "departureAirport": "CYYC",
            "departureDateUTC": "2026-02-11T01:00",
            "arrivalDateUTC": "2026-02-11T03:00",
        }
    ]

    events = extract_maintenance_events(tasks, "C-FSEF")
    df = maintenance_daily_status(events, start_date=date(2026, 2, 10), end_date=date(2026, 2, 11))

    feb10 = df.loc[df["date"] == date(2026, 2, 10)].iloc[0]
    feb11 = df.loc[df["date"] == date(2026, 2, 11)].iloc[0]
    assert int(feb10["scheduled_maintenance"]) == 1
    assert int(feb11["scheduled_maintenance"]) == 0


def test_maintenance_daily_status_fractional_day_mode_prorates():
    tasks = [
        {
            "id": "task_1",
            "taskType": "MAINTENANCE",
            "departureDateUTC": "2026-02-11T00:00",
            "arrivalDateUTC": "2026-02-11T12:00",
        },
        {
            "id": "task_2",
            "taskType": "UNSCHEDULED_MAINTENANCE",
            "departureDateUTC": "2026-02-11T06:00",
            "arrivalDateUTC": "2026-02-11T18:00",
        },
    ]

    events = extract_maintenance_events(tasks, "C-FSEF")
    df = maintenance_daily_status(
        events,
        start_date=date(2026, 2, 11),
        end_date=date(2026, 2, 11),
        fractional_day=True,
    )

    row = df.iloc[0]
    assert row["scheduled_maintenance"] == 0.5
    assert row["unscheduled_maintenance"] == 0.5
    assert row["aog"] == 0
    assert row["total_aircraft_down"] == 0.75


def test_maintenance_daily_status_fractional_day_mode_uses_local_time():
    tasks = [
        {
            "id": "task_1",
            "taskType": "MAINTENANCE",
            "departureAirport": "CYYC",
            "departureDateUTC": "2026-02-11T01:00",
            "arrivalDateUTC": "2026-02-11T03:00",
        }
    ]

    events = extract_maintenance_events(tasks, "C-FSEF")
    df = maintenance_daily_status(
        events,
        start_date=date(2026, 2, 10),
        end_date=date(2026, 2, 11),
        fractional_day=True,
    )

    feb10 = df.loc[df["date"] == date(2026, 2, 10)].iloc[0]
    feb11 = df.loc[df["date"] == date(2026, 2, 11)].iloc[0]
    assert feb10["scheduled_maintenance"] == (2 / 24)
    assert feb11["scheduled_maintenance"] == 0


def test_format_tail_for_fl3xx_inserts_hyphen():
    assert format_tail_for_fl3xx("CFSEF") == "C-FSEF"
