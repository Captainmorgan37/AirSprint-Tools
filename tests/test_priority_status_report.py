import datetime as dt
import pathlib
import sys
from typing import List

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from fl3xx_api import Fl3xxApiConfig
import morning_reports as mr
from morning_reports import run_morning_reports


UTC = dt.timezone.utc


def _iso(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(UTC).isoformat().replace("+00:00", "Z")


def test_priority_report_runs_with_morning_reports(monkeypatch):
    departure = dt.datetime(2024, 4, 1, 15, 0, tzinfo=UTC)
    checkin_early = departure - dt.timedelta(hours=2)
    checkin_late = departure - dt.timedelta(hours=1, minutes=15)

    flight_row = {
        "dep_time": _iso(departure),
        "tail": "C-GABC",
        "priority_label": "Priority Flight",
        "flightId": "FLIGHT-001",
        "bookingIdentifier": "BK-1001",
        "accountName": "Owner Alpha",
    }

    flights_payload = [flight_row]
    fetch_metadata = {"fetched_at": _iso(departure)}

    def fake_build_config(settings):
        return Fl3xxApiConfig(api_token="token")

    def fake_fetch_flights(config, from_date, to_date, now):
        assert config.api_token == "token"
        return flights_payload, fetch_metadata

    def fake_normalize(payload):
        assert payload == {"items": flights_payload}
        return flights_payload, {"normalised": len(flights_payload)}

    def fake_filter(rows):
        assert rows == flights_payload
        return rows, 0

    def fake_postflight(config, flight_identifier):
        assert flight_identifier == "FLIGHT-001"
        return {
            "crew": [
                {"checkin": checkin_early.timestamp()},
                {"checkin": checkin_late.timestamp()},
            ]
        }

    monkeypatch.setattr("morning_reports.build_fl3xx_api_config", fake_build_config)
    monkeypatch.setattr("morning_reports.fetch_flights", fake_fetch_flights)
    monkeypatch.setattr("morning_reports.normalize_fl3xx_payload", fake_normalize)
    monkeypatch.setattr("morning_reports.filter_out_subcharter_rows", fake_filter)
    monkeypatch.setattr("morning_reports.fetch_postflight", fake_postflight)

    original_builder = mr._build_priority_status_report

    def patched_builder(
        rows,
        config,
        *,
        fetch_postflight_fn=fake_postflight,
        threshold_minutes=mr._PRIORITY_CHECKIN_THRESHOLD_MINUTES,
    ):
        return original_builder(
            rows,
            config,
            fetch_postflight_fn=fake_postflight,
            threshold_minutes=threshold_minutes,
        )

    monkeypatch.setattr("morning_reports._build_priority_status_report", patched_builder)

    run = run_morning_reports(
        {"api_token": "token"},
        now=departure,
        from_date=departure.date(),
        to_date=departure.date(),
    )

    report_codes = [report.code for report in run.reports]
    assert "16.1.7" in report_codes

    priority_report = next(report for report in run.reports if report.code == "16.1.7")
    assert priority_report.metadata["total_priority_flights"] == 1
    assert priority_report.metadata["validation_required"] == 1
    assert priority_report.metadata["validated_without_issue"] == 1
    assert priority_report.metadata["issues_found"] == 0
    assert priority_report.rows[0]["status"].startswith("Meets threshold")


def test_priority_report_validates_turn_time_for_subsequent_leg():
    first_departure = dt.datetime(2024, 4, 1, 12, 0, tzinfo=UTC)
    first_arrival = first_departure + dt.timedelta(hours=2)
    priority_departure = first_arrival + dt.timedelta(minutes=120)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arr_time": _iso(first_arrival),
            "tail": "C-GAPP",
            "bookingIdentifier": "BK-0001",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GAPP",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0002",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=lambda *_args, **_kwargs: {},
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["status"].startswith("Turn time meets threshold")
    assert row["turn_gap_minutes"] == 120.0
    assert dt.datetime.fromisoformat(row["previous_arrival_time"]) == first_arrival
    assert row["needs_validation"] is True
    assert row["has_issue"] is False


def test_priority_report_handles_priority_leg_after_utc_rollover():
    first_departure = dt.datetime(2024, 6, 1, 15, 0, tzinfo=UTC)
    first_arrival = first_departure + dt.timedelta(hours=2)
    priority_departure = dt.datetime(2024, 6, 2, 0, 30, tzinfo=UTC)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arr_time": _iso(first_arrival),
            "tail": "C-GROL",
            "bookingIdentifier": "BK-0500",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GROL",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0501",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=lambda *_args, **_kwargs: {},
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["is_first_departure"] is False
    assert row["turn_gap_minutes"] == 450.0
    assert row["needs_validation"] is True
    assert row["has_issue"] is False
    assert row["checkin_count"] is None


def test_priority_report_treats_long_rest_as_new_duty_day():
    last_departure = dt.datetime(2024, 6, 1, 0, 30, tzinfo=UTC)
    final_arrival = last_departure + dt.timedelta(hours=1)
    priority_departure = final_arrival + dt.timedelta(hours=10, minutes=30)
    earliest_checkin = priority_departure - dt.timedelta(hours=2)
    latest_checkin = priority_departure - dt.timedelta(hours=1, minutes=20)

    rows = [
        {
            "dep_time": _iso(last_departure),
            "arr_time": _iso(final_arrival),
            "tail": "C-GREST",
            "bookingIdentifier": "BK-REST-1",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GREST",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-REST-2",
            "flightId": "LEG-REST",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")
    fetch_calls: List[str] = []

    def fake_fetch(config_arg, flight_identifier):
        fetch_calls.append(flight_identifier)
        assert config_arg is config
        assert flight_identifier == "LEG-REST"
        return {
            "crew": [
                {"checkin": earliest_checkin.timestamp()},
                {"checkin": latest_checkin.timestamp()},
            ]
        }

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=fake_fetch,
    )

    assert fetch_calls == ["LEG-REST"]
    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["is_first_departure"] is True
    assert row["turn_gap_minutes"] is None
    assert row["previous_arrival_time"] is None
    assert row["checkin_count"] == 2
    assert row["status"].startswith("Meets threshold")


def test_priority_report_uses_nested_arrival_timestamp():
    first_departure = dt.datetime(2024, 4, 1, 8, 0, tzinfo=UTC)
    first_arrival = first_departure + dt.timedelta(hours=2)
    priority_departure = first_arrival + dt.timedelta(minutes=95)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arrival": {"actualUtc": _iso(first_arrival)},
            "times": {"arrival": {"actual": _iso(first_arrival)}},
            "tail": "C-GNEST",
            "bookingIdentifier": "BK-1000",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GNEST",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-1001",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=lambda *_args, **_kwargs: {},
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["status"].startswith("Turn time meets threshold")
    assert row["turn_gap_minutes"] == 95.0
    assert dt.datetime.fromisoformat(row["previous_arrival_time"]) == first_arrival


def test_priority_report_prefers_estimated_block_on_timestamp():
    first_departure = dt.datetime(2024, 4, 1, 9, 0, tzinfo=UTC)
    estimated_arrival = first_departure + dt.timedelta(hours=2)
    actual_arrival = estimated_arrival - dt.timedelta(minutes=25)
    priority_departure = estimated_arrival + dt.timedelta(minutes=110)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "blockOnEstUTC": _iso(estimated_arrival),
            "arrivalActualUtc": _iso(actual_arrival),
            "tail": "C-GEST",
            "bookingIdentifier": "BK-2000",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GEST",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-2001",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=lambda *_args, **_kwargs: {},
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["status"].startswith("Turn time meets threshold")
    expected_gap = (priority_departure - estimated_arrival).total_seconds() / 60.0
    assert row["turn_gap_minutes"] == expected_gap
    assert dt.datetime.fromisoformat(row["previous_arrival_time"]) == estimated_arrival


def test_priority_report_flags_short_turn_for_subsequent_leg():
    first_departure = dt.datetime(2024, 4, 1, 12, 0, tzinfo=UTC)
    first_arrival = first_departure + dt.timedelta(hours=1, minutes=15)
    priority_departure = first_arrival + dt.timedelta(minutes=30)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arr_time": _iso(first_arrival),
            "tail": "C-GAPP",
            "bookingIdentifier": "BK-0001",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GAPP",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0002",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    def fail_fetch(*_args, **_kwargs):  # pragma: no cover - should not be called
        raise AssertionError("fetch_postflight_fn should not be invoked for turn validations")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=fail_fetch,
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 0
    assert report.metadata["issues_found"] == 1

    row = report.rows[0]
    assert row["status"].startswith("Turn time only 30.0 min before departure")
    assert row["turn_gap_minutes"] == 30.0
    assert row["has_issue"] is True
    assert row["needs_validation"] is True


def test_priority_report_keeps_last_arrival_when_next_leg_missing_arrival():
    first_departure = dt.datetime(2024, 4, 1, 6, 0, tzinfo=UTC)
    first_arrival = first_departure + dt.timedelta(hours=2)
    intermediate_departure = first_arrival + dt.timedelta(hours=1)
    priority_departure = intermediate_departure + dt.timedelta(hours=2)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arr_time": _iso(first_arrival),
            "tail": "C-GRET", 
            "bookingIdentifier": "BK-0100",
        },
        {
            "dep_time": _iso(intermediate_departure),
            "tail": "C-GRET",
            "bookingIdentifier": "BK-0101",
        },
        {
            "dep_time": _iso(priority_departure),
            "tail": "C-GRET",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0102",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=lambda *_args, **_kwargs: {},
    )

    assert report.metadata["total_priority_flights"] == 1
    assert report.metadata["validation_required"] == 1
    assert report.metadata["issues_found"] == 0

    row = report.rows[0]
    assert row["status"].startswith("Turn time meets threshold")
    expected_gap = (priority_departure - first_arrival).total_seconds() / 60.0
    assert row["turn_gap_minutes"] == expected_gap
    assert dt.datetime.fromisoformat(row["previous_arrival_time"]) == first_arrival


def test_priority_report_skips_turn_validation_for_shared_booking_priority_legs():
    duty_start = dt.datetime(2024, 4, 1, 10, 0, tzinfo=UTC)
    first_departure = duty_start + dt.timedelta(minutes=120)
    first_arrival = first_departure + dt.timedelta(hours=1)
    tech_stop_departure = first_arrival + dt.timedelta(minutes=45)

    rows = [
        {
            "dep_time": _iso(first_departure),
            "arr_time": _iso(first_arrival),
            "tail": "C-GAPP",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0007",
            "flightId": "LEG-1",
        },
        {
            "dep_time": _iso(tech_stop_departure),
            "tail": "C-GAPP",
            "priority_label": "Priority Owner",
            "bookingIdentifier": "BK-0007",
            "flightId": "LEG-2",
        },
    ]

    config = Fl3xxApiConfig(api_token="token")

    fetch_calls: List[str] = []

    def fake_fetch(config_arg, flight_identifier):
        fetch_calls.append(flight_identifier)
        assert config_arg is config
        assert flight_identifier == "LEG-1"
        return {
            "crew": [
                {"checkin": (duty_start - dt.timedelta(minutes=5)).timestamp()},
                {"checkin": (duty_start - dt.timedelta(minutes=15)).timestamp()},
            ]
        }

    report = mr._build_priority_status_report(
        rows,
        config,
        fetch_postflight_fn=fake_fetch,
    )

    assert fetch_calls == ["LEG-1"]
    assert report.metadata["total_priority_flights"] == 2
    assert report.metadata["validation_required"] == 1
    assert report.metadata["validated_without_issue"] == 1
    assert report.metadata["issues_found"] == 0

    assert len(report.rows) == 1
    (first_row,) = report.rows

    assert first_row["status"].startswith("Meets threshold")
    assert first_row["needs_validation"] is True
    assert first_row["has_issue"] is False
