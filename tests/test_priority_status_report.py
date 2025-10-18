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

    assert len(report.rows) == 2
    first_row, second_row = report.rows

    assert first_row["status"].startswith("Meets threshold")
    assert first_row["needs_validation"] is True
    assert first_row["has_issue"] is False

    assert second_row["status"] == "Continuation of same booking; turn validation not required"
    assert second_row["needs_validation"] is False
    assert second_row["has_issue"] is False
