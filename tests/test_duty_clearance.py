from datetime import datetime, timedelta, timezone
import json

import pytest

from duty_clearance import (
    _build_preflight_signature,
    _compute_confirm_by,
    _epoch_to_dt_utc,
    _fmt_timeleft,
    _get_report_time_local,
    compute_clearance_table,
)
from fl3xx_api import Fl3xxApiConfig, PreflightChecklistStatus, PreflightCrewCheckin
from zoneinfo_compat import ZoneInfo


@pytest.mark.parametrize(
    "epoch_value, expected",
    [
        (1_700_000_000, datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)),
        (1_700_000_000_000, datetime.fromtimestamp(1_700_000_000_000 / 1000, tz=timezone.utc)),
        (1_700_000_000_000_000, datetime.fromtimestamp(1_700_000_000_000_000 / 1_000_000, tz=timezone.utc)),
        (
            1_700_000_000_000_000_000,
            datetime.fromtimestamp(1_700_000_000_000_000_000 / 1_000_000_000, tz=timezone.utc),
        ),
        (None, None),
    ],
)
def test_epoch_to_dt_utc(epoch_value, expected):
    result = _epoch_to_dt_utc(epoch_value)
    if expected is None:
        assert result is None
    else:
        assert result == expected


def test_build_preflight_signature_sorts_and_filters():
    status = PreflightChecklistStatus(
        crew_checkins=(
            PreflightCrewCheckin(user_id="2", pilot_role="FO"),
            PreflightCrewCheckin(user_id="1", pilot_role="CMD"),
            PreflightCrewCheckin(user_id=None, pilot_role="CMD"),
        )
    )

    signature = _build_preflight_signature(status)
    assert signature == (("CMD", "1"), ("FO", "2"))


def test_get_report_time_local_uses_earliest_epoch():
    tz = ZoneInfo("America/Toronto")
    base = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
    status = PreflightChecklistStatus(
        crew_checkins=(
            PreflightCrewCheckin(checkin=int((base + timedelta(hours=1)).timestamp())),
            PreflightCrewCheckin(checkin_default=int((base + timedelta(hours=2)).timestamp())),
            PreflightCrewCheckin(checkin_actual=int((base - timedelta(hours=1)).timestamp())),
        )
    )

    report_local = _get_report_time_local(status, tz)
    assert report_local == (base - timedelta(hours=1)).astimezone(tz)


def test_get_report_time_local_uses_extra_checkins():
    tz = ZoneInfo("America/Edmonton")
    base = datetime(2024, 7, 4, 8, 30, tzinfo=timezone.utc)
    epoch = int(base.timestamp())
    status = PreflightChecklistStatus(
        crew_checkins=(
            PreflightCrewCheckin(
                extra_checkins=(epoch,),
            ),
        )
    )

    report_local = _get_report_time_local(status, tz)
    assert report_local == base.astimezone(tz)


def test_compute_confirm_by_without_early_duty():
    tz = ZoneInfo("America/Edmonton")
    report_local = datetime(2024, 6, 1, 9, 0, tzinfo=tz)
    first_leg = datetime(2024, 6, 1, 10, 0, tzinfo=tz)

    confirm_by = _compute_confirm_by(report_local, first_leg, has_early_flight=False)

    assert confirm_by == datetime(2024, 5, 31, 22, 0, tzinfo=tz)


def test_compute_confirm_by_with_early_duty():
    tz = ZoneInfo("America/Toronto")
    report_local = datetime(2024, 6, 1, 5, 0, tzinfo=tz)
    first_leg = datetime(2024, 6, 1, 6, 0, tzinfo=tz)

    confirm_by = _compute_confirm_by(report_local, first_leg, has_early_flight=True)

    assert confirm_by == datetime(2024, 5, 31, 19, 0, tzinfo=tz)


def test_fmt_timeleft_positive_and_negative():
    tz = ZoneInfo("America/Edmonton")
    now_local = datetime(2024, 6, 1, 12, 0, tzinfo=tz)
    cutoff_future = datetime(2024, 6, 1, 14, 30, tzinfo=tz)
    cutoff_past = datetime(2024, 6, 1, 10, 45, tzinfo=tz)

    future_label, future_minutes = _fmt_timeleft(now_local, cutoff_future)
    past_label, past_minutes = _fmt_timeleft(now_local, cutoff_past)

    assert future_label == "2h 30m left"
    assert future_minutes == 150
    assert past_label == "OVERDUE by 1h 15m"
    assert past_minutes == -75


def test_compute_clearance_table_includes_checkin_debug(monkeypatch):
    config = Fl3xxApiConfig()
    target_date = datetime(2025, 10, 28, tzinfo=timezone.utc).date()

    legs_by_tail = {
        "C-GABC": [
            {
                "flightId": 102938,
                "dep_dt_utc": datetime(2025, 10, 28, 15, 0, tzinfo=timezone.utc),
                "dep_tz": "America/Edmonton",
            }
        ]
    }

    monkeypatch.setattr(
        "duty_clearance.get_todays_sorted_legs_by_tail",
        lambda _config, _target_date: legs_by_tail,
    )

    preflight_payload = {
        "dtls2": [
            {
                "userId": "321",
                "pilotRole": "CMD",
                "checkin": None,
                "checkinActual": None,
                "checkinDefault": None,
            }
        ]
    }

    monkeypatch.setattr(
        "duty_clearance.fetch_preflight",
        lambda _config, _flight_id: preflight_payload,
    )

    monkeypatch.setattr(
        "duty_clearance.fetch_flight_crew",
        lambda _config, _flight_id: [
            {"role": "CMD", "firstName": "Test", "lastName": "Pilot"},
            {"role": "FO", "firstName": "Sample", "lastName": "Copilot"},
        ],
    )

    display_df, raw_df, troubleshooting_df = compute_clearance_table(
        config,
        target_date,
        now=datetime(2025, 10, 27, 12, 0, tzinfo=timezone.utc),
    )

    assert display_df.empty
    assert raw_df.empty
    assert not troubleshooting_df.empty

    debug_value = troubleshooting_df.loc[0, "Preflight check-ins"]
    parsed_debug = json.loads(debug_value)

    assert parsed_debug == [
        {
            "checkin": None,
            "checkinActual": None,
            "checkinDefault": None,
            "extraCheckins": [],
            "pilotRole": "CMD",
            "userId": "321",
        }
    ]
