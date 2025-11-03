from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Tuple

from fl3xx_api import Fl3xxApiConfig
from oca_reports import MaxFlightTimeAlert, evaluate_flights_for_max_time, format_duration_label


def _flight(
    *,
    flight_id: int = 100,
    quote_id: str = "Q1",
    aircraft_category: str = "C25B",
    pax: int = 3,
    block_off: dt.datetime,
    block_on: dt.datetime,
) -> Dict[str, Any]:
    return {
        "flightId": flight_id,
        "quoteId": quote_id,
        "flightType": "PAX",
        "aircraftCategory": aircraft_category,
        "paxNumber": pax,
        "blocksoffestimated": block_off.isoformat().replace("+00:00", "Z"),
        "blocksonestimated": block_on.isoformat().replace("+00:00", "Z"),
        "airportFrom": "CYUL",
        "airportTo": "CYYZ",
        "registrationNumber": "C-GFSD",
        "bookingReference": "BK-100",
        "flightNumberCompany": "209-100",
    }


def _run_report(
    flights: List[Dict[str, Any]],
    *,
    leg_payloads: Dict[str, Any],
) -> Tuple[List[MaxFlightTimeAlert], Dict[str, Any], Dict[str, Any]]:
    def fake_fetch_flights(config, from_date, to_date):
        return flights, {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
        }

    def fake_fetch_leg_details(config, quote_id, session=None):
        payload = leg_payloads.get(quote_id)
        if isinstance(payload, Exception):
            raise payload
        return payload

    start = dt.date(2025, 10, 1)
    end = start + dt.timedelta(days=3)
    return evaluate_flights_for_max_time(
        Fl3xxApiConfig(),
        from_date=start,
        to_date=end,
        fetch_flights_fn=fake_fetch_flights,
        fetch_leg_details_fn=fake_fetch_leg_details,
    )


def test_identifies_over_max_time_flight():
    block_off = dt.datetime(2025, 10, 2, 1, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=4, minutes=30)
    flights = [_flight(block_off=block_off, block_on=block_on)]

    payloads = {"Q1": {"bookingNote": "FPL RUN BY OCA 2025-10-01"}}

    alerts, metadata, diagnostics = _run_report(flights, leg_payloads=payloads)

    assert metadata["from_date"] == "2025-10-01"
    assert diagnostics["flagged_flights"] == 1
    assert diagnostics["booking_note_confirmations"] == 1

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.overage_minutes == 10
    assert alert.booking_note_present is True
    assert alert.booking_note_confirms_fpl is True
    assert alert.booking_note == "FPL RUN BY OCA 2025-10-01"


def test_handles_missing_booking_note_payload():
    block_off = dt.datetime(2025, 10, 2, 1, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=5)
    flights = [_flight(block_off=block_off, block_on=block_on)]

    payloads = {"Q1": RuntimeError("network failure")}

    alerts, _metadata, diagnostics = _run_report(flights, leg_payloads=payloads)

    assert diagnostics["note_errors"] == 1
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.booking_note_present is False
    assert alert.booking_note is None


def test_skips_flights_below_threshold():
    block_off = dt.datetime(2025, 10, 2, 1, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=4)
    flights = [_flight(block_off=block_off, block_on=block_on)]

    payloads = {"Q1": {"bookingNote": "Checked"}}

    alerts, _metadata, diagnostics = _run_report(flights, leg_payloads=payloads)

    assert diagnostics["flagged_flights"] == 0
    assert alerts == []


def test_format_duration_label_handles_values():
    assert format_duration_label(125) == "2h 05m"
    assert format_duration_label(-30) == "-0h 30m"
    assert format_duration_label(None) == "—"
