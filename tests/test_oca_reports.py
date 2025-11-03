from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Tuple

from fl3xx_api import Fl3xxApiConfig
from oca_reports import (
    MaxFlightTimeAlert,
    ZfwFlightCheck,
    evaluate_flights_for_max_time,
    evaluate_flights_for_zfw_check,
    format_duration_label,
)


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
        "flightReference": "REF-100",
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


def _run_zfw_report(
    flights: List[Dict[str, Any]],
    *,
    leg_payloads: Dict[str, Any],
) -> Tuple[List[ZfwFlightCheck], Dict[str, Any], Dict[str, Any]]:
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

    start = dt.date(2025, 4, 12)
    end = start + dt.timedelta(days=4)
    return evaluate_flights_for_zfw_check(
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
    assert alert.flight_reference == "REF-100"
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


def test_extracts_leg_notes_field():
    block_off = dt.datetime(2025, 10, 2, 1, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=5)
    flights = [_flight(block_off=block_off, block_on=block_on)]

    payloads = {"Q1": {"notes": "Purpose of travel: Example"}}

    alerts, _metadata, diagnostics = _run_report(flights, leg_payloads=payloads)

    assert diagnostics["notes_found"] == 1
    assert alerts[0].booking_note == "Purpose of travel: Example"


def test_format_duration_label_handles_values():
    assert format_duration_label(125) == "2h 05m"
    assert format_duration_label(-30) == "-0h 30m"
    assert format_duration_label(None) == "—"


def test_zfw_report_identifies_threshold_pax_and_confirms_note():
    block_off = dt.datetime(2025, 4, 13, 17, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=2)
    flights = [
        _flight(
            aircraft_category="C25B",
            pax=7,
            block_off=block_off,
            block_on=block_on,
            quote_id="Q100",
            flight_id=200,
        )
    ]
    payloads = {
        "Q100": {
            "bookingNote": "ZFW – CJ3 – OK WITH CURRENT PAX/BAGGAGE – 1002/210 – MND 14APR25",
        }
    }

    items, metadata, diagnostics = _run_zfw_report(flights, leg_payloads=payloads)

    assert metadata["from_date"] == "2025-04-12"
    assert diagnostics["flagged_flights"] == 1
    assert diagnostics["zfw_confirmations"] == 1

    assert len(items) == 1
    item = items[0]
    assert item.pax_threshold == 6
    assert item.flight_reference == "REF-100"
    assert item.booking_note_present is True
    assert item.booking_note_confirms_zfw is True
    assert item.booking_note.startswith("ZFW")


def test_zfw_report_handles_alias_and_missing_notes():
    block_off = dt.datetime(2025, 4, 14, 10, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=2)
    flights = [
        _flight(
            aircraft_category="E550",
            pax=10,
            block_off=block_off,
            block_on=block_on,
            quote_id="Q200",
            flight_id=210,
        )
    ]
    payloads = {"Q200": RuntimeError("leg error")}

    items, _metadata, diagnostics = _run_zfw_report(flights, leg_payloads=payloads)

    assert diagnostics["note_errors"] == 1
    assert diagnostics["notes_requested"] == 0
    assert len(items) == 1
    item = items[0]
    assert item.pax_threshold == 9
    assert item.booking_note_present is False
    assert item.booking_note is None


def test_zfw_report_skips_flights_below_threshold_or_missing_pax():
    block_off = dt.datetime(2025, 4, 15, 8, 0, tzinfo=dt.timezone.utc)
    block_on = block_off + dt.timedelta(hours=2)
    flights = [
        _flight(
            aircraft_category="C25A",
            pax=4,
            block_off=block_off,
            block_on=block_on,
            quote_id="Q300",
            flight_id=220,
        ),
        {
            **_flight(aircraft_category="C25A", pax=6, block_off=block_off, block_on=block_on),
            "paxNumber": None,
            "quoteId": "Q301",
            "flightId": 221,
        },
    ]
    payloads: Dict[str, Any] = {}

    items, _metadata, diagnostics = _run_zfw_report(flights, leg_payloads=payloads)

    assert diagnostics["missing_pax_count"] == 1
    assert diagnostics["flagged_flights"] == 0
    assert items == []
