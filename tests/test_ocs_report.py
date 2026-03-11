import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from morning_reports import _build_ocs_report


def _row(*, leg_id: str, tail: str, flight_type: str, dep: str, arr: str, booking: str):
    return {
        "leg_id": leg_id,
        "tail": tail,
        "flightType": flight_type,
        "dep_time": dep,
        "arrivalTimeUtc": arr,
        "bookingIdentifier": booking,
        "departure_airport": "CYYC",
        "arrival_airport": "CYVR",
    }


def test_ocs_report_flags_long_pos_leg():
    result = _build_ocs_report(
        [
            _row(
                leg_id="L1",
                tail="C-GAAA",
                flight_type="POS",
                dep="2024-05-01T10:00:00Z",
                arr="2024-05-01T12:10:00Z",
                booking="B1",
            )
        ]
    )

    assert result.code == "16.1.13"
    assert len(result.rows) == 1
    assert result.rows[0]["is_long_pos"] is True
    assert result.rows[0]["is_back_to_back_pos"] is False
    assert "POS duration ≥ 2:00" in result.rows[0]["reason"]


def test_ocs_report_flags_back_to_back_pos_without_pax_gap():
    rows = [
        _row(
            leg_id="L1",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T08:00:00Z",
            arr="2024-05-01T08:45:00Z",
            booking="B1",
        ),
        _row(
            leg_id="L2",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T09:30:00Z",
            arr="2024-05-01T10:15:00Z",
            booking="B2",
        ),
    ]

    result = _build_ocs_report(rows)

    assert len(result.rows) == 2
    by_leg = {row["leg_id"]: row for row in result.rows}
    assert by_leg["L1"]["is_back_to_back_pos"] is True
    assert by_leg["L2"]["is_back_to_back_pos"] is True
    assert "Back-to-back POS legs" in by_leg["L1"]["reason"]
    assert "Back-to-back POS legs" in by_leg["L2"]["reason"]


def test_ocs_report_does_not_flag_pos_when_pax_between_legs():
    rows = [
        _row(
            leg_id="L1",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T08:00:00Z",
            arr="2024-05-01T08:45:00Z",
            booking="B1",
        ),
        _row(
            leg_id="L2",
            tail="C-GAAA",
            flight_type="PAX",
            dep="2024-05-01T09:00:00Z",
            arr="2024-05-01T10:00:00Z",
            booking="B2",
        ),
        _row(
            leg_id="L3",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T10:30:00Z",
            arr="2024-05-01T11:15:00Z",
            booking="B3",
        ),
    ]

    result = _build_ocs_report(rows)

    assert result.rows == []


def test_ocs_report_disregards_placeholder_add_remove_lines():
    rows = [
        _row(
            leg_id="L1",
            tail="ADD CJ2+ EAST",
            flight_type="POS",
            dep="2024-05-01T08:00:00Z",
            arr="2024-05-01T11:00:00Z",
            booking="B1",
        ),
        _row(
            leg_id="L2",
            tail="REMOVE LINE",
            flight_type="POS",
            dep="2024-05-01T12:00:00Z",
            arr="2024-05-01T15:00:00Z",
            booking="B2",
        ),
    ]

    result = _build_ocs_report(rows)

    assert result.rows == []


def test_ocs_report_disregards_back_to_back_pos_with_same_booking_reference():
    rows = [
        _row(
            leg_id="L1",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T08:00:00Z",
            arr="2024-05-01T08:45:00Z",
            booking="B1",
        ),
        _row(
            leg_id="L2",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T09:30:00Z",
            arr="2024-05-01T10:15:00Z",
            booking="B1",
        ),
    ]

    result = _build_ocs_report(rows)

    assert result.rows == []


def test_ocs_report_rows_are_sorted_by_departure_time():
    rows = [
        _row(
            leg_id="L2",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T11:00:00Z",
            arr="2024-05-01T13:10:00Z",
            booking="B2",
        ),
        _row(
            leg_id="L1",
            tail="C-GAAA",
            flight_type="POS",
            dep="2024-05-01T08:00:00Z",
            arr="2024-05-01T10:10:00Z",
            booking="B1",
        ),
    ]

    result = _build_ocs_report(rows)

    assert [row["leg_id"] for row in result.rows] == ["L1", "L2"]
