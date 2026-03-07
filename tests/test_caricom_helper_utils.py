from __future__ import annotations

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from caricom_helper_utils import select_booking_leg_for_caricom


def _is_caricom_airport(code: object) -> bool:
    return str(code or "").upper() in {"TAPA", "MKJP", "TBPB"}


def test_select_booking_leg_prefers_arrival_into_caricom() -> None:
    matched_legs = [
        {"dep_time": "2026-01-01T08:00:00Z", "departure_airport": "CYYC", "arrival_airport": "CYUL"},
        {"dep_time": "2026-01-01T12:00:00Z", "departure_airport": "CYUL", "arrival_airport": "TAPA"},
    ]

    selected = select_booking_leg_for_caricom(matched_legs, _is_caricom_airport)

    assert selected is not None
    assert selected["arrival_airport"] == "TAPA"


def test_select_booking_leg_falls_back_to_touching_caricom() -> None:
    matched_legs = [
        {"dep_time": "2026-01-01T08:00:00Z", "departure_airport": "TBPB", "arrival_airport": "KTEB"},
        {"dep_time": "2026-01-01T09:00:00Z", "departure_airport": "KTEB", "arrival_airport": "KBED"},
    ]

    selected = select_booking_leg_for_caricom(matched_legs, _is_caricom_airport)

    assert selected is not None
    assert selected["departure_airport"] == "TBPB"


def test_select_booking_leg_falls_back_to_earliest_departure() -> None:
    matched_legs = [
        {"dep_time": "2026-01-01T10:00:00Z", "departure_airport": "KTEB", "arrival_airport": "KBED"},
        {"dep_time": "2026-01-01T08:00:00Z", "departure_airport": "CYUL", "arrival_airport": "CYYC"},
    ]

    selected = select_booking_leg_for_caricom(matched_legs, _is_caricom_airport)

    assert selected is not None
    assert selected["dep_time"] == "2026-01-01T08:00:00Z"
