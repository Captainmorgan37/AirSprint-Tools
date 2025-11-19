from datetime import datetime, timedelta, timezone

from feasibility.airport_module import SlotPprProfile, evaluate_slot_ppr


def _build_leg(days_ahead: int) -> dict:
    return {"departure_date_utc": (datetime.now(timezone.utc) + timedelta(days=days_ahead)).isoformat()}


def _build_slot_profile(icao: str) -> SlotPprProfile:
    return SlotPprProfile(
        icao=icao,
        slot_required=True,
        ppr_required=False,
        slot_lead_days=None,
        ppr_lead_days=None,
        notes=None,
    )


def test_cyyz_slot_skipped_outside_booking_window() -> None:
    leg = _build_leg(15)
    profile = _build_slot_profile("CYYZ")

    result = evaluate_slot_ppr(profile, leg, "DEP", None)

    assert result.status == "PASS"
    assert result.summary == "No slot/PPR requirement"
    assert result.issues == []


def test_cyvr_slot_flagged_within_booking_window() -> None:
    leg = _build_leg(2)
    profile = _build_slot_profile("CYVR")

    result = evaluate_slot_ppr(profile, leg, "DEP", None)

    assert result.status == "CAUTION"
    assert result.summary == "Slot required"
    assert "Slot required for CYVR." in result.issues
