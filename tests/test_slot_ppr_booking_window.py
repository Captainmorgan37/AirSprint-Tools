from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feasibility.airport_module import SlotPprProfile, _build_slot_ppr_profile, evaluate_slot_ppr
from feasibility.data_access import AirportCategoryRecord


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


def test_ssa_category_does_not_infer_slot_or_ppr() -> None:
    categories = {"MYAM": AirportCategoryRecord(icao="MYAM", category="SSA", notes=None)}

    profile = _build_slot_ppr_profile("MYAM", categories)

    assert profile.slot_required is False
    assert profile.ppr_required is False
