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


def test_slot_outside_lead_window_reports_future_opening() -> None:
    leg = _build_leg(15)
    profile = _build_slot_profile("CYYC")
    parsed_restrictions = {
        "slot_required": True,
        "slot_lead_days": 10,
        "slot_notes": ["Slot required (10 day lead)"],
    }

    result = evaluate_slot_ppr(profile, leg, "DEP", None, parsed_restrictions)

    assert result.status == "PASS"
    assert result.summary == "Slot can only be obtained 10 days out"
    assert "Slot required (10 day lead)" in result.issues


def test_slot_inside_lead_window_flags_failure() -> None:
    leg = _build_leg(5)
    profile = _build_slot_profile("CYYZ")
    parsed_restrictions = {
        "slot_required": True,
        "slot_lead_days": 10,
    }

    result = evaluate_slot_ppr(profile, leg, "DEP", None, parsed_restrictions)

    assert result.status == "FAIL"
    assert result.summary == "Slot required"
    assert any("Inside 10-day" in issue for issue in result.issues)


def test_ssa_category_does_not_infer_slot_or_ppr() -> None:
    categories = {"MYAM": AirportCategoryRecord(icao="MYAM", category="SSA", notes=None)}

    profile = _build_slot_ppr_profile("MYAM", categories)

    assert profile.slot_required is False
    assert profile.ppr_required is False


def test_generic_category_notes_are_ignored_in_slot_ppr_details() -> None:
    categories = {
        "CYYC": AirportCategoryRecord(
            icao="CYYC", category="STANDARD", notes="Primary service area airport with no special handling required."
        )
    }

    profile = _build_slot_ppr_profile("CYYC", categories)
    leg = _build_leg(3)

    result = evaluate_slot_ppr(profile, leg, "DEP", None, {})

    assert profile.notes is None
    assert "Primary service area airport with no special handling required." not in result.issues
