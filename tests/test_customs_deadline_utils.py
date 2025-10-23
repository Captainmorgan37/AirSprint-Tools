from datetime import datetime, timedelta
from zoneinfo_compat import ZoneInfo

from customs_deadline_utils import (
    DEFAULT_BUSINESS_DAY_END,
    DEFAULT_BUSINESS_DAY_START,
    build_followup_candidates,
)


def _select_label(candidates, now):
    selected = candidates[-1]
    for candidate in candidates:
        if now <= candidate.end:
            selected = candidate
            break
    return selected.label


def test_followup_candidates_for_48h_lead():
    tz = ZoneInfo("America/Edmonton")
    event_local = datetime(2024, 4, 20, 18, 0, tzinfo=tz)
    lead_deadline = event_local - timedelta(hours=48)

    candidates = build_followup_candidates(
        event_local=event_local,
        tzinfo=tz,
        rule=None,
        lead_deadline_local=lead_deadline,
        lead_hours=48.0,
        departure_local=None,
    )

    assert [candidate.label for candidate in candidates] == [
        "Lead Deadline Day",
        "Next Day",
        "Same Day",
    ]

    lead_day = lead_deadline.date()
    assert candidates[0].start == datetime.combine(
        lead_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )
    assert candidates[0].end == datetime.combine(
        lead_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )
    assert "Lead time requirement 48h missed" in candidates[0].summary

    next_day = lead_day + timedelta(days=1)
    assert candidates[1].start == datetime.combine(
        next_day, DEFAULT_BUSINESS_DAY_START, tzinfo=tz
    )
    assert candidates[1].end == datetime.combine(
        next_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )
    assert "Next-day clearance window" in candidates[1].summary

    same_day = event_local.date()
    assert candidates[2].start == datetime.combine(
        same_day, DEFAULT_BUSINESS_DAY_START, tzinfo=tz
    )
    assert candidates[2].end == datetime.combine(
        same_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )

    # Stage selection mirrors the dashboard logic
    assert _select_label(candidates, datetime(2024, 4, 18, 16, 30, tzinfo=tz)) == "Lead Deadline Day"
    assert _select_label(candidates, datetime(2024, 4, 19, 10, 0, tzinfo=tz)) == "Next Day"
    assert _select_label(candidates, datetime(2024, 4, 20, 9, 0, tzinfo=tz)) == "Same Day"


def test_followup_candidates_for_24h_lead():
    tz = ZoneInfo("America/Edmonton")
    event_local = datetime(2024, 4, 20, 12, 0, tzinfo=tz)
    lead_deadline = event_local - timedelta(hours=24)

    candidates = build_followup_candidates(
        event_local=event_local,
        tzinfo=tz,
        rule=None,
        lead_deadline_local=lead_deadline,
        lead_hours=24.0,
        departure_local=None,
    )

    assert [candidate.label for candidate in candidates] == [
        "Lead Deadline Day",
        "Same Day",
    ]

    lead_day = lead_deadline.date()
    assert candidates[0].start == datetime(2024, 4, 19, 12, 0, tzinfo=tz)
    assert candidates[0].end == datetime.combine(
        lead_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )
    assert "Lead time requirement 24h missed" in candidates[0].summary

    same_day = event_local.date()
    assert candidates[1].start == datetime.combine(
        same_day, DEFAULT_BUSINESS_DAY_START, tzinfo=tz
    )
    assert candidates[1].end == datetime.combine(
        same_day, DEFAULT_BUSINESS_DAY_END, tzinfo=tz
    )
    assert "Same-day clearance window" in candidates[1].summary
