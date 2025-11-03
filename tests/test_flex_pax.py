"""Unit tests for Flex PAX scheduling behaviour."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("ortools.sat.python.cp_model")

from core.neg_scheduler import Flight, LeverPolicy, NegotiationScheduler, Tail


def test_flex_pax_extra_band_penalized_heavily():
    flights = [
        Flight(
            id="S1",
            origin="AAA",
            dest="BBB",
            duration_min=60,
            earliest_etd_min=60,
            latest_etd_min=60,
            preferred_etd_min=60,
            fleet_class="CJ",
            owner_id="Owner",
            current_tail_id="T1",
            shift_plus_cap=0,
            shift_minus_cap=0,
            original_shift_plus_cap=0,
            original_shift_minus_cap=0,
        ),
        Flight(
            id="S2",
            origin="BBB",
            dest="CCC",
            duration_min=60,
            earliest_etd_min=120,
            latest_etd_min=210,
            preferred_etd_min=120,
            fleet_class="CJ",
            owner_id="Owner",
            current_tail_id="T1",
            shift_plus_cap=60,
            shift_minus_cap=0,
            original_shift_plus_cap=30,
            original_shift_minus_cap=0,
        ),
    ]
    tails = [
        Tail(
            id="T1",
            fleet_class="CJ",
            available_from_min=0,
            available_to_min=24 * 60,
        )
    ]

    policy = LeverPolicy(
        max_shift_plus_min=60,
        max_shift_minus_min=30,
        cost_per_min_shift=1,
        turn_min=30,
        flex_pax_enabled=True,
        flex_pax_plus_cap=60,
        flex_pax_minus_cap=30,
        flex_pax_base_cap=20,
        flex_pax_cost_base=1,
        flex_pax_cost_extra=10,
    )

    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solutions = scheduler.solve(time_limit_s=5, top_n=1)

    assert solutions, "Expected at least one feasible solution"
    assigned = solutions[0]["assigned"]
    assert not assigned.empty

    second = assigned.loc[assigned["flight"] == "S2"].iloc[0]
    assert second["shift_plus"] == 30
    assert second["shift_plus_extra"] == 10
    # Extra minutes are heavily penalised, so the solver only uses them as needed.
    assert second["shift_minus"] == 0
    assert second["shift_minus_extra"] == 0
