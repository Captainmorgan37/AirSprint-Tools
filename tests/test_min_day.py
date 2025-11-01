"""Regression coverage for minimal duty-day feasibility scenarios."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("ortools.sat.python.cp_model")

from core.neg_scheduler import Flight, LeverPolicy, NegotiationScheduler, Tail
from core.reposition import build_reposition_matrix


def _three_leg_single_tail_scenario():
    flights = [
        Flight(
            id="L1",
            origin="CYBW",
            dest="CYVR",
            duration_min=120,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="A",
        ),
        Flight(
            id="L2",
            origin="CYVR",
            dest="CYBW",
            duration_min=60,
            earliest_etd_min=10 * 60,
            latest_etd_min=10 * 60,
            preferred_etd_min=10 * 60,
            fleet_class="CJ",
            owner_id="B",
        ),
        Flight(
            id="L3",
            origin="CYBW",
            dest="CYVR",
            duration_min=60,
            earliest_etd_min=12 * 60,
            latest_etd_min=12 * 60,
            preferred_etd_min=12 * 60,
            fleet_class="CJ",
            owner_id="C",
        ),
    ]
    tails = [Tail(id="C-GCJ1", fleet_class="CJ", available_from_min=8 * 60, available_to_min=14 * 60)]
    return flights, tails


def test_turn_time_requires_positive_shift():
    flights, tails = _three_leg_single_tail_scenario()

    tight_policy = LeverPolicy(
        max_shift_plus_min=0,
        max_shift_minus_min=0,
        cost_per_min_shift=10,
        outsource_cost=1_000,
        turn_min=30,
    )
    scheduler = NegotiationScheduler(flights, tails, tight_policy)
    status, solution = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solution["outsourced"].shape[0] == 1

    relaxed_policy = LeverPolicy(
        max_shift_plus_min=30,
        max_shift_minus_min=0,
        cost_per_min_shift=10,
        outsource_cost=1_000,
        turn_min=30,
    )
    scheduler_relaxed = NegotiationScheduler(flights, tails, relaxed_policy)
    status_relaxed, solution_relaxed = scheduler_relaxed.solve()

    assert status_relaxed == cp.OPTIMAL
    assert solution_relaxed["outsourced"].empty
    assert solution_relaxed["assigned"].shape[0] == len(flights)


def test_solver_handles_long_turn_buffer_window():
    flights = [
        Flight(
            id="L1",
            origin="CYBW",
            dest="CYVR",
            duration_min=5 * 60,
            earliest_etd_min=23 * 60,
            latest_etd_min=23 * 60,
            preferred_etd_min=23 * 60,
            fleet_class="CJ",
            owner_id="A",
        )
    ]
    tails = [
        Tail(
            id="C-GCJ1",
            fleet_class="CJ",
            available_from_min=20 * 60,
            available_to_min=2 * 24 * 60,
        )
    ]

    policy = LeverPolicy(turn_min=3 * 60)
    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solution = scheduler.solve()

    cp = scheduler.cp_model
    assert status in (cp.OPTIMAL, cp.FEASIBLE)
    assert solution["assigned"].shape[0] == len(flights)


def test_reposition_time_enforced_in_schedule():
    flights = [
        Flight(
            id="F1",
            origin="CYBW",
            dest="CYVR",
            duration_min=90,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="A",
            allow_outsource=False,
            shift_plus_cap=0,
            shift_minus_cap=0,
            shift_cost_per_min=0,
        ),
        Flight(
            id="F2",
            origin="CYEG",
            dest="CYBW",
            duration_min=90,
            earliest_etd_min=9 * 60,
            latest_etd_min=14 * 60,
            preferred_etd_min=9 * 60,
            fleet_class="CJ",
            owner_id="B",
            allow_outsource=False,
            shift_plus_cap=300,
            shift_minus_cap=0,
            shift_cost_per_min=0,
        ),
    ]
    tails = [
        Tail(
            id="C-GCJ1",
            fleet_class="CJ",
            available_from_min=7 * 60,
            available_to_min=20 * 60,
        )
    ]

    airports = {
        "CYBW": {"lat": 51.1031, "lon": -114.3740, "tz": "America/Edmonton"},
        "CYVR": {"lat": 49.1939, "lon": -123.1830, "tz": "America/Vancouver"},
        "CYEG": {"lat": 53.3097, "lon": -113.5800, "tz": "America/Edmonton"},
    }

    repo_matrix = build_reposition_matrix(flights, airports)

    policy = LeverPolicy(
        turn_min=30,
        cost_per_min_shift=0,
        outsource_cost=10_000,
        unassigned_penalty=10_000,
        tail_swap_cost=0,
        reposition_cost_per_min=3,
        max_shift_plus_min=300,
        max_shift_minus_min=0,
    )

    scheduler = NegotiationScheduler(
        flights, tails, policy, reposition_min=repo_matrix
    )
    status, solution = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL

    assigned_df = solution["assigned"]
    assert assigned_df.shape[0] == len(flights)

    assigned = {row["flight"]: row for row in assigned_df.to_dict("records")}
    first_start = assigned["F1"]["start_min"]
    second_start = assigned["F2"]["start_min"]

    required_gap = flights[0].duration_min + policy.turn_min + repo_matrix[0][1]
    assert second_start - first_start >= required_gap

    expected_cost = repo_matrix[0][1] * policy.reposition_cost_per_min
    assert solution["objective"] == pytest.approx(expected_cost)
