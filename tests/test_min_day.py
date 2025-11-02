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


def _long_gap_two_leg_scenario():
    flights = [
        Flight(
            id="EARLY",
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
            id="LATE",
            origin="CYVR",
            dest="CYBW",
            duration_min=90,
            earliest_etd_min=22 * 60,
            latest_etd_min=22 * 60,
            preferred_etd_min=22 * 60,
            fleet_class="CJ",
            owner_id="B",
        ),
    ]
    tails = [
        Tail(
            id="C-GCJ1",
            fleet_class="CJ",
            available_from_min=7 * 60,
            available_to_min=24 * 60,
        )
    ]
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
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions
    solution = solutions[0]
    assert solution["outsourced"].shape[0] == 1

    relaxed_policy = LeverPolicy(
        max_shift_plus_min=30,
        max_shift_minus_min=0,
        cost_per_min_shift=10,
        outsource_cost=1_000,
        turn_min=30,
    )
    scheduler_relaxed = NegotiationScheduler(flights, tails, relaxed_policy)
    status_relaxed, solutions_relaxed = scheduler_relaxed.solve()

    assert status_relaxed == cp.OPTIMAL
    assert solutions_relaxed
    solution_relaxed = solutions_relaxed[0]
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
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status in (cp.OPTIMAL, cp.FEASIBLE)
    assert solutions
    solution = solutions[0]
    assert solution["assigned"].shape[0] == len(flights)


def test_positioning_leg_can_be_skipped_before_outsource():
    flights = [
        Flight(
            id="PAX1",
            origin="CYBW",
            dest="CYVR",
            duration_min=120,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="OWN1",
            allow_outsource=False,
        ),
        Flight(
            id="POS1",
            origin="CYVR",
            dest="CYBW",
            duration_min=60,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="OWN2",
            allow_outsource=False,
            intent="POS",
            must_cover=False,
        ),
    ]
    tails = [Tail(id="C-GCJ1", fleet_class="CJ", available_from_min=8 * 60, available_to_min=12 * 60)]

    policy = LeverPolicy(max_shift_plus_min=0, max_shift_minus_min=0)
    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions
    solution = solutions[0]

    assert solution["outsourced"].empty
    assert solution["skipped"].shape[0] == 1
    assert set(solution["assigned"]["flight"]) == {"PAX1"}
    assert set(solution["skipped"]["flight"]) == {"POS1"}


def test_initial_reposition_leg_created_for_first_assignment():
    flights = [
        Flight(
            id="F1",
            origin="CYYZ",
            dest="CYUL",
            duration_min=120,
            earliest_etd_min=9 * 60,
            latest_etd_min=9 * 60,
            preferred_etd_min=9 * 60,
            fleet_class="CJ",
            owner_id="OWN1",
        )
    ]
    tails = [
        Tail(
            id="C-GCJ1",
            fleet_class="CJ",
            available_from_min=0,
            available_to_min=24 * 60,
            last_position_airport="CYUL",
            last_position_ready_min=0,
        )
    ]

    policy = LeverPolicy(turn_min=30)
    scheduler = NegotiationScheduler(
        flights,
        tails,
        policy,
        reposition_min=[[0]],
        initial_reposition_min=[[120]],
    )
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status in (cp.OPTIMAL, cp.FEASIBLE)
    assert solutions
    solution = solutions[0]
    assert solution["assigned"].shape[0] == 1
    assert not solution["reposition"].empty
    row = solution["reposition"].iloc[0]
    assert row["origin"] == "CYUL"
    assert row["dest"] == "CYYZ"
    assert row["duration_min"] == 120
    assert row["source_flight"] is None
    assert row["target_flight"] == "F1"


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
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions

    solution = solutions[0]
    assigned_df = solution["assigned"]
    assert assigned_df.shape[0] == len(flights)

    assigned = {row["flight"]: row for row in assigned_df.to_dict("records")}
    first_start = assigned["F1"]["start_min"]
    second_start = assigned["F2"]["start_min"]

    required_gap = flights[0].duration_min + policy.turn_min + repo_matrix[0][1]
    assert second_start - first_start >= required_gap

    expected_cost = repo_matrix[0][1] * policy.reposition_cost_per_min
    assert solution["objective"] == pytest.approx(expected_cost)


def test_scheduler_handles_ragged_reposition_matrix():
    flights, tails = _three_leg_single_tail_scenario()
    flights = flights[:2]

    ragged_matrix = [[0, 42]]

    policy = LeverPolicy(turn_min=30)
    scheduler = NegotiationScheduler(
        flights,
        tails,
        policy,
        reposition_min=ragged_matrix,
    )

    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status in (cp.OPTIMAL, cp.FEASIBLE)
    assert solutions
    solution = solutions[0]
    assert solution["assigned"].shape[0] == len(flights)

    # Ensure the provided reposition value is preserved for the overlapping portion.
    assert scheduler.reposition_min[0][1] == 42


def test_unscheduled_flight_does_not_use_idle_tail():
    flights = [
        Flight(
            id="F1",
            origin="CYBW",
            dest="CYVR",
            duration_min=120,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="A",
            current_tail_id="C-GCJ1",
            allow_outsource=False,
            allow_tail_swap=False,
            shift_plus_cap=0,
            shift_minus_cap=0,
            shift_cost_per_min=0,
        ),
        Flight(
            id="F2",
            origin="CYBW",
            dest="CYVR",
            duration_min=60,
            earliest_etd_min=9 * 60,
            latest_etd_min=12 * 60,
            preferred_etd_min=9 * 60,
            fleet_class="CJ",
            owner_id="B",
            allow_outsource=False,
            shift_plus_cap=180,
            shift_minus_cap=0,
            shift_cost_per_min=5,
        ),
    ]

    tails = [
        Tail(
            id="C-GCJ1",
            fleet_class="CJ",
            available_from_min=7 * 60,
            available_to_min=20 * 60,
        ),
        Tail(
            id="C-GCJ2",
            fleet_class="CJ",
            available_from_min=7 * 60,
            available_to_min=20 * 60,
        ),
    ]

    policy = LeverPolicy(turn_min=30)
    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions

    solution = solutions[0]
    assigned = solution["assigned"].set_index("flight")
    assert assigned.loc["F1", "tail"] == "C-GCJ1"
    assert assigned.loc["F2", "tail"] == "C-GCJ1"


def test_leg_flight_prefers_leg_tail():
    flights = [
        Flight(
            id="LEG-FLT",
            origin="CYBW",
            dest="CYVR",
            duration_min=90,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="LEG",
            owner_id="O-LEG",
        )
    ]
    tails = [
        Tail(id="C-GLEG1", fleet_class="LEG", available_from_min=7 * 60, available_to_min=20 * 60),
        Tail(id="C-GCJ1", fleet_class="CJ", available_from_min=7 * 60, available_to_min=20 * 60),
    ]

    policy = LeverPolicy()
    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions
    solution = solutions[0]
    assert solution["assigned"].shape[0] == 1
    assert solution["assigned"].iloc[0]["tail"] == "C-GLEG1"


def test_cj_flight_can_use_leg_tail_with_penalty():
    flights = [
        Flight(
            id="CJ-FLT",
            origin="CYBW",
            dest="CYVR",
            duration_min=60,
            earliest_etd_min=9 * 60,
            latest_etd_min=9 * 60,
            preferred_etd_min=9 * 60,
            fleet_class="CJ",
            owner_id="O-CJ",
        )
    ]
    tails = [
        Tail(id="C-GLEG1", fleet_class="LEG", available_from_min=8 * 60, available_to_min=18 * 60)
    ]

    policy = LeverPolicy()
    scheduler = NegotiationScheduler(flights, tails, policy)
    status, solutions = scheduler.solve()

    cp = scheduler.cp_model
    assert status == cp.OPTIMAL
    assert solutions
    solution = solutions[0]
    assert solution["assigned"].shape[0] == 1
    assert solution["assigned"].iloc[0]["tail"] == "C-GLEG1"


def test_max_day_length_limit_outsources_when_exceeded():
    flights, tails = _long_gap_two_leg_scenario()

    no_limit_policy = LeverPolicy(
        turn_min=30,
        outsource_cost=5_000,
        unassigned_penalty=5_000,
        tail_swap_cost=0,
        reposition_cost_per_min=0,
    )

    scheduler_no_limit = NegotiationScheduler(flights, tails, no_limit_policy)
    status_no_limit, solutions_no_limit = scheduler_no_limit.solve()

    cp = scheduler_no_limit.cp_model
    assert status_no_limit == cp.OPTIMAL
    assert solutions_no_limit
    solution_no_limit = solutions_no_limit[0]
    assert solution_no_limit["assigned"].shape[0] == len(flights)

    limited_policy = LeverPolicy(
        turn_min=30,
        outsource_cost=5_000,
        unassigned_penalty=5_000,
        tail_swap_cost=0,
        reposition_cost_per_min=0,
        max_day_length_min=765,
    )

    scheduler_limited = NegotiationScheduler(flights, tails, limited_policy)
    status_limited, solutions_limited = scheduler_limited.solve()

    assert status_limited == cp.OPTIMAL
    assert solutions_limited
    solution_limited = solutions_limited[0]
    assigned_limited = solution_limited["assigned"].shape[0]
    outsourced_limited = solution_limited["outsourced"].shape[0]

    assert assigned_limited + outsourced_limited == len(flights)
    assert assigned_limited < len(flights)


def test_scheduler_returns_multiple_solutions_and_reuses_model():
    flights = [
        Flight(
            id="F1",
            origin="CYBW",
            dest="CYVR",
            duration_min=120,
            earliest_etd_min=8 * 60,
            latest_etd_min=8 * 60,
            preferred_etd_min=8 * 60,
            fleet_class="CJ",
            owner_id="A",
        )
    ]
    tails = [
        Tail(id="CJ1", fleet_class="CJ", available_from_min=7 * 60, available_to_min=22 * 60),
        Tail(id="CJ2", fleet_class="CJ", available_from_min=7 * 60, available_to_min=22 * 60),
    ]

    scheduler = NegotiationScheduler(flights, tails, LeverPolicy())
    status, solutions = scheduler.solve(top_n=2)

    cp = scheduler.cp_model
    assert status in (cp.OPTIMAL, cp.FEASIBLE)
    assert len(solutions) == 2

    assigned_tails = {
        tuple(solution["assigned"]["tail"].tolist())
        for solution in solutions
        if not solution["assigned"].empty and "tail" in solution["assigned"]
    }
    assert len(assigned_tails) == len(solutions)

    status_again, solutions_again = scheduler.solve()
    assert status_again in (cp.OPTIMAL, cp.FEASIBLE)
    assert solutions_again
