"""Core CP-SAT scheduling model for negotiation-aware dispatching."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .contracts import Flight, Tail


def _cp_model():
    """Lazy import OR-Tools so environments without it can still load modules."""

    from ortools.sat.python import cp_model

    return cp_model


@dataclass(slots=True)
class LeverPolicy:
    """Adjustable costs and bounds for negotiation levers."""

    max_shift_plus_min: int = 90
    max_shift_minus_min: int = 30
    cost_per_min_shift: int = 2
    outsource_cost: int = 1800
    unassigned_penalty: int = 5_000
    turn_min: int = 30
    tail_swap_cost: int = 1_200


class NegotiationScheduler:
    """Minimal CP-SAT assignment model with soft negotiation levers."""

    def __init__(self, flights: List[Flight], tails: List[Tail], policy: LeverPolicy):
        self.cp_model = _cp_model()
        self.flights = flights
        self.tails = tails
        self.policy = policy
        self.horizon = 24 * 60
        self.model = self.cp_model.CpModel()
        self._build()

    def _build(self) -> None:
        m = self.model
        F = range(len(self.flights))
        T = range(len(self.tails))
        tail_index = {tail.id: idx for idx, tail in enumerate(self.tails)}

        self.assign: Dict[Tuple[int, int], object] = {}
        self.outsource: Dict[int, object] = {}
        self.shift_plus: Dict[int, object] = {}
        self.shift_minus: Dict[int, object] = {}
        self.start: Dict[int, object] = {}
        self.swap: Dict[int, object] = {}
        self.intervals_per_tail: Dict[int, List[object]] = {k: [] for k in T}

        for i in F:
            flight = self.flights[i]
            self.outsource[i] = m.NewBoolVar(f"outsource[{i}]")
            shift_plus_cap = min(self.policy.max_shift_plus_min, flight.shift_plus_cap)
            shift_minus_cap = min(self.policy.max_shift_minus_min, flight.shift_minus_cap)
            self.shift_plus[i] = m.NewIntVar(0, shift_plus_cap, f"shift_plus[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, shift_minus_cap, f"shift_minus[{i}]")
            self.start[i] = m.NewIntVar(0, self.horizon, f"start[{i}]")

            for k in T:
                tail = self.tails[k]
                self.assign[(i, k)] = m.NewBoolVar(f"assign[{i},{k}]")
                if flight.fleet_class != tail.fleet_class:
                    m.Add(self.assign[(i, k)] == 0)
                    continue

                duration_with_turn = flight.duration_min + self.policy.turn_min
                interval = m.NewOptionalIntervalVar(
                    self.start[i],
                    duration_with_turn,
                    self.start[i] + duration_with_turn,
                    self.assign[(i, k)],
                    f"flight[{i},{k}]",
                )
                self.intervals_per_tail[k].append(interval)

        for i in F:
            m.Add(sum(self.assign[(i, k)] for k in T) + self.outsource[i] == 1)
            flight = self.flights[i]
            m.Add(self.start[i] >= flight.earliest_etd_min)
            m.Add(self.start[i] <= flight.latest_etd_min)
            m.Add(self.start[i] >= flight.preferred_etd_min - self.shift_minus[i])
            m.Add(self.start[i] <= flight.preferred_etd_min + self.shift_plus[i])

            if not flight.allow_outsource:
                m.Add(self.outsource[i] == 0)

            if flight.current_tail_id and flight.current_tail_id in tail_index:
                current_idx = tail_index[flight.current_tail_id]
                current_tail = self.tails[current_idx]
                if current_tail.fleet_class == flight.fleet_class:
                    if not flight.allow_tail_swap:
                        m.Add(self.assign[(i, current_idx)] == 1)
                        for k in T:
                            if k != current_idx:
                                m.Add(self.assign[(i, k)] == 0)
                    else:
                        swap = m.NewBoolVar(f"swap[{i}]")
                        self.swap[i] = swap
                        m.Add(self.assign[(i, current_idx)] == 0).OnlyEnforceIf(swap)
                        m.Add(self.assign[(i, current_idx)] == 1).OnlyEnforceIf(swap.Not())
                        for k in T:
                            if k == current_idx:
                                continue
                            m.Add(self.assign[(i, k)] == 0).OnlyEnforceIf(swap.Not())

        for k in T:
            tail = self.tails[k]
            intervals = self.intervals_per_tail[k]
            if intervals:
                m.AddNoOverlap(intervals)
            for i in F:
                flight = self.flights[i]
                if flight.fleet_class != tail.fleet_class:
                    continue
                m.Add(self.start[i] >= tail.available_from_min).OnlyEnforceIf(self.assign[(i, k)])
                m.Add(self.start[i] + flight.duration_min <= tail.available_to_min).OnlyEnforceIf(
                    self.assign[(i, k)]
                )

        objective_terms = []
        for i in F:
            flight = self.flights[i]
            objective_terms.append(self.outsource[i] * self.policy.outsource_cost)
            objective_terms.append(self.shift_plus[i] * flight.shift_cost_per_min)
            objective_terms.append(self.shift_minus[i] * flight.shift_cost_per_min)
            if i in self.swap:
                objective_terms.append(self.swap[i] * self.policy.tail_swap_cost)

        if not self.tails:
            for i in F:
                objective_terms.append(self.outsource[i] * self.policy.unassigned_penalty)

        m.Minimize(sum(objective_terms))

    def solve(self, time_limit_s: Optional[int] = 5, workers: int = 8):
        cp = self.cp_model
        solver = cp.CpSolver()
        if time_limit_s:
            solver.parameters.max_time_in_seconds = time_limit_s
        solver.parameters.num_search_workers = workers

        status = solver.Solve(self.model)
        assigned_rows, outsourced_rows = [], []
        for i, flight in enumerate(self.flights):
            tail_id = None
            for k, tail in enumerate(self.tails):
                if solver.Value(self.assign[(i, k)]) == 1:
                    tail_id = tail.id
                    break
            if tail_id:
                start_val = solver.Value(self.start[i])
                assigned_rows.append(
                    {
                        "flight": flight.id,
                        "tail": tail_id,
                        "original_tail": flight.current_tail_id,
                        "tail_swapped": bool(
                            flight.current_tail_id and tail_id != flight.current_tail_id
                        ),
                        "origin": flight.origin,
                        "dest": flight.dest,
                        "start_min": start_val,
                        "end_min": start_val + flight.duration_min,
                        "duration_min": flight.duration_min,
                        "shift_plus": solver.Value(self.shift_plus[i]),
                        "shift_minus": solver.Value(self.shift_minus[i]),
                    }
                )
            elif solver.Value(self.outsource[i]) == 1:
                outsourced_rows.append(
                    {
                        "flight": flight.id,
                        "owner": flight.owner_id,
                        "origin": flight.origin,
                        "dest": flight.dest,
                        "preferred_etd_min": flight.preferred_etd_min,
                    }
                )

        return status, {
            "assigned": pd.DataFrame(assigned_rows),
            "outsourced": pd.DataFrame(outsourced_rows),
            "objective": solver.ObjectiveValue()
            if status in (cp.OPTIMAL, cp.FEASIBLE)
            else math.inf,
        }
