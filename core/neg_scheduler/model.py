"""Core CP-SAT scheduling model for negotiation-aware dispatching."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .contracts import Flight, Tail


def _norm_class(s: str) -> str:
    """Normalize fleet classes so variants share compatibility buckets."""

    s = (s or "").upper()
    if s.startswith("CJ"):
        return "CJ"
    if s.startswith("LEG") or s.startswith("E"):
        return "LEG"
    if s == "GEN":
        return "GEN"
    return s


def _class_compatible(fclass: str, tclass: str) -> bool:
    """Return True when a tail can operate a flight based on fleet class."""

    flight_class = _norm_class(fclass)
    tail_class = _norm_class(tclass)
    if tail_class == "GEN":
        return True
    return flight_class == tail_class


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
    reposition_cost_per_min: int = 1


class NegotiationScheduler:
    """Minimal CP-SAT assignment model with soft negotiation levers."""

    def __init__(
        self,
        flights: List[Flight],
        tails: List[Tail],
        policy: LeverPolicy,
        reposition_min: Optional[List[List[int]]] = None,
    ):
        self.cp_model = _cp_model()
        self.flights = flights
        self.tails = tails
        self.policy = policy
        self.reposition_min = reposition_min or [
            [0] * len(flights) for _ in range(len(flights))
        ]
        self.horizon = self._compute_horizon()
        self.model = self.cp_model.CpModel()
        self._build()

    def _compute_horizon(self) -> int:
        """Derive an upper bound for the scheduling horizon."""

        default_horizon = 24 * 60
        latest = default_horizon

        for flight in self.flights:
            latest = max(
                latest,
                flight.latest_etd_min + flight.duration_min + self.policy.turn_min,
            )

        for tail in self.tails:
            latest = max(latest, tail.available_to_min + self.policy.turn_min)

        return latest

    def _build(self) -> None:
        m = self.model
        F = range(len(self.flights))
        T = range(len(self.tails))
        tail_index = {tail.id: idx for idx, tail in enumerate(self.tails)}

        active_tail_ids = {
            flight.current_tail_id
            for flight in self.flights
            if flight.current_tail_id
        }

        self.assign: Dict[Tuple[int, int], object] = {}
        self.outsource: Dict[int, object] = {}
        self.shift_plus: Dict[int, object] = {}
        self.shift_minus: Dict[int, object] = {}
        self.start: Dict[int, object] = {}
        self.swap: Dict[int, object] = {}
        self.intervals_per_tail: Dict[int, List[object]] = {k: [] for k in T}
        self.order: Dict[Tuple[int, int, int], object] = {}

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
                if active_tail_ids and tail.id not in active_tail_ids and (
                    flight.current_tail_id != tail.id
                ):
                    m.Add(self.assign[(i, k)] == 0)
                    continue
                if not _class_compatible(flight.fleet_class, tail.fleet_class):
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
            m.Add(self.start[i] >= flight.earliest_etd_min - self.shift_minus[i])
            m.Add(self.start[i] <= flight.latest_etd_min + self.shift_plus[i])
            m.Add(self.start[i] - flight.preferred_etd_min <= self.shift_plus[i])
            m.Add(flight.preferred_etd_min - self.start[i] <= self.shift_minus[i])

            if not flight.allow_outsource:
                m.Add(self.outsource[i] == 0)

            if flight.current_tail_id:
                k_cur = tail_index.get(flight.current_tail_id)
                if k_cur is None:
                    # Referenced tail is no longer available; treat the flight as free.
                    pass
                else:
                    current_tail = self.tails[k_cur]
                    if not _class_compatible(
                        flight.fleet_class, current_tail.fleet_class
                    ):
                        # Incompatible tail under the solver's rules; allow reassignment.
                        pass
                    else:
                        if not flight.allow_tail_swap:
                            for k in T:
                                m.Add(self.assign[(i, k)] == (1 if k == k_cur else 0))
                        else:
                            swap = m.NewBoolVar(f"swap[{i}]")
                            self.swap[i] = swap

                            # swap = 0 → stay on current tail
                            m.Add(self.assign[(i, k_cur)] == 1).OnlyEnforceIf(swap.Not())
                            for k in T:
                                if k == k_cur:
                                    continue
                                m.Add(self.assign[(i, k)] == 0).OnlyEnforceIf(swap.Not())

                            # swap = 1 → move off current tail
                            m.Add(self.assign[(i, k_cur)] == 0).OnlyEnforceIf(swap)

        for k in T:
            tail = self.tails[k]
            intervals = self.intervals_per_tail[k]
            if intervals:
                m.AddNoOverlap(intervals)
            for i in F:
                flight = self.flights[i]
                if not _class_compatible(flight.fleet_class, tail.fleet_class):
                    continue
                m.Add(self.start[i] >= tail.available_from_min).OnlyEnforceIf(self.assign[(i, k)])
                m.Add(
                    self.start[i] + flight.duration_min + self.policy.turn_min
                    <= tail.available_to_min
                ).OnlyEnforceIf(self.assign[(i, k)])

        o = self.order
        for k in T:
            for i in F:
                for j in F:
                    if i == j:
                        continue
                    o[(i, j, k)] = m.NewBoolVar(f"ord[{i},{j},{k}]")

        for k in T:
            for i in F:
                for j in F:
                    if i == j:
                        continue
                    lhs = o[(i, j, k)] + o[(j, i, k)]
                    m.Add(lhs >= self.assign[(i, k)] + self.assign[(j, k)] - 1)
                    m.Add(lhs <= self.assign[(i, k)] + self.assign[(j, k)])

                    repo = self.reposition_min[i][j]
                    m.Add(
                        self.start[j]
                        >= self.start[i]
                        + self.flights[i].duration_min
                        + self.policy.turn_min
                        + repo
                    ).OnlyEnforceIf(o[(i, j, k)])

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

        for k in T:
            for i in F:
                for j in F:
                    if i == j:
                        continue
                    repo = self.reposition_min[i][j]
                    if repo > 0:
                        objective_terms.append(
                            o[(i, j, k)] * repo * self.policy.reposition_cost_per_min
                        )

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
