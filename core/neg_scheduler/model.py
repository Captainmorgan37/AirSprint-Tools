"""Core CP-SAT scheduling model for negotiation-aware dispatching."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .contracts import Leg, Tail


def _cp_model():
    """Lazy import OR-Tools so environments without it can still load modules."""

    from ortools.sat.python import cp_model

    return cp_model


@dataclass
class LeverPolicy:
    """Adjustable costs and bounds for negotiation levers."""

    max_shift_plus_min: int = 90
    max_shift_minus_min: int = 30
    cost_per_min_shift: int = 2
    outsource_cost: int = 1800
    unassigned_penalty: int = 5_000


class NegotiationScheduler:
    """Minimal CP-SAT assignment model with soft negotiation levers."""

    def __init__(self, legs: List[Leg], tails: List[Tail], policy: LeverPolicy):
        self.cp_model = _cp_model()
        self.legs = legs
        self.tails = tails
        self.policy = policy
        self.horizon = 24 * 60
        self.model = self.cp_model.CpModel()
        self._build()

    def _build(self) -> None:
        cp = self.cp_model
        m = self.model
        F = range(len(self.legs))
        T = range(len(self.tails))

        self.assign: Dict[Tuple[int, int], object] = {}
        self.outsource: Dict[int, object] = {}
        self.shift_plus: Dict[int, object] = {}
        self.shift_minus: Dict[int, object] = {}
        self.start: Dict[int, object] = {}
        self.intervals_per_tail: Dict[int, List[object]] = {k: [] for k in T}

        for i in F:
            leg = self.legs[i]
            self.outsource[i] = m.NewBoolVar(f"outsource[{i}]")
            self.shift_plus[i] = m.NewIntVar(0, self.policy.max_shift_plus_min, f"shift_plus[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, self.policy.max_shift_minus_min, f"shift_minus[{i}]")
            self.start[i] = m.NewIntVar(0, self.horizon, f"start[{i}]")

            for k in T:
                tail = self.tails[k]
                self.assign[(i, k)] = m.NewBoolVar(f"assign[{i},{k}]")
                if leg.fleet_class == tail.fleet_class:
                    duration = leg.block_min
                    interval = m.NewOptionalIntervalVar(
                        self.start[i], duration, self.start[i] + duration, self.assign[(i, k)], f"leg[{i},{k}]"
                    )
                    self.intervals_per_tail[k].append(interval)
                else:
                    m.Add(self.assign[(i, k)] == 0)

        for i in F:
            m.Add(sum(self.assign[(i, k)] for k in T) + self.outsource[i] == 1)
            leg = self.legs[i]
            lo = leg.etd_lo - self.shift_minus[i]
            hi = leg.etd_hi + self.shift_plus[i]
            m.Add(self.start[i] >= lo)
            m.Add(self.start[i] <= hi)

        for k in T:
            tail = self.tails[k]
            intervals = self.intervals_per_tail[k]
            if intervals:
                m.AddNoOverlap(intervals)
            for i in F:
                leg = self.legs[i]
                if leg.fleet_class != tail.fleet_class:
                    continue
                m.Add(self.start[i] >= tail.available_lo).OnlyEnforceIf(self.assign[(i, k)])
                m.Add(self.start[i] + leg.block_min <= tail.available_hi).OnlyEnforceIf(self.assign[(i, k)])

        objective_terms = []
        for i in F:
            objective_terms.append(self.outsource[i] * self.policy.outsource_cost)
            objective_terms.append(self.shift_plus[i] * self.policy.cost_per_min_shift)
            objective_terms.append(self.shift_minus[i] * self.policy.cost_per_min_shift)

        if not self.tails:
            # No tails means everything must be outsourced; penalise via unassigned penalty
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
        for i, leg in enumerate(self.legs):
            tail_id = None
            for k, tail in enumerate(self.tails):
                if solver.Value(self.assign[(i, k)]) == 1:
                    tail_id = tail.id
                    break
            if tail_id:
                assigned_rows.append(
                    {
                        "leg": leg.id,
                        "tail": tail_id,
                        "dep": leg.dep,
                        "arr": leg.arr,
                        "start_min": solver.Value(self.start[i]),
                        "duration_min": leg.block_min,
                        "shift_plus": solver.Value(self.shift_plus[i]),
                        "shift_minus": solver.Value(self.shift_minus[i]),
                    }
                )
            elif solver.Value(self.outsource[i]) == 1:
                outsourced_rows.append(
                    {
                        "leg": leg.id,
                        "owner": leg.owner_id,
                        "dep": leg.dep,
                        "arr": leg.arr,
                        "preferred_etd": leg.preferred_etd,
                    }
                )

        return status, {
            "assigned": pd.DataFrame(assigned_rows),
            "outsourced": pd.DataFrame(outsourced_rows),
            "objective": solver.ObjectiveValue()
            if status in (cp.OPTIMAL, cp.FEASIBLE)
            else math.inf,
        }

