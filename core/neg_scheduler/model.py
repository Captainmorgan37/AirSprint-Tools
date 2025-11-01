# core/neg_scheduler/model.py
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import math
import pandas as pd

from .contracts import Flight, Tail

def _cp_model():
    # Lazy import so other pages don't crash if OR-Tools isn't present
    from ortools.sat.python import cp_model
    return cp_model

@dataclass
class LeverPolicy:
    max_shift_plus_min: int = 90
    max_shift_minus_min: int = 30
    cost_per_min_shift: int = 2
    outsource_cost: int = 1800

class NegotiationScheduler:
    def __init__(self, flights: List[Flight], tails: List[Tail], policy: LeverPolicy):
        self.cp_model = _cp_model()
        self.flights = flights
        self.tails = tails
        self.policy = policy
        self.horizon = 24 * 60
        self.m = self.cp_model.CpModel()
        self._build()

    def _build(self):
        cp = self.cp_model
        m = self.m
        F = range(len(self.flights))
        T = range(len(self.tails))

        self.assign: Dict[Tuple[int, int], any] = {}
        self.outsource: Dict[int, any] = {}
        self.shift_plus: Dict[int, any] = {}
        self.shift_minus: Dict[int, any] = {}
        self.start: Dict[int, any] = {}
        self.intervals_per_tail: Dict[int, List[any]] = {k: [] for k in T}

        for i in F:
            self.outsource[i] = m.NewBoolVar(f"outs[{i}]")
            self.shift_plus[i] = m.NewIntVar(0, self.policy.max_shift_plus_min, f"sp[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, self.policy.max_shift_minus_min, f"sm[{i}]")
            self.start[i] = m.NewIntVar(0, self.horizon, f"s[{i}]")

        for i in F:
            for k in T:
                self.assign[(i, k)] = m.NewBoolVar(f"a[{i},{k}]")
                if self.flights[i].fleet_class == self.tails[k].fleet_class:
                    d = self.flights[i].duration_min
                    iv = m.NewOptionalIntervalVar(
                        self.start[i], d, self.start[i] + d, self.assign[(i, k)], f"int[{i},{k}]"
                    )
                    self.intervals_per_tail[k].append(iv)
                else:
                    m.Add(self.assign[(i, k)] == 0)

        for i in F:
            m.Add(sum(self.assign[(i, k)] for k in T) + self.outsource[i] == 1)
            lo = self.flights[i].earliest_etd_min - self.shift_minus[i]
            hi = self.flights[i].latest_etd_min + self.shift_plus[i]
            m.Add(self.start[i] >= lo)
            m.Add(self.start[i] <= hi)

        for k in T:
            if self.intervals_per_tail[k]:
                m.AddNoOverlap(self.intervals_per_tail[k])
            t = self.tails[k]
            for i in F:
                m.Add(self.start[i] >= t.available_from_min).OnlyEnforceIf(self.assign[(i, k)])
                m.Add(self.start[i] + self.flights[i].duration_min <= t.available_to_min).OnlyEnforceIf(self.assign[(i, k)])

        terms = []
        for i in F:
            terms.append(self.outsource[i] * self.policy.outsource_cost)
            terms.append(self.shift_plus[i] * self.policy.cost_per_min_shift)
            terms.append(self.shift_minus[i] * self.policy.cost_per_min_shift)
        m.Minimize(sum(terms))

    def solve(self, time_limit_s: Optional[int] = 5, workers: int = 8):
        cp = self.cp_model
        solver = cp.CpSolver()
        if time_limit_s:
            solver.parameters.max_time_in_seconds = time_limit_s
        solver.parameters.num_search_workers = workers

        status = solver.Solve(self.m)
        assigned, outsourced = [], []
        for i, f in enumerate(self.flights):
            tail_id = None
            for k, t in enumerate(self.tails):
                if solver.Value(self.assign[(i, k)]) == 1:
                    tail_id = t.id
                    break
            if tail_id:
                assigned.append({
                    "flight": f.id, "tail": tail_id,
                    "origin": f.origin, "dest": f.dest,
                    "start_min": solver.Value(self.start[i]),
                    "duration": f.duration_min,
                    "shift_plus": solver.Value(self.shift_plus[i]),
                    "shift_minus": solver.Value(self.shift_minus[i]),
                })
            elif solver.Value(self.outsource[i]) == 1:
                outsourced.append({
                    "flight": f.id, "owner": f.owner_id,
                    "origin": f.origin, "dest": f.dest,
                    "preferred_etd": f.preferred_etd_min,
                })

        return status, {
            "assigned": pd.DataFrame(assigned),
            "outsourced": pd.DataFrame(outsourced),
            "objective": solver.ObjectiveValue()
                        if status in (cp.OPTIMAL, cp.FEASIBLE) else math.inf,
        }

