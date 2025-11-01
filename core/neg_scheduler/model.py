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
    if flight_class == tail_class:
        return True
    if flight_class == "LEG" and tail_class == "CJ":
        return True
    if flight_class == "CJ" and tail_class == "LEG":
        return True
    return False


def _class_assignment_penalty(
    fclass: str, tclass: str, policy: "LeverPolicy"
) -> int:
    """Return the soft penalty for assigning a flight to a tail class."""

    flight_class = _norm_class(fclass)
    tail_class = _norm_class(tclass)

    if tail_class == "GEN" or flight_class == tail_class:
        return 0
    if flight_class == "LEG" and tail_class == "CJ":
        return policy.leg_to_cj_penalty
    if flight_class == "CJ" and tail_class == "LEG":
        return policy.cj_to_leg_penalty
    return 0


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
    max_day_length_min: Optional[int] = None
    leg_to_cj_penalty: int = 6_000
    cj_to_leg_penalty: int = 500


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
        self.reposition_min = self._normalize_reposition_matrix(
            reposition_min, len(flights)
        )
        self.horizon = self._compute_horizon()
        self.model = self.cp_model.CpModel()
        self.day_active: Dict[int, object] = {}
        self.day_start: Dict[int, object] = {}
        self.day_end: Dict[int, object] = {}
        self.first: Dict[Tuple[int, int], object] = {}
        self.last: Dict[Tuple[int, int], object] = {}
        self._build()

    @staticmethod
    def _normalize_reposition_matrix(
        reposition_min: Optional[List[List[int]]],
        flight_count: int,
    ) -> List[List[int]]:
        """Return a square reposition matrix sized to the provided flights."""

        if flight_count <= 0:
            return []

        if not reposition_min:
            return [[0] * flight_count for _ in range(flight_count)]

        # Convert potential tuples/other iterables to concrete lists so they can be
        # safely sliced when padding/trimming below.
        matrix = [list(row) for row in reposition_min]

        if len(matrix) != flight_count or any(len(row) != flight_count for row in matrix):
            mismatch = f"{len(matrix)}x"
            if matrix:
                mismatch += str(len(matrix[0]))
            else:
                mismatch += "0"
            raise ValueError(
                "reposition_min shape "
                f"{mismatch} does not match flights {flight_count}"
            )

        return matrix

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

        enforce_day_length = (
            self.policy.max_day_length_min is not None
            and self.policy.max_day_length_min > 0
        )

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
                if enforce_day_length:
                    self.first[(i, k)] = m.NewBoolVar(f"first[{i},{k}]")
                    self.last[(i, k)] = m.NewBoolVar(f"last[{i},{k}]")
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
            if enforce_day_length:
                self.day_active[k] = m.NewBoolVar(f"day_active[{k}]")
                self.day_start[k] = m.NewIntVar(0, self.horizon, f"day_start[{k}]")
                self.day_end[k] = m.NewIntVar(0, self.horizon, f"day_end[{k}]")
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

        if enforce_day_length:
            limit = int(self.policy.max_day_length_min)
            for k in T:
                active = self.day_active[k]
                assigned_vars = [self.assign[(i, k)] for i in F]
                first_vars = [self.first[(i, k)] for i in F]
                last_vars = [self.last[(i, k)] for i in F]

                m.Add(sum(assigned_vars) >= active)
                for assign_var in assigned_vars:
                    m.Add(assign_var <= active)

                m.Add(sum(first_vars) == active)
                m.Add(sum(last_vars) == active)

                for i in F:
                    first = self.first[(i, k)]
                    last = self.last[(i, k)]
                    assign_var = self.assign[(i, k)]
                    m.Add(first <= assign_var)
                    m.Add(last <= assign_var)

                    m.Add(self.day_start[k] == self.start[i]).OnlyEnforceIf(first)
                    m.Add(
                        self.day_end[k]
                        == self.start[i] + self.flights[i].duration_min
                    ).OnlyEnforceIf(last)

                    for j in F:
                        if i == j:
                            continue
                        m.Add(self.order[(j, i, k)] <= 1 - first)
                        m.Add(self.order[(i, j, k)] <= 1 - last)

                m.Add(self.day_start[k] == 0).OnlyEnforceIf(active.Not())
                m.Add(self.day_end[k] == 0).OnlyEnforceIf(active.Not())
                m.Add(self.day_end[k] - self.day_start[k] <= limit).OnlyEnforceIf(active)

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
            for k in T:
                tail = self.tails[k]
                penalty = _class_assignment_penalty(
                    flight.fleet_class, tail.fleet_class, self.policy
                )
                if penalty:
                    objective_terms.append(self.assign[(i, k)] * penalty)

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
        tail_sequences: Dict[str, List[Tuple[int, int]]] = {}
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
                tail_sequences.setdefault(tail_id, []).append((i, start_val))
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

        reposition_rows = []
        for tail_id, sequence in tail_sequences.items():
            ordered = sorted(sequence, key=lambda item: item[1])
            for (prev_idx, prev_start), (next_idx, next_start) in zip(ordered, ordered[1:]):
                repo = self.reposition_min[prev_idx][next_idx]
                if repo and repo > 0:
                    prev_flight = self.flights[prev_idx]
                    next_flight = self.flights[next_idx]
                    repo_start = (
                        prev_start
                        + prev_flight.duration_min
                        + self.policy.turn_min
                    )
                    reposition_rows.append(
                        {
                            "tail": tail_id,
                            "start_min": repo_start,
                            "duration_min": repo,
                            "end_min": repo_start + repo,
                            "origin": prev_flight.dest,
                            "dest": next_flight.origin,
                            "source_flight": prev_flight.id,
                            "target_flight": next_flight.id,
                        }
                    )

        return status, {
            "assigned": pd.DataFrame(assigned_rows),
            "outsourced": pd.DataFrame(outsourced_rows),
            "reposition": pd.DataFrame(reposition_rows),
            "objective": solver.ObjectiveValue()
            if status in (cp.OPTIMAL, cp.FEASIBLE)
            else math.inf,
        }
