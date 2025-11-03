"""Core CP-SAT scheduling model for negotiation-aware dispatching."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .contracts import Flight, Tail


def _norm_class(s: str) -> str:
    """Normalize fleet classes so variants share compatibility buckets."""

    text = (s or "").upper()
    compact = text.replace(" ", "").replace("-", "").replace("/", "").replace("+", "")

    # Handle common Citation variants (e.g., "CJ3+", "CJ2/CJ3", "CJ2+").
    if "CJ3" in compact:
        return "CJ3"
    if "CJ2" in compact:
        return "CJ2"
    if text.startswith("CJ") or compact.startswith("CJ"):
        return "CJ"

    # Embraer/Praetor family share the same operating characteristics for scheduling.
    if (
        text.startswith("LEG")
        or text.startswith("E")
        or "PRAETOR" in text
        or compact.startswith("LEG")
    ):
        return "LEG"

    if text == "GEN":
        return "GEN"

    return text


def _class_compatible(
    fclass: str, tclass: str, allow_any_tail: bool = False
) -> bool:
    """Return True when a tail can operate a flight based on fleet class."""

    if allow_any_tail:
        return True

    flight_class = _norm_class(fclass)
    tail_class = _norm_class(tclass)

    if tail_class == "GEN":
        return True
    if flight_class == tail_class:
        return True
    if flight_class in {"CJ", "CJ2", "CJ3"} and tail_class in {"CJ", "CJ2", "CJ3"}:
        return True
    return False


def _class_assignment_penalty(
    fclass: str, tclass: str, policy: "LeverPolicy", allow_any_tail: bool = False
) -> int:
    """Return the soft penalty for assigning a flight to a tail class."""

    if allow_any_tail:
        return 0

    flight_class = _norm_class(fclass)
    tail_class = _norm_class(tclass)

    # Treat all CJ variants as a shared compatibility bucket for penalty purposes.
    def _bucket(value: str) -> str:
        if value in {"CJ", "CJ2", "CJ3"}:
            return "CJ"
        return value

    flight_bucket = _bucket(flight_class)
    tail_bucket = _bucket(tail_class)

    if tail_bucket == "GEN" or flight_bucket == tail_bucket:
        return 0
    if flight_bucket == "LEG" and tail_bucket == "CJ":
        return policy.leg_to_cj_penalty
    if flight_bucket == "CJ" and tail_bucket == "LEG":
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
    outsource_cost: int = 50_000
    unassigned_penalty: int = 5_000
    turn_min: int = 30
    tail_swap_cost: int = 1_200
    reposition_cost_per_min: int = 1
    max_day_length_min: Optional[int] = None
    leg_to_cj_penalty: int = 6_000
    cj_to_leg_penalty: int = 500
    pos_skip_cost: int = 5_000
    pax_skip_cost: int = 1_000_000
    allow_pos_skips: bool = True
    change_penalty: int = 0
    flex_pax_enabled: bool = False
    flex_pax_plus_cap: int = 120
    flex_pax_minus_cap: int = 60
    flex_pax_base_cap: int = 30
    flex_pax_cost_base: int = 2
    flex_pax_cost_extra: int = 12
    flex_protected_owners: tuple[str, ...] = ()


class NegotiationScheduler:
    """Minimal CP-SAT assignment model with soft negotiation levers."""

    def __init__(
        self,
        flights: List[Flight],
        tails: List[Tail],
        policy: LeverPolicy,
        reposition_min: Optional[List[List[int]]] = None,
        initial_reposition_min: Optional[List[List[int]]] = None,
    ):
        self.cp_model = _cp_model()
        self.flights = flights
        self.tails = tails
        self.policy = policy
        self.reposition_min = self._normalize_reposition_matrix(
            reposition_min, len(flights)
        )
        self.initial_reposition_min = self._normalize_initial_reposition_matrix(
            initial_reposition_min, len(tails), len(flights)
        )
        self.horizon = self._compute_horizon()
        self.model = self.cp_model.CpModel()
        self.flex_protected_owner_ids = {
            str(owner)
            for owner in getattr(self.policy, "flex_protected_owners", ())
            if owner not in (None, "")
        }
        self.day_active: Dict[int, object] = {}
        self.day_start: Dict[int, object] = {}
        self.day_end: Dict[int, object] = {}
        self.first: Dict[Tuple[int, int], object] = {}
        self.first_changed: Dict[Tuple[int, int], object] = {}
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

    @staticmethod
    def _normalize_initial_reposition_matrix(
        initial: Optional[List[List[int]]],
        tail_count: int,
        flight_count: int,
    ) -> List[List[int]]:
        """Return a tail/flight matrix of initial reposition minutes."""

        if tail_count <= 0 or flight_count <= 0:
            return [[0] * max(flight_count, 0) for _ in range(max(tail_count, 0))]

        if initial is None:
            return [[0] * flight_count for _ in range(tail_count)]

        matrix = [list(row) for row in initial]
        if len(matrix) != tail_count or any(len(row) != flight_count for row in matrix):
            raise ValueError(
                "initial_reposition_min shape "
                f"{len(matrix)}x"
                f"{len(matrix[0]) if matrix else 0}"
                f" does not match tails {tail_count} and flights {flight_count}"
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
        self.skip: Dict[int, object] = {}
        self.shift_plus: Dict[int, object] = {}
        self.shift_minus: Dict[int, object] = {}
        self.shift_plus_base: Dict[int, object] = {}
        self.shift_minus_base: Dict[int, object] = {}
        self.shift_plus_extra: Dict[int, object] = {}
        self.shift_minus_extra: Dict[int, object] = {}
        self.flex_piecewise: Dict[int, bool] = {}
        self.start: Dict[int, object] = {}
        self.swap: Dict[int, object] = {}
        self.chg: Dict[int, object] = {}
        self.intervals_per_tail: Dict[int, List[object]] = {k: [] for k in T}
        self.order: Dict[Tuple[int, int, int], object] = {}

        enforce_day_length = (
            self.policy.max_day_length_min is not None
            and self.policy.max_day_length_min > 0
        )

        for i in F:
            flight = self.flights[i]
            base_tail_idx = (
                tail_index.get(flight.base_tail_id)
                if flight.base_tail_id is not None
                else None
            )
            self.outsource[i] = m.NewBoolVar(f"outsource[{i}]")
            self.skip[i] = m.NewBoolVar(f"skip[{i}]")
            shift_plus_cap = min(self.policy.max_shift_plus_min, flight.shift_plus_cap)
            shift_minus_cap = min(self.policy.max_shift_minus_min, flight.shift_minus_cap)
            self.shift_plus[i] = m.NewIntVar(0, shift_plus_cap, f"shift_plus[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, shift_minus_cap, f"shift_minus[{i}]")
            use_flex_piecewise = (
                self.policy.flex_pax_enabled
                and flight.intent == "PAX"
                and flight.current_tail_id is not None
                and str(flight.owner_id) not in self.flex_protected_owner_ids
            )
            if use_flex_piecewise and (shift_plus_cap > 0 or shift_minus_cap > 0):
                base_cap = max(0, int(self.policy.flex_pax_base_cap))
                base_plus_cap = min(base_cap, shift_plus_cap)
                base_minus_cap = min(base_cap, shift_minus_cap)
                extra_plus_cap = max(0, shift_plus_cap - base_plus_cap)
                extra_minus_cap = max(0, shift_minus_cap - base_minus_cap)
                plus_base = m.NewIntVar(0, base_plus_cap, f"shift_plus_base[{i}]")
                minus_base = m.NewIntVar(0, base_minus_cap, f"shift_minus_base[{i}]")
                plus_extra = m.NewIntVar(0, extra_plus_cap, f"shift_plus_extra[{i}]")
                minus_extra = m.NewIntVar(0, extra_minus_cap, f"shift_minus_extra[{i}]")
                self.shift_plus_base[i] = plus_base
                self.shift_minus_base[i] = minus_base
                self.shift_plus_extra[i] = plus_extra
                self.shift_minus_extra[i] = minus_extra
                self.flex_piecewise[i] = True
                m.Add(self.shift_plus[i] == plus_base + plus_extra)
                m.Add(self.shift_minus[i] == minus_base + minus_extra)
            else:
                self.flex_piecewise[i] = False
            self.start[i] = m.NewIntVar(0, self.horizon, f"start[{i}]")
            self.chg[i] = m.NewBoolVar(f"chg[{i}]")

            for k in T:
                tail = self.tails[k]
                self.assign[(i, k)] = m.NewBoolVar(f"assign[{i},{k}]")
                self.first[(i, k)] = m.NewBoolVar(f"first[{i},{k}]")
                if enforce_day_length:
                    self.last[(i, k)] = m.NewBoolVar(f"last[{i},{k}]")
                inactive_tail = (
                    active_tail_ids
                    and tail.id not in active_tail_ids
                    and flight.current_tail_id != tail.id
                )
                incompatible_tail = not _class_compatible(
                    flight.fleet_class, tail.fleet_class, flight.allow_any_tail
                )
                if inactive_tail or incompatible_tail:
                    if k != base_tail_idx:
                        m.Add(self.assign[(i, k)] == 0)
                        continue
                    m.Add(self.assign[(i, k)] == 0).OnlyEnforceIf(self.chg[i])
                interval_duration = flight.duration_min
                interval = m.NewOptionalIntervalVar(
                    self.start[i],
                    interval_duration,
                    self.start[i] + interval_duration,
                    self.assign[(i, k)],
                    f"flight[{i},{k}]",
                )
                self.intervals_per_tail[k].append(interval)

        for i in F:
            assigned_any = sum(self.assign[(i, k)] for k in T)
            m.Add(assigned_any + self.outsource[i] + self.skip[i] == 1)
            flight = self.flights[i]
            m.Add(self.start[i] >= flight.earliest_etd_min - self.shift_minus[i]).OnlyEnforceIf(
                self.chg[i]
            )
            m.Add(self.start[i] <= flight.latest_etd_min + self.shift_plus[i]).OnlyEnforceIf(
                self.chg[i]
            )
            m.Add(self.start[i] - flight.preferred_etd_min <= self.shift_plus[i]).OnlyEnforceIf(
                self.chg[i]
            )
            m.Add(flight.preferred_etd_min - self.start[i] <= self.shift_minus[i]).OnlyEnforceIf(
                self.chg[i]
            )

            if not flight.allow_outsource:
                m.Add(self.outsource[i] == 0)

            if flight.must_cover or not self.policy.allow_pos_skips:
                m.Add(self.skip[i] == 0)

            m.Add(self.shift_plus[i] == 0).OnlyEnforceIf(self.skip[i])
            m.Add(self.shift_minus[i] == 0).OnlyEnforceIf(self.skip[i])

            base_tail_idx = (
                tail_index.get(flight.base_tail_id)
                if flight.base_tail_id is not None
                else None
            )
            base_start = flight.base_start_min
            if base_tail_idx is None or base_start is None:
                m.Add(self.chg[i] == 1)
            else:
                m.Add(self.assign[(i, base_tail_idx)] == 1).OnlyEnforceIf(
                    self.chg[i].Not()
                )
                for k in T:
                    if k == base_tail_idx:
                        continue
                    m.Add(self.assign[(i, k)] == 0).OnlyEnforceIf(self.chg[i].Not())
                m.Add(self.start[i] == base_start).OnlyEnforceIf(self.chg[i].Not())

            m.Add(self.outsource[i] == 0).OnlyEnforceIf(self.chg[i].Not())
            m.Add(self.skip[i] == 0).OnlyEnforceIf(self.chg[i].Not())
            m.Add(self.shift_plus[i] == 0).OnlyEnforceIf(self.chg[i].Not())
            m.Add(self.shift_minus[i] == 0).OnlyEnforceIf(self.chg[i].Not())

            if flight.current_tail_id:
                k_cur = tail_index.get(flight.current_tail_id)
                if k_cur is None:
                    # Referenced tail is no longer available; treat the flight as free.
                    pass
                else:
                    current_tail = self.tails[k_cur]
                    if not _class_compatible(
                        flight.fleet_class,
                        current_tail.fleet_class,
                        flight.allow_any_tail,
                    ):
                        # Incompatible tail under the solver's rules; allow reassignment.
                        pass
                    else:
                        if not flight.allow_tail_swap:
                            for k in T:
                                m.Add(self.assign[(i, k)] == (1 if k == k_cur else 0)).OnlyEnforceIf(
                                    self.chg[i]
                                )
                        else:
                            swap = m.NewBoolVar(f"swap[{i}]")
                            self.swap[i] = swap

                            # swap = 0 → stay on current tail
                            m.Add(self.assign[(i, k_cur)] == 1).OnlyEnforceIf(
                                [swap.Not(), self.chg[i]]
                            )
                            for k in T:
                                if k == k_cur:
                                    continue
                                m.Add(self.assign[(i, k)] == 0).OnlyEnforceIf(
                                    [swap.Not(), self.chg[i]]
                                )

                            # swap = 1 → move off current tail
                            m.Add(self.assign[(i, k_cur)] == 0).OnlyEnforceIf(
                                [swap, self.chg[i]]
                            )
                            m.Add(swap == 0).OnlyEnforceIf(self.chg[i].Not())

        for k in T:
            tail = self.tails[k]
            tail_ready_min = max(tail.available_from_min, 0)
            if tail.last_position_ready_min is not None:
                tail_ready_min = max(tail_ready_min, tail.last_position_ready_min)
            if enforce_day_length:
                self.day_active[k] = m.NewBoolVar(f"day_active[{k}]")
                self.day_start[k] = m.NewIntVar(0, self.horizon, f"day_start[{k}]")
                self.day_end[k] = m.NewIntVar(0, self.horizon, f"day_end[{k}]")
            intervals = self.intervals_per_tail[k]
            if intervals:
                m.AddNoOverlap(intervals)
            for i in F:
                flight = self.flights[i]
                if not _class_compatible(
                    flight.fleet_class, tail.fleet_class, flight.allow_any_tail
                ):
                    continue
                m.Add(self.start[i] >= tail_ready_min).OnlyEnforceIf(self.assign[(i, k)])
                m.Add(
                    self.start[i] + flight.duration_min <= tail.available_to_min
                ).OnlyEnforceIf([self.assign[(i, k)], self.chg[i]])

        o = self.order
        for k in T:
            for i in F:
                for j in F:
                    if i == j:
                        continue
                    o[(i, j, k)] = m.NewBoolVar(f"ord[{i},{j},{k}]")

        self.chg_pair: Dict[Tuple[int, int], object] = {}
        for i in F:
            for j in F:
                if i == j:
                    continue
                pair = m.NewBoolVar(f"chg_pair[{i},{j}]")
                self.chg_pair[(i, j)] = pair
                m.AddImplication(self.chg[i], pair)
                m.AddImplication(self.chg[j], pair)
                m.Add(pair <= self.chg[i] + self.chg[j])

        for i in F:
            flight_i = self.flights[i]
            base_tail_i = (
                tail_index.get(flight_i.base_tail_id)
                if flight_i.base_tail_id is not None
                else None
            )
            base_start_i = flight_i.base_start_min
            if base_tail_i is None or base_start_i is None:
                continue
            for j in F:
                if i == j:
                    continue
                flight_j = self.flights[j]
                if (
                    flight_j.base_tail_id != flight_i.base_tail_id
                    or flight_j.base_start_min is None
                ):
                    continue
                if base_start_i < flight_j.base_start_min:
                    m.Add(self.order[(i, j, base_tail_i)] == 1).OnlyEnforceIf(
                        [self.chg[i].Not(), self.chg[j].Not()]
                    )

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
                    ).OnlyEnforceIf([o[(i, j, k)], self.chg_pair[(i, j)]])

        for k in T:
            tail = self.tails[k]
            tail_ready_min = max(tail.available_from_min, 0)
            if tail.last_position_ready_min is not None:
                tail_ready_min = max(tail_ready_min, tail.last_position_ready_min)
            first_vars = [self.first[(i, k)] for i in F]
            if first_vars:
                m.Add(sum(first_vars) <= 1)
            for i in F:
                assign_var = self.assign[(i, k)]
                first_var = self.first[(i, k)]
                m.Add(first_var <= assign_var)
                predecessors = [o[(j, i, k)] for j in F if j != i]
                if predecessors:
                    m.Add(assign_var <= first_var + sum(predecessors))
                    m.Add(first_var >= assign_var - sum(predecessors))
                else:
                    m.Add(first_var == assign_var)

                if self.initial_reposition_min and self.initial_reposition_min[k][i] > 0:
                    repo = self.initial_reposition_min[k][i]
                    m.Add(self.start[i] >= tail_ready_min + repo).OnlyEnforceIf(
                        [assign_var, first_var, self.chg[i]]
                    )
                    gate = m.NewBoolVar(f"first_changed[{i},{k}]")
                    self.first_changed[(i, k)] = gate
                    m.Add(gate <= first_var)
                    m.Add(gate <= self.chg[i])
                    m.Add(gate >= first_var + self.chg[i] - 1)

        objective_terms = []
        for i in F:
            flight = self.flights[i]
            skip_cost = (
                self.policy.pax_skip_cost if flight.must_cover else self.policy.pos_skip_cost
            )
            if skip_cost:
                objective_terms.append(self.skip[i] * skip_cost)
            objective_terms.append(self.outsource[i] * self.policy.outsource_cost)
            if self.flex_piecewise.get(i):
                plus_base = self.shift_plus_base.get(i)
                minus_base = self.shift_minus_base.get(i)
                plus_extra = self.shift_plus_extra.get(i)
                minus_extra = self.shift_minus_extra.get(i)
                cost_base = max(0, int(self.policy.flex_pax_cost_base))
                cost_extra = max(cost_base, int(self.policy.flex_pax_cost_extra))
                if plus_base is not None:
                    objective_terms.append(plus_base * cost_base)
                if minus_base is not None:
                    objective_terms.append(minus_base * cost_base)
                if plus_extra is not None:
                    objective_terms.append(plus_extra * cost_extra)
                if minus_extra is not None:
                    objective_terms.append(minus_extra * cost_extra)
            else:
                objective_terms.append(self.shift_plus[i] * flight.shift_cost_per_min)
                objective_terms.append(self.shift_minus[i] * flight.shift_cost_per_min)
            if self.policy.change_penalty:
                objective_terms.append(self.chg[i] * self.policy.change_penalty)
            if i in self.swap:
                objective_terms.append(self.swap[i] * self.policy.tail_swap_cost)
            for k in T:
                tail = self.tails[k]
                penalty = _class_assignment_penalty(
                    flight.fleet_class,
                    tail.fleet_class,
                    self.policy,
                    flight.allow_any_tail,
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

        if self.initial_reposition_min:
            for k in T:
                for i in F:
                    repo = self.initial_reposition_min[k][i]
                    if repo > 0:
                        gate = self.first_changed.get((i, k))
                        if gate is None:
                            continue
                        objective_terms.append(
                            gate * repo * self.policy.reposition_cost_per_min
                        )

        objective_expr = sum(objective_terms)
        m.Minimize(objective_expr)
        self.objective_expr = objective_expr

    def _build_solution(self, solver_like) -> Dict[str, pd.DataFrame]:
        cp = self.cp_model
        assigned_rows, outsourced_rows, skipped_rows = [], [], []
        tail_sequences: Dict[str, List[Tuple[int, int]]] = {}
        tail_index = {tail.id: idx for idx, tail in enumerate(self.tails)}
        for i, flight in enumerate(self.flights):
            tail_id = None
            for k, tail in enumerate(self.tails):
                if solver_like.Value(self.assign[(i, k)]) == 1:
                    tail_id = tail.id
                    break
            if tail_id:
                start_val = solver_like.Value(self.start[i])
                assigned_rows.append(
                    {
                        "flight": flight.id,
                        "tail": tail_id,
                        "original_tail": flight.current_tail_id,
                        "changed": solver_like.Value(self.chg[i]),
                        "tail_swapped": bool(
                            flight.current_tail_id and tail_id != flight.current_tail_id
                        ),
                        "origin": flight.origin,
                        "dest": flight.dest,
                        "start_min": start_val,
                        "end_min": start_val + flight.duration_min,
                        "duration_min": flight.duration_min,
                        "shift_plus": solver_like.Value(self.shift_plus[i]),
                        "shift_minus": solver_like.Value(self.shift_minus[i]),
                        "shift_plus_extra": (
                            solver_like.Value(self.shift_plus_extra[i])
                            if i in self.shift_plus_extra
                            else 0
                        ),
                        "shift_minus_extra": (
                            solver_like.Value(self.shift_minus_extra[i])
                            if i in self.shift_minus_extra
                            else 0
                        ),
                    }
                )
                tail_sequences.setdefault(tail_id, []).append((i, start_val))
            elif solver_like.Value(self.outsource[i]) == 1:
                outsourced_rows.append(
                    {
                        "flight": flight.id,
                        "owner": flight.owner_id,
                        "origin": flight.origin,
                        "dest": flight.dest,
                        "preferred_etd_min": flight.preferred_etd_min,
                    }
                )
            elif solver_like.Value(self.skip[i]) == 1:
                skipped_rows.append(
                    {
                        "flight": flight.id,
                        "owner": flight.owner_id,
                        "origin": flight.origin,
                        "dest": flight.dest,
                        "intent": flight.intent,
                    }
                )

        reposition_rows = []
        for tail_id, sequence in tail_sequences.items():
            ordered = sorted(sequence, key=lambda item: item[1])
            tail_idx = tail_index.get(tail_id)
            if tail_idx is not None and ordered:
                first_idx, _ = ordered[0]
                repo = 0
                if self.initial_reposition_min:
                    repo = self.initial_reposition_min[tail_idx][first_idx]
                tail = self.tails[tail_idx]
                if repo > 0 and tail.last_position_airport:
                    ready_min = max(tail.available_from_min, 0)
                    if tail.last_position_ready_min is not None:
                        ready_min = max(ready_min, tail.last_position_ready_min)
                    first_start = ordered[0][1]
                    latest_start = first_start - self.policy.turn_min - repo
                    repo_start = max(ready_min, latest_start)
                    reposition_rows.append(
                        {
                            "tail": tail_id,
                            "start_min": repo_start,
                            "duration_min": repo,
                            "end_min": repo_start + repo,
                            "origin": tail.last_position_airport,
                            "dest": self.flights[first_idx].origin,
                            "source_flight": None,
                            "target_flight": self.flights[first_idx].id,
                        }
                    )
            for (prev_idx, prev_start), (next_idx, next_start) in zip(ordered, ordered[1:]):
                repo = self.reposition_min[prev_idx][next_idx]
                if repo and repo > 0:
                    prev_flight = self.flights[prev_idx]
                    next_flight = self.flights[next_idx]
                    prev_ready = (
                        prev_start
                        + prev_flight.duration_min
                        + self.policy.turn_min
                    )
                    latest_start = (
                        next_start - self.policy.turn_min - repo
                    )
                    repo_start = max(prev_ready, latest_start)
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

        objective_value = (
            solver_like.ObjectiveValue()
            if hasattr(solver_like, "ObjectiveValue")
            else math.inf
        )

        return {
            "assigned": pd.DataFrame(assigned_rows),
            "outsourced": pd.DataFrame(outsourced_rows),
            "skipped": pd.DataFrame(skipped_rows),
            "reposition": pd.DataFrame(reposition_rows),
            "objective": objective_value,
        }

    def solve(
        self,
        time_limit_s: Optional[int] = 5,
        workers: int = 8,
        top_n: int = 1,
    ):
        cp = self.cp_model
        top_n = max(1, int(top_n or 1))
        solutions = []
        status = cp.UNKNOWN

        objective_floor: Optional[int] = None
        exclusion_clauses: List[List[Tuple[str, Tuple[int, ...], int]]] = []

        for attempt in range(top_n):
            if attempt == 0:
                working = self
            else:
                working = NegotiationScheduler(
                    self.flights,
                    self.tails,
                    self.policy,
                    reposition_min=self.reposition_min,
                )

            if objective_floor is not None:
                working.model.Add(working.objective_expr >= objective_floor)

            for clause in exclusion_clauses:
                literals = []
                for kind, key, value in clause:
                    if kind == "assign":
                        var = working.assign[tuple(key)]
                    elif kind == "outsource":
                        var = working.outsource[key[0]]
                    elif kind == "skip":
                        var = working.skip[key[0]]
                    else:
                        continue

                    literals.append(var.Not() if value else var)

                if literals:
                    working.model.AddBoolOr(literals)

            solver = cp.CpSolver()
            if time_limit_s:
                solver.parameters.max_time_in_seconds = time_limit_s
            solver.parameters.num_search_workers = workers

            current_status = solver.Solve(working.model)
            if current_status not in (cp.OPTIMAL, cp.FEASIBLE):
                if not solutions:
                    status = current_status
                break

            status = current_status
            solution = working._build_solution(solver)
            solutions.append(solution)

            objective_value = solution.get("objective", math.inf)
            if not math.isfinite(objective_value):
                break

            objective_floor = math.floor(objective_value)

            clause: List[Tuple[str, Tuple[int, ...], int]] = []
            for key, var in working.assign.items():
                clause.append(("assign", tuple(key), int(solver.Value(var))))
            for key, var in working.outsource.items():
                clause.append(("outsource", (key,), int(solver.Value(var))))
            for key, var in working.skip.items():
                clause.append(("skip", (key,), int(solver.Value(var))))

            if clause:
                exclusion_clauses.append(clause)

        return status, solutions
