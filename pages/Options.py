
"""
Negotiation-Aware Scheduler — Minimal Prototype
------------------------------------------------
A small, self-contained CP-SAT model (OR-Tools) plus a Streamlit UI slice
that demonstrates:
  1) Baseline hard solve under current rules
  2) Soft relaxations (owner time shift, outsource)
  3) Ranked lever suggestions for any flight that remained unscheduled

⚙️ Requirements (add to requirements.txt):
  - ortools>=9.10
  - streamlit>=1.36
  - pandas>=2.2

Run locally:
  streamlit run negotiation_scheduler.py

Note: This is a minimal pedagogical build. It uses toy data and simplified legality.
Integrate with FL3XX later by swapping `load_data()` with API pulls and mapping legs/crews.
"""
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

import math
import pandas as pd

try:
    from ortools.sat.python import cp_model
except Exception as e:  # graceful import failure for environments without OR-Tools
    cp_model = None

# -----------------------------
# Data model
# -----------------------------
@dataclass
class Flight:
    id: str
    origin: str
    dest: str
    duration_min: int
    earliest_etd_min: int  # minutes from day start (e.g., 0 = 00:00 local)
    latest_etd_min: int
    preferred_etd_min: int
    fleet_class: str       # e.g., "CJ", "LEG"
    owner_id: str

@dataclass
class Tail:
    id: str
    fleet_class: str
    available_from_min: int = 0
    available_to_min: int = 24*60

@dataclass
class LeverPolicy:
    # Costs and bounds for soft relaxations
    max_shift_plus_min: int = 90
    max_shift_minus_min: int = 30
    cost_per_min_shift: int = 2  # unitless score or $ proxy
    outsource_cost: int = 1800    # proxy DOC or broker cost
    unassigned_penalty: int = 5000  # very high — we try to avoid leaving unassigned


# -----------------------------
# Toy data for demo
# -----------------------------

def load_data() -> Tuple[List[Flight], List[Tail], LeverPolicy]:
    flights = [
        Flight(id="F1", origin="CYBW", dest="CYVR", duration_min=120,
               earliest_etd_min=7*60, latest_etd_min=8*60, preferred_etd_min=7*60+15,
               fleet_class="CJ", owner_id="O100"),
        Flight(id="F2", origin="CYVR", dest="KSEA", duration_min=45,
               earliest_etd_min=9*60, latest_etd_min=10*60, preferred_etd_min=9*60+15,
               fleet_class="CJ", owner_id="O220"),
        Flight(id="F3", origin="CYVR", dest="CYUL", duration_min=300,
               earliest_etd_min=9*60+30, latest_etd_min=12*60, preferred_etd_min=10*60,
               fleet_class="LEG", owner_id="O330"),
        Flight(id="F4", origin="KSEA", dest="CYBW", duration_min=90,
               earliest_etd_min=10*60, latest_etd_min=12*60, preferred_etd_min=10*60+30,
               fleet_class="CJ", owner_id="O220"),
        # This one intentionally conflicts to trigger levers
        Flight(id="F5", origin="CYBW", dest="KDEN", duration_min=160,
               earliest_etd_min=8*60, latest_etd_min=9*60, preferred_etd_min=8*60+15,
               fleet_class="CJ", owner_id="O555"),
    ]

    tails = [
        Tail(id="C-GCJ1", fleet_class="CJ"),
        Tail(id="C-GCJ2", fleet_class="CJ"),
        Tail(id="C-GLEG1", fleet_class="LEG"),
    ]

    policy = LeverPolicy(
        max_shift_plus_min=90,
        max_shift_minus_min=30,
        cost_per_min_shift=2,
        outsource_cost=1800,
        unassigned_penalty=5000,
    )
    return flights, tails, policy


# -----------------------------
# CP-SAT scheduler
# -----------------------------
class NegotiationScheduler:
    def __init__(self, flights: List[Flight], tails: List[Tail], policy: LeverPolicy):
        if cp_model is None:
            raise RuntimeError("OR-Tools not available. Please install ortools>=9.10.")
        self.flights = flights
        self.tails = tails
        self.policy = policy
        self.model = cp_model.CpModel()
        self.horizon = 24*60  # day horizon

        # Decision vars
        self.assign: Dict[Tuple[int, int], cp_model.IntVar] = {}
        self.outsource: Dict[int, cp_model.IntVar] = {}
        self.shift_plus: Dict[int, cp_model.IntVar] = {}
        self.shift_minus: Dict[int, cp_model.IntVar] = {}
        self.start_time: Dict[int, cp_model.IntVar] = {}
        self.intervals_per_tail: Dict[int, List[cp_model.IntervalVar]] = {}

        self._build_model()

    def _build_model(self):
        m = self.model
        F = range(len(self.flights))
        T = range(len(self.tails))

        # Variables
        for i in F:
            self.outsource[i] = m.NewBoolVar(f"outsource[{i}]")
            self.shift_plus[i] = m.NewIntVar(0, self.policy.max_shift_plus_min, f"shift_plus[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, self.policy.max_shift_minus_min, f"shift_minus[{i}]")
            # Start within horizon
            self.start_time[i] = m.NewIntVar(0, self.horizon, f"start[{i}]")

        for k in T:
            self.intervals_per_tail[k] = []

        # Tail assignment booleans and optional intervals
        for i in F:
            for k in T:
                self.assign[(i, k)] = m.NewBoolVar(f"assign[{i},{k}]")
                # Only build interval if fleet class matches
                if self.flights[i].fleet_class == self.tails[k].fleet_class:
                    duration = self.flights[i].duration_min
                    interval = m.NewOptionalIntervalVar(
                        self.start_time[i], duration, self.start_time[i] + duration,
                        self.assign[(i, k)], f"int[{i},{k}]"
                    )
                    self.intervals_per_tail[k].append(interval)
                else:
                    # Disallow assignment if class mismatch
                    m.Add(self.assign[(i, k)] == 0)

        # Each flight: exactly one of (assigned to one tail) OR outsourced
        for i in F:
            m.Add(sum(self.assign[(i, k)] for k in T) + self.outsource[i] == 1)

        # Respect ETD window with shifts
        for i in F:
            f = self.flights[i]
            etd_lo = f.earliest_etd_min - self.shift_minus[i]
            etd_hi = f.latest_etd_min + self.shift_plus[i]
            m.Add(self.start_time[i] >= etd_lo)
            m.Add(self.start_time[i] <= etd_hi)

        # Tail availability windows and non-overlap
        for k in T:
            # Non-overlap on each tail
            m.AddNoOverlap(self.intervals_per_tail[k])
            # Availability window: if any flight assigned to tail k, its start must be within tail window
            t = self.tails[k]
            for i in F:
                # start >= available_from if assigned
                m.Add(self.start_time[i] >= t.available_from_min).OnlyEnforceIf(self.assign[(i, k)])
                # end <= available_to if assigned
                end_i = self.start_time[i] + self.flights[i].duration_min
                m.Add(end_i <= t.available_to_min).OnlyEnforceIf(self.assign[(i, k)])

        # Objective: minimize unassigned/outsourced + shifts
        # (Outsource is our proxy for "we couldn't schedule under current rules")
        terms = []
        for i in F:
            # Penalize outsourcing heavily
            terms.append(self.outsource[i] * self.policy.outsource_cost)
            # Penalize time shifts (owner ask)
            terms.append(self.shift_plus[i] * self.policy.cost_per_min_shift)
            terms.append(self.shift_minus[i] * self.policy.cost_per_min_shift)

        m.Minimize(sum(terms))

    def solve(self, time_limit_s: Optional[int] = 5) -> Tuple[int, Dict]:
        solver = cp_model.CpSolver()
        if time_limit_s:
            solver.parameters.max_time_in_seconds = time_limit_s
        solver.parameters.num_search_workers = 8

        status = solver.Solve(self.model)
        sol = {"status": status}
        assign_rows = []
        outsource_rows = []
        for i, f in enumerate(self.flights):
            assigned_tail = None
            for k, t in enumerate(self.tails):
                if solver.Value(self.assign[(i, k)]) == 1:
                    assigned_tail = t.id
                    break
            if assigned_tail:
                assign_rows.append({
                    "flight": f.id,
                    "tail": assigned_tail,
                    "origin": f.origin,
                    "dest": f.dest,
                    "start_min": solver.Value(self.start_time[i]),
                    "duration": f.duration_min,
                    "shift_plus": solver.Value(self.shift_plus[i]),
                    "shift_minus": solver.Value(self.shift_minus[i]),
                })
            elif solver.Value(self.outsource[i]) == 1:
                outsource_rows.append({
                    "flight": f.id,
                    "owner": f.owner_id,
                    "origin": f.origin,
                    "dest": f.dest,
                    "preferred_etd": f.preferred_etd_min,
                })
        sol["assigned"] = pd.DataFrame(assign_rows)
        sol["outsourced"] = pd.DataFrame(outsource_rows)
        sol["objective_value"] = solver.ObjectiveValue() if status in (cp_model.OPTIMAL, cp_model.FEASIBLE) else math.inf
        return status, sol


# -----------------------------
# Lever suggestion (ranked options per unscheduled/outsourced flight)
# -----------------------------

def suggest_levers(f: Flight, policy: LeverPolicy, assigned_df: pd.DataFrame) -> List[Dict]:
    """Heuristic option generator showing how we'd phrase asks.
    Later, these become alternate models with tighter/looser bounds and re-solves.
    """
    opts = []
    # Option A: Owner time shift +30
    opts.append({
        "option": "Owner shift +30m",
        "ask": f"Request Owner {f.owner_id} to slide ETD +30m",
        "penalty": policy.cost_per_min_shift * 30,
        "notes": "Often unlocks tight turnarounds with minimal impact.",
    })
    # Option B: Owner shift +60
    opts.append({
        "option": "Owner shift +60m",
        "ask": f"Request Owner {f.owner_id} to slide ETD +60m",
        "penalty": policy.cost_per_min_shift * 60,
        "notes": "Larger buffer to align tails without swaps.",
    })
    # Option C: Swap tail (same class) — narrative only here
    opts.append({
        "option": "Tail swap within class",
        "ask": "Swap assignment among CJ tails to free contiguous block",
        "penalty": 120,  # placeholder narrative cost
        "notes": "No owner impact; may add one empty reposition leg.",
    })
    # Option D: Outsource (already chosen by solver), included for comparison
    opts.append({
        "option": "Outsource leg",
        "ask": "Broker this leg",
        "penalty": policy.outsource_cost,
        "notes": "Zero internal perturbation; highest direct cost.",
    })

    # Rank by penalty ascending
    return sorted(opts, key=lambda d: d["penalty"])[:4]


# -----------------------------
# Streamlit UI slice
# -----------------------------

def run_ui():
    import streamlit as st

    st.set_page_config(page_title="Negotiation-Aware Scheduler (Prototype)", layout="wide")
    st.title("🧩 Negotiation-Aware Scheduler — Minimal Prototype")

    st.sidebar.header("Inputs")
    demo = st.sidebar.toggle("Use demo data", value=True)

    if demo:
        flights, tails, policy = load_data()
    else:
        st.info("Upload CSVs for flights and tails using the demo schema.")
        f_file = st.file_uploader("Flights CSV", type=["csv"])
        t_file = st.file_uploader("Tails CSV", type=["csv"])
        if not (f_file and t_file):
            st.stop()
        f_df = pd.read_csv(f_file)
        t_df = pd.read_csv(t_file)
        flights = [
            Flight(
                id=row.id,
                origin=row.origin,
                dest=row.dest,
                duration_min=int(row.duration_min),
                earliest_etd_min=int(row.earliest_etd_min),
                latest_etd_min=int(row.latest_etd_min),
                preferred_etd_min=int(row.preferred_etd_min),
                fleet_class=row.fleet_class,
                owner_id=str(row.owner_id),
            ) for _, row in f_df.iterrows()
        ]
        tails = [
            Tail(
                id=row.id,
                fleet_class=row.fleet_class,
                available_from_min=int(row.available_from_min),
                available_to_min=int(row.available_to_min),
            ) for _, row in t_df.iterrows()
        ]
        policy = LeverPolicy()

    # Policy controls
    st.sidebar.subheader("Lever Policy")
    policy.max_shift_plus_min = st.sidebar.slider("Max shift + (min)", 0, 180, policy.max_shift_plus_min, 5)
    policy.max_shift_minus_min = st.sidebar.slider("Max shift - (min)", 0, 180, policy.max_shift_minus_min, 5)
    policy.cost_per_min_shift = st.sidebar.slider("Cost per shifted minute", 0, 10, policy.cost_per_min_shift, 1)
    policy.outsource_cost = st.sidebar.number_input("Outsource cost proxy", 0, 10000, policy.outsource_cost, 50)

    run = st.button("Run Solver", type="primary")

    if run:
        try:
            sched = NegotiationScheduler(flights, tails, policy)
            status, sol = sched.solve()
        except Exception as e:
            st.error(f"Solver error: {e}")
            st.stop()

        st.subheader("Assigned Schedule")
        if len(sol["assigned"]) > 0:
            st.dataframe(sol["assigned"], use_container_width=True)
        else:
            st.write("No internal assignments.")

        st.subheader("Unscheduled / Outsourced")
        if len(sol["outsourced"]) > 0:
            st.dataframe(sol["outsourced"], use_container_width=True)
        else:
            st.write("None — all flights scheduled internally.")

        st.caption(f"Objective value: {sol['objective_value']:.0f}")

        # Lever suggestions per outsourced flight
        if len(sol["outsourced"]) > 0:
            st.markdown("### 🔧 Lever Suggestions")
            for _, row in sol["outsourced"].iterrows():
                f = next(ff for ff in flights if ff.id == row["flight"]) 
                opts = suggest_levers(f, policy, sol["assigned"]) 
                with st.expander(f"Flight {f.id} {f.origin}→{f.dest} (Owner {f.owner_id}) — Options"):
                    df = pd.DataFrame(opts)
                    st.dataframe(df, use_container_width=True)
                    # Draft ask template for top option
                    top = opts[0]
                    st.markdown("**Draft Owner Ask:**")
                    msg = (
                        f"Hi Owner {f.owner_id},\n\n"
                        f"We can keep your aircraft and crew as requested if you're able to slide your departure by "
                        f"{top['option'].split('+')[-1]}. This avoids a tail swap and keeps everyone within duty limits. "
                        f"As a thank-you, we're happy to offer a small courtesy credit on this leg.\n\n"
                        f"Please let us know if a {top['option'].split('+')[-1]} shift works.\n\n"
                        f"— Dispatch"
                    )
                    st.code(msg)


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    # When run as a script (streamlit will call this)
    try:
        run_ui()
    except Exception as e:
        print("To use the UI, run: streamlit run this_file.py")
        raise



# ---
# Repo scaffold for `Solver-Dev` branch (drop-in)
# -------------------------------------------------
# This augments the minimal prototype above by splitting core vs UI and stubbing FL3XX wiring.
# Create these files alongside your existing repo structure.

# 1) requirements-negotiation.txt (optional dev-only deps)
# -------------------------------------------------------
# contents:
# -r requirements.txt
# ortools==9.10.*
# pydantic>=2.7


# 2) core/neg_scheduler/contracts.py
# ----------------------------------
from pydantic import BaseModel
from typing import Optional, Set
from datetime import datetime

class Leg(BaseModel):
    id: str
    owner_id: str
    dep: str
    arr: str
    block_min: int
    fleet_class: str  # e.g., "CJ", "LEG"
    etd_lo: int       # minutes from day start
    etd_hi: int
    preferred_etd: int

class Tail(BaseModel):
    id: str
    fleet_class: str
    available_lo: int = 0
    available_hi: int = 24*60


# 3) core/neg_scheduler/model.py (CP-SAT core extracted)
# ------------------------------------------------------
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import math
import pandas as pd
from ortools.sat.python import cp_model
from core.neg_scheduler.contracts import Flight, Tail

@dataclass
class LeverPolicy:
    max_shift_plus_min: int = 90
    max_shift_minus_min: int = 30
    cost_per_min_shift: int = 2
    outsource_cost: int = 1800

class NegotiationScheduler:
    def __init__(self, legs: List[Leg], tails: List[Tail], policy: LeverPolicy):
        self.legs = legs
        self.tails = tails
        self.policy = policy
        self.horizon = 24*60
        self.m = cp_model.CpModel()
        self._build()

    def _build(self):
        m = self.m
        F = range(len(self.legs))
        T = range(len(self.tails))
        self.assign: Dict[Tuple[int,int], cp_model.IntVar] = {}
        self.outsource: Dict[int, cp_model.IntVar] = {}
        self.shift_plus: Dict[int, cp_model.IntVar] = {}
        self.shift_minus: Dict[int, cp_model.IntVar] = {}
        self.start: Dict[int, cp_model.IntVar] = {}
        self.intervals_per_tail: Dict[int, List[cp_model.IntervalVar]] = {k: [] for k in T}

        for i in F:
            self.outsource[i] = m.NewBoolVar(f"outs[{i}]")
            self.shift_plus[i] = m.NewIntVar(0, self.policy.max_shift_plus_min, f"sp[{i}]")
            self.shift_minus[i] = m.NewIntVar(0, self.policy.max_shift_minus_min, f"sm[{i}]")
            self.start[i] = m.NewIntVar(0, self.horizon, f"s[{i}]")

        for i in F:
            for k in T:
                self.assign[(i,k)] = m.NewBoolVar(f"a[{i},{k}]")
                if self.legs[i].fleet_class == self.tails[k].fleet_class:
                    d = self.legs[i].block_min
                    m.NewOptionalIntervalVar(self.start[i], d, self.start[i]+d, self.assign[(i,k)], f"int[{i},{k}]")
                else:
                    m.Add(self.assign[(i,k)] == 0)

        for i in F:
            m.Add(sum(self.assign[(i,k)] for k in T) + self.outsource[i] == 1)
            lo = self.legs[i].etd_lo - self.shift_minus[i]
            hi = self.legs[i].etd_hi + self.shift_plus[i]
            m.Add(self.start[i] >= lo)
            m.Add(self.start[i] <= hi)

        # NoOverlap per tail + availability
        for k in T:
            ints = []
            for i in F:
                if self.legs[i].fleet_class == self.tails[k].fleet_class:
                    ints.append(m.GetIntervalVarFromProtoName(f"int[{i},{k}]"))
                    m.Add(self.start[i] >= self.tails[k].available_lo).OnlyEnforceIf(self.assign[(i,k)])
                    m.Add(self.start[i] + self.legs[i].block_min <= self.tails[k].available_hi).OnlyEnforceIf(self.assign[(i,k)])
            m.AddNoOverlap([iv for iv in ints if iv is not None])

        terms = []
        for i in F:
            terms.append(self.outsource[i] * self.policy.outsource_cost)
            terms.append(self.shift_plus[i] * self.policy.cost_per_min_shift)
            terms.append(self.shift_minus[i] * self.policy.cost_per_min_shift)
        m.Minimize(sum(terms))

    def solve(self, time_limit_s: Optional[int]=5, workers:int=8):
        solver = cp_model.CpSolver()
        if time_limit_s:
            solver.parameters.max_time_in_seconds = time_limit_s
        solver.parameters.num_search_workers = workers
        status = solver.Solve(self.m)
        assigned, outsourced = [], []
        for i, leg in enumerate(self.legs):
            tail_id = None
            for k, tail in enumerate(self.tails):
                if solver.Value(self.assign[(i,k)]) == 1:
                    tail_id = tail.id
                    break
            if tail_id:
                assigned.append({
                    "leg": leg.id,
                    "tail": tail_id,
                    "dep": leg.dep, "arr": leg.arr,
                    "start_min": solver.Value(self.start[i]),
                    "dur": leg.block_min,
                    "shift+": solver.Value(self.shift_plus[i]),
                    "shift-": solver.Value(self.shift_minus[i]),
                })
            elif solver.Value(self.outsource[i]) == 1:
                outsourced.append({
                    "leg": leg.id,
                    "owner": leg.owner_id,
                    "dep": leg.dep, "arr": leg.arr,
                    "preferred_etd": leg.preferred_etd,
                })
        return status, {
            "assigned": pd.DataFrame(assigned),
            "outsourced": pd.DataFrame(outsourced),
            "objective": solver.ObjectiveValue() if status in (cp_model.OPTIMAL, cp_model.FEASIBLE) else math.inf,
        }


# 4) integrations/fl3xx_adapter.py (thin wrapper to your existing utils)
# ---------------------------------------------------------------------
from typing import List
from .neg_scheduler.contracts import Leg, Tail

# Import your real helpers (adjust paths to match your repo):
# from integrations.fl3xx import fetch_legs_for_date, fetch_tails, to_minutes_from_midnight

# Placeholder demo conversion until wired:

def get_demo_data() -> tuple[list[Leg], list[Tail]]:
    legs = [
        Leg(id="F1", owner_id="O100", dep="CYBW", arr="CYVR", block_min=120, fleet_class="CJ", etd_lo=7*60, etd_hi=8*60, preferred_etd=7*60+15),
        Leg(id="F2", owner_id="O220", dep="CYVR", arr="KSEA", block_min=45, fleet_class="CJ", etd_lo=9*60, etd_hi=10*60, preferred_etd=9*60+15),
        Leg(id="F3", owner_id="O330", dep="CYVR", arr="CYUL", block_min=300, fleet_class="LEG", etd_lo=9*60+30, etd_hi=12*60, preferred_etd=10*60),
        Leg(id="F4", owner_id="O220", dep="KSEA", arr="CYBW", block_min=90, fleet_class="CJ", etd_lo=10*60, etd_hi=12*60, preferred_etd=10*60+30),
        Leg(id="F5", owner_id="O555", dep="CYBW", arr="KDEN", block_min=160, fleet_class="CJ", etd_lo=8*60, etd_hi=9*60, preferred_etd=8*60+15),
    ]
    tails = [
        Tail(id="C-GCJ1", fleet_class="CJ"),
        Tail(id="C-GCJ2", fleet_class="CJ"),
        Tail(id="C-GLEG1", fleet_class="LEG"),
    ]
    return legs, tails


# 5) apps/negotiation_optimizer/app.py (Streamlit page)
# ----------------------------------------------------
import streamlit as st
import pandas as pd
from core.neg_scheduler.model import NegotiationScheduler, LeverPolicy
from core.neg_scheduler.contracts import Leg, Tail
from integrations.fl3xx_adapter import get_demo_data

st.set_page_config(page_title="Negotiation Optimizer", layout="wide")

st.title("🧩 Negotiation-Aware Scheduler")

with st.sidebar:
    st.header("Inputs")
    use_demo = st.toggle("Use demo data", True)
    max_plus = st.slider("Max shift + (min)", 0, 180, 90, 5)
    max_minus = st.slider("Max shift - (min)", 0, 180, 30, 5)
    cpm = st.slider("Cost per shifted minute", 0, 10, 2)
    outsource = st.number_input("Outsource cost proxy", 0, 10000, 1800, 50)

policy = LeverPolicy(max_shift_plus_min=max_plus, max_shift_minus_min=max_minus,
                     cost_per_min_shift=cpm, outsource_cost=outsource)

if use_demo:
    legs, tails = get_demo_data()
else:
    st.warning("Wire `fl3xx_adapter` to your real FL3XX utils to pull legs and tails.")
    legs, tails = get_demo_data()

if st.button("Run Solver", type="primary"):
    sched = NegotiationScheduler(legs, tails, policy)
    status, sol = sched.solve()
    st.subheader("Assigned")
    st.dataframe(sol["assigned"], use_container_width=True) if len(sol["assigned"]) else st.write("—")
    st.subheader("Unscheduled / Outsourced")
    st.dataframe(sol["outsourced"], use_container_width=True) if len(sol["outsourced"]) else st.write("—")
    st.caption(f"Objective: {sol['objective']:.0f}")

    if len(sol["outsourced"]):
        st.markdown("### 🔧 Lever Suggestions (heuristic)")
        for _, r in sol["outsourced"].iterrows():
            st.write(f"**{r['leg']} {r['dep']}→{r['arr']} (Owner {r['owner']})**")
            opts = [
                {"option": "+30m owner shift", "penalty": policy.cost_per_min_shift*30, "notes":"Often unlocks tight turns."},
                {"option": "+60m owner shift", "penalty": policy.cost_per_min_shift*60, "notes":"More buffer, minimal churn."},
                {"option": "Tail swap within class", "penalty": 120, "notes":"No owner impact; possible extra repo."},
                {"option": "Outsource leg", "penalty": policy.outsource_cost, "notes":"Highest direct cost."},
            ]
            df = pd.DataFrame(sorted(opts, key=lambda d: d["penalty"]))
            st.dataframe(df, use_container_width=True)
            st.code(
                f"Hi Owner {r['owner']},\n\n"
                "Could you slide departure by 30 minutes? This avoids a tail swap and keeps crew within duty limits.\n"
                "We can offer a small courtesy credit.\n\n"
                "— Dispatch\n"
            )



# 6) Optional: Makefile (handy shortcuts)
# ---------------------------------------
# make dev       # run streamlit page locally
# make lint      # placeholder for ruff/black
#
# contents:
# dev:
# 	streamlit run apps/negotiation_optimizer/app.py
