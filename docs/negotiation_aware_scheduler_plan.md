# Negotiation-Aware Solver Initiative

## Vision
Develop a solver add-on that augments the existing schedule optimizer so it can recover infeasible, high-demand days by proposing and coordinating targeted relaxations ("negotiation levers") instead of leaving flights unassigned.

## Problem Statement
- Current tools maximize feasibility under fixed rules but stop once they encounter conflicting legs.
- Operations needs guidance on what schedule adjustments to pursue (owner time shifts, tail swaps, tech stops, etc.) to make every required flight work.
- The new capability must reason about the trade-offs and surface the smallest set of high-value changes.

## Guiding Principles
1. **Safety and regulatory compliance remain hard constraints.**
2. **Owner experience and crew wellbeing drive lever costs.**
3. **Human-in-the-loop approvals for any negotiated change.**
4. **Rapid feedback loop** between solver, negotiation, and re-optimization.

## Core Workflow
1. **Baseline Solve** – Use the existing solver to generate the best schedule under current hard constraints.
2. **Conflict Diagnosis** – For each unassigned leg, identify the blocking constraints (tails, duty, owner windows, maintenance, etc.).
3. **Lever Catalog** – Maintain a structured list of allowable relaxations with eligibility rules and penalty costs, e.g.:
   - Owner ETD/ETA shifts in 30/60/90 minute buckets.
   - Tail swaps within compatible fleet classes.
   - Additional or removed tech stops to adjust duty and performance.
   - Crew swaps, augmentations, or split duties that respect FRMS.
   - Outsourcing options with cost ceilings.
4. **Soft Constraint Re-Solve** – Re-run the optimization, permitting lever activation with associated penalties to find the lowest-cost set of changes that satisfies all legs.
5. **Option Generation** – For each conflicted leg or group, surface 2–4 ranked options summarizing:
   - Lever(s) involved and expected business cost.
   - Affected owners, crews, and assets.
   - Why the option resolves the conflict.
6. **Negotiation Loop** – Provide tooling to send requests (e.g., owner time shift ask, incentive suggestions), capture responses, lock accepted levers, and trigger a new solve.
7. **Learning Layer** – Track lever outcomes to refine penalty weights, owner flexibility profiles, and option ranking.

## Data & Integration Needs
- Pull schedule, tail, crew, and maintenance state from FL3XX plus internal FRMS data.
- Store lever definitions, cost curves, and historical negotiation outcomes.
- Integrate with communications channels (email/SMS) to automate proposal drafts.

## Current Prototype Status

- **Solver kernel hardened.** `core.neg_scheduler.model.NegotiationScheduler` handles fleet-compatible assignments, ± time
  shifts, optional tail swaps, skip/outsource controls, duty-day caps, and reposition penalties via the `LeverPolicy`
  contract while surfacing up to five ranked solutions per run.
- **Domain contracts in production.** The frozen dataclasses in `core.neg_scheduler.contracts` enforce validation on every
  `Flight`/`Tail`, keeping the CP-SAT core resilient to upstream data issues.
- **Streamlit operations console.** `apps/negotiation_optimizer` now fetches FL3XX windows with caching, exposes lever
  sliders, builds reposition matrices from the airport index, renders diagnostics, and presents option tabs with
  Gantt/summary breakdowns for each solver outcome.
- **FL3XX ingestion pipeline.** `integrations.fl3xx_adapter.fetch_negotiation_data` normalizes payloads, classifies
  scheduled vs. add-line demand, infers fleet classes/tails, and returns solver-ready contracts plus metadata for auditing.

## Deliverables by Sprint

### Sprint 1 – Prototype Diagnostics *(✅ complete)*
- Export next-day schedule data and perform baseline hard solve. *(Demo dataset driving CP-SAT solver suffices for feasibility.)*
- Highlight unassigned legs and their blocking constraints inside the Streamlit solver dev app. *(Outsourced table with lever
  suggestions surfaces unresolved demand.)*

### Sprint 2 – Leverized Re-Solver *(✅ complete)*
- Implement a minimal lever catalog (owner ±30/60/90, tail swap, outsource). *(`LeverPolicy` caps/costs drive solver
  penalties, tail swap toggles apply to scheduled legs, and POS drop penalties are configurable in the UI.)*
- Add penalty-aware re-optimization and display ranked resolution options per conflict. *(Objective blends shift, swap,
  outsource, skip, and reposition costs; the Streamlit app surfaces up to five ranked solver alternatives with narrative
  summaries.)*

### Sprint 3 – Operator Workflow *(🚧 in progress)*
- Let ops review multiple solver options side-by-side. *(Top-N option tabs with summaries are live; acceptance/locking flow
  still outstanding.)*
- Extend UI to accept/lock lever choices, trigger re-solves, and capture negotiation outcomes.

### Sprint 4 – Learning & Expansion *(🔜 upcoming)*
- Capture negotiation outcomes to adjust lever costs and owner flexibility heuristics.
- Introduce additional levers (crew tweaks, ground transfers, tech stops) and richer analytics.

## Success Metrics
- Percentage of previously unassigned legs that become covered through negotiated adjustments.
- Average time from conflict identification to resolution.
- Owner satisfaction/approval rates for requested shifts.
- Operational cost impact versus baseline (DOC, crew duty, outsourcing).

## Open Questions
- How to quantify and cap goodwill costs per owner to avoid overuse of time shifts.
- What incentives are acceptable per policy and how to parameterize them.
- Required audit trail for compliance when altering planned duties or owner windows.

## Next Steps
- **Surface blocking diagnostics.** Add per-leg explanations (tail overlap, duty breaches, missing reposition time) to each
  solver option so negotiators understand the rationale behind suggested levers.
- **Implement lever locking.** Persist accepted shifts/swaps/outsourcing decisions in the UI, re-run the solver with those
  constraints, and track decision history for auditability.
- **Connect communications tooling.** Generate owner/crew negotiation drafts from selected levers and push them through the
  existing email/SMS channels while recording responses.
- **Broaden lever catalog.** Model crew augmentations, duty splits, tech stops, and ground transfer substitutions with
  calibrated penalties.
- **Learn from outcomes.** Store negotiation results to tune penalty weights, owner flexibility assumptions, and option
  ranking heuristics over time.
