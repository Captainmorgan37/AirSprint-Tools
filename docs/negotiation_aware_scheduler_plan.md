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

- **Solver kernel in place.** A minimal CP-SAT model (`core.neg_scheduler.model.NegotiationScheduler`) now supports
  fleet-compatible tail assignment, ± time shifts, and outsourcing penalties governed by a `LeverPolicy` dataclass.
- **Contracts formalized.** All solver inputs flow through Pydantic `Leg` and `Tail` contracts so the optimization core has
  consistent typing and validation hooks.
- **Streamlit pilot UI.** The `apps/negotiation_optimizer` page lets the team run the solver on demo data, adjust lever bounds
  interactively, review assignments/outsourcing, and inspect suggested negotiation levers with pre-drafted messaging blocks.
- **Data adapters stubbed.** `integrations.fl3xx_adapter` exposes deterministic demo data today and provides a shim for wiring
  real FL3XX pulls without breaking the UI contract.

## Deliverables by Sprint

### Sprint 1 – Prototype Diagnostics *(✅ complete)*
- Export next-day schedule data and perform baseline hard solve. *(Demo dataset driving CP-SAT solver suffices for feasibility.)*
- Highlight unassigned legs and their blocking constraints inside the Streamlit solver dev app. *(Outsourced table with lever
  suggestions surfaces unresolved demand.)*

### Sprint 2 – Leverized Re-Solver *(🚧 in progress)*
- Implement a minimal lever catalog (owner ±30/60/90, tail swap, outsource). *(Costed shifts and placeholder swap penalty
  available in UI; tail swap mechanics still implicit via solver assignments.)*
- Add penalty-aware re-optimization and display ranked resolution options per conflict. *(Objective includes shift/outsourcing
  costs; UI ranks lever suggestions for unscheduled legs.)*

### Sprint 3 – Operator Workflow *(🔜 upcoming)*
- Extend UI to let ops select an option, auto-draft negotiation messages, and re-solve based on responses.
- Track accepted/declined levers.

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
- **Wire real data.** Replace the FL3XX adapter stub with authenticated pulls (legs, tails, owner preferences) and confirm it
  hydrates the `Leg`/`Tail` contracts without manual cleanup.
- **Expose conflict diagnostics.** Enrich solver outputs with blocking explanations (e.g., specific tail overlap, duty window
  breaches) so negotiators understand *why* a lever is recommended.
- **Interactive lever application.** Allow the Streamlit UI to accept/lock proposed shifts or tail swaps, trigger a re-solve,
  and log accepted versus rejected asks for future tuning.
- **Expand lever catalog.** Model crew augmentation, tech stop insertion, and limited-duty splits with appropriate penalty
  scaffolding.
- **Close the messaging loop.** Integrate with the existing comms tooling (email/SMS templates) to auto-fill and send
  negotiation requests while capturing outcomes for the learning layer.
