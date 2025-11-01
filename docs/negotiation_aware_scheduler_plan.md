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

## Deliverables by Sprint

### Sprint 1 – Prototype Diagnostics
- Export next-day schedule data and perform baseline hard solve.
- Highlight unassigned legs and their blocking constraints inside the Streamlit solver dev app.

### Sprint 2 – Leverized Re-Solver
- Implement a minimal lever catalog (owner ±30/60/90, tail swap, outsource).
- Add penalty-aware re-optimization and display ranked resolution options per conflict.

### Sprint 3 – Operator Workflow
- Extend UI to let ops select an option, auto-draft negotiation messages, and re-solve based on responses.
- Track accepted/declined levers.

### Sprint 4 – Learning & Expansion
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
- Validate lever eligibility rules with ops stakeholders.
- Instrument existing solver outputs to provide detailed blocking constraint metadata.
- Prioritize communication channel integrations needed for automated “ask” drafts.
