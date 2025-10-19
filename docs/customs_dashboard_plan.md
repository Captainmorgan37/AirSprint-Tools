# Customs Dashboard Improvement Plan

## Current Capabilities
The existing Streamlit customs dashboard now delivers the following functionality:

- Pulls flights from FL3XX within a configurable date window using stored credentials when available.
- Filters the dataset to customs-relevant legs via the shared `flight_leg_utils` helper.
- Fetches migration (arrival/departure) status payloads per flight and surfaces key fields (status, by, notes, document names).
- Displays per-leg information (tail, departure/arrival airports, departure timestamps in UTC/local, migration statuses, clearance note).
- Accepts an optional uploaded clearance requirements spreadsheet and maps the first airport/code column to a free-text requirement shown in the results table.
- Loads a bundled `customs_rules.csv` file (with optional override via upload) and exposes lead time, operating hours, after-hours availability, and contacts for each arrival port.
- Calculates a clearance goal window for each leg by translating the arrival event into the arrival airport timezone and rolling back to the prior operating window published in the rules file (falling back to default business hours when rules are missing).
- Derives urgency cues (`OK`, `Overdue`, `Within 2/5 Hours`, `Due Today`, `Pending`) and a human-readable "Time to Clear" countdown using the calculated clearance goal, rendering the table with color-coded styling.
- Presents summary metrics (customs legs count, pending arrivals) alongside the urgency-enhanced table with CSV export, warnings for API errors, and guidance on improving timezone coverage.

These pieces provide a reliable feed of customs legs, surface the raw arrival migration status, and add first-pass compliance cues derived from the rules file. The remaining work focuses on deepening the compliance engine, richer workflow views, and audit support.

## Progress Overview

- **Rules data ingestion**: Bundled CSV loader and upload override implemented; operating hours parsed for each weekday with after-hours and contact metadata surfaced in the UI.
- **Compliance calculations**: Baseline clearance goal window and urgency categorization complete. Evidence-driven filing detection and holiday awareness remain outstanding.
- **Workflow UX**: Table styling, urgency sorting, and high-level metrics shipped. Dedicated queue views (e.g., Now/Next 24h) and port-day boards still to do.
- **Audit & warnings**: API warning expander exists, but no persistent audit trail or advanced warning categories yet.

## Guiding Principles
- **Single source of truth for rules:** Maintain a simple, editable customs rules sheet (CSV/Google Sheet) that includes lead times, open hours, restrictions, and contacts. Cache the sheet and surface a “last loaded” timestamp in-app.
- **Deterministic compliance logic:** Derive status programmatically from event times, rule lead times, and evidence timestamps. Prefer earliest filing evidence and make the computation explainable to the operator.
- **Operations-first UX:** Present queues by urgency (time windows) and by port/day so teams can batch clearances efficiently. Keep critical warnings actionable.

## Roadmap Overview
The enhancement plan is sequenced into four workstreams. Each stream can be developed iteratively, but the ordering below optimizes dependencies.

### 1. Rules Data Model & Ingestion
1. ✅ Bundled `customs_rules.csv` published with lead time, operating hours, after-hours, contacts, and notes columns. Continue curating the sheet and document the refresh workflow.
2. ✅ Loader utility supports bundled CSV plus optional uploaded overrides, normalizes column names, and feeds rule details into the UI. Extend to pull from a hosted URL/Google Sheet via secrets and surface load diagnostics (last refreshed timestamp, missing critical fields, record counts).
3. 🔄 Per-day operating windows parsed and leveraged for clearance goal calculations. Next step is to incorporate jurisdiction/airport-specific holiday calendars and explicit closed days.
4. ⏭️ Add sidebar summary of rule source metadata (refresh status, file origin) and highlight airports missing lead times or timezone coverage.

### 2. Compliance Deadline Engine
1. ✅ Arrival-focused rule lookup wired in; dashboard uses arrival airport metadata when available.
2. ✅ Scheduled event times converted into airport-local timezone with lookup fallbacks.
3. ✅ Clearance window derived by rolling arrival event back to the prior operating window; default business hours used when rules are absent.
4. 🔄 Continue refining deadline adjustments: honor explicit lead-time hour offsets from the sheet, respect after-hours availability flags, and integrate holiday/closure logic.
5. ⏭️ Incorporate evidence tracking (migration timestamps, document uploads, manual overrides) to classify compliance states beyond the current urgency heuristic.
6. ⏭️ Persist and display intermediate calculations (deadline, adjustments applied, evidence source) within a leg detail view for operator transparency.

### 3. Workflow & Visualization Upgrades
1. 🔄 Existing metrics cover leg count and pending arrivals; extend with deadline-aware counters (Late, Due <2h, Due Today, Missing Rules/TZ) derived from the urgency/compliance engine.
2. ⏭️ Add urgency tabs or quick filters for Now / Next 24h / Next 72h windows using the `_urgency_category` and clearance timestamps.
3. ⏭️ Build a Port-Day board view grouping legs by arrival airport and local date with cards showing tail, ETA, pax/crew counts, and status badges.
4. ⏭️ Create a leg detail drawer/modal revealing evidence timestamps, rule text, notes, documents, and compliance reasoning (including after-hours or rule-default badges).
5. ⏭️ Provide quick communication helpers: copy-to-clipboard contact blocks from the rule sheet plus flight specifics; explore webhook triggers for overdue legs once compliance signals mature.

### 4. Audit Trail & Smart Warnings
1. ⏭️ Create an internal audit log model (in-memory or lightweight database/CSV) to record manual confirmations (who, when, notes, artifacts). Surface recent log entries per leg.
2. ⏭️ Track and display evidence sources (“arrivalMigration.updatedAt”, first document timestamp, manual override) and allow operators to override with justification.
3. 🔄 Warnings expander currently surfaces API/data issues. Expand with smart warnings for missing rules, missing timezone data, incompatible port flags (e.g., CANPASS-only), and NOTAM/holiday conflicts.
4. ⏭️ Categorize warnings and surface suggested next actions (e.g., “Arrange alternate airport” when port cannot service full customs) alongside communication helpers.

## Incremental Delivery Suggestions
- **Milestone 1:** Ship the rules loader with compliance deadline calculations, replacing the simple clearance note field with computed status columns while keeping the existing table layout. This delivers immediate value with minimal UI upheaval.
- **Milestone 2:** Layer in the urgency metrics/tabs and Port-Day board once the compliance statuses are trustworthy.
- **Milestone 3:** Add audit logging, manual overrides, and smart warnings, enabling richer collaboration and accountability.
- **Milestone 4:** Integrate communications shortcuts and optional webhook notifications to push urgent items into existing ops tools.

## Dependencies & Considerations
- Confirm ongoing access to FL3XX migration timestamps and document metadata (availability may depend on API permissions).
- Validate timezone coverage in `Airport TZ.txt`; expand as needed to avoid compliance miscalculations.
- Decide hosting strategy for the rules sheet (managed Google Sheet vs. versioned CSV in repo) and implement access controls/secrets accordingly.
- Evaluate persistence requirements for the audit log (Streamlit session state vs. shared storage) to avoid data loss between sessions.
- Coordinate with ops stakeholders to finalize SLA thresholds (e.g., definition of “Due Soon” hours) and webhook destinations.

This plan builds on the current customs dashboard foundation and sequences the suggested enhancements into achievable, high-impact increments.
