# Customs Dashboard Improvement Plan

## Current Capabilities
The existing Streamlit customs dashboard provides the following functionality:

- Pulls flights from FL3XX within a configurable date window using stored credentials when available.
- Filters the dataset to customs-relevant legs via the shared `flight_leg_utils` helper.
- Fetches migration (arrival/departure) status payloads per flight and surfaces key fields (status, by, notes, document count, document names).
- Displays per-leg information (tail, departure/arrival airports, departure timestamps in UTC/local, migration statuses, clearance note).
- Accepts an optional uploaded clearance requirements spreadsheet and maps the first airport/code column to a free-text requirement shown in the results table.
- Presents summary metrics (customs legs count, pending departures) and tabbed views for status distribution and detailed table, with CSV export.
- Highlights missing timezone coverage and API warnings surfaced while fetching flight/migration data.

These pieces provide a reliable feed of customs legs and surface the raw arrival/departure migration status, but they stop short of determining compliance deadlines or providing workflow triage views.

## Guiding Principles
- **Single source of truth for rules:** Maintain a simple, editable customs rules sheet (CSV/Google Sheet) that includes lead times, open hours, restrictions, and contacts. Cache the sheet and surface a “last loaded” timestamp in-app.
- **Deterministic compliance logic:** Derive status programmatically from event times, rule lead times, and evidence timestamps. Prefer earliest filing evidence and make the computation explainable to the operator.
- **Operations-first UX:** Present queues by urgency (time windows) and by port/day so teams can batch clearances efficiently. Keep critical warnings actionable.

## Roadmap Overview
The enhancement plan is sequenced into four workstreams. Each stream can be developed iteratively, but the ordering below optimizes dependencies.

### 1. Rules Data Model & Ingestion
1. Define and publish the `customs_rules` sheet with the columns suggested in the concept brief (airport, lead times, open hours, flags, contacts, notes, updated_at).
2. Implement a loader utility to read the sheet/CSV (support both uploaded file and hosted URL/Google Sheet via secrets). Cache parsed results and expose diagnostics for missing/invalid records.
3. Parse open-hour strings into per-day intervals and ingest optional holiday calendars (either airport-specific sheet or jurisdiction-based lookup).
4. Extend the dashboard sidebar to show rule source summary (last refreshed, record count, warning badges for missing critical fields).

### 2. Compliance Deadline Engine
1. For each customs leg, determine governing airport/event (arrival vs departure) using the rule flags and flight direction.
2. Convert scheduled times to airport-local timezone (guarding for DST and missing tz via the existing airport lookup fallback).
3. Compute raw deadlines: `deadline_raw = event_local_time - lead_time_hours` using arrival/departure lead times with sensible defaults when data is missing.
4. Adjust deadlines for hours of operation:
   - If the deadline falls outside open hours and after-hours service is unavailable, roll back to the last open minute.
   - Respect holiday/closure exceptions and propagate warnings when no valid open window exists.
5. Determine earliest filing evidence using migration timestamps, document upload times, or manual overrides (future audit table) and calculate compliance state (`OK`, `Due Soon`, `Late`, `Missing`, `Filed Late`).
6. Persist intermediate values (deadline, evidence source, adjustments applied) for transparency within the UI.

### 3. Workflow & Visualization Upgrades
1. Replace/augment current metrics with deadline-aware counters (Late, Due <2h, Due Today, OK, Missing Rules/TZ) derived from the compliance engine.
2. Introduce urgency tabs or quick filters for Now / Next 24h / Next 72h windows.
3. Implement Port-Day board view grouping legs by arrival airport and local date, with cards showing key fields (tail, ETA, pax/crew counts when available, status badge).
4. Add leg detail drawer/modal revealing evidence timestamps, rule text, notes, documents, and compliance reasoning (including “after-hours” or “rule default” badges).
5. Provide quick communication helpers: copy-to-clipboard contact blocks populated from rule sheet (emails, phones) and flight specifics; optionally integrate webhook trigger scaffolding for late legs.

### 4. Audit Trail & Smart Warnings
1. Create an internal audit log model (in-memory or lightweight database/CSV) to record manual confirmations (who, when, notes, artifacts). Surface recent log entries per leg.
2. Track and display evidence sources (“arrivalMigration.updatedAt”, first document timestamp, manual override) and allow operators to override with justification.
3. Implement smart warnings for missing rules, missing timezone data, incompatible port flags (e.g., CANPASS-only), and NOTAM/holiday conflicts (using imported holiday list or manual NOTAM entries).
4. Expand the warnings expander to categorize issues and offer suggested next actions (e.g., “Arrange alternate airport” when port cannot service full customs).

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
