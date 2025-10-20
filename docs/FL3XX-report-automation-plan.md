# FL3XX Morning Report Automation Plan

This plan tracks the Operations Lead (OL) morning report coverage that now ships in the automation app (`morning_reports.py`). It documents what has already been delivered and highlights the open work required to complete the remaining reports. Report 16.1.8 (Duty Violation Report) remains out of scope per prior guidance.

## Status Snapshot

| Report | Status | Notes |
| --- | --- | --- |
| 16.1.1 App Booking Workflow | ✅ Implemented | Workflow label filter surfaces legs under the "APP BOOKING" workflow and formats the booking/account tuple for display. |
| 16.1.2 App Line Assignment | ✅ Implemented | Placeholder detection normalises registration prefixes such as `APP`, `APP CJ3+`, etc., and emits matching legs. |
| 16.1.3 Empty Leg | ✅ Implemented | POS legs are screened for the expected `AirSprint Inc.` account; only anomalies are output with warnings. |
| 16.1.4 OCS Flights with Pax | ✅ Implemented | Pulls FL3XX notifications for OCS passenger legs, caches responses, and includes pax counts plus note excerpts. |
| 16.1.5 Owner Continuous Flight Validation | ✅ Implemented | Consecutive legs are grouped by account to flag rapid tail changes (<3 hours). |
| 16.1.6 CJ3 Owners on CJ2 | ✅ Implemented | CJ3 owner legs flown on CJ2 equipment fetch quote details, evaluate pax/block thresholds, and summarise breaches. |
| 16.1.7 Priority Status | ✅ Implemented | Priority departures trigger duty-start validation via post-flight check-in data with fallbacks and warnings. |
| 16.1.9 Upgrade Workflow Validation | ✅ Implemented | Legacy aircraft upgrade requests inspect planning notes and leg details to confirm workflow alignment. |
| 16.1.10 Upgraded Flights | ✅ Implemented | Upgrade workflow legs surface booking vs. assignment transitions, booking notes, and missing quote warnings. |
| 16.1.11 FBO Disconnects | ⏳ Pending | Awaiting definitive FBO identifiers in the payload to compare arrival vs. departure handling. |

The sections below capture implementation specifics for the completed reports and outline what is still required for the remaining ones.

## 16.1.1 App Booking Workflow Report (Implemented)
- **Implementation:** `_build_app_booking_report` filters normalised legs by the `APP BOOKING` workflow name and formats each result as `<Date>-<Booking ID>-<Account Name>` before handing it to the UI layer.
- **Follow-up:** None required beyond monitoring for new workflow labels.

## 16.1.2 App Line Assignment Report (Implemented)
- **Implementation:** `_build_app_line_assignment_report` reuses the shared tail extraction helpers and treats any registration beginning with one of the configured `APP` prefixes as an App Line placeholder.
- **Follow-up:** Keep the `_APP_LINE_PREFIXES` list aligned with any future naming changes.

## 16.1.3 Empty Leg Report (Implemented)
- **Implementation:** `_build_empty_leg_report` pulls every POS leg and highlights those whose account name deviates from `AirSprint Inc.`. Each anomaly updates the line item with an explicit ⚠️ indicator and records an audit warning.
- **Follow-up:** Confirm whether legitimate empty legs should appear even when the account matches (currently only mismatches are emitted).

## 16.1.4 OCS Flights with Pax Report (Implemented)
- **Implementation:** `_build_ocs_pax_report` identifies OCS passenger legs (`flightType == "PAX"` and account `AirSprint Inc.`), fetches the notification payload once per flight identifier, and surfaces pax counts with a trimmed note summary. Network failures are logged as warnings without stopping the run.
- **Follow-up:** Watch for notification schema drift so the note extraction continues to find the briefed text.

## 16.1.5 Owner Continuous Flight Validation Report (Implemented)
- **Implementation:** `_build_owner_continuous_flight_validation_report` groups non-OCS passenger legs by the normalised account name, sorts them chronologically, and flags tail changes with less than three hours between arrival and next departure. Metadata records which accounts were affected.
- **Follow-up:** If a more authoritative owner identifier becomes available, wire it into the grouping logic to avoid collisions between similarly named accounts.

## 16.1.6 CJ3 Owners on CJ2 Report (Implemented)
- **Implementation:** `_build_cj3_owners_on_cj2_report` inspects each CJ3 booking flown on CJ2 hardware. It retrieves the associated quote/leg detail, derives passenger counts and block times, and marks any legs exceeding the ≤5 pax or ≤180 minute limits. Missing data triggers warnings and still surfaces the leg for manual review.
- **Follow-up:** Capture any additional violation rules (e.g., owner class filters) as they are defined.

## 16.1.7 Priority Status Report (Implemented)
- **Implementation:** `_build_priority_status_report` caches the first departure per tail/day, finds priority legs, and either validates or flags the duty-start window. When credentials exist it fetches post-flight check-ins; otherwise it records a warning and marks the leg as needing manual validation.
- **Follow-up:** If planned duty-start times become available, incorporate them to cross-check against actual check-ins.

## 16.1.9 Upgrade Workflow Validation Report (Implemented)
- **Implementation:** `_build_upgrade_workflow_validation_report` focuses on legacy categories (`E550`, `E545`), fetches leg details by booking reference, parses planning notes for CJ upgrade labels, and outputs the booking/tail summary along with metadata describing inspection totals.
- **Follow-up:** Extend the matchers if additional upgrade note formats or aircraft families need coverage.

## 16.1.10 Upgraded Flights Report (Implemented)
- **Current Capability:** `_build_upgrade_flights_report` lists every upgrade workflow leg, displaying the booking reference, requested vs. assigned equipment transition, booking notes, and any missing quote identifier warnings. The report metadata now captures which workflow labels were inspected, how many legs were considered upgrade candidates, and whether quote identifiers or booking references were missing.
- **Required Inputs:** Legs must surface an upgrade-oriented workflow label (e.g., `workflowCustomName` containing "upgrade"), the quote identifier (so we can fetch booking notes and requested types), and ideally the booking reference directly on the flight payload. When either identifier is absent the leg still appears, but the metadata will highlight the missing field counts for troubleshooting.
- **Next Steps:** Monitor for new upgrade workflow variants and extend validation heuristics as additional business rules are introduced. Review the metadata summaries if the report ever renders empty to confirm whether upgrade-labelled workflows were present in the fetch window.

## 16.1.11 FBO Disconnects (Pending)
- **Current Capability:** The normalised payload already exposes airports and timestamps, allowing us to match legs operating from the same field.
- **Required Inputs:** We still need definitive FBO identifiers (arrival and departure) exposed in the ingestion layer.
- **Next Steps:** Introduce the FBO fields into normalisation, then compare same-airport legs for mismatched FBO handling and emit any disconnects.

---

As the missing mappings and business rules are supplied, we can create follow-on tickets to implement the outstanding reports and continue iterating on the OL morning automation.
