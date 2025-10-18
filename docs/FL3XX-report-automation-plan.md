# FL3XX Morning Report Automation Plan

This document captures the current feasibility assessment for automating the morning operational reports inside the AirSprint Tools project. It focuses on the reports that can leverage the existing FL3XX API wiring and notes any additional data points or clarifications required before development can begin. Report 16.1.8 (Duty Violation Report) has been intentionally omitted from this plan per the latest guidance.

For each report, the sections below describe:

- **Current Capability** – What the existing ingestion/normalisation layer already provides.
- **Gaps / Required Inputs** – Additional payload fields, lookup tables, or business rules we still need.
- **Next Steps** – Suggested follow-up actions once the missing inputs are available.

## 16.1.1 App Booking Workflow Report
- **Current Capability:** `fetch_flights` can retrieve all legs in a configurable window, and the normaliser already captures workflow, tags, and note text for each flight.
- **Gaps / Required Inputs:** The FL3XX payload marks these legs with `"workflowCustomName": "App Booking"`; no additional identifiers are required once this label is surfaced.
- **Next Steps:** Extend the workflow filter list with the `App Booking` label and output results using the format:

  ```
  Results Found:
  App Booking Workflow
  <Date>-<Booking ID>-<Account Name>
  ...
  ```

  When no legs match, return:

  ```
  No Results Found:
  App Booking Workflow
  No Results Found
  ```

## 16.1.2 App Line Assignment Report
- **Current Capability:** Aircraft assignment details are normalised, including detection of placeholder tails such as values starting with "ADD"/"REMOVE".
- **Gaps / Required Inputs:** Placeholder tails are confirmed as `registrationNumber` values beginning with `App`, `App CJ2+`, `App CJ2+/CJ3+`, `App CJ3+`, and `App E550`; ensure these remain covered by the detection heuristics.
- **Next Steps:** Update the placeholder-detection logic to treat the confirmed `registrationNumber` prefixes as "App Line" assignments, and surface any matches in the app using the format:

  ```
  Results Found:
  App Line Assignment
  <Date>-<Booking ID>-<Account Name>
  ...
  ```

  When no legs match, return:

  ```
  No Results Found:
  App Line Assignment
  No Results Found
  ```

## 16.1.3 Empty Leg Report
- **Current Capability:** Legs can be queried for any date range, and we already store tail numbers, airports, times, booking codes, and priority flags.
- **Gaps / Required Inputs:** The FL3XX flight payload sets `"flightType": "POS"` for OCS (Empty Leg) segments, and those legs should always carry an `account` value of `AirSprint Inc.`; treat blank or different account values as anomalies that must be surfaced in the report output.
- **Next Steps:** Persist the `flightType` and `account` fields during normalisation, validate that every `POS` leg retains the `AirSprint Inc.` account, flag discrepancies, and output results using the format:

  ```
  Results Found:
  Empty Leg Report
  <Date>-<Booking ID>-<Account Name>-<Aircraft Tail>
  ...
  ```

  When no legs match, return:

  ```
  No Results Found:
  Empty Leg Report
  No Results Found
  ```

## 16.1.4 OCS Flights with Pax Report
- **Current Capability:** The morning report app now flags OCS flights (``flightType == "PAX"`` and ``accountName == "AirSprint Inc."``), fetches the notification payload for each leg, and surfaces passenger counts alongside the notes text.
- **Gaps / Required Inputs:** None — the required identifiers and notification endpoint are available via the standard flight payloads.
- **Next Steps:** Monitor live usage for edge cases (e.g., missing notifications or atypical note formatting) and expand validations when additional business rules are provided.

## 16.1.5 Owner Continuous Flight Validation Report
- **Current Capability:** The ingestion captures tail numbers, airports, and timestamps, which is enough to group consecutive legs by aircraft and day.
- **Gaps / Required Inputs:** Provide the payload key that identifies the owner/account associated with each leg so itineraries can be grouped per owner.
- **Next Steps:** With owner data available, implement logic to ensure contiguous legs for the same owner stay on the same tail and flag discrepancies.

## 16.1.6 CJ3 Owners on CJ2 Report
- **Current Capability:** Leg duration can be computed from departure and arrival timestamps.
- **Gaps / Required Inputs:** We need fields (or a lookup) that reveal: the owner's entitled fleet type, the actual aircraft type assigned, and the passenger count on the leg.
- **Next Steps:** Ingest the requested/entitled aircraft metadata and pax counts, then check each CJ3 owner leg on a CJ2 against the ≤5 pax / ≤3 hours criteria.

## 16.1.7 Priority Status Report
- **Current Capability:** Priority workflows/flags can be detected, and post-flight check-in timestamps are already parsed through `fetch_postflight` for historical validation.
- **Gaps / Required Inputs:** To audit future duty assignments, we need access to the scheduled duty start times from Timeline or the crew scheduling API because current endpoints only expose actual check-in times.
- **Next Steps:** Determine the source of planned duty-start data; once available, compare it against departure times to confirm the 90-minute buffer and surface exceptions.

## 16.1.9 Upgrade Workflow Validation Report
- **Current Capability:** Workflow labels are available, enabling us to find flights that use an upgrade-specific workflow.
- **Gaps / Required Inputs:** Confirm the workflow name(s) used for upgrades and provide the fields that describe the assigned aircraft type, owner class, and any other criteria that should align with the workflow.
- **Next Steps:** Extend normalisation to include the new aircraft metadata and implement cross-checks between workflow, tail assignment, and required notes.

## 16.1.10 Upgraded Flights Report
- **Current Capability:** We can retrieve legs based on workflow labels and already collect general note text.
- **Gaps / Required Inputs:** Identify the fields that capture (a) the requested aircraft type versus the assigned tail, (b) the location of upgrade rationale/billable-hours notes, and (c) the rule set for validating a "proper" upgrade.
- **Next Steps:** Once these data points are mapped, validate each candidate upgrade, ensure notes are populated, and produce the notification payloads for today/tomorrow as described.

## 16.1.11 FBO Disconnects
- **Current Capability:** Airports and schedules are stored for each leg, providing the framework for comparisons.
- **Gaps / Required Inputs:** We currently do not ingest FBO identifiers. Please specify which payload field(s) contain the planned arrival and departure FBO so we can compare them for mismatches.
- **Next Steps:** Capture the FBO data during normalisation and flag same-airport legs where the arrival and departure FBO differ.

---

As you supply the missing field mappings or rule definitions for each report, we can start turning these sections into actionable development tasks and wire them into the multi-report automation app.
