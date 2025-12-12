# CARICOM eAPIS Upload Workflow

This workflow captures the planned user experience, data pulls, and file-generation steps required to support Flight Support with CARICOM Advanced Passenger Information System (eAPIS) submissions via the existing Excel template (`caricomformFlightsV6.xlsx`) or the alternative UN/EDIFACT text format.

## Goals
- Let an operator enter a booking identifier, fetch the relevant flight within the next 72 hours, and immediately indicate whether CARICOM filing is required.
- When CARICOM applies, retrieve all passenger and crew details needed to populate the upload formats with minimal manual data entry.
- Produce a ready-to-upload Excel workbook (primary) and optionally a UN/EDIFACT text file.

## CARICOM coverage check
- **CARICOM countries:** Antigua and Barbuda, Bahamas, Barbados, Belize, Dominica, Grenada, Guyana, Haiti, Jamaica, Montserrat, Saint Kitts and Nevis, Saint Lucia, Saint Vincent and the Grenadines, Suriname, Trinidad and Tobago.
- A flight requires CARICOM filing if any leg departs from or arrives into one of these countries.

## App entry flow (UI)
1. Present a landing screen similar to the Feasibility Checker, prompting for a **Booking Identifier**.
2. On **Fetch**, call the existing `GET Flights` endpoint for **today through +2 days** and locate the matching booking identifier.
3. If no leg touches a CARICOM country, surface a banner: “This flight does not require CARICOM eAPIS.”
4. If any leg enters or exits a CARICOM country, proceed to populate the data view and enable file generation.

## Data pulls
- **Flights query:** `GET Flights` filtered to `startDate=today` and `endDate=today+2` (inclusive). Extract the specific flight/leg matching the booking identifier.
- **Leg details:** Departure/arrival airports (IATA), scheduled date/time, flight ID (IATA/ICAO/reg), aircraft name, counts of crew and passengers.
- **Crew roster:** For each crew member, pull names, nationality (ISO-3), sex (M/F), date of birth, travel document number, issuing country (ISO-3), and expiry date.
- **Passenger roster:** Same fields as crew (names, nationality, sex, DOB, document number, issuing country, expiry).
- **Reporting party:** Name, phone, and email to include on the General Information sheet; supply defaults or a prompt if not present in API data.
- **Ports of movement:** For each person, derive embarkation, debarkation, and clearance ports (IATA) from leg routing; default unknown values to arrival/departure where necessary.

## Excel generation (primary path)
1. Load `docs/caricomformFlightsV6.xlsx` as the template workbook.
2. Populate the **General Information** sheet with flight-level data: flight ID, aircraft name, counts of passengers and crew, departure/arrival ports and times, and reporting-party contact.
3. Fill **Crew List** rows with the crew roster, including document details and movement ports.
4. Fill **Passenger List** rows with the passenger roster using the same field set as crew.
5. Validate row counts against the totals recorded in the General Information sheet and flag discrepancies before exporting the filled workbook.

## UN/EDIFACT text generation (optional path)
- Build a simple formatter that mirrors the Excel content into the UN/EDIFACT schema (pure text). Keep it off by default until the exact segment layout is confirmed; reuse the same data pulls and validations as the Excel path.

## Gaps and follow-ups
- Confirm the API surfaces **reporting-party** contact details; otherwise add a prompt or configuration source.
- Verify all **passport/document** fields (number, issuing country, expiry, middle names) are present for every crew and passenger; add input prompts for missing values.
- Double-check the **ISO-3** country codes and **IATA** airport codes for all legs and documents.
- Decide whether to auto-derive embarkation/debarkation/clearance ports or prompt the operator when multiple legs exist.
- Define the precise **UN/EDIFACT** segment layout once CARICOM accepts a sample text payload.
