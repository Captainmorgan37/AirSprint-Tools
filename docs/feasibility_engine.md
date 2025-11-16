Feasibility Engine — Technical Blueprint

This document defines the architecture, logic, and code structure for integrating the Feasibility Engine into the IOCC Tools suite.

It is written for GPT Codex and is intended as the authoritative source for generating code inside the /pages/Dev workspace.

=======================================
1. Purpose of the Feasibility Engine
=======================================

Given a FL3XX bookingIdentifier (e.g., "ILARD"), the Feasibility Engine should:

Locate the corresponding flight from FL3XX using a smart, reliable lookup method

Run feasibility checks across all major categories:

Aircraft

Airport

Duty / FRMS

OSA / SSA / Jeppesen requirements

Overflight permits

Produce a structured, standardized feasibility report

Allow DMs to copy/paste output into OS notes or DM chat

Integrate cleanly into the existing IOCC Tools Streamlit layout

=======================================
2. BookingIdentifier Flight Lookup
=======================================

FL3XX does not provide an endpoint to query flights directly by bookingIdentifier.

Therefore we use a 3-Tier Smart Search Model:

🟦 Tier 1 — Short-Range Search (Fast Path)

Check flights from today → +4 days, which captures the majority of newly created bookings.

GET /flight/flights?from=today&to=today+4&value=ALL


Search locally for the matching bookingIdentifier.

🟦 Tier 2 — Recent Creations (Solve Cache Freshness Problem)

New flights may be far in the future but newly created.

Query the last 48 hours:

GET /flight/flights?from=today-2&to=today&value=ALL


Search returned flights for the bookingIdentifier.

This guarantees brand-new bookings appear.

🟦 Tier 3 — Future Slab Search (0 → 365+ Days)

To avoid the FL3XX 300-flight limit, search the future in slabs:

Next 30 days: 3–5 day slabs

30 → 180 days: 7–10 day slabs

180 → 365 days: 14–21 day slabs

Example slab:

GET /flight/flights?from=2025-05-01&to=2025-05-07&value=ALL


Each slab should return < 300 flights, ensuring safe pagination.

🟪 Flight Lookup Pseudocode
lookup_booking(bid):
    r = search_range(today, today+4)
    if bid in r: return flight

    r = search_range(today-2, today)
    if bid in r: return flight

    for slab in slabs(today+4 → today+365):
        r = search_range(slab)
        if bid in r: return flight

    return None

🟨 Caching Strategy (Optional but Optimal)

Cache the next 4 days + slab results in st.session_state, but always re-check Tier 3 for newly created flights.

=======================================
3. Feasibility Engine Architecture
=======================================
Module Layout
/feasibility/
    __init__.py
    lookup.py              # bookingIdentifier search subsystem
    checker_aircraft.py    # aircraft feasibility rules
    checker_airport.py     # airport, customs, deice, OSA/SSA
    checker_duty.py        # duty legality using FRMS logic
    checker_trip.py        # Jeppesen / OSA / SSA
    checker_overflight.py  # overflight permit logic
    engine.py              # main feasibility assembler
    schemas.py             # data models

=======================================
4. Feasibility Categories
=======================================

Each category produces a Result object:

Result Schema
{
  "status": "PASS" | "CAUTION" | "FAIL",
  "summary": "Short text summary",
  "issues": ["List of specific findings"]
}

=======================================
5. Category Logic Definitions
=======================================

Below are Codex-friendly descriptions of each feasibility category.

🛩️ 5.1 Aircraft Checks

Inputs:

aircraft type

pax count

baggage

block time

route

performance tables (simplified at first)

Rules:

Flag CJ2/3 flights approaching endurance limits

Ensure performance tables allow safe operation

If range exceeded → FAIL

If marginal → CAUTION

🛬 5.2 Airport Checks

Inputs:

runway lengths

OSA/SSA airport type

airport category (from your CSV)

deice capability

customs capability

contamination / NOTAMs (optional future integration)

Rules:

Runway suitability by aircraft type

SSA/OSA → enforce 90/120 min ground time

Customs availability restrictions

Deice = No + expected freezing temps → CAUTION

Missing airport data → CAUTION

⏱️ 5.3 Duty / FRMS Checks

Inputs:

assigned crew

planned duty (estimated)

cumulative duties

pre/post rest

flight times

split duty / hotel reshuffles

your FRMS calculator logic

Rules:

Illegal duty → FAIL

Duty margin < 30 min → CAUTION

Pattern creating future issues → CAUTION

🌎 5.4 Jeppesen / OSA / SSA Checks

Simple rules:

OSA/SSA airports → Jeppesen planning required

International sectors requiring advance planning

High-risk countries → Jeppesen mandatory

🛫 5.5 Overflight Permits

Inputs:

route coordinates (optional)

country FIR list

overflight rules JSON

Rules:

If permit required AND lead time insufficient → FAIL

If permit required but lead time OK → CAUTION

If no permit needed → PASS

=======================================
6. Feasibility Output Format
=======================================
6.1 JSON Output Structure
{
  "bookingIdentifier": "ILARD",
  "flightId": 12345,
  "overallStatus": "PASS" | "CAUTION" | "FAIL",
  "categories": {
      "aircraft": {...},
      "airport": {...},
      "duty": {...},
      "trip": {...},
      "overflight": {...}
  },
  "notesForOS": "Formatted text for copy/paste",
  "timestamp": "2025-11-16T10:00Z"
}

6.2 Human-Friendly Output (for Streamlit)
Feasibility Result: PASS

• Aircraft: PASS
• Airport: CAUTION – SSA airport; 90 min ground required.
• Duty: PASS
• Trip Planning: CAUTION – Jeppesen required.
• Overflight: PASS

Notes for OS:
- SSA arrival requires 90 min ground time.
- Jeppesen trip planning required.

=======================================
7. Streamlit Integration
=======================================

Suggested placement:
/pages/Dev_Feasibility.py

UI Flow

Input text box: "Enter Booking Identifier"

Button: "Run Feasibility"

Lookup via lookup_booking()

Display result cards per category

Expandable detail panels

Copy-to-clipboard buttons

=======================================
8. Future Enhancements (Optional)
=======================================
8.1 Auto-Feasibility Watcher

Poll FL3XX every 5 min for newly created flights → run feasibility automatically → push DM chat notification.

8.2 Add NOTAMs, TFRs, Weather Risk

Merge with your Arrival Weather Outlook.

8.3 Crew Pairing Suggestion Engine

Recommend legal + optimal pairings.

END OF DOCUMENT
