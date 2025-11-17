🛫 Multi-Leg Feasibility Engine — Phase 1 (Quote-Based)

(Codex-Ready Technical Specification)

This engine evaluates the entire quote as a multi-leg operational day, producing a complete feasibility result before the flight is booked.

It relies on:

/api/external/quote/{id}

/api/external/airports/{icao}/operationalNotes

internal IOCC reference datasets

no crew-assignment data (generic duty logic only)

This forms Feasibility Check → Phase 1, performed during trip setup.

1. Goals

The engine must:

Parse the entire quote (1…N legs)

Construct a timeline of duty, airport usage, turn times, risks

Evaluate each leg individually using the Airport Feasibility Module

Evaluate the entire day using Generic Duty Logic

Combine leg-level + day-level results into a single feasibility result

Produce a clear, human-readable output for Duty Managers

Provide structured data Codex can manipulate and UI can render

The system should not depend on booking state or crew assignment.

2. Engine Structure

Recommended folder structure:

/feasibility/
    engine_phase1.py           # main orchestrator
    airport_module.py           # leg-level airport checks
    duty_module.py              # day-level generic duty logic
    models.py                   # TypedDicts for all data structures
    utils/                      # time conversion, sorting, etc.

3. Inputs

Phase 1 engine receives:

class FeasibilityRequest(TypedDict):
    quote_id: int | str
    quote: dict                 # raw /quote/{id} response
    now_utc: datetime           # current UTC time
    tz_provider: Callable[[str], str]  # function returning airport timezone


Codex can optionally auto-fetch /quote/{id} if not provided.

4. Internal Data Structures
4.1 DayContext (timeline of all legs)
class DayContext(TypedDict):
    quote_id: int
    bookingIdentifier: str
    aircraft_type: str
    aircraft_category: str
    legs: list[LegContext]      # sorted chronologically
    sales_contact: str | None
    createdDate: int | None     # ms timestamp from FL3XX

4.2 LegContext

Constructed from each quote.legs[] entry:

class LegContext(TypedDict):
    leg_id: str
    departure_icao: str
    arrival_icao: str
    departure_date_utc: str
    arrival_date_utc: str
    pax: int
    block_time_minutes: int
    flight_time_minutes: int
    distance_nm: float
    workflow: str
    workflow_custom_name: str | None
    notes: str | None
    planning_notes: str | None
    warnings: list[dict]

5. Workflow Summary
Engine → Build DayContext → Evaluate Legs (Airport Module)
                               ↓
                     Evaluate Multi-Leg Duty Day
                               ↓
                   Combine & Produce Final Result

6. Sorting Legs

Quotes may return legs in correct order, but engine must enforce chronological sorting:

legs = sorted(legs, key=lambda L: L["departure_date_utc"])

7. Leg-Level Evaluation (Airport Module)

For each leg:

leg_result = evaluate_airport_feasibility_for_leg(
    leg=leg_ctx,
    tz_provider=tz_provider,
)


This returns:

class LegFeasibilityResult(TypedDict):
    leg_id: str
    departure: AirportSideResult
    arrival: AirportSideResult


(Defined in the airport module spec already generated.)

8. Multi-Leg Duty Day Evaluation

Performed only after all legs have been parsed.

Generic Duty Logic uses no assigned crew, relying on operational feasibility rules:

Must compute:

duty_start = first leg’s departure_date_local

duty_end = last leg’s arrival_date_local

total_duty_minutes

ground_times between legs

split duty possibility (≥ 6:00 ground)

reset duty possibility (≥ 11:15 ground)

FDP maximum (assume 14:00 standard)

exceedances beyond 14:00 and ≤17:00

multi-leg sequencing risks

duty_module.py interface:
def evaluate_generic_duty_day(day: DayContext) -> DutyFeasibilityResult:
    """
    Returns PASS / CAUTION / FAIL + structured reasoning.
    """

Output:
class DutyFeasibilityResult(TypedDict):
    status: str         # PASS, CAUTION, FAIL
    total_duty: int
    duty_start_local: str
    duty_end_local: str
    turn_times: list[int]          # minutes between legs
    split_duty_possible: bool
    reset_duty_possible: bool
    issues: list[str]
    summary: str


Rules:

≤14:00 → PASS

14:00–17:00 → CAUTION (needs extension feasibility)

≥17:00 → FAIL

9. Final Combined Result
class FullFeasibilityResult(TypedDict):
    quote_id: int
    bookingIdentifier: str
    aircraft_type: str
    legs: list[LegFeasibilityResult]
    duty: DutyFeasibilityResult
    overall_status: str       # PASS, CAUTION, FAIL
    issues: list[str]         # aggregated
    summary: str              # human-readable derived from all sections

10. Determining Overall Status

Priority:

If any leg has a FAIL → overall FAIL
Else if duty FAIL → overall FAIL
Else if any leg or duty has CAUTION → overall CAUTION
Else → PASS

11. Summary Generator

Engine should produce a narrative summary:

Quote PIURB (Embraer Legacy 450)
Two-leg sequence CYYC→CYVR→CYYC on 19 NOV 2025.

Duty Day:
- Total duty 03:00 (PASS)
- No split duty or reset duty eligibility.
- Adequate turn times.

Leg 1 (CYYC→CYVR):
- Airport checks PASS, CYVR requires slot (CAUTION)
- No customs required.
- Deice available and normal.

Leg 2 (CYVR→CYYC):
- All checks PASS.

12. API Call Requirements
12.1 Quote Retrieval

GET /api/external/quote/{id}
GET /api/external/quote/{id}/history (optional for OS notes)

12.2 Operational Notes (per leg, per airport)
GET /api/external/airports/{ICAO}/operationalNotes?from=YYYY-MM-DD&to=YYYY-MM-DD


Date = leg’s local date, not UTC.

12.3 Internal DBs needed

runway lengths

airport approvals

customs rules

deice capability

OSA/SSA regions

slot/PPR rules

overflight permit rules
(All stored internally or in your IOCC app files.)

13. Engine Pseudocode
def run_feasibility_phase1(request: FeasibilityRequest) -> FullFeasibilityResult:
    quote = request["quote"]
    legs_raw = quote["legs"]

    # 1. Build DayContext
    legs = [build_leg_context(L) for L in legs_raw]
    legs = sort_legs(legs)
    
    day = DayContext(
        quote_id=quote["bookingid"],
        bookingIdentifier=quote["bookingIdentifier"],
        aircraft_type=quote["aircraftObj"]["type"],
        aircraft_category=quote["aircraftObj"]["category"],
        legs=legs,
        sales_contact=f"{quote['salesPerson']['firstName']} {quote['salesPerson']['lastName']}",
        createdDate=quote.get("createdDate")
    )

    # 2. Leg-level feasibility (Airport Module)
    leg_results = []
    for leg in day["legs"]:
        leg_result = evaluate_airport_feasibility_for_leg(leg, request["tz_provider"])
        leg_results.append(leg_result)

    # 3. Duty feasibility (Generic)
    duty_result = evaluate_generic_duty_day(day)

    # 4. Combine
    overall_status, issues = combine_results(leg_results, duty_result)

    return FullFeasibilityResult(
        quote_id=day["quote_id"],
        bookingIdentifier=day["bookingIdentifier"],
        aircraft_type=day["aircraft_type"],
        legs=leg_results,
        duty=duty_result,
        overall_status=overall_status,
        issues=issues,
        summary=format_summary(...)
    )

14. Key Design Principles

Deterministic → no hidden randomness

Pure logic → all side effects are API calls

Expandable → Phase 2 (post-booking) can reuse leg structures

Airport-first → because airport suitability is most likely to cause failures

Day-level synthesis → for duty and multi-leg dependencies

Codex-friendly → clear functions, stable TypedDict models

15. Deliverables in This Spec

This document defines:

All data models

Main engine responsibilities

Module layout

Multi-leg sequencing logic

Generic duty-day logic requirements

How to combine results

Required API calls

Expected outputs

Codex can now generate:

engine_phase1.py

duty_module.py

integration into your Streamlit Feasibility Tool
