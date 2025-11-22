🛫 Airport Feasibility Module — Spec (Phase 1 / Quote-Based)

This module evaluates airport-related feasibility for each leg of a Quote, using:

Quote data (/api/external/quote/{id})

FL3XX airport operational notes (/api/external/airports/{code}/operationalNotes)

Internal IOCC reference tables (runways, customs, deice, OSA/SSA, slot/PPR)

It is pre-booking (Phase 1), so no crew / duty / preflight data is assumed.

1. Responsibilities

For each leg in a quote, the Airport Feasibility Module should:

Evaluate Airport Suitability (runway & ops approval)

Evaluate Deice Capability

Evaluate Customs Rules

Detect Slot / PPR Requirements

Detect Overflight Permit Need (route-level, but triggered from airports)

Classify OSA / SSA

Parse Operational Notes for hazards and special procedures

Return a structured AirportFeasibilityResult that the main Feasibility Engine can consume.

2. Inputs & Outputs
2.1 Input: LegContext

Created from the Quote schema.

class LegContext(TypedDict):
    leg_id: str
    departure_icao: str
    arrival_icao: str
    departure_date_utc: str    # "YYYY-MM-DDTHH:MM"
    arrival_date_utc: str
    pax: int
    block_time_minutes: int
    flight_time_minutes: int
    distance_nm: float
    workflow: str              # e.g. "PRIVATE"
    workflow_custom_name: str  # e.g. "FEX Guaranteed"
    notes: str                 # leg-level notes
    planning_notes: str        # leg-level planningNotes
    warnings: list[dict]       # as returned by quote.legs[].warnings
    aircraft_type: str         # e.g. "E545"
    aircraft_category: str     # e.g. "SUPER_MIDSIZE_JET"

2.2 Input: Internal Reference Lookups

These are expected to be provided by other modules or static data files:

get_airport_profile(icao: str) -> AirportProfile

get_deice_profile(icao: str) -> DeiceProfile

get_customs_profile(icao: str) -> CustomsProfile

get_osa_ssa_profile(icao: str) -> OsaSsaProfile

get_slot_ppr_profile(icao: str) -> SlotPprProfile

get_overflight_profile() -> OverflightRules (global, not per airport)

Example profile types (simplified):

class AirportProfile(TypedDict):
    icao: str
    name: str
    longest_runway_ft: int
    is_approved_for_ops: bool
    category: str  # A/B/C etc.
    elevation_ft: int

class DeiceProfile(TypedDict):
    icao: str
    deice_available: bool
    has_cdf: bool
    notes: str

class CustomsProfile(TypedDict):
    icao: str
    type: str      # "AOE", "CANPASS", "AOE/CANPASS", "NONE"
    hours_local: dict  # e.g. {"start": "05:00", "end": "22:00"}
    aoe15: bool
    notes: str

class OsaSsaProfile(TypedDict):
    icao: str
    region: str       # e.g. "CANADA_DOMESTIC", "OSA", "SSA"
    requires_jepp: bool

class SlotPprProfile(TypedDict):
    icao: str
    slot_required: bool
    ppr_required: bool
    slot_lead_days: int | None
    ppr_lead_days: int | None
    notes: str

2.3 Input: Operational Notes API

One function to encapsulate the FL3XX call:

def fetch_operational_notes(
    icao: str,
    date_local: str
) -> list[dict]:
    """
    Call:
      GET /api/external/airports/{icao}/operationalNotes?from=YYYY-MM-DD&to=YYYY-MM-DD

    Returns parsed list of note dicts with fields such as:
      - category / type
      - title
      - body
      - valid_from / valid_to
    """


The module should not assume exact operationalNotes schema, but should treat each item as:

class OperationalNote(TypedDict):
    category: str | None
    type: str | None
    title: str | None
    body: str | None
    valid_from: str | None
    valid_to: str | None

2.4 Output: AirportFeasibilityResult

For each direction (DEP and ARR) leg, produce:

class CategoryResult(TypedDict):
    status: str         # "PASS", "CAUTION", "FAIL"
    summary: str        # short human-readable sentence
    issues: list[str]   # bullet-point list

class AirportSideResult(TypedDict):
    icao: str
    suitability: CategoryResult
    deice: CategoryResult
    customs: CategoryResult
    slot_ppr: CategoryResult
    osa_ssa: CategoryResult
    overflight: CategoryResult   # may be shared per leg, but included for consistency
    operational_notes: CategoryResult  # hazards/alerts summarised
    raw_operational_notes: list[OperationalNote]

class AirportFeasibilityResult(TypedDict):
    leg_id: str
    departure: AirportSideResult
    arrival: AirportSideResult
    aircraft: CategoryResult

3. Module Layout

Recommended file layout:

/feasibility/
    airport_module.py           # main public entry points
    airport_notes_parser.py     # helpers to classify operational notes
    airport_profiles.py         # reference lookups (or wrappers around existing data)

4. Public API Functions
4.1 evaluate_airport_feasibility_for_leg
def evaluate_airport_feasibility_for_leg(
    leg: LegContext,
    tz_provider: Callable[[str], str] | None = None,
) -> AirportFeasibilityResult:
    """
    Main entry point.
    - Resolves airport profiles (runway, customs, deice, osa/ssa, slot/ppr).
    - Fetches and parses operational notes for the flight date.
    - Evaluates all airport-related feasibility categories for BOTH departure and arrival.
    """


Internally, this should:

Resolve departure and arrival AirportProfile, DeiceProfile, etc.

Convert departureDateUTC / arrivalDateUTC to local dates (using tz_provider(icao) if provided).

Fetch and parse operational notes for each airport.

Call per-airport evaluators (evaluate_airport_side).

4.2 evaluate_airport_side
def evaluate_airport_side(
    icao: str,
    date_local: str,
    leg: LegContext,
    airport_profile: AirportProfile,
    deice_profile: DeiceProfile,
    customs_profile: CustomsProfile,
    osa_ssa_profile: OsaSsaProfile,
    slot_ppr_profile: SlotPprProfile,
    operational_notes: list[OperationalNote],
    overflight_rules: OverflightRules,
    side: str,  # "DEP" or "ARR"
) -> AirportSideResult:
    """
    Evaluate one side (DEP/ARR) of a leg.
    Combines static profiles, quote leg info, and operational notes.
    """


It should:

Call category-specific evaluators:

suitability = evaluate_suitability(...)
deice = evaluate_deice(...)
customs = evaluate_customs(...)
slot_ppr = evaluate_slot_ppr(...)
osa_ssa = evaluate_osa_ssa(...)
overflight = evaluate_overflight(...)      # may be leg-level, but keep here
operational_notes_res = summarize_operational_notes(...)

5. Category Evaluators (Logic Contracts)

Each evaluator returns a CategoryResult.

5.1 evaluate_suitability
def evaluate_suitability(
    airport_profile: AirportProfile,
    leg: LegContext,
    operational_notes: list[OperationalNote],
    side: str,
) -> CategoryResult:
    """
    Checks:
      - Fl3xx category from data/Airports_Fl3xx_Categories.csv (A/B/C pass; NC/P/blank fail)
      - runway length vs aircraft type requirements
      - operational notes for closures, restricted runways, 'no GA', curfew, etc.
    """


Rules (typical):

If Fl3xx category is NC, P, or missing → FAIL with a Fl3xx category issue

If required_runway_ft(leg.aircraft_type, leg.pax, leg.distance_nm) > airport_profile.longest_runway_ft → FAIL

If operationalNotes indicate:

full closure during ETA → FAIL

partial closure impacting runway length → CAUTION/FAIL depending on margin

Else → PASS (with possible notes if marginal).

5.2 evaluate_deice
def evaluate_deice(
    deice_profile: DeiceProfile,
    operational_notes: list[OperationalNote],
    leg: LegContext,
    side: str,
) -> CategoryResult:
    """
    Uses deice_profile + notes to determine:
      - Is deice generally available?
      - Any outages or severe limitations on the flight date?
    """


Rules:

If deice_profile.deice_available is False → CAUTION (winter risk) or FAIL if seasonally critical.

If operationalNotes contain deice-outage keywords → override to CAUTION/FAIL.

If deice is available and no outages → PASS.

Exact thresholds can be adjusted later (separate config).

5.3 evaluate_customs
def evaluate_customs(
    customs_profile: CustomsProfile,
    leg: LegContext,
    side: str,
    operational_notes: list[OperationalNote],
) -> CategoryResult:
    """
    Determine if customs is available for this side (DEP/ARR) at the planned time.
    Checks:
      - AOE vs CANPASS vs NONE
      - hours of operation vs ETA/ETD (if time-of-day handling added)
      - operational notes for temporary closures or changes.
    """


Rules:

If customs_profile.type == "NONE" for an international boundary or required entry point → FAIL.

If CANPASS-only and leg implies international arrival:

If notes or external pax data show missing CANPASS → CAUTION.

If ETA/ETD outside hours and no special note → CAUTION.

If notes explicitly state “no customs at this time” → FAIL.

(Phase 1 can treat time-of-day checks as best-effort; detailed time handling can be added later.)

5.4 evaluate_slot_ppr
def evaluate_slot_ppr(
    slot_ppr_profile: SlotPprProfile,
    leg: LegContext,
    side: str,
    operational_notes: list[OperationalNote],
    now_date_local: str | None = None,
) -> CategoryResult:
    """
    Determines whether slot/PPR is required and whether the current date allows
    enough lead time.

    NOTE: This is Phase 1, pre-booking. It *cannot* know if the slot is already obtained.
    It only answers "required?" and "inside lead window?".
    """


Rules:

If slot_ppr_profile.slot_required is True:

If leg is scheduled within slot_ppr_profile.slot_lead_days from now → CAUTION (urgent slot need).

Else → PASS but note requirement.

Same logic for PPR.

Operational notes can upgrade severity if they mention:

“limited GA slots”

“high-traffic period”

“PPR must be requested 72h before arrival”

Output should clearly state that this is a requirement detection, not a status check.

5.5 evaluate_osa_ssa
def evaluate_osa_ssa(
    osa_ssa_profile: OsaSsaProfile,
    leg: LegContext,
    side: str,
) -> CategoryResult:
    """
    Simply classifies the leg side as:
      - DOMESTIC
      - SSA
      - OSA
      - Jeppesen required or not.
    """


Rules:

If osa_ssa_profile.requires_jepp is True → CAUTION with instruction: “Jeppesen ITP required.”

Else → PASS.

This is informational, but important for Jeppesen-task creation upstream.

5.6 evaluate_overflight
def evaluate_overflight(
    overflight_rules: OverflightRules,
    departure_icao: str,
    arrival_icao: str,
) -> CategoryResult:
    """
    Uses a simple route model (e.g. great-circle) and overflight_rules to determine
    whether the leg will cross FIRs/countries that require permits.

    NOTE: This is approximate in Phase 1; precise route can be refined later.
    """


Rules:

Compute rough great-circle path between airports (using lat/long from AirportProfile if available).

Intersect with overflight_rules polygons or simple pairwise lists.

If any permit country encountered:

If flight is beyond some lead-days threshold → CAUTION (“Permit will be required; sufficient lead time.”)

If inside the threshold → CAUTION/FAIL depending on your policy.

5.7 summarize_operational_notes
def summarize_operational_notes(
    icao: str,
    operational_notes: list[OperationalNote],
    leg: LegContext,
    side: str,
) -> CategoryResult:
    """
    Scans the notes for:
      - closures (runway/taxiway/apron)
      - GA restrictions
      - curfew / noise
      - any 'ALERT' or 'WARNING'-type entries.

    Returns:
      - PASS if nothing impactful
      - CAUTION if relevant info but not blocking
      - FAIL if a note indicates a hard stop for the planned operation.
    """


This function should rely on a configurable list of keyword → severity mappings, e.g.:

“closed”, “not available” → possible FAIL

“limited”, “capacity”, “expect delay” → CAUTION

6. Error Handling / Fallbacks

If any profile lookup (AirportProfile, DeiceProfile, etc.) is missing:

Set status to CAUTION with summary “Missing profile for XXXX; manual check required.”

If operationalNotes API fails:

Still return results from static profiles

operational_notes CategoryResult should be CAUTION with issue “Could not retrieve operational notes.”

7. Integration Notes

The module is pure logic: it should not depend on Streamlit.

All external I/O (API calls, DB lookup) should be injected or wrapped, so Codex can easily mock or replace them.

The module should be usable both:

From a Feasibility Engine (per leg), and

From a standalone “Airport Analyzer” UI (e.g., for quick airport lookups).
