Airport Notes & Feasibility v2 — Full Spec (for Codex)

Last updated: 2025-11-18
Context: IOCC Tools – Quote / Feasibility engine, FL3XX data
Scope:

Fix customs / deice / slot / PPR / ops-note parsing

Integrate airport_notes_parser with airport_module.evaluate_airport_feasibility_for_leg

Provide DM-friendly output for legs like CYEG → KPSP

0. High-Level Overview
Objective

Use FL3XX airport operational notes to drive a structured feasibility assessment for each leg, including:

Customs availability + rules

Deice capability

Slot / PPR requirements

Hours / curfews / night ops

Runway / aircraft-type limits

Winter sensitivity / special handling

Crew / pax requirements

and surface them inside:

"parsed_operational_restrictions": { ... },
"parsed_customs_notes": { ... },
"raw_operational_notes": [...]


which are then rendered in the feasibility UI.

1. Data Flow & Architecture
1.1 Flow Diagram (Conceptual)

Engine Phase 1 (engine_phase1.py):

For each leg in quote: calls evaluate_airport_feasibility_for_leg(...) from airport_module.

Airport Module (airport_module.py):

Fetches FL3XX airport notes via operational_notes_fetcher(icao, date).

Splits notes into customs vs operational using split_customs_operational_notes.

Parses:

ParsedCustoms via parse_customs_notes(customs_texts).

ParsedRestrictions via parse_operational_restrictions(operational_texts).

Builds AirportSideResult for departure and arrival (suitability / deice / customs / slot_ppr / operational_notes etc.).

Returns this structured dict to the engine.

UI Layer (Streamlit page):

Uses LegFeasibilityResult dict:

Renders each leg’s departure/arrival sections.

Uses parsed_* fields for summaries & details.

2. Input: FL3XX Airport Notes Format

Each FL3XX airport note object is shaped approximately like:

{
  "id": 15180,
  "operatorId": null,
  "airportId": 19085,
  "icao": "KPSP",
  "country": null,
  "countryWide": false,
  "note": "CUSTOMS INFORMATION:\n\nAvailable:\n• Location: ...",
  "alert": true,
  "start": null,
  "end": null,
  "deleted": null
}


Key points:

The full human text lives in the "note" field.

start / end are often null → notes are effectively timeless.

You fetch them using:

GET /api/external/airports/{icao}/operationalNotes?from=YYYY-MM-DD&to=YYYY-MM-DD


But because of null start/end, a broader range is usually required (see below).

3. Fetching Airport Notes
3.1 Fetcher Behavior

Implement / update a helper (exact name can vary) like:

def fetch_airport_notes(icao: str) -> list[dict]:
    """
    Fetch operational notes for an airport from FL3XX.

    IMPORTANT:
    - Many notes have null start/end.
    - To ensure they are returned, we use a very wide date window.
    - We treat all returned notes as globally applicable unless date-filter logic is added later.
    """
    from_date = "1900-01-01"
    to_date = "2100-01-01"

    # Example: use your existing FL3XX client
    path = f"/airports/{icao}/operationalNotes"
    params = {"from": from_date, "to": to_date}
    return fl3xx_get(path, params=params)  # or whatever your wrapper is


Then in airport_module.evaluate_airport_feasibility_for_leg, instead of passing a date-scoped fetch, you can simply:

dep_notes_raw = fetch_airport_notes(dep_icao)
arr_notes_raw = fetch_airport_notes(arr_icao)


(If you do want date filtering later, apply it after parsing, based on start/end when those exist.)

4. Note Text Extraction

Central helper used by all parsing logic:

from typing import Mapping

def note_text(note: Mapping[str, object]) -> str:
    """
    Extract the human text from an FL3XX airport note.

    FL3XX uses the 'note' field as the actual text body.

    Fallback to 'body' or 'title' for robustness.
    """
    for key in ("note", "body", "title", "category", "type"):
        value = note.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

5. Splitting Customs vs Operational Notes

We route notes into two buckets:

Customs notes → parse_customs_notes

Operational notes → parse_operational_restrictions

5.1 Customs Note Keywords
CUSTOMS_NOTE_KEYWORDS = (
    "customs",
    "clearing customs",
    "clear customs",
    "landing rights",
    "landing right",
    "canpass",
    "aoe",
    "cbsa",
    "cbp",
    "ap is",
    "apis",
    "eapis",
    "e-apis",
    "border",
)

5.2 Split Function
from typing import Tuple, List, Mapping

def split_customs_operational_notes(
    notes: List[Mapping[str, object]]
) -> Tuple[List[str], List[str]]:
    """
    Split FL3XX airport notes into customs-related and general operational notes.

    Returns:
        (customs_texts, operational_texts)
        where each list contains raw text strings.
    """
    customs_texts: List[str] = []
    op_texts: List[str] = []

    for n in notes:
        text = note_text(n)
        if not text:
            continue

        lower = text.lower()
        is_customs = any(kw in lower for kw in CUSTOMS_NOTE_KEYWORDS)

        if is_customs:
            customs_texts.append(text)
        else:
            op_texts.append(text)

    return customs_texts, op_texts

6. Data Models (TypedDicts)

Use these as the standard schema for parsed outputs.

6.1 ParsedCustoms
from typing import TypedDict, List, Optional

class ParsedCustoms(TypedDict):
    customs_available: bool
    customs_hours: List[dict]          # [{"start": "0800", "end": "1700", "days": ["Mon-Fri"]}]
    customs_afterhours_available: bool
    customs_afterhours_requirements: List[str]
    customs_prior_notice_hours: Optional[int]
    customs_prior_notice_days: Optional[int]
    customs_contact_required: bool
    customs_contact_notes: List[str]

    canpass_only: bool
    canpass_notes: List[str]

    location_to_clear: Optional[str]
    location_notes: List[str]

    pax_requirements: List[str]
    crew_requirements: List[str]

    general_customs_notes: List[str]
    raw_notes: List[str]

6.2 ParsedRestrictions
class ParsedRestrictions(TypedDict):
    winter_sensitivity: bool
    winter_notes: List[str]

    deice_limited: bool
    deice_unavailable: bool
    deice_notes: List[str]

    fuel_available: Optional[bool]
    fuel_notes: List[str]

    ppr_required: bool
    ppr_lead_days: Optional[int]
    ppr_lead_hours: Optional[int]
    ppr_notes: List[str]

    slot_required: bool
    slot_lead_days: Optional[int]
    slot_lead_hours: Optional[int]
    slot_validity_minutes: Optional[int]
    slot_time_windows: List[dict]      # e.g. [{"days": ["Mon-Fri"], "start": "0600", "end": "2200"}]
    slot_notes: List[str]

    hours_of_operation: List[dict]     # [{"days":[...], "start":"HHMM", "end":"HHMM"}]
    night_ops_allowed: Optional[bool]
    curfew: Optional[dict]             # {"from": "HHMM" or "SUNSET+0:30", "to": "HHMM"}
    hour_notes: List[str]

    runway_limitations: List[str]
    aircraft_type_limits: List[str]

    generic_restrictions: List[str]
    raw_notes: List[str]

6.3 Initializers
def empty_parsed_customs() -> ParsedCustoms:
    return {
        "customs_available": False,
        "customs_hours": [],
        "customs_afterhours_available": False,
        "customs_afterhours_requirements": [],
        "customs_prior_notice_hours": None,
        "customs_prior_notice_days": None,
        "customs_contact_required": False,
        "customs_contact_notes": [],

        "canpass_only": False,
        "canpass_notes": [],

        "location_to_clear": None,
        "location_notes": [],

        "pax_requirements": [],
        "crew_requirements": [],

        "general_customs_notes": [],
        "raw_notes": [],
    }


def empty_parsed_restrictions() -> ParsedRestrictions:
    return {
        "winter_sensitivity": False,
        "winter_notes": [],

        "deice_limited": False,
        "deice_unavailable": False,
        "deice_notes": [],

        "fuel_available": None,
        "fuel_notes": [],

        "ppr_required": False,
        "ppr_lead_days": None,
        "ppr_lead_hours": None,
        "ppr_notes": [],

        "slot_required": False,
        "slot_lead_days": None,
        "slot_lead_hours": None,
        "slot_validity_minutes": None,
        "slot_time_windows": [],
        "slot_notes": [],

        "hours_of_operation": [],
        "night_ops_allowed": None,
        "curfew": None,
        "hour_notes": [],

        "runway_limitations": [],
        "aircraft_type_limits": [],

        "generic_restrictions": [],
        "raw_notes": [],
    }

7. Customs Parser
7.1 Regex & Helpers
import re

HOURS_RE = r"(\d{3,4})\s*[-–]\s*(\d{3,4})"
PRIOR_HOURS_RE = r"(\d+)\s*(?:hours|hrs)\s*(?:notice|prior)"
PRIOR_DAYS_RE  = r"(\d+)\s*(?:days?)\s*(?:notice|prior)"
LOCATION_RE    = r"(?:location:|clear at|report to|proceed to)\s*([A-Za-z0-9\-\s]+)"

7.2 Core Function
from typing import List

def parse_customs_notes(notes: List[str]) -> ParsedCustoms:
    """
    Parse a list of customs-related note texts into structured data.
    """
    c = empty_parsed_customs()

    for text in notes:
        lower = text.lower()
        c["raw_notes"].append(text)

        # If ANY customs note exists, we consider customs_available True
        c["customs_available"] = True

        # CANPASS airports
        if "canpass" in lower:
            c["canpass_only"] = True
            c["canpass_notes"].append(text)

        # Hours of operation
        if "hours of operation" in lower or "hours:" in lower:
            if match := re.search(HOURS_RE, lower):
                start, end = match.groups()
                c["customs_hours"].append({
                    "start": start,
                    "end": end,
                    "days": ["unknown"],  # can refine if weekdays are detected
                })

        # Afterhours
        if "after hours" in lower or "afterhours" in lower or "weekend/after hours" in lower:
            c["customs_afterhours_available"] = True
            c["customs_afterhours_requirements"].append(text)

        # Prior notice
        if match := re.search(PRIOR_HOURS_RE, lower):
            c["customs_prior_notice_hours"] = int(match.group(1))
        if match := re.search(PRIOR_DAYS_RE, lower):
            c["customs_prior_notice_days"] = int(match.group(1))

        # Contact requirement
        if any(k in lower for k in ("call", "phone", "contact", "notify")):
            c["customs_contact_required"] = True
            c["customs_contact_notes"].append(text)

        # Clearance location
        if match := re.search(LOCATION_RE, lower):
            c["location_to_clear"] = match.group(1).strip()
            c["location_notes"].append(text)

        # Pax / crew requirements
        if "pax" in lower or "passenger" in lower:
            c["pax_requirements"].append(text)
        if "crew" in lower:
            c["crew_requirements"].append(text)

        # Everything is still customs-related
        c["general_customs_notes"].append(text)

    return c

7.3 KPSP Example (Expected Parsed Output)

Given the notes you showed for KPSP (including the big CUSTOMS INFORMATION block), parse_customs_notes should produce something like:

{
  "customs_available": True,
  "customs_hours": [{"start": "0800", "end": "1700", "days": ["unknown"]}],
  "customs_afterhours_available": True,
  "customs_prior_notice_hours": 4,
  "customs_prior_notice_days": None,
  "customs_contact_required": True,
  "location_to_clear": "space between terminal and signature",
  ...
}

8. Operational Restrictions Parser
8.1 Classification Keywords
SLOT_KEYWORDS = ["slot", "slots", "reservation", "reservations", "oc s", "ecvrs"]
PPR_TRUE_PATTERNS = [
    r"\bppr\b",
    r"prior permission required",
    r"prior approval required",
    r"private airport\b",
]
WINTER_KEYWORDS = ["winter", "snow", "ice", "contaminated", "limited winter maintenance", "rwy contamination"]
DEICE_KEYWORDS = ["deice", "de-ice", "anti ice", "anti-ice"]
FUEL_KEYWORDS = ["fuel not available", "no fuel", "fuel unavailable"]
NIGHT_KEYWORDS = ["night ops", "no night ops", "night operations", "night landings", "curfew", "sunset"]
RUNWAY_KEYWORDS = ["rwy", "runway", "landings", "departures"]

8.2 Note Classification
from typing import Set

def is_ppr_note(text: str) -> bool:
    lower = text.lower()
    return any(re.search(pat, lower) for pat in PPR_TRUE_PATTERNS)


def classify_note(text: str) -> Set[str]:
    categories: Set[str] = set()
    lower = text.lower()

    if any(k in lower for k in SLOT_KEYWORDS):
        categories.add("slot")

    if is_ppr_note(text):
        categories.add("ppr")

    if any(k in lower for k in WINTER_KEYWORDS):
        categories.add("winter")

    if any(k in lower for k in DEICE_KEYWORDS):
        categories.add("deice")

    if any(k in lower for k in FUEL_KEYWORDS):
        categories.add("fuel")

    if any(k in lower for k in NIGHT_KEYWORDS):
        categories.add("night")

    if any(k in lower for k in RUNWAY_KEYWORDS):
        categories.add("runway")

    return categories


Anti-false-positive note:
Do NOT look for generic “prior to departure / prior to pushback / prior to de-ice” when deciding PPR. Only use patterns in PPR_TRUE_PATTERNS.

8.3 Regex for Slots / Hours / Validity
SLOT_DAYS_OUT_RE = r"(\d+)\s*days?\s*out"
SLOT_HOURS_RE    = r"(\d+)\s*(?:h|hrs|hours)\s*(?:before|prior)"
SLOT_VALIDITY_RE = r"\+/-\s*(\d+)\s*min"
WITHIN_HOUR_RE   = r"within the hour"

CLOSED_BETWEEN_RE = r"closed between\s*(\d{3,4})[-–](\d{3,4})"
RUNWAY_NUM_RE     = r"rwy\s*(\d{2}[lrc]?)"
ACFT_LIMIT_RE     = r"(embraer|cj2|cj3|legacy|challenger|global)"

8.4 Extractors
8.4.1 Slot Details
def extract_slot_details(note: str, out: ParsedRestrictions) -> None:
    lower = note.lower()
    out["slot_required"] = True

    if match := re.search(SLOT_DAYS_OUT_RE, lower):
        days = int(match.group(1))
        if out["slot_lead_days"] is None or days > out["slot_lead_days"]:
            out["slot_lead_days"] = days

    if match := re.search(SLOT_HOURS_RE, lower):
        hours = int(match.group(1))
        if out["slot_lead_hours"] is None or hours > out["slot_lead_hours"]:
            out["slot_lead_hours"] = hours

    if match := re.search(SLOT_VALIDITY_RE, lower):
        out["slot_validity_minutes"] = int(match.group(1))

    if re.search(WITHIN_HOUR_RE, lower):
        out["slot_validity_minutes"] = 60

    out["slot_notes"].append(note)

8.4.2 PPR Details
def extract_ppr_details(note: str, out: ParsedRestrictions) -> None:
    out["ppr_required"] = True
    out["ppr_notes"].append(note)

8.4.3 Winter / Deice
def extract_winter_details(note: str, out: ParsedRestrictions) -> None:
    out["winter_sensitivity"] = True
    out["winter_notes"].append(note)


def extract_deice_details(note: str, out: ParsedRestrictions) -> None:
    lower = note.lower()

    if "not available" in lower or "no deice" in lower or "no de-ice" in lower:
        out["deice_unavailable"] = True

    if "limited" in lower:
        out["deice_limited"] = True

    out["deice_notes"].append(note)

8.4.4 Fuel
def extract_fuel_details(note: str, out: ParsedRestrictions) -> None:
    lower = note.lower()
    if "fuel not available" in lower or "fuel unavailable" in lower or "no fuel" in lower:
        out["fuel_available"] = False
    out["fuel_notes"].append(note)

8.4.5 Night / Hours / Curfew
def extract_night_details(note: str, out: ParsedRestrictions) -> None:
    lower = note.lower()

    if out["night_ops_allowed"] is None:
        out["night_ops_allowed"] = True

    if ("day operations only" in lower
        or "no night ops" in lower
        or "night landings prohibited" in lower):
        out["night_ops_allowed"] = False

    if match := re.search(CLOSED_BETWEEN_RE, lower):
        start, end = match.groups()
        out["hours_of_operation"].append({"closed_from": start, "closed_to": end})

    if "curfew" in lower:
        out["curfew"] = {"raw": note}

    out["hour_notes"].append(note)

8.4.6 Runway / Aircraft Limits
def extract_runway_details(note: str, out: ParsedRestrictions) -> None:
    if re.search(RUNWAY_NUM_RE, note.lower()):
        out["runway_limitations"].append(note)


def extract_aircraft_limits(note: str, out: ParsedRestrictions) -> None:
    lower = note.lower()
    if re.search(ACFT_LIMIT_RE, lower) and ("only" in lower or "restricted" in lower):
        out["aircraft_type_limits"].append(note)

8.4.7 Generic Restrictions
def extract_generic(note: str, out: ParsedRestrictions) -> None:
    if len(note.split()) > 3:
        out["generic_restrictions"].append(note)

8.5 Main Parser
from typing import List

def parse_operational_restrictions(notes: List[str]) -> ParsedRestrictions:
    out = empty_parsed_restrictions()

    for raw in notes:
        text = raw.strip()
        if not text:
            continue

        out["raw_notes"].append(text)
        categories = classify_note(text)

        if "slot" in categories:
            extract_slot_details(text, out)

        if "ppr" in categories:
            extract_ppr_details(text, out)

        if "winter" in categories:
            extract_winter_details(text, out)

        if "deice" in categories:
            extract_deice_details(text, out)

        if "fuel" in categories:
            extract_fuel_details(text, out)

        if "night" in categories:
            extract_night_details(text, out)

        if "runway" in categories:
            extract_runway_details(text, out)
            extract_aircraft_limits(text, out)

        if len(categories) == 0:
            extract_generic(text, out)

    return out

9. Integration in airport_module.evaluate_airport_feasibility_for_leg
9.1 Pseudocode Patch

Inside evaluate_airport_feasibility_for_leg(...) (or equivalent), for each side (departure and arrival):

# 1. Fetch raw notes from FL3XX
dep_notes_raw = fetch_airport_notes(dep_icao)
arr_notes_raw = fetch_airport_notes(arr_icao)

# 2. Split into customs vs operational
dep_customs_texts, dep_op_texts = split_customs_operational_notes(dep_notes_raw)
arr_customs_texts, arr_op_texts = split_customs_operational_notes(arr_notes_raw)

# 3. Parse
dep_parsed_customs = parse_customs_notes(dep_customs_texts)
dep_parsed_restrictions = parse_operational_restrictions(dep_op_texts)

arr_parsed_customs = parse_customs_notes(arr_customs_texts)
arr_parsed_restrictions = parse_operational_restrictions(arr_op_texts)


Then pass dep_parsed_customs, dep_parsed_restrictions, arr_parsed_customs, arr_parsed_restrictions into your per-category evaluators:

evaluate_customs_side(...)

evaluate_deice_side(...)

evaluate_slot_ppr_side(...)

evaluate_operational_notes_side(...)

and include them in the result dict:

"parsed_customs_notes": dep_parsed_customs,
"parsed_operational_restrictions": dep_parsed_restrictions,
"raw_operational_notes": [note_text(n) for n in dep_notes_raw],


(and analogously for arrival).

9.2 Customs Evaluation Example
def evaluate_customs_side(icao: str, parsed: ParsedCustoms, is_departure: bool) -> dict:
    if is_departure:
        # Typical: no customs required for departure, unless departure country rules apply.
        return {
            "status": "PASS",
            "summary": "Not required for departure",
            "issues": [],
        }

    # Arrival side:
    if not parsed["customs_available"]:
        return {
            "status": "CAUTION",
            "summary": "Unknown customs availability — no notes found.",
            "issues": [],
        }

    issues = []
    summary_parts = []

    if parsed["customs_hours"]:
        h = parsed["customs_hours"][0]  # first block only for now
        summary_parts.append(f"Customs hours {h['start']}-{h['end']} (local)")

    if parsed["customs_afterhours_available"]:
        summary_parts.append("after-hours possible")

    if parsed["customs_prior_notice_hours"]:
        summary_parts.append(f"{parsed['customs_prior_notice_hours']}h prior notice required")

    summary = "Customs available" if summary_parts == [] else "Customs available — " + ", ".join(summary_parts)

    # Additional issues: crew/pax requirements, etc.
    for note in parsed["crew_requirements"]:
        issues.append(f"Crew: {note}")
    for note in parsed["pax_requirements"]:
        issues.append(f"Passengers: {note}")

    if parsed["location_to_clear"]:
        issues.append(f"Clear customs at: {parsed['location_to_clear']}")

    return {
        "status": "PASS",
        "summary": summary,
        "issues": issues,
    }

9.3 Deice Evaluation Example (Arrival)
def evaluate_deice_side(icao: str, parsed: ParsedRestrictions, is_departure: bool) -> dict:
    issues = []

    if parsed["deice_unavailable"]:
        issues.append("Deice/Anti-ice explicitly NOT available in notes.")
        return {
            "status": "CAUTION",
            "summary": "Deice NOT available — confirm no deicing required.",
            "issues": issues,
        }

    if parsed["deice_notes"]:
        return {
            "status": "PASS",
            "summary": "Deice information found in notes.",
            "issues": parsed["deice_notes"],
        }

    return {
        "status": "PASS",
        "summary": "Unknown deice status — no deice notes found.",
        "issues": ["No deice intel available; confirm if icing conditions likely."],
    }

10. UI Guidelines (Streamlit)
10.1 Status Icons
def status_icon(status: str) -> str:
    return {
        "PASS": "✅",
        "CAUTION": "⚠️",
        "FAIL": "❌",
    }.get(status, "❔")

10.2 Leg Layout

Inside the “Leg 1: CYEG → KPSP” expander:

Departure CYEG
  Suitability: ✅ Airport approved
  Deice: ✅ Deice available (T1/T4, Polar Guard, notice to FBO)
  Customs: ✅ Not required for departure
  Slot / PPR: ✅ None
  Other Notes: ...

Arrival KPSP
  Suitability: ✅ Airport approved
  Deice: ⚠️ Deice NOT available — confirm no deicing required.
  Customs: ✅ Customs available — 0800–1700, after-hours with 4h notice, day-prior confirmation.
    • Clear at customs ramp between terminal and Signature.
    • Unaccompanied minor: printed parental consent required.
    • Animals: printed vaccination records required.
    • Do not refile EAPIS solely for time changes.
  Slot / PPR: ✅ None
  Other Notes: ...

11. Test Cases (Sanity)
11.1 KPSP (Your Sample)

Input notes (as you supplied in JSON):

Must result in:

customs_available = True

customs_hours non-empty

customs_prior_notice_hours = 4

customs_afterhours_available = True

deice_unavailable = True

slot_required = False

ppr_required = False

11.2 CYEG

Deice notes talk about:

CDF with Type I/IV

30 minutes prior to departure contact de-ice

Must result in:

deice_unavailable = False

deice_notes non-empty

ppr_required = False (ensure no PPR false positive from “prior to departure”).
