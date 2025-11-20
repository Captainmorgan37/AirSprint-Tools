📘 Airport Operational Notes & Customs Parser — Full Specification (Codex-Ready)

Last updated: 2025-11-18
Author: ChatGPT (for IOCC Tools)

TABLE OF CONTENTS

Airport Operational Notes Parser
1.1 Purpose
1.2 ParsedRestrictions TypedDict
1.3 Classification Keywords
1.4 Category Extractors
1.5 Parser Control Flow
1.6 Default Initialization

Customs Parser
2.1 Purpose
2.2 ParsedCustoms TypedDict
2.3 Classification Categories
2.4 Regex Extraction Rules
2.5 Parser Control Flow
2.6 Default Initialization

Integration Notes

Future Enhancements

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. AIRPORT OPERATIONAL NOTES PARSER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1.1 Purpose

This parser extracts operational constraints from FL3XX airport operational notes, including:

Slot requirements

PPR requirements

Hours / curfews / night ops

Deice limitations

Fuel availability

Winter sensitivity

Runway or approach restrictions

Aircraft-type limitations

General operational notes

These notes come from:

GET /api/external/airports/{icao}/operationalNotes?from=YYYY-MM-DD&to=YYYY-MM-DD

1.2 ParsedRestrictions TypedDict
class ParsedRestrictions(TypedDict):
    winter_sensitivity: bool
    winter_notes: list[str]

    deice_limited: bool
    deice_unavailable: bool
    deice_notes: list[str]

    fuel_available: bool | None
    fuel_notes: list[str]

    ppr_required: bool
    ppr_lead_days: int | None
    ppr_lead_hours: int | None
    ppr_notes: list[str]

    slot_required: bool
    slot_lead_days: int | None
    slot_lead_hours: int | None
    slot_validity_minutes: int | None
    slot_time_windows: list[dict]   # [{"days": [...], "start":"HHMM", "end":"HHMM"}]
    slot_notes: list[str]

    hours_of_operation: list[dict]  # [{"days":[...],"start":"HHMM","end":"HHMM"}]
    night_ops_allowed: bool | None
    curfew: dict | None             # {"from": "...", "to": "..."}
    hour_notes: list[str]

    runway_limitations: list[str]
    aircraft_type_limits: list[str]

    generic_restrictions: list[str]
    raw_notes: list[str]

1.3 Classification Keywords
SLOT_KEYWORDS = [
    "slot", "slots", "reservation", "reservations", "ecvrs", "ocs"
]

PPR_KEYWORDS = [
    "ppr", "prior permission", "private airport"
]

WINTER_KEYWORDS = [
    "winter", "snow", "ice", "contaminated", "rwy contamination",
    "limited winter maintenance", "deice", "antice"
]

FUEL_KEYWORDS = [
    "fuel not available", "fuel unavailable", "no fuel"
]

DEICE_KEYWORDS = [
    "deice", "antice", "de-ice"
]

NIGHT_KEYWORDS = [
    "night ops", "no night ops", "night operations",
    "night landings", "night landing", "curfew", "sunset"
]

RUNWAY_KEYWORDS = [
    "rwy", "runway", "landings", "departures"
]

1.4 Category Extractors
Slot extraction

Patterns:

SLOT_DAYS_OUT_RE = r"(\d+)\s*days?\s*out"
SLOT_HOURS_RE    = r"(\d+)\s*(?:h|hrs|hours)\s*(?:before|prior)"
SLOT_VALIDITY_RE = r"\+/-\s*(\d+)\s*min"
WITHIN_HOUR_RE   = r"within the hour"


Extractor:

def extract_slot_details(note: str, out: ParsedRestrictions):
    lower = note.lower()
    out["slot_required"] = True

    if match := re.search(SLOT_DAYS_OUT_RE, lower):
        out["slot_lead_days"] = max(out["slot_lead_days"] or 0, int(match.group(1)))

    if match := re.search(SLOT_HOURS_RE, lower):
        out["slot_lead_hours"] = max(out["slot_lead_hours"] or 0, int(match.group(1)))

    if match := re.search(SLOT_VALIDITY_RE, lower):
        out["slot_validity_minutes"] = int(match.group(1))

    if re.search(WITHIN_HOUR_RE, lower):
        out["slot_validity_minutes"] = 60

    out["slot_notes"].append(note)

PPR extraction
PPR_DAYS_OUT_RE  = r"(\d+)\s*days?\s*(?:notice|prior)"
PPR_HOURS_OUT_RE = r"(\d+)\s*(?:h|hrs|hours)\s*(?:notice|prior)"


Extractor:

def extract_ppr_details(note: str, out: ParsedRestrictions):
    lower = note.lower()
    out["ppr_required"] = True

    if match := re.search(PPR_DAYS_OUT_RE, lower):
        out["ppr_lead_days"] = int(match.group(1))

    if match := re.search(PPR_HOURS_OUT_RE, lower):
        out["ppr_lead_hours"] = int(match.group(1))

    out["ppr_notes"].append(note)

Winter / Deice
def extract_winter_details(note: str, out: ParsedRestrictions):
    out["winter_sensitivity"] = True
    out["winter_notes"].append(note)

def extract_deice_details(note: str, out: ParsedRestrictions):
    lower = note.lower()

    if "not available" in lower:
        out["deice_unavailable"] = True
    if "limited" in lower:
        out["deice_limited"] = True

    out["deice_notes"].append(note)

Fuel
def extract_fuel_details(note: str, out: ParsedRestrictions):
    lower = note.lower()
    if "fuel not available" in lower or "fuel unavailable" in lower or "no fuel" in lower:
        out["fuel_available"] = False

    out["fuel_notes"].append(note)

Night / Hours / Curfew
CLOSED_BETWEEN_RE = r"closed between\s*(\d{3,4})[-–](\d{3,4})"

def extract_night_details(note: str, out: ParsedRestrictions):
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

Runway / Aircraft Restrictions
RUNWAY_NUM_RE = r"rwy\s*(\d{2}[lrc]?)"
ACFT_LIMIT_RE = r"(embraer|cj2|cj3|legacy|challenger|global)"


Extractors:

def extract_runway_details(note: str, out: ParsedRestrictions):
    if re.search(RUNWAY_NUM_RE, note.lower()):
        out["runway_limitations"].append(note)

def extract_aircraft_limits(note: str, out: ParsedRestrictions):
    lower = note.lower()
    if re.search(ACFT_LIMIT_RE, lower) and ("only" in lower or "restricted" in lower):
        out["aircraft_type_limits"].append(note)

Generic notes
def extract_generic(note: str, out: ParsedRestrictions):
    if len(note.split()) > 3:
        out["generic_restrictions"].append(note)

1.5 Parser Control Flow
def parse_operational_restrictions(notes: list[str]) -> ParsedRestrictions:
    out = _empty_parsed_restrictions()

    for raw in notes:
        text = raw.strip()
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

1.6 Initialization Block
def _empty_parsed_restrictions() -> ParsedRestrictions:
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2. CUSTOMS PARSER MODULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2.1 Purpose

This parser extracts structured customs requirements from FL3XX operational notes, including:

Customs hours

Afterhours capability & fees

CANPASS status

Prior notice rules

Required documentation

Notes for crew

Special procedures

Examples come from your "Info for Parsing – Customs.docx" file.

2.2 ParsedCustoms TypedDict
class ParsedCustoms(TypedDict):
    customs_available: bool
    customs_hours: list[dict]        # [{"days":[...],"start":"HHMM","end":"HHMM"}]
    customs_afterhours_available: bool
    customs_afterhours_requirements: list[str]
    customs_prior_notice_hours: int | None
    customs_prior_notice_days: int | None
    customs_contact_required: bool
    customs_contact_notes: list[str]

    canpass_only: bool
    canpass_notes: list[str]

    location_to_clear: str | None
    location_notes: list[str]

    pax_requirements: list[str]
    crew_requirements: list[str]

    general_customs_notes: list[str]
    raw_notes: list[str]

2.3 Classification Categories
CANPASS

Keywords:

"canpass"

"aoe/canpass"

"canpass only"

Customs Hours

Patterns like:

“hours of operation: 0800-1600”

“open 0900–1700L”

“Customs available Mon–Fri 08:00–18:00”

Afterhours

Keywords:

"after hours"

"afterhours"

"after-hour"

"overtime"

"call out"

"fees apply"

Prior Notice

Patterns:

“x hours notice”

“x days notice”

“advise x hours before arrival”

Clearance Location

Keywords:

“clear at”

“customs located”

“report to”

“meet officer”

Pax/Crew Requirements

Keywords:

“must remain”

“passport”

“documents”

“crew must”

“pax must”

Contact Required

If any note includes:

“call”

“phone”

“contact”

“notify”

2.4 Regex Extraction Rules

Hours:

HOURS_RE = r"(\d{3,4})[-–](\d{3,4})"


Prior notice:

PRIOR_HOURS_RE = r"(\d+)\s*(?:hours|hrs)\s*(?:notice|prior)"
PRIOR_DAYS_RE  = r"(\d+)\s*(?:days?)\s*(?:notice|prior)"


Location:

LOCATION_RE = r"(clear at|report to|meet officer at|customs located at)\s*([A-Za-z0-9\-\s]+)"

2.5 Parser Control Flow
def parse_customs_notes(notes: list[str]) -> ParsedCustoms:
    out = _empty_customs()

    for raw in notes:
        text = raw.strip()
        lower = text.lower()
        out["raw_notes"].append(text)

        # Customs available
        if "customs" in lower:
            out["customs_available"] = True

        # CANPASS
        if "canpass" in lower:
            out["canpass_only"] = True
            out["canpass_notes"].append(text)

        # Hours
        if match := re.search(HOURS_RE, lower):
            start, end = match.groups()
            out["customs_hours"].append({"start": start, "end": end, "days": ["unknown"]})

        # Afterhours
        if "after" in lower and "hour" in lower:
            out["customs_afterhours_available"] = True
            out["customs_afterhours_requirements"].append(text)

        # Prior notice
        if match := re.search(PRIOR_HOURS_RE, lower):
            out["customs_prior_notice_hours"] = int(match.group(1))
        if match := re.search(PRIOR_DAYS_RE, lower):
            out["customs_prior_notice_days"] = int(match.group(1))

        # Contact requirement
        if "call" in lower or "phone" in lower or "contact" in lower or "notify" in lower:
            out["customs_contact_required"] = True
            out["customs_contact_notes"].append(text)

        # Clearance location
        if match := re.search(LOCATION_RE, lower):
            out["location_to_clear"] = match.group(2).strip()
            out["location_notes"].append(text)

        # Pax/Crew requirements
        if "pax" in lower or "passenger" in lower:
            out["pax_requirements"].append(text)
        if "crew" in lower:
            out["crew_requirements"].append(text)

        # Anything else is still customs-related:
        out["general_customs_notes"].append(text)

    return out

2.6 Default Initialization
def _empty_customs() -> ParsedCustoms:
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
3. INTEGRATION NOTES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Both parsers should be called within your Airport Feasibility Engine, in this order:

Pull airport operational notes

Split notes into:

Customs notes (anything containing “customs”, “canpass”, “aoe”, etc.)

Operational notes (everything else)

Feed them into:

parse_customs_notes()

parse_operational_restrictions()

Combine results for final feasibility scoring.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
4. FUTURE ENHANCEMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Replace time extraction with full day-of-week parsing

Add sunset calculation for curfews

Cross-match CANPASS requirements with pax passport data

Validate afterhours availability against ETA/ETD

Tie runway restrictions into performance / tail type

Deep integrate with your winter deice master database

Add risk scoring (Green/Yellow/Red)
