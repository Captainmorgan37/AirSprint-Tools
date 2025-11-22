"""Helpers for classifying and parsing airport operational notes."""

from __future__ import annotations

import re
from typing import Mapping, Sequence, Tuple, TypedDict

from .schemas import CategoryResult, CategoryStatus


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
    slot_time_windows: list[dict[str, object]]
    slot_notes: list[str]

    hours_of_operation: list[dict[str, object]]
    night_ops_allowed: bool | None
    curfew: dict[str, str] | None
    hour_notes: list[str]

    runway_limitations: list[str]
    aircraft_type_limits: list[str]
    surface_contamination: bool

    generic_restrictions: list[str]
    raw_notes: list[str]


class ParsedCustoms(TypedDict):
    customs_available: bool
    customs_hours: list[dict[str, object]]
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


_STATUS_PRIORITY: Mapping[CategoryStatus, int] = {
    "PASS": 0,
    "INFO": 1,
    "CAUTION": 2,
    "FAIL": 3,
}

SLOT_KEYWORDS = ("slot", "slots", "reservation", "reservations", "ecvrs", "ocs")
WINTER_KEYWORDS = (
    "winter",
    "snow",
    "ice",
    "contaminated",
    "rwy contamination",
    "limited winter maintenance",
)
FUEL_KEYWORDS = ("fuel not available", "fuel unavailable", "no fuel")
DEICE_KEYWORDS = ("deice", "antice", "de-ice")
NIGHT_KEYWORDS = ("night ops", "no night ops", "night operations", "night landings", "curfew", "sunset")
RUNWAY_KEYWORDS = ("rwy", "runway", "landings", "departures")
RUNWAY_RESTRICTION_TERMS = (
    "closed",
    "clsd",
    "closure",
    "not available",
    "unavailable",
    "restricted",
    "limitation",
)
RUNWAY_CONTAMINATION_TERMS = ("contaminated", "contamination")
CUSTOMS_NOTE_KEYWORDS = (
    "customs",
    "canpass",
    "aoe",
    "cbsa",
    "cbp",
    "eapis",
    "e-apis",
    "ap is",
    "landing rights",
    "clear customs",
    "clearing customs",
    "customs information",
    "customs procedure",
)
PPR_TRUE_PATTERNS = [
    r"\bppr\b",
    r"prior permission required",
    r"prior approval required",
    r"private airport\b",
]

SLOT_DAYS_OUT_RE = re.compile(r"(\d+)\s*days?\s*out")
SLOT_HOURS_RE = re.compile(r"(\d+)\s*(?:h|hrs|hours)\s*(?:before|prior)")
SLOT_VALIDITY_RE = re.compile(r"\+/-\s*(\d+)\s*min")
WITHIN_HOUR_RE = re.compile(r"within the hour")
PPR_DAYS_OUT_RE = re.compile(r"(\d+)\s*days?\s*(?:notice|prior)")
PPR_HOURS_OUT_RE = re.compile(r"(\d+)\s*(?:h|hrs|hours)\s*(?:notice|prior)")
CLOSED_BETWEEN_RE = re.compile(r"closed between\s*(\d{3,4})[-–](\d{3,4})")
RUNWAY_NUM_RE = re.compile(r"rwy\s*(\d{2}[lrc]?)", re.IGNORECASE)
ACFT_LIMIT_RE = re.compile(r"(embraer|cj2|cj3|legacy|challenger|global)", re.IGNORECASE)

HOURS_RE = re.compile(r"(\d{3,4})[-–](\d{3,4})")
PRIOR_HOURS_RE = re.compile(r"(\d+)\s*(?:hours|hrs)\s*(?:notice|prior)")
PRIOR_DAYS_RE = re.compile(r"(\d+)\s*(?:days?)\s*(?:notice|prior)")
LOCATION_RE = re.compile(
    r"(?:location:|clear at|report to|proceed to|meet officer at|customs located at)\s*([A-Za-z0-9\-\s]+)",
    re.IGNORECASE,
)


def _combine_status(existing: CategoryStatus, candidate: CategoryStatus) -> CategoryStatus:
    return candidate if _STATUS_PRIORITY[candidate] > _STATUS_PRIORITY[existing] else existing


def note_text(note: Mapping[str, object]) -> str:
    """Extract the human-readable text body from an FL3XX airport note."""

    value = note.get("note")
    if isinstance(value, str) and value.strip():
        return value.strip()

    for key in ("title", "body", "category", "type"):
        v = note.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""


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
        "surface_contamination": False,
        "generic_restrictions": [],
        "raw_notes": [],
    }


def _empty_parsed_customs() -> ParsedCustoms:
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


def _contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lower = text.lower()
    return any(keyword in lower for keyword in keywords)


def _has_weight_limitation(text: str) -> bool:
    lower = text.lower()
    if "weight" not in lower:
        return False
    return any(
        keyword in lower
        for keyword in (
            "limit",
            "restricted",
            "restriction",
            "max",
            "maximum",
            "mlw",
            "lbs",
            "ton",
        )
    )


def is_ppr_note(text: str) -> bool:
    lower = text.lower()
    return any(re.search(pattern, lower) for pattern in PPR_TRUE_PATTERNS)


def _classify_operational_note(note: str) -> set[str]:
    categories: set[str] = set()
    lower = note.lower()
    if _contains_keyword(lower, SLOT_KEYWORDS):
        categories.add("slot")
    if is_ppr_note(lower):
        categories.add("ppr")
    if _contains_keyword(lower, WINTER_KEYWORDS):
        categories.add("winter")
    if _contains_keyword(lower, DEICE_KEYWORDS):
        categories.add("deice")
    if _contains_keyword(lower, FUEL_KEYWORDS):
        categories.add("fuel")
    if _contains_keyword(lower, NIGHT_KEYWORDS):
        categories.add("night")
    if _contains_keyword(lower, RUNWAY_KEYWORDS):
        categories.add("runway")
    return categories


_CATEGORY_PRIORITY: tuple[str, ...] = (
    "slot",
    "ppr",
    "deice",
    "winter",
    "fuel",
    "night",
    "runway",
)


def _select_primary_category(categories: set[str]) -> str | None:
    for category in _CATEGORY_PRIORITY:
        if category in categories:
            return category
    return None


def split_customs_operational_notes(
    notes: Sequence[Mapping[str, object]]
) -> Tuple[list[str], list[str]]:
    customs: list[str] = []
    operational: list[str] = []
    for note in notes:
        text = note_text(note)
        if not text:
            continue
        lower = text.lower()
        is_customs = any(keyword in lower for keyword in CUSTOMS_NOTE_KEYWORDS)
        if is_customs:
            customs.append(text)
        else:
            operational.append(text)
    return customs, operational


def parse_operational_restrictions(notes: Sequence[str]) -> ParsedRestrictions:
    parsed = _empty_parsed_restrictions()
    for raw in notes:
        text = raw.strip()
        if not text:
            continue
        parsed["raw_notes"].append(text)
        lower = text.lower()
        categories = _classify_operational_note(text)
        primary = _select_primary_category(categories)
        if "contaminated" in lower:
            parsed["surface_contamination"] = True
        if "slot" in categories:
            _extract_slot_details(text, parsed, add_note=primary == "slot")
        if "ppr" in categories:
            _extract_ppr_details(text, parsed, add_note=primary == "ppr")
        if "winter" in categories:
            _extract_winter_details(text, parsed, add_note=primary == "winter")
        if "deice" in categories:
            _extract_deice_details(text, parsed, add_note=primary == "deice")
        if "fuel" in categories:
            _extract_fuel_details(text, parsed, add_note=primary == "fuel")
        if "night" in categories:
            _extract_night_details(text, parsed, add_note=primary == "night")
        if "runway" in categories:
            _extract_runway_details(text, parsed, add_note=primary == "runway")
            _extract_aircraft_limits(text, parsed, add_note=primary == "runway")
        if not categories:
            _extract_generic(text, parsed)
    return parsed


def _extract_slot_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    out["slot_required"] = True
    if match := SLOT_DAYS_OUT_RE.search(lower):
        value = int(match.group(1))
        current = out["slot_lead_days"] or 0
        out["slot_lead_days"] = max(current, value) or value
    if match := SLOT_HOURS_RE.search(lower):
        value = int(match.group(1))
        current = out["slot_lead_hours"] or 0
        out["slot_lead_hours"] = max(current, value) or value
    if match := SLOT_VALIDITY_RE.search(lower):
        out["slot_validity_minutes"] = int(match.group(1))
    if WITHIN_HOUR_RE.search(lower):
        out["slot_validity_minutes"] = 60
    if add_note:
        out["slot_notes"].append(note)


def _extract_ppr_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    out["ppr_required"] = True
    if match := PPR_DAYS_OUT_RE.search(lower):
        out["ppr_lead_days"] = int(match.group(1))
    if match := PPR_HOURS_OUT_RE.search(lower):
        out["ppr_lead_hours"] = int(match.group(1))
    if add_note:
        out["ppr_notes"].append(note)


def _extract_winter_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    out["winter_sensitivity"] = True
    if add_note:
        out["winter_notes"].append(note)


def _extract_deice_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if "not available" in lower or "no deice" in lower or "no de-ice" in lower:
        out["deice_unavailable"] = True
    if "limited" in lower:
        out["deice_limited"] = True
    if add_note:
        out["deice_notes"].append(note)


def _extract_fuel_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if any(keyword in lower for keyword in FUEL_KEYWORDS):
        out["fuel_available"] = False
    if add_note:
        out["fuel_notes"].append(note)


def _extract_night_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if out["night_ops_allowed"] is None:
        out["night_ops_allowed"] = True
    if (
        "day operations only" in lower
        or "no night ops" in lower
        or "night landings prohibited" in lower
    ):
        out["night_ops_allowed"] = False
    if match := CLOSED_BETWEEN_RE.search(lower):
        start, end = match.groups()
        out["hours_of_operation"].append({"closed_from": start, "closed_to": end})
    if "curfew" in lower:
        out["curfew"] = {"raw": note}
    if add_note:
        out["hour_notes"].append(note)


def _extract_runway_details(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    actionable = False
    if _contains_keyword(lower, RUNWAY_RESTRICTION_TERMS):
        actionable = True
    if _has_weight_limitation(lower):
        actionable = True
    if _contains_keyword(lower, RUNWAY_CONTAMINATION_TERMS):
        out["surface_contamination"] = True
        actionable = True
    if actionable and add_note:
        out["runway_limitations"].append(note)


def _extract_aircraft_limits(
    note: str, out: ParsedRestrictions, *, add_note: bool = True
) -> None:
    lower = note.lower()
    if ACFT_LIMIT_RE.search(lower) and ("only" in lower or "restricted" in lower):
        if add_note:
            out["aircraft_type_limits"].append(note)


def _extract_generic(note: str, out: ParsedRestrictions) -> None:
    if len(note.split()) > 3:
        out["generic_restrictions"].append(note)


DAY_KEYWORDS = (
    ("mon", "Mon"),
    ("tue", "Tue"),
    ("wed", "Wed"),
    ("thu", "Thu"),
    ("fri", "Fri"),
    ("sat", "Sat"),
    ("sun", "Sun"),
)


def _detect_days(lower: str) -> list[str]:
    days: list[str] = []
    for token, label in DAY_KEYWORDS:
        if token in lower:
            days.append(label)
    return days


def parse_customs_notes(notes: Sequence[str]) -> ParsedCustoms:
    parsed = _empty_parsed_customs()
    for raw in notes:
        text = raw.strip()
        if not text:
            continue
        lower = text.lower()
        parsed["raw_notes"].append(text)
        if "customs" in lower or "clearing customs" in lower:
            parsed["customs_available"] = True
        if "canpass" in lower:
            parsed["canpass_only"] = True
            parsed["canpass_notes"].append(text)
        for match in re.finditer(HOURS_RE, lower):
            # Skip phone-number style matches like 760-318-3880 where another
            # dash-and-digits segment immediately follows the match.
            trailing = lower[match.end() :]
            if re.match(r"\s*-\s*\d{2,4}", trailing):
                continue

            start, end = match.groups()
            days = _detect_days(lower)
            if not days:
                days = ["unknown"]
            parsed["customs_hours"].append({"start": start, "end": end, "days": days})
        if "after hours" in lower or "afterhours" in lower:
            parsed["customs_afterhours_available"] = True
            parsed["customs_afterhours_requirements"].append(text)
        if match := re.search(PRIOR_HOURS_RE, lower):
            parsed["customs_prior_notice_hours"] = int(match.group(1))
        if match := re.search(PRIOR_DAYS_RE, lower):
            parsed["customs_prior_notice_days"] = int(match.group(1))
        if any(k in lower for k in ("call", "phone", "contact", "notify")):
            parsed["customs_contact_required"] = True
            parsed["customs_contact_notes"].append(text)
        if match := re.search(LOCATION_RE, lower):
            parsed["location_to_clear"] = match.group(1).strip()
            parsed["location_notes"].append(text)
        if "pax" in lower or "passenger" in lower:
            parsed["pax_requirements"].append(text)
        if "crew" in lower:
            parsed["crew_requirements"].append(text)
        parsed["general_customs_notes"].append(text)
    return parsed


def summarize_operational_notes(
    icao: str,
    notes: Sequence[Mapping[str, object]],
    parsed_restrictions: ParsedRestrictions | None = None,
    parsed_customs: ParsedCustoms | None = None,
) -> CategoryResult:
    has_raw_notes = bool(notes)
    if parsed_restrictions and parsed_restrictions["raw_notes"]:
        has_raw_notes = True
    if parsed_customs and parsed_customs["raw_notes"]:
        has_raw_notes = True
    if not has_raw_notes:
        return CategoryResult(status="PASS", summary="No operational notes", issues=[])

    customs_texts: list[str]
    operational_texts: list[str]
    if parsed_restrictions is None or parsed_customs is None:
        customs_texts, operational_texts = split_customs_operational_notes(notes)
        if parsed_restrictions is None:
            parsed_restrictions = parse_operational_restrictions(operational_texts)
        if parsed_customs is None:
            parsed_customs = parse_customs_notes(customs_texts)
    status: CategoryStatus = "PASS"
    issues: list[str] = []

    def add_issue(message: str, severity: CategoryStatus = "INFO") -> None:
        nonlocal status
        issues.append(message)
        status = _combine_status(status, severity)

    restrictions = parsed_restrictions or _empty_parsed_restrictions()
    customs = parsed_customs or _empty_parsed_customs()

    if restrictions["slot_required"]:
        detail = "Slot required"
        if restrictions["slot_lead_days"]:
            detail += f" ({restrictions['slot_lead_days']} day lead)"
        elif restrictions["slot_lead_hours"]:
            detail += f" ({restrictions['slot_lead_hours']} hour lead)"
        add_issue(detail, "CAUTION")
    if restrictions["ppr_required"]:
        detail = "PPR required"
        if restrictions["ppr_lead_days"]:
            detail += f" ({restrictions['ppr_lead_days']} day notice)"
        elif restrictions["ppr_lead_hours"]:
            detail += f" ({restrictions['ppr_lead_hours']} hour notice)"
        add_issue(detail, "CAUTION")
    if restrictions["deice_unavailable"]:
        add_issue("Operational note: deice unavailable", "CAUTION")
        for note in restrictions["deice_notes"]:
            issues.append(f"Deice note: {note}")
    elif restrictions["deice_limited"]:
        add_issue("Operational note: deice limited", "CAUTION")
    if restrictions["fuel_available"] is False:
        add_issue("Fuel unavailable per operational notes", "CAUTION")
    if restrictions["night_ops_allowed"] is False:
        add_issue("Night operations prohibited", "CAUTION")
    if restrictions["curfew"]:
        add_issue("Curfew in effect per operational notes", "CAUTION")
    if restrictions["surface_contamination"]:
        add_issue("Surface contamination reported in operational notes", "CAUTION")
    for runway_note in restrictions["runway_limitations"]:
        add_issue(f"Runway restriction: {runway_note}", "CAUTION")
    for acft_note in restrictions["aircraft_type_limits"]:
        add_issue(f"Aircraft limit: {acft_note}", "CAUTION")

    if restrictions["winter_sensitivity"] and status == "PASS":
        add_issue("Winter operations sensitivity reported", "INFO")

    if customs["canpass_only"]:
        add_issue("Customs limited to CANPASS", "CAUTION")
    if customs["customs_prior_notice_hours"]:
        add_issue(
            f"Customs requires {customs['customs_prior_notice_hours']} hours notice",
            "CAUTION",
        )
    if customs["customs_prior_notice_days"]:
        add_issue(
            f"Customs requires {customs['customs_prior_notice_days']} day notice",
            "CAUTION",
        )
    if customs["customs_contact_required"]:
        add_issue("Customs contact required", "CAUTION")
    if customs["pax_requirements"]:
        add_issue("Passenger documentation requirements noted", "CAUTION")
    if customs["crew_requirements"]:
        add_issue("Crew documentation requirements noted", "CAUTION")

    summary = "Operational notes reviewed"
    if status == "CAUTION":
        summary = "Operational restrictions detected"
    elif status == "INFO":
        summary = "Operational notes available — no restrictions detected."

    if not issues and has_raw_notes:
        issues.append(summary)
        status = _combine_status(status, "INFO")
    elif status == "PASS" and has_raw_notes:
        status = "INFO"
        summary = "Operational notes available — no restrictions detected."
        if not issues:
            issues.append(summary)

    return CategoryResult(status=status, summary=summary, issues=issues)

