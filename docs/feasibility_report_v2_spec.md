Feasibility Report v2 — Logic + UI Spec (for Codex)

Last updated: 2025-11-18
Scope: Fix incorrect parsing & unreadable output in the Quote Feasibility tool.

0. High-Level Goals

Correctness

Detect customs correctly (KPSP is a full customs port).

Detect deice “NOT AVAILABLE” at KPSP.

Avoid false PPR detection at CYEG.

Avoid “No operational notes” when notes exist.

Stop auto-assuming “SSA ⇒ Jeppesen required”.

Readability

Replace walls of text with structured sections.

Use clear PASS / CAUTION / FAIL indicators.

Separate Departure vs Arrival.

Summarize; raw notes only in an expander.

1. Data Flow Overview

Input

quote from /api/external/quote/{id}

airport_notes[icao] from /api/external/airports/{icao}/operationalNotes

Parsing layer

parse_customs_notes(notes) -> ParsedCustoms

parse_operational_restrictions(notes) -> ParsedRestrictions

Feasibility logic

evaluate_airport_side(...) -> AirportSideResult

evaluate_duty_day(...) -> DutyFeasibilityResult

UI layer (Streamlit)

Takes LegFeasibilityResult + DutyFeasibilityResult

Renders DM-friendly report.

This spec changes parsers and UI, not your FL3XX calls.

2. Parser Fixes
2.1 Note Text Extraction (critical bug)

Replace the existing _note_text helper with:

def _note_text(note: Mapping[str, object]) -> str:
    """
    Extract the human text from an FL3XX airport note.
    FL3XX uses the 'note' field as the actual text body.
    """
    # Primary: 'note'
    value = note.get("note")
    if isinstance(value, str) and value.strip():
        return value.strip()

    # Fallback legacy keys (just in case)
    for key in ("title", "body", "category", "type"):
        v = note.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""


All parsing functions must use _note_text(note) to obtain the text.

2.2 Splitting Customs vs Operational Notes

Create (or update) split_customs_operational_notes:

CUSTOMS_NOTE_KEYWORDS = (
    "customs",
    "canpass",
    "aoe",
    "cbsa",
    "cbp",
    "eapis",
    "e-apis",
    "ap is",       # catch bad spacing
    "landing rights",
    "clear customs",
    "clearing customs",
    "customs information",
    "customs procedure",
)

def split_customs_operational_notes(
    notes: list[Mapping[str, object]]
) -> tuple[list[str], list[str]]:
    """
    Return (customs_notes, operational_notes) as lists of text strings.
    A note can belong to both; for now we route by keyword in the full text.
    """
    customs_notes: list[str] = []
    op_notes: list[str] = []

    for n in notes:
        text = _note_text(n)
        if not text:
            continue

        lower = text.lower()
        is_customs = any(kw in lower for kw in CUSTOMS_NOTE_KEYWORDS)

        if is_customs:
            customs_notes.append(text)
        else:
            op_notes.append(text)

    return customs_notes, op_notes


This guarantees that the big KPSP note containing “CUSTOMS INFORMATION” and “CUSTOMS PROCEDURE” is classified as a customs note.

2.3 Customs Parser — Minimal but Reliable

Replace the current parse_customs_notes with:

HOURS_RE = r"(\d{3,4})[-–](\d{3,4})"
PRIOR_HOURS_RE = r"(\d+)\s*(?:hours|hrs)\s*(?:notice|prior)"
PRIOR_DAYS_RE  = r"(\d+)\s*(?:days?)\s*(?:notice|prior)"
LOCATION_RE    = r"(?:location:|clear at|report to|proceed to)\s*([A-Za-z0-9\-\s]+)"

def parse_customs_notes(notes: list[str]) -> ParsedCustoms:
    c = _empty_customs()

    for text in notes:
        lower = text.lower()
        c["raw_notes"].append(text)

        if "customs" in lower or "clearing customs" in lower:
            c["customs_available"] = True

        # CANPASS
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
                    "days": ["unknown"],  # can refine later
                })

        # Afterhours
        if "after hours" in lower or "afterhours" in lower:
            c["customs_afterhours_available"] = True
            c["customs_afterhours_requirements"].append(text)

        # Prior notice
        if match := re.search(PRIOR_HOURS_RE, lower):
            c["customs_prior_notice_hours"] = int(match.group(1))
        if match := re.search(PRIOR_DAYS_RE, lower):
            c["customs_prior_notice_days"] = int(match.group(1))

        # Contact / phone / notify
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

        c["general_customs_notes"].append(text)

    return c


Expected result for KPSP using the example notes:

customs_available = True

customs_hours = [{"start":"0800","end":"1700","days":["unknown"]}]

customs_afterhours_available = True

customs_prior_notice_hours = 4

location_to_clear set to something like "space between terminal and signature".

2.4 Operational Restrictions Parser Fixes
2.4.1 Deice “NOT AVAILABLE”

Inside extract_deice_details (or equivalent):

def extract_deice_details(note: str, out: ParsedRestrictions):
    lower = note.lower()

    if "not available" in lower or "no deice" in lower or "no de-ice" in lower:
        out["deice_unavailable"] = True

    if "limited" in lower:
        out["deice_limited"] = True

    out["deice_notes"].append(note)


For the KPSP note:

DE-ICE/ANTI ICE:
• Not available


→ deice_unavailable = True
→ Arrival KPSP deice status should not be “Unknown”; it should be “Not available (CAUTION/FAIL)”.

2.4.2 PPR False Positives

Current logic is likely matching phrases like “prior to departure”.
We want to only match true PPR patterns.

Define:

PPR_TRUE_PATTERNS = [
    r"\bppr\b",
    r"prior permission required",
    r"prior approval required",
    r"private airport\b",
]

def is_ppr_note(text: str) -> bool:
    lower = text.lower()
    return any(re.search(pat, lower) for pat in PPR_TRUE_PATTERNS)


In the classification & extraction:

if is_ppr_note(text):
    categories.add("ppr")


Do not treat generic “prior to departure” / “prior to pushback” phrases as PPR.

2.4.3 Operational Notes “none” bug

When building the AirportSideResult, only output:

“No operational notes (PASS)”

if both:

parsed_restrictions.generic_restrictions is empty AND

parsed_restrictions.runway_limitations, aircraft_type_limits, winter_notes, etc., are all empty AND

there are genuinely no raw notes for that airport.

Otherwise, present at least one bullet summary.

3. Jeppesen / OSA / SSA Logic

Do not automatically mark every SSA leg as “Jeppesen required”.

Introduce a simple profile:

class OsaSsaProfile(TypedDict):
    region: str          # "DOMESTIC", "SSA", "OSA"
    requires_jeppesen: bool


For CYEG and KPSP:

# Example; adjust to your real policy
profiles = {
    "CYEG": {"region": "CANADA_DOMESTIC", "requires_jeppesen": False},
    "KPSP": {"region": "SSA", "requires_jeppesen": False},
}


Then:

def evaluate_osa_ssa(
    departure_profile: OsaSsaProfile,
    arrival_profile: OsaSsaProfile
) -> CategoryResult:
    # region logic as you already had, but:
    requires_jepp = departure_profile["requires_jeppesen"] or arrival_profile["requires_jeppesen"]

    if requires_jepp:
        return CategoryResult(
            status="CAUTION",
            summary="SSA/OSA routing — Jeppesen ITP required.",
            issues=["Jeppesen ITP task must be created for this leg."],
        )
    else:
        return CategoryResult(
            status="PASS",
            summary=f"Routing classified as {arrival_profile['region']}. Jeppesen not required by profile.",
            issues=[],
        )


This prevents automatic Jeppesen CAUTION on CYEG–KPSP.

4. UI / Report Layout Spec (Streamlit)
4.1 Overall Structure

Use:

st.subheader("Key Issues") + bullet list.

st.expander("Duty Day Evaluation")

One st.expander per leg: "Leg 1: CYEG → KPSP"

Inside each leg expander:

Departure CYEG
  - Suitability
  - Deice
  - Customs
  - Slot / PPR
  - OSA / SSA
  - Overflight
  - Other Operational Notes

Arrival KPSP
  (same sections)

4.2 Status Indicators

Use emojis for clarity:

✅ PASS

⚠️ CAUTION

❌ FAIL

Example:

def status_icon(status: str) -> str:
    return {"PASS": "✅", "CAUTION": "⚠️", "FAIL": "❌"}.get(status, "❔")


Then in UI:

st.markdown(f"**Suitability:** {status_icon(res['suitability']['status'])} {res['suitability']['summary']}")

4.3 Bullet Summaries, Not Paragraphs

For each section, show:

A one-line summary.

Optional st.expander("Details") containing bullet list from issues.

Example for KPSP customs:

st.markdown(f"**Customs:** {status_icon(customs.status)} {customs.summary}")
with st.expander("Customs details", expanded=False):
    for issue in customs.issues:
        st.markdown(f"- {issue}")


Where customs.summary might be:

“Customs available 0800–1700 Mon–Fri, after-hours possible with 4h notice (day prior only).”

And customs.issues might include:

“Clear at customs ramp between terminal and Signature.”

“Unaccompanied minors require printed parental consent.”

“Animals require printed vaccination records.”

“Do not refile EAPIS solely for time changes.”

4.4 Key Issues Header

Before all expanders, compute a short list of the most important items:

Any FAILs.

Any CAUTIONs on customs, deice, duty, or permits.

Show them as:

st.subheader("Key Issues")
for issue in top_issues:
    st.markdown(f"- {issue}")


Example for CYEG–KPSP:

“Deice NOT available at KPSP — confirm no deicing required.”

“KPSP customs after-hours only confirmed day prior; requires 4h notice.”

5. Expected Output for CYEG → KPSP (Sanity Check)

With this spec implemented, CYEG → KPSP should render:

Duty Day: PASS (short duty)

Departure CYEG:

Suitability: ✅ PASS

Deice: ✅ Available (Type I & IV)

Customs: ✅ Not required (domestic departure)

Slot/PPR: ✅ None

OSA/SSA: ✅ Domestic

Operational notes: “Minimum 1h turn” if applicable

Arrival KPSP:

Suitability: ✅ PASS

Deice: ❌ Not available (or ⚠ depending on your policy)

Customs: ✅ Available; 0800–1700 M–F; after-hours with 4h notice, day-prior confirmation.

Slot/PPR: ✅ None

OSA/SSA: ✅ SSA, Jeppesen not required (per profile)

Operational notes: noise, turn time, customs ramp, etc.

And never:

“No operational notes”

“Customs service type: US” as the only info

“Unknown deice status” for KPSP

“PPR required for CYEG” unless there is a real PPR note.
