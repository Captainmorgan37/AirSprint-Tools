# HOTAC Coverage Monitor – Implementation Plan

## Goal
Build a Streamlit app that answers one operational question quickly for **today** or **tomorrow**:

> “For each pilot, at the end of their duty day, is there an active hotel (HOTAC) booking on the final leg?”

This should reuse the same “end-of-day” logic style used by the Flight Following Report so dispatch can trust the result.

---

## Recommended architecture

### 1) Inputs
- `target_date` selector limited to `today` and `tomorrow`.
- FL3XX API credentials from `st.secrets["fl3xx_api"]`.

### 2) Data pull sequence
1. Pull all flights for the target window from FL3XX flights endpoint.
2. Group and sort legs by tail (existing helper pattern in `flight_leg_utils.py`).
3. Determine each crew member’s **end-of-day leg** (see logic below).
4. For each end-of-day leg, call:
   - `GET /api/external/flight/<flightId>/services`
5. Inspect `arrivalHotac` (and optionally `departureHotac`) for active bookings tied to assigned pilots.

### 3) Output
Produce one row per pilot with:
- Pilot name
- Personnel/trigram (if available)
- Tail
- Flight number / flight ID
- End airport
- End-of-day leg ETD/ETA local
- HOTAC status (`Booked`, `Missing`, `Cancelled-only`, `Unknown`)
- Hotel company (if booked)
- Notes / reason code

---

## End-of-day determination logic (MVP)
Use a deterministic and auditable rule set:

1. Build ordered legs per tail for the selected duty day.
2. For each pilot seen on those legs:
   - Identify all legs where that pilot is assigned.
   - Select the **last chronological leg** for that pilot on that day.
3. That selected leg is the pilot’s “end-of-day leg”.

### Recommended tie-breakers
If timestamps are missing/identical:
1. Prefer later `arrTime`.
2. Else prefer later `depTime`.
3. Else fallback to leg order from FL3XX response.

### Why this works
- It is simple to explain.
- It aligns with practical dispatch use.
- It avoids overfitting to edge cases in v1.

---

## HOTAC interpretation rules
From the services payload, inspect `arrivalHotac` items.

### Match rule
A HOTAC record matches a pilot when:
- `arrivalHotac[].person.id == crew_member.id`

### Status mapping
- `status == "OK"` → `Booked`
- `status in {"CNL", "CANCELED", "CANCELLED"}` → `Cancelled-only` (unless another active record exists)
- No matching HOTAC record → `Missing`
- API error / malformed payload → `Unknown`

If multiple records exist for one pilot on a leg:
- Prefer any active (`OK`) record.
- Else if all are cancelled, mark `Cancelled-only`.

### Useful fields to show
- `hotacService.company`
- `hotacService.phone`
- `documents` present (boolean) for voucher sanity check

---

## UX proposal (Streamlit page)
Page name suggestion: `HOTAC Coverage Monitor`.

### Header metrics
- Pilots ending day
- Booked
- Missing
- Cancelled-only
- Unknown

### Main table filters
- Airport filter
- Status filter
- Tail filter

### Triage-first sort
Default sort by:
1. `Missing`
2. `Cancelled-only`
3. `Unknown`
4. `Booked`

This keeps action items at the top.

---

## Suggested module split
- `hotac_coverage.py`
  - Data collection + normalization
  - End-of-day computation
  - HOTAC status evaluation
  - Returns display DataFrame + troubleshooting DataFrame
- `pages/HOTAC Coverage Monitor.py`
  - Streamlit UI
  - Date picker (today/tomorrow)
  - Render metrics, table, troubleshooting expander

---

## Edge cases to handle explicitly
- Pilot appears on multiple tails in one day.
- Crew swap mid-day (different final leg than original plan).
- Empty `arrivalHotac` but `departureHotac` populated.
- HOTAC exists with null `person` block.
- Airport mismatch / null airport in hotac item.
- Services endpoint returns non-200 for some flights.

All of these should be surfaced in a troubleshooting table instead of silently dropped.

---

## Questions to confirm before build
1. **Scope of crew**: pilots only (`pilot == true`) or include cabin crew later?
2. **Source of truth for assigned crew**: flight crew endpoint vs preflight payload crew block?
3. **Definition of “booked”**: is `status == OK` enough, or should a document/itinerary be required?
4. **Cancelled handling**: should `CNL` count as missing (red) or a separate warning state (amber)?
5. **Airport logic**: arrival HOTAC only, or consider departure HOTAC for special cases?
6. **Notification flow**: just dashboard for now, or export/Slack/email list of missing hotels?
7. **Time zone display**: show all times in local airport time, MT, or both?

---

## Recommended MVP acceptance criteria
- User can run for today or tomorrow.
- App returns one row per pilot ending duty that day.
- HOTAC status is shown with a clear reason.
- Missing/cancelled bookings are easy to filter.
- Partial API failures are visible in troubleshooting output.

---

## Next step
Once answers to the questions above are confirmed, implementation can be delivered in two small PRs:
1. backend logic module + unit tests for status mapping,
2. Streamlit page wiring + UX polish.
