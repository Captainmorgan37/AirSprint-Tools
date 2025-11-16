A structured map of all meaningful fields in FL3XX /flight/{id}/preflight pulls, with usage notes.

1. Root Object

Top-level structure includes:

booking
bookingId
flight        (null if uncrewed)
tailNumber
aircraftId
crewBrief {…}
flightSelection {…}
detailsDeparture {…}
detailsArrival {…}
warnings {…}
flightDetails {…}


Most feasibility-relevant data lives inside:

flightSelection

flightDetails

detailsDeparture

detailsArrival

warnings

2. Booking Information

Located under crewBrief.flightSelection.bookings[].

Useful fields:

bookingIdentifier — you use this as the DM-facing flight reference

statusSymbol — booking status (F, R, etc.)

customerInfo — name, account number, preferences

e.g. authorized bookers, commercial instructions

Useful for notes but not feasibility logic

legInfo

from, to

date (epoch ms)

duration (“2h29”)

fromTimeZone

Usage

Feasibility engine uses this as the baseline schedule (in case flight object is incomplete).

fromTimeZone helps compute duty & FDP local conversions.

Duration used for endurance checks.

3. Flight Object (flightSelection.flight)

This is the core flight entity:

Fields include:

id (flightId)

bookingIdentifier

aircraftId

aircraftType

tailNumber

passengers

maxCargo

bookingId

customerName / customerId

workflow & operatorWorkflow (FEX vs FEX-OCS, CAR604 vs 135 etc.)
(from your file: “FEX Guaranteed” and CAR 604 FEX — PRIVATE)

Usage

Aircraft/airport feasibility

OSA/SSA rules

Duty time classification (CAR604 affects flight plan processes)

Empty leg / OCS detection

4. FlightDetails (Under flightDetails.dep / .arr)

This block holds airport-specific requirements for departure & arrival.

Example from your file:

Fields:

handlerName

catering, numberOfCateringServices

gndTrsService & gndTrs

hotacService & hotacs

slotFromTime, slotToTime

pprFromTime, pprToTime

fuelCurrency, fuelPrice

numberOfOverFlightCountries

overalOverFlightCountriesTodo

Usage

Slot/PPR required?

Deice/hangar → handler relevance

Customs support

Permission requirements

OSA/SSA ground-time check

Fuel availability red flags

Overflight permit decision tree

5. warnings[]

Contains operational notes for the airports, handlers, restrictions.

From your file:

CYWG deice + CDF notes

CYOW deice notes

After-hours rules

Customs procedures

FBO quirks
(These were visible across multiple blocks.)

Usage

Deice capability inference (cross-check with your new DB)

Customs/hours restrictions

FBO suitability warnings

OSA/SSA airport notes

Hangar recommendation augmentation

Operational hazard warnings

This block is one of the most valuable for feasibility.

6. detailsDeparture / detailsArrival

Status of all checklist items for departure and arrival.

Example:

Fields:

hndlgAndSvcs → handler, fuel, customs, migration

notif → movementMsg, manifest, crew, pax

crw → crew assignment, crewBriefing

log → preflight checks, documents, maintenance, deice

nts → notes

flt → weightAndBalance, flightPlan, slot, fuel, overflightPermissions

flightRelease

tasks[]

Values include “OK”, “NR”, “NA”, “DO” meaning:

OK = completed

NR = not required

NA = not applicable

DO = outstanding

Usage

Even though uncrewed feasibility flights will have many DO/NR:

Detect critical missing requirements (slot, customs, W&B)

Flag if flight release is blocked

Identify required permits

Identify missing MVT confirmations

Even uncrewed flights have meaningful items here (slots, PPR, approvals).

7. ffCat & aircraftReqRw

From your file:

ffCat: category (A_3 etc.)

aircraftReqRw: required runway distance (calculated)

Usage

Runway suitability comparison

Automatic flag if reqRw > longestRunway

Valuable for quick “No-Go” checks

8. Crew Data (future flights)

For feasibility flights, crew blocks will be absent or “NA”.

In your file, crew blocks show:

crewAssign: null

dtl: null

no recency data

no qualifications

This confirms uncrewed feasibility preflights will have:

No FDP

No rest info

No pilot recency

Usage

Your feasibility engine should skip crew checks entirely until later.

Duty feasibility is not applicable.

You still can use ground-time rules (OSA/SSA) because they are tied to flight, not crew.

9. Overflight Permissions

Fields across blocks:

numberOfOverFlightCountries

overalOverFlightCountriesTodo

overflightPermissions

Example:

Usage

Determine if overflight permit is needed

Determine if lead time is adequate

Determine feasibility CAUTION/FAIL

10. Flight Status / Workflow

From:

operatorWorkflow.workflow (“FEX Guaranteed”)

fplType (“G”)

operationType.label (“CAR 604 FEX — PRIVATE”)

is91Flight / is135Flight

Usage

Determines planning rules

ICAO flight plan obligations

Determines Jeppesen trip planning requirement

DM feasibility language (e.g., “CAR 604 private flight”)

11. LegInfo (Booking-Level)

From:

Includes:

route (CYWG → CYOW)

date

duration

timezone

Usage

Used as fallback if flight block missing

Helps align the feasibility engine for time computations

Crucial when flight object isn't built yet (common in feasibilities)

12. Catering, Hotac, Ground Transport

Found under flightDetails.dep/arr and detailsDeparture / detailsArrival.hndlgAndSvcs.

Fields include:

catering

hotacs

gndTrsService

Usage

Mostly relevant after flight is confirmed, but can be used to:

Ensure SSA/OSA 90-minute rules are feasible

Ensure bookings requiring HOTAC (overnights) have adequate ground-time logic
