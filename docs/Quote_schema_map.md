For endpoint: /api/external/quote/{quoteId}
Designed for parsing + feasibility engine consumption

1. ROOT OBJECT
quote {
    aircraft
    aircraftObj {…}
    legs: [ Leg {…} ]
    customer {…}
    comment
    price {…}
    quoteNumber
    status
    bookingIdentifier
    bookingid
    workflow
    workflowCustomName
    origin
    salesPerson {…}
    createdDate
    postFlightClosed
}

2. AIRCRAFT BLOCK

Path: aircraftObj

aircraftObj {
    tailNumber
    numberOfSeats
    type                  # e.g. "E545"
    model                 # e.g. "EMB-545 Legacy 450"
    typeName              # "Embraer"
    homebase
    equipment {…}
    wingSpan
    maxFuel
    externalLength
    externalHeight
    cabinHeight
    cabinLength
    cabinWidth
    category              # e.g. SUPER_MIDSIZE_JET
    cargo                 # boolean
    ambulance             # boolean
    id                    # aircraftId
    aircraftName
    aocNumber
    status
    manufacturingDate
    cabinCrew
    bedsN
    ownersApprovalRequired
}

equipment (under aircraftObj)
equipment {
    v110
    v230
    headsets
    tv
    cd_dvd
    wifi
    satPhone
    satTV
    entertainmentSystem
    lavatory
    enclosedLavatory
    coffeePot
    espresso
    iceBin
    microwaveOven
    warmingOven
    smokingAllowed
    petsAllowed
    baggageVolume        # cubic volume
    maxWeight
    standardSuitcases
    skiTube
    golfBags
}

Feasibility Use

Tail type → runway requirements, airport restrictions

Equipment → pet rules, cabin restrictions

Seating → pax limits

Cargo capability → yes/no

Category → OSA/SSA, deice category

Baggage volume → baggage feasibility

Owner approval → special handling

3. LEGS ARRAY

Path: legs[]

Each Quote may contain multiple legs (usually 1–4).

legs: [
  {
    id
    departureAirport
    arrivalAirport
    departureDate
    departureDateUTC
    arrivalDate
    arrivalDateUTC
    pax
    workflow                   # "PRIVATE"
    workflowCustomName         # "FEX Guaranteed"
    notes
    warnings: [ WarningType ]
    flightTime                 # minutes
    blockTime                  # minutes
    distance                   # NM or miles depending on FL3XX tenant
    planningNotes
    postFlightClosed
    status

    departureAirportObj {…}
    arrivalAirportObj {…}

    flightInfo {
        flightId
        fplType
    }

    crew: []
    aircraft                   # string (same as root.aircraft)
  }
]

Airport Object

departureAirportObj and arrivalAirportObj share structure:

airportObj {
    icao
    iata
    name
    faa
    localIdentifier
    aid                # FL3XX internal airport ID
    id                 # airport numeric ID
}

warnings[]

Array of objects like:

warnings: [
  { type: "OPERATIONAL_NOTE" },
  { type: "OPERATIONAL_NOTE_ALERT" }
]


These correspond to FL3XX airport operational notes.

Feasibility Use

ETD/ETA used for weather window, airport hours, customs

Departure & arrival ICAO used for:

runway DB

deice capability DB

customs DB

NOTAM checks

pax → capacity check, W&B placeholder

workflow → OCS/owner rules

planningNotes → owner requests

warnings → operational note indicator

blockTime → duty & OSA/SSA timing

flightTime → fuel stop / CJ2 endurance

4. CUSTOMER BLOCK

Path: customer

customer {
    internalId
    firstName
    lastName
    salutation
    logName
    gender
    birthDate
    birthPlace
    status

    account {
        name
        phone
        address {
            type
            street
            city
            zip
            country
            latitude
            longitude
            state
            links:[]
        }
        accountid
        notes
        accountNumber
        links:[]
    }
}

Feasibility Use

Account notes may include owner preference (“text instead of email”).

Some operators incorporate VIP handling rules.

5. QUOTE METADATA

Located at root.

bookingIdentifier     # e.g. "PIURB"
bookingid             # numeric ID
quoteNumber           # "A-BE25-884132"
status                # "Q" (Quote)
origin                # e.g. "EMAIL"
workflow              # "PRIVATE"
workflowCustomName    # "FEX Guaranteed"
comment               # internal comment string
createdDate           # epoch ms
postFlightClosed      # boolean

Feasibility Use

bookingIdentifier → DM reference

workflow → flex/guaranteed logic

quoteNumber / origin → tracking, logging

comment → often used for instructions

createdDate → request age, urgent flag

6. PRICE BLOCK

Path: price

price {
    charterCost
    gross
    net
    vat
    fet
    tax
    vatPerc
    currency
    exchangeRate
}

Feasibility Use

Typically none (unless used for special OCS rules or owner constraints).

7. SALES PERSON

Path: salesPerson

salesPerson {
    internalId
    firstName
    lastName
    logName
    salutation
    gender
    status
    personnelNumber
    links:[]
}

Use

Usually not needed operationally, except for:

feedback loop

clarifying ambiguous feasibility requirements

8. FIELDS NOT PRESENT AT QUOTE STAGE

Quotes do not contain:

crew

FDP / duty data

slots

PPR status

handler info

W&B data

catering assignments

fuel orders

maintenance flags

permit statuses

flight release tasks

These become available only after:
Quote → Booking → Flight
and then via:
/flight/{id}/preflight.

⭐ SUMMARY BLOCK FOR CODEX (You can paste this into your app directly)
Quote → Root {
    aircraftObj → Aircraft specs, equipment, category, baggage volume, ownership rules
    legs[] → from/to airports, times (local & UTC), pax, block time, notes, warnings
    legs[].arrivalAirportObj / departureAirportObj → airport identifiers
    customer → contact info, account notes
    price → irrelevant for feasibility
    bookingIdentifier → string used by DMs
    bookingid → numeric internal ID
    workflow / workflowCustomName → PRIVATE / FEX Guaranteed
    comment → internal DM/owner instructions
    createdDate → timestamp
}
