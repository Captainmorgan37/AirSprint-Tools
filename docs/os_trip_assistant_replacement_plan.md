# OS Trip Assistant Replacement Plan

## Goal
Build an open-source **Trip Assistant alternative** where Operations Services (OS) can:
1. Enter an address.
2. Find the nearest airports (e.g., top 5 by distance).
3. Exclude airports with runway length below a configurable minimum.
4. Optionally exclude airports outside allowed airport categories (e.g., only A/B/C).

## What already exists in this repo
You already have core pieces that cover ~70% of this workflow:

- **Airport suitability logic** in the feasibility modules (`feasibility/airport_module.py`, `feasibility/data_access.py`).
- **Category + runway filtering patterns** in **Fuel Stop Advisor** (`pages/Fuel Stop Advisor.py`), including category gating and runway constraints.
- **Airport metadata + category CSVs** in `data/` and root CSV assets.

This means the biggest missing piece is not airport logic—it is mostly:
- robust address geocoding,
- nearest-airport ranking from lat/lon,
- and a clean OS-facing UI flow.

## Recommended v1 scope (fastest useful release)

### Inputs
- Address text field (single-line, freeform).
- `max_results` (default 5).
- `min_runway_ft` (default set by aircraft class, overridable).
- `allowed_categories` multi-select (default A/B/C enabled).

### Processing flow
1. **Geocode address -> lat/lon**.
2. **Load airport candidates** with lat/lon + runway + category.
3. Compute great-circle distance from input point to each airport.
4. Filter:
   - runway >= `min_runway_ft`
   - category in `allowed_categories` (if provided)
5. Sort by distance ascending.
6. Return top `max_results`.

### Output table (OS-first)
- ICAO / IATA
- Airport name
- Distance (nm)
- Longest runway (ft)
- Category
- City/region
- Quick flags (customs/deice/slots if available)

## Suggested implementation design

### 1) New reusable module
Create `airport_proximity.py` with pure functions:
- `geocode_address(address: str) -> tuple[float, float]`
- `nearest_airports(lat, lon, *, limit=5, min_runway_ft=None, allowed_categories=None) -> list[AirportCandidate]`
- `haversine_nm(...)`

Keep this module framework-agnostic so it can be reused by Streamlit pages and tests.

### 2) Data normalization layer
Build a helper that standardizes airport records from your current CSV sources into:
- `icao`, `iata`, `name`
- `lat`, `lon`
- `max_runway_length_ft`
- `airport_category`

Normalize categories to uppercase once (`A/B/C/...`) to avoid UI-side errors.

### 3) Streamlit UI page
Add a page like `pages/Airport Proximity Finder.py`:
- Form with address + filters.
- Button to run search.
- Dataframe results + CSV export.
- Optional map visualization for later versions.

## Guardrails and edge cases
- No geocode match -> user-friendly error + suggested address format.
- Multiple geocode matches -> show top candidates for user selection.
- Missing runway/category in airport record -> either exclude or show as "Unknown" based on a strict/lenient mode toggle.
- Tie distances -> secondary sort by runway length descending.
- Cache airport dataset + geocode results to keep UI fast.

## Proposed roadmap

### Milestone 1 (v1, 1-2 days)
- Address -> nearest 5 airports with runway/category filters.
- Table results + CSV download.

### Milestone 2
- Include customs availability in output.
- Add optional map display and richer airport operational flags.

## Data/API options for geocoding
- **Primary**: Mapbox Geocoding (already available in this app via existing token).
- **Fallback option**: Nominatim/Google only if Mapbox limits or policy needs change.

Recommendation: start directly with Mapbox now, while keeping provider boundaries small so a swap remains easy later.

## Acceptance criteria for v1
- Given a valid address, app returns <=5 airports sorted by nearest distance.
- Every returned airport satisfies runway + category filters.
- Empty result set returns a clear explanatory message.
- Result table exports to CSV.

## Why this is a good fit for your current codebase
You already solved the hard airport-operational domain pieces elsewhere in the app. This feature can be delivered quickly by reusing your existing airport metadata and filter patterns, while adding one focused proximity/geocode module plus a thin UI page.
