# Customs Port Locator: Product Placement Recommendation

## Recommendation (short answer)
Build this first as **another tab inside the existing Nearby Airport Finder app**, then split into its own app later only if usage and scope grow.

## Why this is the best first step
- **User workflow match:** users already think "start from an airport code and find nearby options," so customs-filtered results are a natural extension.
- **Lower build effort:** you can reuse airport lookup, distance logic, and result rendering patterns from the current nearby finder.
- **Faster validation:** this lets you ship quickly and confirm demand before maintaining a second app.
- **Single source of truth:** customs rules/hours can live in one customs data pipeline while still sharing core airport data.

## Suggested feature behavior (MVP)
Input: one airport code (ICAO/IATA).

Output:
1. Find the origin airport and its country.
2. Filter airports to the **same country**.
3. Filter to airports marked as **customs-capable** in your customs document.
4. Compute distance from origin.
5. Return top 5 closest customs airports.
6. Show customs availability fields (e.g., open hours, notes, lead-time constraints, prior notice requirements).

## Data model notes
Add/normalize these fields in the customs data table:
- `airport_code`
- `country_code`
- `customs_available` (bool)
- `hours_local` (structured text or machine-readable schedule)
- `hours_timezone`
- `notes`
- `advance_notice_required`
- `source_updated_at`

## UX recommendation
Add a tab named **"Customs Ports"** beside Nearby Airport Finder.

Core controls:
- Airport code input
- Optional radius slider (for future)
- Optional "show unavailable after-hours" toggle

Result columns:
- Airport
- Distance
- Customs status
- Hours (local)
- Notes / notice required

## When to split into a separate app
Consider a dedicated app only when at least one is true:
- Customs workflow adds independent features (e.g., filing workflows, alerting, compliance history).
- Different user group/permissions from nearby finder.
- Much heavier backend jobs (e.g., frequent schedule ingestion from multiple sources).

## Technical caveats to handle early
- Time zone conversion for customs hours.
- Holiday/exception closures.
- Ambiguous airport codes and fallback matching.
- Data freshness indicators so users trust the hours.

## Bottom line
For your current scope ("airport in -> top 5 nearest same-country customs ports + hours"), **a new tab in the Nearby Airport Finder is the best fit**: fastest to deliver, easiest to adopt, and lowest maintenance risk.
