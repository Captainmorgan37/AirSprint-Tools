# ForeFlight API setup (Flights feed)

## Streamlit Cloud secrets

Add the token to Streamlit Cloud **App Settings → Secrets** so the app can read it via `st.secrets`:

```toml
[foreflight_api]
api_token = "FakeTokenInfo"
```

In code, read the token like:

```python
token = st.secrets["foreflight_api"]["api_token"]
```

## Flights endpoint

Base endpoint for the initial flight pull:

```
https://public-api.foreflight.com/public/api/Flights/flights
```

The API expects `fromDate` and `toDate` in UTC `YYYY-MM-DDZ` form (time optional). Example using **date only**:

```
https://public-api.foreflight.com/public/api/Flights/flights?fromDate=2026-01-15Z&toDate=2026-01-19Z
```

### Date-only behavior note

When no time is included, the API assumes `00:00Z` for both bounds. If you set `fromDate=2026-01-15Z` and `toDate=2026-01-16Z` (no times), the request returns **only** flights starting on the 15th.

## Example request (Python)

```python
import requests

base_url = "https://public-api.foreflight.com/public/api/Flights/flights"
params = {
    "fromDate": "2026-01-15Z",
    "toDate": "2026-01-19Z",
}
headers = {
    # ForeFlight requires the API key in the x-api-key header.
    "x-api-key": st.secrets["foreflight_api"]["api_token"],
    "Accept": "application/json",
}

response = requests.get(base_url, params=params, headers=headers, timeout=30)
response.raise_for_status()
flights_payload = response.json()
```

## Response shape (sample)

The response contains a top-level `flights` array. Each flight includes the common fields such as:

- `departure`, `destination`, `route`
- `aircraftRegistration`, `flightId`
- `departureTime`, `arrivalTime`
- `crew` array with `position`, `crewId`, `weight`
- `load` section with passenger/cargo weights

Use these keys as the starting point for mapping into your app’s data model.
