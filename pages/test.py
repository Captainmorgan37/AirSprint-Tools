import requests
import pandas as pd
import time

from fl3xx_api import (
    Fl3xxApiConfig,
    fetch_preflight,
    parse_preflight_payload,
)
from flight_leg_utils import (
    FlightDataError,
    build_fl3xx_api_config,
    get_todays_sorted_legs_by_tail,
)
from Home import configure_page, password_gate, render_sidebar


def _load_fl3xx_settings() -> Optional[dict[str, Any]]:
    """Return FL3XX API credentials from Streamlit secrets when available."""

    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None

    try:
        section = secrets["fl3xx_api"]
    except Exception:
        return None

    if isinstance(section, Mapping):
        return dict(section)

    if isinstance(section, dict):  # pragma: no cover - defensive fallback
        return dict(section)

    items_getter = getattr(section, "items", None)
    if callable(items_getter):  # pragma: no cover - defensive fallback
        return dict(items_getter())

    return None


# --- Page setup ---
configure_page(page_title="DEBUG PREFLIGHT / CHECKINS")
password_gate()
render_sidebar()
st.title("DEBUG: Preflight / Checkins / Legs by Tail")

fl3xx_settings = _load_fl3xx_settings()
if not fl3xx_settings:
    st.error(
        "FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml` under the "
        "`fl3xx_api` section and reload the app."
    )
    st.stop()

try:
    config: Fl3xxApiConfig = build_fl3xx_api_config(fl3xx_settings)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()




API_KEY = "YOUR_API_KEY"
BASE_URL = "https://app.fl3xx.us/api/external"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Load your Canadian airports list
df = pd.read_csv("Canada Airports.csv")

results = []

def get_airport_id(code):
    """Try searching FL3XX airports for a code (ICAO/IATA/FAA)"""
    try:
        r = requests.get(f"{BASE_URL}/airports/search?query={code}", headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data:
            return data[0].get("id")
    except Exception:
        return None

for _, row in df.iterrows():
    airport_code = None
    airport_id = None

    for key in ["ICAO", "IATA", "FAA"]:
        if pd.notna(row.get(key)):
            airport_code = str(row[key]).strip()
            airport_id = get_airport_id(airport_code)
            if airport_id:
                break

    if not airport_id:
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Default FBO": "Airport Not Found in FL3XX"
        })
        continue

    try:
        r = requests.get(f"{BASE_URL}/airports/{airport_id}/services", headers=HEADERS, timeout=10)
        r.raise_for_status()
        services = r.json()
        found_fbo = None

        for s in services:
            if s.get("type", {}).get("name") == "FBO" and s.get("mainContact") == True:
                found_fbo = {
                    "Company": s.get("company"),
                    "Email": s.get("email"),
                    "Phone": s.get("phone"),
                    "Homepage": s.get("homepage"),
                    "Radio": s.get("radio"),
                    "Address": s.get("address"),
                }
                break

        if found_fbo:
            results.append({
                "ICAO": row.get("ICAO"),
                "IATA": row.get("IATA"),
                "FAA": row.get("FAA"),
                **found_fbo
            })
        else:
            results.append({
                "ICAO": row.get("ICAO"),
                "IATA": row.get("IATA"),
                "FAA": row.get("FAA"),
                "Company": "No Default Selected"
            })

    except Exception as e:
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Company": f"Error: {e}"
        })

    time.sleep(0.25)  # gentle delay to avoid rate limiting

# Save results
out_df = pd.DataFrame(results)
out_df.to_csv("default_fbos.csv", index=False)
print("✅ Done! Saved to default_fbos.csv")
