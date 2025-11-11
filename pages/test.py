import requests
import pandas as pd
import time
import streamlit as st

# =========================
# Load credentials from Streamlit secrets
# =========================
API_TOKEN = st.secrets["fl3xx_api"]["api_token"]
AUTH_HEADER_NAME = st.secrets["fl3xx_api"].get("auth_header_name", "Authorization")
BASE_URL = "https://app.fl3xx.us/api/external"

HEADERS = {AUTH_HEADER_NAME: API_TOKEN}

# =========================
# File input (upload or internal)
# =========================
st.title("Default FBO Finder")
st.write("Uploads your airport list and automatically fetches default FBOs via FL3XX API.")

uploaded_file = st.file_uploader("Upload your airport CSV", type=["csv"])
if not uploaded_file:
    st.stop()

df = pd.read_csv(uploaded_file)

# =========================
# Debug logging helpers
# =========================
debug_logs = []


def log_debug(message):
    """Collect debug messages to surface in the UI."""
    debug_logs.append(message)

# =========================
# Helper to find airport ID
# =========================
def get_airport_id(code):
    """Try searching FL3XX airports for ICAO, IATA, or FAA codes"""
    try:
        log_debug(f"Searching FL3XX for code '{code}'")
        r = requests.get(f"{BASE_URL}/airports/search?query={code}", headers=HEADERS, timeout=10)
        log_debug(f"Search response for '{code}': status={r.status_code}")
        r.raise_for_status()
        data = r.json()
        log_debug(f"Search results for '{code}': {len(data)} matches")
        if data:
            airport_id = data[0].get("id")
            log_debug(f"Using airport id {airport_id} for code '{code}'")
            return airport_id
    except Exception as e:
        log_debug(f"Error searching for '{code}': {e}")
    return None

# =========================
# Process airports
# =========================
st.info(f"Found {len(df)} airports in uploaded file. Starting scan...")

results = []
progress_bar = st.progress(0)

for i, (_, row) in enumerate(df.iterrows()):
    airport_id = None
    log_debug(f"Processing row {i}: ICAO={row.get('ICAO')}, IATA={row.get('IATA')}, FAA={row.get('FAA')}")

    # Try ICAO → IATA → FAA
    for key in ["ICAO", "IATA", "FAA"]:
        val = row.get(key)
        if pd.notna(val):
            code = str(val).strip()
            airport_id = get_airport_id(code)
            if airport_id:
                break
            else:
                log_debug(f"No airport id found for code '{code}'")

    if not airport_id:
        log_debug(f"No airport found in FL3XX for row {i}")
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Default FBO Company": "Airport Not Found in FL3XX"
        })
        progress_bar.progress((i + 1) / len(df))
        continue

    try:
        log_debug(f"Fetching services for airport id {airport_id}")
        r = requests.get(f"{BASE_URL}/airports/{airport_id}/services", headers=HEADERS, timeout=10)
        log_debug(f"Services response for airport id {airport_id}: status={r.status_code}")
        r.raise_for_status()
        services = r.json()
        log_debug(f"Services count for airport id {airport_id}: {len(services)}")

        found_fbo = None
        for s in services:
            if s.get("type", {}).get("name") == "FBO" and s.get("mainContact") == True:
                found_fbo = {
                    "Default FBO Company": s.get("company"),
                    "FBO Email": s.get("email"),
                    "FBO Phone": s.get("phone"),
                    "FBO Homepage": s.get("homepage"),
                    "FBO Address": s.get("address"),
                    "FBO Radio": s.get("radio"),
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
                "Default FBO Company": "No Default Selected"
            })

    except Exception as e:
        log_debug(f"Error fetching services for airport id {airport_id}: {e}")
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Default FBO Company": f"Error: {e}"
        })

    progress_bar.progress((i + 1) / len(df))
    time.sleep(0.25)  # small delay for API courtesy

# =========================
# Merge & Display
# =========================
results_df = pd.DataFrame(results)
merged = df.merge(results_df, on=["ICAO", "IATA", "FAA"], how="left")

st.success("✅ FBO lookup complete!")
st.dataframe(merged)

with st.expander("Debug details"):
    if debug_logs:
        for entry in debug_logs:
            st.write(entry)
    else:
        st.write("No debug messages recorded.")

csv = merged.to_csv(index=False).encode("utf-8")
st.download_button("Download Updated CSV", csv, "Canada Airports with FBOs.csv", "text/csv")
