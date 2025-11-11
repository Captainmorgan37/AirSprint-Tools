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
# Helper to find airport ID
# =========================
def get_airport_id(code):
    """Try searching FL3XX airports for ICAO, IATA, or FAA codes"""
    try:
        r = requests.get(f"{BASE_URL}/airports/search?query={code}", headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data:
            return data[0].get("id")
    except Exception:
        return None

# =========================
# Process airports
# =========================
st.info(f"Found {len(df)} airports in uploaded file. Starting scan...")

results = []
progress_bar = st.progress(0)

for i, (_, row) in enumerate(df.iterrows()):
    airport_id = None

    # Try ICAO → IATA → FAA
    for key in ["ICAO", "IATA", "FAA"]:
        val = row.get(key)
        if pd.notna(val):
            code = str(val).strip()
            airport_id = get_airport_id(code)
            if airport_id:
                break

    if not airport_id:
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Default FBO Company": "Airport Not Found in FL3XX"
        })
        progress_bar.progress((i + 1) / len(df))
        continue

    try:
        r = requests.get(f"{BASE_URL}/airports/{airport_id}/services", headers=HEADERS, timeout=10)
        r.raise_for_status()
        services = r.json()

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

csv = merged.to_csv(index=False).encode("utf-8")
st.download_button("Download Updated CSV", csv, "Canada Airports with FBOs.csv", "text/csv")
