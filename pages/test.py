import streamlit as st
import pandas as pd
import requests
import time

# =========================
# Load FL3XX API credentials
# =========================
API_TOKEN = st.secrets["fl3xx_api"]["api_token"]
BASE_URL = "https://app.fl3xx.us/api/external"
HEADERS = {"X-Auth-Token": API_TOKEN}

# =========================
# Streamlit UI setup
# =========================
st.title("🛬 Default FBO Finder (Direct ICAO/IATA/FAA Method)")
st.write(
    """
    Upload your list of airports — the app will directly query the FL3XX API at  
    `/airports/<CODE>/services` for each ICAO, IATA, or FAA code to identify  
    the **default (mainContact)** FBO.
    """
)

uploaded_file = st.file_uploader("📂 Upload your airport CSV", type=["csv"])
if not uploaded_file:
    st.stop()

df = pd.read_csv(uploaded_file)
st.info(f"Loaded {len(df)} airports from your file.")

# =========================
# Helper function
# =========================
def get_fbo_for_airport(code):
    """Fetch default FBO for a given airport code (ICAO/IATA/FAA)."""
    code = str(code).strip().upper()
    try:
        url = f"{BASE_URL}/airports/{code}/services"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 404:
            return {"Default FBO Company": "Airport Not Found in FL3XX"}
        r.raise_for_status()

        data = r.json()
        if not data:
            return {"Default FBO Company": "No Services Found"}

        for s in data:
            if s.get("type", {}).get("name") == "FBO" and s.get("mainContact"):
                return {
                    "Default FBO Company": s.get("company"),
                    "FBO Email": s.get("email"),
                    "FBO Phone": s.get("phone"),
                    "FBO Homepage": s.get("homepage"),
                    "FBO Address": s.get("address"),
                    "FBO Radio": s.get("radio"),
                }

        return {"Default FBO Company": "No Default Selected"}

    except Exception as e:
        return {"Default FBO Company": f"Error: {e}"}

# =========================
# Main processing loop
# =========================
results = []
progress = st.progress(0)
status_text = st.empty()

for i, (_, row) in enumerate(df.iterrows()):
    code = None
    for key in ["ICAO", "IATA", "FAA"]:
        if pd.notna(row.get(key)):
            code = str(row[key]).strip()
            break

    if not code:
        results.append({
            "ICAO": row.get("ICAO"),
            "IATA": row.get("IATA"),
            "FAA": row.get("FAA"),
            "Default FBO Company": "No Code Provided"
        })
        progress.progress((i + 1) / len(df))
        continue

    status_text.text(f"Processing {code} ({i+1}/{len(df)}) …")
    fbo_data = get_fbo_for_airport(code)
    fbo_data.update({
        "ICAO": row.get("ICAO"),
        "IATA": row.get("IATA"),
        "FAA": row.get("FAA"),
    })
    results.append(fbo_data)

    progress.progress((i + 1) / len(df))
    time.sleep(0.2)  # small delay for rate limits

# =========================
# Merge results and display
# =========================
results_df = pd.DataFrame(results)
merged = df.merge(results_df, on=["ICAO", "IATA", "FAA"], how="left")

st.success("✅ FBO lookup complete!")
st.dataframe(merged)

csv = merged.to_csv(index=False).encode("utf-8")
st.download_button("💾 Download Updated CSV", csv, "Airports_with_Default_FBOs.csv", "text/csv")

st.caption("Built by Morgan’s AirSprint Tools 🔧")
