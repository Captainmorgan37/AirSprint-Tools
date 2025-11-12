import streamlit as st
import pandas as pd
import requests
import time
from datetime import date

# =========================
# Shared FL3XX API Setup
# =========================
API_TOKEN = st.secrets["fl3xx_api"]["api_token"]
BASE_URL = "https://app.fl3xx.us/api/external"
HEADERS = {"X-Auth-Token": API_TOKEN}

st.set_page_config(page_title="AirSprint Airport Tools", layout="wide")

# =========================
# Tabs Setup
# =========================
tab1, tab2 = st.tabs(["🏢 Default FBO Finder", "🧊 Deice Availability Checker"])

# ======================================================
# 🏢 TAB 1 – Default FBO Finder
# ======================================================
with tab1:
    st.header("Default FBO Finder (Direct ICAO/IATA/FAA Method)")
    st.write(
        """
        Upload your list of airports — the app will directly query the FL3XX API at  
        `/airports/<CODE>/services` for each ICAO, IATA, or FAA code to identify  
        the **default (mainContact)** FBO.
        """
    )

    fbo_file = st.file_uploader("📂 Upload your airport CSV for FBO Lookup", type=["csv"], key="fbo_file")

    if fbo_file:
        df_fbo = pd.read_csv(fbo_file)
        st.info(f"Loaded {len(df_fbo)} airports from your file.")

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

        results = []
        progress = st.progress(0)
        status_text = st.empty()

        for i, (_, row) in enumerate(df_fbo.iterrows()):
            code = next((str(row[k]).strip() for k in ["ICAO", "IATA", "FAA"] if pd.notna(row.get(k))), None)
            if not code:
                results.append({
                    "ICAO": row.get("ICAO"),
                    "IATA": row.get("IATA"),
                    "FAA": row.get("FAA"),
                    "Default FBO Company": "No Code Provided"
                })
                progress.progress((i + 1) / len(df_fbo))
                continue

            status_text.text(f"Processing {code} ({i+1}/{len(df_fbo)}) …")
            fbo_data = get_fbo_for_airport(code)
            fbo_data.update({
                "ICAO": row.get("ICAO"),
                "IATA": row.get("IATA"),
                "FAA": row.get("FAA"),
            })
            results.append(fbo_data)
            progress.progress((i + 1) / len(df_fbo))
            time.sleep(0.2)

        results_df = pd.DataFrame(results)
        merged = df_fbo.merge(results_df, on=["ICAO", "IATA", "FAA"], how="left")

        st.success("✅ FBO lookup complete!")
        st.dataframe(merged, use_container_width=True)

        csv = merged.to_csv(index=False).encode("utf-8")
        st.download_button("💾 Download Updated CSV", csv, "Airports_with_Default_FBOs.csv", "text/csv")

# ======================================================
# 🧊 TAB 2 – Deice / Anti-Ice Availability Checker
# ======================================================
with tab2:
    st.header("Airport Deice / Anti-Ice Availability Checker")
    st.write(
        """
        Upload your airport list — the app will query each airport’s  
        **operational notes** endpoint for DEICE/ANTI-ICE information and flag  
        airports where deicing is **not available**.
        """
    )

    deice_file = st.file_uploader("📂 Upload your airport CSV for Deice Check", type=["csv"], key="deice_file")
    query_date = st.date_input("📅 Date for query range", value=date.today())
    date_str = query_date.strftime("%Y-%m-%d")

    if deice_file:
        df_deice = pd.read_csv(deice_file)
        st.info(f"Loaded {len(df_deice)} airports from your file.")

        def get_deice_info(code):
            """Fetch deice/anti-ice operational notes for an airport."""
            code = str(code).strip().upper()
            try:
                url = f"{BASE_URL}/airports/{code}/operationalNotes?from={date_str}&to={date_str}"
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code == 404:
                    return {"Deice Info": "Airport Not Found in FL3XX"}

                r.raise_for_status()
                data = r.json()
                if not data:
                    return {"Deice Info": "No Notes Found"}

                deice_notes = []
                deice_unavailable = False

                for note in data:
                    note_text = note.get("note", "")
                    if "DEICE" in note_text.upper() or "ANTI-ICE" in note_text.upper():
                        deice_notes.append(note_text.strip())
                        if "NOT AVAILABLE" in note_text.upper() or "UNAVAILABLE" in note_text.upper():
                            deice_unavailable = True

                if not deice_notes:
                    return {"Deice Info": "No Deice Mentions Found"}

                return {
                    "Deice Info": "\n\n---\n\n".join(deice_notes),
                    "Deice Not Available": "Yes" if deice_unavailable else "No"
                }

            except Exception as e:
                return {"Deice Info": f"Error: {e}"}

        results = []
        progress = st.progress(0)
        status_text = st.empty()

        for i, (_, row) in enumerate(df_deice.iterrows()):
            code = next((str(row[k]).strip() for k in ["ICAO", "IATA", "FAA"] if pd.notna(row.get(k))), None)
            if not code:
                results.append({
                    "ICAO": row.get("ICAO"),
                    "IATA": row.get("IATA"),
                    "FAA": row.get("FAA"),
                    "Deice Info": "No Code Provided"
                })
                progress.progress((i + 1) / len(df_deice))
                continue

            status_text.text(f"Checking {code} ({i+1}/{len(df_deice)}) …")
            deice_data = get_deice_info(code)
            deice_data.update({
                "ICAO": row.get("ICAO"),
                "IATA": row.get("IATA"),
                "FAA": row.get("FAA"),
            })
            results.append(deice_data)
            progress.progress((i + 1) / len(df_deice))
            time.sleep(0.2)

        results_df = pd.DataFrame(results)
        merged = df_deice.merge(results_df, on=["ICAO", "IATA", "FAA"], how="left")

        st.success("✅ Deice lookup complete!")
        st.dataframe(merged, use_container_width=True)

        csv = merged.to_csv(index=False).encode("utf-8")
        st.download_button("💾 Download Deice Info CSV", csv, "Airports_Deice_Info.csv", "text/csv")

st.caption("Built by Morgan’s AirSprint Tools 🔧")
