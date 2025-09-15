import streamlit as st
import pandas as pd
import requests
import json
import re
from io import BytesIO
from datetime import datetime, timedelta

# ----- CONFIG -----
FAA_CLIENT_ID = st.secrets["FAA_CLIENT_ID"]
FAA_CLIENT_SECRET = st.secrets["FAA_CLIENT_SECRET"]
KEYWORDS = ["CLOSED", "CLSD"]  # Add any more keywords here
HIDE_KEYWORDS = ["crane", "RUSSIAN", "CONGO", "OBST RIG", "CANCELLED", "CANCELED", 
                 "SAFETY AREA NOT STD", "GRASS CUTTING", "OBST TOWER", "SFC MARKINGS NOT STD"]

CATEGORY_COLORS = {
    "Runway": "#ff4d4d",
    "PPR": "#ffcc00",
    "Airspace/Navigation": "#4da6ff",
    "Airport Services": "#ffa64d",
    "Other": "#ccc"
}

st.set_page_config(page_title="CFPS/FAA NOTAM Viewer", layout="wide")
st.title("CFPS & FAA NOTAM Viewer")

# ----- RUNWAYS DATA -----
@st.cache_data
def load_runway_data():
    df = pd.read_csv("runways.csv")
    return df

runways_df = load_runway_data()

# ----- FUNCTIONS -----
def highlight_keywords(notam_text: str):
    for kw in KEYWORDS:
        notam_text = notam_text.replace(
            kw, f"<span style='color:red;font-weight:bold'>{kw}</span>"
        )
    return notam_text

def parse_cfps_times(notam_text):
    start_match = re.search(r'\bB\)\s*(\d{10}|PERM)', notam_text)
    end_match = re.search(r'\bC\)\s*(\d{10}|PERM)', notam_text)

    def format_time(t):
        if not t:
            return 'N/A', None
        if t == 'PERM':
            return 'PERM', None
        dt = datetime.strptime(t, "%y%m%d%H%M")
        return dt.strftime("%b %d %Y, %H:%M"), dt

    start, start_dt = format_time(start_match.group(1)) if start_match else ('N/A', None)
    end, end_dt = format_time(end_match.group(1)) if end_match else ('N/A', None)
    return start, end, start_dt, end_dt

def categorize_notam(notam_text):
    text_upper = notam_text.upper()
    # Explicit PPR check (whole word only)
    if re.search(r"\bPPR\b", text_upper):
        return "PPR"
    elif any(rwy_kw in text_upper for rwy_kw in ["RWY", "RUNWAY"]):
        return "Runway"
    elif any(air_kw in text_upper for air_kw in ["SID", "STAR", "APPROACH", "AIRSPACE", "NAVIGATION", "FDC"]):
        return "Airspace/Navigation"
    elif any(ser_kw in text_upper for ser_kw in ["TOWER", "APRON", "GROUND", "SERVICE"]):
        return "Airport Services"
    else:
        return "Other"

def get_cfps_notams(icao: str):
    url = "https://plan.navcanada.ca/weather/api/alpha/"
    params = {
        "site": icao,
        "alpha": ["notam"],
        "notam_choice": "default",
        "_": "1756244240291"
    }
    query_params = []
    for key, value in params.items():
        if isinstance(value, list):
            for v in value:
                query_params.append((key, v))
        else:
            query_params.append((key, value))

    response = requests.get(url, params=query_params)
    response.raise_for_status()
    data = response.json()
    notams = []

    for n in data.get("data", []):
        if n.get("type") == "notam":
            text = n["text"]
            try:
                notam_json = json.loads(text)
                notam_text = notam_json.get("raw", text)
            except:
                notam_text = text

            if any(hide_kw.lower() in notam_text.lower() for hide_kw in HIDE_KEYWORDS):
                continue

            effective_start, effective_end, start_dt, end_dt = parse_cfps_times(notam_text)
            sort_key = start_dt if start_dt else datetime.min

            notams.append({
                "text": notam_text,
                "effectiveStart": effective_start,
                "effectiveEnd": effective_end,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "sortKey": sort_key,
                "category": categorize_notam(notam_text)
            })

    notams.sort(key=lambda x: x["sortKey"], reverse=True)
    return notams

def get_faa_notams(icao: str):
    url = "https://external-api.faa.gov/notamapi/v1/notams"
    headers = {
        "client_id": FAA_CLIENT_ID,
        "client_secret": FAA_CLIENT_SECRET
    }
    params = {
        "icaoLocation": icao.upper(),
        "responseFormat": "geoJson",
        "pageSize": 200
    }

    all_items = []
    page_cursor = None

    while True:
        if page_cursor:
            params["pageCursor"] = page_cursor

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        all_items.extend(items)
        page_cursor = data.get("nextPageCursor")
        if not page_cursor:
            break

    notams = []

    for feature in all_items:
        props = feature.get("properties", {})
        core = props.get("coreNOTAMData", {})
        notam_data = core.get("notam", {})

        notam_text = notam_data.get("text", "")
        translations = core.get("notamTranslation", [])
        simple_text = None
        for t in translations:
            if t.get("type") == "LOCAL_FORMAT":
                simple_text = t.get("simpleText")
        text_to_use = simple_text if simple_text else notam_text

        # Skip ICAO-format NOTAMs (keep only LOCAL_FORMAT / domestic)
        if not simple_text:
            continue

        if any(hide_kw.lower() in text_to_use.lower() for hide_kw in HIDE_KEYWORDS):
            continue

        effective = notam_data.get("effectiveStart", None)
        expiry = notam_data.get("effectiveEnd", None)

        start_dt = end_dt = None
        if effective == "PERM":
            effective_display = "PERM"
        elif effective:
            start_dt = datetime.fromisoformat(effective.replace("Z", ""))
            effective_display = start_dt.strftime("%b %d %Y, %H:%M")
        else:
            effective_display = "N/A"

        if expiry == "PERM":
            expiry_display = "PERM"
        elif expiry:
            end_dt = datetime.fromisoformat(expiry.replace("Z", ""))
            expiry_display = end_dt.strftime("%b %d %Y, %H:%M")
        else:
            expiry_display = "N/A"

        notams.append({
            "text": text_to_use,
            "effectiveStart": effective_display,
            "effectiveEnd": expiry_display,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "sortKey": start_dt if start_dt else datetime.min,
            "category": categorize_notam(text_to_use)
        })

    notams.sort(key=lambda x: x["sortKey"], reverse=True)
    notams = deduplicate_notams(notams)
    return notams


def format_notam_card(notam):
    highlighted_text = highlight_keywords(notam["text"])
    category_color = CATEGORY_COLORS.get(notam["category"], "#ccc")

    if notam["start_dt"] and notam["end_dt"]:
        delta = notam["end_dt"] - notam["start_dt"]
        hours, remainder = divmod(delta.total_seconds(), 3600)
        minutes = remainder // 60
        duration_str = f"{int(hours)}h{int(minutes):02d}m"
    else:
        duration_str = "N/A"

    now = datetime.utcnow()
    if notam["end_dt"]:
        remaining_delta = notam["end_dt"] - now
        if remaining_delta.total_seconds() > 0:
            rem_hours, rem_remainder = divmod(remaining_delta.total_seconds(), 3600)
            rem_minutes = rem_remainder // 60
            remaining_str = f"(in {int(rem_hours)}h{int(rem_minutes):02d}m)"
        else:
            remaining_str = "(expired)"
    else:
        remaining_str = ""

    # Highlight PPR category more prominently
    border_style = f"3px solid {category_color}" if notam["category"] in ["Runway", "PPR"] else "1px solid #ccc"

    card_html = f"""
    <div style='border:{border_style}; padding:10px; margin-bottom:8px; background-color:#111; color:#eee; border-radius:5px;'>
        <p style='margin:0; font-family:monospace;'><strong style="color:{category_color}">[{notam['category']}]</strong></p>
        <p style='margin:0; font-family:monospace; white-space:pre-wrap;'>{highlighted_text}</p>
        <table style='margin-top:5px; font-size:0.9em; color:#aaa; width:100%;'>
            <tr><td><strong>Effective:</strong></td><td>{notam['effectiveStart']}</td><td>{remaining_str}</td></tr>
            <tr><td><strong>Expires:</strong></td><td>{notam['effectiveEnd']}</td></tr>
            <tr><td><strong>Duration:</strong></td><td>{duration_str}</td></tr>
        </table>
    </div>
    """
    return card_html

def normalize_for_dedup(raw_text: str) -> str:
    text = raw_text.lstrip("!").strip()
    text = re.sub(r"\b\d{2}/\d{3}\b", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def deduplicate_notams(notams):
    grouped = {}
    for n in notams:
        norm_text = normalize_for_dedup(n["text"])
        key = (norm_text, n["effectiveStart"], n["effectiveEnd"])
        if key not in grouped:
            grouped[key] = n
        else:
            existing = grouped[key]
            if len(n["text"]) > len(existing["text"]):
                grouped[key] = n
    return list(grouped.values())

def is_runway_closed(notam_text, runway_name):
    text_upper = notam_text.upper()
    runway_upper = runway_name.upper()
    direct_rwy_pattern = rf"RWY\s+{re.escape(runway_upper)}\b.*(?:{'|'.join(KEYWORDS)})"
    twy_context_pattern = rf"TWY\s+[A-Z0-9]+.*RWY\s+{re.escape(runway_upper)}"
    if re.search(direct_rwy_pattern, text_upper):
        if not re.search(twy_context_pattern, text_upper):
            return True
        if "AVBL AS TWY" in text_upper:
            return True
    return False

def normalize_surface(surface):
    s = str(surface).upper()
    if any(a in s for a in ["ASP", "ASPH", "ASPHALT"]):
        return "Asphalt", True
    elif any(c in s for c in ["CON", "CONC", "CONCRETE"]):
        return "Concrete", True
    else:
        return s.title(), False

def get_runway_status(icao: str, airport_notams: list):
    airport_runways = runways_df[runways_df['airport_ident'] == icao.upper()]
    status_list = []
    for _, row in airport_runways.iterrows():
        full_rwy_name = row['le_ident'] + '/' + row['he_ident'] if pd.notna(row['he_ident']) else row['le_ident']
        closed = False
        for n in airport_notams:
            if is_runway_closed(n["text"], full_rwy_name):
                closed = True
                break

        surface_normalized, usable = normalize_surface(row.get('surface', 'Unknown'))

        status_list.append({
            "runway": full_rwy_name,
            "length_ft": row['length_ft'],
            "surface": surface_normalized,
            "usable": usable,
            "status": "closed" if closed else "open"
        })

    return status_list

def sort_notams_for_display(notams):
    def sort_key(n):
        if n["category"] == "Runway":
            return (0, n["sortKey"])
        elif n["category"] == "PPR":
            return (1, n["sortKey"])
        elif n["category"] == "Airspace/Navigation":
            return (2, n["sortKey"])
        elif n["category"] == "Airport Services":
            return (3, n["sortKey"])
        else:
            return (4, n["sortKey"])
    return sorted(notams, key=sort_key)

# ----- USER INPUT -----
icao_input = st.text_input(
    "Enter ICAO code(s) separated by commas (e.g., CYYC, KTEB):"
).upper().strip()

uploaded_file = st.file_uploader(
    "Or upload an Excel/CSV with ICAO codes (columns: 'ICAO', 'From (ICAO)', 'To (ICAO)')",
    type=["xlsx", "csv"]
)

icao_list = []
if icao_input:
    icao_list.extend([code.strip() for code in icao_input.split(",") if code.strip()])

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
        found_codes = []
        for col in ["ICAO", "From (ICAO)", "To (ICAO)"]:
            if col in df.columns:
                found_codes.extend(df[col].dropna().astype(str).str.upper().tolist())
        if found_codes:
            icao_list.extend(list(dict.fromkeys(found_codes)))
        else:
            st.error("Uploaded file must have a valid ICAO column")
    except Exception as e:
        st.error(f"Error reading file: {e}")

# ----- TABS -----
tab1, tab2 = st.tabs(["CFPS/FAA Viewer", "FAA Debug"])

# ---------------- Tab 1: CFPS/FAA Viewer ----------------
with tab1:
    if icao_list:
        st.write(f"Fetching NOTAMs for {len(icao_list)} airport(s)...")
        cfps_list, faa_list = [], []

        for icao in icao_list:
            try:
                if icao.startswith("C"):
                    cfps_list.append({"ICAO": icao, "notams": get_cfps_notams(icao)})
                else:
                    faa_list.append({"ICAO": icao, "notams": get_faa_notams(icao)})
            except Exception as e:
                st.warning(f"Failed to fetch data for {icao}: {e}")

        # Filter input
        filter_input = st.text_input("Filter NOTAMs by keywords (comma-separated):").strip().lower()
        filter_terms = [t.strip() for t in filter_input.split(",") if t.strip()]

        def matches_filter(text: str):
            if not filter_terms:
                return True
            return any(term in text.lower() for term in filter_terms)

        def highlight_search_terms(notam_text: str):
            highlighted = notam_text
            for term in filter_terms:
                highlighted = re.sub(
                    f"({re.escape(term)})",
                    r"<span style='background-color:rgba(255, 255, 0, 0.3); font-weight:bold'>\1</span>",
                    highlighted,
                    flags=re.IGNORECASE,
                )
            return highlighted

        col1, col2 = st.columns(2)

with col1:
    st.subheader("Canadian Airports (CFPS)")
    for airport in cfps_list:
        # Apply filter to NOTAMs before rendering
        filtered_notams = [n for n in sort_notams_for_display(airport["notams"]) if matches_filter(n["text"])]
        if not filtered_notams:
            continue  # Skip airport if no NOTAMs match

        with st.expander(airport["ICAO"], expanded=False):
            # Only show runway status if there are filtered NOTAMs
            runways_status = get_runway_status(airport["ICAO"], filtered_notams)
            if runways_status:
                runway_table_html = "<table style='border-collapse: collapse; width:100%; color:#eee;'>"
                runway_table_html += "<tr><th>Runway</th><th>Length (ft)</th><th>Surface</th><th>Status</th></tr>"
                for r in runways_status:
                    color = "#f00" if r["status"] == "closed" else "#0f0"
                    surface_color = "#f00" if not r["usable"] else "#0f0"
                    runway_table_html += f"<tr><td>{r['runway']}</td><td>{r['length_ft']}</td><td style='color:{surface_color}'>{r['surface']}</td><td style='color:{color}'>{r['status']}</td></tr>"
                runway_table_html += "</table>"
                st.markdown(runway_table_html, unsafe_allow_html=True)

            for notam in filtered_notams:
                notam_copy = notam.copy()
                notam_copy["text"] = highlight_search_terms(notam_copy["text"])
                st.markdown(format_notam_card(notam_copy), unsafe_allow_html=True)

with col2:
    st.subheader("US Airports (FAA)")
    for airport in faa_list:
        # Apply filter to NOTAMs before rendering
        filtered_notams = [n for n in sort_notams_for_display(airport["notams"]) if matches_filter(n["text"])]
        if not filtered_notams:
            continue  # Skip airport if no NOTAMs match

        with st.expander(airport["ICAO"], expanded=False):
            # Only show runway status if there are filtered NOTAMs
            runways_status = get_runway_status(airport["ICAO"], filtered_notams)
            if runways_status:
                runway_table_html = "<table style='border-collapse: collapse; width:100%; color:#eee;'>"
                runway_table_html += "<tr><th>Runway</th><th>Length (ft)</th><th>Surface</th><th>Status</th></tr>"
                for r in runways_status:
                    color = "#f00" if r["status"] == "closed" else "#0f0"
                    surface_color = "#f00" if not r["usable"] else "#0f0"
                    runway_table_html += f"<tr><td>{r['runway']}</td><td>{r['length_ft']}</td><td style='color:{surface_color}'>{r['surface']}</td><td style='color:{color}'>{r['status']}</td></tr>"
                runway_table_html += "</table>"
                st.markdown(runway_table_html, unsafe_allow_html=True)

            for notam in filtered_notams:
                notam_copy = notam.copy()
                notam_copy["text"] = highlight_search_terms(notam_copy["text"])
                st.markdown(format_notam_card(notam_copy), unsafe_allow_html=True)



# ---------------- Tab 2: FAA Debug ----------------
with tab2:
    st.header("FAA NOTAM Debug - Raw Data")
    debug_icao = st.text_input("Enter ICAO for raw FAA NOTAM debug", value="KSFO").upper().strip()

    if debug_icao:
        st.write(f"Fetching raw FAA NOTAMs for {debug_icao}...")
        try:
            url = "https://external-api.faa.gov/notamapi/v1/notams"
            headers = {
                "client_id": FAA_CLIENT_ID,
                "client_secret": FAA_CLIENT_SECRET
            }
            params = {
                "icaoLocation": debug_icao,
                "responseFormat": "geoJson",
                "pageSize": 200
            }

            all_items = []
            page_cursor = None

            while True:
                if page_cursor:
                    params["pageCursor"] = page_cursor

                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                items = data.get("items", [])
                all_items.extend(items)
                page_cursor = data.get("nextPageCursor")
                if not page_cursor:
                    break

            st.write(f"Total NOTAMs received: {len(all_items)}")

            for feature in all_items:
                props = feature.get("properties", {})
                core = props.get("coreNOTAMData", {})
                notam_data = core.get("notam", {})
                text = notam_data.get("text", "")
                st.text(text)

        except Exception as e:
            st.error(f"FAA fetch failed for {debug_icao}: {e}")

