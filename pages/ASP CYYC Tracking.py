import email
import imaplib
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from zoneinfo_compat import ZoneInfo

from Home import configure_page, get_secret, password_gate, render_sidebar, require_secret

configure_page(page_title="Aircraft Presence (McCall/Palmer)")
password_gate()
render_sidebar()

# ----------------------------
# Config
# ----------------------------
st.title("✈️ Aircraft Presence — McCall & Palmer")


# Auto-refresh every 180s
st_autorefresh(interval=180 * 1000, key="gpsfeedrefresh")

# Load credentials from secrets
EMAIL_ACCOUNT = require_secret("EMAIL_ACCOUNT")
EMAIL_PASSWORD = require_secret("EMAIL_PASSWORD")
IMAP_SERVER = get_secret("IMAP_SERVER", "imap.gmail.com")

SENDER = "no-reply@telematics.guru"
SUBJECT = "ASP TRACKING EMAIL"
FILENAME = "IOCCReport-2ndIteration.csv"

LOCAL_TZ = ZoneInfo("America/Edmonton")


# ----------------------------
# Fetch Latest CSV
# ----------------------------
def fetch_latest_csv() -> pd.DataFrame:
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
        mail.select("inbox")

        status, messages = mail.search(None, f'(FROM "{SENDER}" SUBJECT "{SUBJECT}")')
        if status != "OK" or not messages[0]:
            return pd.DataFrame()

        latest_id = messages[0].split()[-1]
        status, msg_data = mail.fetch(latest_id, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get("Content-Disposition") is None:
                continue
            filename = part.get_filename()
            if not filename:
                continue

            filename = filename.strip()
            is_expected_file = filename == FILENAME
            is_csv_attachment = filename.lower().endswith(".csv")

            if is_expected_file or is_csv_attachment:
                payload = part.get_payload(decode=True)
                df = pd.read_csv(pd.io.common.BytesIO(payload))
                return df

        return pd.DataFrame()

    except Exception:
        return pd.DataFrame()


# ----------------------------
# Helpers
# ----------------------------
def parse_df(df: pd.DataFrame):
    if df.empty:
        return df

    df.columns = [c.strip() for c in df.columns]

    column_aliases = {
        "Last Seen UTC": "Last Seen (MT)",
        "Last Connected Date Time": "Last Seen (MT)",
        "Last Location": "Last Location",
        "Location": "Last Location",
    }

    for original, alias in column_aliases.items():
        if original in df.columns and alias not in df.columns:
            df.rename(columns={original: alias}, inplace=True)

    def parse_local(ts: str):
        try:
            return datetime.strptime(ts.strip(), "%d/%m/%Y %H:%M").replace(tzinfo=LOCAL_TZ)
        except Exception:
            return pd.NaT

    if "Last Seen (MT)" in df.columns:
        df["Last Seen (MT)"] = df["Last Seen (MT)"].apply(parse_local)
    else:
        df["Last Seen (MT)"] = pd.NaT

    if "Last Location" in df.columns:
        df["Last Location"] = df["Last Location"].astype(str).str.strip()
    else:
        df["Last Location"] = ""

    # Extract only the tail registration (first token of the Name column)
    if "Name" in df.columns:
        df["Tail"] = df["Name"].apply(lambda x: str(x).split()[0])
    else:
        df["Tail"] = ""

    return df


def get_current(df: pd.DataFrame, window_min: int = 25):
    now = datetime.now(LOCAL_TZ)
    return df[(now - df["Last Seen (MT)"]) <= timedelta(minutes=window_min)].copy()


def render_hangar(name: str, df: pd.DataFrame):
    st.subheader(f"🏢 {name}")

    loc_df = df[df["Last Location"] == name]

    if loc_df.empty:
        st.caption("No aircraft currently at this hangar.")
    else:
        cols = st.columns(len(loc_df))
        for col, (_, row) in zip(cols, loc_df.iterrows()):
            col.success(f"🛩️ {row['Tail']}", icon="✅")


# ----------------------------
# Main
# ----------------------------
df = fetch_latest_csv()
df = parse_df(df)

if df.empty:
    st.warning("No data available yet.")
    st.stop()

current_df = get_current(df)

now_mt = datetime.now(LOCAL_TZ)
st.caption(f"Last refresh (MT): **{now_mt.strftime('%Y-%m-%d %H:%M:%S %Z')}**")
st.divider()

# Render McCall & Palmer
for site in ["McCall Hangar", "Palmer Hangar"]:
    render_hangar(site, current_df)
