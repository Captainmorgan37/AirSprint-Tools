import re

import pandas as pd
import streamlit as st

from fl3xx_api import MOUNTAIN_TIME_ZONE
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from Home import configure_page, password_gate, render_sidebar
from syndicate_audit import run_syndicate_audit

configure_page(page_title="Syndicate Audit")
password_gate()
render_sidebar()

st.title("🧾 Syndicate Audit")

st.write(
    """
    This audit compares syndicate or partner notes in preflight booking notes against the
    list of accounts flying on a selected day. It flags when a syndicate partner is also
    flying on that same date.
    """
)

selected_date = st.date_input(
    "Audit date (Mountain Time)",
    value=pd.Timestamp.now(tz=MOUNTAIN_TIME_ZONE).date(),
)

run_check = st.button("Run Syndicate Audit", type="primary")

if not run_check:
    st.info("Choose a date and run the audit to review syndicate bookings.")
    st.stop()

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error(
        "FL3XX API credentials are missing. Please add them to `.streamlit/secrets.toml` under the `fl3xx_api` section."
    )
    st.stop()

try:
    config = build_fl3xx_api_config(dict(api_settings))
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

with st.spinner("Fetching flights and syndicate booking notes..."):
    result = run_syndicate_audit(config, target_date=selected_date)

summary = result.diagnostics

metrics = st.columns(4)
metrics[0].metric("Flights fetched", summary.get("total_flights", 0))
metrics[1].metric("PAX flights", summary.get("pax_flights", 0))
metrics[2].metric("Unique accounts", summary.get("unique_accounts", 0))
metrics[3].metric("Syndicate matches", summary.get("syndicate_matches", 0))

if result.warnings:
    for warning in result.warnings:
        st.warning(warning)

if not result.entries:
    st.success("No syndicate or partner notes were detected for the selected day.")
    st.stop()

rows = []
for entry in result.entries:
    rows.append(
        {
            "Owner Account": entry.owner_account,
            "Syndicate Partner": entry.partner_account,
            "Partner Flying": "Yes" if entry.partner_present else "No",
            "Partner Account": entry.partner_match or "—",
            "Flight": entry.booking_reference,
            "Aircraft": entry.aircraft_type or "—",
            "Workflow": entry.workflow or "—",
            "Tail": entry.tail,
            "Route": entry.route,
            "Note Type": entry.note_type,
            "Booking Notes Line": entry.note_line,
            "Syndicate Tail Type": entry.syndicate_tail_type or "—",
        }
    )

df = pd.DataFrame(rows)


def _normalize_account(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(cleaned.split())


def _pair_key(row: pd.Series) -> str:
    owner = _normalize_account(row["Owner Account"])
    partner_raw = row["Partner Account"] if row["Partner Account"] != "—" else row["Syndicate Partner"]
    partner = _normalize_account(str(partner_raw))
    return "||".join(sorted([owner, partner]))


df["_pair_key"] = df.apply(_pair_key, axis=1)
df["_as_available"] = df["Workflow"].str.contains("as available", case=False, na=False)
conflict_mask = df["Partner Flying"] == "Yes"

conflicts = df[conflict_mask]
cleared = df[~conflict_mask]

if not conflicts.empty:
    conflicts = conflicts.sort_values(
        by=["_pair_key", "_as_available", "Owner Account", "Partner Account"],
        ascending=[True, False, True, True],
    )

st.subheader("⚠️ Syndicate partners flying the same day")
if conflicts.empty:
    st.success("No syndicate partners were booked on the same day as their owner.")
else:
    conflict_pairs = conflicts.groupby("_pair_key")["_as_available"]
    any_available = conflict_pairs.transform("any")
    has_pair = conflict_pairs.transform("size") > 1
    highlight_yellow = (~any_available) & has_pair

    def _highlight_rows(row: pd.Series) -> list[str]:
        if conflicts.loc[row.name, "_as_available"]:
            return ["background-color: #0f5132; color: #d1e7dd;"] * len(row)
        if highlight_yellow.loc[row.name]:
            return ["background-color: #664d03; color: #fff3cd;"] * len(row)
        return [""] * len(row)

    display_conflicts = conflicts.drop(columns=["_pair_key", "_as_available"])
    styled_conflicts = display_conflicts.style.apply(_highlight_rows, axis=1)
    st.dataframe(styled_conflicts, width="stretch")

st.subheader("✅ Syndicate partners not on the schedule")
if cleared.empty:
    st.info("All syndicate partners are also flying on the selected day.")
else:
    st.dataframe(cleared.drop(columns=["_pair_key", "_as_available"]), width="stretch")
