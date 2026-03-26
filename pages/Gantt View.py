from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from cj_maintenance_status import fetch_aircraft_schedule
from flight_leg_utils import FlightDataError, build_fl3xx_api_config, safe_parse_dt
from fl3xx_api import fetch_staff_roster
from gantt_roster_assignment import assign_roster_to_schedule_rows, roster_window_bounds
from reserve_calendar_checker import select_reserve_dates_in_range


UTC = timezone.utc
LANE_DEFINITIONS: List[str] = [
    "Add EMB West",
    "Add EMB East",
    "C-GASL",
    "C-FASV",
    "C-FLAS",
    "C-FJAS",
    "C-FASF",
    "C-GASE",
    "C-GASK",
    "C-GXAS",
    "C-GBAS",
    "C-FSNY",
    "C-FSYX",
    "C-FSBR",
    "C-FSRX",
    "C-FSJR",
    "C-FASQ",
    "C-FSDO",
    "C-FASN",
    "Remove OCS",
    "Add CJ2+ West",
    "Add CJ2+ East",
    "C-FASP",
    "C-FASR",
    "C-FASW",
    "C-FIAS",
    "C-GASR",
    "C-GZAS",
    "Add CJ3+ West",
    "Add CJ3+ East",
    "C-FASY",
    "C-GASW",
    "C-GAAS",
    "C-FNAS",
    "C-GNAS",
    "C-GFFS",
    "C-FSFS",
    "C-GFSX",
    "C-FSFO",
    "C-FSNP",
    "C-FSQX",
    "C-FSFP",
    "C-FSEF",
    "C-FSDN",
    "C-GFSD",
    "C-FSUP",
    "C-FSRY",
    "C-GFSJ",
    "C-GIAS",
    "C-FSVP",
]

MAINTENANCE_TYPES = {"MAINTENANCE", "UNSCHEDULED_MAINTENANCE", "AOG"}
COLOR_MAP = {
    "Client Flight": "#2ca02c",
    "OCS Flight": "#ff7f0e",
    "Maintenance": "#7f7f7f",
    "Note": "#f1c40f",
    "Lane Placeholder": "rgba(0,0,0,0)",
}

VIEW_PRESETS: Dict[str, Dict[str, str]] = {
    "Legacy View": {
        "start_lane": "Add EMB West",
        "end_lane": "C-FASN",
    },
    "CJ View": {
        "start_lane": "Add CJ2+ West",
        "end_lane": "C-FSVP",
    },
    "Full View": {
        "start_lane": LANE_DEFINITIONS[0],
        "end_lane": LANE_DEFINITIONS[-1],
    },
}


configure_page(page_title="Gantt View")
password_gate()
render_sidebar()

st.title("📊 Gantt View")
st.write(
    "Builds a per-tail FL3XX schedule timeline with color-coded activity: "
    "green = client flight, orange = OCS workflow, grey = maintenance, yellow = note."
)


def _to_utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        dt = safe_parse_dt(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _pick_dt(task: Mapping[str, Any], candidates: List[str]) -> Optional[datetime]:
    for key in candidates:
        if key in task:
            parsed = _to_utc(task.get(key))
            if parsed:
                return parsed
    return None




def _pick_airport(task: Mapping[str, Any], candidates: List[str]) -> str:
    for key in candidates:
        value = task.get(key)
        if value not in (None, ""):
            return str(value).strip().upper()
    return ""

def _extract_workflow(task: Mapping[str, Any]) -> str:
    values: List[str] = []

    workflow_custom_name = task.get("workflowCustomName")
    if workflow_custom_name not in (None, ""):
        values.append(str(workflow_custom_name))

    workflow = task.get("workflow")
    if isinstance(workflow, Mapping):
        values.extend(str(v) for v in workflow.values() if v not in (None, ""))
    elif isinstance(workflow, list):
        values.extend(str(item) for item in workflow if item not in (None, ""))
    elif workflow not in (None, ""):
        values.append(str(workflow))

    for key in ("workflowName", "workflowType", "workflowLabel"):
        value = task.get(key)
        if value not in (None, ""):
            values.append(str(value))

    return " | ".join(values)


def _crew_label(value: Any) -> str:
    names = [part.strip() for part in str(value or "").split("|") if part.strip()]
    if not names:
        return ""
    return "<br>".join(names[:2])


def _classify(task: Mapping[str, Any], workflow_text: str) -> str:
    task_id = str(task.get("id") or "").strip().lower()
    task_type = str(task.get("taskType") or "").strip().upper()
    if task_id.startswith("task"):
        if task_type == "NOTE":
            return "Note"
        return "Maintenance"
    if task_id and not task_id.startswith("flight"):
        return "Maintenance"
    if task_type in MAINTENANCE_TYPES or "MAINT" in task_type:
        return "Maintenance"
    if "OCS" in workflow_text.upper():
        return "OCS Flight"
    return "Client Flight"


def _workflow_contains_keyword(workflow_text: str, keyword: str) -> bool:
    return keyword.casefold() in (workflow_text or "").casefold()


def _daily_flight_minutes_by_category(
    day_start: datetime,
    day_end: datetime,
    flights_df: pd.DataFrame,
) -> Dict[str, float]:
    totals = {"OCS Flight": 0.0, "Client Flight": 0.0}
    for _, row in flights_df.iterrows():
        overlap_start = max(day_start, row["start_utc"])
        overlap_end = min(day_end, row["end_utc"])
        if overlap_end <= overlap_start:
            continue
        minutes = (overlap_end - overlap_start).total_seconds() / 60.0
        category = row["category"]
        if category in totals:
            totals[category] += minutes
    return totals


def _build_daily_metrics_table(
    flights_df: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    daily_rows: List[Dict[str, Any]] = []
    day_cursor = start_dt
    while day_cursor < end_dt:
        next_day = min(day_cursor + timedelta(days=1), end_dt)
        day_rows = flights_df[(flights_df["end_utc"] > day_cursor) & (flights_df["start_utc"] < next_day)]
        ocs_pct_rows = day_rows[~day_rows["lane"].astype(str).str.startswith(("Add ", "Remove "), na=False)]

        minute_totals = _daily_flight_minutes_by_category(day_cursor, next_day, ocs_pct_rows)
        ocs_minutes = minute_totals["OCS Flight"]
        client_minutes = minute_totals["Client Flight"]
        overall_minutes = ocs_minutes + client_minutes
        ocs_percent = (ocs_minutes / overall_minutes * 100.0) if overall_minutes else 0.0

        daily_rows.append(
            {
                "date_utc": day_cursor.date(),
                "ocs_pct": ocs_percent,
                "as_available_flights": int(
                    day_rows["workflow"].fillna("").apply(lambda text: _workflow_contains_keyword(text, "as available")).sum()
                ),
                "upgrade_flights": int(
                    day_rows["workflow"].fillna("").apply(lambda text: _workflow_contains_keyword(text, "upgrade")).sum()
                ),
                "interchange_flights": int(
                    day_rows["workflow"].fillna("").apply(lambda text: _workflow_contains_keyword(text, "interchange")).sum()
                ),
            }
        )
        day_cursor = next_day

    return pd.DataFrame(daily_rows)


def _task_to_row(tail: str, lane: str, task: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    start = _pick_dt(
        task,
        [
            "departureDateUTC",
            "departureDateUtc",
            "scheduledOut",
            "offBlock",
            "departureDate",
            "startDateUTC",
            "startDate",
        ],
    )
    end = _pick_dt(
        task,
        [
            "arrivalDateUTC",
            "arrivalDateUtc",
            "scheduledIn",
            "onBlock",
            "arrivalDate",
            "endDateUTC",
            "endDate",
        ],
    )
    if not start or not end:
        return None
    if end < start:
        start, end = end, start
    if end == start:
        end = start + timedelta(minutes=15)

    workflow_text = _extract_workflow(task)
    category = _classify(task, workflow_text)
    return {
        "lane": lane,
        "tail": tail,
        "start_utc": start,
        "end_utc": end,
        "category": category,
        "task_type": str(task.get("taskType") or ""),
        "workflow": workflow_text,
        "notes": str(task.get("notes") or ""),
        "task_id": str(task.get("id") or ""),
        "departure_airport": _pick_airport(task, ["departureAirport", "fromAirport", "departureAirportIcao"]),
        "arrival_airport": _pick_airport(task, ["arrivalAirport", "toAirport", "arrivalAirportIcao"]),
    }


def _pull_gantt_rows(config: Any) -> tuple[List[Dict[str, Any]], List[str], Dict[str, str]]:
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    lane_targets = list(LANE_DEFINITIONS)

    with requests.Session() as session:
        for lane in lane_targets:
            try:
                schedule = fetch_aircraft_schedule(config, lane, session=session)
            except Exception as exc:
                warnings.append(f"{lane}: {exc}")
                continue

            for task in schedule:
                if not isinstance(task, Mapping):
                    continue
                row = _task_to_row(lane, lane, task)
                if row is not None:
                    rows.append(row)

    roster_window = roster_window_bounds()
    roster_meta = {
        "from": roster_window[0].strftime("%Y-%m-%dT%H:%M"),
        "to": roster_window[1].strftime("%Y-%m-%dT%H:%M"),
    }
    try:
        with requests.Session() as roster_session:
            roster_rows = fetch_staff_roster(
                config,
                from_time=roster_window[0],
                to_time=roster_window[1],
                filter_value="STAFF",
                include_flights=True,
                drop_empty_rows=True,
                session=roster_session,
            )
        rows = assign_roster_to_schedule_rows(rows, roster_rows)
    except Exception as exc:
        warnings.append(f"Roster pull failed: {exc}")

    return rows, warnings, roster_meta


if "gantt_rows" not in st.session_state:
    st.session_state["gantt_rows"] = None
    st.session_state["gantt_warnings"] = []
    st.session_state["gantt_roster_meta"] = {}

try:
    api_settings = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
except Exception:
    api_settings = None

if not api_settings:
    st.error("Missing FL3XX API credentials in `.streamlit/secrets.toml` (`[fl3xx_api]`).")
    st.stop()

try:
    config = build_fl3xx_api_config(dict(api_settings))
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

if st.button("Pull Tail Schedules", type="primary"):
    with st.spinner("Pulling schedules for configured tails..."):
        rows, warnings, roster_meta = _pull_gantt_rows(config)
    st.session_state["gantt_rows"] = rows
    st.session_state["gantt_warnings"] = warnings
    st.session_state["gantt_roster_meta"] = roster_meta

rows = st.session_state.get("gantt_rows")
warnings = st.session_state.get("gantt_warnings", [])
roster_meta = st.session_state.get("gantt_roster_meta", {})

if rows is None:
    st.info("Press **Pull Tail Schedules** to load the current schedule timeline.")
    st.stop()

if warnings:
    st.warning("Some tails could not be loaded:")
    for warning in warnings:
        st.caption(f"• {warning}")

if roster_meta:
    st.caption(
        f"Roster enrichment window (UTC): {roster_meta.get('from', '')} to {roster_meta.get('to', '')} (default -10/+5 days)."
    )

if not rows:
    st.info("No schedule entries found for the configured tails.")
    st.stop()

schedule_df = pd.DataFrame(rows).sort_values(["lane", "start_utc"])

control_col1, control_col2, control_col3 = st.columns([1, 1, 2])
with control_col1:
    selected_view = st.selectbox(
        "Chart view",
        options=list(VIEW_PRESETS.keys()),
        index=2,
        help="Choose a lane subset to focus the chart.",
    )
with control_col2:
    show_notes = st.toggle(
        "Show notes",
        value=False,
        help="Notes are hidden by default to reduce visual noise.",
    )

selected_preset = VIEW_PRESETS[selected_view]
start_index = LANE_DEFINITIONS.index(selected_preset["start_lane"])
end_index = LANE_DEFINITIONS.index(selected_preset["end_lane"])
active_lanes = LANE_DEFINITIONS[start_index : end_index + 1]

filtered_schedule_df = schedule_df[schedule_df["lane"].isin(active_lanes)].copy()
if not show_notes:
    filtered_schedule_df = filtered_schedule_df[filtered_schedule_df["category"] != "Note"].copy()

if filtered_schedule_df.empty:
    st.info("No schedule entries match the selected view and note visibility filters.")
    st.stop()

# Notes can overlap, so assign additional sub-lanes per tail for notes only.
note_slot_labels: Dict[str, str] = {}
for lane in active_lanes:
    lane_notes = filtered_schedule_df[
        (filtered_schedule_df["lane"] == lane) & (filtered_schedule_df["category"] == "Note")
    ].copy()
    if lane_notes.empty:
        continue
    lane_notes = lane_notes.sort_values(["start_utc", "end_utc"])
    active_until: List[datetime] = []
    assigned_slots: List[int] = []
    for _, note_row in lane_notes.iterrows():
        start = note_row["start_utc"]
        end = note_row["end_utc"]
        slot_index: Optional[int] = None
        for idx, active_end in enumerate(active_until):
            if active_end <= start:
                slot_index = idx
                active_until[idx] = end
                break
        if slot_index is None:
            active_until.append(end)
            slot_index = len(active_until) - 1
        assigned_slots.append(slot_index)

    lane_notes.loc[:, "note_slot"] = assigned_slots
    for row_index, note_row in lane_notes.iterrows():
        slot = int(note_row["note_slot"])
        label = lane if slot == 0 else f"{lane} (Note {slot + 1})"
        note_slot_labels[row_index] = label

filtered_schedule_df["lane_plot"] = filtered_schedule_df.apply(
    lambda row: note_slot_labels.get(row.name, row["lane"]),
    axis=1,
)

min_start = filtered_schedule_df["start_utc"].min()
max_end = filtered_schedule_df["end_utc"].max()
min_date = min_start.date()
max_date = max_end.date()
today_utc_date = datetime.now(UTC).date()
default_start_date = max(min_date, today_utc_date - timedelta(days=1))
default_end_date = min(max_date, today_utc_date + timedelta(days=5))
if default_end_date < default_start_date:
    default_start_date = min_date
    default_end_date = max_date

with control_col3:
    selected_dates = st.date_input(
        "Zoom window (UTC dates)",
        value=(default_start_date, default_end_date),
        min_value=default_start_date,
        max_value=default_end_date,
        help="Pick start/end dates to zoom into a specific section of the timeline.",
    )

if isinstance(selected_dates, tuple):
    zoom_start_date, zoom_end_date = selected_dates
else:
    zoom_start_date = selected_dates
    zoom_end_date = selected_dates

if zoom_end_date < zoom_start_date:
    st.warning("End date cannot be before start date; using start date for both.")
    zoom_end_date = zoom_start_date

zoom_start_dt = datetime.combine(zoom_start_date, datetime.min.time(), tzinfo=UTC)
zoom_end_dt = datetime.combine(zoom_end_date + timedelta(days=1), datetime.min.time(), tzinfo=UTC)

now_utc = datetime.now(UTC)
plot_rows: List[Dict[str, Any]] = []
for lane in active_lanes:
    plot_rows.append(
        {
            "lane": lane,
            "lane_plot": lane,
            "tail": lane,
            "start_utc": now_utc,
            "end_utc": now_utc + timedelta(minutes=1),
            "category": "Lane Placeholder",
            "task_type": "",
            "workflow": "",
            "notes": "",
            "task_id": "",
        }
    )

plot_df = pd.concat([filtered_schedule_df, pd.DataFrame(plot_rows)], ignore_index=True)
plot_df["crew_label"] = plot_df["crew"].apply(_crew_label)
plot_df.loc[~plot_df["category"].isin(["Client Flight", "OCS Flight"]), "crew_label"] = ""

lane_plot_order: List[str] = []
for lane in active_lanes:
    lane_plot_order.append(lane)
    lane_note_labels = sorted(
        {label for label in note_slot_labels.values() if label.startswith(f"{lane} (Note ")},
        key=lambda label: int(label.split("Note ")[1].rstrip(")")),
    )
    lane_plot_order.extend(lane_note_labels)

fig = px.timeline(
    plot_df,
    x_start="start_utc",
    x_end="end_utc",
    y="lane_plot",
    color="category",
    color_discrete_map=COLOR_MAP,
    category_orders={"lane_plot": lane_plot_order},
    hover_data={
        "tail": True,
        "task_type": True,
        "workflow": True,
        "notes": True,
        "task_id": True,
        "crew": True,
        "positioning": True,
        "roster_flight_id": True,
        "booking_reference": True,
        "flight_status": True,
        "workflow_name": True,
        "pax_number": True,
        "category": True,
        "start_utc": "|%Y-%m-%d %H:%M UTC",
        "end_utc": "|%Y-%m-%d %H:%M UTC",
    },
    text="crew_label",
)
fig.update_yaxes(title="Tail / Row")
fig.update_xaxes(
    title="Time (UTC)",
    range=[zoom_start_dt, zoom_end_dt],
    rangeslider_visible=True,
)
fig.update_layout(
    height=max(900, 40 * len(lane_plot_order)),
    legend_title_text="Activity Type",
    bargap=0.08,
)
fig.update_traces(textposition="inside", insidetextanchor="start", textfont_size=12)

reserve_dates = set(select_reserve_dates_in_range(zoom_start_date, zoom_end_date))
day_cursor = zoom_start_dt
shade_toggle = False
while day_cursor < zoom_end_dt:
    next_day = min(day_cursor + timedelta(days=1), zoom_end_dt)
    current_date = day_cursor.date()
    if current_date in reserve_dates:
        fig.add_vrect(
            x0=day_cursor,
            x1=next_day,
            fillcolor="rgba(99, 179, 237, 0.20)",
            layer="below",
            line_width=0,
        )
    if shade_toggle:
        fig.add_vrect(
            x0=day_cursor,
            x1=next_day,
            fillcolor="rgba(255,255,255,0.035)",
            layer="below",
            line_width=0,
        )
    shade_toggle = not shade_toggle
    day_cursor = next_day

fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="rgba(255,255,255,0.22)")

for trace in fig.data:
    if trace.name == "Lane Placeholder":
        trace.showlegend = False
        trace.opacity = 0

st.plotly_chart(fig, use_container_width=True)
st.caption("Blue day shading indicates 2026 reserve calendar days.")

visible_flights_df = filtered_schedule_df[
    (filtered_schedule_df["category"].isin(["Client Flight", "OCS Flight"]))
    & (filtered_schedule_df["end_utc"] > zoom_start_dt)
    & (filtered_schedule_df["start_utc"] < zoom_end_dt)
].copy()

daily_metrics_df = _build_daily_metrics_table(visible_flights_df, zoom_start_dt, zoom_end_dt)
if not daily_metrics_df.empty:
    st.subheader("Daily flight metrics (UTC)")
    st.caption(
        "OCS% is calculated as OCS flight minutes divided by total flight minutes (OCS + client) for each day in the zoom window, excluding Add/Remove lanes."
    )
    st.dataframe(
        daily_metrics_df.style.format(
            {
                "ocs_pct": "{:.1f}%",
            }
        ),
        width="stretch",
    )

with st.expander("Raw activity data"):
    st.dataframe(
        filtered_schedule_df[["lane", "tail", "start_utc", "end_utc", "category", "task_type", "workflow", "crew", "positioning", "roster_flight_id", "booking_reference", "flight_status", "workflow_name", "pax_number", "notes"]],
        width="stretch",
    )
