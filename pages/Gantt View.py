from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from cj_maintenance_status import fetch_aircraft_schedule
from flight_leg_utils import FlightDataError, build_fl3xx_api_config, safe_parse_dt


UTC = timezone.utc
TAIL_PATTERN = re.compile(r"^C-[A-Z0-9]{4}$")
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


def _extract_workflow(task: Mapping[str, Any]) -> str:
    workflow = task.get("workflow")
    if isinstance(workflow, Mapping):
        return " ".join(str(v) for v in workflow.values() if v not in (None, ""))
    if isinstance(workflow, list):
        return " ".join(str(item) for item in workflow if item not in (None, ""))
    if workflow not in (None, ""):
        return str(workflow)
    for key in ("workflowName", "workflowType", "workflowLabel"):
        if task.get(key) not in (None, ""):
            return str(task.get(key))
    return ""


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
    }


def _pull_gantt_rows(config: Any) -> tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    tails = [lane for lane in LANE_DEFINITIONS if TAIL_PATTERN.match(lane)]
    lane_for_tail = {tail: tail for tail in tails}

    with requests.Session() as session:
        for tail in tails:
            try:
                schedule = fetch_aircraft_schedule(config, tail, session=session)
            except Exception as exc:
                warnings.append(f"{tail}: {exc}")
                continue

            for task in schedule:
                if not isinstance(task, Mapping):
                    continue
                row = _task_to_row(tail, lane_for_tail[tail], task)
                if row is not None:
                    rows.append(row)

    return rows, warnings


if "gantt_rows" not in st.session_state:
    st.session_state["gantt_rows"] = None
    st.session_state["gantt_warnings"] = []

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
        rows, warnings = _pull_gantt_rows(config)
    st.session_state["gantt_rows"] = rows
    st.session_state["gantt_warnings"] = warnings

rows = st.session_state.get("gantt_rows")
warnings = st.session_state.get("gantt_warnings", [])

if rows is None:
    st.info("Press **Pull Tail Schedules** to load the current schedule timeline.")
    st.stop()

if warnings:
    st.warning("Some tails could not be loaded:")
    for warning in warnings:
        st.caption(f"• {warning}")

if not rows:
    st.info("No schedule entries found for the configured tails.")
    st.stop()

schedule_df = pd.DataFrame(rows).sort_values(["lane", "start_utc"])

# Notes can overlap, so assign additional sub-lanes per tail for notes only.
note_slot_labels: Dict[str, str] = {}
for lane in LANE_DEFINITIONS:
    lane_notes = schedule_df[(schedule_df["lane"] == lane) & (schedule_df["category"] == "Note")].copy()
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

schedule_df["lane_plot"] = schedule_df.apply(
    lambda row: note_slot_labels.get(row.name, row["lane"]),
    axis=1,
)

now_utc = datetime.now(UTC)
plot_rows: List[Dict[str, Any]] = []
for lane in LANE_DEFINITIONS:
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

plot_df = pd.concat([schedule_df, pd.DataFrame(plot_rows)], ignore_index=True)

lane_plot_order: List[str] = []
for lane in LANE_DEFINITIONS:
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
        "category": True,
        "start_utc": "|%Y-%m-%d %H:%M UTC",
        "end_utc": "|%Y-%m-%d %H:%M UTC",
    },
)
fig.update_yaxes(title="Tail / Row")
fig.update_xaxes(
    title="Time (UTC)",
    range=[now_utc - timedelta(hours=2), now_utc + timedelta(days=3)],
    rangeslider_visible=True,
)
fig.update_layout(height=max(750, 24 * len(lane_plot_order)), legend_title_text="Activity Type")
for trace in fig.data:
    if trace.name == "Lane Placeholder":
        trace.showlegend = False
        trace.opacity = 0

st.plotly_chart(fig, use_container_width=True)

with st.expander("Raw activity data"):
    st.dataframe(
        schedule_df[["lane", "tail", "start_utc", "end_utc", "category", "task_type", "workflow", "notes"]],
        width="stretch",
    )
