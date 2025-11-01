"""Streamlit entrypoint for the negotiation-aware scheduling prototype."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone, timedelta

from collections import Counter
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from core.airports import load_airports
from core.neg_scheduler import LeverPolicy, NegotiationScheduler
from core.neg_scheduler.model import _class_compatible
from core.reposition import build_reposition_matrix
from flight_leg_utils import FlightDataError
from integrations.fl3xx_adapter import NegotiationData, fetch_negotiation_data, get_demo_data


_FL3XX_SNAPSHOT_KEY = "negotiation_optimizer_fl3xx_snapshot"


def _store_fl3xx_snapshot(data: NegotiationData, day: date) -> None:
    st.session_state[_FL3XX_SNAPSHOT_KEY] = {
        "data": data,
        "day": day,
        "fetched_at": datetime.now(timezone.utc),
    }


def _get_fl3xx_snapshot() -> dict[str, object] | None:
    snapshot = st.session_state.get(_FL3XX_SNAPSHOT_KEY)
    if isinstance(snapshot, dict) and isinstance(snapshot.get("data"), NegotiationData):
        return snapshot
    return None


def _apply_policy_to_snapshot(data: NegotiationData, policy: LeverPolicy) -> NegotiationData:
    adjusted_flights = [
        replace(
            flight,
            shift_cost_per_min=(
                policy.cost_per_min_shift + 1 if flight.current_tail_id else policy.cost_per_min_shift
            ),
        )
        for flight in data.flights
    ]
    return replace(data, flights=adjusted_flights)


TAIL_SCHEDULE_ORDER: tuple[str, ...] = (
    "CGASL",
    "CFASV",
    "CFLAS",
    "CFJAS",
    "CFASF",
    "CGASE",
    "CGASK",
    "CGXAS",
    "CGBAS",
    "CFSNY",
    "CFSYX",
    "CFSBR",
    "CFSRX",
    "CFSJR",
    "CFASQ",
    "CFSDO",
    "CFASP",
    "CFASR",
    "CFASW",
    "CFIAS",
    "CGASR",
    "CGZAS",
    "CFASY",
    "CGASW",
    "CGAAS",
    "CFNAS",
    "CGNAS",
    "CGFFS",
    "CFSFS",
    "CGFSX",
    "CFSFO",
    "CFSNP",
    "CFSQX",
    "CFSFP",
    "CFSEF",
    "CFSDN",
    "CGFSD",
    "CFSUP",
    "CFSRY",
    "CGFSJ",
)


def _format_leg_rows(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    formatted_rows: list[dict[str, object]] = []
    for row in rows:
        formatted_rows.append(
            {
                "Tail": row.get("tail"),
                "Tail (normalized)": row.get("tail_normalized"),
                "Account": row.get("accountName") or row.get("account"),
                "Workflow": row.get("workflowCustomName") or row.get("workflow"),
                "Flight Type": row.get("flightType"),
                "Booking": row.get("bookingReference") or row.get("bookingId"),
                "From": row.get("departure_airport"),
                "To": row.get("arrival_airport"),
                "Departure (UTC)": row.get("dep_time"),
                "Arrival (UTC)": row.get("arrival_time"),
                "Leg ID": row.get("leg_id"),
                "Flight ID": row.get("flightId"),
                "PAX": row.get("paxNumber"),
            }
        )

    return pd.DataFrame(formatted_rows)


def _lever_options(policy: LeverPolicy) -> list[dict[str, object]]:
    return [
        {
            "option": "+30m owner shift",
            "penalty": policy.cost_per_min_shift * 30,
            "notes": "Often unlocks tight turnarounds with minimal impact.",
        },
        {
            "option": "+60m owner shift",
            "penalty": policy.cost_per_min_shift * 60,
            "notes": "Larger buffer to align tails without swaps.",
        },
        {
            "option": "Tail swap within class",
            "penalty": 120,
            "notes": "No owner impact; may add one empty reposition leg.",
        },
        {
            "option": "Outsource leg",
            "penalty": policy.outsource_cost,
            "notes": "Zero internal perturbation; highest direct cost.",
        },
    ]


def _render_solution_details(
    solution: dict[str, object],
    window_start_utc: datetime,
    policy: LeverPolicy,
) -> None:
    st.subheader("Assigned schedule")
    assigned_df: pd.DataFrame = solution.get("assigned", pd.DataFrame())
    reposition_df: pd.DataFrame = solution.get("reposition", pd.DataFrame())
    if assigned_df.empty:
        st.write("— No internal assignments —")
    else:
        table_tab, gantt_tab = st.tabs(["Table", "Gantt"])
        with table_tab:
            st.dataframe(assigned_df, use_container_width=True)
        with gantt_tab:
            gantt_df = _build_gantt_solution(assigned_df, reposition_df)
            if gantt_df.empty:
                st.info("Not enough timing data to display the Gantt view.")
            else:
                draw_gantt(
                    gantt_df,
                    color_by="assignment_source",
                    color_palette={
                        "Added from unscheduled": "#d62728",
                        "Previously scheduled": "#1f77b4",
                        "Reposition leg": "#ffbf00",
                    },
                    window_start=window_start_utc,
                )

    st.subheader("Unscheduled / outsourced")
    outsourced_df: pd.DataFrame = solution.get("outsourced", pd.DataFrame())
    if outsourced_df.empty:
        st.write("— All legs scheduled internally —")
    else:
        st.dataframe(outsourced_df, use_container_width=True)

    st.subheader("Dropped positioning legs")
    skipped_df: pd.DataFrame = solution.get("skipped", pd.DataFrame())
    if skipped_df.empty:
        st.write("— No positioning legs dropped —")
    else:
        st.dataframe(skipped_df, use_container_width=True)

    objective_value = solution.get("objective", math.inf)
    if math.isfinite(objective_value):
        st.caption(f"Objective value: {objective_value:.0f}")
    else:
        st.caption("Objective value: unavailable")

    if not outsourced_df.empty:
        st.markdown("### 🔧 Lever suggestions")
        for _, row in outsourced_df.iterrows():
            st.write(
                f"**{row['flight']} {row['origin']}→{row['dest']} (Owner {row['owner']})**"
            )
            options = _lever_options(policy)
            options_df = pd.DataFrame(sorted(options, key=lambda d: d["penalty"]))
            st.dataframe(options_df, use_container_width=True)

            message = (
                f"Hi Owner {row['owner']},\n\n"
                "Could you slide departure by 30 minutes? This avoids a tail swap and keeps crew within duty limits.\n"
                "We can offer a small courtesy credit.\n\n"
                "— Dispatch\n"
            )
            st.code(message)


def _parse_timestamp(value: object) -> pd.Timestamp | None:
    if value in (None, ""):
        return None

    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _extract_duration_minutes(row: dict[str, object], fallback: int = 60) -> int:
    duration_keys = (
        "duration_min",
        "durationMinutes",
        "duration",
        "scheduledBlockMinutes",
        "scheduledBlockTime",
        "estimatedBlockTime",
        "blockTime",
        "block_time",
        "flightTime",
        "flight_time",
    )

    for key in duration_keys:
        value = row.get(key)
        if value in (None, ""):
            continue
        minutes = pd.to_numeric(value, errors="coerce")
        if pd.isna(minutes):
            continue
        minutes_int = int(round(float(minutes)))
        if minutes_int > 0:
            return minutes_int

    return fallback


def _build_gantt_schedule(rows: list[dict[str, object]], window_start: object | None) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    window_ref = _parse_timestamp(window_start)
    gantt_rows: list[dict[str, object]] = []

    arrival_keys = (
        "arrival_time",
        "arrivalTimeUtc",
        "arrivalScheduledUtc",
        "arrivalActualUtc",
        "arrivalUtc",
        "onBlockUtc",
    )

    for row in rows:
        tail = row.get("tail_normalized") or row.get("tail")
        dep_ts = _parse_timestamp(row.get("dep_time"))
        if not tail or dep_ts is None:
            continue

        if window_ref is not None:
            start_min = int((dep_ts - window_ref).total_seconds() // 60)
        else:
            start_min = dep_ts.hour * 60 + dep_ts.minute

        arr_ts = None
        for key in arrival_keys:
            arr_ts = _parse_timestamp(row.get(key))
            if arr_ts is not None:
                break

        if arr_ts is not None:
            duration = max(1, int((arr_ts - dep_ts).total_seconds() // 60))
        else:
            duration = max(1, _extract_duration_minutes(row))

        gantt_rows.append(
            {
                "tail": str(tail),
                "start_min": max(0, start_min),
                "duration_min": duration,
                "end_min": max(0, start_min) + duration,
                "origin": row.get("departure_airport"),
                "dest": row.get("arrival_airport"),
                "workflow": row.get("workflowCustomName") or row.get("workflow"),
                "account": row.get("accountName") or row.get("account"),
            }
        )

    return pd.DataFrame(gantt_rows)


def _build_gantt_solution(
    df: pd.DataFrame, reposition_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    if df.empty and (reposition_df is None or reposition_df.empty):
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if not df.empty:
        gantt_df = df.copy()
        if "end_min" not in gantt_df.columns and "duration_min" in gantt_df.columns:
            gantt_df["end_min"] = gantt_df["start_min"] + gantt_df["duration_min"]
        gantt_df["tail"] = gantt_df["tail"].astype(str)
        if "original_tail" in gantt_df.columns:
            gantt_df["was_unscheduled"] = gantt_df["original_tail"].isna() | (
                gantt_df["original_tail"].astype(str).str.len() == 0
            )
            gantt_df["assignment_source"] = gantt_df["was_unscheduled"].map(
                {
                    True: "Added from unscheduled",
                    False: "Previously scheduled",
                }
            )
        frames.append(gantt_df)

    if reposition_df is not None and not reposition_df.empty:
        reposition_gantt = reposition_df.copy()
        if "end_min" not in reposition_gantt.columns and "duration_min" in reposition_gantt.columns:
            reposition_gantt["end_min"] = (
                reposition_gantt["start_min"] + reposition_gantt["duration_min"]
            )
        reposition_gantt["tail"] = reposition_gantt["tail"].astype(str)
        reposition_gantt["assignment_source"] = "Reposition leg"
        reposition_gantt["was_unscheduled"] = False
        frames.append(reposition_gantt)

    return pd.concat(frames, ignore_index=True)


def draw_gantt(
    data: pd.DataFrame,
    *,
    tail_col: str = "tail",
    start_col: str = "start_min",
    dur_col: str = "duration_min",
    end_col: str | None = "end_min",
    label_from: str | None = "origin",
    label_to: str | None = "dest",
    color_by: str | None = None,
    color_palette: dict[object, str] | None = None,
    minutes_range: tuple[int, int] = (0, 24 * 60),
    min_label_width: int = 50,
    window_start: object | None = None,
) -> None:
    required = {tail_col, start_col}
    if not required.issubset(data.columns):
        st.warning(
            f"Gantt missing required columns: {sorted(required - set(data.columns))}"
        )
        return

    df = data.copy()
    if end_col and end_col in df.columns:
        df["_dur_min"] = (df[end_col] - df[start_col]).clip(lower=1)
    else:
        if dur_col not in df.columns:
            st.warning("Provide either end_min or duration_min.")
            return
        df["_dur_min"] = df[dur_col].clip(lower=1)

    lo, hi = minutes_range
    df = df[(df[start_col] < hi) & ((df[start_col] + df["_dur_min"]) > lo)]
    if df.empty:
        st.info("No legs in selected time window.")
        return

    window_ref = _parse_timestamp(window_start)

    order_lookup = {tail: idx for idx, tail in enumerate(TAIL_SCHEDULE_ORDER)}

    def _tail_sort_key(tail: str) -> tuple[int, int | str]:
        tail_upper = tail.upper()
        order_idx = order_lookup.get(tail_upper)
        if order_idx is not None:
            return (0, order_idx)
        return (1, tail_upper)

    tail_values = df[tail_col].dropna()
    tail_strings = [str(value).strip() for value in tail_values]
    tails_sorted = sorted(set(tail_strings), key=_tail_sort_key)
    ymap = {t: i for i, t in enumerate(tails_sorted)}

    colors = None
    legend_info: dict[object, str] = {}
    if color_by and color_by in df.columns:
        color_values = df[color_by]
        if color_palette:
            mapped_colors = [
                color_palette.get(value, color_palette.get(str(value)))
                for value in color_values
            ]
            if all(color is not None for color in mapped_colors):
                colors = mapped_colors
                legend_info = {
                    value: color_palette.get(value, color_palette.get(str(value)))
                    for value in pd.unique(color_values)
                }
        if colors is None:
            values, uniques = pd.factorize(color_values.astype(str))
            cmap = plt.get_cmap("tab20")
            colors = [cmap(v % 20) for v in values]
            legend_info = {label: cmap(i % 20) for i, label in enumerate(uniques)}

    fig_h = max(4, 0.4 * len(tails_sorted))
    fig, ax = plt.subplots(figsize=(12, fig_h))
    for i, record in df.reset_index(drop=True).iterrows():
        tail_label = str(record[tail_col]).strip()
        if tail_label not in ymap:
            continue
        y = ymap[tail_label]
        x = float(record[start_col])
        width = float(record["_dur_min"])
        kwargs: dict[str, object] = {}
        if colors is not None:
            kwargs["facecolors"] = colors[i]
        ax.broken_barh([(x, width)], (y - 0.4, 0.8), **kwargs)

        if (
            width >= min_label_width
            and label_from
            and label_from in df.columns
            and label_to
            and label_to in df.columns
        ):
            ax.text(
                x + width / 2,
                y,
                f"{record[label_from]}→{record[label_to]}",
                ha="center",
                va="center",
                fontsize=8,
            )

    ax.set_yticks(range(len(tails_sorted)), tails_sorted)
    ax.invert_yaxis()
    ax.set_xlim(lo, hi)
    if window_ref is not None:
        tick_step = 60
        tick_start = (lo // tick_step) * tick_step
        if tick_start > lo:
            tick_start -= tick_step
        tick_end = ((hi + tick_step - 1) // tick_step) * tick_step
        xticks = list(range(tick_start, tick_end + tick_step, tick_step))
        ax.set_xticks(xticks)
        ax.set_xticklabels(
            [
                (window_ref + timedelta(minutes=int(offset))).strftime("%Y-%m-%d %H:%MZ")
                for offset in xticks
            ]
        )
        ax.tick_params(axis="x", labelrotation=45)
        ax.set_xlabel("UTC time")
    else:
        ax.set_xlabel("Minutes from window start")
    ax.set_title("Per-tail Gantt (single day)")
    ax.grid(True, axis="x", linestyle=":", linewidth=0.5)
    if legend_info:
        handles = [
            Patch(facecolor=color, edgecolor=color, label=str(label))
            for label, color in legend_info.items()
        ]
        ax.legend(handles=handles, title=color_by)
    st.pyplot(fig, clear_figure=True)


@st.cache_data(show_spinner=False)
def airports_index() -> dict[str, dict[str, object]]:
    """Load the airport reference table used for reposition calculations."""

    reference_path = Path(__file__).resolve().parents[2] / "Airport TZ.txt"
    try:
        return load_airports(reference_path)
    except FileNotFoundError:
        return {}
    except Exception:
        # Streamlit will surface detailed errors when the solver runs; keep the
        # cache resilient here so the UI can continue to operate.
        return {}


def render_page() -> None:
    configure_page(page_title="Negotiation Optimizer")
    password_gate()
    render_sidebar()
    st.title("🧩 Negotiation-Aware Scheduler")

    snapshot = _get_fl3xx_snapshot()
    fetch_requested = False

    with st.sidebar:
        st.header("Inputs")
        dataset = st.selectbox("Data source", ("Demo", "FL3XX"), index=1)
        schedule_day = st.date_input("Schedule day (08Z window)", date.today())
        turn_min = st.slider("Turn buffer (min)", 0, 120, 45, 5)
        max_plus = st.slider("Max shift + (min)", 0, 240, 30, 5)
        max_minus = st.slider("Max shift - (min)", 0, 180, 30, 5)
        cost_per_min = st.slider("Cost per shifted minute", 0, 10, 5)
        reposition_cost = st.slider("Reposition cost / min", 0, 10, 2, 1)
        outsource_cost = st.number_input(
            "Outsource cost proxy", 0, 200_000, 50_000, 500
        )
        allow_pos_skips = st.checkbox(
            "Allow dropping POS legs",
            value=True,
            help="If unchecked, positioning legs must be scheduled or outsourced like PAX legs.",
        )
        pos_skip_cost = st.number_input(
            "Penalty for dropping POS leg", 0, 200_000, 5_000, 250
        )
        pax_skip_cost = st.number_input(
            "Penalty for unscheduled PAX leg", 0, 2_000_000, 1_000_000, 5_000
        )
        enforce_max_day = st.checkbox(
            "Enforce max duty day length", value=False, help="Caps usable duty span to 765 minutes."
        )
        max_day_length_min = 765 if enforce_max_day else None

        if dataset == "FL3XX":
            fetch_requested = st.button("Fetch FL3XX data", use_container_width=True)
            snapshot_day = snapshot.get("day") if snapshot else None
            if isinstance(snapshot_day, date):
                if snapshot_day == schedule_day:
                    fetched_at = snapshot.get("fetched_at") if snapshot else None
                    if isinstance(fetched_at, datetime):
                        st.caption(
                            "Loaded FL3XX snapshot fetched at %s"
                            % fetched_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
                        )
                    else:
                        st.caption("Loaded cached FL3XX snapshot for this day.")
                else:
                    st.caption(
                        "Cached data is for %s. Fetch to load %s."
                        % (snapshot_day.isoformat(), schedule_day.isoformat())
                    )

    fl3xx_settings: dict[str, object] | None = None
    snapshot_day = snapshot.get("day") if snapshot else None
    snapshot_valid = isinstance(snapshot_day, date) and snapshot_day == schedule_day

    if dataset == "FL3XX":
        require_settings = fetch_requested
        if require_settings:
            try:
                secrets_section = st.secrets.get("fl3xx_api")  # type: ignore[attr-defined]
            except Exception:
                secrets_section = None

            if not secrets_section:
                st.error(
                    "Add FL3XX credentials to `.streamlit/secrets.toml` under `[fl3xx_api]` to fetch flights."
                )
                return

            try:
                fl3xx_settings = dict(secrets_section)
            except (TypeError, ValueError):
                st.error("FL3XX API secrets must be provided as key/value pairs.")
                return

    policy = LeverPolicy(
        max_shift_plus_min=max_plus,
        max_shift_minus_min=max_minus,
        cost_per_min_shift=cost_per_min,
        outsource_cost=outsource_cost,
        turn_min=turn_min,
        reposition_cost_per_min=reposition_cost,
        max_day_length_min=max_day_length_min,
        pos_skip_cost=pos_skip_cost,
        pax_skip_cost=pax_skip_cost,
        allow_pos_skips=allow_pos_skips,
    )

    legs: list = []
    tails: list = []
    schedule_rows: list[dict[str, object]] = []
    add_line_rows: list[dict[str, object]] = []
    fetch_metadata: dict[str, object] = {}
    window_start_utc: object | None = None
    window_end_utc: object | None = None
    selected_unscheduled_ids: set[str] | None = None

    if dataset == "Demo":
        legs, tails = get_demo_data()
    else:
        if fetch_requested:
            with st.spinner("Fetching FL3XX flights…"):
                try:
                    data = fetch_negotiation_data(
                        schedule_day, settings=fl3xx_settings, policy=policy
                    )
                except FlightDataError as exc:
                    st.error(f"Unable to load FL3XX flights: {exc}")
                    return
            _store_fl3xx_snapshot(data, schedule_day)
            snapshot = _get_fl3xx_snapshot()
            snapshot_day = snapshot.get("day") if snapshot else None
            snapshot_valid = isinstance(snapshot_day, date) and snapshot_day == schedule_day

        if not snapshot_valid or not snapshot:
            st.info('Press "Fetch FL3XX data" to load flights for the selected window.')
            return

        raw_data = snapshot.get("data")
        if not isinstance(raw_data, NegotiationData):
            st.error("Cached FL3XX snapshot is invalid. Please fetch the data again.")
            return

        data = _apply_policy_to_snapshot(raw_data, policy)

        legs = data.flights
        tails = data.tails
        schedule_rows = data.scheduled_rows
        add_line_rows = data.unscheduled_rows
        fetch_metadata = data.metadata

        st.subheader("FL3XX window snapshot")
        fetched_at = snapshot.get("fetched_at") if snapshot else None
        if isinstance(fetched_at, datetime):
            st.caption(
                "Snapshot fetched %s (UTC)."
                % fetched_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
            )
        col1, col2, col3 = st.columns(3)
        col1.metric("Scheduled legs", fetch_metadata.get("scheduled_count", len(schedule_rows)))
        col2.metric("Add-line legs", fetch_metadata.get("unscheduled_count", len(add_line_rows)))
        col3.metric("Flights sent to solver", len(legs))

        window_start_utc = fetch_metadata.get("window_start_utc")
        window_end_utc = fetch_metadata.get("window_end_utc")
        if window_start_utc and window_end_utc:
            st.caption(f"Window: {window_start_utc} → {window_end_utc} (UTC)")

        skipped_sched = fetch_metadata.get("skipped_scheduled") or []
        skipped_unsched = fetch_metadata.get("skipped_unscheduled") or []
        if skipped_sched or skipped_unsched:
            issues: list[str] = []
            if skipped_sched:
                issues.append(f"{len(skipped_sched)} scheduled")
            if skipped_unsched:
                issues.append(f"{len(skipped_unsched)} add-line")
            st.warning(
                "Skipped %s leg(s) due to missing required timing data."
                % " and ".join(issues)
            )
            with st.expander("View skipped leg identifiers"):
                if skipped_sched:
                    st.markdown("**Scheduled**")
                    st.write("\n".join(skipped_sched))
                if skipped_unsched:
                    st.markdown("**Add-line**")
                    st.write("\n".join(skipped_unsched))

        scheduled_df = _format_leg_rows(schedule_rows)
        if not scheduled_df.empty:
            st.markdown("**Scheduled legs (locked to current tails)**")
            table_tab, gantt_tab = st.tabs(["Table", "Gantt"])
            with table_tab:
                st.dataframe(scheduled_df, use_container_width=True)
            with gantt_tab:
                gantt_df = _build_gantt_schedule(schedule_rows, window_start_utc)
                if gantt_df.empty:
                    st.info("Not enough timing data to display the Gantt view.")
                else:
                    draw_gantt(
                        gantt_df,
                        color_by="workflow",
                        minutes_range=(0, 24 * 60),
                        window_start=window_start_utc,
                    )
        else:
            st.info("No scheduled legs detected for the selected window.")

        unscheduled_df = _format_leg_rows(add_line_rows)
        if not unscheduled_df.empty:
            st.markdown("**Add-line demand (solver targets)**")

            def _row_identifier(row: dict[str, object]) -> str:
                for key in ("leg_id", "flightId", "bookingReference", "bookingId", "id"):
                    value = row.get(key)
                    if value not in (None, ""):
                        return str(value)
                return f"LEG-{hash(frozenset(row.items())) & 0xFFFF:04X}"

            unscheduled_df = unscheduled_df.copy()
            unscheduled_df.index = [_row_identifier(row) for row in add_line_rows]
            unscheduled_df.index.name = "Flight"

            selector_df = unscheduled_df.copy()
            selector_df.insert(0, "Solve?", True)

            edited_df = st.data_editor(
                selector_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Solve?": st.column_config.CheckboxColumn(
                        "Solve?",
                        help="Uncheck to skip this add-line request when running the solver.",
                        default=True,
                    )
                },
            )

            selected_unscheduled_ids = {
                str(idx)
                for idx, include in edited_df["Solve?"].items()
                if bool(include)
            }

            st.caption(
                f"{len(selected_unscheduled_ids)} of {len(edited_df)} add-line legs selected for solving."
            )
        else:
            st.info("No add-line legs found in the selected window.")

    flights = legs

    st.markdown("### 60-second diagnostics")
    # 1) Basic counts
    st.write(f"n_flights={len(flights)}  n_tails={len(tails)}")

    # 2) Fleet classes present
    st.write("flight classes:", Counter([f.fleet_class for f in flights]))
    st.write("tail classes:", Counter([t.fleet_class for t in tails]))

    # 3) Tail availability sanity
    bad_avail = [t for t in tails if t.available_to_min <= 0 or t.available_from_min >= t.available_to_min]
    if bad_avail:
        st.error(
            f"TAIL AVAIL ISSUE → {[ (t.id, t.available_from_min, t.available_to_min) for t in bad_avail ]}"
        )

    # 4) For first few add-line legs, list compatible tails
    adds = [f for f in flights if not f.current_tail_id]
    for f in adds[:5]:
        compatible = [
            t.id for t in tails if _class_compatible(f.fleet_class, t.fleet_class)
        ]
        tail_list = compatible[:10]
        suffix = "..." if len(compatible) > 10 else ""
        st.write(
            f"ADD {f.id}: class={f.fleet_class} → compatible tails: {tail_list}{suffix}"
        )

    # 5) Check per-flight caps (are scheduled legs allowed tiny moves?)
    sched = [f for f in flights if f.current_tail_id]
    caps = [(f.id, f.shift_minus_cap, f.shift_plus_cap) for f in sched[:10]]
    st.write("sample scheduled caps (−/+):", caps)

    # 6) Show policy numbers in use
    st.write("turn_min:", policy.turn_min, " outsource_cost:", policy.outsource_cost)

    if st.button("Run Solver", type="primary"):
        solver_flights = legs
        if selected_unscheduled_ids is not None:
            solver_flights = [
                flight
                for flight in legs
                if not (
                    flight.current_tail_id is None
                    and flight.allow_tail_swap
                    and flight.id not in selected_unscheduled_ids
                )
            ]

        tail_ids = {tail.id for tail in tails}
        missing_refs = [
            (flight.id, flight.current_tail_id)
            for flight in solver_flights
            if flight.current_tail_id and flight.current_tail_id not in tail_ids
        ]
        if missing_refs:
            st.error(
                "Flights reference unknown tails: "
                + str(missing_refs[:6])
                + ("…" if len(missing_refs) > 6 else "")
            )

        tails_by_id = {tail.id: tail for tail in tails}
        incompatible_refs = [
            (
                flight.id,
                flight.current_tail_id,
                flight.fleet_class,
                tails_by_id[flight.current_tail_id].fleet_class,
            )
            for flight in solver_flights
            if flight.current_tail_id in tails_by_id
            and not _class_compatible(
                flight.fleet_class, tails_by_id[flight.current_tail_id].fleet_class
            )
        ]
        if incompatible_refs:
            st.warning(
                "Scheduled flights have incompatible current tails: "
                + str(incompatible_refs[:6])
                + ("…" if len(incompatible_refs) > 6 else "")
            )
        airport_codes = {
            code
            for flight in solver_flights
            for code in (flight.origin, flight.dest)
            if code
        }
        airport_catalog = airports_index()
        relevant_airports = {
            code: airport_catalog[code] for code in airport_codes if code in airport_catalog
        }
        missing_codes = sorted(code for code in airport_codes if code not in airport_catalog)
        if missing_codes:
            display_limit = 6
            suffix = "…" if len(missing_codes) > display_limit else ""
            listed = ", ".join(missing_codes[:display_limit])
            st.info(
                "Missing coordinates for airports: " + listed + suffix + 
                ". Using fallback reposition penalties."
            )
        try:
            repo_matrix = build_reposition_matrix(
                solver_flights, relevant_airports or airport_catalog
            )
            repo_rows = len(repo_matrix)
            repo_cols = len(repo_matrix[0]) if repo_matrix else 0
            if repo_rows != len(solver_flights) or any(
                len(row) != len(solver_flights) for row in repo_matrix
            ):
                st.error(
                    "Reposition matrix dimensions do not match the flights being solved."
                )
                return

            scheduler = NegotiationScheduler(
                solver_flights, tails, policy, reposition_min=repo_matrix
            )
            status, solutions = scheduler.solve(top_n=5)
        except Exception as exc:  # pragma: no cover - surfaced in UI
            st.error(f"Solver error: {exc}")
            return

        if not solutions:
            st.warning("No feasible schedules found by the solver.")
            return

        tab_labels = []
        for idx, solution in enumerate(solutions):
            objective_value = solution.get("objective", math.inf)
            if math.isfinite(objective_value):
                label = f"Option {idx + 1} · cost {objective_value:.0f}"
            else:
                label = f"Option {idx + 1}"
            tab_labels.append(label)

        solution_tabs = st.tabs(tab_labels)
        for tab, solution in zip(solution_tabs, solutions):
            with tab:
                _render_solution_details(solution, window_start_utc, policy)


if __name__ == "__main__":  # pragma: no cover - Streamlit executes as a script
    render_page()
