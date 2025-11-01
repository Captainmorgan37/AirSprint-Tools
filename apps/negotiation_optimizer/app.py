"""Streamlit entrypoint for the negotiation-aware scheduling prototype."""

from __future__ import annotations

from datetime import date

from collections import Counter

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from core.neg_scheduler import LeverPolicy, NegotiationScheduler
from core.neg_scheduler.model import _class_compatible
from flight_leg_utils import FlightDataError
from integrations.fl3xx_adapter import NegotiationData, fetch_negotiation_data, get_demo_data


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


def _build_gantt_solution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    gantt_df = df.copy()
    if "end_min" not in gantt_df.columns and "duration_min" in gantt_df.columns:
        gantt_df["end_min"] = gantt_df["start_min"] + gantt_df["duration_min"]
    gantt_df["tail"] = gantt_df["tail"].astype(str)
    return gantt_df


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
    minutes_range: tuple[int, int] = (0, 24 * 60),
    min_label_width: int = 50,
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

    tails_sorted = sorted(df[tail_col].astype(str).unique())
    ymap = {t: i for i, t in enumerate(tails_sorted)}

    colors = None
    if color_by and color_by in df.columns:
        values, _ = pd.factorize(df[color_by].astype(str))
        cmap = plt.get_cmap("tab20")
        colors = [cmap(v % 20) for v in values]

    fig_h = max(4, 0.4 * len(tails_sorted))
    fig, ax = plt.subplots(figsize=(12, fig_h))
    for i, record in df.reset_index(drop=True).iterrows():
        y = ymap[str(record[tail_col])]
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
    ax.set_xlim(lo, hi)
    ax.set_xlabel("Minutes from window start")
    ax.set_title("Per-tail Gantt (single day)")
    ax.grid(True, axis="x", linestyle=":", linewidth=0.5)
    st.pyplot(fig, clear_figure=True)


def render_page() -> None:
    configure_page(page_title="Negotiation Optimizer")
    password_gate()
    render_sidebar()
    st.title("🧩 Negotiation-Aware Scheduler")

    with st.sidebar:
        st.header("Inputs")
        dataset = st.selectbox("Data source", ("Demo", "FL3XX"), index=0)
        schedule_day = st.date_input("Schedule day (08Z window)", date.today())
        turn_min = st.slider("Turn buffer (min)", 0, 120, 30, 5)
        max_plus = st.slider("Max shift + (min)", 0, 240, 30, 5)
        max_minus = st.slider("Max shift - (min)", 0, 180, 30, 5)
        cost_per_min = st.slider("Cost per shifted minute", 0, 10, 2)
        outsource_cost = st.number_input("Outsource cost proxy", 0, 10000, 1800, 50)

    fl3xx_settings: dict[str, object] | None = None
    if dataset == "FL3XX":
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
    )

    legs: list = []
    tails: list = []
    schedule_rows: list[dict[str, object]] = []
    add_line_rows: list[dict[str, object]] = []
    fetch_metadata: dict[str, object] = {}
    selected_unscheduled_ids: set[str] | None = None

    if dataset == "Demo":
        legs, tails = get_demo_data()
    else:
        with st.spinner("Fetching FL3XX flights…"):
            try:
                data: NegotiationData = fetch_negotiation_data(
                    schedule_day, settings=fl3xx_settings, policy=policy
                )
            except FlightDataError as exc:
                st.error(f"Unable to load FL3XX flights: {exc}")
                return

        legs = data.flights
        tails = data.tails
        schedule_rows = data.scheduled_rows
        add_line_rows = data.unscheduled_rows
        fetch_metadata = data.metadata

        st.subheader("FL3XX window snapshot")
        col1, col2, col3 = st.columns(3)
        col1.metric("Scheduled legs", fetch_metadata.get("scheduled_count", len(schedule_rows)))
        col2.metric("Add-line legs", fetch_metadata.get("unscheduled_count", len(add_line_rows)))
        col3.metric("Flights sent to solver", len(legs))

        window_start = fetch_metadata.get("window_start_utc")
        window_end = fetch_metadata.get("window_end_utc")
        if window_start and window_end:
            st.caption(f"Window: {window_start} → {window_end} (UTC)")

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
                gantt_df = _build_gantt_schedule(schedule_rows, fetch_metadata.get("window_start_utc"))
                if gantt_df.empty:
                    st.info("Not enough timing data to display the Gantt view.")
                else:
                    draw_gantt(
                        gantt_df,
                        color_by="workflow",
                        minutes_range=(0, 24 * 60),
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
        try:
            scheduler = NegotiationScheduler(solver_flights, tails, policy)
            status, solution = scheduler.solve()
        except Exception as exc:  # pragma: no cover - surfaced in UI
            st.error(f"Solver error: {exc}")
            return

        st.subheader("Assigned schedule")
        assigned_df: pd.DataFrame = solution["assigned"]
        if assigned_df.empty:
            st.write("— No internal assignments —")
        else:
            table_tab, gantt_tab = st.tabs(["Table", "Gantt"])
            with table_tab:
                st.dataframe(assigned_df, use_container_width=True)
            with gantt_tab:
                gantt_df = _build_gantt_solution(assigned_df)
                if gantt_df.empty:
                    st.info("Not enough timing data to display the Gantt view.")
                else:
                    draw_gantt(
                        gantt_df,
                        color_by="tail_swapped",
                    )

        st.subheader("Unscheduled / outsourced")
        outsourced_df: pd.DataFrame = solution["outsourced"]
        if outsourced_df.empty:
            st.write("— All legs scheduled internally —")
        else:
            st.dataframe(outsourced_df, use_container_width=True)

        st.caption(f"Objective value: {solution['objective']:.0f}")

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


if __name__ == "__main__":  # pragma: no cover - Streamlit executes as a script
    render_page()
