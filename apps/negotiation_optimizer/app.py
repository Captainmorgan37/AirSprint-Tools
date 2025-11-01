"""Streamlit entrypoint for the negotiation-aware scheduling prototype."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar
from core.neg_scheduler import LeverPolicy, NegotiationScheduler
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

    if dataset == "Demo":
        legs, tails = get_demo_data()
    else:
        with st.spinner("Fetching FL3XX flights…"):
            try:
                data: NegotiationData = fetch_negotiation_data(
                    schedule_day, settings=fl3xx_settings
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
            st.dataframe(scheduled_df, use_container_width=True)
        else:
            st.info("No scheduled legs detected for the selected window.")

        unscheduled_df = _format_leg_rows(add_line_rows)
        if not unscheduled_df.empty:
            st.markdown("**Add-line demand (solver targets)**")
            st.dataframe(unscheduled_df, use_container_width=True)
        else:
            st.info("No add-line legs found in the selected window.")

    if st.button("Run Solver", type="primary"):
        try:
            scheduler = NegotiationScheduler(legs, tails, policy)
            status, solution = scheduler.solve()
        except Exception as exc:  # pragma: no cover - surfaced in UI
            st.error(f"Solver error: {exc}")
            return

        st.subheader("Assigned schedule")
        assigned_df: pd.DataFrame = solution["assigned"]
        if assigned_df.empty:
            st.write("— No internal assignments —")
        else:
            st.dataframe(assigned_df, use_container_width=True)

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
