"""Streamlit entrypoint for the negotiation-aware scheduling prototype."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from core.neg_scheduler import LeverPolicy, NegotiationScheduler
from integrations.fl3xx_adapter import fetch_demo_from_fl3xx, get_demo_data


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
    st.set_page_config(page_title="Negotiation Optimizer", layout="wide")
    st.title("🧩 Negotiation-Aware Scheduler")

    with st.sidebar:
        st.header("Inputs")
        dataset = st.selectbox("Data source", ("Demo", "FL3XX"), index=0)
        turn_min = st.slider("Turn buffer (min)", 0, 120, 30, 5)
        max_plus = st.slider("Max shift + (min)", 0, 240, 30, 5)
        max_minus = st.slider("Max shift - (min)", 0, 180, 30, 5)
        cost_per_min = st.slider("Cost per shifted minute", 0, 10, 2)
        outsource_cost = st.number_input("Outsource cost proxy", 0, 10000, 1800, 50)

    policy = LeverPolicy(
        max_shift_plus_min=max_plus,
        max_shift_minus_min=max_minus,
        cost_per_min_shift=cost_per_min,
        outsource_cost=outsource_cost,
        turn_min=turn_min,
    )

    if dataset == "Demo":
        legs, tails = get_demo_data()
    else:
        st.info("FL3XX integration pending – falling back to demo dataset.")
        legs, tails = fetch_demo_from_fl3xx(date.today())

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

                st.code(
                    "Hi Owner {owner},\n\n"
                    "Could you slide departure by 30 minutes? This avoids a tail swap and keeps crew within duty limits.\n"
                    "We can offer a small courtesy credit.\n\n"
                    "— Dispatch\n".format(owner=row["owner"])
                )


if __name__ == "__main__":  # pragma: no cover - Streamlit executes as a script
    render_page()
