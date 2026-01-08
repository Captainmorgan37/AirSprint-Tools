from __future__ import annotations

from datetime import date
from typing import Any, Dict, Mapping, Sequence

import pandas as pd
import streamlit as st

from delay_codes import collect_delay_code_records
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from Home import configure_page, get_secret, password_gate, render_sidebar


def _default_target_date() -> date:
    return date.today()


def _row_background_color(record: Any, delay_threshold: int) -> str:
    delays_with_reasons = 0
    delay_columns = 0

    off_block_delayed = (
        record.off_block_delay_min is not None
        and record.off_block_delay_min >= delay_threshold
    )
    if off_block_delayed:
        delay_columns += 1
        if record.off_block_reasons or record.takeoff_reasons:
            delays_with_reasons += 1

    on_block_delayed = (
        record.on_block_delay_min is not None
        and record.on_block_delay_min >= delay_threshold
    )
    if on_block_delayed:
        delay_columns += 1
        if record.landing_reasons or record.on_block_reasons:
            delays_with_reasons += 1

    if delay_columns == 0:
        return ""
    if delays_with_reasons == 0:
        return "#fdecea"
    if delays_with_reasons == delay_columns:
        return "#e8f5e9"
    return "#fff8e1"


def _build_row_styles(records: Sequence[Any], delay_threshold: int, columns: int) -> list[list[str]]:
    styles: list[list[str]] = []
    for record in records:
        color = _row_background_color(record, delay_threshold)
        if not color:
            styles.append(["" for _ in range(columns)])
        else:
            styles.append(
                [f"background-color: {color}; color: #111;" for _ in range(columns)]
            )
    return styles


def _render_metadata(metadata: Dict[str, Any], diagnostics: Dict[str, Any]) -> None:
    with st.expander("Diagnostics", expanded=False):
        st.json({"fetch_metadata": metadata, "diagnostics": diagnostics})


configure_page(page_title="Delay Codes")
password_gate()
render_sidebar()

st.title("Delay Codes")
st.caption(
    "Identify flights with 15+ minute block-off/on delays and display postflight delay reasons."
)

fl3xx_settings_raw = get_secret("fl3xx_api", {})
if isinstance(fl3xx_settings_raw, Mapping):
    fl3xx_settings = dict(fl3xx_settings_raw)
else:
    fl3xx_settings = {}

with st.form("delay_codes_form"):
    target_date = st.date_input("Flight date", value=_default_target_date())
    delay_threshold = st.number_input(
        "Delay threshold (minutes)",
        min_value=1,
        max_value=180,
        value=15,
        step=1,
    )
    submitted = st.form_submit_button("Fetch Delay Codes")

if submitted:
    if not fl3xx_settings:
        st.error(
            "FL3XX API credentials are missing. Configure the `fl3xx_api` section in `.streamlit/secrets.toml`."
        )
    else:
        try:
            config = build_fl3xx_api_config(fl3xx_settings)
        except FlightDataError as exc:
            st.error(str(exc))
        else:
            with st.spinner("Fetching flights and postflight delay reasons…"):
                records, metadata, diagnostics = collect_delay_code_records(
                    config,
                    target_date,
                    delay_threshold_min=int(delay_threshold),
                )

            if not records:
                st.warning("No flights met the delay threshold for the selected date.")
                _render_metadata(metadata, diagnostics)
            else:
                table = pd.DataFrame([record.as_dict() for record in records])
                styles = _build_row_styles(records, int(delay_threshold), len(table.columns))
                styles_df = pd.DataFrame(styles, index=table.index, columns=table.columns)
                styled_table = table.style.apply(lambda _: styles_df, axis=None)
                st.dataframe(styled_table, use_container_width=True)
                st.download_button(
                    "Download CSV",
                    data=table.to_csv(index=False),
                    file_name=f"delay_codes_{target_date.isoformat()}.csv",
                    mime="text/csv",
                )
                _render_metadata(metadata, diagnostics)
