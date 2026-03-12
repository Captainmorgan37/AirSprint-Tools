from __future__ import annotations

import streamlit as st

from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Customs Port Finder")
password_gate()
render_sidebar()

st.title("🛃 Customs Port Finder")
st.caption("Enter an airport code to find the nearest customs-capable airports in the same country.")

try:
    from customs_port_finder_utils import candidates_to_dataframe, nearest_customs_ports
except Exception as exc:  # pragma: no cover - defensive UI guard
    st.error(
        "Customs Port Finder could not load its lookup helpers. "
        "Please refresh or contact support if this continues."
    )
    st.exception(exc)
    st.stop()

with st.form("customs-port-finder-form"):
    airport_code = st.text_input("Airport code (ICAO/IATA/LID)", placeholder="KTEB")
    max_results = st.number_input("Max results", min_value=1, max_value=20, value=5, step=1)
    submitted = st.form_submit_button("Find customs ports")

if submitted:
    origin, candidates = nearest_customs_ports(airport_code, limit=int(max_results))
    if origin is None:
        st.error("Could not resolve that airport code. Please enter a valid ICAO/IATA/LID code.")
    else:
        origin_name = origin.get("name") or "Unknown"
        origin_city = origin.get("city") or "Unknown city"
        origin_country = origin.get("country") or "Unknown country"
        st.success(
            f"Origin: {origin.get('icao')} · {origin_name} · {origin_city} · {origin_country}"
        )

        if not candidates:
            st.info("No customs-capable airports found in the same country for this origin.")
        else:
            results_df = candidates_to_dataframe(candidates)
            st.dataframe(results_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download CSV",
                data=results_df.to_csv(index=False).encode("utf-8"),
                file_name="customs_port_finder_results.csv",
                mime="text/csv",
            )
