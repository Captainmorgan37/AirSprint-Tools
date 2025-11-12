from datetime import date

import streamlit as st

from Home import configure_page, password_gate, render_sidebar

configure_page(page_title="Max ZFW (Pax + Cargo) Checker")
password_gate()
render_sidebar()

# --------------------------
# Initialize defaults in session_state
# --------------------------
if "aircraft" not in st.session_state:
    st.session_state.aircraft = "CJ2"
if "season" not in st.session_state:
    st.session_state.season = "Summer"
if "pax_override" not in st.session_state:
    st.session_state.pax_override = False
if "cargo_override" not in st.session_state:
    st.session_state.cargo_override = False


# --------------------------
# Config & helper functions
# --------------------------
MAX_PAX_CARGO = {
    "CJ2":  {"Summer": 1086, "Winter": 1034},
    "CJ3":  {"Summer": 1602, "Winter": 1550},
    "Embraer": {"Summer": 2116, "Winter": 2104},
}

STD_WEIGHTS = {
    "Summer": {"Male": 193, "Female": 159, "Child": 75, "Infant": 30},
    "Winter": {"Male": 199, "Female": 165, "Child": 75, "Infant": 30},
}

def color_text(text: str, color: str, bold: bool = False, size_px: int | None = None):
    style = []
    if color: style.append(f"color:{color}")
    if bold: style.append("font-weight:bold")
    if size_px: style.append(f"font-size:{size_px}px")
    style_str = "; ".join(style)
    return f"<span style='{style_str}'>{text}</span>"

st.title("Max ZFW – Pax + Cargo Checker")

# --------------------------
# Top controls
# --------------------------
cols_top = st.columns([1, 1, 1])
with cols_top[0]:
    aircraft = st.selectbox(
        "Aircraft Type",
        ["CJ2", "CJ3", "Embraer"],
        index=["CJ2", "CJ3", "Embraer"].index(st.session_state.aircraft),
        key="aircraft"
    )

with cols_top[1]:
    # Month dropdown
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    selected_month = st.selectbox(
        "Flight Month",
        months,
        index=date.today().month - 1,
        key="month"
    )

    # Auto-assign season from month
    if selected_month in ["April", "May", "June", "July", "August", "September", "October"]:
        auto_season = "Summer"
    else:
        auto_season = "Winter"
    st.caption(f"Auto-detected season from month: **{auto_season}**")

with cols_top[2]:
    # Manual override (default still comes from auto_season)
    season = st.radio(
        "Season (manual override)",
        ["Summer", "Winter"],
        index=["Summer", "Winter"].index(auto_season),
        horizontal=True,
        key="season"
    )



# --------------------------
# Pax entry (standard vs override)
# --------------------------
st.subheader("Passengers")

pax_override = st.checkbox("Override passenger standard weights (enter each passenger’s actual weight)")

additive_weight = 0

if not pax_override:
    # Standard-weight mode — enter counts per category
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        n_male = st.number_input("Males", min_value=0, step=1, value=0)
    with c2:
        n_female = st.number_input("Females", min_value=0, step=1, value=0)
    with c3:
        n_child = st.number_input("Children", min_value=0, step=1, value=0)
    with c4:
        n_infant = st.number_input("Infants", min_value=0, step=1, value=0)

    total_pax = n_male + n_female + n_child + n_infant
    pax_weight = (
        n_male   * STD_WEIGHTS[season]["Male"] +
        n_female * STD_WEIGHTS[season]["Female"] +
        n_child  * STD_WEIGHTS[season]["Child"] +
        n_infant * STD_WEIGHTS[season]["Infant"]
    )

else:
    # Override mode — enter total pax, then each passenger's actual weight
    total_pax = st.number_input("Total passengers (to generate inputs below)", min_value=0, step=1, value=0)
    pax_weights = []
    if total_pax > 0:
        st.caption("Enter each passenger’s actual weight (lb):")
        # Split inputs into columns for nicer layout
        cols = st.columns(4)
        for i in range(total_pax):
            with cols[i % 4]:
                w = st.number_input(f"Pax {i+1}", min_value=0.0, step=1.0, value=0.0, key=f"pax_w_{i}")
                pax_weights.append(w)
    if total_pax > 0:
        total_declared_weight = sum(pax_weights)
        additive_per_pax = 18 if season == "Summer" else 24
        additive_weight = additive_per_pax * total_pax
        pax_weight = total_declared_weight + additive_weight
        st.info(
            f"Regulatory additive applied: {additive_per_pax} lb × {total_pax} pax (total {additive_weight} lb) has been automatically added to the provided weights."
        )
    else:
        pax_weight = 0

# --------------------------
# Cargo entry (default vs override)
# --------------------------
st.subheader("Cargo")

default_cargo = 30 * total_pax
cargo_override = st.checkbox("Override cargo (enter total cargo weight)", key="cargo_override")

if cargo_override:
    if "cargo_weight" not in st.session_state:
        st.session_state.cargo_weight = float(default_cargo)

    st.session_state.cargo_weight = st.number_input(
        "Cargo weight (lb)",
        min_value=0.0,
        step=1.0,
        value=st.session_state.cargo_weight,
        key="cargo_weight_input"
    )
    cargo_weight = st.session_state.cargo_weight

else:
    cargo_weight = float(default_cargo)
    st.session_state.cargo_weight = cargo_weight  # reset when not overriding
    st.write(f"Assumed cargo: {default_cargo} lb (30 lb × {total_pax} pax)")



# --------------------------
# Calculation & results
# --------------------------
st.markdown("---")
st.subheader("Result")

max_allowed = MAX_PAX_CARGO[aircraft][season]
total_payload = pax_weight + cargo_weight
margin = max_allowed - total_payload

# Summary panel
lcol, rcol = st.columns(2)
with lcol:
    st.markdown("**Inputs**")
    if pax_override:
        st.write(f"Passengers (override): {total_pax}")
        if additive_weight > 0:
            st.caption(
                f"Includes mandatory {additive_weight / total_pax:.0f} lb additive per passenger (total added: {additive_weight} lb)."
            )
    else:
        st.write(f"Passengers (std {season}): {total_pax}")
        stds = STD_WEIGHTS[season]
        st.caption(
            f"Std Weights — Male {stds['Male']} • Female {stds['Female']} • Child {stds['Child']} • Infant {stds['Infant']} lb"
        )
    st.write(f"Cargo: {cargo_weight:.0f} lb")

with rcol:
    st.markdown("**Limits**")
    st.write(f"Aircraft: {aircraft}")
    st.write(f"Season: {season}")
    st.write(f"Max pax + cargo: **{max_allowed} lb**")

st.markdown("---")

# Status
if total_pax == 0 and cargo_weight == 0:
    st.info("Enter passengers and/or cargo to calculate.")
else:
    if margin >= 0:
        st.markdown(color_text(f"WITHIN LIMITS by {margin:.0f} lb", color="green", bold=True, size_px=18), unsafe_allow_html=True)
    else:
        st.markdown(color_text(f"OVER LIMIT by {-margin:.0f} lb", color="red", bold=True, size_px=18), unsafe_allow_html=True)

    # Details
    st.markdown(
        f"- Pax weight: **{pax_weight:.0f} lb**  \n"
        f"- Cargo weight: **{cargo_weight:.0f} lb**  \n"
        f"- Total pax + cargo: **{total_payload:.0f} lb**  \n"
        f"- Max allowed: **{max_allowed} lb**"
    )
    if additive_weight > 0:
        st.caption(
            f"Passenger weight includes {additive_weight} lb regulatory additive (18 lb per voluntary passenger weight)."
        )

# Footer note
st.caption("Note: This tool checks pax + cargo against your planning maxima for each tail type and season. It does not compute full ZFW or CG.")











