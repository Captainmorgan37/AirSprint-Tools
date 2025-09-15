import streamlit as st
from datetime import datetime, timedelta, time

st.set_page_config(layout="wide")

# Helper functions
def parse_time(t_str):
    """Parse HHMM or HH:MM strings to datetime.time, or return None if invalid."""
    t_str = t_str.strip()
    if not t_str:
        return None
    try:
        if ':' in t_str:
            dt = datetime.strptime(t_str, "%H:%M")
        else:
            dt = datetime.strptime(t_str.zfill(4), "%H%M")
        return dt.time()
    except:
        return None

def time_to_datetime(t):
    """Convert datetime.time to datetime.datetime for today."""
    return datetime.combine(datetime.today(), t)

def format_timedelta(td):
    """Format timedelta as HH:MM string."""
    total_minutes = int(td.total_seconds() // 60)
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{h:02d}:{m:02d}"

# Initialize session state for inputs if not present
def init_session_state():
    defaults = {
        # Tab 1: Duty Calculator
        "duty_dep": "",
        "duty_arr": "",
        # Tab 2: Split Duty Calculator
        "split_first_dep": "",
        "split_first_arrival": "",
        "split_second_dep": "",
        "split_last_arrival": "",
        # Tab 3: Rest Calculator
        "rest_landing": "",
        "rest_duty_end": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

# Clear all relevant inputs
def clear_all_inputs():
    keys_to_clear = [
        "duty_dep", "duty_arr",
        "split_first_dep", "split_first_arrival", "split_second_dep", "split_last_arrival",
        "rest_landing", "rest_duty_end",
    ]
    for key in keys_to_clear:
        st.session_state[key] = ""

# Title and Clear All button at top-right
col1, col2 = st.columns([5,1])
with col1:
    st.title("Duty & Rest Calculators")
with col2:
    if st.button("Clear All"):
        clear_all_inputs()



# Tabs
tab1, tab2, tab3 = st.tabs(["Duty Calculator", "Split Duty Calculator", "Rest Calculator"])

# ---------------- Tab 1: Duty Calculator ----------------
with tab1:
    st.header("Duty Calculator")
    st.markdown("**<span style='color:red;'>MAX DUTY: 14 HOURS</span>**", unsafe_allow_html=True)

    # Read directly from session_state so changes trigger immediate output
    dep_str = st.text_input(
        "First Flight Departure Time (UTC - HHMM or HH:MM)",
        value=st.session_state.duty_dep,
        key="duty_dep"
    )
    arr_str = st.text_input(
        "Last Flight Arrival Time (UTC - HHMM or HH:MM)",
        value=st.session_state.duty_arr,
        key="duty_arr"
    )

    # Only run if both fields have some text
    if dep_str.strip() and arr_str.strip():
        dep_time = parse_time(dep_str)
        arr_time = parse_time(arr_str)

        if dep_time and arr_time:
            duty_start = datetime.combine(datetime.today(), dep_time) - timedelta(minutes=60)
            duty_end = datetime.combine(datetime.today(), arr_time) + timedelta(minutes=15)

            # Handle rollover past midnight
            if duty_end <= duty_start:
                duty_end += timedelta(days=1)

            duty_length_td = duty_end - duty_start
            duty_length_hours = duty_length_td.total_seconds() / 3600

            # Colour coding
            if duty_length_hours < 13:
                color = "green"
            elif 13 <= duty_length_hours < 14:
                color = "yellow"
            else:
                color = "red"

            st.markdown(f"**Duty Start (UTC):** {duty_start.strftime('%H:%M')}")
            st.markdown(f"**Duty End (UTC):** {duty_end.strftime('%H:%M')}")
            st.markdown(
                f"<span style='color:{color}; font-weight:bold;'>Duty Length: {format_timedelta(duty_length_td)}</span>",
                unsafe_allow_html=True
            )

            # Earliest Next Departure
            if duty_length_hours < 14:
                earliest_next_dep = duty_end + timedelta(hours=11)
                st.markdown(f"**Earliest Next Departure (UTC):** {earliest_next_dep.strftime('%H:%M')}")
            else:
                st.markdown("**Earliest Next Departure (UTC):** —")

# ---------------- Tab 2: Split Duty Calculator ----------------
with tab2:
    st.header("Split Duty Calculator")

    split_first_dep_input = st.text_input(
        "First Flight Departure Time (UTC - HHMM or HH:MM)",
        value=st.session_state.split_first_dep,
        key="split_first_dep"
    )
    split_first_arrival_input = st.text_input(
        "Landing Time Before Split (UTC - HHMM or HH:MM)",
        value=st.session_state.split_first_arrival,
        key="split_first_arrival"
    )
    split_second_dep_input = st.text_input(
        "Departure Time After Split (UTC - HHMM or HH:MM)",
        value=st.session_state.split_second_dep,
        key="split_second_dep"
    )
    split_last_arrival_input = st.text_input(
        "Last Flight Arrival Time (UTC - HHMM or HH:MM)",
        value=st.session_state.split_last_arrival,
        key="split_last_arrival"
    )

    if all(field.strip() != "" for field in [
        split_first_dep_input,
        split_first_arrival_input,
        split_second_dep_input,
        split_last_arrival_input
    ]):
        first_dep = parse_time(split_first_dep_input)
        first_arrival = parse_time(split_first_arrival_input)
        second_dep = parse_time(split_second_dep_input)
        last_arrival = parse_time(split_last_arrival_input)

        if None not in (first_dep, first_arrival, second_dep, last_arrival):
            dt_start = time_to_datetime(first_dep) - timedelta(minutes=60)
            dt_land = time_to_datetime(first_arrival)
            dt_dep = time_to_datetime(second_dep)
            dt_end = time_to_datetime(last_arrival) + timedelta(minutes=15)

            # Handle cross-midnight
            if dt_land < dt_start:
                dt_land += timedelta(days=1)
            if dt_dep < dt_land:
                dt_dep += timedelta(days=1)
            if dt_end < dt_dep:
                dt_end += timedelta(days=1)

            ground_rest_td = dt_dep - dt_land
            ground_rest_hours = ground_rest_td.total_seconds() / 3600

            allowable_duty = 14
            if ground_rest_hours >= 6:
                allowable_duty += min((ground_rest_hours - 2)/2, 3)

            duty_length_td = dt_end - dt_start
            duty_length_hours = duty_length_td.total_seconds() / 3600
            time_diff_hours = allowable_duty - duty_length_hours

            # Outputs
            st.write(f"Duty Start (60 min before first departure): {dt_start.time().strftime('%H:%M')}")
            st.write(f"Duty End (15 min after last arrival): {dt_end.time().strftime('%H:%M')}")
            st.write(f"Ground Rest Duration: {format_timedelta(ground_rest_td)}")

            if ground_rest_hours < 6:
                st.markdown(
                    "<span style='color:red; font-weight:bold;'>Split Duty Day not applicable as ground rest is less than 6:00 hours!</span>",
                    unsafe_allow_html=True
                )

            # ---------- Allowable Duty / Reset Duty Check ----------
            if ground_rest_td >= timedelta(hours=11, minutes=15):
                # Reset Duty Day
                st.markdown(
                    "<span style='color:red; font-size:20px; font-weight:bold;'>ALLOWABLE DUTY LENGTH: RESET DUTY DAY</span>",
                    unsafe_allow_html=True
                )
            else:
                # Normal Allowable Duty Length
                st.write(f"Allowable Duty Length: {format_timedelta(timedelta(hours=allowable_duty))}")

                # Colour-coded Actual Duty Length
                if duty_length_hours > allowable_duty:
                    st.markdown(
                        f"<span style='color:red; font-weight:bold;'>Actual Duty Length: {format_timedelta(duty_length_td)} "
                        f"(Over allowable duty by {format_timedelta(timedelta(hours=duty_length_hours - allowable_duty))})</span>",
                        unsafe_allow_html=True
                    )
                elif time_diff_hours < 1:
                    st.markdown(
                        f"<span style='color:orange; font-weight:bold;'>Actual Duty Length: {format_timedelta(duty_length_td)}</span>",
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(
                        f"<span style='color:green; font-weight:bold;'>Actual Duty Length: {format_timedelta(duty_length_td)}</span>",
                        unsafe_allow_html=True
                    )

# -------------------------------

# ---------------- Tab 3: Rest Calculator ----------------
with tab3:
    st.header("Assumed vs Deemed Rest Calculator")
    landing_input = st.text_input("Crew Landing Time (HHMM or HH:MM)", value=st.session_state.rest_landing, key="rest_landing")
    duty_end_input = st.text_input("Override Duty End Time (optional)", value=st.session_state.rest_duty_end, key="rest_duty_end")

    # Check the current values first
    ftl_disabled = st.session_state.get("split_duty_toggle", False)
    split_disabled = st.session_state.get("ftl_extension", False)

    col1, col2 = st.columns(2)
    with col1:
        ftl_extension = st.checkbox("FTL Extension", key="ftl_extension", disabled=ftl_disabled)
    with col2:
        split_duty_toggle = st.checkbox("Split/Unforeseen Duty Day", key="split_duty_toggle", disabled=split_disabled)

    # Show extra duty length box only if split toggle is on
    duty_length_time = None
    if split_duty_toggle:
        duty_length_input = st.text_input("Total Duty Length (HHMM or HH:MM)", key="split_duty_length")
        duty_length_time = parse_time(duty_length_input) if duty_length_input.strip() != "" else None

    if landing_input.strip() != "":
        landing_time = parse_time(landing_input)
        duty_end_time = parse_time(duty_end_input) if duty_end_input.strip() != "" else None

        if landing_time and (not split_duty_toggle or duty_length_time):
            default_duty_end_dt = time_to_datetime(landing_time) + timedelta(minutes=15)
            duty_end_dt = time_to_datetime(duty_end_time) if duty_end_time else default_duty_end_dt

            duty_end_hour = duty_end_dt.time().hour
            duty_end_min = duty_end_dt.time().minute

            # Determine rest type
            if (duty_end_hour >= 20) or (duty_end_hour < 2) or (duty_end_hour == 2 and duty_end_min == 0):
                rest_end_dt = duty_end_dt + timedelta(hours=10)
                rest_type = "Deemed Rest"
                rest_color = "orange"
            else:
                assumed_rest_end_dt = datetime.combine(duty_end_dt.date(), time(6, 0))
                if duty_end_dt.time() >= time(6, 0):
                    assumed_rest_end_dt += timedelta(days=1)
                rest_end_dt = assumed_rest_end_dt
                rest_type = "Assumed Rest"
                rest_color = "green"

            # Apply FTL extension ONLY if rest is Deemed Rest
            if ftl_extension and rest_type == "Deemed Rest":
                rest_end_dt += timedelta(hours=1)

            # Apply split/unforeseen duty extension ONLY if rest is Deemed Rest
            if split_duty_toggle and duty_length_time and rest_type == "Deemed Rest":
                duty_length_hours = duty_length_time.hour + duty_length_time.minute / 60
                if duty_length_hours > 14:
                    extra_hours = duty_length_hours - 14
                    rest_end_dt += timedelta(hours=extra_hours)

            callout_dt = rest_end_dt
            departure_dt = callout_dt + timedelta(hours=2, minutes=30)

            # Build display string for rest end
            rest_end_display = rest_end_dt.strftime("%H:%M")
            if rest_type == "Assumed Rest":
                rest_end_display += " (+1 day)"


            st.markdown(f"**Duty End Time:** {duty_end_dt.strftime('%H:%M')}")
            st.markdown(f"<span style='color:{rest_color}; font-weight:bold;'>Rest Type: {rest_type}</span>", unsafe_allow_html=True)
            st.markdown(f"**Rest Ends At:** {rest_end_display}")
            st.markdown(f"**Earliest Callout Time:** {callout_dt.strftime('%H:%M')}")
            st.markdown(f"**Earliest Departure Time:** {departure_dt.strftime('%H:%M')}")
