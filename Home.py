import streamlit as st
import reprlib
st.caption(f"Debug (remove later): secret value = {reprlib.repr(st.secrets.get('app_password'))}")

# --- Basic single-password login gate ---
def password_gate():
    """Simple access restriction with a single shared password."""
    correct_password = st.secrets.get("app_password")
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("🔐 AirSprint Tools Access")
        pw = st.text_input("Enter password", type="password")
        if st.button("Unlock"):
            if pw == correct_password:
                st.session_state.authenticated = True
                st.experimental_rerun()
            else:
                st.error("Incorrect password")
        st.stop()

password_gate()


import streamlit as st

st.set_page_config(page_title="AirSprint Ops Tools", layout="wide")

require_login()

st.title("✈️ AirSprint Operations Tools")

st.write("""
Welcome!  
This app brings together multiple operational tools into one place.  
Use the sidebar to navigate between calculators, parsers, and checkers.
""")

st.subheader("📄 Workflow Documents")

docs = {
    "Cargo Fit Checker Workflow": "docs/Cargo Fit Checker Workflow.docx",
    "Max ZFW Checker Workflow": "docs/Max ZFW Checker Workflow.docx",
    "NOTAM Checker Procedure": "docs/NOTAM Checker Procedure.docx",
    "OCS Slot Parser Website Process": "docs/OCS Slot Parser Website Process.docx"
}

for label, path in docs.items():
    try:
        with open(path, "rb") as f:
            st.download_button(
                label=f"⬇️ Download {label}",
                data=f,
                file_name=path.split("/")[-1],
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
    except FileNotFoundError:
        st.warning(f"{label} not found. Please confirm it’s uploaded to {path}")
