import streamlit_authenticator as stauth
import streamlit as st
st.write("streamlit-authenticator version:", stauth.__version__)
st.stop()


import streamlit as st

from auth import require_login

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
