from __future__ import annotations

import time
from typing import Any

import streamlit as st


_SECRET_RETRY_PREFIX = "_secret_retry__"
_SECRET_RETRY_MAX = 6
_SECRET_RETRY_DELAY_SECONDS = 0.2
_MISSING = object()


def _secret_retry_key(name: str) -> str:
    return f"{_SECRET_RETRY_PREFIX}{name}"


def _fetch_secret(key: str, *, required: bool, default: Any = _MISSING) -> Any:
    """Fetch a secret, retrying briefly if the secrets store isn't ready yet."""

    try:
        if key in st.secrets:
            value = st.secrets[key]
            st.session_state.pop(_secret_retry_key(key), None)
            return value
    except Exception:
        # When the Streamlit runtime is still initialising secrets, accessing
        # ``st.secrets`` can raise or behave like an empty mapping. Treat this
        # as "not yet available" and retry.
        pass

    retry_key = _secret_retry_key(key)
    attempts = int(st.session_state.get(retry_key, 0))

    if attempts < _SECRET_RETRY_MAX:
        st.session_state[retry_key] = attempts + 1
        st.info("Preparing secure configuration…")
        time.sleep(_SECRET_RETRY_DELAY_SECONDS)
        st.rerun()

    if required:
        st.error(
            f"Required secret '{key}' is not configured. Update `.streamlit/secrets.toml` and refresh the app."
        )
        st.stop()

    if default is not _MISSING:
        return default

    return None


def require_secret(key: str) -> Any:
    """Return a secret value, stopping the app if it never becomes available."""

    return _fetch_secret(key, required=True)


def get_secret(key: str, default: Any | None = None) -> Any:
    """Return a secret value if available, otherwise the provided default."""

    sentinel = _MISSING if default is None else default
    return _fetch_secret(key, required=False, default=sentinel)


# --- Basic single-password login gate ---
def password_gate() -> None:
    """Simple access restriction with a single shared password."""

    correct_password = require_secret("app_password")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("🔐 AirSprint Tools Access")
        pw = st.text_input("Enter password", type="password")
        if st.button("Unlock"):
            if pw == correct_password:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
        st.stop()


password_gate()

st.set_page_config(page_title="AirSprint Ops Tools", layout="wide")

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
