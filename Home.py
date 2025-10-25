from __future__ import annotations

import time
from typing import Any

import streamlit as st


_SECRET_RETRY_PREFIX = "_secret_retry__"
_SECRET_RETRY_MAX = 6
_SECRET_RETRY_DELAY_SECONDS = 0.2
_MISSING = object()
_PAGE_CONFIGURED_KEY = "_page_configured"
_DEFAULT_PAGE_TITLE = "AirSprint Ops Tools"
_DEFAULT_PAGE_ICON = "✈️"


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


def _hide_builtin_sidebar_nav() -> None:
    """Remove Streamlit's default page navigator from the sidebar."""

    st.markdown(
        """
        <style>
            section[data-testid="stSidebar"] div[data-testid="stSidebarNav"] {
                display: none;
            }
            section[data-testid="stSidebar"] div[data-testid="stSidebarNav"] + div {
                padding-top: 0;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def configure_page(*, page_title: str | None = None) -> None:
    """Set the Streamlit page configuration once per run."""

    if not st.session_state.get(_PAGE_CONFIGURED_KEY):
        st.set_page_config(
            page_title=page_title or _DEFAULT_PAGE_TITLE,
            page_icon=_DEFAULT_PAGE_ICON,
            layout="wide",
        )
        st.session_state[_PAGE_CONFIGURED_KEY] = True

    _hide_builtin_sidebar_nav()


def _sidebar_links() -> list[dict[str, Any]]:
    return [
        {
            "label": "✈️ Flight Ops Tools",
            "expanded": True,
            "links": [
                {"path": "pages/Duty Calculator.py", "label": "Duty & Rest Calculator"},
                {"path": "pages/Short Turn Checker.py", "label": "Short Turn Checker"},
                {"path": "pages/Task_Splitter.py", "label": "Night Shift Task Splitter"},
                {"path": "pages/OCS Slot Checker.py", "label": "OCS Slot Checker"},
                {"path": "pages/Arrival Weather Outlook.py", "label": "Arrival Weather Outlook"},
                {"path": "pages/Reserve Calendar Day Checker.py", "label": "Reserve Calendar Checker"},
            ],
        },
        {
            "label": "🧾 Compliance & Audits",
            "expanded": False,
            "links": [
                {"path": "pages/NOTAM Checker.py", "label": "NOTAM Checker"},
                {"path": "pages/Jeppesen ITP Required Flight Check.py", "label": "Jeppesen ITP Checker"},
                {"path": "pages/Max ZFW Checker.py", "label": "Max ZFW Checker"},
                {"path": "pages/_Customs Dashboard.py", "label": "Customs Dashboard"},
            ],
        },
        {
            "label": "📊 Reporting & Tracking",
            "expanded": False,
            "links": [
                {"path": "pages/Operations Lead Morning Reports.py", "label": "Morning Reports"},
                {"path": "pages/FBO Disconnect Report.py", "label": "FBO Disconnect Report"},
                {"path": "pages/ASP CYYC Tracking.py", "label": "ASP CYYC Tracking"},
            ],
        },
        {
            "label": "🛎️ Owner Services",
            "expanded": False,
            "links": [
                {"path": "pages/Owner Services Dashboard.py", "label": "Owner Services Dashboard"},
                {"path": "pages/Cargo Juggler.py", "label": "Cargo Juggler"},
            ],
        },
    ]


def render_sidebar() -> None:
    """Display the custom navigation sidebar once the user is authenticated."""

    if not st.session_state.get("authenticated"):
        return

    st.sidebar.title("🧭 Navigation")
    st.sidebar.page_link("Home.py", label="🏠 Operations Dashboard")

    for section in _sidebar_links():
        with st.sidebar.expander(section["label"], expanded=section["expanded"]):
            for link in section["links"]:
                st.page_link(link["path"], label=link["label"])

    st.sidebar.markdown("---")
    st.sidebar.caption("Built by AirSprint Ops • © 2025")


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


def main() -> None:
    configure_page()
    password_gate()
    render_sidebar()

    st.title("✈️ AirSprint Operations Tools")

    st.write("""
    Welcome!
    This app brings together multiple operational tools into one place.
    Use the sidebar to navigate between calculators, parsers, dashboards, and reports.
    """)

    st.subheader("📄 Workflow Documents")

    docs = {
        "Cargo Fit Checker Workflow": "docs/Cargo Fit Checker Workflow.docx",
        "Max ZFW Checker Workflow": "docs/Max ZFW Checker Workflow.docx",
        "NOTAM Checker Procedure": "docs/NOTAM Checker Procedure.docx",
        "OCS Slot Parser Website Process": "docs/OCS Slot Parser Website Process.docx",
    }

    for label, path in docs.items():
        try:
            with open(path, "rb") as f:
                st.download_button(
                    label=f"⬇️ Download {label}",
                    data=f,
                    file_name=path.split("/")[-1],
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
        except FileNotFoundError:
            st.warning(f"{label} not found. Please confirm it’s uploaded to {path}")


if __name__ == "__main__":
    main()
