from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import streamlit as st
import streamlit_authenticator as stauth


_SECRET_RETRY_PREFIX = "_secret_retry__"
_SECRET_RETRY_MAX = 10
_SECRET_RETRY_DELAY_SECONDS = 1
_MISSING = object()
_PAGE_CONFIGURED_KEY = "_page_configured"
_DEFAULT_PAGE_TITLE = "AirSprint Ops Tools"
_DEFAULT_PAGE_ICON = "✈️"
_DEFAULT_AUTH_COOKIE_NAME = "airsprint_tools_auth"
_DEFAULT_AUTH_COOKIE_DAYS = 14


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
            f"Required secret '{key}' is not configured. Add it in Streamlit secrets (Cloud app settings or local `.streamlit/secrets.toml`) and refresh the app."
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
                {"path": "pages/ASP CYYC Tracking.py", "label": "ASP CYYC Tracking"},                
                {"path": "pages/Duty Calculator.py", "label": "Duty & Rest Calculator"},
                {"path": "pages/Short Turn Checker.py", "label": "Short Turn Checker"},
                {"path": "pages/Task_Splitter.py", "label": "Night Shift Task Splitter"},
                {"path": "pages/OCS Slot Checker.py", "label": "OCS Slot Checker"},
                {"path": "pages/Arrival Weather Outlook.py", "label": "Arrival Weather Outlook"},
                {"path": "pages/NOTAM Checker.py", "label": "NOTAM/Weather Checker"},
                {"path": "pages/Crew Qualification Monitor.py", "label": "Crew Qualification Monitor"},
                {"path": "pages/Jeppesen ITP Required Flight Check.py", "label": "Jeppesen ITP Checker"},
                {"path": "pages/Crew Confirmation Monitor.py", "label": "Crew Confirmation Monitor"},
                {"path": "pages/Max ZFW Checker.py", "label": "Max ZFW Checker"},
                {"path": "pages/CARICOM Helper.py", "label": "CARICOM Helper"},
                {"path": "pages/_Customs Dashboard.py", "label": "Customs Dashboard"},
                {"path": "pages/OCA Reports.py", "label": "OCA Reports"},
                {"path": "pages/Operations Lead Morning Reports.py", "label": "OL Morning Reports"},
                {"path": "pages/Flight Following Reports.py", "label": "Flight Following Reports"},
                {"path": "pages/FBO Disconnect Report.py", "label": "FBO Disconnect Report"},
                {"path": "pages/Overwater Route Watch.py", "label": "Overwater Route Watch"},
                {"path": "pages/Delay Codes.py", "label": "Delay Codes"},
            ],
        },
        {
            "label": "🧾 Audit Tools",
            "expanded": False,
            "links": [
                {"path": "pages/Reserve Calendar Day Checker.py", "label": "Reserve Calendar Checker"},
                {"path": "pages/Pax Passport Check.py", "label": "Pax Passport Check"},
                {"path": "pages/Historical Airport Use.py", "label": "Historical Airport Use"},
                {"path": "pages/Route Watcher.py", "label": "Route Watcher"},
            ],
        },
        {
            "label": "🛎️ Owner Services",
            "expanded": False,
            "links": [

                {"path": "pages/Owner Services Dashboard.py", "label": "Owner Services Dashboard"},
                {"path": "pages/Catering GT Calculator.py", "label": "Catering/GT Calculator"},
                {"path": "pages/Cargo Juggler.py", "label": "Cargo Juggler"},
            ],
        },
        {
            "label": "In Progress",
            "expanded": False,
            "links": [
                {"path": "pages/Hangar Recommendation.py", "label": "Hangar Recommendation"},
                {"path": "pages/Syndicate Audit.py", "label": "Syndicate Audit"},
                {"path": "pages/FTL Report Reader.py", "label": "FTL Report Reader"},
                {"path": "pages/Dev_Feasibility.py", "label": "Feasibility Checker"},
                {"path": "pages/Foreflight Test.py", "label": "ForeFlight Test"},
                {"path": "pages/Fuel Planning Assistant.py", "label": "Fuel Planning Assistant"},
                {"path": "pages/Fuel Stop Advisor.py", "label": "Fuel Stop Advisor"},
            ],
        },
        {
            "label": "🛠️ Diagnostics",
            "expanded": False,
            "links": [
                {"path": "pages/System Diagnostics.py", "label": "System Diagnostics"},
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


def _append_auth_event(event_type: str, username: str | None = None) -> None:
    """Write a lightweight auth audit event to a JSONL log file."""

    log_path = get_secret("auth_audit_log_path", "logs/auth_events.log")
    if not log_path:
        return

    event = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event": event_type,
        "username": username,
    }

    path = Path(str(log_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")


def _load_authenticator() -> stauth.Authenticate:
    """Build the authenticator from Streamlit secrets."""

    credentials = require_secret("auth_credentials")
    cookie_key = require_secret("auth_cookie_key")
    cookie_name = get_secret("auth_cookie_name", _DEFAULT_AUTH_COOKIE_NAME)
    cookie_days = int(get_secret("auth_cookie_expiry_days", _DEFAULT_AUTH_COOKIE_DAYS))

    return stauth.Authenticate(
        credentials=credentials,
        cookie_name=str(cookie_name),
        cookie_key=str(cookie_key),
        cookie_expiry_days=cookie_days,
    )


def current_user() -> str | None:
    """Return the logged-in username when user-based auth is enabled."""

    return st.session_state.get("auth_username")


def require_role(*roles: str) -> None:
    """Stop rendering if the signed-in user does not have one of the required roles."""

    if not roles:
        return

    if not get_secret("enable_user_auth", False):
        return

    user_role = st.session_state.get("auth_role")
    if user_role not in roles:
        st.error("You do not have permission to access this page.")
        st.stop()


# --- Basic single-password login gate ---
def password_gate() -> None:
    """Access restriction supporting legacy shared-password and per-user auth."""

    if get_secret("enable_user_auth", False):
        authenticator = _load_authenticator()
        name, authentication_status, username = authenticator.login(location="main")

        if authentication_status:
            if st.session_state.get("authenticated") is not True:
                _append_auth_event("login_success", username)
            st.session_state.authenticated = True
            st.session_state.auth_name = name
            st.session_state.auth_username = username
            user_records = require_secret("auth_credentials").get("usernames", {})
            st.session_state.auth_role = user_records.get(username, {}).get("role", "viewer")

            with st.sidebar:
                st.caption(f"Signed in as **{name or username}**")
                st.caption(f"Role: `{st.session_state.auth_role}`")
                if authenticator.logout("Logout", location="sidebar"):
                    _append_auth_event("logout", username)
                    st.session_state.authenticated = False
                    st.session_state.pop("auth_name", None)
                    st.session_state.pop("auth_username", None)
                    st.session_state.pop("auth_role", None)
                    st.rerun()
            return

        st.session_state.authenticated = False
        st.session_state.pop("auth_name", None)
        st.session_state.pop("auth_username", None)
        st.session_state.pop("auth_role", None)

        st.title("🔐 AirSprint Tools Access")
        if authentication_status is False:
            st.error("Username/password is incorrect")
        else:
            st.info("Please sign in with your assigned account.")
        st.stop()

    correct_password = require_secret("app_password")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("🔐 AirSprint Tools Access")
        pw = st.text_input("Enter password", type="password")
        if st.button("Unlock"):
            if pw == correct_password:
                st.session_state.authenticated = True
                st.session_state.auth_username = "shared-password-user"
                st.session_state.auth_role = "admin"
                _append_auth_event("login_success", "shared-password-user")
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
        "Arrival Weather Outlook Workflow": "docs/Arrival Weather Outlook Workflow.docx",
        "Customs Dashboard Workflow": "docs/Customs Dashboard Workflow.docx",
        "Duty Calculator Workflow": "docs/Duty Calculator.docx",
        "FBO Disconnect Report Workflow": "docs/FBO Disconnect Report.docx",
        "Flight Following Reports Workflow": "docs/Flight Following Reports Workflow.docx",
        "NOTAM and Weather Checker Workflow": "docs/NOTAM Checker.docx",
        "OCS Slot Checker Workflow": "docs/OCS Slot Checker.docx",
        "Operations Lead Morning Reports Workflow": "docs/Operations Lead Morning Reports.docx",
        "Reserve Calendar Day Checker Workflow": "docs/Reserve Calendar Day Checker.docx",
        "Short Turn Checker Workflow": "docs/Short Turn Checker.docx",
        "Task Splitter Workflow": "docs/Task Splitter.docx",
        "Crew Confirmation Monitor Workflow": "docs/Crew Confirmation Monitor Workflow.docx",
        "Crew Qualification Monitor Workflow": "docs/Crew Qualification Monitor Workflow.docx",
        "Owner Services Dashboard Workflow": "docs/Owner Services Dashboard Workflow.docx"
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
