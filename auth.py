"""Authentication helpers for the AirSprint Streamlit app."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Dict, Tuple

import streamlit as st
import streamlit_authenticator as stauth

# ---------------------------------------------------------------------
# Default credentials (for local testing only)
# ---------------------------------------------------------------------
_DEFAULT_CREDENTIALS: Dict[str, Dict[str, Dict[str, str]]] = {
    "usernames": {
        "admin": {
            "name": "Admin User",
            "password": "$2b$12$jKsGZIB9siihwk68PHS7NeVzDUGkG.ksnw2IIrNDGS3us1gPOMM0a",
        },
        "ops": {
            "name": "Operations",
            "password": "$2b$12$DbNC.1iLd2nCEOqG09ia1e8EPEC1gn6e1Y0tZ5imzqByt9A1TrGV6",
        },
    }
}

_DEFAULT_COOKIE_NAME = "airsprint_tools_auth"
_DEFAULT_COOKIE_KEY = "airsprint_tools_signature"
_DEFAULT_COOKIE_EXPIRY_DAYS = 14

_USING_DEFAULT_CREDENTIALS = False
_AUTHENTICATOR_SESSION_KEY = "_authenticator"


def _to_plain_dict(value: Mapping | Dict) -> Dict:
    """Recursively convert mapping-like objects (e.g. ``st.secrets`` sections) to dicts."""

    if isinstance(value, dict):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _to_plain_dict(val) if isinstance(val, Mapping) else val
            for key, val in value.items()
        }
    raise TypeError("Expected a mapping of authentication settings.")


# ---------------------------------------------------------------------
# Load configuration from Streamlit secrets (Cloud or local)
# ---------------------------------------------------------------------
def _load_auth_settings() -> Tuple[Dict[str, Dict[str, Dict[str, str]]], str, str, int]:
    """Load authenticator settings from Streamlit secrets if available."""

    if not hasattr(st, "secrets"):
        raise RuntimeError("Streamlit secrets not found. Configure secrets.toml or Streamlit Cloud secrets.")

    secrets = st.secrets
    global _USING_DEFAULT_CREDENTIALS

    # ✅ Support both [auth.credentials] and [credentials] layouts
    auth_section_raw = secrets.get("auth", {})
    auth_section = (
        _to_plain_dict(auth_section_raw)
        if isinstance(auth_section_raw, Mapping)
        else auth_section_raw
    )

    credentials_source = (
        auth_section.get("credentials") if isinstance(auth_section, Mapping) else None
    )
    if not credentials_source:
        credentials_source = secrets.get("credentials")
    if isinstance(credentials_source, Mapping):
        credentials = _to_plain_dict(credentials_source)
    else:
        credentials = credentials_source or _DEFAULT_CREDENTIALS

    cookie_name = (
        auth_section.get("cookie_name", _DEFAULT_COOKIE_NAME)
        if isinstance(auth_section, Mapping)
        else _DEFAULT_COOKIE_NAME
    )
    cookie_key = (
        auth_section.get("cookie_key", _DEFAULT_COOKIE_KEY)
        if isinstance(auth_section, Mapping)
        else _DEFAULT_COOKIE_KEY
    )
    expiry = (
        auth_section.get("cookie_expiry_days", _DEFAULT_COOKIE_EXPIRY_DAYS)
        if isinstance(auth_section, Mapping)
        else _DEFAULT_COOKIE_EXPIRY_DAYS
    )

    try:
        cookie_expiry_days = int(expiry)
    except (TypeError, ValueError):
        cookie_expiry_days = _DEFAULT_COOKIE_EXPIRY_DAYS

    if isinstance(credentials, Mapping) and not isinstance(credentials, dict):
        credentials = _to_plain_dict(credentials)

    if not isinstance(credentials, dict) or "usernames" not in credentials:
        raise ValueError(
            "Authentication credentials must include a 'usernames' mapping. "
            "Check your [auth] or [credentials] configuration in Streamlit Cloud secrets."
        )

    if isinstance(credentials.get("usernames"), Mapping) and not isinstance(
        credentials["usernames"], dict
    ):
        credentials["usernames"] = _to_plain_dict(credentials["usernames"])

    _USING_DEFAULT_CREDENTIALS = credentials is _DEFAULT_CREDENTIALS
    return credentials, cookie_name, cookie_key, cookie_expiry_days


# ---------------------------------------------------------------------
# Return or create authenticator instance (stored in session)
# ---------------------------------------------------------------------
def get_authenticator() -> stauth.Authenticate:
    """Return the authenticator instance stored in session state."""
    if _AUTHENTICATOR_SESSION_KEY not in st.session_state:
        credentials, cookie_name, cookie_key, cookie_expiry_days = _load_auth_settings()
        st.session_state[_AUTHENTICATOR_SESSION_KEY] = stauth.Authenticate(
            credentials,
            cookie_name,
            cookie_key,
            cookie_expiry_days=cookie_expiry_days,
        )
    return st.session_state[_AUTHENTICATOR_SESSION_KEY]


# ---------------------------------------------------------------------
# Require login before continuing
# ---------------------------------------------------------------------
def require_login() -> Tuple[str, str]:
    """Ensure the current user is authenticated before continuing."""
    authenticator = get_authenticator()

    try:
        name, authentication_status, username = authenticator.login(
            form_name="Login", location="main"
        )
    except TypeError:
        # Fallback for older versions of streamlit-authenticator
        name, authentication_status, username = authenticator.login("Login", "main")

    if authentication_status:
        if name:
            st.session_state["name"] = name
        if username:
            st.session_state["username"] = username

        display_name = st.session_state.get("name") or name or username or ""
        authenticator.logout("Logout", "sidebar")

        if display_name:
            st.sidebar.write(f"Signed in as **{display_name}**")

        if _USING_DEFAULT_CREDENTIALS:
            st.sidebar.warning(
                "⚠️ Default credentials are active. Update the auth configuration in "
                "Streamlit Cloud secrets to restrict access."
            )

        return st.session_state.get("name", name or ""), st.session_state.get(
            "username", username or ""
        )

    if authentication_status is False:
        st.error("Username or password is incorrect.")
    else:
        st.info("Please log in to continue.")

    st.stop()

