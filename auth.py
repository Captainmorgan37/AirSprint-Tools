"""Authentication helpers for the AirSprint Streamlit app."""

from __future__ import annotations

from typing import Dict, Tuple

import streamlit as st
import streamlit_authenticator as stauth

# Default credentials are meant for local development only. Update or override
# them by providing an ``auth`` section in ``.streamlit/secrets.toml``.
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
_DEFAULT_SIGNATURE_KEY = "airsprint_tools_signature"
_DEFAULT_COOKIE_EXPIRY_DAYS = 14

_USING_DEFAULT_CREDENTIALS = False


def _load_auth_settings() -> Tuple[Dict[str, Dict[str, Dict[str, str]]], str, str, int]:
    """Load authenticator settings from Streamlit secrets if available."""

    secrets = st.secrets.get("auth", {}) if hasattr(st, "secrets") else {}

    global _USING_DEFAULT_CREDENTIALS

    credentials = secrets.get("credentials") or _DEFAULT_CREDENTIALS
    cookie_name = secrets.get("cookie_name", _DEFAULT_COOKIE_NAME)
    signature_key = secrets.get("signature_key", _DEFAULT_SIGNATURE_KEY)

    expiry = secrets.get("cookie_expiry_days", _DEFAULT_COOKIE_EXPIRY_DAYS)
    try:
        cookie_expiry_days = int(expiry)
    except (TypeError, ValueError):
        cookie_expiry_days = _DEFAULT_COOKIE_EXPIRY_DAYS

    # streamlit-authenticator expects a ``usernames`` key. Raise a helpful error
    # if the provided configuration is missing or malformed.
    if not isinstance(credentials, dict) or "usernames" not in credentials:
        raise ValueError(
            "Authentication credentials must include a 'usernames' mapping. "
            "Check your auth configuration in secrets."
        )

    _USING_DEFAULT_CREDENTIALS = credentials is _DEFAULT_CREDENTIALS

    return credentials, cookie_name, signature_key, cookie_expiry_days


def get_authenticator() -> stauth.Authenticate:
    """Create and return a fresh authenticator instance."""

    credentials, cookie_name, signature_key, cookie_expiry_days = _load_auth_settings()
    return stauth.Authenticate(
        credentials,
        cookie_name,
        signature_key,
        cookie_expiry_days=cookie_expiry_days,
    )


def require_login() -> Tuple[str, str]:
    """Ensure the current user is authenticated before continuing."""

    authenticator = get_authenticator()
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
                "Default credentials are active. Update the auth configuration in "
                "`.streamlit/secrets.toml` to restrict access."
            )
        return st.session_state.get("name", name or ""), st.session_state.get(
            "username", username or ""
        )

    if authentication_status is False:
        st.error("Username or password is incorrect.")
    else:
        st.info("Please log in to continue.")

    st.stop()
