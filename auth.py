✅ auth.py (drop-in ready)
"""Authentication helpers for the AirSprint Streamlit app."""

from __future__ import annotations
from typing import Dict, Tuple
import streamlit as st
import streamlit_authenticator as stauth

# --- keep your imports and top constants as-is ---

def _safe_dict(d):
    """Return a plain dict (Streamlit Secrets supports mapping-like objects)."""
    try:
        return dict(d)
    except Exception:
        return d

def _summarize_credentials_shape(creds):
    """Return a redaction-safe summary of the credentials object."""
    try:
        usernames = _safe_dict(creds).get("usernames", {})
        return {
            "has_usernames": isinstance(usernames, (dict,)),
            "user_count": len(_safe_dict(usernames)) if isinstance(usernames, (dict,)) else 0,
            "user_keys": list(_safe_dict(usernames).keys()) if isinstance(usernames, (dict,)) else [],
        }
    except Exception:
        return {"has_usernames": False, "user_count": 0, "user_keys": []}

def _extract_credentials(secrets) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Try multiple shapes:
    1) [auth.credentials]...
    2) [credentials]...
    3) Construct from dotted subtables like [credentials.usernames.admin] (rare edge)
    """
    auth_section = _safe_dict(secrets.get("auth", {}))
    # 1) nested under [auth]
    cand = auth_section.get("credentials")
    if isinstance(cand, dict) and "usernames" in cand:
        return cand

    # 2) top-level [credentials]
    top = _safe_dict(secrets.get("credentials", {}))
    if isinstance(top, dict) and "usernames" in top:
        return top

    # 3) reconstruct if only subtables exist (defensive)
    # Some TOML layouts can end up as:
    #   secrets["credentials.usernames.admin"] -> {"name":..., "password":...}
    # Streamlit's Secrets typically doesn't flatten like this, but we’ll try.
    reconstructed = {"usernames": {}}
    for k in list(_safe_dict(secrets).keys()):
        if isinstance(k, str) and k.startswith("credentials.usernames."):
            uname = k.split(".", 2)[-1]
            reconstructed["usernames"][uname] = _safe_dict(secrets[k])
    if reconstructed["usernames"]:
        return reconstructed

    # Nothing worked
    return {}

def _load_auth_settings() -> Tuple[Dict[str, Dict[str, Dict[str, str]]], str, str, int]:
    """Load authenticator settings from Streamlit secrets if available."""
    if not hasattr(st, "secrets"):
        raise RuntimeError("Streamlit secrets not found. Configure Streamlit Cloud secrets or .streamlit/secrets.toml.")

    secrets = st.secrets
    global _USING_DEFAULT_CREDENTIALS

    auth_section = _safe_dict(secrets.get("auth", {}))
    credentials = _extract_credentials(secrets)
    if not credentials:
        # As a last resort, fall back to defaults (but also show a guided error)
        shape_summary = _summarize_credentials_shape(credentials)
        # Provide a tight, actionable error:
        raise ValueError(
            "Authentication credentials are missing or malformed: expected a table "
            "with a 'usernames' mapping. In Streamlit Cloud Secrets, use TOML tables like:\n\n"
            "[credentials]\n"
            "[credentials.usernames.admin]\n"
            "name = \"Admin User\"\n"
            "password = \"$2b$...\"\n\n"
            "and ensure [auth] has cookie_name, cookie_key, cookie_expiry_days.\n"
            f"Current parsed shape (redacted): {shape_summary}"
        )

    cookie_name = auth_section.get("cookie_name", _DEFAULT_COOKIE_NAME)
    cookie_key = auth_section.get("cookie_key", _DEFAULT_COOKIE_KEY)  # IMPORTANT: 'cookie_key' (not 'signature_key')
    expiry = auth_section.get("cookie_expiry_days", _DEFAULT_COOKIE_EXPIRY_DAYS)
    try:
        cookie_expiry_days = int(expiry)
    except (TypeError, ValueError):
        cookie_expiry_days = _DEFAULT_COOKIE_EXPIRY_DAYS

    if not isinstance(credentials, dict) or "usernames" not in credentials:
        raise ValueError(
            "Authentication credentials must include a 'usernames' mapping. "
            "Check your [credentials] or [auth.credentials] configuration in Streamlit Cloud secrets."
        )

    _USING_DEFAULT_CREDENTIALS = (credentials is _DEFAULT_CREDENTIALS)
    return credentials, cookie_name, cookie_key, cookie_expiry_days


# --- Add this tiny debug helper; call it once at top of Home.py when needed ---
def show_auth_debug():
    """Safely show the high-level secrets structure without leaking passwords."""
    try:
        auth_s = _safe_dict(st.secrets.get("auth", {}))
        creds = _extract_credentials(st.secrets)
        st.info("Auth debug (redacted)")
        st.json({
            "auth_keys": list(auth_s.keys()),
            "credentials_summary": _summarize_credentials_shape(creds),
        })
    except Exception as e:
        st.warning(f"Auth debug unavailable: {e}")
