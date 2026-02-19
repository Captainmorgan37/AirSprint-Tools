# User-Based Authentication Rollout (Initial Implementation)

This project now supports two auth modes in `Home.password_gate()`:

1. **Legacy shared password** (default): uses `app_password`.
2. **Per-user auth** (opt-in): set `enable_user_auth = true` and provide `auth_credentials` + cookie settings.

## Important for Streamlit Cloud

You do **not** need to commit a local `.streamlit/secrets.toml` file.

For Streamlit Cloud, add these values in:

**App Settings → Secrets**

The app reads secrets from `st.secrets`, so Cloud-managed secrets work exactly the same as local secrets.

## What was implemented

- Feature flag: `enable_user_auth` controls whether user-based login is active.
- Username/password login with `streamlit-authenticator`.
- Session identity fields (`auth_username`, `auth_name`, `auth_role`).
- Basic role helper (`require_role`) for page-level authorization.
- Auth event logging to JSONL (`logs/auth_events.log` by default).

## Secrets to add in Streamlit Cloud

Paste a TOML block like this into Streamlit Cloud Secrets (replace sample values):

```toml
# --- Legacy fallback ---
app_password = "change-me"

# --- User auth rollout flag ---
enable_user_auth = true

# Used to sign auth cookies. Use a long random value.
auth_cookie_key = "replace-with-long-random-secret"

auth_cookie_name = "airsprint_tools_auth"
auth_cookie_expiry_days = 14

# Optional: where auth events are written as JSONL
# (if filesystem persistence is limited on your host, wire this to external logging later)
auth_audit_log_path = "logs/auth_events.log"

[auth_credentials]
  [auth_credentials.usernames]
    [auth_credentials.usernames.ops_admin]
    name = "Ops Admin"
    # Generate with streamlit_authenticator.Hasher(['StrongPasswordHere']).generate()[0]
    password = "$2b$12$replace_with_bcrypt_hash"
    role = "admin"

    [auth_credentials.usernames.ops_viewer]
    name = "Ops Viewer"
    password = "$2b$12$replace_with_bcrypt_hash"
    role = "viewer"
```

## How to protect pages by role

On any page, after `password_gate()`, add role checks:

```python
from Home import require_role

password_gate()
require_role("admin")
```

Or allow multiple:

```python
require_role("admin", "viewer")
```

## What you should do next

1. **Create real users** and assign least-privilege roles.
2. **Hash passwords** before inserting them into secrets.
3. **Turn on feature flag in staging first**, validate all pages, then production.
4. **Add page-level authorization** (`require_role`) for sensitive tools.
5. **Set up log retention/review** for auth events.
6. **Rotate cookie key and credentials** on a defined schedule.
7. **(Recommended next step)** Move users/roles to a managed identity provider or database-backed user store for lifecycle management (joiners/leavers/password reset).
