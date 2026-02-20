# User-Based Authentication Rollout (Implementation + Operations Guide)

This project supports two auth modes in `Home.password_gate()`:

1. **Legacy shared password** (default): uses `app_password`.
2. **Per-user auth** (opt-in): set `enable_user_auth = true` and provide `auth_credentials` + cookie settings.

---

## 1) Streamlit Cloud secrets setup (source of truth)

For Streamlit Cloud, add secrets in:

**App Settings → Secrets**

The app reads from `st.secrets`, so Cloud-managed secrets are all you need in production.

> ✅ **Known issue fixed:** If you previously saw `TypeError: Secrets does not support item assignment` right after setting `enable_user_auth = true`, that happened because `streamlit-authenticator` mutates the credentials object internally. The app now converts `st.secrets["auth_credentials"]` into a plain mutable `dict` before passing it to `Authenticate`.

### Example secrets block (starting point)

```toml
# Legacy fallback (can be removed after full migration)
app_password = "change-me"

# Feature flag to turn on per-user auth
enable_user_auth = true

# Cookie signing settings
# Use a long random value (32+ chars)
auth_cookie_key = "replace-with-very-long-random-secret"
auth_cookie_name = "airsprint_tools_auth"
auth_cookie_expiry_days = 14

# Optional local JSONL auth log path
auth_audit_log_path = "logs/auth_events.log"

# Optional webhook for durable external auth event shipping (SIEM/log store)
# Example: HTTPS endpoint for Datadog HTTP intake, Logstash HTTP input, Splunk HEC proxy, etc.
auth_audit_webhook_url = "https://your-log-endpoint.example.com/auth-events"
auth_audit_webhook_token = "optional-bearer-token"
auth_audit_webhook_timeout_seconds = 3

# Optional webhook for high-signal alerting events (failed login / lockout)
auth_alert_webhook_url = "https://your-alert-endpoint.example.com/security"
auth_alert_webhook_token = "optional-bearer-token"

# Brute-force protection controls
auth_max_failed_attempts = 5
auth_lockout_seconds = 900
auth_backoff_base_seconds = 1
auth_backoff_cap_seconds = 8

# Optional lockout state store path (persists across browser refreshes)
auth_lockout_store_path = "logs/auth_lockouts.json"

[auth_credentials]
  [auth_credentials.usernames]

    [auth_credentials.usernames.ops_admin]
    name = "Ops Admin"
    password = "$2b$12$replace_with_bcrypt_hash"
    role = "admin"

    [auth_credentials.usernames.ops_lead]
    name = "Ops Lead"
    password = "$2b$12$replace_with_bcrypt_hash"
    role = "ops_lead"

    [auth_credentials.usernames.viewer_jane]
    name = "Jane Viewer"
    password = "$2b$12$replace_with_bcrypt_hash"
    role = "viewer"
```

---

## 2) Create real users and assign least-privilege roles

A practical role model to start with:

- `admin`: user management/security-sensitive pages.
- `ops_lead`: high-sensitivity operational tools.
- `viewer`: read-only / low-risk pages.

### Suggested rollout process

1. List each page and the data sensitivity.
2. Assign the **minimum** role needed.
3. Start with restrictive defaults (only `admin`) and then grant access where needed.

### Example page-to-role matrix

| Page | Sensitivity | Allowed roles |
|---|---|---|
| `System Diagnostics` | high | `admin` |
| `Owner Services Dashboard` | high | `admin`, `ops_lead` |
| `Flight Following Reports` | medium | `admin`, `ops_lead`, `viewer` |
| `Duty Calculator` | low | `admin`, `ops_lead`, `viewer` |

---

## 3) Hash passwords before inserting into secrets

Never store plaintext passwords in Streamlit secrets.

### One-time hash generation (local)

Run:

```bash
python - <<'PY'
import streamlit_authenticator as stauth

passwords = ["TempAdmin#2026", "TempLead#2026", "TempViewer#2026"]
hashes = stauth.Hasher(passwords).generate()
for h in hashes:
    print(h)
PY
```

Copy each resulting hash into the matching user’s `password` field in Cloud secrets.

### Operational tip

- Generate temporary passwords.
- Share via a secure channel.
- Force periodic resets by rotating hashes on a schedule.

---

## 4) Turn on feature flag in staging first, validate, then production

Use a simple phased plan:

### Stage A — Prep

- Add all required secrets in **staging**.
- Keep `enable_user_auth = false` initially.
- Confirm app still works with shared password fallback.

### Stage B — Enable per-user auth in staging

- Set `enable_user_auth = true`.
- Test each role login (`admin`, `ops_lead`, `viewer`).
- Verify denied pages show permission errors.
- Verify logout works.

### Stage C — Production cutover

- Copy validated secret structure to production.
- Enable `enable_user_auth = true` during low-traffic window.
- Keep rollback option: temporarily set flag to `false` if needed.

### Validation checklist (staging)

- Successful login for each role.
- Failed login shows expected error.
- Unauthorized role cannot access restricted page.
- Authorized role can access expected page.
- Logout clears session and requires new login.

---

## 5) Add page-level authorization (`require_role`) for sensitive tools

Add this pattern on pages with sensitive data.

### Admin-only page example

```python
from Home import configure_page, password_gate, render_sidebar, require_role

configure_page(page_title="System Diagnostics")
password_gate()
require_role("admin")
render_sidebar()
```

### Multi-role page example

```python
from Home import configure_page, password_gate, render_sidebar, require_role

configure_page(page_title="Owner Services Dashboard")
password_gate()
require_role("admin", "ops_lead")
render_sidebar()
```

### Recommended implementation order

1. Protect highest-risk pages first.
2. Then medium-risk pages.
3. Finally low-risk pages.

---

## 6) Set up log retention and review for auth events

Current implementation writes JSONL events (login/logout) to `auth_audit_log_path`.

### What to review daily/weekly

- Unexpected login times.
- Repeated failed logins (if/when added).
- Logins from users who should be inactive.
- Missing logout patterns.

### Example JSONL line

```json
{"timestamp_utc":"2026-02-19T20:35:00+00:00","event":"login_success","username":"ops_admin"}
```

### Retention baseline

- Keep at least **90 days** in active storage.
- Archive to longer-term storage (e.g., 1 year) for audits.

> Note: Streamlit Cloud filesystem persistence can be limited depending on deploy/runtime behavior. For long-term reliability, forward events to external logging/storage (e.g., S3, CloudWatch, Datadog, ELK, etc.).

### External log shipping (recommended)

The app can now forward auth events to an external webhook with:

- `auth_audit_webhook_url`
- `auth_audit_webhook_token` (optional bearer token)
- `auth_audit_webhook_timeout_seconds`

This runs alongside local JSONL logging, so you can keep lightweight local logs while sending durable copies to your SIEM.

If you do not have a webhook endpoint yet, leave webhook URL secrets blank. The app will skip external forwarding and continue with local logging only.

---

## 6.1) Failed-login telemetry, backoff, and lockout

The app can now emit high-signal auth telemetry and enforce brute-force protections:

- Event `login_failure` on invalid credentials.
- Event `login_lockout` after repeated failed attempts.
- Exponential backoff per failed attempt.
- Temporary lockout after threshold is reached.

Secrets controlling behavior:

- `auth_max_failed_attempts` (default `5`)
- `auth_lockout_seconds` (default `900`)
- `auth_backoff_base_seconds` (default `1`)
- `auth_backoff_cap_seconds` (default `8`)

Optional alert webhook for `login_failure` / `login_lockout`:

- `auth_alert_webhook_url`
- `auth_alert_webhook_token`

Lockout state is persisted in `auth_lockout_store_path`, so refreshing the page does not clear an active lockout.

Lockout scope:

- With `enable_user_auth = true`, lockout is tracked per username (a locked user does not lock out all other users).
- With shared-password fallback mode, lockout applies to the shared principal.

---

## 7) Rotate cookie key and credentials on a schedule

A simple policy to start:

- **Every 90 days**: rotate user passwords (update hashes).
- **Every 180 days**: rotate `auth_cookie_key`.
- **Immediately** on suspected compromise: rotate both.

### Rotation runbook

1. Generate new hashes (or temporary passwords) for target users.
2. Update Cloud secrets.
3. Rotate `auth_cookie_key` (this invalidates active sessions).
4. Restart/redeploy app.
5. Confirm users can re-login.
6. Record rotation date + owner.

---

## 8) Recommended next step: managed identity provider or DB-backed users

When your user list grows, secrets-only user management becomes operationally heavy.

### Why move beyond secrets-only users

- Better joiner/leaver workflow.
- Password reset flows.
- Centralized access governance.
- Stronger auditability and policy controls.

### Path A: External IdP (preferred long-term)

Use SSO/OIDC (e.g., Okta, Azure AD, Google Workspace via an auth proxy/service).

- Pros: centralized IAM, MFA, automated deprovisioning.
- Cons: integration complexity.

### Path B: DB-backed users/roles

Store users and role mappings in a managed DB.

- Pros: full app control, easier custom workflows.
- Cons: you must build secure reset/lockout/audit features.

### Migration strategy

1. Keep current role names (`admin`, `ops_lead`, `viewer`) as a stable contract.
2. Replace source of user records (from secrets → IdP/DB).
3. Keep `require_role(...)` checks unchanged in pages.
4. Run in parallel for a short validation window.

---

## 9) Immediate action plan (next 1–2 weeks)

1. Define role matrix for all pages.
2. Create named users (no shared accounts).
3. Generate bcrypt hashes and load Cloud secrets.
4. Protect top 5 sensitive pages using `require_role`.
5. Validate in staging, then enable in production.
6. Set calendar reminders for rotation + access review.

If you want, the next implementation step can be: **I can add `require_role(...)` directly to a first batch of high-sensitivity pages and include a proposed role matrix in code comments/docs.**


## 10) Troubleshooting

### Error: `TypeError: Secrets does not support item assignment`

Cause:
- `st.secrets` objects are immutable wrappers.
- `streamlit-authenticator` writes fields into credentials during runtime.

Resolution:
- Ensure you are running the latest code in this repo where auth credentials are converted to a mutable dictionary before being passed into `Authenticate`.
- Restart the Streamlit app after deployment so the updated code is loaded.

Quick verification:
- Set `enable_user_auth = true` in Cloud secrets.
- Refresh app and confirm login form renders without traceback.
