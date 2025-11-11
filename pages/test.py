from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
import streamlit as st

from fl3xx_client import Fl3xxApiConfig
from flight_leg_utils import FlightDataError, build_fl3xx_api_config
from Home import configure_page, password_gate, render_sidebar


def _load_fl3xx_settings() -> Optional[dict[str, Any]]:
    """Return FL3XX API credentials from Streamlit secrets when available."""

    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None

    try:
        section = secrets["fl3xx_api"]
    except Exception:
        return None

    if isinstance(section, Mapping):
        return {str(key): section[key] for key in section}

    if isinstance(section, dict):  # pragma: no cover - defensive fallback
        return dict(section)

    items_getter = getattr(section, "items", None)
    if callable(items_getter):  # pragma: no cover - defensive fallback
        return dict(items_getter())

    return None


def _build_base_url(config: Fl3xxApiConfig) -> str:
    """Return the FL3XX base URL suitable for auxiliary endpoints."""

    base = config.base_url.rstrip("/")
    for suffix in ("/flight/flights", "/flight"):
        if base.lower().endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.rstrip("/")


def _extract_airport_id(payload: Any) -> Optional[int]:
    """Return an airport ID from a FL3XX airport search payload."""

    def _normalise_candidates(obj: Any) -> list[Mapping[str, Any]]:
        if isinstance(obj, list):
            return [item for item in obj if isinstance(item, Mapping)]
        if isinstance(obj, Mapping):
            items = obj.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, Mapping)]
            return [obj]
        return []

    for candidate in _normalise_candidates(payload):
        airport_id = candidate.get("id")
        if isinstance(airport_id, int):
            return airport_id
        if isinstance(airport_id, str) and airport_id.isdigit():
            return int(airport_id)
    return None


def _get_airport_id(
    session: requests.Session,
    base_url: str,
    headers: dict[str, str],
    code: str,
    *,
    timeout: int,
    verify_ssl: bool,
) -> Optional[int]:
    """Try searching FL3XX airports for a code (ICAO/IATA/FAA)."""

    attempts: list[tuple[str, dict[str, str]]] = [
        (f"{base_url}/airports/search", {"query": code}),
        (f"{base_url}/airports/search", {"searchQuery": code}),
        (f"{base_url}/airports", {"searchQuery": code}),
        (f"{base_url}/airports", {"query": code}),
    ]

    last_http_error: Optional[requests.HTTPError] = None
    successful_request = False

    for url, params in attempts:
        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                verify=verify_ssl,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            last_http_error = exc
            continue

        successful_request = True
        airport_id = _extract_airport_id(response.json())
        if airport_id is not None:
            return airport_id

    if not successful_request and last_http_error is not None:
        raise last_http_error

    return None


def _fetch_default_fbo(
    session: requests.Session,
    base_url: str,
    headers: dict[str, str],
    airport_id: int,
    *,
    timeout: int,
    verify_ssl: bool,
) -> Optional[dict[str, Any]]:
    response = session.get(
        f"{base_url}/airports/{airport_id}/services",
        headers=headers,
        timeout=timeout,
        verify=verify_ssl,
    )
    response.raise_for_status()
    services = response.json()
    if not isinstance(services, list):
        return None
    for service in services:
        if not isinstance(service, Mapping):
            continue
        service_type = service.get("type")
        if isinstance(service_type, Mapping) and service_type.get("name") == "FBO":
            if service.get("mainContact") is True:
                return {
                    "Default FBO": service.get("company"),
                    "Default FBO Email": service.get("email"),
                    "Default FBO Phone": service.get("phone"),
                    "Default FBO Homepage": service.get("homepage"),
                    "Default FBO Radio": service.get("radio"),
                    "Default FBO Address": service.get("address"),
                }
    return None


# --- Page setup ---
configure_page(page_title="DEBUG PREFLIGHT / CHECKINS")
password_gate()
render_sidebar()
st.title("DEBUG: Preflight / Checkins / Legs by Tail")

fl3xx_settings = _load_fl3xx_settings()
if not fl3xx_settings:
    st.error(
        "FL3XX API credentials are missing. Add them to `.streamlit/secrets.toml` under the "
        "`fl3xx_api` section and reload the app."
    )
    st.stop()

try:
    config: Fl3xxApiConfig = build_fl3xx_api_config(fl3xx_settings)
except FlightDataError as exc:
    st.error(str(exc))
    st.stop()

headers = config.build_headers()
base_url = _build_base_url(config)
timeout = config.timeout
verify_ssl = config.verify_ssl

uploaded_file = st.file_uploader(
    "Upload airport CSV",
    type=["csv"],
    help=(
        "The file should include `ICAO`, `IATA`, and/or `FAA` columns. "
        "Rows without any of those identifiers will be skipped."
    ),
)

data_file = Path(__file__).resolve().parents[1] / "data" / "canada_airports.csv"

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
    except Exception as exc:  # pragma: no cover - user input handling
        st.error(f"Unable to read the uploaded CSV: {exc}")
        st.stop()
    source_description = f"uploaded file `{uploaded_file.name}`"
else:
    try:
        df = pd.read_csv(data_file)
    except FileNotFoundError:
        try:
            display_path = data_file.relative_to(Path.cwd())
        except ValueError:
            display_path = data_file
        st.error(
            "The airport list could not be found. Expected it at "
            f"`{display_path}` or upload your own CSV above."
        )
        st.stop()
    except Exception as exc:  # pragma: no cover - defensive fallback
        st.error(f"Unable to read the bundled airport list: {exc}")
        st.stop()
    source_description = "bundled sample `canada_airports.csv`"

st.caption(f"Processing airport list from the {source_description}.")

session = requests.Session()
results: list[dict[str, Any]] = []

for _, row in df.iterrows():
    record: dict[str, Any] = {
        "ICAO": row.get("ICAO"),
        "IATA": row.get("IATA"),
        "FAA": row.get("FAA"),
    }

    airport_id: Optional[int] = None
    search_error: Optional[str] = None

    for key in ("ICAO", "IATA", "FAA"):
        value = row.get(key)
        if pd.isna(value):
            continue
        code = str(value).strip()
        if not code:
            continue
        try:
            airport_id = _get_airport_id(
                session,
                base_url,
                headers,
                code,
                timeout=timeout,
                verify_ssl=verify_ssl,
            )
        except requests.RequestException as exc:
            search_error = f"Error searching airport {code}: {exc}"
            break

        if airport_id:
            break

    if search_error:
        record["Default FBO"] = search_error
        results.append(record)
        continue

    if not airport_id:
        record["Default FBO"] = "Airport Not Found in FL3XX"
        results.append(record)
        continue

    try:
        fbo_details = _fetch_default_fbo(
            session,
            base_url,
            headers,
            airport_id,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
    except requests.RequestException as exc:
        record["Default FBO"] = f"Error retrieving services: {exc}"
        results.append(record)
        continue

    if fbo_details:
        record.update(fbo_details)
    else:
        record["Default FBO"] = "No Default Selected"

    results.append(record)
    time.sleep(0.25)  # gentle delay to avoid rate limiting

out_df = pd.DataFrame(results)
st.dataframe(out_df)

csv_bytes = out_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "Download results as CSV",
    data=csv_bytes,
    file_name="default_fbos.csv",
    mime="text/csv",
)
st.success("✅ Done! Use the download button above to save your results.")
