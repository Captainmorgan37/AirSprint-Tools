import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta

st.set_page_config(page_title="FTL Audit: Duty, Rest & 7d/30d Policy", layout="wide")
st.title("FTL Audit: Duty, Rest & 7d/30d Policy")

REST_AFTER_ACT_PATTERNS = [
    r"\bRest\s*After\s*(FDP|Duty)\b.*\(act\)",
    r"\bRest\s*After\s*(FDP|Duty)\b.*\bact\b",
    r"\b(Post|Following)\b.*(FDP|Duty).*\(act\)",
    r"\b(Post|Following)\b.*(FDP|Duty).*\bact\b",
    r"\bTurn\s*Time\s*After\b.*\(act\)",
    r"\bTurn\s*Time\s*After\b.*\bact\b",
]

REST_AFTER_ANY_PATTERNS = [
    r"\bRest\s*After\s*(FDP|Duty)\b",
    r"\b(Post|Following)\b.*(FDP|Duty)\b",
    r"\bTurn\s*Time\s*After\b",
]

REST_BEFORE_ACT_PATTERNS = [
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\(act\)",
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\bact\b",
    r"\bPrior\s*Rest\b.*\(act\)",
    r"\bPrior\s*Rest\b.*\bact\b",
]

REST_BEFORE_ANY_PATTERNS = [
    r"\bRest\s*Before\s*(FDP|Duty)\b",
    r"\bPrior\s*Rest\b",
]

DUTY_PATTERNS = [
    r"\b(FDP|Duty)\b.*\(act\)",
    r"\b(FDP|Duty)\b.*\bact\b",
    r"\b(FDP|Duty)\b",
]

DUTY_MIN_REST_PATTERNS = [
    r"\b(Rest\s*Before\s*(FDP|Duty)\b.*\bmin\b)",
    r"\b(Rest\s*After\s*(FDP|Duty)\b.*\bmin\b)",
    r"\bTurn\s*Time\s*After\b.*\bmin\b",
    r"\bTurn\s*Time\s*Between\b.*\bmin\b",
]

BOUNDARY_DAY_PATTERNS = [
    r"\b(End\s*FDP|End\s*Duty)\b.*\bRest\b",
    r"\bRest\b.*\b(End\s*FDP|End\s*Duty)\b",
    r"\bTurn\s*Time\s*After\b.*\(act\)",
    r"\bTurn\s*Time\s*After\b.*\bact\b",
]

DUTY_MIN_PATTERNS = [
    r"\b(FDP|Duty)\s*\(min\)",
    r"\b(FDP|Duty)\s*Minimum\b",
]

# ------------------------------------------------------
# Helpers
# ------------------------------------------------------


def try_read_csv(uploaded_file):
    if uploaded_file is None:
        return None
    try:
        return pd.read_csv(uploaded_file, sep=None, engine="python", encoding="utf-8")
    except Exception:
        uploaded_file.seek(0)
        try:
            return pd.read_csv(uploaded_file, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")
        except Exception:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, engine="python", encoding="utf-8", on_bad_lines="skip")


def parse_duration_to_hours(val):
    if pd.isna(val):
        return np.nan

    # Excel datetime duration fix
    # FL3XX sometimes encodes durations (e.g. "30:02") as a datetime
    # (e.g. 1900-01-02 06:02:00). Convert that to hours.
    try:
        if isinstance(val, (pd.Timestamp, datetime)) or ("datetime" in str(type(val)).lower()):
            dt = pd.to_datetime(val, errors="coerce")
            if pd.notna(dt):
                hours = dt.hour + dt.minute / 60 + dt.second / 3600
                if dt.day > 1:
                    hours += (dt.day - 1) * 24
                return hours
    except Exception:
        pass

    s = str(val).strip()
    if s == "":
        return np.nan

    # Remove annotations like "(split duty)"
    s = re.sub(r"\([^)]*\)", "", s)

    # Normalize formats
    s = s.replace("hours", ":").replace("hour", ":").replace("H", ":").replace("h", ":")
    s = s.replace(" ", "").replace("::", ":").replace(".", ":")

    # HH:MM or HHH:MM:SS
    m = re.match(r"^(\d{1,3}):(\d{1,2})(?::(\d{1,2}))?$", s)
    if m:
        h = int(m.group(1))
        mi = int(m.group(2))
        se = int(m.group(3)) if m.group(3) else 0
        return h + mi / 60 + se / 3600

    # "45m" or "45 min"
    m2 = re.match(r"^(\d+)\s*(m|min)$", s, flags=re.I)
    if m2:
        return int(m2.group(1)) / 60.0

    # pure number "12.5"
    if re.match(r"^\d+(\.\d+)?$", s):
        return float(s)

    # "12h 30m"
    h = re.search(r"(\d+)\s*h", s, flags=re.I)
    mi = re.search(r"(\d+)\s*m", s, flags=re.I)
    if h or mi:
        hours = int(h.group(1)) if h else 0
        minutes = int(mi.group(1)) if mi else 0
        return hours + minutes / 60

    # last resort: timedelta
    try:
        td = pd.to_timedelta(s)
        return td.total_seconds() / 3600.0
    except Exception:
        return np.nan


def parse_date(val):
    if pd.isna(val):
        return pd.NaT
    s = str(val).strip()
    if not s:
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            continue
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT


def infer_common_columns(df: pd.DataFrame):
    """Try to infer pilot/name and date columns from the FTL CSV."""
    cols_lower = [c.lower() for c in df.columns]

    pilot_col = None
    date_col = None
    for c in df.columns:
        cl = c.lower()
        if any(k in cl for k in ["name", "crew", "pilot"]) and pilot_col is None:
            pilot_col = c
        if "date" in cl and date_col is None:
            date_col = c

    if pilot_col is None and df.shape[1] > 0:
        pilot_col = df.columns[0]
    if date_col is None and df.shape[1] > 1:
        for c, cl in zip(df.columns, cols_lower):
            if any(k in cl for k in ["fltdate", "flightdate"]):
                date_col = c
                break
        if date_col is None:
            date_col = df.columns[1]

    return pilot_col, date_col


def infer_begin_end_columns(df: pd.DataFrame, date_col: str):
    begin_cols = []
    end_cols = []
    for c in df.columns:
        cl = c.lower()
        if any(k in cl for k in ["report", "duty start", "start duty", "check in", "begin duty"]):
            begin_cols.append(c)
        if any(k in cl for k in ["end duty", "off duty", "duty off", "check out"]):
            end_cols.append(c)
    return begin_cols, end_cols


def infer_duty_column(df: pd.DataFrame):
    for c in df.columns:
        cl = c.lower()
        if "duty" in cl and any(k in cl for k in ["act", "actual"]):
            return c
    for c in df.columns:
        cl = c.lower()
        if "duty" in cl:
            return c
    return df.columns[-1]


def infer_duty_day_boundary_column(df: pd.DataFrame):
    for c in df.columns:
        cl = c.lower()
        if "rest" in cl and ("after" in cl or "turn time after" in cl):
            return c
    for c in df.columns:
        cl = c.lower()
        if any(k in cl for k in ["rest", "turn time", "off duty"]):
            return c
    return df.columns[-1]


def infer_rest_before_column(df: pd.DataFrame):
    candidates = []
    for c in df.columns:
        cl = c.lower()
        if any(re.search(p, c, flags=re.I) for p in REST_BEFORE_ACT_PATTERNS):
            candidates.append(c)
    if candidates:
        return candidates[0]
    for c in df.columns:
        cl = c.lower()
        if any(re.search(p, c, flags=re.I) for p in REST_BEFORE_ANY_PATTERNS):
            candidates.append(c)
    if candidates:
        return candidates[0]
    for c in df.columns:
        if "rest before" in c.lower():
            return c
    return None


def infer_rest_after_column(df: pd.DataFrame):
    candidates = []
    for c in df.columns:
        cl = c.lower()
        if any(re.search(p, c, flags=re.I) for p in REST_AFTER_ACT_PATTERNS):
            candidates.append(c)
    if candidates:
        return candidates[0]
    for c in df.columns:
        cl = c.lower()
        if any(re.search(p, c, flags=re.I) for p in REST_AFTER_ANY_PATTERNS):
            candidates.append(c)
    if candidates:
        return candidates[0]
    for c in df.columns:
        if "rest after" in c.lower() or "turn time after" in c.lower():
            return c
    return None


def infer_duty_min_rest_columns(df: pd.DataFrame):
    cols = []
    for c in df.columns:
        if any(re.search(p, c, flags=re.I) for p in DUTY_MIN_REST_PATTERNS):
            cols.append(c)
    return cols


def infer_duty_min_column(df: pd.DataFrame):
    for c in df.columns:
        if any(re.search(p, c, flags=re.I) for p in DUTY_MIN_PATTERNS):
            return c
    for c in df.columns:
        if "min" in c.lower() and ("fdp" in c.lower() or "duty" in c.lower()):
            return c
    return None


def to_csv_download(df, filename: str, key: str):
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download " + filename, csv, file_name=filename, mime="text/csv", key=key)


def mark_short_rest_rows(df, rest_before_col, rest_after_col):
    df["RestBeforeHours"] = df[rest_before_col].map(parse_duration_to_hours)
    df["RestAfterHours"] = df[rest_after_col].map(parse_duration_to_hours)
    df["ShortRestBefore"] = df["RestBeforeHours"] < 11
    df["ShortRestAfter"] = df["RestAfterHours"] < 11
    return df


def compute_consecutive_duty_days(df, pilot_col, date_col, duty_col, threshold_hours=12.0):
    df = df.copy()
    df["DutyHours"] = df[duty_col].map(parse_duration_to_hours)
    df["DateParsed"] = df[date_col].map(parse_date)
    df = df.dropna(subset=[pilot_col, "DateParsed"]).sort_values([pilot_col, "DateParsed"])

    df["IsLong"] = df["DutyHours"] >= threshold_hours
    df["ConsecutiveLongCount"] = 0

    streaks = []
    for pilot, sub in df.groupby(pilot_col):
        sub = sub.sort_values("DateParsed")
        streak = 0
        for idx, row in sub.iterrows():
            if row["IsLong"]:
                streak += 1
            else:
                streak = 0
            df.loc[idx, "ConsecutiveLongCount"] = streak
        streaks.append(sub)

    return df


def compute_7d30d_violations(dv_df):
    df = dv_df.copy()
    df.columns = [str(c) for c in df.columns]
    date_col = None
    pilot_col = None
    flight_time_col = None
    window_col = None

    for c in df.columns:
        cl = c.lower()
        if "name" in cl or "crew" in cl or "pilot" in cl:
            pilot_col = c
        if "date" in cl and date_col is None:
            date_col = c
        if "flight time" in cl and flight_time_col is None:
            flight_time_col = c
        if "window" in cl or "period" in cl:
            window_col = c

    df["DateParsed"] = df[date_col].map(parse_date)
    df["FlightTimeHours"] = df[flight_time_col].map(parse_duration_to_hours)
    return df, pilot_col, date_col, flight_time_col, window_col


# -----------------------------
# Sidebar / Uploads
# -----------------------------
st.sidebar.header("Uploads")
ftl_file = st.sidebar.file_uploader("FTL CSV (for Duty & Short Rest checks)", type=["csv"], key="ftl_csv")
dv_file = st.sidebar.file_uploader("Duty Violation CSV (for 7d/30d Policy + detailed checks)", type=["csv"], key="dv_csv")

ftl_df = try_read_csv(ftl_file) if ftl_file else None
dv_df = try_read_csv(dv_file) if dv_file else None

# Persist dataframes for use across tabs (including Debug / Threshold)
st.session_state["ftl_df"] = ftl_df
st.session_state["dv_df"] = dv_df

# -----------------------------
# Tabs
# -----------------------------
tab_results, tab_rest_duty, tab_policy, tab_min_rest, tab_ft_exceed, tab_debug = st.tabs(
    [
        "Results (FTL)",
        "Rest Days 12+ hr Duty (FTL)",
        "7d/30d Policy (Duty Violation)",
        "Total 12+ Hour Duty Days",
        "Flight Time Threshold Checker",
        "Debug",
    ]
)

# -----------------------------
# Tab: Results (FTL) — Duty streaks & short rest
# -----------------------------
with tab_results:
    if ftl_df is None:
        st.info("Upload the **FTL CSV** in the sidebar to run Duty Streaks and Short Rest checks.")
    else:
        df = ftl_df
        pilot_col, date_col = infer_common_columns(df.copy())
        begin_cols, _ = infer_begin_end_columns(df.copy(), date_col=date_col)

        if not pilot_col or not date_col:
            st.error("Could not confidently identify common columns (Pilot, Date) in the FTL CSV.")
            st.write("Columns:", list(df.columns)[:60])
        else:
            duty_col = infer_duty_column(df.copy())
            duty_boundary_col = infer_duty_day_boundary_column(df.copy())
            rest_before_col = infer_rest_before_column(df.copy())
            rest_after_col = infer_rest_after_column(df.copy())

            st.subheader("Column Mapping")
            c1, c2, c3, c4 = st.columns(4)

            with c1:
                pilot_col = st.selectbox(
                    "Pilot column", df.columns, index=list(df.columns).index(pilot_col) if pilot_col in df.columns else 0
                )
            with c2:
                date_col = st.selectbox(
                    "Date column", df.columns, index=list(df.columns).index(date_col) if date_col in df.columns else 0
                )
            with c3:
                duty_col = st.selectbox(
                    "Duty (act) column",
                    df.columns,
                    index=list(df.columns).index(duty_col) if duty_col in df.columns else 0,
                )
            with c4:
                duty_boundary_col = st.selectbox(
                    "Duty-day boundary column (Rest / Turn Time After)",
                    df.columns,
                    index=list(df.columns).index(duty_boundary_col) if duty_boundary_col in df.columns else 0,
                )

            st.markdown(
                """
**Checks run:**

1. **Duty Streaks** — pilots with ≥2 or ≥3 consecutive days with duty ≥ 12h.
2. **Short Rest** — Rest Before & Rest After FDP (act) both < 11h.
                """
            )

            st.subheader("Duty Streaks (≥12 hr)")

            streak_df = compute_consecutive_duty_days(df, pilot_col, date_col, duty_col, threshold_hours=12.0)

            # Summarize pilots with any 2- or 3-day streaks
            summary_streak = (
                streak_df.groupby(pilot_col)
                .agg(
                    MaxConsecutive=("ConsecutiveLongCount", "max"),
                    DaysWith12hrPlus=("IsLong", "sum"),
                )
                .reset_index()
            )

            summary_streak["Has2DayStreak"] = summary_streak["MaxConsecutive"] >= 2
            summary_streak["Has3DayStreak"] = summary_streak["MaxConsecutive"] >= 3

            violators = summary_streak[(summary_streak["Has2DayStreak"]) | (summary_streak["Has3DayStreak"])].copy()

            if violators.empty:
                st.success("No pilots with 2-day or 3-day consecutive 12+ hr duty streaks found.")
            else:
                st.error("Pilots with 2-day or 3-day consecutive 12+ hr duty streaks identified.")
                st.dataframe(violators, use_container_width=True)
                to_csv_download(violators, "FTL_12hr_consecutive_duty_streaks.csv", key="dl_streaks")

            st.subheader("Short Rest (both Before & After FDP (act) < 11h)")

            rest_before_col = infer_rest_before_column(df.copy())
            rest_after_col = infer_rest_after_column(df.copy())

            if not rest_before_col or not rest_after_col:
                st.warning("Could not clearly identify both Rest Before and Rest After columns in the FTL CSV.")
            else:
                c5, c6 = st.columns(2)
                with c5:
                    rest_before_col = st.selectbox(
                        "Rest Before FDP (act) column",
                        df.columns,
                        index=list(df.columns).index(rest_before_col) if rest_before_col in df.columns else 0,
                    )
                with c6:
                    rest_after_col = st.selectbox(
                        "Rest After FDP (act) column",
                        df.columns,
                        index=list(df.columns).index(rest_after_col) if rest_after_col in df.columns else 0,
                    )

                rest_df = mark_short_rest_rows(df.copy(), rest_before_col, rest_after_col)

                short_rest_violations = rest_df[rest_df["ShortRestBefore"] & rest_df["ShortRestAfter"]].copy()
                if short_rest_violations.empty:
                    st.success("No rows where both Rest Before & Rest After FDP (act) are < 11h.")
                else:
                    st.error("Rows where both Rest Before & Rest After FDP (act) are < 11h were found.")
                    cols_to_show = [pilot_col, date_col, rest_before_col, rest_after_col]
                    extra_cols = [c for c in ["FDP Start", "FDP End", "Duty Start", "Duty End"] if c in rest_df.columns]
                    st.dataframe(short_rest_violations[cols_to_show + extra_cols], use_container_width=True)
                    to_csv_download(
                        short_rest_violations[cols_to_show + extra_cols],
                        "FTL_short_rest_before_after_fdp.csv",
                        key="dl_short_rest",
                    )

# -----------------------------
# Tab: Rest Days 12+ hr Duty (FTL) — Min Rest Counter tab
# -----------------------------
with tab_rest_duty:
    if ftl_df is None:
        st.info("Upload the **FTL CSV** in the sidebar to analyze days with ≥12h duty.")
    else:
        df = ftl_df.copy()

        pilot_col, date_col = infer_common_columns(df.copy())
        duty_col = infer_duty_column(df.copy())
        duty_boundary_col = infer_duty_day_boundary_column(df.copy())
        begin_cols, _ = infer_begin_end_columns(df.copy(), date_col=date_col)

        st.subheader("Column Mapping — 12+ hr Duty Days")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            pilot_col = st.selectbox(
                "Pilot column", df.columns, index=list(df.columns).index(pilot_col) if pilot_col in df.columns else 0
            )
        with c2:
            date_col = st.selectbox(
                "Date column", df.columns, index=list(df.columns).index(date_col) if date_col in df.columns else 0
            )
        with c3:
            # prefer Flight Duty Period (act) if present
            preferred_duty = None
            for col in df.columns:
                if re.search(r"flight\s*duty\s*period.*\(act\)", col, re.I):
                    preferred_duty = col
                    break
            default_duty = preferred_duty or duty_col
            duty_col = st.selectbox(
                "Duty length column (hours)",
                df.columns,
                index=list(df.columns).index(default_duty) if default_duty in df.columns else 0,
            )
        with c4:
            duty_boundary_col = st.selectbox(
                "Rest-marker column (indicates END of a duty day)",
                df.columns,
                index=list(df.columns).index(duty_boundary_col) if duty_boundary_col in df.columns else 0,
            )

        df["DutyHours"] = df[duty_col].map(parse_duration_to_hours)
        df["DateParsed"] = df[date_col].map(parse_date)
        df["DutyDate"] = df["DateParsed"].dt.date

        df["BoundaryRest"] = df[duty_boundary_col].astype(str).str.contains("rest", case=False, na=False)
        df["__row_order"] = np.arange(len(df))

        # reconstruct duty days using rest boundary
        records = []
        for pilot, sub in df.groupby(pilot_col):
            sub = sub.sort_values(["DutyDate", "__row_order"])

            current_date = None
            collected_hours = []

            for _, row in sub.iterrows():
                ddate = row["DutyDate"]
                if pd.isna(ddate):
                    continue

                if current_date is None:
                    current_date = ddate

                if not pd.isna(row["DutyHours"]):
                    collected_hours.append(row["DutyHours"])

                if row["BoundaryRest"]:
                    if collected_hours:
                        records.append({"Pilot": pilot, "Date": current_date, "DutyHours": max(collected_hours)})
                    current_date = None
                    collected_hours = []

            if current_date is not None and collected_hours:
                records.append({"Pilot": pilot, "Date": current_date, "DutyHours": max(collected_hours)})

        duty_days = pd.DataFrame(records)

        if duty_days.empty:
            st.warning("No valid duty days could be reconstructed with the selected columns.")
            st.stop()

        duty_days["Date"] = pd.to_datetime(duty_days["Date"]).dt.date
        duty_days["DutyHours"] = duty_days["DutyHours"].round(2)
        duty_days["LongDuty"] = duty_days["DutyHours"] >= 12.0

        long_only = duty_days[duty_days["LongDuty"]].copy()

        # Deduplicate: one row per (Pilot, Date), keeping final/max duty hours
        detail = (
            long_only.sort_values(["Pilot", "Date", "DutyHours"], ascending=[True, True, False])
            .groupby(["Pilot", "Date"], as_index=False)
            .first()
        )

        summary = (
            detail.groupby("Pilot")
            .agg(Days=("Date", "nunique"), AvgHours=("DutyHours", "mean"), MaxHours=("DutyHours", "max"))
            .reset_index()
        )
        summary["AvgHours"] = summary["AvgHours"].round(2)
        summary["MaxHours"] = summary["MaxHours"].round(2)

        st.subheader("Summary — Days with Duty ≥ 12.0 hr")
        if summary.empty:
            st.success("No pilots have 12+ hr duty days in this period.")
        else:
            total_days = summary["Days"].sum()
            st.error(f"⚠️ {total_days} long duty days across {len(summary)} pilots")

        st.dataframe(summary, use_container_width=True)
        to_csv_download(summary, "FTL_12hr_duty_summary.csv", key="dl_12hr_summary")

        st.subheader("Detail — Each 12+ hr Duty Day (deduplicated)")
        st.dataframe(detail, use_container_width=True)
        to_csv_download(detail, "FTL_12hr_duty_details.csv", key="dl_12hr_details")

# -----------------------------
# Tab: 7d/30d Policy (Duty Violation CSV)
# -----------------------------
with tab_policy:
    if dv_df is None:
        st.info("Upload the **Duty Violation CSV** in the sidebar to run 7d/30d policy checks.")
    else:
        policy_df, pilot_col, date_col, flight_time_col, window_col = compute_7d30d_violations(dv_df)

        st.subheader("7d/30d Policy Violations")
        st.dataframe(policy_df, use_container_width=True)
        to_csv_download(policy_df, "DutyViolation_7d30d_policy_violations.csv", key="dl_7d30d")

# -----------------------------
# Tab: Total 12+ Hour Duty Days (same logic as Rest tab, but totalized)
# -----------------------------
with tab_min_rest:
    if ftl_df is None:
        st.info("Upload the **FTL CSV** in the sidebar to run this check.")
    else:
        df = ftl_df.copy()
        pilot_col, date_col = infer_common_columns(df.copy())
        duty_col = infer_duty_column(df.copy())
        duty_boundary_col = infer_duty_day_boundary_column(df.copy())
        begin_cols, _ = infer_begin_end_columns(df.copy(), date_col=date_col)

        st.subheader("Column Mapping — Total 12+ Hour Duty Days")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            pilot_col = st.selectbox(
                "Pilot column", df.columns, index=list(df.columns).index(pilot_col) if pilot_col in df.columns else 0
            )
        with c2:
            date_col = st.selectbox(
                "Date column", df.columns, index=list(df.columns).index(date_col) if date_col in df.columns else 0
            )
        with c3:
            preferred_duty = None
            for col in df.columns:
                if re.search(r"flight\s*duty\s*period.*\(act\)", col, re.I):
                    preferred_duty = col
                    break
            default_duty = preferred_duty or duty_col
            duty_col = st.selectbox(
                "Duty length column (hours)",
                df.columns,
                index=list(df.columns).index(default_duty) if default_duty in df.columns else 0,
            )
        with c4:
            duty_boundary_col = st.selectbox(
                "Rest-marker column (indicates END of a duty day)",
                df.columns,
                index=list(df.columns).index(duty_boundary_col) if duty_boundary_col in df.columns else 0,
            )

        df["DutyHours"] = df[duty_col].map(parse_duration_to_hours)
        df["DateParsed"] = df[date_col].map(parse_date)
        df["DutyDate"] = df["DateParsed"].dt.date

        df["BoundaryRest"] = df[duty_boundary_col].astype(str).str.contains("rest", case=False, na=False)
        df["__row_order"] = np.arange(len(df))

        records = []
        for pilot, sub in df.groupby(pilot_col):
            sub = sub.sort_values(["DutyDate", "__row_order"])

            current_date = None
            collected_hours = []

            for _, row in sub.iterrows():
                ddate = row["DutyDate"]
                if pd.isna(ddate):
                    continue

                if current_date is None:
                    current_date = ddate

                if not pd.isna(row["DutyHours"]):
                    collected_hours.append(row["DutyHours"])

                if row["BoundaryRest"]:
                    if collected_hours:
                        records.append({"Pilot": pilot, "Date": current_date, "DutyHours": max(collected_hours)})
                    current_date = None
                    collected_hours = []

            if current_date is not None and collected_hours:
                records.append({"Pilot": pilot, "Date": current_date, "DutyHours": max(collected_hours)})

        duty_days = pd.DataFrame(records)

        if duty_days.empty:
            st.warning("No valid duty days could be reconstructed with the selected columns.")
            st.stop()

        duty_days["Date"] = pd.to_datetime(duty_days["Date"]).dt.date
        duty_days["DutyHours"] = duty_days["DutyHours"].round(2)
        duty_days["LongDuty"] = duty_days["DutyHours"] >= 12.0

        long_only = duty_days[duty_days["LongDuty"]].copy()
        detail = (
            long_only.sort_values(["Pilot", "Date", "DutyHours"], ascending=[True, True, False])
            .groupby(["Pilot", "Date"], as_index=False)
            .first()
        )

        summary = (
            detail.groupby("Pilot")
            .agg(
                Days=("Date", "nunique"),
                AvgHours=("DutyHours", "mean"),
                MaxHours=("DutyHours", "max"),
            )
            .reset_index()
        )
        summary["AvgHours"] = summary["AvgHours"].round(2)
        summary["MaxHours"] = summary["MaxHours"].round(2)

        st.subheader("Summary — Total 12+ hr Duty Days per Pilot")
        if summary.empty:
            st.success("No pilots have 12+ hr duty days in this period.")
        else:
            total_days = summary["Days"].sum()
            st.error(f"⚠️ {total_days} long duty days across {len(summary)} pilots")

        st.dataframe(summary, use_container_width=True)
        to_csv_download(summary, "FTL_total_12hr_duty_summary.csv", key="dl_total12_summary")

        st.subheader("Detail — Each 12+ hr Duty Day (deduplicated)")
        st.dataframe(detail, use_container_width=True)
        to_csv_download(detail, "FTL_total_12hr_duty_details.csv", key="dl_total12_details")

# -----------------------------
# Tab: Flight Time Threshold Checker (NEW robust version)
# -----------------------------
with tab_ft_exceed:
    st.header("Flight Time Threshold Checker")

    # Prefer the shared FTL dataframe from session state so this tab
    # works even if other tabs haven't touched it in this run.
    df = st.session_state.get("ftl_df", ftl_df)

    if df is None:
        st.info("Upload the FTL CSV in the sidebar to run this check.")
        st.stop()

    df = df.copy()

    # Column mapping
    default_pilot_col = "Name"
    pilot_col = default_pilot_col if default_pilot_col in df.columns else df.columns[0]

    # Column O usually contains Flight Time in the standard export
    default_flight_time_col = df.columns[14] if len(df.columns) >= 15 else df.columns[-1]

    st.subheader("Column Mapping")
    c1, c2 = st.columns(2)
    with c1:
        pilot_col = st.selectbox(
            "Pilot / Name column",
            df.columns,
            index=list(df.columns).index(pilot_col),
        )
    with c2:
        flight_time_col = st.selectbox(
            "Flight Time column (per-leg or total)",
            df.columns,
            index=list(df.columns).index(default_flight_time_col),
        )

    # Threshold selector
    threshold = st.number_input(
        "Minimum Flight Time to Flag (hours)",
        min_value=0.0,
        max_value=200.0,
        value=64.0,
        step=0.5,
        format="%.2f",
        help="Pilots with total flight time at or above this value in the loaded FTL period will be highlighted.",
    )

    # Preprocess
    df["PilotName"] = df[pilot_col].ffill()
    df["FlightTimeHours"] = df[flight_time_col].map(parse_duration_to_hours)

    base = df.dropna(subset=["PilotName", "FlightTimeHours"]).copy()

    if base.empty:
        st.warning("No parsable flight time values found in the selected column.")
        st.stop()

    # Attempt to detect explicit pilot summary rows:
    summary_rows = pd.DataFrame()
    if len(df.columns) >= 15:
        detail_cols = df.columns[4:14]  # E through N inclusive
        mask_summary = (
            df[flight_time_col].astype(str).str.strip() != ""
        ) & df[detail_cols].astype(str).apply(lambda row: all(x.strip() == "" for x in row), axis=1)

        summary_rows = df.loc[mask_summary, ["PilotName", flight_time_col]].copy()
        summary_rows["FlightTimeHours"] = summary_rows[flight_time_col].map(parse_duration_to_hours)
        summary_rows = summary_rows.dropna(subset=["FlightTimeHours"])

    # If we found explicit summary rows, use those; else fall back to max per pilot.
    if not summary_rows.empty:
        totals = summary_rows.groupby("PilotName", as_index=False)["FlightTimeHours"].max()
    else:
        totals = base.groupby("PilotName", as_index=False)["FlightTimeHours"].max()

    totals["FlightTimeHours"] = totals["FlightTimeHours"].round(2)

    # Pilots exceeding threshold
    exceed = totals[totals["FlightTimeHours"] >= threshold].copy()

    st.subheader("Pilots Exceeding Flight Time Threshold")
    if exceed.empty:
        st.success(f"No pilots exceeded {threshold:.2f} hours of flight time in this period.")
    else:
        st.error(f"⚠️ {len(exceed)} pilot(s) exceeded {threshold:.2f} hours.")
        st.dataframe(exceed, use_container_width=True)
        to_csv_download(
            exceed,
            f"FTL_flight_time_exceeding_{threshold:.2f}_hours.csv",
            key="dl_ft_exceed",
        )

    # Full summary
    st.subheader("All Pilot Flight Time Totals")
    st.dataframe(totals, use_container_width=True)
    to_csv_download(
        totals,
        "FTL_flight_time_totals_all_pilots.csv",
        key="dl_ft_all",
    )

# -----------------------------
# Tab: Debug — raw inspection
# -----------------------------
with tab_debug:
    st.header("FTL Debug — Inspect Raw Columns E–O")

    df = st.session_state.get("ftl_df", ftl_df)
    if df is None:
        st.info("Upload the FTL CSV to inspect its raw structure.")
        st.stop()

    df = df.copy()

    st.write("### First 50 rows (Columns E–O + raw Name)")
    detail = df.iloc[:, 4:15].copy()  # columns E–O
    detail["raw_name"] = df.iloc[:, 0]  # column A (Name)
    st.dataframe(detail.head(50), use_container_width=True)

    st.write("### Rows where Column O (Flight Time) is non-empty")
    if len(df.columns) >= 15:
        col_O = df.columns[14]
        mask_ft = df[col_O].astype(str).str.strip() != ""
        st.dataframe(df[mask_ft].iloc[:, :15], use_container_width=True)
    else:
        st.info("File has fewer than 15 columns; cannot show Column O debug.")
