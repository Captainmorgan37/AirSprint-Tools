
import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import timedelta


REST_AFTER_ACT_PATTERNS = [
    r"\bRest\s*After\s*(FDP|Duty)\b.*\(act\)",
    r"\bRest\s*After\s*(FDP|Duty)\b.*\bact\b",
    r"\b(Post|Following)\b.*(FDP|Duty).*\(act\)",
    r"\b(Post|Following)\b.*(FDP|Duty).*\bact\b",
    r"\bTurn\s*Time\s*After\b.*\(act\)",
    r"\bTurn\s*Time\s*After\b.*\bact\b",
]
REST_AFTER_MIN_PATTERNS = [
    r"\bRest\s*After\s*(FDP|Duty)\b.*\(min\)",
    r"\bRest\s*After\s*(FDP|Duty)\b.*\bmin\b",
    r"\b(Post|Following)\b.*(FDP|Duty).*\(min\)",
    r"\b(Post|Following)\b.*(FDP|Duty).*\bmin\b",
    r"\bTurn\s*Time\s*After\b.*\(min\)",
    r"\bTurn\s*Time\s*After\b.*\bmin\b",
]
REST_BEFORE_ACT_PATTERNS = [
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\(act\)",
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\bact\b",
    r"\b(Pre|Prior)\b.*(FDP|Duty).*\(act\)",
    r"\b(Pre|Prior)\b.*(FDP|Duty).*\bact\b",
    r"\bTurn\s*Time\s*Before\b.*\(act\)",
    r"\bTurn\s*Time\s*Before\b.*\bact\b",
]
REST_BEFORE_MIN_PATTERNS = [
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\(min\)",
    r"\bRest\s*Before\s*(FDP|Duty)\b.*\bmin\b",
    r"\b(Pre|Prior)\b.*(FDP|Duty).*\(min\)",
    r"\b(Pre|Prior)\b.*(FDP|Duty).*\bmin\b",
    r"\bTurn\s*Time\s*Before\b.*\(min\)",
    r"\bTurn\s*Time\s*Before\b.*\bmin\b",
]

st.set_page_config(page_title="FTL Audit: Duty, Rest & 7d/30d Policy", layout="wide")
st.title("FTL Audit: Duty, Rest & 7d/30d Policy")

st.markdown(
    "Upload the relevant CSV exports and the app will run three checks:"
    "\n\n1) **Duty Streaks**: ≥2 and ≥3 consecutive **12+ hr duty** days _(FTL CSV)_"
    "\n2) **Short Rest**: Rest Before & Rest After FDP (act) both < 11 hours _(Duty Violation CSV)_"
    "\n3) **7d/30d Policy + Detailed Duty Violations** _(Duty Violation CSV)_"
)

# -----------------------------
# Helpers
# -----------------------------
def parse_duration_to_hours(val):
    if pd.isna(val): return np.nan
    s = str(val).strip()
    if s == "": return np.nan
    # Remove parenthetical annotations such as "(split duty)" that can appear
    # alongside the duration values so they don't interfere with parsing.
    s = re.sub(r"\([^)]*\)", "", s)
    s = s.replace("hours", ":").replace("hour", ":").replace("H", ":").replace("h", ":")
    s = s.replace(" ", "").replace("::", ":").replace(".", ":")
    m = re.match(r"^(\d{1,3}):(\d{1,2})(?::(\d{1,2}))?$", s)
    if m:
        h=int(m.group(1)); mi=int(m.group(2)); se=int(m.group(3)) if m.group(3) else 0
        return h + mi/60 + se/3600
    m2 = re.match(r"^(\d+)\s*(m|min)$", s, flags=re.I)
    if m2: return int(m2.group(1))/60.0
    if re.match(r"^\d+(\.\d+)?$", s): return float(s)
    h = re.search(r"(\d+)\s*h", s, flags=re.I)
    mi = re.search(r"(\d+)\s*m", s, flags=re.I)
    if h or mi:
        hours=int(h.group(1)) if h else 0
        minutes=int(mi.group(1)) if mi else 0
        return hours + minutes/60
    try:
        td = pd.to_timedelta(s)
        return td.total_seconds()/3600.0
    except Exception:
        return np.nan

def try_read_csv(uploaded_file):
    try:
        return pd.read_csv(uploaded_file, sep=None, engine="python", encoding="utf-8")
    except Exception:
        uploaded_file.seek(0)
        return pd.read_csv(uploaded_file, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")

# ---------- Column inference (FTL CSV) ----------
def infer_common_columns(df):
    cols = [c.strip() for c in df.columns]
    df.columns = cols
    pilot_candidates = [c for c in cols if re.search(r"(pilot|crew|user|employee|person|name)", c, re.I)]
    pilot_col = pilot_candidates[0] if pilot_candidates else None

    parsed_cache = {}

    def parsed_series(col):
        if col is None or col not in df.columns:
            return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        if col not in parsed_cache:
            parsed_cache[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        return parsed_cache[col]

    date_candidates = [c for c in cols if re.search(r"(date|day)", c, re.I)]
    # Also consider columns that explicitly mention begin/start even if they lack "date"/"day"
    date_candidates += [c for c in cols if re.search(r"(begin|start|report)", c, re.I)]
    # Preserve first occurrence order while removing duplicates
    seen = set()
    date_candidates = [c for c in date_candidates if not (c in seen or seen.add(c))]

    def date_score(name):
        lname = name.lower()
        score = 2
        if re.search(r"(begin|start|report)", lname):
            score = 0
        elif re.search(r"(off|out|dep)", lname):
            score = 1
        if re.search(r"(end|arriv|finish|complete|in|release)", lname):
            score += 3
        return score, cols.index(name)

    date_candidates = sorted(date_candidates, key=date_score)

    date_col = None
    for c in date_candidates:
        parsed = parsed_series(c)
        if parsed.notna().mean() > 0.5:
            date_col = c
            break

    return pilot_col, date_col


def infer_begin_end_columns(df, date_col=None):
    cols = [c.strip() for c in df.columns]
    df.columns = cols

    parsed_cache = {}

    def parsed_series(col):
        if col is None or col not in df.columns:
            return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        if col not in parsed_cache:
            parsed_cache[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        return parsed_cache[col]

    begin_cols = []
    end_cols = []
    for c in cols:
        lname = c.lower()
        parsed = parsed_series(c)
        if parsed.notna().mean() <= 0.2:
            continue
        if re.search(r"(begin|start|report|sign\s*in|show)", lname):
            begin_cols.append(c)
        elif re.search(r"(end|finish|release|off|arriv|complete)", lname):
            end_cols.append(c)

    if date_col:
        begin_cols = [c for c in begin_cols if c != date_col]
        end_cols = [c for c in end_cols if c != date_col]

    return begin_cols, end_cols

def infer_duty_column(df):
    cols = list(df.columns)
    duty_candidates = [c for c in cols if re.search(r"(duty).*?(time|duration|hrs|hours)?", c, re.I)]
    duty_candidates += [c for c in cols if re.search(r"(total).*duty", c, re.I)]
    seen = set(); duty_candidates = [x for x in duty_candidates if not (x in seen or seen.add(x))]
    duty_col = None; best_rate = -1.0
    for c in duty_candidates:
        sample = df[c].astype(str).head(300).tolist()
        parsed = [parse_duration_to_hours(x) for x in sample]
        rate = np.mean([not pd.isna(x) for x in parsed])
        if rate > best_rate and rate > 0.3:
            best_rate = rate; duty_col = c
    if duty_col is None:
        for c in cols:
            sample = df[c].astype(str).head(600).tolist()
            parsed = [parse_duration_to_hours(x) for x in sample]
            rate = np.mean([not pd.isna(x) for x in parsed])
            plausible = [x for x in parsed if not pd.isna(x) and 1.0 <= x <= 18.0]
            if rate > 0.4 and len(plausible) >= 10:
                duty_col = c; break
    return duty_col

def infer_duty_day_boundary_column(df):
    cols = list(df.columns)
    boundary_candidates = []
    for c in cols:
        series = df[c]
        if series.dtype.kind not in ("O", "U", "S") and not pd.api.types.is_string_dtype(series):
            continue
        values = series.astype(str).str.strip()
        mask = values.str.contains(r"\brest\b", case=False, na=False)
        if mask.sum() == 0:
            continue
        # We prefer columns that predominantly contain textual markers ("Rest")
        if (mask.sum() >= 3) or (mask.mean() >= 0.01):
            boundary_candidates.append((c, mask.sum()))
    if not boundary_candidates:
        return None
    boundary_candidates.sort(key=lambda x: (-x[1], cols.index(x[0])))
    return boundary_candidates[0][0]


# ---------- Column inference (Duty Violation CSV) with robust Before/After ----------
def infer_policy_columns(dv_df):
    cols = [c.strip() for c in dv_df.columns]
    dv_df.columns = cols
    # Pilot
    pilot_candidates = [c for c in cols if re.search(r"(pilot|crew|user|employee|person|name)", c, re.I)]
    pilot_col = pilot_candidates[0] if pilot_candidates else None
    # 7d/30d
    c7_candidates = [c for c in cols if re.search(r"\b7\s*d(ay|ays)?\b|\b7d\b|past\s*7", c, re.I)]
    c30_candidates = [c for c in cols if re.search(r"\b30\s*d(ay|ays)?\b|\b30d\b|past\s*30", c, re.I)]
    if not c7_candidates:
        c7_candidates = [c for c in cols if re.search(r"(7d|past.?7).*(flight|block|time)", c, re.I)]
    if not c30_candidates:
        c30_candidates = [c for c in cols if re.search(r"(30d|past.?30).*(flight|block|time)", c, re.I)]
    c7 = c7_candidates[0] if c7_candidates else None
    c30 = c30_candidates[0] if c30_candidates else None

    def find_col(patterns, exclude=None):
        for pat in patterns:
            for c in cols:
                if exclude and c == exclude:
                    continue
                if re.search(pat, c, re.I):
                    return c
        return None

    rest_after_act = find_col(REST_AFTER_ACT_PATTERNS)
    rest_after_min = find_col(REST_AFTER_MIN_PATTERNS)
    rest_before_act = find_col(REST_BEFORE_ACT_PATTERNS, exclude=rest_after_act)
    rest_before_min = find_col(REST_BEFORE_MIN_PATTERNS, exclude=rest_after_min)

    # Guard: ensure distinct matches
    if rest_before_act == rest_after_act:
        rest_before_act = find_col(REST_BEFORE_ACT_PATTERNS, exclude=rest_after_act)
    if rest_before_min == rest_after_min:
        rest_before_min = find_col(REST_BEFORE_MIN_PATTERNS, exclude=rest_after_min)

    # FDP act/max
    fdp_act = next((c for c in cols if re.search(r"(Flight\s*)?Duty\s*Period.*\(act\)|\bFDP\b.*\(act\)", c, re.I)), None)
    fdp_max = next((c for c in cols if re.search(r"(Flight\s*)?Duty\s*Period.*\(max\)|\bFDP\b.*\(max\)", c, re.I)), None)

    # Date (optional)
    date_candidates = [c for c in cols if re.search(r"(date|day)", c, re.I)]
    date_col = None
    for c in date_candidates:
        parsed = pd.to_datetime(dv_df[c], errors="coerce", dayfirst=True)
        if parsed.notna().mean() > 0.5:
            date_col = c
            break

    return {
        "pilot_col": pilot_col, "c7": c7, "c30": c30,
        "rest_after_act": rest_after_act, "rest_after_min": rest_after_min,
        "rest_before_act": rest_before_act, "rest_before_min": rest_before_min,
        "fdp_act": fdp_act, "fdp_max": fdp_max, "date_col": date_col,
        "all_cols": cols,
    }

# ---------- Generic utils ----------
def to_csv_download(df, filename, key=None):
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download " + filename, data=csv_bytes, file_name=filename, mime="text/csv", key=key)

def streaks(df, flag_col, min_consecutive=3):
    sequences = []
    for pilot, sub in df.groupby("Pilot"):
        sub = sub.sort_values("Date").reset_index(drop=True)
        count = 0; start_date = None; last_date = None
        for _, r in sub.iterrows():
            if r[flag_col]:
                if last_date is not None and r["Date"] == last_date + timedelta(days=1):
                    count += 1
                else:
                    count = 1; start_date = r["Date"]
                last_date = r["Date"]
            else:
                if count >= min_consecutive and start_date is not None:
                    sequences.append({"Pilot": pilot, "StartDate": start_date.date().isoformat(),
                                      "EndDate": last_date.date().isoformat(), "ConsecutiveDays": count})
                count = 0; start_date = None; last_date = None
        if count >= min_consecutive and start_date is not None:
            sequences.append({"Pilot": pilot, "StartDate": start_date.date().isoformat(),
                              "EndDate": last_date.date().isoformat(), "ConsecutiveDays": count})
    return pd.DataFrame(sequences).sort_values(["Pilot", "StartDate"]) if sequences else pd.DataFrame(
        columns=["Pilot", "StartDate", "EndDate", "ConsecutiveDays"]
    )

def _coalesce_datetime_columns(df, primary_col, fallback_cols):
    parsed_cache = {}

    def parsed(col):
        if col is None or col not in df.columns:
            return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        if col not in parsed_cache:
            parsed_cache[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        return parsed_cache[col]

    series = parsed(primary_col)
    if fallback_cols:
        # Prefer the primary column values and only fall back when they're missing.
        # Previously we let the fallback columns override the detected "Date"
        # column which caused multi-leg duties (with different report times)
        # to be split across multiple days even when the CSV already provided a
        # single date value for the duty.  By combining the fallback values into
        # the primary series instead of the other way around we preserve the
        # duty-day date and only use begin/report timestamps when the primary
        # column is blank.
        for col in fallback_cols:
            series = series.combine_first(parsed(col))
    return series


def _normalize_dates(series):
    if series.empty:
        return series
    dtype = series.dtype
    if hasattr(dtype, "tz") and dtype.tz is not None:
        try:
            series = series.dt.tz_convert(None)
        except TypeError:
            series = series.dt.tz_localize(None)
    return series.dt.floor("D")


def build_duty_table(df, pilot_col, date_col, duty_col, min_hours=12.0, begin_cols=None, end_marker_col=None):
    df = df.copy()
    df[pilot_col] = df[pilot_col].ffill()
    df["__row_order"] = np.arange(len(df))

    begin_cols = begin_cols or []
    date_series = _coalesce_datetime_columns(df, date_col, begin_cols)
    begin_series = None
    if begin_cols:
        primary_begin = begin_cols[0]
        fallback_begin = begin_cols[1:]
        begin_series = _coalesce_datetime_columns(df, primary_begin, fallback_begin)

    df["__date"] = date_series
    df["__normalized_date"] = _normalize_dates(df["__date"])
    if begin_series is not None:
        begin_series = _normalize_dates(begin_series)
        # Prefer the duty start/report date when available so that duties that
        # cross midnight remain associated with the calendar day on which they
        # began instead of appearing as duplicate long-duty days on the
        # following date.
        df["__normalized_date"] = begin_series.combine_first(df["__normalized_date"])

    df["__duty_hours"] = df[duty_col].map(parse_duration_to_hours)

    use_marker = end_marker_col and end_marker_col in df.columns
    if use_marker:
        marker_series = df[end_marker_col].astype(str).str.strip()
        df["__duty_boundary"] = marker_series.str.contains(r"\brest\b", case=False, na=False)

    if use_marker:
        records = []
        for pilot, sub in df.groupby(pilot_col):
            sub = sub.sort_values(["__normalized_date", "__row_order"])
            current_start = None
            collected_hours = []
            for _, row in sub.iterrows():
                duty_date = row["__normalized_date"]
                if pd.isna(duty_date):
                    continue
                if current_start is None:
                    current_start = duty_date
                if not pd.isna(row["__duty_hours"]):
                    collected_hours.append(row["__duty_hours"])
                if row.get("__duty_boundary", False):
                    if collected_hours:
                        total_hours = max(collected_hours)
                    else:
                        total_hours = row["__duty_hours"]
                    if not pd.isna(total_hours) and current_start is not None:
                        records.append({"Pilot": pilot, "Date": current_start, "DutyHours": float(total_hours)})
                    current_start = None
                    collected_hours = []
            if current_start is not None and collected_hours:
                total_hours = max(collected_hours)
                if not pd.isna(total_hours):
                    records.append({"Pilot": pilot, "Date": current_start, "DutyHours": float(total_hours)})
        work = pd.DataFrame(records, columns=["Pilot", "Date", "DutyHours"])
        if work.empty:
            use_marker = False
    if not use_marker:
        work = pd.DataFrame({
            "Pilot": df[pilot_col],
            "Date": df["__normalized_date"],
            "DutyHours": df["__duty_hours"],
        })
    work = work.dropna(subset=["Pilot", "Date", "DutyHours"])
    if not work.empty:
        work["Date"] = _normalize_dates(pd.to_datetime(work["Date"], errors="coerce"))
        work = work.sort_values(["Pilot", "Date"]).groupby(["Pilot", "Date"], as_index=False)["DutyHours"].max()
    else:
        work = pd.DataFrame(columns=["Pilot", "Date", "DutyHours"])
    work["LongDuty"] = work["DutyHours"] >= float(min_hours)
    return work


def infer_rest_pair_columns_ftl(df):
    cols = [c.strip() for c in df.columns]
    df.columns = cols

    def find_col(patterns, exclude=None):
        for pat in patterns:
            for c in cols:
                if exclude and c == exclude:
                    continue
                if re.search(pat, c, re.I):
                    return c
        return None

    rest_before_act = find_col(REST_BEFORE_ACT_PATTERNS)
    rest_after_act = find_col(REST_AFTER_ACT_PATTERNS)

    if rest_before_act == rest_after_act:
        rest_before_act = find_col(REST_BEFORE_ACT_PATTERNS, exclude=rest_after_act)
    if rest_after_act == rest_before_act:
        rest_after_act = find_col(REST_AFTER_ACT_PATTERNS, exclude=rest_before_act)

    return rest_before_act, rest_after_act


def build_consecutive_short_rest_rows(df, pilot_col, date_col, rest_before_col, rest_after_col, short_thresh=11.0):
    work = df[[pilot_col, date_col, rest_before_col, rest_after_col]].copy()
    work.columns = ["Pilot", "DateRaw", "RestBeforeRaw", "RestAfterRaw"]
    work["Pilot"] = work["Pilot"].ffill()

    if date_col:
        work["DutyDate"] = pd.to_datetime(work["DateRaw"], errors="coerce", dayfirst=True)
    else:
        work["DutyDate"] = pd.NaT

    p = parse_duration_to_hours
    work["RestBeforeHours"] = work["RestBeforeRaw"].map(p)
    work["RestAfterHours"] = work["RestAfterRaw"].map(p)
    work = work.dropna(subset=["Pilot", "RestBeforeHours", "RestAfterHours"])

    if date_col:
        work = work.dropna(subset=["DutyDate"])

    if not work.empty:
        group_keys = ["Pilot"] + (["DutyDate"] if date_col else [])
        aggdict = {"RestBeforeHours": "min", "RestAfterHours": "max"}
        grouped = work.groupby(group_keys, as_index=False).agg(aggdict)
    else:
        grouped = work

    grouped["BothShort"] = (grouped["RestBeforeHours"] < float(short_thresh)) & (
        grouped["RestAfterHours"] < float(short_thresh)
    )

    flagged = grouped[grouped["BothShort"]].copy()
    if date_col and "DutyDate" in flagged.columns:
        flagged["DutyDate"] = flagged["DutyDate"].dt.date

    flagged = flagged.drop(columns=["BothShort"], errors="ignore")
    flagged = flagged.rename(columns={
        "RestBeforeHours": "RestBefore_act (hrs)",
        "RestAfterHours": "RestAfter_act (hrs)",
    })
    for col in ["RestBefore_act (hrs)", "RestAfter_act (hrs)"]:
        if col in flagged.columns:
            flagged[col] = flagged[col].round(2)
    return flagged.sort_values(["Pilot"] + (["DutyDate"] if "DutyDate" in flagged.columns else []))

def coverage_table(df, flag_col_name):
    cov = df.groupby("Pilot").agg(
        FirstDate=("Date", "min"),
        LastDate=("Date", "max"),
        DaysCount=("Date", "nunique"),
        FlaggedDays=(flag_col_name, "sum")
    ).reset_index().sort_values("Pilot")
    cov["FirstDate"] = cov["FirstDate"].dt.date
    cov["LastDate"] = cov["LastDate"].dt.date
    return cov

# -----------------------------
# Uploaders
# -----------------------------
st.sidebar.header("Uploads")
ftl_file = st.sidebar.file_uploader("FTL CSV (for Duty & Short Rest checks)", type=["csv"], key="ftl_csv")
dv_file = st.sidebar.file_uploader("Duty Violation CSV (for 7d/30d Policy + detailed checks)", type=["csv"], key="dv_csv")

# -----------------------------
# Tabs
# -----------------------------
tab_results, tab_policy, tab_debug = st.tabs(["Results (FTL)", "7d/30d Policy (Duty Violation)", "Debug"])

with tab_results:
    if ftl_file is None:
        st.info("Upload the **FTL CSV** in the sidebar to run Duty Streaks and Short Rest checks.")
    else:
        df = try_read_csv(ftl_file)
        pilot_col, date_col = infer_common_columns(df.copy())
        begin_cols, _ = infer_begin_end_columns(df.copy(), date_col=date_col)

        if not pilot_col or not date_col:
            st.error("Could not confidently identify common columns (Pilot, Date) in the FTL CSV.")
            st.write("Columns:", list(df.columns)[:60])
        else:
            duty_col = infer_duty_column(df.copy())
            duty_boundary_col = infer_duty_day_boundary_column(df.copy())
            if not duty_col:
                st.error("Could not identify Duty columns in the FTL CSV.")
            else:
                duty_work = None
                if duty_col:
                    duty_work = build_duty_table(
                        df.copy(),
                        pilot_col,
                        date_col,
                        duty_col,
                        min_hours=12.0,
                        begin_cols=begin_cols,
                        end_marker_col=duty_boundary_col,
                    )
                    seq2_duty = streaks(duty_work, "LongDuty", min_consecutive=2)
                    seq3_duty = streaks(duty_work, "LongDuty", min_consecutive=3)
                st.subheader("Duty Streaks (12+ hr days)")
                if duty_work is not None and not seq3_duty.empty:
                    pilots = sorted(seq3_duty["Pilot"].unique().tolist())
                    st.error(f"⚠️ 3-day duty rule TRIGGERED: {len(pilots)} pilot(s): {', '.join(pilots)}")
                else:
                    st.success("✅ No pilots with ≥3 consecutive 12+ hr duty days detected.")

                st.markdown("**≥ 3 consecutive 12+ hr duty days**")
                st.dataframe(seq3_duty if duty_work is not None else pd.DataFrame(), use_container_width=True)
                if duty_work is not None:
                    to_csv_download(seq3_duty, "FTL_3x12hr_Consecutive_Duty_Summary.csv", key="dl_duty3")

                with st.expander("≥ 2 consecutive 12+ hr duty days", expanded=False):
                    st.dataframe(seq2_duty if duty_work is not None else pd.DataFrame(), use_container_width=True)
                    if duty_work is not None:
                        to_csv_download(seq2_duty, "FTL_2x12hr_Consecutive_Duty_Summary.csv", key="dl_duty2")

                rest_pairs = pd.DataFrame()
                rest_before_col, rest_after_col = infer_rest_pair_columns_ftl(df.copy())
                if rest_before_col and rest_after_col and date_col:
                    rest_pairs = build_consecutive_short_rest_rows(
                        df.copy(),
                        pilot_col,
                        date_col,
                        rest_before_col,
                        rest_after_col,
                        short_thresh=11.0,
                    )

                st.markdown("**Consecutive minimum rest (< 11 h) periods**")
                if rest_before_col and rest_after_col:
                    st.caption(f"Columns used: '{rest_before_col}' + '{rest_after_col}'")
                if not date_col:
                    st.info("Could not identify the duty date column in the FTL CSV to evaluate consecutive minimum rests.")
                elif not (rest_before_col and rest_after_col):
                    st.info("Could not locate both 'Rest Before FDP (act)' and 'Rest After FDP (act)' columns in the FTL CSV.")
                elif rest_pairs.empty:
                    st.success("✅ No pilots with consecutive minimum rest (< 11 h) periods detected.")
                    st.dataframe(pd.DataFrame(), use_container_width=True)
                else:
                    pilots = sorted(rest_pairs["Pilot"].unique().tolist())
                    st.error(f"⚠️ Consecutive minimum rest triggered for {len(pilots)} pilot(s): {', '.join(pilots)}")
                    st.dataframe(rest_pairs, use_container_width=True)
                    to_csv_download(rest_pairs, "FTL_consecutive_min_rest_summary.csv", key="dl_rest_consecutive")

with tab_policy:
    if dv_file is None:
        st.info("Upload the **Duty Violation CSV** in the sidebar to run the 7d/30d policy screen and detailed checks.")
    else:
        dv = try_read_csv(dv_file)
        meta = infer_policy_columns(dv.copy())

        # Column mapping expander — hide Rest Before columns
        with st.expander("Column mapping (detected from CSV headers)"):
            mapping_view = {k: v for k, v in meta.items() 
                            if k not in ("all_cols", "rest_before_act", "rest_before_min")}
            st.write(mapping_view)

        pilot_col = meta["pilot_col"]; c7 = meta["c7"]; c30 = meta["c30"]
        rest_after_act = meta["rest_after_act"]; rest_after_min = meta["rest_after_min"]
        # Detected but hidden from mapping view:
        rest_before_act = meta["rest_before_act"]; rest_before_min = meta["rest_before_min"]
        fdp_act = meta["fdp_act"]; fdp_max = meta["fdp_max"]; date_col = meta["date_col"]

        if not (pilot_col and c7 and c30 and rest_after_act):
            st.error("Required columns missing in Duty Violation CSV (need: Pilot, 7d, 30d, Rest After FDP (act) at minimum).")
            st.write("Columns:", list(dv.columns)[:80])
        else:
            dv[pilot_col] = dv[pilot_col].ffill()
            cols = [pilot_col, c7, c30, rest_after_act] + \
                   ([rest_after_min] if rest_after_min else []) + \
                   ([rest_before_act] if rest_before_act else []) + \
                   ([rest_before_min] if rest_before_min else []) + \
                   ([fdp_act] if fdp_act else []) + \
                   ([fdp_max] if fdp_max else []) + \
                   ([date_col] if date_col else [])
            work = dv[cols].copy()

            new_cols = ["Pilot", "Hours7dRaw", "Hours30dRaw", "RestAfter_actRaw"]
            if rest_after_min: new_cols.append("RestAfter_minRaw")
            if rest_before_act: new_cols.append("RestBefore_actRaw")
            if rest_before_min: new_cols.append("RestBefore_minRaw")
            if fdp_act: new_cols.append("FDP_actRaw")
            if fdp_max: new_cols.append("FDP_maxRaw")
            if date_col: new_cols.append("Date")
            work.columns = new_cols

            # Parse
            p = parse_duration_to_hours
            work["Hours7d"] = work["Hours7dRaw"].map(p)
            work["Hours30d"] = work["Hours30dRaw"].map(p)
            work["RestAfter_act"] = work["RestAfter_actRaw"].map(p)
            if "RestAfter_minRaw" in work.columns: work["RestAfter_min"] = work["RestAfter_minRaw"].map(p)
            if "RestBefore_actRaw" in work.columns: work["RestBefore_act"] = work["RestBefore_actRaw"].map(p)
            if "RestBefore_minRaw" in work.columns: work["RestBefore_min"] = work["RestBefore_minRaw"].map(p)
            if "FDP_actRaw" in work.columns: work["FDP_act"] = work["FDP_actRaw"].map(p)
            if "FDP_maxRaw" in work.columns: work["FDP_max"] = work["FDP_maxRaw"].map(p)
            if "Date" in work.columns:
                work["Date"] = pd.to_datetime(work["Date"], errors="coerce", dayfirst=True)

            work = work.dropna(subset=["Pilot", "Hours7d", "Hours30d", "RestAfter_act"])

            # Preserve row-level view for consecutive rest pairing before we aggregate
            short_rest_rows = None
            if "RestBefore_act" in work.columns:
                short_rest_rows = work.dropna(subset=["RestBefore_act", "RestAfter_act"]).copy()
                if "Date" in short_rest_rows.columns:
                    # Present a date-only column for readability while keeping the original timestamp
                    try:
                        short_rest_rows["DutyDate"] = pd.to_datetime(short_rest_rows["Date"], errors="coerce", dayfirst=True).dt.date
                    except Exception:
                        short_rest_rows["DutyDate"] = pd.NaT
            else:
                short_rest_rows = None

            # Aggregate conservative per date (min rest) and max rolling totals
            group_keys = ["Pilot"] + (["Date"] if "Date" in work.columns else [])
            # For rest-after values we want the overnight rest that follows the entire duty, not
            # the shortest turn time that might appear on intermediate legs.  The last leg of a
            # duty period carries the longest "Rest After" value, so we aggregate using "max"
            # instead of "min" (which previously caused false short-rest violations when
            # mid-duty turns were present in the source rows).
            aggdict = {"Hours7d": "max", "Hours30d": "max", "RestAfter_act": "max"}
            if "RestAfter_min" in work.columns: aggdict["RestAfter_min"] = "max"
            if "RestBefore_act" in work.columns: aggdict["RestBefore_act"] = "min"
            if "RestBefore_min" in work.columns: aggdict["RestBefore_min"] = "min"
            if "FDP_act" in work.columns: aggdict["FDP_act"] = "max"
            if "FDP_max" in work.columns: aggdict["FDP_max"] = "max"

            work = work.sort_values(group_keys).groupby(group_keys, as_index=False).agg(aggdict)

            # 7d/30d policy with turn-time
            work["Over40in7d"] = work["Hours7d"] > 40.0
            work["Under48_7d"] = work["Hours7d"] < 48.0
            work["Under70_30d"] = work["Hours30d"] < 70.0
            work["ShortTurn"] = work["RestAfter_act"] < 11.0

            def classify(row):
                if not row["Over40in7d"]:
                    return "OK (≤40 in 7d)"
                if row["Under48_7d"] and row["Under70_30d"]:
                    return "Allowed with 11h turn (PASS)" if not row["ShortTurn"] else "Allowed with 11h turn (FAIL — rest < 11h)"
                if row["Hours7d"] >= 48.0 or row["Hours30d"] >= 70.0:
                    return "Not Allowed (≥48 in 7d or ≥70 in 30d)"
                return "Review"

            work["PolicyStatus"] = work.apply(classify, axis=1)

            # Banners
            not_allowed = work[work["PolicyStatus"].str.contains("Not Allowed", na=False)]
            exception_fail = work[work["PolicyStatus"].str.contains("FAIL", na=False)]
            exception_pass = work[work["PolicyStatus"].str.contains("PASS", na=False)]

            if not not_allowed.empty or not exception_fail.empty:
                bad_pilots = sorted(set(not_allowed["Pilot"].tolist() + exception_fail["Pilot"].tolist()))
                st.error(f"⚠️ 7d/30d Policy VIOLATIONS: {len(bad_pilots)} pilot(s): {', '.join(bad_pilots)}")
            else:
                st.success("✅ 7d/30d policy clean (no 'Not Allowed' or 'Exception FAIL').")

            # Tables
            st.markdown("**Not Allowed (≥48 in 7d or ≥70 in 30d)**")
            st.dataframe(not_allowed, use_container_width=True)
            to_csv_download(not_allowed, "DutyViolation_not_allowed_ge48_7d_or_ge70_30d.csv", key="dl_na")

            st.markdown("**Over 40 in 7d — EXCEPTION FAIL (Rest < 11 h)**")
            st.dataframe(exception_fail, use_container_width=True)
            to_csv_download(exception_fail, "DutyViolation_over40_exception_FAIL_restlt11.csv", key="dl_fail")

            st.markdown("**Over 40 in 7d — EXCEPTION PASS (Rest ≥ 11 h)**")
            st.dataframe(exception_pass, use_container_width=True)
            to_csv_download(exception_pass, "DutyViolation_over40_exception_PASS_rest_ge11.csv", key="dl_pass")

            # ---- Additional detailed duty violations ----
            st.markdown("---")
            st.subheader("Additional Duty Violations")

            # Rest After vs Min (KEEP)
            if "RestAfter_min" in work.columns:
                v_after = work.dropna(subset=["RestAfter_act", "RestAfter_min"]).copy()
                v_after = v_after[v_after["RestAfter_act"] < v_after["RestAfter_min"]]
                if not v_after.empty:
                    st.error(f"⚠️ Rest After FDP violations: {len(v_after)} row(s) (act < min)")
                else:
                    st.success("✅ No Rest After FDP (act < min) violations found.")
                st.markdown("**Rest After FDP (act) < Rest After FDP (min)**")
                st.dataframe(v_after, use_container_width=True)
                to_csv_download(v_after, "Violation_RestAfter_act_lt_min.csv", key="dl_rest_after")
            else:
                st.info("Rest After FDP (min) column not found; skipping that check.")

            # FDP act vs max (KEEP)
            if "FDP_act" in work.columns and "FDP_max" in work.columns:
                v_fdp = work.dropna(subset=["FDP_act", "FDP_max"]).copy()
                v_fdp = v_fdp[v_fdp["FDP_act"] > v_fdp["FDP_max"]]
                if not v_fdp.empty:
                    st.error(f"⚠️ FDP Max violations: {len(v_fdp)} row(s) (act > max)")
                else:
                    st.success("✅ No FDP act > max violations found.")
                st.markdown("**Flight Duty Period (act) > Flight Duty Period (max)**")
                st.dataframe(v_fdp, use_container_width=True)
                to_csv_download(v_fdp, "Violation_FDP_act_gt_max.csv", key="dl_fdp")
            else:
                st.info("FDP (act/max) columns not found; skipping that check.")

            # Consecutive short rest (Rest Before & Rest After both < 11h)
            if "RestBefore_act" in work.columns and short_rest_rows is not None:
                short_both = short_rest_rows[
                    (short_rest_rows["RestAfter_act"] < 11.0) & (short_rest_rows["RestBefore_act"] < 11.0)
                ].copy()

                if "DutyDate" in short_both.columns:
                    display_cols = ["Pilot", "DutyDate"]
                elif "Date" in short_both.columns:
                    display_cols = ["Pilot", "Date"]
                else:
                    display_cols = ["Pilot"]

                for extra_col in [
                    "RestBefore_act",
                    "RestAfter_act",
                    "RestBefore_min",
                    "RestAfter_min",
                    "Hours7d",
                    "Hours30d",
                    "FDP_act",
                    "FDP_max",
                ]:
                    if extra_col in short_both.columns:
                        display_cols.append(extra_col)

                short_both_display = short_both[display_cols].sort_values(display_cols[:2]) if len(display_cols) >= 2 else short_both[display_cols]

                if not short_both_display.empty:
                    st.error(
                        f"⚠️ Consecutive short rest: {len(short_both_display)} row(s) with Rest Before & Rest After FDP (act) < 11 h"
                    )
                else:
                    st.success("✅ No consecutive short rest (Rest Before & Rest After FDP act < 11 h) found.")
                st.markdown("**Rest Before & Rest After FDP (act) both < 11 h**")
                st.dataframe(short_both_display, use_container_width=True)
                to_csv_download(short_both_display, "Violation_RestBefore_and_After_act_lt11.csv", key="dl_rest_before_after")
            else:
                st.info("Rest Before FDP (act) column not found; skipping consecutive short rest check.")

            # Full export
            to_csv_download(work, "DutyViolation_with_Rest_and_DetailedChecks.csv", key="dl_all")

with tab_debug:
    st.caption("If you want coverage/normalized tables in this version's Debug tab, I can add them.")
