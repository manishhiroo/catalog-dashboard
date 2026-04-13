#!/usr/bin/env python3
"""
Catalog Health Dashboard v2 — Streamlit App
Reads from cache/ parquet files (populated by sync_data.py).
Filters: Enabled SPINs, Virtual Combos, L1/L2/Brand.
"""

import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def to_ist(ts):
    """Convert a UTC or naive timestamp to IST string."""
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
    elif isinstance(ts, datetime):
        if ts.tzinfo is None:
            dt = ts.replace(tzinfo=timezone.utc).astimezone(IST)
        else:
            dt = ts.astimezone(IST)
    else:
        return str(ts)
    return dt.strftime("%Y-%m-%d %H:%M")

st.set_page_config(page_title="Catalog Health Dashboard", layout="wide", initial_sidebar_state="expanded")

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
THRESHOLD_COMBO_MATCH = 95.0


# ── Access Control ───────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_access():
    with open(BASE_DIR / "access_control.json") as f:
        return json.load(f)


def log_login(email, name, success=True):
    """Track login attempts to a hidden log file."""
    import csv
    from datetime import datetime
    log_file = BASE_DIR / "cache" / ".login_log.csv"
    file_exists = log_file.exists()
    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "email", "name", "success", "session_id"])
        writer.writerow([
            now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            email, name, success,
            id(st.session_state)
        ])


def check_login():
    """Email-based login with admin PIN protection and login tracking."""
    if "user" in st.session_state and st.session_state.user:
        return st.session_state.user

    access = load_access()
    st.markdown("### Catalog & Master Health App")
    st.markdown("---")
    email = st.text_input("Enter your email to continue", key="login_email")

    # Admin accounts need PIN
    admin_emails = access.get("admin", [])
    need_pin = email.strip().lower() in admin_emails if email else False

    if need_pin:
        pin = st.text_input("Admin PIN", type="password", key="admin_pin")
    else:
        pin = None

    if st.button("Login", type="primary"):
        email = email.strip().lower() if email else ""
        if not email:
            st.error("Please enter your email address.")
            return None
        if email in access["users"]:
            # Admin PIN check
            if email in admin_emails:
                if pin != access.get("admin_pin", "2026"):
                    st.error("Incorrect admin PIN.")
                    log_login(email, access["users"][email]["name"], success=False)
                    return None

            st.session_state.user = access["users"][email]
            st.session_state.user["email"] = email
            log_login(email, access["users"][email]["name"], success=True)
            st.rerun()
        else:
            st.error("Access denied. Contact manish.hiroo@instamart.in for access.")
            log_login(email, "Unknown", success=False)
    return None


def has_access(user, metric):
    """Check if user has access to a metric."""
    if user.get("role") == "admin":
        return True
    return metric in user.get("access", [])


def can_download(user):
    return "download" in user.get("permissions", [])


# ── Helpers ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_config():
    with open(BASE_DIR / "catalog_config.json") as f:
        return json.load(f)

@st.cache_data(ttl=300)
def load_guidelines():
    with open(BASE_DIR / "image_guidelines_config.json") as f:
        return json.load(f)

def C(key):
    """Read cached parquet."""
    f = CACHE_DIR / f"{key}.parquet"
    return pd.read_parquet(f) if f.exists() else pd.DataFrame()


def show_sync_time(cache_keys=None):
    """Show last sync time in IST."""
    if cache_keys:
        times = []
        for key in cache_keys:
            f = CACHE_DIR / f"{key}.parquet"
            if f.exists():
                times.append(f.stat().st_mtime)
        if times:
            st.caption(f"Data updated: {to_ist(max(times))} IST")
    else:
        cache_files = list(CACHE_DIR.glob("*.parquet"))
        if cache_files:
            latest = max(f.stat().st_mtime for f in cache_files)
            st.caption(f"Last synced: {to_ist(latest)} IST")

@st.cache_data(ttl=600)
def get_enabled_set():
    df = C("pod_enabled_items")
    return set(df["ITEM_CODE"].astype(str)) if not df.empty else None

def show_table(df, key=None, height=400):
    if df.empty:
        st.info("No data.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True, height=height, key=key)

def filter_enabled(df, enabled, item_col=None):
    """Filter df to enabled items. Tries multiple column name patterns."""
    if not enabled:
        return df
    eset = get_enabled_set()
    if eset is None:
        return df
    for col in [item_col, "Item Code", "Item_Code", "ITEM_CODE", "COMBO_ITEM_CODE"]:
        if col and col in df.columns:
            return df[df[col].astype(str).isin(eset)]
    return df

JUNK_L1S = {"snacc", "Assure Packaging", "TestCategoryL1", "Packaging material",
            "Flyer", "Freebie", "Print", "Sample"}

def filter_dims(df, fs, l1="L1", l2="L2", brand="BRAND"):
    if fs.get("exclude_junk_l1", False) and l1 in df.columns:
        df = df[~df[l1].isin(JUNK_L1S)]
    if fs.get("exclude_unbranded", False) and brand in df.columns:
        df = df[~df[brand].str.lower().isin(["unbranded", "non-brand"])]
    if fs.get("l1") and l1 in df.columns:
        df = df[df[l1].isin(fs["l1"])]
    if fs.get("l2") and l2 in df.columns:
        df = df[df[l2].isin(fs["l2"])]
    if fs.get("brand") and brand in df.columns:
        df = df[df[brand].isin(fs["brand"])]
    return df


# ── Sidebar ──────────────────────────────────────────────────────────────────
def render_sidebar():
    st.sidebar.title("Catalog Health")
    # User info
    if "user" in st.session_state and st.session_state.user:
        user = st.session_state.user
        st.sidebar.caption(f"Logged in: **{user.get('name', user.get('email', ''))}**")
        if st.sidebar.button("Logout", key="logout"):
            del st.session_state.user
            st.rerun()
    st.sidebar.markdown("---")
    cache_files = list(CACHE_DIR.glob("*.parquet"))
    if cache_files:
        latest = max(f.stat().st_mtime for f in cache_files)
        ts = to_ist(latest)
        age = (time.time() - latest) / 3600
        (st.sidebar.success if age < 6 else st.sidebar.warning)(f"Synced: {ts} IST")
    else:
        st.sidebar.error("No data. Run sync_data.py")

    st.sidebar.markdown("---")
    # Filter metrics by user access
    all_metrics = ["Image Health", "ERP Assortment (BAU)", "ERP Assortment (Events)"]
    user = st.session_state.get("user", {})
    if user.get("role") == "admin":
        available_metrics = all_metrics
    else:
        available_metrics = [m for m in all_metrics if m in user.get("access", [])]
    if not available_metrics:
        available_metrics = ["Image Health"]
    all_metrics.append("Enabled Items Health")
    all_metrics.append("Shelf Life Deviation")
    all_metrics.append("SPIN Lookup")
    if user.get("role") == "admin":
        available_metrics = all_metrics
    else:
        available_metrics = [m for m in all_metrics if m in user.get("access", [])]
    metric = st.sidebar.radio("Metric", available_metrics)
    st.sidebar.markdown("---")
    st.sidebar.subheader("Filters")

    fs = {}
    fs["enabled"] = st.sidebar.checkbox("Enabled SPINs only",
        help="Only items live on storefront (enabled in 1+ pod, excl test pod 3141)")
    fs["normal_only"] = st.sidebar.checkbox("Normal items only",
        help="Exclude Virtual Combos — show only normal (non-combo) SPINs")
    fs["exclude_unbranded"] = st.sidebar.checkbox("Exclude Unbranded", value=True,
        help="Exclude items where Brand is 'Unbranded'")
    fs["exclude_junk_l1"] = st.sidebar.checkbox("Exclude non-catalog L1s", value=True,
        help="Exclude: snacc, Assure Packaging, TestCategoryL1, Packaging material, Flyer, Freebie, Print, Sample")

    for key, label in [("l1", "L1 Category"), ("l2", "L2 Category"), ("brand", "Brand")]:
        opts = C(f"filter_{key}")
        if not opts.empty:
            fs[key] = st.sidebar.multiselect(label, opts.iloc[:, 0].tolist())

    return metric, fs


# ── Image Health ─────────────────────────────────────────────────────────────
def render_image_health(fs):
    st.title("Image Health Monitor")
    show_sync_time()
    col1, col2 = st.columns([1, 3])
    with col1:
        enabled = st.toggle("Enabled SPINs Only", value=fs.get("enabled", False))
        fs["enabled"] = enabled
    with col2:
        if enabled:
            n = len(get_enabled_set() or [])
            st.info(f"Showing **storefront-live items** only ({n:,} enabled item codes)")
        else:
            st.caption("Showing **all SPINs** in catalog")

    config = load_config()
    tabs = st.tabs(["Health Trends", "Coverage", "Onboarding Health", "Half-Yearly Onboarding",
                     "Slot Standardization", "Defect Detection", "Virtual Combos", "Quality vs BK",
                     "Diff Assortment"])

    with tabs[0]: render_trends(fs)
    with tabs[1]: render_coverage(fs)
    with tabs[2]: render_onboarding(fs)
    with tabs[3]: render_halfyear_onboarding(fs)
    with tabs[4]: render_standardization(fs)
    with tabs[5]: render_defects(fs)
    with tabs[6]: render_virtual_combos(fs)
    with tabs[7]: render_quality(fs)
    with tabs[8]: render_diff_assortment(fs)


def render_trends(fs):
    st.subheader("Image Health Trends")
    show_sync_time(["metrics_history"])
    st.caption("Day-on-Day | Week-on-Week | Month-on-Month tracking")

    df = C("metrics_history")
    if df.empty or len(df) < 1:
        st.warning("No historical data yet. Trends will appear after 2+ syncs on different days.")
        st.info("Each sync saves a daily snapshot. Run sync_data.py daily to build history.")
        return

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Latest vs previous
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None

    st.markdown("### Current Snapshot")
    c1, c2, c3, c4, c5 = st.columns(5)

    def delta(col):
        if prev is not None and col in latest and col in prev.index:
            d = latest[col] - prev[col]
            return f"{d:+,.0f}" if abs(d) > 0.5 else None
        return None

    c1.metric("Total SPINs", f"{int(latest.get('total_spins', 0)):,}", delta=delta("total_spins"))
    c2.metric("Coverage %", f"{latest.get('coverage_pct', 0):.1f}%", delta=delta("coverage_pct"))
    c3.metric("Enabled SPINs", f"{int(latest.get('enabled_spins', 0)):,}", delta=delta("enabled_spins"))
    c4.metric("Enabled Coverage %", f"{latest.get('enabled_coverage_pct', 0):.1f}%", delta=delta("enabled_coverage_pct"))
    c5.metric("Combo Match %", f"{latest.get('combo_match_pct', 0):.1f}%", delta=delta("combo_match_pct"))

    if len(df) >= 2:
        st.markdown("---")
        st.markdown("### Coverage Trend")

        # Coverage % over time
        chart_df = df[["date", "coverage_pct", "enabled_coverage_pct"]].set_index("date")
        chart_df.columns = ["All SPINs %", "Enabled SPINs %"]
        st.line_chart(chart_df)

        # Absolute numbers
        st.markdown("### SPIN Counts Over Time")
        cols_chart = ["date", "total_spins", "spins_4plus", "spins_1to3", "spins_0"]
        ch2 = df[[c for c in cols_chart if c in df.columns]].set_index("date")
        ch2.columns = ["Total", "4+ Images", "1-3 Images", "0 Images"]
        st.line_chart(ch2)

        # Defect trend
        st.markdown("### Defect Trend")
        dcols = ["date", "defect_zero_images", "defect_no_main", "defect_low_count"]
        ch3 = df[[c for c in dcols if c in df.columns]].set_index("date")
        ch3.columns = ["Zero Images", "No Main", "1-3 Images"]
        st.line_chart(ch3)

        # Combo match trend
        st.markdown("### Virtual Combo Match Trend")
        ch4 = df[["date", "combo_match_pct"]].set_index("date")
        ch4.columns = ["Match %"]
        st.line_chart(ch4)

        # DoD / WoW / MoM comparison table
        st.markdown("---")
        st.markdown("### Period Comparison")
        today = df.iloc[-1]
        comparisons = []

        # DoD
        if len(df) >= 2:
            yesterday = df.iloc[-2]
            comparisons.append(build_comparison("Day-on-Day", yesterday, today))

        # WoW (7 days ago)
        week_ago = df[df["date"] <= today["date"] - pd.Timedelta(days=6)]
        if not week_ago.empty:
            comparisons.append(build_comparison("Week-on-Week", week_ago.iloc[-1], today))

        # MoM (30 days ago)
        month_ago = df[df["date"] <= today["date"] - pd.Timedelta(days=29)]
        if not month_ago.empty:
            comparisons.append(build_comparison("Month-on-Month", month_ago.iloc[-1], today))

        if comparisons:
            df_comp = pd.DataFrame(comparisons)
            show_table(df_comp, key="trends_comp", height=200)

    # Raw history
    with st.expander("Raw History Data"):
        show_table(df.sort_values("date", ascending=False), key="hist_raw", height=300)


def build_comparison(period, old, new):
    """Build a comparison row between two snapshots."""
    def pct_change(old_val, new_val):
        if old_val and old_val > 0:
            return round((new_val - old_val) / old_val * 100, 1)
        return 0

    return {
        "Period": period,
        "From Date": str(old.get("date", ""))[:10],
        "To Date": str(new.get("date", ""))[:10],
        "Total SPINs": f"{int(new.get('total_spins', 0)):,} ({pct_change(old.get('total_spins'), new.get('total_spins')):+.1f}%)",
        "Coverage %": f"{new.get('coverage_pct', 0):.1f}% ({new.get('coverage_pct', 0) - old.get('coverage_pct', 0):+.1f}pp)",
        "Enabled Coverage %": f"{new.get('enabled_coverage_pct', 0):.1f}% ({new.get('enabled_coverage_pct', 0) - old.get('enabled_coverage_pct', 0):+.1f}pp)",
        "Combo Match %": f"{new.get('combo_match_pct', 0):.1f}% ({new.get('combo_match_pct', 0) - old.get('combo_match_pct', 0):+.1f}pp)",
        "Defects (0 img)": f"{int(new.get('defect_zero_images', 0)):,} ({int(new.get('defect_zero_images', 0)) - int(old.get('defect_zero_images', 0)):+,})",
    }


def render_onboarding(fs):
    en = fs.get("enabled", False)
    label = "Enabled" if en else "All"
    st.subheader(f"Onboarding Image Health ({label})")
    show_sync_time(["spin_image_master"])
    st.caption("How fast are new SPINs getting images? Worst categories on top.")

    # Load master data and filter
    df_master = C("spin_image_master")
    if df_master.empty:
        st.warning("No data. Run sync_data.py.")
        return

    df_master = filter_enabled(df_master, en, item_col="ITEM_CODE")
    df_master = filter_dims(df_master, fs)
    df_master["CREATED_DATE"] = pd.to_datetime(df_master["CREATED_DATE"])
    today = pd.Timestamp.now().normalize()

    # Period selector
    period = st.radio("Onboarding Window", ["Last 10 Days", "Last 20 Days", "Last 30 Days"], horizontal=True)
    days = {"Last 10 Days": 10, "Last 20 Days": 20, "Last 30 Days": 30}[period]

    df = df_master[df_master["CREATED_DATE"] >= today - pd.Timedelta(days=days)].copy()

    if df.empty:
        st.info(f"No new SPINs in the last {days} days matching filters.")
        return

    # Summary
    total_new = len(df)
    complete = len(df[df["IMAGE_COUNT"] >= 4])
    partial = len(df[(df["IMAGE_COUNT"] >= 1) & (df["IMAGE_COUNT"] < 4)])
    zero = len(df[df["IMAGE_COUNT"] == 0])
    no_main = len(df[df["HAS_MAIN"] == "No"])
    pct = round(complete / max(total_new, 1) * 100, 1)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"New SPINs ({period})", f"{total_new:,}")
    c2.metric("4+ Images", f"{complete:,}", delta=f"{pct}%")
    c3.metric("1-3 Images", f"{partial:,}")
    c4.metric("0 Images", f"{zero:,}")
    c5.metric("No Main", f"{no_main:,}")

    # Worst L1 categories
    st.markdown("---")
    st.markdown(f"#### Worst L1 Categories — New SPINs ({period})")

    df_l1 = df.groupby("L1").agg(
        total=("SPIN_ID", "count"),
        complete=("IMAGE_COUNT", lambda x: (x >= 4).sum()),
        partial=("IMAGE_COUNT", lambda x: ((x >= 1) & (x < 4)).sum()),
        zero_img=("IMAGE_COUNT", lambda x: (x == 0).sum()),
        avg_images=("IMAGE_COUNT", "mean"),
    ).reset_index()
    df_l1["Coverage %"] = (df_l1["complete"] / df_l1["total"] * 100).round(1)
    df_l1["avg_images"] = df_l1["avg_images"].round(1)
    df_l1 = df_l1.sort_values("Coverage %")

    ca, cb = st.columns(2)
    with ca:
        ch = df_l1.head(15)
        if not ch.empty:
            st.bar_chart(ch.set_index("L1")["Coverage %"], horizontal=True)
    with cb:
        show_table(df_l1, key=f"onb_l1_{days}")

    # Image tagging delay — SPINs with < 4 images, how old are they?
    st.markdown("---")
    st.markdown("#### Image Tagging Delay by L1")
    st.caption("SPINs created in last 90 days still missing images — avg days waiting")

    df_90 = df_master[df_master["CREATED_DATE"] >= today - pd.Timedelta(days=90)].copy()
    df_pending = df_90[df_90["IMAGE_COUNT"] < 4].copy()
    df_pending["days_pending"] = (today - df_pending["CREATED_DATE"]).dt.days

    if not df_pending.empty:
        delay_l1 = df_pending.groupby("L1").agg(
            total=("SPIN_ID", "count"),
            avg_days=("days_pending", "mean"),
            max_days=("days_pending", "max"),
        ).reset_index()
        delay_l1["avg_days"] = delay_l1["avg_days"].round(0)
        delay_l1 = delay_l1.sort_values("avg_days", ascending=False)

        ca2, cb2 = st.columns(2)
        with ca2:
            ch2 = delay_l1.head(15)
            if not ch2.empty:
                st.bar_chart(ch2.set_index("L1")["avg_days"], horizontal=True)
        with cb2:
            show_table(delay_l1, key="delay_l1")

    # New SPIN watchlist — < 4 images, last 30 days
    st.markdown("---")
    st.markdown("#### New SPIN Watchlist — Created Last 30 Days, < 4 Images")

    df_watch = df[df["IMAGE_COUNT"] < 4].copy()
    df_watch["days_old"] = (today - df_watch["CREATED_DATE"]).dt.days

    if not df_watch.empty:
        watch_display = df_watch[["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "BRAND", "L1", "L2",
                                   "CREATED_DATE", "days_old", "IMAGE_COUNT", "HAS_MAIN"]].rename(columns={
            "SPIN_ID": "SPIN", "ITEM_CODE": "Item Code", "PRODUCT_NAME": "Product",
            "CREATED_DATE": "Created", "days_old": "Days Old", "IMAGE_COUNT": "Images", "HAS_MAIN": "Main"
        }).sort_values("Days Old", ascending=False)

        st.metric("SPINs Pending Images", f"{len(watch_display):,}")

        # By L1
        watch_l1 = df_watch.groupby("L1").agg(
            SPINs=("SPIN_ID", "count"),
            Avg_Days=("days_old", "mean"),
        ).reset_index().sort_values("SPINs", ascending=False)
        watch_l1["Avg_Days"] = watch_l1["Avg_Days"].round(0)

        with st.expander(f"By L1 ({len(watch_l1)} categories)"):
            show_table(watch_l1, key="watch_l1")

        with st.expander(f"Full Watchlist ({len(watch_display)} SPINs)"):
            show_table(watch_display, key="watchlist", height=500)
            st.download_button("Download Watchlist", watch_display.to_csv(index=False),
                               "new_spin_watchlist.csv", "text/csv")
    else:
        st.success(f"All SPINs created in last {days} days have 4+ images!")


def render_halfyear_onboarding(fs):
    """Half-yearly onboarding health: H1 (Apr-Sep 2025) vs H2 (Oct 2025 - Mar 2026)."""
    show_sync_time(["spin_image_master"])
    en = fs.get("enabled", False)
    label = "Enabled" if en else "All"
    st.subheader(f"Half-Yearly Onboarding Health ({label})")
    st.caption("Items onboarded, new brands, top brands, image health — H1 vs H2")

    df_master = C("spin_image_master")
    if df_master.empty:
        st.warning("No data. Run sync_data.py.")
        return

    df_master = filter_enabled(df_master, en, item_col="ITEM_CODE")
    df_master = filter_dims(df_master, fs)
    df_master["CREATED_DATE"] = pd.to_datetime(df_master["CREATED_DATE"])

    # Define half-year periods
    h1_start, h1_end = pd.Timestamp("2025-04-01"), pd.Timestamp("2025-09-30")
    h2_start, h2_end = pd.Timestamp("2025-10-01"), pd.Timestamp("2026-03-31")

    df_h1 = df_master[(df_master["CREATED_DATE"] >= h1_start) & (df_master["CREATED_DATE"] <= h1_end)].copy()
    df_h2 = df_master[(df_master["CREATED_DATE"] >= h2_start) & (df_master["CREATED_DATE"] <= h2_end)].copy()

    # All brands that existed before each period (for "new" brand calculation)
    brands_before_h1 = set(df_master[df_master["CREATED_DATE"] < h1_start]["BRAND"].dropna().unique())
    brands_before_h2 = set(df_master[df_master["CREATED_DATE"] < h2_start]["BRAND"].dropna().unique())

    def _half_stats(df, brands_before):
        spins = df.drop_duplicates("SPIN_ID")
        total_items = len(spins)
        all_brands = set(df["BRAND"].dropna().unique())
        new_brands = all_brands - brands_before
        complete = (spins["IMAGE_COUNT"] >= 4).sum()
        partial = ((spins["IMAGE_COUNT"] >= 1) & (spins["IMAGE_COUNT"] < 4)).sum()
        zero = (spins["IMAGE_COUNT"] == 0).sum()
        no_main = (spins["HAS_MAIN"] == "No").sum()
        coverage = round(int(complete) / max(total_items, 1) * 100, 1)
        avg_img = round(spins["IMAGE_COUNT"].mean(), 1) if total_items > 0 else 0
        top_brands = df.groupby("BRAND")["SPIN_ID"].nunique().sort_values(ascending=False).head(15)
        return {
            "items": total_items, "brands": len(all_brands), "new_brands": len(new_brands),
            "new_brand_names": new_brands, "complete": int(complete), "partial": int(partial),
            "zero": int(zero), "no_main": int(no_main), "coverage": coverage, "avg_img": avg_img,
            "top_brands": top_brands,
        }

    h1 = _half_stats(df_h1, brands_before_h1)
    h2 = _half_stats(df_h2, brands_before_h2)

    # Side-by-side comparison
    st.markdown("### H1 (Apr-Sep 2025) vs H2 (Oct 2025 - Mar 2026)")

    col1, col2 = st.columns(2)
    for col, s, lbl in [(col1, h1, "H1 (Apr-Sep 25)"), (col2, h2, "H2 (Oct 25-Mar 26)")]:
        with col:
            st.markdown(f"#### {lbl}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Items Onboarded", f"{s['items']:,}")
            m2.metric("New Brands", f"{s['new_brands']:,}")
            m3.metric("Coverage (4+ img)", f"{s['coverage']}%")
            m4.metric("Avg Images", f"{s['avg_img']}")

            m5, m6, m7 = st.columns(3)
            m5.metric("4+ Images", f"{s['complete']:,}")
            m6.metric("1-3 Images", f"{s['partial']:,}")
            m7.metric("0 Images", f"{s['zero']:,}")

    # Comparison summary table
    st.markdown("---")
    st.markdown("#### Comparison")
    comp_df = pd.DataFrame({
        "Metric": ["Items Onboarded", "Total Brands", "New Brands", "4+ Images",
                    "1-3 Images", "0 Images", "No Main Image", "Coverage %", "Avg Images"],
        "H1 (Apr-Sep 25)": [h1["items"], h1["brands"], h1["new_brands"], h1["complete"],
                            h1["partial"], h1["zero"], h1["no_main"], h1["coverage"], h1["avg_img"]],
        "H2 (Oct 25-Mar 26)": [h2["items"], h2["brands"], h2["new_brands"], h2["complete"],
                               h2["partial"], h2["zero"], h2["no_main"], h2["coverage"], h2["avg_img"]],
    })
    comp_df["Change"] = comp_df["H2 (Oct 25-Mar 26)"] - comp_df["H1 (Apr-Sep 25)"]
    show_table(comp_df, key="hy_comparison")

    # Top Brands side by side
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Top Brands — H1 (Apr-Sep 25)")
        if not h1["top_brands"].empty:
            tb1 = h1["top_brands"].reset_index()
            tb1.columns = ["Brand", "Items"]
            show_table(tb1, key="hy_top_h1")
    with col2:
        st.markdown("#### Top Brands — H2 (Oct 25-Mar 26)")
        if not h2["top_brands"].empty:
            tb2 = h2["top_brands"].reset_index()
            tb2.columns = ["Brand", "Items"]
            show_table(tb2, key="hy_top_h2")

    # Image health by L1 per half
    st.markdown("---")
    st.markdown("#### Image Health by L1")
    half_sel = st.radio("Half", ["H1 (Apr-Sep 25)", "H2 (Oct 25-Mar 26)"], horizontal=True, key="hy_l1_sel")
    df_sel = df_h1 if "H1" in half_sel else df_h2

    if not df_sel.empty:
        l1_stats = []
        for l1, grp in df_sel.groupby("L1"):
            spins = grp.drop_duplicates("SPIN_ID")
            l1_stats.append({
                "L1": l1,
                "Items": len(spins),
                "Brands": grp["BRAND"].nunique(),
                "4+ Images": int((spins["IMAGE_COUNT"] >= 4).sum()),
                "0 Images": int((spins["IMAGE_COUNT"] == 0).sum()),
                "Coverage %": round((spins["IMAGE_COUNT"] >= 4).sum() / max(len(spins), 1) * 100, 1),
                "Avg Images": round(spins["IMAGE_COUNT"].mean(), 1),
            })
        df_l1 = pd.DataFrame(l1_stats).sort_values("Coverage %")
        ca, cb = st.columns(2)
        with ca:
            ch = df_l1.head(15)
            if not ch.empty:
                st.bar_chart(ch.set_index("L1")["Coverage %"], horizontal=True)
        with cb:
            show_table(df_l1, key="hy_l1_detail")
        st.download_button("Download", df_l1.to_csv(index=False),
                           f"halfyear_l1_{half_sel[:2].lower()}.csv", "text/csv")
    else:
        st.info("No items onboarded in this period matching current filters.")


def render_coverage(fs):
    show_sync_time(["image_coverage_summary", "image_coverage_by_l1"])
    en = fs.get("enabled", False)
    sfx = "_enabled" if en else ""
    label = "Enabled" if en else "All"
    st.subheader(f"Image Count Coverage ({label})")
    st.caption("Target: 4+ images per SPIN | Sorted worst-first")

    df = C(f"image_coverage_summary{sfx}")
    if not df.empty:
        r = df.iloc[0]
        total = int(r.get("TOTAL_SPINS", 0) or 0)
        cols = st.columns(6)
        cols[0].metric(f"Total SPINs", f"{total:,}")
        s4 = int(r.get("SPINS_4PLUS", 0) or 0)
        cols[1].metric("4+ Images", f"{s4:,}", delta=f"{s4/max(total,1)*100:.1f}%")
        cols[2].metric("1-3 Images", f"{int(r.get('SPINS_1TO3', 0) or 0):,}")
        cols[3].metric("0 Images", f"{int(r.get('SPINS_0', 0) or 0):,}")
        cols[4].metric("No Main Image", f"{int(r.get('NO_MAIN_IMAGE', 0) or 0):,}")
        cols[5].metric("Avg/SPIN", f"{r.get('AVG_IMAGES', 0)}")

    st.markdown("---")

    # L1
    df_l1 = C(f"image_coverage_by_l1{sfx}")
    if not df_l1.empty:
        df_l1 = filter_dims(df_l1, fs)
        ca, cb = st.columns(2)
        with ca:
            st.markdown("#### Worst L1 (Coverage %)")
            ch = df_l1.sort_values("Coverage %").head(15)
            st.bar_chart(ch.set_index("L1")["Coverage %"], horizontal=True)
        with cb:
            st.markdown("#### Coverage by L1")
            show_table(df_l1.sort_values("Coverage %"), key="cov_l1")

    # L2
    df_l2 = C("image_coverage_by_l2")
    if not df_l2.empty:
        df_l2 = filter_dims(df_l2, fs)
        with st.expander(f"Coverage by L2 ({len(df_l2)} rows)"):
            show_table(df_l2.sort_values("Coverage %"), key="cov_l2", height=500)

    # Brand
    df_br = C("image_coverage_by_brand")
    if not df_br.empty:
        df_br = filter_dims(df_br, fs)
        with st.expander(f"Coverage by Brand (Bottom 50)"):
            show_table(df_br.sort_values("Coverage %"), key="cov_br", height=500)

    # SPIN detail
    df_sp = C("image_coverage_spins_detail")
    if not df_sp.empty:
        df_sp = filter_enabled(df_sp, en)
        df_sp = filter_dims(df_sp, fs)
        with st.expander(f"SPIN Detail — {len(df_sp)} SPINs with < 4 images"):
            show_table(df_sp, key="cov_sp", height=500)
            st.download_button("Download", df_sp.to_csv(index=False), "spins_low_images.csv", "text/csv")


def render_standardization(fs):
    show_sync_time(["image_slot_fill_rates", "image_slot_fill_by_l1"])
    en = fs.get("enabled", False)
    sfx = "_enabled" if en else ""
    label = "Enabled" if en else "All"
    st.subheader(f"Image Slot Standardization ({label})")
    st.caption("MN=Front | AL1=Back | AL2+=Category-specific")

    df_rates = C("image_slot_fill_rates")
    if not df_rates.empty:
        r = df_rates.iloc[0]
        total = int(r.get("TOTAL_SPINS", 0) or 0)
        cols = st.columns(7)
        cols[0].metric("Total SPINs", f"{total:,}")
        for i, slot in enumerate(["MN (Main/Front)", "AL1 (Back/Slot2)", "AL2 (Slot3)",
                                   "AL3 (Slot4)", "AL4 (Slot5)", "AL5 (Slot6)"]):
            val = int(r.get(slot, 0) or 0)
            cols[i+1].metric(slot.split("(")[0].strip(), f"{val/max(total,1)*100:.1f}%")

    st.markdown("---")
    df_l1 = C(f"image_slot_fill_by_l1{sfx}")
    if not df_l1.empty:
        df_l1 = filter_dims(df_l1, fs)
        ca, cb = st.columns(2)
        with ca:
            st.markdown("#### Worst L1 by AL1 Fill %")
            st.bar_chart(df_l1.sort_values("AL1 %").head(15).set_index("L1")["AL1 %"], horizontal=True)
        with cb:
            show_table(df_l1.sort_values("AL1 %"), key="slot_l1")

    with st.expander("Category Image Guidelines"):
        guidelines = load_guidelines()
        st.json(guidelines.get("mandatory_by_category", guidelines))


def render_defects(fs):
    show_sync_time(["image_defect_no_main", "image_defect_zero"])
    en = fs.get("enabled", False)
    sfx = "_enabled" if en else ""
    label = "Enabled" if en else "All"
    st.subheader(f"Image Defect Detection ({label})")

    # Detail tables — filter first, then count from filtered data
    df_no_main = C("image_defect_no_main")
    df_zero = C("image_defect_zero")
    df_low = C("image_defect_low_count")

    df_no_main = filter_dims(filter_enabled(df_no_main, en), fs)
    df_zero = filter_dims(filter_enabled(df_zero, en), fs)
    df_low = filter_dims(filter_enabled(df_low, en), fs)

    # Counts from filtered data (respects all filters including junk L1, unbranded)
    count_no_main = len(df_no_main)
    count_zero = len(df_zero)
    count_low = len(df_low)

    c1, c2, c3 = st.columns(3)
    c1.metric("Missing Main Image", f"{count_no_main:,}")
    c2.metric("Zero Images", f"{count_zero:,}")
    c3.metric("1-3 Images (Below Target)", f"{count_low:,}")

    with st.expander(f"SPINs With Zero Images ({len(df_zero)} items)"):
        if not df_zero.empty:
            show_table(df_zero, key="def_zero", height=350)
            st.download_button("Download Zero Images List", df_zero.to_csv(index=False),
                               "defect_zero_images.csv", "text/csv")
        else:
            st.info("No items with zero images")

    with st.expander(f"SPINs Missing Main Image ({len(df_no_main)} items)"):
        if not df_no_main.empty:
            show_table(df_no_main, key="def_main", height=350)
            st.download_button("Download Missing Main List", df_no_main.to_csv(index=False),
                               "defect_missing_main.csv", "text/csv")
        else:
            st.info("No items missing main image")

    with st.expander(f"SPINs with 1-3 Images ({len(df_low)} items)"):
        if not df_low.empty:
            show_table(df_low, key="def_low", height=500)
            st.download_button("Download 1-3 Images List", df_low.to_csv(index=False),
                               "defect_low_images.csv", "text/csv")
        else:
            st.info("No items with 1-3 images")

    st.markdown("---")
    st.info("Coming soon: AI-powered image analysis — 'Coming Soon' placeholder detection, wrong/irrelevant image detection, blurry image flagging, manual SPIN check tool")


def render_virtual_combos(fs):
    show_sync_time(["virtual_combo_image_match"])
    en = fs.get("enabled", False)
    label = "Enabled" if en else "All"
    st.subheader(f"Virtual Combo Image Match ({label})")
    st.caption(f"Combo SPIN must have same # of images as base SCM_SPIN_1 | Threshold: {THRESHOLD_COMBO_MATCH}%")

    df = C("virtual_combo_image_match")
    if df.empty:
        st.warning("No data. Run sync_data.py.")
        return

    df = filter_enabled(df, en)
    combo_type = st.radio("Combo Type", ["All", "Homogeneous", "Heterogeneous"], horizontal=True)
    if combo_type != "All":
        df = df[df["COMBO_TYPE"] == combo_type.upper()]
    df = filter_dims(df, fs, l1="L1", l2="L2", brand="BRAND")

    if df.empty:
        st.info("No combos match filters.")
        return

    total = len(df)
    matching = len(df[df["COUNT_MATCH"] == "Yes"])
    pct = matching / max(total, 1) * 100
    passing = pct >= THRESHOLD_COMBO_MATCH

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Combos", f"{total:,}")
    c2.metric("Count Match", f"{matching:,}")
    c3.metric("Mismatched", f"{total - matching:,}")
    c4.metric("Match Rate", f"{pct:.1f}%", delta="PASS" if passing else "FAIL",
              delta_color="normal" if passing else "inverse")

    if passing:
        st.success(f"Match Rate: **{pct:.1f}%** (threshold: {THRESHOLD_COMBO_MATCH}%)")
    else:
        st.error(f"Match Rate: **{pct:.1f}%** — BELOW {THRESHOLD_COMBO_MATCH}% THRESHOLD")

    # By L1
    st.markdown("---")
    df_l1 = C("virtual_combo_summary_by_l1")
    if not df_l1.empty:
        df_l1 = filter_dims(df_l1, fs)
        ca, cb = st.columns(2)
        with ca:
            st.markdown("#### Match Rate by L1")
            st.bar_chart(df_l1.sort_values("Match %").head(15).set_index("L1")["Match %"], horizontal=True)
        with cb:
            df_l1["Status"] = df_l1["Match %"].apply(lambda x: "PASS" if x >= THRESHOLD_COMBO_MATCH else "FAIL")
            show_table(df_l1.sort_values("Match %"), key="combo_l1")

    # Mismatch detail
    st.markdown("---")
    st.markdown("#### Mismatched Combos")
    df_mis = df[df["COUNT_MATCH"] == "No"][[
        "COMBO_SPIN", "COMBO_ITEM_CODE", "COMBO_PRODUCT_NAME", "BASE_SPIN",
        "COMBO_TYPE", "L1", "BRAND", "BASE_IMAGE_COUNT", "COMBO_IMAGE_COUNT",
        "IMAGE_MATCH_PCT", "MAIN_IMAGE_MATCH"
    ]].drop_duplicates(subset=["COMBO_SPIN"]).sort_values("IMAGE_MATCH_PCT")

    if not df_mis.empty:
        show_table(df_mis, key="combo_mis", height=500)
        st.download_button("Download Mismatch", df_mis.to_csv(index=False), "combo_mismatch.csv", "text/csv")
    else:
        st.success("All combos match!")

    with st.expander("Full Status"):
        df_full = df[["COMBO_SPIN", "COMBO_ITEM_CODE", "BASE_SPIN", "COMBO_TYPE", "L1", "BRAND",
                       "BASE_IMAGE_COUNT", "COMBO_IMAGE_COUNT", "COUNT_MATCH", "MAIN_IMAGE_MATCH"
                       ]].drop_duplicates(subset=["COMBO_SPIN"])
        show_table(df_full, key="combo_full", height=500)


def render_quality(fs):
    st.subheader("Image Quality: IM vs BK")
    show_sync_time(["image_compare_results"])
    st.caption("BK Benchmark: 33K+ products matched to IM SPINs")

    df = C("bk_benchmark")
    if df.empty:
        st.warning("No BK benchmark data. Place BK Match Benchmark.xlsx and run parse.")
        return

    en = fs.get("enabled", False)

    # Summary
    total = len(df)
    true_match = len(df[df["Final SKU Match flag"] == "true_match"])
    sf_enabled = pd.to_numeric(df["SF_ENABLED"], errors="coerce")
    enabled_count = int((sf_enabled == 1).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("BK Products Mapped", f"{total:,}")
    c2.metric("True Match", f"{true_match:,}", delta=f"{true_match/max(total,1)*100:.1f}%")
    c3.metric("SF Enabled", f"{enabled_count:,}")
    c4.metric("BK Categories", f"{df['BK category_name'].nunique()}")

    # By BK category
    st.markdown("---")
    st.markdown("#### Match Rate by BK Category")
    cat_stats = df.groupby("BK category_name").agg(
        Total=("BK_product_id", "count"),
        True_Match=("Final SKU Match flag", lambda x: (x == "true_match").sum()),
        Avg_Score=("Match score", lambda x: pd.to_numeric(x, errors="coerce").mean()),
    ).reset_index()
    cat_stats["Match %"] = (cat_stats["True_Match"] / cat_stats["Total"] * 100).round(1)
    cat_stats["Avg_Score"] = cat_stats["Avg_Score"].round(1)
    cat_stats = cat_stats.sort_values("Match %")

    ca3, cb3 = st.columns(2)
    with ca3:
        st.bar_chart(cat_stats.head(15).set_index("BK category_name")["Match %"], horizontal=True)
    with cb3:
        show_table(cat_stats, key="bk_cat")

    # Brand match analysis
    st.markdown("---")
    st.markdown("#### Brand Match Status")
    brand_status = df["Final Brand match tag"].value_counts().reset_index()
    brand_status.columns = ["Status", "Count"]
    show_table(brand_status, key="bk_brand_status", height=200)

    # Image quality comparison results
    st.markdown("---")
    st.markdown("#### Image Quality Comparison (Side-by-Side)")
    df_compare = C("image_quality_comparison")
    if df_compare.empty:
        st.info("Run `python catalog_dashboard/image_compare_v2.py` to analyze images.")
    else:
        valid = df_compare.dropna(subset=["bk_width"])
        im_valid = valid.dropna(subset=["im_width"])

        # Summary
        bk_sc = pd.to_numeric(valid.get("bk_avg_score", pd.Series()), errors="coerce")
        im_sc = pd.to_numeric(valid.get("im_avg_score", pd.Series()), errors="coerce")
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Products Compared", len(valid))
        c2.metric("BK Avg Score", f"{bk_sc.mean():.1f}/100")
        c3.metric("IM Avg Score", f"{im_sc.mean():.1f}/100")
        c4.metric("BK Avg Images", f"{pd.to_numeric(valid['bk_image_count'], errors='coerce').mean():.1f}")
        c5.metric("IM Avg Images", f"{pd.to_numeric(valid['im_image_count'], errors='coerce').mean():.1f}")
        c6.metric("BK Avg Resolution", f"{pd.to_numeric(valid['bk_width'], errors='coerce').mean():.0f}px")

        # Wins summary
        st.markdown("##### Who Wins? (Main Image)")
        w1, w2, w3, w4 = st.columns(4)
        count_wins = valid["count_winner"].value_counts().to_dict()
        res_wins = valid["resolution_winner"].value_counts().to_dict()
        blur_col = next((c for c in ["main_sharpness_winner", "avg_blur_winner", "blur_winner"] if c in valid.columns), None)
        blur_wins = valid[blur_col].value_counts().to_dict() if blur_col else {}
        w1.metric("Image Count", f"BK {count_wins.get('BK',0)} — IM {count_wins.get('IM',0)}")
        w2.metric("Resolution", f"IM {res_wins.get('IM',0)} — BK {res_wins.get('BK',0)}")
        w3.metric("Sharpness", f"BK {blur_wins.get('BK',0)} — IM {blur_wins.get('IM',0)}")

        # Video gap
        if "video_gap" in valid.columns:
            bk_vid = int(valid.get("bk_has_video", pd.Series([False])).sum())
            im_vid = int(valid.get("im_has_video", pd.Series([False])).sum())
            bk_only = len(valid[valid["video_gap"] == "BK has, IM missing"])
            w4.metric("Video Gap (BK has, IM missing)", f"{bk_only}")

            st.markdown("##### Video Comparison")
            vc1, vc2, vc3 = st.columns(3)
            vc1.metric("BK with Video", f"{bk_vid}/{len(valid)}")
            vc2.metric("IM with Video", f"{im_vid}/{len(valid)}")
            vc3.metric("IM Missing Video", f"{bk_only}", delta=f"Action needed" if bk_only > 0 else "All good",
                       delta_color="inverse" if bk_only > 0 else "normal")

            # Video gap detail
            if bk_only > 0:
                gap_df = valid[valid["video_gap"] == "BK has, IM missing"][["spin", "bk_product_name", "bk_category"]]
                with st.expander(f"Products where BK has video but IM doesn't ({bk_only})"):
                    show_table(gap_df, key="vid_gap")

        # Full comparison table
        st.markdown("---")
        st.markdown("##### Slot-wise Comparison (All Images Scored)")
        # Scoring weightage
        st.markdown("##### Scoring Weightage")
        st.caption("Each image scored 0-100: **Resolution (25pts)** | **Sharpness (25pts)** | **Contrast (20pts)** | **Edge Detail (15pts)** | **File Richness (10pts)** | Penalties: upscaled (-10), very blurry (-15)")

        # Summary chart
        st.markdown("##### Score Card")
        metrics_data = {
            "Metric": ["Image Count", "Resolution", "Main Sharpness", "Main Contrast", "Main Score", "Avg Sharpness", "Avg Contrast", "Overall Score", "Video"],
            "Weight": ["—", "25 pts", "25 pts", "20 pts", "100 pts", "25 pts", "20 pts", "100 pts", "—"],
            "BK": [
                f"{pd.to_numeric(valid['bk_image_count'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_width'], errors='coerce').mean():.0f}px",
                f"{pd.to_numeric(valid['bk_main_blur'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_main_contrast'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_main_score'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_avg_blur'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_avg_contrast'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['bk_avg_score'], errors='coerce').mean():.1f}",
                f"{(valid['bk_has_video'].astype(str) == 'True').sum()}/{len(valid)}",
            ],
            "IM": [
                f"{pd.to_numeric(valid['im_image_count'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_width'], errors='coerce').mean():.0f}px",
                f"{pd.to_numeric(valid['im_main_blur'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_main_contrast'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_main_score'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_avg_blur'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_avg_contrast'], errors='coerce').mean():.1f}",
                f"{pd.to_numeric(valid['im_avg_score'], errors='coerce').mean():.1f}",
                "0/{0}".format(len(valid)),
            ],
        }
        winners = []
        for i, metric in enumerate(metrics_data["Metric"]):
            bk_val = metrics_data["BK"][i]
            im_val = metrics_data["IM"][i]
            try:
                bk_n = float(bk_val.replace("px", "").split("/")[0])
                im_n = float(im_val.replace("px", "").split("/")[0])
                winners.append("BK" if bk_n > im_n else ("IM" if im_n > bk_n else "Tie"))
            except:
                winners.append("-")
        metrics_data["Winner"] = winners
        st.dataframe(pd.DataFrame(metrics_data), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("##### Slot-wise Comparison (All Images Scored)")
        table_cols = ["spin", "bk_product_name", "bk_category",
                      "bk_image_count", "im_image_count", "bk_video_count", "count_winner",
                      "bk_main_res", "im_main_res", "resolution_winner",
                      "bk_main_blur", "im_main_blur", "main_sharpness_winner",
                      "bk_main_contrast", "im_main_contrast", "main_contrast_winner",
                      "bk_main_score", "im_main_score", "main_score_winner",
                      "bk_avg_blur", "im_avg_blur", "avg_sharpness_winner",
                      "bk_avg_contrast", "im_avg_contrast", "avg_contrast_winner",
                      "bk_avg_score", "im_avg_score", "overall_winner"]
        df_table = valid[[c for c in table_cols if c in valid.columns]].copy()
        # Rename blur to sharpness for clarity
        rename_map = {
            "bk_main_blur": "bk_main_sharpness", "im_main_blur": "im_main_sharpness",
            "bk_avg_blur": "bk_avg_sharpness", "im_avg_blur": "im_avg_sharpness",
        }
        df_table = df_table.rename(columns={k: v for k, v in rename_map.items() if k in df_table.columns})
        show_table(df_table, key="img_compare", height=450)

        # Text readability
        if "bk_text_confidence" in valid.columns:
            st.markdown("---")
            st.markdown("##### Text Readability (OCR)")
            text_cols = ["spin", "bk_product_name",
                         "bk_text_confidence", "im_text_confidence",
                         "bk_text_readable", "im_text_readable",
                         "bk_text_found", "im_text_found"]
            show_table(valid[[c for c in text_cols if c in valid.columns]], key="text_read", height=350)

        # Product mismatch & multi-product flags
        if "flags" in valid.columns:
            flagged = valid[valid["flags"].astype(str) != ""]
            if not flagged.empty:
                st.markdown("---")
                st.error(f"**{len(flagged)} products flagged** — review needed")
                flag_cols = ["spin", "bk_product_name", "im_product_name", "flags",
                             "im_name_match", "im_objects", "im_likely_upscaled"]
                show_table(flagged[[c for c in flag_cols if c in flagged.columns]], key="flags", height=300)

        # Side-by-side image preview
        st.markdown("---")
        st.markdown("##### Side-by-Side Preview")

        # SPIN selector — upload list or select from dropdown
        preview_mode = st.radio("Select products to preview", ["All (paginated)", "Enter SPINs"], horizontal=True, key="preview_mode")

        if preview_mode == "Enter SPINs":
            spin_input = st.text_area("Paste SPINs (one per line or comma-separated)", height=100, key="spin_input")
            if spin_input.strip():
                input_spins = [s.strip() for s in spin_input.replace(",", "\n").split("\n") if s.strip()]
                preview_df = valid[valid["spin"].isin(input_spins)]
                if preview_df.empty:
                    st.warning(f"None of the {len(input_spins)} SPINs found in comparison data.")
                else:
                    st.info(f"Showing {len(preview_df)} of {len(input_spins)} SPINs entered")
            else:
                preview_df = pd.DataFrame()
        else:
            preview_df = valid

        if preview_df.empty and preview_mode == "All (paginated)":
            preview_df = valid

        # Pagination
        if not preview_df.empty:
            items_per_page = st.select_slider("Items per page", options=[5, 10, 20, 50], value=10, key="items_per_page")
            total_pages = max(1, (len(preview_df) + items_per_page - 1) // items_per_page)
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, key="preview_page")
            start = (page - 1) * items_per_page
            end = start + items_per_page
            page_df = preview_df.iloc[start:end]
            st.caption(f"Showing {start+1}-{min(end, len(preview_df))} of {len(preview_df)} products | Page {page}/{total_pages}")
        else:
            page_df = pd.DataFrame()

        for _, row in page_df.iterrows():
            st.caption(f"SPIN: `{row.get('spin', '')}` | BK ID: `{row.get('bk_product_id', '')}`")
            c_left, c_right = st.columns(2)
            with c_left:
                st.markdown(f"**BK:** {str(row.get('bk_product_name', ''))[:50]}")
                bk_url = row.get("bk_main_url")
                if bk_url and str(bk_url) not in ["None", "nan"]:
                    try:
                        st.image(str(bk_url), width=300)
                    except:
                        st.caption("(Image failed to load)")
                bk_score = float(row.get('bk_avg_score', 0) or 0)
                im_score_val = float(row.get('im_avg_score', 0) or 0)
                bk_blur_val = float(row.get('bk_main_blur', 0) or 0)
                im_blur_val = float(row.get('im_main_blur', 0) or 0)
                bk_color = "green" if bk_score > im_score_val else "red"
                st.caption(f"Res: {row.get('bk_main_res','-')} | Sharpness: {row.get('bk_main_blur','-')} | Contrast: {row.get('bk_main_contrast','-')} | Score: {row.get('bk_avg_score','-')} | Text: {row.get('bk_text_conf','-')}")
                if bk_score > im_score_val:
                    st.success("WINNER")
            with c_right:
                st.markdown(f"**IM:** {str(row.get('im_product_name', ''))[:50]}")
                im_url = row.get("im_main_url")
                if im_url and str(im_url) not in ["None", "nan", ""]:
                    try:
                        st.image(str(im_url), width=300)
                    except:
                        st.caption("(Image failed to load)")
                st.caption(f"Res: {row.get('im_main_res','-')} | Sharpness: {row.get('im_main_blur','-')} | Contrast: {row.get('im_main_contrast','-')} | Score: {row.get('im_avg_score','-')} | Text: {row.get('im_text_conf','-')}")
                if im_score_val >= bk_score:
                    st.success("WINNER")
            flags = str(row.get("flags", ""))
            if flags and flags != "":
                st.warning(f"Flags: {flags}")
            st.markdown("---")

    # Full data
    with st.expander(f"Full BK Benchmark ({len(df):,} products)"):
        display_cols = ["BK category_name", "BK product name", "BK Brand",
                        "Final IM product Name", "spin_id_final", "Final SKU Match flag",
                        "Match score", "SF_ENABLED"]
        show_table(df[[c for c in display_cols if c in df.columns]], key="bk_full", height=500)


def render_diff_assortment(fs):
    show_sync_time(["upgrade_images", "diff_assortment_image_status"])
    en = fs.get("enabled", False)
    label = "Enabled" if en else "All"
    st.subheader(f"Diff Assortment — Image Tracking ({label})")

    diff_csv = BASE_DIR / "diff_assortment_items.csv"
    if not diff_csv.exists():
        st.error("diff_assortment_items.csv not found.")
        return

    df_diff = pd.read_csv(diff_csv)
    df_diff["Item Code"] = df_diff["Item Code"].astype(str).str.strip()
    if fs.get("brand"):
        df_diff = df_diff[df_diff["Brand Name"].isin(fs["brand"])]

    npi_csv = BASE_DIR / "diff_assortment_npi_pending.csv"
    df_npi_full = pd.read_csv(npi_csv) if npi_csv.exists() else pd.DataFrame()
    npi_count = len(df_npi_full)
    if "WIP_Status" in df_npi_full.columns:
        wip_counts = df_npi_full["WIP_Status"].value_counts().to_dict()
    elif "NPI_Status" in df_npi_full.columns:
        wip_counts = {"NPI Key Missing": len(df_npi_full[df_npi_full["NPI_Status"] == "Key Missing"]),
                       "NPI Key Available": npi_count - len(df_npi_full[df_npi_full["NPI_Status"] == "Key Missing"])}
    else:
        wip_counts = {}
    npi_key_missing = wip_counts.get("NPI Key Missing", 0)
    npi_key_available = wip_counts.get("NPI Key Available", 0)
    wip_count = wip_counts.get("WIP", 0)
    blank_count = wip_counts.get("Blank", 0)

    df_images = C("diff_assortment_image_status")
    if en:
        df_diff = filter_enabled(df_diff, True)
        df_images = filter_enabled(df_images, True)

    total = len(df_diff)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Valid Item Codes", f"{total:,}")
    biz = df_diff.groupby("Business")["Item Code"].count()
    c2.metric("New Commerce", f"{biz.get('New Comm', 0):,}")
    c3.metric("FMCG", f"{biz.get('FMCG', 0):,}")
    c4.metric("Electronics + Fresh", f"{biz.get('Electronics', 0) + biz.get('Fresh', 0):,}")

    st.markdown("##### WIP for SPIN Creation")
    w1, w2, w3, w4, w5 = st.columns(5)
    w1.metric("Total WIP", f"{npi_count:,}")
    w2.metric("NPI Key Available", f"{npi_key_available:,}")
    w3.metric("NPI Key Missing", f"{npi_key_missing:,}")
    w4.metric("WIP", f"{wip_count:,}")
    w5.metric("Blank/Other", f"{blank_count + wip_counts.get('Other', 0):,}")

    st.caption(f"Summary: {total:,} valid codes + {npi_count:,} WIP = {total + npi_count:,} total (excl. L0 exclusivity)")

    with st.expander("By Bet Category"):
        df_bet = df_diff.groupby(["Business", "Bet Category"]).size().reset_index(name="Items").sort_values("Items", ascending=False)
        show_table(df_bet, key="bet")

    # === ERP Status for Diff Assortment ===
    st.markdown("---")
    st.markdown("#### ERP Status — Diff Assortment")

    df_erp_detail = C("diff_erp_detail")
    df_erp_flags = C("diff_erp_flags")

    if not df_erp_detail.empty:
        show_sync_time(["diff_erp_detail"])

        # How many diff items are in ERP
        erp_items = df_erp_detail["ITEM CODE"].nunique() if "ITEM CODE" in df_erp_detail.columns else 0
        erp_cities = df_erp_detail["City"].nunique() if "City" in df_erp_detail.columns else 0

        e1, e2, e3 = st.columns(3)
        e1.metric("Diff Items in ERP", f"{erp_items:,}")
        e2.metric("Not in ERP", f"{total - erp_items:,}")
        e3.metric("Cities Covered", f"{erp_cities:,}")

        # Diff assortment filter for ERP
        st.checkbox("Show only Diff Assortment items in ERP tabs", key="diff_erp_filter",
                    help="When checked, ERP BAU tabs will filter to diff assortment items only")

        # Block OTB / Temp Disable flags — exclude "Permanent" (means active, not blocked)
        if not df_erp_flags.empty:
            if "Temp_Disable" in df_erp_flags.columns:
                df_erp_flags = df_erp_flags[df_erp_flags["Temp_Disable"].astype(str).str.strip() != "Permanent"]
            flagged = len(df_erp_flags)
            block_otb = len(df_erp_flags[df_erp_flags.get("Block_OTB", pd.Series("")).astype(str).str.strip() != ""])
            temp_dis = len(df_erp_flags[df_erp_flags.get("Temp_Disable", pd.Series("")).astype(str).str.strip() != ""])

            st.markdown("##### Blocked / Disabled Items")
            b1, b2, b3 = st.columns(3)
            b1.metric("Total Flagged", f"{flagged:,}")
            b2.metric("Block OTB", f"{block_otb:,}")
            b3.metric("Temp Disable", f"{temp_dis:,}")

            if flagged > 0:
                st.error(f"**{flagged:,} diff assortment items** have Block OTB or Temp Disable flags — needs review")
                with st.expander(f"Flagged Items ({flagged})"):
                    show_table(df_erp_flags, key="diff_erp_flags", height=350)
                    st.download_button("Download Flagged Items", df_erp_flags.to_csv(index=False),
                                       "diff_assortment_blocked.csv", "text/csv")
        else:
            st.success("No diff assortment items are blocked or temp disabled")

        # City x Item x Tier download
        with st.expander(f"City x Item x Tier Detail ({len(df_erp_detail):,} rows)"):
            show_table(df_erp_detail, key="diff_erp_detail", height=400)
            st.download_button("Download City x Item x Tier", df_erp_detail.to_csv(index=False),
                               "diff_assortment_city_tier.csv", "text/csv")
    else:
        st.info("No ERP data for diff assortment. Run full sync.")

    # === UPGRADE Image Tracking ===
    st.markdown("---")
    st.markdown("#### Upgrade Image Tracking")
    st.caption("SPINs with UPGRADE tag in image_id — matches differentiator callout images")

    df_upgrade = C("upgrade_images")
    if not df_upgrade.empty:
        upgrade_spins = df_upgrade["SPIN_ID"].nunique()
        total_upgrade_imgs = len(df_upgrade)

        # Cross-reference with diff assortment list
        diff_item_codes = set(df_diff["Item Code"].astype(str))
        upgrade_items = set(df_upgrade["ITEM_CODE"].astype(str))
        matched = diff_item_codes & upgrade_items
        missing = diff_item_codes - upgrade_items

        u1, u2, u3, u4 = st.columns(4)
        u1.metric("SPINs with UPGRADE Image", f"{upgrade_spins:,}")
        u2.metric("Diff Assortment Items", f"{len(diff_item_codes):,}")
        u3.metric("Have UPGRADE Image", f"{len(matched):,}",
                   delta=f"{len(matched)/max(len(diff_item_codes),1)*100:.1f}%")
        u4.metric("Missing UPGRADE Image", f"{len(missing):,}")

        # By shot type
        st.markdown("##### UPGRADE Image by Slot")
        slot_dist = df_upgrade.groupby("UPGRADE_SLOT").size().reset_index(name="Count")
        show_table(slot_dist, key="upgrade_slots", height=200)

        # Fill rate
        upgrade_item_set = set(df_upgrade["ITEM_CODE"].astype(str))
        matched_count = len(diff_item_codes & upgrade_item_set)
        npi_count_val = npi_count
        checkable = total  # all items including NPI for overall rate

        st.markdown("##### UPGRADE Fill Rate")
        # Denominator = only valid item codes (excludes NPI, WIP, blank, L0)
        checkable = total  # total is already filtered to valid numeric codes only
        f1, f2, f3 = st.columns(3)
        f1.metric("Fill Rate", f"{matched_count}/{checkable} ({matched_count/max(checkable,1)*100:.1f}%)")
        f2.metric("Valid Item Codes", f"{checkable:,}")
        f3.metric("UPGRADE Done", f"{matched_count:,}")

        # By bet category
        st.markdown("##### UPGRADE Coverage by Bet Category")
        # Use Bet Category directly from upgrade data (already merged)
        if "Bet Category" in df_upgrade.columns:
            bet_upgrade = df_upgrade.drop_duplicates(subset=["ITEM_CODE"]).groupby("Bet Category").size().reset_index(name="Has UPGRADE")
        else:
            bet_upgrade = pd.DataFrame(columns=["Bet Category", "Has UPGRADE"])

        bet_total = df_diff.groupby("Bet Category")["Item Code"].nunique().reset_index(name="Total Items")
        bet_combined = bet_total.merge(bet_upgrade, on="Bet Category", how="left").fillna(0)
        bet_combined["Has UPGRADE"] = bet_combined["Has UPGRADE"].astype(int)
        bet_combined["Fill %"] = (bet_combined["Has UPGRADE"] / bet_combined["Total Items"] * 100).round(1)
        bet_combined["Missing"] = bet_combined["Total Items"] - bet_combined["Has UPGRADE"]
        bet_combined = bet_combined.sort_values("Fill %", ascending=False)

        ca, cb = st.columns(2)
        with ca:
            st.bar_chart(bet_combined[bet_combined["Fill %"] > 0].set_index("Bet Category")["Fill %"])
        with cb:
            show_table(bet_combined, key="upgrade_bet", height=400)

        st.markdown("##### Upgrade Gap")
        missing_items = diff_item_codes - upgrade_item_set
        df_missing = df_diff[df_diff["Item Code"].astype(str).isin(missing_items)]
        # Exclude NPI
        df_missing = df_missing[~df_missing["Item Code"].astype(str).str.startswith("NPI")]
        st.metric("Missing UPGRADE Image", f"{len(df_missing):,}")
        show_table(df_missing[["Item Code", "Business", "Bet Category", "Brand Name", "SKU Name"]].sort_values("Bet Category"),
                   key="upgrade_missing", height=350)
        st.download_button("Download Missing UPGRADE List", df_missing.to_csv(index=False),
                           "spins_missing_upgrade_image.csv", "text/csv")

        # Downloadable: SPIN + slot mapping
        # Download: Items WITH upgrade image
        st.markdown("##### Items with UPGRADE Image")
        upgrade_with_diff = df_upgrade[df_upgrade["ITEM_CODE"].astype(str).isin(diff_item_codes)]
        dl_cols = ["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "BRAND", "L1", "L2", "L3",
                   "QUANTITY", "UOM", "UPGRADE_SLOT", "Bet Category", "Brand Name", "SKU Name", "UPGRADE_IMAGE_URL"]
        available_cols = [c for c in dl_cols if c in upgrade_with_diff.columns]
        show_table(upgrade_with_diff[available_cols].rename(
            columns={"SPIN_ID": "SPIN", "ITEM_CODE": "Item Code", "PRODUCT_NAME": "Product",
                     "UPGRADE_SLOT": "Slot", "UPGRADE_IMAGE_URL": "Image URL"}
        ), key="upgrade_has", height=300)
        st.download_button("Download Items WITH Upgrade Image", upgrade_with_diff[available_cols].to_csv(index=False),
                           "items_with_upgrade_image.csv", "text/csv")

        st.markdown("---")
        st.markdown("##### Download SPIN → Slot Mapping")
        dl_cols = ["SPIN_ID", "ITEM_CODE", "UPGRADE_SLOT", "UPGRADE_IMAGE_URL"]
        if "Bet Category" in df_upgrade.columns:
            dl_cols.insert(2, "Bet Category")
        if "Brand Name" in df_upgrade.columns:
            dl_cols.insert(3, "Brand Name")
        if "SKU Name" in df_upgrade.columns:
            dl_cols.insert(4, "SKU Name")
        download_df = df_upgrade[[c for c in dl_cols if c in df_upgrade.columns]].rename(columns={
            "SPIN_ID": "SPIN", "ITEM_CODE": "Item Code",
            "UPGRADE_SLOT": "Upgrade Slot", "UPGRADE_IMAGE_URL": "Upgrade Image URL"
        })
        show_table(download_df.head(50), key="upgrade_download", height=300)
        st.download_button("Download Full SPIN-Slot Mapping", download_df.to_csv(index=False),
                           "upgrade_spin_slot_mapping.csv", "text/csv")

        # Visual: show images at bet category level
        st.markdown("---")
        st.markdown("##### Upgrade Images by Bet Category")
        all_bet_options = sorted(df_diff["Bet Category"].dropna().unique().tolist())
        if all_bet_options:
            selected_bet = st.selectbox("Select Bet Category", all_bet_options, key="bet_cat_select")
            if selected_bet:
                # All items in this bet category
                bet_items = df_diff[df_diff["Bet Category"] == selected_bet]
                # Which have UPGRADE
                upgrade_item_set_local = set(df_upgrade["ITEM_CODE"].astype(str))
                total_bet = len(bet_items)
                has_upgrade = len(bet_items[bet_items["Item Code"].astype(str).isin(upgrade_item_set_local)])
                st.caption(f"{has_upgrade}/{total_bet} items have UPGRADE image in {selected_bet}")

                for _, item_row in bet_items.iterrows():
                    item_code = str(item_row["Item Code"])
                    name = str(item_row.get("SKU Name", item_row.get("Brand Name", "")))[:50]
                    brand = str(item_row.get("Brand Name", ""))

                    # Check if this item has UPGRADE image
                    upgrade_match = df_upgrade[df_upgrade["ITEM_CODE"].astype(str) == item_code]

                    col_l, col_r = st.columns([1, 3])
                    with col_l:
                        st.markdown(f"**{name}**")
                        st.caption(f"Item: {item_code} | Brand: {brand}")
                        if not upgrade_match.empty:
                            spin = upgrade_match.iloc[0].get("SPIN_ID", "")
                            slot = upgrade_match.iloc[0].get("UPGRADE_SLOT", "")
                            st.caption(f"SPIN: {spin} | Slot: {slot}")
                    with col_r:
                        if not upgrade_match.empty:
                            url = str(upgrade_match.iloc[0].get("UPGRADE_IMAGE_URL", ""))
                            if url and url not in ["None", "nan", ""]:
                                try:
                                    st.image(url, width=250)
                                except:
                                    st.caption("(Image failed to load)")
                        else:
                            st.warning("No UPGRADE image")
                    st.markdown("---")
    else:
        st.info("No UPGRADE image data. Run `python fetch_upgrade_images.py` to load.")


# ── ERP ──────────────────────────────────────────────────────────────────────
def render_erp(fs):
    st.title("ERP Assortment Monitor (BAU)")
    show_sync_time()

    tabs = st.tabs(["Overview", "Pod Master", "NPI vs Old SKU", "Ratings",
                     "Block OTB / Temp Disable", "City Add/Remove", "City Expansion",
                     "Removed & Re-Added", "Pod Tiering", "Brand View",
                     "Enablement Delta"])

    with tabs[0]:
        render_erp_overview(fs)
    with tabs[1]:
        render_pod_master(fs)
    with tabs[2]:
        render_erp_npi_split(fs)
    with tabs[3]:
        render_ratings(fs, enabled_only=False)
    with tabs[4]:
        render_erp_block_otb(fs)
    with tabs[5]:
        render_erp_city_changes(fs)
    with tabs[6]:
        render_erp_expansion(fs)
    with tabs[7]:
        render_erp_removed_readded(fs)
    with tabs[8]:
        render_erp_tiering(fs)
    with tabs[9]:
        render_erp_brands(fs)
    with tabs[10]:
        render_enablement_delta(fs)


def render_pod_master(fs):
    """Pod master from KMS — city x tier x active/inactive."""
    st.subheader("Pod Master (KMS)")
    show_sync_time(["pod_master"])

    df = C("pod_master")
    if df.empty:
        st.warning("No pod data. Run `python check_pod_data.py`")
        return

    total = len(df)
    active = len(df[df["Active/Non_Active Pod"] == "Active"])
    inactive = total - active
    cities = df["CITY"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Pods", f"{total:,}")
    c2.metric("Active", f"{active:,}", delta=f"{active/max(total,1)*100:.1f}%")
    c3.metric("Non-Active", f"{inactive:,}")
    c4.metric("Cities", f"{cities:,}")

    # By Tier
    st.markdown("---")
    st.markdown("#### By Tier")
    tier_stats = df.groupby("TIER").agg(
        Total=("STORE_ID", "count"),
        Active=("Active/Non_Active Pod", lambda x: (x == "Active").sum()),
        Inactive=("Active/Non_Active Pod", lambda x: (x == "Non Active").sum()),
    ).reset_index()
    tier_stats["Active %"] = (tier_stats["Active"] / tier_stats["Total"] * 100).round(1)
    tier_stats = tier_stats.sort_values("Total", ascending=False)

    ca, cb = st.columns(2)
    with ca:
        st.bar_chart(tier_stats.set_index("TIER")[["Active", "Inactive"]])
    with cb:
        show_table(tier_stats, key="pod_tier", height=350)

    # By City
    st.markdown("---")
    st.markdown("#### By City")
    city_stats = df.groupby("CITY").agg(
        Total=("STORE_ID", "count"),
        Active=("Active/Non_Active Pod", lambda x: (x == "Active").sum()),
        Inactive=("Active/Non_Active Pod", lambda x: (x == "Non Active").sum()),
    ).reset_index()
    city_stats["Active %"] = (city_stats["Active"] / city_stats["Total"] * 100).round(1)
    city_stats = city_stats.sort_values("Total", ascending=False)
    show_table(city_stats, key="pod_city", height=500)

    # City x Tier pivot
    st.markdown("---")
    st.markdown("#### City x Tier (Active Pods)")
    active_df = df[df["Active/Non_Active Pod"] == "Active"]
    pivot = active_df.pivot_table(index="CITY", columns="TIER", values="STORE_ID",
                                   aggfunc="count", fill_value=0).reset_index()
    pivot["Total"] = pivot.select_dtypes(include="number").sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False)
    show_table(pivot, key="pod_city_tier", height=500)

    st.download_button("Download Pod Master", df.to_csv(index=False),
                       "pod_master.csv", "text/csv")


def render_erp_npi_split(fs):
    """NPI vs Old SKU split based on SPIN creation date."""
    st.subheader("NPI vs Old SKU Split")
    show_sync_time(["erp_all_items"])
    st.caption("New = created < 6 months ago | Old = created > 6 months ago")

    df_sim = C("spin_image_master")
    if df_sim.empty:
        st.warning("No spin_image_master data. Run sync.")
        return

    df_sim["CREATED_DATE"] = pd.to_datetime(df_sim["CREATED_DATE"], errors="coerce")
    today = pd.Timestamp.now().normalize()
    six_months_ago = today - pd.Timedelta(days=180)

    df_sim["AGE_CATEGORY"] = df_sim["CREATED_DATE"].apply(
        lambda d: "New (< 6 months)" if pd.notna(d) and d >= six_months_ago else "Old (> 6 months)" if pd.notna(d) else "Unknown"
    )

    df_sim = filter_dims(df_sim, fs)

    # Summary
    total = len(df_sim)
    new_count = len(df_sim[df_sim["AGE_CATEGORY"] == "New (< 6 months)"])
    old_count = len(df_sim[df_sim["AGE_CATEGORY"] == "Old (> 6 months)"])
    unknown = total - new_count - old_count

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total SPINs", f"{total:,}")
    c2.metric("New (< 6 months)", f"{new_count:,}", delta=f"{new_count/max(total,1)*100:.1f}%")
    c3.metric("Old (> 6 months)", f"{old_count:,}", delta=f"{old_count/max(total,1)*100:.1f}%")
    c4.metric("Unknown Date", f"{unknown:,}")

    # By L1
    st.markdown("---")
    st.markdown("#### New vs Old by L1")
    l1_split = df_sim.groupby(["L1", "AGE_CATEGORY"]).size().reset_index(name="Count")
    l1_pivot = l1_split.pivot_table(index="L1", columns="AGE_CATEGORY", values="Count", fill_value=0).reset_index()
    if "New (< 6 months)" in l1_pivot.columns and "Old (> 6 months)" in l1_pivot.columns:
        l1_pivot["Total"] = l1_pivot.get("New (< 6 months)", 0) + l1_pivot.get("Old (> 6 months)", 0) + l1_pivot.get("Unknown", 0)
        l1_pivot["New %"] = (l1_pivot.get("New (< 6 months)", 0) / l1_pivot["Total"] * 100).round(1)
        l1_pivot = l1_pivot.sort_values("Total", ascending=False)

        ca, cb = st.columns(2)
        with ca:
            top15 = l1_pivot.head(15)
            if "New (< 6 months)" in top15.columns and "Old (> 6 months)" in top15.columns:
                st.bar_chart(top15.set_index("L1")[["New (< 6 months)", "Old (> 6 months)"]])
        with cb:
            show_table(l1_pivot, key="erp_npi_l1", height=400)

    st.download_button("Download NPI Split", l1_pivot.to_csv(index=False) if not l1_pivot.empty else "",
                       "erp_npi_split.csv", "text/csv")

    # Recently added items (last 30 days)
    st.markdown("---")
    st.markdown("#### Recently Created SPINs (Last 30 Days)")
    recent = df_sim[df_sim["CREATED_DATE"] >= today - pd.Timedelta(days=30)]
    if not recent.empty:
        recent_l1 = recent.groupby("L1").size().reset_index(name="New SPINs").sort_values("New SPINs", ascending=False)
        show_table(recent_l1, key="erp_recent_l1", height=300)
    else:
        st.info("No SPINs created in last 30 days")


def render_erp_overview(fs):
    st.subheader("ERP Overview")
    show_sync_time(["erp_trend"])

    # Trend
    df_trend = C("erp_trend_30d")
    if not df_trend.empty:
        latest = df_trend.iloc[-1]["ERP_COUNT"]
        prev = df_trend.iloc[-2]["ERP_COUNT"] if len(df_trend) >= 2 else latest
        c1, c2 = st.columns([1, 3])
        with c1:
            st.metric("Total ERP Items", f"{int(latest):,}",
                      delta=f"{int(latest - prev):+,} DoD")
        with c2:
            st.line_chart(df_trend.set_index("DT")["ERP_COUNT"])

    # L1 with daily change
    df_l1 = C("erp_l1_current")
    if not df_l1.empty:
        df_l1 = filter_dims(df_l1, fs)
        st.markdown("#### L1 Categories (with Daily Change)")
        ca, cb = st.columns(2)
        with ca:
            if "Today" in df_l1.columns:
                st.bar_chart(df_l1.sort_values("Today", ascending=False).head(20).set_index("L1")["Today"])
            elif "ERP Count" in df_l1.columns:
                st.bar_chart(df_l1.sort_values("ERP Count", ascending=False).head(20).set_index("L1")["ERP Count"])
        with cb:
            show_table(df_l1, key="erp_l1_ov")


def render_erp_block_otb(fs):
    st.subheader("Block OTB / Temp Disable Tracking")
    show_sync_time(["erp_block_otb_summary"])

    # Diff assortment filter
    diff_only = st.checkbox("Diff Assortment items only", key="block_diff_filter",
                            help="Show only Differentiated Assortment items")

    diff_items = set()
    if diff_only:
        diff_csv = BASE_DIR / "diff_assortment_items.csv"
        if diff_csv.exists():
            diff_items = set(pd.read_csv(diff_csv)["Item Code"].astype(str))

    # Pan-India Summary
    st.markdown("#### Pan-India Summary")
    df_summary = C("erp_block_otb_summary")
    if not df_summary.empty:
        if diff_only and diff_items:
            # Need detail to filter
            df_detail = C("erp_block_otb_detail")
            if not df_detail.empty:
                df_detail = df_detail[df_detail["ITEM CODE"].astype(str).isin(diff_items)]
                df_summary = df_detail.groupby("FLAG_TYPE").agg(
                    ITEMS=("ITEM CODE", "nunique"),
                    CITIES=("City", "nunique")
                ).reset_index()
                df_summary.columns = ["FLAG_TYPE", "ITEMS", "CITIES"]

        total_flagged = int(df_summary["ITEMS"].sum()) if "ITEMS" in df_summary.columns else 0
        st.metric("Total Items Blocked/Disabled", f"{total_flagged:,}")
        show_table(df_summary, key="block_summary", height=200)

    # By City
    st.markdown("---")
    st.markdown("#### By City")
    df_city = C("erp_block_otb_by_city")
    if not df_city.empty:
        if diff_only and diff_items:
            df_detail = C("erp_block_otb_detail")
            if not df_detail.empty:
                df_detail_f = df_detail[df_detail["ITEM CODE"].astype(str).isin(diff_items)]
                df_city = df_detail_f.groupby(["City", "FLAG_TYPE"]).agg(
                    ITEMS=("ITEM CODE", "nunique")).reset_index()
                df_city.columns = ["City", "FLAG_TYPE", "ITEMS"]

        # Pivot by city
        city_pivot = df_city.pivot_table(index="City", columns="FLAG_TYPE",
                                          values="ITEMS", fill_value=0, aggfunc="sum").reset_index()
        city_pivot["Total"] = city_pivot.select_dtypes(include="number").sum(axis=1)
        city_pivot = city_pivot.sort_values("Total", ascending=False)
        show_table(city_pivot, key="block_city", height=400)
        st.download_button("Download City Breakdown", city_pivot.to_csv(index=False),
                           "block_otb_by_city.csv", "text/csv")

    # By Tier
    st.markdown("---")
    st.markdown("#### By Pod Tier")
    df_tier = C("erp_block_otb_by_tier")
    if not df_tier.empty:
        if diff_only and diff_items:
            df_detail = C("erp_block_otb_detail")
            if not df_detail.empty:
                df_detail_f = df_detail[df_detail["ITEM CODE"].astype(str).isin(diff_items)]
                df_tier = df_detail_f.groupby(["TIER", "FLAG_TYPE"]).agg(
                    ITEMS=("ITEM CODE", "nunique")).reset_index()
                df_tier.columns = ["TIER", "FLAG_TYPE", "ITEMS"]

        tier_pivot = df_tier.pivot_table(index="TIER", columns="FLAG_TYPE",
                                          values="ITEMS", fill_value=0, aggfunc="sum").reset_index()
        tier_pivot["Total"] = tier_pivot.select_dtypes(include="number").sum(axis=1)
        tier_pivot = tier_pivot.sort_values("Total", ascending=False)
        show_table(tier_pivot, key="block_tier", height=300)

    # Full detail
    st.markdown("---")
    df_detail = C("erp_block_otb_detail")
    if not df_detail.empty:
        if diff_only and diff_items:
            df_detail = df_detail[df_detail["ITEM CODE"].astype(str).isin(diff_items)]

        df_detail = filter_dims(df_detail, fs)
        with st.expander(f"Full Detail ({len(df_detail):,} rows)"):
            show_table(df_detail, key="block_detail", height=500)
            st.download_button("Download Full Detail", df_detail.to_csv(index=False),
                               "block_otb_full_detail.csv", "text/csv")


def render_erp_city_changes(fs):
    st.subheader("City-Level Daily Add/Remove")
    show_sync_time(["erp_city_add_remove"])
    st.caption("Items added/removed by city — yesterday vs today")

    df = C("erp_city_daily")
    if df.empty:
        st.warning("No data. Run sync.")
        return

    df = filter_dims(df, fs, l1="Region")

    # Summary
    if "Added" in df.columns:
        total_added = int(df["Added"].sum())
        total_removed = int(df["Removed"].sum())
        net = int(df["Net Change"].sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Added", f"{total_added:,}")
        c2.metric("Total Removed", f"{total_removed:,}")
        c3.metric("Net Change", f"{net:+,}")

    show_table(df, key="erp_city", height=500)
    st.download_button("Download", df.to_csv(index=False), "erp_city_daily.csv", "text/csv")

    # Recently added items
    st.markdown("---")
    df_added = C("erp_recently_added")
    if not df_added.empty:
        df_added = filter_dims(df_added, fs)
        st.markdown(f"#### Recently Added Items ({len(df_added)})")
        show_table(df_added, key="erp_added", height=350)

    # Recently removed items
    df_removed = C("erp_recently_removed")
    if not df_removed.empty:
        df_removed = filter_dims(df_removed, fs)
        st.markdown(f"#### Recently Removed Items ({len(df_removed)})")
        show_table(df_removed, key="erp_removed", height=350)


def render_erp_expansion(fs):
    st.subheader("City Expansion Alerts")
    show_sync_time(["erp_expansion_alerts"])
    st.caption("Items that expanded to significantly more cities in last 30 days (>100% growth or 5+ cities added)")

    df = C("erp_city_expansion")
    if df.empty:
        st.warning("No expansion data. Run sync.")
        return

    df = filter_dims(df, fs)

    # Summary
    total_flagged = len(df)
    new_items = len(df[df["Cities Before"] == 0])
    mega_expansion = len(df[df["Expansion %"] >= 500])

    c1, c2, c3 = st.columns(3)
    c1.metric("Items Flagged", f"{total_flagged:,}")
    c2.metric("Brand New (0 → N cities)", f"{new_items:,}")
    c3.metric(">500% Expansion", f"{mega_expansion:,}")

    # By L1
    exp_l1 = df.groupby("L1").size().reset_index(name="Flagged Items").sort_values("Flagged Items", ascending=False)
    ca, cb = st.columns(2)
    with ca:
        st.markdown("#### Expansion by L1")
        st.bar_chart(exp_l1.head(15).set_index("L1"))
    with cb:
        show_table(exp_l1, key="exp_l1")

    # Detail
    st.markdown("---")
    st.markdown("#### Expansion Detail")
    show_table(df, key="exp_detail", height=500)
    st.download_button("Download", df.to_csv(index=False), "erp_expansion.csv", "text/csv")


def render_erp_removed_readded(fs):
    st.subheader("Removed & Re-Added Items")
    show_sync_time(["erp_removed_readded"])
    st.caption("Items removed ~14 days ago that have come back — flag for review")

    df = C("erp_removed_readded")
    if df.empty:
        st.info("No removed-and-re-added items found.")
        return

    df = filter_dims(df, fs)

    st.metric("Items Re-Added After Removal", f"{len(df):,}")
    st.warning("These items were removed from ERP but re-appeared. Review if intentional or escalation-driven.")

    # By L1
    rl1 = df.groupby("L1").size().reset_index(name="Count").sort_values("Count", ascending=False)
    ca, cb = st.columns(2)
    with ca:
        st.bar_chart(rl1.head(15).set_index("L1"))
    with cb:
        show_table(rl1, key="readd_l1")

    st.markdown("---")
    show_table(df, key="readd_detail", height=500)
    st.download_button("Download", df.to_csv(index=False), "erp_removed_readded.csv", "text/csv")


def render_erp_tiering(fs):
    st.subheader("Pod Tiering Distribution")
    show_sync_time(["erp_tiering_history"])
    st.caption("S, S1, M, M1, L, etc. — pan-India and by city")

    # Pan-India summary with DoD/WoW/MoM/QoQ views
    df_hist = C("erp_tiering_history")
    # Fallback to legacy erp_tiering_summary if history not yet synced
    if df_hist.empty:
        df_hist = C("erp_tiering_summary")
        if not df_hist.empty:
            st.markdown("#### Pan-India Tiering (Daily Change)")
            ca, cb = st.columns(2)
            with ca:
                if "Today" in df_hist.columns:
                    st.bar_chart(df_hist.set_index("Tier")["Today"])
            with cb:
                show_table(df_hist, key="tier_summary")
    else:
        import datetime as _dt
        df_hist["Date"] = pd.to_datetime(df_hist["Date"])
        all_dates = sorted(df_hist["Date"].unique())
        latest_date = all_dates[-1]

        view = st.radio("View", ["DoD", "WoW", "MoM", "QoQ"], horizontal=True, key="tier_view")

        if view == "DoD":
            compare_date = latest_date - pd.Timedelta(days=1)
            label_now, label_prev = "Today", "Yesterday"
        elif view == "WoW":
            compare_date = latest_date - pd.Timedelta(days=7)
            label_now, label_prev = "This Week", "Last Week"
        elif view == "MoM":
            compare_date = latest_date - pd.Timedelta(days=30)
            label_now, label_prev = "Current", "30d Ago"
        else:  # QoQ
            compare_date = latest_date - pd.Timedelta(days=90)
            label_now, label_prev = "Current", "90d Ago"

        # Find closest available dates
        dates_arr = pd.Series(all_dates)
        closest_prev = dates_arr[dates_arr <= compare_date]
        if len(closest_prev) > 0:
            compare_date = closest_prev.iloc[-1]
        else:
            compare_date = all_dates[0]

        df_now = df_hist[df_hist["Date"] == latest_date][["Tier", "Items", "Cities", "Brands"]].copy()
        df_prev = df_hist[df_hist["Date"] == compare_date][["Tier", "Items"]].copy()
        df_prev = df_prev.rename(columns={"Items": label_prev})
        df_now = df_now.rename(columns={"Items": label_now})

        df_summary = df_now.merge(df_prev[["Tier", label_prev]], on="Tier", how="outer").fillna(0)
        for c in [label_now, label_prev, "Cities", "Brands"]:
            df_summary[c] = df_summary[c].astype(int)
        df_summary["Change"] = df_summary[label_now] - df_summary[label_prev]
        df_summary["% Change"] = (df_summary["Change"] / df_summary[label_prev].replace(0, 1) * 100).round(1)
        df_summary = df_summary.sort_values(label_now, ascending=False)

        st.markdown(f"#### Pan-India Tiering ({view})")
        st.caption(f"Comparing {pd.Timestamp(latest_date).strftime('%d-%b-%Y')} vs {pd.Timestamp(compare_date).strftime('%d-%b-%Y')}")

        ca, cb = st.columns(2)
        with ca:
            st.bar_chart(df_summary.set_index("Tier")[label_now])
        with cb:
            show_table(df_summary, key="tier_summary")

    # By city
    st.markdown("---")
    df_city = C("erp_tiering_by_city")
    if not df_city.empty:
        st.markdown("#### Tiering by City")

        # Pivot for easier viewing
        df_pivot = df_city.pivot_table(index="City", columns="Tier", values="Items", fill_value=0).reset_index()
        df_pivot["Total"] = df_pivot.select_dtypes(include="number").sum(axis=1)
        df_pivot = df_pivot.sort_values("Total", ascending=False)

        show_table(df_pivot, key="tier_city", height=500)
        st.download_button("Download", df_pivot.to_csv(index=False), "erp_tiering_by_city.csv", "text/csv")

        # Average per city
        st.markdown("#### Average Items per City by Tier")
        avg_df = df_city.groupby("Tier")["Items"].agg(["mean", "median", "sum", "count"]).reset_index()
        avg_df.columns = ["Tier", "Avg Items/City", "Median Items/City", "Total Items", "Cities"]
        avg_df = avg_df.sort_values("Total Items", ascending=False)
        avg_df["Avg Items/City"] = avg_df["Avg Items/City"].round(0)
        avg_df["Median Items/City"] = avg_df["Median Items/City"].round(0)
        show_table(avg_df, key="tier_avg")


def render_erp_brands(fs):
    st.subheader("Brand-Level View")
    show_sync_time(["erp_brand_current"])
    df = C("erp_brand_current")
    if df.empty:
        st.warning("No data.")
        return
    df = filter_dims(df, fs)
    show_table(df, key="erp_brands", height=600)
    st.download_button("Download", df.to_csv(index=False), "erp_brands.csv", "text/csv")


def render_enablement_delta(fs):
    """ERP intended vs actual state machine — find gaps in pod enablement."""
    st.subheader("Enablement Delta — ERP vs State Machine")
    st.caption("Compare ERP intended assortment against actual assortment/force-enable state. Tier cascade: if item is at tier L in ERP, it should be enabled at L and all higher tiers.")
    show_sync_time(["erp_expanded_tiers", "assortment_state", "assortment_overrides"])

    TIER_ORDER = ["XS", "S", "S1", "M", "M1", "L", "L1", "XL", "XL1", "2XL", "3XL", "4XL", "5XL", "6XL", "8XL"]

    df_erp = C("erp_expanded_tiers")
    df_state = C("assortment_state")
    df_overrides = C("assortment_overrides")
    df_pods = C("pod_master")

    if df_erp.empty:
        st.info("ERP expanded tiers not synced yet. Run sync_data.py first.")
        return

    if df_state.empty and df_overrides.empty:
        st.warning("Assortment state not fetched from Databricks yet. Run: `fetch_databricks.py assortment`")

        # Still show ERP intended summary
        st.markdown("#### ERP Intended (with tier cascade)")
        st.metric("Total item × city × tier combinations", f"{len(df_erp):,}")
        by_tier = df_erp.groupby("expected_tier")["item_code"].nunique().reset_index()
        by_tier.columns = ["Tier", "Items"]
        by_tier["Tier"] = pd.Categorical(by_tier["Tier"], categories=TIER_ORDER, ordered=True)
        by_tier = by_tier.sort_values("Tier")
        show_table(by_tier, key="erp_intended_tier")

        by_city = df_erp.groupby("city")["item_code"].nunique().reset_index()
        by_city.columns = ["City", "Items"]
        by_city = by_city.sort_values("Items", ascending=False)
        show_table(by_city, key="erp_intended_city", height=400)
        return

    # --- Compute delta ---
    # Map spin_id from spin_image_master (item_code → spin_id)
    df_sim = C("spin_image_master")
    if not df_sim.empty and "ITEM_CODE" in df_sim.columns and "SPIN_ID" in df_sim.columns:
        item_to_spin = df_sim[["ITEM_CODE", "SPIN_ID"]].drop_duplicates()
        item_to_spin["ITEM_CODE"] = item_to_spin["ITEM_CODE"].astype(str)
        df_erp["item_code"] = df_erp["item_code"].astype(str)
        df_erp_spin = df_erp.merge(item_to_spin, left_on="item_code", right_on="ITEM_CODE", how="left")
    else:
        df_erp_spin = df_erp.copy()
        df_erp_spin["SPIN_ID"] = None

    # Map city name → city_id from pod_master
    if not df_pods.empty:
        city_map = df_pods[["CITY", "CITY_ID"]].drop_duplicates() if "CITY_ID" in df_pods.columns else pd.DataFrame()
    else:
        city_map = pd.DataFrame()

    if not df_state.empty and not city_map.empty:
        df_state["city_id"] = df_state["city_id"].astype(str).str.replace(".0", "", regex=False)
        city_map["CITY_ID"] = city_map["CITY_ID"].astype(str)

        # Merge ERP with state
        df_erp_spin = df_erp_spin.merge(city_map, left_on="city", right_on="CITY", how="left")

        # Check which ERP items are in assortment
        state_keys = set()
        if not df_state.empty:
            for _, r in df_state.iterrows():
                state_keys.add((str(r["spin_id"]), str(r["city_id"]), str(r["tier"])))

        override_enabled = set()
        if not df_overrides.empty:
            fe = df_overrides[df_overrides["assortment_override_state"] == "STATE_FORCE_ENABLED"]
            for _, r in fe.iterrows():
                override_enabled.add((str(r["spin_id"]), str(r["city_id"])))

        df_erp_spin["in_state_machine"] = df_erp_spin.apply(
            lambda r: "Yes" if (str(r.get("SPIN_ID", "")), str(r.get("CITY_ID", "")), str(r["expected_tier"])) in state_keys
            else "Force Enabled" if (str(r.get("SPIN_ID", "")), str(r.get("CITY_ID", ""))) in override_enabled
            else "No", axis=1
        )

        # Delta = items not in state machine
        delta = df_erp_spin[df_erp_spin["in_state_machine"] == "No"]

        # Summary
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total ERP Combinations", f"{len(df_erp_spin):,}")
        c2.metric("In State Machine", f"{len(df_erp_spin[df_erp_spin['in_state_machine'] != 'No']):,}")
        c3.metric("GAPS (Delta)", f"{len(delta):,}", delta_color="inverse")
        c4.metric("Force Enabled", f"{len(df_erp_spin[df_erp_spin['in_state_machine'] == 'Force Enabled']):,}")

        if not delta.empty:
            st.markdown("---")
            st.markdown("#### Gap by City")
            gap_city = delta.groupby("city")["item_code"].nunique().reset_index()
            gap_city.columns = ["City", "Items with Gap"]
            gap_city = gap_city.sort_values("Items with Gap", ascending=False)
            ca, cb = st.columns(2)
            with ca:
                st.bar_chart(gap_city.head(20).set_index("City")["Items with Gap"], horizontal=True)
            with cb:
                show_table(gap_city, key="delta_city")

            st.markdown("#### Gap by Tier")
            gap_tier = delta.groupby("expected_tier")["item_code"].nunique().reset_index()
            gap_tier.columns = ["Tier", "Items with Gap"]
            gap_tier["Tier"] = pd.Categorical(gap_tier["Tier"], categories=TIER_ORDER, ordered=True)
            gap_tier = gap_tier.sort_values("Tier")
            show_table(gap_tier, key="delta_tier")

            st.markdown("#### Item-Level Detail")
            detail_cols = ["item_code", "city", "erp_base_tier", "expected_tier",
                          "sku_name", "brand", "l1", "in_state_machine"]
            available = [c for c in detail_cols if c in delta.columns]
            show_table(delta[available].head(500), key="delta_detail", height=500)
            st.download_button("Download Full Delta", delta[available].to_csv(index=False),
                              "enablement_delta.csv", "text/csv", key="dl_delta")
        else:
            st.success("No gaps! All ERP items are in assortment or force-enabled.")

    else:
        st.markdown("#### ERP Intended (with tier cascade)")
        st.metric("Total item × city × tier combinations", f"{len(df_erp):,}")

    # --- Force Enable/Disable Tracking ---
    if not df_overrides.empty:
        st.markdown("---")
        st.markdown("#### Force Enable / Disable Tracking")
        fe_summary = df_overrides.groupby("assortment_override_state").size().reset_index(name="Count")
        show_table(fe_summary, key="override_summary")

        with st.expander(f"Override Details ({len(df_overrides):,} records)"):
            show_table(df_overrides.head(500), key="override_detail", height=400)


# ── Enabled Items Health ─────────────────────────────────────────────────────
def render_enabled_health(fs):
    st.title("Enabled Items Health")
    show_sync_time()
    st.caption("Image Health + Attribute Health + Value Standardization for storefront-live items")

    tabs = st.tabs(["Overall Health Score", "Image Fill Rate", "Attribute Fill Rate",
                     "Ratings", "Value Standardization"])

    with tabs[0]:
        render_overall_health(fs)
    with tabs[1]:
        render_enabled_image_health(fs)
    with tabs[2]:
        render_attribute_health(fs)
    with tabs[3]:
        render_ratings(fs, enabled_only=True)
    with tabs[4]:
        render_value_standardization(fs)


def render_ratings(fs, enabled_only=True):
    """Item ratings tracking — low rated items flagged."""
    label = "Enabled Items" if enabled_only else "All ERP Items"
    st.subheader(f"Item Ratings — {label}")
    show_sync_time(["item_ratings"])

    # Time window selector
    window = st.radio("Time Window", ["All Time", "Last 90 Days", "Last 30 Days"], horizontal=True,
                       key=f"rating_window_{enabled_only}")
    window_key = {"All Time": "all_time", "Last 90 Days": "90d", "Last 30 Days": "30d"}[window]

    df = C(f"item_ratings_{window_key}")
    if df.empty:
        # Try legacy key
        df = C("item_ratings")
    if df.empty:
        st.warning("No ratings data. Run `python fetch_ratings.py`")
        return

    # Filter — ratings use SPIN_ID, enabled set has ITEM_CODE, need to bridge via spin_image_master
    if enabled_only:
        enabled_set_local = get_enabled_set()
        if enabled_set_local:
            df_sim = C("spin_image_master")
            if not df_sim.empty:
                enabled_spins = set(df_sim[df_sim["ITEM_CODE"].astype(str).isin(enabled_set_local)]["SPIN_ID"].astype(str))
                df = df[df["SPIN_ID"].astype(str).isin(enabled_spins)]
            else:
                df = df[df["SPIN_ID"].astype(str).isin(enabled_set_local)]

    # Bridge SPIN_ID -> numeric ITEM_CODE via spin_image_master
    df_sim_bridge = C("spin_image_master")
    spin_to_item = {}
    if not df_sim_bridge.empty:
        spin_to_item = dict(zip(df_sim_bridge["SPIN_ID"].astype(str), df_sim_bridge["ITEM_CODE"].astype(str)))
    df["NUMERIC_ITEM_CODE"] = df["SPIN_ID"].astype(str).map(spin_to_item)

    # Diff assortment filter
    diff_only = st.checkbox("Diff Assortment only", key=f"rating_diff_{enabled_only}")
    if diff_only:
        diff_csv = BASE_DIR / "diff_assortment_items.csv"
        if diff_csv.exists():
            diff_items = set(pd.read_csv(diff_csv)["Item Code"].astype(str))
            df = df[df["NUMERIC_ITEM_CODE"].astype(str).isin(diff_items)]

    df = filter_dims(df, fs, l1="L1_CATEGORY")
    df["AVG_RATING"] = pd.to_numeric(df.get("AVG_RATING"), errors="coerce")
    df["ORDERS"] = pd.to_numeric(df.get("ORDERS"), errors="coerce")

    total = len(df)
    avg_rating = df["AVG_RATING"].mean()
    low_3 = int((df["AVG_RATING"] < 3.0).sum())
    low_2 = int((df["AVG_RATING"] < 2.0).sum())
    high_4 = int((df["AVG_RATING"] >= 4.0).sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Rated SPINs", f"{total:,}")
    c2.metric("Avg Rating", f"{avg_rating:.2f}")
    c3.metric("4+ Stars", f"{high_4:,}", delta=f"{high_4/max(total,1)*100:.1f}% of total")
    c4.metric("Below 3 Stars", f"{low_3:,}", delta=f"{low_3/max(total,1)*100:.1f}% of total", delta_color="inverse")
    c5.metric("Below 2 Stars", f"{low_2:,}", delta=f"{low_2/max(total,1)*100:.1f}% of total", delta_color="inverse")

    if low_3 > 0:
        st.warning(f"**{low_3:,} items** rated below 3 stars — needs review")

    # In-Assortment / Out-of-Assortment split
    st.markdown("---")
    st.markdown("#### In-Assortment vs Out-of-Assortment")
    st.caption("In-Assortment = Item Code present in ERP for at least 1 city")

    # Get ERP item codes — use erp_all_items if available, else enabled set as proxy
    df_erp_items = C("erp_all_items")
    if not df_erp_items.empty and "ITEM_CODE" in df_erp_items.columns:
        erp_item_set = set(df_erp_items["ITEM_CODE"].astype(str))
    else:
        # Fallback: use diff_erp_detail or enabled set
        df_erp_detail_local = C("diff_erp_detail")
        if not df_erp_detail_local.empty and "ITEM CODE" in df_erp_detail_local.columns:
            erp_item_set = set(df_erp_detail_local["ITEM CODE"].astype(str))
        else:
            erp_item_set = get_enabled_set() or set()

    if erp_item_set and "NUMERIC_ITEM_CODE" in df.columns:
        df["ASSORTMENT"] = df["NUMERIC_ITEM_CODE"].apply(
            lambda x: "In-Assortment" if str(x) in erp_item_set else "Out-of-Assortment"
        )
    elif erp_item_set:
        df["ASSORTMENT"] = df.apply(
            lambda r: "In-Assortment" if str(r.get("NUMERIC_ITEM_CODE", r.get("ITEM_CODE", ""))) in erp_item_set else "Out-of-Assortment",
            axis=1
        )

        in_assort = df[df["ASSORTMENT"] == "In-Assortment"]
        out_assort = df[df["ASSORTMENT"] == "Out-of-Assortment"]

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("In-Assortment", f"{len(in_assort):,}",
                   delta=f"Avg: {in_assort['AVG_RATING'].mean():.2f}" if not in_assort.empty else "")
        a2.metric("Below 3 (In)", f"{(in_assort['AVG_RATING'] < 3).sum():,}" if not in_assort.empty else "0")
        a3.metric("Out-of-Assortment", f"{len(out_assort):,}",
                   delta=f"Avg: {out_assort['AVG_RATING'].mean():.2f}" if not out_assort.empty else "")
        a4.metric("Below 3 (Out)", f"{(out_assort['AVG_RATING'] < 3).sum():,}" if not out_assort.empty else "0")

        # Table split
        assort_summary = df.groupby("ASSORTMENT").agg(
            SPINs=("SPIN_ID", "count"),
            Avg_Rating=("AVG_RATING", "mean"),
            Below_3=("AVG_RATING", lambda x: (x < 3.0).sum()),
            Below_2=("AVG_RATING", lambda x: (x < 2.0).sum()),
            Orders=("ORDERS", "sum"),
        ).reset_index()
        assort_summary["Avg_Rating"] = assort_summary["Avg_Rating"].round(2)
        show_table(assort_summary, key=f"assort_split_{enabled_only}", height=150)

    # By L1
    st.markdown("---")
    st.markdown("#### Rating by L1 Category")
    l1_stats = df.groupby("L1_CATEGORY").agg(
        SPINs=("SPIN_ID", "count"),
        Avg_Rating=("AVG_RATING", "mean"),
        Below_3=("AVG_RATING", lambda x: (x < 3.0).sum()),
        Below_2=("AVG_RATING", lambda x: (x < 2.0).sum()),
        Total_Orders=("ORDERS", "sum"),
    ).reset_index()
    l1_stats["Avg_Rating"] = l1_stats["Avg_Rating"].round(2)
    l1_stats["Low %"] = (l1_stats["Below_3"] / l1_stats["SPINs"] * 100).round(1)
    l1_stats = l1_stats.sort_values("Avg_Rating")

    ca, cb = st.columns(2)
    with ca:
        st.markdown("##### Worst Rated L1s")
        st.bar_chart(l1_stats.head(15).set_index("L1_CATEGORY")["Avg_Rating"], horizontal=True)
    with cb:
        show_table(l1_stats, key=f"rating_l1_{enabled_only}", height=400)

    # Low rated items detail
    st.markdown("---")
    st.markdown("#### Low Rated Items (Below 3 Stars)")
    low_items = df[df["AVG_RATING"] < 3.0].sort_values("AVG_RATING")
    display_cols = ["SPIN_ID", "PRODUCT_NAME", "L1_CATEGORY", "AVG_RATING", "ORDERS",
                    "STATUS", "ENABLED_IN_ATLEAST_ONE_POD"]
    available = [c for c in display_cols if c in low_items.columns]
    if not low_items.empty:
        with st.expander(f"Low Rated Items ({len(low_items):,})"):
            show_table(low_items[available], key=f"rating_low_{enabled_only}", height=400)
            st.download_button("Download Low Rated Items", low_items[available].to_csv(index=False),
                               f"low_rated_items_{'enabled' if enabled_only else 'all'}.csv", "text/csv")
    else:
        st.success("No items below 3 stars!")


def render_overall_health(fs):
    """Combined health score across image, attribute, ratings."""
    st.subheader("Overall Item Master Health Score")
    show_sync_time(["spin_image_master", "item_ratings"])
    st.caption("Combined score: Image Fill Rate + Attribute Fill Rate + Ratings | Tracked daily")

    df = C("spin_image_master")
    if df.empty:
        st.warning("No data. Run sync.")
        return

    # Filter to enabled
    enabled_set_local = get_enabled_set()
    if enabled_set_local:
        df = df[df["ITEM_CODE"].astype(str).isin(enabled_set_local)]
    df = filter_dims(df, fs)

    total = len(df)
    img_count = pd.to_numeric(df["IMAGE_COUNT"], errors="coerce")

    # Image Health Score
    has_4plus = int((img_count >= 4).sum())
    has_mn = int((df.get("HAS_MAIN", pd.Series("No")) == "Yes").sum())
    image_score = round(has_4plus / max(total, 1) * 100, 1)

    # Ratings Score (from ratings cache)
    df_rat = C("item_ratings_all_time")
    rating_score = 0
    rated_count = 0
    if not df_rat.empty:
        if enabled_set_local:
            df_rat = df_rat[df_rat["SPIN_ID"].astype(str).isin(enabled_set_local)]
        df_rat = filter_dims(df_rat, fs, l1="L1_CATEGORY")
        rated_count = len(df_rat)
        if rated_count > 0:
            avg_rat = pd.to_numeric(df_rat["AVG_RATING"], errors="coerce").mean()
            rating_score = round(min(avg_rat / 5 * 100, 100), 1)

    # Attribute Score (placeholder — will be live when attribute data is synced)
    df_attr = C("attribute_fill_rates")
    attribute_score = 0
    if not df_attr.empty:
        attribute_score = 50  # Placeholder
    else:
        attribute_score = None  # Not available

    # Overall Score (weighted)
    # Image: 40%, Ratings: 30%, Attributes: 30%
    if attribute_score is not None:
        overall = round(image_score * 0.4 + rating_score * 0.3 + attribute_score * 0.3, 1)
        weights_text = "Image (40%) + Ratings (30%) + Attributes (30%)"
    else:
        overall = round(image_score * 0.55 + rating_score * 0.45, 1)
        weights_text = "Image (55%) + Ratings (45%) — Attributes pending"

    # Display
    st.markdown("### Health Score")
    st.caption(f"Weightage: {weights_text}")

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Overall Health", f"{overall}/100",
               delta="Good" if overall >= 70 else ("Needs Work" if overall >= 50 else "Critical"),
               delta_color="normal" if overall >= 70 else ("off" if overall >= 50 else "inverse"))
    sc2.metric("Image Fill Rate", f"{image_score}/100",
               delta=f"{has_4plus:,}/{total:,} have 4+ images")
    sc3.metric("Rating Score", f"{rating_score}/100",
               delta=f"{rated_count:,} rated SPINs")
    if attribute_score is not None:
        sc4.metric("Attribute Score", f"{attribute_score}/100")
    else:
        sc4.metric("Attribute Score", "Pending", delta="Awaiting data")

    # Health by L1
    st.markdown("---")
    st.markdown("#### Health Score by L1")

    l1_health = df.groupby("L1").agg(
        Total=("SPIN_ID", "count"),
        Has_4Plus=("IMAGE_COUNT", lambda x: (pd.to_numeric(x, errors="coerce") >= 4).sum()),
    ).reset_index()
    l1_health["Image %"] = (l1_health["Has_4Plus"] / l1_health["Total"] * 100).round(1)

    # Merge ratings by L1
    if not df_rat.empty:
        l1_ratings = df_rat.groupby("L1_CATEGORY").agg(
            Avg_Rating=("AVG_RATING", "mean"),
            Below_3=("AVG_RATING", lambda x: (x < 3).sum()),
        ).reset_index()
        l1_ratings["Rating Score"] = (l1_ratings["Avg_Rating"] / 5 * 100).round(1)
        l1_health = l1_health.merge(l1_ratings, left_on="L1", right_on="L1_CATEGORY", how="left")
    else:
        l1_health["Rating Score"] = None
        l1_health["Avg_Rating"] = None

    # Combined score per L1
    l1_health["Health Score"] = l1_health.apply(
        lambda r: round(r.get("Image %", 0) * 0.55 + (r.get("Rating Score", 0) or 0) * 0.45, 1), axis=1)
    l1_health = l1_health.sort_values("Health Score")

    ca, cb = st.columns(2)
    with ca:
        st.markdown("##### Worst L1s by Health Score")
        ch = l1_health.head(15)
        if not ch.empty:
            st.bar_chart(ch.set_index("L1")["Health Score"], horizontal=True)
    with cb:
        display_cols = ["L1", "Total", "Image %", "Avg_Rating", "Rating Score", "Health Score"]
        available = [c for c in display_cols if c in l1_health.columns]
        show_table(l1_health[available], key="health_l1", height=400)

    st.download_button("Download Health Scores", l1_health.to_csv(index=False),
                       "item_health_scores_by_l1.csv", "text/csv")

    # Daily tracking
    st.markdown("---")
    st.markdown("#### Daily Health Tracking")
    df_hist = C("metrics_history")
    if not df_hist.empty and len(df_hist) > 1:
        df_hist["date"] = pd.to_datetime(df_hist["date"])
        if "coverage_pct" in df_hist.columns:
            st.line_chart(df_hist.set_index("date")[["coverage_pct", "enabled_coverage_pct"]])
    else:
        st.info("Daily tracking will appear after 2+ syncs on different days")


def render_enabled_image_health(fs):
    """Consolidated image health for enabled items."""
    st.subheader("Image Health — Enabled Items")
    show_sync_time(["spin_image_master", "pod_enabled_items"])

    df = C("spin_image_master")
    if df.empty:
        st.warning("No data. Run sync.")
        return

    # Filter to enabled only
    enabled_set = get_enabled_set()
    if enabled_set:
        df = df[df["ITEM_CODE"].astype(str).isin(enabled_set)]

    df = filter_dims(df, fs)
    total = len(df)

    # Image count distribution
    img_count = pd.to_numeric(df["IMAGE_COUNT"], errors="coerce")
    has_4plus = int((img_count >= 4).sum())
    has_1to3 = int(((img_count >= 1) & (img_count < 4)).sum())
    has_0 = int((img_count == 0).sum())
    has_main = int((df.get("HAS_MAIN", pd.Series("No")) == "Yes").sum())
    has_bk = int((df.get("HAS_BK", pd.Series("No")) == "Yes").sum())

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Enabled", f"{total:,}")
    c2.metric("4+ Images", f"{has_4plus:,}", delta=f"{has_4plus/max(total,1)*100:.1f}%")
    c3.metric("1-3 Images", f"{has_1to3:,}")
    c4.metric("0 Images", f"{has_0:,}")
    c5.metric("Has Main (MN)", f"{has_main:,}")
    c6.metric("Has Back (BK)", f"{has_bk:,}")

    # By L1
    st.markdown("---")
    st.markdown("#### Image Coverage by L1 (Enabled)")
    l1_stats = df.groupby("L1").agg(
        Total=("SPIN_ID", "count"),
        Has_4Plus=("IMAGE_COUNT", lambda x: (pd.to_numeric(x, errors="coerce") >= 4).sum()),
    ).reset_index()
    l1_stats["Coverage %"] = (l1_stats["Has_4Plus"] / l1_stats["Total"] * 100).round(1)
    l1_stats = l1_stats.sort_values("Coverage %")

    ca, cb = st.columns(2)
    with ca:
        st.bar_chart(l1_stats.head(15).set_index("L1")["Coverage %"], horizontal=True)
    with cb:
        show_table(l1_stats, key="en_img_l1", height=400)

    # Guidelines compliance
    st.markdown("---")
    guidelines = load_guidelines()
    st.markdown("#### Guidelines Compliance")
    st.caption("Min 7 images per SPIN (BAU guideline), MN + BK mandatory")

    has_7plus = int((img_count >= 7).sum())
    has_mn_bk = int(((df.get("HAS_MAIN", pd.Series("No")) == "Yes") & (df.get("HAS_BK", pd.Series("No")) == "Yes")).sum())
    g1, g2 = st.columns(2)
    g1.metric("7+ Images (BAU Target)", f"{has_7plus:,}", delta=f"{has_7plus/max(total,1)*100:.1f}%")
    g2.metric("Has Both MN + BK", f"{has_mn_bk:,}", delta=f"{has_mn_bk/max(total,1)*100:.1f}%")


def render_attribute_health(fs):
    """Attribute fill rate tracking from CMS — L1/L2 level, highlight <50%, importance ranking."""
    st.subheader("Attribute Fill Rate")
    show_sync_time(["attribute_fill_rates", "l1_attribute_master"])

    df = C("attribute_fill_rates")
    if df.empty:
        st.info("Attribute fill rate data not synced yet. Run sync_data.py to populate.")
        return

    df_master = C("l1_attribute_master")

    # Exclude internal/system attributes
    # Full item master attribute list — EXCLUDE all of these from fill rate calculation
    # Only consumer-facing product attributes (color, material, size, type, etc.) should remain
    SKIP_ATTRS = {
        # ── CMS Item Master fields (from user's full list) ──
        # Identifiers & category
        "super_category/L1", "category/L2", "sub-category/L3", "product name",
        "item_code", "spin_id", "product_id", "parent_product_id", "parent_product_name",
        "parent product name", "item_type", "temp_sku", "item_name", "sub_name", "addendum",
        "catalog_category", "l4_category", "l5_category", "l6_category",
        # Brand
        "brand", "brand_id", "brand_company", "brand_company_id",
        # Pricing & margin
        "cost_price", "cost-price", "mrp", "on_invoice_margin", "on-invoice_margin",
        "total_margin", "is_margin_percent", "is_marg", "is_margin",
        # Quantity & UoM (item master level)
        "quantity", "unit of measure", "uom",
        # Weight & dimensions
        "net_weight", "gross_weight", "height_in_cm", "width_in_cm", "length_in_cm",
        "item_height_in_cm", "item_length_in_cm", "item_width_in_cm",
        "item_weight_in_grams", "weight_in_grams", "volume", "volume_in_cc",
        "length(cms)", "breadth(cms)", "height(cms)",
        "product_packed_type", "pack_type", "case_size",
        # Tax & compliance
        "hsn_code", "hsn", "hsn_description", "conaro_tax_code", "tax_code",
        "cgst", "sgst", "igst", "cess", "additional_cess_value", "ean", "barcode",
        # Shelf life & storage
        "shelf_life", "shelf life number", "sellable shelf life", "sellable_shelf_life",
        "shelf_life_number", "whs_inwarding_cutoff", "whs_outwarding_cutoff",
        "inwarding_cutoff", "outwarding_cutoff", "cx_cutoff",
        "b2b_liquidation_cutoff", "(b2b)_liquidation_cutoff",
        "storage_requirement_temperature", "storage_requirement_type",
        # Supply & logistics
        "supply_status", "dsd_wh_crossdock", "dsd/wh/cross-dock",
        "photo_shoot_required", "secondary_packing_requirement",
        "seasonality_festivity", "seasonality/festivity", "season_festive_code", "season/festive_code",
        "type_of_storage_at_wh", "maintain_selling_mrp_by",
        "country_of_origin", "country of origin",
        # Food type & perishability
        "food_type", "fnv_perishables_non_perishables", "fnv/perishables/non-perishables",
        "edible", "perishable", "organic_normal", "organic/normal", "organic_certification",
        # Licenses
        "drug_licence", "drug licence", "pesticide_license", "pesticide license", "fssai_license",
        # RTV
        "rtv_applicable", "rtv_criteria", "rtv_percentage", "rtv_criteria_eligible_days",
        "rtv_pickup_terms",
        # SCM / virtual combo
        "scm_spin_1", "scm_qty_1", "scm_spin_2", "scm_qty_2", "scm_spin_3", "scm_qty_3",
        "scm_spin_4", "scm_qty_4", "scm_item_type",
        # Max qty & BL
        "max_allowed_qty", "max_allowed_quantity", "applicable_bls", "applicable_bl",
        # Item segment & filters
        "item_segment", "item_segmentation", "filters_tag",
        # Campaign & pharmacy
        "campaign_end_date", "external_pharmacy_item", "external_pharmacy_item_code",
        # Claims & returns
        "claim_window_in_hours", "damage_then_dispose", "replacement_first", "return_eligibility",
        # System / sync fields
        "created_time", "updated_time", "item_status", "last_updated_by",
        "vinculum_feedback", "vinculum_updated_at", "item_sync_feedback",
        "barcode_add_feedback", "barcode_delete_feedback",
        "item_sync_feedback(since_8th_april)", "barcode_add_feedback(since_8th_april)",
        "barcode_delete_feedback(since_8th_april)",
        # SEO & description fields
        "search_keyword", "seo_product_description", "seo_product_specifications",
        "seo_care_instructions", "seo_how_to_use_product", "product long description",
        "product short description", "product_description", "disclaimer",
        "returns_and_refund_policy", "how_to_use_product",
        # Freebie / offers
        "freebie", "offers",
    }
    # Case-insensitive match + normalize spaces/special chars to underscores
    skip_lower = {s.lower().replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "") for s in SKIP_ATTRS}
    df = df[~df["Attribute"].apply(lambda x: x.lower().replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "") in skip_lower)]

    # Summary metrics
    overall_fill = round(df["Fill Rate %"].mean(), 1)
    below_50 = df[df["Fill Rate %"] < 50]
    worst_l1 = df.groupby("L1")["Fill Rate %"].mean().idxmin() if not df.empty else "N/A"
    worst_fill = round(df.groupby("L1")["Fill Rate %"].mean().min(), 1) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Overall Fill Rate", f"{overall_fill}%")
    c2.metric("Worst L1", worst_l1, delta=f"{worst_fill}%")
    c3.metric("Attributes Tracked", f"{df['Attribute'].nunique()}")
    c4.metric("Below 50% Fill", f"{len(below_50):,}", delta="needs attention", delta_color="inverse")

    # --- L1 Fill Rate Pivot ---
    st.markdown("---")
    st.markdown("#### Fill Rate by L1 Category")
    st.caption("Red = below 50% fill rate. Click column headers to sort.")

    # L1 selector (optional filter)
    all_l1s = sorted(df["L1"].unique())
    selected_l1 = st.multiselect("Filter L1 (leave empty for all)", all_l1s, key="attr_l1_filter")
    if selected_l1:
        df_view = df[df["L1"].isin(selected_l1)]
    else:
        df_view = df

    # Top attributes (by prevalence across L1s)
    top_attrs = df_view.groupby("Attribute")["L1"].nunique().sort_values(ascending=False).head(20).index.tolist()

    # Pivot: L1 rows x top attribute columns
    df_pivot = df_view[df_view["Attribute"].isin(top_attrs)].pivot_table(
        index="L1", columns="Attribute", values="Fill Rate %", aggfunc="first"
    ).reset_index()
    df_pivot["Avg Fill %"] = df_pivot.select_dtypes(include="number").mean(axis=1).round(1)
    df_pivot = df_pivot.sort_values("Avg Fill %")

    show_table(df_pivot, key="attr_pivot", height=500)
    st.download_button("Download L1 Fill Rates", df_pivot.to_csv(index=False),
                       "attribute_fill_by_l1.csv", "text/csv", key="dl_attr_l1")

    # --- Highlight categories below 50% ---
    st.markdown("---")
    st.markdown("#### 🔴 Categories Below 50% Fill Rate")
    if not below_50.empty:
        b50 = below_50[["L1", "Attribute", "Filled", "Total", "Fill Rate %"]].sort_values("Fill Rate %")
        st.metric("L1 × Attribute pairs below 50%", f"{len(b50):,}")
        show_table(b50, key="attr_below50", height=400)
    else:
        st.success("All L1 × Attribute combinations are above 50%!")

    # --- L2 Drill-down ---
    st.markdown("---")
    df_l2 = C("attribute_fill_by_l2")
    if not df_l2.empty:
        df_l2 = df_l2[~df_l2["Attribute"].apply(lambda x: x.lower().replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "") in skip_lower)]
        with st.expander("#### L2-Level Drill-Down"):
            sel_l1 = st.selectbox("Select L1", sorted(df_l2["L1"].unique()), key="attr_l2_sel")
            df_l2_view = df_l2[df_l2["L1"] == sel_l1]
            if not df_l2_view.empty:
                l2_pivot = df_l2_view.pivot_table(
                    index="L2", columns="Attribute", values="Fill Rate %", aggfunc="first"
                ).reset_index()
                l2_pivot["Avg Fill %"] = l2_pivot.select_dtypes(include="number").mean(axis=1).round(1)
                l2_pivot = l2_pivot.sort_values("Avg Fill %")
                show_table(l2_pivot, key="attr_l2_pivot", height=400)

    # --- Attribute Importance / P0 Consumer Decision Markers ---
    st.markdown("---")
    st.markdown("#### Attribute Importance — Consumer Decision Markers")
    st.caption("Attributes most used by consumers for purchase decisions. P0 = highest priority beyond mandatory fields.")

    if not df_master.empty:
        # Rank by: how many L1s have this attribute + average fill rate
        attr_importance = df_master.groupby("ATTRIBUTE_NAME").agg(
            L1_Count=("L1", "nunique"),
            Avg_Fill=("FILL_PCT", "mean"),
            Total_SPINs=("SPINS_WITH_ATTR", "sum"),
        ).reset_index()
        attr_importance["Avg_Fill"] = attr_importance["Avg_Fill"].round(1)
        attr_importance = attr_importance[~attr_importance["ATTRIBUTE_NAME"].apply(lambda x: x.lower().replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "") in skip_lower)]
        attr_importance = attr_importance.sort_values("L1_Count", ascending=False)

        # P0 consumer decision attributes (hardcoded for key categories)
        P0_ATTRS = {"color", "colour", "material", "material_type", "pack_size", "quantity",
                    "unit of measure", "gender", "type", "size", "weight", "flavor", "flavour",
                    "brand", "closure_type", "heel_type", "pattern", "water_resistant",
                    "insole_material", "toe_shape", "strap_type", "heel_size",
                    "wattage", "capacity", "power", "voltage", "compatible_devices",
                    "age_group", "skin_type", "hair_type", "fragrance", "spf", "shelf_life"}
        attr_importance["Priority"] = attr_importance["ATTRIBUTE_NAME"].apply(
            lambda x: "P0" if x.lower() in P0_ATTRS else "P1"
        )
        attr_importance.columns = ["Attribute", "L1 Categories", "Avg Fill %", "Total SPINs", "Priority"]
        show_table(attr_importance, key="attr_importance", height=500)
    else:
        st.info("Run sync to generate attribute master from CMS.")


def render_value_standardization(fs):
    """Value standardization / outlier detection."""
    st.subheader("Value Standardization")
    show_sync_time(["value_standardization"])

    df = C("value_standardization")
    if df.empty:
        st.info("Value standardization data not available yet. Needs Databricks access for attribute library.")
        st.markdown("""
        **Planned:**
        - Compare SPIN attribute values vs standardized library
        - Flag non-standard values (e.g., "Cottyon" vs "Cotton")
        - Track % standardized day-to-day
        - RED ALERT when inventory hits pods but attributes not standardized
        - Only library values shown on storefront — unstandardized = broken filters

        **Waiting for:**
        - Databricks access: `analytics_prod.im_catalog_attribute_library`
        - Attribute library contains L3-level standardized values per attribute

        **Impact:** Unstandardized values → missing storefront filters → poor customer discoverability
        """)

        # Show what we'll track
        st.markdown("#### Example")
        example = pd.DataFrame([
            {"L3": "Men's T-Shirts", "Attribute": "Material", "Standard Values": "Cotton, Polyester, Linen", "SPIN Value": "Cottyon", "Status": "FLAGGED"},
            {"L3": "Men's T-Shirts", "Attribute": "Material", "Standard Values": "Cotton, Polyester, Linen", "SPIN Value": "Cotton", "Status": "OK"},
            {"L3": "Men's T-Shirts", "Attribute": "Sleeve", "Standard Values": "Full, Half, Sleeveless", "SPIN Value": "Ful Sleeve", "Status": "FLAGGED"},
        ])
        st.dataframe(example, use_container_width=True, hide_index=True)
        return

    df = filter_dims(df, fs)
    show_table(df, key="val_std", height=500)


# ── SPIN Lookup (Real-time) ──────────────────────────────────────────────────
CDN_BASE = "https://instamart-media-assets.swiggy.com/swiggy/image/upload/"


def _create_snowflake_connection():
    """Create a live Snowflake connection (local only, SSO via Edge)."""
    import snowflake.connector
    import os as _os, webbrowser, subprocess
    _os.environ["SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED"] = "true"
    _orig = webbrowser.open
    def _edge(url, new=0, autoraise=True):
        subprocess.Popen(f'start msedge "{url}"', shell=True)
        return True
    webbrowser.open = _edge
    config = load_config()
    p = dict(config["snowflake"])
    p["client_store_temporary_credential"] = False
    conn = snowflake.connector.connect(**p)
    webbrowser.open = _orig
    return conn


def get_snowflake_connection():
    """Get or create Snowflake connection via session state."""
    if "sf_conn" not in st.session_state:
        st.session_state["sf_conn"] = None
    if st.session_state["sf_conn"] is not None:
        # Test if still alive
        try:
            st.session_state["sf_conn"].cursor().execute("SELECT 1")
            return st.session_state["sf_conn"]
        except Exception:
            st.session_state["sf_conn"] = None
    return st.session_state.get("sf_conn")


def resolve_spin_search(search_text):
    """Resolve SPIN ID or Item Code from cached data."""
    df = C("spin_image_master")
    if df.empty:
        return None
    search = search_text.strip()
    # Try Item Code (numeric)
    if search.isdigit():
        df["ITEM_CODE"] = df["ITEM_CODE"].astype(str)
        match = df[df["ITEM_CODE"] == search]
    else:
        match = df[df["SPIN_ID"] == search.upper()]
    if match.empty:
        # Try partial product name
        match = df[df["PRODUCT_NAME"].str.contains(search, case=False, na=False)].head(5)
    if match.empty:
        return None
    row = match.iloc[0]
    return {
        "spin_id": str(row.get("SPIN_ID", "")),
        "item_code": str(row.get("ITEM_CODE", "")),
        "product_name": str(row.get("PRODUCT_NAME", "")),
        "l1": str(row.get("L1", "")),
        "l2": str(row.get("L2", "")),
        "brand": str(row.get("BRAND", "")),
        "image_count": int(row.get("IMAGE_COUNT", 0)),
        "created_date": str(row.get("CREATED_DATE", "")),
        "multiple_results": len(match) if len(match) > 1 else 0,
        "all_matches": match if len(match) > 1 else None,
    }


def render_spin_lookup():
    st.title("SPIN Lookup")
    st.caption("Real-time product view — search by SPIN ID or Item Code")

    search = st.text_input("🔍 Enter SPIN ID or Item Code", placeholder="e.g. DSM6DGU3TW or 406238",
                           key="spin_search")
    if not search or not search.strip():
        st.info("Enter a SPIN ID or Item Code above to begin.")
        return

    result = resolve_spin_search(search.strip())
    if result is None:
        st.error(f"No match found for '{search}' in cached data.")
        return

    # If multiple matches (product name search), let user pick
    if result["multiple_results"] > 1:
        st.warning(f"Found {result['multiple_results']} matches. Showing first match. Refine your search.")

    # Header card
    st.markdown(f"### {result['product_name']}")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("SPIN ID", result["spin_id"])
    c2.metric("Item Code", result["item_code"])
    c3.metric("L1", result["l1"])
    c4.metric("L2", result["l2"])
    c5.metric("Images", result["image_count"])

    # Check / establish live connection
    conn = get_snowflake_connection()
    if conn is None:
        try:
            # Auto-connect on first search
            with st.spinner("Connecting to Snowflake (SSO — check Edge browser)..."):
                st.session_state["sf_conn"] = _create_snowflake_connection()
                conn = st.session_state["sf_conn"]
                st.success("Connected to Snowflake!")
        except ImportError:
            st.info("Snowflake connector not installed. Showing cached data only (cloud mode).")
        except Exception as e:
            st.warning(f"Could not connect to Snowflake: {e}. Showing cached data.")

    tabs = st.tabs(["General (CMS)", "Enrichment Attributes", "ERP", "Storefront"])

    with tabs[0]:
        render_spin_general(result, conn)
    with tabs[1]:
        st.info("Enrichment Attributes — Coming Soon")
    with tabs[2]:
        render_spin_erp(result, conn)
    with tabs[3]:
        render_spin_storefront(result, conn)


def _fetch_spin_images(conn, spin_id):
    """Fetch images for a SPIN from Snowflake."""
    if not conn:
        return pd.DataFrame()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            k.value:image_id::string as image_id,
            k.value:shot_type::string as shot_type
        FROM cms.cms_ddb.cms_spins_1,
        LATERAL FLATTEN(input => parse_json(cast(images as string))) k
        WHERE hashkey = '{spin_id}'
          AND SORTKEY = 'SPIN'
          AND lower(Businessline) = 'instamart'
        ORDER BY shot_type
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _fetch_spin_attributes(conn, spin_id):
    """Fetch all CMS attributes for a SPIN from Snowflake."""
    if not conn:
        return pd.DataFrame()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            f.key::string as "Attribute",
            f.value::string as "Value"
        FROM cms.cms_ddb.cms_spins_1,
        LATERAL FLATTEN(input => parse_json(cast(attributes as string))) f
        WHERE hashkey = '{spin_id}'
          AND SORTKEY = 'SPIN'
          AND lower(Businessline) = 'instamart'
        ORDER BY f.key
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def render_spin_general(result, conn):
    """General tab — images + full CMS item master attributes."""
    spin_id = result["spin_id"]

    if conn:
        # ── Images ──
        with st.spinner("Fetching images from Snowflake..."):
            df_img = _fetch_spin_images(conn, spin_id)

        if not df_img.empty:
            st.markdown("#### Product Images")
            # Sort: MN first, then BK, then AL1, AL2, etc.
            SHOT_ORDER = {"MN": 0, "BK": 1}
            df_img["_sort"] = df_img["SHOT_TYPE"].apply(
                lambda s: SHOT_ORDER.get(str(s), 10 + int(''.join(filter(str.isdigit, str(s))) or 99))
            )
            df_img = df_img.sort_values("_sort")
            images = []
            for _, row in df_img.iterrows():
                img_id = row["IMAGE_ID"]
                shot = row["SHOT_TYPE"]
                if img_id and str(img_id) != "None":
                    images.append({"url": CDN_BASE + str(img_id), "shot": str(shot)})

            if images:
                # First 5 visible
                cols = st.columns(min(5, len(images)))
                for i, img in enumerate(images[:5]):
                    with cols[i]:
                        st.image(img["url"], caption=img["shot"], width=150)

                # Remaining in expander
                if len(images) > 5:
                    with st.expander(f"All Images ({len(images)} total)"):
                        for batch_start in range(0, len(images), 5):
                            batch = images[batch_start:batch_start+5]
                            cols2 = st.columns(5)
                            for j, img in enumerate(batch):
                                with cols2[j]:
                                    st.image(img["url"], caption=img["shot"], width=140)
            else:
                st.warning("No images found for this SPIN.")
        else:
            st.warning("No image data returned.")

        # ── Item Master Attributes ──
        st.markdown("---")
        st.markdown("#### Item Master Attributes")
        with st.spinner("Fetching attributes from Snowflake..."):
            df_attr = _fetch_spin_attributes(conn, spin_id)

        if not df_attr.empty:
            # Clean up JSON values — extract "value" field if it's a JSON object
            def _clean_val(v):
                if not v or v == "None":
                    return ""
                try:
                    import json as _json
                    parsed = _json.loads(v)
                    if isinstance(parsed, dict) and "value" in parsed:
                        return str(parsed["value"]) if parsed["value"] else ""
                    return str(parsed)
                except Exception:
                    return str(v)

            df_attr["Value"] = df_attr["Value"].apply(_clean_val)
            df_attr = df_attr[df_attr["Value"] != ""]

            # Display in 2-column layout
            ca, cb = st.columns(2)
            half = len(df_attr) // 2
            with ca:
                show_table(df_attr.iloc[:half], key="spin_attr_left", height=600)
            with cb:
                show_table(df_attr.iloc[half:], key="spin_attr_right", height=600)
        else:
            st.warning("No attribute data returned.")
    else:
        # Cache-only fallback
        st.info("Live Snowflake connection not available. Showing cached data only.")
        st.markdown("#### Basic Info (from cache)")
        info = pd.DataFrame([
            {"Field": "SPIN ID", "Value": result["spin_id"]},
            {"Field": "Item Code", "Value": result["item_code"]},
            {"Field": "Product Name", "Value": result["product_name"]},
            {"Field": "L1 Category", "Value": result["l1"]},
            {"Field": "L2 Category", "Value": result["l2"]},
            {"Field": "Brand", "Value": result["brand"]},
            {"Field": "Image Count", "Value": str(result["image_count"])},
            {"Field": "Created Date", "Value": result["created_date"]},
        ])
        show_table(info, key="spin_basic")


def render_spin_erp(result, conn):
    """ERP tab — city × tier distribution for this item."""
    item_code = result["item_code"]
    st.markdown("#### ERP Assortment Status")

    # Try cache first
    df_erp = C("erp_intended_city_tier")
    df_pods = C("pod_master")

    if not df_erp.empty:
        df_erp["ITEM_CODE"] = df_erp["ITEM_CODE"].astype(str)
        df_item = df_erp[df_erp["ITEM_CODE"] == str(item_code)]
    else:
        df_item = pd.DataFrame()

    if df_item.empty and conn:
        # Live query fallback
        with st.spinner("Fetching ERP data from Snowflake..."):
            cur = conn.cursor()
            cur.execute(f"""
                SELECT "City", "POD_TIERING" as tier, "Region",
                       "TEMPORARY DISABLE FLAG" as disable_flag
                FROM TEMP.PUBLIC.IM_ERP_REGION_SHEETS_MASTER
                WHERE "ITEM CODE" = '{item_code}'
                  AND "UPLOAD_DATE_TRIM" = (SELECT MAX("UPLOAD_DATE_TRIM")
                      FROM TEMP.PUBLIC.IM_ERP_REGION_SHEETS_MASTER)
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            df_item = pd.DataFrame(rows, columns=cols)
            if not df_item.empty:
                df_item.columns = [c.upper() for c in df_item.columns]

    if df_item.empty:
        st.warning(f"Item Code {item_code} not found in ERP data.")
        return

    # Summary metrics
    n_cities = df_item["CITY"].nunique()
    tier_col = "ERP_TIER" if "ERP_TIER" in df_item.columns else "TIER"
    n_tiers = df_item[tier_col].nunique() if tier_col in df_item.columns else 0

    c1, c2 = st.columns(2)
    c1.metric("Cities Tagged", n_cities)
    c2.metric("Tiers", n_tiers)

    # Table 1: City × Tier with pod counts
    st.markdown("#### City × Tier Breakdown")
    city_tier = df_item.groupby(["CITY", tier_col]).size().reset_index(name="Entries") if tier_col in df_item.columns else df_item.groupby("CITY").size().reset_index(name="Entries")

    # Join pod counts if available
    if not df_pods.empty and "CITY" in df_pods.columns and "TIER" in df_pods.columns:
        active_pods = df_pods[df_pods.get("Active/Non_Active Pod", df_pods.columns[-1]) == "Active"] if "Active/Non_Active Pod" in df_pods.columns else df_pods
        pod_counts = active_pods.groupby(["CITY", "TIER"])["STORE_ID"].nunique().reset_index(name="Active Pods")
        if tier_col in city_tier.columns:
            city_tier = city_tier.merge(pod_counts, left_on=["CITY", tier_col], right_on=["CITY", "TIER"], how="left")
            if "TIER" in city_tier.columns and tier_col != "TIER":
                city_tier = city_tier.drop(columns=["TIER"])
        city_tier["Active Pods"] = city_tier.get("Active Pods", 0).fillna(0).astype(int)

    show_table(city_tier, key="spin_erp_ct", height=400)

    # Table 2: Tier-level city count
    st.markdown("#### Tier-Level Summary")
    if tier_col in df_item.columns:
        tier_summary = df_item.groupby(tier_col)["CITY"].nunique().reset_index()
        tier_summary.columns = ["Tier", "Cities"]
        tier_summary = tier_summary.sort_values("Cities", ascending=False)
        show_table(tier_summary, key="spin_erp_tier")

    st.download_button("Download ERP Data", city_tier.to_csv(index=False),
                       f"erp_{item_code}.csv", "text/csv", key="dl_spin_erp")


def render_spin_storefront(result, conn=None):
    """Storefront tab — enablement status across pods.
    Logic: If item is at tier M in ERP for a city, it should be live in M + all higher tiers.
    Active pods = sum of pods across those qualifying tiers.
    Enabled pods = live Snowflake query on cms_skus (actual SKU state per pod).
    Enable % = enabled / intended active pods.
    """
    spin_id = result["spin_id"]
    item_code = result["item_code"]

    st.markdown("#### Storefront Enablement Status")
    show_sync_time(["erp_intended_city_tier"])

    # Tier hierarchy: lower index = smaller tier
    TIER_ORDER = ["XS", "XS1", "S", "S1", "M", "M1", "L", "L1", "XL", "XL1",
                  "XXL", "2XL", "3XL", "4XL", "5XL", "6XL", "7XL", "8XL"]
    TIER_RANK = {t: i for i, t in enumerate(TIER_ORDER)}
    # Map XXL ↔ 2XL (pod_master uses XXL, ERP uses 2XL)
    TIER_ALIAS = {"XXL": "2XL", "2XL": "XXL"}

    def _norm_city(s):
        return str(s).replace(".0", "").strip()

    df_erp = C("erp_intended_city_tier")
    df_state = C("assortment_state")
    df_overrides = C("assortment_overrides")
    df_pods = C("pod_master")

    # ── Step 1: Get ERP tier for this item per city ──
    if not df_erp.empty:
        df_erp["ITEM_CODE"] = df_erp["ITEM_CODE"].astype(str)
        item_erp = df_erp[df_erp["ITEM_CODE"] == str(item_code)].copy()
    else:
        item_erp = pd.DataFrame()

    if item_erp.empty:
        st.warning(f"Item {item_code} not found in ERP data.")
        return

    # ── Step 2: Build pod_master city × tier active pod counts ──
    city_map = pd.DataFrame()
    pods_by_city_tier = pd.DataFrame()
    if not df_pods.empty:
        df_pods["CITY_ID_STR"] = df_pods["CITY_ID"].apply(_norm_city)
        city_map = df_pods[["CITY_ID_STR", "CITY"]].drop_duplicates()
        city_name_to_id = dict(zip(city_map["CITY"].str.lower(), city_map["CITY_ID_STR"]))

        active = df_pods[df_pods["Active/Non_Active Pod"] == "Active"].copy()
        pods_by_city_tier = active.groupby(["CITY", "TIER"])["STORE_ID"].nunique().reset_index(name="pods")

    # ── Step 3: For each city in ERP, compute intended pods (tier cascade) ──
    rows = []
    for city, grp in item_erp.groupby("CITY"):
        erp_tier = grp["ERP_TIER"].iloc[0]
        erp_rank = TIER_RANK.get(erp_tier, TIER_RANK.get(TIER_ALIAS.get(erp_tier, ""), -1))
        if erp_rank < 0:
            continue

        # All tiers >= ERP tier rank
        qualifying_tiers = [t for t in TIER_ORDER if TIER_RANK[t] >= erp_rank]

        # Count active pods in this city across qualifying tiers
        city_pods = pods_by_city_tier[pods_by_city_tier["CITY"].str.lower() == city.lower()] if not pods_by_city_tier.empty else pd.DataFrame()

        intended_pods = 0
        tier_pod_detail = {}
        for qt in qualifying_tiers:
            # Check both the tier and its alias (XXL vs 2XL)
            tiers_to_check = [qt]
            if qt in TIER_ALIAS:
                tiers_to_check.append(TIER_ALIAS[qt])
            pod_count = 0
            for tc in tiers_to_check:
                match = city_pods[city_pods["TIER"] == tc]
                if not match.empty:
                    pod_count += int(match["pods"].sum())
            if pod_count > 0:
                tier_pod_detail[qt] = pod_count
                intended_pods += pod_count

        rows.append({
            "City": city,
            "ERP Tier": erp_tier,
            "Qualifying Tiers": ", ".join(qualifying_tiers),
            "Intended Pods": intended_pods,
            "Tier Breakdown": " | ".join([f"{t}:{c}" for t, c in tier_pod_detail.items()]),
        })

    df_intended = pd.DataFrame(rows)

    # ── Step 4: Get ACTUAL enabled pods from live Snowflake query (cms_skus) ──
    enabled_by_city = pd.DataFrame()
    if conn:
        with st.spinner("Fetching live SKU enablement from Snowflake..."):
            try:
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT
                        b.city AS city,
                        COUNT(DISTINCT CASE WHEN lower(skus.state) = 'enabled' THEN skus.hashkey END) AS enabled_skus,
                        COUNT(DISTINCT skus.hashkey) AS total_skus
                    FROM cms.cms_ddb.cms_skus AS skus
                    JOIN analytics.public.sumanth_anobis_storedetails b
                        ON TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) = b.store_id
                    WHERE skus.spinid = '{spin_id}'
                      AND b.store_id != 3141
                    GROUP BY b.city
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                enabled_by_city = pd.DataFrame(rows, columns=cols)
                st.caption(f"Live data: {int(enabled_by_city['ENABLED_SKUS'].sum())} enabled SKUs across {len(enabled_by_city)} cities")
            except Exception as e:
                st.warning(f"Live query failed: {e}")
    else:
        st.info("No live Snowflake connection — enabled pod counts unavailable on cloud.")

    # Join enabled counts to intended
    if not enabled_by_city.empty:
        enabled_map = dict(zip(enabled_by_city["CITY"].str.lower(), enabled_by_city["ENABLED_SKUS"]))
        df_intended["enabled_pods"] = df_intended["City"].apply(
            lambda c: int(enabled_map.get(c.lower(), 0)))
    else:
        df_intended["enabled_pods"] = 0

    # ── Step 5: Force disabled ──
    if not df_overrides.empty:
        spin_overrides = df_overrides[df_overrides["spin_id"] == spin_id].copy()
        fd = spin_overrides[spin_overrides["assortment_override_state"] == "STATE_FORCE_DISABLED"]
        if not fd.empty:
            fd = fd.copy()
            fd["city_id_str"] = fd["city_id"].apply(_norm_city)
            fd_by_city = fd.groupby("city_id_str").agg(
                force_disabled=("spin_id", "count"),
                pod_ids=("pod_id", lambda x: ", ".join([str(int(float(p))) for p in x.dropna() if str(p) != "None"]))
            ).reset_index()
            if "city_id_str" in df_intended.columns:
                df_intended = df_intended.merge(fd_by_city[["city_id_str", "force_disabled"]], on="city_id_str", how="left")
    if "force_disabled" not in df_intended.columns:
        df_intended["force_disabled"] = 0
    df_intended["force_disabled"] = df_intended["force_disabled"].fillna(0).astype(int)

    # ── Step 6: Compute Enable % ──
    df_intended["Enable %"] = (df_intended["enabled_pods"] / df_intended["Intended Pods"].replace(0, 1) * 100).round(1)
    df_intended["Enable %"] = df_intended["Enable %"].clip(upper=100)
    df_intended["Delta %"] = (100 - df_intended["Enable %"]).round(1)

    # ── Display ──
    display_cols = ["City", "ERP Tier", "Intended Pods", "Tier Breakdown",
                    "enabled_pods", "Enable %", "force_disabled", "Delta %"]
    df_display = df_intended[[c for c in display_cols if c in df_intended.columns]].copy()
    df_display = df_display.rename(columns={
        "enabled_pods": "Enabled Pods", "force_disabled": "Force Disabled"
    })
    df_display = df_display.sort_values("City")

    # Summary
    total_intended = int(df_display["Intended Pods"].sum())
    total_enabled = int(df_display["Enabled Pods"].sum())
    total_fd = int(df_display["Force Disabled"].sum())
    overall_enable = round(total_enabled / max(total_intended, 1) * 100, 1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Intended Pods", f"{total_intended:,}")
    c2.metric("Enabled Pods", f"{total_enabled:,}")
    c3.metric("Enable %", f"{overall_enable}%")
    c4.metric("Force Disabled", f"{int(total_fd):,}")

    st.markdown("#### City × Tier Enablement")
    show_table(df_display, key="spin_storefront", height=500)

    # Force disabled detail
    if not df_overrides.empty:
        spin_ov = df_overrides[df_overrides["spin_id"] == spin_id]
        fd_all = spin_ov[spin_ov["assortment_override_state"] == "STATE_FORCE_DISABLED"]
        if not fd_all.empty:
            st.markdown("#### Force Disabled Pod Details")
            fd_det = fd_all[["city_id", "pod_id", "location_type", "updated_by"]].copy()
            fd_det["city_id"] = fd_det["city_id"].apply(_norm_city)
            if not city_map.empty:
                fd_det = fd_det.merge(city_map, left_on="city_id", right_on="CITY_ID_STR", how="left")
            show_table(fd_det, key="spin_fd_detail")

    st.download_button("Download Storefront Data", df_display.to_csv(index=False),
                       f"storefront_{spin_id}.csv", "text/csv", key="dl_spin_sf")


# ── Shelf Life Deviation ─────────────────────────────────────────────────────
def render_shelf_life(fs):
    st.title("Shelf Life Deviation Report")
    show_sync_time(["shelf_life_data"])
    st.caption("Compares actual shelf life cutoffs vs master rules | FMCG focus")

    df = C("shelf_life_data")
    if df.empty:
        st.warning("No shelf life data. Run `python fetch_shelf_life.py`")
        return

    # Filter toggle
    sl_filter = st.radio("Category Filter", ["FMCG Only", "New Commerce Only", "All"], horizontal=True)
    if sl_filter == "FMCG Only":
        df = df[df["IS_FMCG"] == True]
    elif sl_filter == "New Commerce Only":
        df = df[df.get("IS_NC", pd.Series(False)) == True]

    df = filter_dims(df, fs)

    tabs = st.tabs(["Overview", "Deviations Detail", "By L1", "By Shelf Life Range", "Master Rules"])

    with tabs[0]:
        render_sl_overview(df, fs)
    with tabs[1]:
        render_sl_deviations(df, fs)
    with tabs[2]:
        render_sl_by_l1(df, fs)
    with tabs[3]:
        render_sl_by_range(df, fs)
    with tabs[4]:
        render_sl_master_rules()


def render_sl_overview(df, fs):
    st.subheader("Overview")
    total = len(df)
    any_dev = int(df["ANY_DEVIATION"].sum()) if "ANY_DEVIATION" in df.columns else 0
    wh_in_dev = int(df["WH_INWARD_DEVIATION"].sum()) if "WH_INWARD_DEVIATION" in df.columns else 0
    cx_dev = int(df["CX_CUTOFF_DEVIATION"].sum()) if "CX_CUTOFF_DEVIATION" in df.columns else 0
    wh_out_dev = int(df["WH_OUTWARD_DEVIATION"].sum()) if "WH_OUTWARD_DEVIATION" in df.columns else 0
    compliant = total - any_dev

    c1, c2 = st.columns(2)
    c1.metric("Total SPINs", f"{total:,}")
    c2.metric("Any Deviation", f"{any_dev:,}", delta=f"{any_dev/max(total,1)*100:.1f}%", delta_color="inverse")

    # Individual field-level deviations
    st.markdown("#### Field-Level Deviations")
    f1, f2, f3, f4 = st.columns(4)
    f1.metric("WH Inwarding Cutoff", f"{wh_in_dev:,}",
              delta=f"{wh_in_dev/max(total,1)*100:.1f}% deviated", delta_color="inverse")
    f2.metric("CX Cutoff", f"{cx_dev:,}",
              delta=f"{cx_dev/max(total,1)*100:.1f}% deviated", delta_color="inverse")
    f3.metric("WH Outwarding Cutoff", f"{wh_out_dev:,}",
              delta=f"{wh_out_dev/max(total,1)*100:.1f}% deviated", delta_color="inverse")
    f4.metric("Fully Compliant", f"{compliant:,}",
              delta=f"{compliant/max(total,1)*100:.1f}%")

    if any_dev > 0:
        st.error(f"**{any_dev:,} SPINs** ({any_dev/max(total,1)*100:.1f}%) have shelf life deviations from master rules")
    else:
        st.success("All SPINs compliant with shelf life master rules!")

    # Compliance by range with field-level breakdown
    st.markdown("---")
    st.markdown("#### Compliance by Shelf Life Range")
    if "SHELF_LIFE_RANGE" in df.columns:
        range_stats = df.groupby("SHELF_LIFE_RANGE").agg(
            Total=("SPIN_ID", "count"),
            Any_Deviation=("ANY_DEVIATION", "sum"),
            WH_Inward_Dev=("WH_INWARD_DEVIATION", "sum"),
            CX_Cutoff_Dev=("CX_CUTOFF_DEVIATION", "sum"),
            WH_Outward_Dev=("WH_OUTWARD_DEVIATION", "sum"),
        ).reset_index()
        range_stats["Compliant %"] = ((range_stats["Total"] - range_stats["Any_Deviation"]) / range_stats["Total"] * 100).round(1)
        range_stats["WH Inward %"] = (range_stats["WH_Inward_Dev"] / range_stats["Total"] * 100).round(1)
        range_stats["CX Cutoff %"] = (range_stats["CX_Cutoff_Dev"] / range_stats["Total"] * 100).round(1)
        range_stats["WH Outward %"] = (range_stats["WH_Outward_Dev"] / range_stats["Total"] * 100).round(1)
        range_stats = range_stats.sort_values("Any_Deviation", ascending=False)
        ca, cb = st.columns(2)
        with ca:
            st.bar_chart(range_stats.set_index("SHELF_LIFE_RANGE")[["WH Inward %", "CX Cutoff %", "WH Outward %"]])
        with cb:
            show_table(range_stats, key="sl_range_ov")


def render_sl_deviations(df, fs):
    st.subheader("Deviation Details")

    # Filter to only deviations
    dev_df = df[df.get("ANY_DEVIATION", False) == True] if "ANY_DEVIATION" in df.columns else pd.DataFrame()

    if dev_df.empty:
        st.success("No deviations found!")
        return

    st.metric("Total Deviations", f"{len(dev_df):,}")

    # Deviation type selector
    dev_type = st.radio("Deviation Type", ["All", "WH Inwarding", "CX Cutoff", "WH Outwarding"], horizontal=True)
    if dev_type == "WH Inwarding":
        dev_df = dev_df[dev_df["WH_INWARD_DEVIATION"] == True]
    elif dev_type == "CX Cutoff":
        dev_df = dev_df[dev_df["CX_CUTOFF_DEVIATION"] == True]
    elif dev_type == "WH Outwarding":
        dev_df = dev_df[dev_df["WH_OUTWARD_DEVIATION"] == True]

    display_cols = ["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "BRAND", "L1", "L2",
                    "SHELF_LIFE_DAYS", "SHELF_LIFE_RANGE",
                    "WH_INWARDING_CUTOFF", "EXPECTED_WH_INWARD", "WH_INWARD_DEVIATION",
                    "CX_CUTOFF", "EXPECTED_CX_CUTOFF", "CX_CUTOFF_DEVIATION",
                    "WH_OUTWARDING_CUTOFF", "EXPECTED_WH_OUTWARD", "WH_OUTWARD_DEVIATION"]
    available = [c for c in display_cols if c in dev_df.columns]
    show_table(dev_df[available], key="sl_dev_detail", height=500)
    st.download_button("Download Deviations", dev_df[available].to_csv(index=False),
                       "shelf_life_deviations.csv", "text/csv")


def render_sl_by_l1(df, fs):
    st.subheader("Deviations by L1 Category")

    l1_stats = df.groupby("L1").agg(
        Total=("SPIN_ID", "count"),
        Any_Deviation=("ANY_DEVIATION", "sum"),
        WH_Inward_Dev=("WH_INWARD_DEVIATION", "sum"),
        CX_Cutoff_Dev=("CX_CUTOFF_DEVIATION", "sum"),
        WH_Outward_Dev=("WH_OUTWARD_DEVIATION", "sum"),
        Avg_Shelf_Life=("SHELF_LIFE_DAYS", "mean"),
    ).reset_index()
    l1_stats["Deviation %"] = (l1_stats["Any_Deviation"] / l1_stats["Total"] * 100).round(1)
    l1_stats["WH Inward %"] = (l1_stats["WH_Inward_Dev"] / l1_stats["Total"] * 100).round(1)
    l1_stats["CX Cutoff %"] = (l1_stats["CX_Cutoff_Dev"] / l1_stats["Total"] * 100).round(1)
    l1_stats["WH Outward %"] = (l1_stats["WH_Outward_Dev"] / l1_stats["Total"] * 100).round(1)
    l1_stats["Avg_Shelf_Life"] = l1_stats["Avg_Shelf_Life"].round(0)
    l1_stats = l1_stats.sort_values("Any_Deviation", ascending=False)

    ca, cb = st.columns(2)
    with ca:
        st.markdown("#### Worst L1 by Deviation Count")
        ch = l1_stats.head(15)
        if not ch.empty:
            st.bar_chart(ch.set_index("L1")["Deviation %"], horizontal=True)
    with cb:
        show_table(l1_stats, key="sl_l1", height=500)

    st.download_button("Download L1 Summary", l1_stats.to_csv(index=False),
                       "shelf_life_by_l1.csv", "text/csv")


def render_sl_by_range(df, fs):
    st.subheader("By Shelf Life Range")

    for rng in ["0-30", "31-149", "150-180", "181-360", ">360"]:
        rng_df = df[df["SHELF_LIFE_RANGE"] == rng]
        if rng_df.empty:
            continue

        total = len(rng_df)
        devs = int(rng_df["ANY_DEVIATION"].sum())
        with st.expander(f"{rng} Days — {total:,} SPINs | {devs:,} deviations ({devs/max(total,1)*100:.1f}%)"):
            if devs > 0:
                dev_items = rng_df[rng_df["ANY_DEVIATION"] == True]
                display_cols = ["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "L1",
                                "SHELF_LIFE_DAYS", "WH_INWARDING_CUTOFF", "EXPECTED_WH_INWARD",
                                "CX_CUTOFF", "EXPECTED_CX_CUTOFF",
                                "WH_OUTWARDING_CUTOFF", "EXPECTED_WH_OUTWARD"]
                available = [c for c in display_cols if c in dev_items.columns]
                show_table(dev_items[available].head(100), key=f"sl_rng_{rng}", height=350)
            else:
                st.success("All compliant!")


def render_sl_master_rules():
    st.subheader("Master Rules Reference")
    rules = pd.DataFrame([
        {"Shelf Life Range": "0 - 30 Days", "WH Inwarding Cutoff": "70%", "WH Outward / Pod Inward": "CX + 1 Day", "CX Cutoff": "30% of Shelf Life"},
        {"Shelf Life Range": "31 - 149 Days", "WH Inwarding Cutoff": "70%", "WH Outward / Pod Inward": "CX + 2 Days", "CX Cutoff": "30% of Shelf Life"},
        {"Shelf Life Range": "150 - 180 Days", "WH Inwarding Cutoff": "70%", "WH Outward / Pod Inward": "47 Days", "CX Cutoff": "45 Days"},
        {"Shelf Life Range": "181 - 360 Days", "WH Inwarding Cutoff": "60%", "WH Outward / Pod Inward": "50 Days", "CX Cutoff": "45 Days"},
        {"Shelf Life Range": "> 360 Days", "WH Inwarding Cutoff": "50%", "WH Outward / Pod Inward": "52 Days", "CX Cutoff": "45 Days"},
    ])
    st.dataframe(rules, use_container_width=True, hide_index=True)
    st.caption("Source: New Shelf Life Master sheet")


# ── ERP Events ───────────────────────────────────────────────────────────────
def render_erp_events(fs):
    st.title("ERP Assortment Monitor (Events)")
    show_sync_time()

    tabs = st.tabs(["Event Calendar", "Quarterly View", "Upcoming Events",
                     "Completed (JFM)", "P3 Events"])

    with tabs[0]:
        render_event_calendar(fs)
    with tabs[1]:
        render_event_quarterly(fs)
    with tabs[2]:
        render_events_upcoming(fs)
    with tabs[3]:
        render_events_completed(fs)
    with tabs[4]:
        render_events_p3(fs)


def render_event_calendar(fs):
    st.subheader("Event Calendar — 2026 JFM + AMJ")
    st.caption("All events with dates, status, category, and readiness checks")

    df = C("events_calendar")
    if df.empty:
        st.warning("No event data. Run parse_events.py.")
        return

    df["start_date"] = pd.to_datetime(df["start_date"])

    # Summary
    total = len(df)
    completed = len(df[df["phase"] == "Completed"])
    this_week = len(df[df["phase"] == "This Week"])
    next_2w = len(df[df["phase"] == "Next 2 Weeks"])
    upcoming = len(df[df["phase"] == "Upcoming"])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Events", total)
    c2.metric("Completed", completed)
    c3.metric("This Week", this_week)
    c4.metric("Next 2 Weeks", next_2w)
    c5.metric("Upcoming", upcoming)

    # Filter by quarter
    quarter_filter = st.radio("Quarter", ["All", "Q1 (JFM)", "Q2 (AMJ)"], horizontal=True)
    if quarter_filter == "Q1 (JFM)":
        df = df[df["quarter"] == "Q1"]
    elif quarter_filter == "Q2 (AMJ)":
        df = df[df["quarter"] == "Q2"]

    # Filter by phase
    phase_filter = st.multiselect("Phase", df["phase"].unique().tolist())
    if phase_filter:
        df = df[df["phase"].isin(phase_filter)]

    # Full calendar table
    display_cols = ["month", "start_date", "event_name", "region", "event_category",
                    "pan_or_regional", "status", "phase", "days_until",
                    "assortment", "opd_spike_2025_pct"]
    display = df[[c for c in display_cols if c in df.columns]].sort_values("start_date")
    show_table(display, key="evt_cal", height=600)
    st.download_button("Download Calendar", display.to_csv(index=False), "events_calendar.csv", "text/csv")


def render_event_quarterly(fs):
    st.subheader("Quarterly Event Summary")

    df = C("events_calendar")
    if df.empty:
        return

    df["start_date"] = pd.to_datetime(df["start_date"])

    for q, q_label in [("Q1", "Q1 — January / February / March"),
                         ("Q2", "Q2 — April / May / June")]:
        df_q = df[df["quarter"] == q]
        if df_q.empty:
            continue

        st.markdown(f"### {q_label}")

        # By month
        month_summary = df_q.groupby("month").agg(
            Events=("event_name", "count"),
            Completed=("phase", lambda x: (x == "Completed").sum()),
            P0_P1=("event_category", lambda x: x.isin(["P0", "P1"]).sum()),
        ).reset_index()
        # Sort months correctly
        month_order = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6}
        month_summary["order"] = month_summary["month"].map(month_order)
        month_summary = month_summary.sort_values("order").drop(columns=["order"])

        ca, cb = st.columns(2)
        with ca:
            st.bar_chart(month_summary.set_index("month")["Events"])
        with cb:
            show_table(month_summary, key=f"q_{q}_months")

        # By region
        region_counts = df_q.groupby("region").size().reset_index(name="Events").sort_values("Events", ascending=False)
        with st.expander(f"Events by Region ({q})"):
            show_table(region_counts, key=f"q_{q}_region")

        # By category
        if "event_category" in df_q.columns:
            cat_counts = df_q.groupby("event_category").size().reset_index(name="Events").sort_values("Events", ascending=False)
            with st.expander(f"Events by Category ({q})"):
                show_table(cat_counts, key=f"q_{q}_cat")


def render_events_upcoming(fs):
    st.subheader("Upcoming Events — Next 30 Days")
    st.caption("Events requiring assortment readiness")

    df = C("events_calendar")
    if df.empty:
        return

    df["start_date"] = pd.to_datetime(df["start_date"])
    df_upcoming = df[(df["days_until"] >= 0) & (df["days_until"] <= 30)].sort_values("days_until")

    if df_upcoming.empty:
        st.info("No events in the next 30 days.")
        return

    # Readiness check: 30d, 45d, 60d checks
    for _, row in df_upcoming.iterrows():
        days = int(row.get("days_until", 0))
        name = row.get("event_name", "")
        region = row.get("region", "")
        cat = row.get("event_category", "")
        status = row.get("status", "")
        assortment = row.get("assortment", "")

        if days <= 7:
            color = "red"
            urgency = "THIS WEEK"
        elif days <= 14:
            color = "orange"
            urgency = "NEXT 2 WEEKS"
        else:
            color = "blue"
            urgency = f"In {days} days"

        with st.container():
            c1, c2, c3 = st.columns([3, 1, 1])
            c1.markdown(f"**{name}** — {region}")
            c2.markdown(f":{color}[{urgency}]")
            c3.markdown(f"`{cat}` | {status}")
            if assortment and str(assortment) not in ["", "nan", "None", "NaN"]:
                st.caption(f"Assortment: {str(assortment)[:150]}")
            st.markdown("---")


def render_events_completed(fs):
    st.subheader("Completed Events — JFM 2026")
    st.caption("Historical reference + YoY tracking (dates may change in 2027)")

    df = C("events_calendar")
    if df.empty:
        return

    df["start_date"] = pd.to_datetime(df["start_date"])
    df_done = df[df["phase"] == "Completed"].sort_values("start_date")

    if df_done.empty:
        st.info("No completed events.")
        return

    st.metric("Completed Events (JFM)", len(df_done))

    # By month
    month_summary = df_done.groupby("month").agg(
        Events=("event_name", "count"),
    ).reset_index()
    st.bar_chart(month_summary.set_index("month"))

    # Full table
    display_cols = ["month", "start_date", "event_name", "region", "event_category",
                    "pan_or_regional", "status", "assortment", "opd_spike_2025_pct"]
    show_table(df_done[[c for c in display_cols if c in df_done.columns]], key="evt_done", height=500)

    st.info("ERP assortment add/remove tracking per event will be available once the Events ERP table is built in Snowflake.")


def render_events_p3(fs):
    st.subheader("P3 Events — Low Priority / Awareness")
    st.caption("Events tracked for awareness — no dedicated assortment action")

    df = C("events_p3")
    if df.empty:
        st.info("No P3 events data.")
        return

    show_table(df, key="p3_events", height=500)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    # Login check
    user = check_login()
    if not user:
        return

    metric, fs = render_sidebar()

    # Check access
    if not has_access(user, metric):
        st.warning(f"You don't have access to **{metric}**. Contact admin.")
        return

    if metric == "Image Health":
        render_image_health(fs)
    elif metric == "ERP Assortment (BAU)":
        render_erp(fs)
    elif metric == "ERP Assortment (Events)":
        render_erp_events(fs)
    elif metric == "Enabled Items Health":
        render_enabled_health(fs)
    elif metric == "Shelf Life Deviation":
        render_shelf_life(fs)
    elif metric == "SPIN Lookup":
        render_spin_lookup()

    # Eagle Eye — admin only, hidden at bottom
    if user.get("role") == "admin":
        with st.expander("Eagle Eye (Admin Only)", expanded=False):
            log_file = BASE_DIR / "cache" / ".login_log.csv"
            if log_file.exists():
                import csv
                df_log = pd.read_csv(log_file)
                st.caption(f"Login log: {len(df_log)} entries")
                show_table(df_log.sort_values("timestamp", ascending=False), key="login_log", height=300)
            else:
                st.info("No login activity yet.")
    st.markdown("---")
    st.caption(f"Logged in as: {user.get('name', user.get('email', ''))} | Catalog & Master Health App v2.1 | {now_ist().strftime('%Y-%m-%d %H:%M')} IST")

if __name__ == "__main__":
    main()
