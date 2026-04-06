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
from datetime import datetime
from pathlib import Path

st.set_page_config(page_title="Catalog & Master Health App", layout="wide", initial_sidebar_state="expanded")

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
THRESHOLD_COMBO_MATCH = 95.0


# ── Access Control ───────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_access():
    with open(BASE_DIR / "access_control.json") as f:
        return json.load(f)


def check_login():
    """Simple email-based login. Returns user info or None."""
    if "user" in st.session_state and st.session_state.user:
        return st.session_state.user

    access = load_access()
    st.markdown("### Catalog Health Dashboard")
    st.markdown("---")
    email = st.text_input("Enter your email to continue", key="login_email")
    if st.button("Login", type="primary"):
        email = email.strip().lower()
        if email in access["users"]:
            st.session_state.user = access["users"][email]
            st.session_state.user["email"] = email
            st.rerun()
        else:
            st.error("Access denied. Contact manish.hiroo@instamart.in for access.")
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


def show_sync_time():
    """Show last sync time as caption."""
    cache_files = list(CACHE_DIR.glob("*.parquet"))
    if cache_files:
        latest = max(f.stat().st_mtime for f in cache_files)
        ts = datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M")
        st.caption(f"Last synced: {ts}")

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
        ts = datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M")
        age = (time.time() - latest) / 3600
        (st.sidebar.success if age < 6 else st.sidebar.warning)(f"Synced: {ts}")
    else:
        st.sidebar.error("No data. Run sync_data.py")

    st.sidebar.markdown("---")
    metric = st.sidebar.radio("Metric", ["Image Health", "ERP Assortment (BAU)", "ERP Assortment (Events)"])
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
    tabs = st.tabs(["Health Trends", "Coverage", "Onboarding Health", "Slot Standardization",
                     "Defect Detection", "Virtual Combos", "Quality vs BK", "Diff Assortment"])

    with tabs[0]: render_trends(fs)
    with tabs[1]: render_coverage(fs)
    with tabs[2]: render_onboarding(fs)
    with tabs[3]: render_standardization(fs)
    with tabs[4]: render_defects(fs)
    with tabs[5]: render_virtual_combos(fs)
    with tabs[6]: render_quality(fs)
    with tabs[7]: render_diff_assortment(fs)


def render_trends(fs):
    st.subheader("Image Health Trends")
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


def render_coverage(fs):
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
    en = fs.get("enabled", False)
    sfx = "_enabled" if en else ""
    label = "Enabled" if en else "All"
    st.subheader(f"Image Defect Detection ({label})")

    # Use coverage summary for accurate counts (not detail table row counts)
    summary = C(f"image_coverage_summary{sfx}")
    if not summary.empty:
        r = summary.iloc[0]
        count_zero = int(r.get("SPINS_0", 0) or 0)
        count_no_main = int(r.get("NO_MAIN_IMAGE", 0) or 0)
        count_low = int(r.get("SPINS_1TO3", 0) or 0)
    else:
        count_zero = count_no_main = count_low = 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Missing Main Image", f"{count_no_main:,}")
    c2.metric("Zero Images", f"{count_zero:,}")
    c3.metric("1-3 Images (Below Target)", f"{count_low:,}")

    # Detail tables (sample, not full count)
    df_no_main = C("image_defect_no_main")
    df_zero = C("image_defect_zero")
    df_low = C("image_defect_low_count")

    df_no_main = filter_dims(filter_enabled(df_no_main, en), fs)
    df_zero = filter_dims(filter_enabled(df_zero, en), fs)
    df_low = filter_dims(filter_enabled(df_low, en), fs)

    if not df_zero.empty:
        st.markdown("#### SPINs With Zero Images")
        show_table(df_zero, key="def_zero", height=300)

    if not df_no_main.empty:
        st.markdown("#### SPINs Missing Main Image")
        show_table(df_no_main, key="def_main", height=300)

    if not df_low.empty:
        with st.expander(f"SPINs with 1-3 Images ({len(df_low)} items)"):
            show_table(df_low, key="def_low", height=500)
            st.download_button("Download", df_low.to_csv(index=False), "defect_low_images.csv", "text/csv")

    st.markdown("---")
    st.info("Coming soon: AI-powered image analysis — 'Coming Soon' placeholder detection, wrong/irrelevant image detection, blurry image flagging, manual SPIN check tool")


def render_virtual_combos(fs):
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
    npi_key_missing = len(df_npi_full[df_npi_full.get("NPI_Status", pd.Series()) == "Key Missing"]) if "NPI_Status" in df_npi_full.columns else 0
    npi_key_available = npi_count - npi_key_missing

    df_images = C("diff_assortment_image_status")
    if en:
        df_diff = filter_enabled(df_diff, True)
        df_images = filter_enabled(df_images, True)

    total = len(df_diff)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Upgrade Items", f"{total:,}")
    c2.metric("NPI Total", f"{npi_count:,}")
    c3.metric("NPI Key Missing", f"{npi_key_missing:,}")
    c4.metric("NPI Key Available", f"{npi_key_available:,}")
    biz = df_diff.groupby("Business")["Item Code"].count()
    c5.metric("New Commerce", f"{biz.get('New Comm', 0):,}")
    c6.metric("FMCG", f"{biz.get('FMCG', 0):,}")

    with st.expander("By Bet Category"):
        df_bet = df_diff.groupby(["Business", "Bet Category"]).size().reset_index(name="Items").sort_values("Items", ascending=False)
        show_table(df_bet, key="bet")

    if df_images.empty:
        st.warning("No image data. Run sync_data.py.")
        return

    st.markdown("---")
    st.markdown("#### Upgrade Image Status")
    has_s2 = len(df_images[df_images["HAS_BK"] == "Yes"])
    no_s2 = len(df_images[df_images["HAS_BK"] == "No"])
    not_cms = total - len(df_images)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("In CMS", f"{len(df_images):,}")
    m2.metric("Has Upgrade", f"{has_s2:,}", delta=f"{has_s2/max(len(df_images),1)*100:.1f}%")
    m3.metric("Missing Upgrade", f"{no_s2:,}")
    m4.metric("Not in CMS", f"{not_cms:,}")

    merged = df_images.merge(
        df_diff[["Item Code", "Business", "Bet Category", "Brand Name", "SKU Name"]],
        left_on="ITEM_CODE", right_on="Item Code", how="left")

    df_gap = merged[merged["HAS_BK"] == "No"]
    ca, cb = st.columns(2)
    with ca:
        st.markdown("#### Missing Upgrade by Category")
        gap_cat = df_gap.groupby("Bet Category").size().reset_index(name="Missing").sort_values("Missing", ascending=False).head(15)
        if not gap_cat.empty:
            st.bar_chart(gap_cat.set_index("Bet Category"))
    with cb:
        st.markdown("#### Gap List")
        show_table(df_gap[["ITEM_CODE", "SPIN_ID", "Brand Name", "Bet Category", "IMAGE_COUNT", "HAS_MAIN"]].rename(
            columns={"ITEM_CODE": "Item", "SPIN_ID": "SPIN", "IMAGE_COUNT": "Images", "HAS_MAIN": "Main"}
        ), key="gap", height=400)

    if not df_gap.empty:
        st.download_button("Download Gap", df_gap.to_csv(index=False), "gap.csv", "text/csv")

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
        f1, f2, f3 = st.columns(3)
        f1.metric("Overall Fill Rate", f"{matched_count}/{checkable} ({matched_count/max(checkable,1)*100:.1f}%)")
        f2.metric("Checkable (excl NPI)", f"{total - npi_count_val:,}")
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

    tabs = st.tabs(["Overview", "City Add/Remove", "City Expansion",
                     "Removed & Re-Added", "Pod Tiering", "Brand View"])

    with tabs[0]:
        render_erp_overview(fs)
    with tabs[1]:
        render_erp_city_changes(fs)
    with tabs[2]:
        render_erp_expansion(fs)
    with tabs[3]:
        render_erp_removed_readded(fs)
    with tabs[4]:
        render_erp_tiering(fs)
    with tabs[5]:
        render_erp_brands(fs)


def render_erp_overview(fs):
    st.subheader("ERP Overview")

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


def render_erp_city_changes(fs):
    st.subheader("City-Level Daily Add/Remove")
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
    st.caption("S, S1, M, M1, L, etc. — pan-India and by city")

    # Pan-India summary
    df_summary = C("erp_tiering_summary")
    if not df_summary.empty:
        st.markdown("#### Pan-India Tiering (Daily Change)")
        ca, cb = st.columns(2)
        with ca:
            if "Today" in df_summary.columns:
                st.bar_chart(df_summary.set_index("Tier")["Today"])
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
    df = C("erp_brand_current")
    if df.empty:
        st.warning("No data.")
        return
    df = filter_dims(df, fs)
    show_table(df, key="erp_brands", height=600)
    st.download_button("Download", df.to_csv(index=False), "erp_brands.csv", "text/csv")


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
    st.markdown("---")
    st.caption(f"Logged in as: {user.get('name', user.get('email', ''))} | Catalog Health Dashboard v2.1 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
