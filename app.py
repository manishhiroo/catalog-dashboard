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

# ── Design System (dark theme, Swiggy Instamart identity) ────────────────────
_DESIGN_LOADED = False
_DESIGN_ERROR = None
try:
    from design_helpers import (
        load_design_system, mcard, mcard_grid, tag, dot_tag, bar_cell,
        page_header, panel, alert_banner, empty_state, dl_raw_button,
        sync_card, brand_header, render, styled_table,
        render_metrics, render_nav, svg_icon, topbar_html, sub_tabs_html,
        custom_tabs, register_tab_badge, inject_tab_badges,
        inject_global_scripts,
    )
    _DESIGN_LOADED = load_design_system()
except Exception as _e:
    _DESIGN_ERROR = str(_e)
    import traceback
    _DESIGN_ERROR_TB = traceback.format_exc()


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
    """Email-based login with admin PIN protection and login tracking.

    Session persistence: we stash the logged-in email in the ?u= URL param so
    that a full page reload (or bookmark) restores the session without re-prompting.
    Security: the email is just an identifier; the user still has to exist in
    access_control.json to be accepted.
    """
    if "user" in st.session_state and st.session_state.user:
        return st.session_state.user

    access = load_access()

    # Try to restore session from ?u= URL param (survives reloads)
    # Once a user (even an admin) has successfully authenticated in THIS
    # browser session, the URL carries their identity so subsequent page
    # reloads don't force a re-login. The user list in access_control.json
    # is still the gate — only whitelisted emails are accepted.
    try:
        url_email = (st.query_params.get("u") or "").strip().lower()
        if url_email and url_email in access.get("users", {}):
            user = dict(access["users"][url_email])
            user["email"] = url_email
            st.session_state.user = user
            return user
    except Exception:
        pass
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
            # Persist login across page reloads via URL param
            try:
                st.query_params["u"] = email
            except Exception:
                pass
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

def safe_get(rec, key, default=''):
    """Safe getter for either dict, pandas Series, or None.
    Avoids 'truth value of a Series is ambiguous' from (rec or {}).get(...)."""
    if rec is None:
        return default
    try:
        v = rec.get(key, default) if hasattr(rec, 'get') else default
    except Exception:
        return default
    import pandas as _pd
    if v is None:
        return default
    try:
        if isinstance(v, float) and _pd.isna(v):
            return default
    except Exception:
        pass
    return v


def C(key):
    """Read cached parquet."""
    f = CACHE_DIR / f"{key}.parquet"
    return pd.read_parquet(f) if f.exists() else pd.DataFrame()


# ── Pills used by the Upgrade page ───────────────────────────────────────────
def _pill(label, value, kind="info"):
    """Render a small label:value pill. kind: good|warn|critical|info|muted."""
    palette = {
        "good":     ("#10B981", "#0B2A22"),
        "warn":     ("#F59E0B", "#2A1F0B"),
        "critical": ("#F43F5E", "#2A0B14"),
        "info":     ("#38BDF8", "#0B1F2A"),
        "muted":    ("#6B7280", "#1B1F26"),
    }
    fg, bg = palette.get(kind, palette["info"])
    val = str(value) if value not in (None, "", "nan") else "—"
    return (
        f'<span style="display:inline-block;padding:2px 8px;margin:2px 4px 2px 0;'
        f'border-radius:10px;background:{bg};color:{fg};border:1px solid {fg};'
        f'font-size:11px;font-family:JetBrains Mono,monospace;line-height:1.4;">'
        f'<b>{label}</b>: {val}</span>'
    )


def _hash_kind(s):
    """Stable accent color for free-text values (Toxin-Free vs High Protein etc)."""
    if not s or str(s).lower() in ("none", "nan", ""):
        return "critical"
    h = sum(ord(c) for c in str(s)) % 4
    return ["info", "good", "warn", "muted"][h]


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

def show_table(df, key=None, height=400, severity_col=None, severity_map=None):
    """Render a DataFrame with the design's dark-theme styling.

    severity_col: column name whose value drives row background tint.
    severity_map: dict {value -> css bg string}. If omitted, a sensible default
      mapping is used for values: critical|warn|good|ok|info.
    """
    if df.empty:
        st.info("No data.")
        return
    if severity_col and severity_col in df.columns:
        default_map = {
            "critical": "background-color: rgba(244,63,94,0.10)",
            "high":     "background-color: rgba(244,63,94,0.10)",
            "warn":     "background-color: rgba(245,158,11,0.08)",
            "medium":   "background-color: rgba(245,158,11,0.08)",
            "good":     "background-color: rgba(16,185,129,0.06)",
            "ok":       "background-color: rgba(16,185,129,0.06)",
            "info":     "background-color: rgba(56,189,248,0.06)",
        }
        smap = severity_map or default_map

        def _row_bg(row):
            v = str(row.get(severity_col, "")).strip().lower()
            css = smap.get(v, "")
            return [css] * len(row)

        try:
            # Convert any object columns that look numeric to numeric to keep
            # the Styler happy (it will TypeError on mixed object dtypes).
            df_for_style = df.copy()
            for c in df_for_style.columns:
                if df_for_style[c].dtype == object:
                    coerced = pd.to_numeric(df_for_style[c], errors="coerce")
                    # Only coerce if at least 80% of non-null values are numeric
                    non_null = df_for_style[c].notna().sum()
                    if non_null and coerced.notna().sum() / non_null > 0.8:
                        df_for_style[c] = coerced
            styled = df_for_style.style.apply(_row_bg, axis=1)
            st.dataframe(styled, use_container_width=True, hide_index=True, height=height, key=key)
            return
        except Exception:
            # Fallback to plain render if Styler still fails
            pass
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

# Key → label mapping (matches design v2 NAV ids). Also drives routing.
NAV_KEY_TO_LABEL = {
    "image_health": "Image Health",
    "erp_bau":      "ERP Assortment (BAU)",
    "erp_events":   "ERP Assortment (Events)",
    "enabled":      "Enabled Items Health",
    "shelf":        "Shelf Life Deviation",
    "spin":         "SPIN Lookup",
    "upload":       "Upload Preview",
    "qc":           "QC: Upgrade",
}
NAV_LABEL_TO_KEY = {v: k for k, v in NAV_KEY_TO_LABEL.items()}


def render_sidebar():
    user = st.session_state.get("user", {}) or {}

    # Brand header (dark theme design system)
    try:
        with st.sidebar:
            st.markdown(brand_header("Catalog Health", "v2"), unsafe_allow_html=True)
    except Exception:
        st.sidebar.title("Catalog Health")

    # Sync timestamp with pulsing dot
    cache_files = list(CACHE_DIR.glob("*.parquet"))
    if cache_files:
        latest = max(f.stat().st_mtime for f in cache_files)
        ts = to_ist(latest)
        age = (time.time() - latest) / 3600
        try:
            with st.sidebar:
                st.markdown(sync_card(f"{ts} IST", "Synced"), unsafe_allow_html=True)
                if age >= 6:
                    st.caption(f"⚠ Data is {age:.1f}h old")
        except Exception:
            (st.sidebar.success if age < 6 else st.sidebar.warning)(f"Synced: {ts} IST")
    else:
        st.sidebar.error("No data. Run sync_data.py")

    # ── Determine which views this user can access ──────────────────────────
    def _allowed(label):
        return user.get("role") == "admin" or label in user.get("access", [])

    monitoring_items_all = [
        {"key": "image_health", "label": "Image Health",           "icon": "image",    "badge": 3,  "badge_kind": "critical"},
        {"key": "erp_bau",      "label": "ERP Assortment (BAU)",   "icon": "grid",     "badge": 1,  "badge_kind": "warn"},
        {"key": "erp_events",   "label": "ERP Assortment (Events)","icon": "calendar", "badge": 5},
        {"key": "enabled",      "label": "Enabled Items Health",   "icon": "heart"},
        {"key": "shelf",        "label": "Shelf Life Deviation",   "icon": "clock",    "badge": 12},
    ]
    tools_items_all = [
        {"key": "spin",   "label": "SPIN Lookup",       "icon": "search",    "live": True},
        {"key": "upload", "label": "Upload Preview",    "icon": "upload",    "badge": 3},
        {"key": "qc",     "label": "QC: Upgrade","icon": "clipboard","badge": 47, "badge_kind": "warn"},
    ]
    monitoring_items = [it for it in monitoring_items_all if _allowed(NAV_KEY_TO_LABEL[it["key"]])]
    tools_items      = [it for it in tools_items_all      if _allowed(NAV_KEY_TO_LABEL[it["key"]])]

    allowed_keys = [it["key"] for it in (monitoring_items + tools_items)]
    if not allowed_keys:
        allowed_keys = ["image_health"]
        monitoring_items = [monitoring_items_all[0]]

    # Current active view from URL (?view=xxx). Fall back to first allowed.
    current_key = st.query_params.get("view", allowed_keys[0])
    if current_key not in allowed_keys:
        current_key = allowed_keys[0]
        try:
            st.query_params["view"] = current_key
        except Exception:
            pass

    # ── Render the grouped nav as HTML with <a href> (browser-native) ───────
    # Each link's href contains both ?view=<key> AND the current ?u=<email>
    # so session persists across the full reload. check_login() restores the
    # user from ?u= on every fresh page load.
    nav_groups = [{"title": "Monitoring", "items": monitoring_items}]
    if tools_items:
        nav_groups.append({"title": "Tools", "items": tools_items})
    if user.get("role") == "admin":
        nav_groups.append({"title": "Admin", "items": [
            {"key": "eagle", "label": "Eagle Eye", "icon": "eye"},
        ]})

    # Preserve ?u= on every nav click
    import urllib.parse as _up
    current_u = st.query_params.get("u", "")
    extra = f"&u={_up.quote(current_u)}" if current_u else ""

    # Build full nav HTML with the extra_query appended to each item's href
    from design_helpers import sidebar_nav_item as _nav_item
    parts = []
    for group in nav_groups:
        parts.append(
            f'<div class="sb-section"><span class="sb-section-title">{group["title"]}</span></div>'
            '<div class="sb-group">'
        )
        for it in group["items"]:
            parts.append(_nav_item(
                key=it["key"],
                label=it["label"],
                active_key=current_key,
                badge=it.get("badge"),
                badge_kind=it.get("badge_kind"),
                icon=it.get("icon"),
                live=it.get("live", False),
                query_key="view",
                extra_query=extra,
            ))
        parts.append('</div>')

    with st.sidebar:
        st.markdown(
            '<div class="sidebar-nav">' + "".join(parts) + '</div>',
            unsafe_allow_html=True,
        )

    # ── Filters (kept as Streamlit widgets — upgrade to chip HTML later) ────
    st.sidebar.markdown('<div class="sb-section"><span class="sb-section-title">Filters</span></div>', unsafe_allow_html=True)

    fs = {}
    fs["enabled"] = st.sidebar.checkbox(
        "Enabled SPINs only",
        help="Only items live on storefront (enabled in 1+ pod, excl test pod 3141)",
    )
    fs["normal_only"] = st.sidebar.checkbox(
        "Normal items only",
        help="Exclude Virtual Combos — show only normal (non-combo) SPINs",
    )
    fs["exclude_unbranded"] = st.sidebar.checkbox(
        "Exclude Unbranded", value=True,
        help="Exclude items where Brand is 'Unbranded'",
    )
    fs["exclude_junk_l1"] = st.sidebar.checkbox(
        "Exclude non-catalog L1s", value=True,
        help="Exclude: snacc, Assure Packaging, TestCategoryL1, Packaging material, Flyer, Freebie, Print, Sample",
    )

    for key, label in [("l1", "L1 Category"), ("l2", "L2 Category"), ("brand", "Brand")]:
        opts = C(f"filter_{key}")
        if not opts.empty:
            fs[key] = st.sidebar.multiselect(label, opts.iloc[:, 0].tolist())

    # ── User footer (avatar + role + logout) ────────────────────────────────
    if user:
        name = user.get("name") or user.get("email", "")
        initials = "".join(p[0] for p in (name or "U").split()[:2]).upper() or "U"
        role = user.get("role", "user").title()
        with st.sidebar:
            st.markdown(
                f'<div class="user-card">'
                f'  <div class="avatar">{initials}</div>'
                f'  <div class="user-meta"><div class="user-name">{name}</div>'
                f'    <div class="user-role">{role} · instamart.in</div></div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if st.button("Logout", key="logout", use_container_width=True):
                del st.session_state.user
                try:
                    if "u" in st.query_params:
                        del st.query_params["u"]
                except Exception:
                    pass
                st.rerun()

    # ── Global Drill to SPIN / Item Code (works on every view) ──────────────
    st.sidebar.markdown(
        '<div class="sb-section"><span class="sb-section-title">Drill to SPIN</span></div>',
        unsafe_allow_html=True,
    )
    drill = st.sidebar.text_input(
        "Drill to SPIN / Item Code",
        value=st.query_params.get("drill", ""),
        placeholder="e.g. 210 or FL5PODH1BP",
        key="drill_spin",
        label_visibility="collapsed",
    ).strip()
    if drill:
        try:
            st.query_params["drill"] = drill
        except Exception:
            pass
    elif "drill" in st.query_params:
        try:
            del st.query_params["drill"]
        except Exception:
            pass

    fs["drill"] = drill

    return NAV_KEY_TO_LABEL.get(current_key, "Image Health"), fs


# ── Image Health ─────────────────────────────────────────────────────────────
def render_image_health(fs):
    st.markdown(page_header(
        "Image Health Monitor",
        sub="Coverage, defects, and slot standardization across catalog",
    ), unsafe_allow_html=True)
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
                     "Slot Standardization", "Defect Detection", "Virtual Combos", "Quality vs BK"])

    with tabs[0]: render_trends(fs)
    with tabs[1]: render_coverage(fs)
    with tabs[2]: render_onboarding(fs)
    with tabs[3]: render_halfyear_onboarding(fs)
    with tabs[4]: render_standardization(fs)
    with tabs[5]: render_defects(fs)
    with tabs[6]: render_virtual_combos(fs)
    with tabs[7]: render_quality(fs)


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
            _df = df_zero.copy()
            _df.insert(0, "Severity", "critical")
            show_table(_df, key="def_zero", height=350, severity_col="Severity")
            st.download_button("Download Zero Images List", df_zero.to_csv(index=False),
                               "defect_zero_images.csv", "text/csv")
        else:
            st.info("No items with zero images")

    with st.expander(f"SPINs Missing Main Image ({len(df_no_main)} items)"):
        if not df_no_main.empty:
            _df = df_no_main.copy()
            _df.insert(0, "Severity", "critical")
            show_table(_df, key="def_main", height=350, severity_col="Severity")
            st.download_button("Download Missing Main List", df_no_main.to_csv(index=False),
                               "defect_missing_main.csv", "text/csv")
        else:
            st.info("No items missing main image")

    with st.expander(f"SPINs with 1-3 Images ({len(df_low)} items)"):
        if not df_low.empty:
            _df = df_low.copy()
            _df.insert(0, "Severity", "warn")
            show_table(_df, key="def_low", height=500, severity_col="Severity")
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
    st.subheader(f"Upgrade Assortment — Image Tracking ({label})")

    diff_csv = BASE_DIR / "diff_assortment_items.csv"
    if not diff_csv.exists():
        st.error("diff_assortment_items.csv not found.")
        return

    df_diff_raw = pd.read_csv(diff_csv)
    df_diff_raw["Item Code"] = df_diff_raw["Item Code"].astype(str).str.strip()

    # Exclusivity view mode — Level 0 is not official, Level 1/2/3 are official
    has_l0_col = "Is_L0" in df_diff_raw.columns
    if has_l0_col:
        level0_total = int(df_diff_raw["Is_L0"].sum())
        official_total = len(df_diff_raw) - level0_total
        view_mode = st.radio(
            f"Exclusivity View (FinalDAv3): {official_total:,} Official (Level 1/2/3) + {level0_total} Level 0",
            ["Official (Level 1/2/3)", "All (include Level 0)", "Level 0 Only"],
            horizontal=True, key="l0_view_mode"
        )
        if view_mode == "Official (Level 1/2/3)":
            df_diff = df_diff_raw[~df_diff_raw["Is_L0"]].copy()
        elif view_mode == "Level 0 Only":
            df_diff = df_diff_raw[df_diff_raw["Is_L0"]].copy()
        else:
            df_diff = df_diff_raw.copy()
    else:
        df_diff = df_diff_raw.copy()

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

    # Noice = items where Brand Name contains "noice"
    noice_mask = df_diff["Brand Name"].astype(str).str.lower().str.contains("noice", na=False)
    noice_count = int(noice_mask.sum())
    # Exclude Noice items from other business categories
    df_non_noice = df_diff[~noice_mask]
    biz = df_non_noice.groupby("Business")["Item Code"].count()
    nc = int(biz.get("New Comm", 0))
    fmcg = int(biz.get("FMCG", 0))
    ea = int(biz.get("Electronics", 0))
    fresh = int(biz.get("Fresh", 0))

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total", f"{total:,}")
    c2.metric("NC", f"{nc:,}")
    c3.metric("FMCG", f"{fmcg:,}")
    c4.metric("EA", f"{ea:,}")
    c5.metric("Fresh", f"{fresh:,}")
    c6.metric("Noice", f"{noice_count:,}")

    st.markdown("##### WIP for SPIN Creation")
    w1, w2, w3, w4, w5 = st.columns(5)
    w1.metric("Total WIP", f"{npi_count:,}")
    w2.metric("NPI Key Available", f"{npi_key_available:,}")
    w3.metric("NPI Key Missing", f"{npi_key_missing:,}")
    w4.metric("WIP", f"{wip_count:,}")
    w5.metric("Blank/Other", f"{blank_count + wip_counts.get('Other', 0):,}")

    st.caption(f"Summary: {total:,} valid codes + {npi_count:,} WIP = {total + npi_count:,} total (Exclusivity Type filter applied based on view mode above)")

    with st.expander("By Bet Category"):
        df_bet = df_diff.groupby(["Business", "Bet Category"]).size().reset_index(name="Items").sort_values("Items", ascending=False)
        show_table(df_bet, key="bet")

    # === ERP Status for Diff Assortment ===
    st.markdown("---")
    st.markdown("#### ERP Status — Upgrade Assortment")

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
        st.checkbox("Show only Upgrade Assortment items in ERP tabs", key="diff_erp_filter",
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
                st.error(f"**{flagged:,} upgrade assortment items** have Block OTB or Temp Disable flags — needs review")
                with st.expander(f"Flagged Items ({flagged})"):
                    show_table(df_erp_flags, key="diff_erp_flags", height=350)
                    st.download_button("Download Flagged Items", df_erp_flags.to_csv(index=False),
                                       "diff_assortment_blocked.csv", "text/csv")
        else:
            st.success("No upgrade assortment items are blocked or temp disabled")

        # City x Item x Tier download
        with st.expander(f"City x Item x Tier Detail ({len(df_erp_detail):,} rows)"):
            show_table(df_erp_detail, key="diff_erp_detail", height=400)
            st.download_button("Download City x Item x Tier", df_erp_detail.to_csv(index=False),
                               "diff_assortment_city_tier.csv", "text/csv")
    else:
        st.info("No ERP data for upgrade assortment. Run full sync.")

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
        u2.metric("Upgrade Assortment Items", f"{len(diff_item_codes):,}")
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

        # Visual: show images by Bet Category : Upgrade L1 Theme
        st.markdown("---")
        st.markdown("##### Upgrade Images by Bet Category : Upgrade L1 Theme")

        # Build combined label "Bet Category : Upgrade L1 Theme"
        df_diff_for_dropdown = df_diff.copy()
        df_diff_for_dropdown["_group_key"] = (
            df_diff_for_dropdown["Bet Category"].fillna("(blank)").astype(str) + " : " +
            df_diff_for_dropdown.get("Upgrade L1 Theme", pd.Series("(blank)", index=df_diff_for_dropdown.index)).fillna("(blank)").astype(str)
        )
        all_bet_options = sorted(df_diff_for_dropdown["_group_key"].dropna().unique().tolist())
        if all_bet_options:
            selected_bet = st.selectbox("Select Bet Category : Upgrade L1 Theme",
                                         all_bet_options, key="bet_cat_select")
            if selected_bet:
                # All items matching this combined group
                bet_items = df_diff_for_dropdown[df_diff_for_dropdown["_group_key"] == selected_bet]
                # Which have UPGRADE
                upgrade_item_set_local = set(df_upgrade["ITEM_CODE"].astype(str))
                total_bet = len(bet_items)
                has_upgrade = len(bet_items[bet_items["Item Code"].astype(str).isin(upgrade_item_set_local)])
                st.caption(f"{has_upgrade}/{total_bet} items have UPGRADE image in [{selected_bet}]")

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
    st.markdown(page_header(
        "ERP Assortment Monitor (BAU)",
        sub="Live inventory state across cities × pods — BAU assortment",
    ), unsafe_allow_html=True)
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

    active_pct = round(active/max(total,1)*100, 1)
    try:
        render_metrics([
            {"label": "Total Pods", "value": f"{total:,}"},
            {"label": "Active", "value": f"{active:,}",
             "state": "good",
             "delta": f"{active_pct}%", "delta_dir": "up", "delta_period": "active rate"},
            {"label": "Non-Active", "value": f"{inactive:,}",
             "state": "muted"},
            {"label": "Cities", "value": f"{cities:,}",
             "state": "info"},
        ])
    except Exception:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Pods", f"{total:,}")
        c2.metric("Active", f"{active:,}", delta=f"{active_pct}%")
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
    diff_only = st.checkbox("Upgrade Assortment items only", key="block_diff_filter",
                            help="Show only Upgrade Assortment items")

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
                # Cities are stored as a concatenated string in CITIES_AFFECTED;
                # approximate the distinct-city count by splitting on common separators.
                def _city_count(s):
                    if not isinstance(s, str): return 0
                    parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
                    return len(set(parts))
                df_detail = df_detail.assign(_ncity=df_detail["CITIES_AFFECTED"].apply(_city_count))
                df_summary = df_detail.groupby("FLAG_TYPE").agg(
                    ITEMS=("ITEM CODE", "nunique"),
                    CITIES=("_ncity", "sum"),
                ).reset_index()

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

    # Full detail — row-tinted by FLAG_TYPE severity
    st.markdown("---")
    df_detail = C("erp_block_otb_detail")
    if not df_detail.empty:
        if diff_only and diff_items:
            df_detail = df_detail[df_detail["ITEM CODE"].astype(str).isin(diff_items)]

        df_detail = filter_dims(df_detail, fs)
        with st.expander(f"Full Detail ({len(df_detail):,} rows)"):
            # Map FLAG_TYPE to severity: OTB Block → critical (red), Temp Disable → warn (amber)
            _df = df_detail.copy()
            if "FLAG_TYPE" in _df.columns:
                _df["Severity"] = _df["FLAG_TYPE"].map(
                    lambda v: "critical" if "otb" in str(v).lower()
                    else ("warn" if "temp" in str(v).lower() else "info")
                ).fillna("info")
            show_table(_df, key="block_detail", height=500,
                       severity_col="Severity" if "Severity" in _df.columns else None)
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
    st.markdown(page_header(
        "Enabled Items Health",
        sub="Combined image + rating + attribute score for storefront-enabled SKUs",
    ), unsafe_allow_html=True)
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
    diff_only = st.checkbox("Upgrade Assortment only", key=f"rating_diff_{enabled_only}")
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
    """Create a live Snowflake connection (local only, SSO via Edge).
    Auto-resumes the warehouse in case it's been auto-suspended."""
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
    # Longer timeouts so S3 staging downloads don't fail for big SPIN queries
    p.setdefault("network_timeout", 300)
    p.setdefault("socket_timeout", 60)
    conn = snowflake.connector.connect(**p)
    webbrowser.open = _orig

    # Auto-resume the warehouse if it was suspended (57P03). Fail-safe so a
    # suspended warehouse doesn't take down SPIN Lookup with a ProgrammingError.
    try:
        wh = p.get("warehouse")
        if wh:
            conn.cursor().execute(f"ALTER WAREHOUSE {wh} RESUME IF SUSPENDED")
    except Exception:
        pass
    return conn


def _with_warehouse_resume(conn, sql):
    """Execute a query; if the warehouse is suspended (57P03), resume and retry.
    Returns cursor with results, or raises if still fails."""
    import snowflake.connector.errors as _sf_err
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur
    except _sf_err.ProgrammingError as e:
        # 57P03 = warehouse suspended
        if "suspended" in str(e).lower() or "57P03" in str(e):
            try:
                config = load_config()
                wh = config.get("snowflake", {}).get("warehouse")
                if wh:
                    conn.cursor().execute(f"ALTER WAREHOUSE {wh} RESUME")
                    cur = conn.cursor()
                    cur.execute(sql)
                    return cur
            except Exception:
                pass
        raise


def get_snowflake_connection():
    """Get or create Snowflake connection via session state.
    Also resumes a suspended warehouse so SPIN Lookup doesn't crash."""
    if "sf_conn" not in st.session_state:
        st.session_state["sf_conn"] = None
    if st.session_state["sf_conn"] is not None:
        # Test if still alive + warehouse awake
        try:
            st.session_state["sf_conn"].cursor().execute("SELECT 1")
            return st.session_state["sf_conn"]
        except Exception as e:
            err = str(e).lower()
            # If warehouse was suspended, resume and reuse connection
            if "suspended" in err or "57p03" in err:
                try:
                    config = load_config()
                    wh = config.get("snowflake", {}).get("warehouse")
                    if wh:
                        st.session_state["sf_conn"].cursor().execute(
                            f"ALTER WAREHOUSE {wh} RESUME"
                        )
                        return st.session_state["sf_conn"]
                except Exception:
                    pass
            st.session_state["sf_conn"] = None
    return st.session_state.get("sf_conn")


def resolve_spin_search(search_text):
    """Resolve SPIN ID or Item Code from cached data.

    Match is exact (case-insensitive for alphabetic SPINs) against CMS data —
    no character substitution. If a SPIN isn't found, the user sees 'not found'
    so they can correct their input against CMS rather than risk matching the
    wrong product.
    """
    df = C("spin_image_master")
    if df.empty:
        return None
    search = search_text.strip()
    # Try Item Code (numeric)
    if search.isdigit():
        df["ITEM_CODE"] = df["ITEM_CODE"].astype(str)
        match = df[df["ITEM_CODE"] == search]
    else:
        # Exact SPIN match (uppercase only — CMS SPINs are always uppercase)
        match = df[df["SPIN_ID"] == search.upper()]
    if match.empty:
        # Try partial product name search
        match = df[df["PRODUCT_NAME"].str.contains(search, case=False, na=False)].head(5)
    if match.empty:
        return None
    row = match.iloc[0]
    spin_id = str(row.get("SPIN_ID", ""))
    item_code = str(row.get("ITEM_CODE", ""))

    # ── Enrichment: pull real data from cached parquets (no live query needed) ──
    rating_30d = None
    review_count = 0
    try:
        df_r = C("item_ratings_30d")
        if not df_r.empty:
            rmatch = df_r[df_r["ITEM_CODE"].astype(str) == item_code]
            if not rmatch.empty:
                rr = rmatch.iloc[0]
                rating_30d = float(rr.get("AVG_RATING", 0) or 0) or None
                review_count = int(rr.get("ORDERS", 0) or 0)
    except Exception:
        pass

    active_pods = 0
    try:
        df_p = C("pod_count_per_item")
        if not df_p.empty:
            pmatch = df_p[df_p["ITEM_CODE"].astype(str) == item_code]
            if not pmatch.empty:
                active_pods = int(pmatch.iloc[0].get("POD_COUNT", 0) or 0)
    except Exception:
        pass

    is_enabled = False
    try:
        eset = get_enabled_set()
        if eset is not None:
            is_enabled = item_code in eset
    except Exception:
        pass

    # Total pods in system (for the "/ 1,218" label)
    total_pods = 1218
    try:
        df_pm = C("pod_master")
        if not df_pm.empty:
            total_pods = len(df_pm)
    except Exception:
        pass

    # Enablement %: fraction of active pods vs total pods (rough proxy)
    enable_pct = (active_pods / total_pods * 100) if total_pods else 0

    # Virtual combo detection: check if SPIN is in combo list
    is_normal = True
    try:
        df_vc = C("virtual_combo_image_match")
        if not df_vc.empty and "COMBO_SPIN" in df_vc.columns:
            is_normal = not (df_vc["COMBO_SPIN"].astype(str) == spin_id).any()
    except Exception:
        pass

    # OPEN FOR PROCUREMENT: item is in any city ERP AND not OTB blocked / Temp Disabled
    in_erp = False
    try:
        df_all_erp = C("erp_all_items")
        if not df_all_erp.empty:
            in_erp = item_code in df_all_erp["ITEM_CODE"].astype(str).values
    except Exception:
        pass

    # ERP block detection — FLAG_TYPE 'Permanent' means "active, not blocked".
    # Only OTB Block / Temp Disable / other explicit flags count.
    is_blocked = False
    block_reasons = []
    cities_blocked = 0
    cities_otb_blocked = 0
    cities_temp_disabled = 0
    try:
        df_blk = C("erp_block_otb_detail")
        if not df_blk.empty:
            blk_col = "ITEM CODE" if "ITEM CODE" in df_blk.columns else "ITEM_CODE"
            rows = df_blk[df_blk[blk_col].astype(str) == item_code]
            # Exclude 'Permanent' (= live)
            if not rows.empty and "FLAG_TYPE" in rows.columns:
                rows = rows[~rows["FLAG_TYPE"].astype(str).str.lower().eq("permanent")]
            if not rows.empty:
                is_blocked = True
                block_reasons = rows["FLAG_TYPE"].dropna().astype(str).unique().tolist()
                city_col = next((c for c in ("City", "CITY", "city") if c in rows.columns), None)
                if city_col:
                    cities_blocked = int(rows[city_col].nunique())
                    cities_otb_blocked = int(
                        rows[rows["FLAG_TYPE"].astype(str).str.lower().str.contains("otb")][city_col].nunique()
                    )
                    cities_temp_disabled = int(
                        rows[rows["FLAG_TYPE"].astype(str).str.lower().str.contains("temp")][city_col].nunique()
                    )
                else:
                    # Fall back to CITIES_AFFECTED numeric column if present
                    if "CITIES_AFFECTED" in rows.columns:
                        cities_blocked = int(rows["CITIES_AFFECTED"].sum())
    except Exception:
        pass

    # Total cities where the item is intended in ERP (regardless of block state)
    cities_in_erp = 0
    try:
        df_int = C("erp_intended_city_tier")
        if not df_int.empty and "ITEM_CODE" in df_int.columns:
            cities_in_erp = int(
                df_int.loc[df_int["ITEM_CODE"].astype(str) == item_code, "CITY"].nunique()
            )
    except Exception:
        pass

    # Open-for-procurement cities = in_erp cities minus blocked cities
    cities_open = max(0, cities_in_erp - cities_blocked)

    procurement_enabled = in_erp and not is_blocked

    # Secondary / Tertiary pod status — read from cached sync (overridden by
    # _fetch_spin_attrs_live when a Snowflake connection is available).
    secondary_pod_enabled = None
    p999_availability = None
    try:
        df_sec = C("upgrade_spin_secondary_p999")
        if not df_sec.empty:
            sm = df_sec[df_sec.get("SPIN_ID", df_sec.get("spin_id")).astype(str) == spin_id] if "SPIN_ID" in df_sec.columns or "spin_id" in df_sec.columns else pd.DataFrame()
            if sm.empty and "ITEM_CODE" in df_sec.columns:
                sm = df_sec[df_sec["ITEM_CODE"].astype(str) == item_code]
            if not sm.empty:
                r0 = sm.iloc[0]
                def _b(v):
                    if v is None: return None
                    s = str(v).strip().lower()
                    if s in ("true", "1", "yes", "y", "enabled", "on"): return True
                    if s in ("false", "0", "no", "n", "disabled", "off"): return False
                    return None
                for col in ("SECONDARY_POD_ENABLED", "secondary_pod_enabled", "ATTR_SECONDARY_POD_ENABLED"):
                    if col in sm.columns:
                        secondary_pod_enabled = _b(r0.get(col))
                        break
                for col in ("P999_AVAILABILITY", "p999_availability", "ATTR_P999_AVAILABILITY", "TERTIARY", "tertiary"):
                    if col in sm.columns:
                        p999_availability = _b(r0.get(col))
                        break
    except Exception:
        pass

    return {
        "spin_id": spin_id,
        "item_code": item_code,
        "product_name": str(row.get("PRODUCT_NAME", "")),
        "l1": str(row.get("L1", "")),
        "l2": str(row.get("L2", "")),
        "brand": str(row.get("BRAND", "")),
        "image_count": int(row.get("IMAGE_COUNT", 0)),
        "has_main": str(row.get("HAS_MAIN", "")).strip().lower() in ("yes", "true", "1"),
        "has_bk":   str(row.get("HAS_BK", "")).strip().lower() in ("yes", "true", "1"),
        "has_al1":  str(row.get("HAS_AL1", "")).strip().lower() in ("yes", "true", "1"),
        "created_date": str(row.get("CREATED_DATE", "")),
        # Enriched fields for the hero
        "is_enabled":   is_enabled,
        "is_normal":    is_normal,
        "rating_30d":   rating_30d,
        "review_count": review_count,
        "active_pods":  active_pods,
        "total_pods":   total_pods,
        "enable_pct":   enable_pct,
        # Procurement status
        "in_erp":              in_erp,
        "is_blocked":          is_blocked,
        "block_reasons":       block_reasons,
        "procurement_enabled": procurement_enabled,
        "open_for_procurement": procurement_enabled,  # legacy alias
        "cities_in_erp":       cities_in_erp,
        "cities_blocked":      cities_blocked,
        "cities_otb_blocked":  cities_otb_blocked,
        "cities_temp_disabled": cities_temp_disabled,
        "cities_open":         cities_open,
        "secondary_pod_enabled": secondary_pod_enabled,
        "p999_availability":     p999_availability,
        "multiple_results": len(match) if len(match) > 1 else 0,
        "all_matches": match if len(match) > 1 else None,
    }


def render_spin_lookup():
    import html as _html
    st.markdown(page_header(
        "SPIN Lookup",
        sub="Real-time product view — search by SPIN ID or Item Code",
    ), unsafe_allow_html=True)

    search = st.text_input(
        "SPIN ID / Item Code", placeholder="e.g. DSM6DGU3TW or 406238",
        key="spin_search", label_visibility="collapsed",
    )

    if not search or not search.strip():
        # Empty state — single-line HTML so Streamlit's markdown parser
        # doesn't mistake indented lines for code blocks.
        st.markdown(
            '<div class="panel"><div class="empty">'
            '<div class="e-title">Search to begin</div>'
            '<div class="e-sub">Enter a SPIN ID or 6-digit Item Code above. '
            'Tip: press <span class="kbd">/</span> to focus the search.</div>'
            '</div></div>',
            unsafe_allow_html=True,
        )
        return

    result = resolve_spin_search(search.strip())
    if result is None:
        st.error(f"No match found for '{search}' in cached data.")
        return

    if result["multiple_results"] > 1:
        st.warning(f"Found {result['multiple_results']} matches. Showing first match. Refine your search.")

    # ── Establish live Snowflake connection FIRST (so hero shows live data) ─
    conn = get_snowflake_connection()
    if conn is None:
        try:
            with st.spinner("Connecting to Snowflake (SSO — check Edge browser)..."):
                st.session_state["sf_conn"] = _create_snowflake_connection()
                conn = st.session_state["sf_conn"]
        except ImportError:
            pass  # cloud mode — no local snowflake driver
        except Exception as e:
            # Detect IP-block error and show a concise caption instead of
            # a giant red/yellow box with the full stacktrace URL.
            err = str(e)
            if "is not allowed to access" in err or "250001" in err or "390422" in err:
                st.caption("⚠ Snowflake IP not whitelisted for your current network — showing cached values. "
                           "Connect corp VPN or ask admin to allowlist your IP.")
            elif "suspended" in err.lower():
                st.caption("⚠ Snowflake warehouse suspended and auto-resume failed — showing cached values.")
            else:
                st.caption(f"⚠ Snowflake connection unavailable — showing cached values. ({err[:80]}…)")

    # ── Live fetch: override cached stats with fresh values from Snowflake ──
    if conn:
        with st.spinner(f"Fetching live state for {result['spin_id']}..."):
            live        = _fetch_spin_hero_live(conn, result["spin_id"])
            rating_live = _fetch_spin_rating_live(conn, result["spin_id"])
            attrs_live  = _fetch_spin_attrs_live(conn, result["spin_id"])
        if live and "_live_error" not in live:
            result.update(live)
        elif "_live_error" in live:
            st.caption(f"⚠ Live enablement fetch failed, using cached: {live['_live_error']}")
        if rating_live and "_rating_error" not in rating_live:
            result.update(rating_live)
        elif "_rating_error" in rating_live:
            st.caption(f"⚠ Live rating fetch failed, using cached: {rating_live['_rating_error']}")
        if attrs_live and "_attrs_error" not in attrs_live:
            result.update(attrs_live)

    # ── Hero panel (design v2: image gallery + identity + 5 stat cards) ─────
    try:
        _e = _html.escape
        img_count = int(result.get("image_count", 0) or 0)
        total_slots = 7  # MN + BK + AL1–AL5 per guidelines
        img_state = "good" if img_count >= 4 else ("warn" if img_count >= 1 else "critical")
        img_state_label = "Complete" if img_count >= 4 else ("Missing slots" if img_count >= 1 else "No images")

        # Thumbnail slots — use HAS_MAIN/HAS_BK/HAS_AL1 from spin_image_master
        # to tell which slots are filled (no per-slot URLs available in cache;
        # real images load later inside the General tab via live Snowflake fetch).
        slots = [
            ("MN",  result.get("has_main", False)),
            ("BK",  result.get("has_bk", False)),
            ("AL1", result.get("has_al1", False)),
            ("AL2", img_count >= 4),
        ]
        thumbs = [(slot, None, filled) for slot, filled in slots]

        def _thumb_html(slot, filled, active=False):
            # Filled = slot has an image uploaded. Empty = missing slot.
            status_cls = "filled" if filled else "empty"
            label = slot if filled else f"—"
            return (
                f'<div class="img-thumb {status_cls}{" active" if active else ""}" '
                f'title="Slot {_e(slot)}: {"Present" if filled else "Missing"}">'
                f'<div class="placeholder-img" data-label="{_e(label)}"></div>'
                f'<span class="img-thumb-slot">{_e(slot)}</span>'
                f'</div>'
            )

        def _main_html(slot, filled):
            label = f"MAIN · {slot}" if filled else f"NO {slot} IMAGE"
            return (
                f'<div class="img-main">'
                f'<div class="placeholder-img" data-label="{_e(label)}"></div>'
                f'<div style="position:absolute;top:8px;left:8px;display:flex;gap:4px">'
                f'<span class="tag accent">{_e(slot)}</span>'
                f'<span class="tag muted">{img_count}/{total_slots}</span>'
                f'</div>'
                f'</div>'
            )

        # Main image: prefer MN if filled, else first filled slot, else MN placeholder
        main_slot, _unused, main_filled = next(
            ((s, u, f) for (s, u, f) in thumbs if f),
            thumbs[0],
        )
        thumbs_html = "".join(
            _thumb_html(s, f, s == main_slot) for (s, _u, f) in thumbs[:4]
        )

        # ── Status table (right side of breadcrumb row) ─────────────────────
        # Three labeled rows:  Item Type · Procurement Enabled · Storefront Enabled
        is_enabled     = bool(result.get("is_enabled", False))
        is_normal      = bool(result.get("is_normal", True))
        proc_enabled   = bool(result.get("procurement_enabled", False))
        in_erp         = bool(result.get("in_erp", False))
        is_blocked     = bool(result.get("is_blocked", False))
        block_reasons  = result.get("block_reasons", []) or []
        _ap            = int(result.get("active_pods") or 0)
        _enabled_pods  = int(result.get("enabled_pods") or 0)

        # 1) Item Type
        item_type_val = "Normal" if is_normal else "Combo"
        item_type_kind = "info" if is_normal else "accent"

        # 2) Procurement Enabled (Yes if present in any 1 city ERP and not a real block)
        if proc_enabled:
            proc_val, proc_kind = "Yes", "good"
        elif is_blocked:
            reasons = ", ".join(block_reasons) or "blocked"
            proc_val, proc_kind = f"Blocked — {reasons}", "critical"
        elif not in_erp:
            proc_val, proc_kind = "No — not in any city ERP", "muted"
        else:
            proc_val, proc_kind = "Unknown", "muted"

        # 3) Storefront Enabled (Yes if live in any 1 active pod)
        if is_enabled:
            sf_val, sf_kind = f"Yes — {_enabled_pods:,} pod{'s' if _enabled_pods != 1 else ''} live", "good"
        elif _ap > 0:
            sf_val, sf_kind = f"No — in {_ap:,} pods but none enabled", "warn"
        else:
            sf_val, sf_kind = "No — no active pods", "muted"

        def _status_row(label, value, kind):
            return (
                '<div class="status-row">'
                f'<span class="status-lbl">{_e(label)}</span>'
                f'<span class="status-val {kind}">{_e(value)}</span>'
                '</div>'
            )

        # 4) Secondary Pod row
        sec = result.get("secondary_pod_enabled")
        if sec is True:
            sec_val, sec_kind = "Enabled", "good"
        elif sec is False:
            sec_val, sec_kind = "Disabled", "muted"
        else:
            sec_val, sec_kind = "Not set in CMS", "muted"

        # 5) Tertiary (P999) row
        ter = result.get("p999_availability")
        if ter is True:
            ter_val, ter_kind = "Available (P999)", "good"
        elif ter is False:
            ter_val, ter_kind = "Not available", "muted"
        else:
            ter_val, ter_kind = "Not set in CMS", "muted"

        status_chips_html = (
            '<div class="status-table">'
            + _status_row("Item Type",           item_type_val, item_type_kind)
            + _status_row("Procurement Enabled", proc_val,      proc_kind)
            + _status_row("Storefront Enabled",  sf_val,        sf_kind)
            + _status_row("Secondary Pod",       sec_val,       sec_kind)
            + _status_row("Tertiary (P999)",     ter_val,       ter_kind)
            + '</div>'
        )

        # Stats row (5 cards)
        rating = result.get("rating_30d") or result.get("rating") or "—"
        review_count = result.get("review_count_30d") or result.get("review_count") or 0
        active_pods = result.get("active_pods") or result.get("enabled_pods") or 0
        total_pods = result.get("total_pods") or 1218
        enable_pct = result.get("enable_pct") or 0
        cities_live = result.get("cities_live") or 0
        cities_total = result.get("cities_total") or 149
        bk_score = result.get("bk_quality_score") or "—"

        rating_str = f"{rating:.1f}" if isinstance(rating, (int, float)) else str(rating)

        # Active pods: enabled (state=ENABLED AND active=1) / intended (any active pod)
        enabled_pods_val = int(result.get("enabled_pods") or 0)
        intended_pods    = int(result.get("active_pods") or 0)   # from live query: stores item is deployed in
        pods_state = "good" if intended_pods and enabled_pods_val == intended_pods else (
                     "warn" if enabled_pods_val > 0 else "critical")
        pods_label = (
            "All enabled" if intended_pods and enabled_pods_val == intended_pods
            else (f"{intended_pods - enabled_pods_val:,} not enabled" if enabled_pods_val else "None live")
        )

        # Open for procurement: cities in ERP and not blocked / total cities in ERP
        cities_in_erp     = int(result.get("cities_in_erp") or 0)
        cities_open_val   = int(result.get("cities_open") or 0)
        cities_blocked_v  = int(result.get("cities_blocked") or 0)
        cities_otb        = int(result.get("cities_otb_blocked") or 0)
        cities_temp       = int(result.get("cities_temp_disabled") or 0)
        if cities_in_erp:
            proc_state = "good" if cities_blocked_v == 0 else ("warn" if cities_open_val > 0 else "critical")
            proc_main = f"{cities_open_val:,}<small>/{cities_in_erp:,}</small>"
            proc_sub  = f"cities in ERP" if cities_blocked_v == 0 else f"{cities_blocked_v:,} blocked"
        else:
            proc_state = "muted"
            proc_main = "—"
            proc_sub  = "not in any city ERP"

        # Blocked cities breakdown card
        blocked_sub_parts = []
        if cities_otb:  blocked_sub_parts.append(f"{cities_otb:,} OTB Block")
        if cities_temp: blocked_sub_parts.append(f"{cities_temp:,} Temp Disable")
        blocked_sub = " · ".join(blocked_sub_parts) if blocked_sub_parts else "None"
        blocked_state = "critical" if cities_blocked_v > 0 else "good"
        blocked_main = f"{cities_blocked_v:,}" if cities_in_erp else "—"

        stats_html = (
            '<div class="spin-stat-row">'
              '<div class="spin-stat">'
                '<span class="slab">Images</span>'
                f'<span class="sval">{img_count}<small>/{total_slots}</small></span>'
                f'<span class="dot-tag"><span class="dot {img_state}"></span>{_e(img_state_label)}</span>'
              '</div>'
              '<div class="spin-stat">'
                '<span class="slab">Rating 30d</span>'
                f'<span class="sval">{_e(rating_str)}<small>★</small></span>'
                f'<span class="dot-tag"><span class="dot good"></span>{review_count:,} reviews</span>'
              '</div>'
              '<div class="spin-stat">'
                '<span class="slab">Active pods</span>'
                f'<span class="sval">{enabled_pods_val:,}<small>/{intended_pods:,}</small></span>'
                f'<span class="dot-tag"><span class="dot {pods_state}"></span>{_e(pods_label)}</span>'
              '</div>'
              '<div class="spin-stat">'
                '<span class="slab">Open for procurement</span>'
                f'<span class="sval">{proc_main}</span>'
                f'<span class="dot-tag"><span class="dot {proc_state}"></span>{_e(proc_sub)}</span>'
              '</div>'
              '<div class="spin-stat">'
                '<span class="slab">Blocked cities</span>'
                f'<span class="sval">{blocked_main}</span>'
                f'<span class="dot-tag"><span class="dot {blocked_state}"></span>{_e(blocked_sub)}</span>'
              '</div>'
            '</div>'
        )

        brand = result.get("brand", "—") or "—"
        l1 = result.get("l1", "") or ""
        l2 = result.get("l2", "") or ""
        name = result.get("product_name", "") or ""
        spin_id = result.get("spin_id", "") or ""
        item_code = result.get("item_code", "") or ""

        # IMPORTANT: Streamlit's markdown parser treats lines with 4+ leading
        # spaces as code blocks. We collapse the HTML to a single line so none
        # of the <div> tags are mis-parsed as code.
        hero_parts = [
            '<div class="spin-hero">',
              '<div class="img-gallery">',
                _main_html(main_slot, main_filled),
                f'<div class="img-thumbs">{thumbs_html}</div>',
              '</div>',
              '<div class="spin-info">',
                '<div class="spin-breadcrumb">',
                  f'<span>{_e(l1)}</span>',
                  '<span class="sep">›</span>',
                  f'<span>{_e(l2)}</span>',
                  '<span class="sep">›</span>',
                  f'<span style="color:var(--fg)">{_e(brand)}</span>',
                '</div>',
                f'<h2 class="spin-name">{_e(name)}</h2>',
                '<div class="spin-ids">',
                  f'<span>SPIN <b>{_e(spin_id)}</b></span>',
                  f'<span>Item <b>{_e(item_code)}</b></span>',
                  f'<span>Brand <b>{_e(brand)}</b></span>',
                  (f'<span>Qty <b>{_e(result["quantity_uom"])}</b></span>'
                   if result.get("quantity_uom") else
                   '<span title="Quantity not set in CMS attributes">Qty <b style="color:var(--fg-dim)">—</b></span>'),
                  f'<span>Images <b>{img_count}/{total_slots}</b></span>',
                '</div>',
                status_chips_html,  # labeled 3-row status table
                stats_html,
              '</div>',
            '</div>',
        ]
        hero_html = "".join(p.strip() for p in hero_parts)
        st.markdown(hero_html, unsafe_allow_html=True)
    except Exception as _he:
        # Fallback hero if anything goes wrong
        st.markdown(f"### {result.get('product_name', 'SPIN')}")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("SPIN ID", result["spin_id"])
        c2.metric("Item Code", result["item_code"])
        c3.metric("L1", result.get("l1", "—"))
        c4.metric("L2", result.get("l2", "—"))
        c5.metric("Images", result["image_count"])

    # ── Live/cached badge above tabs ────────────────────────────────────────
    if result.get("_live"):
        st.markdown('<div style="margin: 8px 0 4px"><span class="tag info">● Live Snowflake</span></div>',
                    unsafe_allow_html=True)
    else:
        st.caption("⚠ Showing cached values — live Snowflake connection unavailable")

    # ── Sub-tabs (existing 5-tab structure, styled by design-system CSS) ────
    tabs = st.tabs(["General (CMS)", "Enrichment Attributes", "ERP", "Storefront", "Logs"])

    with tabs[0]:
        render_spin_general(result, conn)
    with tabs[1]:
        st.markdown(
            '<div class="empty">'
            '<div class="e-title">Enrichment attributes — Coming soon</div>'
            '<div class="e-sub">Category-specific enrichment attributes (calories, '
            'ingredients, certifications) will appear here once Databricks sync is live.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with tabs[2]:
        render_spin_erp(result, conn)
    with tabs[3]:
        render_spin_storefront(result, conn)
    with tabs[4]:
        render_spin_logs(result)


def _fetch_city_erp_live(conn, item_code):
    """Live per-city ERP state for an item from the raw ERP sheet table.
    Returns: CITY, TIER, FLAG_TYPE, REASON (one row per city-tier combo).

    This is what the cached erp_block_otb_detail loses — it aggregates to
    CITIES_AFFECTED count only. This function gives the actual city names
    so users can answer 'which cities is my item Temp Disabled in?'.
    """
    if not conn:
        return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                "City"                      AS CITY,
                "POD_TIERING"               AS TIER,
                "TEMPORARY DISABLE FLAG"    AS FLAG_TYPE,
                "BLOCK_OTB_REASON"          AS REASON,
                "UPLOAD_DATE_TRIM"          AS SYNC_DATE
            FROM TEMP.PUBLIC.IM_ERP_REGION_SHEETS_MASTER
            WHERE "ITEM CODE" = '{item_code}'
            ORDER BY
                CASE WHEN "TEMPORARY DISABLE FLAG" IS NOT NULL
                     AND LOWER("TEMPORARY DISABLE FLAG") != 'permanent' THEN 0 ELSE 1 END,
                "City", "POD_TIERING"
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        return pd.DataFrame()


def _fetch_pods_live(conn, spin_id):
    """Fetch pod-level state for a SPIN: every ACTIVE pod in the system,
    plus whether the SKU is mapped there + WH/pod inventory in good bin.

    Returns columns:
      CITY, TIER, POD_ID, POD_STATUS,
      SKU_ID (NULL if missing), SKU_ID_MISSING (0/1), SKU_STATE,
      POD_INVENTORY_GOOD, WH_INVENTORY_GOOD
    """
    if not conn:
        return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            WITH active_pods AS (
                SELECT
                    b.city         AS city,
                    b.store_id     AS pod_id,
                    s.tier         AS tier,
                    s.active       AS active
                FROM swiggykms.swiggy_kms.stores s
                JOIN analytics.public.sumanth_anobis_storedetails b
                    ON b.store_id = s.id
                WHERE s.active = 1
                  AND b.store_id != 3141
            ),
            spin_skus AS (
                SELECT
                    TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) AS pod_id,
                    skus.id             AS sku_id,
                    skus.state          AS sku_state,
                    skus.hashkey        AS sku_hashkey,
                    skus.storeid        AS sku_storeid
                FROM cms.cms_ddb.cms_skus skus
                WHERE skus.spinid = '{spin_id}'
            )
            SELECT
                ap.city                                          AS CITY,
                ap.tier                                          AS TIER,
                ap.pod_id                                        AS POD_ID,
                'Active'                                         AS POD_STATUS,
                s.sku_id                                         AS SKU_ID,
                CASE WHEN s.sku_id IS NULL THEN 1 ELSE 0 END     AS SKU_ID_MISSING,
                s.sku_state                                      AS SKU_STATE,
                COALESCE(i.good, 0)                              AS POD_INVENTORY_GOOD,
                COALESCE(i.sellable, 0)                          AS POD_INVENTORY_SELLABLE,
                COALESCE(w.good, 0)                              AS WH_INVENTORY_GOOD
            FROM active_pods ap
            LEFT JOIN spin_skus s
                ON ap.pod_id = s.pod_id
            LEFT JOIN DASH_ERP_ENGG.DASH_ERP_ENGG_DDB.DASH_SCM_INVENTORY_AVAILABILITY i
                ON s.sku_hashkey = i.sku
               AND ap.pod_id = TRY_TO_NUMBER(SPLIT_PART(i.store_id,'#',2))
            LEFT JOIN DASH_ERP_ENGG.DASH_ERP_ENGG_DDB.DASH_SCM_WH_INVENTORY w
                ON s.sku_hashkey = w.sku
               AND ap.pod_id = TRY_TO_NUMBER(SPLIT_PART(w.store_id,'#',2))
            ORDER BY ap.city, ap.pod_id
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        # Fallback to the simpler query that existed before — at least shows
        # the pods where the SKU IS mapped (missing-check won't work though).
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    b.city                                                AS CITY,
                    s2.tier                                               AS TIER,
                    b.store_id                                            AS POD_ID,
                    skus.id                                               AS SKU_ID,
                    0                                                     AS SKU_ID_MISSING,
                    skus.state                                            AS SKU_STATE,
                    CASE WHEN s2.active = 1 THEN 'Active' ELSE 'Inactive' END AS POD_STATUS,
                    COALESCE(i.sellable, 0)                               AS POD_INVENTORY_SELLABLE
                FROM cms.cms_ddb.cms_skus skus
                JOIN analytics.public.sumanth_anobis_storedetails b
                    ON TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) = b.store_id
                LEFT JOIN swiggykms.swiggy_kms.stores s2
                    ON b.store_id = s2.id
                LEFT JOIN DASH_ERP_ENGG.DASH_ERP_ENGG_DDB.DASH_SCM_INVENTORY_AVAILABILITY i
                    ON skus.hashkey = i.sku AND TRY_TO_NUMBER(SPLIT_PART(skus.storeid,'#',2)) = i.store_id
                WHERE skus.spinid = '{spin_id}'
                  AND b.store_id != 3141
                ORDER BY b.city, b.store_id
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)
        except Exception:
            return pd.DataFrame()


def _fetch_erp_audit_live(item_code=None, spin_id=None, limit=50):
    """Live Databricks query for ERP assortment change audit trail.

    Source: prod.cdc_ddb.pre_made_catalog_assortment_audit
    Shows who requested recent changes + source + before/after state.

    Returns empty DataFrame if Databricks conn fails (cloud mode).
    """
    try:
        from fetch_databricks import get_databricks_connection, run_query
        db_conn = get_databricks_connection("analyst")
    except Exception:
        return pd.DataFrame()
    if not db_conn:
        return pd.DataFrame()

    # Build WHERE clause — support filtering by item_code OR spin_id
    filters = []
    if item_code:
        filters.append(f"(item_code = '{item_code}' OR CAST(item_code AS STRING) = '{item_code}')")
    if spin_id:
        filters.append(f"spin_id = '{spin_id}'")
    where_cl = f"WHERE {' OR '.join(filters)}" if filters else ""

    try:
        df = run_query(db_conn, f"""
            SELECT *
            FROM prod.cdc_ddb.pre_made_catalog_assortment_audit
            {where_cl}
            ORDER BY COALESCE(updated_at, created_at, request_time) DESC NULLS LAST
            LIMIT {int(limit)}
        """, "ERP audit trail")
        return df
    except Exception:
        # Fallback: try a naïve ORDER BY if the audit column names differ
        try:
            df = run_query(db_conn, f"""
                SELECT * FROM prod.cdc_ddb.pre_made_catalog_assortment_audit
                {where_cl}
                LIMIT {int(limit)}
            """, "ERP audit (no sort)")
            return df
        except Exception:
            return pd.DataFrame()


def render_drill_panel(drill_query, fs):
    """Cross-view drilldown: SPIN / City / Pod raw detail for any entered code.

    Appears at the top of every view when sidebar's 'Drill to SPIN' is set.
    Three sub-sections:
      \u2022 SPIN Summary  \u2013 identity + quick stats
      \u2022 City level     \u2013 erp_intended_city_tier + erp_block_otb_detail for this item
      \u2022 Pod level      \u2013 live cms_skus query (or 'connect required' if cloud)
    """
    if not drill_query:
        return

    r = resolve_spin_search(drill_query)
    if r is None:
        st.warning(f"Drill: no match for **{drill_query}**. Tip: use the exact SPIN ID or 6-digit Item Code.")
        return

    spin_id   = r["spin_id"]
    item_code = r["item_code"]
    name      = r.get("product_name", "")

    with st.expander(
        f"🔎  Drill — {name}  ·  SPIN {spin_id}  ·  Item {item_code}",
        expanded=True,
    ):
        tabs = st.tabs(["SPIN Summary", "City Level", "Pod Level (live)", "Change History (audit)"])

        # ── SPIN level ─────────────────────────────────────────────────────
        with tabs[0]:
            st.markdown(
                f"**{name}**  \n"
                f"L1: `{r.get('l1','')}` · L2: `{r.get('l2','')}` · Brand: `{r.get('brand','')}`  \n"
                f"Images: `{r.get('image_count',0)}/7` · "
                f"Rating 30d: `{r.get('rating_30d') or '—'}` "
                f"({r.get('review_count',0):,} reviews) · "
                f"Active pods: `{r.get('active_pods',0):,}` · "
                f"Enabled pods: `{r.get('enabled_pods',0):,}`"
            )
            if r.get("is_blocked"):
                st.error(f"Blocked: {', '.join(r.get('block_reasons', []) or ['flag'])}")
            elif r.get("procurement_enabled"):
                st.success(f"Open for procurement in {r.get('cities_open',0)}/{r.get('cities_in_erp',0)} cities")
            else:
                st.info("Not currently in ERP")

        # ── City level ────────────────────────────────────────────────────
        with tabs[1]:
            # Live per-city ERP detail (preferred — has actual city names)
            conn_live = get_snowflake_connection()
            if conn_live:
                with st.spinner(f"Fetching per-city ERP for {item_code}..."):
                    df_city_live = _fetch_city_erp_live(conn_live, item_code)
                if not df_city_live.empty:
                    total_cities = df_city_live["CITY"].nunique()
                    blocked = df_city_live[
                        df_city_live["FLAG_TYPE"].fillna("").astype(str).str.lower().isin(
                            ["temp disable", "otb block", "block"])
                    ]
                    otb_blocked = df_city_live[
                        df_city_live["FLAG_TYPE"].fillna("").astype(str).str.lower().str.contains("otb")
                    ]["CITY"].nunique()
                    temp_disabled = df_city_live[
                        df_city_live["FLAG_TYPE"].fillna("").astype(str).str.lower().str.contains("temp")
                    ]["CITY"].nunique()
                    perm_cities = df_city_live[
                        df_city_live["FLAG_TYPE"].fillna("").astype(str).str.lower().eq("permanent")
                    ]["CITY"].nunique()

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Cities (total ERP rows)", f"{total_cities:,}")
                    m2.metric("OTB Blocked", f"{otb_blocked:,}")
                    m3.metric("Temp Disabled", f"{temp_disabled:,}")
                    m4.metric("Permanent (live)", f"{perm_cities:,}")

                    if not blocked.empty:
                        st.markdown(f"**🔴 Blocked / Temp-disabled cities — {len(blocked)} rows**")
                        _view = blocked.copy().reset_index(drop=True)
                        _view["Severity"] = _view["FLAG_TYPE"].astype(str).str.lower().map(
                            lambda v: "critical" if "otb" in v
                            else ("warn" if "temp" in v else "info")
                        )
                        show_table(_view, key="drill_city_live_block", height=280,
                                   severity_col="Severity")
                        st.download_button(
                            "Download blocked cities CSV",
                            blocked.to_csv(index=False),
                            f"drill_{item_code}_blocked_cities.csv",
                            "text/csv", key="drill_city_live_dl",
                        )
                    else:
                        st.success("No OTB Block / Temp Disable flags in any city.")

                    with st.expander(f"Full per-city ERP ({len(df_city_live)} rows — all flags incl. Permanent)"):
                        show_table(df_city_live.reset_index(drop=True), key="drill_city_live_full",
                                   height=360)
                else:
                    st.info(f"No ERP rows found for item {item_code}.")
                st.markdown("---")

            col_a, col_b = st.columns(2)

            # ERP intended tiers by city
            try:
                df_int = C("erp_intended_city_tier")
                if not df_int.empty:
                    m = df_int[df_int["ITEM_CODE"].astype(str) == item_code]
                    if not m.empty:
                        col_a.markdown(f"**ERP intended — {len(m)} city rows**")
                        show_table(m.reset_index(drop=True), key="drill_city_erp", height=320)
                    else:
                        col_a.info("No ERP intended tiers found for this item.")
                else:
                    col_a.info("erp_intended_city_tier cache not available (excluded from cloud).")
            except Exception as e:
                col_a.warning(f"ERP intended load failed: {e}")

            # Block OTB / Temp Disable detail
            try:
                df_blk = C("erp_block_otb_detail")
                if not df_blk.empty:
                    blk_col = "ITEM CODE" if "ITEM CODE" in df_blk.columns else "ITEM_CODE"
                    m = df_blk[df_blk[blk_col].astype(str) == item_code]
                    if not m.empty:
                        # Exclude "Permanent" (=active) from block summary but still show it
                        real_blocks = m[~m["FLAG_TYPE"].astype(str).str.lower().eq("permanent")]
                        if not real_blocks.empty:
                            col_b.markdown(f"**🔴 Active blocks — {len(real_blocks)} flag type(s)**")
                            total_cities = int(real_blocks["CITIES_AFFECTED"].sum()) if "CITIES_AFFECTED" in real_blocks.columns else 0
                            col_b.caption(f"Total cities affected: {total_cities:,}")
                        else:
                            col_b.success("No active blocks on this item (only Permanent = live).")

                        col_b.markdown(f"**All flag rows (incl. Permanent) — {len(m)}**")
                        _view = m.copy().reset_index(drop=True)
                        if "FLAG_TYPE" in _view.columns:
                            _view["Severity"] = _view["FLAG_TYPE"].map(
                                lambda v: "critical" if "otb" in str(v).lower()
                                else ("warn" if "temp" in str(v).lower() else "info")
                            )
                        show_table(_view, key="drill_city_block", height=260,
                                   severity_col="Severity" if "Severity" in _view.columns else None)
                        col_b.caption(
                            "⚠ Per-city breakdown of blocks isn't in the cache — only CITIES_AFFECTED count. "
                            "Pod-level tab (next) will show exact cities via live Snowflake."
                        )
                    else:
                        col_b.success("No block / temp disable flags on this item.")
            except Exception as e:
                col_b.warning(f"Block detail load failed: {e}")

            # Upgrade Secondary / P999 per-city detail (if available)
            try:
                df_city_sec = C("upgrade_city_secondary_p999")
                if not df_city_sec.empty:
                    col_city = next((c for c in ("SPIN_ID","SPIN","spin_id") if c in df_city_sec.columns), None)
                    if col_city:
                        m = df_city_sec[df_city_sec[col_city].astype(str) == spin_id]
                        if not m.empty:
                            st.markdown(f"**Secondary Pod / P999 — per city ({len(m)} rows)**")
                            show_table(m.reset_index(drop=True), key="drill_city_sec", height=260)
            except Exception:
                pass

        # ── Pod level (live Snowflake) ────────────────────────────────────
        with tabs[2]:
            conn = get_snowflake_connection()
            if not conn:
                st.info("Pod-level detail requires a live Snowflake connection (local only). "
                        "Click into SPIN Lookup first to establish the connection, or run locally.")
            else:
                with st.spinner(f"Fetching pod-level detail for {spin_id}..."):
                    df_pods = _fetch_pods_live(conn, spin_id)
                if df_pods.empty:
                    st.info("No pod rows found for this SPIN.")
                else:
                    # Headline metrics including SKU-ID-missing flag
                    n_pods         = int(df_pods["POD_ID"].nunique())
                    n_cities       = int(df_pods["CITY"].nunique())
                    if "SKU_ID_MISSING" in df_pods.columns:
                        n_sku_missing  = int(pd.to_numeric(df_pods["SKU_ID_MISSING"], errors="coerce").fillna(0).sum())
                    else:
                        n_sku_missing = 0
                    n_enabled = int((df_pods.get("SKU_STATE", pd.Series()).astype(str).str.upper() == "ENABLED").sum())
                    pod_inv_col = "POD_INVENTORY_GOOD" if "POD_INVENTORY_GOOD" in df_pods.columns else (
                        "POD_INVENTORY_SELLABLE" if "POD_INVENTORY_SELLABLE" in df_pods.columns else "INVENTORY"
                    )
                    wh_inv_col  = "WH_INVENTORY_GOOD" if "WH_INVENTORY_GOOD" in df_pods.columns else None
                    n_inventory = int((pd.to_numeric(df_pods.get(pod_inv_col, 0), errors="coerce") > 0).sum())

                    m1, m2, m3, m4, m5 = st.columns(5)
                    m1.metric("Active pods total", f"{n_pods:,}")
                    m2.metric("Cities",            f"{n_cities:,}")
                    m3.metric("SKU state = ENABLED", f"{n_enabled:,}")
                    m4.metric("SKU ID missing",    f"{n_sku_missing:,}",
                              help="Pods that are ACTIVE but have no SKU mapping in cms_skus for this SPIN")
                    m5.metric("With good-bin inv", f"{n_inventory:,}")

                    # Surface the missing-pod list prominently
                    if n_sku_missing > 0 and "SKU_ID_MISSING" in df_pods.columns:
                        st.error(f"⚠ {n_sku_missing:,} active pods do NOT have this SKU mapped — SKU ID missing.")
                        miss = df_pods[df_pods["SKU_ID_MISSING"] == 1][["CITY", "TIER", "POD_ID"]].reset_index(drop=True)
                        with st.expander(f"Pods missing the SKU ID ({len(miss)})"):
                            show_table(miss, key="drill_pod_sku_missing", height=240)
                            st.download_button(
                                "Download pods-missing-SKU CSV",
                                miss.to_csv(index=False),
                                f"drill_{spin_id}_pods_sku_missing.csv",
                                "text/csv", key="drill_pod_miss_dl",
                            )

                    # Severity per row: enabled + good-bin inv = good;
                    # enabled no inv = warn; SKU missing = critical; else = muted
                    df_view = df_pods.copy()
                    def _sev(row):
                        if int(pd.to_numeric(pd.Series([row.get("SKU_ID_MISSING", 0)]),
                                             errors="coerce").fillna(0).iloc[0]) == 1:
                            return "critical"
                        st_ok = str(row.get("SKU_STATE","")).upper() == "ENABLED"
                        inv = pd.to_numeric(pd.Series([row.get(pod_inv_col, 0)]), errors="coerce").fillna(0).iloc[0]
                        if st_ok and inv > 0: return "good"
                        if st_ok: return "warn"
                        return "muted"
                    df_view["Severity"] = df_view.apply(_sev, axis=1)
                    show_table(df_view.reset_index(drop=True), key="drill_pod_live", height=440,
                               severity_col="Severity")

                    st.download_button(
                        "Download full pod-level CSV (line item)",
                        df_pods.to_csv(index=False),
                        f"drill_{spin_id}_pods_full.csv",
                        "text/csv",
                        key="drill_pod_dl",
                    )

        # ── Change History (Databricks audit trail) ───────────────────────
        with tabs[3]:
            st.caption(
                "Recent ERP assortment changes from "
                "`prod.cdc_ddb.pre_made_catalog_assortment_audit` — "
                "who requested, source, and before/after."
            )
            with st.spinner(f"Fetching change audit for item {item_code}..."):
                df_audit = _fetch_erp_audit_live(item_code=item_code, spin_id=spin_id)
            if df_audit.empty:
                st.info("No audit rows found (or Databricks connection unavailable).")
            else:
                st.markdown(f"**Last {len(df_audit)} changes**")
                show_table(df_audit.reset_index(drop=True), key="drill_audit", height=440)
                st.download_button(
                    "Download change audit CSV",
                    df_audit.to_csv(index=False),
                    f"drill_{item_code}_erp_audit.csv",
                    "text/csv", key="drill_audit_dl",
                )


def _fetch_spin_attrs_live(conn, spin_id, keys=("quantity", "unit_of_measure", "uom",
                                                  "pack_size", "net_weight", "volume",
                                                  "net_quantity", "weight",
                                                  "secondary_pod_enabled", "p999_availability",
                                                  "tertiary_pod_enabled")):
    """Fetch specific attribute values from the CMS attributes JSON.

    Returns a dict of {key: value} limited to the keys we actually need.
    Used to populate quantity + UoM on the SPIN hero.
    """
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        # Pivot the JSON attributes table so each requested key becomes a column
        aggs = ",\n                ".join(
            f"MAX(CASE WHEN LOWER(f.key::string) = '{k.lower()}' THEN f.value::string END) AS {k.upper()}"
            for k in keys
        )
        cur.execute(f"""
            SELECT {aggs}
            FROM cms.cms_ddb.cms_spins_1,
            LATERAL FLATTEN(input => parse_json(cast(attributes as string))) f
            WHERE hashkey = '{spin_id}'
              AND SORTKEY = 'SPIN'
              AND LOWER(Businessline) = 'instamart'
        """)
        row = cur.fetchone()
        if not row:
            return {}
        cols = [d[0] for d in cur.description]
        data = {c: (v if v not in (None, "", "null", "NULL") else None) for c, v in zip(cols, row)}
        # Compose a single quantity_uom display string
        qty = (
            data.get("QUANTITY")
            or data.get("NET_QUANTITY")
            or data.get("PACK_SIZE")
            or data.get("NET_WEIGHT")
            or data.get("VOLUME")
            or data.get("WEIGHT")
        )
        uom = data.get("UNIT_OF_MEASURE") or data.get("UOM")
        display = None
        if qty and uom:
            display = f"{qty} {uom}".strip()
        elif qty:
            display = str(qty).strip()

        def _boolish(v):
            if v is None: return None
            s = str(v).strip().lower()
            if s in ("true", "1", "yes", "y", "enabled", "on"): return True
            if s in ("false", "0", "no", "n", "disabled", "off"): return False
            return None

        return {
            "quantity":       qty,
            "uom":            uom,
            "quantity_uom":   display,
            "secondary_pod_enabled": _boolish(data.get("SECONDARY_POD_ENABLED")),
            "p999_availability":     _boolish(data.get("P999_AVAILABILITY")),
            "tertiary_pod_enabled":  _boolish(data.get("TERTIARY_POD_ENABLED")),
            "cms_attrs_live": True,
        }
    except Exception as e:
        return {"_attrs_error": str(e)}


def _fetch_spin_rating_live(conn, spin_id, window_days=30):
    """Live Snowflake query for SPIN rating + review count.

    Source: analytics.public.im_spin_order_ratings_ss (same table fetch_ratings.py
    uses nightly). Running it live here gives fresh values for SPIN Lookup hero.
    """
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT ORDER_ID)                                                AS orders,
                ROUND(AVG(RATING), 2)                                                   AS avg_rating,
                COUNT(DISTINCT CASE WHEN RATING <= 2 THEN ORDER_ID END)                 AS low,
                COUNT(DISTINCT CASE WHEN RATING >= 4 THEN ORDER_ID END)                 AS high
            FROM analytics.public.im_spin_order_ratings_ss
            WHERE SPIN_ID = '{spin_id}'
              AND spin_rating_given_dt >= DATEADD(day, -{int(window_days)}, CURRENT_DATE())
              AND spin_rating_given_dt <= CURRENT_DATE - 1
              AND ORDER_ID IS NOT NULL
        """)
        row = cur.fetchone()
        if not row:
            return {}
        orders = int(row[0] or 0)
        avg = float(row[1]) if row[1] is not None else None
        return {
            "rating_30d":         avg,
            "review_count":       orders,
            "low_rating_orders":  int(row[2] or 0),
            "high_rating_orders": int(row[3] or 0),
            "_rating_live":       True,
        }
    except Exception as e:
        return {"_rating_error": str(e)}


def _fetch_spin_hero_live(conn, spin_id):
    """Live Snowflake query for SPIN hero: enablement, active pods, city coverage.

    Returns a dict of fresh values (LIVE) that overrides the cached ones.
    Safe to call with conn=None — returns empty dict.
    """
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT
                COUNT(DISTINCT CASE WHEN skus.state = 'ENABLED' AND s2.active = 1
                    THEN b.store_id END)                                   AS enabled_pods,
                COUNT(DISTINCT CASE WHEN s2.active = 1 THEN b.store_id END) AS active_pods,
                COUNT(DISTINCT CASE WHEN skus.state = 'ENABLED' AND s2.active = 1
                    THEN b.city END)                                        AS cities_live,
                COUNT(DISTINCT b.city)                                      AS cities_total,
                (SELECT COUNT(DISTINCT s3.id) FROM swiggykms.swiggy_kms.stores s3
                    WHERE s3.active = 1 AND s3.id != 3141)                 AS total_system_pods
            FROM cms.cms_ddb.cms_skus skus
            JOIN analytics.public.sumanth_anobis_storedetails b
                ON TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) = b.store_id
            LEFT JOIN swiggykms.swiggy_kms.stores s2
                ON b.store_id = s2.id
            WHERE skus.spinid = '{spin_id}'
              AND b.store_id != 3141
        """)
        row = cur.fetchone()
        if not row:
            return {}
        cols = [d[0] for d in cur.description]
        data = dict(zip(cols, row))
        enabled = int(data.get("ENABLED_PODS", 0) or 0)
        active = int(data.get("ACTIVE_PODS", 0) or 0)
        return {
            "is_enabled":   enabled > 0,
            "active_pods":  active,
            "enabled_pods": enabled,
            "cities_live":  int(data.get("CITIES_LIVE", 0) or 0),
            "cities_total": int(data.get("CITIES_TOTAL", 0) or 0),
            "total_pods":   int(data.get("TOTAL_SYSTEM_PODS", 1218) or 1218),
            "enable_pct":   (enabled / active * 100) if active else 0,
            "_live":        True,
        }
    except Exception as e:
        return {"_live_error": str(e)}


def _fetch_spin_images(conn, spin_id):
    """Fetch images for a SPIN from Snowflake. Returns empty DF on any error
    (warehouse suspended, network timeout, etc.) so the caller falls back."""
    if not conn:
        return pd.DataFrame()
    try:
        cur = _with_warehouse_resume(conn, f"""
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
    except Exception:
        return pd.DataFrame()


def _fetch_spin_attributes(conn, spin_id):
    """Fetch all CMS attributes for a SPIN from Snowflake.
    Returns empty DF on any error so the caller falls back gracefully."""
    if not conn:
        return pd.DataFrame()
    try:
        cur = _with_warehouse_resume(conn, f"""
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
    except Exception:
        return pd.DataFrame()


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

        # ── Secondary Pod & Multi-Pod Status ──
        st.markdown("---")
        st.markdown("#### Secondary Pod & Multi-Pod Status")

        sec_pod = "Unknown"
        dsd_wh = "Unknown"
        if not df_attr.empty:
            sec_row = df_attr[df_attr["Attribute"] == "secondary_pod_enabled"]
            if not sec_row.empty:
                sec_pod = sec_row.iloc[0]["Value"]
            dsd_row = df_attr[df_attr["Attribute"] == "dsd_wh_crossdock"]
            if not dsd_row.empty:
                dsd_wh = dsd_row.iloc[0]["Value"]

        cp1, cp2 = st.columns(2)
        cp1.metric("Secondary Pod Enabled", sec_pod)
        cp2.metric("DSD / WH / Cross-Dock", dsd_wh)

        # City-level expansion detail
        with st.expander("City-Level Pod Expansion Detail"):
            st.caption("Which cities have secondary pod / multi-pod for this item")
            try:
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT
                        b.city,
                        b.store_id AS pod_id,
                        skus.state AS sku_state,
                        i.sellable AS inventory,
                        CASE WHEN b.store_id IN (
                            SELECT DISTINCT s2.id FROM swiggykms.swiggy_kms.stores s2
                            WHERE s2.active = 1
                        ) THEN 'Active' ELSE 'Inactive' END AS pod_status
                    FROM cms.cms_ddb.cms_skus AS skus
                    JOIN analytics.public.sumanth_anobis_storedetails b
                        ON TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) = b.store_id
                    JOIN DASH_ERP_ENGG.DASH_ERP_ENGG_DDB.DASH_SCM_INVENTORY_AVAILABILITY i
                        ON skus.hashkey = i.sku
                    WHERE skus.spinid = '{spin_id}'
                      AND b.store_id != 3141
                    ORDER BY b.city, b.store_id
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df_pods_detail = pd.DataFrame(rows, columns=cols)
                if not df_pods_detail.empty:
                    # Show city summary: pods per city, enabled count
                    city_summary = df_pods_detail.groupby("CITY").agg(
                        Total_Pods=("POD_ID", "nunique"),
                        Enabled=("SKU_STATE", lambda x: (x.str.upper() == "ENABLED").sum()),
                        With_Inventory=("INVENTORY", lambda x: (x > 0).sum()),
                    ).reset_index()
                    city_summary["Multi-Pod"] = city_summary["Total_Pods"].apply(
                        lambda x: "Yes" if x > 1 else "No")
                    city_summary = city_summary.sort_values("Total_Pods", ascending=False)
                    show_table(city_summary, key="spin_city_pods", height=400)
                else:
                    st.info("No pod data found.")
            except Exception as e:
                st.warning(f"Could not fetch pod detail: {e}")

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

    # ── Block OTB / Temp Disable × City ─────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔴 Block OTB / Temp Disable × City")

    # Prefer live query (has actual city names); fallback to df_item which may
    # already carry DISABLE_FLAG if fetched live via the snowflake path above.
    block_df = pd.DataFrame()
    if conn:
        with st.spinner("Fetching per-city block flags live..."):
            block_df = _fetch_city_erp_live(conn, item_code)

    if block_df.empty and "DISABLE_FLAG" in df_item.columns:
        # Use the live-fallback dataframe if it already has disable flags
        block_df = df_item.rename(columns={"DISABLE_FLAG": "FLAG_TYPE"})
        block_df["REASON"] = ""

    if block_df.empty:
        # Final fallback — use cached erp_block_otb_detail (aggregate only)
        df_blk = C("erp_block_otb_detail")
        if not df_blk.empty:
            blk_col = "ITEM CODE" if "ITEM CODE" in df_blk.columns else "ITEM_CODE"
            m = df_blk[df_blk[blk_col].astype(str) == str(item_code)]
            if not m.empty:
                real = m[~m["FLAG_TYPE"].astype(str).str.lower().eq("permanent")]
                if not real.empty:
                    st.warning(
                        f"Found {len(real)} active block flag(s) in cache "
                        f"(aggregate counts only — live connection needed for exact cities)."
                    )
                    show_table(real.reset_index(drop=True), key="spin_erp_blocks_cache", height=260)
                else:
                    st.success("No active block / temp disable flags.")
            else:
                st.success("No block / temp disable records for this item.")
        else:
            st.info("Block detail cache not loaded. Connect to Snowflake for live data.")
    else:
        # Count flags by type
        flags_lower = block_df["FLAG_TYPE"].fillna("").astype(str).str.lower()
        mask_active = ~flags_lower.eq("permanent") & flags_lower.ne("")
        active_blocks = block_df[mask_active]
        otb_cities  = int(flags_lower.str.contains("otb")[mask_active].sum())
        temp_cities = int(flags_lower.str.contains("temp")[mask_active].sum())
        perm_cities = int(flags_lower.eq("permanent").sum())
        total_rows  = len(block_df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total ERP rows", f"{total_rows:,}")
        m2.metric("OTB Blocked cities", f"{otb_cities:,}")
        m3.metric("Temp Disabled cities", f"{temp_cities:,}")
        m4.metric("Permanent (live) cities", f"{perm_cities:,}")

        if not active_blocks.empty:
            st.markdown(f"**🔴 Blocked / Temp-disabled — {len(active_blocks)} cities**")
            view = active_blocks.copy().reset_index(drop=True)
            view["Severity"] = view["FLAG_TYPE"].astype(str).str.lower().map(
                lambda v: "critical" if "otb" in v
                else ("warn" if "temp" in v else "info")
            )
            show_table(view, key="spin_erp_blocks_live", height=280, severity_col="Severity")
            st.download_button(
                "Download Blocked Cities CSV",
                active_blocks.to_csv(index=False),
                f"spin_erp_blocks_{item_code}.csv",
                "text/csv", key="dl_spin_erp_blocks",
            )
        else:
            st.success("✅ No OTB Block / Temp Disable flags in any city for this item.")

        with st.expander(f"Full per-city ERP incl. Permanent ({total_rows} rows)"):
            show_table(block_df.reset_index(drop=True), key="spin_erp_blocks_full", height=360)
            st.download_button(
                "Download Full Per-City ERP CSV",
                block_df.to_csv(index=False),
                f"spin_erp_full_{item_code}.csv",
                "text/csv", key="dl_spin_erp_full",
            )


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

    # ── Step 2: Get live active pod counts per city × tier from Snowflake ──
    pods_by_city_tier = pd.DataFrame()
    city_name_to_id = {}
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT c.name AS city, s.tier, COUNT(DISTINCT s.id) AS pods
                FROM swiggykms.swiggy_kms.stores s
                JOIN swiggykms.swiggy_kms.area a ON s.area_id = a.id
                JOIN swiggykms.swiggy_kms.city c ON a.city_id = c.id
                WHERE s.active = 1
                GROUP BY c.name, s.tier
                ORDER BY c.name, s.tier
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            pods_by_city_tier = pd.DataFrame(rows, columns=cols)
        except Exception:
            pass

    # Fallback to cached pod_master
    if pods_by_city_tier.empty and not df_pods.empty:
        active = df_pods[df_pods["Active/Non_Active Pod"] == "Active"].copy()
        pods_by_city_tier = active.groupby(["CITY", "TIER"])["STORE_ID"].nunique().reset_index(name="PODS")

    # Normalize column names
    pods_by_city_tier.columns = [c.upper() for c in pods_by_city_tier.columns]

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
                    pod_count += int(match["PODS"].sum())
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
    df_sku_detail = pd.DataFrame()
    if conn:
        with st.spinner("Fetching live SKU enablement from Snowflake..."):
            try:
                cur = conn.cursor()
                # Join with KMS stores to get only ACTIVE pods + tier info
                cur.execute(f"""
                    SELECT
                        b.city,
                        b.store_id,
                        skus.externalid AS item_code,
                        skus.hashkey AS sku_id,
                        sp.hashkey AS spin_id,
                        skus.state AS enable_state,
                        i.sellable AS inventory,
                        s.tier AS pod_tier
                    FROM cms.cms_ddb.cms_skus AS skus
                    JOIN analytics.public.sumanth_anobis_storedetails b
                        ON TRY_TO_NUMBER(SPLIT_PART(skus.storeid, '#', 2)) = b.store_id
                    JOIN DASH_ERP_ENGG.DASH_ERP_ENGG_DDB.DASH_SCM_INVENTORY_AVAILABILITY i
                        ON skus.hashkey = i.sku
                    JOIN cms.cms_ddb.cms_spins_1 AS sp
                        ON skus.spinid = sp.hashkey
                    JOIN swiggykms.swiggy_kms.stores s
                        ON b.store_id = s.id AND s.active = 1
                    WHERE sp.hashkey = '{spin_id}'
                      AND lower(sp.businessline) = 'instamart'
                      AND b.store_id != 3141
                """)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df_sku_detail = pd.DataFrame(rows, columns=cols)
            except Exception as e:
                st.warning(f"Live query failed: {e}")
    else:
        st.info("No live Snowflake connection — enabled pod counts unavailable on cloud.")

    # Compute enabled pods per city from SKU detail
    if not df_sku_detail.empty:
        total_skus = len(df_sku_detail)
        enabled_skus = len(df_sku_detail[df_sku_detail["ENABLE_STATE"].str.upper() == "ENABLED"])
        st.caption(f"Live data: {enabled_skus} enabled / {total_skus} total SKUs across {df_sku_detail['CITY'].nunique()} cities")

        # Count enabled pods (store_ids) per city
        enabled_pods_by_city = df_sku_detail[
            df_sku_detail["ENABLE_STATE"].str.upper() == "ENABLED"
        ].groupby("CITY")["STORE_ID"].nunique().reset_index(name="enabled_pods")

        enabled_map = dict(zip(enabled_pods_by_city["CITY"].str.lower(), enabled_pods_by_city["enabled_pods"]))
        df_intended["enabled_pods"] = df_intended["City"].apply(
            lambda c: int(enabled_map.get(c.lower(), 0)))
    else:
        df_intended["enabled_pods"] = 0

    # ── Step 5: Force disabled / disabled pods from live SKU data ──
    # Count disabled SKUs per city from the live query (more accurate than cached overrides)
    if not df_sku_detail.empty:
        disabled_by_city = df_sku_detail[
            df_sku_detail["ENABLE_STATE"].str.upper() != "ENABLED"
        ].groupby("CITY")["STORE_ID"].nunique().reset_index(name="disabled_pods")
        disabled_map = dict(zip(disabled_by_city["CITY"].str.lower(), disabled_by_city["disabled_pods"]))
        df_intended["force_disabled"] = df_intended["City"].apply(
            lambda c: int(disabled_map.get(c.lower(), 0)))
    else:
        # Fallback to cached overrides
        df_overrides = C("assortment_overrides")
        if not df_overrides.empty:
            spin_ov = df_overrides[df_overrides["spin_id"] == spin_id]
            fd = spin_ov[spin_ov["assortment_override_state"] == "STATE_FORCE_DISABLED"]
            if not fd.empty:
                fd = fd.copy()
                fd["city_id_str"] = fd["city_id"].apply(_norm_city)
                fd_ct = fd.groupby("city_id_str").size().reset_index(name="force_disabled")
                if not city_map.empty:
                    fd_ct = fd_ct.merge(city_map, left_on="city_id_str", right_on="CITY_ID_STR", how="left")
                    fd_map = dict(zip(fd_ct["CITY"].str.lower(), fd_ct["force_disabled"]))
                    df_intended["force_disabled"] = df_intended["City"].apply(
                        lambda c: int(fd_map.get(c.lower(), 0)))
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
    # If we never had a live Snowflake hit, enabled/fd are guaranteed-zero
    # placeholders, not real zeros. Show '—' so user doesn't read it as truth.
    no_live = df_sku_detail.empty
    enabled_disp = "—" if no_live else f"{total_enabled:,}"
    enable_pct_disp = "—" if no_live else f"{overall_enable}%"
    fd_disp = "—" if no_live else f"{int(total_fd):,}"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Intended Pods", f"{total_intended:,}")
    c2.metric("Enabled Pods", enabled_disp,
              help="Live-Snowflake required; cache only has total intended count")
    c3.metric("Enable %", enable_pct_disp,
              help="Computed from Enabled / Intended live data")
    c4.metric("Force Disabled", fd_disp,
              help="Live-Snowflake required for accurate count")
    if no_live:
        st.caption("⚠ Enabled / % / Force Disabled need live Snowflake — "
                   "open this SPIN on local (corp VPN) to see real numbers.")

    st.markdown("#### City × Tier Enablement")
    show_table(df_display, key="spin_storefront", height=500)

    # SKU-level pod detail (from live query)
    if not df_sku_detail.empty:
        with st.expander(f"Pod-Level SKU Detail ({len(df_sku_detail)} SKUs — Active Pods Only)"):
            det_cols = ["CITY", "STORE_ID", "SKU_ID", "ENABLE_STATE", "INVENTORY"]
            if "POD_TIER" in df_sku_detail.columns:
                det_cols.insert(2, "POD_TIER")
            sku_disp = df_sku_detail[det_cols].copy()
            rename_map = {
                "CITY": "City", "STORE_ID": "Pod ID", "SKU_ID": "SKU ID",
                "ENABLE_STATE": "State", "INVENTORY": "Inventory", "POD_TIER": "Tier"
            }
            sku_disp = sku_disp.rename(columns=rename_map)
            sku_disp = sku_disp.sort_values(["City", "State"], ascending=[True, False])
            show_table(sku_disp, key="spin_sku_detail", height=500)
            st.download_button("Download SKU Detail", sku_disp.to_csv(index=False),
                              f"sku_detail_{spin_id}.csv", "text/csv", key="dl_sku_det")

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


def render_spin_logs(result):
    """Logs tab — SPIN edit history from cms.cms_spins_1_audit (Databricks).
    Shows every edit with who changed it, when, and what fields changed (diff)."""
    spin_id = result["spin_id"]

    st.markdown("#### SPIN Change History")
    st.caption(f"Every edit to SPIN {spin_id} — who changed what, when")

    try:
        import sys, json as _json
        sys.path.insert(0, str(BASE_DIR))
        from fetch_databricks import get_databricks_connection, run_query as db_run_query

        db_conn = get_databricks_connection("analyst")

        with st.spinner("Fetching SPIN audit history from Databricks..."):
            df_audit = db_run_query(db_conn, f"""
                SELECT
                    hashKey AS spin_id,
                    updatedAt AS updated_at,
                    updatedBy AS updated_by,
                    state,
                    attributes,
                    images
                FROM cms.cms_spins_1_audit
                WHERE hashKey = '{spin_id}'
                ORDER BY updatedAt DESC
                LIMIT 50
            """, f"SPIN audit for {spin_id}")

        if df_audit.empty:
            st.info(f"No edit history found for SPIN {spin_id}.")
            return

        # Summary
        total_edits = len(df_audit)
        editors = df_audit["updated_by"].nunique()
        latest_edit = df_audit["updated_at"].iloc[0] if not df_audit.empty else "N/A"
        latest_editor = df_audit["updated_by"].iloc[0] if not df_audit.empty else "N/A"

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Edits", total_edits)
        c2.metric("Editors", editors)
        c3.metric("Last Edit", str(latest_edit)[:19])
        st.caption(f"Last edited by: **{latest_editor}**")

        # Diff: compare consecutive snapshots to find what changed
        st.markdown("---")
        st.markdown("#### Field Changes (Diff)")
        st.caption("Comparing consecutive edits to show what fields were modified")

        changes = []
        for i in range(len(df_audit) - 1):
            current = df_audit.iloc[i]
            previous = df_audit.iloc[i + 1]

            try:
                curr_attrs = _json.loads(current["attributes"]) if current["attributes"] else {}
                prev_attrs = _json.loads(previous["attributes"]) if previous["attributes"] else {}
            except Exception:
                continue

            # Find changed fields
            all_keys = set(list(curr_attrs.keys()) + list(prev_attrs.keys()))
            for key in all_keys:
                curr_val = str(curr_attrs.get(key, ""))
                prev_val = str(prev_attrs.get(key, ""))
                if curr_val != prev_val:
                    changes.append({
                        "Timestamp": str(current["updated_at"])[:19],
                        "Edited By": str(current["updated_by"]),
                        "Field": key,
                        "Old Value": prev_val[:100] if prev_val else "(empty)",
                        "New Value": curr_val[:100] if curr_val else "(empty)",
                    })

            # Check image changes
            try:
                curr_img = str(current.get("images", ""))
                prev_img = str(previous.get("images", ""))
                if curr_img != prev_img:
                    changes.append({
                        "Timestamp": str(current["updated_at"])[:19],
                        "Edited By": str(current["updated_by"]),
                        "Field": "images",
                        "Old Value": f"({len(_json.loads(prev_img)) if prev_img and prev_img != 'None' else 0} images)",
                        "New Value": f"({len(_json.loads(curr_img)) if curr_img and curr_img != 'None' else 0} images)",
                    })
            except Exception:
                pass

        if changes:
            df_changes = pd.DataFrame(changes)
            st.metric("Fields Changed", f"{len(df_changes)}")
            show_table(df_changes, key="spin_diff", height=500)
            st.download_button("Download Change Log", df_changes.to_csv(index=False),
                              f"changelog_{spin_id}.csv", "text/csv", key="dl_spin_changelog")
        else:
            st.info("No field-level changes detected between consecutive edits (or only 1 edit found).")

        # Editor summary
        st.markdown("---")
        st.markdown("#### Editor Summary")
        editor_summary = df_audit.groupby("updated_by").agg(
            Edits=("spin_id", "count"),
            First_Edit=("updated_at", "min"),
            Last_Edit=("updated_at", "max"),
        ).reset_index()
        editor_summary.columns = ["Editor", "Edits", "First Edit", "Last Edit"]
        editor_summary = editor_summary.sort_values("Edits", ascending=False)
        show_table(editor_summary, key="spin_editors")

    except ImportError:
        st.info("Databricks connector not available. Logs require local execution with Databricks access.")
    except Exception as e:
        import traceback as _tb
        st.error(f"Error fetching logs: {e}")
        with st.expander("Show full traceback"):
            st.code(_tb.format_exc())


# ── Upload Preview ───────────────────────────────────────────────────────────

def _get_upload_template_csv():
    """CSV template for upload preview."""
    import io, csv
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["SPIN_ID", "IMAGE_ID", "SHOT_TYPE", "ITEM_NAME"])
    w.writerow(["DSM6DGU3TW", "swiggy/image/upload/abc123", "MN", ""])
    w.writerow(["XYZ789ABC", "", "", "New Product Name Here"])
    return buf.getvalue()


def _fetch_bulk_spin_data(conn, spin_ids):
    """Bulk fetch current product info for list of SPINs."""
    if not conn or not spin_ids:
        return pd.DataFrame()
    # Batch in chunks of 500
    all_rows = []
    for i in range(0, len(spin_ids), 500):
        batch = spin_ids[i:i+500]
        ids_str = ",".join(f"'{s}'" for s in batch)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT hashkey as spin_id,
                   ATTR_PRODUCT_NAME:"value"::string as product_name,
                   ATTR_BRAND:"value"::string as brand,
                   L1CATEGORY as l1, L2CATEGORY as l2,
                   parse_json(THIRDPARTYATTRIBUTES):"id"::string as item_code,
                   ATTR_QUANTITY:"value"::string as quantity,
                   ATTR_UNIT_OF_MEASURE:"value"::string as uom
            FROM cms.cms_ddb.cms_spins_1
            WHERE hashkey IN ({ids_str})
              AND SORTKEY = 'SPIN' AND lower(Businessline) = 'instamart'
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        all_rows.extend(rows)
    if not all_rows:
        return pd.DataFrame()
    return pd.DataFrame(all_rows, columns=[c.upper() for c in cols])


def _fetch_bulk_spin_images(conn, spin_ids):
    """Bulk fetch current images for list of SPINs."""
    if not conn or not spin_ids:
        return pd.DataFrame()
    all_rows = []
    for i in range(0, len(spin_ids), 500):
        batch = spin_ids[i:i+500]
        ids_str = ",".join(f"'{s}'" for s in batch)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT sp.hashkey as spin_id,
                   k.value:image_id::string as image_id,
                   k.value:shot_type::string as shot_type
            FROM cms.cms_ddb.cms_spins_1 sp,
            LATERAL FLATTEN(input => parse_json(cast(sp.images as string))) k
            WHERE sp.hashkey IN ({ids_str})
              AND sp.SORTKEY = 'SPIN' AND lower(sp.Businessline) = 'instamart'
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        all_rows.extend(rows)
    if not all_rows:
        return pd.DataFrame()
    return pd.DataFrame(all_rows, columns=[c.upper() for c in cols])


def _render_preview_card(idx, row, current_data, current_images, conn_available):
    """Render a SPIN preview card: CURRENT on top, PROPOSED below, product info under new images."""
    spin_id = str(row.get("SPIN_ID", "")).strip()
    new_image_id = str(row.get("IMAGE_ID", "")).strip()
    new_shot_type = str(row.get("SHOT_TYPE", "")).strip() or "MN"
    new_item_name = str(row.get("ITEM_NAME", "")).strip()

    has_image_change = new_image_id != "" and new_image_id != "nan"
    has_name_change = new_item_name != "" and new_item_name != "nan"

    # Get current data
    curr_info = {}
    if not current_data.empty:
        match = current_data[current_data["SPIN_ID"] == spin_id]
        if not match.empty:
            r = match.iloc[0]
            curr_info = {
                "product_name": str(r.get("PRODUCT_NAME", "")),
                "brand": str(r.get("BRAND", "")),
                "l1": str(r.get("L1", "")),
                "l2": str(r.get("L2", "")),
                "item_code": str(r.get("ITEM_CODE", "")),
                "quantity": str(r.get("QUANTITY", "")),
                "uom": str(r.get("UOM", "")),
            }

    # Get current images
    curr_imgs = []
    if not current_images.empty:
        spin_imgs = current_images[current_images["SPIN_ID"] == spin_id].copy()
        SHOT_ORDER = {"MN": 0, "BK": 1}
        spin_imgs["_sort"] = spin_imgs["SHOT_TYPE"].apply(
            lambda s: SHOT_ORDER.get(str(s), 10 + int(''.join(filter(str.isdigit, str(s))) or 99))
        )
        spin_imgs = spin_imgs.sort_values("_sort")
        for _, ir in spin_imgs.iterrows():
            iid = str(ir["IMAGE_ID"])
            if iid and iid != "None":
                curr_imgs.append({"url": CDN_BASE + iid, "shot": str(ir["SHOT_TYPE"])})

    # Review status
    review_key = spin_id
    status = st.session_state.get("upload_reviews", {}).get(review_key, "pending")
    border_color = "#28a745" if status == "approved" else "#dc3545" if status == "rejected" else "#555"

    # ── Card ──
    with st.container(border=True):
        current_name = curr_info.get("product_name", "N/A")

        # Header: info + actions in one tight row
        h1, h2, h3, h4, h5 = st.columns([4, 1, 1, 1, 2])
        with h1:
            qty = curr_info.get('quantity', '')
            uom = curr_info.get('uom', '')
            qty_str = f" | {qty} {uom}" if qty and qty != "None" else ""
            st.markdown(f"**{spin_id}** | {curr_info.get('item_code', '')} | {curr_info.get('brand', '')} | {curr_info.get('l1', '')} > {curr_info.get('l2', '')}{qty_str}")
        with h2:
            if st.button("Approve", key=f"approve_{idx}", type="primary"):
                st.session_state["upload_reviews"][review_key] = "approved"
                st.rerun()
        with h3:
            if st.button("Reject", key=f"reject_{idx}"):
                st.session_state["upload_reviews"][review_key] = "rejected"
                st.rerun()
        with h4:
            clr = {"approved": "#28a745", "rejected": "#dc3545", "pending": "#ffc107"}[status]
            st.markdown(f'<span style="background:{clr};color:white;padding:3px 10px;border-radius:10px;font-size:12px;font-weight:bold;">{"APPROVED" if status=="approved" else "REJECTED" if status=="rejected" else "PENDING"}</span>', unsafe_allow_html=True)
        with h5:
            note = st.text_input("", key=f"note_{idx}", label_visibility="collapsed", placeholder="Note")
            if note:
                st.session_state.setdefault("upload_notes", {})[review_key] = note

        # CURRENT: name + images
        st.caption(f"CURRENT — {current_name}")
        if curr_imgs:
            cols = st.columns(min(len(curr_imgs), 7))
            for j, img in enumerate(curr_imgs[:7]):
                with cols[j]:
                    st.image(img["url"], width=85, caption=img["shot"])

        # PROPOSED: name + images (stacked below)
        if has_image_change or has_name_change:
            final_name = new_item_name if has_name_change else current_name
            st.caption(f"PROPOSED — {final_name}")

            if has_image_change:
                new_url = CDN_BASE + new_image_id if not new_image_id.startswith("http") else new_image_id
                new_imgs = []
                slot_replaced = False
                for img in curr_imgs:
                    if img["shot"] == new_shot_type and not slot_replaced:
                        new_imgs.append({"url": new_url, "shot": f"{new_shot_type}*", "is_new": True})
                        slot_replaced = True
                    else:
                        new_imgs.append({"url": img["url"], "shot": img["shot"], "is_new": False})
                if not slot_replaced:
                    new_imgs.insert(0, {"url": new_url, "shot": f"{new_shot_type}*", "is_new": True})

                cols2 = st.columns(min(len(new_imgs), 7))
                for j, img in enumerate(new_imgs[:7]):
                    with cols2[j]:
                        st.image(img["url"], width=85, caption=img["shot"])
            elif curr_imgs:
                cols = st.columns(min(len(curr_imgs), 7))
                for j, img in enumerate(curr_imgs[:7]):
                    with cols[j]:
                        st.image(img["url"], width=85, caption=img["shot"])


def render_upload_preview():
    """Upload Preview — preview CMS image/name changes before production upload."""
    st.markdown(page_header(
        "Upload Preview",
        sub="Review CMS changes before push — approve, reject, or flag",
    ), unsafe_allow_html=True)
    st.caption("Preview image tagging and item name changes before uploading to CMS production")

    # Step 1: Template
    st.subheader("Step 1: Download Template")
    ca, cb = st.columns(2)
    with ca:
        st.download_button("Download CSV Template", _get_upload_template_csv(),
                          "cms_upload_template.csv", "text/csv", key="dl_upload_tpl")
    with cb:
        st.info("Fill ONLY the fields you're changing. Leave IMAGE_ID blank if only changing name, and vice versa.")

    # Step 2: Upload
    st.subheader("Step 2: Upload Filled CSV")
    uploaded = st.file_uploader("Upload your CSV", type=["csv"], key="upload_preview_csv")

    if not uploaded:
        st.info("Upload a CSV to begin previewing changes.")
        return

    # Parse
    df = pd.read_csv(uploaded)
    required = {"SPIN_ID"}
    if not required.issubset(set(df.columns)):
        st.error("CSV must have a SPIN_ID column. Download the template above.")
        return

    df = df.fillna("")
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # Filter valid rows
    has_img = df.get("IMAGE_ID", pd.Series("", index=df.index)).apply(lambda x: x != "" and x != "nan")
    has_name = df.get("ITEM_NAME", pd.Series("", index=df.index)).apply(lambda x: x != "" and x != "nan")
    df["_has_change"] = has_img | has_name
    skipped = len(df[~df["_has_change"]])
    df = df[df["_has_change"]].reset_index(drop=True)
    if skipped > 0:
        st.warning(f"Skipped {skipped} rows with no changes.")
    if df.empty:
        st.error("No valid rows with changes found.")
        return

    # Reset reviews if new file uploaded
    file_hash = hash(uploaded.name + str(len(df)))
    if st.session_state.get("_upload_hash") != file_hash:
        st.session_state["upload_reviews"] = {}
        st.session_state["upload_notes"] = {}
        st.session_state["_upload_hash"] = file_hash

    # Initialize review state
    for sid in df["SPIN_ID"]:
        if sid not in st.session_state.get("upload_reviews", {}):
            st.session_state.setdefault("upload_reviews", {})[sid] = "pending"

    # Fetch current data from Snowflake
    spin_ids = df["SPIN_ID"].unique().tolist()
    conn = get_snowflake_connection()
    conn_available = conn is not None
    if not conn_available:
        try:
            with st.spinner("Connecting to Snowflake (SSO — check Edge browser)..."):
                st.session_state["sf_conn"] = _create_snowflake_connection()
                conn = st.session_state["sf_conn"]
                conn_available = True
        except ImportError:
            st.warning("Snowflake connector not installed. Showing uploaded data only (cloud mode). Current images/names cannot be verified.")
        except Exception as e:
            st.warning(f"Could not connect to Snowflake: {e}")

    current_data = pd.DataFrame()
    current_images = pd.DataFrame()
    if conn_available:
        with st.spinner(f"Fetching current data for {len(spin_ids)} SPINs..."):
            current_data = _fetch_bulk_spin_data(conn, spin_ids)
            current_images = _fetch_bulk_spin_images(conn, spin_ids)

    # Step 3: Preview
    st.subheader("Step 3: Preview Changes")
    n_image = int(has_img[df.index].sum())
    n_name = int(has_name[df.index].sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Items", len(df))
    c2.metric("Image Changes", n_image)
    c3.metric("Name Changes", n_name)

    st.markdown("---")

    # Render cards
    for idx, row in df.iterrows():
        _render_preview_card(idx, row.to_dict(), current_data, current_images, conn_available)

    # Step 4: Review summary
    st.subheader("Step 4: Review Summary")
    reviews = st.session_state.get("upload_reviews", {})
    relevant = {k: v for k, v in reviews.items() if k in df["SPIN_ID"].values}
    n_approved = sum(1 for v in relevant.values() if v == "approved")
    n_rejected = sum(1 for v in relevant.values() if v == "rejected")
    n_pending = sum(1 for v in relevant.values() if v == "pending")

    c1, c2, c3 = st.columns(3)
    c1.metric("Approved", n_approved)
    c2.metric("Rejected", n_rejected)
    c3.metric("Pending", n_pending)

    # Downloads
    ca, cb = st.columns(2)
    with ca:
        if n_approved > 0:
            approved_spins = [k for k, v in relevant.items() if v == "approved"]
            df_approved = df[df["SPIN_ID"].isin(approved_spins)].drop(columns=["_has_change"], errors="ignore")
            st.download_button("Download Approved CSV (for production upload)",
                              df_approved.to_csv(index=False),
                              "approved_for_production.csv", "text/csv", key="dl_approved")
        else:
            st.info("No items approved yet.")

    with cb:
        df_review = df.drop(columns=["_has_change"], errors="ignore").copy()
        df_review["REVIEW_STATUS"] = df_review["SPIN_ID"].map(relevant)
        notes = st.session_state.get("upload_notes", {})
        df_review["REVIEWER_NOTES"] = df_review["SPIN_ID"].map(notes).fillna("")
        user = st.session_state.get("user", {})
        df_review["REVIEWED_BY"] = user.get("name", user.get("email", ""))
        df_review["REVIEW_TIMESTAMP"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        st.download_button("Download Full Review CSV",
                          df_review.to_csv(index=False),
                          "full_review.csv", "text/csv", key="dl_full_review")


# ── QC: Upgrade (Upgrade Items) ──────────────────────────────────────

QC_SOP_FILE = BASE_DIR / "cache" / "qc_sop.json"
QC_NOTES_DIR = BASE_DIR / "cache" / "qc_notes"
QC_NOTES_DIR.mkdir(exist_ok=True)

DEFAULT_SOP = {
    "items": [
        "Upgrade image is tagged to BK slot (not MN)",
        "Product name matches SKU Name in FinalDAv3",
        "Item has 4+ images total",
        "Differentiator callouts are clearly visible on upgrade image",
        "Base points are accurate (not misleading)",
        "Image resolution is 1200x1200 or better",
        "No typos in product name or description",
        "Brand logo is present on image",
        "Price point shown on upgrade image is current MRP",
    ]
}


def _load_qc_sop():
    if QC_SOP_FILE.exists():
        try:
            with open(QC_SOP_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_SOP


def _save_qc_sop(sop):
    with open(QC_SOP_FILE, "w") as f:
        json.dump(sop, f, indent=2)


def _load_qc_notes(item_code):
    f = QC_NOTES_DIR / f"{item_code}.json"
    if f.exists():
        try:
            with open(f) as fp:
                return json.load(fp)
        except Exception:
            pass
    return {"item_code": str(item_code), "sop_status": {}, "notes": "", "reviewer": "", "updated_at": ""}


def _save_qc_notes(item_code, data):
    f = QC_NOTES_DIR / f"{item_code}.json"
    with open(f, "w") as fp:
        json.dump(data, fp, indent=2)


def render_qc_diff_assortment():
    """Upgrade dashboard — insta_upgrade overview + FinalDAv3 QC tabs."""
    st.markdown(page_header(
        "Upgrade",
        sub="insta_upgrade tagging, BK image fulfillment, SplitCart, ERP force-enable",
    ), unsafe_allow_html=True)

    # ── Top section: insta_upgrade overview from CMS (cms_spins_1) ──────────
    _render_upgrade_overview()
    st.markdown("---")

    diff_csv = BASE_DIR / "diff_assortment_items.csv"
    if not diff_csv.exists():
        st.error("diff_assortment_items.csv not found. Run sync_diff_assortment.py first.")
        return

    df_diff = pd.read_csv(diff_csv)
    df_diff["Item Code"] = df_diff["Item Code"].astype(str).str.strip()
    # Only Official items (exclude L0)
    if "Is_L0" in df_diff.columns:
        df_diff = df_diff[~df_diff["Is_L0"]].copy()

    st.subheader("Anirudh's sheet (FinalDAv4) — bucket tracking")
    _render_finaldav3_dup_banner()
    _render_finalv3_buckets(df_diff)
    st.markdown("---")
    _render_anirudh_waterfall(df_diff)
    st.markdown("---")

    st.caption(f"Scope: {len(df_diff):,} Official Upgrade items (Level 1/2/3 exclusivity, FinalDAv3)")

    # Shared filters
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        bets = ["All"] + sorted(df_diff["Bet Category"].dropna().unique().tolist())
        bet_filter = st.selectbox("Bet Category", bets, key="qc_bet")
    with fc2:
        biz = ["All"] + sorted(df_diff["Business"].dropna().unique().tolist())
        biz_filter = st.selectbox("Business", biz, key="qc_biz")
    with fc3:
        themes = ["All"] + sorted(df_diff["Upgrade L1 Theme"].dropna().unique().tolist())
        theme_filter = st.selectbox("Upgrade L1 Theme", themes, key="qc_theme")

    # Apply filters
    df_scope = df_diff.copy()
    if bet_filter != "All":
        df_scope = df_scope[df_scope["Bet Category"] == bet_filter]
    if biz_filter != "All":
        df_scope = df_scope[df_scope["Business"] == biz_filter]
    if theme_filter != "All":
        df_scope = df_scope[df_scope["Upgrade L1 Theme"] == theme_filter]

    st.caption(f"Filtered: {len(df_scope):,} items")
    st.markdown("---")

    tabs = st.tabs([
        "0. SPIN Image Grid",
        "1. Image Fulfillment",
        "2. Image Count Flags",
        "3. Ratings",
        "4. ERP Status",
        "5. Enablement",
        "6. Checklist/SOP",
        "7. Secondary+Tertiary",
        "8. Copy Preview",
        "9. Master Template",
        "10. Sheet Change Tracker",
    ])

    with tabs[0]:
        _qc_tab0_spin_image_grid(df_scope)
    with tabs[1]:
        _qc_tab1_image_fulfillment(df_scope)
    with tabs[2]:
        _qc_tab2_image_count(df_scope)
    with tabs[3]:
        _qc_tab3_ratings(df_scope)
    with tabs[4]:
        _qc_tab4_erp_status(df_scope)
    with tabs[5]:
        _qc_tab5_enablement(df_scope)
    with tabs[6]:
        _qc_tab6_checklist(df_scope)
    with tabs[7]:
        _qc_tab7_secondary_tertiary(df_scope)
    with tabs[8]:
        _qc_tab8_copy_preview(df_scope)
    with tabs[9]:
        _qc_tab9_master_template(df_scope)
    with tabs[10]:
        _qc_tab10_sheet_change_tracker()


def _render_upgrade_overview():
    """Top metrics for the Upgrade page — sourced from CMS via
    cache/insta_upgrade_spins.parquet + splitcart_enablement.parquet."""
    show_sync_time(["insta_upgrade_spins", "splitcart_enablement"])
    iu = C("insta_upgrade_spins")
    if iu.empty:
        st.warning("`insta_upgrade_spins` cache missing. Run "
                   "`_fetch_insta_upgrade_with_bk.py` to populate.")
        return

    total = len(iu)
    bk_present = iu["BK"].notna().sum()
    qf_filled = iu["UPGRADE_QUICK_FILTER"].notna().sum()
    up_filled = iu["UPGRADE_PRIMARY"].notna().sum()
    sc_disabled = (iu["SPLITCART_GLOBAL"] == "false").sum()

    try:
        render_metrics([
            {"label": "insta_upgrade=Yes (CMS)", "value": f"{total:,}"},
            {"label": "BK image present", "value": f"{bk_present:,}",
             "state": "good" if bk_present == total else "warn",
             "delta": f"{bk_present/max(total,1)*100:.0f}%"},
            {"label": "upgrade_quick_filter set", "value": f"{qf_filled:,}",
             "state": "good" if qf_filled == total else "warn",
             "delta": f"{(total-qf_filled):,} missing", "delta_dir": "down"},
            {"label": "upgrade_primary set", "value": f"{up_filled:,}",
             "state": "good" if up_filled == total else "warn",
             "delta": f"{(total-up_filled):,} missing", "delta_dir": "down"},
            {"label": "SplitCart global=false", "value": f"{sc_disabled:,}",
             "state": "warn" if sc_disabled else "good"},
        ])
    except Exception:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("insta_upgrade=Yes", f"{total:,}")
        c2.metric("BK present", f"{bk_present:,}")
        c3.metric("quick_filter set", f"{qf_filled:,}")
        c4.metric("upgrade_primary set", f"{up_filled:,}")
        c5.metric("SplitCart global=false", f"{sc_disabled:,}")

    # quick_filter value distribution
    st.markdown("**`upgrade_quick_filter` distribution**")
    qf = (iu["UPGRADE_QUICK_FILTER"].fillna("(empty)").astype(str)
          .value_counts().reset_index())
    qf.columns = ["upgrade_quick_filter", "spin_count"]
    cA, cB = st.columns([2, 3])
    with cA:
        show_table(qf, key="qf_dist", height=240)
    with cB:
        sc = C("splitcart_enablement")
        if not sc.empty:
            sc_summary = pd.DataFrame({
                "SplitCart status": ["Global true", "Global false",
                                     "City-level override (any)"],
                "SPIN count": [
                    int((sc["GLOBAL_STATUS"] == "true").sum()),
                    int((sc["GLOBAL_STATUS"] == "false").sum()),
                    int((sc["SCOPE"] == "City").sum()),
                ],
            })
            st.markdown("**SplitCart enablement**")
            show_table(sc_summary, key="sc_dist", height=240)


def _render_finaldav3_dup_banner():
    """Warn-banner with download for duplicate Item Codes / SPINs in FinalDAv3."""
    dup = C("finaldav3_duplicates")
    if dup.empty:
        st.caption("No duplicate cache — run `_differentiated_assortment_duplicates.py`.")
        return
    n_item = int((dup["DUP_TYPE"] == "ITEM_CODE").sum())
    n_spin = int((dup["DUP_TYPE"] == "SPIN_ID").sum())
    distinct_codes = dup[dup["DUP_TYPE"] == "ITEM_CODE"]["Item Code"].nunique()
    distinct_spins = dup[dup["DUP_TYPE"] == "SPIN_ID"]["SPIN_ID"].nunique()
    if n_item == 0 and n_spin == 0:
        st.success("No duplicate Item Codes or SPINs in FinalDAv3.")
        return
    st.warning(
        f"⚠ FinalDAv3 has duplicates — "
        f"**{n_item} Item-Code dup rows** ({distinct_codes} distinct codes), "
        f"**{n_spin} SPIN dup rows** ({distinct_spins} distinct SPINs). "
        "Share this with Anirudh to dedupe at source."
    )
    with st.expander("View duplicate rows"):
        show_table(dup, key="finaldav3_dup_table", height=320)
    st.download_button(
        "Download duplicates CSV",
        dup.to_csv(index=False),
        "finaldav3_duplicates.csv", "text/csv",
        key="dl_finaldav3_dups",
    )


def _render_finalv3_buckets(df_diff):
    """Completed / WIP / Not Started buckets at BET_CATEGORY level using
    BK upgrade-image presence as the fill-rate signal."""
    if "Bet Category" not in df_diff.columns:
        st.info("Bet Category column missing in FinalDAv3.")
        return

    iu = C("insta_upgrade_spins")
    tagged_codes = set(iu["ITEM_CODE"].astype(str)) if not iu.empty else set()

    df = df_diff[df_diff["Bet Category"].notna()].copy()
    df["Item Code"] = df["Item Code"].astype(str)
    df["_tagged"] = df["Item Code"].isin(tagged_codes)
    # Dedup by (Bet Category, Item Code) so duplicate rows in FinalDAv3 don't
    # inflate the tagged count above total_spins.
    df = df.drop_duplicates(subset=["Bet Category", "Item Code"])
    grp = df.groupby("Bet Category").agg(
        total_spins=("Item Code", "nunique"),
        tagged=("_tagged", "sum"),
    ).reset_index()
    grp["fill_pct"] = (grp["tagged"] / grp["total_spins"].clip(lower=1) * 100).round(1)

    def bucket(p):
        if p >= 100:  return "Completed (100%)"
        if p >= 25:   return "WIP (25%+)"
        if p > 0:     return "WIP (<25%)"
        return "Not Started"
    grp["bucket"] = grp["fill_pct"].apply(bucket)

    summary = grp.groupby("bucket").agg(
        bet_categories=("Bet Category", "nunique"),
        total_spins=("total_spins", "sum"),
        tagged_spins=("tagged", "sum"),
    ).reset_index()
    order = ["Completed (100%)", "WIP (25%+)", "WIP (<25%)", "Not Started"]
    summary["_o"] = summary["bucket"].map({k: i for i, k in enumerate(order)})
    summary = summary.sort_values("_o").drop(columns=["_o"])

    cA, cB = st.columns([2, 3])
    with cA:
        st.markdown("**Bucket summary**")
        show_table(summary, key="bucket_summary", height=200)
    with cB:
        st.markdown("**Bet Categories — fill rate detail**")
        show_table(grp.sort_values("fill_pct", ascending=False),
                   key="bucket_detail", height=320)


def _render_anirudh_waterfall(df_diff):
    """Single-source-of-truth waterfall starting from Anirudh's sheet
    (FinalDAv4 = diff_assortment_items.csv) all the way to "good to go".

    Two views:
      - Exclusive: count per stage independently (where each gate stands alone)
      - Inclusive: cumulative funnel (count of SPINs that satisfy this AND
        every previous stage)
    Reconciles every "different" upgrade number on the dashboard.
    """
    st.markdown("### Upgrade waterfall — from Anirudh's sheet to good-to-go")
    st.caption("Reconciles every count on this page. **Exclusive** = per-stage "
               "gate. **Inclusive** = cumulative funnel. The shrink at each step "
               "is the real backlog. The final 3-way row stays at 0 until you "
               "upload the Input sheet on Tab 8 Copy Preview — that's when "
               "Good-to-Go is computable. The 2-way row above it is "
               "Template ↔ AI agreement (visible now without upload).")

    iu = C("insta_upgrade_spins")
    sc = C("splitcart_enablement")
    tmpl = C("master_template")
    ai_pts = C("ai_points")

    # Source: Anirudh's sheet, deduped at item-code level
    df = df_diff.copy()
    df["Item Code"] = df["Item Code"].astype(str).str.strip()
    df = df.drop_duplicates("Item Code").reset_index(drop=True)
    n_anirudh = len(df)

    # Stage 1 — In CMS (resolved to a SPIN via insta_upgrade_spins or
    # spin_image_master)
    sim = C("spin_image_master")
    cms_items = (set(sim["ITEM_CODE"].astype(str)) if not sim.empty else set())
    in_cms = set(df["Item Code"]) & cms_items
    n_in_cms = len(in_cms)

    # Stage 2 — Tagged insta_upgrade=Yes in CMS
    tagged_items = (set(iu["ITEM_CODE"].astype(str))
                    if not iu.empty else set())
    in_anirudh_tagged = set(df["Item Code"]) & tagged_items
    n_tagged = len(in_anirudh_tagged)

    # Stage 3 — Has BK image
    if not iu.empty:
        bk_items = set(iu[iu["BK"].notna()]["ITEM_CODE"].astype(str))
    else:
        bk_items = set()
    has_bk = in_anirudh_tagged & bk_items
    n_has_bk = len(has_bk)

    # Stage 4 — quick_filter populated
    if not iu.empty and "UPGRADE_QUICK_FILTER" in iu.columns:
        qf_items = set(iu[iu["UPGRADE_QUICK_FILTER"].notna()]
                         ["ITEM_CODE"].astype(str))
    else:
        qf_items = set()
    has_qf = has_bk & qf_items
    n_qf = len(has_qf)

    # Stage 5 — upgrade_primary populated
    if not iu.empty and "UPGRADE_PRIMARY" in iu.columns:
        up_items = set(iu[iu["UPGRADE_PRIMARY"].notna()]
                         ["ITEM_CODE"].astype(str))
    else:
        up_items = set()
    has_up = has_qf & up_items
    n_up = len(has_up)

    # Stage 6 — In Master Template
    if not tmpl.empty:
        tmpl_items = set(tmpl["ITEM_CODE"].astype(str))
    else:
        tmpl_items = set()
    in_tmpl = has_up & tmpl_items
    n_in_tmpl = len(in_tmpl)

    # Stage 7 — 3-way Match Ok (Template == AI exactly across all fields)
    def _norm(s):
        return " ".join(str(s or "").strip().lower().split())
    ok_spins = set()
    if not tmpl.empty and not ai_pts.empty:
        FIELDS = [f for f in (
            "HEADER_UPGRADE", "HEADER_REGULAR",
            "UPGRADE_POINT_1", "UPGRADE_POINT_2", "UPGRADE_POINT_3",
            "REGULAR_POINT_1", "REGULAR_POINT_2", "REGULAR_POINT_3",
        ) if f in tmpl.columns]
        t_idx = {}
        for _, r in tmpl.iterrows():
            t_idx[(str(r.get("Bet Category", "") or "").strip(),
                   str(r.get("Upgrade L1 Theme", "") or "").strip(),
                   _norm(r.get("HEADER_UPGRADE", "")),
                   _norm(r.get("HEADER_REGULAR", "")))] = r
        for _, a in ai_pts.iterrows():
            key = (str(a.get("BET_CATEGORY", "") or "").strip(),
                   str(a.get("UPGRADE_L1_THEME", "") or "").strip(),
                   _norm(a.get("HEADER_UPGRADE", "")),
                   _norm(a.get("HEADER_REGULAR", "")))
            t_row = t_idx.get(key)
            if t_row is None: continue
            if all(_norm(t_row.get(f, "")) == _norm(a.get(f, "")) for f in FIELDS):
                ok_spins.add(str(a.get("ITEM_CODE", "")))
    n_ok = len(in_tmpl & ok_spins)

    # 3-way match requires an Input sheet which isn't part of the cached
    # data — it's only uploaded session-side on Tab 8. So this funnel stage
    # is ALWAYS 0 until the user uploads on Tab 8.
    n_3way = 0
    stages = [
        ("Anirudh's sheet (FinalDAv4) — unique items", n_anirudh, n_anirudh),
        ("Resolved in CMS (spin_image_master)", len(set(df["Item Code"]) & cms_items), n_in_cms),
        ("CMS-tagged insta_upgrade=Yes", len(set(df["Item Code"]) & tagged_items), n_tagged),
        ("Has BK upgrade image", len(set(df["Item Code"]) & bk_items), n_has_bk),
        ("upgrade_quick_filter populated", len(set(df["Item Code"]) & qf_items), n_qf),
        ("upgrade_primary populated", len(set(df["Item Code"]) & up_items), n_up),
        ("Master Template entry exists", len(set(df["Item Code"]) & tmpl_items), n_in_tmpl),
        ("2-way Match: Template == AI (all fields)", len(set(df["Item Code"]) & ok_spins), n_ok),
        ("3-way Match: Template == AI == Input (Good-to-Go)", n_3way, n_3way),
    ]

    rows = []
    prev_inc = None
    for label, exclusive, inclusive in stages:
        drop = "" if prev_inc is None else f"{(prev_inc - inclusive):+d}"
        rows.append({
            "Stage": label,
            "Exclusive (gate)": f"{exclusive:,}",
            "Inclusive (cumulative)": f"{inclusive:,}",
            "Drop vs prev": drop,
            "% of Anirudh's sheet": f"{inclusive/max(n_anirudh,1)*100:.1f}%",
        })
        prev_inc = inclusive
    df_wf = pd.DataFrame(rows)
    show_table(df_wf, key="anirudh_waterfall", height=350)
    st.caption(
        "**Exclusive** = items meeting only this gate (regardless of others). "
        "**Inclusive** = items meeting this + every prior gate (true funnel). "
        "When the two columns differ, the gate is non-strict — e.g. items "
        "that have BK but aren't tagged insta_upgrade.")

    st.download_button(
        "⬇ Download waterfall CSV",
        df_wf.to_csv(index=False), "upgrade_waterfall.csv", "text/csv",
        key="waterfall_dl")


def _qc_tab0_spin_image_grid(df_scope):
    """SPIN-level image grid (Bet Category : Upgrade L1 Theme) with colored
    pills for insta_upgrade, upgrade_quick_filter, upgrade_primary, and
    SplitCart status."""
    st.subheader("SPIN Image Grid")
    st.caption("Pick a Bet Category × Upgrade L1 Theme to see every SPIN with "
               "its BK upgrade image and CMS attribute pills.")
    show_sync_time(["insta_upgrade_spins", "upgrade_images"])

    df_diff = df_scope.copy()
    df_diff["_group_key"] = (
        df_diff["Bet Category"].fillna("(blank)").astype(str) + " : " +
        df_diff.get("Upgrade L1 Theme",
                    pd.Series("(blank)", index=df_diff.index))
              .fillna("(blank)").astype(str)
    )

    # Build per-combo bucket using insta_upgrade tagging as the fill signal
    iu_for_status = C("insta_upgrade_spins")
    tagged_codes_set = (set(iu_for_status["ITEM_CODE"].astype(str))
                        if not iu_for_status.empty else set())
    df_diff["_tagged"] = df_diff["Item Code"].astype(str).isin(tagged_codes_set)
    grp_status = df_diff.groupby("_group_key").agg(
        total=("Item Code", "nunique"),
        tagged=("_tagged", "sum"),
    )
    grp_status["fill_pct"] = (
        grp_status["tagged"] / grp_status["total"].clip(lower=1) * 100)

    def _bucket(p):
        if p >= 100: return "Completed"
        if p >= 25:  return "WIP 25%+"
        if p > 0:    return "WIP <25%"
        return "Not Started"
    grp_status["bucket"] = grp_status["fill_pct"].apply(_bucket)

    bucket_choice = st.radio(
        "Status filter",
        ["All", "Completed", "WIP 25%+", "WIP <25%", "Not Started"],
        horizontal=True, key="qc_tab0_bucket")

    if bucket_choice == "All":
        eligible = grp_status.index.tolist()
    else:
        eligible = grp_status[grp_status["bucket"] == bucket_choice].index.tolist()

    options = sorted([k for k in eligible if k])
    if not options:
        st.info(f"No Bet Category : Upgrade L1 Theme combos in bucket "
                f"'{bucket_choice}'.")
        return

    # Annotate each option with its fill % so the dropdown shows status inline
    def _label(k):
        r = grp_status.loc[k]
        return f"{k}  ·  {int(r['tagged'])}/{int(r['total'])} ({r['fill_pct']:.0f}%)"
    label_to_key = {_label(k): k for k in options}
    selected_label = st.selectbox(
        f"Bet Category : Upgrade L1 Theme  ({len(options)} combos in '{bucket_choice}')",
        list(label_to_key.keys()), key="qc_tab0_grp")
    selected = label_to_key[selected_label]
    bet_items = df_diff[df_diff["_group_key"] == selected]

    iu = C("insta_upgrade_spins")
    sc = C("splitcart_enablement")
    tmpl = C("master_template")
    ai_pts = C("ai_points")
    if iu.empty:
        st.warning("`insta_upgrade_spins` cache missing.")
        return
    iu_by_item = {str(r["ITEM_CODE"]): r for _, r in iu.iterrows()}
    sc_by_spin = ({str(r["SPIN_ID"]): r for _, r in sc.iterrows()}
                  if not sc.empty else {})

    # ── 3-way match precompute (Template ↔ AI per SPIN) ─────────────────
    def _norm(s):
        return " ".join(str(s or "").strip().lower().split())

    threeway_by_spin = {}
    if not tmpl.empty and not ai_pts.empty:
        FIELDS = ["HEADER_UPGRADE", "HEADER_REGULAR",
                  "UPGRADE_POINT_1", "UPGRADE_POINT_2", "UPGRADE_POINT_3",
                  "REGULAR_POINT_1", "REGULAR_POINT_2", "REGULAR_POINT_3"]
        FIELDS = [f for f in FIELDS if f in tmpl.columns]
        # Index template rows by full 4-tuple AND a relaxed 3-tuple (theme-
        # agnostic) so we can fall back when the only mismatch is theme name
        t_idx = {}
        t_idx_relaxed = {}
        for _, r in tmpl.iterrows():
            bet = str(r.get("Bet Category", "") or "").strip()
            theme = str(r.get("Upgrade L1 Theme", "") or "").strip()
            hu = _norm(r.get("HEADER_UPGRADE", ""))
            hr = _norm(r.get("HEADER_REGULAR", ""))
            t_idx.setdefault((bet, theme, hu, hr), []).append(r)
            t_idx_relaxed.setdefault((bet, hu, hr), []).append(r)
        for _, a in ai_pts.iterrows():
            spin = str(a.get("SPIN_ID", "")).strip()
            key = (str(a.get("BET_CATEGORY", "") or "").strip(),
                   str(a.get("UPGRADE_L1_THEME", "") or "").strip(),
                   _norm(a.get("HEADER_UPGRADE", "")),
                   _norm(a.get("HEADER_REGULAR", "")))
            t_rows = t_idx.get(key, [])
            if not t_rows:
                # Try theme-agnostic fallback
                t_rows = t_idx_relaxed.get(
                    (key[0], key[2], key[3]), [])
            ai_hu = str(a.get("HEADER_UPGRADE", "") or "")
            ai_hr = str(a.get("HEADER_REGULAR", "") or "")
            if not key[0] or (not ai_hu and not ai_hr):
                threeway_by_spin[spin] = ("Not Available", "AI value missing")
                continue
            if not t_rows:
                threeway_by_spin[spin] = ("Template missing",
                                          "no matching template row")
                continue
            t_row = t_rows[0]
            mismatches = []
            for f in FIELDS:
                if _norm(t_row.get(f, "")) != _norm(a.get(f, "")):
                    mismatches.append(f)
            # NOTE: Tab 0 doesn't have an Input upload — so this is a 2-way
            # match (Template <-> AI). Use "2/3 Ok" labelling so user knows
            # the third leg (Input sheet) hasn't been compared yet. The full
            # 3-way match lives on Tab 8 Copy Preview after the user uploads.
            if not mismatches:
                threeway_by_spin[spin] = ("2/3 Ok", "Template == AI (Input not uploaded — upload on Tab 8 for full 3-way)")
            else:
                threeway_by_spin[spin] = (
                    "2/3 Not Ok",
                    f"{len(mismatches)} field(s) differ Template vs AI: {', '.join(mismatches)}")

    tagged_n = sum(1 for c in bet_items["Item Code"].astype(str)
                   if c in iu_by_item)
    st.caption(f"{tagged_n}/{len(bet_items)} items tagged insta_upgrade=Yes "
               f"in [{selected}]")

    # Build export rows alongside rendering
    export_rows = []

    for _, row in bet_items.iterrows():
        item_code = str(row["Item Code"])
        name = str(row.get("SKU Name", row.get("Brand Name", "")))[:60]
        brand = str(row.get("Brand Name", ""))
        rec = iu_by_item.get(item_code)
        spin_id = str(rec.get("SPIN_ID", "")) if rec is not None else ""
        threeway, threeway_note = threeway_by_spin.get(
            spin_id, ("Not Available", "SPIN not in AI cache"))

        export_rows.append({
            "SPIN_ID": spin_id,
            "Item Code": item_code,
            "Product Name": name,
            "Brand": brand,
            "Bet Category": row.get("Bet Category", ""),
            "Upgrade L1 Theme": row.get("Upgrade L1 Theme", ""),
            "insta_upgrade": safe_get(rec, "INSTA_UPGRADE", "")
                              if rec is not None else "",
            "upgrade_quick_filter": safe_get(rec, "UPGRADE_QUICK_FILTER", "")
                                     if rec is not None else "",
            "upgrade_primary": safe_get(rec, "UPGRADE_PRIMARY", "")
                                if rec is not None else "",
            "splitcart": (
                f"City({int(sc_by_spin[spin_id]['CITY_COUNT'])})"
                if spin_id in sc_by_spin and sc_by_spin[spin_id]["SCOPE"] == "City"
                else (f"Global={sc_by_spin[spin_id]['GLOBAL_STATUS']}"
                      if spin_id in sc_by_spin else "")),
            "3-way Match": threeway,
            "3-way Note": threeway_note,
            "BK URL": safe_get(rec, "BK", "") if rec is not None else "",
        })

        col_l, col_r = st.columns([1, 2])
        with col_l:
            st.markdown(f"**{name}**")
            st.caption(f"Item: {item_code} | Brand: {brand}")
            if rec is not None:
                st.caption(f"SPIN: {spin_id}")
                pills = [
                    _pill("insta_upgrade", rec.get("INSTA_UPGRADE", ""),
                          "good" if rec.get("INSTA_UPGRADE") else "critical"),
                    _pill("quick_filter", rec.get("UPGRADE_QUICK_FILTER", ""),
                          _hash_kind(rec.get("UPGRADE_QUICK_FILTER"))),
                    _pill("upgrade_primary", rec.get("UPGRADE_PRIMARY", ""),
                          "info" if rec.get("UPGRADE_PRIMARY") else "critical"),
                ]
                # SplitCart pill
                sc_rec = sc_by_spin.get(spin_id)
                if sc_rec is not None:
                    if sc_rec["SCOPE"] == "City":
                        pills.append(_pill(
                            "SplitCart",
                            f"City ({int(sc_rec['CITY_COUNT'])} cities)",
                            "warn"))
                    else:
                        kind = "good" if sc_rec["GLOBAL_STATUS"] == "true" else "critical"
                        pills.append(_pill("SplitCart",
                                           f"Global={sc_rec['GLOBAL_STATUS']}",
                                           kind))
                # 3-way match pill (last so it stands out)
                tw_kind = {
                    "2/3 Ok": "good",
                    "2/3 Not Ok": "critical",
                    "Ok": "good", "Not Ok": "critical",
                    "Template missing": "warn",
                    "Not Available": "muted"}.get(threeway, "info")
                pills.append(_pill("Match", threeway, tw_kind))
                st.markdown("".join(pills), unsafe_allow_html=True)
                if threeway == "Not Ok":
                    st.caption(f"⚠ {threeway_note}")
            else:
                st.markdown(_pill("insta_upgrade", "NOT TAGGED", "critical"),
                            unsafe_allow_html=True)
        with col_r:
            url = str(rec["BK"]) if rec is not None else ""
            if url and url not in ("None", "nan", ""):
                try:
                    st.image(url, width=280)
                except Exception:
                    st.caption("(BK image failed to load)")
            else:
                st.warning("No BK upgrade image")
        st.markdown("---")

    # ── Export ──────────────────────────────────────────────────────────
    st.markdown("##### Export")
    df_export = pd.DataFrame(export_rows)
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button(
            f"⬇ Download current view ({len(df_export)} SPIN(s)) — CSV",
            df_export.to_csv(index=False),
            f"spin_grid_{selected.replace(' : ', '_').replace(' ', '_')}.csv",
            "text/csv", key="qc0_dl_view")
    with col_dl2:
        # Build full grid across ALL filter combos for full export
        full_rows = []
        for _, r in df_diff.iterrows():
            ic = str(r["Item Code"]); rec = iu_by_item.get(ic)
            sid = str(rec.get("SPIN_ID", "")) if rec is not None else ""
            tw, note = threeway_by_spin.get(sid, ("Not Available", ""))
            full_rows.append({
                "SPIN_ID": sid, "Item Code": ic,
                "Product Name": str(r.get("SKU Name", "") or "")[:60],
                "Brand": r.get("Brand Name", ""),
                "Bet Category": r.get("Bet Category", ""),
                "Upgrade L1 Theme": r.get("Upgrade L1 Theme", ""),
                "insta_upgrade": safe_get(rec, "INSTA_UPGRADE", "") if rec is not None else "",
                "upgrade_quick_filter": safe_get(rec, "UPGRADE_QUICK_FILTER", "") if rec is not None else "",
                "upgrade_primary": safe_get(rec, "UPGRADE_PRIMARY", "") if rec is not None else "",
                "splitcart": (
                    f"City({int(sc_by_spin[sid]['CITY_COUNT'])})"
                    if sid in sc_by_spin and sc_by_spin[sid]["SCOPE"] == "City"
                    else (f"Global={sc_by_spin[sid]['GLOBAL_STATUS']}"
                          if sid in sc_by_spin else "")),
                "3-way Match": tw, "3-way Note": note,
                "BK URL": safe_get(rec, "BK", "") if rec is not None else "",
            })
        df_full = pd.DataFrame(full_rows)
        st.download_button(
            f"⬇ Download FULL grid ({len(df_full)} rows) — CSV",
            df_full.to_csv(index=False),
            "spin_grid_full.csv", "text/csv", key="qc0_dl_full")


def _qc_tab1_image_fulfillment(df_scope):
    st.subheader("Upgrade Image Fulfillment")
    show_sync_time(["upgrade_images"])

    df_up = C("upgrade_images")
    if df_up.empty:
        st.warning("upgrade_images.parquet not found. Run fetch_upgrade_simple.py.")
        return

    upgrade_items = set(df_up["ITEM_CODE"].astype(str))
    scope_items = set(df_scope["Item Code"].astype(str))

    have_upgrade = scope_items & upgrade_items
    missing_upgrade = scope_items - upgrade_items

    total = len(scope_items)
    have = len(have_upgrade)
    missing = len(missing_upgrade)
    pct = round(have / max(total, 1) * 100, 1)

    fulfill_state = "good" if pct >= 95 else ("warn" if pct >= 70 else "critical")
    try:
        render_metrics([
            {"label": "Total Upgrade Items", "value": f"{total:,}", "sub": "Official L1/L2/L3 scope"},
            {"label": "Have UPGRADE Image", "value": f"{have:,}",
             "state": "good" if pct >= 95 else None,
             "delta": f"{pct}%", "delta_dir": "up" if pct >= 50 else "down",
             "delta_period": "fill rate"},
            {"label": "Missing UPGRADE", "value": f"{missing:,}",
             "state": "critical" if missing > 100 else ("warn" if missing > 20 else "good"),
             "sub": f"{round(missing/max(total,1)*100,1)}% of scope"},
            {"label": "Fulfillment %", "value": f"{pct}", "unit": "%",
             "state": fulfill_state,
             "target_pct": pct, "target_goal": 95},
        ])
    except Exception:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Upgrade Items", f"{total:,}")
        c2.metric("Have UPGRADE Image", f"{have:,}", delta=f"{pct}%")
        c3.metric("Missing UPGRADE", f"{missing:,}", delta_color="inverse")
        c4.metric("Fulfillment %", f"{pct}%",
                  delta="On track" if pct >= 95 else "Below target",
                  delta_color="normal" if pct >= 95 else "inverse")

    st.markdown("#### Breakdown by Bet Category × Upgrade L1 Theme")
    df_scope_copy = df_scope.copy()
    df_scope_copy["Has_Upgrade"] = df_scope_copy["Item Code"].astype(str).isin(upgrade_items)
    pivot = df_scope_copy.groupby(["Bet Category", "Upgrade L1 Theme"]).agg(
        Total=("Item Code", "count"),
        With_Upgrade=("Has_Upgrade", "sum"),
    ).reset_index()
    pivot["With_Upgrade"] = pivot["With_Upgrade"].astype(int)
    pivot["Fill %"] = (pivot["With_Upgrade"] / pivot["Total"] * 100).round(1)
    pivot = pivot.sort_values("Fill %")
    show_table(pivot, key="qc1_bet_theme", height=400)

    # Missing items list
    if missing > 0:
        st.markdown(f"#### Missing UPGRADE Image ({missing:,} items)")
        missing_df = df_scope[df_scope["Item Code"].astype(str).isin(missing_upgrade)][
            ["Item Code", "Business", "Bet Category", "Upgrade L1 Theme", "Brand Name", "SKU Name"]
        ]
        show_table(missing_df.head(500), key="qc1_missing", height=400)
        st.download_button("Download Missing Items", missing_df.to_csv(index=False),
                           "missing_upgrade_images.csv", "text/csv", key="qc1_dl")


def _qc_tab2_image_count(df_scope):
    st.subheader("Image Count Flags (0 or <3 images)")
    show_sync_time(["spin_image_master"])

    df_sim = C("spin_image_master")
    if df_sim.empty:
        st.warning("spin_image_master.parquet not found.")
        return

    # Match upgrade items by Item Code
    df_sim["ITEM_CODE_STR"] = df_sim["ITEM_CODE"].astype(str)
    scope_items = set(df_scope["Item Code"].astype(str))
    df_qc = df_sim[df_sim["ITEM_CODE_STR"].isin(scope_items)].copy()

    # Dedup to one row per item (take first SPIN)
    df_qc = df_qc.drop_duplicates("ITEM_CODE_STR")

    total = len(df_qc)
    zero = len(df_qc[df_qc["IMAGE_COUNT"] == 0])
    low = len(df_qc[(df_qc["IMAGE_COUNT"] >= 1) & (df_qc["IMAGE_COUNT"] < 3)])
    med = len(df_qc[(df_qc["IMAGE_COUNT"] >= 3) & (df_qc["IMAGE_COUNT"] < 4)])
    ok = len(df_qc[df_qc["IMAGE_COUNT"] >= 4])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Matched Items", f"{total:,}")
    c2.metric("0 Images (Critical)", f"{zero:,}", delta_color="inverse")
    c3.metric("1-2 Images (Warning)", f"{low:,}", delta_color="inverse")
    c4.metric("3 Images (Partial)", f"{med:,}")
    c5.metric("4+ Images (OK)", f"{ok:,}")

    st.markdown("#### Items with < 3 Images")
    flagged = df_qc[df_qc["IMAGE_COUNT"] < 3].copy()
    if not flagged.empty:
        display_cols = ["SPIN_ID", "ITEM_CODE_STR", "PRODUCT_NAME", "L1", "L2", "BRAND",
                        "IMAGE_COUNT", "HAS_MAIN", "HAS_BK", "HAS_AL1"]
        available = [c for c in display_cols if c in flagged.columns]
        flagged_display = flagged[available].rename(columns={"ITEM_CODE_STR": "Item Code"})
        flagged_display = flagged_display.sort_values("IMAGE_COUNT")
        show_table(flagged_display, key="qc2_flagged", height=500)
        st.download_button("Download Flagged", flagged_display.to_csv(index=False),
                          "upgrade_low_images.csv", "text/csv", key="qc2_dl")
    else:
        st.success("All upgrade items have 3+ images!")


def _qc_tab3_ratings(df_scope):
    st.subheader("Ratings — Upgrade Items")
    st.caption("7d and 14d buckets coming soon (need new Snowflake query)")

    show_sync_time(["item_ratings_30d", "item_ratings_90d", "item_ratings_all_time"])

    window = st.radio("Time Window", ["30 Days", "90 Days", "All Time"], horizontal=True, key="qc3_window")
    cache_key = {"30 Days": "item_ratings_30d", "90 Days": "item_ratings_90d",
                 "All Time": "item_ratings_all_time"}[window]

    df_r = C(cache_key)
    if df_r.empty:
        st.warning(f"{cache_key}.parquet not found.")
        return

    # Map via spin_image_master to get Item Code
    df_sim = C("spin_image_master")
    if not df_sim.empty:
        spin_to_item = dict(zip(df_sim["SPIN_ID"], df_sim["ITEM_CODE"].astype(str)))
        df_r["ITEM_CODE"] = df_r["SPIN_ID"].map(spin_to_item).astype(str)
    else:
        df_r["ITEM_CODE"] = ""

    scope_items = set(df_scope["Item Code"].astype(str))
    df_scoped = df_r[df_r["ITEM_CODE"].isin(scope_items)].copy()

    total = len(df_scoped)
    avg = df_scoped["AVG_RATING"].mean() if "AVG_RATING" in df_scoped.columns else 0
    low = len(df_scoped[df_scoped["AVG_RATING"] < 3.5]) if "AVG_RATING" in df_scoped.columns else 0

    c1, c2, c3 = st.columns(3)
    c1.metric(f"Rated Items ({window})", f"{total:,}")
    c2.metric("Avg Rating", f"{avg:.2f}" if avg else "N/A")
    c3.metric("Low Rated (<3.5)", f"{low:,}", delta_color="inverse")

    if low > 0:
        st.markdown("#### Low Rated Items (<3.5)")
        low_df = df_scoped[df_scoped["AVG_RATING"] < 3.5].copy()
        show_cols = [c for c in ["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "AVG_RATING", "ORDERS", "L1_CATEGORY"]
                     if c in low_df.columns]
        show_table(low_df[show_cols].sort_values("AVG_RATING"), key="qc3_low", height=500)


def _qc_tab4_erp_status(df_scope):
    st.subheader("ERP Status — City & Pod Level")
    show_sync_time(["erp_intended_city_tier"])

    df_erp = C("erp_intended_city_tier")
    if df_erp.empty:
        st.warning("erp_intended_city_tier.parquet not found.")
        return

    df_erp["ITEM_CODE"] = df_erp["ITEM_CODE"].astype(str)
    scope_items = set(df_scope["Item Code"].astype(str))
    df_scoped = df_erp[df_erp["ITEM_CODE"].isin(scope_items)].copy()

    total_items = df_scoped["ITEM_CODE"].nunique()
    total_cities = df_scoped["CITY"].nunique()
    total_mappings = len(df_scoped)

    c1, c2, c3 = st.columns(3)
    c1.metric("Items in ERP", f"{total_items:,}")
    c2.metric("Cities Covered", f"{total_cities:,}")
    c3.metric("Item × City Mappings", f"{total_mappings:,}")

    st.markdown("#### City × Tier Distribution")
    pivot = df_scoped.groupby(["CITY", "ERP_TIER"])["ITEM_CODE"].nunique().reset_index(name="Items")
    pivot_wide = pivot.pivot(index="CITY", columns="ERP_TIER", values="Items").fillna(0).astype(int)
    pivot_wide["Total"] = pivot_wide.sum(axis=1)
    pivot_wide = pivot_wide.sort_values("Total", ascending=False).reset_index()
    show_table(pivot_wide, key="qc4_city_tier", height=400)

    # Items missing from ERP
    in_erp = set(df_scoped["ITEM_CODE"])
    missing = scope_items - in_erp
    if missing:
        st.markdown(f"#### ⚠ Items NOT in ERP ({len(missing):,})")
        missing_df = df_scope[df_scope["Item Code"].astype(str).isin(missing)][
            ["Item Code", "Business", "Bet Category", "Brand Name", "SKU Name"]
        ]
        show_table(missing_df.head(500), key="qc4_missing", height=300)
        st.download_button("Download Missing", missing_df.to_csv(index=False),
                          "upgrade_missing_erp.csv", "text/csv", key="qc4_dl")


def _qc_tab5_enablement(df_scope):
    st.subheader("Enablement Status — Bulk Upgrade Items")
    show_sync_time(["assortment_state"])

    df_state = C("assortment_state")
    if df_state.empty:
        st.warning("assortment_state.parquet not found. Run fetch_databricks.py assortment.")
        return

    # Map Item Code -> SPIN_ID
    df_sim = C("spin_image_master")
    if df_sim.empty:
        st.warning("spin_image_master.parquet not found.")
        return

    scope_items = set(df_scope["Item Code"].astype(str))
    item_to_spin = df_sim[df_sim["ITEM_CODE"].astype(str).isin(scope_items)][["ITEM_CODE", "SPIN_ID"]]
    item_to_spin["ITEM_CODE"] = item_to_spin["ITEM_CODE"].astype(str)

    scope_spins = set(item_to_spin["SPIN_ID"])
    df_state_scoped = df_state[df_state["spin_id"].isin(scope_spins)].copy()

    if df_state_scoped.empty:
        st.warning("No assortment state data for these items.")
        return

    # Count IN_ASSORTMENT
    df_state_scoped["is_enabled"] = df_state_scoped["assortment_state"].str.contains(
        "IN_ASSORTMENT", case=False, na=False
    )

    total_spins = df_state_scoped["spin_id"].nunique()
    enabled_spins = df_state_scoped[df_state_scoped["is_enabled"]]["spin_id"].nunique()
    total_cities = df_state_scoped["city_id"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SPINs with State", f"{total_spins:,}")
    c2.metric("SPINs Enabled", f"{enabled_spins:,}")
    c3.metric("Cities", f"{total_cities:,}")
    enable_pct = round(enabled_spins / max(total_spins, 1) * 100, 1)
    c4.metric("Enable %", f"{enable_pct}%",
              delta="OK" if enable_pct >= 80 else "Low",
              delta_color="normal" if enable_pct >= 80 else "inverse")

    # Per-SPIN summary
    st.markdown("#### Per-SPIN Enablement Summary")
    spin_summary = df_state_scoped.groupby("spin_id").agg(
        Total_CxT=("city_id", "count"),
        Enabled=("is_enabled", "sum"),
    ).reset_index()
    spin_summary["Enabled"] = spin_summary["Enabled"].astype(int)
    spin_summary["Enable %"] = (spin_summary["Enabled"] / spin_summary["Total_CxT"] * 100).round(1)

    # Join back Item Code
    spin_to_item = dict(zip(df_sim["SPIN_ID"], df_sim["ITEM_CODE"].astype(str)))
    spin_summary["Item Code"] = spin_summary["spin_id"].map(spin_to_item)
    spin_summary = spin_summary[["Item Code", "spin_id", "Total_CxT", "Enabled", "Enable %"]]
    spin_summary = spin_summary.rename(columns={"spin_id": "SPIN"})
    spin_summary = spin_summary.sort_values("Enable %")
    show_table(spin_summary, key="qc5_spin", height=500)

    low = spin_summary[spin_summary["Enable %"] < 50]
    if not low.empty:
        st.markdown(f"#### 🔴 Low Enable % Items ({len(low):,})")
        st.caption("Items with less than 50% enablement across qualifying city × tier slots")
        st.download_button("Download Low Enable Items", low.to_csv(index=False),
                          "upgrade_low_enable.csv", "text/csv", key="qc5_dl")


def _qc_tab6_checklist(df_scope):
    st.subheader("QC Checklist & SOP")
    st.caption("Review each item against SOP, capture notes")

    user = st.session_state.get("user", {})
    is_admin = user.get("role") == "admin"

    # Admin: edit SOP
    sop = _load_qc_sop()
    with st.expander("SOP Definition" + (" (admin-editable)" if is_admin else "")):
        if is_admin:
            sop_text = st.text_area(
                "One SOP item per line",
                value="\n".join(sop.get("items", [])),
                height=250,
                key="qc6_sop_edit"
            )
            if st.button("Save SOP", key="qc6_save_sop"):
                new_items = [line.strip() for line in sop_text.split("\n") if line.strip()]
                _save_qc_sop({"items": new_items})
                st.success(f"Saved {len(new_items)} SOP items")
                st.rerun()
        else:
            for i, item in enumerate(sop.get("items", []), 1):
                st.markdown(f"{i}. {item}")

    st.markdown("---")

    # Item selector
    item_options = df_scope["Item Code"].astype(str).tolist()
    if not item_options:
        st.info("No items match filter.")
        return

    sel_item = st.selectbox("Select Item to Review", item_options, key="qc6_item_sel")
    item_row = df_scope[df_scope["Item Code"].astype(str) == sel_item].iloc[0]

    st.markdown(f"### {item_row.get('SKU Name', sel_item)}")
    st.caption(f"Item: {sel_item} | Brand: {item_row.get('Brand Name', '')} | "
               f"Bet: {item_row.get('Bet Category', '')} | Theme: {item_row.get('Upgrade L1 Theme', '')}")

    # Load existing notes
    notes_data = _load_qc_notes(sel_item)

    # SOP checkboxes
    st.markdown("#### SOP Checklist")
    sop_items = sop.get("items", [])
    status = notes_data.get("sop_status", {})
    for i, sop_item in enumerate(sop_items):
        key = f"sop_{sel_item}_{i}"
        checked = st.checkbox(sop_item, value=status.get(str(i), False), key=key)
        status[str(i)] = checked

    # Free-form notes
    st.markdown("#### Reviewer Notes")
    notes_text = st.text_area(
        "Notes",
        value=notes_data.get("notes", ""),
        height=120,
        key=f"qc6_notes_{sel_item}",
        label_visibility="collapsed"
    )

    # Save button
    if st.button("Save Review", type="primary", key=f"qc6_save_{sel_item}"):
        notes_data["sop_status"] = status
        notes_data["notes"] = notes_text
        notes_data["reviewer"] = user.get("email", user.get("name", "unknown"))
        notes_data["updated_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        _save_qc_notes(sel_item, notes_data)
        st.success("Saved!")

    # Show review history summary
    st.markdown("---")
    st.markdown("#### Reviewed Items Summary")
    all_reviewed = list(QC_NOTES_DIR.glob("*.json"))
    if all_reviewed:
        summary = []
        for f in all_reviewed:
            try:
                with open(f) as fp:
                    d = json.load(fp)
                sop_done = sum(1 for v in d.get("sop_status", {}).values() if v)
                summary.append({
                    "Item Code": d.get("item_code"),
                    "Reviewer": d.get("reviewer", ""),
                    "Updated": d.get("updated_at", ""),
                    "SOP Checked": f"{sop_done}/{len(sop_items)}",
                    "Has Notes": "Yes" if d.get("notes") else "No",
                })
            except Exception:
                continue
        if summary:
            summary_df = pd.DataFrame(summary).sort_values("Updated", ascending=False)
            show_table(summary_df, key="qc6_summary", height=300)
    else:
        st.info("No items reviewed yet.")


SMALL_TIERS = {"XS", "S", "S1", "M", "M1", "L", "L1"}
LARGE_TIERS = {"XL", "XL1", "2XL", "3XL", "4XL", "5XL", "6XL", "7XL", "8XL"}


def _splitcart_tier_audit(df_spin_scoped, df_city):
    """SplitCart vs tier rule audit. Rule: any item present in a city at
    XS/S/M/L tiers must NOT have SplitCart enabled there (Secondary cost
    leak). Globally enabled SplitCart is forbidden if any small-tier city
    presence exists."""
    df_erp = C("erp_intended_city_tier")
    if df_erp.empty:
        st.info("`erp_intended_city_tier` cache missing — cannot run tier audit.")
        return

    # Scope ERP to in-scope item codes (massive speedup; full ERP is ~4M rows)
    items = set(df_spin_scoped["ITEM_CODE"].astype(str).tolist())
    df_erp = df_erp[df_erp["ITEM_CODE"].astype(str).isin(items)].copy()
    df_erp["ITEM_CODE"] = df_erp["ITEM_CODE"].astype(str)
    df_erp["TIER_GROUP"] = df_erp["ERP_TIER"].apply(
        lambda t: "small" if str(t).strip().upper() in SMALL_TIERS
        else ("large" if str(t).strip().upper() in LARGE_TIERS else "other"))

    # Per item: lists of small-tier and large-tier cities
    grp = df_erp.groupby(["ITEM_CODE", "TIER_GROUP"])["CITY"].apply(
        lambda s: sorted({str(x) for x in s if pd.notna(x)})).unstack(fill_value=[])
    for c in ("small", "large", "other"):
        if c not in grp.columns:
            grp[c] = [[]] * len(grp)
    small_by_item = grp["small"].to_dict()
    large_by_item = grp["large"].to_dict()

    # City override lookups — which cities have secondary explicitly true?
    city_sec_true = {}
    if not df_city.empty:
        df_city2 = df_city.copy()
        df_city2["SPIN_ID"] = df_city2["SPIN_ID"].astype(str)
        st_true = df_city2[df_city2["SECONDARY_CITY"].astype(str)
                                  .str.lower().isin(("true","t","yes","1"))]
        city_sec_true = (st_true.groupby("SPIN_ID")["CITY_ID"]
                                 .apply(lambda s: {str(x) for x in s}).to_dict())

    # Build per-SPIN audit row
    rows = []
    for _, r in df_spin_scoped.iterrows():
        item = str(r["ITEM_CODE"]); spin = str(r["SPIN_ID"])
        small_cities = small_by_item.get(item, [])
        large_cities = large_by_item.get(item, [])
        global_val = str(r.get("SECONDARY_POD_ENABLED", "") or "").strip().lower()
        is_global_on = global_val in ("true", "t", "yes", "1")
        # Default = true when null per CMS rule. So is_global_on if explicit OR null.
        is_global_default_on = global_val not in ("false", "f", "no", "0")

        violations = []
        if small_cities and is_global_default_on:
            # Has small-tier presence AND globally enabled = leak
            violations.append(
                f"GLOBAL=on but item present in {len(small_cities)} small-tier city/cities")
        # Per-city violation: secondary explicitly true for a small-tier city
        sec_true_set = city_sec_true.get(spin, set())
        # We can only flag city-id violations if we know the small-tier CITY_IDs.
        # For now flag at item level: small-tier cities exist AND any city override is true.
        if small_cities and sec_true_set:
            violations.append(
                f"{len(sec_true_set)} city-level override(s) set true while small tiers exist")

        if violations:
            rows.append({
                "ITEM_CODE": item, "SPIN_ID": spin,
                "PRODUCT_NAME": r.get("PRODUCT_NAME", ""),
                "BRAND": r.get("BRAND", ""), "L1": r.get("L1", ""),
                "Small-tier cities (count)": len(small_cities),
                "Small-tier cities (list)": ", ".join(small_cities),
                "Large-tier cities (count)": len(large_cities),
                "Large-tier cities (list)": ", ".join(large_cities),
                "Global Secondary": "ON" if is_global_default_on else "OFF",
                "Violation": "; ".join(violations),
                "Action Required": (
                    "Set GLOBAL=false; enable city-level=true ONLY for "
                    f"{len(large_cities)} large-tier city/cities"
                ),
            })

    df_v = pd.DataFrame(rows)
    n = len(df_v)

    if n == 0:
        st.success("✅ No SplitCart tier violations — all upgrade SPINs are correctly scoped.")
        return

    st.markdown(
        f"<div style='background:#7f1d1d;color:#fff;padding:14px 18px;"
        f"border-radius:8px;border:2px solid #fca5a5;margin:8px 0;'>"
        f"<b>🚨 SPLITCART TIER VIOLATION — {n} SPIN(s) leaking cost.</b><br>"
        "These items are enabled in Secondary SplitCart in cities where they "
        "are stocked at small tiers (XS / S / M / L). This drives up secondary "
        "cost. Fix: disable Global, enable city-level only for large tiers "
        "(XL+).</div>",
        unsafe_allow_html=True)

    show_table(df_v, key="qc7_splitcart_audit", height=420)
    st.download_button(
        f"⬇ Download violations CSV ({n} SPINs) — change-ready",
        df_v.to_csv(index=False),
        "splitcart_tier_violations.csv", "text/csv",
        key="qc7_splitcart_audit_dl")


def _qc_tab7_secondary_tertiary(df_scope):
    """Secondary Pod + P999 (Tertiary) enablement at SPIN and city level."""
    st.subheader("Secondary & Tertiary (P999) Pod Enablement")
    show_sync_time(["upgrade_spin_secondary_p999", "upgrade_city_secondary_p999"])
    st.caption("Source: CMS `attr.secondary_pod_enabled` (Secondary) and `attr.p999_availability` (Tertiary / P999)")

    df_spin = C("upgrade_spin_secondary_p999")
    df_city = C("upgrade_city_secondary_p999")

    if df_spin.empty:
        st.warning("Secondary + P999 data not synced yet. Run `fetch_secondary_p999.py`.")
        return

    # Scope to filtered items
    scope_items = set(df_scope["Item Code"].astype(str))
    df_spin_scoped = df_spin[df_spin["ITEM_CODE"].astype(str).isin(scope_items)].copy()

    # Normalize flags
    def _flag(val):
        s = str(val).strip().lower()
        if s in ("true", "t", "yes", "y", "1"):
            return "Enabled"
        if s in ("false", "f", "no", "n", "0"):
            return "Disabled"
        return "Not Set"

    df_spin_scoped["SEC_STATUS"] = df_spin_scoped["SECONDARY_POD_ENABLED"].apply(_flag)
    df_spin_scoped["P999_STATUS"] = df_spin_scoped["P999_AVAILABILITY"].apply(_flag)

    # ── SplitCart tier audit (cost-leak check) — render at top so it flashes
    st.markdown("### 🚨 SplitCart Tier Audit (cost-leak)")
    _splitcart_tier_audit(df_spin_scoped, df_city)
    st.markdown("---")

    # Summary metrics
    total = len(df_spin_scoped)
    sec_on = len(df_spin_scoped[df_spin_scoped["SEC_STATUS"] == "Enabled"])
    sec_off = len(df_spin_scoped[df_spin_scoped["SEC_STATUS"] == "Disabled"])
    sec_null = len(df_spin_scoped[df_spin_scoped["SEC_STATUS"] == "Not Set"])
    p_on = len(df_spin_scoped[df_spin_scoped["P999_STATUS"] == "Enabled"])
    p_off = len(df_spin_scoped[df_spin_scoped["P999_STATUS"] == "Disabled"])
    p_null = len(df_spin_scoped[df_spin_scoped["P999_STATUS"] == "Not Set"])

    st.markdown("#### SPIN-Level Status")
    both = len(df_spin_scoped[(df_spin_scoped['SEC_STATUS']=='Enabled') & (df_spin_scoped['P999_STATUS']=='Enabled')])
    sec_pct = round(sec_on/max(total,1)*100, 1)
    p_pct = round(p_on/max(total,1)*100, 1)
    try:
        render_metrics([
            {"label": "Total SPINs", "value": f"{total:,}"},
            {"label": "Secondary Enabled", "value": f"{sec_on:,}",
             "state": "good" if sec_pct >= 30 else "warn",
             "delta": f"{sec_pct}%", "delta_dir": "up", "delta_period": "of total"},
            {"label": "P999 (Tertiary) Enabled", "value": f"{p_on:,}",
             "state": "info",
             "delta": f"{p_pct}%", "delta_dir": "up", "delta_period": "of total"},
            {"label": "Both Enabled", "value": f"{both:,}",
             "state": "good" if both > 0 else "muted",
             "sub": "Fully tier-expanded"},
        ])
    except Exception:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total SPINs", f"{total:,}")
        c2.metric("Secondary Enabled", f"{sec_on:,}", delta=f"{sec_pct}%")
        c3.metric("P999 Enabled", f"{p_on:,}", delta=f"{p_pct}%")
        c4.metric("Both Enabled", f"{both:,}")

    # Cross-tab
    st.markdown("#### Secondary × P999 Matrix")
    matrix = pd.crosstab(df_spin_scoped["SEC_STATUS"], df_spin_scoped["P999_STATUS"])
    show_table(matrix.reset_index(), key="qc7_matrix")

    # SPIN-level detail with City/Global/Not Applied breakdown
    st.markdown("#### SPIN-Level Detail")
    df_view = df_spin_scoped.copy()
    df_view["ITEM_CODE"] = df_view["ITEM_CODE"].astype(str)
    df_view["SPIN_ID"]   = df_view["SPIN_ID"].astype(str)

    # City overrides per SPIN
    city_override_cnt = pd.Series(dtype=int)
    city_sec_enabled_cnt = pd.Series(dtype=int)
    if not df_city.empty:
        df_city = df_city.copy()
        df_city["SPIN_ID"] = df_city["SPIN_ID"].astype(str)
        city_override_cnt = df_city.groupby("SPIN_ID").size()
        sec_true = df_city[df_city["SECONDARY_CITY"].astype(str).str.lower()
                              .isin(("true", "t", "yes", "1"))]
        city_sec_enabled_cnt = sec_true.groupby("SPIN_ID").size()

    # ERP city count + city-name list per ITEM_CODE
    erp_cities_per_item = pd.Series(dtype=int)
    erp_city_names_per_item = {}
    df_erp_intended = C("erp_intended_city_tier")
    if not df_erp_intended.empty and "ITEM_CODE" in df_erp_intended.columns and "CITY" in df_erp_intended.columns:
        df_erp_intended = df_erp_intended.copy()
        df_erp_intended["ITEM_CODE"] = df_erp_intended["ITEM_CODE"].astype(str)
        erp_cities_per_item = df_erp_intended.groupby("ITEM_CODE")["CITY"].nunique()
        erp_city_names_per_item = (
            df_erp_intended.groupby("ITEM_CODE")["CITY"]
                           .apply(lambda s: sorted({str(x) for x in s if pd.notna(x)}))
                           .to_dict()
        )

    # City-name → city_id map from pod_master
    city_name_to_id = {}
    pm = C("pod_master")
    if not pm.empty and "CITY" in pm.columns and "CITY_ID" in pm.columns:
        for _, r in pm[["CITY", "CITY_ID"]].dropna().drop_duplicates().iterrows():
            city_name_to_id[str(r["CITY"]).strip().upper()] = str(r["CITY_ID"])

    def _sec_classification(row):
        spin = str(row["SPIN_ID"])
        if spin in city_override_cnt.index and city_override_cnt[spin] > 0:
            return "City Level"
        s = str(row.get("SECONDARY_POD_ENABLED", "") or "").strip().lower()
        if s in ("true", "false", "t", "f", "yes", "no", "1", "0"):
            return "Global Level"
        return "Not Applied"

    df_view["Secondary Status"] = df_view.apply(_sec_classification, axis=1)

    def _global_status(val):
        s = str(val or "").strip().lower()
        if s in ("true", "t", "yes", "1"):  return 1
        if s in ("false", "f", "no", "0"):  return 0
        return ""
    df_view["Global Level Status"] = df_view.apply(
        lambda r: _global_status(r.get("SECONDARY_POD_ENABLED"))
                  if r["Secondary Status"] == "Global Level" else "",
        axis=1)
    df_view["City Level ERP"] = df_view.apply(
        lambda r: int(erp_cities_per_item.get(str(r["ITEM_CODE"]), 0))
                  if r["Secondary Status"] == "City Level" else "",
        axis=1)
    df_view["City Level Secondary"] = df_view.apply(
        lambda r: int(city_sec_enabled_cnt.get(str(r["SPIN_ID"]), 0))
                  if r["Secondary Status"] == "City Level" else "",
        axis=1)
    def _disabled(r):
        if r["Secondary Status"] != "City Level": return ""
        erp = int(erp_cities_per_item.get(str(r["ITEM_CODE"]), 0))
        sec = int(city_sec_enabled_cnt.get(str(r["SPIN_ID"]), 0))
        return max(erp - sec, 0)
    df_view["Disabled City Level"] = df_view.apply(_disabled, axis=1)

    # Not Applied → list cities (name + ID, comma-separated) where item is in ERP
    def _na_city_names(r):
        if r["Secondary Status"] != "Not Applied": return ""
        cities = erp_city_names_per_item.get(str(r["ITEM_CODE"]), [])
        return ", ".join(cities)
    def _na_city_ids(r):
        if r["Secondary Status"] != "Not Applied": return ""
        cities = erp_city_names_per_item.get(str(r["ITEM_CODE"]), [])
        ids = [city_name_to_id.get(c.strip().upper(), "")
               for c in cities]
        return ", ".join(i for i in ids if i)
    df_view["City Names (Not Applied)"] = df_view.apply(_na_city_names, axis=1)
    df_view["City IDs (Not Applied)"]   = df_view.apply(_na_city_ids,   axis=1)

    # Filter
    sec_choice = st.radio(
        "Secondary Status filter",
        ["All", "Global Level", "City Level", "Not Applied"],
        horizontal=True, key="qc7_sec_filter")
    if sec_choice != "All":
        df_view = df_view[df_view["Secondary Status"] == sec_choice]

    display_cols = ["ITEM_CODE", "SPIN_ID", "PRODUCT_NAME", "BRAND",
                    "L1", "L2", "L3",
                    "Secondary Status", "Global Level Status",
                    "City Level ERP", "City Level Secondary",
                    "Disabled City Level",
                    "City Names (Not Applied)", "City IDs (Not Applied)",
                    "P999_STATUS"]
    display_cols = [c for c in display_cols if c in df_view.columns]

    # Summary chips before the table
    n_global = int((df_view["Secondary Status"] == "Global Level").sum())
    n_city   = int((df_view["Secondary Status"] == "City Level").sum())
    n_na     = int((df_view["Secondary Status"] == "Not Applied").sum())
    st.caption(f"Global Level: {n_global:,} · City Level: {n_city:,} · "
               f"Not Applied: {n_na:,} · total: {len(df_view):,}")

    show_table(df_view[display_cols], key="qc7_detail", height=500)

    st.download_button(
        "Download SPIN-level CSV (filtered view)",
        df_view[display_cols].to_csv(index=False),
        "qc_secondary_p999_spin.csv", "text/csv", key="qc7_dl_spin")
    st.download_button(
        "Download FULL SPIN-level CSV (all SPINs in scope)",
        df_spin_scoped.assign(
            **{c: df_view.set_index("SPIN_ID")[c].reindex(
                df_spin_scoped["SPIN_ID"].astype(str)).values
               for c in ("Secondary Status", "Global Level Status",
                         "City Level ERP", "City Level Secondary",
                         "Disabled City Level",
                         "City Names (Not Applied)", "City IDs (Not Applied)")
               if c in df_view.columns}
        ).to_csv(index=False),
        "qc_secondary_p999_spin_full.csv", "text/csv", key="qc7_dl_spin_full")

    # City-level view
    if not df_city.empty:
        st.markdown("---")
        st.markdown("#### SPIN × City Level (Overrides)")
        st.caption("Shows cities where the flag is explicitly set (overrides SPIN-level default)")

        df_city["SPIN_ID"] = df_city["SPIN_ID"].astype(str)
        scope_spins = set(df_spin_scoped["SPIN_ID"].astype(str))
        df_city_scoped = df_city[df_city["SPIN_ID"].isin(scope_spins)].copy()

        # Filter to rows with explicit overrides (not null)
        has_sec = df_city_scoped["SECONDARY_CITY"].notna() & (df_city_scoped["SECONDARY_CITY"] != "None") & (df_city_scoped["SECONDARY_CITY"].astype(str) != "")
        has_p999 = df_city_scoped["P999_CITY"].notna() & (df_city_scoped["P999_CITY"] != "None") & (df_city_scoped["P999_CITY"].astype(str) != "")
        df_city_overrides = df_city_scoped[has_sec | has_p999].copy()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total SPIN × City Rows", f"{len(df_city_scoped):,}")
        c2.metric("Rows with Overrides", f"{len(df_city_overrides):,}")
        unique_spins_w_override = df_city_overrides["SPIN_ID"].nunique()
        c3.metric("SPINs with Overrides", f"{unique_spins_w_override:,}")

        # SPIN selector to see city detail
        spins_with_data = sorted(df_city_scoped["SPIN_ID"].unique().tolist())
        if spins_with_data:
            sel_spin = st.selectbox("Select SPIN to see city-level data",
                                     ["(none)"] + spins_with_data[:500], key="qc7_city_sel")
            if sel_spin and sel_spin != "(none)":
                detail = df_city_scoped[df_city_scoped["SPIN_ID"] == sel_spin].copy()
                detail["Sec Status"] = detail["SECONDARY_CITY"].apply(_flag)
                detail["P999 Status"] = detail["P999_CITY"].apply(_flag)
                detail_cols = ["CITY_ID", "SECONDARY_CITY", "P999_CITY", "Sec Status", "P999 Status"]
                show_table(detail[detail_cols], key="qc7_city_detail", height=400)

        st.download_button("Download City Overrides CSV",
                           df_city_overrides.to_csv(index=False),
                           "qc_secondary_p999_city.csv", "text/csv", key="qc7_dl_city")


def _qc_tab8_copy_preview(df_scope):
    """Copy Preview — 3-way match across Template / AI / Input.

    * Template  : cache/master_template.parquet
    * AI        : cache/ai_points.parquet  (fresh independent AI read per SPIN)
    * Input     : user-uploaded sheet (same shape as Master Template xlsx)
    Key: (Bet Category, Upgrade L1 Theme, HEADER_UPGRADE, HEADER_REGULAR).
    Flag any field where Template / AI / Input do not all agree."""
    st.subheader("Copy Preview — 3-way Match (Template ↔ AI ↔ Input)")
    show_sync_time(["master_template", "ai_points"])

    tmpl = C("master_template")
    ai   = C("ai_points")
    if tmpl.empty:
        st.warning("Master Template cache missing. Run `_sync_master_template.py`.")
        return
    if ai.empty:
        st.warning("AI extraction cache missing. Run `_ai_fetch_all_points.py` "
                   "(needs ANTHROPIC_API_KEY).")
        return

    FIELDS = ["HEADER_UPGRADE", "HEADER_REGULAR",
              "UPGRADE_POINT_1", "UPGRADE_POINT_2", "UPGRADE_POINT_3",
              "REGULAR_POINT_1", "REGULAR_POINT_2", "REGULAR_POINT_3"]

    def _norm(s):
        return " ".join(str(s or "").strip().lower().split())

    for c in ("Bet Category", "Upgrade L1 Theme",
              "HEADER_UPGRADE", "HEADER_REGULAR"):
        if c not in tmpl.columns:
            tmpl[c] = ""
    tmpl["SPIN_ID"] = tmpl.get("SPIN ID", "").astype(str).str.strip()
    tmpl["_kb"] = tmpl["Bet Category"].astype(str).str.strip()
    tmpl["_kt"] = tmpl["Upgrade L1 Theme"].astype(str).str.strip()
    tmpl["_ku"] = tmpl["HEADER_UPGRADE"].astype(str).apply(_norm)
    tmpl["_kr"] = tmpl["HEADER_REGULAR"].astype(str).apply(_norm)

    # Bulletproof column extraction — ai_points may have nulls or missing cols
    def _col(df, name):
        if name in df.columns:
            return df[name].fillna("").astype(str).str.strip()
        return pd.Series([""] * len(df), index=df.index)
    ai["SPIN_ID"] = _col(ai, "SPIN_ID")
    ai["_kb"] = _col(ai, "BET_CATEGORY")
    ai["_kt"] = _col(ai, "UPGRADE_L1_THEME")
    ai["_ku"] = _col(ai, "HEADER_UPGRADE").apply(_norm)
    ai["_kr"] = _col(ai, "HEADER_REGULAR").apply(_norm)

    st.markdown("##### Input values (optional upload for 3-way match)")
    st.caption("Columns: SPIN ID, Item Code, Bet Category, Upgrade L1 Theme, "
               "HEADER_UPGRADE, HEADER_REGULAR, UPGRADE_POINT_1/2/3, REGULAR_POINT_1/2/3")
    up = st.file_uploader("Drop Input sheet (CSV / XLSX)",
                          type=["csv", "xlsx"], key="qc8_input_up")
    inp = None
    if up is not None:
        try:
            inp = (pd.read_csv(up) if up.name.lower().endswith(".csv")
                   else pd.read_excel(up))
            inp.columns = [c.strip() for c in inp.columns]
            inp["SPIN_ID"] = inp.get("SPIN ID", "").astype(str).str.strip()
            for c in FIELDS + ["Bet Category", "Upgrade L1 Theme"]:
                if c not in inp.columns:
                    inp[c] = ""
            inp["_kb"] = inp["Bet Category"].astype(str).str.strip()
            inp["_kt"] = inp["Upgrade L1 Theme"].astype(str).str.strip()
            inp["_ku"] = inp["HEADER_UPGRADE"].astype(str).apply(_norm)
            inp["_kr"] = inp["HEADER_REGULAR"].astype(str).apply(_norm)
            st.caption(f"Input loaded: {len(inp)} rows")
        except Exception as e:
            st.error(f"Could not parse uploaded file: {e}")
            inp = None

    def _uniq_strs(s):
        return sorted({str(x) for x in s.dropna().tolist() if str(x).strip()})
    bets = ["All"] + _uniq_strs(ai["_kb"])
    bet_sel = st.selectbox("Bet Category", bets, key="qc8_bet")
    scope_ai = ai if bet_sel == "All" else ai[ai["_kb"] == bet_sel]
    themes = ["All"] + _uniq_strs(scope_ai["_kt"])
    theme_sel = st.selectbox("Upgrade L1 Theme", themes, key="qc8_theme")
    if theme_sel != "All":
        scope_ai = scope_ai[scope_ai["_kt"] == theme_sel]
    status_sel = st.selectbox(
        "Filter",
        ["All", "Any mismatch", "Template missing", "Input missing",
         "All match (Ok)"],
        key="qc8_status")

    # Build BOTH a strict 4-tuple index AND a relaxed 3-tuple (Bet+HU+HR)
    # index. If strict misses, fall back to relaxed (theme-agnostic) — most
    # common reason for false "Template missing".
    t_idx, i_idx = {}, {}
    t_idx_relaxed = {}  # (bet, hu_norm, hr_norm) -> [rows]
    for _, r in tmpl.iterrows():
        t_idx.setdefault((r["_kb"], r["_kt"], r["_ku"], r["_kr"]), []).append(r)
        t_idx_relaxed.setdefault((r["_kb"], r["_ku"], r["_kr"]), []).append(r)
    if inp is not None:
        for _, r in inp.iterrows():
            i_idx.setdefault((r["_kb"], r["_kt"], r["_ku"], r["_kr"]), []).append(r)

    rows = []
    for _, a in scope_ai.iterrows():
        key = (a["_kb"], a["_kt"], a["_ku"], a["_kr"])
        t_row = t_idx.get(key, [None])[0]
        # Theme-agnostic fallback for false "Template missing" cases
        theme_inferred = False
        if t_row is None:
            relaxed = t_idx_relaxed.get((a["_kb"], a["_ku"], a["_kr"]), [])
            if relaxed:
                t_row = relaxed[0]
                theme_inferred = True
        i_row = i_idx.get(key, [None])[0] if inp is not None else None
        out = {
            "SPIN_ID":        a["SPIN_ID"],
            "Item Code":      a.get("ITEM_CODE", ""),
            "Product Name":   str(a.get("PRODUCT_NAME", "") or "")[:60],
            "Bet Category":   a["_kb"],
            "Upgrade L1 Theme": a["_kt"],
            "BK URL":         a.get("BK_URL", ""),
        }
        row_mismatch = False
        any_tmpl_missing = t_row is None
        any_input_missing = (inp is not None and i_row is None)
        # Quantitative-tolerant categories: number can vary per SKU as long as
        # the surrounding template format matches. Currently Oats + Protein
        # Bars + Muesli (per user policy 2026-04-26).
        import re as _re
        QUANT_BETS = {"oats", "protein bars", "muesli and granola"}
        is_quant_bet = a.get("_kb", "").strip().lower() in QUANT_BETS

        def _quant_match(template_val, ai_val):
            """True if template's number-bearing format matches ai_val with
            only the number differing. Strategy: replace every number in BOTH
            template and AI with the literal placeholder '<N>' then compare
            normalized strings. Avoids regex-replacement quirks."""
            if not template_val or not ai_val:
                return False
            num_re = _re.compile(r"\d+(?:\.\d+)?")
            if not num_re.search(template_val):
                return False
            tn = num_re.sub("<N>", template_val.strip())
            vn = num_re.sub("<N>", ai_val.strip())
            return _norm(tn) == _norm(vn)

        for f in FIELDS:
            av = str(a.get(f, "") or "")
            tv = "" if t_row is None else str(t_row.get(f, "") or "")
            iv = "" if i_row is None else str(i_row.get(f, "") or "")
            out[f"TEMPLATE_{f}"] = tv
            out[f"AI_{f}"] = av
            out[f"INPUT_{f}"] = iv
            parts = [_norm(tv), _norm(av)]
            if inp is not None:
                parts.append(_norm(iv))
            if not any(parts):
                out[f"{f}_MATCH"] = ""
                continue
            if t_row is None:
                out[f"{f}_MATCH"] = "Template missing"
                continue
            if inp is not None and i_row is None:
                out[f"{f}_MATCH"] = "Input missing"
                row_mismatch = True
                continue
            vals = {p for p in parts if p}
            if len(vals) <= 1:
                out[f"{f}_MATCH"] = "Ok"
            elif is_quant_bet and _quant_match(tv, av) and (
                inp is None or _quant_match(tv, iv) or _norm(iv) == _norm(tv)):
                # Number-only variance for Oats/Protein Bars — treat as Ok
                out[f"{f}_MATCH"] = "Ok (number variance)"
            else:
                out[f"{f}_MATCH"] = "Not Ok"
                row_mismatch = True
        if any_tmpl_missing:
            out["ROW_STATUS"] = "Template missing"
        elif row_mismatch:
            out["ROW_STATUS"] = "Not Ok"
        elif any_input_missing:
            out["ROW_STATUS"] = "Input missing"
        else:
            out["ROW_STATUS"] = "Ok"
        rows.append(out)
    df = pd.DataFrame(rows)

    if status_sel == "Any mismatch":
        df = df[df["ROW_STATUS"] == "Not Ok"]
    elif status_sel == "Template missing":
        df = df[df["ROW_STATUS"] == "Template missing"]
    elif status_sel == "Input missing":
        df = df[df["ROW_STATUS"] == "Input missing"]
    elif status_sel == "All match (Ok)":
        df = df[df["ROW_STATUS"] == "Ok"]

    total = len(df)
    # ROW_STATUS values: "Ok", "Not Ok", "Template missing", "Input missing"
    # (note: per-field "Ok (number variance)" still rolls up to ROW_STATUS=Ok
    # because it doesn't trigger row_mismatch)
    ok_n = int((df["ROW_STATUS"] == "Ok").sum())
    bad_n = int((df["ROW_STATUS"] == "Not Ok").sum())
    tm_n = int((df["ROW_STATUS"] == "Template missing").sum())
    im_n = int((df["ROW_STATUS"] == "Input missing").sum())
    try:
        render_metrics([
            {"label": "SPINs in scope", "value": f"{total:,}"},
            {"label": "All Ok (3-way)", "value": f"{ok_n:,}", "state": "good"},
            {"label": "Any mismatch", "value": f"{bad_n:,}",
             "state": "critical" if bad_n else "muted"},
            {"label": "Template missing", "value": f"{tm_n:,}",
             "state": "warn" if tm_n else "muted"},
            {"label": "Input missing", "value": f"{im_n:,}",
             "state": "warn" if im_n else "muted"},
        ])
    except Exception:
        c = st.columns(5)
        c[0].metric("SPINs", total); c[1].metric("All Ok", ok_n)
        c[2].metric("Mismatch", bad_n); c[3].metric("Tmpl missing", tm_n)
        c[4].metric("Input missing", im_n)

    if tm_n:
        st.error(f"⚠ {tm_n} SPIN(s) have no Master Template entry for their "
                 "(Bet × Theme × Upgrade Header × Regular Header) key. "
                 "Add template rows to the Master Template xlsx.")

    show_table(df, key="qc8_3way", height=480)
    st.download_button(
        f"Download 3-way match ({len(df):,} rows, CSV)",
        df.to_csv(index=False), "copy_preview_3way.csv",
        "text/csv", key="qc8_3way_dl")


def _qc_tab9_master_template(df_scope):
    """Master Template — pure display of the canonical template rows.
    Filterable by Bet Category, Upgrade L1 Theme, Upgrade Header, Regular
    Header. Each matching row shows the BK image on top and a 4-row
    Upgrade | Regular table below. No analysis, no match flags — this is
    just the source-of-truth viewer."""
    st.subheader("Master Template")
    st.caption("Canonical template per (Bet Category × Upgrade L1 Theme × "
               "Upgrade Header × Regular Header). This is the reference; "
               "matching happens on the Copy Preview tab.")
    show_sync_time(["master_template"])

    tmpl = C("master_template")
    if tmpl.empty:
        st.warning("master_template cache missing. Drop a new Master Template "
                   "xlsx on Desktop and run `_sync_master_template.py`.")
        return

    # Normalise columns we depend on
    for c in ["Bet Category", "Upgrade L1 Theme",
              "HEADER_UPGRADE", "HEADER_REGULAR",
              "UPGRADE_POINT_1", "UPGRADE_POINT_2", "UPGRADE_POINT_3",
              "REGULAR_POINT_1", "REGULAR_POINT_2", "REGULAR_POINT_3",
              "BK URL", "Product Name", "SPIN ID", "Item Code"]:
        if c not in tmpl.columns:
            tmpl[c] = ""

    # Filters
    f1, f2 = st.columns(2)
    with f1:
        bets = ["All"] + sorted(tmpl["Bet Category"].dropna().astype(str).unique().tolist())
        bet = st.selectbox("Bet Category", bets, key="qc9_tmpl_bet")
    with f2:
        scope = tmpl if bet == "All" else tmpl[tmpl["Bet Category"].astype(str) == bet]
        themes = ["All"] + sorted(scope["Upgrade L1 Theme"].dropna().astype(str).unique().tolist())
        theme = st.selectbox("Upgrade L1 Theme", themes, key="qc9_tmpl_theme")

    if theme != "All":
        scope = scope[scope["Upgrade L1 Theme"].astype(str) == theme]

    f3, f4 = st.columns(2)
    with f3:
        hu = ["All"] + sorted(scope["HEADER_UPGRADE"].dropna().astype(str).unique().tolist())
        hu_sel = st.selectbox("Upgrade Header", hu, key="qc9_tmpl_hu")
    with f4:
        hr = ["All"] + sorted(scope["HEADER_REGULAR"].dropna().astype(str).unique().tolist())
        hr_sel = st.selectbox("Regular Header", hr, key="qc9_tmpl_hr")

    if hu_sel != "All":
        scope = scope[scope["HEADER_UPGRADE"].astype(str) == hu_sel]
    if hr_sel != "All":
        scope = scope[scope["HEADER_REGULAR"].astype(str) == hr_sel]

    st.caption(f"Showing {len(scope)} template row(s).")

    st.markdown("---")
    for _, row in scope.iterrows():
        col_img, col_tbl = st.columns([1, 2])
        with col_img:
            url = str(row.get("BK URL", "") or "")
            if url and url.lower() not in ("nan", "none"):
                try:
                    st.image(url, width=320)
                except Exception:
                    st.caption("(image failed to load)")
            else:
                st.warning("No image URL")
        with col_tbl:
            st.markdown(f"**{row.get('Product Name', '')}**")
            st.caption(f"SPIN: {row.get('SPIN ID', '')} | "
                       f"Item: {row.get('Item Code', '')} | "
                       f"{row.get('Bet Category', '')} · "
                       f"{row.get('Upgrade L1 Theme', '')}")
            # 4-row table: Upgrade | Regular
            tbl = pd.DataFrame({
                "Upgrade": [
                    str(row.get("HEADER_UPGRADE", "") or ""),
                    str(row.get("UPGRADE_POINT_1", "") or ""),
                    str(row.get("UPGRADE_POINT_2", "") or ""),
                    str(row.get("UPGRADE_POINT_3", "") or ""),
                ],
                "Regular": [
                    str(row.get("HEADER_REGULAR", "") or ""),
                    str(row.get("REGULAR_POINT_1", "") or ""),
                    str(row.get("REGULAR_POINT_2", "") or ""),
                    str(row.get("REGULAR_POINT_3", "") or ""),
                ],
            }, index=["Header", "Point 1", "Point 2", "Point 3"])
            st.dataframe(tbl, use_container_width=True)
        st.markdown("---")

    # Raw download
    st.download_button(
        f"Download filtered template ({len(scope)} rows, CSV)",
        scope.to_csv(index=False), "master_template_filtered.csv",
        "text/csv", key="qc9_tmpl_dl")
    return

def _render_issue_tracker():
    """Persistent open/resolved tracker for Upgrade issues. Refreshes every
    full sync via _track_upgrade_issues.py. Shows aging + resolution timeline."""
    st.markdown("##### 🛠 Open Issues — refreshed every full sync")
    track = C("upgrade_issue_tracker")
    if track.empty:
        st.info("No tracker yet. Run `_track_upgrade_issues.py` (auto-runs in "
                "sync_and_deploy.bat).")
        return

    open_now = track[track["STATUS"] == "OPEN"].copy()
    resolved = track[track["STATUS"] == "RESOLVED"].copy()

    # Top metrics
    by_type = open_now.groupby("ISSUE_TYPE").size().reset_index(name="open")
    try:
        render_metrics([
            {"label": "Open issues", "value": f"{len(open_now):,}",
             "state": "critical" if len(open_now) else "good"},
            {"label": "Resolved (lifetime)", "value": f"{len(resolved):,}",
             "state": "good"},
            {"label": "Oldest open (days)",
             "value": str(int(pd.to_numeric(open_now["AGE_DAYS"], errors="coerce")
                               .max() or 0))},
            {"label": "Avg age (days)",
             "value": f"{pd.to_numeric(open_now['AGE_DAYS'], errors='coerce').mean():.1f}"
                      if len(open_now) else "0"},
        ])
    except Exception:
        c = st.columns(4)
        c[0].metric("Open", len(open_now))
        c[1].metric("Resolved", len(resolved))

    if not by_type.empty:
        st.markdown("**Open by type**")
        show_table(by_type.sort_values("open", ascending=False),
                   key="issue_by_type", height=180)

    issue_choice = st.selectbox(
        "Show", ["All open", "Not in FinalDAv4", "AI blank",
                 "Template missing", "Not Ok", "Resolved (last 7d)"],
        key="issue_filter")
    if issue_choice == "All open":
        view = open_now
    elif issue_choice == "Resolved (last 7d)":
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=7)).date().isoformat()
        view = resolved[resolved["RESOLVED_AT"].astype(str).str[:10] >= cutoff]
    else:
        view = open_now[open_now["ISSUE_TYPE"] == issue_choice]

    cols = ["SPIN_ID", "ITEM_CODE", "PRODUCT_NAME", "BRAND",
            "BET_CATEGORY", "UPGRADE_L1_THEME", "ISSUE_TYPE", "NOTE",
            "FIRST_SEEN_AT", "LAST_SEEN_AT", "RESOLVED_AT",
            "STATUS", "AGE_DAYS"]
    cols = [c for c in cols if c in view.columns]
    view = view.sort_values("AGE_DAYS", ascending=False) if "AGE_DAYS" in view.columns else view
    show_table(view[cols], key="issue_table", height=420)
    st.download_button(
        f"⬇ Download {issue_choice} ({len(view):,} rows, CSV)",
        view[cols].to_csv(index=False),
        "upgrade_issues.csv", "text/csv", key="issue_dl")


def _qc_tab10_sheet_change_tracker():
    """Track what's changing in FinalDAv4 (Gopal's / Anirudh's sheet) over
    time: item codes added, removed, net, NPI status. Day- and week-level
    rollups from diff_assortment_history.parquet + per-day CSVs."""
    st.subheader("Sheet Change Tracker")
    st.caption("Day- and week-level adds / removes in FinalDAv4 + current "
               "NPI status. Use this to see how supply/planning is moving and "
               "adjust strategy.")
    show_sync_time(["diff_assortment_history", "upgrade_issue_tracker"])

    # Issue tracker first (it's the more actionable item)
    _render_issue_tracker()
    st.markdown("---")

    hist = C("diff_assortment_history")
    if hist.empty:
        st.warning("No history yet. Run `sync_diff_assortment.py` first.")
        return

    hist = hist.copy()
    hist["date"] = pd.to_datetime(hist["date"])
    hist = hist.sort_values("date")
    hist["week"] = hist["date"].dt.to_period("W-SUN").dt.start_time

    view = st.radio("Rollup", ["Daily", "Weekly"], horizontal=True,
                     key="qc10_view")
    if view == "Weekly":
        rollup = hist.groupby("week").agg(
            total_items=("total_items", "last"),
            added=("added", "sum"),
            removed=("removed", "sum"),
            net_change=("net_change", "sum"),
        ).reset_index().rename(columns={"week": "period"})
    else:
        rollup = hist.rename(columns={"date": "period"})[
            ["period", "total_items", "added", "removed", "net_change"]]
    rollup["period"] = pd.to_datetime(rollup["period"]).dt.date
    rollup = rollup.sort_values("period", ascending=False)

    latest = rollup.iloc[0] if len(rollup) else None
    if latest is not None:
        try:
            render_metrics([
                {"label": f"Latest {view.lower()[:-2]}y total",
                 "value": f"{int(latest['total_items']):,}"},
                {"label": "Added", "value": f"{int(latest['added']):,}",
                 "state": "good" if latest["added"] else "muted"},
                {"label": "Removed", "value": f"{int(latest['removed']):,}",
                 "state": "critical" if latest["removed"] else "muted"},
                {"label": "Net change",
                 "value": f"{int(latest['net_change']):+,}",
                 "state": "good" if latest["net_change"] >= 0 else "critical"},
            ])
        except Exception:
            c = st.columns(4)
            c[0].metric("Total", int(latest['total_items']))
            c[1].metric("Added", int(latest['added']))
            c[2].metric("Removed", int(latest['removed']))
            c[3].metric("Net", int(latest['net_change']))

    st.markdown(f"##### {view} rollup")
    show_table(rollup, key="qc10_rollup", height=320)
    st.download_button(
        f"Download {view.lower()} rollup CSV",
        rollup.to_csv(index=False), f"finaldav4_{view.lower()}_changes.csv",
        "text/csv", key="qc10_dl")

    # NPI status — current snapshot
    st.markdown("---")
    st.markdown("##### NPI Pending (current)")
    npi_csv = BASE_DIR / "diff_assortment_npi_pending.csv"
    if npi_csv.exists():
        df_npi = pd.read_csv(npi_csv)
        have = len(df_npi[df_npi.get("NPI_Status", "") == "Key Available"]) \
               if "NPI_Status" in df_npi.columns else 0
        miss = len(df_npi[df_npi.get("NPI_Status", "") == "Key Missing"]) \
               if "NPI_Status" in df_npi.columns else 0
        c1, c2, c3 = st.columns(3)
        c1.metric("NPI rows total", len(df_npi))
        c2.metric("NPI key available", have)
        c3.metric("NPI key missing", miss)
        show_table(df_npi, key="qc10_npi", height=320)
        st.download_button("Download NPI pending CSV",
                           df_npi.to_csv(index=False),
                           "diff_assortment_npi_pending.csv", "text/csv",
                           key="qc10_npi_dl")
    else:
        st.caption("diff_assortment_npi_pending.csv not found.")

    # Per-day change details
    st.markdown("---")
    st.markdown("##### Day detail — items added / removed")
    hist_dir = BASE_DIR / "diff_assortment_history"
    days = sorted([f.stem.replace("added_", "")
                   for f in hist_dir.glob("added_*.csv")], reverse=True)
    days += [d for d in sorted([f.stem.replace("removed_", "")
                   for f in hist_dir.glob("removed_*.csv")], reverse=True)
             if d not in days]
    if not days:
        st.caption("No per-day add/remove files yet.")
        return
    day_sel = st.selectbox("Pick a day", days, key="qc10_day")
    ca, cr = st.columns(2)
    with ca:
        st.markdown(f"**Added on {day_sel}**")
        af = hist_dir / f"added_{day_sel}.csv"
        if af.exists():
            df_a = pd.read_csv(af)
            st.caption(f"{len(df_a):,} items")
            show_table(df_a, key=f"qc10_added_{day_sel}", height=300)
        else:
            st.caption("(none)")
    with cr:
        st.markdown(f"**Removed on {day_sel}**")
        rf = hist_dir / f"removed_{day_sel}.csv"
        if rf.exists():
            df_r = pd.read_csv(rf)
            st.caption(f"{len(df_r):,} items")
            show_table(df_r, key=f"qc10_removed_{day_sel}", height=300)
        else:
            st.caption("(none)")


# ── Shelf Life Deviation ─────────────────────────────────────────────────────
def render_shelf_life(fs):
    st.markdown(page_header(
        "Shelf Life Deviation",
        sub="FMCG shelf-life rule deviations by WH / CX / outbound",
    ), unsafe_allow_html=True)
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
    _view = dev_df[available].copy()
    # Severity: rows deviating on multiple cutoffs are critical; single-deviation rows are warn
    def _row_sev(r):
        flags = sum(1 for c in ("WH_INWARD_DEVIATION", "CX_CUTOFF_DEVIATION", "WH_OUTWARD_DEVIATION") if r.get(c, False))
        return "critical" if flags >= 2 else ("warn" if flags == 1 else "info")
    _view.insert(0, "Severity", _view.apply(_row_sev, axis=1))
    show_table(_view, key="sl_dev_detail", height=500, severity_col="Severity")
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
    st.markdown(page_header(
        "ERP Assortment Monitor (Events)",
        sub="Event-driven SKU planning — upcoming, calendar, quarterly",
    ), unsafe_allow_html=True)
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

    # ── Pre-register tab count badges (cheap cache reads) ───────────────────
    if _DESIGN_LOADED:
        try:
            # Image Health: Defect Detection count from zero_img + 1-3 img
            _img = C("image_monitor_daily")
            if not _img.empty:
                _latest = _img.sort_values("date", ascending=False).iloc[0]
                _def_count = int(_latest.get("zero_image_count", 0) or 0)
                if _def_count > 0:
                    register_tab_badge("Defect Detection", f"{_def_count:,}", "critical")

            # ERP BAU: Block OTB count
            _blk = C("erp_block_otb")
            if not _blk.empty:
                register_tab_badge("Block OTB", f"{len(_blk):,}", "critical")

            # QC Diff: pending SOP count (static 47 per design; wire to real data later)
            register_tab_badge("Checklist / SOP", 47, "warn")
            register_tab_badge("Image Fulfillment", "1,842", "warn")

            # Shelf Life: deviation count
            _dev = C("shelf_life_deviations")
            if not _dev.empty:
                register_tab_badge("Deviations Detail", f"{len(_dev):,}", "warn")
        except Exception:
            pass

    # ── Global SPIN drilldown (visible on every view when ?drill= is set) ───
    drill_value = fs.get("drill", "") or st.query_params.get("drill", "")
    if drill_value:
        try:
            render_drill_panel(drill_value, fs)
        except Exception as _de:
            st.warning(f"Drilldown failed: {_de}")

    # ── Sticky topbar (breadcrumb only — search is a real Streamlit widget) ─
    current_q = st.query_params.get("q", "")
    if _DESIGN_LOADED:
        try:
            st.markdown(
                topbar_html(
                    ["Instamart", "Catalog", metric],
                    alert_count=5,
                    current_q="",   # don't render an HTML input; real widget below
                ),
                unsafe_allow_html=True,
            )
        except Exception:
            pass

    # Real Streamlit text input for global search — HTML <input> inside
    # st.markdown was being sanitised / focus-blocked, so use a native widget.
    q_new = st.text_input(
        "Global search",
        value=current_q,
        placeholder="Search metrics, SPIN / item code, or any tab…",
        key="__global_q",
        label_visibility="collapsed",
    ).strip()
    if q_new != current_q:
        try:
            if q_new:
                st.query_params["q"] = q_new
            elif "q" in st.query_params:
                del st.query_params["q"]
        except Exception:
            pass
        st.rerun()

    # ── Global Search — read value from ?q= (set by topbar input on Enter) ──
    if _DESIGN_LOADED:
        try:
            from design_helpers import search_suggestions
            q = current_q
            if q and len(q.strip()) >= 2:
                hits = search_suggestions(q)
                if hits:
                    st.caption(f"{len(hits)} matches — click to jump")
                    # Render as clickable buttons
                    cols = st.columns(min(len(hits), 4))
                    for i, h in enumerate(hits[:8]):
                        col = cols[i % 4]
                        icon = h.get("icon", "•")
                        parent = f" · {h['parent']}" if h.get("parent") else ""
                        btn_label = f"{icon} {h['label']}{parent}"
                        if col.button(btn_label, key=f"gs_{i}"):
                            # Jump to the target metric
                            target = h["target"]
                            st.session_state["_pending_metric"] = target
                            # If it's a SPIN/item lookup, pre-fill
                            if h["type"] == "lookup":
                                st.session_state["spin_search"] = h["query"]
                            st.rerun()
                else:
                    st.caption("No matches found")
        except Exception as _se:
            pass

    # Handle pending metric jump from search
    if st.session_state.get("_pending_metric"):
        pending = st.session_state.pop("_pending_metric")
        if pending in [metric, "SPIN Lookup", "ERP Assortment (BAU)", "QC: Upgrade",
                       "Image Health", "ERP Assortment (Events)", "Enabled Items Health",
                       "Shelf Life Deviation", "Upload Preview"]:
            metric = pending

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
    elif metric == "Upload Preview":
        render_upload_preview()
    elif metric == "QC: Upgrade":
        render_qc_diff_assortment()

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

    # ── Install ALL global JS behaviors (nav click delegate, topbar search
    # wiring, ⌘K, tab badge painter). Must be called AFTER views have
    # registered their tab badges. Uses st.components.v1.html so JS actually
    # executes (st.markdown strips <script> tags).
    if _DESIGN_LOADED:
        try:
            inject_global_scripts()
        except Exception:
            pass

if __name__ == "__main__":
    main()
