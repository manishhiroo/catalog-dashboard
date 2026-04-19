"""Design helpers — Python functions that emit HTML matching the
Catalog Health design system (styles.css).

Full implementation of Claude Design v2 mockup for Streamlit.
Per STREAMLIT_IMPLEMENTATION.md: avoid st.metric/st.radio/st.tabs — use HTML.
"""
from pathlib import Path
import streamlit as st
import html as _html

BASE_DIR = Path(__file__).parent
CSS_FILE = BASE_DIR / "styles.css"


def load_design_system():
    """Inject styles.css + Inter/JetBrains Mono fonts. Call once at top of app.py."""
    if not CSS_FILE.exists():
        st.error(f"⚠ Design CSS not found at {CSS_FILE}")
        return False

    css = CSS_FILE.read_text(encoding="utf-8")
    fonts_html = """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    """
    st.markdown(fonts_html, unsafe_allow_html=True)
    st.markdown(
        f"<style id='catalog-health-design-system' data-version='v2'>{css}</style>",
        unsafe_allow_html=True
    )
    return True


def _esc(s):
    return _html.escape(str(s)) if s is not None else ""


# ── Core components ─────────────────────────────────────────────────────────

def mcard(label, value, unit=None, state=None, delta=None, delta_dir=None, delta_period=None,
          target_pct=None, target_goal=None, sub=None, spark=None):
    """Rich metric card with state stripe, big value, optional delta/target/sparkline.

    state: None, "good", "warn", "critical", "info", "muted"
    delta_dir: "up" | "down" | "up-bad" | "down-good"
    spark: list of numeric values for sparkline (optional)
    """
    state_cls = f" {state}" if state else ""
    state_dot = ""
    if state:
        state_text = {
            "critical": "Action needed",
            "warn": "Below target",
            "good": "On target",
            "info": "Live",
            "muted": "Reference",
        }.get(state, "")
        state_dot = f'<span class="state-dot"><span class="d"></span>{_esc(state_text)}</span>'

    unit_html = f'<span class="unit">{_esc(unit)}</span>' if unit else ""

    delta_html = ""
    if delta is not None:
        dir_cls = delta_dir or "up"
        arrow = {"up": "↑", "down": "↓", "up-bad": "↑", "down-good": "↓"}.get(dir_cls, "—")
        period = f'<span class="period">{_esc(delta_period)}</span>' if delta_period else ""
        delta_html = f'<div class="mdeltas"><span class="delta {dir_cls}">{arrow}{_esc(delta)}{period}</span></div>'

    target_html = ""
    if target_pct is not None and target_goal is not None:
        target_html = f"""
        <div class="target-bar">
          <div class="target-bar-track">
            <div class="target-bar-fill" style="width:{target_pct}%"></div>
            <div class="target-bar-target" style="left:{target_goal}%"></div>
          </div>
          <div class="target-bar-meta"><span>{target_pct:.1f}% actual</span><span>Target {target_goal}%</span></div>
        </div>"""

    spark_html = ""
    if spark and len(spark) >= 2:
        spark_html = _sparkline_svg(spark, state)

    sub_html = f'<div class="page-sub" style="margin-top:4px;font-size:11px">{_esc(sub)}</div>' if sub else ""

    return f"""
    <div class="mcard{state_cls}">
      <div class="mlabel"><span class="mlabel-text">{_esc(label)}</span>{state_dot}</div>
      <div class="mvalue">{_esc(value)}{unit_html}</div>
      {sub_html}
      {spark_html}
      {target_html}
      {delta_html}
    </div>
    """


def _sparkline_svg(data, state=None, w=100, h=28):
    """Render a mini SVG sparkline."""
    if not data or len(data) < 2:
        return ""
    color = {
        "critical": "#F43F5E", "warn": "#F59E0B",
        "good": "#10B981", "info": "#38BDF8",
    }.get(state, "#F97316")
    mn, mx = min(data), max(data)
    rng = (mx - mn) or 1
    step = w / (len(data) - 1)
    pts = [(i * step, h - ((v - mn) / rng) * (h - 4) - 2) for i, v in enumerate(data)]
    line = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(pts))
    return f"""
    <svg class="sparkline" viewBox="0 0 {w} {h}" preserveAspectRatio="none">
      <path d="{line}" stroke="{color}" stroke-width="1.5" fill="none"/>
      <circle cx="{pts[-1][0]:.1f}" cy="{pts[-1][1]:.1f}" r="2" fill="{color}"/>
    </svg>"""


def mcard_grid(cards_html_list):
    """Wrap list of mcard HTML into a responsive grid."""
    body = "\n".join(cards_html_list)
    return f'<div class="metric-grid">{body}</div>'


def render_metrics(specs):
    """Render grid of mcards from list of dicts. See mcard() for spec keys."""
    cards = []
    for s in specs:
        cards.append(mcard(
            label=s.get("label", ""),
            value=s.get("value", ""),
            unit=s.get("unit"),
            state=s.get("state"),
            delta=s.get("delta"),
            delta_dir=s.get("delta_dir"),
            delta_period=s.get("delta_period"),
            target_pct=s.get("target_pct"),
            target_goal=s.get("target_goal"),
            sub=s.get("sub"),
            spark=s.get("spark"),
        ))
    st.markdown(mcard_grid(cards), unsafe_allow_html=True)


def tag(text, kind="muted"):
    """Pill-style tag. kind: critical|warn|good|info|muted|accent"""
    return f'<span class="tag {kind}">{_esc(text)}</span>'


def dot_tag(text, kind="good"):
    return f'<span class="dot-tag"><span class="dot {kind}"></span>{_esc(text)}</span>'


def bar_cell(pct, width_px=130, label=None):
    """Horizontal progress bar with % value. Auto-colored."""
    try:
        p = float(pct)
    except Exception:
        p = 0
    color = "var(--critical)" if p < 50 else ("var(--warn)" if p < 75 else "var(--good)")
    lbl = label if label is not None else f"{p:.1f}%"
    return f"""
    <div class="bar-cell" style="min-width:{width_px}px">
      <div class="bar-track"><div class="bar-fill" style="width:{p}%;background:{color}"></div></div>
      <span class="bar-val">{_esc(lbl)}</span>
    </div>"""


# ── Layout primitives ───────────────────────────────────────────────────────

def page_header(title, sub=None, badge=None, actions_html=""):
    """Big page title + subtitle + optional badge + right-side actions."""
    badge_html = f' {tag(badge, "accent")}' if badge else ""
    sub_html = f'<div class="page-sub">{_esc(sub)}</div>' if sub else ""
    return f"""
    <div class="page-head">
      <div>
        <h1 class="page-title">{_esc(title)}{badge_html}</h1>
        {sub_html}
      </div>
      <div class="page-head-actions">{actions_html}</div>
    </div>
    """


def panel_begin(title, sub=None, actions_html="", flush=False):
    """Open a panel — call panel_end() to close.
    actions_html goes in the top-right (e.g. dl_raw_button())."""
    sub_html = f' <span class="panel-sub">{_esc(sub)}</span>' if sub else ""
    flush_cls = " flush" if flush else ""
    st.markdown(f"""
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">{_esc(title)}{sub_html}</div>
        <div class="panel-actions">{actions_html}</div>
      </div>
      <div class="panel-body{flush_cls}">
    """, unsafe_allow_html=True)


def panel_end():
    st.markdown("</div></div>", unsafe_allow_html=True)


def panel(title, body_html, sub=None, actions_html="", flush=False):
    """One-shot panel that wraps HTML body."""
    sub_html = f' <span class="panel-sub">{_esc(sub)}</span>' if sub else ""
    flush_cls = " flush" if flush else ""
    return f"""
    <div class="panel">
      <div class="panel-head">
        <div class="panel-title">{_esc(title)}{sub_html}</div>
        <div class="panel-actions">{actions_html}</div>
      </div>
      <div class="panel-body{flush_cls}">{body_html}</div>
    </div>
    """


def alert_banner(title, sub, kind="info", actions_html=""):
    """Banner alert: critical|warn|info|good"""
    return f"""
    <div class="alert-banner {kind}">
      <div class="ab-body">
        <div class="ab-title">{_esc(title)}</div>
        <div class="ab-sub">{_esc(sub)}</div>
      </div>
      <div class="ab-actions">{actions_html}</div>
    </div>
    """


def empty_state(title, sub=None, actions_html=""):
    sub_html = f'<div class="e-sub">{_esc(sub)}</div>' if sub else ""
    actions_wrap = f'<div class="e-actions">{actions_html}</div>' if actions_html else ""
    return f"""
    <div class="empty">
      <div class="e-title">{_esc(title)}</div>
      {sub_html}
      {actions_wrap}
    </div>
    """


def dl_raw_button(label="Raw", fmt="CSV", rows=None):
    """Visual download raw button (for panel headers). Pair with st.download_button for real download."""
    rows_txt = f" · {rows:,} rows" if rows else ""
    return f'<button class="dl-raw">⬇ {_esc(label)}<span class="fmt">{_esc(fmt)}{rows_txt}</span></button>'


def sync_card(ts_text, label="Synced"):
    """Pulsing green dot + timestamp for sidebar."""
    return f"""
    <div class="sync-card">
      <span class="sync-dot"></span>
      <span>{_esc(label)}</span>
      <span class="sync-time">{_esc(ts_text)}</span>
    </div>
    """


def brand_header(name="Catalog Health", env="PROD"):
    return f"""
    <div class="brand">
      <div class="brand-mark">🔶</div>
      <div class="brand-name">{_esc(name)}</div>
      <div class="brand-env">{_esc(env)}</div>
    </div>
    """


def render(html_string):
    st.markdown(html_string, unsafe_allow_html=True)


# ── Feather-style SVG icons (matches components.jsx) ─────────────────────────

_ICON_PATHS = {
    "image":   '<rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21,15 16,10 5,21"/>',
    "grid":    '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>',
    "calendar":'<rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    "heart":   '<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>',
    "clock":   '<circle cx="12" cy="12" r="10"/><polyline points="12,6 12,12 16,14"/>',
    "search":  '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    "upload":  '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17,8 12,3 7,8"/><line x1="12" y1="3" x2="12" y2="15"/>',
    "clipboard":'<path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><rect x="8" y="2" width="8" height="4" rx="1"/>',
    "eye":     '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>',
    "bell":    '<path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/>',
    "refresh": '<polyline points="23,4 23,10 17,10"/><polyline points="1,20 1,14 7,14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>',
    "info":    '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>',
    "download":'<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7,10 12,15 17,10"/><line x1="12" y1="15" x2="12" y2="3"/>',
    "star":    '<polygon points="12,2 15.09,8.26 22,9.27 17,14.14 18.18,21.02 12,17.77 5.82,21.02 7,14.14 2,9.27 8.91,8.26"/>',
    "chevron_right": '<polyline points="9,18 15,12 9,6"/>',
}


def svg_icon(name, size=15):
    """Feather-style line SVG icon by name."""
    body = _ICON_PATHS.get(name, "")
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
        f'stroke="currentColor" stroke-width="1.75" stroke-linecap="round" '
        f'stroke-linejoin="round" class="icon">{body}</svg>'
    )


# ── Sidebar (custom HTML replacing st.radio) ────────────────────────────────

def sidebar_nav_item(key, label, active_key=None, badge=None, badge_kind=None, icon=None, live=False, query_key="view"):
    """Single sidebar nav item. Clicks set `?{query_key}={key}`.

    badge: number or string; if "live" a green dot is shown.
    badge_kind: critical|warn|good|info (affects count color class).
    icon: feather icon name (see _ICON_PATHS).
    """
    active_cls = " active" if key == active_key else ""
    badge_html = ""
    if live:
        badge_html = '<span class="live-dot" title="Live"></span>'
    elif badge is not None and str(badge) != "":
        kind_cls = f" {badge_kind}" if badge_kind else ""
        badge_html = f'<span class="count{kind_cls}">{_esc(badge)}</span>'

    icon_html = svg_icon(icon, 15) if icon else ""

    # Use query_params href so Streamlit reruns with the new view
    return (
        f'<a class="nav-item{active_cls}" href="?{query_key}={_esc(key)}" target="_self">'
        f'{icon_html}'
        f'<span class="nav-label">{_esc(label)}</span>'
        f'{badge_html}'
        f'</a>'
    )


def sidebar_nav_group(group_title, items, active_key, query_key="view", right_html=""):
    """Sidebar section: group title + list of nav items.

    items: list of dicts with keys: key, label, icon, badge, badge_kind, live
    """
    item_html = "".join(
        sidebar_nav_item(
            key=it["key"], label=it["label"], active_key=active_key,
            badge=it.get("badge"), badge_kind=it.get("badge_kind"),
            icon=it.get("icon"), live=it.get("live", False),
            query_key=query_key,
        )
        for it in items
    )
    right = f'<span class="sb-section-action">{right_html}</span>' if right_html else ""
    return (
        f'<div class="sb-section"><span class="sb-section-title">{_esc(group_title)}</span>{right}</div>'
        f'<div class="sb-group">{item_html}</div>'
    )


def render_nav(active_key, groups, query_key="view"):
    """Full sidebar nav HTML.

    groups: list of dicts: {"title": str, "items": [...]}
    items each: {key, label, icon, badge?, badge_kind?, live?}
    """
    return "".join(
        sidebar_nav_group(g["title"], g["items"], active_key, query_key=query_key)
        for g in groups
    )


def sidebar_section(title, items_html, right_html=""):
    """Legacy section wrapper (kept for compat)."""
    right = f'<span class="sb-section-action">{right_html}</span>' if right_html else ""
    return (
        f'<div class="sb-section"><span class="sb-section-title">{_esc(title)}</span>{right}</div>'
        f'{items_html}'
    )


# ── Topbar (sticky, with breadcrumb + search + bell) ────────────────────────

def topbar_html(breadcrumb_parts, alert_count=0, current_q="",
                search_placeholder="Search metrics, SPIN / item code, or any tab…"):
    """Render topbar HTML with a REAL <input> that writes to ?q= on Enter.

    The input preserves its value from `current_q` so the server-side Python
    can read it via `st.query_params.get("q", "")`. ⌘K focuses it.
    """
    crumbs = []
    for i, p in enumerate(breadcrumb_parts):
        is_last = i == len(breadcrumb_parts) - 1
        cls = "current" if is_last else ""
        crumbs.append(f'<span class="{cls}">{_esc(p)}</span>')
        if not is_last:
            crumbs.append('<span class="sep">/</span>')
    crumb_html = "".join(crumbs)

    badge_html = f'<span class="badge">{alert_count}</span>' if alert_count > 0 else ""
    q_val = _esc(current_q) if current_q else ""

    return f"""
    <div class="topbar">
      <div class="breadcrumb">{crumb_html}</div>
      <div class="gsearch">
        <div class="gsearch-input">
          {svg_icon("search", 14)}
          <input id="global-search-input" type="text"
                 placeholder="{_esc(search_placeholder)}"
                 value="{q_val}"
                 autocomplete="off" spellcheck="false" />
          <span class="kbd">⌘K</span>
        </div>
      </div>
      <div class="topbar-actions">
        <button class="icon-btn" title="Alerts">{svg_icon("bell", 15)}{badge_html}</button>
        <button class="icon-btn" title="Refresh data" onclick="window.location.reload()">{svg_icon("refresh", 15)}</button>
        <button class="icon-btn" title="Help">{svg_icon("info", 15)}</button>
        <button class="btn sm">{svg_icon("download", 12)} Export</button>
      </div>
    </div>
    <script>
      (function() {{
        const input = document.getElementById('global-search-input');
        if (!input || input.dataset.wired === '1') return;
        input.dataset.wired = '1';

        function commit() {{
          const url = new URL(window.location.href);
          const v = input.value.trim();
          if (v) url.searchParams.set('q', v);
          else   url.searchParams.delete('q');
          window.location.href = url.toString();
        }}

        input.addEventListener('keydown', (e) => {{
          if (e.key === 'Enter') {{ e.preventDefault(); commit(); }}
          if (e.key === 'Escape') {{ input.value = ''; input.blur(); commit(); }}
        }});

        // ⌘K / Ctrl+K focuses the search
        window.addEventListener('keydown', (e) => {{
          if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {{
            e.preventDefault();
            input.focus();
            input.select();
          }}
        }});
      }})();
    </script>
    """


# ── Custom tabs (with count badges) ──────────────────────────────────────────

def sub_tabs_html(tabs, active_key):
    """Render sub-tabs with optional count badges.
    tabs: list of dicts [{key, label, num=None, num_kind=None}]
    Returns the active_key based on query param (consumer reads st.query_params)."""
    parts = []
    for t in tabs:
        k = t["key"]
        active = " active" if k == active_key else ""
        num_html = ""
        if t.get("num") is not None:
            kind_style = ""
            if t.get("num_kind") == "critical":
                kind_style = 'style="background:var(--critical-bg);color:var(--critical);"'
            elif t.get("num_kind") == "warn":
                kind_style = 'style="background:var(--warn-bg);color:var(--warn);"'
            elif t.get("num_kind") == "good":
                kind_style = 'style="background:var(--good-bg);color:var(--good);"'
            num_html = f'<span class="num" {kind_style}>{t["num"]}</span>'
        parts.append(
            f'<a class="sub-tab{active}" href="?tab={_esc(k)}" target="_self">{_esc(t["label"])}{num_html}</a>'
        )
    return f'<div class="sub-tabs">{"".join(parts)}</div>'


def custom_tabs(tabs, state_key="tab", default_key=None):
    """Render tabs and return the active key using st.query_params.
    tabs: list of dicts [{key, label, num=None, num_kind=None}]

    Note: this doesn't replace tab CONTENT rendering — caller still uses if/else.
    """
    active = st.query_params.get(state_key, default_key or tabs[0]["key"])
    st.markdown(sub_tabs_html(tabs, active), unsafe_allow_html=True)
    return active


# ── Styled table (with row tints, bar cells) ────────────────────────────────

def styled_table(rows, columns, row_class_fn=None, col_formatters=None, max_height=460):
    """Build HTML table with design-system styling.

    rows: list of dicts
    columns: list of dicts [{name, key, num=False, type="text"|"bar"|"tag"|"mono"}]
    row_class_fn: fn(row_dict) -> "critical" | "warn" | "good" | ""
    col_formatters: {col_key: fn(value) -> html}
    """
    col_formatters = col_formatters or {}

    thead_parts = []
    for c in columns:
        num_cls = " num" if c.get("num") else ""
        thead_parts.append(f'<th class="{num_cls.strip()}">{_esc(c["name"])}</th>')
    thead = f'<thead><tr>{"".join(thead_parts)}</tr></thead>'

    tbody_rows = []
    for r in rows:
        row_dict = r if isinstance(r, dict) else {c["key"]: v for c, v in zip(columns, r)}
        tr_cls = row_class_fn(row_dict) if row_class_fn else ""
        cells = []
        for c in columns:
            key = c["key"]
            raw = row_dict.get(key, "")
            if key in col_formatters:
                cell_html = col_formatters[key](raw)
            elif c.get("type") == "bar":
                try: cell_html = bar_cell(float(raw))
                except Exception: cell_html = _esc(raw)
            elif c.get("type") == "mono":
                cell_html = f'<span class="mono" style="font-size:11px">{_esc(raw)}</span>'
            elif c.get("type") == "tag":
                cell_html = tag(raw, c.get("tag_kind", "muted"))
            else:
                cell_html = _esc(raw)
            num_cls = " num" if c.get("num") else ""
            cells.append(f'<td class="{num_cls.strip()}">{cell_html}</td>')
        tbody_rows.append(f'<tr class="{tr_cls}">{"".join(cells)}</tr>')
    tbody = f'<tbody>{"".join(tbody_rows)}</tbody>'

    return f"""
    <div class="tbl-wrap" style="max-height:{max_height}px">
      <table class="tbl">
        {thead}
        {tbody}
      </table>
    </div>
    """


# ── Global search ───────────────────────────────────────────────────────────

def global_search_bar(placeholder="Global search — metrics, SPIN ID, item code, tabs..."):
    """Render global search input. Returns the entered text."""
    q = st.text_input(
        "Search", placeholder=placeholder, key="global_search",
        label_visibility="collapsed",
    )
    return q


SEARCH_INDEX = [
    {"type": "metric", "label": "Image Health", "target": "Image Health", "icon": "📷"},
    {"type": "metric", "label": "ERP Assortment (BAU)", "target": "ERP Assortment (BAU)", "icon": "📊"},
    {"type": "metric", "label": "ERP Assortment (Events)", "target": "ERP Assortment (Events)", "icon": "📅"},
    {"type": "metric", "label": "Enabled Items Health", "target": "Enabled Items Health", "icon": "✅"},
    {"type": "metric", "label": "Shelf Life Deviation", "target": "Shelf Life Deviation", "icon": "⏰"},
    {"type": "metric", "label": "SPIN Lookup", "target": "SPIN Lookup", "icon": "🔍"},
    {"type": "metric", "label": "Upload Preview", "target": "Upload Preview", "icon": "⬆"},
    {"type": "metric", "label": "QC: Diff Assortment", "target": "QC: Diff Assortment", "icon": "✓"},
    # Image Health tabs
    {"type": "tab", "parent": "Image Health", "label": "Health Trends", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Coverage", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Onboarding Health", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Defect Detection", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Virtual Combos", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Quality vs BK", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Diff Assortment", "target": "Image Health"},
    # ERP BAU tabs
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Overview", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Pod Master", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Block OTB", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Pod Tiering", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Enablement Delta", "target": "ERP Assortment (BAU)"},
    # QC tabs
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Image Fulfillment", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Image Count Flags", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Checklist SOP", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Secondary Tertiary P999", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Copy Preview", "target": "QC: Diff Assortment"},
    # SPIN Lookup tabs
    {"type": "tab", "parent": "SPIN Lookup", "label": "General CMS", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "ERP per SPIN", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "Storefront", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "Logs Change History", "target": "SPIN Lookup"},
]


def search_suggestions(query):
    """Return matching entries. Also detects SPIN/Item Code for lookup."""
    if not query or len(query.strip()) < 2:
        return []
    q = query.lower().strip()
    hits = []

    q_stripped = query.strip()
    if q_stripped.isdigit() and len(q_stripped) >= 3:
        hits.append({
            "type": "lookup", "label": f"Item Code lookup: {q_stripped}",
            "target": "SPIN Lookup", "query": q_stripped, "icon": "🔎",
        })
    elif len(q_stripped) == 10 and q_stripped.isalnum():
        hits.append({
            "type": "lookup", "label": f"SPIN ID lookup: {q_stripped.upper()}",
            "target": "SPIN Lookup", "query": q_stripped.upper(), "icon": "🔎",
        })

    for entry in SEARCH_INDEX:
        label_l = entry["label"].lower()
        if q in label_l:
            hits.append(entry)
        elif all(word in label_l for word in q.split()):
            hits.append(entry)

    def _score(h):
        lab = h["label"].lower()
        if lab == q: return 0
        if lab.startswith(q): return 1
        if q in lab: return 2
        return 3
    hits.sort(key=_score)

    # Dedup
    seen = set()
    out = []
    for h in hits:
        key = (h.get("type"), h.get("label"), h.get("target"))
        if key not in seen:
            seen.add(key)
            out.append(h)
    return out[:12]
