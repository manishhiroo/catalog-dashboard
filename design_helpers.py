"""Design helpers — Python functions that emit HTML matching the
Catalog Health design system (styles.css).

Use from app.py:
    from design_helpers import load_design_system, mcard, tag, panel, dl_raw

    load_design_system()   # once at top of app.py
    st.markdown(mcard("Total SPINs", "2,469", state="good"), unsafe_allow_html=True)
"""
from pathlib import Path
import streamlit as st
import html as _html

BASE_DIR = Path(__file__).parent
CSS_FILE = BASE_DIR / "styles.css"


def load_design_system():
    """Inject styles.css + Inter/JetBrains Mono fonts + global search bar.
    Call once at top of app.py."""
    if not CSS_FILE.exists():
        return False

    # Combine fonts + CSS into a single markdown injection
    # (multiple separate st.markdown calls can race with Streamlit's DOM)
    css = CSS_FILE.read_text(encoding="utf-8")
    html_payload = f"""
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
    {css}
    </style>
    """
    st.markdown(html_payload, unsafe_allow_html=True)
    return True


def global_search_bar(placeholder="Global search — metrics, SPIN ID, item code, tabs..."):
    """Render a global search bar above the page content.
    Returns the search text entered."""
    import streamlit as st
    st.markdown("""
    <div style="display:flex; align-items:center; gap:8px; margin-bottom:16px;
                padding:10px 14px; background:var(--bg-panel, #14171C);
                border:1px solid var(--border, #262B33); border-radius:6px;">
      <span style="color:var(--fg-muted, #9AA3B2); font-size:12px; font-weight:500;
                   text-transform:uppercase; letter-spacing:0.08em;">Global Search</span>
    </div>
    """, unsafe_allow_html=True)
    # Use Streamlit's native input so it's interactive
    q = st.text_input(
        "Search",
        placeholder=placeholder,
        key="global_search",
        label_visibility="collapsed",
    )
    return q


# Suggestion database for global search
SEARCH_INDEX = [
    # Metrics
    {"type": "metric", "label": "Image Health", "target": "Image Health", "icon": "📷"},
    {"type": "metric", "label": "ERP Assortment (BAU)", "target": "ERP Assortment (BAU)", "icon": "📊"},
    {"type": "metric", "label": "ERP Assortment (Events)", "target": "ERP Assortment (Events)", "icon": "📅"},
    {"type": "metric", "label": "Enabled Items Health", "target": "Enabled Items Health", "icon": "✅"},
    {"type": "metric", "label": "Shelf Life Deviation", "target": "Shelf Life Deviation", "icon": "⏰"},
    {"type": "metric", "label": "SPIN Lookup", "target": "SPIN Lookup", "icon": "🔍"},
    {"type": "metric", "label": "Upload Preview", "target": "Upload Preview", "icon": "⬆"},
    {"type": "metric", "label": "QC: Diff Assortment", "target": "QC: Diff Assortment", "icon": "✓"},

    # Tabs within Image Health
    {"type": "tab", "parent": "Image Health", "label": "Health Trends", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Coverage", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Onboarding Health", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Half-Yearly Onboarding", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Slot Standardization", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Defect Detection", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Virtual Combos", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Quality vs BK", "target": "Image Health"},
    {"type": "tab", "parent": "Image Health", "label": "Diff Assortment", "target": "Image Health"},

    # Tabs within ERP BAU
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Overview", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Pod Master", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "NPI vs Old SKU", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Ratings", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Block OTB / Temp Disable", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "City Add/Remove", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "City Expansion", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Pod Tiering", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Brand View", "target": "ERP Assortment (BAU)"},
    {"type": "tab", "parent": "ERP Assortment (BAU)", "label": "Enablement Delta", "target": "ERP Assortment (BAU)"},

    # Tabs within QC
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Image Fulfillment", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Image Count Flags", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Ratings QC", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "ERP Status", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Enablement QC", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Checklist SOP", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Secondary + Tertiary (P999)", "target": "QC: Diff Assortment"},
    {"type": "tab", "parent": "QC: Diff Assortment", "label": "Copy Preview", "target": "QC: Diff Assortment"},

    # SPIN lookup tabs
    {"type": "tab", "parent": "SPIN Lookup", "label": "General (CMS)", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "ERP (per SPIN)", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "Storefront", "target": "SPIN Lookup"},
    {"type": "tab", "parent": "SPIN Lookup", "label": "Logs / Change History", "target": "SPIN Lookup"},
]


def search_suggestions(query):
    """Return matching entries from SEARCH_INDEX sorted by relevance.
    Also detects SPIN ID / Item Code patterns for lookup suggestions."""
    if not query or len(query.strip()) < 2:
        return []
    q = query.lower().strip()
    hits = []

    # SPIN/Item code heuristic
    q_stripped = query.strip()
    if q_stripped.isdigit() and len(q_stripped) >= 3:
        hits.append({
            "type": "lookup", "label": f"Item Code lookup: {q_stripped}",
            "target": "SPIN Lookup", "query": q_stripped,
        })
    elif len(q_stripped) == 10 and q_stripped.isalnum():
        hits.append({
            "type": "lookup", "label": f"SPIN ID lookup: {q_stripped.upper()}",
            "target": "SPIN Lookup", "query": q_stripped.upper(),
        })

    # Fuzzy match against index
    for entry in SEARCH_INDEX:
        label_l = entry["label"].lower()
        if q in label_l:
            hits.append(entry)
        elif all(word in label_l for word in q.split()):
            hits.append(entry)

    # Sort: exact match first, then prefix, then contains
    def _score(h):
        lab = h["label"].lower()
        if lab == q: return 0
        if lab.startswith(q): return 1
        if q in lab: return 2
        return 3
    hits.sort(key=_score)
    return hits[:10]


def _esc(s):
    return _html.escape(str(s)) if s is not None else ""


def mcard(label, value, unit=None, state=None, delta=None, delta_dir=None, delta_period=None,
          target_pct=None, target_goal=None, sub=None):
    """Rich metric card with state stripe, big value, optional delta or target bar.

    state: None, "good", "warn", "critical", "info", "muted"
    delta_dir: "up" | "down" | "up-bad" | "down-good"
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

    sub_html = f'<div class="page-sub" style="margin-top:4px;font-size:11px">{_esc(sub)}</div>' if sub else ""

    return f"""
    <div class="mcard{state_cls}">
      <div class="mlabel"><span class="mlabel-text">{_esc(label)}</span>{state_dot}</div>
      <div class="mvalue">{_esc(value)}{unit_html}</div>
      {sub_html}
      {target_html}
      {delta_html}
    </div>
    """


def mcard_grid(cards_html_list):
    """Wrap a list of mcard HTML strings in a responsive grid."""
    body = "\n".join(cards_html_list)
    return f'<div class="metric-grid">{body}</div>'


def tag(text, kind="muted"):
    """Pill-style tag. kind: critical|warn|good|info|muted|accent"""
    return f'<span class="tag {kind}">{_esc(text)}</span>'


def dot_tag(text, kind="good"):
    return f'<span class="dot-tag"><span class="dot {kind}"></span>{_esc(text)}</span>'


def bar_cell(pct, width_px=130):
    """Horizontal progress bar cell with value. Auto-color based on threshold."""
    try:
        p = float(pct)
    except Exception:
        p = 0
    if p < 50:
        color = "var(--critical)"
    elif p < 75:
        color = "var(--warn)"
    else:
        color = "var(--good)"
    return f"""
    <div class="bar-cell" style="min-width:{width_px}px">
      <div class="bar-track"><div class="bar-fill" style="width:{p}%;background:{color}"></div></div>
      <span class="bar-val">{p:.1f}%</span>
    </div>"""


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


def panel(title, body_html, sub=None, actions_html="", flush=False):
    """Card panel with header + body. Use for sections within a page."""
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
    """Banner alert with icon, title, sub, actions. kind: critical|warn|info|good"""
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
    """Empty state with title + sub + CTAs."""
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
    """Small 'Download raw' button for panel headers (visual only — for real download use st.download_button)."""
    rows_txt = f" · {rows:,} rows" if rows else ""
    return f'<button class="dl-raw">⬇ {_esc(label)}<span class="fmt">{_esc(fmt)}{rows_txt}</span></button>'


def sync_card(ts_text, label="Synced"):
    """Sidebar sync indicator — pulsing green dot + timestamp."""
    return f"""
    <div class="sync-card">
      <span class="sync-dot"></span>
      <span>{_esc(label)}</span>
      <span class="sync-time">{_esc(ts_text)}</span>
    </div>
    """


def brand_header(name="Catalog Health", env="PROD"):
    """Sidebar brand mark + name + env pill."""
    return f"""
    <div class="brand">
      <div class="brand-mark">🔶</div>
      <div class="brand-name">{_esc(name)}</div>
      <div class="brand-env">{_esc(env)}</div>
    </div>
    """


# Convenience: render to Streamlit
def render(html_string):
    """Shortcut: st.markdown(html, unsafe_allow_html=True)"""
    st.markdown(html_string, unsafe_allow_html=True)


def render_metrics(specs):
    """Render a grid of rich metric cards from a list of dicts.

    Each spec: {
        "label": str,
        "value": str,            # Already formatted (e.g. "2,469")
        "unit": str = None,      # e.g. "%"
        "state": str = None,     # good|warn|critical|info|muted
        "delta": str = None,
        "delta_dir": str = None, # up|down|up-bad|down-good
        "delta_period": str = None,  # DoD|WoW|MoM
        "sub": str = None,
        "target_pct": float = None,
        "target_goal": float = None,
    }
    """
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
        ))
    st.markdown(mcard_grid(cards), unsafe_allow_html=True)


def styled_table(rows, columns, row_class_fn=None, col_formatters=None, max_height=460):
    """Build a HTML table with design-system styling and optional row tints.

    rows: list of dicts or list of lists
    columns: list of dicts [{name, key, num=False, type="text"|"bar"|"tag"|"mono"}]
    row_class_fn: fn(row_dict) -> "critical" | "warn" | "good" | "" (row tint)
    col_formatters: {col_key: fn(value) -> html}
    """
    col_formatters = col_formatters or {}

    # Header
    thead_parts = []
    for c in columns:
        num_cls = " num" if c.get("num") else ""
        thead_parts.append(f'<th class="{num_cls.strip()}">{_esc(c["name"])}</th>')
    thead = f'<thead><tr>{"".join(thead_parts)}</tr></thead>'

    # Body
    tbody_rows = []
    for r in rows:
        row_dict = r if isinstance(r, dict) else {c["key"]: v for c, v in zip(columns, r)}
        tr_cls = row_class_fn(row_dict) if row_class_fn else ""
        cells = []
        for c in columns:
            key = c["key"]
            raw_val = row_dict.get(key, "")
            if key in col_formatters:
                cell_html = col_formatters[key](raw_val)
            elif c.get("type") == "bar":
                try:
                    cell_html = bar_cell(float(raw_val))
                except Exception:
                    cell_html = _esc(raw_val)
            elif c.get("type") == "mono":
                cell_html = f'<span class="mono" style="font-size:11px">{_esc(raw_val)}</span>'
            elif c.get("type") == "tag":
                cell_html = tag(raw_val, c.get("tag_kind", "muted"))
            else:
                cell_html = _esc(raw_val)
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
