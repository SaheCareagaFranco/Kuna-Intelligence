"""
components/ui.py
────────────────────────────────────────────────────────────────────────────────
Todos los componentes HTML/CSS del dashboard Kuna Intelligence.
Cada función devuelve HTML listo para st.markdown(..., unsafe_allow_html=True)

Uso en app.py:
    from components.ui import load_css, kpi_card, stat_strip, section_title, logo_html
"""

from pathlib import Path
import pandas as pd
import streamlit as st


# ══════════════════════════════════════════════════════════════════════════════
# CSS LOADER
# ══════════════════════════════════════════════════════════════════════════════

def load_css() -> None:
    """Inyecta todo el CSS de Kuna + Google Fonts en la app."""
    css = """
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800;900&family=Noto+Sans:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
html,body,[class*="css"]{font-family:'Noto Sans',sans-serif!important;background:#F6F7F7!important}
h1,h2,h3,h4,[data-testid="stMetricValue"],[data-testid="stMetricLabel"]{font-family:'Outfit',sans-serif!important}

/* Sidebar */
[data-testid="stSidebar"]{background:#171D1C!important;border-right:1px solid #2C3533!important}
[data-testid="stSidebar"] *{color:#C0CEC9!important}
[data-testid="stSidebar"] .stRadio label{font-size:.84rem!important;padding:8px 10px!important;border-radius:8px!important;cursor:pointer!important}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stDateInput label{font-family:'Outfit',sans-serif!important;font-size:.6rem!important;font-weight:700!important;text-transform:uppercase!important;letter-spacing:.09em!important;color:#748C86!important}
[data-testid="stSidebar"] .stSelectbox>div>div,
[data-testid="stSidebar"] .stMultiSelect>div>div{background:#2C3533!important;border:1px solid #323D3A!important;border-radius:8px!important;color:#C0CEC9!important;font-size:.8rem!important}
[data-testid="stSidebar"] input[type="text"]{background:#2C3533!important;color:#C0CEC9!important}

/* Layout */
.main .block-container{padding:2rem 2.5rem!important;max-width:100%!important}

/* Botones */
.stButton>button{background:#1AC77C!important;color:#fff!important;border:none!important;border-radius:8px!important;font-family:'Outfit',sans-serif!important;font-weight:700!important;font-size:.8rem!important;padding:8px 16px!important;transition:opacity .15s!important}
.stButton>button:hover{opacity:.85!important;border:none!important}
.stDownloadButton>button{background:#002236!important;color:#fff!important;border:none!important;border-radius:8px!important;font-family:'Outfit',sans-serif!important;font-weight:700!important}

/* Inputs */
.stSelectbox>div>div,.stMultiSelect>div>div{background:#fff!important;border:1.5px solid #E0E7E4!important;border-radius:8px!important;font-size:.8rem!important}
.stSelectbox>div>div:focus-within,.stMultiSelect>div>div:focus-within{border-color:#1AC77C!important;box-shadow:0 0 0 3px rgba(26,199,124,.12)!important}
.streamlit-expanderHeader{font-family:'Outfit',sans-serif!important;font-size:.8rem!important;font-weight:700!important;background:#fff!important;border:1px solid #E0E7E4!important;border-radius:12px!important}
hr{border-color:#E0E7E4!important;margin:16px 0!important}

/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:#F6F7F7}
::-webkit-scrollbar-thumb{background:#E0E7E4;border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:#C0CEC9}

/* Plotly transparente */
.js-plotly-plot .plotly .bg{fill:transparent!important}
</style>
"""
    st.markdown(css, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# LOGO
# ══════════════════════════════════════════════════════════════════════════════

LOGO_SVG = """
<svg width="26" height="26" viewBox="0 0 80 80" fill="none" xmlns="http://www.w3.org/2000/svg">
  <g transform="rotate(0,40,40)">
    <path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/>
  </g>
  <g transform="rotate(120,40,40)">
    <path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/>
  </g>
  <g transform="rotate(240,40,40)">
    <path d="M44 40 L44 14 Q44 6 36 6 L28 6 Q20 6 20 14 L20 22 Q20 28 28 28 L36 28 L36 40 Z" fill="#1AC77C"/>
  </g>
</svg>"""

def logo_html() -> str:
    """Logo Kuna para el sidebar."""
    return f"""
<div style="display:flex;align-items:center;gap:10px;padding:8px 4px 20px">
  {LOGO_SVG}
  <div>
    <div style="font-family:'Outfit',sans-serif;font-size:1.25rem;font-weight:900;
                color:#F6F7F7;letter-spacing:-.03em">kuna</div>
    <div style="font-size:.55rem;text-transform:uppercase;letter-spacing:.15em;color:#485856">
      Intelligence</div>
  </div>
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# KPI CARD
# ══════════════════════════════════════════════════════════════════════════════

def kpi_card(label: str, value: str, meta: str = "", badge: str = "") -> str:
    """
    Tarjeta KPI con borde verde izquierdo.

    Uso:
        st.markdown(kpi_card("Contratos", "1,861", "de 3,842 leads", "48.4% conv."),
                    unsafe_allow_html=True)
    """
    badge_html = (
        f'<span style="background:rgba(26,199,124,.12);color:#1AC77C;font-weight:700;'
        f'font-size:.62rem;padding:2px 7px;border-radius:20px;margin-left:4px">{badge}</span>'
        if badge else ""
    )
    return f"""
<div style="background:#fff;border-radius:12px;padding:20px 18px;
            border-left:4px solid #1AC77C;
            box-shadow:0 1px 8px rgba(23,29,28,.08);margin-bottom:4px">
  <div style="font-family:'Outfit',sans-serif;font-size:.62rem;text-transform:uppercase;
              letter-spacing:.1em;color:#748C86;margin-bottom:7px;font-weight:600">
    {label}
  </div>
  <div style="font-family:'Outfit',sans-serif;font-size:1.9rem;font-weight:900;
              color:#171D1C;line-height:1;letter-spacing:-.03em">
    {value}
  </div>
  <div style="font-size:.68rem;color:#748C86;margin-top:5px">
    {meta}{badge_html}
  </div>
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# STAT STRIP
# ══════════════════════════════════════════════════════════════════════════════

def stat_strip(s: pd.Series) -> str:
    """
    Franja de estadísticas del ciclo: Min P10 Q1 Mediana Promedio Q3 P90 Max StdDev

    Uso:
        ciclo = df["ciclo_dias_limpio"].dropna()
        st.markdown(stat_strip(ciclo), unsafe_allow_html=True)
    """
    if s.empty:
        return "<div style='background:#fff;border-radius:12px;padding:14px;text-align:center;color:#748C86'>Sin datos</div>"

    stats = [
        (f"{s.min():.0f}",          "Mín",       False),
        (f"{s.quantile(.10):.1f}",  "P10",       False),
        (f"{s.quantile(.25):.1f}",  "Q1",        False),
        (f"{s.median():.1f}",       "Mediana",   True),
        (f"{s.mean():.1f}",         "Promedio",  True),
        (f"{s.quantile(.75):.1f}",  "Q3",        False),
        (f"{s.quantile(.90):.1f}",  "P90",       False),
        (f"{s.max():.0f}",          "Máx",       False),
        (f"{s.std():.1f}",          "Desv. Est.",False),
    ]
    items = ""
    for val, lbl, hl in stats:
        color = "#1AC77C" if hl else "#171D1C"
        items += f"""
<div style="flex:1;text-align:center;padding:13px 0;border-right:1px solid #E0E7E4">
  <div style="font-family:'Outfit',sans-serif;font-size:1.05rem;font-weight:800;color:{color}">{val}</div>
  <div style="font-size:.58rem;text-transform:uppercase;letter-spacing:.08em;color:#748C86;margin-top:3px">{lbl}</div>
</div>"""
    # quitar border-right del último
    items = items.rstrip()
    last_idx = items.rfind("border-right:1px solid #E0E7E4")
    items = items[:last_idx] + "border-right:none" + items[last_idx+30:]

    return f"""
<div style="display:flex;background:#fff;border-radius:12px;overflow:hidden;
            box-shadow:0 1px 8px rgba(23,29,28,.08);margin-bottom:16px">
  {items}
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# SECTION TITLE
# ══════════════════════════════════════════════════════════════════════════════

def section_title(text: str) -> str:
    """Título de sección en uppercase con estilo Outfit."""
    return f"""
<div style="font-family:'Outfit',sans-serif;font-size:.72rem;font-weight:700;
            text-transform:uppercase;letter-spacing:.1em;color:#748C86;
            margin:20px 0 10px;padding-left:2px">
  {text}
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# FILTROS — contador de registros
# ══════════════════════════════════════════════════════════════════════════════

def records_badge(n_contratos: int, n_total: int) -> str:
    """Chip verde que muestra cuántos registros están filtrados."""
    return f"""
<div style="text-align:right;font-size:.72rem;color:#748C86;margin-bottom:-8px">
  <b style="color:#1AC77C">{n_contratos:,}</b> contratos ·
  <b style="color:#171D1C">{n_total:,}</b> registros filtrados
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# R² CARD  (para Correlaciones)
# ══════════════════════════════════════════════════════════════════════════════

def r2_card(r2: float) -> str:
    """Tarjeta grande con el R² y su interpretación."""
    if r2 >= .5:
        color, label = "#1AC77C", "Correlación fuerte"
    elif r2 >= .25:
        color, label = "#E8A838", "Correlación moderada"
    else:
        color, label = "#748C86", "Correlación débil"

    return f"""
<div style="background:#F6F7F7;border:1px solid #E0E7E4;border-radius:12px;
            padding:18px;text-align:center;margin-bottom:12px">
  <div style="font-family:'Outfit',sans-serif;font-size:.6rem;text-transform:uppercase;
              letter-spacing:.1em;color:#748C86;margin-bottom:6px">Coeficiente R²</div>
  <div style="font-family:'Outfit',sans-serif;font-size:2.2rem;font-weight:900;
              color:{color};letter-spacing:-.04em;line-height:1">{r2:.3f}</div>
  <div style="font-size:.7rem;color:#748C86;margin-top:5px">{label}</div>
</div>"""


# ══════════════════════════════════════════════════════════════════════════════
# PLOTLY — layout base para todas las gráficas
# ══════════════════════════════════════════════════════════════════════════════

PLOT_BASE = dict(
    paper_bgcolor = "rgba(0,0,0,0)",
    plot_bgcolor  = "rgba(0,0,0,0)",
    font          = dict(family="Noto Sans", color="#485856", size=11),
    margin        = dict(t=40, b=28, l=8, r=8),
    legend        = dict(orientation="h", yanchor="bottom", y=-0.30,
                         xanchor="left", x=0, font=dict(size=10)),
    xaxis         = dict(gridcolor="#E0E7E4", linecolor="#C0CEC9", zerolinecolor="#E0E7E4"),
    yaxis         = dict(gridcolor="#E0E7E4", linecolor="#C0CEC9", zerolinecolor="#E0E7E4"),
)

def apply_layout(fig, height: int = 300):
    """Aplica el layout estándar Kuna a cualquier figura Plotly."""
    fig.update_layout(height=height, **PLOT_BASE)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# COLORES  de marca para gráficas
# ══════════════════════════════════════════════════════════════════════════════

GREEN   = "#1AC77C"
GREEN2  = "#1DE08C"
NAVY    = "#002236"
NAVY2   = "#00304C"
AMBER   = "#E8A838"
NEUTRAL = "#748C86"

PALETTE = [GREEN, NAVY, AMBER, GREEN2, "#0074B4", NEUTRAL, "#C0CEC9"]

def green_scale(value: float, low: float, high: float) -> str:
    """
    Devuelve un color verde (intensidad) según posición en [low, high].
    - Por debajo del promedio bajo → verde sólido
    - Alrededor del promedio → verde medio
    - Por encima → verde transparente
    """
    if value <= low:    return GREEN
    if value <= high:   return "rgba(26,199,124,.42)"
    return "rgba(26,199,124,.18)"


# ══════════════════════════════════════════════════════════════════════════════
# FORMAT helpers
# ══════════════════════════════════════════════════════════════════════════════

def fmt_money(v) -> str:
    """Formatea número como dinero compacto: $342K, $1.2M."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"

def fmt_pct(v, decimals=1) -> str:
    try:    return f"{float(v):.{decimals}f}%"
    except: return "—"

def fmt_days(v, decimals=1) -> str:
    try:    return f"{float(v):.{decimals}f} días"
    except: return "—"
