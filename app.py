from config.secrets_manager import load_env_secure
load_env_secure(
    prefer_plain=True,
    enc_path="config/.env.enc",
    pass_env_var="MAPAS_SECRET_PASSPHRASE",
    cache=False
)

import os
import re
import time
import logging
import streamlit as st
import pandas as pd
from pathlib import Path
from PIL import Image
import base64
import validators
from datetime import datetime

from mapa_muestras import generar_mapa_muestras_visual

# ── Excepciones internas de Streamlit que NO deben capturarse ─────────────────
# Streamlit usa StopException para detener el script cuando el usuario presiona
# "Stop" o cuando se produce un rerun. RuntimeStoppedError ocurre si llega un
# mensaje mientras el runtime está terminando. Ambas deben relanzarse siempre.
try:
    from streamlit.runtime.scriptrunner.exceptions import StopException as _StopException
except ImportError:
    _StopException = None

try:
    from streamlit.runtime.runtime import RuntimeStoppedError as _RuntimeStoppedError
except ImportError:
    _RuntimeStoppedError = None

_ST_STOP_EXC = tuple(e for e in [_StopException, _RuntimeStoppedError] if e is not None)

# ── Entorno ───────────────────────────────────────────────────────────────────
ENVIRONMENT  = os.getenv("ENVIRONMENT", "development")
FLASK_SERVER = os.getenv("FLASK_SERVER_URL", "http://localhost:5000") \
               if ENVIRONMENT == "production" else "http://localhost:5000"

if not validators.url(FLASK_SERVER) and not FLASK_SERVER.startswith("http://localhost"):
    raise ValueError(f"FLASK_SERVER_URL no es una URL válida: {FLASK_SERVER}")

print(f"Servidor activo en: {FLASK_SERVER} | Entorno: {ENVIRONMENT}")

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="errors.log",
)

def manejar_error(funcion, *args, **kwargs):
    try:
        return funcion(*args, **kwargs)
    except _ST_STOP_EXC:
        # Detención normal de Streamlit (Stop / rerun): relanzar siempre
        raise
    except Exception as e:
        # logging.exception registra el traceback completo → facilita depuración
        logging.exception(f"Error en {funcion.__name__}")
        st.error(f"❌ Ocurrió un error en {funcion.__name__}. Revisa los logs.")
        return None

# ── Marca y rutas ─────────────────────────────────────────────────────────────
APP_TITLE = "Atlas TA"
BASE_DIR   = Path(__file__).resolve().parent
LOGO_FILE  = BASE_DIR / "static" / "img" / "Atlas_TA.png"

def img_to_b64(p: Path) -> str:
    return base64.b64encode(p.read_bytes()).decode("utf-8")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title=APP_TITLE,
    page_icon=Image.open(LOGO_FILE) if LOGO_FILE.exists() else "🗺️",
    layout="wide",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
:root{
    --primary:#5B21B6; --primary-600:#6D28D9; --accent:#FACC15;
    --bg:#F8F7FF; --card:#FFFFFF; --text:#1F1A2F; --muted:#6B7280; --border:#E5E7EB;
    --bg-dark:#0F1116; --card-dark:#161923; --text-dark:#EAEAF0;
    --muted-dark:#A3A8B3; --border-dark:#2A2F3A;
}
.block-container { max-width: 1100px; }
.hero-wrap{ display:flex; justify-content:center; margin: 12px 0 24px 0; }
.hero{ display:flex; flex-direction:column; align-items:center; text-align:center; gap:12px; }
.hero .logo{ width:280px; height:auto; display:block; }
.hero .tagline{ font-weight:600; font-size:clamp(18px,2.6vw,26px); margin-top:4px; color:var(--text,#1F1A2F); }
@media (max-width:820px){
    .hero{ flex-direction:column; text-align:center; gap:10px; }
    .hero .logo{ width:140px; }
}
h1,h2,h3{ letter-spacing:-0.015em; }
.muted{ color:var(--muted); }
.pill{ display:inline-block; padding:6px 12px; border:1px solid var(--border);
    border-radius:999px; font-size:.9rem; color:var(--text); background:#F9FAFB; }
a.pill{ display:block; text-align:center; text-decoration:none; color:#fff;
    background:var(--primary); border:1px solid var(--primary);
    padding:10px 14px; border-radius:10px; font-weight:600; }
a.pill:hover{ background:var(--primary-600); border-color:var(--primary-600); }
.btn-row{ display:flex; justify-content:center; }
.btn-row > div{ width:320px; }
@media (prefers-color-scheme:dark){
    .block-container{ background:var(--bg-dark); }
    .muted{ color:var(--muted-dark); }
    body,.stMarkdown,.stText,.stRadio,.stSelectbox,.stMultiSelect{ color:var(--text-dark) !important; }
    a.pill{ background:var(--primary-600); border-color:var(--primary-600); }
    a.pill:hover{ filter:brightness(1.05); }
    .hero .tagline{ color:var(--muted-dark,#A3A8B3); }
}
</style>
""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
logo_b64 = img_to_b64(LOGO_FILE) if LOGO_FILE.exists() else None
st.markdown(
    f"""
    <div class="hero-wrap">
        <div class="hero">
            {'<img class="logo" src="data:image/png;base64,' + logo_b64 + '" alt="Atlas TA">' if logo_b64 else ''}
            <div class="tagline">El mapa de tu operación</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Sidebar — ciudad ──────────────────────────────────────────────────────────
st.sidebar.header("Seleccione una ciudad")
CIUDADES = ["Barranquilla", "Bogotá", "Bucaramanga", "Cali", "Manizales", "Medellín", "Pereira"]
ciudad = st.sidebar.radio("Ciudad:", CIUDADES, index=3)

# Limpiar estado al cambiar ciudad
if st.session_state.get("last_ciudad") != ciudad:
    st.session_state["map_url"]               = None
    st.session_state["muestras_last_filename"] = None
    st.session_state["muestras_export_df"]     = None
    st.session_state["muestras_export_meta"]   = None
    st.session_state["map_auto_opened"]        = False
    st.session_state["last_ciudad"]            = ciudad

st.divider()

# ── Formulario de filtros ─────────────────────────────────────────────────────
with st.form(key="filtros_form"):
    c1, c2 = st.columns(2)
    with c1:
        fecha_inicio = st.date_input("Fecha de Inicio")
    with c2:
        fecha_fin = st.date_input("Fecha de Fin")

    agrupar_por = st.selectbox(
        "Agrupar por:",
        options=["Promotor", "Mes"],
        index=0,
    )

    _, col_btn, _ = st.columns([1, 1, 1])
    with col_btn:
        submit_button = st.form_submit_button(
            "Generar Mapa", use_container_width=True, type="primary"
        )

# ── Placeholder del link de mapa ──────────────────────────────────────────────
link_placeholder = st.empty()

# ── Descarga HTML ─────────────────────────────────────────────────────────────
map_filename = st.session_state.get("muestras_last_filename")
html_path    = os.path.join("static", "maps", map_filename) if map_filename else None

if map_filename and html_path and os.path.exists(html_path):
    ciudad_slug  = re.sub(r'[^A-Za-z0-9]', '', ciudad.upper()
                          .replace('Á','A').replace('É','E')
                          .replace('Í','I').replace('Ó','O').replace('Ú','U'))
    filename_dl  = f"Mapa_Muestras_{ciudad_slug}_{datetime.now().strftime('%Y%m%d')}.html"
    with open(html_path, "rb") as f:
        html_bytes = f.read()
    st.download_button(
        label="📥 Descargar HTML del mapa",
        data=html_bytes, file_name=filename_dl, mime="text/html",
        type="secondary", use_container_width=True,
    )
else:
    st.button("📥 Descargar HTML del mapa", disabled=True,
              type="secondary", use_container_width=True,
              help="Genera un mapa primero.")

# ── Descarga CSV ──────────────────────────────────────────────────────────────
df_export   = st.session_state.get("muestras_export_df")
export_meta = st.session_state.get("muestras_export_meta")

def _fmt_date(x) -> str:
    try:
        return x.strftime("%Y%m%d") if hasattr(x, 'strftime') else str(x).replace("-","")[:8]
    except Exception:
        return datetime.now().strftime("%Y%m%d")

if df_export is not None and not df_export.empty and export_meta:
    ciudad_slug = re.sub(r'[^A-Za-z0-9]', '', export_meta.get("ciudad", ciudad).upper()
                         .replace('Á','A').replace('É','E')
                         .replace('Í','I').replace('Ó','O').replace('Ú','U'))
    fname_csv   = (f"Muestras_{ciudad_slug}"
                   f"_{_fmt_date(export_meta.get('fecha_inicio'))}"
                   f"_{_fmt_date(export_meta.get('fecha_fin'))}.csv")
    csv_data    = df_export.to_csv(index=False, sep=';').encode('utf-8-sig')
    st.download_button(
        label="📥 Descargar CSV (resumen de operación)",
        data=csv_data, file_name=fname_csv, mime="text/csv",
        type="secondary", use_container_width=True,
    )
else:
    st.button("📥 Descargar CSV (resumen de operación)", disabled=True,
              type="secondary", use_container_width=True,
              help="Genera un mapa primero.")

# ── Procesamiento ─────────────────────────────────────────────────────────────
if submit_button:
    try:
        resultado = manejar_error(
            generar_mapa_muestras_visual,
            fecha_inicio=str(fecha_inicio),
            fecha_fin=str(fecha_fin),
            ciudad=ciudad,
            agrupar_por=agrupar_por,
            auditoria=False,
            override_fc=None,
        )
        if resultado:
            try:
                fname, n_puntos, df_exp = resultado
            except Exception:
                fname, n_puntos, df_exp = None, 0, None

            if fname and os.path.exists(os.path.join("static", "maps", fname)):
                ts      = int(time.time())
                map_url = f"{FLASK_SERVER}/static/maps/{fname}?t={ts}"
                st.session_state["map_url"]               = map_url
                st.session_state["muestras_last_filename"] = fname
                st.session_state["map_auto_opened"]        = False
                st.session_state["muestras_export_df"]     = df_exp
                st.session_state["muestras_export_meta"]   = {
                    "ciudad": ciudad,
                    "fecha_inicio": fecha_inicio,
                    "fecha_fin":    fecha_fin,
                }
            else:
                st.session_state["map_url"]               = None
                st.session_state["muestras_last_filename"] = None
                st.session_state["muestras_export_df"]     = None
                st.session_state["muestras_export_meta"]   = None
                link_placeholder.markdown(
                    '<div class="muted" style="text-align:center;">'
                    'No se generó ningún mapa. Ajusta los filtros e inténtalo de nuevo.'
                    '</div>', unsafe_allow_html=True,
                )
        else:
            st.session_state["map_url"]               = None
            st.session_state["muestras_last_filename"] = None
            st.session_state["muestras_export_df"]     = None
            st.session_state["muestras_export_meta"]   = None

    except _ST_STOP_EXC:
        raise  # dejar que Streamlit gestione su propia detención
    except Exception:
        logging.exception("Error inesperado en submit")
        st.error("⚠️ Se produjo un error inesperado. Revisa los logs.")
        st.session_state["map_url"]               = None
        st.session_state["muestras_last_filename"] = None

# ── Render del mapa ───────────────────────────────────────────────────────────
map_url = st.session_state.get("map_url")

if map_url:
    if not st.session_state.get("map_auto_opened", False):
        st.session_state["map_auto_opened"] = True
        st.markdown(
            f"<script>try{{window.open('{map_url}','_blank');}}catch(e){{}}</script>",
            unsafe_allow_html=True,
        )
    link_placeholder.markdown(
        f'<div class="btn-row"><div>'
        f'<a href="{map_url}" target="_blank" rel="noopener" class="pill">'
        f'🗺️ Ver Mapa en Nueva Pestaña</a></div></div>',
        unsafe_allow_html=True,
    )
else:
    link_placeholder.markdown(
        '<div class="muted" style="text-align:center;">'
        'No se ha generado ningún mapa. Ajusta los filtros e inténtalo de nuevo.'
        '</div>',
        unsafe_allow_html=True,
    )
