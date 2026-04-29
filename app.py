from config.secrets_manager import load_env_secure
load_env_secure()  # Usa config/.env.enc + MAPAS_SECRET_PASSPHRASE del OS

import os
import time
import logging
import json
import streamlit as st
import pandas as pd
from pathlib import Path
from PIL import Image
import base64
# from mapa_pruebas import generar_mapa_pruebas
# from mapa_pedidos import generar_mapa_pedidos
# from mapa_facturas_vencidas import generar_mapa_facturas_vencidas
# from mapa_visitas import generar_mapa_visitas_individuales
# Nuevo flujo (datos + visual)
from mapa_muestras import (
    generar_mapa_muestras as generar_mapa_muestras_datos,
    generar_mapa_muestras_visual,
    generar_mapa_muestras_clientes,
    generar_mapa_muestras_auditoria,
)
from pre_procesamiento.new_preprocesamiento_muestras import listar_promotores
from mapa_consultores import generar_mapa_consultores
from mapa_consultores_simple import generar_mapa_consultores_simple
import validators
from datetime import datetime

from  mapa_muestras import generar_mapa_muestras_visual

#serbot software de verificacion y certificacion de https
# Configuración de entorno
# FAVOR NO BORRAR ESTOS COMANDOS :
# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
# .venv\Scripts\activate  python flask_server.py 
# $env:MAPAS_SECRET_PASSPHRASE=


ENVIRONMENT = os.getenv("ENVIRONMENT", "development")  # Por defecto, "development"
FLASK_SERVER = os.getenv("FLASK_SERVER_URL", "http://localhost:5000") if ENVIRONMENT == "production" else "http://localhost:5000"

# Permitir localhost en desarrollo
if not validators.url(FLASK_SERVER) and not FLASK_SERVER.startswith("http://localhost"):
    raise ValueError(f"❌ Error: `FLASK_SERVER_URL` no es una URL válida: {FLASK_SERVER}")

print(f"🌍 Servidor activo en: {FLASK_SERVER} | Entorno: {ENVIRONMENT}")

# Configuración de logs
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s", filename="errors.log")

def manejar_error(funcion, *args, **kwargs):
    """ Ejecuta una función y captura cualquier error. """
    try:
        return funcion(*args, **kwargs)
    except Exception as e:  # ⬅️ Asegura que esta línea esté presente
        logging.error(f"Error en {funcion.__name__}: {str(e)}")
        st.error(f"❌ Ocurrió un error en {funcion.__name__}. Revisa los logs.")
        return None


# Función para cargar los datos de cada ciudad
def cargar_datos_ciudad(ciudad):
    ciudad_folder = ciudad.upper().replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
    base_path = f"ciudades/{ciudad_folder}/"
    
    archivos = ["rutas_logistica.csv", "rutas_cobro.csv", "barrios.csv"]
    datos = {}

    for archivo in archivos:
        file_path = os.path.join(base_path, archivo)
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            df = pd.read_csv(file_path)
            if df.empty:
                raise ValueError(f"El archivo {archivo} está vacío.")
            datos[archivo.split('.')[0]] = df
        except Exception as e:
            st.error(f"Error cargando {archivo}: {e}")
            datos[archivo.split('.')[0]] = pd.DataFrame()

    return datos 

# Variables de marca - rutas sólidas
APP_TITLE = "Atlas TA"

BASE_DIR = Path(__file__).resolve().parent        # carpeta donde está app.py
LOGO_FILE = BASE_DIR / "static" / "img" / "Atlas_TA.png"

def img_to_b64(p: Path) -> str:
    return base64.b64encode(p.read_bytes()).decode("utf-8")

# === Helpers Auditoría ===
MESES_AUDITORIA = [
    (1, "Enero"), (2, "Febrero"), (3, "Marzo"), (4, "Abril"),
    (5, "Mayo"), (6, "Junio"), (7, "Julio"), (8, "Agosto"),
    (9, "Septiembre"), (10, "Octubre"), (11, "Noviembre"), (12, "Diciembre"),
]

def obtener_meses_auditoria():
    return MESES_AUDITORIA

# Configuración de la página - DEBE ser el PRIMER st.* del archivo
st.set_page_config(
    page_title=APP_TITLE,
    page_icon=Image.open(LOGO_FILE) if LOGO_FILE.exists() else "🗺️",
    layout="wide"
)

# CSS con paleta de marca (morado + amarillo) y hero centrado
st.markdown("""
<style>
:root{
    --primary:#5B21B6; --primary-600:#6D28D9; --accent:#FACC15;
    --bg:#F8F7FF; --card:#FFFFFF; --text:#1F1A2F; --muted:#6B7280; --border:#E5E7EB;
    --bg-dark:#0F1116; --card-dark:#161923; --text-dark:#EAEAF0; --muted-dark:#A3A8B3; --border-dark:#2A2F3A;
}
.block-container { max-width: 1100px; }

/* ===== HERO de marca ===== */
.hero-wrap{
    display:flex;
    justify-content:center;
    margin: 12px 0 24px 0;
}
.hero{
    display:flex;
    flex-direction: column;      /* Logo arriba, texto abajo */
    align-items:center;
    text-align:center;
    gap:12px;
}
.hero .logo{
    width: 280px;                /* Logo más grande */
    height:auto;
    display:block;
}
.hero .title{ display:none; }
.hero .subtitle{ display:none; }
.hero .tagline{
    font-weight: 600;
    font-size: clamp(18px, 2.6vw, 26px);
    margin-top: 4px;
    color: var(--text, #1F1A2F);
}
@media (max-width: 820px){
    .hero{ flex-direction: column; text-align: center; gap: 10px; }
    .hero .logo{ width: 140px; }
}

/* tipografía */
h1, h2, h3 { letter-spacing: -0.015em; }
.subtle { color: var(--muted); font-size: .95rem; }
/* cards */
.card{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:18px; }
.card + .card{ margin-top:16px; }
.card-header{ font-weight:700; font-size:1.05rem; margin-bottom:8px; }
.muted{ color:var(--muted); }
/* chips y enlaces de acción */
.pill{
  display:inline-block; padding:6px 12px; border:1px solid var(--border);
  border-radius:999px; font-size:.9rem; color:var(--text); background:#F9FAFB;
}
a.pill, .btn-link{
  display:block; text-align:center; text-decoration:none; color:#fff;
  background:var(--primary); border:1px solid var(--primary);
  padding:10px 14px; border-radius:10px; font-weight:600;
}
a.pill:hover, .btn-link:hover{ background:var(--primary-600); border-color:var(--primary-600); }
a.pill .icon{ margin-right:.35rem; }
.btn-row{ display:flex; justify-content:center; }
.btn-row > div{ width:320px; }
/* acento */
.emphasis{ color:var(--accent); }
/* espaciados */
.sp-2{ margin: 1rem 0; } .sp-3{ margin: 1.5rem 0; }

/* Dark mode alto contraste */
@media (prefers-color-scheme: dark){
  .block-container{ background:var(--bg-dark); }
  .card{ background:var(--card-dark); border-color:var(--border-dark); }
  .pill{ background:#0F1220; color:var(--text-dark); border-color:var(--border-dark); }
  .subtle, .muted{ color:var(--muted-dark); }
  body, .stMarkdown, .stText, .stRadio, .stSelectbox, .stMultiSelect{ color:var(--text-dark) !important; }
  a.pill, .btn-link{ background:var(--primary-600); border-color:var(--primary-600); }
  a.pill:hover{ filter:brightness(1.05); }
    .hero .title, .hero .subtitle{ color: var(--text-dark, #EAEAF0); }
    .hero .tagline{ color: var(--muted-dark, #A3A8B3); }
}
</style>
""", unsafe_allow_html=True)

# UI de Streamlit - Hero centrado
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
        unsafe_allow_html=True
)

# Debug checks (opcional - descomentarlas si necesitas verificar rutas)
# st.write("CWD:", os.getcwd())
# st.write("Logo existe:", LOGO_FILE.exists())
# st.write("Logo (abspath):", str(LOGO_FILE))

# Toolbar compacta (resumen de selección)
st.markdown('<div class="toolbar"><div></div><div id="toolbar-pill"></div></div>', unsafe_allow_html=True)

st.sidebar.header("Seleccione una ciudad")
ciudades = ["Barranquilla", "Bogotá", "Bucaramanga", "Cali", "Manizales", "Medellín", "Pereira"]
ciudad = st.sidebar.radio("Ciudad:", ciudades, index=3)

# Card "Configuración y Filtros"
#st.markdown('<div class="card"><div class="card-header">Configuración y Filtros</div>', unsafe_allow_html=True)

tipos_mapa = ["Clientes X Muestras"]
tipo_mapa = st.selectbox("Tipo de Mapa:", tipos_mapa)
ES_MAPA_MUESTRAS = (tipo_mapa == "Clientes X Muestras")

# Compatibilidad temporal para sesiones con "Gestores"
if tipo_mapa == "Gestores":
    tipo_mapa = "Consultores"

# Limpiar URL del mapa si cambian ciudad o tipo de mapa
current_selection = f"{ciudad}_{tipo_mapa}"
if "last_selection" not in st.session_state:
    st.session_state["last_selection"] = current_selection
elif st.session_state["last_selection"] != current_selection:
    st.session_state["map_url"] = None
    st.session_state["muestras_last_filename"] = None
    st.session_state["last_selection"] = current_selection

# Cargar datos según la ciudad seleccionada SOLO para tipos que lo requieren
datos_ciudad = None
if tipo_mapa in ("Consultores", "Pedidos", "Visitas", "Facturas Vencidas"):
    datos_ciudad = cargar_datos_ciudad(ciudad)

st.divider()

# Formulario dinámico de filtros

with st.form(key="filtros_form"):
    # if tipo_mapa == "Pedidos":
    #     rutas_disponibles = datos_ciudad["rutas_logistica"]["nombre_ruta"].sort_values().unique()
    #     ruta = st.selectbox("Seleccione una ruta logística (opcional):", options=[""] + list(rutas_disponibles))
    #     fecha_inicio = st.date_input("Fecha de Inicio")
    #     fecha_fin = st.date_input("Fecha de Fin")
    # if tipo_mapa == "Facturas Vencidas":
    #     edad_min = st.number_input("Edad mínima (días):", min_value=0, value=91)
    #     edad_max = st.number_input("Edad máxima (días):", min_value=0, value=120)
    #     rutas_cobro_disponibles = datos_ciudad["rutas_cobro"]["ruta"].sort_values().unique()
    #     ruta_cobro = st.selectbox("Seleccione una ruta de cobro (opcional):", options=[""] + list(rutas_cobro_disponibles))
    if ES_MAPA_MUESTRAS:
        # Barrios (filtro desactivado en UI)
        barrios = []
        
        # Fechas en dos columnas
        c1, c2 = st.columns(2)
        with c1: 
            fecha_inicio = st.date_input("Fecha de Inicio")
        with c2: 
            fecha_fin = st.date_input("Fecha de Fin")
        
        # Nuevo selector de agrupación (sustituye color_mode)
        agrupar_por = st.selectbox(
            "Agrupar por:",
            options=["Promotor", "Mes"],
            index=0,
        )

        # Compat: mapear a color_mode legacy si se usa el flujo anterior
        color_mode = "Promotores" if agrupar_por == "Promotor" else "Temporalidad (mes)"
        st.session_state["color_mode_muestras"] = color_mode

        # (Opciones avanzadas y cuadrantes ocultos en esta versión de UI)
        # DESACTIVAR EDITOR DE CUADRANTES
        # Expander de cuadrantes (opcional)
        # with st.expander("🗺️ Cuadrantes (opcional)"):
        #     st.write("Sube un archivo GeoJSON personalizado para usarlo como base en lugar de las comunas por defecto.")
        #     uploaded_file = st.file_uploader(
        #         "Archivo GeoJSON:",
        #         type=["geojson"],
        #         key="muestras_geojson_uploader"
        #     )
        #     if uploaded_file is not None:
        #         try:
        #             raw = uploaded_file.read().decode("utf-8")
        #             obj = json.loads(raw)
        #             if obj.get("type") == "FeatureCollection":
        #                 st.session_state["muestras_override_fc"] = obj
        #                 st.success(f"Se cargó '{uploaded_file.name}'. Se usará en lugar de las comunas por defecto.")
        #                 st.caption("Este GeoJSON reemplazará las comunas base para el mapa de Muestras.")
        #             else:
        #                 st.error("El archivo debe ser un FeatureCollection válido.")
        #                 st.session_state["muestras_override_fc"] = None
        #         except Exception:
        #             st.error("Error leyendo el GeoJSON. Verifica el formato.")
        #             st.session_state["muestras_override_fc"] = None
        #     else:
        #         if "muestras_override_fc" in st.session_state:
        #             del st.session_state["muestras_override_fc"]

        # === Panel Auditoría (Clientes X Muestras) ===
        agrupar_por_local = "Promotor" if st.session_state.get("color_mode_muestras") == "Promotores" else "Mes"
        if "muestras_modo_auditoria" not in st.session_state:
            st.session_state["muestras_modo_auditoria"] = False
        if "promotor_auditoria" not in st.session_state:
            st.session_state["promotor_auditoria"] = None
        if "mes_auditoria" not in st.session_state:
            st.session_state["mes_auditoria"] = None
        #ACTIVAR DESACTIVAR MODO AUDITORIA

        # with st.expander("Auditoría de Muestras", expanded=False):
        #     activar = st.checkbox(
        #         "Activar modo auditoría",
        #         value=st.session_state["muestras_modo_auditoria"],
        #         help="Enfoca el mapa en un promotor o mes específico según agrupación"
        #     )
        #     st.session_state["muestras_modo_auditoria"] = activar
        #     if activar:
        #         if agrupar_por_local == "Promotor":
        #             from new_mapa_muestras import CENTROOPES

        #             ciudad_norm = ciudad.upper().replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U")
        #             centroope = CENTROOPES.get(ciudad_norm)

        #             if centroope is not None:
        #                 cache_key = f"audit_promotores_{ciudad_norm}_{fecha_inicio}_{fecha_fin}"
        #                 if st.session_state.get("audit_promotores_cache_key") != cache_key:
        #                     try:
        #                         df_prom = listar_promotores(centroope, str(fecha_inicio), str(fecha_fin))
        #                     except Exception:
        #                         df_prom = pd.DataFrame()

        #                     prom_rows = []
        #                     if not df_prom.empty:
        #                         col_id = "id_promotor"
        #                         col_nombre = "apellido_promotor" if "apellido_promotor" in df_prom.columns else "nombre_promotor"
        #                         tmp = df_prom[[col_id, col_nombre]].dropna(subset=[col_id]).drop_duplicates(col_id)

        #                         for _, r in tmp.iterrows():
        #                             pid = int(r[col_id])
        #                             nombre = str(r[col_nombre] or pid).strip()
        #                             prom_rows.append((pid, nombre))

        #                     st.session_state["audit_promotores_cache"] = prom_rows
        #                     st.session_state["audit_promotores_cache_key"] = cache_key

        #                 lista_prom = st.session_state.get("audit_promotores_cache", [])
        #                 opciones_prom = [f"{pid} - {nombre}" for pid, nombre in lista_prom]

        #                 sel_prom = st.selectbox(
        #                     "Promotor a auditar",
        #                     options=["(ninguno)"] + opciones_prom,
        #                     index=0,
        #                 )

        #                 if sel_prom != "(ninguno)" and lista_prom:
        #                     try:
        #                         pid_str = sel_prom.split(" - ", 1)[0].strip()
        #                         st.session_state["promotor_auditoria"] = int(pid_str)
        #                     except Exception:
        #                         st.session_state["promotor_auditoria"] = None
        #                 else:
        #                     st.session_state["promotor_auditoria"] = None
        #             else:
        #                 st.info("No se encontró centro de operación para esta ciudad.")
        #                 st.session_state["promotor_auditoria"] = None
        #             # En modo Promotor no necesitamos mes
        #             st.session_state["mes_auditoria"] = None
        #         elif agrupar_por_local == "Mes":
        #             meses = obtener_meses_auditoria()
        #             opciones_meses = [f"{num:02d} - {nombre}" for num, nombre in meses]

        #             sel_mes = st.selectbox(
        #                 "Mes a auditar",
        #                 options=["(ninguno)"] + opciones_meses,
        #                 index=0,
        #             )

        #             if sel_mes != "(ninguno)":
        #                 try:
        #                     mes_num = int(sel_mes.split(" - ", 1)[0])
        #                     st.session_state["mes_auditoria"] = mes_num
        #                 except Exception:
        #                     st.session_state["mes_auditoria"] = None
        #             else:
        #                 st.session_state["mes_auditoria"] = None
        #             # En modo Mes no necesitamos promotor
        #             st.session_state["promotor_auditoria"] = None
        #     else:
        #         st.session_state["promotor_auditoria"] = None
        #         st.session_state["mes_auditoria"] = None
    elif tipo_mapa == "Visitas":
        # Lista de rutas desde BD (id_ruta, ruta) - usando mismo flujo que Consultores
        from pre_procesamiento.preprocesamiento_consultores import listar_rutas_simple
        df_rutas = listar_rutas_simple(ciudad)  # columnas: id_ruta, ruta
        if df_rutas is None or df_rutas.empty:
            st.warning("No hay rutas disponibles para la ciudad seleccionada.")
            id_ruta_visitas = None
            nombre_ruta_ui_visitas = None
        else:
            import re
            # Crear lista con ordenamiento robusto descendente (mismo flujo que Consultores)
            rutas_list = []
            for _, r in df_rutas.iterrows():
                ruta_nombre = str(r.ruta)
                # Extraer número inicial si existe
                match = re.match(r'^(\d+)', ruta_nombre)
                num = int(match.group()) if match else None
                rutas_list.append((int(r.id_ruta), ruta_nombre, num))
            
            # Ordenar: primero rutas numéricas (desc), luego alfanuméricas (desc)
            rutas_list.sort(key=lambda x: (0 if x[2] is not None else 1, -x[2] if x[2] is not None else 0, x[1].upper()), reverse=True)
            
            # Crear diccionario para mapear texto → id_ruta
            options_dict = {ruta_nombre: id_ruta for id_ruta, ruta_nombre, _ in rutas_list}
            options_list = [ruta_nombre for _, ruta_nombre, _ in rutas_list]
            
            # Selector que muestra solo el nombre de la ruta
            ruta_seleccionada = st.selectbox("Seleccione una ruta de cobro:", options=[""] + options_list)
            id_ruta_visitas = options_dict.get(ruta_seleccionada) if ruta_seleccionada else None
            nombre_ruta_ui_visitas = ruta_seleccionada if ruta_seleccionada else None
        
        fecha_inicio = st.date_input("Fecha de Inicio")
        fecha_fin = st.date_input("Fecha de Fin")
    elif tipo_mapa == "Consultores":
        # Ruta (obligatorio)
        from pre_procesamiento.preprocesamiento_consultores import listar_rutas_simple
        df_rutas = listar_rutas_simple(ciudad)  # columnas: id_ruta, ruta
        if df_rutas is None or df_rutas.empty:
            st.warning("No hay rutas disponibles para la ciudad seleccionada.")
            id_ruta = None
            nombre_ruta_ui = None
        else:
            import re
            # Crear lista con ordenamiento robusto descendente
            rutas_list = []
            for _, r in df_rutas.iterrows():
                ruta_nombre = str(r.ruta)
                # Extraer número inicial si existe
                match = re.match(r'^(\d+)', ruta_nombre)
                num = int(match.group()) if match else None
                rutas_list.append((int(r.id_ruta), ruta_nombre, num))
            
            # Ordenar: primero rutas numéricas (desc), luego alfanuméricas (desc)
            rutas_list.sort(key=lambda x: (0 if x[2] is not None else 1, -x[2] if x[2] is not None else 0, x[1].upper()), reverse=True)
            
            # Crear diccionario para mapear texto → id_ruta
            options_dict = {ruta_nombre: id_ruta for id_ruta, ruta_nombre, _ in rutas_list}
            options_list = [ruta_nombre for _, ruta_nombre, _ in rutas_list]
            
            # Selector que muestra solo el nombre de la ruta
            ruta_seleccionada = st.selectbox("Seleccione la ruta (obligatorio):", options=options_list)
            id_ruta = options_dict.get(ruta_seleccionada) if ruta_seleccionada else None
            nombre_ruta_ui = ruta_seleccionada if ruta_seleccionada else None
        
        # Fechas en dos columnas
        c1, c2 = st.columns(2)
        with c1: 
            fecha_inicio = st.date_input("Fecha de Inicio")
        with c2: 
            fecha_fin = st.date_input("Fecha de Fin")
        
        # Toggle para modo simple (sin cuadrantes)
        modo_simple = st.checkbox("Modo simple (sin cuadrantes)", value=True, help="Muestra todos los puntos sobre la capa base de la ciudad, sin cálculo de métricas por cuadrante")
        
        # Opción para mostrar puntos fuera de cuadrante (solo visible en modo completo)
        if not modo_simple:
            mostrar_fuera = st.checkbox("Mostrar puntos fuera de cuadrante", value=False, help="Incluye eventos que no están dentro de ningún cuadrante")
        else:
            mostrar_fuera = False  # No aplica en modo simple
    elif tipo_mapa == "Pruebas":
        # Ruta (obligatorio)
        from pre_procesamiento.preprocesamiento_consultores import listar_rutas_simple
        df_rutas = listar_rutas_simple(ciudad)  # columnas: id_ruta, ruta
        if df_rutas is None or df_rutas.empty:
            st.warning("No hay rutas disponibles para la ciudad seleccionada.")
            id_ruta_pruebas = None
            nombre_ruta_ui_pruebas = None
        else:
            import re
            # Crear lista con ordenamiento robusto descendente
            rutas_list = []
            for _, r in df_rutas.iterrows():
                ruta_nombre = str(r.ruta)
                # Extraer número inicial si existe
                match = re.match(r'^(\d+)', ruta_nombre)
                num = int(match.group()) if match else None
                rutas_list.append((int(r.id_ruta), ruta_nombre, num))
            
            # Ordenar: primero rutas numéricas (desc), luego alfanuméricas (desc)
            rutas_list.sort(key=lambda x: (0 if x[2] is not None else 1, -x[2] if x[2] is not None else 0, x[1].upper()), reverse=True)
            
            # Crear diccionario para mapear texto → id_ruta
            options_dict = {ruta_nombre: id_ruta for id_ruta, ruta_nombre, _ in rutas_list}
            options_list = [ruta_nombre for _, ruta_nombre, _ in rutas_list]
            
            # Agregar opción "TODOS" al inicio
            options_list_plus = ["TODOS"] + options_list
            
            # Selector mostrando el nombre de ruta (incluye "TODOS")
            ruta_seleccionada = st.selectbox("Seleccione la ruta:", options=options_list_plus, index=0)
            
            if ruta_seleccionada == "TODOS":
                id_ruta_pruebas = None           # ← clave: None significa NO filtrar por ruta
                nombre_ruta_ui_pruebas = "TODOS"
            else:
                id_ruta_pruebas = options_dict.get(ruta_seleccionada)
                nombre_ruta_ui_pruebas = ruta_seleccionada
        
        # Fechas en dos columnas
        c1, c2 = st.columns(2)
        with c1: 
            fecha_inicio = st.date_input("Fecha de Inicio")
        with c2: 
            fecha_fin = st.date_input("Fecha de Fin")
    # elif tipo_mapa == "Pruebas":
    #     # Lista de rutas desde BD (id_ruta, ruta) - usando mismo flujo que Consultores
    #     from pre_procesamiento.preprocesamiento_consultores import listar_rutas_simple
    #     df_rutas = listar_rutas_simple(ciudad)  # columnas: id_ruta, ruta
    #     if df_rutas is None or df_rutas.empty:
    #         st.warning("No hay rutas disponibles para la ciudad seleccionada.")
    #         id_ruta_pruebas = None
    #         nombre_ruta_ui_pruebas = None
    #     else:
    #         import re
    #         # Crear lista con ordenamiento robusto descendente (mismo flujo que Consultores)
    #         rutas_list = []
    #         for _, r in df_rutas.iterrows():
    #             ruta_nombre = str(r.ruta)
    #             # Extraer número inicial si existe
    #             match = re.match(r'^(\d+)', ruta_nombre)
    #             num = int(match.group()) if match else None
    #             rutas_list.append((int(r.id_ruta), ruta_nombre, num))
    #         
    #         # Ordenar: primero rutas numéricas (desc), luego alfanuméricas (desc)
    #         rutas_list.sort(key=lambda x: (0 if x[2] is not None else 1, -x[2] if x[2] is not None else 0, x[1].upper()), reverse=True)
    #         
    #         # Crear diccionario para mapear texto → id_ruta
    #         options_dict = {ruta_nombre: id_ruta for id_ruta, ruta_nombre, _ in rutas_list}
    #         options_list = [ruta_nombre for _, ruta_nombre, _ in rutas_list]
    #         
    #         # Selector que muestra solo el nombre de la ruta
    #         ruta_seleccionada = st.selectbox("Seleccione una ruta de cobro:", options=[""] + options_list)
    #         id_ruta_pruebas = options_dict.get(ruta_seleccionada) if ruta_seleccionada else None
    #         nombre_ruta_ui_pruebas = ruta_seleccionada if ruta_seleccionada else None
    #     
    #     # Campo fecha objetivo con default = mañana (America/Bogota)
    #     from datetime import datetime, timedelta
    #     import pytz
    #     
    #     # Obtener fecha de mañana en zona horaria Colombia
    #     try:
    #         tz_colombia = pytz.timezone('America/Bogota')
    #         hoy_colombia = datetime.now(tz_colombia).date()
    #         manana_colombia = hoy_colombia + timedelta(days=1)
    #     except:
    #         # Fallback si hay problemas con timezone
    #         from datetime import date
    #         manana_colombia = date.today() + timedelta(days=1)
    #     
    #     fecha_objetivo = st.date_input(
    #         "Fecha objetivo (proyección visitas):", 
    #         value=manana_colombia,
    #         help="Fecha para la cual se proyectan las visitas (por defecto: mañana)"
    #     )
    
    # Limpieza de cualquier estado previo relacionado con ISM
    st.session_state.pop("pph_override", None)
    st.session_state.pop("hogares_por_m2_override", None)
    st.session_state.pop("muestras_enable_ism", None)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        submit_button = st.form_submit_button("Generar Mapa", use_container_width=True, type="primary")

# Cerrar card "Configuración y Filtros"
st.markdown('</div>', unsafe_allow_html=True)

# (Eliminado) Pill dinámico con ciudad y fechas: se solicita no mostrar nada aquí.
# if submit_button and 'fecha_inicio' in locals() and 'fecha_fin' in locals():
#     st.markdown(
#         f'<div class="pill" style="text-align: center; margin: 1rem 0;">{ciudad} · {str(fecha_inicio)} → {str(fecha_fin)}</div>',
#         unsafe_allow_html=True
#     )

# Card "Resultados y Descargas"

# Placeholder único para enlace de mapa
link_placeholder = st.empty()

# Descargas (tres botones centrados)

# 1. Descarga HTML del mapa
if ES_MAPA_MUESTRAS:
    map_filename = st.session_state.get("muestras_last_filename")
    if map_filename:
        try:
            _abs_session = os.path.abspath(os.path.join("static", "maps", map_filename))
            print(f"[STREAMLIT] Mapa muestras filename en sesión: {map_filename} | abs={_abs_session} | exists={os.path.exists(_abs_session)}", flush=True)
        except Exception as _e_sess:
            print(f"[STREAMLIT] Error verificando filename en sesión: {_e_sess}", flush=True)
    
    if map_filename and os.path.exists(os.path.join("static", "maps", map_filename)):
        from datetime import datetime
        import re
        
        ciudad_html = re.sub(r'[^A-Za-z0-9]', '', ciudad.upper())
        ciudad_html = ciudad_html.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
        fecha_actual = datetime.now().strftime("%Y%m%d")
        filename_html = f"Mapa_Muestras_{ciudad_html}_{fecha_actual}.html"
        
        with open(os.path.join("static", "maps", map_filename), "rb") as f:
            html_bytes = f.read()
        
        st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar HTML del mapa",
            data=html_bytes,
            file_name=filename_html,
            mime="text/html",
            type="secondary",
            use_container_width=True,
            help="Descarga el archivo HTML del mapa generado"
        )
    else:
        st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
        st.button(
            "📥 Descargar HTML del mapa",
            disabled=True,
            type="secondary",
            use_container_width=True,
            help="Genere un mapa para habilitar esta descarga."
        )

elif tipo_mapa == "Consultores":
    st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
    st.button(
        "📥 Descargar HTML del mapa",
        disabled=True,
        type="secondary", 
        use_container_width=True,
        help="Descarga HTML no disponible para Consultores"
    )

# 2. Descarga CSV (resumen de operación)
if tipo_mapa == "Consultores":
    df_export = st.session_state.get("consultores_export_df")
    export_meta = st.session_state.get("consultores_export_meta")
    
    if df_export is not None and not df_export.empty and export_meta is not None:
        from datetime import datetime
        import re
        
        ciudad_csv = re.sub(r'[^A-Za-z0-9]', '', export_meta["ciudad"].upper())
        ciudad_csv = ciudad_csv.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
        
        fecha_ini_str = export_meta["fecha_inicio"].strftime("%Y%m%d")
        fecha_fin_str = export_meta["fecha_fin"].strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%H%M%S")
        
        filename_csv = f"consultores_{ciudad_csv}_{export_meta['id_ruta']}_{fecha_ini_str}-{fecha_fin_str}_{timestamp}.csv"
        
        df_csv = df_export.copy()
        if 'fecha_evento' in df_csv.columns:
            df_csv['fecha_evento'] = pd.to_datetime(df_csv['fecha_evento']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'id_evento_tipo' in df_csv.columns and 'tipo_evento' in df_csv.columns:
            cols = list(df_csv.columns)
            cols.remove('tipo_evento')
            insert_at = cols.index('id_evento_tipo') + 1
            cols.insert(insert_at, 'tipo_evento')
            df_csv = df_csv[cols]
        
        csv_data = df_csv.to_csv(index=False, sep=';').encode('utf-8-sig')
        
        st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
        st.download_button(
            label="📥 Descargar CSV (resumen de operación)",
            data=csv_data,
            file_name=filename_csv,
            mime="text/csv",
            type="secondary",
            use_container_width=True,
            help="Descarga los datos mostrados en el mapa"
        )
    else:
        st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
        st.button(
            "📥 Descargar CSV (resumen de operación)",
            disabled=True,
            type="secondary",
            use_container_width=True,
            help="Genere un mapa para habilitar esta descarga."
        )

elif ES_MAPA_MUESTRAS:
    df_export = st.session_state.get("muestras_export_df")
    export_meta = st.session_state.get("muestras_export_meta")
    
    if df_export is not None and not df_export.empty and export_meta is not None:
        from datetime import datetime
        import re
        
        def _fmt_yyyymmdd(x):
            try:
                if hasattr(x, 'strftime'):
                    return x.strftime("%Y%m%d")
                if isinstance(x, str) and x:
                    cleaned = x.replace("-", "")
                    if len(cleaned) >= 8 and cleaned[:8].isdigit():
                        return cleaned[:8]
            except Exception:
                pass
            return datetime.now().strftime("%Y%m%d")
        
        ciudad_csv = re.sub(r'[^A-Za-z0-9]', '', export_meta.get("ciudad", ciudad).upper())
        ciudad_csv = ciudad_csv.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
        
        fecha_inicio_fmt = _fmt_yyyymmdd(export_meta.get("fecha_inicio"))
        fecha_fin_fmt = _fmt_yyyymmdd(export_meta.get("fecha_fin"))
        
        filename_csv = f"Muestras_{ciudad_csv}_{fecha_inicio_fmt}_{fecha_fin_fmt}.csv"
        
        csv_data = df_export.to_csv(index=False, sep=';').encode('utf-8-sig')
        
        st.download_button(
            label="📥 Descargar CSV (resumen de operación)",
            data=csv_data,
            file_name=filename_csv,
            mime="text/csv",
            type="secondary",
            use_container_width=True,
            help="Descarga los datos mostrados en el mapa"
        )
    else:
        st.markdown('<div class="btn-row"><div>', unsafe_allow_html=True)
        st.button(
            "📥 Descargar CSV (resumen de operación)",
            disabled=True,
            type="secondary",
            use_container_width=True,
            help="Genere un mapa para habilitar esta descarga."
        )

# 3. (Eliminado) Descarga CSV ISM

# Cerrar card "Resultados y Descargas"

# Separador entre cards y procesamiento

# Separador sutil entre secciones

# Card secundario para Cuadrantes (opcional)
ciudad_normalizada = ciudad.upper().replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
editor_url = f"{FLASK_SERVER}/editor/cuadrantes?city={ciudad_normalizada}"

st.markdown(
    f"""
    <style>
    .card-cuadrantes {{
        background: linear-gradient(135deg, #5B21B6 0%, #6D28D9 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1.5rem 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(255,255,255,.15);
    }}
    .card-cuadrantes h3 {{
        margin: 0 0 0.5rem 0;
        font-size: 1.25rem;
        font-weight: bold;
    }}
    .cta-editor {{
        display: inline-block;
        background: rgba(255, 255, 255, 0.2);
        color: white;
        text-decoration: none;
        padding: 0.75rem 1.5rem;
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s ease;
        border: 1px solid rgba(255, 255, 255, 0.3);
    }}
    .cta-editor:hover {{
        background: rgba(255, 255, 255, 0.3);
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        text-decoration: none;
        color: white;
    }}
    @media (prefers-color-scheme: dark) {{
        .card-cuadrantes {{
            border-color: rgba(255,255,255,.25);
        }}
        .card-cuadrantes h3 {{
            color: #f7fafc;
        }}
        .card-cuadrantes p {{
            color: #a0aec0;
        }}
    }}
    
    
    </div>
    """, 
    unsafe_allow_html=True
)

# # Card: Segmentación de ciudades (enlace al editor)
# ciudad_normalizada = ciudad.upper().replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
# editor_url = f"{FLASK_SERVER}/editor/cuadrantes?city={ciudad_normalizada}"
# st.markdown("### Segmentación de ciudades")
# st.caption("Abre el editor para segmentar y gestionar cuadrantes por ciudad.")
# st.link_button("🗺️ Abrir editor de cuadrantes", editor_url)

# Procesamiento
if submit_button:
    try:
        # if tipo_mapa == "Pedidos":
        #     filename = manejar_error(generar_mapa_pedidos, fecha_inicio, fecha_fin, ciudad, ruta)
        #     map_type = "pedidos"
        # elif tipo_mapa == "Visitas":
        #     if not id_ruta_visitas:
        #         st.error("Seleccione una ruta válida.")
        #         filename = None
        #     else:
        #         filename = manejar_error(
        #             generar_mapa_visitas_individuales,
        #             ciudad,
        #             id_ruta_visitas,  # Pasar ID entero directamente
        #             nombre_ruta_ui_visitas,  # Pasar nombre para mostrar en el mapa
        #             str(fecha_inicio),
        #             str(fecha_fin)
        #         )
        #     map_type = "visitas"
        # elif tipo_mapa == "Facturas Vencidas":
        #     filename = manejar_error(generar_mapa_facturas_vencidas, ciudad, edad_min, edad_max, ruta_cobro)
        #     map_type = "facturas"
        if ES_MAPA_MUESTRAS:
            override_fc = st.session_state.get("muestras_override_fc")
            promotores_sel = st.session_state.get("promotores_sel")  # filtro normal múltiple
            # Lectura de estado de auditoría
            modo_auditoria = st.session_state.get("muestras_modo_auditoria", False)
            promotor_auditoria = st.session_state.get("promotor_auditoria")
            mes_auditoria = st.session_state.get("mes_auditoria")

            # Flag para modo clientes únicos
            clientes_x_muestras = True

            # Recalcular agrupar_por_local (ya definido en formulario)
            agrupar_por_local = "Promotor" if st.session_state.get("color_mode_muestras") == "Promotores" else "Mes"

            if modo_auditoria:
                if agrupar_por_local == "Promotor" and promotor_auditoria is not None:
                    resultado = manejar_error(
                        generar_mapa_muestras_auditoria,
                        fecha_inicio=str(fecha_inicio),
                        fecha_fin=str(fecha_fin),
                        ciudad=ciudad,
                        agrupar_por="Promotor",
                        id_promotor=promotor_auditoria,
                        mes_auditoria=None,
                    )
                    if resultado:
                        try:
                            map_filename, n_puntos, df_areas = resultado
                            filename = map_filename
                            st.session_state["muestras_export_df"] = None
                            st.session_state["muestras_export_meta"] = None
                        except Exception:
                            filename, n_puntos = None, 0
                        st.session_state["muestras_last_filename"] = filename
                    else:
                        filename, n_puntos = None, 0
                        st.session_state["muestras_last_filename"] = None
                elif agrupar_por_local == "Mes" and mes_auditoria is not None:
                    resultado = manejar_error(
                        generar_mapa_muestras_auditoria,
                        fecha_inicio=str(fecha_inicio),
                        fecha_fin=str(fecha_fin),
                        ciudad=ciudad,
                        agrupar_por="Mes",
                        id_promotor=None,
                        mes_auditoria=mes_auditoria,
                    )
                    if resultado:
                        try:
                            map_filename, n_puntos, df_areas = resultado
                            filename = map_filename
                            st.session_state["muestras_export_df"] = None
                            st.session_state["muestras_export_meta"] = None
                        except Exception:
                            filename, n_puntos = None, 0
                        st.session_state["muestras_last_filename"] = filename
                    else:
                        filename, n_puntos = None, 0
                        st.session_state["muestras_last_filename"] = None
                else:
                    st.warning("Modo auditoría activo pero falta seleccionar Promotor/Mes válido.")
                    filename, n_puntos = None, 0
                    st.session_state["muestras_last_filename"] = None
                    st.session_state["muestras_export_df"] = None
                    st.session_state["muestras_export_meta"] = None
            else:
                # Flujo normal clientes X muestras
                try:
                    df_original, df_filtrado, df_agrupado = generar_mapa_muestras_datos(
                        fecha_inicio=str(fecha_inicio),
                        fecha_fin=str(fecha_fin),
                        ciudad=ciudad,
                        agrupar_por=agrupar_por_local,
                    )
                except Exception as e:
                    logging.error(f"Error generando datos (nuevo flujo clientes): {e}")
                    df_original = pd.DataFrame(); df_filtrado = pd.DataFrame(); df_agrupado = pd.DataFrame()
                resultado = manejar_error(
                    generar_mapa_muestras_visual,
                    fecha_inicio=str(fecha_inicio),
                    fecha_fin=str(fecha_fin),
                    ciudad=ciudad,
                    agrupar_por=agrupar_por_local,
                    auditoria=False,
                    override_fc=override_fc,
                )
                if resultado:
                    try:
                        map_filename, n_puntos, df_export = resultado
                        filename = map_filename
                        st.session_state["muestras_export_df"] = df_export
                        st.session_state["muestras_export_meta"] = {
                            "ciudad": ciudad,
                            "fecha_inicio": fecha_inicio,
                            "fecha_fin": fecha_fin,
                        }
                        # Debug: filename generado y verificación de existencia local antes de invocar Flask
                        try:
                            _abs_gen = os.path.abspath(os.path.join("static", "maps", filename))
                            print(f"[STREAMLIT] mapa_muestras filename generado: {filename} | abs={_abs_gen} | exists={os.path.exists(_abs_gen)}", flush=True)
                        except Exception as _e_gen:
                            print(f"[STREAMLIT] Error verificando ruta generada: {_e_gen}", flush=True)
                    except Exception:
                        filename, n_puntos = None, 0
                        st.session_state["muestras_export_df"] = None
                        st.session_state["muestras_export_meta"] = None
                    st.session_state["muestras_last_filename"] = filename
                else:
                    filename, n_puntos = None, 0
                    st.session_state["muestras_last_filename"] = None
                    st.session_state["muestras_export_df"] = None
                    st.session_state["muestras_export_meta"] = None
            map_type = "muestras"
        elif tipo_mapa == "Consultores":
            # Validar que se haya seleccionado una ruta válida
            if not id_ruta:
                st.error("Seleccione una ruta válida.")
                filename = None
                n_puntos = 0
            else:
                # Desvío según modo simple o completo
                if modo_simple:
                    # Modo simple: sin cuadrantes, solo capa base + puntos
                    resultado = manejar_error(
                        generar_mapa_consultores_simple,
                        ciudad,
                        int(id_ruta),
                        fecha_inicio,  # Pasar date directamente
                        fecha_fin      # Pasar date directamente
                    )
                    if resultado:
                        filename, n_puntos = resultado
                    else:
                        filename, n_puntos = None, 0
                    # Limpiar session state (simple no exporta CSV)
                    st.session_state["consultores_export_df"] = None
                    st.session_state["consultores_export_meta"] = None
                else:
                    # Modo completo: con cuadrantes y métricas
                    resultado = manejar_error(
                        generar_mapa_consultores,
                        str(fecha_inicio),
                        str(fecha_fin),
                        ciudad,
                        int(id_ruta),
                        nombre_ruta_ui if nombre_ruta_ui else "",
                        mostrar_fuera
                    )
                    if resultado:
                        filename, n_puntos, df_export = resultado
                        # Guardar DataFrame para descarga CSV
                        st.session_state["consultores_export_df"] = df_export
                        st.session_state["consultores_export_meta"] = {
                            "ciudad": ciudad,
                            "id_ruta": id_ruta,
                            "fecha_inicio": fecha_inicio,
                            "fecha_fin": fecha_fin
                        }
                    else:
                        filename, n_puntos = None, 0
                        st.session_state["consultores_export_df"] = None
                        st.session_state["consultores_export_meta"] = None
            map_type = "consultores"
        elif tipo_mapa == "Pruebas":
            # id_ruta_pruebas puede ser None (para "TODOS") o un int
            resultado = manejar_error(
                
                ciudad,               # str (con acentos tal como viene del radio)
                id_ruta_pruebas,      # int | None (None para "TODOS")
                fecha_inicio,         # date
                fecha_fin             # date
            )
            if resultado:
                filename, n_puntos = resultado
            else:
                filename, n_puntos = None, 0
            map_type = "pruebas"

        if filename:
            # Validación: asegurar que el archivo existe en static/maps
            try:
                if not os.path.exists(os.path.join("static", "maps", filename)):
                    raise FileNotFoundError(f"No se encontró el mapa generado: {filename}")
            except Exception as _e:
                logging.error(f"Validación de mapa falló: {_e}")
                st.error("❌ No se encontró el mapa generado. Reintenta o verifica el backend.")
                st.session_state["map_url"] = None
                st.session_state["muestras_last_filename"] = None
                st.session_state["map_auto_opened"] = False
            else:
                # Nuevo mapa generado: construir URL, resetear auto-open
                timestamp = int(time.time())
                map_url = f"{FLASK_SERVER}/static/maps/{filename}?t={timestamp}"
                st.session_state["map_url"] = map_url
                st.session_state["map_auto_opened"] = False  # reset para permitir auto-open
                print(f"[STREAMLIT] map_url generado (static): {map_url}", flush=True)
            # Warning si hay filtro y no hubo puntos
            if ES_MAPA_MUESTRAS and st.session_state.get("filtrar_por_promotor") and st.session_state.get("promotores_sel") and n_puntos == 0:
                st.warning("No hay datos para los promotores seleccionados en el rango de fechas.")
        else:
            st.session_state["map_url"] = None
            st.session_state["map_auto_opened"] = False
            link_placeholder.markdown(
                '<div class="muted" style="text-align:center;">No se generó ningún mapa. Ajusta los filtros e inténtalo de nuevo.</div>',
                unsafe_allow_html=True
            )

    except Exception as e:
        logging.error(f"❌ Error inesperado: {str(e)}")
        st.error("⚠️ Se produjo un error inesperado. Revisa los logs.")
        # Asegurarnos de no mostrar un mapa viejo tras un error crítico
        st.session_state["map_url"] = None
        st.session_state["muestras_last_filename"] = None

# Bloque único de auto-open + botón respaldo
map_url = st.session_state.get("map_url")
if map_url:
    if not st.session_state.get("map_auto_opened", False):
        st.session_state["map_auto_opened"] = True
        st.markdown(
            f"""
            <script>
            try {{ window.open('{map_url}', '_blank'); }}
            catch(e) {{ console.log('Popup bloqueado:', e); }}
            </script>
            """,
            unsafe_allow_html=True,
        )
    link_placeholder.markdown(
        f'<div class="btn-row"><div><a href="{map_url}" target="_blank" rel="noopener" class="pill">'
        '🗺️ Ver Mapa en Nueva Pestaña</a></div></div>',
        unsafe_allow_html=True
    )
else:
    link_placeholder.markdown(
        '<div class="muted" style="text-align:center;">No se ha generado ningún mapa. Ajusta los filtros e inténtalo de nuevo.</div>',
        unsafe_allow_html=True
    )

# Leyenda detalle y resumen cuando se usa el nuevo flujo 
# DESACTIVAR EL DETALLE DE METRICAS RESUMEN 
# try:
#     if ES_MAPA_MUESTRAS and 'df_agrupado' in locals() and isinstance(df_agrupado, pd.DataFrame) and not df_agrupado.empty:
#         st.divider()
#         st.subheader("Detalle de métricas")

#         df_leg = df_agrupado.copy()
#         # Calcular clientes_por_area_m2
#         if 'area_m2' in df_leg.columns and 'clientes_total' in df_leg.columns:
#             df_leg['clientes_por_area_m2'] = df_leg.apply(lambda r: (r['clientes_total'] / r['area_m2']) if (pd.notna(r.get('area_m2')) and float(r['area_m2']) > 0) else None, axis=1)

#         # Renombrar columnas para UI
#         rename_cols = {
#             'apellido_promotor': 'Promotor',
#             'muestras_total': '#Muestras',
#             'clientes_total': '#Clientes',
#             'area_m2': 'Área (m²)',
#             'clientes_por_dia_habil': 'Clientes/día hábil',
#             'pct_clientes_no_fieles': '% Clientes NO fieles',
#             'pct_total_muestras_contactables': '% Clientes contactables',
#             'pct_contactabilidad_no_fieles': '% Contactabilidad NO fieles',
#             'clientes_por_area_m2': 'Clientes/m²',
#         }
#         df_leg_show = df_leg.rename(columns=rename_cols)

#         if 'mes' in df_leg_show.columns and agrupar_por == "Mes":
#             nombre_mes = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
#             df_leg_show['Mes'] = df_leg_show['mes'].map(nombre_mes)
#             order_cols = ['Mes', '#Muestras', '#Clientes', 'Área (m²)', 'Clientes/m²', 'Clientes/día hábil', '% Clientes NO fieles', '% Clientes contactables', '% Contactabilidad NO fieles']
#             order_cols = [c for c in order_cols if c in df_leg_show.columns]
#             df_leg_show = df_leg_show.sort_values('mes').reset_index(drop=True)
#         else:
#             order_cols = ['Promotor', '#Muestras', '#Clientes', 'Área (m²)', 'Clientes/m²', 'Clientes/día hábil', '% Clientes NO fieles', '% Clientes contactables', '% Contactabilidad NO fieles']
#             order_cols = [c for c in order_cols if c in df_leg_show.columns]
#             # por defecto ordenar por #Clientes desc si disponible
#             sort_key = '#Clientes' if '#Clientes' in df_leg_show.columns else '#Muestras'
#             df_leg_show = df_leg_show.sort_values(sort_key, ascending=False).reset_index(drop=True)

#         st.dataframe(df_leg_show[order_cols], use_container_width=True)

#         # Resumen superior
#         if 'df_original' in locals() and isinstance(df_original, pd.DataFrame):
#             total_muestras = len(df_original)
#             total_clientes = int(df_filtrado['id_contacto'].nunique()) if 'id_contacto' in df_filtrado.columns else len(df_filtrado)
#             dias_habiles_global = int(df_original['fecha_evento'].dt.date.nunique()) if 'fecha_evento' in df_original.columns else 0
#             clientes_por_dia = (total_clientes / dias_habiles_global) if dias_habiles_global > 0 else 0.0
#             c1, c2, c3, c4 = st.columns(4)
#             c1.metric("Total eventos", f"{total_muestras:,}")
#             c2.metric("Total clientes", f"{total_clientes:,}")
#             c3.metric("Días hábiles", f"{dias_habiles_global}")
#             c4.metric("Clientes/día hábil", f"{clientes_por_dia:.1f}")
# except Exception as _e:
#     logging.warning(f"Detalle nuevo flujo no disponible: {_e}")
