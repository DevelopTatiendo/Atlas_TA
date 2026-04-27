"""Nueva lógica de mapa de muestras (Fase 2 sin Folium).

Esta versión:
 - Obtiene datos vía new_preprocesamiento_muestras.consultar_db
 - Normaliza con crear_df
 - Construye:
      df_original  (todas las muestras válidas)
      df_filtrado  (clientes únicos por promotor, última muestra)
      df_agrupado  (métricas agregadas para leyenda detalle)

No genera mapa ni HTML. No calcula áreas ni densidad. Siempre modo clientes.
"""

from __future__ import annotations

import math
import json
import logging
import re
import unicodedata
from hashlib import md5
from typing import Literal

import numpy as np
import pandas as pd
import folium
from folium import FeatureGroup
from folium.plugins import FeatureGroupSubGroup

from shapely.geometry import shape, Point
from shapely.prepared import prep
from pyproj import Geod

from pre_procesamiento.new_preprocesamiento_muestras import (
    consultar_db,
    crear_df,
    consultar_llamadas_raw,
    aplicar_contactabilidad_temporal,
)
from pre_procesamiento.metricas_areas import areas_muestras_resumen
from utils.gestor_mapas import guardar_mapa_controlado
from mapa_consultores import _es_cuadrante_padre, _es_cuadrante_hijo, _style_cuadrante

# Catálogo completo de id_contacto_categoria
# 1  BRONCE                      2  VETADOS                3  PROVEEDOR
# 10 RECUPERACION                12 EMPLEADOS              17 MANEJO ESPECIAL
# 20 ZONA RESTRINGIDA            22 CLIENTE ESPECIAL       29 CLIENTE DIGITAL
# 30 INCONTACTABLE               31 SACAR DEL SISTEMA      36 INACTIVOS
# 37 PROBLEMA DE COBRO           38 CLIENTE PERDIDO        39 TIPIFICACION SACAR DEL SISTEMA
# 40 ORO                         41 DIAMANTE               42 TRANSICION FRECUENTE
# 43 PLATA                       44 PORTAL WEB             46 CLIENTES POTENCIALES
# 49 ZAGUS                       50 PRIVILEGIOS BOLIVAR 1  53 TIENDAS
# 54 CAVASA                      55 CLIENTE ESPECIAL FRECUENTE
# 56 CLIENTE ESPECIAL NO FRECUENTE  57 TRANSICION NO FRECUENTE
# 58 TICKET BAJO                 59 TICKET MEDIO           60 TICKET ALTO
# 61 SUSCRIPTORES FULLIMP

# Fieles = Tickets (58-60) + Frecuentes (42, 55)
# Un cliente es NO FIEL si su categoría NO está en este conjunto.
CATEGORIAS_FIELES = {42, 55, 58, 59, 60}

# Mínimo de muestras por promotor para aparecer en el mapa y métricas
MIN_MUESTRAS_PROMOTOR: int = 3

# Mapeo ciudad -> centroope (copiado del flujo legacy)
CENTROOPES = {
    'CALI': 2,
    'MEDELLIN': 3,
    'MANIZALES': 6,
    'PEREIRA': 5,
    'BOGOTA': 4,
    'BARRANQUILLA': 8,
    'BUCARAMANGA': 7,
}


coordenadas_ciudades = {
    'CALI': ([3.4516, -76.5320], 'geojson/rutas/cali/cuadrantes_rutas_cali.geojson'),
    'MEDELLIN': ([6.2442, -75.5812], 'geojson/rutas/medellin/cuadrantes_rutas_medellin.geojson'),
    'MANIZALES': ([5.0672, -75.5174], 'geojson/rutas/manizales/cuadrantes_rutas_manizales.geojson'),
    'PEREIRA': ([4.8087, -75.6906], 'geojson/rutas/pereira/cuadrantes_rutas_pereira.geojson'),
    'BOGOTA': ([4.7110, -74.0721], 'geojson/rutas/bogota/cuadrantes_rutas_bogota.geojson'),
    'BARRANQUILLA': ([10.9720, -74.7962], 'geojson/rutas/barranquilla/cuadrantes_rutas_barranquilla.geojson'),
    'BUCARAMANGA': ([7.1193, -73.1227], 'geojson/rutas/bucaramanga/cuadrantes_rutas_bucaramanga.geojson'),
}

# === Paletas de colores ===
PALETA_MESES = {
    1: "#1f78b4", 2: "#a6cee3", 3: "#33a02c", 4: "#b2df8a",
    5: "#e31a1c", 6: "#fb9a99", 7: "#ff7f00", 8: "#fdbf6f",
    9: "#6a3d9a", 10: "#cab2d6", 11: "#b15928", 12: "#ffff99",
}

PALETTE_PROMOTORES = [
    "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
    "#DB2777", "#0D9488", "#1D4ED8", "#B45309", "#065F46",
    "#9333EA", "#EA580C", "#047857", "#9D174D", "#4F46E5",
    "#BE123C", "#0EA5E9", "#6D28D9", "#16A34A", "#B91C1C",
]

def color_for_promotor(centroope: int, id_promotor: int) -> str:
    try:
        h = md5(f"{centroope}-{int(id_promotor)}".encode()).hexdigest()
        idx = int(h, 16) % len(PALETTE_PROMOTORES)
        return PALETTE_PROMOTORES[idx]
    except Exception:
        return "#64748B"

# === Flags / constantes visuales ===
DEBUG_AREAS = False

# Intentar usar TreeLayerControl si existe
try:
    from folium.plugins import TreeLayerControl
    HAS_TREE_CONTROL = True
except Exception:
    HAS_TREE_CONTROL = False


def _normalizar_ciudad(ciudad: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', ciudad) if unicodedata.category(c) != 'Mn').upper()


def resolver_nombre_ruta(ciudad_norm: str, codigo: str, props: dict) -> str:
    """Resolver nombre legible de ruta/cuadrante sin depender del preprocesamiento legacy.

    Usa propiedades comunes si existen; fallback al código.
    """
    try:
        for k in ("nombre", "NOMBRE", "ruta", "RUTA", "display", "display_name"):
            v = props.get(k)
            if v and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return str(codigo or "").strip() or "Sin nombre"


# ==== Helpers de formato / leyenda ====
def __fmt_es(valor, decimales=0, miles=True):
    try:
        if valor is None or (isinstance(valor, float) and math.isnan(valor)):
            return "0" if decimales == 0 else ("0," + ("0" * decimales))
        if decimales > 0:
            s = f"{float(valor):,.{decimales}f}"
        else:
            s = f"{float(valor):,.0f}"
        s = s.replace(",", "_").replace(".", ",").replace("_", ".")
        if not miles:
            if "," in s:
                entier, frac = s.split(",", 1)
                entier = entier.replace(".", "")
                return f"{entier},{frac}"
            return s.replace(".", "")
        return s
    except Exception:
        return str(valor)

_TA_SORTABLE_ASSETS_ADDED = False

def inject_sort_assets(mapa):
    global _TA_SORTABLE_ASSETS_ADDED
    if _TA_SORTABLE_ASSETS_ADDED:
        return
    css_block = """
    <style id="ta-sortable-css">
    .ta-sortable th { cursor:pointer; white-space:nowrap; }
    .ta-sortable th .ta-sort-arrow { margin-left:6px; opacity:.5; }
    .ta-sortable th.ta-sort-asc  .ta-sort-arrow::after { content:"▲"; opacity:1; }
    .ta-sortable th.ta-sort-desc .ta-sort-arrow::after { content:"▼"; opacity:1; }
    </style>
    """
    js_block = """
    <script id="ta-sortable-js">
    (function(){
        if (window.TASortable) return;
        function normNum(s){
            if (s==null) return NaN;
            s = String(s).trim().toLowerCase();
            if (s==="—" || s==="" || s==="na" || s==="n/d") return NaN;
            s = s.replace(/\s/g,"");
            s = s.replace(/[%,$€]/g,"");
            s = s.replace(/\./g,"");
            s = s.replace(/,/g,".");
            var v = parseFloat(s);
            return isNaN(v) ? NaN : v;
        }
        function getCellVal(td, type){
            const txt = td?.textContent ?? "";
            if (type==="num" || type==="percent" || type==="money") return normNum(txt);
            if (type==="date") return new Date(txt).getTime() || 0;
            return txt.toString().toLowerCase().normalize("NFD").replace(/\p{Diacritic}/gu,"");
        }
        function sortTable(tbl, colIdx, type, dir){
            const tbody = tbl.tBodies[0];
            const rows = Array.from(tbody.rows);
            rows.sort((a,b)=>{
                const va = getCellVal(a.cells[colIdx], type);
                const vb = getCellVal(b.cells[colIdx], type);
                if (isNaN(va) && isNaN(vb)) return 0;
                if (isNaN(va)) return  1;
                if (isNaN(vb)) return -1;
                return (va<vb?-1:va>vb?1:0) * (dir==="asc"?1:-1);
            });
            rows.forEach(r=>tbody.appendChild(r));
        }
        function attach(table){
            if (!table || table.__taSortable) return;
            table.__taSortable = true;
            const ths = table.tHead ? Array.from(table.tHead.rows[0].cells) : [];
            ths.forEach((th, i)=>{
                const type = th.dataset.type || "text";
                const span = document.createElement("span"); span.className="ta-sort-arrow"; th.appendChild(span);
                th.addEventListener("click", ()=>{
                    const cur = th.classList.contains("ta-sort-asc") ? "asc" : th.classList.contains("ta-sort-desc") ? "desc" : "";
                    ths.forEach(h=>h.classList.remove("ta-sort-asc","ta-sort-desc"));
                    const dir = (cur==="" || cur==="desc") ? "asc" : "desc";
                    th.classList.add(dir==="asc"?"ta-sort-asc":"ta-sort-desc");
                    sortTable(table, i, type, dir);
                });
            });
        }
        window.TASortable = { initAll: function(){ document.querySelectorAll("table.ta-sortable").forEach(attach); }, attach };
    })();
    </script>
    """
    try:
        mapa.get_root().html.add_child(folium.Element(css_block))
        mapa.get_root().html.add_child(folium.Element(js_block))
        _TA_SORTABLE_ASSETS_ADDED = True
    except Exception:
        pass


def _build_legend_row_struct(
    etiqueta: str,
    muestras_total: int,
    clientes_total: int,
    dias_habiles: int,
    pct_no_fieles: float,
    pct_contactables: float,
    pct_contactables_nofieles: float,
    area_km2: float | None = None,
    muestras_por_km2: float | None = None,
    muestras_por_dia_habil: float | None = None,
    color_hex: str | None = None,
    pct_conversion: float = 0.0,
    pct_nofiel_contactable: float = 0.0,
):
    import pandas as _pd
    return {
        "etiqueta": etiqueta,
        "muestras_total": int(muestras_total) if _pd.notna(muestras_total) else 0,
        "clientes_total": int(clientes_total) if _pd.notna(clientes_total) else 0,
        "dias_habiles": int(dias_habiles) if _pd.notna(dias_habiles) else 0,
        "pct_no_fieles": float(pct_no_fieles) if _pd.notna(pct_no_fieles) else 0.0,
        "pct_contactables": float(pct_contactables) if _pd.notna(pct_contactables) else 0.0,
        "pct_contactables_nofieles": float(pct_contactables_nofieles) if _pd.notna(pct_contactables_nofieles) else 0.0,
        "area_km2": float(area_km2) if (area_km2 is not None and _pd.notna(area_km2)) else None,
        "muestras_por_km2": float(muestras_por_km2) if (muestras_por_km2 is not None and _pd.notna(muestras_por_km2)) else None,
        "muestras_por_dia_habil": float(muestras_por_dia_habil) if (muestras_por_dia_habil is not None and _pd.notna(muestras_por_dia_habil)) else None,
        "color_hex": color_hex,
        "pct_conversion": float(pct_conversion) if _pd.notna(pct_conversion) else 0.0,
        # % de clientes totales que son NO fieles Y fueron contactados (captación efectiva)
        "pct_nofiel_contactable": float(pct_nofiel_contactable) if _pd.notna(pct_nofiel_contactable) else 0.0,
    }


def _render_legend_html_muestras(lista_structs: list[dict], titulo: str, label_col: str) -> str:
    def _fmt_int(v):
        try:
            return f"{int(round(v)):,}".replace(',', '.')
        except Exception:
            return '—'
    def _fmt_pct(v):
        try:
            return f"{float(v):.1f}%"
        except Exception:
            return '—'
    def _fmt_area_km2(v):
        if v is None:
            return '—'
        try:
            # Mostrar siempre 5 decimales en formato ES (coma decimal, punto miles)
            return f"{float(v):,.5f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        except Exception:
            return '—'
    def _fmt_muestras_km2(v):
        if v is None:
            return '—'
        try:
            return f"{float(v):,.10f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        except Exception:
            return '—'
    rows_html = []
    for r in lista_structs:
        color_span = ''
        if r.get('color_hex'):
            color_span = f"<span style='display:inline-block;width:12px;height:12px;border-radius:3px;background:{r['color_hex']};'></span>"
        muestras_dia_habil = r.get('muestras_por_dia_habil')
        muestras_dia_habil_fmt = _fmt_int(muestras_dia_habil) if muestras_dia_habil is not None else '—'
        rows_html.append(f"""
        <tr>
            <td style='padding:6px 8px;display:flex;align-items:center;gap:8px;'>
                {color_span}<span>{r['etiqueta']}</span>
            </td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_int(r['muestras_total'])}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_int(r['clientes_total'])}</td>
            <td style='padding:6px 8px;text-align:center;'>{_fmt_area_km2(r.get('area_km2'))}</td>
            <td style='padding:6px 8px;text-align:center;'>
                { _fmt_muestras_km2(r.get('muestras_por_km2')/1000.0) if r.get('muestras_por_km2') is not None else '—' }
            </td>
            <td style='padding:6px 8px;text-align:right;'>{muestras_dia_habil_fmt}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_pct(r['pct_no_fieles'])}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_pct(r['pct_contactables'])}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_pct(r['pct_contactables_nofieles'])}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_pct(r.get('pct_nofiel_contactable', 0))}</td>
            <td style='padding:6px 8px;text-align:right;'>{_fmt_pct(r.get('pct_conversion', 0))}</td>
        </tr>
        """)
    return f"""
    <div id='legend-promotores' style='
        position: fixed; bottom: 20px; left: 20px; z-index: 1000;
        background: white; border: 1px solid #e5e7eb; border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,.12); padding: 10px 12px; max-height: 45vh; overflow-y: auto;'>
      <details open>
        <summary style='cursor:pointer;font-weight:600;color:#111;'>{titulo}</summary>
        <div style='margin-top:8px;'>
          <table style='border-collapse:collapse; width:100%; font-size:12px;' class='ta-sortable'>
            <thead>
              <tr>
                <th style='text-align:left; padding:6px 8px; border-bottom:1px solid #eee;'>{label_col}</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='Cantidad total de visitas registradas en el período, incluyendo re-visitas al mismo cliente' data-type='num'>#Muestras</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='Personas distintas visitadas. Si el mismo cliente recibió varias muestras, se cuenta una sola vez (la más reciente). Todas las métricas se calculan sobre este número.' data-type='num'>#Clientes</th>
                <th style='text-align:center; padding:6px 4px; border-bottom:1px solid #eee;' title='Tamaño del territorio cubierto, calculado a partir de los puntos GPS registrados' data-type='num'>Área km²</th>
                <th style='text-align:center; padding:6px 4px; border-bottom:1px solid #eee;' title='Qué tan concentrado está el trabajo en el territorio. Un número alto indica rutas más eficientes.' data-type='num'>Clientes/km²</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='Promedio de clientes visitados en los días donde hubo al menos 2 registros (días de operación real)' data-type='num'>Clientes/día hábil</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='Clientes que aún no tienen relación activa con la empresa (no son Ticket, Frecuente ni Especial). Son el público objetivo de la muestra.' data-type='percent'>% No fieles</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='De todos los clientes visitados, cuántos contestaron al menos una llamada después de recibir la muestra. Solo cuentan llamadas posteriores a la fecha de visita.' data-type='percent'>% Contactabilidad</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='De los clientes nuevos (no fieles) que visité, cuántos contestaron el teléfono después de la muestra. Mide qué tan alcanzable es ese segmento para telemercadeo.' data-type='percent'>% Contactab. No Fieles</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='De cada 100 clientes visitados (nuevos y fieles), cuántos son nuevos Y además contestaron. Es la tasa de captación real: visita + contacto telefónico efectivo.' data-type='percent'>% Captación</th>
                <th style='text-align:right; padding:6px 8px; border-bottom:1px solid #eee;' title='De los clientes que contestaron después de la muestra, cuántos terminaron comprando. Mide qué tan efectivo fue el cierre de telemercadeo una vez logrado el contacto.' data-type='percent'>% Conversión</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>
        </div>
      </details>
    </div>
    """


def compactar_dos_palabras(nombre_completo, pid=""):
    if not nombre_completo:
        return f"id {pid}".strip()
    tokens = [t.lower() for t in re.split(r"\s+", str(nombre_completo).strip()) if t]
    n = len(tokens)
    if n == 1:  return tokens[0]
    if n == 2:  return f"{tokens[0]} {tokens[1]}"
    if n == 3:  return f"{tokens[0]} {tokens[1]}"
    return f"{tokens[0]} {tokens[2]}"


def get_promotor_display_name(pid, df_filtrado, legend_name_map=None):
    pid_str = str(pid)
    if legend_name_map and pid_str in legend_name_map:
        return legend_name_map[pid_str]
    datos_promotor = df_filtrado[df_filtrado['id_promotor'] == pid]
    if not datos_promotor.empty:
        row = datos_promotor.iloc[0]
        for col in ['apellido_promotor', 'apellido', 'promotor_nombre', 'nombre_autor', 'nombre_completo']:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return compactar_dos_palabras(str(row[col]), pid_str)
    return f"Promotor {pid_str}"


def build_promotores_groups(
    df: pd.DataFrame,
    parent_group,
    colores_promotores_map,
    legend_name_map=None,
    mapa=None,
    grupos_por_promotor=None,
):
    if grupos_por_promotor is None:
        grupos_por_promotor = dict(tuple(df.groupby('id_promotor')))

    promotor_counts = {pid: len(df_sub) for pid, df_sub in grupos_por_promotor.items()}
    orden_ids = sorted(promotor_counts.keys(), key=promotor_counts.get, reverse=True)

    grupos_promotores = []
    valores_colores = list(colores_promotores_map.values()) or ["#64748B"]
    for idx, pid in enumerate(orden_ids):
        datos_promotor = grupos_por_promotor.get(pid)
        if datos_promotor is None or datos_promotor.empty:
            continue
        count = promotor_counts.get(pid, len(datos_promotor))

        nombre_promotor = get_promotor_display_name(pid, df, legend_name_map)
        color_promotor = (
            colores_promotores_map.get(str(pid))
            or colores_promotores_map.get(pid)
            or valores_colores[idx % len(valores_colores)]
        )

        sg = FeatureGroupSubGroup(parent_group, name=nombre_promotor, show=True)
        if mapa is not None:
            mapa.add_child(sg)

        # Convertir a arrays numpy para evitar iterrows() fila por fila
        lat_col  = 'coordenada_latitud'  if 'coordenada_latitud'  in datos_promotor.columns else 'latitud'
        lng_col  = 'coordenada_longitud' if 'coordenada_longitud' in datos_promotor.columns else 'longitud'
        arr = datos_promotor[[lat_col, lng_col, 'id_contacto', 'fecha_evento']].copy()
        arr[lat_col] = pd.to_numeric(arr[lat_col], errors='coerce')
        arr[lng_col] = pd.to_numeric(arr[lng_col], errors='coerce')
        arr = arr.dropna(subset=[lat_col, lng_col])

        for row_vals in arr.itertuples(index=False):
            lat = row_vals[0]
            lng = row_vals[1]
            id_c = row_vals[2]
            fecha = str(row_vals[3])[:10]          # solo fecha YYYY-MM-DD
            # tooltip liviano (texto plano) — mucho más rápido que Popup con HTML
            tooltip_txt = f"{nombre_promotor} · {id_c} · {fecha}"
            folium.CircleMarker(
                location=[float(lat), float(lng)],
                radius=5,
                color=color_promotor,
                fill=True,
                fillColor=color_promotor,
                fillOpacity=0.9,
                tooltip=tooltip_txt,
            ).add_to(sg)

        grupos_promotores.append((nombre_promotor, sg, count, color_promotor))

    return grupos_promotores


# ==== Helpers de áreas / cuadrantes ====
def area_m2_geodesic(geom_geojson: dict) -> float:
    g = Geod(ellps="WGS84")
    geom = shape(geom_geojson)
    area = 0.0
    try:
        if geom.geom_type == 'Polygon':
            lons, lats = geom.exterior.coords.xy
            area_poly, _ = g.polygon_area_perimeter(lons, lats)
            area += abs(area_poly)
            for interior in geom.interiors:
                lons, lats = interior.coords.xy
                hole_area, _ = g.polygon_area_perimeter(lons, lats)
                area -= abs(hole_area)
        elif geom.geom_type == 'MultiPolygon':
            for poly in geom.geoms:
                lons, lats = poly.exterior.coords.xy
                area_poly, _ = g.polygon_area_perimeter(lons, lats)
                area += abs(area_poly)
                for interior in poly.interiors:
                    lons, lats = interior.coords.xy
                    hole_area, _ = g.polygon_area_perimeter(lons, lats)
                    area -= abs(hole_area)
        else:
            return 0.0
        return float(area)
    except Exception:
        return 0.0


def _calcular_area_m2_fallback(geom_geojson: dict) -> float:
    try:
        geom = shape(geom_geojson)
        bounds = geom.bounds
        lat_center = (bounds[1] + bounds[3]) / 2
        lat_rad = math.radians(lat_center)
        meters_per_degree_lat = 111000
        meters_per_degree_lon = 111000 * math.cos(lat_rad)
        width_m = (bounds[2] - bounds[0]) * meters_per_degree_lon
        height_m = (bounds[3] - bounds[1]) * meters_per_degree_lat
        area_deg2 = geom.area
        bbox_deg2 = (bounds[2]-bounds[0]) * (bounds[3]-bounds[1])
        escala = (width_m * height_m / bbox_deg2) if bbox_deg2 > 0 else 0.0
        return max(0.0, area_deg2 * escala)
    except Exception:
        return 0.0


def _contar_muestras_en_geom(feature_geom: dict, df_pts: pd.DataFrame) -> int:
    try:
        geom = shape(feature_geom)
        prep_geom = prep(geom)
        count = 0
        for _, r in df_pts.iterrows():
            p = Point(float(r['lon']), float(r['lat']))
            if prep_geom.contains(p):
                count += 1
        return count
    except Exception:
        return 0


def _dias_activos_global(df_pts: pd.DataFrame) -> int:
    if df_pts.empty or 'fecha_dia' not in df_pts.columns:
        return 0
    return int(df_pts['fecha_dia'].nunique())


def _dias_activos_en_geom(feature_geom: dict, df_pts: pd.DataFrame) -> int:
    if df_pts.empty or 'fecha_dia' not in df_pts.columns:
        return 0
    try:
        geom = shape(feature_geom)
        prep_geom = prep(geom)
        dias = set()
        for _, r in df_pts.iterrows():
            p = Point(float(r['lon']), float(r['lat']))
            if prep_geom.contains(p):
                dias.add(r['fecha_dia'])
        return len(dias)
    except Exception:
        return 0


def _calcular_metricas_hijo(feature: dict, df_filtrado: pd.DataFrame) -> dict:
    props = feature.get('properties', {})
    codigo = props.get('codigo', '')
    try:
        area_m2 = area_m2_geodesic(feature.get('geometry', {}))
        metodo_area = "geodésico"
    except Exception:
        area_m2 = _calcular_area_m2_fallback(feature.get('geometry', {}))
        metodo_area = "fallback"
        logging.warning(f"[AREAS] Fallback en hijo {codigo}")
    total_muestras = _contar_muestras_en_geom(feature.get('geometry', {}), df_filtrado)
    dias_activos = _dias_activos_en_geom(feature.get('geometry', {}), df_filtrado)
    result = {
        'codigo': codigo,
        'area_m2': area_m2,
        'total_muestras': total_muestras,
        'dias_activos': dias_activos
    }
    if DEBUG_AREAS:
        result['metodo_area'] = metodo_area
    return result


def _calcular_metricas_padre(feature_padre: dict, features_hijos: list, metricas_hijos: dict, df_for_conteo: pd.DataFrame) -> dict:
    props_padre = feature_padre.get('properties', {})
    codigo_padre = props_padre.get('codigo', '')
    try:
        area_total = area_m2_geodesic(feature_padre.get('geometry', {}))
        metodo_area = "geodésico"
    except Exception:
        area_total = _calcular_area_m2_fallback(feature_padre.get('geometry', {}))
        metodo_area = "fallback"
        logging.warning(f"[AREAS] Fallback en padre {codigo_padre}")
    muestras_total = _contar_muestras_en_geom(feature_padre.get('geometry', {}), df_for_conteo)
    dias_activos = _dias_activos_en_geom(feature_padre.get('geometry', {}), df_for_conteo)
    result = {
        'codigo': codigo_padre,
        'area_m2': area_total,
        'total_muestras': muestras_total,
        'dias_activos': dias_activos
    }
    if DEBUG_AREAS:
        result['metodo_area'] = metodo_area
    return result


def _popup_cuadrante_muestras(codigo: str, area_m2: float, total_local: int, dias_activos: int, metodo_area: str = None, tipo_capa: str = None, verificacion_info: dict = None, ciudad: str = None, n_promotores: int = None) -> str:
    area_km2 = area_m2 / 1_000_000 if area_m2 > 0 else 0.0
    if n_promotores is None:
        n_promotores = 1
    tasa = (total_local / (n_promotores * dias_activos)) if (n_promotores > 0 and dias_activos > 0) else 0.0
    hogares_estimados = 0
    area_m2_fmt = __fmt_es(area_m2, 0)
    area_km2_fmt = __fmt_es(area_km2, 2, False)
    cantidad_fmt = __fmt_es(total_local, 0)
    tasa_fmt = __fmt_es(tasa, 2, False)
    hogares_estimados_fmt = __fmt_es(hogares_estimados, 0) if hogares_estimados > 0 else "N/D"
    debug_lines = ""
    if DEBUG_AREAS:
        if metodo_area:
            debug_lines += f'<div style="font-size:11px;color:#6b7280;margin-bottom:2px;">Método de área: {metodo_area}</div>'
        if tipo_capa:
            debug_lines += f'<div style="font-size:11px;color:#6b7280;margin-bottom:2px;">Código: {codigo} · Capa: {tipo_capa}</div>'
        if verificacion_info:
            if verificacion_info['verificado']:
                debug_lines += f'<div style="font-size:11px;color:#16a34a;margin-bottom:2px;">✓ Área verificada (geodésica)</div>'
            else:
                diff_pct = verificacion_info['diff_pct']
                debug_lines += f'<div style="font-size:11px;color:#dc2626;margin-bottom:2px;">⚠ Mismatch área cache vs draw: {diff_pct:.1f}%</div>'
            tipo_geom = verificacion_info['tipo_geom']
            num_anillos = verificacion_info['num_anillos']
            debug_lines += f'<div style="font-size:10px;color:#9ca3af;margin-bottom:4px;">{tipo_geom} ({num_anillos} anillo{"s" if num_anillos != 1 else ""})</div>'
    return f"""
    <div style="font-family: Inter, system-ui; font-size: 14px; line-height: 1.3;">
        <div style="font-weight:600; margin-bottom:8px; font-size:16px;">{codigo}</div>
        {debug_lines}
        <div style="margin-top:8px; font-size:13px; line-height:1.4;">
            <div><strong>Muestras (local):</strong> {cantidad_fmt}</div>
            <div><strong>Días de operación:</strong> {dias_activos}</div>
            <div><strong>Promotores:</strong> {n_promotores}</div>
            <div><strong>Tasa:</strong> {tasa_fmt}</div>
        </div>
        <div style="margin-top:6px; font-size:11px; color:#6b7280;">
            Área: {area_m2_fmt} m² ({area_km2_fmt} km²) • Hogares estimados: {hogares_estimados_fmt}
        </div>
    </div>
    """


def _style_cuadrante_padre(feat):
    base = _style_cuadrante(feat)
    base.update({'fillOpacity': 0.5, 'weight': 1.5})
    return base


def _asignar_cuadrante_a_puntos(df_pts: pd.DataFrame, features: list) -> pd.Series:
    if df_pts.empty:
        return pd.Series([None] * 0, name="cod_cuadrante")
    puntos = []
    for _, r in df_pts.iterrows():
        try:
            puntos.append(Point(float(r['lon']), float(r['lat'])))
        except Exception:
            puntos.append(None)
    res = [None] * len(df_pts)
    for feat in features:
        props = feat.get('properties', {})
        codigo = props.get('codigo') or props.get('CODIGO') or props.get('code') or ''
        if not codigo:
            continue
        try:
            geom = shape(feat.get('geometry', {}))
            pgeom = prep(geom)
            for i, p in enumerate(puntos):
                if p is None:
                    continue
                if res[i] is None and pgeom.contains(p):
                    res[i] = codigo
        except Exception:
            continue
    return pd.Series(res, index=df_pts.index, name="cod_cuadrante")


def _calcular_metricas_agrupadas(
    df_original: pd.DataFrame,
    df_filtrado: pd.DataFrame,
    agrupar_por: Literal["Promotor", "Mes"],
) -> pd.DataFrame:
    if df_original.empty or df_filtrado.empty:
        return pd.DataFrame()

    # Copia de df_filtrado para cálculos de fidelidad/contactabilidad
    df_c = df_filtrado.copy()

    # Flags de fidelidad
    df_c['es_no_fiel'] = ~df_c['id_contacto_categoria'].isin(CATEGORIAS_FIELES)

    # Contactabilidad real (fuente: aplicar_contactabilidad_temporal)
    # Columnas es_contactable y es_venta ya vienen procesadas con filtro temporal
    # (llamada POSTERIOR a la muestra del promotor para ese contacto).
    # Si por algún motivo no están, cae a 0.
    for _flag in ['es_contactable', 'es_venta']:
        if _flag not in df_c.columns:
            df_c[_flag] = 0
        df_c[_flag] = df_c[_flag].fillna(0).astype(int)

    df_c['es_contactado']          = df_c['es_contactable'].astype(bool)
    df_c['es_venta_realizada']     = df_c['es_venta'].astype(bool)
    df_c['es_contactado_no_fiel']  = df_c['es_no_fiel'] & df_c['es_contactado']

    # Asegurar fecha_dia en original y filtrado
    if 'fecha_evento' in df_original.columns and 'fecha_dia' not in df_original.columns:
        df_original['fecha_dia'] = df_original['fecha_evento'].dt.date
    if 'fecha_evento' in df_c.columns and 'fecha_dia' not in df_c.columns:
        df_c['fecha_dia'] = df_c['fecha_evento'].dt.date

    if agrupar_por == 'Promotor':
        # Muestras totales (volumen de eventos) sobre df_original
        muestras_total_prom = (
            df_original.groupby('id_promotor')['id_muestra'].size().rename('muestras_total').reset_index()
        )
        # Clientes únicos sobre df_filtrado (df_c)
        clientes_por_prom = (
            df_c.groupby('id_promotor')['id_contacto'].nunique().rename('clientes_total').reset_index()
        )
        # Días hábiles: días con >= 2 muestras en df_original
        df_dias = (
            df_original.groupby(['id_promotor', 'fecha_dia']).size().reset_index(name='n_dia')
        )
        dias_habiles = (
            df_dias.assign(es_habil=df_dias['n_dia'] >= 2)
                    .groupby('id_promotor')['es_habil'].sum().rename('dias_habiles').reset_index()
        )
        # Agregados fidelidad/contacto sobre df_c (fuente real: llamadas post-muestra)
        agg_contacto = (
            df_c.groupby('id_promotor').agg(
                clientes_no_fieles=('es_no_fiel', 'sum'),
                clientes_contactables=('es_contactado', 'sum'),
                clientes_contactables_no_fieles=('es_contactado_no_fiel', 'sum'),
                clientes_venta=('es_venta_realizada', 'sum'),
            ).reset_index()
        )
        df_agrupado = (
            muestras_total_prom
            .merge(clientes_por_prom, on='id_promotor', how='left')
            .merge(agg_contacto, on='id_promotor', how='left')
            .merge(dias_habiles, on='id_promotor', how='left')
        )
        # Añadir nombre/apellido de promotor a df_agrupado
        nombres = (
            df_filtrado[["id_promotor", "apellido_promotor"]]
            .dropna(subset=["id_promotor"])
            .drop_duplicates(subset=["id_promotor"])
        )
        df_agrupado = df_agrupado.merge(nombres, on="id_promotor", how="left")
    elif agrupar_por == 'Mes':
        # Asegurar columna mes
        if 'mes' not in df_original.columns:
            df_original['mes'] = df_original['fecha_evento'].dt.month
        if 'mes' not in df_c.columns:
            df_c['mes'] = df_c['fecha_evento'].dt.month
        muestras_por_mes = (
            df_original.groupby('mes')['id_muestra'].size().rename('muestras_total').reset_index()
        )
        clientes_por_mes = (
            df_c.groupby('mes')['id_contacto'].nunique().rename('clientes_total').reset_index()
        )
        df_dias_mes = (
            df_original.groupby(['mes', 'fecha_dia']).size().reset_index(name='n_dia')
        )
        dias_habiles_mes = (
            df_dias_mes.assign(es_habil=df_dias_mes['n_dia'] >= 2)
                       .groupby('mes')['es_habil'].sum().rename('dias_habiles').reset_index()
        )
        agg_contacto_mes = (
            df_c.groupby('mes').agg(
                clientes_no_fieles=('es_no_fiel', 'sum'),
                clientes_contactables=('es_contactado', 'sum'),
                clientes_contactables_no_fieles=('es_contactado_no_fiel', 'sum'),
                clientes_venta=('es_venta_realizada', 'sum'),
            ).reset_index()
        )
        df_agrupado = (
            muestras_por_mes
            .merge(clientes_por_mes, on='mes', how='left')
            .merge(agg_contacto_mes, on='mes', how='left')
            .merge(dias_habiles_mes, on='mes', how='left')
        )
    else:
        raise ValueError(f"Valor agrupar_por no soportado: {agrupar_por}")

    if df_agrupado.empty:
        return df_agrupado

    # Porcentajes y métricas derivadas
    def _pct(a, b):
        try:
            return 100.0 * float(a) / float(b) if b and b != 0 else 0.0
        except Exception:
            return 0.0

    df_agrupado['pct_clientes_no_fieles'] = df_agrupado.apply(
        lambda r: _pct(r['clientes_no_fieles'], r['clientes_total']), axis=1
    )
    # % Contactabilidad real: clientes con llamada contestada o exitosa / clientes únicos
    df_agrupado['pct_total_muestras_contactables'] = df_agrupado.apply(
        lambda r: _pct(r['clientes_contactables'], r['clientes_total']), axis=1
    )
    # % Contactabilidad No Fieles real: no fieles contactados / total no fieles
    df_agrupado['pct_contactabilidad_no_fieles'] = df_agrupado.apply(
        lambda r: _pct(r['clientes_contactables_no_fieles'], r['clientes_no_fieles']), axis=1
    )
    # % Captación efectiva: no fieles contactados / total clientes
    # Responde: "¿Qué % de todos mis clientes son nuevos Y me dijeron aló?"
    # Ejemplo Diana: 1.127 / 2.417 = 46,6%
    df_agrupado['pct_nofiel_contactable'] = df_agrupado.apply(
        lambda r: _pct(r.get('clientes_contactables_no_fieles', 0), r['clientes_total']), axis=1
    )
    # % Conversión: es_venta=1 / clientes contactados reales (no sobre total)
    df_agrupado['pct_conversion'] = df_agrupado.apply(
        lambda r: _pct(r.get('clientes_venta', 0), r.get('clientes_contactables', 0)), axis=1
    )
    df_agrupado['clientes_por_dia_habil'] = df_agrupado.apply(
        lambda r: (r['clientes_total'] / r['dias_habiles']) if r['dias_habiles'] and r['dias_habiles'] > 0 else None,
        axis=1,
    )

    return df_agrupado


def _t(label: str, t0: float, t_prev: float) -> float:
    """Imprime el tiempo de un paso y retorna el timestamp actual."""
    import time as _time
    now = _time.perf_counter()
    elapsed = now - t_prev
    tag = "BD " if elapsed > 0.5 else "MEM"
    print(f"  [{tag}] {label:<30} {elapsed:>6.2f} s")
    return now


def generar_mapa_muestras(
    fecha_inicio: str,
    fecha_fin: str,
    ciudad: str,
    agrupar_por: Literal["Promotor", "Mes"],
):
    """Nueva versión lógica del mapa de muestras (modo clientes siempre).

    Retorna:
      df_original : todas las muestras válidas
      df_filtrado : clientes únicos (última muestra por promotor)
      df_agrupado : métricas agregadas para leyenda detalle
    """
    import time as _time
    _t0 = _time.perf_counter()
    _tp = _t0
    print(f"\n{'─'*55}")
    print(f"  ATLAS TIMING  {ciudad}  [{agrupar_por}]  {fecha_inicio} → {fecha_fin}")
    print(f"{'─'*55}")

    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in CENTROOPES:
        raise ValueError(f"Ciudad desconocida: {ciudad}")
    centroope = CENTROOPES[ciudad_norm]

    # 1. Obtener datos crudos y normalizarlos
    df_raw = consultar_db(
        id_centroope=centroope,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ids_promotor=None,
    )
    _tp = _t("consultar_db (muestras)", _t0, _tp)

    df_original = crear_df(df_raw)
    _tp = _t("crear_df + normalizar", _t0, _tp)

    if df_original.empty:
        print(f"  [---] Sin datos → saliendo\n{'─'*55}\n")
        return df_original, df_original.copy(), pd.DataFrame()

    # 2. fecha_dia para df_original (puede requerirse en agrupados)
    if 'fecha_evento' in df_original.columns:
        df_original['fecha_dia'] = df_original['fecha_evento'].dt.date

    # 2b. Excluir promotores con menos de MIN_MUESTRAS_PROMOTOR muestras
    #     Se cuenta sobre df_original (todas las muestras, sin deduplicar).
    conteo_por_promotor = df_original.groupby('id_promotor', observed=True).size()
    promotores_validos  = conteo_por_promotor[conteo_por_promotor >= MIN_MUESTRAS_PROMOTOR].index
    df_original = df_original[df_original['id_promotor'].isin(promotores_validos)].copy()
    _tp = _t(f"filtro ≥{MIN_MUESTRAS_PROMOTOR} muestras", _t0, _tp)

    if df_original.empty:
        print(f"  [---] Sin promotores válidos → saliendo\n{'─'*55}\n")
        return df_original, df_original.copy(), pd.DataFrame()

    # 3. Construir df_filtrado (modo clientes: última muestra por promotor-contacto)
    df_filtrado = (
        df_original.copy()
                  .sort_values('fecha_evento')
                  .drop_duplicates(subset=['id_promotor', 'id_contacto'], keep='last')
    )
    _tp = _t("dedup clientes únicos", _t0, _tp)

    # 4. Contactabilidad con filtro temporal
    #    Regla: solo llamadas POSTERIORES a la muestra del promotor cuentan.
    #    consultar_llamadas_raw devuelve (id_contacto, fecha_llamada, es_contactable, es_venta)
    #    aplicar_contactabilidad_temporal cruza por (promotor, contacto) y filtra
    #    fecha_llamada > fecha_evento antes de agregar los flags.
    ids_contacto = tuple(
        sorted(df_original['id_contacto'].dropna().astype(int).unique().tolist())
    )
    df_llamadas_raw = consultar_llamadas_raw(
        ids_contacto=ids_contacto,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    _tp = _t(f"consultar_llamadas_raw ({len(ids_contacto)} ids)", _t0, _tp)

    df_filtrado = aplicar_contactabilidad_temporal(df_filtrado, df_llamadas_raw)
    _tp = _t("aplicar_contactabilidad_temporal", _t0, _tp)

    # 5. Métricas de contacto / fidelidad
    df_metricas = _calcular_metricas_agrupadas(df_original, df_filtrado, agrupar_por)
    _tp = _t("métricas agrupadas", _t0, _tp)

    # Si no hay métricas, devolvemos tal cual
    if df_metricas.empty:
        print(f"  [---] Sin métricas\n{'─'*55}\n")
        return df_original, df_filtrado, df_metricas

    # 6. Áreas (M2) según agrupar_por, usando el df_filtrado (modo clientes)
    # calcular_areas_por_promotor espera columna id_autor
    df_para_areas = df_filtrado.rename(columns={"id_promotor": "id_autor"})
    df_areas = areas_muestras_resumen(
        df=df_para_areas,
        centroope=centroope,
        agrupar_por=agrupar_por,
    )
    _tp = _t("áreas M2 (geoespacial)", _t0, _tp)

    # 6. Merge métricas + áreas
    if agrupar_por == "Promotor":
        if not df_areas.empty:
            df_agrupado = df_metricas.merge(
                df_areas[["id_autor", "area_m2"]],
                left_on="id_promotor",
                right_on="id_autor",
                how="left",
            ).drop(columns=["id_autor"])
        else:
            df_agrupado = df_metricas.copy()
            df_agrupado["area_m2"] = None
    elif agrupar_por == "Mes":
        if not df_areas.empty and "mes" in df_metricas.columns:
            df_agrupado = df_metricas.merge(
                df_areas[["mes", "area_m2"]],
                on="mes",
                how="left",
            )
        else:
            df_agrupado = df_metricas.copy()
            df_agrupado["area_m2"] = None
    else:
        df_agrupado = df_metricas.copy()

    # Ajuste de densidad con factor 1000 para continuidad con legacy
    try:
        if 'area_m2' in df_agrupado.columns and 'clientes_total' in df_agrupado.columns:
            df_agrupado['clientes_por_area_m2'] = df_agrupado.apply(
                lambda r: (float(r['clientes_total']) * 1000.0 / float(r['area_m2'])) if pd.notna(r['area_m2']) and float(r['area_m2']) > 0 else None,
                axis=1
            )
    except Exception:
        pass

    import time as _time
    _total = _time.perf_counter() - _t0
    print(f"  {'─'*45}")
    print(f"  {'TOTAL datos':.<30} {_total:>6.2f} s")
    print(f"{'─'*55}\n")

    return df_original, df_filtrado, df_agrupado


# ==== Visual: construcción del mapa y leyendas ====
def generar_mapa_muestras_visual(
    fecha_inicio: str,
    fecha_fin: str,
    ciudad: str,
    agrupar_por: Literal["Promotor", "Mes"],
    auditoria: bool = False,
    override_fc=None,
):
    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in CENTROOPES or ciudad_norm not in coordenadas_ciudades:
        mapa = folium.Map(location=[4.7110, -74.0721], zoom_start=12)
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras", permitir_multiples=False)
        return filename, 0, None

    import time as _time
    _tv0 = _time.perf_counter()
    _tvp = _tv0

    centroope = CENTROOPES[ciudad_norm]
    location, geojson_file_path = coordenadas_ciudades[ciudad_norm]

    df_original, df_filtrado, df_agrupado = generar_mapa_muestras(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ciudad=ciudad,
        agrupar_por=agrupar_por,
    )
    _tvp = _t("  datos totales (↑ desglose)", _tv0, _tvp)

    # Chequeo vacío
    if df_original is None or df_original.empty:
        mapa = folium.Map(location=location, zoom_start=12)
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras", permitir_multiples=False)
        return filename, 0, None

    # Crear mapa base
    mapa = folium.Map(location=location, zoom_start=12)
    # CSS z-index
    zindex_css = """
    <style>
    .leaflet-interactive[style*='stroke-width: 3'] { z-index: 630 !important; }
    .leaflet-interactive[style*='stroke-width: 2'] { z-index: 620 !important; }
    .leaflet-overlay-pane .leaflet-interactive { position: relative; }
    </style>
    """
    mapa.get_root().html.add_child(folium.Element(zindex_css))

    # Grupos base
    comunas_group = folium.FeatureGroup(name="Comunas", show=True).add_to(mapa)
    cuadrantes_padres_group = folium.FeatureGroup(name="Cuadrantes (Padres)", show=True).add_to(mapa)
    cuadrantes_hijos_group = folium.FeatureGroup(name="Cuadrantes (Hijos)", show=True).add_to(mapa)

    # Cargar GeoJSON
    if override_fc is not None:
        barrios_geojson = override_fc
    else:
        try:
            with open(geojson_file_path, 'r', encoding='utf-8') as f:
                barrios_geojson = json.load(f)
        except Exception:
            barrios_geojson = {"type": "FeatureCollection", "features": []}

    # Separar features
    features_comunas = []
    features_cuadrantes = []
    for feature in barrios_geojson.get('features', []):
        props = feature.get('properties', {})
        codigo_val = (props.get('codigo') or props.get('CODIGO') or props.get('code') or '').strip()
        props['display_name'] = resolver_nombre_ruta(ciudad_norm, codigo_val, props)
        if codigo_val:
            if _es_cuadrante_padre(feature) or _es_cuadrante_hijo(feature):
                features_cuadrantes.append(feature)
            else:
                features_comunas.append(feature)
        else:
            features_comunas.append(feature)

    # Dibujar comunas
    for feature in features_comunas:
        folium.GeoJson(
            data=feature,
            style_function=lambda x: {'fillColor': 'transparent', 'color': '#000000', 'weight': 1.5, 'fillOpacity': 0.0}
        ).add_to(comunas_group)

    # Preparar DF para conteo por geometría (clientes únicos)
    df_for_conteo = df_filtrado.copy()
    if 'fecha_evento' in df_for_conteo.columns:
        df_for_conteo['fecha_dia'] = df_for_conteo['fecha_evento'].dt.date
    df_for_conteo['lat'] = df_for_conteo.apply(lambda r: r.get('coordenada_latitud', r.get('latitud', None)), axis=1)
    df_for_conteo['lon'] = df_for_conteo.apply(lambda r: r.get('coordenada_longitud', r.get('longitud', None)), axis=1)
    df_for_conteo = df_for_conteo.dropna(subset=['lat', 'lon'])

    features_padres = [f for f in features_cuadrantes if _es_cuadrante_padre(f)]
    features_hijos = [f for f in features_cuadrantes if _es_cuadrante_hijo(f)]
    metricas_cache = {}
    area_map = {}
    for fh in features_hijos:
        met = _calcular_metricas_hijo(fh, df_for_conteo)
        metricas_cache[('HIJO', met['codigo'])] = met
        area_map[met['codigo']] = met['area_m2']
    for fp in features_padres:
        met = _calcular_metricas_padre(fp, features_hijos, metricas_cache, df_for_conteo)
        metricas_cache[('PADRE', met['codigo'])] = met
        area_map[met['codigo']] = met['area_m2']

    # Dibujar padres
    for feature_padre in features_padres:
        props = feature_padre.get('properties', {})
        codigo = str(props.get('codigo', ''))
        nombre_display = props.get('display_name', codigo)
        m = metricas_cache.get(('PADRE', codigo))
        if not m:
            continue
        metodo_area = m.get('metodo_area') if DEBUG_AREAS else None
        total_local = m.get('total_muestras', 0)
        dias_activos = m.get('dias_activos', 0)
        popup_html = _popup_cuadrante_muestras(
            codigo,
            m['area_m2'],
            total_local,
            dias_activos,
            metodo_area=metodo_area,
            tipo_capa='PADRE',
            verificacion_info=None,
            ciudad=ciudad_norm,
            n_promotores=None,
        )
        layer_padre = folium.GeoJson(
            data=feature_padre,
            style_function=_style_cuadrante_padre,
            popup=folium.Popup(popup_html, max_width=500),
            tooltip=folium.Tooltip(f"<b>{nombre_display}</b>"),
        )
        layer_padre.add_to(cuadrantes_padres_group)

    # Dibujar hijos
    for feature_hijo in features_hijos:
        props = feature_hijo.get('properties', {})
        codigo = str(props.get('codigo', ''))
        nombre_display = props.get('display_name', codigo)
        m = metricas_cache.get(('HIJO', codigo))
        if not m:
            continue
        metodo_area = m.get('metodo_area') if DEBUG_AREAS else None
        total_local = m.get('total_muestras', 0)
        dias_activos = m.get('dias_activos', 0)
        popup_html = _popup_cuadrante_muestras(
            codigo,
            m['area_m2'],
            total_local,
            dias_activos,
            metodo_area=metodo_area,
            tipo_capa='HIJO',
            verificacion_info=None,
            ciudad=ciudad_norm,
            n_promotores=None,
        )
        layer_hijo = folium.GeoJson(
            data=feature_hijo,
            style_function=_style_cuadrante,
            popup=folium.Popup(popup_html, max_width=500),
            tooltip=folium.Tooltip(f"<b>{nombre_display}</b>"),
        )
        layer_hijo.add_to(cuadrantes_hijos_group)

    # Grupos de puntos
    # NOTA: los puntos del mapa usan df_original (TODAS las muestras, incluyendo
    # re-visitas al mismo cliente). Las métricas de la leyenda siguen calculadas
    # sobre df_filtrado (cliente único por promotor, última muestra).
    legend_html = ""
    if agrupar_por == "Promotor":
        fg_promotores = folium.FeatureGroup(name="PROMOTORES", show=True).add_to(mapa)
        grupos_por_promotor = dict(tuple(df_original.groupby('id_promotor')))
        promotores_ordenados = sorted(
            grupos_por_promotor.keys(),
            key=lambda pid: len(grupos_por_promotor[pid]),
            reverse=True,
        )
        promotores_ordenados = [int(pid) for pid in promotores_ordenados]
        colores_promotores_map = {str(pid): color_for_promotor(centroope, pid) for pid in promotores_ordenados}

        # legend_name_map desde df_original (apellido_promotor)
        legend_name_map = {}
        if 'apellido_promotor' in df_original.columns:
            tmp = df_original[["id_promotor", "apellido_promotor"]].dropna().drop_duplicates("id_promotor")
            for _, r in tmp.iterrows():
                pid = str(r['id_promotor'])
                legend_name_map[pid] = compactar_dos_palabras(r['apellido_promotor'], pid)

        grupos_promotores = build_promotores_groups(
            df_original,                        # todas las muestras en el mapa
            parent_group=fg_promotores,
            colores_promotores_map=colores_promotores_map,
            legend_name_map=legend_name_map,
            mapa=mapa,
            grupos_por_promotor=grupos_por_promotor,
        )

        # Control de capas
        if HAS_TREE_CONTROL:
            TreeLayerControl(collapsed=True, position='topright').add_to(mapa)
        else:
            folium.LayerControl(collapsed=True, position='topright').add_to(mapa)

        # Leyenda detalle desde df_agrupado
        rows_struct = []
        df_leg = df_agrupado.copy()
        if not df_leg.empty:
            df_leg = df_leg.sort_values('muestras_total', ascending=False)
            for _, r in df_leg.iterrows():
                pid = r.get('id_promotor')
                etiqueta = compactar_dos_palabras(r.get('apellido_promotor'), pid) if pd.notna(r.get('apellido_promotor')) else get_promotor_display_name(pid, df_filtrado, legend_name_map)
                area_m2 = r.get('area_m2')
                # Convertir siempre a km² correctamente
                area_km2 = (float(area_m2) / 1_000_000.0) if (pd.notna(area_m2) and float(area_m2) > 0) else None
                clientes_total = int(r.get('clientes_total') or 0)
                muestras_por_km2 = (clientes_total / area_km2) if (area_km2 is not None and area_km2 > 0) else None
                rows_struct.append(_build_legend_row_struct(
                    etiqueta=etiqueta,
                    muestras_total=int(r.get('muestras_total') or 0),
                    clientes_total=clientes_total,
                    dias_habiles=int(r.get('dias_habiles') or 0),
                    pct_no_fieles=float(r.get('pct_clientes_no_fieles') or 0.0),
                    pct_contactables=float(r.get('pct_total_muestras_contactables') or 0.0),
                    pct_contactables_nofieles=float(r.get('pct_contactabilidad_no_fieles') or 0.0),
                    area_km2=area_km2,
                    muestras_por_km2=muestras_por_km2,
                    muestras_por_dia_habil=r.get('clientes_por_dia_habil'),
                    color_hex=colores_promotores_map.get(str(int(pid))) if pd.notna(pid) else None,
                    pct_conversion=float(r.get('pct_conversion') or 0.0),
                    pct_nofiel_contactable=float(r.get('pct_nofiel_contactable') or 0.0),
                ))
        legend_html = _render_legend_html_muestras(rows_struct, titulo="Métricas por promotor (clientes únicos)", label_col="Promotor")

    elif agrupar_por == "Mes":
        fg_mes = folium.FeatureGroup(name="MESES", show=True).add_to(mapa)
        df_meswork = df_original.copy()  # todas las muestras en el mapa
        if 'mes' not in df_meswork.columns and 'fecha_evento' in df_meswork.columns:
            df_meswork['mes'] = df_meswork['fecha_evento'].dt.month
        meses_presentes = (
            df_meswork.groupby(['mes']).size().reset_index(name='n').sort_values('mes')
        )
        for _, rr in meses_presentes.iterrows():
            mes = int(rr['mes'])
            color_mes = PALETA_MESES.get(mes, '#999999')
            nombre_mes = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}.get(mes, str(mes))
            sg_mes = FeatureGroupSubGroup(fg_mes, name=nombre_mes, show=True)
            sg_mes.add_to(mapa)
            datos_mes = df_meswork[df_meswork['mes'] == mes]
            lat_col = 'coordenada_latitud'  if 'coordenada_latitud'  in datos_mes.columns else 'latitud'
            lng_col = 'coordenada_longitud' if 'coordenada_longitud' in datos_mes.columns else 'longitud'
            arr_mes = datos_mes[[lat_col, lng_col, 'id_contacto', 'fecha_evento', 'id_promotor']].copy()
            arr_mes[lat_col] = pd.to_numeric(arr_mes[lat_col], errors='coerce')
            arr_mes[lng_col] = pd.to_numeric(arr_mes[lng_col], errors='coerce')
            arr_mes = arr_mes.dropna(subset=[lat_col, lng_col])
            for rv in arr_mes.itertuples(index=False):
                tooltip_txt = f"{nombre_mes} · {rv[2]} · {str(rv[3])[:10]}"
                folium.CircleMarker(
                    location=[float(rv[0]), float(rv[1])],
                    radius=4,
                    color='white',
                    weight=1,
                    fillColor=color_mes,
                    fillOpacity=0.8,
                    tooltip=tooltip_txt,
                ).add_to(sg_mes)
        if HAS_TREE_CONTROL:
            TreeLayerControl(collapsed=True, position='topright').add_to(mapa)
        else:
            folium.LayerControl(collapsed=True, position='topright').add_to(mapa)

        # Leyenda por mes usando df_agrupado
        rows_struct_mes = []
        if not df_agrupado.empty and 'mes' in df_agrupado.columns:
            # Ordenar leyenda por el orden natural de los meses (1..12)
            orden_meses = list(range(1, 13))
            df_leg = (
                df_agrupado
                .copy()
                .assign(_orden=lambda d: d['mes'].map({m: i for i, m in enumerate(orden_meses)}) )
                .sort_values(['_orden', 'mes'], ascending=True)
                .drop(columns=['_orden'])
            )
            for _, r in df_leg.iterrows():
                mes = int(r.get('mes')) if pd.notna(r.get('mes')) else None
                if mes is None:
                    continue
                nombre_mes = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}.get(mes, str(mes))
                area_m2 = r.get('area_m2')
                area_km2 = (area_m2 / 1e6) if (pd.notna(area_m2) and float(area_m2) > 0) else None
                clientes_total = int(r.get('clientes_total') or 0)
                muestras_por_km2 = (clientes_total / area_km2) if (area_km2 is not None and area_km2 > 0) else None
                rows_struct_mes.append(_build_legend_row_struct(
                    etiqueta=nombre_mes,
                    muestras_total=int(r.get('muestras_total') or 0),
                    clientes_total=clientes_total,
                    dias_habiles=int(r.get('dias_habiles') or 0),
                    pct_no_fieles=float(r.get('pct_clientes_no_fieles') or 0.0),
                    pct_contactables=float(r.get('pct_total_muestras_contactables') or 0.0),
                    pct_contactables_nofieles=float(r.get('pct_contactabilidad_no_fieles') or 0.0),
                    area_km2=area_km2,
                    muestras_por_km2=muestras_por_km2,
                    muestras_por_dia_habil=r.get('clientes_por_dia_habil'),
                    color_hex=PALETA_MESES.get(mes, '#999999'),
                    pct_conversion=float(r.get('pct_conversion') or 0.0),
                    pct_nofiel_contactable=float(r.get('pct_nofiel_contactable') or 0.0),
                ))
        legend_html = _render_legend_html_muestras(rows_struct_mes, titulo="Métricas por mes (clientes únicos)", label_col="Mes")

    # ── JS: cascade PROMOTORES checkbox → todos sus hijos ────────────────────
    # Cuando el usuario desmarca "PROMOTORES" todos los sub-grupos individuales
    # se desmarcan también, y viceversa. Funciona con TreeLayerControl y con
    # el LayerControl estándar (estructura plana).
    _cascade_js = """
<script>
(function(){
    function taSetupCascade(){
        var done = false;

        /* ── TreeLayerControl (estructura anidada) ── */
        document.querySelectorAll('.leaflet-layerstree-header').forEach(function(hdr){
            var spans = hdr.querySelectorAll('span');
            var txt = spans.length ? spans[spans.length-1].textContent.trim() : '';
            if(txt.toUpperCase() !== 'PROMOTORES') return;
            var parentCb = hdr.querySelector('input[type="checkbox"]');
            var node = hdr.closest('.leaflet-layerstree-node');
            var kids = node ? node.querySelector('.leaflet-layerstree-children') : null;
            if(parentCb && kids && !parentCb.__taCascade){
                parentCb.__taCascade = true;

                /* Padre → hijos: desmarcar/marcar todos */
                parentCb.addEventListener('change', function(e){
                    var checked = e.target.checked;
                    kids.querySelectorAll('input[type="checkbox"]').forEach(function(c){
                        if(c.checked !== checked) c.click();
                    });
                });

                /* Hijos → padre: activar padre si cualquier hijo se marca */
                kids.querySelectorAll('input[type="checkbox"]').forEach(function(childCb){
                    if(!childCb.__taParentLink){
                        childCb.__taParentLink = true;
                        childCb.addEventListener('change', function(e){
                            if(e.target.checked && !parentCb.checked) parentCb.click();
                        });
                    }
                });
                done = true;
            }
        });
        if(done) return;

        /* ── LayerControl estándar (estructura plana) ── */
        var overlays = document.querySelector('.leaflet-control-layers-overlays');
        if(!overlays){ setTimeout(taSetupCascade, 400); return; }
        var labels = Array.from(overlays.querySelectorAll('label'));
        var promIdx = -1, promCb = null;
        labels.forEach(function(lbl, i){
            var spans = lbl.querySelectorAll('span');
            var t = spans.length ? spans[spans.length-1].textContent.trim() : lbl.textContent.trim();
            if(t.toUpperCase() === 'PROMOTORES' && promIdx === -1){
                promIdx = i;
                promCb = lbl.querySelector('input[type="checkbox"]');
            }
        });
        if(promCb && !promCb.__taCascade){
            promCb.__taCascade = true;

            /* Padre → hijos */
            promCb.addEventListener('change', function(e){
                var checked = e.target.checked;
                labels.slice(promIdx + 1).forEach(function(lbl){
                    var c = lbl.querySelector('input[type="checkbox"]');
                    if(c && c.checked !== checked) c.click();
                });
            });

            /* Hijos → padre: activar padre al marcar cualquier hijo */
            labels.slice(promIdx + 1).forEach(function(lbl){
                var c = lbl.querySelector('input[type="checkbox"]');
                if(c && !c.__taParentLink){
                    c.__taParentLink = true;
                    c.addEventListener('change', function(e){
                        if(e.target.checked && !promCb.checked) promCb.click();
                    });
                }
            });
        } else if(promIdx === -1){
            setTimeout(taSetupCascade, 400);
        }
    }
    [200, 700, 1500].forEach(function(d){ setTimeout(taSetupCascade, d); });
})();
</script>
"""
    mapa.get_root().html.add_child(folium.Element(_cascade_js))

    # Insertar leyenda
    if legend_html:
        inject_sort_assets(mapa)
        mapa.get_root().html.add_child(folium.Element(legend_html))
        mapa.get_root().html.add_child(folium.Element("<script>window.TASortable && window.TASortable.initAll();</script>"))

    # Resumen flotante (ajustado a Clientes)
    try:
        dias_activos_global = 0
        if 'fecha_evento' in df_filtrado.columns:
            df_tmp = df_filtrado.copy()
            df_tmp['fecha_dia'] = df_tmp['fecha_evento'].dt.date
            dias_activos_global = int(df_tmp['fecha_dia'].nunique())
        total_clientes = int(df_filtrado['id_contacto'].nunique()) if 'id_contacto' in df_filtrado.columns else len(df_filtrado)
        total_muestras = int(len(df_original))
        promedio_clientes = (total_clientes / dias_activos_global) if dias_activos_global > 0 else 0.0
        html_resumen = f"""
        <div id='legend-resumen' class='legend-box' style='
            position: fixed; top: 20px; left: 20px; background-color: white; padding: 15px; border-radius: 5px;
            box-shadow: 0 0 10px rgba(0,0,0,0.2); z-index: 1000; font-family: Arial, sans-serif; min-width: 250px;'>
            <div class='legend-header' onclick="toggleLegend('legend-resumen')" style='cursor: pointer; display: flex; justify-content: space-between; align-items: center; margin: 0 0 10px 0;'>
                <h4 style='margin: 0; color: #111;'>Resumen de Clientes</h4>
                <span id='legend-resumen-toggle' class='toggle-icon' style='margin-left: 10px; transition: transform 0.3s ease; font-size: 12px; color: #6b7280;'>▼</span>
            </div>
            <div id='legend-resumen-body' class='legend-body'>
                <table style='width: 100%; border-collapse: collapse;'>
                    <tr><td style='padding: 3px 0;'>Fechas:</td><td style='padding: 3px 0;'><b>{fecha_inicio} - {fecha_fin}</b></td></tr>
                    <tr><td style='padding: 3px 0;'>Clientes/día:</td><td style='padding: 3px 0;'><b>{promedio_clientes:.1f}</b></td></tr>
                    <tr style='border-top: 1px solid #eee;'><td style='padding: 5px 0;'><b>Total clientes:</b></td><td style='padding: 5px 0;'><b>{total_clientes}</b></td></tr>
                    <tr><td style='padding: 5px 0;'><b>Total muestras:</b></td><td style='padding: 5px 0;'><b>{total_muestras}</b></td></tr>
                </table>
            </div>
        </div>
        <style>
            .legend-box.collapsed .legend-body {{ display: none; }}
            .legend-box.collapsed .toggle-icon {{ transform: rotate(-90deg); }}
            .legend-header:hover {{ background-color: #f9fafb; border-radius: 4px; padding: 2px; }}
            .toggle-icon {{ font-size: 12px; color: #6b7280; }}
        </style>
        <script>
            function toggleLegend(legendId) {{
                const legend = document.getElementById(legendId);
                const toggle = document.getElementById(legendId + '-toggle');
                const body = document.getElementById(legendId + '-body');
                if (legend.classList.contains('collapsed')) {{
                    legend.classList.remove('collapsed'); toggle.style.transform = 'rotate(0deg)'; body.style.display = 'block';
                }} else {{
                    legend.classList.add('collapsed'); toggle.style.transform = 'rotate(-90deg)'; body.style.display = 'none';
                }}
                setTimeout(repositionZoomControls, 100);
            }}
            function repositionZoomControls() {{
                const resumenLegend = document.getElementById('legend-resumen');
                const zoomControl = document.querySelector('.leaflet-control-zoom');
                if (zoomControl && resumenLegend) {{
                    const resumenRect = resumenLegend.getBoundingClientRect();
                    const topPosition = resumenRect.bottom + 10; zoomControl.style.top = topPosition + 'px'; zoomControl.style.left = '20px'; zoomControl.style.position = 'fixed';
                }}
            }}
            document.addEventListener('DOMContentLoaded', function() {{ setTimeout(repositionZoomControls, 500); }});
            window.addEventListener('load', function() {{ setTimeout(repositionZoomControls, 1000); }});
        </script>
        """
        mapa.get_root().html.add_child(folium.Element(html_resumen))
    except Exception:
        pass

    # CSV exportable (siempre clientes únicos)
    df_csv = None
    try:
        if not df_filtrado.empty:
            df_csv = df_filtrado.copy()
            df_csv['lat'] = df_csv.apply(lambda r: r.get('coordenada_latitud', r.get('latitud', None)), axis=1)
            df_csv['lot'] = df_csv.apply(lambda r: r.get('coordenada_longitud', r.get('longitud', None)), axis=1)
            if 'fecha_evento' not in df_csv.columns and 'fecha' in df_csv.columns:
                df_csv['fecha_evento'] = pd.to_datetime(df_csv['fecha'], errors='coerce')
            df_csv = df_csv.dropna(subset=['lat', 'lot'])
            sort_cols = []
            if 'fecha_evento' in df_csv.columns:
                sort_cols.append('fecha_evento')
            sort_cols.extend(['lat', 'lot'])
            df_csv = df_csv.sort_values(sort_cols, ascending=True)
            df_csv = df_csv.reset_index(drop=True)
            df_csv['id'] = df_csv.index + 1
            df_csv['lon'] = df_csv['lot']
            cod_series = _asignar_cuadrante_a_puntos(df_csv, features_cuadrantes)
            df_csv['cod_cuadrante'] = cod_series
            # Mapear áreas de cuadrante
            area_map_series = {}
            for f in features_cuadrantes:
                props = f.get('properties', {})
                codigo = props.get('codigo') or props.get('CODIGO') or props.get('code') or ''
                if not codigo:
                    continue
                try:
                    a = area_m2_geodesic(f.get('geometry', {}))
                except Exception:
                    a = 0
                area_map_series[codigo] = a
            df_csv['area_m2_cuadrante'] = df_csv['cod_cuadrante'].map(area_map_series).fillna(0).round().astype(int)
            cols_finales = ['id','fecha_evento','id_promotor','lat','lot','lon','cod_cuadrante','area_m2_cuadrante']
            if 'id_contacto' in df_csv.columns:
                insert_pos = cols_finales.index('id_promotor') + 1
                cols_finales = cols_finales[:insert_pos] + ['id_contacto'] + cols_finales[insert_pos:]
            cols_disponibles = [c for c in cols_finales if c in df_csv.columns]
            df_csv = df_csv[cols_disponibles]
    except Exception as e:
        logging.error(f"Error construyendo DF CSV: {e}")
        df_csv = None

    # Guardar mapa y retornar
    _tvp = _t("construir mapa (folium)", _tv0, _tvp)
    filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras", permitir_multiples=False)
    _tvp = _t("guardar HTML", _tv0, _tvp)
    _total_visual = _time.perf_counter() - _tv0
    print(f"  {'─'*45}")
    print(f"  {'TOTAL end-to-end':.<30} {_total_visual:>6.2f} s\n")

    n_puntos = len(df_filtrado) if not df_filtrado.empty else 0
    return filename, n_puntos, df_csv


__all__ = [
    'generar_mapa_muestras',
    'generar_mapa_muestras_visual',
    'generar_mapa_muestras_clientes',
    'generar_datos_auditoria_muestras',
    'generar_mapa_muestras_auditoria',
    'CATEGORIAS_FIELES',
]

# === Wrapper público para flujo Clientes X Muestras ===
def generar_mapa_muestras_clientes(
    fecha_inicio: str,
    fecha_fin: str,
    ciudad: str,
    color_mode: str = "Promotores",
    barrios: list[str] | None = None,
):
    """
    Punto de entrada unificado para "Clientes X Muestras".

    Retorna:
      - map_filename (str): nombre del HTML guardado en static/maps
      - df_export (DataFrame): datos para descarga CSV
      - export_meta (dict): { 'ciudad', 'fecha_inicio', 'fecha_fin' }
      - legend_html (str): leyenda HTML (opcional, ya embebida en el HTML)
    """
    # Derivar agrupar_por desde color_mode
    agrupar_por = "Promotor" if str(color_mode).strip().lower().startswith("promotor") else "Mes"

    # Generar mapa con el visual (incluye leyendas y resumen ya embebidos)
    filename, _n_puntos, df_export = generar_mapa_muestras_visual(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ciudad=ciudad,
        agrupar_por=agrupar_por,
        auditoria=False,
        override_fc=None,
    )

    export_meta = {
        "ciudad": ciudad,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
    }
    legend_html = ""  # ya está renderizada dentro del HTML del mapa
    return filename, (df_export if df_export is not None else pd.DataFrame()), export_meta, legend_html


# ===================== Auditoría (Datos) =====================
def generar_datos_auditoria_muestras(
    fecha_inicio: str,
    fecha_fin: str,
    ciudad: str,
    agrupar_por: Literal["Promotor", "Mes"],
    id_promotor: int | None = None,
    mes_auditoria: int | None = None,
) -> tuple[pd.DataFrame, int, int | None]:
    """Genera DataFrame de clientes únicos para auditoría.

    Retorna:
      - df_clientes_unicos: clientes únicos filtrados según el modo de auditoría.
      - centroope: código centro de operación derivado de la ciudad.
      - valor_filtro: id_promotor (modo promotor) o mes_auditoria (modo mes) según corresponda.

    Reglas:
      - Usa exclusivamente new_preprocesamiento_muestras (consultar_db, crear_df).
      - Filtra por id_promotor (Promotor) o por mes (Mes).
      - Normaliza id_promotor → columna id_autor para compatibilidad con metricas_areas.
      - Para auditoría por Mes se unifica id_autor en un valor sintético (ej: -1) para permitir subclustering global.
    """
    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in CENTROOPES:
        raise ValueError(f"Ciudad desconocida para auditoría: {ciudad}")
    centroope = CENTROOPES[ciudad_norm]

    # Consulta base
    df_raw = consultar_db(
        id_centroope=centroope,
        fecha_inicio=str(fecha_inicio),
        fecha_fin=str(fecha_fin),
        ids_promotor=None,
    )
    df_base = crear_df(df_raw)
    if df_base is None or df_base.empty:
        return pd.DataFrame(columns=["id_contacto", "id_autor", "fecha_evento", "coordenada_latitud", "coordenada_longitud"]), centroope, (id_promotor if agrupar_por == "Promotor" else mes_auditoria)

    # Normalizar fecha_evento y columna mes si se necesita
    if 'fecha_evento' in df_base.columns:
        df_base['fecha_evento'] = pd.to_datetime(df_base['fecha_evento'], errors='coerce')
    else:
        raise ValueError("El DataFrame base no contiene columna 'fecha_evento'.")

    # Alinear id_promotor a id_autor para compatibilidad áreas
    if 'id_promotor' in df_base.columns:
        df_base = df_base.rename(columns={'id_promotor': 'id_autor'})
    elif 'id_autor' not in df_base.columns:
        raise ValueError("El DataFrame base no incluye 'id_promotor' ni 'id_autor'.")

    # Filtro según modo
    if agrupar_por == 'Promotor':
        if id_promotor is None:
            raise ValueError("id_promotor es requerido para auditoría por Promotor.")
        df_filtrado = df_base[df_base['id_autor'] == int(id_promotor)].copy()
        valor_filtro = int(id_promotor)
    elif agrupar_por == 'Mes':
        if mes_auditoria is None:
            raise ValueError("mes_auditoria es requerido para auditoría por Mes.")
        if 'mes' not in df_base.columns:
            df_base['mes'] = df_base['fecha_evento'].dt.month
        df_filtrado = df_base[df_base['mes'] == int(mes_auditoria)].copy()
        # Unificar id_autor en un valor sintético para clustering global
        df_filtrado['id_autor'] = -1
        valor_filtro = int(mes_auditoria)
    else:
        raise ValueError(f"Modo agrupar_por no soportado en auditoría: {agrupar_por}")

    if df_filtrado.empty:
        return pd.DataFrame(columns=["id_contacto", "id_autor", "fecha_evento", "coordenada_latitud", "coordenada_longitud"]), centroope, valor_filtro

    # Clientes únicos (última muestra por cliente) → consistente con flujo clientes
    df_clientes_unicos = (
        df_filtrado.sort_values('fecha_evento')
                   .drop_duplicates(subset=['id_contacto'], keep='last')
                   .copy()
    )

    # Limpiar lat/lon nulos
    lat_cols = [c for c in ['coordenada_latitud', 'latitud', 'lat'] if c in df_clientes_unicos.columns]
    lon_cols = [c for c in ['coordenada_longitud', 'longitud', 'lon'] if c in df_clientes_unicos.columns]
    if not lat_cols or not lon_cols:
        raise ValueError("No se encontraron columnas de coordenadas válidas en df_clientes_unicos.")
    lat_col, lon_col = lat_cols[0], lon_cols[0]
    df_clientes_unicos = df_clientes_unicos.dropna(subset=[lat_col, lon_col])

    return df_clientes_unicos, centroope, valor_filtro


# ===================== Auditoría (Visual) =====================
def generar_mapa_muestras_auditoria(
    fecha_inicio: str,
    fecha_fin: str,
    ciudad: str,
    agrupar_por: Literal["Promotor", "Mes"],
    id_promotor: int | None = None,
    mes_auditoria: int | None = None,
):
    """Construye mapa Folium de auditoría Clientes X Muestras.

    Retorna (filename, total_puntos, df_areas_subclusters).
    Para modo Mes se agrupan todos los puntos en un id_autor sintético (-1) antes de subclustering.
    """
    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in CENTROOPES or ciudad_norm not in coordenadas_ciudades:
        mapa = folium.Map(location=[4.7110, -74.0721], zoom_start=12)
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras_auditoria", permitir_multiples=False)
        return filename, 0, None

    df_clientes_unicos, centroope, valor_filtro = generar_datos_auditoria_muestras(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ciudad=ciudad,
        agrupar_por=agrupar_por,
        id_promotor=id_promotor,
        mes_auditoria=mes_auditoria,
    )

    location, geojson_file_path = coordenadas_ciudades[ciudad_norm]

    if df_clientes_unicos.empty:
        mapa = folium.Map(location=location, zoom_start=12)
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras_auditoria", permitir_multiples=False)
        return filename, 0, None

    # Subclusters y métricas (solo promotor único o id_autor sintético)
    try:
        from pre_procesamiento.metricas_areas import areas_muestras_auditoria
        df_areas, fc = areas_muestras_auditoria(df_clientes_unicos, centroope)
    except Exception as e:
        logging.error(f"Error áreas auditoría: {e}")
        df_areas = pd.DataFrame()
        fc = {"type": "FeatureCollection", "features": []}

    mapa = folium.Map(location=location, zoom_start=12)

    # Cargar comunas base
    try:
        with open(geojson_file_path, 'r', encoding='utf-8') as f:
            geo_base = json.load(f)
    except Exception:
        geo_base = {"type": "FeatureCollection", "features": []}

    comunas_group = folium.FeatureGroup(name="Comunas", show=True).add_to(mapa)
    for feat in geo_base.get('features', []):
        folium.GeoJson(
            data=feat,
            style_function=lambda x: {'fillColor': 'transparent', 'color': '#111111', 'weight': 1, 'fillOpacity': 0.0}
        ).add_to(comunas_group)

    # Dibujar subclusters auditoría
    auditoria_group = folium.FeatureGroup(name="Subclusters Auditoría", show=True).add_to(mapa)
    for feat in fc.get('features', []):
        props = feat.get('properties', {})
        area_m2 = float(props.get('area_m2', 0.0))
        perim_m = float(props.get('perimetro_m', 0.0))
        n_puntos = int(props.get('n_puntos', 0))
        compacidad = float(props.get('compacidad', 0.0))
        dens_compacta = float(props.get('densidad_compacta', 0.0))
        densidad_factor1000 = dens_compacta * 1000.0  # continuidad interpretativa
        area_miles_m2 = area_m2 / 1000.0
        popup_html = f"""
        <div style='font-family:Inter,system-ui;font-size:13px;'>
          <div style='font-weight:600;margin-bottom:6px;'>Subcluster {int(props.get('id_subcluster', 0))}</div>
          <div>Área: {area_miles_m2:,.1f} mil m²</div>
          <div>Perímetro: {perim_m:,.0f} m</div>
          <div>Puntos usados: {n_puntos}</div>
          <div>Compacidad: {compacidad:.2f}</div>
          <div>Densidad compuesta (x1000): {densidad_factor1000:,.2f}</div>
        </div>
        """.replace(',', 'X').replace('.', ',').replace('X', '.')
        folium.GeoJson(
            data=feat,
            style_function=lambda x: {'fillColor': 'transparent', 'color': '#DC2626', 'weight': 2.2, 'fillOpacity': 0},
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=folium.Tooltip(f"Subcluster {int(props.get('id_subcluster', 0))}"),
        ).add_to(auditoria_group)

    # Capa de puntos (clientes únicos)
    puntos_group = folium.FeatureGroup(name="Clientes únicos", show=True).add_to(mapa)
    lat_col = next((c for c in ['coordenada_latitud','latitud','lat'] if c in df_clientes_unicos.columns), None)
    lon_col = next((c for c in ['coordenada_longitud','longitud','lon'] if c in df_clientes_unicos.columns), None)
    for _, r in df_clientes_unicos.iterrows():
        lat = r.get(lat_col)
        lon = r.get(lon_col)
        if pd.notna(lat) and pd.notna(lon):
            folium.CircleMarker(
                location=[float(lat), float(lon)],
                radius=4,
                color="#1D4ED8",
                fill=True,
                fillColor="#1D4ED8",
                fillOpacity=0.85,
                popup=folium.Popup(f"Cliente: {r.get('id_contacto','-')}<br>Fecha: {r.get('fecha_evento','-')}", max_width=260)
            ).add_to(puntos_group)

    # Control de capas
    try:
        from folium.plugins import TreeLayerControl as _TLC
        _TLC(collapsed=True, position='topright').add_to(mapa)
    except Exception:
        folium.LayerControl(collapsed=True, position='topright').add_to(mapa)

    # Leyenda / resumen
    if agrupar_por == 'Promotor':
        encabezado = f"Auditoría Promotor {valor_filtro}" if valor_filtro is not None else "Auditoría Promotor"
    else:
        nombre_mes = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}.get(int(valor_filtro), str(valor_filtro))
        encabezado = f"Auditoría Mes {nombre_mes}" if valor_filtro is not None else "Auditoría Mes"

    total_clientes = int(df_clientes_unicos['id_contacto'].nunique()) if 'id_contacto' in df_clientes_unicos.columns else len(df_clientes_unicos)
    resumen_html = f"""
    <div style='position:fixed;top:20px;left:20px;z-index:1000;background:white;border:1px solid #e5e7eb;border-radius:8px;padding:12px;box-shadow:0 4px 12px rgba(0,0,0,.12);font-size:13px;font-family:Inter,system-ui;min-width:240px;'>
      <div style='font-weight:600;margin-bottom:4px;'>{encabezado}</div>
      <div style='color:#374151;margin-bottom:6px;'>{fecha_inicio} → {fecha_fin}</div>
      <div style='margin-bottom:4px;'>Clientes únicos (Clientes X Muestras): <b>{total_clientes}</b></div>
      <div style='font-size:12px;color:#6B7280;'>Subclusters: {len(fc.get('features', []))}</div>
    </div>
    """
    mapa.get_root().html.add_child(folium.Element(resumen_html))

    filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_muestras_auditoria", permitir_multiples=False)
    total_puntos = len(df_clientes_unicos)
    return filename, total_puntos, df_areas
