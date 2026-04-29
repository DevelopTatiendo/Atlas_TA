# -*- coding: utf-8 -*-
"""
Script de pruebas M3 (grid regular) para muestras por asesor.

Objetivo: Construir una grilla (celdas cuadradas en UTM) sobre el bounding box de las
muestras de un asesor en una ciudad y calcular métricas simples por celda:
- n_muestras (conteo de puntos dentro de la celda)
- area_m2 (= CELL_SIZE_M ** 2)
- densidad (n_muestras / area_m2)
- centroid_lat, centroid_lon (para referencia geográfica)

Salida:
- CSV con métricas por celda
- Mapa Folium con polígonos coloreados según n_muestras

Ajustes rápidos:
- CIUDAD, FECHA_INICIO, FECHA_FIN
- PROMOTORES_IDS (lista de IDs a filtrar)
- CELL_SIZE_M (tamaño de lado de la celda en metros)
- MARGIN_M (margen adicional alrededor del bounding box de los puntos)

NOTA: Este es un script de prueba aislado que no modifica lógica productiva.
"""
import os, sys, json, shutil, logging
from dataclasses import dataclass
from typing import Tuple, Dict
import numpy as np
import pandas as pd
import folium
from shapely.geometry import Polygon, Point, box, mapping
from pyproj import Transformer

# --- Bootstrap de rutas del proyecto (NO MOVER) ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR    = os.path.abspath(os.path.join(CURRENT_DIR, ".."))           # raíz del repo
PREPROC_DIR = os.path.join(BASE_DIR, "pre_procesamiento")
for p in (BASE_DIR, PREPROC_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)
# -------------------------------------------------

from pre_procesamiento.preprocesamiento_muestras import crear_df

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

CIUDADES = {
    "CALI":        {"centroope": 2, "center": [3.4516, -76.5320], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_CALI.csv"),
},
    "MEDELLIN":    {"centroope": 3, "center": [6.2442, -75.5812], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_MEDELLIN.csv"),},
    "MANIZALES":   {"centroope": 6, "center": [5.0672, -75.5174], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_MANIZALES.csv")},
    "PEREIRA":     {"centroope": 5, "center": [4.8087, -75.6906], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_PEREIRA.csv")},
    "BOGOTA":      {"centroope": 4, "center": [4.7110, -74.0721], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BOGOTA.csv")},
    "BARRANQUILLA":{"centroope": 8, "center": [10.9720, -74.7962],"epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BARRANQUILLA.csv")},
    "BUCARAMANGA": {"centroope": 7, "center": [7.1193, -73.1227], "epsg_utm": "EPSG:32618", "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BUCARAMANGA.csv")},
}

# Parámetros principales
CIUDAD         = "MEDELLIN"   # Cambiar según necesidad
FECHA_INICIO   = "2025-01-01"
FECHA_FIN      = "2025-11-01"
PROMOTORES_IDS = [17254]      # Lista de IDs de promotores a incluir (vacía/None = todos)
CELL_SIZE_M    = 100.0         # Lado de cada celda en metros
MARGIN_M       = 75.0        # Margen alrededor del bounding box en metros
DEC_COMAS      = True         # Exportar números con coma decimal y ; separador

RESULTADOS_DIR = os.path.join(BASE_DIR, "Pruebas", "Resultados M3")
os.makedirs(RESULTADOS_DIR, exist_ok=True)

@dataclass
class GridCell:
    ix: int
    iy: int
    n_muestras: int
    area_m2: float
    densidad: float
    centroid_lat: float
    centroid_lon: float
    minx: float
    miny: float
    maxx: float
    maxy: float

# ------------------ Utilidades ------------------

def _format_decimal_comma(df: pd.DataFrame, decimals: int = 3) -> pd.DataFrame:
    out = df.copy()
    num_cols = [c for c in out.columns if np.issubdtype(out[c].dtype, np.number)]
    fmt = "{:." + str(decimals) + "f}"
    for c in num_cols:
        out[c] = out[c].apply(lambda x: "" if pd.isna(x) else fmt.format(float(x)).replace(".", ","))
    return out

def dump_csv(df: pd.DataFrame, path: str, decimals: int = 3):
    try:
        to_write = _format_decimal_comma(df, decimals) if DEC_COMAS else df
        to_write.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    except Exception:
        df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")

def _resolver_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    lat_col = next((c for c in ["coordenada_latitud","latitud","lat"] if c in df.columns), None)
    lon_col = next((c for c in ["coordenada_longitud","longitud","lon"] if c in df.columns), None)
    if lat_col is None or lon_col is None:
        raise ValueError("No se encontraron columnas de lat/lon en el DataFrame.")
    df = df.copy()
    df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    return df.dropna(subset=["_lat","_lon"])

def _to_utm(df_plot: pd.DataFrame, epsg_utm: str) -> Tuple[np.ndarray, Transformer]:
    transformer = Transformer.from_crs("EPSG:4326", epsg_utm, always_xy=True)
    xs, ys = transformer.transform(df_plot["_lon"].astype(float).values,
                                   df_plot["_lat"].astype(float).values)
    return np.column_stack([xs, ys]), transformer

def _utm_to_lonlat(xy: np.ndarray, transformer: Transformer) -> np.ndarray:
    lon, lat = transformer.transform(xy[:,0], xy[:,1], direction="INVERSE")
    return np.column_stack([lat, lon])

def _build_grid(minx: float, miny: float, maxx: float, maxy: float, cell_size: float):
    nx = int(np.ceil((maxx - minx) / cell_size))
    ny = int(np.ceil((maxy - miny) / cell_size))
    return nx, ny

# ------------------ Lógica principal ------------------

def cargar_muestras_promotor() -> pd.DataFrame:
    """Carga las muestras filtrando por una lista explícita de IDs de promotores.

    Lógica:
    1. Se pasa PROMOTORES_IDS directamente a crear_df (filtrado en la consulta).
    2. Se resuelven columnas lat/lon.
    3. Si PROMOTORES_IDS está vacío/None se devuelve el DF completo.
    4. Se hace un filtro adicional con isin por seguridad.
    """
    cfg = CIUDADES[CIUDAD]
    centroope = cfg["centroope"]
    csv_rutas = cfg["csv_rutas"]

    logging.info(f"Cargando muestras ciudad={CIUDAD} fechas {FECHA_INICIO}..{FECHA_FIN}")
    try:
        df = crear_df(centroope, FECHA_INICIO, FECHA_FIN, csv_rutas, promotores=PROMOTORES_IDS)
    except Exception as e:
        logging.error(f"Fallo creando DF base: {e}")
        return pd.DataFrame()
    if df.empty:
        return df

    try:
        df = _resolver_lat_lon(df)
    except Exception as e:
        logging.error(f"Fallo resolviendo lat/lon: {e}")
        return pd.DataFrame()

    if "id_autor" not in df.columns:
        logging.warning("No hay columna id_autor; se trabajará con todos los puntos.")
        return df

    if PROMOTORES_IDS is None or len(PROMOTORES_IDS) == 0:
        logging.warning("PROMOTORES_IDS vacío; usando todos los puntos.")
        return df

    try:
        ids = [int(x) for x in PROMOTORES_IDS]
    except Exception:
        logging.error("PROMOTORES_IDS contiene valores no convertibles a int; usando DF completo.")
        return df

    df_sel = df[df["id_autor"].astype(int).isin(ids)].copy()
    if df_sel.empty:
        logging.warning(f"No se encontraron muestras para los promotores {ids}; usando DF completo.")
        return df

    logging.info(f"Filtrando a promotores {ids} n_puntos={len(df_sel)}")
    return df_sel

def generar_grid(df_promotor: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df_promotor.empty:
        return pd.DataFrame(), pd.DataFrame()
    cfg = CIUDADES[CIUDAD]
    XY, transformer = _to_utm(df_promotor, cfg["epsg_utm"])
    xs = XY[:,0]; ys = XY[:,1]
    minx = float(xs.min()) - MARGIN_M
    maxx = float(xs.max()) + MARGIN_M
    miny = float(ys.min()) - MARGIN_M
    maxy = float(ys.max()) + MARGIN_M
    nx, ny = _build_grid(minx, miny, maxx, maxy, CELL_SIZE_M)
    logging.info(f"Grid nx={nx} ny={ny} cell_size={CELL_SIZE_M}m")
    # Índices de celda para cada punto
    ix = np.floor((xs - minx) / CELL_SIZE_M).astype(int)
    iy = np.floor((ys - miny) / CELL_SIZE_M).astype(int)
    df_pts = df_promotor.copy()
    df_pts["cell_ix"] = ix
    df_pts["cell_iy"] = iy
    # Agregación por celda
    grupos = df_pts.groupby(["cell_ix","cell_iy"], as_index=False).size().rename(columns={"size":"n_muestras"})
    rows = []
    for _, r in grupos.iterrows():
        cix = int(r.cell_ix); ciy = int(r.cell_iy)
        x0 = minx + cix * CELL_SIZE_M
        y0 = miny + ciy * CELL_SIZE_M
        x1 = x0 + CELL_SIZE_M
        y1 = y0 + CELL_SIZE_M
        poly = box(x0, y0, x1, y1)
        centroid_xy = np.array([[poly.centroid.x, poly.centroid.y]])
        centroid_ll = _utm_to_lonlat(centroid_xy, transformer)[0]
        n_m = int(r.n_muestras)
        area_m2 = CELL_SIZE_M * CELL_SIZE_M
        dens = n_m / area_m2 if area_m2 > 0 else 0.0
        rows.append(GridCell(
            ix=cix,
            iy=ciy,
            n_muestras=n_m,
            area_m2=area_m2,
            densidad=dens,
            centroid_lat=float(centroid_ll[0]),
            centroid_lon=float(centroid_ll[1]),
            minx=x0, miny=y0, maxx=x1, maxy=y1
        ))
    df_cells = pd.DataFrame([c.__dict__ for c in rows])
    return df_pts, df_cells

def generar_mapa(df_pts: pd.DataFrame, df_cells: pd.DataFrame, html_path: str):
    cfg = CIUDADES[CIUDAD]
    m = folium.Map(location=cfg["center"], zoom_start=12, tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}", attr="Esri", zoom_control=False)
    # Color scale simple por n_muestras
    if not df_cells.empty:
        vmax = max(1, int(df_cells["n_muestras"].max()))
        # evitar división por cero
        for _, row in df_cells.iterrows():
            cix = row.ix; ciy = row.iy
            x0 = row.minx; y0 = row.miny; x1 = row.maxx; y1 = row.maxy
            # polígono lon/lat
            transformer = Transformer.from_crs(cfg["epsg_utm"], "EPSG:4326", always_xy=True)
            lon0, lat0 = transformer.transform(x0, y0)
            lon1, lat1 = transformer.transform(x1, y0)
            lon2, lat2 = transformer.transform(x1, y1)
            lon3, lat3 = transformer.transform(x0, y1)
            poly_geojson = {
                "type": "Polygon",
                "coordinates": [[
                    [lon0, lat0],[lon1, lat1],[lon2, lat2],[lon3, lat3],[lon0, lat0]
                ]]
            }
            ratio = row.n_muestras / vmax
            # gradiente simple azul -> rojo
            r = int(255 * ratio)
            b = int(255 * (1 - ratio))
            color = f"#{r:02x}00{b:02x}"
            popup = folium.Popup(
                f"Celda ({cix},{ciy})<br>n_muestras: {row.n_muestras}<br>Área (m²): {int(row.area_m2)}<br>Densidad: {row.densidad:.4f}",
                max_width=260
            )
            folium.GeoJson(poly_geojson, style_function=lambda f, col=color: {
                "color": "#222","weight": 1,"fillColor": color,"fillOpacity": 0.35
            }).add_child(popup).add_to(m)
    # Puntos
    for _, r in df_pts.iterrows():
        try:
            folium.CircleMarker([float(r["_lat"]), float(r["_lon"])], radius=3, color="#095b9d", fill=True, fillOpacity=0.65).add_to(m)
        except Exception:
            continue
    # Leyenda
    legend_html = f"""
    <div style='position: fixed; top: 20px; left: 20px; z-index:1000; background: rgba(255,255,255,0.92); padding:10px 12px; border-radius:8px; box-shadow:0 2px 10px rgba(0,0,0,.15); font:12px/1.25 Inter, system-ui;'>
      <div style='font-weight:600; margin-bottom:6px;'>Grid M3 {CIUDAD}</div>
      <div><b>Cell size:</b> {CELL_SIZE_M:.0f} m</div>
      <div><b>Margen:</b> {MARGIN_M:.0f} m</div>
      <div><b>#Celdas activas:</b> {len(df_cells)}</div>
      <div><b>#Muestras:</b> {len(df_pts)}</div>
      <div><b>Densidad:</b> n_muestras / m²</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    try:
        m.save(html_path)
    except Exception as e:
        logging.error(f"No se pudo guardar mapa M3: {e}")

# ------------------ Main ------------------

def main():
    cfg = CIUDADES[CIUDAD]
    BASE_CIUDAD_DIR = os.path.join(RESULTADOS_DIR, CIUDAD)
    os.makedirs(BASE_CIUDAD_DIR, exist_ok=True)
    df_promotor = cargar_muestras_promotor()
    asesor_id = 0
    if not df_promotor.empty and "id_autor" in df_promotor.columns:
        try:
            asesor_id = int(df_promotor["id_autor"].dropna().astype(int).unique()[0])
        except Exception:
            asesor_id = 0
    ASESOR_DIR = os.path.join(BASE_CIUDAD_DIR, f"asesor_{asesor_id}")
    shutil.rmtree(ASESOR_DIR, ignore_errors=True)
    os.makedirs(ASESOR_DIR, exist_ok=True)
    HTML_MAP = os.path.join(ASESOR_DIR, "grid_m3.html")
    CSV_CELDAS = os.path.join(ASESOR_DIR, "grid_celdas_m3.csv")
    CSV_PUNTOS = os.path.join(ASESOR_DIR, "puntos_con_celda_m3.csv")
    if df_promotor.empty:
        logging.warning("DF promotor vacío; se generan archivos vacíos.")
        pd.DataFrame().to_csv(CSV_CELDAS, index=False, sep=";", encoding="utf-8-sig")
        pd.DataFrame().to_csv(CSV_PUNTOS, index=False, sep=";", encoding="utf-8-sig")
        folium.Map(location=cfg["center"], zoom_start=12, tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}", attr="Esri").save(HTML_MAP)
        return
    df_pts, df_cells = generar_grid(df_promotor)
    # Exportar puntos con índices de celda
    dump_csv(df_pts, CSV_PUNTOS, decimals=6)
    # Exportar celdas
    dump_csv(df_cells, CSV_CELDAS, decimals=6)
    # Mapa
    generar_mapa(df_pts, df_cells, HTML_MAP)
    logging.info(f"Mapa M3: {HTML_MAP}")
    logging.info(f"CSV celdas: {CSV_CELDAS}")
    logging.info(f"CSV puntos: {CSV_PUNTOS}")

if __name__ == "__main__":
    main()
