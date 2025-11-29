# --- Bootstrap de rutas del proyecto (NO MOVER) ---
import os, sys, shutil, json, logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR    = os.path.abspath(os.path.join(CURRENT_DIR, ".."))           # raíz del repo
PREPROC_DIR = os.path.join(BASE_DIR, "pre_procesamiento")

for p in (BASE_DIR, PREPROC_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)
# -----------------------------------------------

from datetime import datetime
import pandas as pd
import numpy as np
import folium
import matplotlib.pyplot as plt
from shapely.geometry import Point, shape, mapping
from shapely.geometry import MultiPoint, Polygon
from shapely.ops import unary_union, transform as shp_transform, triangulate
from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import davies_bouldin_score, calinski_harabasz_score
from pyproj import Transformer

# === Configuración multi-ciudad ===
CIUDADES = {
    "CALI": {
        "centroope": 2,
        "center": [3.4516, -76.5320],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "cali", "cuadrantes_rutas_cali.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_CALI.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "MEDELLIN": {
        "centroope": 3,
        "center": [6.2442, -75.5812],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "medellin", "cuadrantes_rutas_medellin.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_MEDELLIN.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "MANIZALES": {
        "centroope": 6,
        "center": [5.0672, -75.5174],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "manizales", "cuadrantes_rutas_manizales.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_MANIZALES.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "PEREIRA": {
        "centroope": 5,
        "center": [4.8087, -75.6906],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "pereira", "cuadrantes_rutas_pereira.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_PEREIRA.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "BOGOTA": {
        "centroope": 4,
        "center": [4.7110, -74.0721],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "bogota", "cuadrantes_rutas_bogota.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BOGOTA.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "BARRANQUILLA": {
        "centroope": 8,
        "center": [10.9720, -74.7962],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "barranquilla", "cuadrantes_rutas_barranquilla.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BARRANQUILLA.csv"),
        "epsg_utm": "EPSG:32618",
    },
    "BUCARAMANGA": {
        "centroope": 7,
        "center": [7.1193, -73.1227],
        "geojson": os.path.join(BASE_DIR, "geojson", "rutas", "bucaramanga", "cuadrantes_rutas_bucaramanga.geojson"),
        "csv_rutas": os.path.join(BASE_DIR, "pre_procesamiento", "data", "BARRIOS_COORDENADAS_RUTAS_COMPLETO_BUCARAMANGA.csv"),
        "epsg_utm": "EPSG:32618",
    },
}

# Selección de ciudad
CIUDAD = "MEDELLIN"  # "MEDELLIN" | "MANIZALES" | "PEREIRA" | "BOGOTA" | "BARRANQUILLA" | "BUCARAMANGA" | "CALI"
_cfg = CIUDADES[CIUDAD]
CENTROOPE = _cfg["centroope"]

FECHA_INICIO = "2025-01-01"
FECHA_FIN    = "2025-11-01"
promotor_num = 1
MANUAL_k = False
K_target = 4

# === Salidas M2 (directorios base) ===
RESULTADOS_DIR = os.path.join(BASE_DIR, "Pruebas", "Resultados M2")
os.makedirs(RESULTADOS_DIR, exist_ok=True)

# Placeholders para rutas que se redefinen dentro de main() por asesor
HTML_BASE      = None
CSV_OUT        = None
HTML_OUT_CLUST = None
ELBOW_PNG      = None
METRICS_CSV    = None
METRICAS_K_CSV = None
TAU_ELBOW      = 0.12

# === Auditoría sub-agrupación KMeans (igual M1) ===
P_OUTLIER           = 0.10
SUBK_KMAX_ABS       = 20
SUBK_KMAX_FRAC      = 0.10
ELBOW_POLICY        = "min"
MIN_SUB_FRAC        = 0.05
PALETA_SUBS = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd",
               "#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"]
SUBK_P_OUTLIER      = 0.00

# Concave Hull (M2)
MIN_PTS_CONCAVE: int = 5
ALPHA_MODE: str = "fixed"   # "fixed" | "auto"
ALPHA_FIXED: float = 500.0
ALPHA_QNN_PCTL: int = 80
ALPHA_SCALE: float = 2.0
HOLE_MIN_FRAC: float = 0.03
HOLE_MIN_ABS: float = 2000
SMOOTHING_BUFFER_M: float = 90.0



# Suavizado extra de bordes (en metros, 0 = sin suavizado)
SMOOTHING_BUFFER_M = 40.0

# Clipping
CLIP_A_RUTAS      = True

# Capa de rutas
ADD_RUTAS_BASE       = True
RUTAS_STROKE_COLOR   = "#2a6fef"
RUTAS_STROKE_WEIGHT  = 2
RUTAS_FILL_COLOR     = "#2a6fef"
RUTAS_FILL_OPACITY   = 0.12

# CSV con coma decimal
DEC_COMAS            = True

# Fallback palette
FALLBACK_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#005f73", "#9b2226",
    "#bb3e03", "#0a9396", "#94d2bd", "#ee9b00", "#ca6702", "#ae2012",
    "#b56576", "#6d597a"
]

from pre_procesamiento.preprocesamiento_muestras import crear_df, obtener_promotores_por_ids
from pre_procesamiento.metricas_areas import areas_muestras_auditoria

try:
    from mapa_muestras import color_for_promotor
    _HAS_COLOR_FN = True
except Exception:
    _HAS_COLOR_FN = False
    def color_for_promotor(co, pid):
        idx = abs(int(pid)) % len(FALLBACK_COLORS)
        return FALLBACK_COLORS[idx]

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

# ==== Helpers de E/S y formato ====

def _format_decimal_comma(df, decimals=3):
    import numpy as _np
    import pandas as _pd
    out = df.copy()
    num_cols = [c for c in out.columns if _np.issubdtype(out[c].dtype, _np.number)]
    fmt = "{:." + str(decimals) + "f}"
    for c in num_cols:
        out[c] = out[c].apply(lambda x: "" if _pd.isna(x) else fmt.format(float(x)).replace(".", ","))
    return out

def dump_csv_coma_decimal(df: pd.DataFrame, path: str, decimals: int = 6):
    try:
        to_write = _format_decimal_comma(df, decimals=decimals) if DEC_COMAS else df
        to_write.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    except Exception:
        df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")

# ==== Helpers de proyección ====

def _to_utm_xy(df_plot):
    transformer = Transformer.from_crs("EPSG:4326", _cfg["epsg_utm"], always_xy=True)
    xs, ys = transformer.transform(df_plot["_lon"].astype(float).values,
                                   df_plot["_lat"].astype(float).values)
    X = np.column_stack([xs, ys])
    return X, transformer

def _from_utm_to_lonlat(centroids_xy, transformer):
    lon, lat = transformer.transform(centroids_xy[:,0], centroids_xy[:,1], direction="INVERSE")
    return np.column_stack([lat, lon])

# ==== Helpers de rutas ====

def _add_rutas_layer(mapa):
    try:
        if ADD_RUTAS_BASE and os.path.exists(_cfg.get("geojson", "")):
            with open(_cfg["geojson"], "r", encoding="utf-8") as f:
                gj = json.load(f)
            folium.GeoJson(
                gj,
                name=f"Rutas {CIUDAD.title()}",
                style_function=lambda feat: {
                    "color": RUTAS_STROKE_COLOR,
                    "weight": RUTAS_STROKE_WEIGHT,
                    "fillColor": RUTAS_FILL_COLOR,
                    "fillOpacity": RUTAS_FILL_OPACITY,
                },
            ).add_to(mapa)
    except Exception as e:
        logging.warning(f"No se pudo cargar capa rutas: {e}")

# ==== Helpers clustering y codo ====

def _k_range(n_points):
    return list(range(2, max(2, min(12, n_points - 1)) + 1))

def _curva_elbow_y_metricas(X, resultados_dir, elbow_png, csv_por_k_path):
    ks = _k_range(X.shape[0])
    inertias, dbis, chis = [], [], []
    use_mini = X.shape[0] > 5000
    for k in ks:
        km = (MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=2048, n_init="auto") if use_mini
              else KMeans(n_clusters=k, random_state=42, n_init="auto"))
        labels = km.fit_predict(X)
        inertias.append(km.inertia_)
        try:
            dbis.append(davies_bouldin_score(X, labels))
        except Exception:
            dbis.append(np.nan)
        try:
            chis.append(calinski_harabasz_score(X, labels))
        except Exception:
            chis.append(np.nan)
    fig = plt.figure()
    plt.plot(ks, inertias, marker="o")
    plt.xlabel("K"); plt.ylabel("SSE / Inertia"); plt.title("Gráfica de codo")
    fig.tight_layout(); fig.savefig(elbow_png, dpi=150); plt.close(fig)
    dfk = pd.DataFrame({"K": ks, "inertia": inertias, "davies_bouldin": dbis, "calinski_harabasz": chis})
    dfk["metrica"] = "M2"
    dfk = _format_decimal_comma(dfk, decimals=3)
    dfk.to_csv(csv_por_k_path, index=False, sep=";", encoding="utf-8-sig")
    return ks, inertias

def _k_por_codo_threshold(ks, inertias, tau=0.12):
    rel = []
    for i in range(1, len(inertias)):
        prev, cur = inertias[i-1], inertias[i]
        rel.append((prev - cur) / max(prev, 1e-9))
    for i, r in enumerate(rel, start=1):
        if r < tau:
            return ks[i-1]
    return ks[-1]

def _k_por_curvatura(ks, inertias):
    y = np.asarray(inertias, dtype=float)
    if y.max() - y.min() > 0:
        y = (y - y.min()) / (y.max() - y.min())
    curv = np.zeros_like(y)
    curv[1:-1] = y[:-2] - 2*y[1:-1] + y[2:]
    idx = int(np.argmax(curv))
    return ks[idx]

def _elegir_k_elbow(ks, inertias, tau=0.12):
    k_tau  = _k_por_codo_threshold(ks, inertias, tau=tau)
    k_curv = _k_por_curvatura(ks, inertias)
    return max(k_tau, k_curv)

def _compute_metrics_csv(X, labels, km, transformer, out_csv_path):
    try:
        dbi = davies_bouldin_score(X, labels)
    except Exception:
        dbi = np.nan
    try:
        chi = calinski_harabasz_score(X, labels)
    except Exception:
        chi = np.nan
    rmse_global = float(np.sqrt(km.inertia_ / X.shape[0])) if X.shape[0] else np.nan
    glob = {
        "scope": "global",
        "n_points": int(X.shape[0]),
        "k": int(km.n_clusters),
        "inertia_m2": float(km.inertia_),
        "rmse_global_m": rmse_global,
        "davies_bouldin": float(dbi) if dbi==dbi else np.nan,
        "calinski_harabasz": float(chi) if chi==chi else np.nan
    }
    rows = [glob]
    centers = km.cluster_centers_
    d2 = np.sum((X - centers[labels])**2, axis=1)
    d  = np.sqrt(d2)
    for cl in sorted(np.unique(labels)):
        mask = (labels == cl)
        Xi = X[mask]
        di = d[mask]
        ni = Xi.shape[0]
        sse = float(np.sum((Xi - centers[cl])**2)) if ni else np.nan
        rmse_m = float(np.sqrt(sse / ni)) if ni else np.nan
        cent_ll = _from_utm_to_lonlat(centers[[cl]], transformer)[0]
        rows.append({
            "scope": "cluster",
            "cluster": int(cl),
            "n_points": int(ni),
            "centroid_lat": float(cent_ll[0]),
            "centroid_lon": float(cent_ll[1]),
            "rmse_m": rmse_m,
        })
    dfm = pd.DataFrame(rows)
    dfm["metrica"] = "M2"
    dfm = _format_decimal_comma(dfm, decimals=3)
    dfm.to_csv(out_csv_path, index=False, sep=";", encoding="utf-8-sig")

# ==== Helpers básicos ==== 

def _resolver_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    lat_col = None
    for c in ["coordenada_latitud", "latitud", "lat"]:
        if c in df.columns:
            lat_col = c; break
    lon_col = None
    for c in ["coordenada_longitud", "longitud", "lon"]:
        if c in df.columns:
            lon_col = c; break
    if not lat_col or not lon_col:
        raise ValueError("No se encontraron columnas de lat/lon en el DataFrame.")
    df = df.copy()
    df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    return df.dropna(subset=["_lat", "_lon"])

# ==== Concave Hull helpers (M2) ====

def _alpha_auto_from_nn(X_utm: np.ndarray) -> float:
    n = len(X_utm)
    if n < 2:
        return ALPHA_FIXED
    nn = NearestNeighbors(n_neighbors=min(2, n)).fit(X_utm)
    dists, _ = nn.kneighbors(X_utm)
    d1 = dists[:, 1]
    q = float(np.percentile(d1, ALPHA_QNN_PCTL)) if len(d1) else 0.0
    if q <= 0:
        logging.warning("ALPHA auto: qNN=0; usando ALPHA_FIXED")
        return max(5.0, float(ALPHA_FIXED))
    alpha_m = float(ALPHA_SCALE * q)
    if alpha_m < 5.0:
        logging.info("ALPHA auto muy pequeño (<5m); elevando a 5.0m")
        alpha_m = 5.0
    return alpha_m

def _filter_small_holes(poly: Polygon, thr_area: float) -> Polygon:
    try:
        holes = []
        for ring in poly.interiors:
            try:
                a = Polygon(ring).area
                if a >= thr_area:
                    holes.append(ring)
            except Exception:
                continue
        return Polygon(poly.exterior, holes).buffer(0)
    except Exception:
        return poly.buffer(0)

def _concave_hull_from_points_utm(X_utm: np.ndarray, alpha_m: float):
    try:
        if len(X_utm) == 0:
            return None
        if len(X_utm) < MIN_PTS_CONCAVE:
            logging.info(f"Sub n={len(X_utm)} < MIN_PTS_CONCAVE={MIN_PTS_CONCAVE}; usando ConvexHull")
            return MultiPoint([(float(x), float(y)) for x, y in X_utm]).convex_hull

        mpt = MultiPoint([(float(x), float(y)) for x, y in X_utm])
        tris = triangulate(mpt)
        keep = []
        a2 = float(alpha_m)
        for t in tris:
            xs, ys = t.exterior.coords.xy
            coords = list(zip(xs, ys))
            edges = [
                np.hypot(coords[i+1][0]-coords[i][0], coords[i+1][1]-coords[i][1])
                for i in range(3)
            ]
            max_edge = max(edges)
            if max_edge <= a2:
                keep.append(t)

        if not keep:
            logging.info("Triangulación filtrada vacía; usando ConvexHull")
            return mpt.convex_hull

        geom = unary_union(keep).buffer(0)

        # Filtrar agujeros pequeños
        try:
            total_area = float(geom.area)
            thr_area = max(HOLE_MIN_FRAC * total_area, float(HOLE_MIN_ABS))
            if geom.geom_type == "Polygon":
                geom = _filter_small_holes(geom, thr_area)
            elif geom.geom_type == "MultiPolygon":
                parts = []
                for p in geom.geoms:
                    parts.append(_filter_small_holes(p, thr_area))
                geom = unary_union(parts).buffer(0)
        except Exception:
            pass

        # Suavizado opcional de bordes (buffer positivo + negativo)
        if SMOOTHING_BUFFER_M and SMOOTHING_BUFFER_M > 0:
            try:
                geom = geom.buffer(SMOOTHING_BUFFER_M).buffer(-SMOOTHING_BUFFER_M)
            except Exception:
                pass

        return geom

    except Exception as e:
        logging.warning(f"Concave hull falló: {e}")
        try:
            return MultiPoint([(float(x), float(y)) for x, y in X_utm]).convex_hull
        except Exception:
            return None

def _city_perimeter_union_utm(cfg, transformer):
    try:
        if not os.path.exists(cfg.get("geojson", "")):
            return None
        with open(cfg["geojson"], "r", encoding="utf-8") as f:
            gj = json.load(f)
        geoms = []
        def proj_ll_to_utm(x, y, z=None):
            X, Y = transformer.transform(x, y)
            return (X, Y)
        for feat in gj.get("features", []):
            try:
                g_ll = shape(feat.get("geometry", {}))
                g_utm = shp_transform(proj_ll_to_utm, g_ll)
                geoms.append(g_utm)
            except Exception:
                continue
        if not geoms:
            return None
        return unary_union(geoms).buffer(0)
    except Exception:
        return None

# ==== Auditoría sub-clusters (igual M1, sin cambios visuales) ====

def _podar_outliers_xy(X, p=P_OUTLIER):
    if p <= 0 or len(X) < 5:
        return X, np.ones(len(X), dtype=bool)
    c = X.mean(axis=0)
    r = np.sqrt(((X - c) ** 2).sum(axis=1))
    thr = np.quantile(r, 1 - p)
    keep = r <= thr
    return X[keep], keep


def _elbow_min_k(X, kmax):
    wcss = []
    for k in range(1, int(kmax) + 1):
        km = KMeans(n_clusters=k, n_init="auto", random_state=42)
        km.fit(X)
        wcss.append(km.inertia_)
    if len(wcss) == 1:
        return 1, wcss
    y = np.log(np.array(wcss))
    d1 = np.diff(y)
    d2 = np.diff(d1)
    idx_codo = int(np.argmax(d2)) + 2
    kstar = max(1, min(int(kmax), idx_codo))
    return kstar, wcss


def _export_subclusters_kmeans(df_cluster, transformer, out_dir, filename_html="C_subkmeans.html", filename_csv="C_resumen.csv"):
    """Simplified: treat entire df_cluster as ONE subcluster (sub_id=0)."""
    os.makedirs(out_dir, exist_ok=True)
    if "_lat" not in df_cluster.columns or "_lon" not in df_cluster.columns:
        try:
            df_cluster = _resolver_lat_lon(df_cluster)
        except Exception:
            pass
    latlon = df_cluster[["_lat","_lon"]].to_numpy(float)
    X_full, _ = _to_utm_xy(df_cluster)
    Xp, keep_mask = _podar_outliers_xy(X_full, P_OUTLIER)  # single global prune
    df_pod = df_cluster.loc[df_cluster.index[keep_mask]].copy()
    latlon_pod = df_pod[["_lat","_lon"]].to_numpy(float)
    n = len(Xp)
    center = [float(latlon[:,0].mean()), float(latlon[:,1].mean())] if len(latlon) else [0,0]
    m = folium.Map(location=center, zoom_start=13, zoom_control=False)
    try:
        _add_rutas_layer(m)
    except Exception:
        pass
    # Raw points (light)
    for la, lo in latlon:
        folium.CircleMarker([float(la), float(lo)], radius=3, color="#c5c8ce", fill=True, fillOpacity=0.45).add_to(m)
    # Pruned points (blue)
    for la, lo in latlon_pod:
        folium.CircleMarker([float(la), float(lo)], radius=4, color="#5b9bd5", fill=True, fillOpacity=0.85).add_to(m)
    # Subcluster single metrics
    if len(Xp):
        cx, cy = Xp.mean(axis=0)
        clatlon = np.array(_from_utm_to_lonlat(np.array([[cx, cy]]), transformer))[0]
        folium.CircleMarker([float(clatlon[0]), float(clatlon[1])], radius=7, color="black", fill=True, fillColor="white", fillOpacity=1).add_to(m)
        di = np.sqrt(((Xp - np.array([cx, cy]))**2).sum(axis=1))
        bbox_w = float(Xp[:,0].max() - Xp[:,0].min()) if len(Xp) else 0.0
        bbox_h = float(Xp[:,1].max() - Xp[:,1].min()) if len(Xp) else 0.0
        bbox_d = float(np.sqrt(bbox_w**2 + bbox_h**2))
    else:
        clatlon = [center[0], center[1]]
        di = np.array([])
        bbox_d = 0.0
    rows = [{
        "sub_id": 0,
        "n_pts": int(len(Xp)),
        "pct_cluster": 1.0,
        "centroid_lat": float(clatlon[0]),
        "centroid_lon": float(clatlon[1]),
        "mean_dist_m": float(di.mean()) if len(di) else np.nan,
        "max_dist_m": float(di.max()) if len(di) else np.nan,
        "bbox_diag_m": bbox_d,
        "k_opt": 1  # single subcluster
    }]
    legend_simple = f"""
    <div style='position: fixed; top: 20px; left: 20px; z-index:1000; background: rgba(255,255,255,0.92); padding:8px 10px; border-radius:6px; font:12px/1.2 Inter, system-ui;'>
      <div style='font-weight:600;'>Subcluster único</div>
      <div>n pts usados: {len(Xp)}</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_simple))
    m.save(os.path.join(out_dir, filename_html))
    cols = ["sub_id","n_pts","pct_cluster","centroid_lat","centroid_lon","mean_dist_m","max_dist_m","bbox_diag_m","k_opt"]
    df_rows = pd.DataFrame(rows, columns=cols)
    df_rows = _format_decimal_comma(df_rows, decimals=6)
    df_rows.to_csv(os.path.join(out_dir, filename_csv), index=False, sep=";", encoding="utf-8-sig")
    # sub_index_map with single entry for concave hull phase
    sub_index_map = {0: (df_pod, Xp)}
    return rows, sub_index_map

# ==== Flujo de clustering y dibujo ====

def _cluster_and_draw(df_plot, resultados_dir, mapa, cluster_palette):
    n = len(df_plot)
    if n < 3:
        logging.warning("Muy pocos puntos para clustering (n<3). Se omite clustering.")
        return df_plot
    X, transformer = _to_utm_xy(df_plot)
    if MANUAL_k:
        valid_ks = _k_range(X.shape[0])
        k_req = int(K_target)
        k_clamped = max(valid_ks[0], min(valid_ks[-1], k_req))
        k_best = k_clamped
        logging.info(f"K manual seleccionado (solo codo): {k_best}")
    else:
        ks, inertias = _curva_elbow_y_metricas(X, resultados_dir, ELBOW_PNG, METRICAS_K_CSV)
        k_best = _elegir_k_elbow(ks, inertias, tau=TAU_ELBOW)
        logging.info(f"K seleccionado (solo codo): {k_best}  |  piso por tau={TAU_ELBOW}")
    use_mini = X.shape[0] > 5000
    km = (MiniBatchKMeans(n_clusters=k_best, random_state=42, batch_size=2048, n_init="auto") if use_mini else KMeans(n_clusters=k_best, random_state=42, n_init="auto"))
    labels = km.fit_predict(X)
    df_plot = df_plot.copy()
    df_plot["cluster"] = labels
    _compute_metrics_csv(X, labels, km, transformer, METRICS_CSV)
    cent_latlon = _from_utm_to_lonlat(km.cluster_centers_, transformer)
    uniq = sorted(np.unique(labels).tolist())
    color_map = {cl: cluster_palette[cl % len(cluster_palette)] for cl in uniq}
    for _, row in df_plot.iterrows():
        lat = float(row["_lat"]); lon = float(row["_lon"]); cl = int(row["cluster"])
        c = color_map[cl]
        folium.CircleMarker(location=[lat, lon], radius=4, color=c, fill=True, fillColor=c, fillOpacity=0.85).add_to(mapa)
    for cl, (lat, lon) in enumerate(cent_latlon):
        folium.CircleMarker(location=[float(lat), float(lon)], radius=7, color="black", fill=True, fillColor="black", fillOpacity=0.95).add_to(mapa)
    return df_plot, k_best

# ==== Export concave sub ====

def _polygon_metrics(geom_utm, X_used):
    if geom_utm is None:
        return {"area_m2": np.nan, "perimetro_m": np.nan, "pct_puntos_cubiertos": np.nan, "bbox_diag_m": np.nan}
    area = float(geom_utm.area)
    peri = float(geom_utm.length)
    covered = 0
    for x, y in X_used:
        try:
            if geom_utm.contains(Point(float(x), float(y))):
                covered += 1
        except Exception:
            pass
    pct = covered / max(1, len(X_used))
    minx, miny, maxx, maxy = geom_utm.bounds
    bbox_diag = float(np.hypot(maxx - minx, maxy - miny))
    return {"area_m2": area, "perimetro_m": peri, "pct_puntos_cubiertos": pct, "bbox_diag_m": bbox_diag}

def _geom_utm_to_lonlat(geom_utm, transformer):
    if geom_utm is None:
        return None
    def proj_utm_to_ll(x, y, z=None):
        lon, lat = transformer.transform(x, y, direction="INVERSE")
        return (lon, lat)
    try:
        return shp_transform(proj_utm_to_ll, geom_utm)
    except Exception:
        return None

def _export_concave_sub(df_sub: pd.DataFrame, transformer: Transformer, out_dir: str, clip_city_utm=None, sub_idx: int=0, cluster_id: int=0, rutas_utm=None, mapa_global=None):
    os.makedirs(out_dir, exist_ok=True)
    # Proyección
    X, _ = _to_utm_xy(df_sub)
    # ALPHA
    if ALPHA_MODE == "fixed":
        alpha_m = float(ALPHA_FIXED)
        if alpha_m < 5.0: alpha_m = 5.0
    else:
        alpha_m = _alpha_auto_from_nn(X)
    # Concave
    geom_utm = _concave_hull_from_points_utm(X, alpha_m)
    if CLIP_A_RUTAS and clip_city_utm is not None and geom_utm is not None:
        try:
            g2 = geom_utm.intersection(clip_city_utm).buffer(0)
            if g2.is_empty:
                logging.info(f"Clip vacío (cluster {cluster_id} sub {sub_idx}); exportando sin clip")
            else:
                geom_utm = g2
        except Exception:
            pass
    geom_ll = _geom_utm_to_lonlat(geom_utm, transformer)
    # Métricas
    mets = _polygon_metrics(geom_utm, X)
    mets.update({
        "alpha_m": float(alpha_m),
        "alpha_mode": ALPHA_MODE,
        "n_puntos_usados": int(len(X))
    })
    # GeoJSON
    gj_path = os.path.join(out_dir, f"SC_sub_{sub_idx}_concave.geojson")
    try:
        if geom_ll is not None:
            gj = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": mapping(geom_ll)}]}
            with open(gj_path, "w", encoding="utf-8") as fgj:
                json.dump(gj, fgj)
    except Exception:
        pass
    # CSV de métricas (incluye area_km2 derivada)
    df_m = pd.DataFrame([{
        "cluster": int(cluster_id),
        "sub_id": int(sub_idx),
        "area_m2": mets["area_m2"],
        "area_km2": mets["area_m2"]/1_000_000.0 if pd.notna(mets["area_m2"]) else np.nan,
        "perimetro_m": mets["perimetro_m"],
        "n_puntos_usados": mets["n_puntos_usados"],
        "pct_puntos_cubiertos": mets["pct_puntos_cubiertos"],
        "bbox_diag_m": mets["bbox_diag_m"],
        "alpha_m": mets["alpha_m"],
        "alpha_mode": mets["alpha_mode"],
    }])
    dump_csv_coma_decimal(df_m, os.path.join(out_dir, f"SC_sub_{sub_idx}_metricas.csv"), decimals=3)
    # HTML auditoría
    latlon_sc = df_sub[["_lat","_lon"]].to_numpy(float)
    center_sc = [float(latlon_sc[:,0].mean()), float(latlon_sc[:,1].mean())] if len(latlon_sc) else _cfg["center"]
    ma = None
    if mapa_global is None:
        ma = folium.Map(location=center_sc, zoom_start=13, zoom_control=False)
        try:
            _add_rutas_layer(ma)
        except Exception:
            pass
        # Solo en mapa local dibujamos puntos crudos
        for la, lo in latlon_sc:
            folium.CircleMarker([float(la), float(lo)], radius=3, color="#5b9bd5", fill=True, fillOpacity=0.8).add_to(ma)
    cuadrante_code = "sin determinar"
    if rutas_utm and geom_utm is not None:
        try:
            for g_ruta, props_ruta in rutas_utm:
                try:
                    if geom_utm.intersects(g_ruta):
                        inter = geom_utm.intersection(g_ruta)
                        if not inter.is_empty and inter.area > 0:
                            # Try common property keys
                            for k in ["codigo", "ruta", "id", "name", "CUADRANTE", "CODIGO", "RUTA"]:
                                if k in props_ruta and props_ruta[k]:
                                    cuadrante_code = str(props_ruta[k])
                                    break
                            break
                except Exception:
                    continue
        except Exception:
            pass
    # Popup HTML
    popup_html = f"""
    <b>Cluster:</b> {cluster_id}<br>
    <b>Sub_id:</b> {sub_idx}<br>
    <b>Área (m²):</b> {int(round(mets['area_m2'])) if pd.notna(mets['area_m2']) else 'N/A'}<br>
    <b>Perímetro (m):</b> {int(round(mets['perimetro_m'])) if pd.notna(mets['perimetro_m']) else 'N/A'}<br>
    <b>Puntos usados:</b> {mets['n_puntos_usados']}<br>
    <b>Cuadrante:</b> {cuadrante_code}
    """
    target_map = mapa_global if mapa_global is not None else ma
    if geom_ll is not None and target_map is not None:
        gj = folium.GeoJson(
            mapping(geom_ll),
            name="concave",
            style_function=lambda x: {"color":"#111","weight":2,"fillColor":"#2ca02c","fillOpacity":0.25}
        )
        gj.add_child(folium.Popup(popup_html, max_width=260))
        gj.add_to(target_map)

        # centroid marker en el mapa destino
        try:
            cy = geom_ll.centroid
            folium.CircleMarker(
                [float(cy.y), float(cy.x)],
                radius=7,
                color="black",
                fill=True,
                fillColor="black",
                fillOpacity=0.95
            ).add_to(target_map)
        except Exception:
            pass
    area_km2_val = (mets['area_m2']/1_000_000.0) if pd.notna(mets['area_m2']) else float('nan')
    legend = (
        "<div style='position: fixed; top: 20px; left: 20px; z-index: 1000; background: rgba(255,255,255,0.9); padding: 10px 12px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,.15); font: 12px/1.2 Inter, system-ui;'>"
        f"<div style='font-weight:600; margin-bottom:6px;'>Cluster {cluster_id} · sub {sub_idx}</div>"
        f"<div>α={mets['alpha_m']:.1f} m ({mets['alpha_mode']})</div>"
        f"<div>Área: {area_km2_val:,.3f} km²</div>"
        f"<div>Perímetro: {mets['perimetro_m']:.0f} m</div>"
        f"<div>Puntos usados: {mets['n_puntos_usados']}</div>"
        f"<div>Cuadrante: {cuadrante_code}</div>"
        "</div>"
    )
    if ma is not None:
        # Leyenda y guardado solo en mapa local (debug)
        ma.get_root().html.add_child(folium.Element(legend))
        ma.save(os.path.join(out_dir, f"SC_sub_{sub_idx}_concave.html"))
    # Para el resumen por asesor
    return {
        "cluster": int(cluster_id),
        "sub_id": int(sub_idx),
        "area_m2": float(mets["area_m2"]) if pd.notna(mets["area_m2"]) else np.nan,
        "area_km2": (float(mets["area_m2"]) / 1_000_000.0) if pd.notna(mets["area_m2"]) else np.nan,
        "perimetro_m": float(mets["perimetro_m"]) if pd.notna(mets["perimetro_m"]) else np.nan,
        "n_puntos_usados": int(mets["n_puntos_usados"]),
    }

# ==== Concave a nivel de partición (cluster completo) ====
def _export_concave_cluster_from_submap(
    df_cluster: pd.DataFrame,
    sub_map: dict,
    transformer: Transformer,
    out_dir: str,
    clip_city_utm=None,
    cluster_id: int = 0,
):
    os.makedirs(out_dir, exist_ok=True)
    # Construir X_cluster (UTM) desde TODO el cluster (una sola poda a nivel cluster)
    try:
        if "_lat" not in df_cluster.columns or "_lon" not in df_cluster.columns:
            df_cluster = _resolver_lat_lon(df_cluster)
    except Exception:
        pass
    X_original, _ = _to_utm_xy(df_cluster)
    X_cluster, keep_mask = _podar_outliers_xy(X_original, P_OUTLIER)
    if len(X_cluster) == 0 and len(X_original) > 0:
        # Fallback: usar todos los puntos sin podar
        X_cluster = X_original
    # Alpha
    if ALPHA_MODE == "fixed":
        alpha_m = float(ALPHA_FIXED)
        if alpha_m < 5.0:
            alpha_m = 5.0
    else:
        alpha_m = _alpha_auto_from_nn(X_cluster)
    # Concave
    geom_utm = _concave_hull_from_points_utm(X_cluster, alpha_m)
    if CLIP_A_RUTAS and clip_city_utm is not None and geom_utm is not None:
        try:
            g2 = geom_utm.intersection(clip_city_utm).buffer(0)
            if g2.is_empty:
                logging.info(f"Clip vacío (cluster {cluster_id}); exportando sin clip")
            else:
                geom_utm = g2
        except Exception:
            pass
    # Garantizar un polígono único (evitar islas): si MultiPolygon, usar convex_hull
    try:
        if geom_utm is not None and getattr(geom_utm, "geom_type", "") == "MultiPolygon":
            geom_utm = geom_utm.convex_hull
    except Exception:
        pass
    # Métricas
    mets = _polygon_metrics(geom_utm, X_cluster)
    mets.update({
        "alpha_m": float(alpha_m),
        "alpha_mode": ALPHA_MODE,
        "n_puntos_usados": int(len(X_cluster)),
    })
    # Transformar a lon/lat
    geom_ll = _geom_utm_to_lonlat(geom_utm, transformer)
    # Export GeoJSON
    gj_path = os.path.join(out_dir, f"C{cluster_id}_cluster_concave.geojson")
    try:
        if geom_ll is not None:
            gj = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": mapping(geom_ll)}]}
            with open(gj_path, "w", encoding="utf-8") as fgj:
                json.dump(gj, fgj)
    except Exception:
        pass
    # Export HTML
    try:
        # Convertir puntos a lat/lon para mostrar
        latlon_pts = _from_utm_to_lonlat(X_cluster, transformer)
        if geom_ll is not None:
            try:
                cy = geom_ll.centroid
                center = [float(cy.y), float(cy.x)]  # folium: [lat, lon]
            except Exception:
                center = [float(np.mean(latlon_pts[:,0])), float(np.mean(latlon_pts[:,1]))]
        else:
            center = [float(np.mean(latlon_pts[:,0])), float(np.mean(latlon_pts[:,1]))]
        mc = folium.Map(location=center, zoom_start=13, zoom_control=False)
        try:
            _add_rutas_layer(mc)
        except Exception:
            pass
        # Puntos
        for la, lo in latlon_pts:
            folium.CircleMarker([float(la), float(lo)], radius=3, color="#5b9bd5", fill=True, fillOpacity=0.8).add_to(mc)
        # Polígono
        if geom_ll is not None:
            folium.GeoJson(mapping(geom_ll), name="cluster_concave",
                           style_function=lambda x: {"color":"#111","weight":2,"fillColor":"#ff7f0e","fillOpacity":0.22}).add_to(mc)
        # Leyenda
        area_km2_val = (mets['area_m2']/1_000_000.0) if pd.notna(mets['area_m2']) else float('nan')
        legend = f"""
        <div style='position: fixed; top: 20px; left: 20px; z-index: 1000; background: rgba(255,255,255,0.9); padding: 10px 12px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,.15); font: 12px/1.2 Inter, system-ui;'>
          <div style='font-weight:600; margin-bottom:6px;'>cluster {cluster_id} · α={mets['alpha_m']:.1f} m ({mets['alpha_mode']})</div>
          <div>área: {area_km2_val:,.3f} km²</div>
          <div>perímetro: {mets['perimetro_m']:.0f} m</div>
          <div>n usados: {mets['n_puntos_usados']}</div>
          <div>pctl NN: {ALPHA_QNN_PCTL}</div>
          <div>β: {ALPHA_SCALE:.2f}</div>
        </div>
        """
        mc.get_root().html.add_child(folium.Element(legend))
        mc.save(os.path.join(out_dir, f"C{cluster_id}_cluster_concave.html"))
    except Exception as e:
        logging.warning(f"No fue posible exportar HTML de cluster {cluster_id}: {e}")
    # Fila de resumen por cluster (sub_id=0 consolidado)
    return {
        "cluster": int(cluster_id),
        "sub_id": 0,
        "area_m2": float(mets["area_m2"]) if pd.notna(mets["area_m2"]) else np.nan,
        "area_km2": (float(mets["area_m2"]) / 1_000_000.0) if pd.notna(mets["area_m2"]) else np.nan,
        "perimetro_m": float(mets["perimetro_m"]) if pd.notna(mets["perimetro_m"]) else np.nan,
        "n_puntos_usados": int(mets["n_puntos_usados"]),
    }

# ==== Main ====

def main():
    logging.info(f"Iniciando generación de mapa de muestras (M2) {CIUDAD} 2025")
    BASE_CIUDAD_DIR = os.path.join(RESULTADOS_DIR, CIUDAD)
    os.makedirs(BASE_CIUDAD_DIR, exist_ok=True)

    # Consulta de datos
    if not os.path.exists(_cfg["csv_rutas"]):
        logging.warning(f"No existe archivo de coordenadas: {_cfg['csv_rutas']}. Continuando sin merge de barrios.")
    try:
        df = crear_df(CENTROOPE, FECHA_INICIO, FECHA_FIN, _cfg["csv_rutas"], promotores=None)
    except Exception as e:
        logging.error(f"Error al crear DF base: {e}")
        df = pd.DataFrame()

    # Resolver lat/lon
    if not df.empty:
        if "fecha_evento" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["fecha_evento"]):
            df["fecha_evento"] = pd.to_datetime(df["fecha_evento"], errors="coerce")
        try:
            df = _resolver_lat_lon(df)
        except Exception as e:
            logging.error(f"Abortando: {e}")
            df = pd.DataFrame()

    # Selector de promotor (rank) para nombrar carpeta
    df_plot = df.copy()
    selected_pid = 0
    selected_count = None
    if not df.empty and "id_autor" in df.columns:
        counts = None
        try:
            counts = df["id_autor"].dropna().astype(int).value_counts()
        except Exception as e:
            logging.warning(f"No fue posible calcular el ranking de promotores: {e}")
            counts = pd.Series(dtype=int)
        total_promotores = int(counts.shape[0]) if counts is not None else 0
        if total_promotores == 0:
            logging.warning("No se encontraron asesores/promotores; usando asesor_0")
            selected_pid = 0
        else:
            if promotor_num < 1 or promotor_num > total_promotores:
                raise ValueError(f"promotor_num ({promotor_num}) es inválido: hay {total_promotores} asesores encontrados")
            selected_pid = int(counts.index[promotor_num - 1])
            selected_count = int(counts.iloc[promotor_num - 1])
            df_plot = df[df["id_autor"].astype("Int64") == selected_pid].copy()

    # Estructura de salida por asesor
    ASESOR_DIR      = os.path.join(BASE_CIUDAD_DIR, f"asesor_{selected_pid}")
    CLUSTERS_DIR    = os.path.join(ASESOR_DIR, "clusters")
    SUBCLUSTERS_DIR = os.path.join(ASESOR_DIR, "sub_clusters")
    POLIGONOS_DIR   = os.path.join(ASESOR_DIR, "poligonos")  # opcional debug
    shutil.rmtree(ASESOR_DIR, ignore_errors=True)
    os.makedirs(CLUSTERS_DIR, exist_ok=True)
    os.makedirs(SUBCLUSTERS_DIR, exist_ok=True)
    os.makedirs(POLIGONOS_DIR, exist_ok=True)

    # Definir rutas de archivos por asesor
    global HTML_BASE, CSV_OUT, HTML_OUT_CLUST, METRICS_CSV, METRICAS_K_CSV, ELBOW_PNG
    HTML_BASE       = os.path.join(ASESOR_DIR,   "muestras_simple_base.html")
    CSV_OUT         = os.path.join(ASESOR_DIR,   "muestras_con_clusters.csv")
    HTML_OUT_CLUST  = os.path.join(CLUSTERS_DIR, "clusters_m2.html")
    METRICS_CSV     = os.path.join(CLUSTERS_DIR, "clusters_resumen.csv")
    METRICAS_K_CSV  = os.path.join(CLUSTERS_DIR, "metricas_por_k.csv")
    ELBOW_PNG       = os.path.join(CLUSTERS_DIR, "codo.png")

    if df.empty:
        logging.warning("DF vacío: se generará mapa sin puntos.")
        mapa_vacio = folium.Map(location=_cfg["center"], zoom_start=12, zoom_control=False)
        mapa_vacio.save(HTML_BASE)
        pd.DataFrame().to_csv(CSV_OUT, index=False, sep=";", encoding="utf-8-sig")
        logging.info(f"HTML vacío: {HTML_BASE}")
        logging.info(f"CSV vacío: {CSV_OUT}")
        return

    CLUSTER_PALETTE = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
        "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#393b79", "#637939"
    ]
    # Guardar CSV crudo
    try:
        df.to_csv(CSV_OUT, index=False, sep=";", encoding="utf-8-sig")
    except Exception as e:
        logging.error(f"No fue posible guardar CSV crudo: {e}")

    # Mapa base (puntos crudos) para HTML_BASE
    mapa_base = folium.Map(location=_cfg["center"], zoom_start=12, zoom_control=False)
    try:
        if os.path.exists(_cfg["geojson"]):
            with open(_cfg["geojson"], "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
            folium.GeoJson(geojson_data, name=f"Rutas {CIUDAD.title()}").add_to(mapa_base)
    except Exception:
        pass
    for _, r in df.iterrows():
        try:
            folium.CircleMarker([float(r["_lat"]), float(r["_lon"])], radius=3, color="#5b9bd5", fill=True, fillOpacity=0.7).add_to(mapa_base)
        except Exception:
            continue
    try:
        mapa_base.save(HTML_BASE)
    except Exception:
        logging.warning("No se pudo guardar mapa base")

    # Clustering global
    mapa_clusters = folium.Map(location=_cfg["center"], zoom_start=12, zoom_control=False)
    try:
        _add_rutas_layer(mapa_clusters)
    except Exception:
        pass
    df_plot, k_chosen = _cluster_and_draw(df_plot, CLUSTERS_DIR, mapa_clusters, CLUSTER_PALETTE)

    # Actualizar CSV con clusters
    try:
        df_out = df.copy()
        if 'cluster' in df_plot.columns:
            df_out.loc[df_plot.index, "cluster"] = df_plot["cluster"].values
        df_out.to_csv(CSV_OUT, index=False, sep=";", encoding="utf-8-sig")
    except Exception as e:
        logging.warning(f"No fue posible actualizar CSV con clusters: {e}")

    # Preparar rutas_utm individuales para cuadrante detección
    rutas_utm = []
    try:
        transformer_audit = Transformer.from_crs("EPSG:4326", _cfg["epsg_utm"], always_xy=True)
        if os.path.exists(_cfg["geojson"]):
            with open(_cfg["geojson"], "r", encoding="utf-8") as fr:
                gj_rutas = json.load(fr)
            def proj_ll_to_utm(x, y, z=None):
                X, Y = transformer_audit.transform(x, y)
                return (X, Y)
            for feat in gj_rutas.get("features", []):
                try:
                    g_ll = shape(feat.get("geometry", {}))
                    g_utm = shp_transform(proj_ll_to_utm, g_ll)
                    rutas_utm.append((g_utm, feat.get("properties", {}) or {}))
                except Exception:
                    continue
        clip_city_utm = _city_perimeter_union_utm(_cfg, transformer_audit) if CLIP_A_RUTAS else None
    except Exception as e:
        logging.warning(f"No se pudo preparar rutas_utm: {e}")
        transformer_audit = Transformer.from_crs("EPSG:4326", _cfg["epsg_utm"], always_xy=True)
        clip_city_utm = None

    # Mapa global de subclusters (todas las geometrías)
    mapa_subs_global = folium.Map(location=_cfg["center"], zoom_start=12, zoom_control=False)
    try:
        _add_rutas_layer(mapa_subs_global)
    except Exception:
        pass

    # Export por cluster -> subcluster único + concave
    resumen_rows = []
    if 'cluster' in df_plot.columns:
        for cl in sorted(df_plot['cluster'].dropna().unique().astype(int)):
            df_c = df_plot[df_plot['cluster'] == cl].copy()
            if "_lat" not in df_c.columns or "_lon" not in df_c.columns:
                try:
                    df_c = _resolver_lat_lon(df_c)
                except Exception:
                    pass
            out_dir_sub = os.path.join(SUBCLUSTERS_DIR, f"cluster_{cl}")
            os.makedirs(out_dir_sub, exist_ok=True)
            rows_sub, sub_map = _export_subclusters_kmeans(df_c, transformer_audit, out_dir_sub,
                                                           filename_html=f"C{cl}_subkmeans.html",
                                                           filename_csv=f"C{cl}_resumen.csv")
            # Concave hull único (sub_id=0)
            for sub_id, (df_iso_pruned, Xi2) in sub_map.items():
                sub_dir = os.path.join(out_dir_sub, f"sub_{int(sub_id)}")
                os.makedirs(sub_dir, exist_ok=True)
                rec_sub = _export_concave_sub(df_iso_pruned, transformer_audit, sub_dir,
                                              clip_city_utm=clip_city_utm,
                                              sub_idx=int(sub_id), cluster_id=int(cl), rutas_utm=rutas_utm,
                                              mapa_global=mapa_subs_global)
                resumen_rows.append(rec_sub)
            # Polígono consolidado por cluster (opcional) reutiliza función existente
            out_dir_cluster = os.path.join(POLIGONOS_DIR, f"cluster_{cl}")
            os.makedirs(out_dir_cluster, exist_ok=True)
            _ = _export_concave_cluster_from_submap(df_cluster=df_c, sub_map=sub_map,
                                                    transformer=transformer_audit, out_dir=out_dir_cluster,
                                                    clip_city_utm=clip_city_utm, cluster_id=int(cl))

    # Resumen de áreas por cluster/sub (CSV global)
    if isinstance(resumen_rows, list) and len(resumen_rows):
        df_res = pd.DataFrame(resumen_rows)
        df_res["metrica"] = "M2"
        cols_keep = ["cluster", "sub_id", "area_m2", "area_km2", "perimetro_m", "n_puntos_usados", "metrica"]
        df_res = df_res[cols_keep]
        out_global = os.path.join(SUBCLUSTERS_DIR, "subclusters_resumen.csv")
        dump_csv_coma_decimal(df_res, out_global, decimals=3)

    # Guardar mapa global de subclusters
    try:
        subclusters_html_path = os.path.join(SUBCLUSTERS_DIR, "subclusters_m2.html")
        mapa_subs_global.save(subclusters_html_path)
    except Exception as e:
        logging.warning(f"No fue posible guardar HTML de subclusters global: {e}")

    # Guardar mapa de clusters final
    try:
        mapa_clusters.save(HTML_OUT_CLUST)
    except Exception as e:
        logging.error(f"No fue posible guardar HTML clusters: {e}")

    logging.info(f"HTML base: {HTML_BASE}")
    logging.info(f"HTML clusters: {HTML_OUT_CLUST}")
    logging.info(f"CSV muestras/clusters: {CSV_OUT}")

if __name__ == "__main__":
    main()
