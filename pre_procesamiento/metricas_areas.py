import numpy as np
import pandas as pd
import logging
from typing import Dict, Tuple, Iterable, List, Literal
from dataclasses import dataclass

from shapely.geometry import Point, MultiPoint, Polygon, mapping
from shapely.ops import unary_union, triangulate, transform as shp_transform
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from pyproj import Transformer

# =============================================================
# EPSG por centro de operación (CO)
# Basado en la configuración global CIUDADES (todos usan 32618 por ahora).
# =============================================================
EPSG_UTM_POR_CENTROOPE: dict[int, int] = {
    2: 32618,  # CALI
    3: 32618,  # MEDELLIN
    6: 32618,  # MANIZALES
    5: 32618,  # PEREIRA
    4: 32618,  # BOGOTA
    8: 32618,  # BARRANQUILLA
    7: 32618,  # BUCARAMANGA
}

_TRANSFORMERS_UTM: dict[int, Transformer] = {}


def get_transformer_utm(centroope: int | None) -> Transformer:
    """
    Retorna un Transformer WGS84 -> UTM para el centroope dado.
    Si centroope es None o no está en el diccionario, usa 32618 por defecto.
    """
    epsg = EPSG_UTM_POR_CENTROOPE.get(int(centroope) if centroope is not None else -1, 32618)
    if epsg in _TRANSFORMERS_UTM:
        return _TRANSFORMERS_UTM[epsg]
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    _TRANSFORMERS_UTM[epsg] = transformer
    return transformer

# =============================================================
# Constantes tomadas de la lógica M2 (solo geométricas)
# =============================================================
# Poda global y por subcluster (M2)
P_OUTLIER: float = 0.05
SUBK_P_OUTLIER: float = 0.025

# Selección de K para subclustering (M2)
SUBK_KMAX_ABS: int = 20
SUBK_KMAX_FRAC: float = 0.10
MIN_SUB_FRAC: float = 0.01

# Concave Hull (M2)
MIN_PTS_CONCAVE: int = 8
ALPHA_MODE: str = "fixed"   # "fixed" | "auto"
ALPHA_FIXED: float = 500.0
ALPHA_QNN_PCTL: int = 80
ALPHA_SCALE: float = 2.0
HOLE_MIN_FRAC: float = 0.03
HOLE_MIN_ABS: float = 2000
SMOOTHING_BUFFER_M: float = 90.0

# =============================================================
# Clustering global para modo auditoría (basado en MapaMetricasM2)
# =============================================================
# Selección de K (auditoría) por rango y elbow con umbral y curvatura
AUD_TAU_ELBOW: float = 0.12  # equivalente a TAU_ELBOW
AUD_K_MAX_ABS: int = 12      # tope superior para K global auditoría

# =============================================================
# Helpers internos compartidos
# =============================================================

def _resolver_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un df con columnas '_lat' y '_lon' aseguradas, a partir de las columnas
    estándar usadas en el proyecto: lat, lon, latitud, longitud, coordenada_latitud, coordenada_longitud.
    (Lógica equivalente a la de los scripts M1/M2)
    """
    lat_col = None
    for c in ["coordenada_latitud", "latitud", "lat"]:
        if c in df.columns:
            lat_col = c
            break
    lon_col = None
    for c in ["coordenada_longitud", "longitud", "lon"]:
        if c in df.columns:
            lon_col = c
            break
    if not lat_col or not lon_col:
        raise ValueError("No se encontraron columnas de lat/lon en el DataFrame.")
    out = df.copy()
    out["_lat"] = pd.to_numeric(out[lat_col], errors="coerce")
    out["_lon"] = pd.to_numeric(out[lon_col], errors="coerce")
    out = out.dropna(subset=["_lat", "_lon"]).copy()
    return out


def _from_lonlat_to_utm(xy: np.ndarray, centroope: int | None) -> np.ndarray:
    """
    Recibe array Nx2 [lon, lat] en grados y devuelve Nx2 [x_m, y_m] en UTM.
    Usa un Transformer dependiente del centroope.
    """
    if xy.size == 0:
        return xy.reshape(0, 2)
    mask_valid = np.isfinite(xy).all(axis=1)
    xy_valid = xy[mask_valid]
    if xy_valid.size == 0:
        return xy.reshape(0, 2)
    transformer = get_transformer_utm(centroope)
    xs, ys = transformer.transform(
        xy_valid[:, 0].astype(float),
        xy_valid[:, 1].astype(float)
    )
    X = np.column_stack([xs, ys])
    return X


def _build_X_por_promotor(df_ll: pd.DataFrame, centroope: int | None) -> Dict[int, np.ndarray]:
    """
    A partir de un df con columnas 'id_autor', '_lat', '_lon',
    devuelve un dict {id_autor: np.ndarray de shape (n_i, 2) en metros}.
    Adjunta también columnas 'x_m', 'y_m' al DataFrame (no retornado).
    """
    if "id_autor" not in df_ll.columns:
        return {}
    # Array lon/lat → UTM con alineación por máscara válida
    lonlat = df_ll[["_lon", "_lat"]].to_numpy(float)
    mask_valid = np.isfinite(lonlat).all(axis=1)
    df_ll = df_ll.loc[df_ll.index[mask_valid]].copy()
    lonlat_valid = lonlat[mask_valid]
    X = _from_lonlat_to_utm(lonlat_valid, centroope)
    df_ll["x_m"] = X[:, 0]
    df_ll["y_m"] = X[:, 1]
    out: Dict[int, np.ndarray] = {}
    for pid, sub in df_ll.groupby("id_autor"):
        try:
            A = sub[["x_m", "y_m"]].to_numpy(float)
            if A.size == 0:
                continue
            out[int(pid)] = A
        except Exception:
            continue
    return out


def _podar_outliers_xy(X: np.ndarray, p: float) -> Tuple[np.ndarray, np.ndarray]:
    """Poda radial respecto al centroide; devuelve (X_filtrado, mask_keep)."""
    n = len(X)
    if p <= 0 or n < 5:
        return X, np.ones(n, dtype=bool)
    c = X.mean(axis=0)
    r = np.sqrt(((X - c) ** 2).sum(axis=1))
    thr = np.quantile(r, 1 - p)
    keep = r <= thr
    return X[keep], keep


def _convex_hull_geom_utm(X: np.ndarray):
    if len(X) == 0:
        return None
    try:
        return MultiPoint([(float(x), float(y)) for x, y in X]).convex_hull
    except Exception:
        return None


def _elbow_min_k(X: np.ndarray, kmax: int) -> Tuple[int, Iterable[float]]:
    """
    Selecciona k* por 'primer codo' sobre log(WCSS), evaluando k=1..kmax (kmax>=1).

    Notas de robustez:
    - Si solo hay 1 valor de WCSS, devolvemos k=1.
    - Si hay exactamente 2 valores, no tiene sentido segunda derivada; devolvemos k=2 (o kmax si <2).
    - Solo aplicamos la lógica de diff/diff cuando hay al menos 3 puntos.
    """
    wcss: List[float] = []
    kmax_int = max(1, int(kmax))

    for k in range(1, kmax_int + 1):
        km = KMeans(n_clusters=k, n_init="auto", random_state=42)
        km.fit(X)
        wcss.append(float(km.inertia_))

    if len(wcss) == 0:
        return 1, wcss
    if len(wcss) == 1:
        return 1, wcss
    if len(wcss) == 2:
        return min(2, kmax_int), wcss

    y = np.log(np.array(wcss))
    d1 = np.diff(y)
    d2 = np.diff(d1)
    if d2.size == 0:
        return min(len(wcss), kmax_int), wcss
    idx_codo = int(np.argmax(d2)) + 2  # +2 por doble diff
    kstar = max(1, min(kmax_int, idx_codo))
    return kstar, wcss


def _alpha_auto_from_nn(X_utm: np.ndarray) -> float:
    n = len(X_utm)
    if n < 2:
        return max(5.0, float(ALPHA_FIXED))
    nn = NearestNeighbors(n_neighbors=min(2, n)).fit(X_utm)
    dists, _ = nn.kneighbors(X_utm)
    d1 = dists[:, 1]
    q = float(np.percentile(d1, ALPHA_QNN_PCTL)) if len(d1) else 0.0
    if q <= 0:
        return max(5.0, float(ALPHA_FIXED))
    alpha_m = float(ALPHA_SCALE * q)
    return max(5.0, alpha_m)


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
            return MultiPoint([(float(x), float(y)) for x, y in X_utm]).convex_hull
        mpt = MultiPoint([(float(x), float(y)) for x, y in X_utm])
        tris = triangulate(mpt)
        keep = []
        a2 = float(alpha_m)
        for t in tris:
            xs, ys = t.exterior.coords.xy
            coords = list(zip(xs, ys))
            edges = [
                np.hypot(coords[i + 1][0] - coords[i][0], coords[i + 1][1] - coords[i][1])
                for i in range(3)
            ]
            max_edge = max(edges)
            if max_edge <= a2:
                keep.append(t)
        if not keep:
            return mpt.convex_hull
        geom = unary_union(keep).buffer(0)
        # Filtrar agujeros pequeños
        try:
            total_area = float(geom.area)
            thr_area = max(HOLE_MIN_FRAC * total_area, float(HOLE_MIN_ABS))
            if getattr(geom, "geom_type", "") == "Polygon":
                geom = _filter_small_holes(geom, thr_area)
            elif getattr(geom, "geom_type", "") == "MultiPolygon":
                parts = []
                for p in geom.geoms:
                    parts.append(_filter_small_holes(p, thr_area))
                geom = unary_union(parts).buffer(0)
        except Exception:
            pass
        # Suavizado opcional de bordes
        if SMOOTHING_BUFFER_M and SMOOTHING_BUFFER_M > 0:
            try:
                geom = geom.buffer(SMOOTHING_BUFFER_M).buffer(-SMOOTHING_BUFFER_M)
            except Exception:
                pass
        return geom
    except Exception:
        return None

@dataclass
class SubclusterM2:
    id_subcluster: int
    area_m2: float
    perimetro_m: float
    n_puntos: int
    X_utm: np.ndarray
    geom_utm: object | None
    compacidad: float
    densidad_compacta: float


def _subclusters_m2_detalle(X: np.ndarray) -> List[SubclusterM2]:
    """
    Recibe X en metros (puntos de un promotor) y aplica la lógica M2 multi-subcluster:
      - poda global de outliers (P_OUTLIER)
      - selección de K (codo) hasta SUBK_KMAX_ABS o SUBK_KMAX_FRAC
      - KMeans para etiquetar subclusters
      - poda opcional por subcluster (SUBK_P_OUTLIER) y filtro por tamaño (MIN_SUB_FRAC)
      - concave hull / convex hull por subcluster
    Devuelve una lista de SubclusterM2 (una entrada por subcluster válido).
    """
    n = len(X)
    if n < 3:
        return []
    # 1) Poda global de outliers
    Xp, _ = _podar_outliers_xy(X, P_OUTLIER)
    if len(Xp) == 0:
        return []

    # 2) Cálculo de K máximo permitido
    kmax_raw = max(1, int(len(Xp) * float(SUBK_KMAX_FRAC)))
    kmax = max(1, min(int(SUBK_KMAX_ABS), int(kmax_raw)))

    # Si kmax == 1, fallback a un único subcluster
    if kmax == 1:
        if ALPHA_MODE == "fixed":
            alpha_m = float(ALPHA_FIXED)
            if alpha_m < 5.0:
                alpha_m = 5.0
        else:
            alpha_m = _alpha_auto_from_nn(Xp)
        geom = _concave_hull_from_points_utm(Xp, alpha_m)
        if geom is None:
            return []
        area = float(getattr(geom, 'area', 0.0))
        perim = float(getattr(geom, 'length', 0.0))
        # Polsby–Popper compactness: 4*pi*A / P^2, acotada [0,1]
        comp = float((4.0 * float(np.pi) * area) / (perim ** 2)) if perim > 0 and area > 0 else 0.0
        comp = max(0.0, min(1.0, comp))
        dens_c = float((len(Xp) / area) * comp) if area > 0 else 0.0
        return [SubclusterM2(
            id_subcluster=0,
            area_m2=area,
            perimetro_m=perim,
            n_puntos=int(len(Xp)),
            X_utm=Xp,
            geom_utm=geom,
            compacidad=comp,
            densidad_compacta=dens_c,
        )]

    # 3) Elección de K mediante elbow
    k_star, _ = _elbow_min_k(Xp, kmax)
    k_star = max(1, min(int(kmax), int(k_star)))

    # 4) Aplicar KMeans sobre Xp
    km = KMeans(n_clusters=int(k_star), n_init="auto", random_state=42)
    labels = km.fit_predict(Xp)

    # 5) Construir subclusters por etiqueta
    subclusters: List[SubclusterM2] = []
    uniq_labels = sorted(set(int(l) for l in labels.tolist()))
    total_after_prune = len(Xp)
    for lab in uniq_labels:
        mask = (labels == lab)
        X_lab = Xp[mask]
        n_lab_total = len(X_lab)
        if n_lab_total < 3:
            continue
        # Poda opcional por subcluster
        X_lab_pod, _ = _podar_outliers_xy(X_lab, SUBK_P_OUTLIER)
        n_lab = len(X_lab_pod)
        if n_lab < 3:
            continue
        # Filtro por tamaño mínimo relativo
        if n_lab < (float(MIN_SUB_FRAC) * float(total_after_prune)):
            continue
        # Geometría concave/convex por subcluster
        if ALPHA_MODE == "fixed":
            alpha_m = float(ALPHA_FIXED)
            if alpha_m < 5.0:
                alpha_m = 5.0
        else:
            alpha_m = _alpha_auto_from_nn(X_lab_pod)
        geom = _concave_hull_from_points_utm(X_lab_pod, alpha_m)
        if geom is None:
            continue
        area = float(getattr(geom, 'area', 0.0))
        perim = float(getattr(geom, 'length', 0.0))
        comp = float((4.0 * float(np.pi) * area) / (perim ** 2)) if perim > 0 and area > 0 else 0.0
        comp = max(0.0, min(1.0, comp))
        dens_c = float((len(X_lab_pod) / area) * comp) if area > 0 else 0.0
        subclusters.append(SubclusterM2(
            id_subcluster=int(lab),
            area_m2=area,
            perimetro_m=perim,
            n_puntos=int(n_lab),
            X_utm=X_lab_pod,
            geom_utm=geom,
            compacidad=comp,
            densidad_compacta=dens_c,
        ))

    # 6) Orden y devolución
    if not subclusters:
        return []
    subclusters.sort(key=lambda sc: int(sc.id_subcluster))
    return subclusters


def _geom_utm_to_lonlat(geom_utm, centroope: int | None):
    if geom_utm is None:
        return None
    transformer = get_transformer_utm(centroope)
    def proj_utm_to_ll(x, y, z=None):
        lon, lat = transformer.transform(x, y, direction="INVERSE")
        return (lon, lat)
    try:
        return shp_transform(proj_utm_to_ll, geom_utm)
    except Exception:
        return None

# =============================================================
# Helpers de clustering global para modo auditoría
# =============================================================

def _aud_k_range(n_points: int) -> List[int]:
    """Devuelve lista de K candidatos [2..Kmax] con Kmax=min(AUD_K_MAX_ABS, n_points-1) y al menos 2."""
    try:
        n = int(n_points)
    except Exception:
        n = 0
    if n < 3:
        return []  # se manejará en _aud_cluster_labels
    kmax = max(2, min(int(AUD_K_MAX_ABS), n - 1))
    return list(range(2, kmax + 1))

def _aud_curva_elbow(X: np.ndarray) -> Tuple[List[int], List[float]]:
    """Calcula (ks, inertias) sobre KMeans estándar para candidatos de _aud_k_range."""
    n = len(X)
    ks = _aud_k_range(n)
    if not ks:
        return [], []
    inertias: List[float] = []
    for k in ks:
        km = KMeans(n_clusters=int(k), random_state=42, n_init="auto")
        km.fit(X)
        inertias.append(float(km.inertia_))
    return ks, inertias

def _aud_k_por_codo_threshold(ks: List[int], inertias: List[float], tau: float) -> int:
    """Selecciona K por primer caída relativa (< tau) en mejora de SSE. Fallback: máximo K."""
    if not ks:
        return 1
    if len(inertias) != len(ks):
        return ks[0]
    # Mejoras relativas entre pasos consecutivos
    prev = inertias[0]
    for i in range(1, len(ks)):
        cur = inertias[i]
        if prev <= 0:
            prev = cur
            continue
        mejora_rel = (prev - cur) / prev
        if mejora_rel < tau:
            return ks[i - 1]  # K anterior al descenso bajo el umbral
        prev = cur
    return ks[-1]

def _aud_k_por_curvatura(ks: List[int], inertias: List[float]) -> int:
    """Selecciona K por máxima curvatura (segundo diferencial sobre log(inertia))."""
    if not ks:
        return 1
    arr = np.array(inertias, dtype=float)
    if arr.size <= 2:
        return ks[0]
    y = np.log(arr + 1e-9)
    d1 = np.diff(y)
    d2 = np.diff(d1)
    idx = int(np.argmax(d2)) + 2  # +2 por doble diff offset
    # Mapear idx (1..len(ks)) dentro de rango ks
    k_curv = ks[min(len(ks) - 1, max(0, idx - 2))]
    return k_curv

def _aud_elegir_k_elbow(ks: List[int], inertias: List[float], tau: float = AUD_TAU_ELBOW) -> int:
    """Elige K combinando criterio de umbral y curvatura (max entre ambos)."""
    if not ks:
        return 1
    k_tau = _aud_k_por_codo_threshold(ks, inertias, tau)
    k_curv = _aud_k_por_curvatura(ks, inertias)
    k_best = max(int(k_tau), int(k_curv))
    # Asegurar pertenencia al dominio
    if k_best not in ks:
        # escoger el más cercano presente
        k_best = ks[-1]
    return k_best

def _aud_cluster_labels(X: np.ndarray) -> Tuple[np.ndarray, int]:
    """Clustering global en UTM para auditoría devolviendo (labels, k_best)."""
    n = len(X)
    if n < 3:
        return np.zeros(n, dtype=int), 1
    ks, inertias = _aud_curva_elbow(X)
    if not ks:
        return np.zeros(n, dtype=int), 1
    k_best = _aud_elegir_k_elbow(ks, inertias, tau=AUD_TAU_ELBOW)
    # Ajuste defensivo
    if k_best < 1:
        k_best = 1
    if k_best == 1:
        return np.zeros(n, dtype=int), 1
    km = KMeans(n_clusters=int(k_best), random_state=42, n_init="auto")
    labels = km.fit_predict(X)
    return labels.astype(int), int(k_best)

def _aud_subclusters_por_cluster(X: np.ndarray) -> List[SubclusterM2]:
    """Construye SubclusterM2 por cluster global (auditoría)."""
    n = len(X)
    if n < 3:
        return []
    labels, k_best = _aud_cluster_labels(X)
    if k_best <= 1:
        # Un único cluster: aplicar lógica igual que resto (sin MIN_SUB_FRAC opcional)
        Xp, _ = _podar_outliers_xy(X, P_OUTLIER)
        if len(Xp) < 3:
            return []
        if ALPHA_MODE == "fixed":
            alpha_m = float(ALPHA_FIXED) if ALPHA_FIXED >= 5.0 else 5.0
        else:
            alpha_m = _alpha_auto_from_nn(Xp)
        geom = _concave_hull_from_points_utm(Xp, alpha_m)
        if geom is None:
            return []
        area = float(getattr(geom, 'area', 0.0))
        perim = float(getattr(geom, 'length', 0.0))
        comp = float((4.0 * float(np.pi) * area) / (perim ** 2)) if perim > 0 and area > 0 else 0.0
        comp = max(0.0, min(1.0, comp))
        dens_c = float((len(Xp) / area) * comp) if area > 0 else 0.0
        return [SubclusterM2(
            id_subcluster=0,
            area_m2=area,
            perimetro_m=perim,
            n_puntos=int(len(Xp)),
            X_utm=Xp,
            geom_utm=geom,
            compacidad=comp,
            densidad_compacta=dens_c,
        )]
    subclusters: List[SubclusterM2] = []
    total_n = len(X)
    unique_labels = sorted(set(int(l) for l in labels.tolist()))
    for lab in unique_labels:
        X_clust = X[labels == lab]
        if len(X_clust) < 3:
            continue
        # Poda interna opcional
        X_clust_pod, _ = _podar_outliers_xy(X_clust, P_OUTLIER)
        if len(X_clust_pod) < 3:
            continue
        # Filtro por tamaño relativo (opcional, mantiene coherencia con modo normal)
        if len(X_clust_pod) < (float(MIN_SUB_FRAC) * float(total_n)):
            continue
        if ALPHA_MODE == "fixed":
            alpha_m = float(ALPHA_FIXED) if ALPHA_FIXED >= 5.0 else 5.0
        else:
            alpha_m = _alpha_auto_from_nn(X_clust_pod)
        geom = _concave_hull_from_points_utm(X_clust_pod, alpha_m)
        if geom is None or getattr(geom, 'is_empty', False):
            continue
        area = float(getattr(geom, 'area', 0.0))
        perim = float(getattr(geom, 'length', 0.0))
        comp = float((4.0 * float(np.pi) * area) / (perim ** 2)) if perim > 0 and area > 0 else 0.0
        comp = max(0.0, min(1.0, comp))
        dens_c = float((len(X_clust_pod) / area) * comp) if area > 0 else 0.0
        subclusters.append(SubclusterM2(
            id_subcluster=int(lab),
            area_m2=area,
            perimetro_m=perim,
            n_puntos=int(len(X_clust_pod)),
            X_utm=X_clust_pod,
            geom_utm=geom,
            compacidad=comp,
            densidad_compacta=dens_c,
        ))
    if not subclusters:
        return []
    subclusters.sort(key=lambda sc: int(sc.id_subcluster))
    return subclusters

# =============================================================
# API pública
# =============================================================

def _ensure_id_autor(df: pd.DataFrame) -> pd.DataFrame:
    if "id_autor" not in df.columns:
        raise ValueError("El DataFrame debe incluir la columna 'id_autor'.")
    return df


def calcular_areas_por_promotor(
    df: pd.DataFrame,
    centroope: int | None,
) -> pd.DataFrame:
    """
    Calcula la huella de muestreo por promotor (M2) en m².

    - df: DataFrame con al menos columnas de lat/lon estándar y 'id_autor'.
    - centroope: código del centro de operación (2=CALI, 3=MEDELLIN, etc.).
    """
    # logging.info(f"[AREAS-DEBUG] inicio calcular_areas_por_promotor centroope={centroope} n_filas={0 if df is None else len(df)}")  # DEBUG deshabilitado
    if df is None or df.empty:
        return pd.DataFrame(columns=["id_autor", "area_total_m2", "puntos_usados_total", "puntos_totales", "densidad_compacta_promotor"])

    _ensure_id_autor(df)
    df_ll = _resolver_lat_lon(df)
    # logging.info(f"[AREAS-DEBUG] df_ll_rows={len(df_ll)} cols={list(df_ll.columns)}")  # DEBUG deshabilitado
    if df_ll.empty:
        return pd.DataFrame(columns=["id_autor", "area_total_m2", "puntos_usados_total", "puntos_totales", "densidad_compacta_promotor"])

    X_por_promotor = _build_X_por_promotor(df_ll, centroope)
    # logging.info(f"[AREAS-DEBUG] n_promotores_X={len(X_por_promotor)} keys={list(X_por_promotor.keys())}")  # DEBUG deshabilitado
    if not X_por_promotor:
        return pd.DataFrame(columns=["id_autor", "area_total_m2", "puntos_usados_total", "puntos_totales", "densidad_compacta_promotor"])

    rows: List[dict] = []
    for pid, X in X_por_promotor.items():
        detalles = _subclusters_m2_detalle(X)
        if not detalles:
            area_total = 0.0
            usados = 0
            dens_comp_prom = 0.0
        else:
            area_total = float(sum(sc.area_m2 for sc in detalles))
            usados = int(sum(sc.n_puntos for sc in detalles))
            # Promedio ponderado por n_puntos de la densidad compacta de subclusters
            wsum = float(sum(float(sc.densidad_compacta) * float(sc.n_puntos) for sc in detalles))
            nsum = float(sum(float(sc.n_puntos) for sc in detalles))
            dens_comp_prom = float(wsum / nsum) if nsum > 0 else 0.0
        # puntos_totales = registros originales de este id_autor (con lat/lon válidos)
        puntos_totales = int(len(df_ll[df_ll['id_autor'] == pid]))
        rows.append({
            "id_autor": int(pid),
            "area_total_m2": area_total,
            "puntos_usados_total": usados,
            "puntos_totales": puntos_totales,
            "densidad_compacta_promotor": dens_comp_prom,
        })

    return pd.DataFrame(rows, columns=["id_autor", "area_total_m2", "puntos_usados_total", "puntos_totales", "densidad_compacta_promotor"])\
        .drop_duplicates("id_autor")


def generar_geojson_subclusters_promotor(
    df_promotor: pd.DataFrame,
    centroope: int | None,
) -> Tuple[pd.DataFrame, dict]:
    """
    Genera las métricas agregadas y el FeatureCollection GeoJSON
    de los subclusters M2 para un solo promotor.
    """
    if df_promotor is None or df_promotor.empty:
        return (
            pd.DataFrame(columns=["id_autor", "area_total_m2", "puntos_usados_total", "puntos_totales"]),
            {"type": "FeatureCollection", "features": []}
        )

    _ensure_id_autor(df_promotor)
    pids = [int(x) for x in pd.Series(df_promotor['id_autor']).dropna().astype(int).unique().tolist()]
    if len(pids) != 1:
        raise ValueError("df_promotor debe contener datos de un único id_autor.")
    pid = int(pids[0])

    df_ll = _resolver_lat_lon(df_promotor)
    puntos_totales = int(len(df_ll))
    lonlat = df_ll[["_lon", "_lat"]].to_numpy(float)
    mask_valid = np.isfinite(lonlat).all(axis=1)
    lonlat_valid = lonlat[mask_valid]
    X = _from_lonlat_to_utm(lonlat_valid, centroope)

    # Auditoría: usar clustering global tipo MapaMetricasM2
    detalles = _aud_subclusters_por_cluster(X)
    if not detalles:
        df_metrics = pd.DataFrame([{
            "id_autor": pid,
            "area_total_m2": 0.0,
            "puntos_usados_total": 0,
            "puntos_totales": puntos_totales,
            "densidad_compacta_promotor": 0.0,
        }])
        return df_metrics, {"type": "FeatureCollection", "features": []}

    # NO recalcular n_puntos; ya refleja los puntos del subcluster tras poda y clustering.
    # NO modificar densidad_compacta aquí.

    area_total = float(sum(sc.area_m2 for sc in detalles))
    usados_total = int(sum(sc.n_puntos for sc in detalles))
    wsum = float(sum(float(sc.densidad_compacta) * float(sc.n_puntos) for sc in detalles))
    nsum = float(sum(float(sc.n_puntos) for sc in detalles))
    dens_comp_prom = float(wsum / nsum) if nsum > 0 else 0.0
    df_metrics = pd.DataFrame([{
        "id_autor": pid,
        "area_total_m2": area_total,
        "puntos_usados_total": usados_total,
        "puntos_totales": puntos_totales,
        "densidad_compacta_promotor": dens_comp_prom,
    }])

    # GeoJSON FeatureCollection
    features: List[dict] = []
    for sc in detalles:
        geom_ll = _geom_utm_to_lonlat(sc.geom_utm, centroope)
        if geom_ll is None:
            continue
        feat = {
            "type": "Feature",
            "geometry": mapping(geom_ll),
            "properties": {
                "id_autor": int(pid),
                "id_subcluster": int(sc.id_subcluster),
                "area_m2": float(sc.area_m2),
                "perimetro_m": float(sc.perimetro_m),
                "n_puntos": int(sc.n_puntos),
                "compacidad": float(sc.compacidad),
                "densidad_compacta": float(sc.densidad_compacta),
            },
        }
        features.append(feat)
    fc = {"type": "FeatureCollection", "features": features}
    return df_metrics, fc

# =============================================================
# Notas:
# - Este módulo calcula geometrías de subclusters tipo M2 en metros.
# - Provee:
#     * calcular_areas_por_promotor: resumen de área total (m²) por promotor.
#     * generar_geojson_subclusters_promotor: detalle por subcluster + GeoJSON para auditoría.

# =============================================================
# Wrappers específicos para el módulo de Muestras
# =============================================================

def areas_muestras_resumen(
    df: pd.DataFrame,
    centroope: int | None,
    agrupar_por: Literal["Promotor", "Mes"] = "Promotor",
) -> pd.DataFrame:
    """
    Resumen de áreas para el módulo de Muestras.

    - agrupar_por="Promotor": comportamiento actual, devuelve columnas
      [id_autor, area_m2, densidad_compacta_promotor].
    - agrupar_por="Mes": suma de áreas por mes, devuelve columnas [mes, area_m2].
    """
    # Estructuras vacías según agrupación
    if df is None or df.empty:
        if agrupar_por == "Promotor":
            return pd.DataFrame(columns=["id_autor", "area_m2", "densidad_compacta_promotor"])
        else:
            return pd.DataFrame(columns=["mes", "area_m2"])

    if agrupar_por == "Promotor":
        df_base = calcular_areas_por_promotor(df, centroope)
        if df_base is None or df_base.empty:
            return pd.DataFrame(columns=["id_autor", "area_m2", "densidad_compacta_promotor", "n_puntos", "clientes_por_km2"])

        out = (
            df_base[["id_autor", "area_total_m2", "densidad_compacta_promotor", "puntos_totales"]]
            .copy()
            .rename(columns={"area_total_m2": "area_m2"})
        )
        # Aseguramos tipos limpios
        out["id_autor"] = pd.to_numeric(out["id_autor"], errors="coerce").astype("Int64")
        out["area_m2"] = pd.to_numeric(out["area_m2"], errors="coerce")
        if "densidad_compacta_promotor" in out.columns:
            out["densidad_compacta_promotor"] = pd.to_numeric(
                out["densidad_compacta_promotor"], errors="coerce"
            )
        # n_puntos para compatibilidad con densidad legacy
        out = out.rename(columns={"puntos_totales": "n_puntos"})
        out["n_puntos"] = pd.to_numeric(out["n_puntos"], errors="coerce").fillna(0).astype(int)
        # Densidad legacy (factor 1000): clientes_por_km2 = n_puntos * 1000 / area_m2
        # Nota: replica la convención antigua para continuidad de interpretación.
        out["clientes_por_km2"] = out.apply(
            lambda r: (float(r["n_puntos"]) * 1000.0 / float(r["area_m2"])) if pd.notna(r["area_m2"]) and float(r["area_m2"]) > 0 else None,
            axis=1,
        )
        out = out.dropna(subset=["id_autor"]).reset_index(drop=True)
        return out

    elif agrupar_por == "Mes":
        df_local = df.copy()
        # Asegurar columna 'mes'
        if "mes" not in df_local.columns:
            if "fecha_evento" not in df_local.columns:
                raise ValueError("Para agrupar por 'Mes' se requiere columna 'mes' o 'fecha_evento'.")
            df_local["mes"] = pd.to_datetime(df_local["fecha_evento"], errors="coerce").dt.month

        meses = sorted(pd.Series(df_local["mes"]).dropna().unique().tolist())
        rows: List[dict] = []
        for m in meses:
            try:
                m_int = int(m)
            except Exception:
                continue
            df_mes = df_local[df_local["mes"].astype("Int64") == m_int]
            if df_mes.empty:
                continue
            df_prom_mes = calcular_areas_por_promotor(df_mes, centroope)
            if df_prom_mes is None or df_prom_mes.empty:
                area_total_mes = 0.0
                puntos_total_mes = 0
            else:
                area_total_mes = float(pd.to_numeric(df_prom_mes["area_total_m2"], errors="coerce").fillna(0).sum())
                puntos_total_mes = int(pd.to_numeric(df_prom_mes["puntos_totales"], errors="coerce").fillna(0).sum())
            # Densidad legacy por mes: n_puntos_total * 1000 / area_m2_total
            clientes_por_km2_mes = (puntos_total_mes * 1000.0 / area_total_mes) if area_total_mes > 0 else None
            rows.append({"mes": m_int, "area_m2": area_total_mes, "n_puntos": puntos_total_mes, "clientes_por_km2": clientes_por_km2_mes})

        if not rows:
            return pd.DataFrame(columns=["mes", "area_m2"])

        out = pd.DataFrame(rows)
        out["mes"] = pd.to_numeric(out["mes"], errors="coerce").astype("Int64")
        out["area_m2"] = pd.to_numeric(out["area_m2"], errors="coerce")
        if "n_puntos" in out.columns:
            out["n_puntos"] = pd.to_numeric(out["n_puntos"], errors="coerce").fillna(0).astype(int)
        # clientes_por_km2 ya calculado arriba por fila
        out = out.dropna(subset=["mes"]).reset_index(drop=True)
        return out

    else:
        raise ValueError(f"Valor no soportado para agrupar_por: {agrupar_por}")


# Alias explícito para auditoría de Muestras: retorna (df_metrics, feature_collection)
areas_muestras_auditoria = generar_geojson_subclusters_promotor
