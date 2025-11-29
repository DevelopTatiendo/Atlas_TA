import os
import logging
from pathlib import Path
import pandas as pd
import folium
import numpy as np
import sys
from pathlib import Path

# Añadir la carpeta raíz del proyecto al PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
# Importaciones limitadas desde módulos existentes (según especificación)
from pre_procesamiento.preprocesamiento_muestras import consultar_muestras_db
from pre_procesamiento.metricas_areas import (
    areas_muestras_auditoria,
    _resolver_lat_lon,
    _from_lonlat_to_utm,
    _aud_cluster_labels,
    _aud_subclusters_por_cluster,
    SubclusterM2,
    _geom_utm_to_lonlat,
    get_transformer_utm,
)

# ==========================================
# Constantes de test (fácilmente modificables)
# ==========================================
TEST_CIUDAD = "CALI"
TEST_ID_AUTOR = 17357
TEST_FECHA_INI = "2025-01-01"
TEST_FECHA_FIN = "2025-11-01"
TEST_CLIENTES_X_MUESTRAS = False
SALIDA_DIR = "Pruebas/Resultados_M2_Test"

# Diccionario de ciudades y geojson base (copiado localmente)
COORDENADAS_CIUDADES = {
    'CALI': ([3.4516, -76.5320], 'geojson/comunas_cali.geojson'),
    'MEDELLIN': ([6.2442, -75.5812], 'geojson/comunas_medellin.geojson'),
    'MANIZALES': ([5.0672, -75.5174], 'geojson/comunas_manizales.geojson'),
    'PEREIRA': ([4.8087, -75.6906], 'geojson/comunas_pereira.geojson'),
    'BOGOTA': ([4.7110, -74.0721], 'geojson/comunas_bogota.geojson'),
    'BARRANQUILLA': ([10.9720, -74.7962], 'geojson/comunas_barranquilla.geojson'),
    'BUCARAMANGA': ([7.1193, -73.1227], 'geojson/comunas_bucaramanga.geojson'),
}

# Centroope mínimo requerido (copiado de auditoría)
CENTROOPES = {'CALI': 2, 'MEDELLIN': 3, 'BOGOTA': 1}


def _normalizar_ciudad(ciudad: str) -> str:
    import unicodedata
    s = ''.join(c for c in unicodedata.normalize('NFD', ciudad) if unicodedata.category(c) != 'Mn')
    s = s.upper()
    return ''.join(ch for ch in s if ch.isalnum())


def _ensure_salida_dir() -> Path:
    p = Path(SALIDA_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p


def cargar_datos_promotor_test() -> pd.DataFrame | None:
    ciudad_norm = _normalizar_ciudad(TEST_CIUDAD)
    if ciudad_norm not in CENTROOPES:
        logging.error(f"Ciudad no soportada para test: {TEST_CIUDAD}")
        return None
    centroope = CENTROOPES[ciudad_norm]
    logging.info(f"Consultando BD para promotor {TEST_ID_AUTOR} ciudad {ciudad_norm} rango {TEST_FECHA_INI} - {TEST_FECHA_FIN}")
    df = consultar_muestras_db(
        centroope=centroope,
        fecha_inicio=TEST_FECHA_INI,
        fecha_fin=TEST_FECHA_FIN,
        promotores=[TEST_ID_AUTOR],
    )
    if df is None or df.empty:
        logging.warning("DataFrame vacío de la consulta")
        return None
    df = df.copy()
    if TEST_CLIENTES_X_MUESTRAS and 'id_contacto' in df.columns:
        logging.info("Aplicando modo Clientes x Muestras (última muestra por cliente)")
        df = (
            df.sort_values('fecha_evento')
              .drop_duplicates(subset=['id_contacto'], keep='last')
        )
    return df


def debug_aud_subclusters_por_cluster(X: np.ndarray) -> tuple[list[SubclusterM2], np.ndarray]:
    """Reproduce la clasificación final de auditoría devolviendo subclusters y labels_final.

    labels_final:
      -1 -> punto descartado (poda/outlier o subcluster filtrado)
      id_subcluster -> punto usado en ese SubclusterM2.
    """
    if X is None or len(X) == 0:
        return [], np.array([], dtype=int)
    labels_raw, k_best = _aud_cluster_labels(X)
    # Obtener subclusters finales (usa misma lógica de producción real)
    subclusters = _aud_subclusters_por_cluster(X)
    if not subclusters:
        return [], np.full(len(X), -1, dtype=int)
    # Conjunto de ids válidos
    valid_ids = {int(sc.id_subcluster) for sc in subclusters}
    # Mapear coordenadas usadas por cada subcluster para filtrar outliers internos
    coords_por_id = {}
    for sc in subclusters:
        # Convertir a tuplas para hashing
        coords_por_id[int(sc.id_subcluster)] = { (float(r[0]), float(r[1])) for r in sc.X_utm }
    labels_final = np.full(len(X), -1, dtype=int)
    for i, lab in enumerate(labels_raw):
        lab_int = int(lab)
        if lab_int in valid_ids:
            tup = (float(X[i,0]), float(X[i,1]))
            if tup in coords_por_id[lab_int]:  # sobrevivió poda interna
                labels_final[i] = lab_int
    return subclusters, labels_final


def construir_df_puntos_con_clusters(df: pd.DataFrame, centroope: int):
    df_ll = _resolver_lat_lon(df)
    lonlat = df_ll[["_lon", "_lat"]].to_numpy(float)
    mask_valid = np.isfinite(lonlat).all(axis=1)
    lonlat_valid = lonlat[mask_valid]
    X = _from_lonlat_to_utm(lonlat_valid, centroope)
    subclusters, labels_final = debug_aud_subclusters_por_cluster(X)
    df_valid = df_ll.loc[mask_valid].copy()
    if len(df_valid) != len(X):
        logging.warning("Diferencia inesperada entre puntos válidos y matriz X")
    # Añadir coordenadas en metros
    if len(X):
        df_valid['x_m'] = X[:, 0]
        df_valid['y_m'] = X[:, 1]
    else:
        df_valid['x_m'] = []
        df_valid['y_m'] = []
    # Labels finales
    df_valid['id_subcluster_modelo'] = labels_final
    df_valid['usado_en_modelo'] = df_valid['id_subcluster_modelo'] >= 0
    return df_valid, subclusters


def guardar_csv_puntos(df_valid: pd.DataFrame) -> Path:
    out_dir = _ensure_salida_dir()
    cols_existentes = [c for c in [
        'id_muestra', 'id_contacto', 'fecha_evento', 'id_autor', '_lat', '_lon', 'x_m', 'y_m', 'id_subcluster_modelo', 'usado_en_modelo'
    ] if c in df_valid.columns or c in ['x_m','y_m','id_subcluster_modelo','usado_en_modelo']]
    out_path = out_dir / f"test_m2_puntos_por_subcluster_{TEST_ID_AUTOR}.csv"
    df_valid.to_csv(out_path, sep=';', index=False, encoding='utf-8-sig', columns=cols_existentes)
    logging.info(f"CSV puntos guardado: {out_path}")
    return out_path


def guardar_csv_subclusters(subclusters: list[SubclusterM2], df_valid: pd.DataFrame) -> Path:
    out_dir = _ensure_salida_dir()
    rows = []
    for sc in subclusters:
        n_label = int((df_valid['id_subcluster_modelo'] == int(sc.id_subcluster)).sum())
        rows.append({
            'id_autor': TEST_ID_AUTOR,
            'id_subcluster': int(sc.id_subcluster),
            'area_m2': float(sc.area_m2),
            'perimetro_m': float(sc.perimetro_m),
            'n_puntos_modelo': int(sc.n_puntos),
            'compacidad': float(sc.compacidad),
            'densidad_compacta': float(sc.densidad_compacta),
            'n_puntos_label': n_label,
            # 'n_puntos_geo': opcional (no calculado para evitar dependencia extra)
        })
    df_sc = pd.DataFrame(rows, columns=[
        'id_autor','id_subcluster','area_m2','perimetro_m','n_puntos_modelo','compacidad','densidad_compacta','n_puntos_label'
    ])
    out_path = out_dir / f"test_m2_subclusters_resumen_{TEST_ID_AUTOR}.csv"
    df_sc.to_csv(out_path, sep=';', index=False, encoding='utf-8-sig')
    logging.info(f"CSV subclusters guardado: {out_path}")
    return out_path


def guardar_csv_promotor(df: pd.DataFrame, centroope: int):
    out_dir = _ensure_salida_dir()
    df_areas_prom, fc = areas_muestras_auditoria(df, centroope)
    out_path = out_dir / f"test_m2_promotor_resumen_{TEST_ID_AUTOR}.csv"
    df_areas_prom.to_csv(out_path, sep=';', index=False, encoding='utf-8-sig')
    logging.info(f"CSV promotor guardado: {out_path}")
    return out_path, fc


def generar_mapa_html_test(df_valid: pd.DataFrame, fc: dict, centroope: int) -> Path:
    ciudad_norm = _normalizar_ciudad(TEST_CIUDAD)
    center_point, path_geojson = COORDENADAS_CIUDADES.get(ciudad_norm, ([3.4516,-76.5320], None))
    mapa = folium.Map(location=center_point, zoom_start=13)
    # Capa comunas
    if path_geojson and Path(path_geojson).exists():
        try:
            import json
            with open(path_geojson, 'r', encoding='utf-8') as f:
                gj_base = json.load(f)
            folium.GeoJson(
                gj_base,
                name="Comunas",
                style_function=lambda feature: {
                    "color": "black",
                    "weight": 1,
                    "fillOpacity": 0.0
                }
            ).add_to(mapa)
        except Exception as e:
            logging.error(f"No se pudo cargar geojson comunas: {e}")
    # Capa subclusters
    fg_sub = folium.FeatureGroup(name="Subclusters M2")
    for feat in fc.get("features", []):
        props = feat.get("properties", {})
        popup_html = (
            f"<b>Subcluster:</b> {props.get('id_subcluster','')}<br>"
            f"<b>Área m²:</b> {int(round(props.get('area_m2',0))):,}<br>"
            f"<b>Perímetro m:</b> {int(round(props.get('perimetro_m',0))):,}<br>"
            f"<b>Puntos usados:</b> {props.get('n_puntos','')}<br>"
            f"<b>Compacidad:</b> {props.get('compacidad',0):.4f}<br>"
            f"<b>Densidad compacta:</b> {props.get('densidad_compacta',0):.4f}"
        )
        gj = folium.GeoJson(feat, style_function=lambda f: {
            "color": "#1f77b4",
            "weight": 2,
            "fillColor": "#1f77b4",
            "fillOpacity": 0.15,
        })
        gj.add_child(folium.Popup(popup_html, max_width=300))
        gj.add_to(fg_sub)
    fg_sub.add_to(mapa)
    # Capa puntos
    fg_pts = folium.FeatureGroup(name="Puntos M2")
    for _, row in df_valid.iterrows():
        popup_pt = (
            f"<b>Fecha:</b> {row.get('fecha_evento','')}<br>"
            f"<b>Contacto:</b> {row.get('id_contacto','')}<br>"
            f"<b>Subcluster modelo:</b> {row.get('id_subcluster_modelo','')}<br>"
            f"<b>Usado:</b> {bool(row.get('usado_en_modelo', False))}"
        )
        folium.CircleMarker(
            location=[row.get('_lat'), row.get('_lon')],
            radius=4,
            color='#ff7f0e' if row.get('usado_en_modelo') else '#aaaaaa',
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(popup_pt, max_width=250)
        ).add_to(fg_pts)
    fg_pts.add_to(mapa)
    folium.LayerControl().add_to(mapa)
    out_dir = _ensure_salida_dir()
    out_path = out_dir / f"mapa_muestras_m2_test_{TEST_ID_AUTOR}.html"
    mapa.save(str(out_path))
    logging.info(f"Mapa HTML guardado: {out_path}")
    return out_path


def ejecutar_test_m2():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    ciudad_norm = _normalizar_ciudad(TEST_CIUDAD)
    if ciudad_norm not in CENTROOPES:
        logging.error("Ciudad no soportada en test.")
        return
    centroope = CENTROOPES[ciudad_norm]
    df = cargar_datos_promotor_test()
    if df is None:
        logging.error("Abortando: sin datos.")
        return
    df_valid, subclusters = construir_df_puntos_con_clusters(df, centroope)
    puntos_csv = guardar_csv_puntos(df_valid)
    subclusters_csv = guardar_csv_subclusters(subclusters, df_valid)
    promotor_csv, fc = guardar_csv_promotor(df, centroope)
    mapa_html = generar_mapa_html_test(df_valid, fc, centroope)
    print("=== Archivos generados ===")
    print(puntos_csv)
    print(subclusters_csv)
    print(promotor_csv)
    print(mapa_html)


if __name__ == "__main__":
    ejecutar_test_m2()
