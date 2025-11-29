"""
Utilidades espaciales para operaciones geoespaciales reutilizables.

Este módulo evita dependencias circulares proporcionando funciones
independientes para asignación de puntos a cuadrantes y cálculo de áreas.
"""

import pandas as pd
from shapely.geometry import shape, Point
from shapely.prepared import prep
from pyproj import Geod


def assign_quadrant_to_points(df_pts: pd.DataFrame, features_cuadrantes: list, codigo_key: str = 'codigo') -> pd.Series:
    """
    Asigna cada punto a su cuadrante correspondiente usando Point-in-Polygon.
    
    Args:
        df_pts: DataFrame con columnas 'lat' (float) y 'lon' (float)
        features_cuadrantes: Lista de features GeoJSON de cuadrantes
        codigo_key: Nombre del campo en feature['properties'] que contiene el código del cuadrante
        
    Returns:
        pd.Series: Serie indexada como df_pts.index con el código del cuadrante
                   que contiene cada punto, o None si no está en ningún cuadrante.
                   
    Notes:
        - Usa pre-filtro por bounding box para optimizar rendimiento
        - Utiliza shapely.prepared.prep para acelerar operaciones contains
        - No requiere geopandas, compatible con stack actual
        - Mantiene consistencia con implementación existente en mapa_muestras.py
    """
    if df_pts.empty or not features_cuadrantes:
        return pd.Series([None] * len(df_pts), index=df_pts.index, name="cod_cuadrante")

    # Validar que existan las columnas requeridas
    if 'lat' not in df_pts.columns or 'lon' not in df_pts.columns:
        raise ValueError("DataFrame debe contener columnas 'lat' y 'lon'")

    # Filtrar puntos válidos (no NaN)
    work = df_pts[['lat', 'lon']].dropna()
    
    if work.empty:
        return pd.Series([None] * len(df_pts), index=df_pts.index, name="cod_cuadrante")

    # Precomputar polígonos preparados y sus bounding boxes
    prepared_polys = []
    for feature in features_cuadrantes:
        props = feature.get('properties', {})
        codigo = props.get(codigo_key, '')
        
        if not codigo:
            continue
            
        try:
            geom = shape(feature.get('geometry', {}))
            prep_geom = prep(geom)
            bounds = geom.bounds  # (minx, miny, maxx, maxy)
            prepared_polys.append((codigo, prep_geom, bounds))
        except Exception:
            # Skip invalid geometries
            continue

    # Inicializar resultado
    result = pd.Series([None] * len(work), index=work.index, name="cod_cuadrante")
    
    # Asignar cuadrante a cada punto válido
    for idx, row in work.iterrows():
        try:
            lat, lon = float(row['lat']), float(row['lon'])
            point = Point(lon, lat)  # shapely usa (x, y) = (lon, lat)
            
            # Probar contra cada cuadrante con pre-filtro bbox
            for codigo, prep_geom, bounds in prepared_polys:
                # Pre-filtro: verificar si el punto está dentro del bounding box
                minx, miny, maxx, maxy = bounds
                if minx <= lon <= maxx and miny <= lat <= maxy:
                    # Verificar contains solo si pasó el pre-filtro
                    if prep_geom.contains(point):
                        result.at[idx] = codigo
                        break  # Asignar al primer cuadrante que contenga el punto
                        
        except Exception:
            # Mantener None para puntos con problemas
            continue
    
    # Expandir resultado al índice completo del DataFrame original
    full_result = pd.Series([None] * len(df_pts), index=df_pts.index, name="cod_cuadrante")
    full_result.loc[result.index] = result
    
    return full_result


def area_m2_geodesic(feature_geom: dict) -> float:
    """
    Calcula el área geodésica exacta de una geometría GeoJSON en metros cuadrados.
    
    Args:
        feature_geom: Diccionario con geometría GeoJSON (ej: feature['geometry'])
        
    Returns:
        float: Área en metros cuadrados (valor absoluto)
        
    Notes:
        - Utiliza pyproj.Geod con elipsoide WGS84 para cálculos geodésicos exactos
        - Retorna valor absoluto para evitar áreas negativas por orientación
        - Compatible con geometrías Polygon y MultiPolygon
        
    Raises:
        Exception: Si la geometría es inválida o no se puede procesar
    """
    # Inicializar calculadora geodésica
    geod = Geod(ellps="WGS84")
    
    # Convertir a geometría shapely
    geom = shape(feature_geom)
    
    # Calcular área y perímetro geodésicos
    area, _ = geod.geometry_area_perimeter(geom)
    
    # Retornar valor absoluto (el área puede ser negativa dependiendo de la orientación)
    return abs(area)