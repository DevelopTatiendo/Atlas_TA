"""
Utilidades geoespaciales para análisis de consultores por cuadrantes.
Implementa operaciones de point-in-polygon, cálculo de áreas y agregaciones espaciales.
"""
import os
import json
import logging
import time
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, shape
import pyproj
from pyproj import CRS, Transformer

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# EPSG:3116 - MAGNA-SIRGAS / Colombia Bogota zone
# Sistema de coordenadas proyectado oficial para Colombia
COLOMBIA_CRS = "EPSG:3116"
WGS84_CRS = "EPSG:4326"

def cargar_geojson_cuadrantes(geojson_path: str) -> gpd.GeoDataFrame:
    """
    Carga un archivo GeoJSON de cuadrantes y lo convierte a GeoDataFrame.
    
    Args:
        geojson_path (str): Ruta al archivo GeoJSON
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame con cuadrantes cargados
    
    Raises:
        FileNotFoundError: Si el archivo no existe
        ValueError: Si el archivo no es un GeoJSON válido
    """
    inicio_tiempo = time.time()
    logger.info(f"Cargando GeoJSON de cuadrantes desde: {geojson_path}")
    
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"Archivo GeoJSON no encontrado: {geojson_path}")
    
    try:
        # Cargar usando geopandas
        gdf = gpd.read_file(geojson_path)
        
        # Validar que tenga geometría
        if gdf.empty:
            raise ValueError("El archivo GeoJSON está vacío")
        
        if 'geometry' not in gdf.columns:
            raise ValueError("El archivo GeoJSON no tiene columna de geometría")
        
        # Asegurar que esté en WGS84 inicialmente
        if gdf.crs is None:
            gdf.set_crs(WGS84_CRS, inplace=True)
            logger.warning("CRS no definido, asumiendo WGS84")
        
        # Validar que tenga campo codigo
        if 'codigo' not in gdf.columns:
            logger.warning("Campo 'codigo' no encontrado en properties")
            gdf['codigo'] = [f"CUADRANTE_{i+1}" for i in range(len(gdf))]
        
        tiempo_ejecucion = time.time() - inicio_tiempo
        logger.info(f"GeoJSON cargado exitosamente en {tiempo_ejecucion:.2f}s - {len(gdf)} features")
        
        return gdf
        
    except Exception as e:
        logger.error(f"Error cargando GeoJSON: {str(e)}")
        raise e

def calcular_areas_cuadrantes(gdf_cuadrantes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Calcula el área en metros cuadrados de cada cuadrante usando EPSG:3116.
    
    Args:
        gdf_cuadrantes (gpd.GeoDataFrame): GeoDataFrame con cuadrantes
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame con columna 'area_m2' agregada
    
    Raises:
        ValueError: Si hay áreas inválidas o negativas
    """
    inicio_tiempo = time.time()
    logger.info("Calculando áreas de cuadrantes en EPSG:3116")
    
    # Crear copia para no modificar el original
    gdf_areas = gdf_cuadrantes.copy()
    
    # Proyectar a EPSG:3116 para cálculo de área preciso
    gdf_projected = gdf_areas.to_crs(COLOMBIA_CRS)
    
    # Calcular área en metros cuadrados
    gdf_projected['area_m2'] = gdf_projected.geometry.area
    
    # Validar áreas
    areas_invalidas = gdf_projected[gdf_projected['area_m2'] <= 0]
    if not areas_invalidas.empty:
        logger.warning(f"Se encontraron {len(areas_invalidas)} cuadrantes con área inválida")
        for idx, row in areas_invalidas.iterrows():
            logger.warning(f"Cuadrante {row.get('codigo', idx)}: área = {row['area_m2']}")
    
    # Mantener geometría en WGS84 pero agregar área calculada
    gdf_areas['area_m2'] = gdf_projected['area_m2']
    
    # Estadísticas de áreas
    area_total = gdf_areas['area_m2'].sum()
    area_promedio = gdf_areas['area_m2'].mean()
    
    tiempo_ejecucion = time.time() - inicio_tiempo
    logger.info(f"Áreas calculadas en {tiempo_ejecucion:.2f}s")
    logger.info(f"Área total: {area_total:,.0f} m², Área promedio: {area_promedio:,.0f} m²")
    
    return gdf_areas

def puntos_en_cuadrantes(df_eventos: pd.DataFrame, gdf_cuadrantes: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Realiza operación point-in-polygon para asignar eventos a cuadrantes.
    
    Args:
        df_eventos (pd.DataFrame): DataFrame con eventos que incluye 'lat', 'lon'
        gdf_cuadrantes (gpd.GeoDataFrame): GeoDataFrame con cuadrantes
    
    Returns:
        pd.DataFrame: DataFrame original con columna 'codigo_cuadrante' agregada
    
    Notes:
        - Eventos fuera de cuadrantes tendrán codigo_cuadrante = None
        - Se loggea el tiempo de join espacial
    """
    inicio_tiempo = time.time()
    logger.info(f"Iniciando point-in-polygon para {len(df_eventos)} eventos y {len(gdf_cuadrantes)} cuadrantes")
    
    if df_eventos.empty:
        logger.warning("DataFrame de eventos está vacío")
        df_resultado = df_eventos.copy()
        df_resultado['codigo_cuadrante'] = None
        return df_resultado
    
    if gdf_cuadrantes.empty:
        logger.warning("GeoDataFrame de cuadrantes está vacío")
        df_resultado = df_eventos.copy()
        df_resultado['codigo_cuadrante'] = None
        return df_resultado
    
    # Crear puntos de geometría a partir de lat/lon
    geometry = [Point(lon, lat) for lon, lat in zip(df_eventos['lon'], df_eventos['lat'])]
    gdf_eventos = gpd.GeoDataFrame(df_eventos, geometry=geometry, crs=WGS84_CRS)
    
    # Asegurar que ambos GeoDataFrames estén en el mismo CRS
    if gdf_cuadrantes.crs != gdf_eventos.crs:
        gdf_cuadrantes = gdf_cuadrantes.to_crs(gdf_eventos.crs)
    
    # Realizar spatial join (point-in-polygon)
    gdf_resultado = gpd.sjoin(gdf_eventos, gdf_cuadrantes[['codigo', 'geometry']], 
                             how='left', predicate='within')
    
    # Renombrar columna del join
    if 'codigo' in gdf_resultado.columns:
        gdf_resultado = gdf_resultado.rename(columns={'codigo': 'codigo_cuadrante'})
    else:
        gdf_resultado['codigo_cuadrante'] = None
    
    # Convertir de vuelta a DataFrame regular
    df_resultado = pd.DataFrame(gdf_resultado.drop(columns='geometry'))
    
    # Limpiar columnas auxiliares del join
    columnas_auxiliares = ['index_right']
    for col in columnas_auxiliares:
        if col in df_resultado.columns:
            df_resultado = df_resultado.drop(columns=[col])
    
    # Estadísticas del join espacial
    eventos_en_cuadrantes = df_resultado['codigo_cuadrante'].notna().sum()
    eventos_fuera = len(df_resultado) - eventos_en_cuadrantes
    
    tiempo_ejecucion = time.time() - inicio_tiempo
    logger.info(f"Join espacial completado en {tiempo_ejecucion:.2f}s")
    logger.info(f"Eventos en cuadrantes: {eventos_en_cuadrantes}, Eventos fuera: {eventos_fuera}")
    
    return df_resultado

def generar_resumen_por_cuadrante(df_eventos_con_cuadrantes: pd.DataFrame, 
                                 gdf_cuadrantes: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Genera resumen agregado por cuadrante con métricas de consultores.
    
    Args:
        df_eventos_con_cuadrantes (pd.DataFrame): Eventos con codigo_cuadrante asignado
        gdf_cuadrantes (gpd.GeoDataFrame): Cuadrantes con áreas calculadas
    
    Returns:
        pd.DataFrame: Resumen con columnas ['codigo_cuadrante', 'area_m2', 'visitas_tot', 
                     'visitas_por_m2', 'aperturas_tot', 'aperturas_sac_tot', 'ventas_58_tot', 'ventas_fuera_tot', 'total_venta_tot', 'consultores']
    """
    inicio_tiempo = time.time()
    logger.info("Generando resumen por cuadrante")
    
    # Filtrar solo eventos que están en cuadrantes
    df_en_cuadrantes = df_eventos_con_cuadrantes[
        df_eventos_con_cuadrantes['codigo_cuadrante'].notna()
    ].copy()
    
    if df_en_cuadrantes.empty:
        logger.warning("No hay eventos dentro de cuadrantes")
        # Retornar estructura vacía pero correcta
        return pd.DataFrame(columns=['codigo_cuadrante', 'area_m2', 'visitas_tot', 
                                   'visitas_por_m2', 'aperturas_tot', 'aperturas_sac_tot', 
                                   'ventas_58_tot', 'ventas_fuera_tot', 'total_venta_tot', 'consultores'])
    
    # Agregar por cuadrante - usando nuevos campos unificados
    resumen = df_en_cuadrantes.groupby('codigo_cuadrante').agg({
        'es_visita': 'sum',           # Total visitas
        'apertura': 'sum',            # Total aperturas
        'apertura_sac': 'sum',        # Total aperturas SAC
        'venta_ruta': 'sum',          # Total ventas 58
        'venta_fuera_ruta': 'sum',    # Total ventas fuera
        'id_consultor': 'nunique'     # Número de consultores únicos
    }).reset_index()
    
    # Renombrar columnas según especificación
    resumen = resumen.rename(columns={
        'es_visita': 'visitas_tot',
        'apertura': 'aperturas_tot',
        'apertura_sac': 'aperturas_sac_tot',
        'venta_ruta': 'ventas_58_tot',
        'venta_fuera_ruta': 'ventas_fuera_tot',
        'id_consultor': 'consultores'
    })
    
    # Manejar compatibilidad si algunas columnas no existen
    if 'aperturas_tot' not in resumen.columns:
        resumen['aperturas_tot'] = 0
    if 'aperturas_sac_tot' not in resumen.columns:
        resumen['aperturas_sac_tot'] = 0
    if 'ventas_58_tot' not in resumen.columns:
        resumen['ventas_58_tot'] = 0
    if 'ventas_fuera_tot' not in resumen.columns:
        resumen['ventas_fuera_tot'] = 0
    
    # Agregar áreas desde GeoDataFrame
    areas_df = gdf_cuadrantes[['codigo', 'area_m2']].copy()
    areas_df = areas_df.rename(columns={'codigo': 'codigo_cuadrante'})
    
    # Join con áreas
    resumen = resumen.merge(areas_df, on='codigo_cuadrante', how='left')
    
    # Calcular visitas por m2
    resumen['visitas_por_m2'] = 0.0
    mask_area_valida = resumen['area_m2'] > 0
    resumen.loc[mask_area_valida, 'visitas_por_m2'] = (
        resumen.loc[mask_area_valida, 'visitas_tot'] / 
        resumen.loc[mask_area_valida, 'area_m2']
    )
    
    # Agregar total_venta_tot (requiere datos de ventas con valor)
    # Por ahora inicializar en 0, se actualizará con datos de ventas reales
    resumen['total_venta_tot'] = 0.0
    
    # Reordenar columnas según especificación
    columnas_finales = ['codigo_cuadrante', 'area_m2', 'visitas_tot', 'visitas_por_m2', 
                       'aperturas_tot', 'aperturas_sac_tot', 'ventas_58_tot', 'ventas_fuera_tot', 'total_venta_tot', 'consultores']
    resumen = resumen[columnas_finales]
    
    # Validaciones
    areas_invalidas = resumen[resumen['area_m2'] <= 0]
    if not areas_invalidas.empty:
        logger.warning(f"Cuadrantes con áreas inválidas: {areas_invalidas['codigo_cuadrante'].tolist()}")
    
    tiempo_ejecucion = time.time() - inicio_tiempo
    logger.info(f"Resumen por cuadrante generado en {tiempo_ejecucion:.2f}s - {len(resumen)} cuadrantes")
    
    return resumen

def generar_detalle_por_cuadrante_consultor(df_eventos_con_cuadrantes: pd.DataFrame) -> pd.DataFrame:
    """
    Genera detalle por cuadrante-consultor con métricas individuales.
    
    Args:
        df_eventos_con_cuadrantes (pd.DataFrame): Eventos con codigo_cuadrante asignado
    
    Returns:
        pd.DataFrame: Detalle con columnas ['codigo_cuadrante', 'id_consultor', 'apellido', 
                     'visitas', 'aperturas', 'aperturas_sac', 'sac', 'ventas_58', 'ventas_fuera', 'total_venta_conIVA']
    """
    inicio_tiempo = time.time()
    logger.info("Generando detalle por cuadrante-consultor")
    
    # Filtrar solo eventos que están en cuadrantes
    df_en_cuadrantes = df_eventos_con_cuadrantes[
        df_eventos_con_cuadrantes['codigo_cuadrante'].notna()
    ].copy()
    
    if df_en_cuadrantes.empty:
        logger.warning("No hay eventos dentro de cuadrantes")
        return pd.DataFrame(columns=['codigo_cuadrante', 'id_consultor', 'apellido', 
                                   'visitas', 'aperturas', 'ventas', 'total_venta_conIVA'])
    
    # Agregar por cuadrante y consultor - usando nuevos campos unificados
    # Usar 'consultor' si existe, sino usar 'apellido' para compatibilidad
    nombre_col = 'consultor' if 'consultor' in df_en_cuadrantes.columns else 'apellido'
    detalle = df_en_cuadrantes.groupby(['codigo_cuadrante', 'id_consultor', nombre_col]).agg({
        'es_visita': 'sum',                    # COUNT(*) -> visitas
        'apertura': 'sum',                     # SUM(apertura) -> aperturas
        'apertura_sac': 'sum',                 # SUM(apertura_sac) -> aperturas_sac
        'entrega_muestras': 'sum',             # SUM(entrega_muestras) -> muestras
        'venta_ruta': 'sum',                   # SUM(venta_ruta) -> ventas_58
        'venta_fuera_ruta': 'sum'              # SUM(venta_fuera_ruta) -> ventas_fuera
    }).reset_index()
    
    # Renombrar columnas según especificación
    rename_dict = {
        'es_visita': 'visitas',
        'apertura': 'aperturas',
        'apertura_sac': 'aperturas_sac',
        'entrega_muestras': 'muestras',
        'venta_ruta': 'ventas_58',
        'venta_fuera_ruta': 'ventas_fuera'
    }
    # Si usamos 'consultor', renombrarlo a 'apellido' para compatibilidad del popup
    if nombre_col == 'consultor':
        rename_dict['consultor'] = 'apellido'
    
    detalle = detalle.rename(columns=rename_dict)
    
    # Manejar compatibilidad si algunas columnas no existen
    if 'aperturas' not in detalle.columns:
        detalle['aperturas'] = 0
    if 'aperturas_sac' not in detalle.columns:
        detalle['aperturas_sac'] = 0
    if 'muestras' not in detalle.columns:
        detalle['muestras'] = 0
    if 'ventas_58' not in detalle.columns:
        detalle['ventas_58'] = 0
    if 'ventas_fuera' not in detalle.columns:
        detalle['ventas_fuera'] = 0
    
    # Agregar total_venta_conIVA (se actualizará con datos de ventas reales)
    detalle['total_venta_conIVA'] = 0.0
    
    # Agregar alias 'sac' para el popup
    detalle['sac'] = detalle['aperturas_sac']
    
    # Reordenar columnas según especificación (incluyendo 'sac' para el popup)
    columnas_finales = ['codigo_cuadrante', 'id_consultor', 'apellido', 
                       'visitas', 'aperturas', 'aperturas_sac', 'sac', 'muestras', 'ventas_58', 'ventas_fuera', 'total_venta_conIVA']
    detalle = detalle[columnas_finales]
    
    # Validaciones de consistencia
    total_visitas = detalle['visitas'].sum()
    total_aperturas = detalle['aperturas'].sum()
    total_aperturas_sac = detalle['aperturas_sac'].sum()
    total_muestras = detalle['muestras'].sum()
    total_ventas_58 = detalle['ventas_58'].sum()
    total_ventas_fuera = detalle['ventas_fuera'].sum()
    
    tiempo_ejecucion = time.time() - inicio_tiempo
    logger.info(f"Detalle por cuadrante-consultor generado en {tiempo_ejecucion:.2f}s - {len(detalle)} registros")
    logger.info(f"Totales: {total_visitas} visitas, {total_aperturas} aperturas, {total_aperturas_sac} aperturas SAC, {total_muestras} muestras, {total_ventas_58} ventas 58, {total_ventas_fuera} ventas fuera")
    
    return detalle

def actualizar_valores_venta(df_resumen: pd.DataFrame, 
                           df_detalle: pd.DataFrame, 
                           df_ventas_con_coords: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Actualiza los DataFrames de resumen y detalle con valores de venta reales.
    
    Args:
        df_resumen (pd.DataFrame): Resumen por cuadrante
        df_detalle (pd.DataFrame): Detalle por cuadrante-consultor
        df_ventas_con_coords (pd.DataFrame): Ventas con coordenadas y codigo_cuadrante
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Resumen y detalle actualizados con valores de venta
    """
    inicio_tiempo = time.time()
    logger.info("Actualizando valores de venta en resumen y detalle")
    
    if df_ventas_con_coords.empty:
        logger.warning("No hay datos de ventas para actualizar")
        return df_resumen.copy(), df_detalle.copy()
    
    # Realizar point-in-polygon para ventas si no tienen codigo_cuadrante
    # (Asumir que ya viene con codigo_cuadrante del proceso principal)
    
    # Agregar ventas por cuadrante
    ventas_por_cuadrante = df_ventas_con_coords.groupby('codigo_cuadrante').agg({
        'valor_conIVA': 'sum'
    }).reset_index()
    ventas_por_cuadrante = ventas_por_cuadrante.rename(columns={'valor_conIVA': 'total_venta_tot'})
    
    # Actualizar resumen
    df_resumen_actualizado = df_resumen.copy()
    df_resumen_actualizado = df_resumen_actualizado.merge(
        ventas_por_cuadrante, on='codigo_cuadrante', how='left', suffixes=('', '_nuevo')
    )
    df_resumen_actualizado['total_venta_tot'] = df_resumen_actualizado['total_venta_tot_nuevo'].fillna(0)
    df_resumen_actualizado = df_resumen_actualizado.drop(columns=['total_venta_tot_nuevo'])
    
    # Agregar ventas por cuadrante-consultor
    ventas_por_consultor = df_ventas_con_coords.groupby(['codigo_cuadrante', 'id_consultor']).agg({
        'valor_conIVA': 'sum'
    }).reset_index()
    ventas_por_consultor = ventas_por_consultor.rename(columns={'valor_conIVA': 'total_venta_conIVA'})
    
    # Actualizar detalle
    df_detalle_actualizado = df_detalle.copy()
    df_detalle_actualizado = df_detalle_actualizado.merge(
        ventas_por_consultor, on=['codigo_cuadrante', 'id_consultor'], how='left', suffixes=('', '_nuevo')
    )
    df_detalle_actualizado['total_venta_conIVA'] = df_detalle_actualizado['total_venta_conIVA_nuevo'].fillna(0)
    df_detalle_actualizado = df_detalle_actualizado.drop(columns=['total_venta_conIVA_nuevo'])
    
    tiempo_ejecucion = time.time() - inicio_tiempo
    logger.info(f"Valores de venta actualizados en {tiempo_ejecucion:.2f}s")
    
    return df_resumen_actualizado, df_detalle_actualizado

def procesar_consultores_por_cuadrantes(geojson_path: str,
                                       df_eventos: pd.DataFrame,
                                       df_ventas: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Función principal que procesa todos los análisis geoespaciales de consultores por cuadrantes.
    
    Args:
        geojson_path (str): Ruta al archivo GeoJSON de cuadrantes
        df_eventos (pd.DataFrame): DataFrame con eventos de consultores
        df_ventas (pd.DataFrame, optional): DataFrame con ventas con coordenadas
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (resumen_por_cuadrante, detalle_por_cuadrante_consultor)
    
    Raises:
        Exception: Si hay errores en el procesamiento geoespacial
    """
    inicio_tiempo = time.time()
    logger.info("Iniciando procesamiento completo de consultores por cuadrantes")
    
    try:
        # 1. Cargar GeoJSON de cuadrantes
        gdf_cuadrantes = cargar_geojson_cuadrantes(geojson_path)
        
        # 2. Calcular áreas
        gdf_cuadrantes = calcular_areas_cuadrantes(gdf_cuadrantes)
        
        # 3. Point-in-polygon para eventos
        df_eventos_con_cuadrantes = puntos_en_cuadrantes(df_eventos, gdf_cuadrantes)
        
        # 4. Generar resumen por cuadrante
        df_resumen = generar_resumen_por_cuadrante(df_eventos_con_cuadrantes, gdf_cuadrantes)
        
        # 5. Generar detalle por cuadrante-consultor
        df_detalle = generar_detalle_por_cuadrante_consultor(df_eventos_con_cuadrantes)
        
        # 6. Actualizar con valores de venta si están disponibles
        if df_ventas is not None and not df_ventas.empty:
            # Asignar cuadrantes a ventas
            df_ventas_con_cuadrantes = puntos_en_cuadrantes(df_ventas, gdf_cuadrantes)
            # Actualizar valores
            df_resumen, df_detalle = actualizar_valores_venta(df_resumen, df_detalle, df_ventas_con_cuadrantes)
        
        tiempo_ejecucion = time.time() - inicio_tiempo
        logger.info(f"Procesamiento completo terminado en {tiempo_ejecucion:.2f}s")
        logger.info(f"Resultado: {len(df_resumen)} cuadrantes, {len(df_detalle)} registros detalle")
        
        return df_resumen, df_detalle
        
    except Exception as e:
        logger.error(f"Error en procesamiento geoespacial: {str(e)}")
        raise e

def validar_consistencia_datos(df_resumen: pd.DataFrame, df_detalle: pd.DataFrame) -> Dict[str, Any]:
    """
    Valida la consistencia entre resumen y detalle de datos.
    
    Args:
        df_resumen (pd.DataFrame): Resumen por cuadrante
        df_detalle (pd.DataFrame): Detalle por cuadrante-consultor
    
    Returns:
        Dict[str, Any]: Diccionario con resultados de validación
    """
    logger.info("Validando consistencia de datos")
    
    resultados = {
        'valido': True,
        'errores': [],
        'advertencias': [],
        'estadisticas': {}
    }
    
    try:
        # Validar que no haya cuadrantes duplicados en resumen
        duplicados_resumen = df_resumen['codigo_cuadrante'].duplicated().sum()
        if duplicados_resumen > 0:
            resultados['errores'].append(f"Cuadrantes duplicados en resumen: {duplicados_resumen}")
            resultados['valido'] = False
        
        # Validar áreas positivas
        areas_invalidas = (df_resumen['area_m2'] <= 0).sum()
        if areas_invalidas > 0:
            resultados['advertencias'].append(f"Cuadrantes con área inválida: {areas_invalidas}")
        
        # Validar consistencia de totales
        for cuadrante in df_resumen['codigo_cuadrante']:
            detalle_cuadrante = df_detalle[df_detalle['codigo_cuadrante'] == cuadrante]
            resumen_cuadrante = df_resumen[df_resumen['codigo_cuadrante'] == cuadrante].iloc[0]
            
            # Sumar desde detalle
            visitas_detalle = detalle_cuadrante['visitas'].sum()
            aperturas_detalle = detalle_cuadrante['aperturas'].sum()
            ventas_detalle = detalle_cuadrante['ventas'].sum()
            valor_detalle = detalle_cuadrante['total_venta_conIVA'].sum()
            
            # Comparar con resumen
            if visitas_detalle != resumen_cuadrante['visitas_tot']:
                resultados['errores'].append(f"Inconsistencia visitas en {cuadrante}")
                resultados['valido'] = False
            
            if aperturas_detalle != resumen_cuadrante['aperturas_tot']:
                resultados['errores'].append(f"Inconsistencia aperturas en {cuadrante}")
                resultados['valido'] = False
                
            if abs(valor_detalle - resumen_cuadrante['total_venta_tot']) > 0.01:  # Tolerancia para decimales
                resultados['errores'].append(f"Inconsistencia valores en {cuadrante}")
                resultados['valido'] = False
        
        # Estadísticas generales
        resultados['estadisticas'] = {
            'cuadrantes_total': len(df_resumen),
            'registros_detalle': len(df_detalle),
            'visitas_total': df_resumen['visitas_tot'].sum(),
            'aperturas_total': df_resumen['aperturas_tot'].sum(),
            'ventas_total': df_resumen['ventas_tot'].sum(),
            'valor_total': df_resumen['total_venta_tot'].sum(),
            'area_total_m2': df_resumen['area_m2'].sum()
        }
        
        logger.info(f"Validación completada - Válido: {resultados['valido']}")
        if resultados['errores']:
            logger.error(f"Errores encontrados: {len(resultados['errores'])}")
        if resultados['advertencias']:
            logger.warning(f"Advertencias encontradas: {len(resultados['advertencias'])}")
        
        return resultados
        
    except Exception as e:
        logger.error(f"Error en validación: {str(e)}")
        resultados['valido'] = False
        resultados['errores'].append(f"Error de validación: {str(e)}")
        return resultados
