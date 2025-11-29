"""
Módulo "Consultores (Simple)" - Genera mapas básicos con puntos sobre comunas.
Sin cuadrantes, sin métricas globales, solo puntos filtrados por ruta + fechas.
"""

import folium
import json
import unicodedata
import pandas as pd
import os
import logging
from datetime import date
from utils.gestor_mapas import guardar_mapa_controlado
from pre_procesamiento.preprocesamiento_consultores import (
    eventos_con_coordenadas_por_ruta_y_rango,
    get_co
)

# Configurar logging
logger = logging.getLogger(__name__)

def _norm_city(ciudad: str) -> str:
    """Normalizar ciudad removiendo acentos y convirtiendo a mayúsculas."""
    return ''.join(c for c in unicodedata.normalize('NFD', ciudad) if unicodedata.category(c) != 'Mn').upper()

def _coords_and_geojson():
    """Diccionario con coordenadas centrales y paths de GeoJSON de comunas por ciudad."""
    return {
        'CALI': ([3.4516, -76.5320], 'geojson/comunas_cali.geojson'),
        'MEDELLIN': ([6.2442, -75.5812], 'geojson/comunas_medellin.geojson'),
        'MANIZALES': ([5.0672, -75.5174], 'geojson/comunas_manizales.geojson'),
        'PEREIRA': ([4.8087, -75.6906], 'geojson/comunas_pereira.geojson'),
        'BOGOTA': ([4.7110, -74.0721], 'geojson/comunas_bogota.geojson'),
        'BARRANQUILLA': ([10.9720, -74.7962], 'geojson/rutas/barranquilla/cuadrantes_rutas_barranquilla.geojson'),
        'BUCARAMANGA': ([7.1193, -73.1227], 'geojson/comunas_bucaramanga.geojson')
    }

def generar_mapa_consultores_simple(ciudad: str, id_ruta: int, fecha_inicio: date, fecha_fin: date) -> tuple[str, int]:
    """
    Genera un mapa Folium simple con eventos de consultores sobre capa de comunas.
    
    Proceso:
    1) Resuelve centro de operaciones (CO) a partir de 'ciudad'
    2) Convierte fechas date → strings 'YYYY-MM-DD 00:00:00' / '23:59:59'  
    3) Llama a eventos_con_coordenadas_por_ruta_y_rango(CO, id_ruta, f_ini, f_fin)
    4) Crea folium.Map centrado en la ciudad y sobrepone GeoJSON de comunas
    5) Itera df_eventos y dibuja CircleMarker por fila con popup mínimo
    6) Guarda el html en static/maps y retorna (filename, n_puntos)
    
    Args:
        ciudad (str): Nombre de la ciudad
        id_ruta (int): ID de la ruta de cobro  
        fecha_inicio (date): Fecha de inicio
        fecha_fin (date): Fecha de fin
    
    Returns:
        tuple[str, int]: (filename, n_puntos)
        - filename: Nombre del archivo HTML generado
        - n_puntos: Total de eventos renderizados
    
    Raises:
        ValueError: Si la ciudad no es reconocida
        Exception: Si hay errores en la generación del mapa
    """
    try:
        # 1. Normalizar ciudad y validar
        ciudadN = _norm_city(ciudad)
        centers = _coords_and_geojson()
        
        if ciudadN not in centers:
            raise ValueError(f"Ciudad no reconocida: {ciudad}")
        
        location, geojson_path = centers[ciudadN]
        
        # 2. Obtener centroope
        co = get_co(ciudadN)
        
        # 3. Convertir fechas date → strings con horarios completos
        fecha_inicio_str = f"{fecha_inicio.strftime('%Y-%m-%d')} 00:00:00"
        fecha_fin_str = f"{fecha_fin.strftime('%Y-%m-%d')} 23:59:59"
        
        # 4. Consultar eventos con coordenadas
        logger.info(f"Consultando eventos - CO:{co}, Ruta:{id_ruta}, Fechas:{fecha_inicio_str} a {fecha_fin_str}")
        df_eventos = eventos_con_coordenadas_por_ruta_y_rango(co, id_ruta, fecha_inicio_str, fecha_fin_str)
        
        if df_eventos is None or df_eventos.empty:
            logger.warning("No se encontraron eventos para los parámetros especificados")
            df_eventos = pd.DataFrame()
        
        # 5. Crear mapa base centrado en la ciudad
        mapa = folium.Map(location=location, zoom_start=12)
        
        # 6. Cargar y añadir capa de comunas como base geográfica
        geojson_loaded = False
        # Intentar ruta principal primero
        if os.path.exists(geojson_path):
            try:
                with open(geojson_path, 'r', encoding='utf-8') as file:
                    comunas_geojson = json.load(file)
                
                # Añadir capa de comunas con estilo tenue
                folium.GeoJson(
                    data=comunas_geojson,
                    name="Comunas", 
                    style_function=lambda feature: {
                        'fillColor': '#e5e7eb',
                        'color': '#6b7280',
                        'weight': 1,
                        'fillOpacity': 0.12
                    }
                ).add_to(mapa)
                
                logger.info(f"✓ Capa cargada desde: {geojson_path}")
                geojson_loaded = True
            except Exception as e:
                logger.warning(f"No se pudo cargar el archivo GeoJSON desde {geojson_path}: {e}")
        
        # Fallback 1: Para Barranquilla, intentar archivo de comunas genérico
        if not geojson_loaded and ciudadN == 'BARRANQUILLA':
            fallback_comunas = 'geojson/comunas_barranquilla.geojson'
            if os.path.exists(fallback_comunas):
                try:
                    with open(fallback_comunas, 'r', encoding='utf-8') as file:
                        comunas_geojson = json.load(file)
                    
                    folium.GeoJson(
                        data=comunas_geojson,
                        name="Comunas", 
                        style_function=lambda feature: {
                            'fillColor': '#e5e7eb',
                            'color': '#6b7280',
                            'weight': 1,
                            'fillOpacity': 0.12
                        }
                    ).add_to(mapa)
                    
                    logger.info(f"✓ Capa cargada desde fallback (comunas): {fallback_comunas}")
                    geojson_loaded = True
                except Exception as e:
                    logger.warning(f"No se pudo cargar el archivo GeoJSON desde fallback {fallback_comunas}: {e}")
        
        # Fallback 2: ciudades/{CIUDAD}/comunas.geojson
        if not geojson_loaded:
            fallback_path = f"ciudades/{ciudadN}/comunas.geojson"
            if os.path.exists(fallback_path):
                try:
                    with open(fallback_path, 'r', encoding='utf-8') as file:
                        comunas_geojson = json.load(file)
                    
                    folium.GeoJson(
                        data=comunas_geojson,
                        name="Comunas", 
                        style_function=lambda feature: {
                            'fillColor': '#e5e7eb',
                            'color': '#6b7280',
                            'weight': 1,
                            'fillOpacity': 0.12
                        }
                    ).add_to(mapa)
                    
                    logger.info(f"✓ Capa cargada desde fallback (ciudades): {fallback_path}")
                    geojson_loaded = True
                except Exception as e:
                    logger.warning(f"No se pudo cargar el archivo GeoJSON desde fallback {fallback_path}: {e}")
        
        if not geojson_loaded:
            logger.warning(f"⚠️ No se pudo cargar capa de comunas para {ciudadN} - el mapa continuará sin ella")
        
        # 7. Renderizar TODOS los eventos como CircleMarkers
        n_puntos = 0
        if not df_eventos.empty:
            for _, row in df_eventos.iterrows():
                lat = row.get('lat')
                lon = row.get('lon')
                
                if pd.notna(lat) and pd.notna(lon):
                    # Preparar información del popup según especificaciones exactas
                    apellido = row.get('apellido', 'Sin consultor')
                    id_contacto = row.get('id_contacto', 'N/A')
                    fecha_evento = row.get('fecha_evento', 'Sin fecha')
                    tipo_evento = row.get('tipo_evento', '')
                    id_evento_tipo = row.get('id_evento_tipo', '')
                    
                    # Formatear fecha YYYY-MM-DD HH:MM:SS
                    if pd.notna(fecha_evento) and hasattr(fecha_evento, 'strftime'):
                        fecha_str = fecha_evento.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        fecha_str = str(fecha_evento)
                    
                    # Manejar tipo_evento: si aún fuera nula, mostrar "Desconocido (id X)"
                    if pd.isna(tipo_evento) or not tipo_evento or tipo_evento == '':
                        tipo_evento_display = f"Desconocido ({id_evento_tipo})"
                    else:
                        tipo_evento_display = f"{tipo_evento} ({id_evento_tipo})"
                    
                    # Popup según formato exacto especificado
                    popup_text = f"""
                    <div style="font-family: Arial, sans-serif; font-size: 12px;">
                        <b>Consultor:</b> {apellido}<br>
                        <b>ID Contacto:</b> {id_contacto}<br>
                        <b>Fecha:</b> {fecha_str}<br>
                        <b>Tipo de evento:</b> {tipo_evento_display}
                    </div>
                    """
                    
                    # CircleMarker neutral según especificaciones (radio 4-6, gris/azul tenue)
                    folium.CircleMarker(
                        location=[lat, lon],
                        radius=5,  # Radio 4-6 según especificaciones
                        color='#6b7280',  # Color gris tenue
                        fill=True,
                        fillColor='#9ca3af',  # Gris azulado tenue
                        fillOpacity=0.6,
                        weight=1,
                        popup=folium.Popup(popup_text, max_width=280)
                    ).add_to(mapa)
                    
                    n_puntos += 1
        
        # 8. NO añadir métricas globales ni leyendas (según especificaciones)
        # Las secciones de etiquetas flotantes están comentadas para este modo
        
        # 9. Guardar mapa usando el helper central
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_consultores_simple", permitir_multiples=False)
        filepath = f"static/maps/{filename}"
        mapa.save(filepath)
        
        logger.info(f"Mapa consultores simple generado: {filename} con {n_puntos} puntos")
        
        return filename, n_puntos
        
    except ValueError as e:
        logger.error(f"Error de parámetros en mapa consultores simple: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Error generando mapa consultores simple: {str(e)}")
        raise e
