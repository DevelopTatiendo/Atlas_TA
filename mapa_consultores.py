import folium
from folium.plugins import AntPath
from branca.element import MacroElement, Template
import json
import unicodedata
import re
import pandas as pd
import tempfile
import os
import glob
import math
from datetime import datetime
from shapely.geometry import shape, Point
from shapely.ops import unary_union
from shapely.prepared import prep
from utils.gestor_mapas import guardar_mapa_controlado
from pre_procesamiento.preprocesamiento_consultores import (
    eventos_por_ruta_en_rango, 
    get_co,
    eventos_con_coordenadas_por_ruta_y_rango,
    ventas_con_coordenadas_por_ruta_y_rango,
    ventas_totales_por_consultores,
    conteo_eventos_sin_coords_por_consultor,
    eventos_sin_coordenadas_por_ruta_y_rango,
    eventos_tipo20_por_consultor
)
from utils.utilidades_geoespaciales import (
    procesar_consultores_por_cuadrantes,
    validar_consistencia_datos
)
import logging

# Configurar logging
logger = logging.getLogger(__name__)

def _norm_city(ciudad: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', ciudad) if unicodedata.category(c) != 'Mn').upper()

def _norm_token(token: str) -> str:
    """Normalizar token de ruta: sin tildes, espacios → guiones bajos, mayúsculas."""
    if not token:
        return ""
    # Remover tildes
    sin_tildes = ''.join(c for c in unicodedata.normalize('NFD', token) if unicodedata.category(c) != 'Mn')
    # Reemplazar espacios y símbolos con guiones bajos, luego mayúsculas
    normalizado = re.sub(r'[^\w]', '_', sin_tildes).upper()
    return normalizado

def _parse_dt(s):
    """Parse datetime string with multiple format support."""
    for fmt in ("%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def _es_un_solo_dia(fecha_inicio: str, fecha_fin: str) -> bool:
    """Devuelve True si ambas fechas están en el mismo día calendario y el rango es <= 24h."""
    fi = _parse_dt(fecha_inicio)
    ff = _parse_dt(fecha_fin)
    if not fi or not ff:
        return False
    # Si vienen sin hora, asume 00:00:00 y 23:59:59 del mismo día
    if len(fecha_inicio.strip()) == 10:  # "YYYY-MM-DD"
        fi = fi.replace(hour=0, minute=0, second=0)
    if len(fecha_fin.strip()) == 10:
        ff = ff.replace(hour=23, minute=59, second=59)
    return fi.date() == ff.date() and (ff - fi).total_seconds() <= 24*3600 + 60

def _formatear_rango_leyenda(fecha_inicio: str, fecha_fin: str) -> str:
    """Devuelve el texto para leyenda según si es un día o un rango."""
    fi = _parse_dt(fecha_inicio)
    ff = _parse_dt(fecha_fin)
    if not fi or not ff:
        # Fallback defensivo: tomar solo la parte de fecha
        fi_txt = (fecha_inicio or "")[:10]
        ff_txt = (fecha_fin or "")[:10]
        return f"Fecha: {fi_txt}" if fi_txt == ff_txt else f"Fechas: {fi_txt} – {ff_txt}"

    if fi.date() == ff.date():
        return f"Fecha: {fi.strftime('%Y-%m-%d')}"
    else:
        return f"Fechas: {fi.strftime('%Y-%m-%d')} – {ff.strftime('%Y-%m-%d')}"

def _haversine_km(lat1, lon1, lat2, lon2):
    """Distancia geodésica (km) entre dos puntos (WGS84)."""
    R = 6371.0088
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(Δλ/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c



def resolve_route_id(ruta_id_ui, ruta_nombre_ui, ciudad):
    """
    Resuelve el id_ruta real basado en la entrada de la UI.
    
    Args:
        ruta_id_ui: ID de ruta desde UI (puede ser int, str o None)
        ruta_nombre_ui: Nombre de ruta desde UI (str)
        ciudad: Nombre de la ciudad (str)
        
    Returns:
        tuple: (id_ruta_real: int, nombre_ruta_resuelto: str)
        
    Raises:
        ValueError: Si no se puede resolver la ruta
        
    Examples:
        resolve_route_id(None, "ruta 7", "Cali") -> (13, "ruta 7")
        resolve_route_id(780, "ruta 16 Palmira", "Cali") -> (780, "ruta 16 Palmira")
    """
    # Normalizar ciudad
    ciudadN = _norm_city(ciudad)
    
    try:
        # Si tenemos ID directo de UI, validarlo contra BD
        if ruta_id_ui is not None:
            id_ruta_candidato = int(ruta_id_ui)
            co = get_co(ciudadN)
            
            # Validar que existe en BD
            from pre_procesamiento.preprocesamiento_consultores import nombre_ruta
            nombre_bd = nombre_ruta(co, id_ruta_candidato)
            
            if nombre_bd is not None:
                return id_ruta_candidato, ruta_nombre_ui or nombre_bd
            else:
                logger.warning(f"ID de ruta {id_ruta_candidato} no encontrado en BD para {ciudadN}")
        
        # Si no hay ID directo o no se encontró, resolver por nombre
        if ruta_nombre_ui:
            co = get_co(ciudadN)
            from pre_procesamiento.preprocesamiento_consultores import listar_rutas_simple
            df_rutas = listar_rutas_simple(ciudad)
            
            if df_rutas.empty:
                raise ValueError(f"No hay rutas disponibles para {ciudadN}")
            
            # Mapeo específico para casos conocidos
            mapeo_especifico = {
                "ruta 7": 13,
                "ruta 16 palmira": 780,
                # Agregar más mapeos según se necesiten
            }
            
            nombre_normalizado = ruta_nombre_ui.lower().strip()
            
            # Intentar mapeo específico primero
            if nombre_normalizado in mapeo_especifico:
                id_ruta_resuelto = mapeo_especifico[nombre_normalizado]
                # Validar que existe
                nombre_bd = nombre_ruta(co, id_ruta_resuelto)
                if nombre_bd is not None:
                    return id_ruta_resuelto, ruta_nombre_ui
            
            # Buscar por coincidencia parcial en BD
            for _, row in df_rutas.iterrows():
                nombre_bd = str(row['ruta']).lower().strip()
                if nombre_normalizado in nombre_bd or nombre_bd in nombre_normalizado:
                    return int(row['id_ruta']), ruta_nombre_ui
            
            # Si no encontramos coincidencia, intentar extraer número
            import re
            match = re.search(r'\b(\d+)\b', ruta_nombre_ui)
            if match:
                numero_ruta = int(match.group(1))
                # Buscar ruta que contenga ese número
                for _, row in df_rutas.iterrows():
                    if str(numero_ruta) in str(row['ruta']):
                        return int(row['id_ruta']), ruta_nombre_ui
        
        # Si llegamos aquí, no pudimos resolver
        raise ValueError(f"No se pudo resolver ruta: id_ui={ruta_id_ui}, nombre_ui='{ruta_nombre_ui}' para {ciudadN}")
        
    except Exception as e:
        logger.error(f"Error resolviendo ruta: {e}")
        raise ValueError(f"Error resolviendo ruta: {e}")

def _coords_and_geojson():
    return {
        'CALI': ([3.4516, -76.5320], 'geojson/comunas_cali.geojson'),
        'MEDELLIN': ([6.2442, -75.5812], 'geojson/comunas_medellin.geojson'),
        'MANIZALES': ([5.0672, -75.5174], 'geojson/comunas_manizales.geojson'),
        'PEREIRA': ([4.8087, -75.6906], 'geojson/comunas_pereira.geojson'),
        'BOGOTA': ([4.7110, -74.0721], 'geojson/comunas_bogota.geojson'),
        'BARRANQUILLA': ([10.9720, -74.7962], 'geojson/comunas_barranquilla.geojson'),
        'BUCARAMANGA': ([7.1193, -73.1227], 'geojson/comunas_bucaramanga.geojson')
    }

def _es_cuadrante(feature):
    """Determina si un feature es un cuadrante válido (padre o hijo)."""
    return _es_cuadrante_padre(feature) or _es_cuadrante_hijo(feature)

def _style_cuadrante(feature):
    """Estilo para cuadrantes basado en properties del GeoJSON."""
    p = feature.get('properties', {})
    return {
        'fillColor': p.get('fillColor', '#ffd24d'),
        'color': p.get('color', '#111111'),
        'weight': p.get('weight', 1),
        'fillOpacity': p.get('fillOpacity', 0.35),
    }

def _style_no_cuadrante(_):
    """Estilo para features que no son cuadrantes (contorno transparente)."""
    return {
        'fillColor': 'transparent',
        'color': '#000000',
        'weight': 0.8,
        'fillOpacity': 0.0
    }

def _generar_popup_cuadrante(codigo_cuadrante: str, df_resumen: pd.DataFrame, df_detalle: pd.DataFrame) -> str:
    """
    Genera el HTML del popup para un cuadrante específico.
    
    Args:
        codigo_cuadrante (str): Código del cuadrante
        df_resumen (pd.DataFrame): DataFrame de resumen por cuadrante
        df_detalle (pd.DataFrame): DataFrame de detalle por cuadrante-consultor
        
    Returns:
        str: HTML del popup
    """
    # Verificar que los DataFrames no estén vacíos
    if df_resumen.empty:
        return f"<b>{codigo_cuadrante}</b><br>Sin datos disponibles"
    
    # Buscar datos de resumen para este cuadrante
    resumen_cuadrante = df_resumen[df_resumen['codigo_cuadrante'] == codigo_cuadrante]
    if resumen_cuadrante.empty:
        return f"<b>{codigo_cuadrante}</b><br>Sin datos disponibles"
    
    # Obtener datos del cuadrante
    row = resumen_cuadrante.iloc[0]
    area_m2 = float(row.get('area_m2', 0))
    visitas_tot = float(row.get('visitas_tot', 0))
    
    # Obtener visitas_por_m2 con fallback para compatibilidad
    if 'visitas_por_m2' in row.index:
        visitas_por_m2 = float(row.get('visitas_por_m2', 0))
    else:
        # Fallback: calcular visitas_por_m2 = visitas_tot / area_m2
        visitas_por_m2 = visitas_tot / area_m2 if area_m2 > 0 else 0.0
    
    # Calcular valores derivados para mostrar en ambas unidades
    if area_m2 > 0:
        area_km2 = area_m2 / 1_000_000  # Convertir m² a km²
        visitas_por_km2 = visitas_por_m2 * 1_000_000  # Convertir visitas/m² a visitas/km²
        
        # Formatear valores con los formatos especificados
        area_m2_fmt = f"{area_m2:,.0f}"  # Área en m² con separador de miles, 0 decimales
        area_km2_fmt = f"{area_km2:.2f}"  # Área en km² con 2 decimales
        visitas_m2_fmt = f"{visitas_por_m2:.6f}"  # Visitas/m² con 6 decimales
        visitas_km2_fmt = f"{visitas_por_km2:.1f}"  # Visitas/km² con 1 decimal
        
        area_texto = f"{area_m2_fmt} m² ({area_km2_fmt} km²)"
        densidad_texto = f"{visitas_m2_fmt} (≈ {visitas_km2_fmt} visitas/km²)"
    else:
        # Si area_m2 no está o es 0, mostrar s/d
        area_texto = "s/d"
        densidad_texto = "s/d"
    
    # Obtener detalles por consultor para este cuadrante
    if df_detalle.empty:
        detalle_cuadrante = pd.DataFrame()
    else:
        detalle_cuadrante = df_detalle[df_detalle['codigo_cuadrante'] == codigo_cuadrante].copy()
    
    # Ordenar por visitas descendente
    if not detalle_cuadrante.empty:
        detalle_cuadrante = detalle_cuadrante.sort_values('visitas', ascending=False)
    
    # Construir HTML del popup
    html = f"""
    <div style="
      font-family: Arial, sans-serif;
      max-width: 600px;     /* más ancho para evitar scroll */
      min-width: 600px;     /* fija ancho estable */
      max-height: 560px;    /* más alto */
      overflow-x: hidden;   /* sin scroll horizontal */
      overflow-y: auto;     /* solo vertical si hace falta */
    ">
        <h4 style="margin: 0 0 8px 0; color: #2563eb;">{codigo_cuadrante}</h4>
        <p style="margin: 0 0 12px 0; font-size: 12px; color: #6b7280;">
            <b>Área:</b> {area_texto} | <b>Visitas/m²:</b> {densidad_texto}
        </p>
    """
    
    # Agregar tabla de consultores si hay datos
    if not detalle_cuadrante.empty:
        html += """
        <table style="width: 100%; border-collapse: collapse; font-size: 12px; table-layout: auto;">
            <colgroup>
              <col span="7">
              <col style="width: auto;">
            </colgroup>
            <thead>
                <tr style="background: #f3f4f6; text-align: left;">
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Consultor</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Visitas</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Aperturas</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">SAC</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Muestras</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Venta en ruta</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Venta no ruta</th>
                    <th style="padding: 4px 6px; border: 1px solid #d1d5db;">Total venta</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for _, det_row in detalle_cuadrante.iterrows():
            apellido = str(det_row.get('apellido', 'N/A'))
            visitas = int(float(det_row.get('visitas', 0)))
            aperturas = int(float(det_row.get('aperturas', 0)))
            # Nuevos campos con compatibilidad - usar 0 si no existen
            sac = int(float(det_row.get('sac', 0)))
            muestras = int(float(det_row.get('muestras', 0)))
            ventas_58 = int(float(det_row.get('ventas_58', 0)))
            ventas_fuera = int(float(det_row.get('ventas_fuera', 0)))
            total_venta = float(det_row.get('total_venta_conIVA', 0))
            
            html += f"""
                <tr>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db;">{apellido}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{visitas}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{aperturas}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{sac}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{muestras}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{ventas_58}</td>
                    <td style="padding: 3px 6px; border: 1px solid #d1d5db; text-align: center;">{ventas_fuera}</td>
                    <td style="text-align: right; white-space: nowrap;">${total_venta:,.0f}</td>
                </tr>
            """
        
        html += """
            </tbody>
        </table>
        """
    else:
        html += "<p style='color: #6b7280; font-size: 11px; font-style: italic;'>Sin actividad de consultores en este período</p>"
    
    html += "</div>"
    
    return html

def filter_features_by_route(feature_collection: dict, id_ruta_real: int, mostrar_todos: bool = False) -> tuple:
    """
    Filtra features del FeatureCollection por ruta específica.
    
    Args:
        feature_collection (dict): FeatureCollection completo
        id_ruta_real (int): ID real de la ruta a filtrar
        mostrar_todos (bool): Si True, omite filtrado y devuelve todo
        
    Returns:
        tuple: (padres_ruta: list, hijos_ruta: list, subset_fc: dict)
        - padres_ruta: Lista de features padre de la ruta
        - hijos_ruta: Lista de features hijo de la ruta  
        - subset_fc: FeatureCollection con solo features de la ruta
    """
    if mostrar_todos:
        # Caso "TODOS": devolver todo sin filtrar
        features = feature_collection.get('features', [])
        padres = [f for f in features if _es_cuadrante_padre(f)]
        hijos = [f for f in features if _es_cuadrante_hijo(f)]
        return padres, hijos, feature_collection
    
    padres_ruta = []
    hijos_ruta = []
    
    for feature in feature_collection.get('features', []):
        props = feature.get('properties', {})
        
        # Método 1: Usar propiedades explícitas (preferido)
        if 'id_ruta' in props and 'nivel' in props:
            feature_id_ruta = props.get('id_ruta')
            nivel = props.get('nivel', '').lower()
            
            # Convertir id_ruta a int para comparación
            try:
                feature_id_ruta = int(feature_id_ruta)
            except (ValueError, TypeError):
                continue
                
            if feature_id_ruta == id_ruta_real:
                if nivel == 'cuadrante':
                    padres_ruta.append(feature)
                elif nivel == 'subcuadrante':
                    hijos_ruta.append(feature)
        
        # Método 2: Fallback usando patrones de código
        else:
            codigo = props.get('codigo', '').upper()
            if not codigo:
                continue
                
            # Detectar padres: CL_{id_ruta}_00, CL_{id_ruta}_00A, etc.
            padre_pattern = rf'^CL_{id_ruta_real}_00[A-Z]*$'
            if re.match(padre_pattern, codigo):
                padres_ruta.append(feature)
                continue
            
            # Detectar hijos: CL_{id_ruta}_XX donde XX != 00
            hijo_pattern = rf'^CL_{id_ruta_real}_(\d{{2}}[A-Z]*)$'
            match = re.match(hijo_pattern, codigo)
            if match:
                sufijo = match.group(1)
                # Excluir 00 y variantes (son padres)
                if not sufijo.startswith('00'):
                    hijos_ruta.append(feature)
                    continue
            
            # Detectar hijos por codigo_padre
            codigo_padre = props.get('codigo_padre', '')
            if codigo_padre:
                # Verificar si codigo_padre pertenece a algún padre de esta ruta
                padre_pattern_check = rf'^CL_{id_ruta_real}_00[A-Z]*$'
                if re.match(padre_pattern_check, codigo_padre.upper()):
                    hijos_ruta.append(feature)
    
    # Crear FeatureCollection filtrado
    features_filtradas = padres_ruta + hijos_ruta
    subset_fc = {
        'type': 'FeatureCollection',
        'features': features_filtradas
    }
    
    logger.info(f"Filtrado por ruta {id_ruta_real}: {len(padres_ruta)} padres, {len(hijos_ruta)} hijos")
    
    return padres_ruta, hijos_ruta, subset_fc

def _es_cuadrante_padre(feature: dict) -> bool:
    """
    Detecta si una feature es un cuadrante padre.
    Soporta tanto el formato estándar (nivel='PADRE') como el formato legacy (nivel='cuadrante').
    """
    props = feature.get('properties', {})
    
    # Normalizar strings para robustez
    def safe_str(value):
        return str(value or "").strip().lower()
    
    # Método 1: Por nivel explícito (estándar y legacy)
    nivel = safe_str(props.get('nivel'))
    if nivel in {'cuadrante', 'padre'}:
        return True
    
    # Método 2: Por flag es_hijo (formato estándar)
    es_hijo = props.get('es_hijo')
    if es_hijo is False or (isinstance(es_hijo, str) and safe_str(es_hijo) == 'false'):
        return True
    
    # Método 3: Por patrón de código general con prefijos de ciudad
    codigo = str(props.get('codigo', '')).strip().upper()
    if codigo:
        # Patrón estándar nuevo: PREFIJO_###
        if re.match(r'^[A-Z]{2}_[0-9]{1,3}$', codigo):
            return True
        # Patrón legacy: CL_X_00, CL_X_00A, etc.
        if re.match(r'^[A-Z]{2}_[0-9]+_00[A-Z]*$', codigo):
            return True
    
    return False

def _es_cuadrante_hijo(feature: dict) -> bool:
    """
    Detecta si una feature es un subcuadrante hijo.
    Soporta tanto el formato estándar como el formato legacy.
    """
    props = feature.get('properties', {})
    
    # Normalizar strings para robustez
    def safe_str(value):
        return str(value or "").strip().lower()
    
    # Método 1: Por nivel explícito (estándar y legacy)
    nivel = safe_str(props.get('nivel'))
    if nivel in {'subcuadrante', 'hijo'}:
        return True
    
    # Método 2: Por flag es_hijo (formato estándar)
    es_hijo = props.get('es_hijo')
    if es_hijo is True or (isinstance(es_hijo, str) and safe_str(es_hijo) == 'true'):
        return True
    
    # Método 3: Por codigo_padre existente (cualquier prefijo de ciudad)
    codigo_padre = str(props.get('codigo_padre', '')).strip()
    if codigo_padre and re.match(r'^[A-Z]{2}_', codigo_padre.upper()):
        return True
    
    # Método 4: Por patrón de código legacy
    codigo = str(props.get('codigo', '')).strip().upper()
    if codigo:
        # Patrón legacy: PREFIJO_X_YY donde YY != 00
        match = re.match(r'^([A-Z]{2})_([0-9]+)_([0-9]{2}[A-Z]*)$', codigo)
        if match:
            prefijo, ruta, sufijo = match.groups()
            if not sufijo.startswith('00'):
                return True
    
    return False

def _cargar_geojson_ciudad_unico(ciudad: str) -> dict:
    """
    Carga el archivo GeoJSON único para la ciudad especificada.
    
    Args:
        ciudad (str): Nombre de la ciudad
        
    Returns:
        dict: FeatureCollection del archivo único
        
    Raises:
        FileNotFoundError: Si no se encuentra el archivo para la ciudad
        Exception: Si hay error leyendo el archivo
    """
    ciudadN = _norm_city(ciudad)
    ciudad_slug = ciudadN.lower()
    
    # Construir ruta del archivo único
    filename = f"cuadrantes_rutas_{ciudad_slug}.geojson"
    path_rel = os.path.join("geojson", "rutas", ciudad_slug, filename)
    
    # También intentar ruta absoluta como fallback
    base_abs = os.path.join(os.getcwd(), "geojson", "rutas", ciudad_slug)
    path_abs = os.path.join(base_abs, filename)
    
    for path in (path_rel, path_abs):
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    geojson_data = json.load(f)
                logger.info(f"✓ Archivo único cargado: {path}")
                return geojson_data
            except Exception as e:
                logger.error(f"Error leyendo {path}: {e}")
                continue
    
    # Si no encontramos el archivo, error claro
    error_msg = f"No se encontró el archivo GeoJSON único para {ciudad}: {path_rel}"
    logger.error(error_msg)
    raise FileNotFoundError(error_msg)



def generar_mapa_consultores(fecha_inicio, fecha_fin, ciudad, ruta_id, ruta_nombre, mostrar_fuera: bool = False):
    """
    Genera mapa de consultores usando archivo único por ciudad y filtrado por ruta.
    
    Args:
        fecha_inicio: string formato 'YYYY-MM-DD HH:MM:SS'
        fecha_fin: string formato 'YYYY-MM-DD HH:MM:SS'
        ciudad: nombre de la ciudad
        ruta_id: id_ruta numérico (desde UI)
        ruta_nombre: nombre de la ruta (string mostrado en UI)
        mostrar_fuera: bool para mostrar puntos fuera de cuadrantes en rojo (default: False)
    """
    # 1) Normalizar ciudad y resolver centro
    ciudadN = _norm_city(ciudad)
    centers = _coords_and_geojson()
    if ciudadN not in centers:
        return None, 0, pd.DataFrame()
    location, comunas_geojson_path = centers[ciudadN]
    
    # 2) Resolver ID de ruta real
    try:
        id_ruta_real, nombre_ruta_resuelto = resolve_route_id(ruta_id, ruta_nombre, ciudad)
        logger.info(f"Ruta resuelta: {ruta_nombre} -> ID {id_ruta_real}")
    except ValueError as e:
        logger.error(f"Error resolviendo ruta: {e}")
        return None, 0, pd.DataFrame()
    
    # 3) Intentar cargar archivo GeoJSON de cuadrantes (opcional - no bloqueante)
    geojson_completo = None
    try:
        geojson_completo = _cargar_geojson_ciudad_unico(ciudad)
        logger.info(f"Cuadrantes cargados para {ciudad}")
    except FileNotFoundError as e:
        logger.warning(f"No hay cuadrantes disponibles para {ciudad}: {e}")
        # Continuar sin cuadrantes
    except Exception as e:
        logger.warning(f"Error cargando cuadrantes (no bloqueante): {e}")
        # Continuar sin cuadrantes
    
    # 4) Filtrar features por ruta específica (solo si hay cuadrantes)
    padres_ruta = []
    hijos_ruta = []
    subset_fc = {'type': 'FeatureCollection', 'features': []}
    
    if geojson_completo:
        mostrar_todos = (ruta_nombre and ruta_nombre.strip().upper() == "TODOS")
        padres_ruta, hijos_ruta, subset_fc = filter_features_by_route(
            geojson_completo, id_ruta_real, mostrar_todos
        )
        
        if not padres_ruta and not hijos_ruta:
            logger.warning(f"No hay cuadrantes para la ruta {id_ruta_real} en el archivo")
            # Continuar sin cuadrantes
    
    # 5) Obtener datos de eventos usando ID real
    co = get_co(ciudadN)
    df_eventos = eventos_con_coordenadas_por_ruta_y_rango(co, id_ruta_real, fecha_inicio, fecha_fin)
    logger.info(f"Recorridos: columnas df_eventos = {list(df_eventos.columns)}")
    total_eventos = len(df_eventos) if df_eventos is not None else 0
    
    # 5.1) Calcular totales globales de ventas por consultor para popups padre
    from pre_procesamiento.preprocesamiento_consultores import ventas_totales_por_consultores
    
    ids_evento = sorted(df_eventos['id_autor'].dropna().astype(int).unique().tolist()) if df_eventos is not None and not df_eventos.empty else []
    df_totales = ventas_totales_por_consultores(fecha_inicio, fecha_fin, ids_evento)
    mapa_totales = {int(r.id_consultor): float(r.total_venta_conIVA) for _, r in df_totales.iterrows()}  # dict
    logger.info(f"Totales globales calculados para {len(mapa_totales)} consultores")
    
    # 5.2) Obtener contadores sin coordenadas para popup padre
    from pre_procesamiento.preprocesamiento_consultores import conteo_eventos_sin_coords_por_consultor
    df_agg_sin = conteo_eventos_sin_coords_por_consultor(co, id_ruta_real, fecha_inicio, fecha_fin)
    mapa_agg_sin = {int(r.id_consultor): r for _, r in df_agg_sin.iterrows()}  # dict de filas completas
    logger.info(f"Contadores sin coordenadas calculados para {len(mapa_agg_sin)} consultores")
    
    # 6) Obtener DataFrames de agregación usando el subconjunto filtrado
    df_resumen = pd.DataFrame()
    df_detalle = pd.DataFrame()
    
    if subset_fc.get('features'):
        try:
            # Guardar subconjunto temporalmente para análisis
            with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as temp_file:
                json.dump(subset_fc, temp_file)
                geojson_path = temp_file.name
            
            df_resumen = obtener_resumen_cuadrantes_consultores(fecha_inicio, fecha_fin, ciudad, id_ruta_real, geojson_path)
            df_detalle = obtener_detalle_cuadrantes_consultores(fecha_inicio, fecha_fin, ciudad, id_ruta_real, geojson_path)
            
            # Limpiar archivo temporal
            try:
                os.unlink(geojson_path)
            except:
                pass
                
            logger.info(f"Datos agregación obtenidos: {len(df_resumen)} cuadrantes, {len(df_detalle)} detalles")
            
        except Exception as e:
            logger.warning(f"No se pudieron obtener datos de agregación para popups: {e}")
    
    # 7) Crear mapa base
    mapa = folium.Map(location, zoom_start=12)
    
    # 7.1) Cargar y añadir capa de comunas como base geográfica (SIEMPRE, independiente de cuadrantes)
    if os.path.exists(comunas_geojson_path):
        try:
            with open(comunas_geojson_path, 'r', encoding='utf-8') as file:
                comunas_geojson = json.load(file)
            
            # Añadir capa de comunas con estilo tenue
            folium.GeoJson(
                data=comunas_geojson,
                name="Comunas",
                style_function=lambda feature: {
                    'fillColor': '#e5e7eb',
                    'color': '#6b7280',
                    'weight': 1,
                    'fillOpacity': 0.08
                }
            ).add_to(mapa)
            
            logger.info(f"Capa de comunas cargada desde {comunas_geojson_path}")
        except Exception as e:
            logger.warning(f"No se pudo cargar el archivo GeoJSON de comunas: {e}")
    else:
        logger.warning(f"Archivo GeoJSON de comunas no encontrado: {comunas_geojson_path}")
    
    # 8) Dibujar solo features del subconjunto filtrado (cuadrantes - opcional)
    if subset_fc.get('features'):
        fg_contorno = folium.FeatureGroup(name="Contorno", show=True, control=False)
        fg_cuadrantes = folium.FeatureGroup(name="Cuadrantes", show=True)
        
        for feat in subset_fc['features']:
            # Verificar si es cuadrante (padre o hijo)
            if _es_cuadrante_padre(feat) or _es_cuadrante_hijo(feat):
                # Obtener código del cuadrante para el popup
                codigo_cuadrante = feat.get('properties', {}).get('codigo', '')
                
                # Generar popup con datos de agregación
                if not df_resumen.empty and not df_detalle.empty:
                    # Diferenciar entre cuadrante padre e hijo
                    if _es_cuadrante_padre(feat):
                        # PADRE: usar totales globales en lugar de geolocalizados
                        det_padre = df_detalle[df_detalle['codigo_cuadrante'] == codigo_cuadrante].copy()
                        if not det_padre.empty:
                            # Sobrescribir con totales globales (ventas desde pedidos)
                            det_padre['total_venta_conIVA'] = det_padre['id_consultor'].map(mapa_totales).fillna(0.0)
                            
                            # Sobrescribir contadores con datos sin coordenadas
                            def aplicar_contadores_sin_coords(row):
                                id_cons = int(row['id_consultor'])
                                if id_cons in mapa_agg_sin:
                                    agg_row = mapa_agg_sin[id_cons]
                                    row['visitas'] = int(agg_row.cant_visitas)
                                    row['aperturas'] = int(agg_row.cant_aperturas)
                                    row['sac'] = int(agg_row.cant_sac)
                                    row['ventas_58'] = int(agg_row.cant_venta_ruta)
                                    row['ventas_fuera'] = int(agg_row.cant_venta_no_ruta)
                                return row
                            
                            det_padre = det_padre.apply(aplicar_contadores_sin_coords, axis=1)
                        
                        popup_html = _generar_popup_cuadrante(codigo_cuadrante, df_resumen, det_padre)
                    else:
                        # HIJO: mantener venta geolocalizada (sin cambios)
                        popup_html = _generar_popup_cuadrante(codigo_cuadrante, df_resumen, df_detalle)
                    
                    popup = folium.Popup(popup_html, max_width=600)
                else:
                    # Popup básico si no hay datos de agregación
                    popup_html = f"<b>{codigo_cuadrante}</b><br>Datos de agregación no disponibles"
                    popup = folium.Popup(popup_html, max_width=300)
                
                # Tooltip para mostrar código al hover
                tooltip = folium.Tooltip(f"<b>{codigo_cuadrante}</b>")
                
                folium.GeoJson(
                    data=feat,
                    style_function=_style_cuadrante,
                    popup=popup,
                    tooltip=tooltip
                ).add_to(fg_cuadrantes)
            else:
                # Features no-cuadrante: solo contorno transparente
                folium.GeoJson(
                    data=feat,
                    style_function=_style_no_cuadrante,
                    popup=False,
                    tooltip=False
                ).add_to(fg_contorno)
        
        # ORDEN: contorno primero, cuadrantes después
        fg_contorno.add_to(mapa)
        fg_cuadrantes.add_to(mapa)
    else:
        # Mensaje si no hay features para la ruta
        info_html = f"""
        <div style="position: fixed; 
                    top: 10px; right: 10px; width: 350px; height: 90px; 
                    background-color: rgba(255, 255, 255, 0.9); z-index:9999; 
                    font-size:14px; padding: 10px; border: 2px solid orange; border-radius: 5px;">
            <b>⚠️ No hay cuadrantes para la ruta seleccionada</b><br>
            <small>Ruta: {nombre_ruta_resuelto} (ID: {id_ruta_real})<br>
            El mapa mostrará solo los puntos de eventos.</small>
        </div>
        """
        mapa.get_root().html.add_child(folium.Element(info_html))

    # 9) Filtro espacial de eventos usando geometría del subconjunto (SOLO si existen cuadrantes)
    df_filtrados = pd.DataFrame()
    mask_in = None  # Para tracking de puntos dentro/fuera
    
    # Determinar qué eventos pintar: TODOS si no hay cuadrantes, filtrados si hay
    if df_eventos is not None and not df_eventos.empty:
        if padres_ruta or hijos_ruta:
            # HAY cuadrantes: aplicar filtro espacial
            features_para_filtro = hijos_ruta if hijos_ruta else padres_ruta
            
            polygons = []
            for feat in features_para_filtro:
                try:
                    geom = shape(feat['geometry'])
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                    polygons.append(geom)
                except Exception:
                    continue
            
            if polygons:
                # Crear unión preparada para consultas rápidas
                union_geom = unary_union(polygons)
                if not union_geom.is_valid:
                    union_geom = union_geom.buffer(0)
                prepped_union = prep(union_geom)
                
                # Filtrar eventos que caen dentro de los polígonos
                def punto_dentro(row):
                    try:
                        point = Point(float(row['lon']), float(row['lat']))
                        return prepped_union.intersects(point)
                    except:
                        return False
                
                mask_in = df_eventos.apply(punto_dentro, axis=1)
                df_filtrados = df_eventos[mask_in].reset_index(drop=True)
            else:
                # No se pudieron procesar polígonos: usar todos los eventos
                df_filtrados = df_eventos.copy()
        else:
            # NO HAY cuadrantes: pintar TODOS los eventos
            logger.info("No hay cuadrantes - pintando TODOS los eventos")
            df_filtrados = df_eventos.copy()

    def _color_evento(row, fuera=False):
        """
        Retorna el color del punto según el tipo de evento y si está fuera de cuadrante.
        Regla nueva: id_evento_tipo en [58,57] -> VERDE (tanto dentro como fuera).
        """
        try:
            tipo = int(row.get('id_evento_tipo'))
        except Exception:
            tipo = None

        # 58, 57 = Ventas (verde)
        if tipo in [58, 57]:
            return "#16a34a"  # green-600

        # Si no es 58: mantener comportamiento actual
        if fuera:
            return "#B91C1C"  # rojo para puntos fuera
        return "#374151"      # gris oscuro para puntos dentro

    # 10) Pintar eventos (TODOS si no hay cuadrantes, filtrados si hay)
    if df_filtrados is not None and not df_filtrados.empty:
        for _, r in df_filtrados.iterrows():
            lat, lon = float(r.lat), float(r.lon)
            popup = folium.Popup(
                f"<b>Evento:</b> {r.id_evento}<br><b>Contacto:</b> {r.id_contacto}<br><b>Fecha:</b> {r.fecha_evento}",
                max_width=300
            )
            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color=_color_evento(r),
                fill=True,
                fillColor=_color_evento(r),
                fillOpacity=0.7,
                popup=popup
            ).add_to(mapa)

        # 10.1) Pintar eventos fuera en rojo (si mostrar_fuera=True)
        if mostrar_fuera and df_eventos is not None and not df_eventos.empty and (padres_ruta or hijos_ruta):
            df_fuera = df_eventos[~mask_in].reset_index(drop=True)
            
            for _, r in df_fuera.iterrows():
                lat, lon = float(r.lat), float(r.lon)
                popup = folium.Popup(
                    f"<b>Evento FUERA:</b> {r.id_evento}<br><b>Contacto:</b> {r.id_contacto}<br><b>Fecha:</b> {r.fecha_evento}",
                    max_width=300
                )
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,
                    color=_color_evento(r, fuera=True),
                    fill=True,
                    fillColor=_color_evento(r, fuera=True),
                    fillOpacity=0.85,
                    popup=popup
                ).add_to(mapa)

        # 11) Fit bounds a los puntos filtrados
        try:
            coords = [[float(r.lat), float(r.lon)] for _, r in df_filtrados.iterrows()]
            if coords:
                mapa.fit_bounds(coords)
        except Exception:
            pass

    def _pick_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _ensure_lat_lon(df):
        # normaliza nombres típicos
        rename_map = {}
        for c in df.columns:
            lc = c.lower()
            if lc in ("lat", "latitude", "latitud"):
                rename_map[c] = "lat"
            if lc in ("lon", "lng", "long", "longitud", "longitude"):
                rename_map[c] = "lon"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    # --- Recorridos (solo cuando es un solo día) ---
    try:
        # "Un solo día" si la parte de fecha (YYYY-MM-DD) coincide
        un_solo_dia = fecha_inicio[:10] == fecha_fin[:10]
        cols_ok = {'id_consultor', 'apellido', 'fecha_evento', 'lat', 'lon'}.issubset(df_eventos.columns)

        # Variables para leyenda (se usarán si realmente trazamos recorridos)
        rec_stats = {
            "tracks": 0,
            "points": 0,
            "km_total": 0.0,
            "hora_ini": None,
            "hora_fin": None,
        }

        if un_solo_dia and cols_ok:
            fg_paths = folium.FeatureGroup(name="Recorridos (1 día)", show=True)

            # Usar los mismos puntos que ya estamos mostrando (filtrados por cuadrantes si existen)
            df_rutas = df_filtrados if (df_filtrados is not None and not df_filtrados.empty) else df_eventos
            if not df_rutas.empty:
                # Asegurar datetime
                df_rutas = df_rutas.copy()
                df_rutas['fecha_evento'] = pd.to_datetime(df_rutas['fecha_evento'], errors='coerce')
                df_rutas = df_rutas.dropna(subset=['fecha_evento'])

                # Stats globales de ventana temporal
                rec_stats["hora_ini"] = df_rutas['fecha_evento'].min()
                rec_stats["hora_fin"]  = df_rutas['fecha_evento'].max()

                # Orden temporal
                df_rutas = df_rutas.sort_values('fecha_evento')

                for id_cons, g in df_rutas.groupby('id_consultor'):
                    if len(g) < 2:
                        continue

                    # Coordenadas en orden temporal
                    coords = g[['lat','lon']].astype(float).values.tolist()

                    # Distancia del consultor
                    dist_km = 0.0
                    for i in range(len(coords)-1):
                        lat1, lon1 = coords[i]
                        lat2, lon2 = coords[i+1]
                        dist_km += _haversine_km(lat1, lon1, lat2, lon2)

                    # Trazo
                    AntPath(
                        locations=coords,
                        color="#000000",        # antes "#111111"
                        weight=4,               # antes 3
                        delay=600,              # antes 800 (más fluido)
                        dash_array=[8, 12],     # antes [12, 18] (más definido en zooms bajos)
                        pulse_color="#00FFFF"   # antes "#2563eb"
                    ).add_to(fg_paths)

                    # Acumular métricas
                    rec_stats["tracks"]  += 1
                    rec_stats["points"]  += len(coords)
                    rec_stats["km_total"] += dist_km

            fg_paths.add_to(mapa)
        else:
            logger.warning("Recorridos: no es un solo día o faltan columnas; no se trazan recorridos.")
    except Exception as e:
        logger.error(f"Recorridos: error trazando rutas: {e}")

    # 12) Leyenda se calculará más adelante después de la deduplicación del CSV

    _css = """
{% macro html(this, kwargs) %}
<style>
/* --- Leyenda compacta (tarjeta) — fija al viewport --- */
.legend-consultores{
  position: fixed;          /* fijo al viewport para que no "empuje" el mapa */
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  z-index: 1000;

  /* tamaño y estética de tarjeta */
  max-width: 260px;         /* tamaño compacto como antes */
  background: #ffffff;
  padding: 10px 12px;
  border-radius: 8px;
  box-shadow: 0 1px 6px rgba(0,0,0,.15);
  font-family: Arial, sans-serif;
  line-height: 1.25;
}

/* Asegurar que el control de zoom conserve su margen por defecto */
.leaflet-top.leaflet-left .leaflet-control-zoom{
  margin-top: 0;
  margin-left: 4px;
}

/* En pantallas pequeñas, subimos la leyenda para no tapar */
@media (max-height: 640px), (max-width: 640px){
  .legend-consultores{
    top: 10px;
    transform: none;
  }
}
</style>
{% endmacro %}
"""
    macro = MacroElement()
    macro._template = Template(_css)
    mapa.get_root().add_child(macro)

    # 12) Preparar DataFrame de exportación
    df_export = pd.DataFrame()
    
    # Obtener eventos sin coordenadas para inclusión completa en CSV
    df_eventos_sin_coords = pd.DataFrame()
    try:
        df_eventos_sin_coords = eventos_sin_coordenadas_por_ruta_y_rango(co, id_ruta_real, fecha_inicio, fecha_fin)
        if df_eventos_sin_coords is not None and not df_eventos_sin_coords.empty:
            # Filtrar solo eventos tipo 20 para reducir ruido
            df_eventos_sin_coords = df_eventos_sin_coords[df_eventos_sin_coords['id_evento_tipo'] == 20].copy()
            # Asegurar flag defensivo
            df_eventos_sin_coords['venta_fuera_ruta'] = 1
            logger.info(f"Se obtuvieron {len(df_eventos_sin_coords)} eventos tipo 20 sin coordenadas para CSV")
    except Exception as e:
        logger.warning(f"Error obteniendo eventos sin coordenadas: {e}")
        df_eventos_sin_coords = pd.DataFrame()
    
    # Obtener eventos tipo 20 por consultor (sin depender de mapeo de ruta)
    df_tipo20 = pd.DataFrame()
    if ids_evento:  # Solo si hay consultores identificados
        try:
            df_tipo20 = eventos_tipo20_por_consultor(fecha_inicio, fecha_fin, ids_evento)
            if df_tipo20 is not None and not df_tipo20.empty:
                logger.info(f"Se obtuvieron {len(df_tipo20)} eventos tipo 20 por consultor para CSV")
        except Exception as e:
            logger.warning(f"Error obteniendo eventos tipo 20 por consultor: {e}")
            df_tipo20 = pd.DataFrame()
    
    if df_eventos is not None and not df_eventos.empty:
        if padres_ruta or hijos_ruta:
            # Hay cuadrantes: separar dentro/fuera
            df_in = df_filtrados.copy() if not df_filtrados.empty else pd.DataFrame()
            df_out = pd.DataFrame()
            
            if mostrar_fuera and 'mask_in' in locals():
                df_out = df_eventos[~mask_in].reset_index(drop=True)
            
            # Construir df_export según mostrar_fuera
            if mostrar_fuera:
                # Incluir ambos: dentro y fuera
                if not df_in.empty:
                    df_in = df_in.copy()
                    df_in['dentro_cuadrante'] = True
                    df_in['origen'] = 'con_coordenadas'
                
                if not df_out.empty:
                    df_out = df_out.copy()
                    df_out['dentro_cuadrante'] = False
                    df_out['origen'] = 'con_coordenadas'
                
                # Concatenar
                dfs_to_concat = []
                if not df_in.empty:
                    dfs_to_concat.append(df_in)
                if not df_out.empty:
                    dfs_to_concat.append(df_out)
                
                if dfs_to_concat:
                    df_export = pd.concat(dfs_to_concat, ignore_index=True)
            else:
                # Solo dentro de cuadrantes
                if not df_in.empty:
                    df_export = df_in.copy()
                    df_export['dentro_cuadrante'] = True
                    df_export['origen'] = 'con_coordenadas'
        else:
            # No hay cuadrantes: todos los eventos son "fuera"
            df_export = df_eventos.copy()
            df_export['dentro_cuadrante'] = False
            df_export['origen'] = 'con_coordenadas'
    
    # Fusionar con eventos sin coordenadas si existen
    if not df_eventos_sin_coords.empty:
        df_eventos_sin_coords = df_eventos_sin_coords.copy()
        df_eventos_sin_coords['dentro_cuadrante'] = False  # Sin coordenadas = fuera de cuadrantes
        df_eventos_sin_coords['origen'] = 'sin_coordenadas'
        
        # Fusionar DataFrames
        if not df_export.empty:
            df_export = pd.concat([df_export, df_eventos_sin_coords], ignore_index=True)
        else:
            df_export = df_eventos_sin_coords
    
    # Fusionar con eventos tipo 20 por consultor si existen
    if not df_tipo20.empty:
        df_tipo20 = df_tipo20.copy()
        df_tipo20['dentro_cuadrante'] = False  # Sin coordenadas = fuera de cuadrantes
        df_tipo20['origen'] = 'sin_coordenadas'
        
        # Fusionar DataFrames
        if not df_export.empty:
            df_export = pd.concat([df_export, df_tipo20], ignore_index=True)
        else:
            df_export = df_tipo20
    
    # Log de verificación del export
    logger.info(f"CSV export: total={len(df_export) if df_export is not None and not df_export.empty else 0} | "
                f"tipo20_por_consultor={0 if df_tipo20.empty else len(df_tipo20)}")
    
    # Reordenar columnas en df_export para asegurar tipo_evento después de id_evento_tipo y origen al final
    if df_export is not None and not df_export.empty:
        cols = list(df_export.columns)
        if 'id_evento_tipo' in cols and 'tipo_evento' in cols:
            cols.remove('tipo_evento')
            insert_at = cols.index('id_evento_tipo') + 1
            cols.insert(insert_at, 'tipo_evento')
        
        # Mover origen y dentro_cuadrante al final
        for col in ['dentro_cuadrante', 'origen']:
            if col in cols:
                cols.remove(col)
                cols.append(col)
        
        df_export = df_export[cols]
    
    # Deduplicar df_export para evitar doble conteo (especialmente eventos tipo 20 que pueden venir por ruta y consultor)
    df_export_clean = df_export.copy() if df_export is not None and not df_export.empty else pd.DataFrame()
    if not df_export_clean.empty:
        if 'id_evento' in df_export_clean.columns:
            # Usar id_evento como clave principal de deduplicación
            df_export_clean = df_export_clean.drop_duplicates(subset=['id_evento'])
        else:
            # Respaldo si no hay id_evento: usar combinación de campos
            df_export_clean = df_export_clean.drop_duplicates(subset=['id_evento','id_consultor','id_evento_tipo','fecha_evento'])
        
        logger.info(f"Deduplicación CSV: {len(df_export)} → {len(df_export_clean)} filas")
    
    # 12.5) Leyenda basada en df_export_clean (mismo dataset que el CSV)
    total_csv = len(df_export_clean)
    dentro_csv = int(df_export_clean.get('dentro_cuadrante', pd.Series([False] * len(df_export_clean))).sum()) if not df_export_clean.empty else 0
    pct_csv = (100 * dentro_csv / total_csv) if total_csv > 0 else 0.0
    
    # Preparar texto de fechas
    texto_fecha = _formatear_rango_leyenda(fecha_inicio, fecha_fin)
    
    lineas_leyenda = [
        f"<b>Consultores — {nombre_ruta_resuelto} ({ciudadN})</b>",
        texto_fecha,
        f"Total: {total_csv}",
        f"Dentro: {dentro_csv} ({pct_csv:.1f}%)"
    ]
    
    # Añadir métricas de recorrido si se calcularon
    try:
        if 'rec_stats' in locals() and rec_stats["tracks"] > 0:
            km_tot = rec_stats["km_total"]
            h_ini  = rec_stats["hora_ini"].strftime("%H:%M") if rec_stats["hora_ini"] is not None else "—"
            h_fin  = rec_stats["hora_fin"].strftime("%H:%M") if rec_stats["hora_fin"] is not None else "—"

            lineas_leyenda.append(f"<hr style='border:none;border-top:1px solid #e5e7eb;margin:6px 0;'>")
            lineas_leyenda.append(f"<b>Recorridos (1 día):</b>")
            lineas_leyenda.append(f"{rec_stats['tracks']} consultor(es) · {rec_stats['points']} puntos")
            lineas_leyenda.append(f"Distancia total: {km_tot:.1f} km")
            lineas_leyenda.append(f"Horario: {h_ini} – {h_fin}")
    except Exception as _:
        pass
    
    html_leyenda = f"""
    <div class="legend-consultores">
      {'<br>'.join(lineas_leyenda)}
    </div>"""
    mapa.get_root().html.add_child(folium.Element(html_leyenda))
    
    # 13) Guardar y retornar
    folium.LayerControl(collapsed=False, position='topright').add_to(mapa)
    filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_consultores", permitir_multiples=False)
    mapa.save(f"static/maps/{filename}")
    
    # Retornar tupla de 3 elementos: (filename, n_puntos, df_export)
    # n_puntos = total de eventos en el CSV (después de deduplicación)
    n_puntos = total_csv
    return filename, n_puntos, df_export_clean

def analizar_consultores_por_cuadrantes(fecha_inicio: str, fecha_fin: str, ciudad: str, 
                                       ruta_id: int, geojson_path: str = None) -> tuple:
    """
    Realiza análisis geoespacial completo de consultores por cuadrantes.
    
    Args:
        fecha_inicio (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        fecha_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
        ciudad (str): Nombre de la ciudad
        ruta_id (int): ID de la ruta
        geojson_path (str, optional): Ruta al archivo GeoJSON de cuadrantes
    
    Returns:
        tuple: (df_resumen, df_detalle, validacion_resultado)
        - df_resumen: DataFrame con resumen por cuadrante
        - df_detalle: DataFrame con detalle por cuadrante-consultor  
        - validacion_resultado: Dict con resultados de validación
    
    Raises:
        Exception: Si hay errores en el análisis geoespacial
    """
    logger.info(f"Iniciando análisis geoespacial para {ciudad}, ruta {ruta_id}")
    
    try:
        # 1. Obtener datos base
        ciudadN = _norm_city(ciudad)
        co = get_co(ciudadN)
        
        # 2. Determinar archivo GeoJSON
        if geojson_path is None:
            geojson_path = f"geojson/cuadrantes_{ciudadN.lower()}_rutas_consultores.geojson"
        
        # 3. Obtener eventos con coordenadas
        df_eventos = eventos_con_coordenadas_por_ruta_y_rango(co, ruta_id, fecha_inicio, fecha_fin)
        
        # Logging para debugging de estructura de datos
        if df_eventos is not None and not df_eventos.empty:
            logger.info(f"Recorridos: columnas df_eventos = {list(df_eventos.columns)}")
        
        # 4. Obtener ventas con coordenadas (opcional)
        try:
            df_ventas = ventas_con_coordenadas_por_ruta_y_rango(co, ruta_id, fecha_inicio, fecha_fin)
        except Exception as e:
            logger.warning(f"No se pudieron obtener datos de ventas: {e}")
            df_ventas = None
        
        # 5. Procesar análisis geoespacial
        df_resumen, df_detalle = procesar_consultores_por_cuadrantes(
            geojson_path, df_eventos, df_ventas
        )
        
        # 6. Validar consistencia
        validacion = validar_consistencia_datos(df_resumen, df_detalle)
        
        logger.info(f"Análisis completado: {len(df_resumen)} cuadrantes, {len(df_detalle)} registros detalle")
        
        return df_resumen, df_detalle, validacion
        
    except Exception as e:
        logger.error(f"Error en análisis geoespacial: {str(e)}")
        raise e

def obtener_resumen_cuadrantes_consultores(fecha_inicio: str, fecha_fin: str, ciudad: str, 
                                          ruta_id: int, geojson_path: str = None) -> pd.DataFrame:
    """
    Función simplificada para obtener solo el resumen por cuadrantes.
    
    Args:
        fecha_inicio (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        fecha_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
        ciudad (str): Nombre de la ciudad
        ruta_id (int): ID de la ruta
        geojson_path (str, optional): Ruta al archivo GeoJSON de cuadrantes
    
    Returns:
        pd.DataFrame: Resumen con columnas ['codigo_cuadrante', 'area_m2', 'visitas_tot', 
                     'visitas_por_m2', 'aperturas_tot', 'ventas_tot', 'total_venta_tot', 'consultores']
    """
    try:
        df_resumen, _, _ = analizar_consultores_por_cuadrantes(
            fecha_inicio, fecha_fin, ciudad, ruta_id, geojson_path
        )
        return df_resumen
    except Exception as e:
        logger.error(f"Error obteniendo resumen: {str(e)}")
        return pd.DataFrame()

def obtener_detalle_cuadrantes_consultores(fecha_inicio: str, fecha_fin: str, ciudad: str, 
                                          ruta_id: int, geojson_path: str = None) -> pd.DataFrame:
    """
    Función simplificada para obtener solo el detalle por cuadrante-consultor.
    
    Args:
        fecha_inicio (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        fecha_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
        ciudad (str): Nombre de la ciudad
        ruta_id (int): ID de la ruta
        geojson_path (str, optional): Ruta al archivo GeoJSON de cuadrantes
    
    Returns:
        pd.DataFrame: Detalle con columnas ['codigo_cuadrante', 'id_consultor', 'apellido', 
                     'visitas', 'aperturas', 'ventas', 'total_venta_conIVA']
    """
    try:
        _, df_detalle, _ = analizar_consultores_por_cuadrantes(
            fecha_inicio, fecha_fin, ciudad, ruta_id, geojson_path
        )
        return df_detalle
    except Exception as e:
        logger.error(f"Error obteniendo detalle: {str(e)}")
        return pd.DataFrame()