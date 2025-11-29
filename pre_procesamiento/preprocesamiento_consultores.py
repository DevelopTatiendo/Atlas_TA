import os
import pandas as pd
import unicodedata
import logging
import time
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from .db_utils import sql_read

# === IMPORTS DE BD ===
try:
    import mysql.connector as mysql
except ImportError:
    # Fallback por si el paquete está expuesto como 'mysql'
    try:
        from mysql import connector as mysql
    except ImportError:
        logging.error("No se pudo importar mysql.connector. Instale el paquete: pip install mysql-connector-python")
        mysql = None

# Cargar variables de entorno desde .env
dotenv_path = Path(__file__).resolve().parents[1] / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path, override=False)
else:
    print(f"⚠️ Advertencia: Archivo .env no encontrado en {dotenv_path}")

# === Helper de conexión ===
def _get_conn():
    """
    Crear conexión a MySQL usando variables de entorno.
    Intenta usar DB_HOST/DB_USER/DB_PASSWORD o fallback a MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD.
    """
    if mysql is None:
        raise RuntimeError("mysql.connector no está disponible. Instale: pip install mysql-connector-python")
    
    try:
        return mysql.connect(
            host=os.getenv("DB_HOST") or os.getenv("MYSQL_HOST"),
            database=os.getenv("DB_NAME", "fullclean_contactos"),
            user=os.getenv("DB_USER") or os.getenv("MYSQL_USER"),
            password=os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD"),
            autocommit=True
        )
    except Exception as e:
        logging.error(f"[BD] Error al conectar a MySQL: {e}")
        raise

# --- Resolver CO por ciudad (reusar mapping de otros módulos) ---
CENTROOPES = {'CALI':2,'MEDELLIN':3,'MANIZALES':6,'PEREIRA':5,'BOGOTA':4,'BARRANQUILLA':8,'BUCARAMANGA':7}
def get_co(ciudadN:str)->int:
    return CENTROOPES[ciudadN]

def _norm_city(ciudad: str) -> str:
    """Normalizar ciudad removiendo acentos y convirtiendo a mayúsculas."""
    return ''.join(c for c in unicodedata.normalize('NFD', ciudad) if unicodedata.category(c) != 'Mn').upper()

def _conn():
    """
    DEPRECATED: Usar _get_conn() en su lugar.
    Crear conexión a MySQL validando variables de entorno obligatorias.
    """
    # Delegar a _get_conn() para evitar duplicación
    return _get_conn()

def ping_db():
    """Prueba básica de conectividad a la base de datos."""
    try:
        with _get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
        logging.info("Ping BD exitoso")
        return result is not None
    except Exception as e:
        logging.error(f"[BD] Ping BD falló: {e}")
        return False

def _pick_col(df, candidates):
    """Busca la primera columna que existe en el DataFrame de una lista de candidatos."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _ensure_lat_lon(df):
    """Normaliza nombres de columnas de coordenadas a 'lat' y 'lon'."""
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

def listar_rutas_simple(ciudad:str)->pd.DataFrame:
    """Devuelve id_ruta, ruta para la ciudad (sin depender de eventos)."""
    # Normalizar ciudad removiendo acentos
    ciudad_norm = _norm_city(ciudad)
    co = get_co(ciudad_norm)
    
    q = """
    SELECT r.id AS id_ruta, r.ruta
    FROM fullclean_contactos.rutas_cobro r
    WHERE r.id_centroope = :co
    ORDER BY r.ruta;
    """
    # Usar SQLAlchemy Engine a través del helper db_utils con parámetros con nombres
    df = sql_read(q, params={'co': co})
    return df

def eventos_por_ruta_en_rango(centroope:int, id_ruta:int, f_ini:str, f_fin:str)->pd.DataFrame:
    """
    Retorna todos los eventos de la ruta en el rango de fechas con coordenadas válidas.
    Columnas principales: id_evento, id_autor, id_consultor, apellido, lat, lon, fecha_evento, id_cargo, cargo
    """
    q = """
    SELECT  e.idEvento                AS idEvento,
            e.id_autor                AS id_autor,         -- Semántica original (EVENTOS)
            p.apellido                AS apellido,         -- Alias estándar que usa el mapa
            e.id_contacto             AS id_contacto,
            e.fecha_evento            AS fecha_evento,
            e.id_evento_tipo          AS id_evento_tipo,
            e.tipo_evento             AS tipo_evento,
            e.coordenada_longitud     AS coordenada_longitud,
            e.coordenada_latitud      AS coordenada_latitud,
            e.coordenada_altitud      AS coordenada_altitud,
            e.medio_contacto          AS medio_contacto,
            
            -- Aliases legacy / técnicos para mantener compatibilidad aguas abajo
            e.idEvento                AS id_evento,
            e.coordenada_latitud      AS lat,
            e.coordenada_longitud     AS lon,
            p.id_cargo                AS id_cargo,
            ca.cargo                  AS cargo,
            
            -- Alias técnico para no romper groupby/rutas: id_consultor := id_autor (EVENTOS)
            e.id_autor                AS id_consultor
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.vwContactos c           ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b               ON b.id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rc    ON rc.id_barrio = b.id
    JOIN fullclean_contactos.rutas_cobro r           ON r.id = rc.id_ruta_cobro
    JOIN fullclean_personal.personal p               ON p.id = e.id_autor
    JOIN fullclean_personal.cargos ca                ON ca.Id_cargo = p.id_cargo
    WHERE c.estado = 1
      AND c.estado_cxc IN (0,1)
      AND r.id_centroope = %s
      AND r.id = %s
      AND e.fecha_evento BETWEEN %s AND %s
      AND e.coordenada_latitud  IS NOT NULL
      AND e.coordenada_longitud IS NOT NULL
      AND e.coordenada_latitud  <> 0
      AND e.coordenada_longitud <> 0
      AND e.coordenada_latitud  BETWEEN -5 AND 13
      AND e.coordenada_longitud BETWEEN -81 AND -66
       AND ca.Id_cargo = 5
        AND e.id_evento_tipo = 10 --  not in (48,51, 66,65)
      -- AND ca.Id_cargo in (181, 5)
    ORDER BY e.fecha_evento ASC;
    """
    
    try:
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=[centroope, id_ruta, f_ini, f_fin])
        
        # Normalizar tipos por seguridad
        if not df.empty:
            df['id_autor'] = pd.to_numeric(df['id_autor'], errors='coerce')
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce')
            df['apellido'] = df['apellido'].fillna('').astype(str)
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
            df['fecha_evento'] = pd.to_datetime(df['fecha_evento'], errors='coerce')
            df = df.dropna(subset=['lat','lon'])
        return df
    except Exception as e:
        logging.error(f"[BD] Error en eventos_por_ruta_en_rango: {e}")
        raise

def nombre_ruta(centroope: int, id_ruta: int) -> str:
    """
    Retorna el nombre de la ruta (r.ruta) para el CO e id_ruta dados.
    Si no encuentra, retorna None.
    """
    q = """
    SELECT r.ruta
    FROM fullclean_contactos.rutas_cobro r
    WHERE r.id_centroope = %s AND r.id = %s
    LIMIT 1;
    """
    
    try:
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=[centroope, id_ruta])
        return None if df.empty else str(df.iloc[0]['ruta'])
    except Exception as e:
        logging.error(f"[BD] Error en nombre_ruta: {e}")
        raise

def eventos_con_coordenadas_por_ruta_y_rango(co: int, id_ruta: int, f_ini: str, f_fin: str) -> pd.DataFrame:
    """
    Trae eventos crudos filtrados para consultores (cargo=5) con coordenadas válidas.
    Consulta SQL específica para el módulo Consultores (simple) sin cuadrantes ni métricas.
    
    Args:
        co (int): ID del centro de operaciones
        id_ruta (int): ID de la ruta de cobro
        f_ini (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        f_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_evento','id_contacto','lat','lon','fecha_evento',
                     'id_autor','apellido','id_cargo','cargo','id_evento_tipo','tipo_evento']
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"[CONSULTA_EVENTOS] CO={co} ruta={id_ruta} f_ini={f_ini} f_fin={f_fin}")
    
    # Consulta SQL específica para Consultores (simple) con columnas exactas según especificaciones
    q = """
    SELECT
        e.idEvento            AS id_evento,
        e.id_contacto         AS id_contacto,
        e.coordenada_latitud  AS lat,
        e.coordenada_longitud AS lon,
        e.fecha_evento        AS fecha_evento,
        e.id_evento_tipo      AS id_evento_tipo,
        e.tipo_evento         AS tipo_evento,
        p.apellido            AS apellido
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.vwContactos c      ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b          ON b.Id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rc ON rc.id_barrio = b.Id
    JOIN fullclean_contactos.rutas_cobro r      ON r.id = rc.id_ruta_cobro
    JOIN fullclean_personal.personal p          ON p.id = e.id_autor
    JOIN fullclean_personal.cargos ca           ON ca.Id_cargo = p.id_cargo
    WHERE c.estado = 1
      AND c.estado_cxc IN (0,1)
      AND r.id_centroope = :co
      AND r.id = :id_ruta
      AND e.fecha_evento BETWEEN :f_ini AND :f_fin
      AND e.coordenada_latitud  IS NOT NULL
      AND e.coordenada_longitud IS NOT NULL
      AND e.coordenada_latitud  <> 0
      AND e.coordenada_longitud <> 0
      AND e.coordenada_latitud  BETWEEN -5  AND 13
      AND e.coordenada_longitud BETWEEN -81 AND -66
      AND ca.Id_cargo = 5
      AND e.id_evento_tipo = 10 -- IN (3,10,11,13,15,16,17,21,22,40,45,46,55,56,57,58,62,64,71,73,74,76,77,78)
    ORDER BY e.fecha_evento ASC;
    """
    
    try:
        # Usar SQLAlchemy Engine a través del helper db_utils con parámetros con nombres
        params_dict = {'co': co, 'id_ruta': id_ruta, 'f_ini': f_ini, 'f_fin': f_fin}
        df = sql_read(q, params=params_dict)
        
        # Logging de resultado
        filas_resultado = len(df)
        logging.info(f"[CONSULTA_EVENTOS] CO={co} ruta={id_ruta} f_ini={f_ini} f_fin={f_fin} → rows={filas_resultado}")
        
        if filas_resultado == 0:
            # Warning con query parametrizada para depuración (sin credenciales)
            query_debug = q.replace(':co', str(co)).replace(':id_ruta', str(id_ruta)).replace(':f_ini', f"'{f_ini}'").replace(':f_fin', f"'{f_fin}'")
            logging.warning(f"Query sin resultados: {query_debug}")
        
        # Normalizar tipos de datos según especificaciones exactas
        if not df.empty:
            # Normalización básica según especificaciones
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce') 
            df['fecha_evento'] = pd.to_datetime(df['fecha_evento'], errors='coerce')
            
            # Normalización adicional para integridad
            df['id_evento'] = pd.to_numeric(df['id_evento'], errors='coerce')
            df['id_contacto'] = pd.to_numeric(df['id_contacto'], errors='coerce')
            df['id_evento_tipo'] = pd.to_numeric(df['id_evento_tipo'], errors='coerce')
            df['apellido'] = df['apellido'].fillna('').astype(str)
            
            # Enriquecimiento de tipo_evento según Opción B - mapeo manual
            EVENT_TYPE_LABELS = {
                3:'Visita', 10:'Apertura', 11:'Apertura', 13:'Apertura', 15:'Muestra', 
                16:'Apertura', 17:'Apertura', 20:'Venta fuera ruta', 21:'Apertura', 22:'Apertura', 
                40:'Gestión', 45:'Gestión', 46:'Gestión', 55:'Venta', 56:'Venta', 57:'Venta ruta', 
                58:'Venta ruta', 62:'Gestión', 64:'Gestión', 71:'Gestión', 73:'Gestión', 
                74:'Apertura SAC', 76:'Apertura SAC', 77:'Gestión', 78:'Gestión'
            }
            
            # Aplicar enriquecimiento: primero mapear nulos, luego fallback a ID como string
            df['tipo_evento'] = df['tipo_evento'].fillna(df['id_evento_tipo'].map(EVENT_TYPE_LABELS))
            df['tipo_evento'] = df['tipo_evento'].fillna(df['id_evento_tipo'].astype(str))
            df['tipo_evento'] = df['tipo_evento'].astype(str)
            
            # Eliminar filas con coordenadas inválidas después de conversión
            df = df.dropna(subset=['lat', 'lon', 'fecha_evento'])
            
            # Validar coordenadas realistas para Colombia
            df = df[
                (df['lat'].between(-5, 13)) & 
                (df['lon'].between(-81, -66))
            ]
            
            # Logging de verificación (una sola vez) según especificaciones
            logging.info(f"Columnas del DataFrame: {df.columns.tolist()}")
            if len(df) > 0:
                logging.info(f"Muestra primeras 3 filas:\n{df.head(3).to_string()}")
        
        # Logging de tiempo de ejecución
        tiempo_ejecucion = time.time() - inicio_tiempo
        logging.info(f"eventos_con_coordenadas_por_ruta_y_rango completada en {tiempo_ejecucion:.2f}s - {len(df)} eventos retornados")
        
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error en eventos_con_coordenadas_por_ruta_y_rango: {str(e)}")
        raise

def ventas_con_coordenadas_por_ruta_y_rango(id_centroope: int, id_ruta: int, f_ini: str, f_fin: str) -> pd.DataFrame:
    """
    Construye un DataFrame de ventas con coordenadas usando la lógica de herencia de coordenadas.
    
    Para cada venta (pedido):
    1. Si existe evento de venta (tipo 58) con coordenadas, usa esas coordenadas
    2. Si no, hereda coordenadas del evento más cercano (±24h, mismo consultor y contacto)
    
    Args:
        id_centroope (int): ID del centro de operaciones
        id_ruta (int): ID de la ruta de cobro
        f_ini (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        f_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_pedido', 'id_contacto', 'id_consultor', 'apellido',
                     'lat', 'lon', 'fecha_factura', 'valor_conIVA', 'origen_coords']
                     
    Notes:
        - origen_coords indica: 'evento_venta' o 'evento_heredado'
        - Solo incluye ventas con coordenadas válidas (realistas para Colombia)
        - Aplica ventana de ±24h para herencia de coordenadas
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"Iniciando ventas_con_coordenadas_por_ruta_y_rango - CO:{id_centroope}, Ruta:{id_ruta}, Rango:{f_ini} a {f_fin}")
    
    try:
        # Paso 1: Obtener todas las ventas (pedidos) en el rango
        q_ventas = """
        SELECT 
            pe.id                     AS id_pedido,
            pe.id_contacto            AS id_contacto,
            pe.id_vendedor            AS id_consultor,
            p.apellido                AS apellido,
            pe.fecha_factura          AS fecha_factura,
            pe.total_conIVA           AS valor_conIVA
        FROM fullclean_telemercadeo.pedidos pe
        JOIN fullclean_personal.personal p               ON p.id = pe.id_vendedor
        JOIN fullclean_personal.cargos ca                ON ca.Id_cargo = p.id_cargo
        JOIN fullclean_contactos.vwContactos c           ON c.id = pe.id_contacto
        JOIN fullclean_contactos.barrios b               ON b.id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rc    ON rc.id_barrio = b.id
        JOIN fullclean_contactos.rutas_cobro r           ON r.id = rc.id_ruta_cobro
        WHERE 
              c.estado = 1
          AND c.estado_cxc IN (0,1)
          AND r.id_centroope = %s
          AND r.id = %s
          AND pe.fecha_factura BETWEEN %s AND %s
          AND ca.Id_cargo = 181
        ORDER BY 
            pe.fecha_factura ASC,
            pe.id_vendedor ASC;
        """
        
        with _get_conn() as cn:
            df_ventas = pd.read_sql(q_ventas, cn, params=[id_centroope, id_ruta, f_ini, f_fin])
        
        if df_ventas.empty:
            logging.info("No se encontraron ventas en el rango especificado")
            return pd.DataFrame(columns=['id_pedido', 'id_contacto', 'id_consultor', 'apellido', 
                                       'lat', 'lon', 'fecha_factura', 'valor_conIVA', 'origen_coords'])
        
        # Normalizar tipos de datos de ventas
        df_ventas['id_pedido'] = pd.to_numeric(df_ventas['id_pedido'], errors='coerce')
        df_ventas['id_contacto'] = pd.to_numeric(df_ventas['id_contacto'], errors='coerce')
        df_ventas['id_consultor'] = pd.to_numeric(df_ventas['id_consultor'], errors='coerce')
        df_ventas['fecha_factura'] = pd.to_datetime(df_ventas['fecha_factura'], errors='coerce')
        df_ventas['valor_conIVA'] = pd.to_numeric(df_ventas['valor_conIVA'], errors='coerce')
        df_ventas['apellido'] = df_ventas['apellido'].fillna('').astype(str)
        
        # Paso 2: Obtener todos los eventos con coordenadas para hacer matching
        # Expandir ventana temporal ±24h para permitir herencia
        from datetime import datetime, timedelta
        f_ini_dt = datetime.strptime(f_ini[:19], '%Y-%m-%d %H:%M:%S')
        f_fin_dt = datetime.strptime(f_fin[:19], '%Y-%m-%d %H:%M:%S')
        f_ini_expandido = (f_ini_dt - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        f_fin_expandido = (f_fin_dt + timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        
        q_eventos = """
        SELECT 
            e.idEvento               AS id_evento,
            e.id_contacto            AS id_contacto,
            p.id                     AS id_consultor,
            e.coordenada_latitud     AS lat,
            e.coordenada_longitud    AS lon,
            e.fecha_evento           AS fecha_evento,
            e.id_evento_tipo         AS id_evento_tipo,
            e.tipo_evento            AS tipo_evento
        FROM fullclean_contactos.vwEventos e
        JOIN fullclean_contactos.vwContactos c           ON c.id = e.id_contacto
        JOIN fullclean_contactos.barrios b               ON b.id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rc    ON rc.id_barrio = b.id
        JOIN fullclean_contactos.rutas_cobro r           ON r.id = rc.id_ruta_cobro
        JOIN fullclean_personal.personal p               ON p.id = e.id_autor
        JOIN fullclean_personal.cargos ca                ON ca.Id_cargo = p.id_cargo
        WHERE 
              c.estado = 1
          AND c.estado_cxc IN (0,1)
          AND r.id_centroope = %s
          AND r.id = %s
          AND e.fecha_evento BETWEEN %s AND %s
          AND e.coordenada_latitud  IS NOT NULL
          AND e.coordenada_longitud IS NOT NULL
          AND e.coordenada_latitud  <> 0
          AND e.coordenada_longitud <> 0
          AND e.coordenada_latitud  BETWEEN -5  AND 13
          AND e.coordenada_longitud BETWEEN -81 AND -66
          AND ca.Id_cargo = 181
          AND e.id_evento_tipo NOT IN (48,51,50,66,65)
        ORDER BY 
            e.fecha_evento ASC;
        """
        
        with _get_conn() as cn:
            df_eventos = pd.read_sql(q_eventos, cn, params=[id_centroope, id_ruta, f_ini_expandido, f_fin_expandido])
        
        if df_eventos.empty:
            logging.info("No se encontraron eventos con coordenadas para hacer matching")
            return pd.DataFrame(columns=['id_pedido', 'id_contacto', 'id_consultor', 'apellido', 
                                       'lat', 'lon', 'fecha_factura', 'valor_conIVA', 'origen_coords'])
        
        # Normalizar tipos de datos de eventos
        df_eventos['id_evento'] = pd.to_numeric(df_eventos['id_evento'], errors='coerce')
        df_eventos['id_contacto'] = pd.to_numeric(df_eventos['id_contacto'], errors='coerce')
        df_eventos['id_consultor'] = pd.to_numeric(df_eventos['id_consultor'], errors='coerce')
        df_eventos['lat'] = pd.to_numeric(df_eventos['lat'], errors='coerce')
        df_eventos['lon'] = pd.to_numeric(df_eventos['lon'], errors='coerce')
        df_eventos['fecha_evento'] = pd.to_datetime(df_eventos['fecha_evento'], errors='coerce')
        df_eventos['id_evento_tipo'] = pd.to_numeric(df_eventos['id_evento_tipo'], errors='coerce')
        
        # Filtrar eventos con coordenadas válidas
        df_eventos = df_eventos.dropna(subset=['lat', 'lon', 'fecha_evento'])
        df_eventos = df_eventos[
            (df_eventos['lat'].between(-5, 13)) & 
            (df_eventos['lon'].between(-81, -66))
        ]
        
        # Paso 3: Para cada venta, buscar coordenadas
        ventas_con_coords = []
        
        for _, venta in df_ventas.iterrows():
            id_contacto = venta['id_contacto']
            id_consultor = venta['id_consultor']
            fecha_factura = venta['fecha_factura']
            
            # Buscar primero evento de venta (tipo 58) exacto
            eventos_venta = df_eventos[
                (df_eventos['id_contacto'] == id_contacto) &
                (df_eventos['id_consultor'] == id_consultor) &
                (df_eventos['id_evento_tipo'] == 58)
            ]
            
            lat, lon, origen = None, None, None
            
            if not eventos_venta.empty:
                # Si hay eventos de venta, tomar el más cercano en tiempo
                eventos_venta['diff_tiempo'] = abs((eventos_venta['fecha_evento'] - fecha_factura).dt.total_seconds())
                evento_mejor = eventos_venta.loc[eventos_venta['diff_tiempo'].idxmin()]
                lat, lon, origen = evento_mejor['lat'], evento_mejor['lon'], 'evento_venta'
            else:
                # Buscar evento más cercano (±24h, mismo consultor y contacto)
                ventana_24h = 24 * 3600  # 24 horas en segundos
                eventos_candidatos = df_eventos[
                    (df_eventos['id_contacto'] == id_contacto) &
                    (df_eventos['id_consultor'] == id_consultor)
                ]
                
                if not eventos_candidatos.empty:
                    eventos_candidatos['diff_tiempo'] = abs((eventos_candidatos['fecha_evento'] - fecha_factura).dt.total_seconds())
                    eventos_en_ventana = eventos_candidatos[eventos_candidatos['diff_tiempo'] <= ventana_24h]
                    
                    if not eventos_en_ventana.empty:
                        evento_mejor = eventos_en_ventana.loc[eventos_en_ventana['diff_tiempo'].idxmin()]
                        lat, lon, origen = evento_mejor['lat'], evento_mejor['lon'], 'evento_heredado'
            
            # Si encontramos coordenadas, agregar a resultado
            if lat is not None and lon is not None:
                ventas_con_coords.append({
                    'id_pedido': venta['id_pedido'],
                    'id_contacto': venta['id_contacto'],
                    'id_consultor': venta['id_consultor'],
                    'apellido': venta['apellido'],
                    'lat': lat,
                    'lon': lon,
                    'fecha_factura': venta['fecha_factura'],
                    'valor_conIVA': venta['valor_conIVA'],
                    'origen_coords': origen
                })
        
        # Crear DataFrame resultado
        df_resultado = pd.DataFrame(ventas_con_coords)
        
        # Logging de tiempo de ejecución y estadísticas
        tiempo_ejecucion = time.time() - inicio_tiempo
        total_ventas = len(df_ventas)
        ventas_con_coords_count = len(df_resultado)
        eventos_venta_count = len(df_resultado[df_resultado['origen_coords'] == 'evento_venta']) if not df_resultado.empty else 0
        eventos_heredados_count = len(df_resultado[df_resultado['origen_coords'] == 'evento_heredado']) if not df_resultado.empty else 0
        
        logging.info(f"ventas_con_coordenadas_por_ruta_y_rango completada en {tiempo_ejecucion:.2f}s")
        logging.info(f"Estadísticas: {total_ventas} ventas totales, {ventas_con_coords_count} con coordenadas")
        logging.info(f"Origen coordenadas: {eventos_venta_count} eventos_venta, {eventos_heredados_count} eventos_heredados")
        
        return df_resultado
        
    except Exception as e:
        logging.error(f"[BD] Error en ventas_con_coordenadas_por_ruta_y_rango: {str(e)}")
        raise

def consultores_metricas_por_ruta_y_rango(id_centroope: int, id_ruta: int, f_ini: str, f_fin: str) -> pd.DataFrame:
    """
    Ejecuta consulta SQL agregada por consultor para obtener métricas de visitas, aperturas, ventas y total de ventas.
    
    Args:
        id_centroope (int): ID del centro de operaciones
        id_ruta (int): ID de la ruta de cobro
        f_ini (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        f_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_consultor', 'apellido', 'cant_visitas', 'cant_aperturas', 'cant_ventas', 'total_venta_conIVA']
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"Iniciando consultores_metricas_por_ruta_y_rango - CO:{id_centroope}, Ruta:{id_ruta}, Rango:{f_ini} a {f_fin}")
    
    # Consulta SQL parametrizada con nueva regla de negocio: total_venta_conIVA directo desde pedidos
    q = """
    SELECT
        eagg.id_consultor,
        eagg.apellido,
        eagg.cant_visitas,
        eagg.cant_aperturas,
        eagg.cant_ventas,
        COALESCE(v.total_venta_conIVA, 0) AS total_venta_conIVA
    FROM (
        /* Agregado por consultor desde eventos */
        SELECT
            p.id  AS id_consultor,
            p.apellido,
            COUNT(e.idEvento)                                                     AS cant_visitas,
            SUM(CASE WHEN e.id_evento_tipo IN (73,62,71,64,74) THEN 1 ELSE 0 END) AS cant_aperturas,
            SUM(CASE WHEN e.id_evento_tipo = 58             THEN 1 ELSE 0 END)    AS cant_ventas
        FROM fullclean_contactos.vwEventos e
        JOIN fullclean_contactos.vwContactos c           ON c.id = e.id_contacto
        JOIN fullclean_contactos.barrios b               ON b.id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rc    ON rc.id_barrio = b.id
        JOIN fullclean_contactos.rutas_cobro r           ON r.id = rc.id_ruta_cobro
        JOIN fullclean_personal.personal p               ON p.id = e.id_autor
        JOIN fullclean_personal.cargos ca                ON ca.Id_cargo = p.id_cargo
        WHERE
              c.estado = 1
          AND c.estado_cxc IN (0,1)
          AND r.id_centroope = %s
          AND r.id = %s
          AND e.fecha_evento BETWEEN %s AND %s
          AND e.coordenada_latitud  IS NOT NULL
          AND e.coordenada_longitud IS NOT NULL
          AND e.coordenada_latitud  <> 0
          AND e.coordenada_longitud <> 0
          AND e.coordenada_latitud  BETWEEN -5  AND 13
          AND e.coordenada_longitud BETWEEN -81 AND -66
          AND ca.Id_cargo = 181
          AND e.id_evento_tipo NOT IN (48,51,50,66,65)
        GROUP BY
            p.id, p.apellido
    ) AS eagg
    LEFT JOIN (
        /* Sumar ventas directamente desde pedidos por consultor (sin depender de eventos tipo 58) */
        SELECT
            pe.id_vendedor           AS id_consultor,
            SUM(pe.total_conIVA)     AS total_venta_conIVA
        FROM fullclean_telemercadeo.pedidos pe
        JOIN fullclean_personal.personal p               ON p.id = pe.id_vendedor
        JOIN fullclean_personal.cargos ca                ON ca.Id_cargo = p.id_cargo
        JOIN fullclean_contactos.vwContactos c           ON c.id = pe.id_contacto
        JOIN fullclean_contactos.barrios b               ON b.id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rc    ON rc.id_barrio = b.id
        JOIN fullclean_contactos.rutas_cobro r           ON r.id = rc.id_ruta_cobro
        WHERE
              c.estado = 1
          AND c.estado_cxc IN (0,1)
          AND r.id_centroope = %s
          AND r.id = %s
          AND pe.fecha_factura BETWEEN %s AND %s
          AND ca.Id_cargo = 181
        GROUP BY pe.id_vendedor
    ) AS v
      ON v.id_consultor = eagg.id_consultor
    ORDER BY
        eagg.cant_visitas DESC;
    """
    
    try:
        # Parámetros simplificados: id_centroope, id_ruta, f_ini, f_fin para ambas subconsultas
        params = [
            id_centroope, id_ruta, f_ini, f_fin,  # Primera subconsulta (eagg)
            id_centroope, id_ruta, f_ini, f_fin   # Segunda subconsulta (v) - pedidos directos
        ]
        
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=params)
        
        # Asegurar tipos de datos correctos con COALESCE para valores nulos
        if not df.empty:
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce')
            df['cant_visitas'] = pd.to_numeric(df['cant_visitas'], errors='coerce').fillna(0).astype(int)
            df['cant_aperturas'] = pd.to_numeric(df['cant_aperturas'], errors='coerce').fillna(0).astype(int)
            df['cant_ventas'] = pd.to_numeric(df['cant_ventas'], errors='coerce').fillna(0).astype(int)
            df['total_venta_conIVA'] = pd.to_numeric(df['total_venta_conIVA'], errors='coerce').fillna(0.0)
            df['apellido'] = df['apellido'].fillna('').astype(str)
        
        # Logging de tiempo de ejecución y tamaño
        tiempo_ejecucion = time.time() - inicio_tiempo
        filas_resultado = len(df)
        logging.info(f"consultores_metricas_por_ruta_y_rango completada en {tiempo_ejecucion:.2f}s - {filas_resultado} filas retornadas")
        
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error en consultores_metricas_por_ruta_y_rango: {str(e)}")
        raise

def ventas_totales_por_consultores(fi: str, ff: str, ids_consultor: list[int]) -> pd.DataFrame:
    """
    Obtiene totales de ventas por consultor(es) para un rango de fechas (global día).
    
    Args:
        fi (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        ff (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
        ids_consultor (list[int]): Lista de IDs de consultores
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_consultor', 'consultor', 'n_pedidos', 'total_venta_conIVA']
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"Iniciando ventas_totales_por_consultores - Rango:{fi} a {ff}, {len(ids_consultor)} consultores")
    
    # Si la lista está vacía, retornar DF vacío con columnas correctas
    if not ids_consultor:
        logging.warning("Lista de consultores vacía - retornando DataFrame vacío")
        return pd.DataFrame(columns=['id_consultor', 'consultor', 'n_pedidos', 'total_venta_conIVA'])
    
    # Crear placeholder dinámico para IN clause
    placeholders = ', '.join(['%s'] * len(ids_consultor))
    
    # SALES_GLOBAL_POR_CONSULTORES
    q = f"""
    SELECT 
        pe.id_vendedor       AS id_consultor,
        pr.apellido          AS consultor,
        COUNT(*)             AS n_pedidos,
        SUM(pe.total_conIVA) AS total_venta_conIVA
    FROM fullclean_telemercadeo.pedidos pe
    JOIN fullclean_personal.personal pr ON pr.id = pe.id_vendedor
    WHERE
          pr.estado = 1
      AND pr.id_cargo = 181
      AND pe.id_vendedor IN ({placeholders})
      AND pe.fecha_hora_pedido BETWEEN %s AND %s
    GROUP BY pe.id_vendedor, pr.apellido
    ORDER BY total_venta_conIVA DESC;
    """
    
    try:
        # Parámetros: ids_consultor + fi + ff
        params = ids_consultor + [fi, ff]
        
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=params)
        
        # Normalizar tipos de datos
        if not df.empty:
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce')
            df['consultor'] = df['consultor'].fillna('').astype(str)
            df['n_pedidos'] = pd.to_numeric(df['n_pedidos'], errors='coerce').fillna(0).astype(int)
            df['total_venta_conIVA'] = pd.to_numeric(df['total_venta_conIVA'], errors='coerce').fillna(0.0)
        
        # Logging de tiempo de ejecución y tamaño
        tiempo_ejecucion = time.time() - inicio_tiempo
        filas_resultado = len(df)
        logging.info(f"ventas_totales_por_consultores completada en {tiempo_ejecucion:.2f}s - {filas_resultado} consultores con ventas")
        
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error en ventas_totales_por_consultores: {str(e)}")
        raise

def conteo_eventos_sin_coords_por_consultor(id_centroope: int, id_ruta: int, f_ini: str, f_fin: str) -> pd.DataFrame:
    """
    Obtiene contadores agregados por consultor sin exigir coordenadas (para popup padre).
    
    Args:
        id_centroope (int): ID del centro de operaciones
        id_ruta (int): ID de la ruta de cobro
        f_ini (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        f_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_consultor', 'consultor', 'cant_visitas', 
                     'cant_aperturas', 'cant_sac', 'cant_venta_ruta', 'cant_venta_no_ruta']
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"Iniciando conteo_eventos_sin_coords_por_consultor - CO:{id_centroope}, Ruta:{id_ruta}, Rango:{f_ini} a {f_fin}")
    
    # EV_AGG_SIN_COORDS: contadores por consultor sin exigir coordenadas
    q = """
    /* EV_AGG_SIN_COORDS: contadores por consultor sin exigir coordenadas */
    SELECT
        p.id                         AS id_consultor,
        p.apellido                   AS consultor,
        COUNT(*)                     AS cant_visitas,
        SUM(CASE WHEN e.id_evento_tipo IN (3,10,11,13,15,16,17,21,22,40,45,46,55,56,57,58,62,64,71,73,74,77,78) THEN 1 ELSE 0 END) AS cant_aperturas,
        SUM(CASE WHEN e.id_evento_tipo IN (42,72,74,75,76) THEN 1 ELSE 0 END) AS cant_sac,
        SUM(CASE WHEN e.id_evento_tipo IN (58,57) THEN 1 ELSE 0 END) AS cant_venta_ruta,
        SUM(CASE WHEN e.id_evento_tipo = 20 THEN 1 ELSE 0 END) AS cant_venta_no_ruta
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.vwContactos c        ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b            ON b.id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rc ON rc.id_barrio = b.id
    JOIN fullclean_contactos.rutas_cobro r        ON r.id = rc.id_ruta_cobro
    JOIN fullclean_personal.personal p            ON p.id = e.id_autor
    JOIN fullclean_personal.cargos ca             ON ca.Id_cargo = p.id_cargo
    WHERE
          c.estado = 1
      AND c.estado_cxc IN (0,1)
      AND p.id_cargo = 181
      AND r.id_centroope = %s
      AND r.id = %s
      AND e.fecha_evento BETWEEN %s AND %s
      AND e.id_evento_tipo NOT IN (48,51,50,66,65)
    GROUP BY p.id, p.apellido;
    """
    
    try:
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=[id_centroope, id_ruta, f_ini, f_fin])
        
        # Normalizar tipos de datos
        if not df.empty:
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce')
            df['consultor'] = df['consultor'].fillna('').astype(str)
            df['cant_visitas'] = pd.to_numeric(df['cant_visitas'], errors='coerce').fillna(0).astype(int)
            df['cant_aperturas'] = pd.to_numeric(df['cant_aperturas'], errors='coerce').fillna(0).astype(int)
            df['cant_sac'] = pd.to_numeric(df['cant_sac'], errors='coerce').fillna(0).astype(int)
            df['cant_venta_ruta'] = pd.to_numeric(df['cant_venta_ruta'], errors='coerce').fillna(0).astype(int)
            df['cant_venta_no_ruta'] = pd.to_numeric(df['cant_venta_no_ruta'], errors='coerce').fillna(0).astype(int)
        
        # Logging de tiempo de ejecución y tamaño
        tiempo_ejecucion = time.time() - inicio_tiempo
        filas_resultado = len(df)
        logging.info(f"conteo_eventos_sin_coords_por_consultor completada en {tiempo_ejecucion:.2f}s - {filas_resultado} consultores")
        
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error en conteo_eventos_sin_coords_por_consultor: {str(e)}")
        raise

def eventos_tipo20_por_consultor(fi: str, ff: str, ids_consultor: list) -> pd.DataFrame:
    """
    Retorna filas de eventos tipo 20 (venta no ruta) sin exigir coordenadas,
    filtradas por los consultores pasados y por rango de fechas.
    
    Args:
        fi: Fecha inicial en formato 'YYYY-MM-DD HH:MM:SS'
        ff: Fecha final en formato 'YYYY-MM-DD HH:MM:SS'
        ids_consultor: Lista de IDs de consultores
    
    Returns:
        pd.DataFrame: DataFrame con eventos tipo 20 y columnas de clasificación
    """
    if not ids_consultor:
        return pd.DataFrame()
    
    # Crear placeholders dinámicos para la consulta
    placeholders = ', '.join(['%s'] * len(ids_consultor))
    
    query = f"""
    SELECT
        e.idEvento       AS id_evento,
        e.id_autor       AS id_consultor,
        p.apellido       AS consultor,
        e.id_contacto    AS id_contacto,
        e.fecha_evento   AS fecha_evento,
        e.id_evento_tipo AS id_evento_tipo,
        e.tipo_evento    AS tipo_evento,
        NULL             AS lat,
        NULL             AS lon,
        CASE WHEN e.id_evento_tipo IN (3,10,11,13,15,16,17,21,22,40,45,46,55,56,57,58,62,64,71,73,74,77,78) THEN 1 ELSE 0 END AS apertura,
        CASE WHEN e.id_evento_tipo IN (42,72,74,75,76) THEN 1 ELSE 0 END AS apertura_sac,
        CASE WHEN e.id_evento_tipo IN (58,57) THEN 1 ELSE 0 END AS venta_ruta,
        CASE WHEN e.id_evento_tipo = 20 THEN 1 ELSE 0 END AS venta_fuera_ruta,
        CASE WHEN e.id_evento_tipo = 15 THEN 1 ELSE 0 END AS entrega_muestras
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_personal.personal p ON p.id = e.id_autor
    WHERE p.id_cargo = 181
      AND e.fecha_evento BETWEEN %s AND %s
      AND e.id_evento_tipo = 20
      AND e.id_autor IN ({placeholders})
    ORDER BY e.fecha_evento ASC
    """
    
    # Parámetros: fechas + lista de consultores
    params = [fi, ff] + list(ids_consultor)
    
    try:
        with _get_conn() as cn:
            df = pd.read_sql(query, cn, params=params)
        
        if df.empty:
            logging.info("No se encontraron eventos tipo 20 para los consultores especificados")
            return pd.DataFrame()
        
        logging.info(f"Eventos tipo 20 por consultor: {len(df)} registros encontrados")
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error obteniendo eventos tipo 20 por consultor: {e}")
        return pd.DataFrame()

def eventos_sin_coordenadas_por_ruta_y_rango(id_centroope: int, id_ruta: int, f_ini: str, f_fin: str) -> pd.DataFrame:
    """
    Obtiene eventos sin exigir coordenadas para complementar el CSV con eventos tipo 20.
    
    Args:
        id_centroope (int): ID del centro de operaciones
        id_ruta (int): ID de la ruta de cobro
        f_ini (str): Fecha inicio en formato 'YYYY-MM-DD HH:MM:SS'
        f_fin (str): Fecha fin en formato 'YYYY-MM-DD HH:MM:SS'
    
    Returns:
        pd.DataFrame: DataFrame con columnas ['id_evento', 'id_consultor', 'consultor', 'id_contacto',
                     'fecha_evento', 'id_evento_tipo', 'tipo_evento', 'lat', 'lon', 'apertura', 
                     'apertura_sac', 'venta_ruta', 'venta_fuera_ruta', 'entrega_muestras']
    
    Raises:
        Exception: Si hay error en la conexión o ejecución de la consulta SQL
    """
    inicio_tiempo = time.time()
    logging.info(f"Iniciando eventos_sin_coordenadas_por_ruta_y_rango - CO:{id_centroope}, Ruta:{id_ruta}, Rango:{f_ini} a {f_fin}")
    
    # EV_DET_SIN_COORDS: filas de eventos sin exigir coordenadas
    q = """
    /* EV_DET_SIN_COORDS: filas de eventos sin exigir coordenadas */
    SELECT
        e.idEvento             AS id_evento,
        e.id_autor             AS id_consultor,
        p.apellido             AS consultor,
        e.id_contacto          AS id_contacto,
        e.fecha_evento         AS fecha_evento,
        e.id_evento_tipo       AS id_evento_tipo,
        e.tipo_evento          AS tipo_evento,
        NULL                   AS lat,     -- sin coordenadas
        NULL                   AS lon,     -- sin coordenadas
        /* flags */
        CASE WHEN e.id_evento_tipo IN (3,10,11,13,15,16,17,21,22,40,45,46,55,56,57,58,62,64,71,73,74,77,78) THEN 1 ELSE 0 END AS apertura,
        CASE WHEN e.id_evento_tipo IN (42,72,74,75,76) THEN 1 ELSE 0 END AS apertura_sac,
        CASE WHEN e.id_evento_tipo IN (58,57) THEN 1 ELSE 0 END AS venta_ruta,
        CASE WHEN e.id_evento_tipo = 20 THEN 1 ELSE 0 END AS venta_fuera_ruta,
        CASE WHEN e.id_evento_tipo = 15 THEN 1 ELSE 0 END AS entrega_muestras
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.vwContactos c        ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b            ON b.id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rc ON rc.id_barrio = b.id
    JOIN fullclean_contactos.rutas_cobro r        ON r.id = rc.id_ruta_cobro
    JOIN fullclean_personal.personal p            ON p.id = e.id_autor
    JOIN fullclean_personal.cargos ca             ON ca.Id_cargo = p.id_cargo
    WHERE
          c.estado = 1
      AND c.estado_cxc IN (0,1)
      AND p.id_cargo = 181
      AND r.id_centroope = %s
      AND r.id = %s
      AND e.fecha_evento BETWEEN %s AND %s
      AND e.id_evento_tipo NOT IN (48,51,50,66,65)
    ORDER BY e.fecha_evento ASC;
    """
    
    try:
        with _get_conn() as cn:
            df = pd.read_sql(q, cn, params=[id_centroope, id_ruta, f_ini, f_fin])
        
        # Normalizar tipos de datos
        if not df.empty:
            df['id_evento'] = pd.to_numeric(df['id_evento'], errors='coerce')
            df['id_consultor'] = pd.to_numeric(df['id_consultor'], errors='coerce')
            df['consultor'] = df['consultor'].fillna('').astype(str)
            df['id_contacto'] = pd.to_numeric(df['id_contacto'], errors='coerce')
            df['fecha_evento'] = pd.to_datetime(df['fecha_evento'], errors='coerce')
            df['id_evento_tipo'] = pd.to_numeric(df['id_evento_tipo'], errors='coerce')
            # lat y lon quedan como None (NULL)
            
            # Flags
            df['apertura'] = pd.to_numeric(df['apertura'], errors='coerce').fillna(0).astype(int)
            df['apertura_sac'] = pd.to_numeric(df['apertura_sac'], errors='coerce').fillna(0).astype(int)
            df['venta_ruta'] = pd.to_numeric(df['venta_ruta'], errors='coerce').fillna(0).astype(int)
            df['venta_fuera_ruta'] = pd.to_numeric(df['venta_fuera_ruta'], errors='coerce').fillna(0).astype(int)
            df['entrega_muestras'] = pd.to_numeric(df['entrega_muestras'], errors='coerce').fillna(0).astype(int)
        
        # Logging de tiempo de ejecución y tamaño
        tiempo_ejecucion = time.time() - inicio_tiempo
        filas_resultado = len(df)
        logging.info(f"eventos_sin_coordenadas_por_ruta_y_rango completada en {tiempo_ejecucion:.2f}s - {filas_resultado} eventos sin coordenadas")
        
        return df
        
    except Exception as e:
        logging.error(f"[BD] Error en eventos_sin_coordenadas_por_ruta_y_rango: {str(e)}")
        raise

def eventos_con_coordenadas_ciudad_y_rango(ciudad: str, f_ini: str, f_fin: str, id_ruta: int | None = None) -> pd.DataFrame:
    """
    Devuelve eventos con coordenadas para una ciudad y rango de fechas.
    Si id_ruta es None → trae TODAS las rutas de la ciudad.
    Columnas: id_evento, id_contacto, lat, lon, fecha_evento, id_evento_tipo, tipo_evento, apellido
    """
    co = get_co(_norm_city(ciudad))

    q = f"""
    SELECT
        e.idEvento            AS id_evento,
        e.id_contacto         AS id_contacto,
        e.coordenada_latitud  AS lat,
        e.coordenada_longitud AS lon,
        e.fecha_evento        AS fecha_evento,
        e.id_evento_tipo      AS id_evento_tipo,
        e.tipo_evento         AS tipo_evento,
        p.apellido            AS apellido
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.vwContactos c        ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b            ON b.Id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rc ON rc.id_barrio = b.Id
    JOIN fullclean_contactos.rutas_cobro r        ON r.id = rc.id_ruta_cobro
    JOIN fullclean_personal.personal p            ON p.id = e.id_autor
    JOIN fullclean_personal.cargos ca             ON ca.Id_cargo = p.id_cargo
    WHERE
          c.estado = 1
      AND c.estado_cxc IN (0,1)
      AND r.id_centroope = :co
      {"AND r.id = :id_ruta" if id_ruta is not None else ""}
      AND e.fecha_evento BETWEEN :f_ini AND :f_fin
      AND e.coordenada_latitud  IS NOT NULL
      AND e.coordenada_longitud IS NOT NULL
      AND e.coordenada_latitud  <> 0
      AND e.coordenada_longitud <> 0
      AND e.coordenada_latitud  BETWEEN -5  AND 13
      AND e.coordenada_longitud BETWEEN -81 AND -66
      AND ca.Id_cargo = 5
      AND id_evento_tipo = 10 
    ORDER BY e.fecha_evento ASC;
    """

    params = {"co": co, "f_ini": f_ini, "f_fin": f_fin}
    if id_ruta is not None:
        params["id_ruta"] = id_ruta

    df = sql_read(q, params=params)

    if not df.empty:
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df['fecha_evento'] = pd.to_datetime(df['fecha_evento'], errors='coerce')
        df['id_evento'] = pd.to_numeric(df['id_evento'], errors='coerce')
        df['id_contacto'] = pd.to_numeric(df['id_contacto'], errors='coerce')
        df['id_evento_tipo'] = pd.to_numeric(df['id_evento_tipo'], errors='coerce')
        df['apellido'] = df['apellido'].fillna('').astype(str)
        df = df.dropna(subset=['lat', 'lon', 'fecha_evento'])

    return df
