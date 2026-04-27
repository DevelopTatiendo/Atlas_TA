# utils/agentes_utils.py
#
# Refactor: reemplaza mysql.connector directo por sql_read() de db_utils.
# - Elimina dependencia de mysql.connector y load_dotenv manual
# - Usa queries parametrizadas (sin f-strings) → previene SQL injection
# - Reutiliza el pool de conexiones SQLAlchemy con LRU cache de db_utils

import pandas as pd
from pre_procesamiento.db_utils import sql_read

SCHEMA = "fullclean_contactos"


def obtener_agentes_por_ciudad(
    centroope,
    fecha_inicio: str = "2024-01-01",
    fecha_fin: str    = "2024-12-31",
) -> list:
    """
    Retorna lista de id_autor (agentes) con eventos de muestreo (tipo 15)
    en el centro de operación y rango de fechas indicados.
    """
    query = """
    SELECT DISTINCT e.id_autor
    FROM fullclean_contactos.vwEventosAgente e
    LEFT JOIN fullclean_contactos.vwContactos con ON e.id_contacto = con.id
    LEFT JOIN fullclean_contactos.ciudades     ciu ON ciu.id = con.id_ciudad
    WHERE e.id_evento_tipo = 15
      AND ciu.id_centroope = :centroope
      AND e.fecha_evento BETWEEN :fecha_inicio AND :fecha_fin
      AND e.coordenada_longitud <> 0
      AND e.coordenada_latitud  <> 0
    """
    params = {
        "centroope":    str(centroope),
        "fecha_inicio": f"{fecha_inicio} 00:00:00",
        "fecha_fin":    f"{fecha_fin} 23:59:59",
    }

    df = sql_read(query, params=params, schema=SCHEMA)

    if df is None or df.empty:
        return []

    return df["id_autor"].dropna().sort_values().unique().tolist()
