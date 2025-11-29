# utils/agentes_utils.py

import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def obtener_agentes_por_ciudad(centroope, fecha_inicio="2024-01-01", fecha_fin="2024-12-31"):
    conexion = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    query = f"""
    SELECT DISTINCT e.id_autor
    FROM fullclean_contactos.vwEventosAgente e
    LEFT JOIN fullclean_contactos.vwContactos con ON e.id_contacto = con.id
    LEFT JOIN fullclean_contactos.ciudades ciu ON ciu.id = con.id_ciudad
    
    WHERE e.id_evento_tipo = 15
      AND ciu.id_centroope = '{centroope}'
      AND e.fecha_evento BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
      AND coordenada_longitud <> 0 AND coordenada_latitud <> 0
    """
    df = pd.read_sql(query, conexion)
    conexion.close()
    return df["id_autor"].dropna().sort_values().unique().tolist()
