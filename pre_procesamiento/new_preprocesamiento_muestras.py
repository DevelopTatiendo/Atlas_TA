"""Nuevo módulo de preprocesamiento para muestras (Fase 1).

Responsabilidad limitada a:
1. Consultar la BD (consultar_db)
2. Limpiar / normalizar DataFrame crudo (crear_df)

No calcula métricas, no agrupa, no deduplica. Preparación básica de datos.
"""

from __future__ import annotations

import pandas as pd
from typing import List, Optional
import unicodedata
from .db_utils import sql_read  # Reutilizamos helper existente de lectura SQL

# Columnas estándar esperadas en la salida normalizada
COLUMNAS_ESTANDAR = [
    "id_muestra",
    "id_contacto",
    "fecha_evento",
    "id_evento_tipo",
    "id_promotor",
    "coordenada_longitud",
    "coordenada_latitud",
    "medio_contacto",
    "tipo_evento",
    "id_contacto_categoria",
    "ultima_llamada",
    "id_barrio",
    "barrio",
    "apellido_promotor",
    "mes",
]

def listar_promotores(id_centroope: int, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """Lista promotores (cargo=39) con actividad en el rango y centro dados.

    Retorna columnas estándar para UI:
      - id_promotor (int)
      - apellido_promotor (str)
    """
    query = (
        """
        SELECT DISTINCT
            per.id AS id_promotor,
            per.apellido AS apellido_promotor
        FROM fullclean_contactos.vwEventos e
        INNER JOIN fullclean_personal.personal per
            ON per.id = e.id_autor AND per.id_cargo = 39
        INNER JOIN fullclean_contactos.vwContactos con
            ON con.id = e.id_contacto
        INNER JOIN fullclean_contactos.ciudades ciu
            ON ciu.id = con.id_ciudad
        WHERE
            e.fecha_evento BETWEEN :fecha_inicio AND :fecha_fin
            AND ciu.id_centroope = :id_centroope
            AND e.id_evento_tipo = 15
        ORDER BY per.apellido
        """
    )
    params = {
        "fecha_inicio": f"{fecha_inicio} 00:00:00",
        "fecha_fin": f"{fecha_fin} 23:59:59",
        "id_centroope": int(id_centroope),
    }
    df = sql_read(query, params=params, schema="fullclean_contactos")
    if df is None or df.empty:
        return pd.DataFrame(columns=["id_promotor", "apellido_promotor"])
    # Normalización de tipos
    if "id_promotor" in df.columns:
        df["id_promotor"] = pd.to_numeric(df["id_promotor"], errors="coerce").astype("Int64")
    if "apellido_promotor" in df.columns:
        df["apellido_promotor"] = df["apellido_promotor"].fillna("").astype(str)
    return df[[c for c in ["id_promotor", "apellido_promotor"] if c in df.columns]].dropna(subset=["id_promotor"]).drop_duplicates("id_promotor").reset_index(drop=True)

# --- Helpers locales para rutas ---
def _normalizar_ciudad_local(ciudad: str) -> str:
    s = str(ciudad or "")
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').upper().strip()

# Mapeo local de ciudad -> id_centroope
CO_BY_CITY = {
    'CALI': 2,
    'MEDELLIN': 3,
    'MANIZALES': 6,
    'PEREIRA': 5,
    'BOGOTA': 4,
    'BARRANQUILLA': 8,
    'BUCARAMANGA': 7,
}

def listar_rutas_simple(ciudad: str) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas:
      - id_ruta (Int64)
      - ruta (str)
    para la ciudad, consultando fullclean_contactos.rutas_cobro por id_centroope.
    """
    ciudad_norm = _normalizar_ciudad_local(ciudad)
    co = CO_BY_CITY.get(ciudad_norm)
    if co is None:
        raise ValueError(f"Ciudad no reconocida para rutas: {ciudad}")

    query = (
        """
        SELECT r.id AS id_ruta, r.ruta
        FROM fullclean_contactos.rutas_cobro r
        WHERE r.id_centroope = :co
        ORDER BY r.ruta;
        """
    )
    params = {"co": int(co)}
    df = sql_read(query, params=params, schema="fullclean_contactos")
    if df is None or df.empty:
        return pd.DataFrame(columns=["id_ruta", "ruta"])

    # Normalización de tipos
    if "id_ruta" in df.columns:
        df["id_ruta"] = pd.to_numeric(df["id_ruta"], errors="coerce").astype("Int64")
    if "ruta" in df.columns:
        df["ruta"] = df["ruta"].fillna("").astype(str)

    df = df[[c for c in ["id_ruta", "ruta"] if c in df.columns]]
    df = df.dropna(subset=["id_ruta"]).drop_duplicates("id_ruta").reset_index(drop=True)
    return df
def consultar_db(
    id_centroope: int,
    fecha_inicio: str,
    fecha_fin: str,
    ids_promotor: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Ejecuta una consulta única para eventos de muestras.

    Retorna DataFrame crudo con las columnas solicitadas. Si no hay filas,
    devuelve DataFrame vacío con las columnas esperadas.
    """
    query = (
        """
        SELECT
            e.idEvento        AS id_muestra,
            e.id_contacto     AS id_contacto,
            e.fecha_evento    AS fecha_evento,
            e.id_evento_tipo  AS id_evento_tipo,
            e.id_autor        AS id_promotor,
            e.coordenada_longitud,
            e.coordenada_latitud,
            e.medio_contacto,
            e.tipo_evento,
            con.id_categoria  AS id_contacto_categoria,
            con.ultima_llamada AS ultima_llamada,
            con.id_barrio     AS id_barrio,
            bar.barrio        AS barrio,
            per.apellido      AS apellido_promotor,
            MONTH(e.fecha_evento) AS mes
        FROM fullclean_contactos.vwEventos e
        INNER JOIN fullclean_contactos.vwContactos con ON con.id = e.id_contacto
        LEFT JOIN fullclean_contactos.barrios bar ON bar.id = con.id_barrio
        INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = con.id_ciudad
        INNER JOIN fullclean_personal.personal per ON per.id = e.id_autor AND per.id_cargo = 39
        WHERE
            e.fecha_evento BETWEEN :fecha_inicio AND :fecha_fin
            AND ciu.id_centroope = :id_centroope
            AND e.id_evento_tipo = 15
            AND e.coordenada_longitud <> 0
            AND e.coordenada_latitud <> 0
        """
    )

    params = {
        "fecha_inicio": f"{fecha_inicio} 00:00:00",
        "fecha_fin": f"{fecha_fin} 23:59:59",
        "id_centroope": id_centroope,
    }

    # Filtro opcional por lista de promotores
    if ids_promotor:
        ids_promotor_limpios = [int(x) for x in ids_promotor if str(x).strip()]
        if ids_promotor_limpios:
            placeholders = ",".join([f":pid_{i}" for i in range(len(ids_promotor_limpios))])
            query += f" AND e.id_autor IN ({placeholders})"
            for i, v in enumerate(ids_promotor_limpios):
                params[f"pid_{i}"] = v

    query += ";"

    df_raw = sql_read(query, params=params, schema="fullclean_contactos")
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)
    return df_raw


def crear_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Normaliza columnas, tipos y limpia filas inválidas.

    No aplica deduplicación ni cálculos de métricas.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=COLUMNAS_ESTANDAR)

    df = df_raw.copy()

    # Posibles variantes de nombres -> estándar
    mapping_variantes = {
        "ID_MUESTRA": "id_muestra",
        "ID_EVENTO": "id_muestra",
        "ID_CONTACTO": "id_contacto",
        "FECHA_EVENTO": "fecha_evento",
        "ID_EVENTO_TIPO": "id_evento_tipo",
        "ID_AUTOR": "id_promotor",
        "ID_PROMOTOR": "id_promotor",
        "COORDENADA_LONGITUD": "coordenada_longitud",
        "COORDENADA_LATITUD": "coordenada_latitud",
        "MEDIO_CONTACTO": "medio_contacto",
        "TIPO_EVENTO": "tipo_evento",
        "ID_CATEGORIA": "id_contacto_categoria",
        "ID_CONTACTO_CATEGORIA": "id_contacto_categoria",
        "ULTIMA_LLAMADA": "ultima_llamada",
        "ID_BARRIO": "id_barrio",
        "BARRIO": "barrio",
        "APELLIDO_PROMOTOR": "apellido_promotor",
    }

    # Renombrar columnas por coincidencia exacta en mayúsculas
    cols_actuales = df.columns.tolist()
    rename_map = {}
    for c in cols_actuales:
        cu = c.upper()
        if cu in mapping_variantes:
            rename_map[c] = mapping_variantes[cu]
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Asegurar presencia de todas las columnas estándar (crear vacías si faltan)
    for col in COLUMNAS_ESTANDAR:
        if col not in df.columns:
            df[col] = pd.NA

    # Tipos básicos
    for col in ["fecha_evento", "ultima_llamada"]:
        if col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["coordenada_latitud", "coordenada_longitud"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["id_muestra", "id_contacto", "id_evento_tipo", "id_promotor", "id_contacto_categoria", "id_barrio"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Filtrado básico
    if "fecha_evento" in df.columns:
        df = df[df["fecha_evento"].notna()]
    if {"coordenada_latitud", "coordenada_longitud"}.issubset(df.columns):
        df = df[df["coordenada_latitud"].notna() & df["coordenada_longitud"].notna()]

    # Columna mes
    if "mes" not in df.columns or df["mes"].isna().all():
        if "fecha_evento" in df.columns:
            df["mes"] = df["fecha_evento"].dt.month.astype("Int64")
        else:
            df["mes"] = pd.NA

    return df.reset_index(drop=True)[COLUMNAS_ESTANDAR]


__all__ = [
    "consultar_db",
    "crear_df",
    "COLUMNAS_ESTANDAR",
    "listar_promotores",
    "listar_rutas_simple",
]
