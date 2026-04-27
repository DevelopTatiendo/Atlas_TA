"""Nuevo módulo de preprocesamiento para muestras (Fase 1).

Responsabilidad limitada a:
1. Consultar la BD (consultar_db)
2. Limpiar / normalizar DataFrame crudo (crear_df)

No calcula métricas, no agrupa, no deduplica. Preparación básica de datos.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
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

@st.cache_data(ttl=1800, show_spinner="Cargando promotores...", max_entries=20)
def listar_promotores(id_centroope: int, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
    """Lista promotores (cargo=39) con actividad en el rango y centro dados.

    Retorna columnas estándar para UI:
      - id_promotor (int)
      - apellido_promotor (str)

    Resultados cacheados por Streamlit durante 30 minutos (ttl=1800).
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
@st.cache_data(ttl=1800, show_spinner="Consultando base de datos...", max_entries=10)
def consultar_db(
    id_centroope: int,
    fecha_inicio: str,
    fecha_fin: str,
    ids_promotor: Optional[tuple] = None,
) -> pd.DataFrame:
    """Ejecuta una consulta única para eventos de muestras.

    Retorna DataFrame crudo con las columnas solicitadas. Si no hay filas,
    devuelve DataFrame vacío con las columnas esperadas.

    Args:
        ids_promotor: tuple de ints (no List) — requerido por @st.cache_data
                      para que el argumento sea hashable. Pasar como:
                      tuple(ids) o None.
    Resultados cacheados por Streamlit durante 30 minutos (ttl=1800).
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

    # Filtro opcional por tuple de promotores
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


# Máximo de IDs que se pasan en una sola cláusula IN a MySQL.
# Si hay más, se divide en lotes y se concatenan los resultados.
_CONTACTABILIDAD_BATCH = 2000

@st.cache_data(ttl=1800, show_spinner="Consultando llamadas post-muestra...", max_entries=10)
def consultar_llamadas_raw(
    ids_contacto: tuple,
    fecha_inicio: str,
    fecha_fin: str,
) -> pd.DataFrame:
    """Retorna llamadas por id_contacto con fecha y flags — SIN agregar.

    La agregación final (es_contactable, es_venta) se hace en Python
    aplicando el filtro temporal: solo llamadas POSTERIORES a la muestra
    del promotor (fecha_llamada > fecha_muestra del promotor).

    Fuente: fullclean_telemercadeo.llamadas + llamadas_respuestas
    Una llamada es contacto real si: lr.contestada=1 OR lr.contacto_exitoso=1
    Una llamada es venta indirecta si: lr.es_venta=1

    Columnas retornadas:
        id_contacto   int
        fecha_llamada date   — DATE(fecha_inicio_llamada), para comparar con fecha_evento
        es_contactable int   — 1 si la llamada fue contestada o exitosa
        es_venta       int   — 1 si la llamada resultó en venta

    La query agrupa por (id_contacto, fecha_llamada) para reducir filas
    sin perder la dimensión temporal necesaria para el filtro.
    Cacheado 30 min (ttl=1800, max_entries=10).
    """
    if not ids_contacto:
        return pd.DataFrame(columns=["id_contacto", "fecha_llamada", "es_contactable", "es_venta"])

    fecha_ini_str = f"{fecha_inicio} 00:00:00"
    fecha_fin_str = f"{fecha_fin} 23:59:59"

    ids_list = list(ids_contacto)
    partes: list[pd.DataFrame] = []

    for start in range(0, len(ids_list), _CONTACTABILIDAD_BATCH):
        lote = ids_list[start : start + _CONTACTABILIDAD_BATCH]
        placeholders = ", ".join([f":cid_{i}" for i in range(len(lote))])
        query = f"""
        SELECT
            l.id_contacto,
            DATE(l.fecha_inicio_llamada)                                            AS fecha_llamada,
            MAX(CASE WHEN lr.contestada = 1 OR lr.contacto_exitoso = 1 THEN 1 ELSE 0 END) AS es_contactable,
            MAX(CASE WHEN lr.es_venta = 1                               THEN 1 ELSE 0 END) AS es_venta
        FROM fullclean_telemercadeo.llamadas l
        INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
               ON  lr.id = l.id_respuesta
        WHERE l.fecha_inicio_llamada BETWEEN :fecha_inicio AND :fecha_fin
          AND l.id_contacto IN ({placeholders})
        GROUP BY l.id_contacto, DATE(l.fecha_inicio_llamada)
        """
        params: dict = {"fecha_inicio": fecha_ini_str, "fecha_fin": fecha_fin_str}
        for i, v in enumerate(lote):
            params[f"cid_{i}"] = int(v)

        df_lote = sql_read(query, params=params, schema="fullclean_telemercadeo")
        if df_lote is not None and not df_lote.empty:
            partes.append(df_lote)

    if not partes:
        return pd.DataFrame(columns=["id_contacto", "fecha_llamada", "es_contactable", "es_venta"])

    df = pd.concat(partes, ignore_index=True)
    df["id_contacto"]  = pd.to_numeric(df["id_contacto"], errors="coerce").astype("Int64")
    df["fecha_llamada"] = pd.to_datetime(df["fecha_llamada"], errors="coerce").dt.normalize()
    for col in ["es_contactable", "es_venta"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["id_contacto", "fecha_llamada"]).reset_index(drop=True)


def aplicar_contactabilidad_temporal(
    df_filtrado: pd.DataFrame,
    df_llamadas_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Cruza df_filtrado con llamadas y aplica el filtro temporal por promotor.

    Regla de negocio:
      Una llamada se atribuye al promotor X para el cliente Y si:
        DATE(llamada.fecha_inicio_llamada) > DATE(muestra_X_Y.fecha_evento)

    Pasos:
      1. Merge df_filtrado (id_promotor, id_contacto, fecha_evento)
         con df_llamadas_raw (id_contacto, fecha_llamada, flags)
         → resultado: (id_promotor, id_contacto, fecha_evento, fecha_llamada, flags)
      2. Filtro temporal: conservar solo filas donde fecha_llamada > fecha_evento
      3. Agregar por (id_promotor, id_contacto): MAX de cada flag

    Retorna df_filtrado con columnas adicionales:
        es_contactable  int  — 1 si hubo aló posterior a la muestra
        es_venta        int  — 1 si hubo venta posterior a la muestra (venta indirecta)
    """
    cols_needed = ["id_promotor", "id_contacto", "fecha_evento"]
    if df_llamadas_raw.empty or df_filtrado.empty:
        df_out = df_filtrado.copy()
        df_out["es_contactable"] = 0
        df_out["es_venta"] = 0
        return df_out

    # Asegurar tipos comparables
    df_f = df_filtrado.copy()
    df_f["fecha_evento"] = pd.to_datetime(df_f["fecha_evento"], errors="coerce").dt.normalize()
    df_f["id_contacto"]  = pd.to_numeric(df_f["id_contacto"], errors="coerce").astype("Int64")

    # Merge: cada fila de filtrado x todas sus llamadas
    cruzado = df_f[cols_needed].merge(
        df_llamadas_raw[["id_contacto", "fecha_llamada", "es_contactable", "es_venta"]],
        on="id_contacto",
        how="left",
    )

    # Filtro temporal: llamada POSTERIOR a la muestra (mañana en adelante)
    mask_posterior = cruzado["fecha_llamada"] > cruzado["fecha_evento"]
    cruzado_valido = cruzado[mask_posterior]

    # Agregar por (promotor, contacto) → MAX de flags
    if cruzado_valido.empty:
        flags = pd.DataFrame(
            df_f[["id_promotor", "id_contacto"]].drop_duplicates()
        )
        flags["es_contactable"] = 0
        flags["es_venta"]        = 0
    else:
        flags = (
            cruzado_valido
            .groupby(["id_promotor", "id_contacto"], as_index=False)
            .agg(es_contactable=("es_contactable", "max"),
                 es_venta=("es_venta", "max"))
        )

    # Merge de vuelta al df_filtrado original
    df_out = df_filtrado.copy()
    # Limpiar columnas anteriores si existen
    for col in ["es_contactable", "es_venta"]:
        if col in df_out.columns:
            df_out = df_out.drop(columns=[col])
    df_out["id_contacto"] = pd.to_numeric(df_out["id_contacto"], errors="coerce").astype("Int64")
    df_out = df_out.merge(flags, on=["id_promotor", "id_contacto"], how="left")
    for col in ["es_contactable", "es_venta"]:
        df_out[col] = df_out[col].fillna(0).astype(int)
    return df_out


__all__ = [
    "consultar_db", "crear_df", "COLUMNAS_ESTANDAR",
    "listar_promotores", "consultar_llamadas_raw", "aplicar_contactabilidad_temporal",
]

# NOTA DE USO: consultar_db ahora recibe ids_promotor como tuple (no list).
# Ejemplo de llamada correcta:
#   consultar_db(id_centroope=2, fecha_inicio="2024-01-01", fecha_fin="2024-12-31",
#                ids_promotor=tuple(ids) if ids else None)
