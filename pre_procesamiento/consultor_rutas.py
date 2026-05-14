"""Queries SQL para el módulo de Rutas Consultores.

Funciones:
  - listar_consultores(ciudad, f_ini, f_fin)  → DataFrame con id_autor, apellido, n_eventos
  - rutas_por_consultor(ciudad, f_ini, f_fin, ids_consultor) → DataFrame con eventos ordenados
"""

from __future__ import annotations

from typing import Sequence

import pandas as pd

from pre_procesamiento.db_utils import sql_read

# ── Centroopes ────────────────────────────────────────────────────────────────
CENTROOPES: dict[str, int] = {
    "CALI": 2,
    "MEDELLIN": 3,
    "MANIZALES": 6,
    "PEREIRA": 5,
    "BOGOTA": 4,
    "BARRANQUILLA": 8,
    "BUCARAMANGA": 7,
}


def _norm(ciudad: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFD", ciudad.upper().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _centroope(ciudad: str) -> int:
    co = CENTROOPES.get(_norm(ciudad))
    if co is None:
        raise ValueError(f"Ciudad no reconocida: {ciudad!r}")
    return co


# ── listar_consultores ────────────────────────────────────────────────────────

def listar_consultores(
    ciudad: str,
    f_ini: str,
    f_fin: str,
) -> pd.DataFrame:
    """
    Retorna los consultores (id_cargo = 5) que tuvieron eventos con coordenadas
    en el período indicado, ordenados por número de eventos descendente.

    Columnas: id_autor, apellido, nombre, n_eventos
    """
    co = _centroope(ciudad)
    sql = """
SELECT
    p.id                          AS id_autor,
    p.apellido                    AS apellido,
    p.nombre                      AS nombre,
    COUNT(e.idEvento)             AS n_eventos
FROM fullclean_contactos.vwEventos e
JOIN fullclean_contactos.contactos c
    ON c.id = e.id_contacto
JOIN fullclean_contactos.ciudades ci
    ON ci.id = c.id_ciudad
   AND ci.id_centroope = :co
JOIN fullclean_personal.personal p
    ON p.id = e.id_autor
   AND p.id_cargo = 5
WHERE e.fecha_evento BETWEEN :f_ini AND :f_fin
  AND e.coordenada_latitud  BETWEEN -5  AND 13
  AND e.coordenada_longitud BETWEEN -81 AND -66
  AND e.coordenada_latitud  <> 0
  AND e.coordenada_longitud <> 0
GROUP BY p.id, p.apellido, p.nombre
ORDER BY n_eventos DESC
"""
    params = {
        "co": co,
        "f_ini": f"{f_ini} 00:00:00",
        "f_fin": f"{f_fin} 23:59:59",
    }
    df = sql_read(sql, params=params, schema="fullclean_contactos")
    if df is None:
        return pd.DataFrame(columns=["id_autor", "apellido", "nombre", "n_eventos"])
    df["id_autor"] = pd.to_numeric(df["id_autor"], errors="coerce")
    df["n_eventos"] = pd.to_numeric(df["n_eventos"], errors="coerce").fillna(0).astype(int)
    return df


# ── rutas_por_consultor ───────────────────────────────────────────────────────

def rutas_por_consultor(
    ciudad: str,
    f_ini: str,
    f_fin: str,
    ids_consultor: Sequence[int] | None = None,
) -> pd.DataFrame:
    """
    Retorna todos los eventos con coordenadas de los consultores solicitados,
    ordenados por (id_autor, fecha_evento ASC).

    Columnas:
        id_evento, id_autor, apellido, nombre, id_contacto,
        lat, lon, fecha_evento, id_evento_tipo, tipo_evento
    """
    co = _centroope(ciudad)

    # Cláusula de filtro por IDs (si se especifica)
    ids_clause = ""
    params: dict = {
        "co": co,
        "f_ini": f"{f_ini} 00:00:00",
        "f_fin": f"{f_fin} 23:59:59",
    }
    if ids_consultor:
        placeholders = ", ".join(f":id_{i}" for i, _ in enumerate(ids_consultor))
        ids_clause = f"AND p.id IN ({placeholders})"
        for i, id_c in enumerate(ids_consultor):
            params[f"id_{i}"] = int(id_c)

    sql = f"""
SELECT
    e.idEvento                    AS id_evento,
    e.id_autor,
    p.apellido,
    p.nombre,
    e.id_contacto,
    e.coordenada_latitud          AS lat,
    e.coordenada_longitud         AS lon,
    e.fecha_evento,
    e.id_evento_tipo,
    COALESCE(et.tipo_evento, '')  AS tipo_evento
FROM fullclean_contactos.vwEventos e
JOIN fullclean_contactos.contactos c
    ON c.id = e.id_contacto
JOIN fullclean_contactos.ciudades ci
    ON ci.id = c.id_ciudad
   AND ci.id_centroope = :co
JOIN fullclean_personal.personal p
    ON p.id = e.id_autor
   AND p.id_cargo = 5
LEFT JOIN fullclean_contactos.vwEventosTipos et
    ON et.id = e.id_evento_tipo
WHERE e.fecha_evento BETWEEN :f_ini AND :f_fin
  AND e.coordenada_latitud  BETWEEN -5  AND 13
  AND e.coordenada_longitud BETWEEN -81 AND -66
  AND e.coordenada_latitud  <> 0
  AND e.coordenada_longitud <> 0
  {ids_clause}
ORDER BY e.id_autor, e.fecha_evento ASC
"""
    df = sql_read(sql, params=params, schema="fullclean_contactos")
    if df is None:
        return pd.DataFrame(
            columns=[
                "id_evento", "id_autor", "apellido", "nombre", "id_contacto",
                "lat", "lon", "fecha_evento", "id_evento_tipo", "tipo_evento",
            ]
        )

    df["id_autor"]  = pd.to_numeric(df["id_autor"],  errors="coerce")
    df["lat"]       = pd.to_numeric(df["lat"],        errors="coerce")
    df["lon"]       = pd.to_numeric(df["lon"],        errors="coerce")
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"], errors="coerce")
    df = df.dropna(subset=["lat", "lon", "fecha_evento"])
    return df


__all__ = ["listar_consultores", "rutas_por_consultor", "CENTROOPES"]
