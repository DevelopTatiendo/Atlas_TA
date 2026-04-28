"""Cache de coordenadas de clientes — almacenado en Parquet local.

El cache guarda SOLO: id_contacto | lat | lon
Todo lo demás (nombre, deuda, ruta, etc.) viene del SQL que escribe el agente.

Lógica de coordenadas:
  - Solo contactos activos con historial real de interacción.
  - Centroide = AVG(lat), AVG(lon) de todos sus eventos con coordenadas válidas.
  - Si no tiene ningún evento con coord válida → lat = NaN, lon = NaN
    (el registro existe para poder hacer merge, pero sin ubicación).

Filtro de contactos incluidos:
  - estado = 1 (activo)
  - estado_cxc IN (0, 1)
  - cant_obsequios > 0  OR  ultimo_obsequio IS NOT NULL AND != ''

Procesamiento:
  - Un archivo Parquet por CO: static/datos/coords_co_{id_centroope}.parquet
  - construir_cache_todas() procesa los 7 COs en paralelo.
  - Excluidos de git (PII + datos grandes).

Uso:
    from agente.coord_cache_parquet import construir_cache_todas, buscar_coords

    # Una vez (o cuando se quiera refrescar):
    construir_cache_todas()

    # En cada generación de mapa:
    df_coords = buscar_coords([12345, 67890, ...], ciudad_id=3)
    # → DataFrame con id_contacto, lat, lon (solo filas con coords válidas)
"""

from __future__ import annotations

import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.secrets_manager import load_env_secure
load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

from pre_procesamiento.db_utils import sql_read

# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────

_DATOS_DIR = _ROOT / "static" / "datos"
_DATOS_DIR.mkdir(parents=True, exist_ok=True)

# id_centroope → nombre legible
CIUDADES: dict[int, str] = {
    2: "Cali",
    3: "Medellín",
    4: "Bogotá",
    5: "Pereira",
    6: "Manizales",
    7: "Bucaramanga",
    8: "Barranquilla",
}

# Bounds de Colombia para descartar coords inválidas (GPS erróneo/cero)
_LAT_MIN, _LAT_MAX = -4.5, 12.5
_LON_MIN, _LON_MAX = -82.0, -66.0

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SQL de construcción
# ─────────────────────────────────────────────────────────────────────────────

_SQL_COORDS = """
SELECT
    c.id          AS id_contacto,
    AVG(
        CASE
            WHEN e.coordenada_latitud  IS NOT NULL
             AND e.coordenada_latitud  != ''
             AND e.coordenada_latitud  != '0'
             AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
             AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN :lon_min AND :lon_max
            THEN CAST(e.coordenada_latitud AS DECIMAL(10,6))
            ELSE NULL
        END
    ) AS lat,
    AVG(
        CASE
            WHEN e.coordenada_latitud  IS NOT NULL
             AND e.coordenada_latitud  != ''
             AND e.coordenada_latitud  != '0'
             AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
             AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN :lon_min AND :lon_max
            THEN CAST(e.coordenada_longitud AS DECIMAL(10,6))
            ELSE NULL
        END
    ) AS lon,
    COUNT(
        CASE
            WHEN e.coordenada_latitud  IS NOT NULL
             AND e.coordenada_latitud  != ''
             AND e.coordenada_latitud  != '0'
             AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
            THEN 1
        END
    ) AS n_eventos_con_coords
FROM fullclean_contactos.contactos c
JOIN fullclean_contactos.barrios    b   ON b.Id       = c.id_barrio
JOIN fullclean_contactos.ciudades   ciu ON ciu.id     = b.id_ciudad
                                       AND ciu.id_centroope = :ciudad
LEFT JOIN fullclean_contactos.vwEventos e ON e.id_contacto = c.id
WHERE c.estado = 1
  AND c.estado_cxc IN (0, 1)
  AND (
        c.cant_obsequios > 0
        OR (c.ultimo_obsequio IS NOT NULL AND c.ultimo_obsequio != '')
      )
GROUP BY c.id
"""


# ─────────────────────────────────────────────────────────────────────────────
# Funciones públicas
# ─────────────────────────────────────────────────────────────────────────────

def _parquet_path(ciudad_id: int) -> Path:
    return _DATOS_DIR / f"coords_co_{ciudad_id}.parquet"


def construir_cache_ciudad(ciudad_id: int, verbose: bool = True) -> dict:
    """Construye y guarda el Parquet de coordenadas para un CO.

    Args:
        ciudad_id:  id_centroope del CO (2–8).
        verbose:    Si True, imprime progreso.

    Returns:
        Diccionario con stats: total, con_coords, sin_coords, path.
    """
    nombre = CIUDADES.get(ciudad_id, str(ciudad_id))
    if verbose:
        print(f"  [{nombre}] consultando BD...", flush=True)

    params = {
        "ciudad":  ciudad_id,
        "lat_min": _LAT_MIN,
        "lat_max": _LAT_MAX,
        "lon_min": _LON_MIN,
        "lon_max": _LON_MAX,
    }

    try:
        df = sql_read(_SQL_COORDS, params=params, schema="fullclean_contactos")
    except Exception as e:
        logger.error(f"[{nombre}] Error en consulta: {e}")
        return {"ciudad": nombre, "ok": False, "error": str(e)}

    # Convertir a tipos correctos
    df["id_contacto"] = df["id_contacto"].astype("int64")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["n_eventos_con_coords"] = pd.to_numeric(df["n_eventos_con_coords"], errors="coerce").fillna(0).astype("int32")

    # Guardar Parquet (solo las 3 columnas clave + n_eventos para diagnóstico)
    out_path = _parquet_path(ciudad_id)
    df[["id_contacto", "lat", "lon", "n_eventos_con_coords"]].to_parquet(
        out_path, index=False, compression="snappy"
    )

    total        = len(df)
    con_coords   = df["lat"].notna().sum()
    sin_coords   = total - con_coords
    size_kb      = out_path.stat().st_size // 1024

    if verbose:
        print(
            f"  [{nombre}] ✅  {total:,} contactos "
            f"| {con_coords:,} con coords ({100*con_coords//total if total else 0}%) "
            f"| {sin_coords:,} sin coords "
            f"| {size_kb} KB → {out_path.name}"
        )

    return {
        "ciudad":      nombre,
        "ciudad_id":   ciudad_id,
        "ok":          True,
        "total":       total,
        "con_coords":  int(con_coords),
        "sin_coords":  int(sin_coords),
        "path":        str(out_path),
        "size_kb":     size_kb,
    }


def construir_cache_todas(ciudades: list[int] | None = None, workers: int = 4) -> list[dict]:
    """Construye el cache de coordenadas para todas las ciudades en paralelo.

    Args:
        ciudades:  Lista de id_centroope a procesar. None = todas (2-8).
        workers:   Número de hilos paralelos (default 4, máx 7).

    Returns:
        Lista de dicts con stats por ciudad.
    """
    targets = ciudades or list(CIUDADES.keys())
    workers = min(workers, len(targets))

    print(f"\n🗺️  Construyendo cache de coordenadas — {len(targets)} ciudades, {workers} hilos paralelos")
    print("─" * 60)

    resultados = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuros = {pool.submit(construir_cache_ciudad, cid): cid for cid in targets}
        for futuro in as_completed(futuros):
            try:
                resultado = futuro.result()
                resultados.append(resultado)
            except Exception as e:
                cid = futuros[futuro]
                logger.error(f"Error inesperado en ciudad {cid}: {e}")
                resultados.append({"ciudad_id": cid, "ok": False, "error": str(e)})

    print("─" * 60)
    exitosos = [r for r in resultados if r.get("ok")]
    total_contactos = sum(r.get("total", 0) for r in exitosos)
    total_coords    = sum(r.get("con_coords", 0) for r in exitosos)
    print(f"✅  Completado: {len(exitosos)}/{len(targets)} ciudades")
    print(f"   {total_contactos:,} contactos totales | {total_coords:,} con coordenadas")
    return resultados


def cargar_coords(ciudad_id: int) -> pd.DataFrame:
    """Carga el Parquet de coordenadas para una ciudad.

    Returns:
        DataFrame con columnas: id_contacto, lat, lon, n_eventos_con_coords.
        DataFrame vacío si no existe el archivo (avisa con print).
    """
    path = _parquet_path(ciudad_id)
    if not path.exists():
        nombre = CIUDADES.get(ciudad_id, str(ciudad_id))
        print(f"⚠️  Cache no encontrado para {nombre}. Ejecuta: construir_cache_ciudad({ciudad_id})")
        return pd.DataFrame(columns=["id_contacto", "lat", "lon", "n_eventos_con_coords"])
    return pd.read_parquet(path)


def buscar_coords(ids_contacto: Sequence[int], ciudad_id: int) -> pd.DataFrame:
    """Devuelve las coordenadas para una lista de IDs de contacto.

    Solo retorna filas con lat/lon válidas (no nulos).
    Úsalo para hacer el merge antes de pintar un mapa.

    Args:
        ids_contacto:  Lista de ids a buscar.
        ciudad_id:     id_centroope de la ciudad.

    Returns:
        DataFrame con id_contacto, lat, lon. Solo filas con coords válidas.
    """
    df_cache = cargar_coords(ciudad_id)
    if df_cache.empty:
        return df_cache[["id_contacto", "lat", "lon"]]

    # Filtrar a los IDs solicitados y descartar sin coords
    mascara = df_cache["id_contacto"].isin(ids_contacto) & df_cache["lat"].notna()
    return df_cache.loc[mascara, ["id_contacto", "lat", "lon"]].reset_index(drop=True)


def estado_cache() -> pd.DataFrame:
    """Devuelve un DataFrame con el estado actual del cache por ciudad."""
    filas = []
    for cid, nombre in CIUDADES.items():
        path = _parquet_path(cid)
        if path.exists():
            df = pd.read_parquet(path)
            con_coords = df["lat"].notna().sum()
            filas.append({
                "ciudad":       nombre,
                "id_centroope": cid,
                "total":        len(df),
                "con_coords":   int(con_coords),
                "pct_coords":   round(100 * con_coords / len(df), 1) if len(df) else 0,
                "size_kb":      path.stat().st_size // 1024,
                "modificado":   pd.Timestamp(path.stat().st_mtime, unit="s"),
            })
        else:
            filas.append({
                "ciudad":       nombre,
                "id_centroope": cid,
                "total":        None,
                "con_coords":   None,
                "pct_coords":   None,
                "size_kb":      None,
                "modificado":   None,
            })
    return pd.DataFrame(filas)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Construir cache de coordenadas de clientes")
    parser.add_argument("--ciudad", type=int, nargs="+",
                        help="id_centroope(s) a procesar (ej: 3 o 3 2 4). Default: todas.")
    parser.add_argument("--workers", type=int, default=4,
                        help="Hilos paralelos (default: 4)")
    parser.add_argument("--estado", action="store_true",
                        help="Mostrar estado actual del cache sin reconstruir")
    args = parser.parse_args()

    if args.estado:
        df_estado = estado_cache()
        print("\n📊 Estado del cache de coordenadas:")
        print(df_estado.to_string(index=False))
    else:
        construir_cache_todas(ciudades=args.ciudad, workers=args.workers)
