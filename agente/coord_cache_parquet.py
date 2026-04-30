"""
Cache de coordenadas de clientes — almacenado en Parquet local.

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
  - cant_obsequios > 0 OR ultimo_obsequio IS NOT NULL AND != ''

Procesamiento:
  - Un archivo Parquet por CO: static/datos/coords_co_{id_centroope}.parquet
  - construir_cache_todas() procesa los 7 COs en paralelo.
  - Excluidos de git (PII + datos grandes).

Uso:
    from agente.coord_cache_parquet import construir_cache_todas, buscar_coords

    # Una vez o cuando se quiera refrescar:
    construir_cache_todas()

    # En cada generación de mapa:
    df_coords = buscar_coords([12345, 67890, ...], ciudad_id=3)
    # → DataFrame con id_contacto, lat, lon solo filas con coords válidas
"""

from __future__ import annotations

import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Resolución robusta de raíz del proyecto
# ─────────────────────────────────────────────────────────────────────────────

_ROOT = Path(__file__).resolve().parent.parent

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─────────────────────────────────────────────────────────────────────────────
# Carga de credenciales desde .env plano
# ─────────────────────────────────────────────────────────────────────────────

from config.secrets_manager import load_env_secure

_ENV_ENC = _ROOT / "config" / ".env.enc"


def _cargar_credenciales() -> None:
    """
    Carga credenciales exclusivamente desde config/.env.enc.

    En este modo NO se usa .env plano.
    Requiere que MAPAS_SECRET_PASSPHRASE esté definida en la terminal.
    """
    if not _ENV_ENC.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo encriptado de credenciales: {_ENV_ENC}"
        )

    load_env_secure(
        prefer_plain=False,
        enc_path=str(_ENV_ENC),
        pass_env_var="MAPAS_SECRET_PASSPHRASE",
        cache=False,
    )


def _diagnosticar_credenciales() -> None:
    """
    Diagnóstico mínimo y seguro de variables de entorno.
    No imprime contraseñas ni secretos.
    """
    variables_requeridas = [
        "DB_HOST",
        "DB_USER",
        "DB_NAME",
        "DB_PORT",
    ]

    faltantes = [var for var in variables_requeridas if not os.environ.get(var)]

    if faltantes:
        print(
            "⚠️ Variables de BD no cargadas correctamente desde config/.env.enc: "
            + ", ".join(faltantes)
        )
        print(f"   Ruta esperada del archivo encriptado: {_ENV_ENC}")
        print("   Verifica que MAPAS_SECRET_PASSPHRASE esté definida en esta terminal.")
    else:
        db_host = os.environ.get("DB_HOST", "")
        db_user = os.environ.get("DB_USER", "")
        db_name = os.environ.get("DB_NAME", "")
        db_port = os.environ.get("DB_PORT", "")

        print("✅ Credenciales de BD cargadas desde config/.env.enc")
        print(f"   Ruta .env.enc: {_ENV_ENC}")
        print(f"   DB_HOST: {db_host[:20]}...")
        print(f"   DB_USER: {db_user[:4]}...")
        print(f"   DB_NAME: {db_name}")
        print(f"   DB_PORT: {db_port}")


_cargar_credenciales()



from pre_procesamiento.db_utils import sql_read


# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────

_DATOS_DIR = _ROOT / "static" / "datos"
_DATOS_DIR.mkdir(parents=True, exist_ok=True)

CIUDADES: dict[int, str] = {
    2: "Cali",
    3: "Medellín",
    4: "Bogotá",
    5: "Pereira",
    6: "Manizales",
    7: "Bucaramanga",
    8: "Barranquilla",
}

_LAT_MIN, _LAT_MAX = -4.5, 12.5
_LON_MIN, _LON_MAX = -82.0, -66.0

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SQL de construcción
# ─────────────────────────────────────────────────────────────────────────────

_SQL_COORDS = """
SELECT
    c.id AS id_contacto,

    AVG(
        CASE
            WHEN e.coordenada_latitud IS NOT NULL
             AND e.coordenada_latitud != ''
             AND e.coordenada_latitud != '0'
             AND e.coordenada_longitud IS NOT NULL
             AND e.coordenada_longitud != ''
             AND e.coordenada_longitud != '0'
             AND CAST(e.coordenada_latitud AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
             AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN :lon_min AND :lon_max
            THEN CAST(e.coordenada_latitud AS DECIMAL(10,6))
            ELSE NULL
        END
    ) AS lat,

    AVG(
        CASE
            WHEN e.coordenada_latitud IS NOT NULL
             AND e.coordenada_latitud != ''
             AND e.coordenada_latitud != '0'
             AND e.coordenada_longitud IS NOT NULL
             AND e.coordenada_longitud != ''
             AND e.coordenada_longitud != '0'
             AND CAST(e.coordenada_latitud AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
             AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN :lon_min AND :lon_max
            THEN CAST(e.coordenada_longitud AS DECIMAL(10,6))
            ELSE NULL
        END
    ) AS lon,

    COUNT(
        CASE
            WHEN e.coordenada_latitud IS NOT NULL
             AND e.coordenada_latitud != ''
             AND e.coordenada_latitud != '0'
             AND e.coordenada_longitud IS NOT NULL
             AND e.coordenada_longitud != ''
             AND e.coordenada_longitud != '0'
             AND CAST(e.coordenada_latitud AS DECIMAL(10,6)) BETWEEN :lat_min AND :lat_max
             AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN :lon_min AND :lon_max
            THEN 1
            ELSE NULL
        END
    ) AS n_eventos_con_coords

FROM fullclean_contactos.contactos c

JOIN fullclean_contactos.barrios b
    ON b.Id = c.id_barrio

JOIN fullclean_contactos.ciudades ciu
    ON ciu.id = b.id_ciudad
   AND ciu.id_centroope = :ciudad

LEFT JOIN fullclean_contactos.vwEventos e
    ON e.id_contacto = c.id

WHERE c.estado = 1
  AND c.estado_cxc IN (0, 1)
  AND (
        c.cant_obsequios > 0
        OR (
            c.ultimo_obsequio IS NOT NULL
            AND c.ultimo_obsequio != ''
        )
      )

GROUP BY c.id
"""


# ─────────────────────────────────────────────────────────────────────────────
# Funciones internas
# ─────────────────────────────────────────────────────────────────────────────

def _parquet_path(ciudad_id: int) -> Path:
    return _DATOS_DIR / f"coords_co_{ciudad_id}.parquet"


def _validar_ciudad(ciudad_id: int) -> None:
    if ciudad_id not in CIUDADES:
        raise ValueError(
            f"ciudad_id inválido: {ciudad_id}. "
            f"Valores permitidos: {sorted(CIUDADES.keys())}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Funciones públicas
# ─────────────────────────────────────────────────────────────────────────────

def construir_cache_ciudad(ciudad_id: int, verbose: bool = True) -> dict:
    """
    Construye y guarda el Parquet de coordenadas para un CO.
    """
    try:
        _validar_ciudad(ciudad_id)
    except ValueError as e:
        return {
            "ciudad_id": ciudad_id,
            "ok": False,
            "error": str(e),
        }

    nombre = CIUDADES[ciudad_id]

    if verbose:
        print(f"  [{nombre}] consultando BD...", flush=True)

    params = {
        "ciudad": ciudad_id,
        "lat_min": _LAT_MIN,
        "lat_max": _LAT_MAX,
        "lon_min": _LON_MIN,
        "lon_max": _LON_MAX,
    }

    try:
        df = sql_read(
            _SQL_COORDS,
            params=params,
            schema="fullclean_contactos",
        )
    except Exception as e:
        logger.exception("[%s] Error en consulta", nombre)
        return {
            "ciudad": nombre,
            "ciudad_id": ciudad_id,
            "ok": False,
            "error": str(e),
        }

    columnas_esperadas = {
        "id_contacto",
        "lat",
        "lon",
        "n_eventos_con_coords",
    }

    columnas_faltantes = columnas_esperadas - set(df.columns)

    if columnas_faltantes:
        return {
            "ciudad": nombre,
            "ciudad_id": ciudad_id,
            "ok": False,
            "error": (
                "La consulta no devolvió las columnas esperadas. "
                f"Faltan: {sorted(columnas_faltantes)}"
            ),
        }

    if df.empty:
        out_path = _parquet_path(ciudad_id)
        df = pd.DataFrame(
            columns=[
                "id_contacto",
                "lat",
                "lon",
                "n_eventos_con_coords",
            ]
        )
        df.to_parquet(out_path, index=False, compression="snappy")

        if verbose:
            print(f"  [{nombre}] ⚠️ Consulta sin resultados → {out_path.name}")

        return {
            "ciudad": nombre,
            "ciudad_id": ciudad_id,
            "ok": True,
            "total": 0,
            "con_coords": 0,
            "sin_coords": 0,
            "path": str(out_path),
            "size_kb": out_path.stat().st_size // 1024,
        }

    df["id_contacto"] = pd.to_numeric(
        df["id_contacto"],
        errors="coerce",
    ).astype("Int64")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    df["n_eventos_con_coords"] = (
        pd.to_numeric(df["n_eventos_con_coords"], errors="coerce")
        .fillna(0)
        .astype("int32")
    )

    df = df[df["id_contacto"].notna()].copy()
    df["id_contacto"] = df["id_contacto"].astype("int64")

    coords_invalidas = (
        df["lat"].notna()
        & df["lon"].notna()
        & ~(
            df["lat"].between(_LAT_MIN, _LAT_MAX)
            & df["lon"].between(_LON_MIN, _LON_MAX)
        )
    )

    if coords_invalidas.any():
        df.loc[coords_invalidas, ["lat", "lon"]] = pd.NA

    out_path = _parquet_path(ciudad_id)

    df[
        [
            "id_contacto",
            "lat",
            "lon",
            "n_eventos_con_coords",
        ]
    ].to_parquet(
        out_path,
        index=False,
        compression="snappy",
    )

    total = len(df)
    con_coords = int((df["lat"].notna() & df["lon"].notna()).sum())
    sin_coords = total - con_coords
    size_kb = out_path.stat().st_size // 1024

    if verbose:
        pct = round(100 * con_coords / total, 1) if total else 0
        print(
            f"  [{nombre}] ✅ {total:,} contactos "
            f"| {con_coords:,} con coords ({pct}%) "
            f"| {sin_coords:,} sin coords "
            f"| {size_kb} KB → {out_path.name}"
        )

    return {
        "ciudad": nombre,
        "ciudad_id": ciudad_id,
        "ok": True,
        "total": total,
        "con_coords": con_coords,
        "sin_coords": sin_coords,
        "path": str(out_path),
        "size_kb": size_kb,
    }


def construir_cache_todas(
    ciudades: list[int] | None = None,
    workers: int = 4,
) -> list[dict]:
    """
    Construye el cache de coordenadas para todas las ciudades en paralelo.
    """
    targets = ciudades or list(CIUDADES.keys())

    targets_validos = []
    resultados = []

    for cid in targets:
        if cid in CIUDADES:
            targets_validos.append(cid)
        else:
            resultados.append(
                {
                    "ciudad_id": cid,
                    "ok": False,
                    "error": (
                        f"ciudad_id inválido: {cid}. "
                        f"Valores permitidos: {sorted(CIUDADES.keys())}"
                    ),
                }
            )

    if not targets_validos:
        print("⚠️ No hay ciudades válidas para procesar.")
        return resultados

    workers = max(1, min(workers, len(targets_validos)))

    print(
        f"\n🗺️ Construyendo cache de coordenadas — "
        f"{len(targets_validos)} ciudades, {workers} hilos paralelos"
    )
    print("─" * 70)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futuros = {
            pool.submit(construir_cache_ciudad, cid): cid
            for cid in targets_validos
        }

        for futuro in as_completed(futuros):
            cid = futuros[futuro]

            try:
                resultado = futuro.result()
            except Exception as e:
                logger.exception("Error inesperado en ciudad %s", cid)
                resultado = {
                    "ciudad_id": cid,
                    "ciudad": CIUDADES.get(cid, str(cid)),
                    "ok": False,
                    "error": str(e),
                }

            resultados.append(resultado)

    print("─" * 70)

    exitosos = [r for r in resultados if r.get("ok")]
    total_contactos = sum(r.get("total", 0) or 0 for r in exitosos)
    total_coords = sum(r.get("con_coords", 0) or 0 for r in exitosos)

    print(f"✅ Completado: {len(exitosos)}/{len(targets_validos)} ciudades válidas")
    print(f"   {total_contactos:,} contactos totales | {total_coords:,} con coordenadas")

    fallidos = [r for r in resultados if not r.get("ok")]

    if fallidos:
        print("\n⚠️ Ciudades con error:")
        for r in fallidos:
            print(f"   - {r.get('ciudad', r.get('ciudad_id'))}: {r.get('error')}")

    return resultados


def cargar_coords(ciudad_id: int) -> pd.DataFrame:
    """
    Carga el Parquet de coordenadas para una ciudad.
    """
    _validar_ciudad(ciudad_id)

    path = _parquet_path(ciudad_id)

    if not path.exists():
        nombre = CIUDADES.get(ciudad_id, str(ciudad_id))
        print(
            f"⚠️ Cache no encontrado para {nombre}. "
            f"Ejecuta: construir_cache_ciudad({ciudad_id})"
        )
        return pd.DataFrame(
            columns=[
                "id_contacto",
                "lat",
                "lon",
                "n_eventos_con_coords",
            ]
        )

    return pd.read_parquet(path)


def buscar_coords(
    ids_contacto: Sequence[int],
    ciudad_id: int,
) -> pd.DataFrame:
    """
    Devuelve las coordenadas para una lista de IDs de contacto.

    Solo retorna filas con lat/lon válidas.
    """
    if not ids_contacto:
        return pd.DataFrame(columns=["id_contacto", "lat", "lon"])

    df_cache = cargar_coords(ciudad_id)

    if df_cache.empty:
        return df_cache[["id_contacto", "lat", "lon"]]

    ids_normalizados = pd.Series(ids_contacto).dropna().astype("int64").unique()

    mascara = (
        df_cache["id_contacto"].isin(ids_normalizados)
        & df_cache["lat"].notna()
        & df_cache["lon"].notna()
    )

    return (
        df_cache.loc[mascara, ["id_contacto", "lat", "lon"]]
        .reset_index(drop=True)
    )


def estado_cache() -> pd.DataFrame:
    """
    Devuelve un DataFrame con el estado actual del cache por ciudad.
    """
    filas = []

    for cid, nombre in CIUDADES.items():
        path = _parquet_path(cid)

        if path.exists():
            df = pd.read_parquet(path)

            con_coords = int((df["lat"].notna() & df["lon"].notna()).sum())
            total = len(df)

            filas.append(
                {
                    "ciudad": nombre,
                    "id_centroope": cid,
                    "total": total,
                    "con_coords": con_coords,
                    "pct_coords": round(100 * con_coords / total, 1)
                    if total
                    else 0,
                    "size_kb": path.stat().st_size // 1024,
                    "modificado": pd.Timestamp(path.stat().st_mtime, unit="s"),
                    "path": str(path),
                }
            )
        else:
            filas.append(
                {
                    "ciudad": nombre,
                    "id_centroope": cid,
                    "total": None,
                    "con_coords": None,
                    "pct_coords": None,
                    "size_kb": None,
                    "modificado": None,
                    "path": str(path),
                }
            )

    return pd.DataFrame(filas)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Construir cache de coordenadas de clientes"
    )

    parser.add_argument(
        "--ciudad",
        type=int,
        nargs="+",
        help="id_centroope(s) a procesar. Ej: 3 o 3 2 4. Default: todas.",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Hilos paralelos. Default: 4.",
    )

    parser.add_argument(
        "--estado",
        action="store_true",
        help="Mostrar estado actual del cache sin reconstruir.",
    )

    args = parser.parse_args()

    if args.estado:
        df_estado = estado_cache()
        print("\n📊 Estado del cache de coordenadas:")
        print(df_estado.to_string(index=False))
    else:
        construir_cache_todas(
            ciudades=args.ciudad,
            workers=args.workers,
        )