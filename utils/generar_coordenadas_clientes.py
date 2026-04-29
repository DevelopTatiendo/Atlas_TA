# utils/generar_coordenadas_clientes.py
"""
Genera archivos CSV de coordenadas medianas por CO para la base de clientes.

CRITERIO DE INCLUSIÓN:
  - contactos.estado_cxc IN (0, 1)
  - OR contactos.cant_obsequios > 0  (ha recibido al menos un obsequio)

MÉTODO DE COORDENADA (mediana):
  - Más robusta que media aritmética ante GPS erróneos o lecturas en ciudad equivocada
  - Filtra coords fuera de bounds Colombia: lat [-4.5, 12.5] lon [-82.0, -66.0]
  - Si no tiene eventos válidos → lat=NULL, lon=NULL

OUTPUT:
  static/datos/coordenadas/clientes_co_{id}_{nombre}.csv
  Columnas: id_contacto, lat, lon, n_eventos

EJECUCIÓN:
  python -m utils.generar_coordenadas_clientes              # todos los COs
  python -m utils.generar_coordenadas_clientes --co 2       # solo CO 2
  python -m utils.generar_coordenadas_clientes --workers 4  # workers paralelos
"""

import sys
import argparse
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# ── Bootstrap de credenciales ─────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.secrets_manager import load_env_secure
load_env_secure()
from pre_procesamiento.db_utils import sql_read

# ── Configuración ─────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "static" / "datos" / "coordenadas"

LAT_MIN, LAT_MAX =  -4.5, 12.5
LON_MIN, LON_MAX = -82.0, -66.0

# Campos confirmados de la tabla contactos
CAMPO_CANT_OBSEQUIOS = "cant_obsequios"    # INT — > 0 significa que recibió obsequio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# QUERIES
# =============================================================================

def get_centros_operacion() -> pd.DataFrame:
    """Devuelve todos los COs activos con id y nombre."""
    return sql_read(
        """
        SELECT id, descripcion
        FROM fullclean_general.centroope
        ORDER BY id
        """,
        schema="fullclean_general",
    )


def get_clientes_por_co(id_co: int) -> pd.DataFrame:
    """
    Clientes activos del CO indicado.
    Criterio: estado_cxc IN (0,1) O cant_obsequios > 0.
    """
    query = """
        SELECT DISTINCT co.id AS id_contacto
        FROM fullclean_contactos.contactos co
        INNER JOIN fullclean_contactos.ciudades ciu
            ON ciu.id = co.id_ciudad
        WHERE ciu.id_centroope = %s
          AND (
              co.estado_cxc IN (0, 1)
              OR co.cant_obsequios > 0
          )
    """
    return sql_read(query, params=[id_co], schema="fullclean_contactos")


def get_eventos_por_ids(ids: list[int]) -> pd.DataFrame:
    """
    Devuelve todos los eventos con coordenadas válidas para una lista de ids.
    Filtra coords inválidas (0, vacío, fuera de Colombia).
    """
    if not ids:
        return pd.DataFrame(columns=["id_contacto", "lat", "lon"])

    # MySQL IN() con lista — se construye como placeholders
    placeholders = ",".join(["%s"] * len(ids))
    query = f"""
        SELECT
            e.id_contacto,
            CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
            CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon
        FROM fullclean_contactos.vwEventos e
        WHERE e.id_contacto IN ({placeholders})
          AND e.coordenada_latitud  NOT IN ('', '0')
          AND e.coordenada_longitud NOT IN ('', '0')
          AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN {LON_MIN} AND {LON_MAX}
    """
    return sql_read(query, params=ids, schema="fullclean_contactos")


# =============================================================================
# CÁLCULO DE COORDENADA MEDIANA
# =============================================================================

def calcular_coordenadas_medianas(
    clientes_df: pd.DataFrame,
    eventos_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Para cada id_contacto calcula la mediana de lat y lon de sus eventos.
    Clientes sin eventos válidos quedan con lat=NaN, lon=NaN.
    """
    if eventos_df.empty:
        clientes_df = clientes_df.copy()
        clientes_df["lat"] = np.nan
        clientes_df["lon"] = np.nan
        clientes_df["n_eventos"] = 0
        return clientes_df

    coords = (
        eventos_df
        .groupby("id_contacto")
        .agg(
            lat=("lat", "median"),
            lon=("lon", "median"),
            n_eventos=("lat", "count"),
        )
        .reset_index()
    )

    result = clientes_df.merge(coords, on="id_contacto", how="left")
    result["n_eventos"] = result["n_eventos"].fillna(0).astype(int)
    return result


# =============================================================================
# PROCESAMIENTO POR CO
# =============================================================================

def procesar_co(id_co: int, nombre_co: str) -> tuple[int, pd.DataFrame]:
    """
    Pipeline completo para un CO:
      1. Lee clientes activos del CO
      2. Lee sus eventos con coordenadas válidas
      3. Calcula medianas
      4. Devuelve (id_co, DataFrame)
    """
    log.info(f"[CO {id_co} | {nombre_co}] Cargando clientes...")
    clientes = get_clientes_por_co(id_co)
    n_clientes = len(clientes)
    log.info(f"[CO {id_co} | {nombre_co}] {n_clientes:,} clientes encontrados")

    if n_clientes == 0:
        return id_co, pd.DataFrame(columns=["id_contacto", "lat", "lon", "n_eventos"])

    # Leer eventos en lotes de 10k para no saturar la query
    BATCH = 10_000
    ids = clientes["id_contacto"].tolist()
    batches = [ids[i:i + BATCH] for i in range(0, len(ids), BATCH)]

    eventos_parts = []
    for i, batch in enumerate(batches, 1):
        log.info(f"[CO {id_co}] Eventos lote {i}/{len(batches)}...")
        parte = get_eventos_por_ids(batch)
        eventos_parts.append(parte)

    eventos = pd.concat(eventos_parts, ignore_index=True) if eventos_parts else pd.DataFrame()

    n_con_gps = eventos["id_contacto"].nunique() if not eventos.empty else 0
    log.info(f"[CO {id_co} | {nombre_co}] {n_con_gps:,} clientes con GPS válido")

    result = calcular_coordenadas_medianas(clientes, eventos)
    result["id_centroope"] = id_co
    result["nombre_co"] = nombre_co
    return id_co, result


# =============================================================================
# GUARDAR CSV
# =============================================================================

def guardar_csv(id_co: int, nombre_co: str, df: pd.DataFrame) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    nombre_archivo = f"clientes_co_{id_co}_{nombre_co.lower().replace(' ', '_')}.csv"
    ruta = OUTPUT_DIR / nombre_archivo
    df.to_csv(ruta, index=False)
    log.info(f"[CO {id_co}] Guardado → {ruta} ({len(df):,} filas)")
    return ruta


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Genera CSVs de coordenadas medianas por CO"
    )
    parser.add_argument("--co",      type=int, default=None, help="Procesar solo este CO id")
    parser.add_argument("--workers", type=int, default=4,    help="Threads paralelos (default: 4)")
    args = parser.parse_args()

    # Obtener COs
    cos_df = get_centros_operacion()
    if args.co:
        cos_df = cos_df[cos_df["id"] == args.co]
        if cos_df.empty:
            log.error(f"CO {args.co} no encontrado en centroope")
            sys.exit(1)

    log.info(f"Procesando {len(cos_df)} CO(s) con {args.workers} worker(s)...")

    resultados = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(procesar_co, row["id"], row["descripcion"]): row
            for _, row in cos_df.iterrows()
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                id_co, df = future.result()
                resultados[id_co] = (row["descripcion"], df)
            except Exception as e:
                log.error(f"Error en CO {row['id']} ({row['descripcion']}): {e}")

    # Guardar CSVs individuales
    archivos = []
    for id_co, (nombre_co, df) in sorted(resultados.items()):
        ruta = guardar_csv(id_co, nombre_co, df)
        archivos.append(ruta)

    # Resumen global
    print("\n" + "="*60)
    print("RESUMEN FINAL")
    print("="*60)
    total = 0
    for id_co, (nombre_co, df) in sorted(resultados.items()):
        con_gps = df["lat"].notna().sum()
        print(f"  CO {id_co:2d} {nombre_co:<20} {len(df):>8,} clientes | {con_gps:>7,} con GPS")
        total += len(df)
    print(f"  {'TOTAL':<24} {total:>8,} clientes")
    print(f"\n  Archivos en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
