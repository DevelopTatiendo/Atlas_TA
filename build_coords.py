"""
build_coords.py — Script simple para construir el cache de coordenadas por CO.

Corre desde la raíz del proyecto con VPN activa:
    python build_coords.py

O solo una ciudad:
    python build_coords.py --ciudad 2

Lo que hace:
  1. Prueba conexión a la BD con un SELECT 1
  2. Por cada CO: consulta lat/lon promedio de cada cliente desde vwEventos
  3. Guarda static/datos/coords_co_{id}.parquet
"""

import sys
import os
import traceback
from pathlib import Path

# ── Cargar credenciales ───────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

from config.secrets_manager import load_env_secure
load_env_secure(
    prefer_plain=(_ROOT / ".env").exists(),
    enc_path=str(_ROOT / "config" / ".env.enc"),
    pass_env_var="MAPAS_SECRET_PASSPHRASE",
    cache=False,
)

import pandas as pd
from pre_procesamiento.db_utils import sql_read

# ── Configuración ─────────────────────────────────────────────────────────────
CIUDADES = {
    2: "Cali",
    3: "Medellín",
    4: "Bogotá",
    5: "Pereira",
    6: "Manizales",
    7: "Bucaramanga",
    8: "Barranquilla",
}

OUT_DIR = _ROOT / "static" / "datos"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LAT_MIN, LAT_MAX = -4.5, 12.5
LON_MIN, LON_MAX = -82.0, -66.0

SQL_TEST = "SELECT 1 AS ok"

SQL_COORDS = """
SELECT
    c.id AS id_contacto,
    AVG(CASE
        WHEN e.coordenada_latitud  IS NOT NULL AND e.coordenada_latitud  != '' AND e.coordenada_latitud  != '0'
         AND e.coordenada_longitud IS NOT NULL AND e.coordenada_longitud != '' AND e.coordenada_longitud != '0'
         AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN {lat_min} AND {lat_max}
         AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN {lon_min} AND {lon_max}
        THEN CAST(e.coordenada_latitud  AS DECIMAL(10,6))
        ELSE NULL
    END) AS lat,
    AVG(CASE
        WHEN e.coordenada_latitud  IS NOT NULL AND e.coordenada_latitud  != '' AND e.coordenada_latitud  != '0'
         AND e.coordenada_longitud IS NOT NULL AND e.coordenada_longitud != '' AND e.coordenada_longitud != '0'
         AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN {lat_min} AND {lat_max}
         AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN {lon_min} AND {lon_max}
        THEN CAST(e.coordenada_longitud AS DECIMAL(10,6))
        ELSE NULL
    END) AS lon
FROM fullclean_contactos.contactos c
JOIN fullclean_contactos.barrios b    ON b.Id  = c.id_barrio
JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = {ciudad}
LEFT JOIN fullclean_contactos.vwEventos e ON e.id_contacto = c.id
WHERE c.estado = 1
  AND c.estado_cxc IN (0, 1)
  AND (c.cant_obsequios > 0 OR (c.ultimo_obsequio IS NOT NULL AND c.ultimo_obsequio != ''))
GROUP BY c.id
"""


def probar_conexion() -> bool:
    print("\n🔌 Probando conexión a la BD...", flush=True)
    try:
        df = sql_read(SQL_TEST, schema="fullclean_contactos")
        print(f"   ✅ Conexión OK — resultado: {df.to_dict(orient='records')}", flush=True)
        return True
    except Exception as e:
        print(f"   ❌ Conexión FALLIDA: {e}", flush=True)
        traceback.print_exc()
        return False


def construir_ciudad(ciudad_id: int) -> dict:
    nombre = CIUDADES[ciudad_id]
    out_path = OUT_DIR / f"coords_co_{ciudad_id}.parquet"
    sql = SQL_COORDS.format(
        ciudad=ciudad_id,
        lat_min=LAT_MIN, lat_max=LAT_MAX,
        lon_min=LON_MIN, lon_max=LON_MAX,
    )

    print(f"\n[{nombre}] Consultando BD...", flush=True)
    print(f"   SQL (primeras 3 líneas):", flush=True)
    for line in sql.strip().splitlines()[:3]:
        print(f"     {line}", flush=True)

    try:
        df = sql_read(sql, schema="fullclean_contactos")
    except Exception as e:
        print(f"   ❌ Error SQL: {e}", flush=True)
        traceback.print_exc()
        return {"ciudad": nombre, "ok": False, "error": str(e)}

    print(f"   Filas recibidas: {len(df)}", flush=True)
    if df.empty:
        print(f"   ⚠️  Sin resultados", flush=True)
        df = pd.DataFrame(columns=["id_contacto", "lat", "lon"])
        df.to_parquet(out_path, index=False)
        return {"ciudad": nombre, "ok": True, "total": 0, "con_coords": 0}

    # Normalizar
    df["id_contacto"] = pd.to_numeric(df["id_contacto"], errors="coerce").astype("Int64")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df[df["id_contacto"].notna()].copy()
    df["id_contacto"] = df["id_contacto"].astype("int64")

    # Guardar solo las tres columnas
    df[["id_contacto", "lat", "lon"]].to_parquet(out_path, index=False, compression="snappy")

    total      = len(df)
    con_coords = int(df["lat"].notna().sum())
    pct        = round(100 * con_coords / total, 1) if total else 0
    kb         = out_path.stat().st_size // 1024

    print(f"   ✅ {total:,} contactos | {con_coords:,} con coords ({pct}%) | {kb} KB → {out_path.name}", flush=True)
    return {"ciudad": nombre, "ok": True, "total": total, "con_coords": con_coords}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Construir cache de coordenadas")
    parser.add_argument("--ciudad", type=int, nargs="+",
                        help="id_centroope(s). Ej: 2 3 4. Default: todas.")
    args = parser.parse_args()

    targets = args.ciudad if args.ciudad else list(CIUDADES.keys())
    invalidos = [c for c in targets if c not in CIUDADES]
    if invalidos:
        print(f"❌ IDs inválidos: {invalidos}. Válidos: {list(CIUDADES.keys())}")
        sys.exit(1)

    print(f"\nCiudades a procesar: {[CIUDADES[c] for c in targets]}")

    if not probar_conexion():
        print("\n❌ Sin conexión a la BD. Verifica que la VPN esté activa.")
        sys.exit(1)

    resultados = []
    for cid in targets:
        r = construir_ciudad(cid)
        resultados.append(r)

    print("\n" + "─" * 60)
    print("RESUMEN:")
    for r in resultados:
        status = "✅" if r.get("ok") else "❌"
        if r.get("ok"):
            print(f"  {status} {r['ciudad']}: {r.get('total',0):,} contactos, {r.get('con_coords',0):,} con coords")
        else:
            print(f"  {status} {r['ciudad']}: {r.get('error','?')}")


if __name__ == "__main__":
    main()
