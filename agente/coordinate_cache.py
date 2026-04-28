"""coordinate_cache.py — Cache local de coordenadas de clientes.

Construye un mapa simulado acumulativo de la operación:
  · coordenada_latitud / coordenada_longitud en vwEventos son VARCHAR — se castean a float
  · id_autor en vwEventos es el promotor (no id_promotor)
  · id_centroope llega por JOIN: vwContactos → ciudades → centroope
  · barrios.barrio  es el nombre del barrio (no .nombre)

Cada generación de mapa enriquece el cache automáticamente.
El cache NUNCA escribe en la BD — solo lee.
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_CACHE_PATH = _ROOT / "static" / "datos" / "coordinate_cache.json"


def _to_float(val) -> float | None:
    """Convierte VARCHAR de coordenada a float, filtrando valores inválidos."""
    try:
        v = float(str(val).replace(",", ".").strip())
        return v if v != 0 else None
    except (ValueError, TypeError):
        return None


class CoordinateCache:
    """Cache persistente de coordenadas de clientes y barrios.
    Acumula con cada uso — no reemplaza.
    """

    def __init__(self, path: str | Path = _CACHE_PATH):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._cargar()

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _cargar(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "version": "2.0",
            "ultima_actualizacion": None,
            "n_actualizaciones": 0,
            "clientes": {},   # id_contacto → {id_barrio, nombre_barrio, coordenadas:[]}
            "barrios": {},    # id_barrio   → {nombre, centroide, bbox, coords_observadas:[]}
        }

    def guardar(self):
        self._data["ultima_actualizacion"] = datetime.now().isoformat()
        self.path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    # ── Actualización desde BD ────────────────────────────────────────────────

    def actualizar_desde_bd(
        self,
        ciudad: int = 3,
        fecha_inicio: str = "2026-01-01",
        fecha_fin: str | None = None,
        verbose: bool = True,
    ) -> dict:
        """Consulta vwEventos y enriquece el cache con coordenadas reales.

        Columnas correctas según diccionario:
          vwEventos.coordenada_latitud / coordenada_longitud (VARCHAR → cast)
          vwEventos.id_autor  → el promotor
          id_centroope llega por: vwContactos.id_ciudad → ciudades.id_centroope
        """
        from pre_procesamiento.db_utils import sql_read

        if fecha_fin is None:
            fecha_fin = datetime.today().strftime("%Y-%m-%d")

        query = """
            SELECT
                e.id_contacto,
                con.id_barrio,
                bar.barrio          AS nombre_barrio,
                e.id_autor          AS id_promotor,
                e.fecha_evento,
                CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
                CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon
            FROM fullclean_contactos.vwEventos e
            INNER JOIN fullclean_contactos.vwContactos  con ON con.id          = e.id_contacto
            INNER JOIN fullclean_contactos.ciudades     ciu ON ciu.id          = con.id_ciudad
            LEFT  JOIN fullclean_contactos.barrios      bar ON bar.Id          = con.id_barrio
            WHERE ciu.id_centroope    = :ciudad
              AND e.fecha_evento      BETWEEN :fi AND :ff
              AND e.coordenada_latitud  IS NOT NULL
              AND e.coordenada_latitud  != ''
              AND e.coordenada_latitud  != '0'
            ORDER BY e.fecha_evento DESC
        """

        df = sql_read(
            query,
            params={"ciudad": ciudad, "fi": fecha_inicio, "ff": fecha_fin},
            schema="fullclean_contactos",
        )

        if df.empty:
            if verbose:
                print(f"[Cache] Sin eventos con coordenadas para ciudad {ciudad}")
            return {"nuevos": 0, "actualizados": 0,
                    "total_clientes": len(self._data["clientes"])}

        return self._procesar_df(df, verbose=verbose)

    def actualizar_desde_eventos_df(self, df: pd.DataFrame) -> dict:
        """Enriquece el cache desde un DataFrame ya cargado (llamado por mapa_muestras).

        Acepta columnas con nombres de vwEventos o nombres internos del proyecto.
        Coordenadas pueden ser string o numeric — se convierten automáticamente.
        """
        # Mapeo flexible de columnas
        def _find(aliases):
            for a in aliases:
                if a in df.columns:
                    return a
            return None

        col_id   = _find(["id_contacto"])
        col_lat  = _find(["coordenada_latitud", "lat", "latitud"])
        col_lon  = _find(["coordenada_longitud", "lon", "longitud"])
        col_fecha = _find(["fecha_evento", "fecha"])
        col_bar  = _find(["id_barrio"])
        col_pro  = _find(["id_autor", "id_promotor"])

        if not col_id or not col_lat or not col_lon:
            return {"error": "DataFrame sin columnas mínimas"}

        df2 = df[[c for c in [col_id, col_lat, col_lon, col_fecha, col_bar, col_pro]
                  if c]].copy()
        df2 = df2.rename(columns={
            col_id: "id_contacto", col_lat: "lat", col_lon: "lon",
            **({"id_contacto": "id_contacto"}),
        })
        if col_fecha:  df2 = df2.rename(columns={col_fecha: "fecha_evento"})
        if col_bar:    df2 = df2.rename(columns={col_bar: "id_barrio"})
        if col_pro:    df2 = df2.rename(columns={col_pro: "id_promotor"})

        return self._procesar_df(df2, verbose=False)

    # ── Core de procesamiento ─────────────────────────────────────────────────

    def _procesar_df(self, df: pd.DataFrame, verbose: bool = True) -> dict:
        nuevos = actualizados = 0

        for _, row in df.iterrows():
            lat = _to_float(row.get("lat") or row.get("coordenada_latitud"))
            lon = _to_float(row.get("lon") or row.get("coordenada_longitud"))

            if lat is None or lon is None:
                continue
            if not (1 <= abs(lat) <= 12) or not (70 <= abs(lon) <= 82):
                continue  # fuera de Colombia

            try:
                id_c = str(int(row["id_contacto"]))
            except (ValueError, TypeError):
                continue

            fecha = str(row.get("fecha_evento", ""))[:10]
            prom  = None
            try:
                prom = int(row["id_promotor"]) if pd.notna(row.get("id_promotor")) else None
            except (ValueError, TypeError, KeyError):
                pass

            barr = None
            try:
                barr = int(row["id_barrio"]) if pd.notna(row.get("id_barrio")) else None
            except (ValueError, TypeError, KeyError):
                pass

            nombre_barrio = str(row.get("nombre_barrio", "")) if "nombre_barrio" in df.columns else ""

            coord = {"lat": round(lat, 6), "lon": round(lon, 6),
                     "fecha": fecha, "id_promotor": prom}

            if id_c not in self._data["clientes"]:
                self._data["clientes"][id_c] = {
                    "id_contacto": int(row["id_contacto"]),
                    "id_barrio": barr,
                    "nombre_barrio": nombre_barrio,
                    "coordenadas": [coord],
                    "n_visitas_con_coord": 1,
                }
                nuevos += 1
            else:
                cl = self._data["clientes"][id_c]
                existe = any(
                    c["lat"] == coord["lat"] and c["lon"] == coord["lon"] and c["fecha"] == fecha
                    for c in cl["coordenadas"]
                )
                if not existe:
                    cl["coordenadas"].append(coord)
                    cl["n_visitas_con_coord"] = len(cl["coordenadas"])
                    actualizados += 1
                if nombre_barrio and not cl.get("nombre_barrio"):
                    cl["nombre_barrio"] = nombre_barrio

            # Acumular barrio
            if barr:
                id_b = str(barr)
                if id_b not in self._data["barrios"]:
                    self._data["barrios"][id_b] = {
                        "id_barrio": barr,
                        "nombre": nombre_barrio,
                        "coordenadas_observadas": [],
                    }
                b = self._data["barrios"][id_b]
                if not any(c["lat"] == coord["lat"] and c["lon"] == coord["lon"]
                           for c in b["coordenadas_observadas"]):
                    b["coordenadas_observadas"].append(
                        {"lat": coord["lat"], "lon": coord["lon"]}
                    )
                if nombre_barrio and not b.get("nombre"):
                    b["nombre"] = nombre_barrio

        self._recalcular_barrios()
        if nuevos > 0 or actualizados > 0:
            self._data["n_actualizaciones"] = self._data.get("n_actualizaciones", 0) + 1
            self.guardar()

        resumen = {
            "clientes_nuevos": nuevos,
            "clientes_actualizados": actualizados,
            "total_clientes": len(self._data["clientes"]),
            "total_barrios": len(self._data["barrios"]),
        }
        if verbose:
            print(f"[Cache] +{nuevos} nuevos, ~{actualizados} actualizados "
                  f"→ {resumen['total_clientes']} clientes, "
                  f"{resumen['total_barrios']} barrios")
        return resumen

    # ── Geometría ─────────────────────────────────────────────────────────────

    def _recalcular_barrios(self):
        for id_b, b in self._data["barrios"].items():
            coords = b.get("coordenadas_observadas", [])
            if not coords:
                continue
            lats = [c["lat"] for c in coords]
            lons = [c["lon"] for c in coords]
            b["centroide"] = {"lat": round(sum(lats)/len(lats), 6),
                              "lon": round(sum(lons)/len(lons), 6)}
            b["bbox"] = {"lat_min": min(lats), "lat_max": max(lats),
                         "lon_min": min(lons), "lon_max": max(lons)}
            b["n_coordenadas"] = len(coords)

    @staticmethod
    def _dist_km(lat1, lon1, lat2, lon2) -> float:
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + \
            math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(min(1.0, a)))

    # ── Consultas ─────────────────────────────────────────────────────────────

    def obtener_cliente(self, id_contacto: int) -> dict | None:
        data = self._data["clientes"].get(str(id_contacto))
        if data and data.get("coordenadas"):
            sorted_c = sorted(data["coordenadas"], key=lambda x: x.get("fecha", ""), reverse=True)
            data = dict(data)
            data["coordenada_principal"] = sorted_c[0]
        return data

    def obtener_barrio(self, id_barrio: int) -> dict | None:
        return self._data["barrios"].get(str(id_barrio))

    def clientes_en_bbox(self, lat_min, lat_max, lon_min, lon_max) -> list[dict]:
        result = []
        for id_c, cl in self._data["clientes"].items():
            if not cl.get("coordenadas"):
                continue
            c = sorted(cl["coordenadas"], key=lambda x: x.get("fecha", ""), reverse=True)[0]
            if lat_min <= c["lat"] <= lat_max and lon_min <= c["lon"] <= lon_max:
                result.append({"id_contacto": cl["id_contacto"],
                                "nombre_barrio": cl.get("nombre_barrio", ""),
                                "lat": c["lat"], "lon": c["lon"]})
        return result

    def area_cubierta(self, coords: list[dict]) -> dict:
        """Calcula área aprox (km²) y distancia total de una lista de coordenadas."""
        if not coords:
            return {}
        lats = [c["lat"] for c in coords]
        lons = [c["lon"] for c in coords]
        dlat = self._dist_km(min(lats), min(lons), max(lats), min(lons))
        dlon = self._dist_km(min(lats), min(lons), min(lats), max(lons))
        dist = sum(
            self._dist_km(coords[i-1]["lat"], coords[i-1]["lon"],
                          coords[i]["lat"], coords[i]["lon"])
            for i in range(1, len(coords))
        )
        return {
            "area_bbox_km2": round(dlat * dlon, 2),
            "distancia_total_km": round(dist, 2),
            "n_puntos": len(coords),
            "centro": {"lat": round(sum(lats)/len(lats), 6),
                       "lon": round(sum(lons)/len(lons), 6)},
            "bbox": {"lat_min": min(lats), "lat_max": max(lats),
                     "lon_min": min(lons), "lon_max": max(lons)},
        }

    def stats(self) -> dict:
        n = len(self._data["clientes"])
        total_coords = sum(len(c.get("coordenadas", [])) for c in self._data["clientes"].values())
        return {
            "clientes_en_cache": n,
            "barrios_mapeados": len(self._data["barrios"]),
            "total_coordenadas_acumuladas": total_coords,
            "promedio_coords_por_cliente": round(total_coords / n, 1) if n else 0,
            "ultima_actualizacion": self._data.get("ultima_actualizacion"),
            "n_actualizaciones": self._data.get("n_actualizaciones", 0),
            "archivo": str(self.path),
        }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from config.secrets_manager import load_env_secure
    load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                    pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ciudad", type=int, default=3)
    parser.add_argument("--desde", type=str, default="2026-01-01")
    parser.add_argument("--hasta", type=str, default=None)
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    cache = CoordinateCache()
    if args.stats:
        print(json.dumps(cache.stats(), indent=2))
    else:
        print(f"Actualizando cache ciudad={args.ciudad} desde {args.desde}...")
        r = cache.actualizar_desde_bd(args.ciudad, args.desde, args.hasta)
        print(json.dumps(r, indent=2))
        print("\nEstadísticas:")
        print(json.dumps(cache.stats(), indent=2))
