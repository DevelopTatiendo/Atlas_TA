"""analisis_ruta.py — Análisis profundo por ruta comercial.

Une datos de BD (clientes, visitas, pedidos, llamadas, quejas)
con el cache de coordenadas para producir análisis espaciales
que el agente puede interpretar y narrar.

REGLA: Solo SELECT en BD. Cache local para coordenadas.

Columnas correctas según diccionario:
  rutas_cobro.ruta          (nombre de la ruta — no .nombre)
  barrios.barrio            (nombre del barrio — no .nombre)
  contactos.id              (PK — se aliasa como id_contacto)
  contactos.nombre          (nombre del cliente — no .nombre_contacto)
  vwEventos.id_autor        (promotor — no .id_promotor)
  vwEventos.coordenada_latitud / coordenada_longitud  (VARCHAR — se castean)
  vwEventos: sin id_centroope directo — filtrar via JOIN ciudades
  pedidos_det: sin nombre_producto — usar id_item
  quejas.cat                (categoría de queja — no join inconformidad.nombre)
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pre_procesamiento.db_utils import sql_read
from agente.coordinate_cache import CoordinateCache


# ── HERRAMIENTA 1: Universo de clientes de una ruta ──────────────────────────

def consultar_ruta_completa(
    ciudad: int,
    id_ruta: int | None = None,
    nombre_ruta: str | None = None,
    fecha_inicio: str = "2026-01-01",
    fecha_fin: str | None = None,
) -> dict[str, Any]:
    """Análisis completo de una ruta: quiénes están, quiénes fueron visitados,
    quiénes compraron, quiénes son oportunidad.

    Args:
        ciudad:       id_centroope de la ciudad.
        id_ruta:      ID numérico de la ruta (o usar nombre_ruta).
        nombre_ruta:  Nombre de la ruta (búsqueda parcial LIKE).
        fecha_inicio: Inicio del período.
        fecha_fin:    Fin del período (default: hoy).

    Returns:
        Dict con universo, visitados, convertidos, oportunidades y métricas.
    """
    if fecha_fin is None:
        fecha_fin = datetime.today().strftime("%Y-%m-%d")

    if id_ruta is None and nombre_ruta is None:
        raise ValueError("Debe especificar id_ruta o nombre_ruta")

    # Resolver id_ruta si solo viene nombre
    # rutas_cobro.ruta es el nombre (no .nombre)
    if id_ruta is None:
        rutas_df = sql_read(
            "SELECT id, ruta FROM fullclean_contactos.rutas_cobro "
            "WHERE ruta LIKE :n LIMIT 5",
            params={"n": f"%{nombre_ruta}%"},
            schema="fullclean_contactos",
        )
        if rutas_df.empty:
            return {"error": f"No se encontró ruta con nombre '{nombre_ruta}'"}
        id_ruta = int(rutas_df.iloc[0]["id"])
        nombre_ruta_real = str(rutas_df.iloc[0]["ruta"])
    else:
        df_r = sql_read(
            "SELECT ruta FROM fullclean_contactos.rutas_cobro WHERE id = :id",
            params={"id": id_ruta},
            schema="fullclean_contactos",
        )
        nombre_ruta_real = str(df_r.iloc[0]["ruta"]) if not df_r.empty else str(id_ruta)

    # ── 1. Universo de clientes en la ruta ────────────────────────────────────
    # contactos.id es la PK; se aliasa como id_contacto para el resto del código
    # barrios.barrio es el nombre del barrio (no .nombre)
    df_universo = sql_read(
        """
        SELECT
            c.id                AS id_contacto,
            c.nombre            AS nombre_contacto,
            c.id_categoria,
            cat.categoria       AS nombre_categoria,
            c.id_barrio,
            b.barrio            AS nombre_barrio,
            c.telefono_1
        FROM fullclean_contactos.contactos c
        JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
        JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
        LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
        WHERE rcz.id_ruta_cobro = :ruta
          AND b.id_ciudad = :ciudad
          AND c.id > 0
        """,
        params={"ruta": id_ruta, "ciudad": ciudad},
        schema="fullclean_contactos",
    )

    if df_universo.empty:
        return {"error": f"Ruta {id_ruta} no tiene clientes en ciudad {ciudad}"}

    ids_ruta = df_universo["id_contacto"].tolist()
    ids_str = ",".join(str(i) for i in ids_ruta[:2000])  # límite de seguridad

    # ── 2. Visitas/muestras en el período ─────────────────────────────────────
    # vwEventos.id_autor es el promotor (no id_promotor)
    # vwEventos: coordenada_latitud/longitud son VARCHAR — CAST a decimal
    # No hay id_centroope en vwEventos: ya filtramos por ids_str de la ciudad
    df_visitas = sql_read(
        f"""
        SELECT
            e.id_contacto,
            e.id_autor                                            AS id_promotor,
            p.apellido                                            AS apellido_promotor,
            COUNT(*)                                              AS n_visitas,
            MAX(e.fecha_evento)                                   AS ultima_visita,
            MAX(CAST(e.coordenada_latitud  AS DECIMAL(10,6)))     AS lat,
            MAX(CAST(e.coordenada_longitud AS DECIMAL(10,6)))     AS lon
        FROM fullclean_contactos.vwEventos e
        LEFT JOIN fullclean_personal.personal p ON p.id = e.id_autor
        WHERE e.id_contacto IN ({ids_str})
          AND e.fecha_evento BETWEEN :fi AND :ff
        GROUP BY e.id_contacto, e.id_autor, p.apellido
        """,
        params={"fi": fecha_inicio, "ff": fecha_fin},
        schema="fullclean_contactos",
    )

    # ── 3. Pedidos en el período ───────────────────────────────────────────────
    # pedidos_det NO tiene nombre_producto — usar id_item + JOIN a items si existe
    # Por ahora agrupamos por id_item y obtenemos el top de items pedidos
    df_pedidos = sql_read(
        f"""
        SELECT
            ped.id_contacto,
            COUNT(DISTINCT ped.id)   AS n_pedidos,
            SUM(pd.valor_total)      AS valor_total,
            GROUP_CONCAT(DISTINCT pd.id_item ORDER BY pd.id_item SEPARATOR ',') AS items_ids
        FROM fullclean_telemercadeo.pedidos ped
        LEFT JOIN fullclean_telemercadeo.pedidos_det pd ON pd.id_pedido = ped.id
        WHERE ped.id_contacto IN ({ids_str})
          AND ped.fecha_hora_pedido BETWEEN :fi AND :ff
        GROUP BY ped.id_contacto
        """,
        params={"fi": fecha_inicio, "ff": fecha_fin},
        schema="fullclean_telemercadeo",
    )

    # ── 4. Llamadas en el período ─────────────────────────────────────────────
    df_llamadas = sql_read(
        f"""
        SELECT
            l.id_contacto,
            COUNT(*)                                                        AS n_llamadas,
            SUM(CASE WHEN lr.contestada    = 1 THEN 1 ELSE 0 END)          AS n_contestadas,
            SUM(CASE WHEN lr.contacto_exitoso = 1 THEN 1 ELSE 0 END)       AS n_rpc,
            MIN(l.fecha_inicio_llamada)                                     AS primera_llamada,
            MAX(l.fecha_inicio_llamada)                                     AS ultima_llamada
        FROM fullclean_telemercadeo.llamadas l
        LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta
        WHERE l.id_contacto IN ({ids_str})
          AND l.fecha_inicio_llamada BETWEEN :fi AND :ff
          AND l.id_vendedor NOT IN (0, 1)
        GROUP BY l.id_contacto
        """,
        params={"fi": fecha_inicio, "ff": fecha_fin},
        schema="fullclean_telemercadeo",
    )

    # ── 5. Quejas por cliente en la ruta ─────────────────────────────────────
    # quejas.cat es la categoría (no JOIN inconformidad.nombre)
    df_quejas = sql_read(
        f"""
        SELECT
            q.id_contacto,
            COUNT(*)                                    AS n_quejas,
            GROUP_CONCAT(DISTINCT q.cat SEPARATOR ', ') AS tipos_queja
        FROM fullclean_quejas.quejas q
        WHERE q.id_contacto IN ({ids_str})
        GROUP BY q.id_contacto
        """,
        schema="fullclean_quejas",
    )

    # ── Cruzar todo ───────────────────────────────────────────────────────────
    df = df_universo.copy()
    if not df_visitas.empty:
        df = df.merge(
            df_visitas[["id_contacto", "apellido_promotor", "n_visitas", "ultima_visita", "lat", "lon"]],
            on="id_contacto", how="left",
        )
    else:
        df[["apellido_promotor", "n_visitas", "ultima_visita", "lat", "lon"]] = None

    if not df_pedidos.empty:
        df = df.merge(
            df_pedidos[["id_contacto", "n_pedidos", "valor_total", "items_ids"]],
            on="id_contacto", how="left",
        )
    else:
        df[["n_pedidos", "valor_total", "items_ids"]] = None

    if not df_llamadas.empty:
        df = df.merge(
            df_llamadas[["id_contacto", "n_llamadas", "n_contestadas", "n_rpc"]],
            on="id_contacto", how="left",
        )
    else:
        df[["n_llamadas", "n_contestadas", "n_rpc"]] = None

    if not df_quejas.empty:
        df = df.merge(
            df_quejas[["id_contacto", "n_quejas", "tipos_queja"]],
            on="id_contacto", how="left",
        )
    else:
        df[["n_quejas", "tipos_queja"]] = None

    # Enriquecer con cache de coordenadas (rellenar faltantes)
    cache = CoordinateCache()
    for col in ["lat", "lon"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    def _coord_desde_cache(id_c):
        c = cache.obtener_cliente(int(id_c))
        if c and c.get("coordenada_principal"):
            return c["coordenada_principal"]["lat"], c["coordenada_principal"]["lon"]
        return None, None

    mask = df["lat"].isna() | (df["lat"] == 0)
    if mask.any():
        coords_cache = df.loc[mask, "id_contacto"].apply(_coord_desde_cache)
        df.loc[mask, "lat"] = [c[0] for c in coords_cache]
        df.loc[mask, "lon"] = [c[1] for c in coords_cache]

    # ── Clasificar clientes ───────────────────────────────────────────────────
    df["fue_visitado"] = df["n_visitas"].notna() & (df["n_visitas"] > 0)
    df["genero_pedido"] = df["n_pedidos"].notna() & (df["n_pedidos"] > 0)
    df["es_nofiel"] = ~df["nombre_categoria"].isin(["Ticket", "Frecuente"])
    df["tiene_coord"] = df["lat"].notna() & (df["lat"] != 0)

    # ── Métricas de la ruta ───────────────────────────────────────────────────
    total = len(df)
    visitados = int(df["fue_visitado"].sum())
    con_pedido = int(df["genero_pedido"].sum())
    sin_visitar = total - visitados
    no_fieles_sin_visitar = int(df[~df["fue_visitado"] & df["es_nofiel"]].shape[0])
    con_queja = int(df["n_quejas"].notna().sum())

    metricas = {
        "total_clientes_ruta": int(total),
        "visitados_periodo": visitados,
        "pct_cobertura": round(100 * visitados / total, 1) if total else 0,
        "con_pedido": con_pedido,
        "pct_conversion": round(100 * con_pedido / visitados, 1) if visitados else 0,
        "sin_visitar": int(sin_visitar),
        "no_fieles_sin_visitar": no_fieles_sin_visitar,
        "clientes_con_queja": con_queja,
        "pct_con_coordenada": round(100 * df["tiene_coord"].sum() / total, 1) if total else 0,
    }

    # ── Análisis de items más pedidos (sin nombre de producto aún) ───────────
    top_items = []
    if not df_pedidos.empty and "items_ids" in df_pedidos.columns:
        from collections import Counter
        todos_items = []
        for items in df_pedidos["items_ids"].dropna():
            todos_items.extend([i.strip() for i in str(items).split(",") if i.strip()])
        top_items = [{"id_item": k, "n_pedidos": v}
                     for k, v in Counter(todos_items).most_common(10)]

    # ── Barrios de la ruta ────────────────────────────────────────────────────
    barrios_resumen = (
        df.groupby("nombre_barrio", dropna=False)
        .agg(
            n_clientes=("id_contacto", "count"),
            n_visitados=("fue_visitado", "sum"),
            n_con_pedido=("genero_pedido", "sum"),
            n_quejas=("n_quejas", "sum"),
        )
        .reset_index()
        .sort_values("n_clientes", ascending=False)
        .head(20)
        .to_dict(orient="records")
    )

    # ── Oportunidades: no fieles, sin visitar y con coordenada ───────────────
    oportunidades = (
        df[~df["fue_visitado"] & df["es_nofiel"] & df["tiene_coord"]]
        [["id_contacto", "nombre_contacto", "nombre_barrio", "nombre_categoria", "lat", "lon"]]
        .head(50)
        .to_dict(orient="records")
    )

    # ── Promotores que operaron en la ruta ────────────────────────────────────
    promotores_ruta = []
    if "apellido_promotor" in df.columns:
        promotores_ruta = (
            df[df["fue_visitado"]]
            .groupby("apellido_promotor", dropna=False)
            .agg(clientes_visitados=("id_contacto", "count"))
            .reset_index()
            .to_dict(orient="records")
        )

    return {
        "ruta": {"id": id_ruta, "nombre": nombre_ruta_real},
        "ciudad": ciudad,
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "metricas": metricas,
        "barrios": barrios_resumen,
        "promotores": promotores_ruta,
        "top_items": top_items,
        "oportunidades_geolocalizadas": oportunidades,
    }


# ── HERRAMIENTA 2: Listar rutas de una ciudad ─────────────────────────────────

def listar_rutas_ciudad(ciudad: int, fecha_inicio: str = "2026-01-01") -> list[dict]:
    """Lista rutas activas de una ciudad con métricas de actividad reciente.

    IMPORTANTE: ciudad es id_centroope (no id_ciudad directa).
    El filtro correcto pasa por: rutas_cobro_zonas → barrios → ciudades.id_centroope

    Args:
        ciudad:       id_centroope de la ciudad (3=Medellín, 2=Cali, 4=Bogotá...).
        fecha_inicio: Desde cuándo medir visitados y pedidos recientes.

    Returns:
        Lista ordenada por % cobertura desc, con:
        id, ruta, n_barrios, n_clientes, visitados_periodo, pct_cobertura,
        con_pedido_periodo, ultima_visita, sin_visitar (oportunidad directa)
    """
    # Un JOIN entre vwEventos y pedidos en la misma query puede ser lento.
    # Usamos subconsultas correlacionadas para mantener el plan de ejecución limpio.
    df = sql_read(
        """
        SELECT
            rc.id,
            rc.ruta,
            COUNT(DISTINCT rcz.id_barrio)                               AS n_barrios,
            COUNT(DISTINCT c.id)                                        AS n_clientes,
            COUNT(DISTINCT e.id_contacto)                               AS visitados_periodo,
            ROUND(
                100.0 * COUNT(DISTINCT e.id_contacto)
                      / NULLIF(COUNT(DISTINCT c.id), 0)
            , 1)                                                        AS pct_cobertura,
            COUNT(DISTINCT ped.id_contacto)                             AS con_pedido_periodo,
            MAX(e.fecha_evento)                                         AS ultima_visita,
            COUNT(DISTINCT c.id) - COUNT(DISTINCT e.id_contacto)       AS sin_visitar
        FROM fullclean_contactos.rutas_cobro rc
        LEFT  JOIN fullclean_contactos.rutas_cobro_zonas rcz
              ON rcz.id_ruta_cobro = rc.id
        LEFT  JOIN fullclean_contactos.barrios b
              ON b.Id = rcz.id_barrio
        INNER JOIN fullclean_contactos.ciudades ciu
              ON ciu.id = b.id_ciudad
             AND ciu.id_centroope = :ciudad
        LEFT  JOIN fullclean_contactos.contactos c
              ON c.id_barrio = b.Id
        LEFT  JOIN fullclean_contactos.vwEventos e
              ON e.id_contacto = c.id
             AND e.fecha_evento >= :fi
        LEFT  JOIN fullclean_telemercadeo.pedidos ped
              ON ped.id_contacto = c.id
             AND ped.fecha_hora_pedido >= :fi
        GROUP BY rc.id, rc.ruta
        HAVING n_clientes > 0
        ORDER BY pct_cobertura DESC, n_clientes DESC
        """,
        params={"ciudad": ciudad, "fi": fecha_inicio},
        schema="fullclean_contactos",
    )
    return df.to_dict(orient="records")


# ── HERRAMIENTA 3: Análisis de zona del promotor ─────────────────────────────

def analizar_zona_promotor(
    id_promotor: int,
    ciudad: int,
    fecha_inicio: str,
    fecha_fin: str | None = None,
) -> dict[str, Any]:
    """Analiza el área real recorrida por un promotor: territorio cubierto,
    densidad de clientes, oportunidades no tocadas, patrones de pedido,
    señales de quejas en la zona.

    El agente puede narrar:
    'La promotora X recorrió Z km² en N días. En esa zona hay K clientes
    sin visitar. El producto más pedido fue P. Hay Q quejas activas.'
    """
    if fecha_fin is None:
        fecha_fin = datetime.today().strftime("%Y-%m-%d")

    # Visitas del promotor con coordenadas
    # id_autor es el promotor en vwEventos (no id_promotor)
    # id_centroope no está en vwEventos — JOIN a vwContactos → ciudades
    df_visitas = sql_read(
        """
        SELECT
            e.id_contacto,
            c.nombre            AS nombre_contacto,
            c.id_barrio,
            b.barrio            AS nombre_barrio,
            c.id_categoria,
            cat.categoria       AS nombre_categoria,
            e.fecha_evento,
            CAST(e.coordenada_latitud  AS DECIMAL(10,6))  AS lat,
            CAST(e.coordenada_longitud AS DECIMAL(10,6))  AS lon
        FROM fullclean_contactos.vwEventos e
        JOIN fullclean_contactos.vwContactos c ON c.id = e.id_contacto
        INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = c.id_ciudad
        LEFT JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
        LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
        WHERE e.id_autor = :prom
          AND ciu.id_centroope = :ciudad
          AND e.fecha_evento BETWEEN :fi AND :ff
          AND e.coordenada_latitud IS NOT NULL
          AND e.coordenada_latitud != ''
          AND e.coordenada_latitud != '0'
        ORDER BY e.fecha_evento
        """,
        params={"prom": id_promotor, "ciudad": ciudad,
                "fi": fecha_inicio, "ff": fecha_fin},
        schema="fullclean_contactos",
    )

    if df_visitas.empty:
        return {"error": f"Promotor {id_promotor} sin visitas con coordenadas en el período"}

    # Calcular área cubierta usando el cache
    cache = CoordinateCache()
    coords = [
        {"lat": float(r["lat"]), "lon": float(r["lon"])}
        for _, r in df_visitas.iterrows()
        if pd.notna(r["lat"]) and pd.notna(r["lon"])
    ]
    area_info = cache.area_cubierta(coords)

    # Clientes en esa zona (del cache) que no fueron visitados
    clientes_zona: list[dict] = []
    clientes_no_visitados_zona: list[dict] = []
    if area_info.get("bbox"):
        bbox = area_info["bbox"]
        clientes_zona = cache.clientes_en_bbox(
            bbox["lat_min"], bbox["lat_max"],
            bbox["lon_min"], bbox["lon_max"],
        )
        ids_visitados = set(df_visitas["id_contacto"].astype(str).tolist())
        clientes_no_visitados_zona = [
            c for c in clientes_zona
            if str(c["id_contacto"]) not in ids_visitados
        ]

    # Pedidos de los clientes visitados
    # pedidos_det NO tiene nombre_producto — se usa id_item
    df_pedidos: pd.DataFrame = pd.DataFrame()
    df_quejas_zona: pd.DataFrame = pd.DataFrame()

    if not df_visitas.empty:
        ids_str = ",".join(df_visitas["id_contacto"].astype(str).unique()[:500].tolist())

        df_pedidos = sql_read(
            f"""
            SELECT
                pd.id_item,
                COUNT(DISTINCT ped.id) AS n_pedidos,
                SUM(pd.valor_total)    AS valor_total
            FROM fullclean_telemercadeo.pedidos ped
            JOIN fullclean_telemercadeo.pedidos_det pd ON pd.id_pedido = ped.id
            WHERE ped.id_contacto IN ({ids_str})
              AND ped.fecha_hora_pedido BETWEEN :fi AND :ff
            GROUP BY pd.id_item
            ORDER BY n_pedidos DESC
            LIMIT 15
            """,
            params={"fi": fecha_inicio, "ff": fecha_fin},
            schema="fullclean_telemercadeo",
        )

        # Quejas activas en la zona
        # quejas.cat es la categoría (no JOIN inconformidad.nombre)
        df_quejas_zona = sql_read(
            f"""
            SELECT
                q.cat                AS tipo_queja,
                COUNT(*)             AS n_casos,
                b.barrio             AS barrio
            FROM fullclean_quejas.quejas q
            JOIN fullclean_contactos.contactos c ON c.id = q.id_contacto
            LEFT JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
            WHERE q.id_contacto IN ({ids_str})
            GROUP BY q.cat, b.barrio
            ORDER BY n_casos DESC
            LIMIT 20
            """,
            schema="fullclean_quejas",
        )

    # Composición de cartera visitada
    total_visitados = int(df_visitas["id_contacto"].nunique())
    no_fieles = int(df_visitas[
        ~df_visitas["nombre_categoria"].isin(["Ticket", "Frecuente"])
    ]["id_contacto"].nunique())
    pct_nofiel = round(100 * no_fieles / total_visitados, 1) if total_visitados else 0

    return {
        "promotor": {"id": id_promotor},
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "zona_cubierta": area_info,
        "clientes_visitados": total_visitados,
        "pct_nofiel_visitado": pct_nofiel,
        "clientes_en_zona_segun_cache": len(clientes_zona),
        "clientes_zona_no_visitados": len(clientes_no_visitados_zona),
        "muestra_no_visitados": clientes_no_visitados_zona[:10],
        "top_items_zona": df_pedidos.to_dict(orient="records") if not df_pedidos.empty else [],
        "quejas_activas_zona": df_quejas_zona.to_dict(orient="records") if not df_quejas_zona.empty else [],
        "barrios_visitados": df_visitas["nombre_barrio"].value_counts().head(10).to_dict(),
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from config.secrets_manager import load_env_secure
    load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                    pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

    import json
    result = listar_rutas_ciudad(ciudad=3)  # Medellín
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
