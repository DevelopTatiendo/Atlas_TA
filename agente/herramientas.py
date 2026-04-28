"""Herramientas del agente Atlas TA.

Cada función aquí es un "tool" que el agente Claude puede invocar.
Son wrappers sobre los módulos existentes del proyecto, devolviendo
siempre dicts/listas serializables a JSON.

Catálogo de herramientas:
    consultar_metricas      — métricas por promotor para una ciudad y período
    generar_mapa            — genera el HTML del mapa y devuelve su ruta
    capturar_mapa           — toma screenshot PNG del HTML
    comparar_ciudades       — métricas de las 7 ciudades en un período
    consultar_cliente       — historial de un cliente específico
    listar_promotores_act   — promotores activos en un período
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

# Añadir el directorio raíz al path para imports relativos
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pre_procesamiento.preprocesamiento_muestras import (
    consultar_db,
    crear_df,
    consultar_llamadas_raw,
    aplicar_contactabilidad_temporal,
    listar_promotores,
)
from pre_procesamiento.db_utils import sql_read
from mapa_muestras import generar_mapa_muestras_visual, _calcular_metricas_agrupadas

# ── Mapa de ciudades ──────────────────────────────────────────────────────────

CIUDADES: dict[str, int] = {
    "cali": 2,
    "medellín": 3,
    "medellin": 3,
    "bogotá": 4,
    "bogota": 4,
    "pereira": 5,
    "manizales": 6,
    "bucaramanga": 7,
    "barranquilla": 8,
}

CIUDADES_NOMBRE: dict[int, str] = {v: k.capitalize() for k, v in CIUDADES.items() if not k.endswith("n")}


def _resolver_ciudad(ciudad: str) -> tuple[int, str]:
    """Devuelve (id_centroope, nombre_normalizado) para una ciudad."""
    key = ciudad.lower().strip()
    if key not in CIUDADES:
        opciones = ", ".join(CIUDADES.keys())
        raise ValueError(f"Ciudad '{ciudad}' no reconocida. Opciones: {opciones}")
    cid = CIUDADES[key]
    nombre = CIUDADES_NOMBRE.get(cid, ciudad.capitalize())
    return cid, nombre


# ── Helpers de fecha ──────────────────────────────────────────────────────────

def _fechas_por_defecto() -> tuple[str, str]:
    """Devuelve el rango del mes actual como (fecha_inicio, fecha_fin)."""
    hoy = datetime.today()
    inicio = hoy.replace(day=1).strftime("%Y-%m-%d")
    fin = hoy.strftime("%Y-%m-%d")
    return inicio, fin


# ── HERRAMIENTA 1: consultar_metricas ────────────────────────────────────────

def consultar_metricas(
    ciudad: str,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    promotor_id: int | None = None,
) -> dict[str, Any]:
    """Calcula métricas de operación para una ciudad y período.

    Args:
        ciudad:       Nombre de ciudad (Cali, Medellín, Bogotá, etc.)
        fecha_inicio: 'YYYY-MM-DD'. Por defecto: primer día del mes actual.
        fecha_fin:    'YYYY-MM-DD'. Por defecto: hoy.
        promotor_id:  Filtrar por un promotor específico (opcional).

    Returns:
        Dict con:
            ciudad, periodo, promotores: list[dict con todas las métricas],
            resumen: dict con totales agregados
    """
    if fecha_inicio is None or fecha_fin is None:
        fecha_inicio, fecha_fin = _fechas_por_defecto()

    id_centroope, nombre_ciudad = _resolver_ciudad(ciudad)

    # Datos crudos
    df_raw = consultar_db(id_centroope=id_centroope, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
    if df_raw.empty:
        return {"ciudad": nombre_ciudad, "periodo": f"{fecha_inicio} → {fecha_fin}",
                "promotores": [], "resumen": {}, "mensaje": "Sin datos para el período."}

    df = crear_df(df_raw)

    # Filtro por promotor si se especifica
    if promotor_id is not None:
        df = df[df["id_promotor"] == promotor_id]
        if df.empty:
            return {"ciudad": nombre_ciudad, "periodo": f"{fecha_inicio} → {fecha_fin}",
                    "promotores": [], "resumen": {}, "mensaje": f"Promotor {promotor_id} sin datos."}

    # Dedup y contactabilidad temporal
    df_filtrado = df.sort_values("fecha_evento").groupby(
        ["id_promotor", "id_contacto"], as_index=False
    ).last()

    ids_contacto = df_filtrado["id_contacto"].dropna().astype(int).tolist()
    if ids_contacto:
        df_llamadas = consultar_llamadas_raw(
            ids_contacto=ids_contacto,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
        )
        df_filtrado = aplicar_contactabilidad_temporal(df_filtrado, df_llamadas)

    # Calcular métricas
    df_agrupado = _calcular_metricas_agrupadas(df, df_filtrado, agrupar_por="Promotor")

    # Serializar a lista de dicts
    cols_metricas = [
        "apellido_promotor", "id_promotor",
        "muestras_total", "clientes_total",
        "pct_clientes_no_fieles", "pct_total_muestras_contactables",
        "pct_nofiel_contactable", "pct_conversion",
    ]
    cols_presentes = [c for c in cols_metricas if c in df_agrupado.columns]
    promotores_list = df_agrupado[cols_presentes].to_dict(orient="records")

    # Resumen agregado
    def _mean(col):
        return df_agrupado[col].mean() if col in df_agrupado.columns else None

    n_promotores = len(promotores_list)
    total_clientes = df_agrupado["clientes_total"].sum() if "clientes_total" in df_agrupado.columns else 0
    prom_contactabilidad = _mean("pct_total_muestras_contactables")
    prom_captacion       = _mean("pct_nofiel_contactable")
    prom_conversion      = _mean("pct_conversion")

    return {
        "ciudad": nombre_ciudad,
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "promotores": promotores_list,
        "resumen": {
            "n_promotores": n_promotores,
            "total_clientes": int(total_clientes),
            "prom_contactabilidad_pct": round(float(prom_contactabilidad), 1) if prom_contactabilidad is not None else None,
            "prom_captacion_pct": round(float(prom_captacion), 1) if prom_captacion is not None else None,
            "prom_conversion_pct": round(float(prom_conversion), 1) if prom_conversion is not None else None,
        },
    }


# ── HERRAMIENTA 2: generar_mapa ───────────────────────────────────────────────

def generar_mapa(
    ciudad: str,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    agrupacion: str = "Promotor",
) -> dict[str, Any]:
    """Genera el mapa HTML interactivo para una ciudad y período.

    Args:
        ciudad:       Nombre de ciudad.
        fecha_inicio: 'YYYY-MM-DD'. Por defecto: primer día del mes actual.
        fecha_fin:    'YYYY-MM-DD'. Por defecto: hoy.
        agrupacion:   'Promotor' o 'Mes'.

    Returns:
        Dict con:
            ciudad, periodo, html_path (ruta al archivo generado),
            n_promotores, n_clientes
    """
    if fecha_inicio is None or fecha_fin is None:
        fecha_inicio, fecha_fin = _fechas_por_defecto()

    _, nombre_ciudad = _resolver_ciudad(ciudad)

    # generar_mapa_muestras_visual devuelve (filename, n_puntos, df_csv)
    # filename es solo el nombre del archivo, no la ruta completa
    filename, n_puntos, df_csv = generar_mapa_muestras_visual(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        ciudad=ciudad,
        agrupar_por=agrupacion,
    )

    # Construir ruta completa: static/maps/<filename>
    html_path = str(_ROOT / "static" / "maps" / filename) if filename else ""

    return {
        "ciudad": nombre_ciudad,
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "agrupacion": agrupacion,
        "html_path": html_path,
        "html_existe": os.path.exists(html_path) if html_path else False,
        "n_puntos": n_puntos,
        "n_promotores": 0,
        "n_clientes": 0,
    }


# ── HERRAMIENTA 3: capturar_mapa ─────────────────────────────────────────────

def capturar_mapa(
    html_path: str,
    output_png: str | None = None,
    delay_ms: int = 2500,
) -> dict[str, Any]:
    """Toma screenshot PNG de un mapa HTML generado.

    Requiere que Playwright esté instalado:
        pip install playwright && playwright install chromium

    Args:
        html_path:  Ruta al HTML del mapa.
        output_png: Ruta destino del PNG (opcional).
        delay_ms:   Milisegundos de espera para carga completa.

    Returns:
        Dict con png_path y tamaño en KB.
    """
    from agente.captura import capturar_mapa_html

    png_path = capturar_mapa_html(html_path, output_png, delay_ms=delay_ms)
    size_kb = round(os.path.getsize(png_path) / 1024, 1) if os.path.exists(png_path) else 0

    return {
        "png_path": png_path,
        "tamaño_kb": size_kb,
        "ok": os.path.exists(png_path),
    }


# ── HERRAMIENTA 4: comparar_ciudades ─────────────────────────────────────────

def comparar_ciudades(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> dict[str, Any]:
    """Genera métricas resumidas para las 7 ciudades en un período.

    Returns:
        Dict con lista de ciudades y sus métricas de resumen.
        Ordenado por prom_captacion_pct descendente.
    """
    if fecha_inicio is None or fecha_fin is None:
        fecha_inicio, fecha_fin = _fechas_por_defecto()

    resultados = []
    errores = []

    for nombre, cid in {v: k for k, v in CIUDADES.items() if not k.endswith("n")}.items():
        try:
            res = consultar_metricas(
                ciudad=CIUDADES_NOMBRE.get(nombre, str(nombre)),
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
            )
            fila = {"ciudad": res["ciudad"], **res["resumen"]}
            resultados.append(fila)
        except Exception as e:
            errores.append({"ciudad": str(nombre), "error": str(e)})

    # Ordenar por captación
    resultados.sort(key=lambda x: x.get("prom_captacion_pct") or 0, reverse=True)

    return {
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "ciudades": resultados,
        "errores": errores,
    }


# ── HERRAMIENTA 5: consultar_cliente ─────────────────────────────────────────

def consultar_cliente(
    id_contacto: int | None = None,
    nombre: str | None = None,
    ciudad: str | None = None,
) -> dict[str, Any]:
    """Consulta el historial completo de un cliente específico.

    Busca por id_contacto (exacto) o por nombre (búsqueda parcial).
    Devuelve muestras recibidas, llamadas post-muestra y flag de venta.

    Args:
        id_contacto: ID único del contacto en fullclean_contactos.
        nombre:      Nombre parcial del cliente (búsqueda LIKE).
        ciudad:      Filtrar por ciudad (opcional).
    """
    if id_contacto is None and nombre is None:
        raise ValueError("Debe especificar id_contacto o nombre.")

    # Construir filtro sobre vwContactos.id (PK) o vwContactos.nombre
    condiciones_c = []
    condiciones_e = []
    params: dict = {}

    if id_contacto is not None:
        condiciones_c.append("c.id = :id_c")
        params["id_c"] = id_contacto
    elif nombre is not None:
        condiciones_c.append("c.nombre LIKE :nombre")
        params["nombre"] = f"%{nombre}%"

    ciudad_join = ""
    if ciudad is not None:
        id_centroope, _ = _resolver_ciudad(ciudad)
        # id_centroope no está en vwEventos — filtrar via JOIN ciudades
        ciudad_join = "INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = c.id_ciudad"
        condiciones_e.append(f"ciu.id_centroope = {id_centroope}")

    # Muestras recibidas
    # vwEventos.id_autor = promotor, coordenada_latitud/longitud son VARCHAR
    where_parts = condiciones_c + condiciones_e
    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    sql_muestras = f"""
        SELECT
            c.id                                               AS id_contacto,
            c.nombre                                           AS nombre_contacto,
            c.id_categoria,
            e.id_autor                                         AS id_promotor,
            e.fecha_evento,
            CAST(e.coordenada_latitud  AS DECIMAL(10,6))       AS latitud,
            CAST(e.coordenada_longitud AS DECIMAL(10,6))       AS longitud
        FROM fullclean_contactos.vwEventos e
        JOIN fullclean_contactos.vwContactos c ON c.id = e.id_contacto
        {ciudad_join}
        {where_clause}
        ORDER BY e.fecha_evento DESC
        LIMIT 200
    """

    df_muestras = sql_read(sql_muestras, params=params, schema="fullclean_contactos")

    if df_muestras.empty:
        return {"encontrado": False, "mensaje": "No se encontraron registros."}

    # Llamadas post-muestra
    ids = df_muestras["id_contacto"].unique().tolist()
    ids_str = ",".join(str(i) for i in ids)

    # llamadas_respuestas.contestada = contactabilidad; contacto_exitoso = RPC
    sql_llamadas = f"""
        SELECT
            l.id_contacto,
            l.fecha_inicio_llamada,
            lr.contestada,
            lr.contacto_exitoso,
            lr.es_venta
        FROM fullclean_telemercadeo.llamadas l
        LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta
        WHERE l.id_contacto IN ({ids_str})
        ORDER BY l.fecha_inicio_llamada DESC
        LIMIT 200
    """

    try:
        df_llamadas = sql_read(sql_llamadas, schema="fullclean_telemercadeo")
    except Exception:
        df_llamadas = pd.DataFrame()

    muestras_list = df_muestras.to_dict(orient="records")
    llamadas_list = df_llamadas.to_dict(orient="records") if not df_llamadas.empty else []

    # Convertir Timestamps a string para JSON
    for row in muestras_list:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

    for row in llamadas_list:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

    return {
        "encontrado": True,
        "n_muestras": len(muestras_list),
        "n_llamadas": len(llamadas_list),
        "muestras": muestras_list,
        "llamadas": llamadas_list,
    }


# ── HERRAMIENTA 6: listar_promotores_activos ─────────────────────────────────

def listar_promotores_activos(
    ciudad: str,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> dict[str, Any]:
    """Lista los promotores activos para una ciudad y período.

    Returns:
        Dict con ciudad, periodo y lista de promotores (id + nombre).
    """
    if fecha_inicio is None or fecha_fin is None:
        fecha_inicio, fecha_fin = _fechas_por_defecto()

    id_centroope, nombre_ciudad = _resolver_ciudad(ciudad)

    promotores = listar_promotores(id_centroope, fecha_inicio, fecha_fin)

    return {
        "ciudad": nombre_ciudad,
        "periodo": f"{fecha_inicio} → {fecha_fin}",
        "promotores": promotores,
        "n_promotores": len(promotores),
    }
