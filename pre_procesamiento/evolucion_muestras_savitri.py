"""Analisis post-muestra para clientes que recibieron muestras Savitri.

Este modulo construye un mapa Folium categorico para seguir que paso despues
de una entrega de muestra: si el cliente compro la misma marca, compro otra
marca o no volvio a comprar dentro de una ventana de seguimiento.
"""

from __future__ import annotations

import html
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import folium
import pandas as pd

from pre_procesamiento.db_utils import sql_read
from utils.gestor_mapas import guardar_mapa_controlado


ROOT = Path(__file__).resolve().parent.parent

CENTROOPES = {
    "CALI": 2,
    "MEDELLIN": 3,
    "MANIZALES": 6,
    "PEREIRA": 5,
    "BOGOTA": 4,
    "BARRANQUILLA": 8,
    "BUCARAMANGA": 7,
}

COORDENADAS_CIUDADES = {
    "CALI": ([3.4516, -76.5320], ROOT / "geojson/rutas/cali/cuadrantes_rutas_cali.geojson"),
    "MEDELLIN": ([6.2442, -75.5812], ROOT / "geojson/rutas/medellin/cuadrantes_rutas_medellin.geojson"),
    "MANIZALES": ([5.0672, -75.5174], ROOT / "geojson/rutas/manizales/cuadrantes_rutas_manizales.geojson"),
    "PEREIRA": ([4.8087, -75.6906], ROOT / "geojson/rutas/pereira/cuadrantes_rutas_pereira.geojson"),
    "BOGOTA": ([4.7110, -74.0721], ROOT / "geojson/rutas/bogota/cuadrantes_rutas_bogota.geojson"),
    "BARRANQUILLA": ([10.9720, -74.7962], ROOT / "geojson/rutas/barranquilla/cuadrantes_rutas_barranquilla.geojson"),
    "BUCARAMANGA": ([7.1193, -73.1227], ROOT / "geojson/rutas/bucaramanga/cuadrantes_rutas_bucaramanga.geojson"),
}

COLORES_RESULTADO = {
    "VOLVIO A COMPRAR MISMA MARCA": "#16A34A",
    "VOLVIO A COMPRAR PERO CAMBIO DE MARCA": "#2563EB",
    "NO VOLVIO A COMPRAR": "#DC2626",
    "SIN MUESTRA IDENTIFICADA": "#9CA3AF",
}

LABELS_RESULTADO = {
    "VOLVIO A COMPRAR MISMA MARCA": "Volvio A Comprar Misma Marca",
    "VOLVIO A COMPRAR PERO CAMBIO DE MARCA": "Volvio A Comprar Pero Cambio De Marca",
    "NO VOLVIO A COMPRAR": "No Volvio A Comprar",
    "SIN MUESTRA IDENTIFICADA": "Sin Muestra Identificada",
}

CARGOS_EVENTO = {
    "PROMOTOR": {"ids": [39], "label": "Promotor"},
    "CONSULTOR": {"ids": [181], "label": "Consultor"},
}


def _normalizar_ciudad(ciudad: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(ciudad or ""))
        if unicodedata.category(c) != "Mn"
    ).upper().strip()


def _clasificar_marca_sql(alias: str = "i") -> str:
    return f"""
        CASE
            WHEN UPPER({alias}.item) LIKE '%FULLIMP%' THEN 'FULLIMP'
            WHEN UPPER({alias}.item) LIKE '%SAVITRI%' THEN 'SAVITRI'
            WHEN UPPER({alias}.item) LIKE '%GIORGIO%' THEN 'GIORGIO'
            ELSE 'OTRA / SIN CLASIFICAR'
        END
    """


def _clasificar_linea_sql(alias: str = "i") -> str:
    return f"""
        CASE
            WHEN UPPER({alias}.item) LIKE '%QUITAGRASA%' THEN 'QUITAGRASA'
            WHEN UPPER({alias}.item) LIKE '%ACONDICIONADOR%' THEN 'ACONDICIONADOR'
            WHEN UPPER({alias}.item) LIKE '%HIDROKERATINA%' THEN 'HIDROKERATINA'
            ELSE 'OTRA LINEA / SIN CLASIFICAR'
        END
    """


def _sql_evolucion_savitri(
    id_centroope: int,
    fecha_inicio: str,
    fecha_fin: str,
    dias_seguimiento: int,
    marca_muestra: str,
    solo_ultima_muestra: bool,
    cargo_evento: str,
) -> tuple[str, dict[str, Any]]:
    dias = max(1, min(int(dias_seguimiento), 730))
    marca = re.sub(r"[^A-Za-z0-9 _/-]", "", marca_muestra or "SAVITRI").strip().upper()
    marca_like = f"%{marca}%"
    cargo_key = str(cargo_evento or "PROMOTOR").upper().strip()
    cargo_cfg = CARGOS_EVENTO.get(cargo_key, CARGOS_EVENTO["PROMOTOR"])
    cargo_ids = [int(x) for x in cargo_cfg["ids"]]
    cargo_placeholders = ", ".join(f":cargo_{i}" for i in range(len(cargo_ids)))

    filtro_marca = ""
    if marca and marca not in {"TODAS", "TODAS LAS MARCAS"}:
        filtro_marca = "AND UPPER(i.item) LIKE :marca_like"

    dedupe_where = ""
    if solo_ultima_muestra:
        dedupe_where = "WHERE rn_cliente = 1"

    sql = f"""
WITH muestras_base AS (
    SELECT
        ev.idEvento,
        ev.id_contacto,
        ev.fecha_evento,
        ev.id_autor,
        per.apellido AS autor_nombre,
        per.id_cargo AS id_cargo_autor,
        ca.cargo AS cargo_autor,
        ev.coordenada_latitud,
        ev.coordenada_longitud,

        ob.Id AS id_obsequio,
        ob.id_item AS id_item_muestra,
        ob.fecha_obsequio,
        ob.id_promotor,
        ob.id_centroope AS id_centroope_obsequio,

        i.item AS item_muestra,
        {_clasificar_marca_sql("i")} AS marca_muestra,
        {_clasificar_linea_sql("i")} AS linea_muestra,

        ROW_NUMBER() OVER (
            PARTITION BY ev.id_contacto
            ORDER BY ev.fecha_evento DESC, ev.idEvento DESC
        ) AS rn_cliente

    FROM fullclean_contactos.vwEventos ev
    INNER JOIN fullclean_contactos.contactos c
        ON c.id = ev.id_contacto
    INNER JOIN fullclean_contactos.ciudades ci
        ON ci.id = c.id_ciudad
       AND ci.id_centroope = :id_centroope
    INNER JOIN fullclean_personal.personal per
        ON per.id = ev.id_autor
       AND per.id_cargo IN ({cargo_placeholders})
    LEFT JOIN fullclean_personal.cargos ca
        ON ca.Id_cargo = per.id_cargo
    LEFT JOIN fullclean_telemercadeo.obsequios ob
        ON ob.id_contacto = ev.id_contacto
       AND ob.id_promotor = ev.id_autor
       AND DATE(ob.fecha_obsequio) = DATE(ev.fecha_evento)
    LEFT JOIN fullclean_bodega.items i
        ON i.id = ob.id_item
    WHERE ev.id_evento_tipo = 15
      AND ev.fecha_evento >= :fecha_inicio
      AND ev.fecha_evento < DATE_ADD(:fecha_fin, INTERVAL 1 DAY)
      AND ev.coordenada_latitud BETWEEN -4.5 AND 12.5
      AND ev.coordenada_longitud BETWEEN -82 AND -66
      AND ev.coordenada_latitud <> 0
      AND ev.coordenada_longitud <> 0
      {filtro_marca}
),
muestras AS (
    SELECT *
    FROM muestras_base
    {dedupe_where}
),
compras_posteriores AS (
    SELECT
        m.idEvento,
        m.id_contacto,
        pe.id AS id_pedido,
        pe.fecha_hora_pedido,
        pd.id_item AS id_item_comprado,
        ic.item AS item_comprado,
        {_clasificar_marca_sql("ic")} AS marca_compra,
        {_clasificar_linea_sql("ic")} AS linea_compra
    FROM muestras m
    INNER JOIN fullclean_telemercadeo.pedidos pe
        ON pe.id_contacto = m.id_contacto
       AND pe.fecha_hora_pedido > m.fecha_evento
       AND pe.fecha_hora_pedido <= DATE_ADD(m.fecha_evento, INTERVAL {dias} DAY)
    INNER JOIN fullclean_telemercadeo.pedidos_det pd
        ON pd.id_pedido = pe.id
    LEFT JOIN fullclean_bodega.items ic
        ON ic.id = pd.id_item
    WHERE pe.estado_pedido = 1
      AND pe.anulada = 0
      AND pe.autorizar IN (1, 2)
      AND pe.autorizacion_descuento = 0
      AND pe.tipo_documento < 2
),
resumen_compras AS (
    SELECT
        idEvento,
        id_contacto,
        COUNT(DISTINCT id_pedido) AS pedidos_posteriores,
        COUNT(DISTINCT id_item_comprado) AS items_diferentes_comprados,
        MIN(fecha_hora_pedido) AS primera_compra_posterior,
        MAX(fecha_hora_pedido) AS ultima_compra_posterior,
        GROUP_CONCAT(DISTINCT marca_compra ORDER BY marca_compra SEPARATOR ', ') AS marcas_compradas_despues,
        GROUP_CONCAT(DISTINCT linea_compra ORDER BY linea_compra SEPARATOR ', ') AS lineas_compradas_despues,
        GROUP_CONCAT(DISTINCT item_comprado ORDER BY item_comprado SEPARATOR ' | ') AS items_comprados_despues
    FROM compras_posteriores
    GROUP BY idEvento, id_contacto
)
SELECT
    m.idEvento AS id_muestra,
    m.id_contacto,
    m.id_autor,
    m.autor_nombre,
    m.id_cargo_autor,
    m.cargo_autor,
    c.nombre AS nombre_cliente,
    c.tel1,
    c.celular,
    b.barrio,
    ci.ciudad,
    co.descripcion AS CO,
    m.fecha_evento,
    m.fecha_obsequio,
    m.id_item_muestra,
    m.item_muestra,
    m.marca_muestra,
    m.linea_muestra,
    m.coordenada_latitud AS lat,
    m.coordenada_longitud AS lon,
    COALESCE(rc.pedidos_posteriores, 0) AS pedidos_posteriores,
    COALESCE(rc.items_diferentes_comprados, 0) AS items_diferentes_comprados,
    rc.primera_compra_posterior,
    rc.ultima_compra_posterior,
    rc.marcas_compradas_despues,
    rc.lineas_compradas_despues,
    rc.items_comprados_despues,
    CASE
        WHEN m.item_muestra IS NULL THEN 'SIN MUESTRA IDENTIFICADA'
        WHEN rc.pedidos_posteriores IS NULL THEN 'NO VOLVIO A COMPRAR'
        WHEN FIND_IN_SET(m.marca_muestra, REPLACE(rc.marcas_compradas_despues, ', ', ',')) > 0
             THEN 'VOLVIO A COMPRAR MISMA MARCA'
        ELSE 'VOLVIO A COMPRAR PERO CAMBIO DE MARCA'
    END AS resultado_marca,
    CASE
        WHEN m.item_muestra IS NULL THEN 'SIN MUESTRA IDENTIFICADA'
        WHEN rc.pedidos_posteriores IS NULL THEN 'NO VOLVIO A COMPRAR'
        WHEN FIND_IN_SET(m.linea_muestra, REPLACE(rc.lineas_compradas_despues, ', ', ',')) > 0
             THEN 'VOLVIO A COMPRAR MISMA LINEA'
        ELSE 'VOLVIO A COMPRAR OTRA LINEA'
    END AS resultado_linea
FROM muestras m
LEFT JOIN resumen_compras rc
    ON rc.idEvento = m.idEvento
   AND rc.id_contacto = m.id_contacto
LEFT JOIN fullclean_contactos.contactos c
    ON c.id = m.id_contacto
LEFT JOIN fullclean_contactos.barrios b
    ON b.Id = c.id_barrio
LEFT JOIN fullclean_contactos.ciudades ci
    ON ci.id = c.id_ciudad
LEFT JOIN fullclean_general.centroope co
    ON co.id = ci.id_centroope
ORDER BY m.fecha_evento DESC, m.id_contacto
"""
    params = {
        "id_centroope": int(id_centroope),
        "fecha_inicio": f"{fecha_inicio} 00:00:00",
        "fecha_fin": f"{fecha_fin} 00:00:00",
        "marca_like": marca_like,
    }
    for i, cargo_id in enumerate(cargo_ids):
        params[f"cargo_{i}"] = cargo_id
    return sql, params


def consultar_evolucion_muestras_savitri(
    ciudad: str,
    fecha_inicio: str,
    fecha_fin: str,
    dias_seguimiento: int = 90,
    marca_muestra: str = "SAVITRI",
    solo_ultima_muestra: bool = True,
    cargo_evento: str = "PROMOTOR",
) -> pd.DataFrame:
    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in CENTROOPES:
        return pd.DataFrame()

    sql, params = _sql_evolucion_savitri(
        id_centroope=CENTROOPES[ciudad_norm],
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        dias_seguimiento=dias_seguimiento,
        marca_muestra=marca_muestra,
        solo_ultima_muestra=solo_ultima_muestra,
        cargo_evento=cargo_evento,
    )
    df = sql_read(sql, params=params, schema="fullclean_contactos")
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    for col in ["lat", "lon", "pedidos_posteriores", "items_diferentes_comprados"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["fecha_evento", "fecha_obsequio", "primera_compra_posterior", "ultima_compra_posterior"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _fmt_fecha(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)[:10]


def _safe(value: Any) -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
    except Exception:
        pass
    text = html.escape(str(value), quote=True)
    return (
        text
        .replace("\\", "&#92;")
        .replace("`", "&#96;")
        .replace("$", "&#36;")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _popup_cliente(row: pd.Series) -> str:
    campos = [
        ("Generado por", row.get("autor_nombre")),
        ("Cargo", row.get("cargo_autor")),
        ("Cliente", row.get("nombre_cliente")),
        ("Tel", row.get("tel1")),
        ("Cel", row.get("celular")),
        ("Barrio", row.get("barrio")),
        ("Fecha muestra", _fmt_fecha(row.get("fecha_evento"))),
        ("Muestra", row.get("item_muestra")),
        ("Linea muestra", row.get("linea_muestra")),
        ("Pedidos posteriores", row.get("pedidos_posteriores")),
        ("Primera compra", _fmt_fecha(row.get("primera_compra_posterior"))),
        ("Marcas despues", row.get("marcas_compradas_despues")),
        ("Lineas despues", row.get("lineas_compradas_despues")),
        ("Resultado marca", row.get("resultado_marca")),
        ("Resultado linea", row.get("resultado_linea")),
    ]
    filas = "".join(
        f"<tr><td style='padding:3px 8px;color:#555'>{label}</td>"
        f"<td style='padding:3px 0;font-weight:600'>{_safe(value)}</td></tr>"
        for label, value in campos
    )
    return (
        "<div style='font-family:Arial,sans-serif;font-size:12px;max-width:360px'>"
        f"<div style='font-size:14px;font-weight:700;margin-bottom:6px'>ID {_safe(row.get('id_contacto'))}</div>"
        f"<table style='border-collapse:collapse'>{filas}</table>"
        "</div>"
    )


def _agregar_geojson(mapa: folium.Map, ciudad_norm: str) -> None:
    geo_path = COORDENADAS_CIUDADES.get(ciudad_norm, [None, None])[1]
    if not geo_path or not Path(geo_path).exists():
        return
    try:
        with open(geo_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)
    except Exception:
        return

    def _style(feature: dict) -> dict:
        props = feature.get("properties") or {}
        return {
            "fillColor": props.get("fillColor", "#FACC15"),
            "color": props.get("color", "#333333"),
            "weight": props.get("weight", 1.2),
            "fillOpacity": props.get("fillOpacity", 0.18),
        }

    sample_props = {}
    for feat in geojson_data.get("features", []):
        sample_props = feat.get("properties") or {}
        break
    tooltip = None
    if "codigo" in sample_props:
        tooltip = folium.GeoJsonTooltip(fields=["codigo"], aliases=["Ruta"], sticky=False)

    kwargs = {
        "data": geojson_data,
        "name": "Cuadrantes / Rutas",
        "style_function": _style,
    }
    if tooltip:
        kwargs["tooltip"] = tooltip
    folium.GeoJson(**kwargs).add_to(mapa)


def _resumen(df: pd.DataFrame) -> dict[str, Any]:
    total_eventos = int(len(df))
    total_clientes = int(df["id_contacto"].nunique()) if "id_contacto" in df.columns else total_eventos
    por_resultado = (
        df["resultado_marca"].fillna("SIN CLASIFICAR").value_counts().to_dict()
        if "resultado_marca" in df.columns else {}
    )
    compraron_misma = int(por_resultado.get("VOLVIO A COMPRAR MISMA MARCA", 0))
    compraron_otra = int(por_resultado.get("VOLVIO A COMPRAR PERO CAMBIO DE MARCA", 0))
    no_compraron = int(por_resultado.get("NO VOLVIO A COMPRAR", 0))
    conversion = round(100 * compraron_misma / total_eventos, 1) if total_eventos else 0.0
    recompra = round(100 * (compraron_misma + compraron_otra) / total_eventos, 1) if total_eventos else 0.0
    return {
        "total_eventos": total_eventos,
        "total_clientes": total_clientes,
        "compraron_misma_marca": compraron_misma,
        "compraron_otra_marca": compraron_otra,
        "no_volvieron": no_compraron,
        "pct_recompra": recompra,
        "pct_conversion_misma_marca": conversion,
        "por_resultado": por_resultado,
    }


def _legend_html(resumen: dict[str, Any], fecha_inicio: str, fecha_fin: str, dias: int, marca: str, cargo_label: str) -> str:
    rows = ""
    labels = [
        "VOLVIO A COMPRAR MISMA MARCA",
        "VOLVIO A COMPRAR PERO CAMBIO DE MARCA",
        "NO VOLVIO A COMPRAR",
        "SIN MUESTRA IDENTIFICADA",
    ]
    for label in labels:
        color = COLORES_RESULTADO.get(label, "#9CA3AF")
        count = int(resumen.get("por_resultado", {}).get(label, 0))
        display = LABELS_RESULTADO.get(label, label.title())
        rows += (
            f"<button type='button' class='ta-layer-toggle is-active' data-layer='{html.escape(display, quote=True)}'>"
            f"<span class='ta-dot' style='background:{color}'></span>"
            f"<span class='ta-label'>{html.escape(display)}</span>"
            f"<span class='ta-count'>{count:,}</span>"
            f"</button>"
        )
    return f"""
    <div style="position:fixed;top:18px;left:18px;z-index:9999;
                background:rgba(255,255,255,.95);border-radius:8px;
                box-shadow:0 2px 10px rgba(0,0,0,.18);padding:12px 14px;
                font-family:Arial,sans-serif;font-size:12px;min-width:280px">
      <div style="font-weight:800;font-size:14px;margin-bottom:4px">Evolucion post-muestra {html.escape(marca)}</div>
      <div style="color:#555;margin-bottom:8px">{fecha_inicio} a {fecha_fin} - ventana {dias} dias - {html.escape(cargo_label)}</div>
      <div class="ta-layer-toggles">{rows}</div>
      <div style="border-top:1px solid #e5e7eb;padding-top:7px;line-height:1.6">
        Clientes: <b>{resumen.get('total_clientes', 0):,}</b> - Eventos: <b>{resumen.get('total_eventos', 0):,}</b><br>
        Recompra total: <b>{resumen.get('pct_recompra', 0)}%</b><br>
        Conversion misma marca: <b>{resumen.get('pct_conversion_misma_marca', 0)}%</b>
      </div>
      <style>
        .ta-layer-toggles {{
          display:flex; flex-direction:column; gap:6px; margin:10px 0 10px 0;
        }}
        .ta-layer-toggle {{
          display:grid; grid-template-columns:16px 1fr auto; align-items:center; gap:4px;
          width:100%; border:1px solid transparent; background:transparent;
          border-radius:6px; padding:5px 6px; text-align:left; color:#333;
          font:inherit; cursor:pointer;
        }}
        .ta-layer-toggle:hover {{ background:#f3f4f6; border-color:#e5e7eb; }}
        .ta-layer-toggle:not(.is-active) {{ opacity:.42; text-decoration:line-through; }}
        .ta-dot {{ width:10px; height:10px; border-radius:50%; display:inline-block; }}
        .ta-label {{ white-space:nowrap; padding-right:8px; }}
        .ta-count {{ font-weight:700; text-align:right; }}
      </style>
    </div>
    """


def _layer_toggle_js(mapa) -> str:
    map_name = mapa.get_name()
    return f"""
    <script>
    (function(){{
      function initAtlasToggles(){{
        var map = window.{map_name};
        if (!map) {{ setTimeout(initAtlasToggles, 250); return; }}

        function findLayerByName(name){{
          var found = null;
          map.eachLayer(function(layer){{
            if (found) return;
            if (layer && layer.options && layer.options.name === name) found = layer;
          }});
          return found;
        }}

        document.querySelectorAll('.ta-layer-toggle').forEach(function(btn){{
          if (btn.__taBound) return;
          btn.__taBound = true;
          btn.addEventListener('click', function(){{
            var layerName = btn.getAttribute('data-layer');
            var layer = findLayerByName(layerName);
            if (!layer) return;
            if (map.hasLayer(layer)) {{
              map.removeLayer(layer);
              btn.classList.remove('is-active');
            }} else {{
              map.addLayer(layer);
              btn.classList.add('is-active');
            }}
          }});
        }});
      }}
      [100, 400, 1000].forEach(function(ms){{ setTimeout(initAtlasToggles, ms); }});
    }})();
    </script>
    """


def generar_mapa_evolucion_muestras_savitri(
    ciudad: str,
    fecha_inicio: str,
    fecha_fin: str,
    dias_seguimiento: int = 90,
    marca_muestra: str = "SAVITRI",
    solo_ultima_muestra: bool = True,
    cargo_evento: str = "PROMOTOR",
) -> tuple[str | None, int, pd.DataFrame | None, dict[str, Any]]:
    ciudad_norm = _normalizar_ciudad(ciudad)
    if ciudad_norm not in COORDENADAS_CIUDADES:
        return None, 0, None, {"error": f"Ciudad no reconocida: {ciudad}"}

    df = consultar_evolucion_muestras_savitri(
        ciudad=ciudad,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        dias_seguimiento=dias_seguimiento,
        marca_muestra=marca_muestra,
        solo_ultima_muestra=solo_ultima_muestra,
        cargo_evento=cargo_evento,
    )
    cargo_key = str(cargo_evento or "PROMOTOR").upper().strip()
    cargo_label = CARGOS_EVENTO.get(cargo_key, CARGOS_EVENTO["PROMOTOR"])["label"]

    location = COORDENADAS_CIUDADES[ciudad_norm][0]
    mapa = folium.Map(
        location=location,
        zoom_start=12,
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
    )
    _agregar_geojson(mapa, ciudad_norm)

    if df.empty:
        resumen = _resumen(pd.DataFrame(columns=["id_contacto", "resultado_marca"]))
        mapa.get_root().html.add_child(folium.Element(_legend_html(resumen, fecha_inicio, fecha_fin, int(dias_seguimiento), marca_muestra, cargo_label)))
        filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_evolucion_savitri", permitir_multiples=False)
        return filename, 0, df, resumen

    df_map = df.dropna(subset=["lat", "lon"]).copy()
    resumen = _resumen(df)

    grupos: dict[str, folium.FeatureGroup] = {}
    for resultado in COLORES_RESULTADO:
        grupos[resultado] = folium.FeatureGroup(
            name=LABELS_RESULTADO.get(resultado, resultado.title()),
            show=True,
        ).add_to(mapa)

    for _, row in df_map.iterrows():
        resultado = str(row.get("resultado_marca") or "SIN MUESTRA IDENTIFICADA")
        color = COLORES_RESULTADO.get(resultado, "#9CA3AF")
        group = grupos.get(resultado) or mapa
        folium.CircleMarker(
            location=[float(row["lat"]), float(row["lon"])],
            radius=5,
            color="#ffffff",
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=0.86,
            tooltip=f"{_safe(row.get('nombre_cliente'))} | {_safe(resultado.title())}",
            popup=folium.Popup(_popup_cliente(row), max_width=420),
        ).add_to(group)

    mapa.get_root().html.add_child(folium.Element(_legend_html(resumen, fecha_inicio, fecha_fin, int(dias_seguimiento), marca_muestra, cargo_label)))
    mapa.get_root().html.add_child(folium.Element(_layer_toggle_js(mapa)))
    folium.LayerControl(collapsed=True, position="topright").add_to(mapa)

    filename = guardar_mapa_controlado(mapa, tipo_mapa="mapa_evolucion_savitri", permitir_multiples=False)
    return filename, int(len(df_map)), df, resumen


__all__ = [
    "consultar_evolucion_muestras_savitri",
    "generar_mapa_evolucion_muestras_savitri",
]
