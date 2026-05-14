"""Módulo de Rutas por Consultor.

Genera un mapa Folium interactivo que traza el recorrido diario de cada
consultor seleccionado:
  - AntPath animado por consultor/día
  - Marcadores numerados en cada parada
  - Panel flotante JS con checkboxes por consultor y selector de día
  - Tabla resumen con km, paradas y horario
  - GeoJSON cuadrantes/rutas overlay (mismo estilo que el resto de Atlas TA)
"""

from __future__ import annotations

import json
import math
import unicodedata
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

import folium
from folium.plugins import AntPath, Fullscreen
import pandas as pd

from utils.gestor_mapas import guardar_mapa_controlado

ROOT = Path(__file__).resolve().parent

# ── Centroopes y GeoJSON ──────────────────────────────────────────────────────
CENTROOPES: dict[str, int] = {
    "CALI": 2, "MEDELLIN": 3, "MANIZALES": 6,
    "PEREIRA": 5, "BOGOTA": 4, "BARRANQUILLA": 8, "BUCARAMANGA": 7,
}

COORDENADAS_CIUDADES: dict[str, tuple[list[float], Path]] = {
    "CALI":        ([3.4516,  -76.5320], ROOT / "geojson/rutas/cali/cuadrantes_rutas_cali.geojson"),
    "MEDELLIN":    ([6.2442,  -75.5812], ROOT / "geojson/rutas/medellin/cuadrantes_rutas_medellin.geojson"),
    "MANIZALES":   ([5.0672,  -75.5174], ROOT / "geojson/rutas/manizales/cuadrantes_rutas_manizales.geojson"),
    "PEREIRA":     ([4.8087,  -75.6906], ROOT / "geojson/rutas/pereira/cuadrantes_rutas_pereira.geojson"),
    "BOGOTA":      ([4.7110,  -74.0721], ROOT / "geojson/rutas/bogota/cuadrantes_rutas_bogota.geojson"),
    "BARRANQUILLA":([10.9720, -74.7962], ROOT / "geojson/rutas/barranquilla/cuadrantes_rutas_barranquilla.geojson"),
    "BUCARAMANGA": ([7.1193,  -73.1227], ROOT / "geojson/rutas/bucaramanga/cuadrantes_rutas_bucaramanga.geojson"),
}

# Paleta de colores para consultores (hasta 20 consultores distintos)
_PALETA = [
    "#2563EB", "#DC2626", "#16A34A", "#D97706", "#7C3AED",
    "#0891B2", "#BE123C", "#15803D", "#B45309", "#6D28D9",
    "#0284C7", "#9F1239", "#166534", "#92400E", "#5B21B6",
    "#0369A1", "#881337", "#14532D", "#78350F", "#4C1D95",
]


def _norm(ciudad: str) -> str:
    s = unicodedata.normalize("NFD", ciudad.upper().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _color_consultor(idx: int) -> str:
    return _PALETA[idx % len(_PALETA)]


def _fmt_hora(ts) -> str:
    try:
        return pd.to_datetime(ts).strftime("%H:%M")
    except Exception:
        return "-"


def _fmt_fecha(ts) -> str:
    try:
        return pd.to_datetime(ts).strftime("%Y-%m-%d")
    except Exception:
        return str(ts)[:10]


# ── GeoJSON overlay ───────────────────────────────────────────────────────────

def _agregar_geojson(mapa: folium.Map, ciudad_norm: str) -> None:
    entry = COORDENADAS_CIUDADES.get(ciudad_norm)
    if not entry:
        return
    geo_path = entry[1]
    if not Path(geo_path).exists():
        return
    try:
        with open(geo_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)
    except Exception:
        return

    def _style(feature: dict) -> dict:
        p = feature.get("properties") or {}
        return {
            "fillColor": p.get("fillColor", "#FACC15"),
            "color":     p.get("color",     "#333333"),
            "weight":    p.get("weight",    1.2),
            "fillOpacity": p.get("fillOpacity", 0.20),
        }

    # tooltip con código/ruta si existe
    sample = next(iter(geojson_data.get("features", [])), {})
    sp = sample.get("properties") or {}
    tooltip = None
    if "codigo" in sp:
        tooltip = folium.GeoJsonTooltip(
            fields=["codigo"], aliases=["Ruta"], sticky=False
        )

    fg = folium.FeatureGroup(name="Cuadrantes / Rutas", show=True)
    kwargs: dict = {"data": geojson_data, "style_function": _style}
    if tooltip:
        kwargs["tooltip"] = tooltip
    folium.GeoJson(**kwargs).add_to(fg)
    fg.add_to(mapa)


# ── Marcador numerado ─────────────────────────────────────────────────────────

def _marcador_numerado(
    lat: float, lon: float, numero: int, color: str,
    hora: str, id_contacto, tipo_evento: str,
) -> folium.CircleMarker:
    popup_html = (
        f"<div style='font-family:Arial,sans-serif;font-size:12px;min-width:160px'>"
        f"<b style='font-size:13px'>#{numero}</b> &nbsp; {hora}<br>"
        f"<span style='color:#555'>Cliente:</span> <b>{id_contacto}</b><br>"
        f"<span style='color:#555'>Tipo:</span> {tipo_evento or '—'}"
        f"</div>"
    )
    # DivIcon con número
    icon_html = (
        f"<div style='"
        f"background:{color};color:#fff;border-radius:50%;"
        f"width:22px;height:22px;line-height:22px;"
        f"text-align:center;font-size:10px;font-weight:700;"
        f"border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.4)'>"
        f"{numero}"
        f"</div>"
    )
    marker = folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(html=icon_html, icon_size=(22, 22), icon_anchor=(11, 11)),
        popup=folium.Popup(popup_html, max_width=220),
        tooltip=f"#{numero} {hora} · {id_contacto}",
    )
    return marker


# ── Panel de control JS ───────────────────────────────────────────────────────

def _panel_control_js(
    consultores: list[dict],   # [{"id": int, "label": str, "color": str, "dias": [str...]}]
    dias_ordenados: list[str], # ["2026-05-05", "2026-05-06", ...]
) -> str:
    """
    Panel flotante a la izquierda:
    - Checkboxes por consultor (toggle su AntPath + markers)
    - Botones de día que muestran sólo los layers de ese día (para todos los
      consultores activos)
    Usa los IDs de FeatureGroup que definimos como: fg_{id_autor}_{fecha}
    """
    # Serializar datos para JS
    import json as _json
    cons_js    = _json.dumps(consultores)
    dias_js    = _json.dumps(dias_ordenados)

    return f"""
<script>
(function() {{
  var CONSULTORES = {cons_js};
  var DIAS        = {dias_js};
  var diaActivo   = null;   // null → mostrar todos los días

  /* ─── helpers ─── */
  function layerId(id_autor, fecha) {{
    return "fg_" + id_autor + "_" + fecha.replace(/-/g,"");
  }}

  function getLayer(id) {{
    var layers = window._map && window._map._layers;
    if (!layers) return null;
    for (var k in layers) {{
      if (layers[k].options && layers[k].options.layerId === id) return layers[k];
    }}
    return null;
  }}

  function setLayerVisible(lid, vis) {{
    var L = window.leafletMap;
    if (!L) return;
    L.eachLayer(function(layer) {{
      if (layer.options && layer.options.layerId === lid) {{
        if (vis) {{ if (!L.hasLayer(layer)) L.addLayer(layer); }}
        else     {{ if (L.hasLayer(layer))  L.removeLayer(layer); }}
      }}
    }});
  }}

  function refreshLayers() {{
    CONSULTORES.forEach(function(con) {{
      var cbEl = document.getElementById("cb_" + con.id);
      var activo = cbEl ? cbEl.checked : true;
      con.dias.forEach(function(d) {{
        var visible = activo && (diaActivo === null || diaActivo === d);
        setLayerVisible(layerId(con.id, d), visible);
      }});
    }});
    // Resaltar botón activo
    DIAS.forEach(function(d) {{
      var btn = document.getElementById("btn_dia_" + d.replace(/-/g,""));
      if (btn) btn.style.background = (diaActivo === d) ? "#2563EB" : "#E5E7EB";
      if (btn) btn.style.color      = (diaActivo === d) ? "#fff"    : "#1F1A2F";
    }});
  }}

  /* ─── construir panel ─── */
  function buildPanel() {{
    var panel = document.createElement("div");
    panel.id = "rutas-panel";
    panel.style.cssText = [
      "position:fixed","left:12px","top:50%","transform:translateY(-50%)",
      "z-index:9999","background:#fff","padding:12px 14px","border-radius:10px",
      "box-shadow:0 2px 10px rgba(0,0,0,.18)","font-family:Arial,sans-serif",
      "font-size:12px","min-width:190px","max-width:230px","max-height:80vh",
      "overflow-y:auto"
    ].join(";");

    /* título */
    var title = document.createElement("div");
    title.style.cssText = "font-weight:700;font-size:13px;margin-bottom:8px;color:#1F1A2F";
    title.textContent = "🗂 Rutas Consultores";
    panel.appendChild(title);

    /* selector de día */
    if (DIAS.length > 1) {{
      var secDia = document.createElement("div");
      secDia.style.cssText = "margin-bottom:10px";
      var lblDia = document.createElement("div");
      lblDia.style.cssText = "color:#6B7280;font-size:11px;margin-bottom:4px";
      lblDia.textContent = "Filtrar por día:";
      secDia.appendChild(lblDia);

      // Botón "Todos"
      var btnTodos = document.createElement("button");
      btnTodos.textContent = "Todos";
      btnTodos.id = "btn_dia_todos";
      btnTodos.style.cssText = "margin:2px;padding:3px 7px;border:1px solid #CBD5E1;border-radius:4px;cursor:pointer;background:#2563EB;color:#fff;font-size:11px";
      btnTodos.onclick = function() {{ diaActivo = null; refreshLayers(); btnTodos.style.background="#2563EB"; btnTodos.style.color="#fff"; }};
      secDia.appendChild(btnTodos);

      DIAS.forEach(function(d) {{
        var btn = document.createElement("button");
        var label = d.slice(5);  // "MM-DD"
        btn.textContent = label;
        btn.id = "btn_dia_" + d.replace(/-/g,"");
        btn.style.cssText = "margin:2px;padding:3px 7px;border:1px solid #CBD5E1;border-radius:4px;cursor:pointer;background:#E5E7EB;color:#1F1A2F;font-size:11px";
        btn.onclick = (function(dia, b) {{
          return function() {{
            diaActivo = dia;
            btnTodos.style.background="#E5E7EB"; btnTodos.style.color="#1F1A2F";
            refreshLayers();
          }};
        }})(d, btn);
        secDia.appendChild(btn);
      }});
      panel.appendChild(secDia);
    }}

    /* sep */
    var sep = document.createElement("div");
    sep.style.cssText = "border-top:1px solid #E5E7EB;margin:6px 0";
    panel.appendChild(sep);

    /* consultores */
    var lblCon = document.createElement("div");
    lblCon.style.cssText = "color:#6B7280;font-size:11px;margin-bottom:6px";
    lblCon.textContent = "Consultores:";
    panel.appendChild(lblCon);

    CONSULTORES.forEach(function(con) {{
      var row = document.createElement("label");
      row.style.cssText = "display:flex;align-items:center;gap:7px;margin-bottom:5px;cursor:pointer";

      var cb = document.createElement("input");
      cb.type = "checkbox"; cb.checked = true; cb.id = "cb_" + con.id;
      cb.style.accentColor = con.color;
      cb.onchange = function() {{ refreshLayers(); }};

      var dot = document.createElement("span");
      dot.style.cssText = "width:10px;height:10px;border-radius:50%;background:" + con.color + ";flex-shrink:0";

      var lbl = document.createElement("span");
      lbl.style.cssText = "font-size:11px;color:#1F1A2F;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px";
      lbl.title = con.label;
      lbl.textContent = con.label;

      row.appendChild(cb); row.appendChild(dot); row.appendChild(lbl);
      panel.appendChild(row);
    }});

    document.body.appendChild(panel);
  }}

  /* ─── inyectar layerId en las capas Folium ─── */
  function tagLayers() {{
    var lm = null;
    for (var k in window) {{
      if (window[k] && window[k]._layers && window[k].getCenter) {{ lm = window[k]; break; }}
    }}
    if (!lm) return;
    window.leafletMap = lm;

    lm.eachLayer(function(layer) {{
      if (layer.options && layer.options._ruta_lid) {{
        layer.options.layerId = layer.options._ruta_lid;
      }}
    }});
  }}

  function init() {{
    tagLayers();
    buildPanel();
  }}

  if (document.readyState === "loading") {{
    document.addEventListener("DOMContentLoaded", init);
  }} else {{
    setTimeout(init, 800);
  }}
}})();
</script>
"""


# ── Tabla resumen inferior ────────────────────────────────────────────────────

def _tabla_resumen_html(stats: list[dict]) -> str:
    """
    stats = [{"apellido": str, "color": str, "fecha": str,
              "paradas": int, "km": float, "hora_ini": str, "hora_fin": str}]
    Tabla fija inferior-derecha.
    """
    if not stats:
        return ""

    filas = ""
    for s in sorted(stats, key=lambda x: (x["apellido"], x["fecha"])):
        filas += (
            f"<tr>"
            f"<td><span style='display:inline-block;width:9px;height:9px;"
            f"border-radius:50%;background:{s['color']};margin-right:5px'></span>"
            f"<b>{s['apellido']}</b></td>"
            f"<td>{s['fecha']}</td>"
            f"<td style='text-align:center'>{s['paradas']}</td>"
            f"<td style='text-align:right'>{s['km']:.1f} km</td>"
            f"<td>{s['hora_ini']} – {s['hora_fin']}</td>"
            f"</tr>"
        )

    return f"""
<div id="tabla-resumen-rutas" style="
  position:fixed;bottom:12px;right:12px;z-index:9998;
  background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.15);
  font-family:Arial,sans-serif;font-size:11px;padding:10px 14px;
  max-width:420px;max-height:200px;overflow-y:auto">
  <table style="border-collapse:collapse;width:100%">
    <thead>
      <tr style="color:#6B7280;border-bottom:1px solid #E5E7EB">
        <th style="text-align:left;padding:2px 6px">Consultor</th>
        <th style="padding:2px 6px">Día</th>
        <th style="text-align:center;padding:2px 6px">Paradas</th>
        <th style="text-align:right;padding:2px 6px">Km est.</th>
        <th style="padding:2px 6px">Horario</th>
      </tr>
    </thead>
    <tbody>{filas}</tbody>
  </table>
</div>
"""


# ── Función principal ─────────────────────────────────────────────────────────

def generar_mapa_rutas(
    ciudad: str,
    f_ini: str,
    f_fin: str,
    ids_consultor: Sequence[int] | None = None,
) -> dict:
    """
    Genera el mapa interactivo de rutas.

    Returns:
        dict con claves:
          ok (bool), html_path (str), filename (str),
          n_consultores (int), n_eventos (int), error (str|None)
    """
    from pre_procesamiento.consultor_rutas import rutas_por_consultor

    ciudad_norm = _norm(ciudad)
    if ciudad_norm not in COORDENADAS_CIUDADES:
        return {"ok": False, "error": f"Ciudad no soportada: {ciudad!r}"}

    location, _ = COORDENADAS_CIUDADES[ciudad_norm]

    # ── 1. Obtener datos ──────────────────────────────────────────────────────
    df = rutas_por_consultor(ciudad, f_ini, f_fin, ids_consultor)
    if df is None or df.empty:
        return {"ok": False, "error": "No se encontraron eventos con coordenadas para los filtros aplicados."}

    # ── 2. Preparar estructura por consultor / día ─────────────────────────────
    df["fecha"] = df["fecha_evento"].dt.strftime("%Y-%m-%d")
    consultores_ids = sorted(df["id_autor"].dropna().unique().tolist())
    color_map = {cid: _color_consultor(i) for i, cid in enumerate(consultores_ids)}

    dias_ordenados = sorted(df["fecha"].unique().tolist())

    # ── 3. Crear mapa base ────────────────────────────────────────────────────
    mapa = folium.Map(
        location=location,
        zoom_start=13,
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
    )
    Fullscreen(position="topright").add_to(mapa)

    # ── 4. GeoJSON overlay ────────────────────────────────────────────────────
    _agregar_geojson(mapa, ciudad_norm)

    # ── 5. Trazar rutas ───────────────────────────────────────────────────────
    stats: list[dict] = []
    consultores_meta: list[dict] = []

    for cid in consultores_ids:
        df_c   = df[df["id_autor"] == cid].copy()
        color  = color_map[cid]
        apellido = str(df_c["apellido"].iloc[0]) if not df_c.empty else str(cid)
        nombre   = str(df_c["nombre"].iloc[0])   if not df_c.empty else ""
        label    = f"{apellido} {nombre[:1]}." if nombre else apellido

        dias_consultor = sorted(df_c["fecha"].unique().tolist())
        consultores_meta.append({
            "id": int(cid), "label": label,
            "color": color, "dias": dias_consultor,
        })

        for dia in dias_consultor:
            df_dia = df_c[df_c["fecha"] == dia].sort_values("fecha_evento").reset_index(drop=True)
            if df_dia.empty:
                continue

            coords = df_dia[["lat", "lon"]].astype(float).values.tolist()
            fg_id  = f"fg_{int(cid)}_{dia.replace('-', '')}"

            # FeatureGroup con layerId personalizado para control JS
            fg = folium.FeatureGroup(name=f"{label} · {dia}", show=True)
            fg.options["_ruta_lid"] = fg_id

            # AntPath (si ≥ 2 puntos)
            if len(coords) >= 2:
                AntPath(
                    locations=coords,
                    color=color,
                    weight=4,
                    delay=700,
                    dash_array=[10, 15],
                    pulse_color="#ffffff",
                    opacity=0.85,
                ).add_to(fg)

            # Marcadores numerados
            for seq, row in df_dia.iterrows():
                _marcador_numerado(
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    numero=seq + 1,
                    color=color,
                    hora=_fmt_hora(row["fecha_evento"]),
                    id_contacto=row.get("id_contacto", "—"),
                    tipo_evento=str(row.get("tipo_evento", "")),
                ).add_to(fg)

            fg.add_to(mapa)

            # Stats
            km = sum(
                _haversine_km(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
                for i in range(len(coords) - 1)
            )
            stats.append({
                "apellido": label,
                "color":    color,
                "fecha":    dia,
                "paradas":  len(df_dia),
                "km":       round(km, 2),
                "hora_ini": _fmt_hora(df_dia["fecha_evento"].iloc[0]),
                "hora_fin": _fmt_hora(df_dia["fecha_evento"].iloc[-1]),
            })

    # ── 6. Panel de control JS ────────────────────────────────────────────────
    mapa.get_root().html.add_child(
        folium.Element(_panel_control_js(consultores_meta, dias_ordenados))
    )

    # ── 7. Tabla resumen ──────────────────────────────────────────────────────
    mapa.get_root().html.add_child(
        folium.Element(_tabla_resumen_html(stats))
    )

    # ── 8. Layer control estándar (respaldo) ──────────────────────────────────
    folium.LayerControl(collapsed=True, position="topright").add_to(mapa)

    # ── 9. Guardar ────────────────────────────────────────────────────────────
    filename = guardar_mapa_controlado(
        mapa, tipo_mapa="rutas_consultor", permitir_multiples=False
    )
    html_path = str(Path("static") / "maps" / filename)

    return {
        "ok":           True,
        "html_path":    html_path,
        "filename":     filename,
        "n_consultores": len(consultores_ids),
        "n_eventos":    int(len(df)),
        "error":        None,
    }


__all__ = ["generar_mapa_rutas"]
