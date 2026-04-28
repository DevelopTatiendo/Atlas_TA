"""Renderer de mapas Folium para Atlas Agent.

Recibe un DataFrame estándar (id_contacto, lat, lon + columnas del agente)
y produce un mapa HTML. El agente nunca escribe código Folium — solo elige
el tipo de mapa y las columnas a visualizar.

Tipos disponibles:
  - puntos_bicolor          visitado/no visitado (verde/rojo)
  - circulos_proporcionales tamaño ∝ campo_valor (ej: deuda)
  - heatmap                 densidad de actividad
  - clusters                agrupación automática con popups

Uso típico desde generar_mapa_clientes:
    df = sql_result merged with buscar_coords(...)
    html_path = pintar_mapa(df, tipo="circulos_proporcionales",
                             campo_valor="deuda_total",
                             titulo="Deuda Cali > $50K")
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────

_ROOT     = Path(__file__).resolve().parent.parent
_MAPS_DIR = _ROOT / "static" / "maps"
_MAPS_DIR.mkdir(parents=True, exist_ok=True)

_TILES = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/"
    "World_Street_Map/MapServer/tile/{z}/{y}/{x}"
)
_ATTR = "Esri"

# Centros aproximados por id_centroope para mapa vacío
_CENTROS: dict[int, tuple[float, float]] = {
    2: (3.4516, -76.5320),   # Cali
    3: (6.2442, -75.5812),   # Medellín
    4: (4.7110, -74.0721),   # Bogotá
    5: (4.8133, -75.6961),   # Pereira
    6: (5.0703, -75.5138),   # Manizales
    7: (7.1193, -73.1227),   # Bucaramanga
    8: (10.9685, -74.7813),  # Barranquilla
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _nombre_archivo(nombre: str) -> str:
    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug   = re.sub(r"[^a-z0-9_]", "_", nombre.lower()) if nombre else "mapa"
    return f"{slug}_{ts}.html"


def _centro(df: pd.DataFrame, ciudad_id: int | None) -> tuple[float, float]:
    """Calcula el centro del mapa a partir del DataFrame o usa el default de la ciudad."""
    df_geo = df[df["lat"].notna() & df["lon"].notna()]
    if not df_geo.empty:
        return float(df_geo["lat"].mean()), float(df_geo["lon"].mean())
    if ciudad_id and ciudad_id in _CENTROS:
        return _CENTROS[ciudad_id]
    return (4.5709, -74.2973)  # Colombia centro


def _base_map(df: pd.DataFrame, ciudad_id: int | None, zoom: int = 13):
    import folium
    lat, lon = _centro(df, ciudad_id)
    return folium.Map(location=[lat, lon], zoom_start=zoom, tiles=_TILES, attr=_ATTR)


def _popup_text(row: pd.Series, excluir: list[str] | None = None) -> str:
    """Genera texto de popup mostrando todas las columnas relevantes del row."""
    excluir = set(excluir or ["lat", "lon", "id_contacto"])
    partes  = []
    nombre  = row.get("nombre", f"ID {row.get('id_contacto','?')}")
    partes.append(f"<b>{nombre}</b>")
    for col, val in row.items():
        if col in excluir or col == "nombre" or pd.isna(val):
            continue
        # Formatear valores monetarios grandes
        if isinstance(val, (int, float)) and abs(val) > 1000:
            partes.append(f"{col}: ${val:,.0f}")
        else:
            partes.append(f"{col}: {val}")
    return "<br>".join(partes)


# ─────────────────────────────────────────────────────────────────────────────
# Renderizadores por tipo
# ─────────────────────────────────────────────────────────────────────────────

def _render_puntos_bicolor(mapa, df: pd.DataFrame, campo_color: str,
                            color_verdadero: str = "green",
                            color_falso: str = "red") -> int:
    """Puntos verde/rojo según campo booleano/0-1."""
    import folium
    n = 0
    for _, row in df.iterrows():
        if pd.isna(row["lat"]):
            continue
        val   = row.get(campo_color, 0)
        color = color_verdadero if val else color_falso
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            popup=folium.Popup(_popup_text(row), max_width=250),
        ).add_to(mapa)
        n += 1
    return n


def _render_circulos_proporcionales(mapa, df: pd.DataFrame, campo_valor: str,
                                    color: str = "#DC2626") -> int:
    """Círculos cuyo radio es proporcional a campo_valor."""
    import folium
    vals_validos = pd.to_numeric(df[campo_valor], errors="coerce").dropna()
    if vals_validos.empty:
        return 0

    vmax = float(vals_validos.quantile(0.95))  # percentil 95 para evitar outliers gigantes
    vmin = float(vals_validos.min())

    n = 0
    for _, row in df.iterrows():
        if pd.isna(row["lat"]):
            continue
        val = pd.to_numeric(row.get(campo_valor, 0), errors="coerce")
        if pd.isna(val) or val <= 0:
            continue
        # Radio: 6–40 metros de visualización (Folium usa metros reales)
        radio = 6 + 34 * ((float(val) - vmin) / (vmax - vmin + 1))
        radio = max(6, min(40, radio)) * 25  # escalar a metros aprox

        folium.Circle(
            location=[row["lat"], row["lon"]],
            radius=radio,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.45,
            popup=folium.Popup(_popup_text(row), max_width=250),
        ).add_to(mapa)
        n += 1
    return n


def _render_heatmap(mapa, df: pd.DataFrame, campo_peso: str | None = None) -> int:
    """Heatmap de densidad de puntos."""
    from folium.plugins import HeatMap
    filas = []
    for _, row in df.iterrows():
        if pd.isna(row["lat"]):
            continue
        peso = 1.0
        if campo_peso and campo_peso in row:
            p = pd.to_numeric(row[campo_peso], errors="coerce")
            if not pd.isna(p):
                peso = max(0.1, float(p))
        filas.append([float(row["lat"]), float(row["lon"]), peso])
    if filas:
        HeatMap(filas, radius=18, blur=12, min_opacity=0.3).add_to(mapa)
    return len(filas)


def _render_clusters(mapa, df: pd.DataFrame) -> int:
    """Markers agrupados en clusters con popup completo."""
    import folium
    from folium.plugins import MarkerCluster
    cluster = MarkerCluster().add_to(mapa)
    n = 0
    for _, row in df.iterrows():
        if pd.isna(row["lat"]):
            continue
        folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(_popup_text(row), max_width=280),
            icon=folium.Icon(color="blue", icon="user", prefix="fa"),
        ).add_to(cluster)
        n += 1
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Función pública principal
# ─────────────────────────────────────────────────────────────────────────────

TIPOS_VALIDOS = {"puntos_bicolor", "circulos_proporcionales", "heatmap", "clusters"}


def pintar_mapa(
    df: pd.DataFrame,
    tipo: str,
    titulo: str = "",
    ciudad_id: int | None = None,
    campo_valor: str | None = None,
    campo_color: str | None = None,
    color_verdadero: str = "green",
    color_falso: str = "red",
    color_circulos: str = "#DC2626",
    nombre_archivo: str = "",
) -> dict:
    """Genera un mapa HTML a partir de un DataFrame estándar.

    El DataFrame DEBE tener columnas: id_contacto, lat, lon.
    Las columnas adicionales se usan para colores/tamaños y popups.

    Args:
        df:               DataFrame con id_contacto, lat, lon + atributos extra.
        tipo:             Tipo de visualización (ver TIPOS_VALIDOS).
        titulo:           Título mostrado en el nombre del archivo.
        ciudad_id:        id_centroope para centrar el mapa si no hay coords.
        campo_valor:      Columna numérica para circulos_proporcionales.
        campo_color:      Columna 0/1 para puntos_bicolor.
        color_verdadero:  Color cuando campo_color == 1 (default: green).
        color_falso:      Color cuando campo_color == 0 (default: red).
        color_circulos:   Color base para círculos proporcionales.
        nombre_archivo:   Prefijo para el nombre del HTML generado.

    Returns:
        Dict con: ok, html_path, filename, n_puntos, n_sin_coords, tipo.
    """
    import folium  # import local para no fallar si folium no está disponible en tests

    if tipo not in TIPOS_VALIDOS:
        return {"ok": False, "error": f"Tipo '{tipo}' inválido. Válidos: {TIPOS_VALIDOS}"}

    if df.empty:
        return {"ok": False, "error": "DataFrame vacío — sin clientes para mapear."}

    if "lat" not in df.columns or "lon" not in df.columns:
        return {"ok": False, "error": "El DataFrame debe tener columnas 'lat' y 'lon'."}

    # Asegurar tipos numéricos
    df = df.copy()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    n_total      = len(df)
    n_sin_coords = int(df["lat"].isna().sum())

    mapa = _base_map(df, ciudad_id)

    # Inyectar título como control HTML
    if titulo:
        title_html = f"""
        <div style="position:fixed;top:12px;left:50%;transform:translateX(-50%);
                    background:rgba(255,255,255,0.92);border-radius:8px;
                    padding:8px 18px;font-size:14px;font-weight:600;
                    box-shadow:0 2px 8px rgba(0,0,0,0.15);z-index:9999;">
          {titulo}
        </div>"""
        mapa.get_root().html.add_child(folium.Element(title_html))

    # Renderizar según tipo
    if tipo == "puntos_bicolor":
        if not campo_color:
            return {"ok": False, "error": "puntos_bicolor requiere campo_color."}
        n_puntos = _render_puntos_bicolor(mapa, df, campo_color, color_verdadero, color_falso)

    elif tipo == "circulos_proporcionales":
        if not campo_valor:
            return {"ok": False, "error": "circulos_proporcionales requiere campo_valor."}
        n_puntos = _render_circulos_proporcionales(mapa, df, campo_valor, color_circulos)

    elif tipo == "heatmap":
        n_puntos = _render_heatmap(mapa, df, campo_peso=campo_valor)

    elif tipo == "clusters":
        n_puntos = _render_clusters(mapa)

    else:
        n_puntos = 0

    # Guardar HTML
    fname     = _nombre_archivo(nombre_archivo or titulo or tipo)
    html_path = _MAPS_DIR / fname
    mapa.save(str(html_path))

    return {
        "ok":          True,
        "html_path":   str(html_path),
        "filename":    fname,
        "n_puntos":    n_puntos,
        "n_total":     n_total,
        "n_sin_coords": n_sin_coords,
        "tipo":        tipo,
    }
