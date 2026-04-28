"""Ejecutor de código de mapa dinámico para Atlas Agent.

El agente escribe código Python + SQL + Folium como string y lo ejecuta aquí
en un namespace controlado. El código debe asignar el mapa a la variable `mapa`.

Ejemplo de código que el agente genera:
─────────────────────────────────────────
import folium
from folium.plugins import MarkerCluster

# Query
df = sql_read('''
    SELECT c.id, c.nombre,
           CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
           CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
           MAX(e.fecha_evento) AS ultima_visita
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.contactos c ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.ciudades ciu
         ON ciu.id = b.id_ciudad AND ciu.id_centroope = 3
    WHERE e.coordenada_latitud != '' AND e.coordenada_latitud != '0'
      AND e.coordenada_latitud IS NOT NULL
    GROUP BY c.id, c.nombre
''', schema='fullclean_contactos')

# Mapa
mapa = folium.Map(
    location=[6.2442, -75.5812],
    zoom_start=12,
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}',
    attr='Esri'
)
cluster = MarkerCluster().add_to(mapa)
for _, row in df.iterrows():
    folium.Marker([row['lat'], row['lon']], popup=row['nombre']).add_to(cluster)
─────────────────────────────────────────

El resultado se guarda automáticamente en static/maps/ con nombre único.
"""

from __future__ import annotations

import traceback
import textwrap
import re
from datetime import datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
_MAPS_DIR = _ROOT / "static" / "maps"
_MAPS_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# NAMESPACE SEGURO
# ─────────────────────────────────────────────────────────────────────────────

def _build_namespace(ciudad: int | None = None) -> dict[str, Any]:
    """Construye el namespace que estará disponible en el código del agente."""
    import folium
    import folium.plugins  # noqa: F401 — available as folium.plugins in exec
    import pandas as pd

    # sql_read de la utilidad del proyecto
    import sys
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    from utils.db import sql_read  # type: ignore[import]

    namespace: dict[str, Any] = {
        # Librerías disponibles
        "folium": folium,
        "pd": pd,
        "json": __import__("json"),
        "datetime": datetime,
        "Path": Path,
        # Utilidad BD
        "sql_read": sql_read,
        # Constante de ciudad por defecto (puede ser usada por el código)
        "CIUDAD": ciudad,
        # Builtins seguros
        "print": print,
        "range": range,
        "len": len,
        "list": list,
        "dict": dict,
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "min": min,
        "max": max,
        "abs": abs,
        "round": round,
        "zip": zip,
        "enumerate": enumerate,
        "sorted": sorted,
        "sum": sum,
        "any": any,
        "all": all,
        # Variable de resultado — el código del agente debe asignar aquí
        "mapa": None,
    }
    return namespace


# ─────────────────────────────────────────────────────────────────────────────
# VALIDACIÓN BÁSICA DE SEGURIDAD
# ─────────────────────────────────────────────────────────────────────────────

# Palabras clave que nunca deben aparecer en código generado por el agente
_BLOCKED_PATTERNS = [
    r"\bos\.system\b",
    r"\bsubprocess\b",
    r"\beval\s*\(",
    r"\bexec\s*\(",
    r"\b__import__\s*\(",
    r"\bopen\s*\(",        # no lectura/escritura arbitraria de archivos
    r"\bshutil\b",
    r"\bpickle\b",
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bCREATE\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
]

_BLOCKED_RE = re.compile("|".join(_BLOCKED_PATTERNS), re.IGNORECASE)


def _validar_codigo(codigo: str) -> str | None:
    """Devuelve mensaje de error si el código contiene patrones bloqueados, o None si es seguro."""
    match = _BLOCKED_RE.search(codigo)
    if match:
        return f"Código rechazado: patrón no permitido detectado → '{match.group()}'"
    if "mapa" not in codigo:
        return "El código debe asignar el mapa Folium a la variable 'mapa'."
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_codigo_mapa(codigo: str, ciudad: int | None = None, nombre: str = "") -> dict[str, Any]:
    """Ejecuta código Python generado por el agente y guarda el mapa resultante.

    Args:
        codigo:  Código Python con Folium. Debe asignar el objeto folium.Map a `mapa`.
        ciudad:  id_centroope de ciudad para inyectar como constante CIUDAD en el namespace.
        nombre:  Prefijo opcional para el nombre del archivo (ej: "cobertura_medellin").

    Returns:
        dict con:
          - ok (bool)          : True si se generó el mapa
          - html_path (str)    : ruta al archivo HTML (solo si ok=True)
          - error (str)        : mensaje de error (solo si ok=False)
          - lineas_ejecutadas  : número de líneas del código
    """
    # 1. Validación
    error_val = _validar_codigo(codigo)
    if error_val:
        return {"ok": False, "error": error_val}

    # 2. Namespace controlado
    ns = _build_namespace(ciudad)

    # 3. Ejecución
    try:
        exec(textwrap.dedent(codigo), ns)  # noqa: S102
    except Exception:
        tb = traceback.format_exc()
        return {"ok": False, "error": f"Error ejecutando código:\n{tb}"}

    # 4. Recuperar mapa
    mapa = ns.get("mapa")
    if mapa is None:
        return {
            "ok": False,
            "error": "El código ejecutó sin errores pero no asignó ningún valor a 'mapa'.",
        }

    # 5. Guardar HTML
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefijo = re.sub(r"[^a-z0-9_]", "_", nombre.lower()) if nombre else "mapa_agente"
    filename = f"{prefijo}_{ts}.html"
    html_path = _MAPS_DIR / filename

    try:
        mapa.save(str(html_path))
    except Exception as e:
        return {"ok": False, "error": f"Error guardando HTML: {e}"}

    return {
        "ok": True,
        "html_path": str(html_path),
        "filename": filename,
        "lineas_ejecutadas": len([l for l in codigo.splitlines() if l.strip()]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# TOOL DEFINITION para atlas_agent.py
# ─────────────────────────────────────────────────────────────────────────────

TOOL_DEFINICION = {
    "name": "ejecutar_codigo_mapa",
    "description": (
        "Ejecuta código Python + Folium generado por el agente para crear un mapa interactivo. "
        "El código puede hacer queries SQL con sql_read(), procesar resultados con pandas, "
        "y construir cualquier mapa Folium. El código DEBE asignar el mapa a la variable 'mapa'. "
        "Devuelve la ruta al HTML generado. "
        "Úsala para CUALQUIER visualización geográfica: cobertura de rutas, puntos de clientes, "
        "heatmaps de visitas, círculos de deuda, clusters por barrio, etc."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "codigo": {
                "type": "string",
                "description": (
                    "Código Python completo. Debe: (1) importar folium, "
                    "(2) usar sql_read(sql, schema='fullclean_contactos') para queries, "
                    "(3) construir el mapa con tiles Esri, "
                    "(4) asignar el objeto folium.Map a la variable `mapa`. "
                    "NO llamar mapa.save() — el ejecutor lo hace automáticamente."
                ),
            },
            "ciudad": {
                "type": "integer",
                "description": "id_centroope de la ciudad (disponible como constante CIUDAD en el código).",
            },
            "nombre": {
                "type": "string",
                "description": "Prefijo descriptivo para el nombre del archivo HTML (ej: 'cobertura_medellin').",
            },
        },
        "required": ["codigo"],
    },
}
