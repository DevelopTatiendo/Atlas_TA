"""Contexto de esquema fijo para Atlas Agent.

Este módulo expone un bloque de texto compacto (~900 tokens) con:
  - Tablas clave y columnas relevantes
  - Los dos sistemas de rutas (logístico vs cobro)
  - Gotchas SQL críticos descubiertos en producción
  - Patrones Folium para los 4 tipos de mapa más usados
  - 5 JOINs críticos como referencia

Cargado una vez por sesión e inyectado en el system prompt del agente.
"""

from __future__ import annotations
from pathlib import Path
import json

# ─────────────────────────────────────────────────────────────────────────────
# ESQUEMA FIJO  (~900 tokens)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_TEXT = '''
=== ESQUEMA BD — Atlas Agent (referencia fija) ===

SCHEMAS PRINCIPALES: fullclean_contactos | fullclean_telemercadeo | fullclean_cartera

─── TABLAS CLAVE ─────────────────────────────────────────────────────────────

ciudades          id, nombre, id_centroope
                  → id_centroope es el identificador de ciudad usado en filtros
                  → Cali=2, Medellín=3, Bogotá=4, Pereira=5, Manizales=6, Bucaramanga=7, Barranquilla=8

contactos         id (PK), nombre, id_barrio, id_categoria
                  → PK es .id, NO .id_contacto
                  → no-fieles: id_categoria NOT IN (42, 55, 58, 59, 60)

barrios           Id (PK), barrio (nombre), id_ciudad
                  → columna nombre = .barrio, NO .nombre

vwEventos         id, id_contacto, id_autor (= promotor), fecha_evento,
                  coordenada_latitud VARCHAR, coordenada_longitud VARCHAR
                  → id_autor es el promotor; no existe id_promotor en esta vista
                  → coords son VARCHAR: CAST(coordenada_latitud AS DECIMAL(10,6))
                  → coordenada válida: != '' AND != '0' AND IS NOT NULL
                  → NO tiene id_centroope; filtrar por ciudad = JOIN ciudades vía barrios

quejas            id, id_contacto, cat (categoría), fecha, activa
                  → categoría en columna .cat, no JOIN a tabla inconformidad

─── DOS SISTEMAS DE RUTAS ────────────────────────────────────────────────────

SISTEMA 1 — Logístico (zonas geográficas):
  rutas           id, nombre, id_centroope  ← tiene id_centroope directo
  rutas_barrios   id_ruta, id_barrio

SISTEMA 2 — Cobro (por persona, con flags de pago):
  rutas_cobro       id, ruta (nombre), id_cobrador, activa, meta_cobro
                    → columna nombre = .ruta, NO .nombre
  rutas_cobro_zonas id, id_ruta_cobro, id_barrio

─── TABLAS TELEMERCADEO ──────────────────────────────────────────────────────

pedidos           id, id_contacto, fecha_hora_pedido, es_venta, id_cobro
pedidos_det       id_pedido, id_item, cantidad
                  → NO tiene nombre_producto; usar id_item

llamadas          id, id_contacto, fecha_llamada, id_promotor
llamadas_respuestas id_llamada, contestada (1/0), id_respuesta

─── TABLAS CARTERA ───────────────────────────────────────────────────────────

facturas          id, id_contacto, fecha_factura, total, saldo_pendiente, vencida
cobros            id, id_factura, fecha_cobro, valor_cobrado

─── GOTCHAS CRÍTICOS ─────────────────────────────────────────────────────────

1. COORDENADAS son VARCHAR → siempre CAST:
   CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS latitud
   CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS longitud
   WHERE e.coordenada_latitud != '' AND e.coordenada_latitud != '0'
     AND e.coordenada_latitud IS NOT NULL

2. CIUDAD FILTER en vwEventos y rutas_cobro requiere JOIN:
   INNER JOIN barrios b ON b.Id = c.id_barrio
   INNER JOIN ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad

3. PROMOTOR en vwEventos = e.id_autor (no id_promotor)

4. RUTA COBRO nombre = rc.ruta (no rc.nombre)

5. BARRIO nombre = b.barrio (no b.nombre)

6. CONTACTOS PK = c.id (no c.id_contacto)

7. QUEJAS categoría = q.cat (no JOIN a tabla inconformidad)

─── JOINS CRÍTICOS DE REFERENCIA ─────────────────────────────────────────────

# J1: Clientes de una ciudad de cobro (rutas_cobro)
FROM rutas_cobro rc
JOIN rutas_cobro_zonas rcz ON rcz.id_ruta_cobro = rc.id
JOIN barrios b ON b.Id = rcz.id_barrio
JOIN ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
JOIN contactos c ON c.id_barrio = b.Id

# J2: Eventos con coords válidas filtrando por ciudad
FROM vwEventos e
JOIN contactos c ON c.id = e.id_contacto
JOIN barrios b ON b.Id = c.id_barrio
JOIN ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
WHERE CAST(e.coordenada_latitud AS DECIMAL(10,6)) BETWEEN -12 AND -1
  AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN -82 AND -66

# J3: Cobertura de ruta cobro en período
SELECT rc.id, rc.ruta,
  COUNT(DISTINCT c.id) AS n_clientes,
  COUNT(DISTINCT e.id_contacto) AS visitados
FROM rutas_cobro rc
JOIN rutas_cobro_zonas rcz ON rcz.id_ruta_cobro = rc.id
JOIN barrios b ON b.Id = rcz.id_barrio
JOIN ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
LEFT JOIN contactos c ON c.id_barrio = b.Id
LEFT JOIN vwEventos e ON e.id_contacto = c.id AND e.fecha_evento >= :fi
GROUP BY rc.id, rc.ruta

# J4: Contactabilidad real (llamadas con respuesta)
SELECT c.id, c.nombre,
  SUM(lr.contestada) AS llamadas_contestadas
FROM contactos c
JOIN llamadas l ON l.id_contacto = c.id AND l.fecha_llamada >= :fi
JOIN llamadas_respuestas lr ON lr.id_llamada = l.id
GROUP BY c.id, c.nombre

# J5: Deuda vencida por ruta cobro
SELECT rc.ruta,
  SUM(f.saldo_pendiente) AS deuda_total,
  COUNT(DISTINCT f.id_contacto) AS clientes_con_deuda
FROM fullclean_cartera.facturas f
JOIN contactos c ON c.id = f.id_contacto
JOIN barrios b ON b.Id = c.id_barrio
JOIN rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
JOIN rutas_cobro rc ON rc.id = rcz.id_ruta_cobro
WHERE f.vencida = 1
GROUP BY rc.id, rc.ruta

─── PATRONES FOLIUM ──────────────────────────────────────────────────────────

TILES (usar siempre este provider — funciona en file://):
  folium.Map(location=[lat, lon], zoom_start=13,
             tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}',
             attr='Esri')

PUNTOS BICOLOR (visitados=verde / sin visitar=rojo):
  color = 'green' if row['visitado'] else 'red'
  folium.CircleMarker([lat, lon], radius=6, color=color, fill=True).add_to(mapa)

HEATMAP de densidad:
  from folium.plugins import HeatMap
  HeatMap([[lat, lon, peso], ...]).add_to(mapa)

CÍRCULOS PROPORCIONALES (ej: deuda):
  radio = max(5, min(30, valor / 1_000_000 * 10))
  folium.Circle([lat, lon], radius=radio*100, popup=f"${valor:,.0f}").add_to(mapa)

CLUSTERS para muchos puntos:
  from folium.plugins import MarkerCluster
  cluster = MarkerCluster().add_to(mapa)
  folium.Marker([lat, lon], popup=texto).add_to(cluster)

GUARDAR: mapa.save('/ruta/al/archivo.html')

─── TIPOS DE MAPA — PLANTILLAS COMPLETAS ────────────────────────────────────

## TIPO 1: COBERTURA DE RUTA (visitados verde / sin visitar rojo)
```python
import folium
from folium.plugins import MarkerCluster
TILES = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}'

df = sql_read("""
    SELECT c.id, c.nombre,
           CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
           CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
           MAX(e.fecha_evento) AS ultima_visita,
           1 AS visitado
    FROM fullclean_contactos.contactos c
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
    JOIN fullclean_contactos.rutas_cobro rc ON rc.id = rcz.id_ruta_cobro
    JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
    JOIN fullclean_contactos.vwEventos e ON e.id_contacto = c.id AND e.fecha_evento >= :fi
    WHERE e.coordenada_latitud != '' AND e.coordenada_latitud != '0' AND e.coordenada_latitud IS NOT NULL
      AND rc.ruta LIKE :ruta
    GROUP BY c.id, c.nombre, lat, lon
""", params={"ciudad": CIUDAD, "fi": "2026-01-01", "ruta": "%Laureles%"}, schema="fullclean_contactos")

df_sinvisitar = sql_read("""
    SELECT c.id, c.nombre, NULL AS lat, NULL AS lon, NULL AS ultima_visita, 0 AS visitado
    FROM fullclean_contactos.contactos c
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
    JOIN fullclean_contactos.rutas_cobro rc ON rc.id = rcz.id_ruta_cobro
    JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
    WHERE rc.ruta LIKE :ruta
""", params={"ciudad": CIUDAD, "ruta": "%Laureles%"}, schema="fullclean_contactos")

df_todos = pd.concat([df, df_sinvisitar]).drop_duplicates("id", keep="first")
df_geo = df_todos[df_todos["lat"].notna() & (df_todos["lat"] != 0)]

centro = [df_geo["lat"].mean(), df_geo["lon"].mean()] if not df_geo.empty else [6.24, -75.58]
mapa = folium.Map(location=centro, zoom_start=14, tiles=TILES, attr='Esri')
for _, row in df_geo.iterrows():
    color = "green" if row["visitado"] else "red"
    folium.CircleMarker([row["lat"], row["lon"]], radius=7, color=color, fill=True,
                        fill_opacity=0.8,
                        popup=f"{row['nombre']}<br>Última: {row['ultima_visita']}").add_to(mapa)
```

## TIPO 2: DEUDA VENCIDA (círculos proporcionales al saldo)
```python
import folium
TILES = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}'

df = sql_read("""
    SELECT c.id, c.nombre,
           CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
           CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
           SUM(f.saldo_pendiente) AS deuda_total
    FROM fullclean_cartera.facturas f
    JOIN fullclean_contactos.contactos c ON c.id = f.id_contacto
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
    JOIN fullclean_contactos.vwEventos e ON e.id_contacto = c.id
    WHERE f.vencida = 1
      AND e.coordenada_latitud != '' AND e.coordenada_latitud != '0' AND e.coordenada_latitud IS NOT NULL
    GROUP BY c.id, c.nombre, lat, lon
    HAVING deuda_total > 0
""", params={"ciudad": CIUDAD}, schema="fullclean_cartera")

df = df[df["lat"].notna() & (df["lat"] != 0)]
centro = [df["lat"].mean(), df["lon"].mean()] if not df.empty else [6.24, -75.58]
mapa = folium.Map(location=centro, zoom_start=13, tiles=TILES, attr='Esri')
for _, row in df.iterrows():
    radio = max(8, min(50, float(row["deuda_total"]) / 50_000))
    folium.Circle([row["lat"], row["lon"]], radius=radio * 15,
                  color="#DC2626", fill=True, fill_opacity=0.5,
                  popup=f"{row['nombre']}<br>Deuda: ${row['deuda_total']:,.0f}").add_to(mapa)
```

## TIPO 3: CALOR DE ACTIVIDAD (heatmap de visitas)
```python
import folium
from folium.plugins import HeatMap
TILES = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}'

df = sql_read("""
    SELECT CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
           CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
           COUNT(*) AS visitas
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.contactos c ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
    WHERE e.fecha_evento >= :fi
      AND e.coordenada_latitud != '' AND e.coordenada_latitud != '0' AND e.coordenada_latitud IS NOT NULL
    GROUP BY lat, lon
""", params={"ciudad": CIUDAD, "fi": "2026-04-01"}, schema="fullclean_contactos")

df = df[df["lat"].notna() & (df["lat"] != 0)]
centro = [df["lat"].mean(), df["lon"].mean()] if not df.empty else [6.24, -75.58]
mapa = folium.Map(location=centro, zoom_start=13, tiles=TILES, attr='Esri')
heat_data = [[row["lat"], row["lon"], row["visitas"]] for _, row in df.iterrows()]
HeatMap(heat_data, radius=15, blur=10).add_to(mapa)
```

## TIPO 4: CLIENTES ACTIVOS con clusters
```python
import folium
from folium.plugins import MarkerCluster
TILES = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}'

df = sql_read("""
    SELECT c.id, c.nombre, rc.ruta,
           CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
           CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
           MAX(e.fecha_evento) AS ultima_visita
    FROM fullclean_contactos.vwEventos e
    JOIN fullclean_contactos.contactos c ON c.id = e.id_contacto
    JOIN fullclean_contactos.barrios b ON b.Id = c.id_barrio
    JOIN fullclean_contactos.ciudades ciu ON ciu.id = b.id_ciudad AND ciu.id_centroope = :ciudad
    LEFT JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
    LEFT JOIN fullclean_contactos.rutas_cobro rc ON rc.id = rcz.id_ruta_cobro
    WHERE e.fecha_evento >= :fi
      AND e.coordenada_latitud != '' AND e.coordenada_latitud != '0' AND e.coordenada_latitud IS NOT NULL
    GROUP BY c.id, c.nombre, rc.ruta, lat, lon
""", params={"ciudad": CIUDAD, "fi": "2026-04-01"}, schema="fullclean_contactos")

df = df[df["lat"].notna() & (df["lat"] != 0)]
centro = [df["lat"].mean(), df["lon"].mean()] if not df.empty else [6.24, -75.58]
mapa = folium.Map(location=centro, zoom_start=13, tiles=TILES, attr='Esri')
cluster = MarkerCluster().add_to(mapa)
for _, row in df.iterrows():
    folium.Marker([row["lat"], row["lon"]],
                  popup=f"{row['nombre']}<br>Ruta: {row['ruta']}<br>Última: {row['ultima_visita']}",
                  icon=folium.Icon(color="blue", icon="user")).add_to(cluster)
```
=== FIN ESQUEMA ===
'''

# ─────────────────────────────────────────────────────────────────────────────
# BANCO DE EJEMPLOS  (few-shot learning desde archivos jsonl)
# ─────────────────────────────────────────────────────────────────────────────

_EXAMPLES_DIR = Path(__file__).resolve().parent / "ejemplos"
_BUENAS_PATH  = _EXAMPLES_DIR / "buenas.jsonl"
_ERRORES_PATH = _EXAMPLES_DIR / "errores.jsonl"

MAX_BUENAS  = 5   # máximo ejemplos buenos a inyectar
MAX_ERRORES = 3   # máximo errores recientes a inyectar


def _cargar_jsonl(path: Path, n: int) -> list[dict]:
    """Carga las últimas n entradas de un archivo JSONL."""
    if not path.exists():
        return []
    lineas = [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    ultimas = lineas[-n:]
    resultado = []
    for linea in ultimas:
        try:
            resultado.append(json.loads(linea))
        except json.JSONDecodeError:
            pass
    return resultado


def cargar_ejemplos_texto() -> str:
    """Devuelve bloque de texto con ejemplos buenos y errores para el system prompt."""
    _EXAMPLES_DIR.mkdir(exist_ok=True)

    buenas  = _cargar_jsonl(_BUENAS_PATH, MAX_BUENAS)
    errores = _cargar_jsonl(_ERRORES_PATH, MAX_ERRORES)

    if not buenas and not errores:
        return ""

    partes = ["=== EJEMPLOS DE CONSULTAS ==="]

    if buenas:
        partes.append("\n-- CONSULTAS CORRECTAS (usar como modelo) --")
        for e in buenas:
            pregunta = e.get("pregunta", "")
            sql      = e.get("sql", e.get("codigo", ""))
            partes.append(f"P: {pregunta}\nSQL:\n{sql}")

    if errores:
        partes.append("\n-- ERRORES CONOCIDOS (NUNCA repetir estos patrones) --")
        for e in errores:
            error    = e.get("error", "")
            patron   = e.get("patron_incorrecto", "")
            fix      = e.get("fix", "")
            partes.append(f"ERROR: {error}\nMAL: {patron}\nBIEN: {fix}")

    partes.append("=== FIN EJEMPLOS ===")
    return "\n".join(partes)


def registrar_buena(pregunta: str, sql: str, herramienta: str = "") -> None:
    """Registra una consulta exitosa en el banco de ejemplos."""
    _EXAMPLES_DIR.mkdir(exist_ok=True)
    entrada = {"pregunta": pregunta, "sql": sql, "herramienta": herramienta}
    with _BUENAS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")


def registrar_error(error: str, patron_incorrecto: str, fix: str) -> None:
    """Registra un error y su corrección en el banco de errores."""
    _EXAMPLES_DIR.mkdir(exist_ok=True)
    entrada = {"error": error, "patron_incorrecto": patron_incorrecto, "fix": fix}
    with _ERRORES_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")


def get_full_context() -> str:
    """Devuelve esquema + ejemplos como string único para inyectar en system prompt."""
    ejemplos = cargar_ejemplos_texto()
    if ejemplos:
        return SCHEMA_TEXT.strip() + "\n\n" + ejemplos
    return SCHEMA_TEXT.strip()
