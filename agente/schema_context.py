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
=== ESQUEMA BD — Atlas Agent ===

SCHEMAS: fullclean_contactos | fullclean_telemercadeo | fullclean_cartera

─── TABLAS CLAVE ─────────────────────────────────────────────────────────────
ciudades       id, nombre, id_centroope  (Cali=2 Med=3 Bog=4 Per=5 Man=6 Buc=7 Bar=8)
contactos      id (PK), nombre, id_barrio, id_categoria
               → PK = .id  |  no-fieles: id_categoria NOT IN (42,55,58,59,60)
barrios        Id (PK), barrio (nombre), id_ciudad  → columna nombre = .barrio
vwEventos      id, id_contacto, id_autor(=promotor), fecha_evento,
               coordenada_latitud VARCHAR, coordenada_longitud VARCHAR
               → coords VARCHAR: CAST(... AS DECIMAL(10,6))  |  filtrar !='' !='0' IS NOT NULL
               → SIN id_centroope: filtrar ciudad via JOIN barrios→ciudades
quejas         id, id_contacto, cat(categoría), fecha, activa  → categoría en .cat

─── RUTAS ────────────────────────────────────────────────────────────────────
Logísticas:   rutas(id,nombre,id_centroope) + rutas_barrios(id_ruta,id_barrio)
Cobro:        rutas_cobro(id,ruta,id_cobrador,activa) + rutas_cobro_zonas(id,id_ruta_cobro,id_barrio)
              → nombre de ruta cobro = .ruta  (NO .nombre)

─── fullclean_telemercadeo (pedidos, llamadas) ───────────────────────────────
pedidos        id, id_contacto, fecha_hora_pedido, es_venta, id_cobro
               ⚠️ SIEMPRE fullclean_telemercadeo.pedidos — NUNCA en fullclean_cartera
pedidos_det    id_pedido, id_item, cantidad  (sin nombre_producto)
               ⚠️ SIEMPRE fullclean_telemercadeo.pedidos_det — NUNCA en fullclean_cartera
               → id_item FK → fullclean_bodega.items.id (para filtrar por nombre de producto)
llamadas       id, id_contacto, fecha_llamada, id_promotor
llamadas_resp  id_llamada, contestada(1/0), id_respuesta

─── fullclean_cartera (deuda) ────────────────────────────────────────────────
facturas       id, id_contacto, fecha_factura, total, saldo_pendiente, vencida
cobros         id, id_factura, fecha_cobro, valor_cobrado

─── fullclean_bodega (productos) ─────────────────────────────────────────────
items          id, nombre, id_presentacion
               → filtrar por producto: JOIN fullclean_bodega.items i ON i.id=pd.id_item
                 WHERE i.nombre LIKE '%NombreProducto%'

─── GOTCHAS ──────────────────────────────────────────────────────────────────
1. CAST coords: CAST(e.coordenada_latitud AS DECIMAL(10,6))  WHERE !='',!='0',IS NOT NULL
2. CIUDAD en vwEventos/rutas_cobro: JOIN barrios b ON b.Id=c.id_barrio
                                     JOIN ciudades ciu ON ciu.id=b.id_ciudad AND ciu.id_centroope=?
3. PROMOTOR en vwEventos = e.id_autor  (no id_promotor)
4. BARRIO nombre = b.barrio  |  RUTA COBRO nombre = rc.ruta  |  CONTACTO PK = c.id
5. QUEJAS categoría = q.cat  (no JOIN a tabla inconformidad)
6. pedidos/pedidos_det → fullclean_telemercadeo  |  facturas/cobros → fullclean_cartera
7. PRODUCTO por nombre → JOIN fullclean_bodega.items i ON i.id=pd.id_item WHERE i.nombre LIKE '%X%'
8. pedidos_det.id_pedido → pedidos.id  (NO se unen via facturas)

─── JOINS CRÍTICOS ───────────────────────────────────────────────────────────
# Ciudad por ruta cobro
FROM rutas_cobro rc
JOIN rutas_cobro_zonas rcz ON rcz.id_ruta_cobro=rc.id
JOIN barrios b ON b.Id=rcz.id_barrio
JOIN ciudades ciu ON ciu.id=b.id_ciudad AND ciu.id_centroope=?
JOIN contactos c ON c.id_barrio=b.Id

# Clientes con pedido de producto (por nombre — ej: BluePet)
SELECT c.id AS id_contacto, c.nombre
FROM fullclean_contactos.contactos c
JOIN fullclean_contactos.barrios b ON b.Id=c.id_barrio
JOIN fullclean_contactos.ciudades ciu ON ciu.id=b.id_ciudad AND ciu.id_centroope=4
JOIN fullclean_telemercadeo.pedidos p ON p.id_contacto=c.id
JOIN fullclean_telemercadeo.pedidos_det pd ON pd.id_pedido=p.id
JOIN fullclean_bodega.items i ON i.id=pd.id_item
WHERE p.es_venta=1 AND i.nombre LIKE '%BluePet%'
  AND p.fecha_hora_pedido BETWEEN '2026-01-01' AND '2026-03-31'
GROUP BY c.id, c.nombre

# Deuda vencida por ruta
SELECT rc.ruta, SUM(f.saldo_pendiente) deuda, COUNT(DISTINCT f.id_contacto) clientes
FROM fullclean_cartera.facturas f
JOIN fullclean_contactos.contactos c ON c.id=f.id_contacto
JOIN fullclean_contactos.barrios b ON b.Id=c.id_barrio
JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio=b.Id
JOIN fullclean_contactos.rutas_cobro rc ON rc.id=rcz.id_ruta_cobro
WHERE f.vencida=1 GROUP BY rc.id,rc.ruta

─── FOLIUM (ejecutar_codigo_mapa) ────────────────────────────────────────────
TILES='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}'
CircleMarker radius=5-6 para puntos | HeatMap para densidad | MarkerCluster para >500 pts
Variable destino: mapa  (NO llamar mapa.save())
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
