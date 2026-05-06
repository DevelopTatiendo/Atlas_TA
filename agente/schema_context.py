"""Contexto de esquema fijo para Atlas Agent.

Expone SCHEMA_TEXT (~1200 tokens) inyectado en el system prompt:
  - Tablas clave y columnas relevantes
  - IDs de CO desde agente.constants (fuente única)
  - Reglas críticas de negocio: pedido válido, llamadas, contactabilidad, mapas
  - Cadenas de JOIN más usadas como referencia
  - Banco de aprendizaje: buenas.jsonl / errores.jsonl
"""

from __future__ import annotations
from pathlib import Path
import json

from agente.constants import CIUDADES_LABEL

# ─────────────────────────────────────────────────────────────────────────────
# ESQUEMA FIJO
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_TEXT = f'''
=== ESQUEMA BD — Atlas Agent ===

SCHEMAS: fullclean_contactos | fullclean_telemercadeo | fullclean_cartera | fullclean_bodega | fullclean_personal | fullclean_general

─── CENTROS DE OPERACIÓN (id_centroope) ──────────────────────────────────────
{CIUDADES_LABEL}
Join obligatorio para filtrar ciudad:
    INNER JOIN fullclean_contactos.ciudades ci ON ci.id = c.id_ciudad
    Filtrar: WHERE ci.id_centroope = [N]

─── TABLAS CLAVE ─────────────────────────────────────────────────────────────
contactos      id (PK→alias id_contacto), nombre, id_barrio, id_ciudad,
               id_categoria, id_canal(2=directo), id_vendedor, id_base,
               id_subestado, estado_cxc(0,1=activo), cant_obsequios,
               saldo, edad_deuda, ultima_compra, id_resp_ult_llamada
               → NO usar ultima_llamada como contactabilidad real
ciudades       id, ciudad, id_centroope
barrios        Id (PK, mayúscula), barrio  → nombre = .barrio
categorias     id, categoria
bases          id, base
contactos_subestado    contactos_subestadoid (PK), descripcion
contactos_medios_de_contacto   id, medio_contacto
vwEventos      idEvento, id_contacto, id_autor(=promotor), fecha_evento,
               coordenada_latitud VARCHAR, coordenada_longitud VARCHAR
               → CAST a DECIMAL(10,6) | filtrar !='' !='0' IS NOT NULL

─── LLAMADAS (regla crítica) ─────────────────────────────────────────────────
llamadas       Id(PK), id_contacto, id_vendedor, fecha_inicio_llamada,
               id_respuesta(FK→llamadas_respuestas.id), estado(1=válida)
llamadas_resp  id, respuesta, es_venta(1=venta), contestada(1=aló real)

⚠️  TODA consulta de llamadas DEBE incluir INNER JOIN con llamadas_respuestas:
    INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
        ON lr.Id = l.id_respuesta
    Sin este JOIN no existe lectura confiable de contestada ni es_venta.
    Contactabilidad real: lr.contestada = 1   |   Venta: lr.es_venta = 1
    NUNCA usar contactos.ultima_llamada como indicador de contacto real.

─── PEDIDOS (5 filtros obligatorios) ────────────────────────────────────────
pedidos        id, id_contacto, fecha_pedido(DATE), fecha_hora_pedido(DATETIME),
               estado_pedido, anulada, autorizacion_descuento, autorizar,
               tipo_documento, num_factura, valor_total
⚠️  Pedido válido — aplicar SIEMPRE los 5 filtros:
    AND pe.estado_pedido = 1
    AND pe.anulada = 0
    AND pe.autorizar IN (1, 2)
    AND pe.autorizacion_descuento = 0
    AND pe.tipo_documento < 2
pedidos_det    id_pedido, id_item(FK→items.id)
               ⚠️ SIEMPRE en fullclean_telemercadeo — NUNCA en fullclean_cartera

─── RUTAS DE COBRO ───────────────────────────────────────────────────────────
Cadena obligatoria para ruta de cobro de un cliente:
    contactos.id_barrio → barrios.Id → rutas_cobro_zonas.id_barrio → rutas_cobro.id
rutas_cobro    id, ruta(nombre), id_centroope   → nombre = .ruta (NO .nombre)
rutas_cobro_zonas  id, id_ruta_cobro, id_barrio

─── CARTERA ──────────────────────────────────────────────────────────────────
facturas       id, id_contacto, fecha_factura, total, saldo_pendiente, vencida
cobros         id, id_factura, fecha_cobro, valor_cobrado

─── PRODUCTOS ────────────────────────────────────────────────────────────────
items          id, item, id_producto, id_presentacion, id_linea
lineas         id, linea  → ej: FULLIMP, BLUE PET, SAVITRI, ZAGUS
productos      id, producto
presentaciones id, presentacion
Cadena: pedidos_det.id_item → items → productos / presentaciones / lineas
Filtrar línea: INNER JOIN fullclean_bodega.lineas lin ON lin.id = it.id_linea
               WHERE lin.linea LIKE \'%BLUE PET%\'

─── PUNTOS ───────────────────────────────────────────────────────────────────
contactos_movimientos  id, id_contacto, id_concepto, fecha, valor(+acum/-reden)
conceptos              id, titulo(\'Compra\',\'Redención\',...)

─── PERSONAL ─────────────────────────────────────────────────────────────────
personal       id, apellido(nombre completo), id_cargo
cargos         Id_cargo(PK), cargo

─── MAPAS DE CLIENTES (regla crítica) ────────────────────────────────────────
El SQL para mapas DEBE devolver id_contacto + atributos de negocio.
NUNCA devolver lat/lon en el SQL: las coordenadas se anexan desde el cache.
Ejemplo de SELECT correcto para mapa:
    SELECT c.id AS id_contacto, c.nombre, b.barrio, rc.ruta, SUM(pe.valor_total) AS valor_pedidos

─── CLIENTE ACTIVO ───────────────────────────────────────────────────────────
(c.estado_cxc IN (0, 1) OR c.cant_obsequios > 0)
Canal directo (puerta a puerta): c.id_canal = 2

─── GOTCHAS SQL ──────────────────────────────────────────────────────────────
1. barrios PK = b.Id (I mayúscula)   |   personal PK = Id_cargo (mixto)
2. llamadas PK = l.Id (I mayúscula)  |   llamadas join = lr.Id = l.id_respuesta
3. PROMOTOR en vwEventos = e.id_autor (no id_promotor)
4. BARRIO nombre = b.barrio          |   RUTA COBRO nombre = rc.ruta
5. pedidos/pedidos_det → fullclean_telemercadeo (NUNCA en fullclean_cartera)
6. COORDS vwEventos: CAST a DECIMAL | filtrar !=\'\' !='0\' IS NOT NULL
7. NO-FIEL: id_categoria NOT IN (42, 55, 58, 59, 60)
8. Febrero 2026 tiene 28 días → usar \'2026-02-28\' como fecha fin

─── JOINS CRÍTICOS (referencia rápida) ──────────────────────────────────────
# CO de un cliente
JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
WHERE ci.id_centroope = [N]

# Ruta de cobro
JOIN fullclean_contactos.barrios b           ON b.Id = c.id_barrio
JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
JOIN fullclean_contactos.rutas_cobro rc      ON rc.id = rcz.id_ruta_cobro

# Llamadas contestadas (INNER JOIN obligatorio)
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = c.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.Id = l.id_respuesta AND lr.contestada = 1
WHERE l.estado = 1

# Producto por línea
JOIN fullclean_telemercadeo.pedidos_det pd ON pd.id_pedido = pe.id
JOIN fullclean_bodega.items it             ON it.id = pd.id_item
JOIN fullclean_bodega.lineas lin           ON lin.id = it.id_linea
WHERE lin.linea LIKE \'%BLUE PET%\'

─── FOLIUM (ejecutar_codigo_mapa) ────────────────────────────────────────────
TILES='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{{z}}/{{y}}/{{x}}'
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
