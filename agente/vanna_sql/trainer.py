# agente/vanna_sql/trainer.py
"""
Script de entrenamiento único para AtlasVanna.

Qué hace:
  1. Carga las credenciales cifradas (config/.env.enc).
  2. Instancia AtlasVanna (Groq + ChromaDB).
  3. Inyecta en ChromaDB tres tipos de conocimiento:
       A. DDL  → estructura real de las tablas más usadas.
       B. Docs → reglas de negocio, gotchas, relaciones confirmadas.
       C. SQL  → pares pregunta/SQL extraídos de un año de consultas reales.
  4. (Opcional) valida con un set de 10 preguntas de prueba.

Cómo correrlo:
    python -m agente.vanna_sql.trainer

Correr UNA SOLA VEZ por entorno. Si cambias DDL o reglas, borra
agente/vanna_sql/chroma_store/ y vuelve a correr.
"""

import sys
from pathlib import Path

# ── Bootstrap de credenciales ─────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.secrets_manager import load_env_secure
load_env_secure()

from agente.vanna_sql.atlas_vanna import get_vanna

# =============================================================================
# A. DDL — Estructura de tablas con campos confirmados
#    Nota: Vanna NO ejecuta este DDL. Lo almacena como texto en ChromaDB
#    para que el LLM entienda qué columnas existen en cada tabla.
# =============================================================================

DDL_BLOCKS = [

    # ── fullclean_telemercadeo.llamadas ───────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas (
        Id                    INT           PRIMARY KEY,
        id_vendedor           INT,          -- FK → fullclean_personal.personal.id
        id_contacto           INT,          -- FK → fullclean_contactos.contactos.id
        id_zona               INT,
        id_llamada            INT,
        fecha_inicio_llamada  DATETIME,     -- campo principal para filtrar por fecha
        fecha_fin_llamada     DATETIME,
        duracion              INT,          -- duración en segundos
        se_hablo_con          VARCHAR(255),
        id_respuesta          INT,          -- FK → fullclean_telemercadeo.llamadas_respuestas.id
        proxima_llamada       DATETIME,
        notas                 TEXT,
        id_canal              INT,
        tipo_llamada          VARCHAR(50),  -- revisar significado según análisis
        id_autor              INT,
        tiene_grabacion       TINYINT,
        estado                TINYINT,      -- 1 = válida; filtrar siempre por estado=1
        telefono_llamado      VARCHAR(30),
        procedencia           VARCHAR(50),
        tipo                  VARCHAR(50),
        id_call               VARCHAR(100), -- FK cruce con Wolkvox.conn_id
        clase_llamada         VARCHAR(50),
        id_grupo              INT,
        modo                  VARCHAR(30)   -- predictivo / manual / entrante
    );
    """,

    # ── fullclean_telemercadeo.llamadas_respuestas ────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas_respuestas (
        id                  INT  PRIMARY KEY,
        respuesta           VARCHAR(255),  -- descripción de la tipificación
        mostrar             TINYINT,
        activo              TINYINT,
        es_venta            TINYINT,       -- 1 = llamada tipificada como venta
        codigo              VARCHAR(50),
        oculto_vendedor     TINYINT,
        contacto_exitoso    TINYINT,
        referencia_aplica   TINYINT,
        referido_aplica     TINYINT,
        nota_aplica         TINYINT,
        tipo                VARCHAR(50),
        reprogramar_dias    INT,
        reprogramar_aplica  TINYINT,
        contestada          TINYINT        -- 1 = llamada contestada (aló real); indicador oficial de contactabilidad
    );
    """,

    # ── fullclean_contactos.contactos ─────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.contactos (
        id              INT  PRIMARY KEY,
        id_categoria    INT,          -- FK → fullclean_contactos.categorias.id
        id_barrio       INT,          -- FK → fullclean_contactos.barrios.id
        id_ciudad       INT,
        id_base         INT,
        id_grupo        INT,
        ultima_llamada  DATETIME,     -- NO usar como indicador de contacto real
        ultima_compra   DATETIME,
        fecha_creacion  DATETIME
    );
    """,

    # ── fullclean_contactos.categorias ────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.categorias (
        id        INT  PRIMARY KEY,
        categoria VARCHAR(100)
    );
    """,

    # ── fullclean_contactos.barrios ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.barrios (
        id INT PRIMARY KEY
        -- otros campos deben confirmarse con DESCRIBE antes de usarlos
    );
    """,

    # ── fullclean_personal.personal ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.personal (
        id       INT  PRIMARY KEY,
        apellido VARCHAR(100)
        -- otros campos como nombre, id_cargo, estado deben validarse con DESCRIBE
    );
    """,

    # ── fullclean_telemercadeo.pedidos ────────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.pedidos (
        id                      INT  PRIMARY KEY,
        id_contacto             INT,          -- FK → fullclean_contactos.contactos.id
        id_vendedor             INT,          -- FK → fullclean_personal.personal.id
        fecha_hora_pedido       DATETIME,
        estado_pedido           VARCHAR(50),
        anulada                 TINYINT,
        autorizacion_descuento  TINYINT,
        autorizar               TINYINT,
        tipo_documento          VARCHAR(50)
    );
    """,

    # ── fullclean_contactos.vwEventos ─────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.vwEventos (
        id_evento             INT,
        id_contacto           INT,
        id_ciudad             INT,
        coordenada_latitud    VARCHAR(20),  -- SIEMPRE usar CAST(coordenada_latitud AS DECIMAL(10,6))
        coordenada_longitud   VARCHAR(20),  -- SIEMPRE usar CAST(coordenada_longitud AS DECIMAL(10,6))
        fecha_evento          DATETIME
        -- coordenadas vacías vienen como '' o '0', no como NULL
        -- bounds válidos Colombia: lat [-4.5, 12.5], lon [-82.0, -66.0]
    );
    """,

    # ── fullclean_personal.cargos ─────────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.cargos (
        Id_cargo            INT  PRIMARY KEY,
        cargo               VARCHAR(100),
        tipo_cargo          VARCHAR(50),
        excluir_sms_ruta    TINYINT,
        estado              TINYINT,
        tiempo_consecucion  INT,
        id_area             INT
    );
    """,
]


# =============================================================================
# B. DOCUMENTACIÓN — Reglas de negocio, gotchas y relaciones confirmadas
#    Cada bloque cubre un tema analítico concreto.
# =============================================================================

DOC_BLOCKS = [

    """
    REGLA CRÍTICA — Contactabilidad real:
    NO usar contactos.ultima_llamada como indicador de último contacto real.
    Se detectó que ese campo se actualiza sin que haya habido un aló efectivo.
    Para medir contactabilidad real usar SIEMPRE:
      fullclean_telemercadeo.llamadas l
      JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta
      WHERE lr.contestada = 1
    Si se requiere análisis más fino, usar sets de id_respuesta validados manualmente.
    """,

    """
    REGLA — Filtrado de basura en llamadas:
    Registros basura habituales en fullclean_telemercadeo.llamadas:
      - id_vendedor = 1 (usuario sistema)
      - id_contacto = 0 (sin contacto asignado)
  
    Para análisis comercial aplicar siempre:
  
      AND l.id_contacto <> 0
      AND l.id_vendedor NOT IN (0, 1)
    """,

    """
    RELACIONES CONFIRMADAS entre tablas:
      llamadas.id_vendedor    = fullclean_personal.personal.id
      llamadas.id_contacto    = fullclean_contactos.contactos.id
      llamadas.id_respuesta   = llamadas_respuestas.id
      llamadas.id_call        = Wolkvox.conn_id  (puede haber duplicados en ambos lados)
      pedidos.id_vendedor     = fullclean_personal.personal.id
      pedidos.id_contacto     = fullclean_contactos.contactos.id
      contactos.id_categoria  = fullclean_contactos.categorias.id
      contactos.id_barrio     = fullclean_contactos.barrios.id
      barrios → rutas_cobro_zonas → rutas_cobro  (estructura intermedia a validar con DESCRIBE)
    """,

    
    """
    BASES DE DATOS DEL SISTEMA y su propósito:
      fullclean_contactos   → identidad comercial/geográfica del cliente (contactos, barrios, rutas, zonas, vwEventos)
      fullclean_telemercadeo → llamadas, respuestas, pedidos, campañas, muestreo
      fullclean_personal    → vendedores, asesores, promotores, cargos, áreas
      fullclean_bodega      → artículos, productos, inventario, producción, compras, precios
      fullclean_cartera     → pagos, recibos, gestión de cobro
      quejas                → inconformidades, categorías, seguimiento de servicio
      fullclean_general     → usuarios, centros de operación, canales, configuración
    """,

    """
    REGLA — Qué NO hacer al generar SQL:
    1. No inventar campos como id_ruta, id_ruta_cobro, id_zona, id_barrio en tablas no inspeccionadas.
    2. No afirmar que contactos.ultima_llamada es último contacto real.
    3. No asumir que Wolkvox.customer_id siempre coincide con id_contacto.
    4. No usar INNER JOIN por defecto en exploración; preferir LEFT JOIN para auditar pérdida de registros.
    5. No mezclar rutas, rutas_barrios, rutas_cobro y rutas_cobro_zonas sin confirmar cuál representa qué.
    6. No conectar fullclean_bodega con pedidos_det sin confirmar la llave exacta del producto/item/artículo.
    """,

    """
    EXPLORACIÓN RECOMENDADA antes de construir SQL sobre tablas no confirmadas:
      SHOW COLUMNS FROM fullclean_contactos.rutas_cobro_zonas;
      SHOW COLUMNS FROM fullclean_contactos.rutas_cobro;
      SHOW COLUMNS FROM fullclean_contactos.barrios;
      SHOW COLUMNS FROM fullclean_contactos.vwEventos;
      SHOW COLUMNS FROM fullclean_personal.personal;
      SHOW COLUMNS FROM fullclean_personal.cargos;
      SHOW COLUMNS FROM fullclean_bodega.items;
      SHOW COLUMNS FROM fullclean_telemercadeo.pedidos_det;
      SELECT * FROM nombre_base.nombre_tabla LIMIT 10;
    """,

    """
    CAMPOS DE LLAMADAS más usados en análisis comercial:
    De fullclean_telemercadeo.llamadas:
      Id, id_vendedor, id_contacto, fecha_inicio_llamada, fecha_fin_llamada,
      duracion, id_respuesta, estado, telefono_llamado, id_call, modo, tipo_llamada, id_grupo
    De fullclean_telemercadeo.llamadas_respuestas (join por id_respuesta):
      respuesta, contestada, es_venta, contacto_exitoso
    De fullclean_personal.personal (join por id_vendedor):
      apellido
    De fullclean_contactos.contactos (join por id_contacto):
      id_categoria
    De fullclean_contactos.categorias (join por id_categoria):
      categoria
    """,
]


# =============================================================================
# C. EJEMPLOS SQL — Pares pregunta / SQL validados en producción real
#    Estos son el entrenamiento más poderoso: el LLM aprende el patrón exacto.
# =============================================================================

SQL_EXAMPLES = [

    {
        "question": "Dame todas las llamadas de marzo de 2026 con vendedor, categoría del contacto y respuesta",
        "sql": """
SELECT
    l.*,
    p.apellido,
    c.id_categoria,
    cat.categoria,
    lr.respuesta,
    lr.contestada,
    lr.es_venta
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
LEFT JOIN fullclean_contactos.contactos c
    ON c.id = l.id_contacto
LEFT JOIN fullclean_contactos.categorias cat
    ON cat.id = c.id_categoria
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.fecha_inicio_llamada BETWEEN '2026-03-01 00:00:00' AND '2026-03-31 23:59:59'
ORDER BY l.fecha_inicio_llamada DESC;
        """,
    },

    {
        "question": "Solo las llamadas contestadas de marzo 2026 con vendedor y categoría",
        "sql": """
SELECT
    l.*,
    p.apellido,
    c.id_categoria,
    cat.categoria,
    lr.respuesta,
    lr.contestada
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
LEFT JOIN fullclean_contactos.contactos c
    ON c.id = l.id_contacto
LEFT JOIN fullclean_contactos.categorias cat
    ON cat.id = c.id_categoria
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.fecha_inicio_llamada BETWEEN '2026-03-01 00:00:00' AND '2026-03-31 23:59:59'
  AND l.estado = 1
  AND lr.contestada = 1
ORDER BY l.fecha_inicio_llamada DESC;
        """,
    },

    {
        "question": "Total de llamadas y ventas por vendedor en marzo 2026",
        "sql": """
SELECT
    l.id_vendedor                                          AS ID_VENDEDOR,
    p.apellido                                             AS NOMBRE_VENDEDOR,
    COUNT(l.id)                                            AS total_llamadas,
    SUM(CASE WHEN lr.es_venta = 1 THEN 1 ELSE 0 END)      AS llamadas_venta
FROM fullclean_telemercadeo.llamadas l
INNER JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
INNER JOIN fullclean_contactos.contactos c
    ON c.id = l.id_contacto
INNER JOIN fullclean_contactos.categorias cat
    ON cat.id = c.id_categoria
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.estado = 1
  AND l.fecha_inicio_llamada BETWEEN '2026-03-01 00:00:00' AND '2026-03-31 23:59:59'
  AND l.id_contacto <> 0
GROUP BY l.id_vendedor, p.apellido
ORDER BY total_llamadas DESC;
        """,
    },

    {
        "question": "Total de pedidos por vendedor en marzo 2026",
        "sql": """
SELECT
    ped.id_vendedor,
    p.apellido          AS vendedor,
    COUNT(*)            AS total_pedidos
FROM fullclean_telemercadeo.pedidos ped
LEFT JOIN fullclean_personal.personal p
    ON p.id = ped.id_vendedor
WHERE ped.fecha_hora_pedido BETWEEN '2026-03-01 00:00:00' AND '2026-03-31 23:59:59'
GROUP BY ped.id_vendedor, p.apellido
ORDER BY total_pedidos DESC;
        """,
    },

    {
        "question": "Historial de llamadas del contacto 165962 en marzo 2026",
        "sql": """
SELECT
    l.id_vendedor,
    p.apellido,
    l.fecha_inicio_llamada,
    l.id_contacto,
    l.id_call,
    lr.respuesta,
    lr.contestada
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.fecha_inicio_llamada BETWEEN '2026-03-01 00:00:00' AND '2026-03-31 23:59:59'
  AND l.id_contacto = 165962
  AND l.id_vendedor NOT IN (0, 1)
  AND l.estado = 1
ORDER BY l.fecha_inicio_llamada DESC;
        """,
    },

    {
        "question": "Llamadas de la primera semana de enero 2026 con todos los datos del contacto y categoría, solo las que tienen id_call",
        "sql": """
SELECT
    l.Id,
    l.id_vendedor,
    p.apellido,
    l.id_contacto,
    c.ultima_llamada,
    c.ultima_compra,
    c.fecha_creacion,
    c.id_categoria,
    cat.categoria,
    l.id_llamada,
    l.fecha_inicio_llamada,
    l.fecha_fin_llamada,
    l.duracion,
    l.id_respuesta,
    lr.respuesta,
    lr.contestada,
    l.notas,
    l.telefono_llamado,
    l.id_call,
    l.id_grupo,
    l.estado
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
LEFT JOIN fullclean_contactos.contactos c
    ON c.id = l.id_contacto
LEFT JOIN fullclean_contactos.categorias cat
    ON cat.id = c.id_categoria
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.fecha_inicio_llamada BETWEEN '2026-01-01 00:00:00' AND '2026-01-05 23:59:59'
  AND l.id_call IS NOT NULL
  AND l.id_call <> ''
  AND l.estado = 1;
        """,
    },

    {
        "question": "Tasa de contactabilidad real por vendedor esta semana",
        "sql": """
SELECT
    l.id_vendedor,
    p.apellido                                                AS vendedor,
    COUNT(l.Id)                                               AS total_llamadas,
    SUM(CASE WHEN lr.contestada = 1 THEN 1 ELSE 0 END)       AS contestadas,
    ROUND(
        100.0 * SUM(CASE WHEN lr.contestada = 1 THEN 1 ELSE 0 END) / COUNT(l.Id), 2
    )                                                         AS pct_contactabilidad
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_personal.personal p
    ON p.id = l.id_vendedor
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.estado = 1
  AND l.id_contacto <> 0
  AND l.id_vendedor NOT IN (0, 1)
  AND l.fecha_inicio_llamada >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY l.id_vendedor, p.apellido
ORDER BY pct_contactabilidad DESC;
        """,
    },

    {
        "question": "Clientes con GPS válido en Medellín (id_ciudad para Medellín es el centro de operación 3)",
        "sql": """
SELECT
    e.id_contacto,
    CAST(e.coordenada_latitud  AS DECIMAL(10,6)) AS lat,
    CAST(e.coordenada_longitud AS DECIMAL(10,6)) AS lon,
    COUNT(*)                                      AS n_eventos
FROM fullclean_contactos.vwEventos e
WHERE e.coordenada_latitud  NOT IN ('', '0')
  AND e.coordenada_longitud NOT IN ('', '0')
  AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN -4.5 AND 12.5
  AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN -82.0 AND -66.0
  AND e.id_ciudad = 3
GROUP BY e.id_contacto, lat, lon
ORDER BY n_eventos DESC;
        """,
    },

    {
        "question": "Llamadas de hoy por modo (predictivo, manual, entrante)",
        "sql": """
SELECT
    l.modo,
    COUNT(*)                                               AS total,
    SUM(CASE WHEN lr.contestada = 1 THEN 1 ELSE 0 END)    AS contestadas,
    SUM(CASE WHEN lr.es_venta    = 1 THEN 1 ELSE 0 END)   AS ventas
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.estado = 1
  AND DATE(l.fecha_inicio_llamada) = CURDATE()
GROUP BY l.modo
ORDER BY total DESC;
        """,
    },

    {
        "question": "Ranking de categorías con más llamadas contestadas en el último mes",
        "sql": """
SELECT
    cat.categoria,
    COUNT(l.Id)                                            AS total_llamadas,
    SUM(CASE WHEN lr.contestada = 1 THEN 1 ELSE 0 END)    AS contestadas
FROM fullclean_telemercadeo.llamadas l
LEFT JOIN fullclean_contactos.contactos c
    ON c.id = l.id_contacto
LEFT JOIN fullclean_contactos.categorias cat
    ON cat.id = c.id_categoria
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta
WHERE l.estado = 1
  AND l.id_contacto <> 0
  AND l.fecha_inicio_llamada >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY cat.categoria
ORDER BY contestadas DESC;
        """,
    },
]


# =============================================================================
# FUNCIÓN PRINCIPAL DE ENTRENAMIENTO
# =============================================================================

def train(vn, verbose: bool = True) -> None:
    """
    Inyecta DDL, documentación y ejemplos SQL en el vector store ChromaDB.
    """

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    # ── A. DDL ────────────────────────────────────────────────────────────────
    log("\n=== Entrenando DDL ===")
    for i, ddl in enumerate(DDL_BLOCKS, 1):
        vn.train(ddl=ddl)
        log(f"  [DDL {i}/{len(DDL_BLOCKS)}] OK")

    # ── B. Documentación ──────────────────────────────────────────────────────
    log("\n=== Entrenando Documentación ===")
    for i, doc in enumerate(DOC_BLOCKS, 1):
        vn.train(documentation=doc)
        log(f"  [DOC {i}/{len(DOC_BLOCKS)}] OK")

    # ── C. Ejemplos SQL ───────────────────────────────────────────────────────
    log("\n=== Entrenando Ejemplos SQL ===")
    for i, example in enumerate(SQL_EXAMPLES, 1):
        vn.train(question=example["question"], sql=example["sql"])
        log(f"  [SQL {i}/{len(SQL_EXAMPLES)}] {example['question'][:60]}...")

    log("\n✅ Entrenamiento completo.")
    log(f"   DDL: {len(DDL_BLOCKS)} bloques")
    log(f"   Docs: {len(DOC_BLOCKS)} bloques")
    log(f"   SQL: {len(SQL_EXAMPLES)} ejemplos")


# =============================================================================
# VALIDACIÓN — 10 preguntas de prueba para verificar calidad
# =============================================================================

TEST_QUESTIONS = [
    "¿Cuántas llamadas contestadas hubo ayer?",
    "Dame el top 5 de vendedores por total de llamadas del mes actual",
    "¿Cuál fue la tasa de contactabilidad de la semana pasada por vendedor?",
    "Muéstrame los pedidos de abril 2026 con nombre del vendedor",
    "Llamadas del contacto 100234 en los últimos 30 días",
    "¿Qué tipificaciones (respuestas) se usaron más esta semana?",
    "Clientes con coordenadas GPS válidas en Cali",
    "Cuántas llamadas en modo predictivo vs manual hoy",
    "¿Qué categorías de cliente tienen mayor tasa de venta este mes?",
    "Dame todas las llamadas de hoy con estado=1 y su respuesta",
]


def validate(vn, verbose: bool = True) -> None:
    """
    Genera SQL para cada pregunta de prueba e imprime el resultado.
    No ejecuta contra BD — solo muestra el SQL generado para revisión visual.
    """
    print("\n=== VALIDACIÓN — SQL generado por pregunta ===\n")
    for i, question in enumerate(TEST_QUESTIONS, 1):
        print(f"[{i:02d}] PREGUNTA: {question}")
        try:
            sql = vn.generate_sql(question)
            print(f"     SQL:\n{sql}\n")
        except Exception as e:
            print(f"     ERROR: {e}\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Trainer para AtlasVanna")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Solo corre las preguntas de prueba sin re-entrenar",
    )
    parser.add_argument(
        "--model",
        default="llama-3.3-70b-versatile",
        help="Modelo Groq a usar (default: llama-3.3-70b-versatile)",
    )
    args = parser.parse_args()

    # El trainer NO necesita conexión MySQL:
    # - train()    → escribe en ChromaDB local (solo texto)
    # - validate() → genera SQL pero no lo ejecuta contra la BD
    print("🔌 Iniciando AtlasVanna (sin conexión MySQL)...")
    vn = get_vanna(model=args.model, connect_db=False)

    if not args.validate_only:
        train(vn)

    validate(vn)
