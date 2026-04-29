# agente/vanna_sql/trainer.py
"""
Trainer v3 — AtlasVanna (Gemini / Groq + ChromaDB).
Enfocado en consultas orientadas a clientes y mapas.

Proveedor activo: se detecta automáticamente por GEMINI_API_KEY (prioridad) o GROQ_API_KEY.

Cómo correr:
    python -m agente.vanna_sql.trainer              # entrena + valida (usa default del proveedor)
    python -m agente.vanna_sql.trainer --validate-only          # solo prueba
    python -m agente.vanna_sql.trainer --model gemini-1.5-pro   # forzar modelo Gemini
    python -m agente.vanna_sql.trainer --validate-model gemini-1.5-flash  # modelo ligero en validación

Para re-entrenar desde cero:
    rmdir /s /q agente\\vanna_sql\\chroma_store
    python -m agente.vanna_sql.trainer
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.secrets_manager import load_env_secure
load_env_secure()
from agente.vanna_sql.atlas_vanna import get_vanna


# =============================================================================
# A. DDL — Estructura de tablas con campos confirmados
# =============================================================================

DDL_BLOCKS = [

    # ── fullclean_telemercadeo.llamadas ───────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas (
        Id                    INT           PRIMARY KEY,
        id_vendedor           INT,          -- FK → fullclean_personal.personal.id
        id_contacto           INT,          -- FK → fullclean_contactos.vwContactos.id
        id_zona               INT,
        fecha_inicio_llamada  DATETIME,     -- campo principal para filtrar por fecha
        fecha_fin_llamada     DATETIME,
        duracion              INT,
        id_respuesta          INT,          -- FK → fullclean_telemercadeo.llamadas_respuestas.id
        notas                 TEXT,
        id_canal              INT,
        tipo_llamada          VARCHAR(50),
        estado                TINYINT,      -- 1 = válida; filtrar SIEMPRE por estado=1
        telefono_llamado      VARCHAR(30),
        id_call               VARCHAR(100), -- cruce con Wolkvox conn_id
        id_grupo              INT,
        modo                  VARCHAR(30)   -- predictivo / manual / entrante
    );
    """,

    # ── fullclean_telemercadeo.llamadas_respuestas ────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas_respuestas (
        id               INT  PRIMARY KEY,
        respuesta        VARCHAR(255),
        es_venta         TINYINT,    -- 1 = llamada tipificada como venta
        contestada       TINYINT     -- 1 = aló real; ÚNICA fuente válida de contactabilidad
    );
    """,

    # ── fullclean_contactos.contactos (tabla base) + vwContactos (vista) ────────
    # REGLA: en consultas siempre usar co.id AS id_contacto para nombrar el campo
    """
    CREATE TABLE fullclean_contactos.contactos (
        id                       INT PRIMARY KEY,  -- SIEMPRE alias como id_contacto en SELECT
        id_ciudad                INT,              -- FK → fullclean_contactos.ciudades.id
        id_barrio                INT,              -- FK → fullclean_contactos.barrios.id
        id_zona                  INT,
        id_categoria             INT,              -- FK → fullclean_contactos.categorias.id
        id_canal                 INT,
        id_vendedor              INT,              -- FK → fullclean_personal.personal.id
        fecha_asignacion         DATE,
        nombre                   VARCHAR(255),
        primer_nombre            VARCHAR(100),
        segundo_nombre           VARCHAR(100),
        primer_apellido          VARCHAR(100),
        segundo_apellido         VARCHAR(100),
        nom_empresa              VARCHAR(255),
        direccion                VARCHAR(255),
        direccion2               VARCHAR(255),
        email                    VARCHAR(150),
        indicativo               VARCHAR(10),
        tel1                     VARCHAR(30),
        tel2                     VARCHAR(30),
        tel3                     VARCHAR(30),
        telefono_backup          VARCHAR(30),
        telefonos_repetidos      VARCHAR(255),
        dia                      TINYINT,
        mes                      TINYINT,
        ano                      SMALLINT,
        cedula                   VARCHAR(30),
        cedula_vieja             VARCHAR(30),
        ext1                     VARCHAR(10),
        ext2                     VARCHAR(10),
        celular                  VARCHAR(30),
        fax                      VARCHAR(30),
        notas                    TEXT,
        estado                   TINYINT,
        estado_cxc               TINYINT,          -- estado cuenta: filtrar IN(0,1) para clientes activos
        nit                      VARCHAR(30),
        proxima_llamada          DATETIME,
        ultima_llamada           DATETIME,         -- NO usar como indicador de contacto real
        ultima_compra            DATETIME,
        fecha_creacion           DATETIME,
        saldo                    DECIMAL(12,2),
        edad_deuda               INT,
        ultimo_abono             DATETIME,
        proximo_cobro            DATETIME,
        pagos_tmp                DECIMAL(12,2),
        resta                    DECIMAL(12,2),
        saldo2                   DECIMAL(12,2),
        cedula_actualizada       TINYINT,
        vivienda_propia          TINYINT,
        cant_facturas            INT,
        tel2_backup              VARCHAR(30),
        celular_backup           VARCHAR(30),
        tel1_backup              VARCHAR(30),
        telefonos_repetidos2     VARCHAR(255),
        nombre_backup            VARCHAR(255),
        nom_empresa_backup       VARCHAR(255),
        direccion_backup         VARCHAR(255),
        notas_backup             TEXT,
        cant_obsequios           INT,              -- cantidad de obsequios recibidos; > 0 = cliente activo
        ultimo_obsequio          DATE,             -- fecha del último obsequio; NOT NULL = cliente con obsequio
        digito_verificacion      VARCHAR(5),
        cedula_bck_dic_2009      VARCHAR(30),
        id_bodega                INT,
        fecha_ultimo_analisis    DATE,
        tiene_analisis           TINYINT,
        cant_encuestas           INT,
        telefonos_repetidos3     VARCHAR(255),
        foto                     VARCHAR(255),
        envio_defacturas         TINYINT,
        coordenadas              VARCHAR(50),
        es_empresa               TINYINT,
        cantidad_referidos       INT,
        cantidad_referencias     INT,
        codigo_ciuu              VARCHAR(20),
        id_base                  INT,
        id_subestado             INT,
        id_subestado_det         INT,
        id_resp_ult_llamada      INT,
        sobrenombre              VARCHAR(100),
        sexo                     CHAR(1),
        fecha_nacimiento         DATE,
        conyuge                  VARCHAR(100),
        horario_preferido        VARCHAR(50),
        hijos_cant               INT,
        hijos_nombres            VARCHAR(255),
        mascota_aplica           TINYINT,
        mascota_tipo             VARCHAR(50),
        mascota_nombre           VARCHAR(100),
        acontecimientos          TEXT,
        piso_casas               INT,
        preferencia_llamar       VARCHAR(50),
        id_original              INT,
        celular2                 VARCHAR(30),
        referenciaotro           VARCHAR(100),
        direccion_entrega        VARCHAR(255),
        direccion_entrega2       VARCHAR(255),
        id_barrio_entrega        INT,
        estado_despacho          TINYINT,
        estado_tramite           TINYINT,
        lavado_ropa              TINYINT,
        notificaciones           TINYINT,
        tipo_factura             TINYINT,
        id_tipoident             INT,
        forma_pago               VARCHAR(50),
        id_usuarioweb            INT,
        emailweb                 VARCHAR(150),
        tipo_notificacion        TINYINT,
        objetivo_ventas_anno     DECIMAL(12,2),
        incremento               DECIMAL(5,2),
        fecha_objetivo           DATE,
        id_hipotesis             INT,
        fecha_hipotesis          DATE,
        objetivo_ventas_pedido   DECIMAL(12,2),
        fecha_prox_visita_venta  DATE,
        tipo_cabello             VARCHAR(50),
        tipo_cuero_cabelludo     VARCHAR(50),
        id_medio_contacto        INT,
        qr_predet                VARCHAR(100)
    );
    -- vwContactos es una vista sobre esta tabla; tiene los mismos campos.
    -- Usar fullclean_contactos.vwContactos o fullclean_contactos.contactos indistintamente.
    """,

    # ── fullclean_contactos.ciudades ──────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.ciudades (
        id            INT  PRIMARY KEY,
        ciudad        VARCHAR(100),
        id_centroope  INT            -- FK → fullclean_general.centroope.id (CO)
    );
    """,

    # ── fullclean_general.centroope ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_general.centroope (
        id          INT  PRIMARY KEY,
        descripcion VARCHAR(100)   -- nombre del CO, ej: 'Medellín', 'Cali', 'Bogotá'
    );
    """,

    # ── fullclean_contactos.barrios ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.barrios (
        id     INT  PRIMARY KEY,
        barrio VARCHAR(100)
    );
    """,

    # ── fullclean_contactos.categorias ────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.categorias (
        id        INT  PRIMARY KEY,
        categoria VARCHAR(100)
    );
    """,

    # ── fullclean_contactos.vwEventos ─────────────────────────────────────────
    """
    CREATE VIEW fullclean_contactos.vwEventos (
        idEvento              INT,
        id_contacto           INT,          -- FK → vwContactos.id
        fecha_evento          DATETIME,
        id_evento_tipo        INT,          -- tipo de evento (muestra=15, apertura, etc.)
        id_autor              INT,          -- FK → fullclean_personal.personal.id (promotor/gestor)
        coordenada_longitud   VARCHAR(20),  -- VARCHAR, CAST a DECIMAL(10,6); 0=inválido
        coordenada_latitud    VARCHAR(20),  -- VARCHAR, CAST a DECIMAL(10,6); 0=inválido
        tipo_evento           VARCHAR(50)
    );
    """,

    # ── fullclean_personal.personal ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.personal (
        id        INT  PRIMARY KEY,
        apellido  VARCHAR(100),
        id_cargo  INT            -- FK → fullclean_personal.cargos.Id_cargo
    );
    """,

    # ── fullclean_personal.cargos ─────────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.cargos (
        Id_cargo   INT  PRIMARY KEY,
        cargo      VARCHAR(100),
        tipo_cargo VARCHAR(50),
        estado     TINYINT,
        id_area    INT
    );
    -- Cargos conocidos: promotor=39, gestor de confianza (confirmar id)
    """,

    # ── fullclean_telemercadeo.pedidos ────────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.pedidos (
        id                      INT  PRIMARY KEY,
        id_contacto             INT,      -- FK → vwContactos.id
        id_vendedor             INT,      -- FK → fullclean_personal.personal.id
        fecha_hora_pedido       DATETIME,
        num_factura             VARCHAR(50),
        fecha_pedido            DATE,
        estado_pedido           INT,      -- pedido válido: estado_pedido = 1
        anulada                 TINYINT,  -- pedido válido: anulada = 0
        autorizacion_descuento  TINYINT,  -- pedido válido: autorizacion_descuento = 0
        autorizar               TINYINT,  -- pedido válido: autorizar IN (1, 2)
        tipo_documento          INT       -- pedido válido: tipo_documento < 2
    );
    """,

    # ── fullclean_telemercadeo.pedidos_det ────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.pedidos_det (
        id         INT  PRIMARY KEY,
        id_pedido  INT,  -- FK → fullclean_telemercadeo.pedidos.id
        id_item    INT   -- FK → fullclean_bodega.items.id
    );
    """,

    # ── fullclean_bodega.items ────────────────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.items (
        id              INT  PRIMARY KEY,
        item            VARCHAR(200),   -- nombre del ítem/SKU
        id_producto     INT,            -- FK → fullclean_bodega.productos.id
        id_presentacion INT             -- FK → fullclean_bodega.presentaciones.id
    );
    """,

    # ── fullclean_bodega.productos ────────────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.productos (
        id       INT  PRIMARY KEY,
        producto VARCHAR(200)
    );
    """,

    # ── fullclean_bodega.presentaciones ──────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.presentaciones (
        id           INT  PRIMARY KEY,
        presentacion VARCHAR(100)
    );
    """,
]


# =============================================================================
# B. DOCUMENTACIÓN — Reglas de negocio y protocolo de respuesta
# =============================================================================

DOC_BLOCKS = [

    # ── REGLA CRÍTICA 1: CO OBLIGATORIO ──────────────────────────────────────
    """
    REGLA CRÍTICA — Centro de Operación (CO) SIEMPRE obligatorio:
    NUNCA generar una consulta que mezcle clientes de distintos COs sin agrupación.
    El CO se identifica via:
        fullclean_contactos.vwContactos co
        INNER JOIN fullclean_contactos.ciudades ciu ON ciu.id = co.id_ciudad
        INNER JOIN fullclean_general.centroope ce ON ce.id = ciu.id_centroope
    Filtrar por: ciu.id_centroope = [id_co] o ce.descripcion = '[nombre_co]'
    Si el usuario no especifica CO, preguntar antes de generar la consulta.
    """,

    # ── REGLA CRÍTICA 2: SIEMPRE LISTA DE id_contacto ────────────────────────
    """
    REGLA CRÍTICA — Siempre devolver id_contacto:
    Toda consulta de clientes DEBE incluir co.id AS id_contacto (o pe.id_contacto) como primer campo.
    El campo id en la tabla contactos/vwContactos se llama 'id' pero SIEMPRE se alias como id_contacto.
    Ejemplo correcto:   SELECT co.id AS id_contacto, ...
    Ejemplo incorrecto: SELECT co.id, ...
    Esto permite cruzar con el archivo de coordenadas base para mapear los resultados.
    Si la consulta solo hace COUNT(*), igualmente acompañar con la lista de IDs en una
    subconsulta o CTE cuando sea solicitada la visualización en mapa.
    """,

    # ── REGLA CRÍTICA 3: LÍMITE 25K ──────────────────────────────────────────
    """
    REGLA DE AUDITORÍA — Límite de resultados:
    Si una consulta puede retornar más de 25,000 clientes (por ejemplo, consultar
    todos los clientes de un CO sin filtro de fecha o producto), el agente debe:
    1. Informar que el resultado puede ser muy grande.
    2. Sugerir acotar por: rango de fechas, categoría, producto, barrio o campaña.
    Ejemplo de aviso: "Esta consulta puede devolver más de 25,000 registros.
    ¿Deseas acotar por fecha, categoría o producto?"
    """,

    # ── REGLA: PEDIDO VÁLIDO ──────────────────────────────────────────────────
    """
    REGLA — Filtros obligatorios para pedido válido:
    Para que un pedido sea considerado válido, aplicar SIEMPRE estos 5 filtros:
        AND pe.estado_pedido = 1
        AND pe.anulada = 0
        AND pe.autorizar IN (1, 2)
        AND pe.autorizacion_descuento = 0
        AND pe.tipo_documento < 2
    NUNCA consultar pedidos sin estos filtros salvo que se pida explícitamente.
    El campo de fecha válido en pedidos es fecha_pedido (DATE) o fecha_hora_pedido (DATETIME).
    """,

    # ── REGLA: CONTACTABILIDAD REAL ──────────────────────────────────────────
    """
    REGLA — Contactabilidad real en llamadas:
    Para medir si un cliente fue contactado (aló real):
      USAR:   llamadas_respuestas.contestada = 1
      NUNCA:  contactos.ultima_llamada (puede actualizarse sin aló real)
    Para identificar ventas via llamadas:
      USAR:   llamadas_respuestas.es_venta = 1
    Filtros adicionales para llamadas limpias:
      AND l.estado = 1
      AND l.id_contacto <> 0
      AND l.id_vendedor NOT IN (0, 1)
    """,

    # ── REGLA: CAMPOS CLAVE CONTACTOS ────────────────────────────────────────
    """
    REGLA — Campos clave de la tabla contactos para segmentación y filtrado:
    Campos de estado activo del cliente:
      - estado_cxc IN (0, 1)            → clientes con cuenta activa
      - cant_obsequios > 0              → clientes que han recibido obsequio
      - ultimo_obsequio IS NOT NULL     → tiene fecha de último obsequio
    Criterio combinado para base de mapas:
      WHERE co.estado_cxc IN (0, 1) OR co.cant_obsequios > 0
    Campos de nombre completo: primer_nombre, segundo_nombre, primer_apellido, segundo_apellido
    Campo nombre es el nombre compuesto. nom_empresa para clientes empresa (es_empresa=1).
    Campos de contacto: tel1, tel2, tel3, celular, celular2, email
    Campos geográficos: id_ciudad, id_barrio, id_zona, id_barrio_entrega, direccion, direccion_entrega
    Saldo y cartera: saldo, edad_deuda, ultimo_abono, proximo_cobro, cant_facturas
    """,

    # ── REGLA: COORDENADAS DE CLIENTES ───────────────────────────────────────
    """
    REGLA — Coordenadas de clientes para mapas:
    Las coordenadas de un cliente se obtienen del archivo base generado por
    utils/generar_coordenadas_clientes.py, almacenado en:
      static/datos/coordenadas/clientes_co_{id}_{nombre}.csv
    Columnas: id_contacto, lat, lon, n_eventos

    Para eventos específicos (muestras, visitas) usar vwEventos con filtros:
      AND e.coordenada_latitud  <> '0' AND e.coordenada_latitud  <> ''
      AND e.coordenada_longitud <> '0' AND e.coordenada_longitud <> ''
      AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN -4.5 AND 12.5
      AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN -82.0 AND -66.0
    """,

    # ── REGLA: PROMOTORES Y GESTORES ─────────────────────────────────────────
    """
    REGLA — Promotores y gestores en eventos:
    El autor de un evento está en vwEventos.id_autor → fullclean_personal.personal.id
    El cargo del personal está en fullclean_personal.personal.id_cargo → fullclean_personal.cargos.Id_cargo
    Cargos conocidos: promotor = id_cargo 39 (confirmar otros con DESCRIBE)
    Para filtrar por cargo en consultas de eventos:
      INNER JOIN fullclean_personal.personal per ON per.id = e.id_autor AND per.id_cargo = 39
    Para llamadas, el vendedor/asesor se une via:
      llamadas.id_vendedor = fullclean_personal.personal.id (sin filtro de cargo)
    """,

    # ── REGLA: BODEGA → PEDIDOS ───────────────────────────────────────────────
    """
    REGLA — Cadena de joins para producto en pedidos:
    pedidos_det.id_item → fullclean_bodega.items.id → items.id_producto → fullclean_bodega.productos.id
                                                    → items.id_presentacion → fullclean_bodega.presentaciones.id
    Para filtrar por producto específico: WHERE pro.id = [id]
    Para filtrar por presentación específica: WHERE pre.id = [id]
    Para listar todos los ítems de un pedido: GROUP_CONCAT(i.item SEPARATOR '; ')
    NO conectar pedidos_det directamente a productos sin pasar por items.
    """,

    # ── REGLA: ESTRUCTURA DE RESPUESTA ───────────────────────────────────────
    """
    PROTOCOLO DE RESPUESTA — Estructura estándar para consultas de clientes:
    1. Siempre incluir id_contacto como primer campo.
    2. Siempre incluir el CO (ce.descripcion AS CO o ciu.id_centroope).
    3. Incluir los campos solicitados: fecha, producto, asesor, etc.
    4. Si piden mapa o coordenadas: la lista de id_contacto se cruza con el archivo base.
    5. Si la consulta puede retornar >25k filas: avisar y sugerir acotar.
    6. Preguntar siempre el CO si no fue especificado.

    Flujo de auditoría antes de generar SQL:
      a. ¿Está definido el CO?            → si no, preguntar
      b. ¿Está acotado por fecha?         → si no y puede ser grande, advertir
      c. ¿Se aplican filtros de negocio?  → pedido válido, estado=1 en llamadas
    """,

    # ── RELACIONES CONFIRMADAS ────────────────────────────────────────────────
    """
    RELACIONES CONFIRMADAS entre tablas:
      vwContactos.id_ciudad     = ciudades.id
      ciudades.id_centroope     = centroope.id              ← la llave del CO
      vwContactos.id_categoria  = categorias.id
      vwContactos.id_barrio     = barrios.id
      llamadas.id_vendedor      = personal.id
      llamadas.id_contacto      = vwContactos.id
      llamadas.id_respuesta     = llamadas_respuestas.id
      pedidos.id_vendedor       = personal.id
      pedidos.id_contacto       = vwContactos.id
      pedidos_det.id_pedido     = pedidos.id
      pedidos_det.id_item       = items.id
      items.id_producto         = productos.id
      items.id_presentacion     = presentaciones.id
      vwEventos.id_contacto     = vwContactos.id
      vwEventos.id_autor        = personal.id
      personal.id_cargo         = cargos.Id_cargo
    """,
]


# =============================================================================
# C. EJEMPLOS SQL — 20 pares pregunta/SQL orientados a segmentación de clientes
# =============================================================================

SQL_EXAMPLES = [

    # 1. Clientes más activos — ranking por pedidos válidos en el CO
    {
        "question": "¿Cuáles son los clientes más activos por cantidad de pedidos válidos en el CO Medellín en 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    COUNT(DISTINCT pe.id)           AS total_pedidos
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos co  ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu  ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar    ON bar.id = co.id_barrio
WHERE pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 2
GROUP BY pe.id_contacto, co.nombre, cat.categoria, bar.barrio, ce.descripcion
ORDER BY total_pedidos DESC;
        """,
    },

    # 2. Clientes frecuentes — múltiples pedidos en un período
    {
        "question": "¿Cuáles clientes compraron con mayor frecuencia en los últimos 6 meses en el CO Cali?",
        "sql": """
SELECT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    ce.descripcion                  AS CO,
    COUNT(DISTINCT pe.id)           AS total_pedidos
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos co  ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu  ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = co.id_categoria
WHERE pe.fecha_pedido >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 3
GROUP BY pe.id_contacto, co.nombre, cat.categoria, ce.descripcion
HAVING total_pedidos >= 2
ORDER BY total_pedidos DESC;
        """,
    },

    # 3. Contestan pero no compran — alto potencial
    {
        "question": "¿Cuáles clientes tienen varias llamadas contestadas pero cero pedidos válidos en abril 2026 CO Medellín?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = co.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta AND lr.contestada = 1
LEFT JOIN fullclean_telemercadeo.pedidos pe
    ON pe.id_contacto = co.id
    AND pe.fecha_pedido BETWEEN '2026-04-01' AND '2026-04-30'
    AND pe.estado_pedido = 1 AND pe.anulada = 0
    AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
WHERE l.estado = 1
  AND l.fecha_inicio_llamada BETWEEN '2026-04-01 00:00:00' AND '2026-04-30 23:59:59'
  AND ciu.id_centroope = 2
  AND pe.id IS NULL
GROUP BY co.id, co.nombre, cat.categoria, bar.barrio, ce.descripcion
HAVING llamadas_contestadas >= 2
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 4. Clientes que compraron BluePet (línea de producto)
    {
        "question": "¿Cuáles clientes compraron productos BluePet en el CO Bogotá en 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    ce.descripcion                  AS CO,
    GROUP_CONCAT(DISTINCT pro.producto SEPARATOR '; ') AS productos_bluepet
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe  ON pe.id = pd.id_pedido
INNER JOIN fullclean_bodega.items it          ON it.id = pd.id_item
INNER JOIN fullclean_bodega.productos pro     ON pro.id = it.id_producto
INNER JOIN fullclean_bodega.lineas lin        ON lin.id = it.id_linea
INNER JOIN fullclean_contactos.contactos co   ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
WHERE lin.linea LIKE '%BluePet%'
  AND pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 4
GROUP BY pe.id_contacto, co.nombre, cat.categoria, ce.descripcion
ORDER BY pe.id_contacto;
        """,
    },

    # 5. Clientes por producto y presentación específica
    {
        "question": "¿Cuáles clientes compraron el producto 2 en presentación 103 en febrero 2026 CO Medellín?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    pe.num_factura,
    pe.fecha_pedido,
    it.item,
    pre.presentacion,
    ce.descripcion                  AS CO
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe    ON pd.id_pedido = pe.id
INNER JOIN fullclean_contactos.contactos co     ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu     ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce       ON ce.id = ciu.id_centroope
INNER JOIN fullclean_bodega.items it            ON it.id = pd.id_item
INNER JOIN fullclean_bodega.productos pro       ON pro.id = it.id_producto
INNER JOIN fullclean_bodega.presentaciones pre  ON pre.id = it.id_presentacion
WHERE pro.id = 2 AND pre.id = 103
  AND pe.fecha_pedido BETWEEN '2026-02-01' AND '2026-02-28'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 2
ORDER BY pe.fecha_pedido DESC;
        """,
    },

    # 6. Clientes por barrio específico
    {
        "question": "¿Cuáles clientes activos pertenecen al barrio Poblado en el CO Medellín?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
INNER JOIN fullclean_contactos.barrios bar    ON bar.id = co.id_barrio
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
WHERE bar.barrio LIKE '%Poblado%'
  AND ciu.id_centroope = 2
  AND co.estado_cxc IN (0, 1)
ORDER BY co.nombre;
        """,
    },

    # 7. Clientes por categoría específica
    {
        "question": "¿Cuáles clientes pertenecen a la categoría 5 en el CO Cali?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    co.direccion,
    bar.barrio,
    cat.categoria,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
INNER JOIN fullclean_contactos.categorias cat ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
WHERE co.id_categoria = 5
  AND ciu.id_centroope = 3
  AND co.estado_cxc IN (0, 1)
ORDER BY co.nombre;
        """,
    },

    # 8. Clientes con más de N llamadas contestadas en el mes
    {
        "question": "¿Cuáles clientes tuvieron más de 3 llamadas contestadas en abril 2026 CO Medellín?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = co.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta AND lr.contestada = 1
WHERE l.estado = 1
  AND l.fecha_inicio_llamada BETWEEN '2026-04-01 00:00:00' AND '2026-04-30 23:59:59'
  AND ciu.id_centroope = 2
GROUP BY co.id, co.nombre, cat.categoria, bar.barrio, ce.descripcion
HAVING llamadas_contestadas > 3
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 9. Clientes sin contacto real en los últimos 60 días
    {
        "question": "¿Cuáles clientes del CO Cali no han tenido ninguna llamada contestada en los últimos 60 días?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
LEFT JOIN (
    SELECT DISTINCT l.id_contacto
    FROM fullclean_telemercadeo.llamadas l
    INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta AND lr.contestada = 1
    WHERE l.estado = 1
      AND l.fecha_inicio_llamada >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
) contactados ON contactados.id_contacto = co.id
WHERE ciu.id_centroope = 3
  AND co.estado_cxc IN (0, 1)
  AND contactados.id_contacto IS NULL
ORDER BY co.nombre;
        """,
    },

    # 10. Clientes pre-churn — compraron antes, no recientemente
    {
        "question": "¿Cuáles clientes compraron en 2025 pero no han vuelto a comprar en 2026 en el CO Medellín?",
        "sql": """
SELECT DISTINCT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    MAX(pe_ant.fecha_pedido)        AS ultima_compra_2025
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
INNER JOIN fullclean_telemercadeo.pedidos pe_ant
    ON pe_ant.id_contacto = co.id
    AND pe_ant.fecha_pedido BETWEEN '2025-01-01' AND '2025-12-31'
    AND pe_ant.estado_pedido = 1 AND pe_ant.anulada = 0
    AND pe_ant.autorizar IN (1, 2) AND pe_ant.autorizacion_descuento = 0 AND pe_ant.tipo_documento < 2
LEFT JOIN fullclean_telemercadeo.pedidos pe_rec
    ON pe_rec.id_contacto = co.id
    AND pe_rec.fecha_pedido >= '2026-01-01'
    AND pe_rec.estado_pedido = 1 AND pe_rec.anulada = 0
    AND pe_rec.autorizar IN (1, 2) AND pe_rec.autorizacion_descuento = 0 AND pe_rec.tipo_documento < 2
WHERE ciu.id_centroope = 2
  AND pe_rec.id IS NULL
GROUP BY co.id, co.nombre, cat.categoria, bar.barrio, ce.descripcion
ORDER BY ultima_compra_2025 DESC;
        """,
    },

    # 11. Clientes recuperables — historial + llamada reciente + sin compra reciente
    {
        "question": "¿Cuáles clientes del CO Bogotá compraron en 2025, contestaron llamada en 2026 pero no han comprado en 2026?",
        "sql": """
SELECT DISTINCT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    ce.descripcion                  AS CO,
    MAX(pe_hist.fecha_pedido)       AS ultima_compra_historica
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
INNER JOIN fullclean_telemercadeo.pedidos pe_hist
    ON pe_hist.id_contacto = co.id
    AND pe_hist.fecha_pedido BETWEEN '2025-01-01' AND '2025-12-31'
    AND pe_hist.estado_pedido = 1 AND pe_hist.anulada = 0
    AND pe_hist.autorizar IN (1, 2) AND pe_hist.autorizacion_descuento = 0 AND pe_hist.tipo_documento < 2
INNER JOIN fullclean_telemercadeo.llamadas l
    ON l.id_contacto = co.id AND l.estado = 1
    AND l.fecha_inicio_llamada >= '2026-01-01'
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta AND lr.contestada = 1
LEFT JOIN fullclean_telemercadeo.pedidos pe_rec
    ON pe_rec.id_contacto = co.id
    AND pe_rec.fecha_pedido >= '2026-01-01'
    AND pe_rec.estado_pedido = 1 AND pe_rec.anulada = 0
    AND pe_rec.autorizar IN (1, 2) AND pe_rec.autorizacion_descuento = 0 AND pe_rec.tipo_documento < 2
WHERE ciu.id_centroope = 4
  AND pe_rec.id IS NULL
GROUP BY co.id, co.nombre, cat.categoria, ce.descripcion
ORDER BY ultima_compra_historica DESC;
        """,
    },

    # 12. Clientes nuevos — primera compra en período
    {
        "question": "¿Cuáles clientes compraron por primera vez en el CO Medellín en abril 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    MIN(pe.fecha_pedido)            AS primera_compra
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos co  ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu  ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar    ON bar.id = co.id_barrio
WHERE pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 2
GROUP BY pe.id_contacto, co.nombre, co.tel1, co.celular, cat.categoria, bar.barrio, ce.descripcion
HAVING MIN(pe.fecha_pedido) BETWEEN '2026-04-01' AND '2026-04-30'
ORDER BY primera_compra;
        """,
    },

    # 13. Clientes reactivados — brecha larga + compra reciente
    {
        "question": "¿Cuáles clientes del CO Cali se reactivaron en 2026 después de más de 6 meses sin comprar?",
        "sql": """
SELECT
    pe_rec.id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    ce.descripcion                  AS CO,
    MAX(pe_ant.fecha_pedido)        AS ultima_compra_anterior,
    MIN(pe_rec.fecha_pedido)        AS fecha_reactivacion,
    DATEDIFF(MIN(pe_rec.fecha_pedido), MAX(pe_ant.fecha_pedido)) AS dias_sin_compra
FROM fullclean_telemercadeo.pedidos pe_rec
INNER JOIN fullclean_contactos.contactos co   ON co.id = pe_rec.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
INNER JOIN fullclean_telemercadeo.pedidos pe_ant
    ON pe_ant.id_contacto = pe_rec.id_contacto
    AND pe_ant.fecha_pedido < '2026-01-01'
    AND pe_ant.estado_pedido = 1 AND pe_ant.anulada = 0
    AND pe_ant.autorizar IN (1, 2) AND pe_ant.autorizacion_descuento = 0 AND pe_ant.tipo_documento < 2
WHERE pe_rec.fecha_pedido >= '2026-01-01'
  AND pe_rec.estado_pedido = 1 AND pe_rec.anulada = 0
  AND pe_rec.autorizar IN (1, 2) AND pe_rec.autorizacion_descuento = 0 AND pe_rec.tipo_documento < 2
  AND ciu.id_centroope = 3
GROUP BY pe_rec.id_contacto, co.nombre, cat.categoria, ce.descripcion
HAVING dias_sin_compra > 180
ORDER BY dias_sin_compra DESC;
        """,
    },

    # 14. Clientes con factura generada
    {
        "question": "¿Cuáles clientes tienen pedidos válidos con factura generada en el CO Medellín en abril 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    pe.num_factura,
    pe.fecha_pedido,
    cat.categoria,
    ce.descripcion                  AS CO
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos co  ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu  ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = co.id_categoria
WHERE pe.num_factura IS NOT NULL
  AND pe.num_factura <> ''
  AND pe.fecha_pedido BETWEEN '2026-04-01' AND '2026-04-30'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 2
ORDER BY pe.fecha_pedido DESC;
        """,
    },

    # 15. Clientes que compran múltiples líneas de producto
    {
        "question": "¿Cuáles clientes compran más de una línea o familia de productos en el CO Bogotá en 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    co.nombre                               AS nombre_cliente,
    cat.categoria,
    ce.descripcion                          AS CO,
    COUNT(DISTINCT it.id_linea)             AS lineas_distintas,
    GROUP_CONCAT(DISTINCT lin.linea SEPARATOR ', ') AS lineas
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe    ON pe.id = pd.id_pedido
INNER JOIN fullclean_bodega.items it            ON it.id = pd.id_item
LEFT JOIN fullclean_bodega.lineas lin           ON lin.id = it.id_linea
INNER JOIN fullclean_contactos.contactos co     ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu     ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce       ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat    ON cat.id = co.id_categoria
WHERE pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 4
GROUP BY pe.id_contacto, co.nombre, cat.categoria, ce.descripcion
HAVING lineas_distintas > 1
ORDER BY lineas_distintas DESC;
        """,
    },

    # 16. Clientes con comportamiento repetitivo — siempre el mismo producto
    {
        "question": "¿Cuáles clientes del CO Medellín siempre compran el mismo producto en 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    cat.categoria,
    ce.descripcion                  AS CO,
    COUNT(DISTINCT pd.id_item)      AS items_distintos,
    COUNT(DISTINCT pe.id)           AS total_pedidos,
    GROUP_CONCAT(DISTINCT it.item SEPARATOR ', ') AS items_comprados
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe  ON pe.id = pd.id_pedido
INNER JOIN fullclean_bodega.items it          ON it.id = pd.id_item
INNER JOIN fullclean_contactos.contactos co   ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
WHERE pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 2
GROUP BY pe.id_contacto, co.nombre, cat.categoria, ce.descripcion
HAVING items_distintos = 1 AND total_pedidos >= 2
ORDER BY total_pedidos DESC;
        """,
    },

    # 17. Clientes gestionados por un vendedor específico
    {
        "question": "¿Cuáles clientes fueron gestionados por el vendedor 45 en pedidos válidos del CO Cali en 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    p.apellido                      AS consultor
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_personal.personal p      ON p.id = pe.id_vendedor
INNER JOIN fullclean_contactos.contactos co   ON co.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
WHERE pe.id_vendedor = 45
  AND pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ciu.id_centroope = 3
ORDER BY co.nombre;
        """,
    },

    # 18. Clientes con muchas llamadas y baja conversión
    {
        "question": "¿Cuáles clientes del CO Medellín tienen más de 5 llamadas contestadas pero ningún pedido válido en 2026?",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    co.tel1,
    co.celular,
    cat.categoria,
    bar.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = co.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta AND lr.contestada = 1
LEFT JOIN fullclean_telemercadeo.pedidos pe
    ON pe.id_contacto = co.id
    AND pe.fecha_pedido >= '2026-01-01'
    AND pe.estado_pedido = 1 AND pe.anulada = 0
    AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
WHERE l.estado = 1
  AND l.fecha_inicio_llamada >= '2026-01-01'
  AND ciu.id_centroope = 2
  AND pe.id IS NULL
GROUP BY co.id, co.nombre, co.tel1, co.celular, cat.categoria, bar.barrio, ce.descripcion
HAVING llamadas_contestadas > 5
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 19. Ranking combinado calidad comercial
    {
        "question": "¿Cuáles clientes del CO Bogotá tienen mejor calidad comercial combinando pedidos, llamadas contestadas y categoría en 2026?",
        "sql": """
SELECT
    co.id                                               AS id_contacto,
    co.nombre                                           AS nombre_cliente,
    cat.categoria,
    bar.barrio,
    ce.descripcion                                      AS CO,
    COALESCE(ped.total_pedidos, 0)                      AS total_pedidos,
    COALESCE(lls.llamadas_contestadas, 0)               AS llamadas_contestadas,
    (COALESCE(ped.total_pedidos, 0) * 3
     + COALESCE(lls.llamadas_contestadas, 0))           AS score_comercial
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
LEFT JOIN (
    SELECT pe.id_contacto, COUNT(DISTINCT pe.id) AS total_pedidos
    FROM fullclean_telemercadeo.pedidos pe
    WHERE pe.fecha_pedido >= '2026-01-01'
      AND pe.estado_pedido = 1 AND pe.anulada = 0
      AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
    GROUP BY pe.id_contacto
) ped ON ped.id_contacto = co.id
LEFT JOIN (
    SELECT l.id_contacto, COUNT(l.Id) AS llamadas_contestadas
    FROM fullclean_telemercadeo.llamadas l
    INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = l.id_respuesta AND lr.contestada = 1
    WHERE l.estado = 1 AND l.fecha_inicio_llamada >= '2026-01-01'
    GROUP BY l.id_contacto
) lls ON lls.id_contacto = co.id
WHERE ciu.id_centroope = 4
  AND co.estado_cxc IN (0, 1)
  AND (COALESCE(ped.total_pedidos, 0) + COALESCE(lls.llamadas_contestadas, 0)) > 0
ORDER BY score_comercial DESC
LIMIT 500;
        """,
    },

    # 20. Clientes activos para base de mapa por CO
    {
        "question": "Dame la lista de clientes activos del CO Medellín con categoría y barrio para mapa",
        "sql": """
SELECT
    co.id                           AS id_contacto,
    co.nombre                       AS nombre_cliente,
    co.direccion,
    co.cant_obsequios,
    cat.categoria,
    bar.barrio,
    ciu.ciudad,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos co
INNER JOIN fullclean_contactos.ciudades ciu   ON ciu.id = co.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ciu.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = co.id_categoria
LEFT JOIN fullclean_contactos.barrios bar     ON bar.id = co.id_barrio
WHERE ciu.id_centroope = 2
  AND (co.estado_cxc IN (0, 1) OR co.cant_obsequios > 0)
ORDER BY co.id;
        """,
    },
]


# =============================================================================
# FUNCIÓN DE ENTRENAMIENTO
# =============================================================================

def train(vn, verbose: bool = True) -> None:
    def log(msg):
        if verbose: print(msg)

    log("\n=== Entrenando DDL ===")
    for i, ddl in enumerate(DDL_BLOCKS, 1):
        vn.train(ddl=ddl)
        log(f"  [DDL {i}/{len(DDL_BLOCKS)}] OK")

    log("\n=== Entrenando Documentación ===")
    for i, doc in enumerate(DOC_BLOCKS, 1):
        vn.train(documentation=doc)
        log(f"  [DOC {i}/{len(DOC_BLOCKS)}] OK")

    log("\n=== Entrenando Ejemplos SQL ===")
    for i, ex in enumerate(SQL_EXAMPLES, 1):
        vn.train(question=ex["question"], sql=ex["sql"])
        log(f"  [SQL {i}/{len(SQL_EXAMPLES)}] {ex['question'][:65]}...")

    log(f"\n✅ Entrenamiento completo — DDL:{len(DDL_BLOCKS)} | Docs:{len(DOC_BLOCKS)} | SQL:{len(SQL_EXAMPLES)}")


# =============================================================================
# PREGUNTAS DE VALIDACIÓN — 20 orientadas a clientes y mapas
# =============================================================================

TEST_QUESTIONS = [
    # Bloque 1: Base activa — lista plana de clientes
    "Dame la lista de clientes activos del CO Medellín con categoría, barrio y teléfono a corte del 29 de abril de 2026",
    "Lista de clientes del CO Bogotá creados entre el 1 y el 29 de abril de 2026 con nombre, teléfono y dirección",
    "Clientes activos del CO Cali con saldo pendiente mayor a cero (edad_deuda > 0) al 29 de abril de 2026",

    # Bloque 2: Contactabilidad real vía llamadas
    "Lista de clientes del CO Medellín que contestaron al menos una llamada entre el 1 y el 31 de marzo de 2026",
    "Clientes del CO Cali a los que se les registró venta por llamada entre el 1 y el 29 de abril de 2026",
    "Clientes del CO Medellín con más de 3 llamadas contestadas entre el 30 de marzo y el 29 de abril de 2026",

    # Bloque 3: Pedidos válidos
    "Lista de clientes con pedidos válidos en el CO Medellín entre el 1 y el 29 de abril de 2026 con categoría y barrio",
    "Clientes del CO Cali que compraron el producto 2 en presentación 103 entre el 1 y el 28 de febrero de 2026",
    "Clientes del CO Bogotá que realizaron más de 2 pedidos válidos entre el 1 y el 31 de marzo de 2026",

    # Bloque 4: Eventos y muestras
    "Lista de clientes del CO Medellín que recibieron muestra (evento tipo 15) entre el 1 y el 29 de abril de 2026",
    "Clientes del CO Bogotá que recibieron muestra entre el 1 y el 28 de febrero de 2026 pero no compraron entre el 1 de febrero y el 31 de marzo de 2026",
    "Clientes del CO Cali con evento tipo 15 entre el 1 y el 31 de marzo de 2026 con su barrio y nombre del promotor",

    # Bloque 5: Segmentos para mapa
    "Base de clientes activos del CO Medellín con estado_cxc IN (0,1) o al menos un obsequio, a 29 de abril de 2026, para mapa",
    "Clientes del CO Bogotá con al menos un pedido válido entre el 1 de enero y el 29 de abril de 2026 para mapear su ubicación",
    "Clientes del CO Cali con cant_obsequios mayor a cero al 29 de abril de 2026, con nombre, dirección y barrio",

    # Bloque 6: Cruces avanzados
    "Clientes del CO Medellín que contestaron llamada Y tienen pedido válido entre el 1 y el 29 de abril de 2026",
    "Clientes del CO Cali que recibieron muestra entre el 1 de enero y el 29 de abril de 2026 pero no registran ningún pedido válido en ese mismo período",
    "Lista de clientes de categoría 1 del CO Bogotá que compraron entre el 1 de enero y el 29 de abril de 2026 con total de pedidos",

    # Bloque 7: Protocolo — deben generar advertencia o solicitar CO
    "Dame todos los pedidos de la ruta 7",
    "Clientes con muestra en toda Colombia entre enero y abril de 2026",
]


def validate(vn, verbose: bool = True) -> None:
    """
    Corre las preguntas de prueba con pausa entre llamadas para no exceder
    el rate limit del proveedor (Gemini: 1M tokens/día — Groq: 100k tokens/día).
    Pausa de 4 segundos entre preguntas → ~80 seg total para 20 preguntas.
    """
    import time

    print("\n=== VALIDACIÓN — SQL generado ===\n")
    for i, q in enumerate(TEST_QUESTIONS, 1):
        print(f"[{i:02d}] {q}")
        try:
            sql = vn.generate_sql(q)
            print(f"     → {sql.strip()[:300]}\n")
        except Exception as e:
            err = str(e)
            if "rate_limit_exceeded" in err or "429" in err:
                print(f"     ⏳ Rate limit — esperando 30s...\n")
                time.sleep(30)
                try:
                    sql = vn.generate_sql(q)
                    print(f"     → {sql.strip()[:300]}\n")
                except Exception as e2:
                    print(f"     ERROR (reintento): {e2}\n")
            else:
                print(f"     ERROR: {e}\n")

        if i < len(TEST_QUESTIONS):
            time.sleep(4)  # pausa entre preguntas


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-only", action="store_true",
                        help="Solo corre las preguntas de prueba sin re-entrenar")
    parser.add_argument("--provider", default=None, choices=["gemini", "groq"],
                        help="Proveedor LLM a usar. Si no se indica, detecta automáticamente "
                             "por GEMINI_API_KEY (prioridad) o GROQ_API_KEY.")
    parser.add_argument("--model", default=None,
                        help="Modelo a usar. Si no se indica, usa el default del proveedor "
                             "(Gemini: gemini-2.0-flash | Groq: llama-3.3-70b-versatile).")
    parser.add_argument("--validate-model", default=None,
                        help="Modelo para validación. Si no se indica, usa --model.")
    args = parser.parse_args()

    print("🔌 Iniciando AtlasVanna (sin conexión MySQL)...")
    vn_train = get_vanna(model=args.model, provider=args.provider, connect_db=False)

    if not args.validate_only:
        train(vn_train)

    # Para validación puede usarse un modelo o proveedor distinto
    val_model = args.validate_model or args.model
    if val_model != args.model:
        print(f"\n🔄 Cambiando modelo para validación: {val_model}")
        vn_val = get_vanna(model=val_model, provider=args.provider, connect_db=False)
    else:
        vn_val = vn_train

    validate(vn_val)
