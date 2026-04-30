# agente/vanna_sql/trainer.py
"""
Trainer v4 — AtlasVanna (Gemini / Groq + ChromaDB).
Enfocado en consultas orientadas a clientes y mapas.

Mejoras v4 respecto a v3:
  - DDL ampliado: 8 tablas nuevas (lineas, bases, rutas_cobro, rutas_cobro_zonas,
    contactos_movimientos, conceptos, contactos_subestado, contactos_medios_de_contacto)
  - items actualizado con id_linea · llamadas con se_hablo_con y clase_llamada
  - DOC: catálogo de cargos, productos, presentaciones con ids reales
  - DOC: canal directo id_canal=2, consulta base de clientes, rutas, puntos
  - DOC: protocolo COUNT — si >50k filas avisa y no ejecuta
  - SQL: ejemplos corregidos (BluePet, puntos, rutas) · id_canal=2 en base activa
  - Test reducido a 10 preguntas estratégicas

Comandos:
    python -m agente.vanna_sql.trainer --provider groq              # entrena + valida
    python -m agente.vanna_sql.trainer --validate-only --provider groq
    python -m agente.vanna_sql.trainer --provider groq --model llama-3.3-70b-versatile

Re-entrenar desde cero:
    rmdir /s /q agente\\vanna_sql\\chroma_store
    python -m agente.vanna_sql.trainer --provider groq
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.secrets_manager import load_env_secure
load_env_secure()
from agente.vanna_sql.atlas_vanna import get_vanna


# =============================================================================
# A. DDL — Estructura de tablas (23 bloques)
# =============================================================================

DDL_BLOCKS = [

    # ── 1. fullclean_telemercadeo.llamadas ─────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas (
        Id                    INT           PRIMARY KEY,  -- nota: campo es 'Id', no 'id'
        id_vendedor           INT,          -- FK → fullclean_personal.personal.id
        id_contacto           INT,          -- FK → fullclean_contactos.contactos.id
        id_zona               INT,
        fecha_inicio_llamada  DATETIME,     -- campo principal para filtrar por fecha
        fecha_fin_llamada     DATETIME,
        duracion              INT,
        se_hablo_con          VARCHAR(100), -- con quién se habló (nombre)
        id_respuesta          INT,          -- FK → fullclean_telemercadeo.llamadas_respuestas.id
        proxima_llamada       DATETIME,
        notas                 TEXT,
        id_canal              INT,
        tipo_llamada          VARCHAR(50),
        clase_llamada         VARCHAR(50),  -- entrante / saliente / etc.
        estado                TINYINT,      -- 1 = válida; filtrar SIEMPRE por estado=1
        telefono_llamado      VARCHAR(30),
        id_call               VARCHAR(100), -- cruce con Wolkvox conn_id
        id_grupo              INT,
        modo                  VARCHAR(30)   -- predictivo / manual / entrante
    );
    """,

    # ── 2. fullclean_telemercadeo.llamadas_respuestas ──────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.llamadas_respuestas (
        id               INT  PRIMARY KEY,
        respuesta        VARCHAR(255),
        es_venta         TINYINT,    -- 1 = llamada tipificada como venta
        contestada       TINYINT     -- 1 = aló real; ÚNICA fuente válida de contactabilidad
    );
    """,

    # ── 3. fullclean_contactos.contactos ───────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.contactos (
        id                       INT PRIMARY KEY,  -- alias siempre como id_contacto
        id_ciudad                INT,              -- FK → ciudades.id
        id_barrio                INT,              -- FK → barrios.id
        id_zona                  INT,
        id_categoria             INT,              -- FK → categorias.id
        id_canal                 INT,              -- 2 = canal directo (puerta a puerta)
        id_vendedor              INT,              -- FK → personal.id (último vendedor)
        id_base                  INT,              -- FK → bases.id
        id_subestado             INT,              -- FK → contactos_subestado.contactos_subestadoid
        id_resp_ult_llamada      INT,              -- FK → llamadas_respuestas.id
        id_medio_contacto        INT,              -- FK → contactos_medios_de_contacto.id
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
        celular                  VARCHAR(30),
        celular2                 VARCHAR(30),
        notas                    TEXT,
        estado                   TINYINT,
        estado_cxc               TINYINT,          -- 0,1 = activo; IN(0,1) para clientes activos
        nit                      VARCHAR(30),
        cedula                   VARCHAR(30),
        proxima_llamada          DATETIME,
        ultima_llamada           DATETIME,         -- NO usar como indicador de contacto real
        ultima_compra            DATETIME,
        fecha_creacion           DATETIME,
        saldo                    DECIMAL(12,2),
        edad_deuda               INT,
        ultimo_abono             DATETIME,
        proximo_cobro            DATETIME,
        resta                    DECIMAL(12,2),
        saldo2                   DECIMAL(12,2),
        cant_facturas            INT,
        cant_obsequios           INT,              -- > 0 = cliente que ha recibido obsequio
        ultimo_obsequio          DATE,
        cantidad_referidos       INT,
        mascota_aplica           TINYINT,
        mascota_tipo             VARCHAR(50),
        mascota_nombre           VARCHAR(100),
        sexo                     CHAR(1),
        fecha_nacimiento         DATE,
        horario_preferido        VARCHAR(50),
        forma_pago               VARCHAR(50),
        es_empresa               TINYINT,
        direccion_entrega        VARCHAR(255),
        direccion_entrega2       VARCHAR(255),
        id_barrio_entrega        INT,
        fecha_prox_visita_venta  DATE,
        qr_predet                VARCHAR(100)
    );
    -- vwContactos es una vista sobre esta tabla con los mismos campos.
    -- Usar fullclean_contactos.contactos o fullclean_contactos.vwContactos indistintamente.
    """,

    # ── 4. fullclean_contactos.ciudades ────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.ciudades (
        id            INT  PRIMARY KEY,
        ciudad        VARCHAR(100),
        id_centroope  INT            -- FK → fullclean_general.centroope.id (CO)
    );
    """,

    # ── 5. fullclean_general.centroope ─────────────────────────────────────────
    """
    CREATE TABLE fullclean_general.centroope (
        id          INT  PRIMARY KEY,
        descripcion VARCHAR(100),  -- nombre del CO
        codigo     VARCHAR(10)    -- codigo corto
    -- IDs reales: Cali=2(CLO), Medellín=3(MDE), Bogotá=4(BOG),
    -- Pereira=5(PEI), Manizales=6(MZL), Bucaramanga=7(BGA), Barranquilla=8(BAQ)
    );
    """,

    # ── 6. fullclean_contactos.barrios ─────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.barrios (
        Id     INT  PRIMARY KEY,   -- nota: campo es 'Id', no 'id'
        barrio VARCHAR(100)
    );
    """,

    # ── 7. fullclean_contactos.categorias ──────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.categorias (
        id        INT  PRIMARY KEY,
        categoria VARCHAR(100)
    );
    """,

    # ── 8. fullclean_contactos.bases ───────────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.bases (
        id   INT  PRIMARY KEY,
        base VARCHAR(100)   -- nombre de la base / segmento de origen del cliente
    );
    """,

    # ── 9. fullclean_contactos.contactos_subestado ─────────────────────────────
    """
    CREATE TABLE fullclean_contactos.contactos_subestado (
        contactos_subestadoid  INT  PRIMARY KEY,  -- PK con nombre especial
        descripcion            VARCHAR(100)
    );
    -- Join: LEFT JOIN fullclean_contactos.contactos_subestado sub
    --           ON sub.contactos_subestadoid = c.id_subestado
    """,

    # ── 10. fullclean_contactos.contactos_medios_de_contacto ───────────────────
    """
    CREATE TABLE fullclean_contactos.contactos_medios_de_contacto (
        id             INT  PRIMARY KEY,
        medio_contacto VARCHAR(100)   -- ej: WhatsApp, Teléfono, Email, Presencial
    );
    """,

    # ── 11. fullclean_contactos.vwEventos ──────────────────────────────────────
    """
    CREATE VIEW fullclean_contactos.vwEventos (
        idEvento              INT,
        id_contacto           INT,          -- FK → contactos.id
        fecha_evento          DATETIME,
        id_evento_tipo        INT,          -- 15 = muestra PAP
        id_autor              INT,          -- FK → personal.id (promotor/gestor)
        coordenada_longitud   VARCHAR(20),  -- VARCHAR, CAST a DECIMAL; '0' o '' = inválido
        coordenada_latitud    VARCHAR(20),
        tipo_evento           VARCHAR(50)
    );
    """,

    # ── 12. fullclean_contactos.rutas_cobro ────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.rutas_cobro (
        id            INT  PRIMARY KEY,
        ruta          VARCHAR(100),   -- nombre de la ruta de cobro
        id_centroope  INT             -- FK → centroope.id
    );
    """,

    # ── 13. fullclean_contactos.rutas_cobro_zonas ──────────────────────────────
    """
    CREATE TABLE fullclean_contactos.rutas_cobro_zonas (
        id             INT  PRIMARY KEY,
        id_ruta_cobro  INT,   -- FK → rutas_cobro.id
        id_barrio      INT    -- FK → barrios.Id
    );
    -- La ruta de cobro de un cliente se obtiene por su barrio:
    -- contactos.id_barrio → barrios.Id → rutas_cobro_zonas.id_barrio → rutas_cobro.id
    """,

    # ── 14. fullclean_contactos.contactos_movimientos ──────────────────────────
    """
    CREATE TABLE fullclean_contactos.contactos_movimientos (
        id            INT  PRIMARY KEY,
        id_contacto   INT,          -- FK → contactos.id
        id_concepto   INT,          -- FK → conceptos.id (tipo de movimiento)
        fecha         DATETIME,
        valor         DECIMAL(12,2) -- positivo = acumulación, negativo = redención/pérdida
    );
    -- Usar para: puntos ganados, puntos redimidos, puntos perdidos por concepto.
    """,

    # ── 15. fullclean_contactos.conceptos ──────────────────────────────────────
    """
    CREATE TABLE fullclean_contactos.conceptos (
        id     INT  PRIMARY KEY,
        titulo VARCHAR(100)   -- nombre del concepto: 'Compra', 'Redención', 'Vencimiento', etc.
    );
    """,

    # ── 16. fullclean_personal.personal ────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.personal (
        id        INT  PRIMARY KEY,
        apellido  VARCHAR(100),   -- campo nombre completo; usar p.apellido AS nombre_completo
        id_cargo  INT             -- FK → cargos.Id_cargo
    );
    """,

    # ── 17. fullclean_personal.cargos ──────────────────────────────────────────
    """
    CREATE TABLE fullclean_personal.cargos (
        Id_cargo   INT  PRIMARY KEY,
        cargo      VARCHAR(100),
        tipo_cargo VARCHAR(50),
        estado     TINYINT,
        id_area    INT
    );
    """,

    # ── 18. fullclean_telemercadeo.pedidos ─────────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.pedidos (
        id                      INT  PRIMARY KEY,
        id_contacto             INT,      -- FK → contactos.id
        id_vendedor             INT,      -- FK → personal.id
        fecha_hora_pedido       DATETIME,
        num_factura             VARCHAR(50),
        fecha_pedido            DATE,     -- campo principal para filtrar por fecha
        estado_pedido           INT,      -- pedido válido: = 1
        anulada                 TINYINT,  -- pedido válido: = 0
        autorizacion_descuento  TINYINT,  -- pedido válido: = 0
        autorizar               TINYINT,  -- pedido válido: IN (1, 2)
        tipo_documento          INT       -- pedido válido: < 2
    );
    """,

    # ── 19. fullclean_telemercadeo.pedidos_det ─────────────────────────────────
    """
    CREATE TABLE fullclean_telemercadeo.pedidos_det (
        id         INT  PRIMARY KEY,
        id_pedido  INT,  -- FK → pedidos.id
        id_item    INT   -- FK → fullclean_bodega.items.id
    );
    """,

    # ── 20. fullclean_bodega.items ─────────────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.items (
        id              INT  PRIMARY KEY,
        item            VARCHAR(200),   -- nombre del SKU
        id_producto     INT,            -- FK → productos.id
        id_presentacion INT,            -- FK → presentaciones.id
        id_linea        INT             -- FK → lineas.id
    );
    """,

    # ── 21. fullclean_bodega.lineas ────────────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.lineas (
        id    INT  PRIMARY KEY,
        linea VARCHAR(100)   -- ej: 'FULLIMP', 'ZAGUS', 'SAVITRI', 'BLUE PET'
    );
    -- Join: INNER JOIN fullclean_bodega.lineas lin ON lin.id = it.id_linea
    -- Filtrar por línea: WHERE lin.linea LIKE '%BLUE PET%'
    """,

    # ── 22. fullclean_bodega.productos ─────────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.productos (
        id       INT  PRIMARY KEY,
        producto VARCHAR(200)
    );
    """,

    # ── 23. fullclean_bodega.presentaciones ────────────────────────────────────
    """
    CREATE TABLE fullclean_bodega.presentaciones (
        id           INT  PRIMARY KEY,
        presentacion VARCHAR(100)
    );
    """,
]


# =============================================================================
# B. DOCUMENTACIÓN — Reglas de negocio (16 bloques)
# =============================================================================

DOC_BLOCKS = [

    # ── 1. CO OBLIGATORIO ──────────────────────────────────────────────────────
    """
    REGLA CRÍTICA — Centro de Operación (CO) SIEMPRE obligatorio:
    NUNCA generar una consulta que mezcle clientes de distintos COs.
    El CO se filtra vía:
        contactos c
        INNER JOIN fullclean_contactos.ciudades ci ON ci.id = c.id_ciudad
        INNER JOIN fullclean_general.centroope ce  ON ce.id = ci.id_centroope
    Filtrar por: ci.id_centroope = [N] o ce.descripcion = '[nombre]'
    COs conocidos: Medellín = 2, Cali = 3, Bogotá = 4 (confirmar otros con SELECT).
    Si el usuario NO especifica CO, el SQL debe incluir el comentario:
    -- ADVERTENCIA: CO no especificado. Agregar filtro ci.id_centroope = ?
    """,

    # ── 2. SIEMPRE id_contacto ─────────────────────────────────────────────────
    """
    REGLA CRÍTICA — Toda consulta de clientes devuelve id_contacto:
    El campo id de contactos SIEMPRE se alias como id_contacto.
    Correcto:   SELECT c.id AS id_contacto
    Incorrecto: SELECT c.id
    Esto permite cruzar con coordenadas base para visualizar en mapa.
    """,

    # ── 3. PROTOCOLO COUNT ANTES DE EJECUTAR ───────────────────────────────────
    """
    PROTOCOLO DE VOLUMEN — COUNT antes de ejecutar:
    Cuando una consulta no tiene filtro de fecha O puede retornar muchos registros,
    primero generar la versión COUNT para estimar el volumen:

    SELECT COUNT(DISTINCT c.id) AS total_clientes
    FROM fullclean_contactos.contactos c
    ...mismos JOINs y WHERE que la consulta principal...

    Si el COUNT supera 50,000 registros, NO ejecutar la consulta completa.
    En su lugar responder:
    "Esta consulta retornaría X clientes. Para ejecutarla acota por:
     fecha, barrio, categoría, ruta o producto."

    Excepciones donde NO se necesita COUNT previo:
    - La consulta ya tiene filtro de fecha en un período corto (≤ 3 meses)
    - La consulta tiene filtro de producto o barrio específico
    """,

    # ── 4. CANAL DIRECTO ───────────────────────────────────────────────────────
    """
    REGLA — Canal directo (puerta a puerta):
    El canal de ventas directas de T Atiendo es id_canal = 2.
    Para consultas de la base activa de clientes del canal directo agregar siempre:
        AND c.id_canal = 2
    Excepciones: si el usuario pide explícitamente otro canal o todos los canales,
    omitir este filtro.
    """,

    # ── 5. CLIENTE ACTIVO ──────────────────────────────────────────────────────
    """
    REGLA — Criterio de cliente activo para mapas y base comercial:
    Un cliente es activo si cumple AL MENOS una de estas condiciones:
        c.estado_cxc IN (0, 1)    → cuenta activa
        c.cant_obsequios > 0       → ha recibido al menos un obsequio
    Filtro combinado:
        WHERE (c.estado_cxc IN (0, 1) OR c.cant_obsequios > 0)
    Para cartera: usar también c.estado = 1 si se necesita cliente no bloqueado.
    """,

    # ── 6. PEDIDO VÁLIDO ───────────────────────────────────────────────────────
    """ 
    REGLA — Filtros obligatorios para pedido válido (5 condiciones):
        AND pe.estado_pedido = 1
        AND pe.anulada = 0
        AND pe.autorizar IN (1, 2)
        AND pe.autorizacion_descuento = 0
        AND pe.tipo_documento < 2
    Para análisis de facturación, agregar:
        AND pe.num_factura IS NOT NULL
    Campo de fecha: fecha_pedido (DATE) o fecha_hora_pedido (DATETIME).
    NUNCA consultar pedidos sin los 5 filtros salvo solicitud explícita.
    """,

    # ── 7. CONTACTABILIDAD REAL ────────────────────────────────────────────────
    """
    REGLA — Contactabilidad y llamadas:
    Para aló real (contacto real):    lr.contestada = 1
    Para venta por llamada:           lr.es_venta = 1
    NUNCA usar contactos.ultima_llamada como indicador de contacto real.
    Siempre unir llamadas con llamadas_respuestas:
        LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr
            ON lr.id = l.id_respuesta
    Filtros adicionales para llamadas limpias:
        AND l.estado = 1
        AND l.id_contacto <> 0
    """,

    # ── 8. CONSULTA BASE DE CLIENTES (plantilla) ───────────────────────────────
    """
    CONSULTA BASE DE CLIENTES — plantilla recomendada:
    Partir siempre de esta estructura y agregar columnas o filtros según la pregunta.

    SELECT
        c.id AS id_contacto,
        c.nombre,
        c.direccion,
        c.estado_cxc,
        c.saldo,
        c.cant_obsequios,
        c.ultima_compra,
        ci.ciudad,
        ce.descripcion AS CO,
        b.barrio,
        cat.categoria,
        ba.base,
        p.apellido AS ultimo_vendedor,
        lr.respuesta AS respuesta_ultima_llamada,
        lr.contestada AS ultima_llamada_contestada,
        sub.descripcion AS subestado,
        mc.medio_contacto
    FROM fullclean_contactos.contactos c
    LEFT JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
    LEFT JOIN fullclean_general.centroope ce      ON ce.id = ci.id_centroope
    LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
    LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
    LEFT JOIN fullclean_personal.personal p       ON p.id = c.id_vendedor
    LEFT JOIN fullclean_contactos.bases ba        ON ba.id = c.id_base
    LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = c.id_resp_ult_llamada
    LEFT JOIN fullclean_contactos.contactos_subestado sub
        ON sub.contactos_subestadoid = c.id_subestado
    LEFT JOIN fullclean_contactos.contactos_medios_de_contacto mc
        ON mc.id = c.id_medio_contacto
    WHERE ci.id_centroope = [CO]
      AND c.id_canal = 2
      AND (c.estado_cxc IN (0, 1) OR c.cant_obsequios > 0);
    """,

    # ── 9. CADENA RUTAS DE COBRO ───────────────────────────────────────────────
    """
    REGLA — Rutas de cobro:
    La ruta de cobro de un cliente se obtiene a través del barrio, NO directamente.
    Cadena obligatoria:
        contactos.id_barrio → barrios.Id → rutas_cobro_zonas.id_barrio → rutas_cobro.id

    JOIN correcto:
    JOIN fullclean_contactos.barrios b
        ON b.Id = c.id_barrio
    JOIN fullclean_contactos.rutas_cobro_zonas rcz
        ON rcz.id_barrio = b.Id
    JOIN fullclean_contactos.rutas_cobro r
        ON r.id = rcz.id_ruta_cobro

    Filtrar por ruta específica: WHERE r.id = [N]  o  WHERE r.ruta LIKE '%nombre%'
    No asumir relación directa entre contactos y rutas_cobro.
    """,

    # ── 10. PUNTOS Y MOVIMIENTOS ────────────────────────────────────────────────
    """
    REGLA — Puntos y movimientos de clientes:
    Para análisis de puntos (ganados, redimidos, perdidos) usar:
        fullclean_contactos.contactos_movimientos cm
        JOIN fullclean_contactos.conceptos co ON co.id = cm.id_concepto

    El campo valor puede ser positivo (acumulación) o negativo (redención/pérdida).
    El campo titulo de conceptos identifica el tipo de movimiento.
    Join correcto: cm.id_concepto = co.id  |  cm.id_contacto = c.id
    """,

    # ── 11. CADENA PRODUCTO EN PEDIDOS ─────────────────────────────────────────
    """
    REGLA — Cadena producto en pedidos:
    pedidos_det.id_item → items.id → items.id_producto → productos.id
                                   → items.id_presentacion → presentaciones.id
                                   → items.id_linea → lineas.id

    Para filtrar por línea de producto:
        INNER JOIN fullclean_bodega.lineas lin ON lin.id = it.id_linea
        WHERE lin.linea LIKE '%BLUE PET%'      -- o FULLIMP, SAVITRI, ZAGUS

    Para filtrar por producto específico:  WHERE pro.id = [N]
    Para filtrar por presentación:         WHERE pre.id = [N]
    NO conectar pedidos_det directamente a productos sin pasar por items.
    """,

    # ── 12. CATÁLOGO CARGOS ────────────────────────────────────────────────────
    """
    CATÁLOGO DE CARGOS (fullclean_personal.cargos):
    id_cargo | cargo
    39   PROMOTOR(A) DE VENTAS PAP
    100  ASESOR(A) DE VENTAS
    137  EMBAJADOR DE SERVICIOS
    144  EMBAJADOR DE SERVICIOS CARRO
    145  EMBAJADOR DE SERVICIOS MOTO
    174  EMBAJADOR DE SERVICIO MOTO-CARRO
    181  CONSULTOR DE VENTAS
    164  ASESOR(A) DE VENTAS FIELES
    165  ASESOR(A) DE VENTAS NUEVOS
    166  ASESOR(A) DE VENTAS REFERIDOS
    167  ASESOR(A) DE VENTAS SALIENTES
    177  ASESOR DE VENTA TAT
    37   SUPERVISOR DE VENTAS
    134  SUPERVISOR DE VENTAS PAP
    42   COBRADOR CONTRATISTA
    29   SUBDIRECTOR VENTAS
    Filtrar promotores PAP:  WHERE p.id_cargo = 39
    Filtrar asesores:        WHERE p.id_cargo = 100
    Filtrar embajadores:     WHERE p.id_cargo IN (137, 144, 145, 174)
    """,

    # ── 13. CATÁLOGO PRODUCTOS Y PRESENTACIONES ────────────────────────────────
    """
    CATÁLOGO DE PRODUCTOS PRINCIPALES (fullclean_bodega.productos):
    id | producto
    1   AMBIENTADOR DE PISOS FULLIMP
    2   QUITAGRASA MULTIUSOS FULLIMP
    3   DESMANCHADOR DE PISOS FULLIMP
    4   JABON LIQUIDO FULLIMP
    5   LIMPIA VIDRIOS FULLIMP
    6   SUAVIZANTE DE ROPA FULLIMP
    8   BLANQUEADOR DESINFECTANTE FULLIMP
    15  DETERGENTE LIQUIDO FULLIMP
    17  LAVAPLATOS LIQUIDO FULLIMP
    58  AMBIENTADOR DE PISOS ZAGUS
    198 SHAMPOO SAVITRI
    199 TRATAMIENTO SAVITRI
    204 BRUMA HIDRATANTE BLUE PET
    205 TRATAMIENTO BLUE PET
    206 TOALLITAS HUMEDAS BLUE PET
    207 SHAMPOO BLUE PET

    CATÁLOGO DE PRESENTACIONES (fullclean_bodega.presentaciones):
    id  | presentacion
    102  1900 ML
    103  3950 ML
    104  19000 ML
    107  150 ML
    108  1000 ML REPUESTO
    110  500 ML REPUESTO
    112  1000 ML
    114  500 ML
    116  NO APLICA
    118  KIT
    131  OFERTA
    134  NORMAL
    135  REPUESTO

    Ejemplo: producto 2 (QUITAGRASA) presentación 103 (3950 ML).
    Febrero tiene 28 días: usar '2026-02-28' como fecha fin.
    """,

    # ── 14. COORDENADAS DE CLIENTES ────────────────────────────────────────────
    """
    REGLA — Coordenadas de clientes para mapas:
    Las coordenadas base por CO están en:
      static/datos/coordenadas/clientes_co_{id}_{nombre}.csv
      Columnas: id_contacto, lat, lon, n_eventos

    Para coordenadas de eventos específicos usar vwEventos con filtros:
      AND e.coordenada_latitud  NOT IN ('', '0')
      AND e.coordenada_longitud NOT IN ('', '0')
      AND CAST(e.coordenada_latitud  AS DECIMAL(10,6)) BETWEEN -4.5 AND 12.5
      AND CAST(e.coordenada_longitud AS DECIMAL(10,6)) BETWEEN -82.0 AND -66.0
    """,

    # ── 15. RELACIONES CONFIRMADAS ─────────────────────────────────────────────
    """
    RELACIONES CONFIRMADAS entre tablas:
    -- Contactos
    contactos.id_ciudad              = ciudades.id
    contactos.id_barrio              = barrios.Id          ← Id (mayúscula)
    contactos.id_categoria           = categorias.id
    contactos.id_base                = bases.id
    contactos.id_vendedor            = personal.id
    contactos.id_resp_ult_llamada    = llamadas_respuestas.id
    contactos.id_subestado           = contactos_subestado.contactos_subestadoid
    contactos.id_medio_contacto      = contactos_medios_de_contacto.id
    -- CO
    ciudades.id_centroope            = centroope.id
    -- Llamadas
    llamadas.id_contacto             = contactos.id
    llamadas.id_vendedor             = personal.id
    llamadas.id_respuesta            = llamadas_respuestas.id
    -- Pedidos
    pedidos.id_contacto              = contactos.id
    pedidos.id_vendedor              = personal.id
    pedidos_det.id_pedido            = pedidos.id
    pedidos_det.id_item              = items.id
    items.id_producto                = productos.id
    items.id_presentacion            = presentaciones.id
    items.id_linea                   = lineas.id
    -- Personal
    personal.id_cargo                = cargos.Id_cargo     ← Id_cargo (mixto)
    -- Rutas cobro
    barrios.Id                       = rutas_cobro_zonas.id_barrio
    rutas_cobro_zonas.id_ruta_cobro  = rutas_cobro.id
    -- Puntos
    contactos_movimientos.id_contacto = contactos.id
    contactos_movimientos.id_concepto = conceptos.id
    -- Eventos
    vwEventos.id_contacto            = contactos.id
    vwEventos.id_autor               = personal.id
    """,

    # ── 16. PROTOCOLO DE CLARIFICACIÓN ─────────────────────────────────────────
    """
    PROTOCOLO DE CLARIFICACIÓN — Antes de generar SQL verificar:
    1. ¿Está definido el CO? → si no, incluir comentario: -- CO no especificado
    2. ¿Está acotado por fecha? → si no y puede ser >50k, avisar
    3. ¿Se aplican filtros de negocio? → pedido válido (5 filtros), llamadas (estado=1)
    4. ¿El período solicitado existe? → febrero tiene 28 días en 2026, no 31

    Flujo estándar de respuesta:
      a. Verificar CO → falta = advertir
      b. Estimar volumen con COUNT si no hay fecha
      c. Generar SQL con todos los filtros de negocio
      d. Incluir co.id AS id_contacto como primer campo
    """,
]


# =============================================================================
# C. EJEMPLOS SQL — 20 pares pregunta/SQL
# =============================================================================

SQL_EXAMPLES = [

    # 1. Base activa completa para mapa — plantilla enriquecida
    {
        "question": "Dame la base de clientes activos del CO Medellín con categoría, barrio y subestado para mapa",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.direccion,
    c.estado_cxc,
    c.cant_obsequios,
    c.ultima_compra,
    ci.ciudad,
    ce.descripcion                  AS CO,
    b.barrio,
    cat.categoria,
    ba.base,
    p.apellido                      AS ultimo_vendedor,
    lr.respuesta                    AS respuesta_ultima_llamada,
    lr.contestada                   AS ultima_llamada_contestada,
    sub.descripcion                 AS subestado,
    mc.medio_contacto
FROM fullclean_contactos.contactos c
LEFT JOIN fullclean_contactos.ciudades ci       ON ci.id = c.id_ciudad
LEFT JOIN fullclean_general.centroope ce         ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.barrios b          ON b.Id = c.id_barrio
LEFT JOIN fullclean_contactos.categorias cat     ON cat.id = c.id_categoria
LEFT JOIN fullclean_personal.personal p          ON p.id = c.id_vendedor
LEFT JOIN fullclean_contactos.bases ba           ON ba.id = c.id_base
LEFT JOIN fullclean_telemercadeo.llamadas_respuestas lr ON lr.id = c.id_resp_ult_llamada
LEFT JOIN fullclean_contactos.contactos_subestado sub
    ON sub.contactos_subestadoid = c.id_subestado
LEFT JOIN fullclean_contactos.contactos_medios_de_contacto mc
    ON mc.id = c.id_medio_contacto
WHERE ci.id_centroope = 3
  AND c.id_canal = 2
  AND (c.estado_cxc IN (0, 1) OR c.cant_obsequios > 0)
ORDER BY c.id;
        """,
    },

    # 2. Clientes más activos por pedidos válidos
    {
        "question": "¿Cuáles son los clientes más activos por cantidad de pedidos válidos en el CO Medellín en 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    COUNT(DISTINCT pe.id)           AS total_pedidos
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos c   ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci   ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b      ON b.Id = c.id_barrio
WHERE pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 3
GROUP BY pe.id_contacto, c.nombre, cat.categoria, b.barrio, ce.descripcion
ORDER BY total_pedidos DESC;
        """,
    },

    # 3. Clientes frecuentes — múltiples pedidos en período
    {
        "question": "¿Cuáles clientes compraron con mayor frecuencia en el primer trimestre de 2026 en el CO Cali?",
        "sql": """
SELECT
    pe.id_contacto,
    c.nombre,
    cat.categoria,
    ce.descripcion                  AS CO,
    COUNT(DISTINCT pe.id)           AS total_pedidos
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos c   ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci   ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
WHERE pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-03-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 2
GROUP BY pe.id_contacto, c.nombre, cat.categoria, ce.descripcion
HAVING total_pedidos >= 2
ORDER BY total_pedidos DESC;
        """,
    },

    # 4. Contestan pero no compran — alto potencial
    {
        "question": "¿Cuáles clientes tienen varias llamadas contestadas pero cero pedidos válidos en abril 2026 CO Medellín?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = c.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta AND lr.contestada = 1
LEFT JOIN fullclean_telemercadeo.pedidos pe
    ON pe.id_contacto = c.id
    AND pe.fecha_pedido BETWEEN '2026-04-01' AND '2026-04-30'
    AND pe.estado_pedido = 1 AND pe.anulada = 0
    AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
WHERE l.estado = 1
  AND l.fecha_inicio_llamada BETWEEN '2026-04-01 00:00:00' AND '2026-04-30 23:59:59'
  AND ci.id_centroope = 3
  AND c.id_canal = 2
  AND pe.id IS NULL
GROUP BY c.id, c.nombre, cat.categoria, b.barrio, ce.descripcion
HAVING llamadas_contestadas >= 2
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 5. Clientes línea BLUE PET
    {
        "question": "¿Cuáles clientes compraron productos de la línea BLUE PET en el CO Bogotá en 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    c.nombre,
    cat.categoria,
    ce.descripcion                  AS CO,
    GROUP_CONCAT(DISTINCT pro.producto SEPARATOR '; ') AS productos_bluepet
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe    ON pe.id = pd.id_pedido
INNER JOIN fullclean_bodega.items it             ON it.id = pd.id_item
INNER JOIN fullclean_bodega.lineas lin           ON lin.id = it.id_linea
INNER JOIN fullclean_bodega.productos pro        ON pro.id = it.id_producto
INNER JOIN fullclean_contactos.contactos c       ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci       ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce        ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat     ON cat.id = c.id_categoria
WHERE lin.linea LIKE '%BLUE PET%'
  AND pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 4
GROUP BY pe.id_contacto, c.nombre, cat.categoria, ce.descripcion
ORDER BY pe.id_contacto;
        """,
    },

    # 6. Clientes por producto y presentación específica
    {
        "question": "¿Cuáles clientes compraron el producto 2 (QUITAGRASA) en presentación 103 (3950 ML) en febrero 2026 CO Medellín?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    c.nombre,
    pe.num_factura,
    pe.fecha_pedido,
    it.item,
    pro.producto,
    pre.presentacion,
    ce.descripcion                  AS CO
FROM fullclean_telemercadeo.pedidos_det pd
INNER JOIN fullclean_telemercadeo.pedidos pe     ON pe.id = pd.id_pedido
INNER JOIN fullclean_contactos.contactos c        ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci        ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce         ON ce.id = ci.id_centroope
INNER JOIN fullclean_bodega.items it              ON it.id = pd.id_item
INNER JOIN fullclean_bodega.productos pro         ON pro.id = it.id_producto
INNER JOIN fullclean_bodega.presentaciones pre    ON pre.id = it.id_presentacion
WHERE pro.id = 2 AND pre.id = 103
  AND pe.fecha_pedido BETWEEN '2026-02-01' AND '2026-02-28'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND pe.num_factura IS NOT NULL
  AND ci.id_centroope = 3
ORDER BY pe.fecha_pedido DESC;
        """,
    },

    # 7. Clientes por barrio específico
    {
        "question": "¿Cuáles clientes activos del canal directo pertenecen al barrio Poblado en el CO Medellín?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
INNER JOIN fullclean_contactos.barrios b      ON b.Id = c.id_barrio
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
WHERE b.barrio LIKE '%Poblado%'
  AND ci.id_centroope = 3
  AND c.id_canal = 2
  AND c.estado_cxc IN (0, 1)
ORDER BY c.nombre;
        """,
    },

    # 8. Clientes por categoría específica
    {
        "question": "¿Cuáles clientes pertenecen a la categoría 5 en el CO Cali canal directo?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    c.direccion,
    b.barrio,
    cat.categoria,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
INNER JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
WHERE c.id_categoria = 5
  AND ci.id_centroope = 2
  AND c.id_canal = 2
  AND c.estado_cxc IN (0, 1)
ORDER BY c.nombre;
        """,
    },

    # 9. Clientes con más de N llamadas contestadas en el mes
    {
        "question": "¿Cuáles clientes tuvieron más de 3 llamadas contestadas en abril 2026 CO Medellín?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = c.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta AND lr.contestada = 1
WHERE l.estado = 1
  AND l.fecha_inicio_llamada BETWEEN '2026-04-01 00:00:00' AND '2026-04-30 23:59:59'
  AND ci.id_centroope = 3
GROUP BY c.id, c.nombre, cat.categoria, b.barrio, ce.descripcion
HAVING llamadas_contestadas > 3
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 10. Clientes sin contacto real en los últimos 60 días
    {
        "question": "¿Cuáles clientes del CO Cali no han tenido ninguna llamada contestada entre el 28 de febrero y el 29 de abril de 2026?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
LEFT JOIN (
    SELECT DISTINCT l.id_contacto
    FROM fullclean_telemercadeo.llamadas l
    INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
        ON lr.id = l.id_respuesta AND lr.contestada = 1
    WHERE l.estado = 1
      AND l.fecha_inicio_llamada BETWEEN '2026-02-28 00:00:00' AND '2026-04-29 23:59:59'
) contactados ON contactados.id_contacto = c.id
WHERE ci.id_centroope = 2
  AND c.id_canal = 2
  AND c.estado_cxc IN (0, 1)
  AND contactados.id_contacto IS NULL
ORDER BY c.nombre;
        """,
    },

    # 11. Clientes pre-churn — compraron antes, no recientemente
    {
        "question": "¿Cuáles clientes compraron en 2025 pero no han vuelto a comprar en 2026 en el CO Medellín?",
        "sql": """
SELECT DISTINCT
    c.id                            AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    MAX(pe_ant.fecha_pedido)        AS ultima_compra_2025
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_telemercadeo.pedidos pe_ant
    ON pe_ant.id_contacto = c.id
    AND pe_ant.fecha_pedido BETWEEN '2025-01-01' AND '2025-12-31'
    AND pe_ant.estado_pedido = 1 AND pe_ant.anulada = 0
    AND pe_ant.autorizar IN (1, 2) AND pe_ant.autorizacion_descuento = 0 AND pe_ant.tipo_documento < 2
LEFT JOIN fullclean_telemercadeo.pedidos pe_rec
    ON pe_rec.id_contacto = c.id
    AND pe_rec.fecha_pedido >= '2026-01-01'
    AND pe_rec.estado_pedido = 1 AND pe_rec.anulada = 0
    AND pe_rec.autorizar IN (1, 2) AND pe_rec.autorizacion_descuento = 0 AND pe_rec.tipo_documento < 2
WHERE ci.id_centroope = 3
  AND c.id_canal = 2
  AND pe_rec.id IS NULL
GROUP BY c.id, c.nombre, cat.categoria, b.barrio, ce.descripcion
ORDER BY ultima_compra_2025 DESC;
        """,
    },

    # 12. Clientes nuevos — primera compra en período
    {
        "question": "¿Cuáles clientes compraron por primera vez en el CO Medellín en abril 2026?",
        "sql": """
SELECT
    pe.id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    MIN(pe.fecha_pedido)            AS primera_compra
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos c   ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci   ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b      ON b.Id = c.id_barrio
WHERE pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 3
GROUP BY pe.id_contacto, c.nombre, c.tel1, c.celular, cat.categoria, b.barrio, ce.descripcion
HAVING MIN(pe.fecha_pedido) BETWEEN '2026-04-01' AND '2026-04-30'
ORDER BY primera_compra;
        """,
    },

    # 13. Clientes reactivados
    {
        "question": "¿Cuáles clientes del CO Cali se reactivaron en 2026 después de más de 6 meses sin comprar?",
        "sql": """
SELECT
    pe_rec.id_contacto,
    c.nombre,
    cat.categoria,
    ce.descripcion                  AS CO,
    MAX(pe_ant.fecha_pedido)        AS ultima_compra_anterior,
    MIN(pe_rec.fecha_pedido)        AS fecha_reactivacion,
    DATEDIFF(MIN(pe_rec.fecha_pedido), MAX(pe_ant.fecha_pedido)) AS dias_sin_compra
FROM fullclean_telemercadeo.pedidos pe_rec
INNER JOIN fullclean_contactos.contactos c    ON c.id = pe_rec.id_contacto
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
INNER JOIN fullclean_telemercadeo.pedidos pe_ant
    ON pe_ant.id_contacto = pe_rec.id_contacto
    AND pe_ant.fecha_pedido < '2026-01-01'
    AND pe_ant.estado_pedido = 1 AND pe_ant.anulada = 0
    AND pe_ant.autorizar IN (1, 2) AND pe_ant.autorizacion_descuento = 0 AND pe_ant.tipo_documento < 2
WHERE pe_rec.fecha_pedido >= '2026-01-01'
  AND pe_rec.estado_pedido = 1 AND pe_rec.anulada = 0
  AND pe_rec.autorizar IN (1, 2) AND pe_rec.autorizacion_descuento = 0 AND pe_rec.tipo_documento < 2
  AND ci.id_centroope = 2
GROUP BY pe_rec.id_contacto, c.nombre, cat.categoria, ce.descripcion
HAVING dias_sin_compra > 180
ORDER BY dias_sin_compra DESC;
        """,
    },

    # 14. Clientes con factura
    {
        "question": "¿Cuáles clientes tienen pedidos válidos con factura generada en el CO Medellín en abril 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    c.nombre,
    pe.num_factura,
    pe.fecha_pedido,
    cat.categoria,
    ce.descripcion                  AS CO
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_contactos.contactos c   ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci   ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce    ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat ON cat.id = c.id_categoria
WHERE pe.num_factura IS NOT NULL AND pe.num_factura <> ''
  AND pe.fecha_pedido BETWEEN '2026-04-01' AND '2026-04-30'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 3
ORDER BY pe.fecha_pedido DESC;
        """,
    },

    # 15. Clientes con puntos acumulados
    {
        "question": "¿Cuáles clientes del CO Medellín tienen movimientos de puntos registrados en el primer trimestre de 2026?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    SUM(cm.valor)                   AS puntos_netos,
    COUNT(cm.id)                    AS total_movimientos,
    GROUP_CONCAT(DISTINCT co.titulo SEPARATOR ', ') AS conceptos
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_contactos.contactos_movimientos cm ON cm.id_contacto = c.id
INNER JOIN fullclean_contactos.conceptos co   ON co.id = cm.id_concepto
WHERE cm.fecha BETWEEN '2026-01-01' AND '2026-03-31'
  AND ci.id_centroope = 3
  AND c.id_canal = 2
GROUP BY c.id, c.nombre, cat.categoria, b.barrio, ce.descripcion
ORDER BY puntos_netos DESC;
        """,
    },

    # 16. Clientes por ruta de cobro
    {
        "question": "¿Cuáles clientes activos pertenecen a la ruta de cobro 13 en el CO Medellín?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.direccion_entrega,
    c.ultima_compra,
    c.saldo,
    c.edad_deuda,
    b.barrio,
    r.ruta                          AS nombre_ruta,
    ce.descripcion                  AS CO
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
JOIN fullclean_contactos.barrios b            ON b.Id = c.id_barrio
JOIN fullclean_contactos.rutas_cobro_zonas rcz ON rcz.id_barrio = b.Id
JOIN fullclean_contactos.rutas_cobro r         ON r.id = rcz.id_ruta_cobro
WHERE r.id = 13
  AND ci.id_centroope = 3
  AND c.estado_cxc IN (0, 1)
  AND c.estado = 1
ORDER BY b.barrio, c.nombre;
        """,
    },

    # 17. Clientes por vendedor específico
    {
        "question": "¿Cuáles clientes fueron gestionados por el vendedor 45 en pedidos válidos del CO Cali en 2026?",
        "sql": """
SELECT DISTINCT
    pe.id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    p.apellido                      AS consultor
FROM fullclean_telemercadeo.pedidos pe
INNER JOIN fullclean_personal.personal p      ON p.id = pe.id_vendedor
INNER JOIN fullclean_contactos.contactos c    ON c.id = pe.id_contacto
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
WHERE pe.id_vendedor = 45
  AND pe.fecha_pedido BETWEEN '2026-01-01' AND '2026-12-31'
  AND pe.estado_pedido = 1 AND pe.anulada = 0
  AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
  AND ci.id_centroope = 2
ORDER BY c.nombre;
        """,
    },

    # 18. Clientes con muchas llamadas y baja conversión
    {
        "question": "¿Cuáles clientes del CO Medellín tienen más de 5 llamadas contestadas pero ningún pedido válido en 2026?",
        "sql": """
SELECT
    c.id                            AS id_contacto,
    c.nombre,
    c.tel1,
    c.celular,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    COUNT(l.Id)                     AS llamadas_contestadas
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l  ON l.id_contacto = c.id
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta AND lr.contestada = 1
LEFT JOIN fullclean_telemercadeo.pedidos pe
    ON pe.id_contacto = c.id
    AND pe.fecha_pedido >= '2026-01-01'
    AND pe.estado_pedido = 1 AND pe.anulada = 0
    AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
WHERE l.estado = 1
  AND l.fecha_inicio_llamada >= '2026-01-01'
  AND ci.id_centroope = 3
  AND c.id_canal = 2
  AND pe.id IS NULL
GROUP BY c.id, c.nombre, c.tel1, c.celular, cat.categoria, b.barrio, ce.descripcion
HAVING llamadas_contestadas > 5
ORDER BY llamadas_contestadas DESC;
        """,
    },

    # 19. Clientes con llamada contestada Y pedido en el mismo mes
    {
        "question": "Clientes del CO Medellín que contestaron llamada Y tienen pedido válido entre el 1 y el 29 de abril de 2026",
        "sql": """
SELECT DISTINCT
    c.id                            AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                  AS CO,
    MIN(pe.fecha_pedido)            AS primer_pedido_mes
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
INNER JOIN fullclean_telemercadeo.llamadas l
    ON l.id_contacto = c.id AND l.estado = 1
    AND l.fecha_inicio_llamada BETWEEN '2026-04-01' AND '2026-04-29 23:59:59'
INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
    ON lr.id = l.id_respuesta AND lr.contestada = 1
INNER JOIN fullclean_telemercadeo.pedidos pe
    ON pe.id_contacto = c.id
    AND pe.fecha_pedido BETWEEN '2026-04-01' AND '2026-04-29'
    AND pe.estado_pedido = 1 AND pe.anulada = 0
    AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
WHERE ci.id_centroope = 3
  AND c.id_canal = 2
GROUP BY c.id, c.nombre, cat.categoria, b.barrio, ce.descripcion
ORDER BY c.nombre;
        """,
    },

    # 20. Ranking calidad comercial con subqueries
    {
        "question": "¿Cuáles clientes del CO Bogotá tienen mejor score combinando pedidos y llamadas contestadas en 2026?",
        "sql": """
SELECT
    c.id                                               AS id_contacto,
    c.nombre,
    cat.categoria,
    b.barrio,
    ce.descripcion                                     AS CO,
    COALESCE(ped.total_pedidos, 0)                     AS total_pedidos,
    COALESCE(lls.llamadas_contestadas, 0)              AS llamadas_contestadas,
    (COALESCE(ped.total_pedidos, 0) * 3
     + COALESCE(lls.llamadas_contestadas, 0))          AS score_comercial
FROM fullclean_contactos.contactos c
INNER JOIN fullclean_contactos.ciudades ci    ON ci.id = c.id_ciudad
INNER JOIN fullclean_general.centroope ce     ON ce.id = ci.id_centroope
LEFT JOIN fullclean_contactos.categorias cat  ON cat.id = c.id_categoria
LEFT JOIN fullclean_contactos.barrios b       ON b.Id = c.id_barrio
LEFT JOIN (
    SELECT pe.id_contacto, COUNT(DISTINCT pe.id) AS total_pedidos
    FROM fullclean_telemercadeo.pedidos pe
    WHERE pe.fecha_pedido >= '2026-01-01'
      AND pe.estado_pedido = 1 AND pe.anulada = 0
      AND pe.autorizar IN (1, 2) AND pe.autorizacion_descuento = 0 AND pe.tipo_documento < 2
    GROUP BY pe.id_contacto
) ped ON ped.id_contacto = c.id
LEFT JOIN (
    SELECT l.id_contacto, COUNT(l.Id) AS llamadas_contestadas
    FROM fullclean_telemercadeo.llamadas l
    INNER JOIN fullclean_telemercadeo.llamadas_respuestas lr
        ON lr.id = l.id_respuesta AND lr.contestada = 1
    WHERE l.estado = 1 AND l.fecha_inicio_llamada >= '2026-01-01'
    GROUP BY l.id_contacto
) lls ON lls.id_contacto = c.id
WHERE ci.id_centroope = 4
  AND c.id_canal = 2
  AND c.estado_cxc IN (0, 1)
  AND (COALESCE(ped.total_pedidos, 0) + COALESCE(lls.llamadas_contestadas, 0)) > 0
ORDER BY score_comercial DESC
LIMIT 500;
        """,
    },
]


# =============================================================================
# D. ENTRENAMIENTO
# =============================================================================


def train(vn, verbose: bool = True) -> None:
    def log(msg):
        if verbose: print(msg)

    log("\n=== Entrenando DDL ===")
    for i, ddl in enumerate(DDL_BLOCKS, 1):
        vn.train(ddl=ddl)
        log(f"  [DDL {i:02d}/{len(DDL_BLOCKS)}] OK")

    log("\n=== Entrenando Documentacion ===")
    for i, doc in enumerate(DOC_BLOCKS, 1):
        vn.train(documentation=doc)
        log(f"  [DOC {i:02d}/{len(DOC_BLOCKS)}] OK")

    log("\n=== Entrenando Ejemplos SQL ===")
    for i, ex in enumerate(SQL_EXAMPLES, 1):
        vn.train(question=ex["question"], sql=ex["sql"])
        log(f"  [SQL {i:02d}/{len(SQL_EXAMPLES)}] {ex['question'][:70]}...")

    log(f"\n Entrenamiento completo -- DDL:{len(DDL_BLOCKS)} | Docs:{len(DOC_BLOCKS)} | SQL:{len(SQL_EXAMPLES)}")


TEST_QUESTIONS = [
    "Dame la base de clientes activos del CO Medellin con categoria, barrio y telefono a 29 de abril de 2026",
    "Lista de clientes del CO Medellin que contestaron al menos una llamada entre el 1 y el 31 de marzo de 2026",
    "Clientes del CO Cali a los que se les registro venta por llamada entre el 1 y el 29 de abril de 2026",
    "Lista de clientes con pedidos validos en el CO Medellin entre el 1 y el 29 de abril de 2026 con categoria y barrio",
    "Clientes del CO Cali que compraron el producto 2 en presentacion 103 entre el 1 y el 28 de febrero de 2026",
    "Lista de clientes del CO Medellin que recibieron muestra (evento tipo 15) entre el 1 y el 29 de abril de 2026",
    "Clientes del CO Medellin que contestaron llamada Y tienen pedido valido entre el 1 y el 29 de abril de 2026",
    "Dame los clientes activos de la ruta de cobro 13 del CO Medellin con su barrio y saldo",
    "Dame todos los pedidos de la ruta 7",
    "Clientes con muestra en toda Colombia entre enero y abril de 2026",
]


def validate(vn, verbose: bool = True) -> None:
    import time
    SEP = "-" * 70
    print("\n=== VALIDACION -- SQL generado ===\n")
    for i, q in enumerate(TEST_QUESTIONS, 1):
        print(f"\n{SEP}")
        print(f"[{i:02d}] {q}")
        print(SEP)
        try:
            sql = vn.generate_sql(q)
            print(sql.strip())
        except Exception as e:
            err = str(e)
            if "rate_limit_exceeded" in err or "429" in err:
                print("     Rate limit -- esperando 30s...")
                time.sleep(30)
                try:
                    sql = vn.generate_sql(q)
                    print(sql.strip())
                except Exception as e2:
                    print(f"     ERROR (reintento): {e2}")
            else:
                print(f"     ERROR: {e}")
        if i < len(TEST_QUESTIONS):
            time.sleep(4)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate-only", action="store_true",
                        help="Solo valida, no re-entrena (no gasta tokens en training)")
    parser.add_argument("--no-validate", action="store_true",
                        help="Solo entrena, no valida (0 tokens de LLM consumidos)")
    parser.add_argument("--provider", default=None, choices=["gemini", "groq"])
    parser.add_argument("--model", default=None)
    parser.add_argument("--validate-model", default=None)
    args = parser.parse_args()

    print("Iniciando AtlasVanna v4 (sin conexion MySQL)...")
    vn_train = get_vanna(model=args.model, provider=args.provider, connect_db=False)

    if not args.validate_only:
        train(vn_train)

    if args.no_validate:
        print("\n Training completo. Validacion omitida (--no-validate).")
        print(" Corre luego: python -m agente.vanna_sql.trainer --validate-only --provider groq")
        exit(0)

    val_model = args.validate_model or args.model
    if val_model != args.model:
        print(f"Cambiando modelo para validacion: {val_model}")
        vn_val = get_vanna(model=val_model, provider=args.provider, connect_db=False)
    else:
        vn_val = vn_train

    