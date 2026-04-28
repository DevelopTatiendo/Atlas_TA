"""Atlas Agent — Agente conversacional para análisis de operaciones de campo.

Usa la API de Claude con tool_use para responder preguntas en lenguaje natural
sobre las métricas de promotores, mapas de cobertura y datos de clientes.

Incluye:
  - schema_context inyectado en system prompt (esquema fijo + ejemplos few-shot)
  - ejecutar_codigo_mapa para generación dinámica de mapas Folium
  - Banco de aprendizaje: buenas.jsonl / errores.jsonl

Uso:
    from agente.atlas_agent import AtlasAgent
    agent = AtlasAgent()
    respuesta = agent.preguntar("¿Cómo está Cali esta semana?")
    print(respuesta)

Variables de entorno requeridas:
    ANTHROPIC_API_KEY   — clave de la API de Anthropic
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ── Definición de herramientas para la API ────────────────────────────────────

from agente.mapa_ejecutor import TOOL_DEFINICION as _MAPA_EJECUTOR_TOOL

TOOLS_DEFINICION = [
    {
        "name": "consultar_metricas",
        "description": (
            "Calcula las métricas de operación (% contactabilidad, % captación, "
            "% conversión, clientes visitados, etc.) para una ciudad y período. "
            "Úsala cuando el usuario pregunte por el desempeño de promotores o de una ciudad."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {
                    "type": "string",
                    "description": "Nombre de la ciudad: Cali, Medellín, Bogotá, Pereira, Manizales, Bucaramanga o Barranquilla.",
                },
                "fecha_inicio": {
                    "type": "string",
                    "description": "Fecha de inicio en formato YYYY-MM-DD. Si no se especifica, usar el primer día del mes actual.",
                },
                "fecha_fin": {
                    "type": "string",
                    "description": "Fecha de fin en formato YYYY-MM-DD. Si no se especifica, usar hoy.",
                },
                "promotor_id": {
                    "type": "integer",
                    "description": "ID numérico del promotor para filtrar (opcional).",
                },
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "generar_mapa",
        "description": (
            "Genera el mapa HTML interactivo de muestras para una ciudad y período. "
            "Devuelve la ruta al archivo HTML. Úsala cuando el usuario pida ver el mapa "
            "o quiera compartirlo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "string", "description": "Nombre de la ciudad."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
                "agrupacion": {
                    "type": "string",
                    "enum": ["Promotor", "Mes"],
                    "description": "Cómo agrupar los puntos en el mapa.",
                },
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "capturar_mapa",
        "description": (
            "Toma un screenshot PNG del mapa HTML generado. Úsala después de generar_mapa "
            "cuando el usuario quiera una imagen para incluir en un reporte."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "html_path": {
                    "type": "string",
                    "description": "Ruta al archivo HTML del mapa (resultado de generar_mapa).",
                },
            },
            "required": ["html_path"],
        },
    },
    {
        "name": "comparar_ciudades",
        "description": (
            "Obtiene métricas resumidas de las 7 ciudades para un período. "
            "Úsala cuando el usuario quiera comparar el desempeño nacional o saber qué ciudad va mejor."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": [],
        },
    },
    {
        "name": "consultar_cliente",
        "description": (
            "Busca el historial de un cliente específico: muestras recibidas, "
            "llamadas post-muestra y si hubo venta. Úsala cuando el usuario pregunte "
            "por un cliente puntual por nombre o ID."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id_contacto": {
                    "type": "integer",
                    "description": "ID numérico único del contacto.",
                },
                "nombre": {
                    "type": "string",
                    "description": "Nombre parcial del cliente para búsqueda.",
                },
                "ciudad": {
                    "type": "string",
                    "description": "Filtrar por ciudad (opcional).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "listar_promotores_activos",
        "description": (
            "Lista los promotores activos en una ciudad durante un período. "
            "Úsala para saber quiénes están operando o para obtener IDs antes de consultar métricas individuales."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "string", "description": "Nombre de la ciudad."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "listar_rutas_ciudad",
        "description": (
            "Lista rutas de cobro activas de una ciudad con métricas de actividad: "
            "n_clientes (universo), visitados_periodo, pct_cobertura, con_pedido_periodo, "
            "ultima_visita y sin_visitar (oportunidad directa). "
            "Punto de entrada obligatorio para análisis por ruta. "
            "Úsala para identificar qué rutas tienen baja cobertura o alta oportunidad."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {
                    "type": "integer",
                    "description": "id_centroope: Cali=2, Medellín=3, Bogotá=4, Pereira=5, Manizales=6, Bucaramanga=7, Barranquilla=8.",
                },
                "fecha_inicio": {
                    "type": "string",
                    "description": "Desde cuándo medir (YYYY-MM-DD, default 2026-01-01).",
                },
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "consultar_ruta_completa",
        "description": (
            "Análisis profundo de una ruta: universo de clientes, cobertura de visitas, "
            "conversión a pedidos, quejas activas, productos más vendidos en la zona, "
            "oportunidades no-fieles sin visitar con coordenadas. "
            "Herramienta central para entender qué pasa en una ruta específica."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "integer", "description": "id_centroope de la ciudad."},
                "id_ruta": {"type": "integer", "description": "ID de la ruta."},
                "nombre_ruta": {"type": "string", "description": "Nombre parcial de la ruta."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "analizar_zona_promotor",
        "description": (
            "Análisis del territorio real de un promotor: área recorrida, distancia, "
            "clientes visitados vs sin visitar en la zona, productos más pedidos, "
            "quejas activas. Úsala para ver si un promotor está optimizando su territorio."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id_promotor": {"type": "integer", "description": "ID del promotor."},
                "ciudad": {"type": "integer", "description": "id_centroope de la ciudad."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": ["id_promotor", "ciudad", "fecha_inicio"],
        },
    },
    {
        "name": "explorar_tabla",
        "description": (
            "Exploración autónoma de la BD: listar tablas, ver columnas, contar registros, "
            "explorar dominios, o ejecutar un SELECT personalizado. "
            "Usar cuando necesites entender la estructura antes de construir una consulta. "
            "NUNCA ejecuta escrituras — solo SELECT y SHOW."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "accion": {
                    "type": "string",
                    "enum": ["listar_tablas", "describir_tabla", "muestra", "contar", "explorar_columna", "select"],
                    "description": "Qué operación ejecutar.",
                },
                "tabla": {"type": "string", "description": "Nombre de la tabla."},
                "schema": {"type": "string", "description": "Base de datos (default: fullclean_contactos)."},
                "columna": {"type": "string", "description": "Columna para explorar_columna."},
                "sql": {"type": "string", "description": "Query SELECT para accion=select."},
                "filtro": {"type": "string", "description": "Cláusula WHERE (solo condición, sin WHERE)."},
                "limite": {"type": "integer", "description": "Límite de filas."},
            },
            "required": ["accion"],
        },
    },
    {
        "name": "actualizar_cache_coordenadas",
        "description": (
            "Actualiza el cache local de coordenadas de clientes. "
            "Llamar antes de analizar_zona_promotor o cuando se quiera enriquecer el mapa simulado. "
            "El cache acumula coordenadas — mejora con el tiempo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "integer", "description": "id_centroope (default: 3 = Medellín)."},
                "fecha_inicio": {"type": "string", "description": "Fecha desde (YYYY-MM-DD)."},
                "fecha_fin": {"type": "string", "description": "Fecha hasta (YYYY-MM-DD, default: hoy)."},
            },
            "required": [],
        },
    },
    # ── Consulta previa OBLIGATORIA antes de generar mapa ────────────────────
    {
        "name": "consultar_clientes",
        "description": (
            "PASO 1 OBLIGATORIO antes de generar cualquier mapa. "
            "Ejecuta el SQL de filtro, cuenta cuántos clientes cumplen la condición, "
            "cuántos tienen coordenadas en el cache, y calcula KPIs automáticos "
            "según las columnas del resultado (monetarias, flags, fechas, conteos). "
            "Muestra el resumen al usuario y ESPERA su confirmación antes de generar el mapa. "
            "NUNCA llames generar_mapa_clientes sin haber llamado consultar_clientes primero."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql_clientes": {
                    "type": "string",
                    "description": (
                        "SELECT que devuelve id_contacto + atributos a visualizar. "
                        "DEBE incluir id_contacto. NO incluir lat/lon."
                    ),
                },
                "ciudad": {
                    "type": "integer",
                    "description": "id_centroope: Cali=2, Medellín=3, Bogotá=4, Pereira=5, Manizales=6, Bucaramanga=7, Barranquilla=8.",
                },
                "schema_sql": {
                    "type": "string",
                    "description": "Schema de BD (default: fullclean_contactos).",
                },
                "tipo_mapa_sugerido": {
                    "type": "string",
                    "enum": ["puntos_bicolor", "circulos_proporcionales", "heatmap", "clusters"],
                    "description": "Tipo de mapa que usarás cuando el usuario confirme.",
                },
                "campo_valor": {
                    "type": "string",
                    "description": "Columna numérica para circulos_proporcionales o peso heatmap.",
                },
                "campo_color": {
                    "type": "string",
                    "description": "Columna 0/1 para puntos_bicolor.",
                },
                "titulo": {
                    "type": "string",
                    "description": "Título descriptivo del mapa.",
                },
            },
            "required": ["sql_clientes", "ciudad"],
        },
    },
    # ── Mapa estándar (SQL → cache coords → renderer) ────────────────────────
    {
        "name": "generar_mapa_clientes",
        "description": (
            "Genera un mapa a partir de un SQL que filtra clientes y las coordenadas del cache local. "
            "FLUJO: ejecuta sql_clientes → obtiene id_contacto + atributos → "
            "hace merge con cache de coords (lat/lon centroide del cliente) → pinta el mapa. "
            "NO requiere que el SQL traiga lat/lon — las coordenadas vienen del cache. "
            "El SQL DEBE incluir id_contacto como columna. "
            "Usar para todos los mapas de negocio: cobertura, deuda, actividad, clientes activos."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql_clientes": {
                    "type": "string",
                    "description": (
                        "SELECT que devuelve id_contacto + atributos a visualizar. "
                        "Ejemplo: SELECT c.id AS id_contacto, c.nombre, SUM(f.saldo_pendiente) AS deuda_total "
                        "FROM ... WHERE ... GROUP BY c.id, c.nombre. "
                        "NO incluir lat/lon en el SQL — vienen del cache."
                    ),
                },
                "ciudad": {
                    "type": "integer",
                    "description": "id_centroope para cargar el cache correcto. Cali=2, Medellín=3, Bogotá=4, Pereira=5, Manizales=6, Bucaramanga=7, Barranquilla=8.",
                },
                "tipo": {
                    "type": "string",
                    "enum": ["puntos_bicolor", "circulos_proporcionales", "heatmap", "clusters"],
                    "description": (
                        "Tipo de visualización: "
                        "puntos_bicolor=verde/rojo (requiere campo_color 0/1), "
                        "circulos_proporcionales=tamaño∝valor (requiere campo_valor numérico), "
                        "heatmap=densidad de puntos, "
                        "clusters=agrupación automática con popup."
                    ),
                },
                "campo_valor": {
                    "type": "string",
                    "description": "Nombre de columna numérica del SQL para circulos_proporcionales o peso en heatmap.",
                },
                "campo_color": {
                    "type": "string",
                    "description": "Nombre de columna 0/1 del SQL para puntos_bicolor (ej: 'visitado').",
                },
                "titulo": {
                    "type": "string",
                    "description": "Título visible en el mapa y nombre del archivo HTML.",
                },
                "schema_sql": {
                    "type": "string",
                    "description": "Schema de la BD para el SQL (default: fullclean_contactos).",
                },
            },
            "required": ["sql_clientes", "ciudad", "tipo"],
        },
    },
    # Herramienta de código dinámico — importada desde mapa_ejecutor.py
    _MAPA_EJECUTOR_TOOL,
]


# ── System Prompt con schema context ─────────────────────────────────────────

def _build_system_prompt() -> str:
    """Construye el system prompt inyectando el schema context y ejemplos."""
    from agente.schema_context import get_full_context
    schema_ctx = get_full_context()

    return f"""Eres Atlas Agent, el asistente de inteligencia operacional de T Atiendo S.A.

Tu misión: analizar la operación de campo de promotores que entregan muestras físicas en 7 ciudades de Colombia, cruzando datos de visitas, pedidos, llamadas, quejas y territorios para detectar oportunidades y alertas que los humanos no ven.

CIUDADES (id_centroope): Cali=2, Medellín=3, Bogotá=4, Pereira=5, Manizales=6, Bucaramanga=7, Barranquilla=8.

GLOSARIO OPERACIONAL:
- Muestra/Evento: visita del promotor a un cliente para entregar muestra física (tabla vwEventos).
- No-fiel: cliente con id_categoria NOT IN (42, 55, 58, 59, 60). Mayor potencial de conquista.
- Contactabilidad real: llamadas_respuestas.contestada = 1 (NO usar contactos.ultima_llamada).
- Captación: visitados no-fieles que además contestaron llamada posterior a la muestra.
- Conversión: de los que contestaron, cuántos generaron pedido (es_venta=1) después de la muestra.
- Cache coordenadas: archivo local con GPS de clientes. Actualizar con actualizar_cache_coordenadas.

FLUJO PARA MAPAS:
1. Construye el SQL de filtro. El SQL debe traer id_contacto + atributos. NUNCA lat/lon.
2. Llama consultar_clientes(sql, ciudad, tipo_mapa_sugerido, campo_valor/campo_color, titulo).
3. Si n_con_coords > 0 → llama INMEDIATAMENTE generar_mapa_clientes con EL MISMO sql y parámetros. Sin esperar confirmación.
4. Si n_con_coords == 0 → informa en 1 línea.

REGLAS DE MAPA CRÍTICAS:
- USA SIEMPRE generar_mapa_clientes para mapas de clientes. NUNCA ejecutar_codigo_mapa para esto.
- ejecutar_codigo_mapa es SOLO para visualizaciones que no sean listas de clientes (ej: polígonos de zona, rutas de línea).
- generar_mapa_clientes hace el merge con el cache de coordenadas internamente — NUNCA hagas el merge tú mismo.

RESPUESTAS — FORMATO ESTRICTO:
- Máximo 1-2 líneas de texto. NADA más.
- NUNCA escribas tablas, listas de KPIs ni números en tu respuesta. La UI los muestra automáticamente.
- Solo di: qué mapa se generó y dónde está. Ej: "Mapa listo: 58 clientes Aranjuez con deuda 30-720d."

OTROS FLUJOS:
- Ciudad → listar_rutas_ciudad primero.
- Ruta específica → consultar_ruta_completa.
- Tabla desconocida → explorar_tabla antes de asumir columnas.
- NUNCA inventes datos. SOLO SELECT/SHOW en BD.

REGLAS PARA ejecutar_codigo_mapa:
- El código DEBE asignar el mapa Folium a la variable `mapa`.
- NO llamar mapa.save() — el ejecutor lo hace solo.
- Usar tiles Esri: tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{{z}}/{{y}}/{{x}}', attr='Esri'
- Usar sql_read(sql_string, schema='fullclean_contactos') para queries.
- Aplicar TODOS los gotchas del esquema (CAST coords, JOIN ciudades, columnas correctas).

FORMATO DE RESPUESTA:
- Máximo 3 párrafos. Sin tablas largas. Sin bullets excesivos.
- Primero los números clave. Luego la interpretación. Luego la acción concreta.
- Si generaste mapa: indica la ruta del HTML y en una línea qué muestra.
- Si faltan datos: una línea explicando qué herramienta resolvería el vacío.

EJEMPLO INSIGHT BUENO:
"Ruta Laureles tiene 134 no-fieles sin visitar este mes. De ellos, 89 tienen coordenadas concentradas en Laureles Norte, que además acumula 12 quejas de 'producto no llegó'. Corrección logística + visita a ese cluster = 15-20 pedidos potenciales."

{schema_ctx}
"""


# ── Dispatcher de herramientas ────────────────────────────────────────────────

def _ejecutar_herramienta(nombre: str, argumentos: dict) -> Any:
    """Llama a la función correspondiente según el nombre de herramienta."""
    from agente import herramientas as h
    from agente import analisis_ruta as ar
    from agente import explorar_bd as eb
    from agente.coordinate_cache import CoordinateCache
    from agente.mapa_ejecutor import ejecutar_codigo_mapa

    def _explorar_tabla(**kwargs):
        accion = kwargs.get("accion")
        tabla  = kwargs.get("tabla", "")
        schema = kwargs.get("schema", "fullclean_contactos")
        if accion == "listar_tablas":
            return eb.listar_tablas(schema)
        elif accion == "describir_tabla":
            return eb.describir_tabla(tabla, schema)
        elif accion == "muestra":
            return eb.muestra_tabla(tabla, schema, kwargs.get("limite", 5), kwargs.get("filtro", ""))
        elif accion == "contar":
            return eb.contar_registros(tabla, schema, kwargs.get("filtro", ""))
        elif accion == "explorar_columna":
            return eb.explorar_relacion(tabla, kwargs.get("columna", ""), schema)
        elif accion == "select":
            return eb.ejecutar_select(kwargs.get("sql", ""), schema, kwargs.get("limite", 200))
        return {"error": f"Accion desconocida: {accion}"}

    def _actualizar_cache(**kwargs):
        cache = CoordinateCache()
        return cache.actualizar_desde_bd(
            ciudad=kwargs.get("ciudad", 3),
            fecha_inicio=kwargs.get("fecha_inicio", "2026-01-01"),
            fecha_fin=kwargs.get("fecha_fin"),
        )

    def _consultar_clientes(**kwargs):
        from pre_procesamiento.db_utils import sql_read
        from agente.coord_cache_parquet import buscar_coords
        from agente.kpi_auto import calcular_kpis, kpis_a_markdown

        sql        = kwargs.get("sql_clientes", "")
        ciudad     = kwargs.get("ciudad", 3)
        schema_sql = kwargs.get("schema_sql", "fullclean_contactos")

        # 1. Ejecutar SQL
        try:
            df = sql_read(sql, schema=schema_sql)
        except Exception as e:
            return {"ok": False, "error": f"Error en SQL: {e}"}

        if df.empty:
            return {"ok": False, "error": "El SQL no devolvió clientes con esos filtros."}

        if "id_contacto" not in df.columns:
            return {"ok": False, "error": "El SQL debe incluir 'id_contacto'."}

        n_total = len(df)

        # 2. Cruzar con cache de coords
        ids = df["id_contacto"].dropna().astype(int).tolist()
        df_coords = buscar_coords(ids, ciudad)
        n_con_coords = len(df_coords)

        # 3. Merge para KPIs (usamos df completo, no solo los con coords)
        df_merge = df.merge(df_coords, on="id_contacto", how="left")

        # 4. KPIs automáticos
        kpis = calcular_kpis(df_merge, n_con_coords)
        tabla_md = kpis_a_markdown(kpis)

        # 5. Muestra de primeras filas (sin lat/lon)
        cols_muestra = [c for c in df.columns if c not in ("lat", "lon")]
        muestra = df[cols_muestra].head(5).to_dict(orient="records")

        resultado = {
            "ok":           True,
            "n_total":      n_total,
            "n_con_coords": n_con_coords,
            "pct_coords":   round(100 * n_con_coords / n_total, 1) if n_total else 0,
            "columnas":     list(df.columns),
            "kpis":         kpis,
            "tabla_kpis_md": tabla_md,
            "muestra":      muestra,
            # Parámetros para reusar en generar_mapa_clientes
            "sql_para_mapa":        sql,
            "ciudad":               ciudad,
            "schema_sql":           schema_sql,
            "tipo_mapa_sugerido":   kwargs.get("tipo_mapa_sugerido", "clusters"),
            "campo_valor":          kwargs.get("campo_valor"),
            "campo_color":          kwargs.get("campo_color"),
            "titulo":               kwargs.get("titulo", ""),
        }
        return resultado

    def _generar_mapa_clientes(**kwargs):
        from pre_procesamiento.db_utils import sql_read
        from agente.coord_cache_parquet import buscar_coords
        from agente.map_renderer import pintar_mapa

        sql        = kwargs.get("sql_clientes", "")
        ciudad     = kwargs.get("ciudad", 3)
        tipo       = kwargs.get("tipo", "clusters")
        schema_sql = kwargs.get("schema_sql", "fullclean_contactos")
        titulo     = kwargs.get("titulo", "")
        campo_valor = kwargs.get("campo_valor")
        campo_color = kwargs.get("campo_color")

        # 1. Ejecutar SQL del agente
        try:
            df_clientes = sql_read(sql, schema=schema_sql)
        except Exception as e:
            return {"ok": False, "error": f"Error en SQL: {e}"}

        if df_clientes.empty:
            return {"ok": False, "error": "El SQL no devolvió ningún cliente con esos filtros."}

        if "id_contacto" not in df_clientes.columns:
            return {"ok": False, "error": "El SQL debe incluir 'id_contacto' como columna."}

        n_clientes = len(df_clientes)

        # 2. Merge con cache de coordenadas
        ids = df_clientes["id_contacto"].dropna().astype(int).tolist()
        df_coords = buscar_coords(ids, ciudad)

        if df_coords.empty:
            return {
                "ok": False,
                "error": (
                    f"Cache de coordenadas vacío para ciudad {ciudad}. "
                    "Ejecuta: python -m agente.coord_cache_parquet --ciudad {ciudad}"
                ),
            }

        # Merge: solo clientes que tienen coordenadas
        df_merge = df_clientes.merge(df_coords, on="id_contacto", how="inner")
        n_con_coords = len(df_merge)

        if df_merge.empty:
            return {
                "ok": False,
                "error": (
                    f"Ninguno de los {n_clientes} clientes del filtro tiene coordenadas en el cache. "
                    "Considera actualizar el cache con construir_cache_ciudad()."
                ),
            }

        # 3. Pintar mapa
        resultado = pintar_mapa(
            df_merge,
            tipo=tipo,
            titulo=titulo,
            ciudad_id=ciudad,
            campo_valor=campo_valor,
            campo_color=campo_color,
            nombre_archivo=titulo or tipo,
        )

        resultado["n_clientes_filtro"] = n_clientes
        resultado["n_con_coords"]      = n_con_coords
        resultado["pct_cobertura_geo"] = round(100 * n_con_coords / n_clientes, 1) if n_clientes else 0

        return resultado

    mapa_funciones = {
        "consultar_metricas":         h.consultar_metricas,
        "generar_mapa":               h.generar_mapa,
        "capturar_mapa":              h.capturar_mapa,
        "comparar_ciudades":          h.comparar_ciudades,
        "consultar_cliente":          h.consultar_cliente,
        "listar_promotores_activos":  h.listar_promotores_activos,
        "listar_rutas_ciudad":        ar.listar_rutas_ciudad,
        "consultar_ruta_completa":    ar.consultar_ruta_completa,
        "analizar_zona_promotor":     ar.analizar_zona_promotor,
        "explorar_tabla":             _explorar_tabla,
        "actualizar_cache_coordenadas": _actualizar_cache,
        "consultar_clientes":         _consultar_clientes,
        "generar_mapa_clientes":      _generar_mapa_clientes,
        "ejecutar_codigo_mapa":       ejecutar_codigo_mapa,
    }

    if nombre not in mapa_funciones:
        return {"error": f"Herramienta '{nombre}' no encontrada."}

    try:
        return mapa_funciones[nombre](**argumentos)
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "herramienta": nombre,
            "args": argumentos,
        }


# ── Clase principal del agente ────────────────────────────────────────────────

class AtlasAgent:
    """Agente conversacional para Atlas TA.

    Mantiene historial de conversación, inyecta schema context en el primer mensaje
    y ejecuta herramientas según necesite (incluida generación dinámica de mapas).
    """

    def __init__(self, model: str = "claude-sonnet-4-6"):
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "La librería anthropic no está instalada. "
                "Ejecuta: pip install anthropic"
            )

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "Variable ANTHROPIC_API_KEY no definida. "
                "Agrégala al archivo .env en la raíz del proyecto."
            )

        self._client    = anthropic.Anthropic(api_key=api_key)
        self._model     = model
        self._historial: list[dict] = []
        # System prompt construido una vez por sesión (incluye schema context + ejemplos)
        self._system    = _build_system_prompt()
        # Último mapa generado (ruta al HTML). Lo consume atlas_chat.py tras cada respuesta.
        self._ultimo_mapa: str | None = None
        # Última consulta de clientes (resultado de consultar_clientes). La UI renderiza los KPIs.
        self._ultima_consulta: dict | None = None

    def preguntar(self, mensaje: str) -> str:
        """Envía un mensaje al agente y devuelve la respuesta como texto.

        El agente puede invocar múltiples herramientas de forma encadenada
        antes de dar la respuesta final.

        Args:
            mensaje: Pregunta o instrucción en lenguaje natural.

        Returns:
            Respuesta textual del agente.
        """
        self._historial.append({"role": "user", "content": mensaje})

        while True:
            respuesta = self._client.messages.create(
                model=self._model,
                max_tokens=2500,
                system=self._system,
                tools=TOOLS_DEFINICION,
                messages=self._historial,
            )

            # ── El agente quiere usar herramientas ──────────────────────────
            if respuesta.stop_reason == "tool_use":
                self._historial.append({
                    "role": "assistant",
                    "content": respuesta.content,
                })

                tool_results = []
                for bloque in respuesta.content:
                    if bloque.type == "tool_use":
                        args_preview = json.dumps(bloque.input, ensure_ascii=False)[:80]
                        print(f"  [→ {bloque.name}] {args_preview}...")
                        resultado = _ejecutar_herramienta(bloque.name, bloque.input)
                                        # Registrar último mapa / última consulta para la UI
                        if bloque.name in ("ejecutar_codigo_mapa", "generar_mapa_clientes") \
                                and isinstance(resultado, dict) and resultado.get("ok"):
                            self._ultimo_mapa = resultado.get("html_path")
                        if bloque.name == "consultar_clientes" \
                                and isinstance(resultado, dict) and resultado.get("ok"):
                            self._ultima_consulta = resultado
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": bloque.id,
                            "content": json.dumps(resultado, ensure_ascii=False, default=str),
                        })

                self._historial.append({
                    "role": "user",
                    "content": tool_results,
                })
                # Continuar el loop — el agente puede usar más herramientas

            # ── Respuesta final de texto ────────────────────────────────────
            elif respuesta.stop_reason == "end_turn":
                texto = ""
                for bloque in respuesta.content:
                    if hasattr(bloque, "text"):
                        texto += bloque.text

                self._historial.append({
                    "role": "assistant",
                    "content": texto,
                })
                return texto

            # ── Caso inesperado ─────────────────────────────────────────────
            else:
                return f"[Agente detuvo con stop_reason inesperado: {respuesta.stop_reason}]"

    def limpiar_historial(self) -> None:
        """Reinicia el historial de conversación (nueva sesión)."""
        self._historial = []
        self._ultimo_mapa = None
        self._ultima_consulta = None

    def recargar_contexto(self) -> None:
        """Recarga el system prompt (útil si se agregaron nuevos ejemplos al banco)."""
        self._system = _build_system_prompt()
