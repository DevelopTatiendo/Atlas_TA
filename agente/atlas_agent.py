"""Atlas Agent — Agente conversacional para análisis de operaciones de campo.

Usa la API de Claude con tool_use para responder preguntas en lenguaje natural
sobre las métricas de promotores, mapas de cobertura y datos de clientes.

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
            "Lista rutas comerciales activas de una ciudad con métricas de actividad: "
            "n_clientes (universo), visitados_periodo, pct_cobertura, con_pedido_periodo, "
            "ultima_visita y sin_visitar (oportunidad directa). "
            "Punto de entrada obligatorio para cualquier análisis por ruta — "
            "úsala para identificar qué rutas tienen baja cobertura o alta oportunidad."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "integer", "description": "id_centroope de la ciudad (3=Medellín, 2=Cali, 4=Bogotá, 5=Pereira, 6=Manizales, 7=Bucaramanga, 8=Barranquilla)."},
                "fecha_inicio": {"type": "string", "description": "Desde cuándo medir visitas y pedidos recientes (YYYY-MM-DD, default 2026-01-01)."},
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "consultar_ruta_completa",
        "description": (
            "Análisis profundo de una ruta comercial: universo de clientes, cobertura de visitas, conversión a pedidos, "
            "quejas activas, productos más vendidos en la zona, oportunidades no-fieles sin visitar con coordenadas. "
            "Esta es la herramienta central para entender qué está pasando en una ruta específica."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "integer", "description": "ID numérico de la ciudad."},
                "id_ruta": {"type": "integer", "description": "ID de la ruta."},
                "nombre_ruta": {"type": "string", "description": "Nombre parcial de la ruta (alternativa a id_ruta)."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": ["ciudad"],
        },
    },
    {
        "name": "analizar_zona_promotor",
        "description": (
            "Análisis del territorio real cubierto por un promotor: área en km² recorrida, distancia total, "
            "clientes visitados vs clientes en la zona sin visitar (del cache), productos más pedidos, "
            "quejas activas en esa zona. Úsala para entender si un promotor está optimizando su territorio."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id_promotor": {"type": "integer", "description": "ID del promotor."},
                "ciudad": {"type": "integer", "description": "ID numérico de la ciudad."},
                "fecha_inicio": {"type": "string", "description": "Fecha inicio YYYY-MM-DD."},
                "fecha_fin": {"type": "string", "description": "Fecha fin YYYY-MM-DD."},
            },
            "required": ["id_promotor", "ciudad", "fecha_inicio"],
        },
    },
    {
        "name": "explorar_tabla",
        "description": (
            "El agente usa esto para explorar la BD de forma autónoma: listar tablas, ver columnas, "
            "contar registros, explorar dominios de columnas, o ejecutar un SELECT personalizado. "
            "Usar cuando el agente necesite entender la estructura antes de construir una consulta compleja. "
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
                "tabla": {"type": "string", "description": "Nombre de la tabla (para describir, muestra, contar, explorar_columna)."},
                "schema": {"type": "string", "description": "Base de datos (default: fullclean_contactos)."},
                "columna": {"type": "string", "description": "Nombre de columna (para explorar_columna)."},
                "sql": {"type": "string", "description": "Consulta SELECT para accion=select."},
                "filtro": {"type": "string", "description": "Cláusula WHERE para muestra/contar (solo texto de condición, sin WHERE)."},
                "limite": {"type": "integer", "description": "Límite de filas (default: 5 para muestra, 200 para select)."},
            },
            "required": ["accion"],
        },
    },
    {
        "name": "actualizar_cache_coordenadas",
        "description": (
            "Actualiza el cache local de coordenadas de clientes consultando eventos recientes de la BD. "
            "Llamar antes de analizar_zona_promotor o cuando se quiera enriquecer el mapa simulado. "
            "El cache acumula coordenadas con cada actualización — mejora con el tiempo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ciudad": {"type": "integer", "description": "ID de ciudad (default: 3 = Medellín)."},
                "fecha_inicio": {"type": "string", "description": "Fecha desde (YYYY-MM-DD)."},
                "fecha_fin": {"type": "string", "description": "Fecha hasta (YYYY-MM-DD, default: hoy)."},
            },
            "required": [],
        },
    },
]


# ── Sistema de contexto de negocio ────────────────────────────────────────────

SYSTEM_PROMPT = """Eres Atlas Agent, el asistente de inteligencia operacional de T Atiendo S.A.

Tu misión es analizar la operación de campo de promotores que entregan muestras físicas en 7 ciudades de Colombia, cruzando datos de visitas, pedidos, llamadas, quejas y territorios para encontrar oportunidades comerciales y alertas que los humanos no detectan fácilmente.

CIUDADES: Cali (2), Medellín (3), Bogotá (4), Pereira (5), Manizales (6), Bucaramanga (7), Barranquilla (8).

GLOSARIO OPERACIONAL:
- Ruta: territorio comercial definido por barrios. Unidad principal de análisis.
- Muestra/Evento: visita del promotor a un cliente para entregar muestra física.
- No-fiel: cliente cuya categoría NO es Ticket (58-60) ni Frecuente (42, 55). Mayor potencial de conquista.
- Contactabilidad real: llamadas con contestada=1 en llamadas_respuestas (NO usar contactos.ultima_llamada).
- Captación: visitados que son no-fieles Y contestaron llamada posterior.
- Conversión: de los que contestaron, cuántos generaron pedido (es_venta=1), solo si la llamada fue POSTERIOR a la muestra.
- Cache de coordenadas: archivo local que acumula coordenadas de clientes de visitas históricas. Mejora con cada actualización.

CÓMO PENSAR Y ACTUAR:
1. EXPLORA ANTES DE ANALIZAR: Si no conoces una tabla, usa explorar_tabla para ver sus columnas y un sample. No asumas estructuras.
2. LA RUTA ES TU UNIDAD: Siempre enmarca el análisis a nivel de ruta, no solo ciudad o promotor.
3. CRUZA DATOS: Un hallazgo vale solo si está respaldado por al menos 2 fuentes (ej: visitas + pedidos, quejas + zona geográfica).
4. USA EL CACHE DE COORDENADAS: Para análisis de zona, llama actualizar_cache_coordenadas primero si no hay datos recientes.
5. NEVER INVENTES DATOS: Si la BD no lo dice, no lo digas. Indica cuando hay incertidumbre.
6. SOLO SELECT/SHOW: Nunca sugieras ni ejecutes escrituras en BD.

FORMATO DE RESPUESTA:
- Máximo 3 párrafos cortos. Sin tablas largas ni bullet points excesivos.
- Primero los números clave. Luego la interpretación. Luego la acción concreta.
- Si generaste un mapa, di la ruta del archivo y qué muestra. Nada más.
- Si no tienes datos suficientes, dilo en una línea y sugiere qué herramienta llamar.

EJEMPLO DE INSIGHT BUENO:
"En la Ruta 7 hay 134 clientes no-fieles sin visitar este mes. De ellos, 89 tienen coordenadas conocidas y están concentrados en el barrio Laureles Norte. Ese barrio tiene 12 quejas activas de tipo 'producto no llegó' — posible problema de logística que puede estar afectando la disposición del cliente. Si se corrige y se visita ese cluster, el potencial de captación estimado es de 15-20 pedidos adicionales."

EJEMPLO DE INSIGHT MALO (no hagas esto):
"La Ruta 7 tiene baja cobertura. Se recomienda visitar más clientes."
"""


# ── Dispatcher de herramientas ────────────────────────────────────────────────

def _ejecutar_herramienta(nombre: str, argumentos: dict) -> Any:
    """Llama a la función correspondiente en herramientas.py."""
    from agente import herramientas as h

    from agente import analisis_ruta as ar
    from agente import explorar_bd as eb
    from agente.coordinate_cache import CoordinateCache

    def _explorar_tabla(**kwargs):
        accion = kwargs.get("accion")
        tabla = kwargs.get("tabla", "")
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

    mapa_funciones = {
        "consultar_metricas": h.consultar_metricas,
        "generar_mapa": h.generar_mapa,
        "capturar_mapa": h.capturar_mapa,
        "comparar_ciudades": h.comparar_ciudades,
        "consultar_cliente": h.consultar_cliente,
        "listar_promotores_activos": h.listar_promotores_activos,
        # Nuevas herramientas de análisis por ruta
        "listar_rutas_ciudad": ar.listar_rutas_ciudad,
        "consultar_ruta_completa": ar.consultar_ruta_completa,
        "analizar_zona_promotor": ar.analizar_zona_promotor,
        "explorar_tabla": _explorar_tabla,
        "actualizar_cache_coordenadas": _actualizar_cache,
    }

    if nombre not in mapa_funciones:
        return {"error": f"Herramienta '{nombre}' no encontrada."}

    try:
        return mapa_funciones[nombre](**argumentos)
    except Exception as e:
        return {"error": str(e), "herramienta": nombre, "args": argumentos}


# ── Clase principal del agente ────────────────────────────────────────────────

class AtlasAgent:
    """Agente conversacional para Atlas TA.

    Mantiene historial de conversación y ejecuta herramientas según necesite.
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
                "Agrégala a config/.env o como variable del sistema."
            )

        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._historial: list[dict] = []

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
                max_tokens=1800,
                system=SYSTEM_PROMPT,
                tools=TOOLS_DEFINICION,
                messages=self._historial,
            )

            # Si el agente quiere usar herramientas
            if respuesta.stop_reason == "tool_use":
                # Registrar respuesta del asistente con las tool calls
                self._historial.append({
                    "role": "assistant",
                    "content": respuesta.content,
                })

                # Ejecutar cada herramienta y recopilar resultados
                tool_results = []
                for bloque in respuesta.content:
                    if bloque.type == "tool_use":
                        print(f"[Atlas Agent] → {bloque.name}({json.dumps(bloque.input, ensure_ascii=False)[:80]}...)")
                        resultado = _ejecutar_herramienta(bloque.name, bloque.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": bloque.id,
                            "content": json.dumps(resultado, ensure_ascii=False, default=str),
                        })

                # Devolver resultados al agente para que continúe
                self._historial.append({
                    "role": "user",
                    "content": tool_results,
                })
                # Continuar el loop hasta stop_reason == "end_turn"

            elif respuesta.stop_reason == "end_turn":
                # Respuesta final de texto
                texto = ""
                for bloque in respuesta.content:
                    if hasattr(bloque, "text"):
                        texto += bloque.text
                self._historial.append({"role": "assistant", "content": texto})
                return texto

            else:
                # Caso inesperado
                return f"[stop_reason inesperado: {respuesta.stop_reason}]"

    def limpiar_historial(self):
        """Limpia el historial para empezar una conversación nueva."""
        self._historial = []
        print("[Atlas Agent] Historial limpiado.")
