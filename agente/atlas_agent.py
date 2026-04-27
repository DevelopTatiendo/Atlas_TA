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
]


# ── Sistema de contexto de negocio ────────────────────────────────────────────

SYSTEM_PROMPT = """Eres Atlas Agent, el asistente de análisis de operaciones de campo de T Atiendo S.A.

Tienes acceso a datos reales de promotores que entregan muestras físicas a clientes en 7 ciudades de Colombia:
Cali (id 2), Medellín (id 3), Bogotá (id 4), Pereira (id 5), Manizales (id 6), Bucaramanga (id 7), Barranquilla (id 8).

GLOSARIO DE MÉTRICAS:
- Muestras: visitas totales realizadas por un promotor (incluye re-visitas al mismo cliente).
- Clientes: personas DISTINTAS visitadas por un promotor.
- No fieles: clientes cuya categoría NO es Ticket (58-60) ni Frecuente (42, 55). Son clientes nuevos o potenciales.
- % Contactabilidad: de todos los clientes visitados, cuántos contestaron una llamada posterior a la muestra.
- % Captación: de todos los clientes visitados, cuántos son nuevos (no fieles) Y contestaron. Mide la efectividad real de conquista de nuevos clientes.
- % Conversión: de los clientes que contestaron, cuántos compraron (es_venta=1). Solo se cuenta si la llamada fue POSTERIOR a la muestra del mismo promotor.
- Clientes/km²: densidad del territorio de operación.
- La atribución temporal es clave: una llamada solo le cuenta a un promotor si ocurrió DESPUÉS de que ese promotor entregó la muestra.

REGLAS DE RESPUESTA:
1. Siempre usa las herramientas para obtener datos reales; nunca inventes números.
2. Cuando el usuario pida métricas, usa consultar_metricas primero.
3. Cuando el usuario pida ver el mapa, usa generar_mapa. Si además pide imagen/screenshot, encadena con capturar_mapa.
4. Para comparar desempeño nacional, usa comparar_ciudades.
5. Para datos de un cliente específico, usa consultar_cliente.
6. Responde en español, en tono profesional pero directo.
7. Cuando presentes métricas, interprétalas en contexto: ¿es bueno un 74% de contactabilidad? Sí, es alto. ¿Un 27% de captación? Depende del mix de cartera.
8. Si hay promotores con métricas anómalas (muy altas o muy bajas), señálalo proactivamente.
"""


# ── Dispatcher de herramientas ────────────────────────────────────────────────

def _ejecutar_herramienta(nombre: str, argumentos: dict) -> Any:
    """Llama a la función correspondiente en herramientas.py."""
    from agente import herramientas as h

    mapa_funciones = {
        "consultar_metricas": h.consultar_metricas,
        "generar_mapa": h.generar_mapa,
        "capturar_mapa": h.capturar_mapa,
        "comparar_ciudades": h.comparar_ciudades,
        "consultar_cliente": h.consultar_cliente,
        "listar_promotores_activos": h.listar_promotores_activos,
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

    def __init__(self, model: str = "claude-opus-4-6"):
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
                max_tokens=4096,
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
        """Reinicia la conversación."""
        self._historial = []

    @property
    def n_turnos(self) -> int:
        """Número de turnos en el historial actual."""
        return len([m for m in self._historial if m["role"] == "user"])
