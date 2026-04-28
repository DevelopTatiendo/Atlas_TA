"""Interfaz de línea de comandos para Atlas Agent.

Uso interactivo:
    python -m agente.cli

Pregunta única (para scripts/automatizaciones):
    python -m agente.cli --pregunta "¿Cómo está Cali esta semana?"

Batch desde archivo de preguntas:
    python -m agente.cli --batch preguntas.txt --salida respuestas.txt
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.secrets_manager import load_env_secure
load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

# Diagnóstico: verificar variables críticas después de cargar el .env
import os as _os
_api_key = _os.getenv("ANTHROPIC_API_KEY", "")
if not _api_key:
    _dotenv_path = str(_ROOT / ".env")
    print(f"⚠️  ANTHROPIC_API_KEY no encontrada después de cargar {_dotenv_path}")
    print("   Verifica que el archivo .env contiene exactamente: ANTHROPIC_API_KEY=sk-ant-...")
    print("   (sin espacios antes del '=' y sin comillas alrededor del valor)")
    raise SystemExit(1)


def modo_interactivo():
    """Sesión de chat interactiva con el agente."""
    from agente.atlas_agent import AtlasAgent

    print("=" * 60)
    print("  Atlas Agent — T Atiendo S.A.")
    print("  Escribe tu pregunta. 'salir' para terminar. 'reset' para nueva sesión.")
    print("=" * 60)

    agente = AtlasAgent()

    while True:
        try:
            entrada = input("\n[Tú] ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nHasta luego.")
            break

        if not entrada:
            continue

        if entrada.lower() == "salir":
            print("Hasta luego.")
            break

        if entrada.lower() == "reset":
            agente.limpiar_historial()
            print("[Sesión reiniciada]")
            continue

        respuesta = agente.preguntar(entrada)
        print(f"\n[Atlas Agent]\n{respuesta}")


def modo_pregunta_unica(pregunta: str) -> str:
    """Responde una sola pregunta y termina."""
    from agente.atlas_agent import AtlasAgent
    agente = AtlasAgent()
    return agente.preguntar(pregunta)


def modo_batch(archivo_entrada: str, archivo_salida: str):
    """Procesa una lista de preguntas desde un archivo."""
    from agente.atlas_agent import AtlasAgent

    preguntas = Path(archivo_entrada).read_text(encoding="utf-8").strip().splitlines()
    agente = AtlasAgent()
    lineas_salida = []

    for i, pregunta in enumerate(preguntas, 1):
        pregunta = pregunta.strip()
        if not pregunta:
            continue
        print(f"[{i}/{len(preguntas)}] {pregunta[:60]}...")
        respuesta = agente.preguntar(pregunta)
        lineas_salida.append(f"# Pregunta {i}: {pregunta}\n\n{respuesta}\n\n---\n")
        agente.limpiar_historial()  # Nueva sesión por pregunta en batch

    Path(archivo_salida).write_text("\n".join(lineas_salida), encoding="utf-8")
    print(f"\nRespuestas guardadas en: {archivo_salida}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Atlas Agent CLI")
    parser.add_argument("--pregunta", type=str, help="Pregunta única (modo no interactivo)")
    parser.add_argument("--batch", type=str, help="Archivo .txt con preguntas (una por línea)")
    parser.add_argument("--salida", type=str, default="respuestas_agente.txt",
                        help="Archivo de salida para modo batch")
    args = parser.parse_args()

    if args.pregunta:
        print(modo_pregunta_unica(args.pregunta))
    elif args.batch:
        modo_batch(args.batch, args.salida)
    else:
        modo_interactivo()
