# agente/vanna_sql/atlas_vanna.py
"""
Clase principal Vanna para Atlas TA.
Combina:
  - OpenAI_Chat (legacy) apuntando a Groq → LLM gratuito vía API OpenAI-compatible
  - ChromaDB_VectorStore (legacy)         → vector store local en disco (sin costo)
  - MySQL                                 → fullclean_* (solo lectura)

Por qué "legacy":
  vanna 2.0 reescribió su arquitectura interna (nuevo agente framework).
  El patrón de herencia múltiple (mixin) que usamos aquí sigue viviendo en
  vanna.legacy y es completamente estable para uso directo.

Por qué Groq via OpenAI-compat:
  Groq expone una API 100% compatible con OpenAI en https://api.groq.com/openai/v1.
  Pasamos un cliente OpenAI custom a OpenAI_Chat — sin cambiar nada más.

Uso rápido:
    from config.secrets_manager import load_env_secure
    load_env_secure()
    from agente.vanna_sql.atlas_vanna import get_vanna
    vn = get_vanna()
    sql = vn.generate_sql("¿Cuántas llamadas contestadas tuvo el asesor García en marzo?")
    df  = vn.run_sql(sql)
"""

import os
from pathlib import Path

from openai import OpenAI
from vanna.legacy.chromadb import ChromaDB_VectorStore
from vanna.legacy.openai  import OpenAI_Chat

# ── Ruta persistente del vector store (gitignoreada) ─────────────────────────
_CHROMA_DIR = Path(__file__).parent / "chroma_store"

# ── URL base de Groq — compatible con OpenAI ─────────────────────────────────
_GROQ_BASE_URL = "https://api.groq.com/openai/v1"


class AtlasVanna(ChromaDB_VectorStore, OpenAI_Chat):
    """
    Clase Vanna para Atlas TA.
    Herencia:  ChromaDB_VectorStore (vector store) + OpenAI_Chat (LLM via Groq).
    El orden importa: ChromaDB primero para que su MRO tenga prioridad en train().
    """

    def __init__(self, client: OpenAI, config: dict | None = None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, client=client, config=config)

    def log(self, message: str, title: str = "Info") -> None:
        """Silencia el verbose interno de Vanna (prompts, LLM responses, etc.)."""
        pass


def get_vanna(
    model: str = "llama-3.3-70b-versatile",
    connect_db: bool = True,
) -> AtlasVanna:
    """
    Fábrica principal. Lee credenciales del entorno (ya descifradas por
    secrets_manager antes de llamar a esta función).

    Parámetros:
        model       → modelo Groq a usar
        connect_db  → si False, omite la conexión MySQL (útil para training
                      y generación de SQL sin ejecutar contra la BD)

    Modelos Groq activos (abril 2026):
      - llama-3.3-70b-versatile  → recomendado, 128K contexto, gratis
      - llama-3.1-8b-instant     → más rápido, menor costo, algo menos preciso
      - mixtral-8x7b-32768       → buena alternativa
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GROQ_API_KEY no encontrada en el entorno. "
            "Asegúrate de llamar a load_env_secure() antes de get_vanna()."
        )

    # Cliente OpenAI apuntando a Groq
    groq_client = OpenAI(
        api_key=api_key,
        base_url=_GROQ_BASE_URL,
    )

    vn = AtlasVanna(
        client=groq_client,
        config={
            "model": model,
            "path":  str(_CHROMA_DIR),   # ChromaDB: carpeta persistente local
        },
    )

    if connect_db:
        vn.connect_to_mysql(
            host=os.environ.get("DB_HOST"),
            dbname=os.environ.get("DB_NAME", "fullclean_telemercadeo"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            port=int(os.environ.get("DB_PORT", 3306)),
        )

    return vn
