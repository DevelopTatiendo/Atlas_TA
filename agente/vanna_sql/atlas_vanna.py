# agente/vanna_sql/atlas_vanna.py
"""
Clase principal Vanna para Atlas TA.
Combina:
  - OpenAI_Chat (legacy) → soporta Gemini y Groq vía API OpenAI-compatible
  - ChromaDB_VectorStore → vector store local en disco (sin costo)
  - MySQL                → fullclean_* (solo lectura)

Proveedor activo (configurable por variable de entorno):
  GEMINI_API_KEY  → usa Google Gemini (gemini-2.0-flash) — 1M tokens/día gratis
  GROQ_API_KEY    → usa Groq (llama-3.3-70b-versatile)   — 100k tokens/día gratis

La función get_vanna() detecta automáticamente cuál key está disponible.
Se puede forzar un proveedor con el parámetro provider='gemini' | 'groq'.

Uso rápido:
    from config.secrets_manager import load_env_secure
    load_env_secure()
    from agente.vanna_sql.atlas_vanna import get_vanna
    vn = get_vanna()
    sql = vn.generate_sql("Lista de clientes del CO Medellín con pedido en abril 2026")
    df  = vn.run_sql(sql)
"""

import os
from pathlib import Path

from openai import OpenAI
from vanna.legacy.chromadb import ChromaDB_VectorStore
from vanna.legacy.openai  import OpenAI_Chat

# ── Ruta persistente del vector store (gitignoreada) ─────────────────────────
_CHROMA_DIR = Path(__file__).parent / "chroma_store"

# ── Endpoints OpenAI-compatibles ─────────────────────────────────────────────
_PROVIDERS = {
    "gemini": {
        "base_url":    "https://generativelanguage.googleapis.com/v1beta/openai/",
        "env_key":     "GEMINI_API_KEY",
        "default_model": "gemini-2.0-flash",
        "label":       "Google Gemini (1M tokens/día gratis)",
    },
    "groq": {
        "base_url":    "https://api.groq.com/openai/v1",
        "env_key":     "GROQ_API_KEY",
        "default_model": "llama-3.3-70b-versatile",
        "label":       "Groq (100k tokens/día gratis)",
    },
}


class AtlasVanna(ChromaDB_VectorStore, OpenAI_Chat):
    """
    Clase Vanna para Atlas TA.
    Herencia: ChromaDB_VectorStore (vector store) + OpenAI_Chat (LLM).
    El orden importa: ChromaDB primero para que su MRO tenga prioridad en train().
    """

    def __init__(self, client: OpenAI, config: dict | None = None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, client=client, config=config)

    def log(self, message: str, title: str = "Info") -> None:
        """Silencia el verbose interno de Vanna (prompts, LLM responses, etc.)."""
        pass

    def submit_prompt(self, prompt, **kwargs):
        """
        Sanitiza el prompt antes de mandarlo al LLM.

        Gemini (endpoint OpenAI-compatible) rechaza con 400 INVALID_ARGUMENT si:
          - content es None/null
          - el mensaje tiene campos extra con None (function_call, tool_calls, name, etc.)
            que el SDK serializa como null en el JSON

        Este override reconstruye solo {role, content} para cada mensaje,
        descartando cualquier campo adicional que Vanna o el SDK puedan añadir.
        """
        if isinstance(prompt, list):
            sanitized = []
            for msg in prompt:
                if not isinstance(msg, dict):
                    continue
                role    = msg.get("role")
                content = msg.get("content")
                # Descartar mensajes con role o content nulos
                if not role or content is None:
                    continue
                # SOLO role + content — no propagar function_call/tool_calls/name/etc.
                sanitized.append({"role": str(role), "content": str(content)})
            prompt = sanitized
        return super().submit_prompt(prompt, **kwargs)


def get_vanna(
    model:      str | None = None,
    provider:   str | None = None,
    connect_db: bool = True,
) -> AtlasVanna:
    """
    Fábrica principal.

    Parámetros:
        model      → modelo a usar; si None usa el default del proveedor
        provider   → 'gemini' | 'groq'; si None detecta automáticamente
                     (prioridad: Gemini > Groq)
        connect_db → si False omite la conexión MySQL (útil para training)

    Modelos recomendados:
      Gemini:  gemini-2.0-flash (rápido, 1M tokens/día, gratis)
               gemini-1.5-pro   (más potente, mismo límite)
      Groq:    llama-3.3-70b-versatile (100k tokens/día)
    """
    # ── Detectar proveedor ────────────────────────────────────────────────────
    if provider is None:
        if os.environ.get("GEMINI_API_KEY"):
            provider = "gemini"
        elif os.environ.get("GROQ_API_KEY"):
            provider = "groq"
        else:
            raise EnvironmentError(
                "No se encontró GEMINI_API_KEY ni GROQ_API_KEY en el entorno. "
                "Llama a load_env_secure() antes de get_vanna()."
            )

    cfg = _PROVIDERS.get(provider)
    if not cfg:
        raise ValueError(f"Proveedor desconocido: '{provider}'. Usa 'gemini' o 'groq'.")

    api_key = os.environ.get(cfg["env_key"])
    if not api_key:
        raise EnvironmentError(
            f"{cfg['env_key']} no encontrada. "
            "Asegúrate de llamar a load_env_secure() antes de get_vanna()."
        )

    active_model = model or cfg["default_model"]
    print(f"🤖 Proveedor: {cfg['label']} | Modelo: {active_model}")

    # ── Crear cliente OpenAI-compatible ──────────────────────────────────────
    llm_client = OpenAI(
        api_key=api_key,
        base_url=cfg["base_url"],
    )

    vn = AtlasVanna(
        client=llm_client,
        config={
            "model": active_model,
            "path":  str(_CHROMA_DIR),
        },
    )

    # ── Conexión MySQL (opcional) ─────────────────────────────────────────────
    if connect_db:
        vn.connect_to_mysql(
            host=os.environ.get("DB_HOST"),
            dbname=os.environ.get("DB_NAME", "fullclean_telemercadeo"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            port=int(os.environ.get("DB_PORT", 3306)),
        )

    return vn
