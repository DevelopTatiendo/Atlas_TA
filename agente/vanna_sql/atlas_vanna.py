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
import hashlib
from pathlib import Path

from openai import OpenAI
from vanna.legacy.chromadb import ChromaDB_VectorStore
from vanna.legacy.openai  import OpenAI_Chat


# ─────────────────────────────────────────────────────────────────────────────
# Embedding — sentence-transformers con fallback a hash puro (stdlib)
#
# Preferido: sentence-transformers/all-MiniLM-L6-v2
#   • 384 dimensiones, retrieval semántico real
#   • Requiere: pip install sentence-transformers>=2.7.0
#
# Fallback: _HashEmbeddingFunction
#   • Determinista, sin dependencias externas
#   • Activado automáticamente si sentence-transformers no está instalado
#   • También se activa si onnxruntime tiene incompatibilidad de ABI (Windows)
# ─────────────────────────────────────────────────────────────────────────────

class _HashEmbeddingFunction:
    """
    Embedding determinista de 384 dimensiones basado en SHA-256.
    No requiere onnxruntime, torch ni sentence-transformers.
    Compatible con ChromaDB 1.x — requiere name() y __call__(list[str]) → list[list[float]]
    """
    _DIM = 384

    def name(self) -> str:  # requerido por ChromaDB 1.x
        return "atlas-hash-ef-v1"

    def embed_query(self, text: str) -> list:
        return self([text])[0]

    def embed_documents(self, texts: list) -> list:
        return self(texts)

    def __call__(self, input: list) -> list:  # type: ignore[override]
        results = []
        for text in input:
            seed = hashlib.sha256(text.lower().strip().encode("utf-8")).digest()
            nums: list[float] = []
            cur = seed
            while len(nums) < self._DIM:
                cur = hashlib.sha256(cur).digest()
                for b in cur:
                    nums.append((b - 128) / 128.0)
            nums = nums[: self._DIM]
            mag = sum(x * x for x in nums) ** 0.5 or 1.0
            results.append([x / mag for x in nums])
        return results


class _SentenceTransformerEF:
    """
    Wrapper sentence-transformers/all-MiniLM-L6-v2 para ChromaDB.
    Ofrece retrieval semántico real (cosine similarity sobre embeddings densos).
    """
    _MODEL_NAME = "all-MiniLM-L6-v2"

    def __init__(self):
        from sentence_transformers import SentenceTransformer  # lazy import
        self._model = SentenceTransformer(self._MODEL_NAME)

    def name(self) -> str:
        return f"sentence-transformers/{self._MODEL_NAME}"

    def embed_query(self, text: str) -> list:
        return self([text])[0]

    def embed_documents(self, texts: list) -> list:
        return self(texts)

    def __call__(self, input: list) -> list:  # type: ignore[override]
        embeddings = self._model.encode(input, normalize_embeddings=True)
        return embeddings.tolist()


def _build_embedding_function():
    """
    Intenta construir _SentenceTransformerEF; si falla por cualquier razón
    (paquete no instalado, incompatibilidad de ABI con onnxruntime, etc.)
    cae silenciosamente a _HashEmbeddingFunction.
    """
    try:
        ef = _SentenceTransformerEF()
        print("🔍 Embedding: sentence-transformers/all-MiniLM-L6-v2 (semántico)")
        return ef
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️  sentence-transformers no disponible ({exc}). Usando hash embedding.")
        return _HashEmbeddingFunction()


_EMBEDDING_FN = _build_embedding_function()

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
    model:         str | None = None,
    provider:      str | None = None,
    connect_db:    bool = True,
    _key_override: str | None = None,   # Para forzar una API key concreta
) -> AtlasVanna:
    """
    Fábrica principal.

    Parámetros:
        model          → modelo a usar; si None usa el default del proveedor
        provider       → 'gemini' | 'groq'; si None detecta automáticamente
                         (prioridad: Gemini > Groq)
        connect_db     → si False omite la conexión MySQL (útil para training)
        _key_override  → API key directa (útil para forzar GROQ_API_KEY2)

    Modelos recomendados:
      Gemini:  gemini-2.0-flash (rápido, 1M tokens/día, gratis)
               gemini-1.5-pro   (más potente, mismo límite)
      Groq:    llama-3.3-70b-versatile (100k tokens/día por key)
    """
    # ── Detectar proveedor ────────────────────────────────────────────────────
    if provider is None:
        if os.environ.get("GEMINI_API_KEY"):
            provider = "gemini"
        elif os.environ.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEY2"):
            provider = "groq"
        else:
            raise EnvironmentError(
                "No se encontró GEMINI_API_KEY ni GROQ_API_KEY en el entorno. "
                "Llama a load_env_secure() antes de get_vanna()."
            )

    cfg = _PROVIDERS.get(provider)
    if not cfg:
        raise ValueError(f"Proveedor desconocido: '{provider}'. Usa 'gemini' o 'groq'.")

    # ── Resolver API key (KEY1 → KEY2 → override) ────────────────────────────
    if _key_override:
        api_key = _key_override
        key_label = "(override directo)"
    else:
        api_key = os.environ.get(cfg["env_key"])
        key_label = cfg["env_key"]
        # Groq: fallback automático a KEY2 si KEY1 no existe
        if not api_key and provider == "groq":
            api_key = os.environ.get("GROQ_API_KEY2")
            key_label = "GROQ_API_KEY2 (fallback)"

    if not api_key:
        raise EnvironmentError(
            f"{cfg['env_key']} no encontrada. "
            "Asegúrate de llamar a load_env_secure() antes de get_vanna()."
        )

    active_model = model or cfg["default_model"]
    print(f"🤖 Proveedor: {cfg['label']} [{key_label}] | Modelo: {active_model}")

    # ── Crear cliente OpenAI-compatible ──────────────────────────────────────
    llm_client = OpenAI(
        api_key=api_key,
        base_url=cfg["base_url"],
    )

    vn = AtlasVanna(
        client=llm_client,
        config={
            "model":              active_model,
            "path":               str(_CHROMA_DIR),
            "embedding_function": _EMBEDDING_FN,
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


def get_vanna_groq_key2(
    model:      str | None = None,
    connect_db: bool = False,
) -> AtlasVanna:
    """
    Fábrica que fuerza GROQ_API_KEY2 como API key.
    Útil como fallback cuando GROQ_API_KEY1 ha alcanzado el rate limit (429).

    Uso típico (en el agente):
        try:
            vn = get_vanna(connect_db=False)
            sql = vn.generate_sql(pregunta)
        except Exception as e:
            if "429" in str(e):
                vn2 = get_vanna_groq_key2()
                sql  = vn2.generate_sql(pregunta)
    """
    api_key = os.environ.get("GROQ_API_KEY2")
    if not api_key:
        raise EnvironmentError(
            "GROQ_API_KEY2 no encontrada. "
            "Verifica que este en el .env y que load_env_secure() haya sido llamado."
        )
    return get_vanna(
        model=model,
        provider="groq",
        connect_db=connect_db,
        _key_override=api_key,
    )
