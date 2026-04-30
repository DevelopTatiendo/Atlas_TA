# CLAUDE.md — Atlas TA · Contexto de desarrollo activo

> Documento de contexto para sesiones de desarrollo con Claude (Cowork).
> Actualizado: 30 de abril de 2026 · Rama: `produccion_2`

---

## 🏢 Qué es Atlas TA

Visor interactivo de operaciones comerciales de **T Atiendo S.A.** — mapas, segmentación de clientes y análisis geoespacial sobre múltiples Centros de Operación (COs) en Colombia. Stack: Python · Streamlit · Flask · MySQL (RDS) · Folium · Plotly.

---

## 🗂️ Estructura de carpetas clave

```
Atlas_TA/
├── agente/
│   ├── atlas_agent.py          # Agente conversacional principal (Claude API)
│   └── vanna_sql/
│       ├── __init__.py
│       ├── atlas_vanna.py      # Clase AtlasVanna + fábrica get_vanna()
│       ├── trainer.py          # Pipeline de entrenamiento ChromaDB
│       └── chroma_store/       # Vector store local (gitignoreado)
├── config/
│   ├── secrets_manager.py      # load_env_secure() — descifra .env.enc
│   ├── .env.enc                # Credenciales cifradas (SÍ va al repo)
│   └── .env.example            # Plantilla sin valores
├── pre_procesamiento/
│   └── db_utils.py             # sql_read() — helper de lectura MySQL
├── utils/
│   └── generar_coordenadas_clientes.py  # Extrae lat/lon mediana por CO
├── static/
│   └── datos/coordenadas/      # CSVs base de mapa por CO (SÍ van al repo)
├── requirements.txt
└── CLAUDE.md                   ← este archivo
```

---

## 🔐 Credenciales y entorno

| Variable | Uso |
|---|---|
| `GEMINI_API_KEY` | Google Gemini — LLM Text-to-SQL (1M tokens/día gratis) |
| `GROQ_API_KEY` | Groq fallback — LLM Text-to-SQL (100k tokens/día gratis) |
| `DB_HOST / DB_USER / DB_PASSWORD / DB_PORT` | MySQL RDS (requiere VPN) |
| `DB_NAME` | `fullclean_telemercadeo` (default) |

Cargar siempre con:
```python
from config.secrets_manager import load_env_secure
load_env_secure()
```

---

## 🤖 Módulo Vanna SQL — Estado actual

### Arquitectura

```
AtlasVanna
  ├── ChromaDB_VectorStore  (vanna.legacy.chromadb) — vector store en disco
  └── OpenAI_Chat           (vanna.legacy.openai)   — LLM vía API OpenAI-compat
```

`get_vanna()` detecta proveedor automáticamente: prioridad Gemini → Groq.

### Proveedor activo para desarrollo

**Groq** (`llama-3.3-70b-versatile`) — Gemini tiene un bug pendiente con el endpoint OpenAI-compatible que manda campos `null` en los mensajes (Gemini devuelve `400 INVALID_ARGUMENT`). El fix está implementado en `submit_prompt()` dentro de `AtlasVanna` pero no se ha podido validar por límite de tokens. Gemini está listo para retomar.

### Comandos

```bash
# Re-entrenar desde cero
rmdir /s /q agente\vanna_sql\chroma_store
python -m agente.vanna_sql.trainer --provider groq

# Solo validar (sin re-entrenar)
python -m agente.vanna_sql.trainer --validate-only --provider groq

# Forzar proveedor o modelo específico
python -m agente.vanna_sql.trainer --provider groq --model llama-3.3-70b-versatile
```

### Contenido del entrenamiento (trainer.py)

| Tipo | Cantidad | Descripción |
|---|---|---|
| DDL | 15 bloques | Tablas completas con comentarios de negocio |
| Documentación | 9 bloques | Reglas de negocio, filtros obligatorios, protocolo |
| SQL ejemplos | 20 pares | Pregunta + SQL orientados a listas de clientes |
| Preguntas de test | 20 | Con fechas reales ancladas a abril 2026 |

---

## 📐 Reglas de negocio críticas (para cualquier SQL)

1. **CO siempre obligatorio** — join: `contactos → ciudades.id_centroope → centroope.id`
2. **id_contacto como alias** — siempre `co.id AS id_contacto` en el SELECT
3. **Pedido válido = 5 filtros** — `estado_pedido=1, anulada=0, autorizar IN(1,2), autorizacion_descuento=0, tipo_documento<2`
4. **Contactabilidad real** — solo `llamadas_respuestas.contestada=1`; NUNCA `contactos.ultima_llamada`
5. **Cliente activo** — `estado_cxc IN (0,1)` OR `cant_obsequios > 0`
6. **Coordenadas** — mediana de `vwEventos` filtrada a bounds Colombia (`lat[-4.5,12.5] lon[-82,-66]`)

---

## 🗺️ Flujo completo del agente (visión objetivo)

```
Usuario (lenguaje natural)
        │
        ▼
  Atlas Agent (atlas_agent.py · Claude API)
        │
        ├─► [Tool: vanna_sql]  ──► AtlasVanna.generate_sql()
        │           │                    │
        │           │              ChromaDB RAG (DDL + docs + ejemplos)
        │           │                    │
        │           │              Groq / Gemini → SQL generado
        │           │                    │
        │           └──► MySQL RDS ──► DataFrame resultado
        │
        ├─► [Tool: generar_mapa]  → Folium map con clientes del DataFrame
        │
        └─► Respuesta al usuario con SQL + mapa embebido o link
```

---

## 📍 Fase actual de construcción — 29 abril 2026

### ✅ Completado

- [x] Clase `AtlasVanna` con multi-herencia ChromaDB + OpenAI_Chat
- [x] Auto-detección de proveedor (Gemini / Groq) por env vars
- [x] `connect_db=False` para training sin VPN
- [x] 15 DDL con todos los campos de `contactos` (~100 campos)
- [x] 9 bloques de reglas de negocio (CO, pedido válido, contactabilidad, coords, etc.)
- [x] 20 SQL examples orientados a segmentación de clientes para mapas
- [x] 20 preguntas de test con fechas reales (Q1 2026 + abril 2026)
- [x] `--provider` / `--model` / `--validate-only` en trainer CLI
- [x] Fix `submit_prompt()` para sanitizar nulls (Gemini compat)
- [x] `utils/generar_coordenadas_clientes.py` — extrae coords medianas por CO
- [x] `static/datos/coordenadas/` configurado en `.gitignore` (versionado)
- [x] `agente/vanna_sql/chroma_store/` gitignoreado

### 🔄 En progreso

- [ ] **Validación completa del trainer** — pendiente correr `--validate-only --provider groq` y revisar los 10 SQLs de test. Requiere que se haya re-entrenado con CO IDs corregidos y que el cupo diario Groq esté disponible.

### ✅ Completado (30 abril 2026)

- [x] **GROQ_API_KEY2 fallback** — `get_vanna()` acepta `_key_override`; `get_vanna_groq_key2()` fuerza KEY2. Fallback automático en el agente si KEY1 devuelve 429.
- [x] **Tool `generar_sql_vanna`** integrada en `atlas_agent.py`:
  - Acepta pregunta en lenguaje natural + ciudad
  - Llama a AtlasVanna.generate_sql()
  - Fallback automático a GROQ_API_KEY2 en rate limit
  - El SQL generado se pasa a consultar_clientes → generar_mapa_clientes (flujo existente)
- [x] **System prompt actualizado** con Variante A (SQL directo) y Variante B (Vanna RAG)

### ⏳ Siguiente fase

- [ ] **Re-entrenar con CO IDs corregidos** → `rmdir /s /q agente\vanna_sql\chroma_store` luego `python -m agente.vanna_sql.trainer --provider groq --no-validate`
- [ ] **Validar 10 preguntas de test** → `python -m agente.vanna_sql.trainer --validate-only --provider groq` (cuando cupo Groq disponible)
- [ ] **Ejecutar** `utils/generar_coordenadas_clientes.py` con VPN activa para generar CSVs de coordenadas base por CO
- [ ] **Verificar campo** `ultimo_obsequio` en `vwContactos`: `SHOW COLUMNS FROM fullclean_contactos.vwContactos LIKE '%obsequio%'`
- [ ] Commit y push a rama `produccion_2`

---

## ⚠️ Problemas conocidos y soluciones aplicadas

| Problema | Causa | Solución |
|---|---|---|
| `ModuleNotFoundError: vanna.groq` | vanna 2.0 reescribió la API | Usar `vanna.legacy.openai` + endpoint OpenAI-compat |
| MySQL al init sin VPN | `get_vanna()` conectaba siempre | Parámetro `connect_db=False` |
| `llama-3.1-70b-versatile` decommissioned | Groq retiró el modelo | Default cambiado a `llama-3.3-70b-versatile` |
| Rate limit 429 Groq KEY1 | 100k tokens/día, 20 validaciones juntas | Sleep 4s entre preguntas + retry 30s; fallback automático a GROQ_API_KEY2 en el agente |
| Rate limit 413 llama-3.1-8b | 6k TPM, prompt de 6,969 tokens | Cambio a modelos con más contexto |
| Gemini 400 `null` en mensajes | Vanna añade `function_call: None` etc. | Override `submit_prompt()` en `AtlasVanna` (fix aplicado, pendiente validar) |
| Verbose de Vanna en consola | Vanna imprime prompts completos | Override `log()` en `AtlasVanna` |

---

## 🔗 Tablas y esquemas relevantes

| Esquema | Tablas principales |
|---|---|
| `fullclean_contactos` | `contactos`, `vwContactos`, `vwEventos`, `ciudades`, `barrios`, `categorias` |
| `fullclean_telemercadeo` | `pedidos`, `pedidos_det`, `llamadas`, `llamadas_respuestas` |
| `fullclean_general` | `centroope` |
| `fullclean_bodega` | `items`, `productos`, `presentaciones` |
| `fullclean_personal` | `personal`, `cargos` |
