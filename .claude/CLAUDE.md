# CLAUDE.md — Atlas TA · Contexto del Proyecto

> Archivo de contexto persistente para sesiones de IA (Cowork / Claude Code).
> Actualizar cuando cambien decisiones de arquitectura, estructura o convenciones.

---

## ¿Qué es este proyecto?

**Atlas TA** es una aplicación Streamlit + Flask para T Atiendo S.A.
Visualiza la operación de campo de promotores que entregan muestras físicas
en 7 ciudades de Colombia: Cali, Medellín, Bogotá, Pereira, Manizales,
Bucaramanga y Barranquilla.

Cruza datos de visitas (vwEventos), pedidos, llamadas, quejas y territorios
para detectar oportunidades y alertas de cobertura.

---

## Estructura del proyecto

```
Atlas_TA/
├── app.py                        # Entry point Streamlit
├── flask_server.py               # Servidor Flask para servir mapas HTML
├── config/
│   ├── secrets_manager.py        # Gestión de secretos (SIEMPRE usa .enc)
│   ├── .env.enc                  # Credenciales cifradas con Fernet+PBKDF2HMAC
│   └── .env.example              # Plantilla de variables (sin valores reales)
├── pre_procesamiento/
│   └── db_utils.py               # sql_read() — acceso de solo lectura a MySQL
├── agente/                       # Atlas Agent (rama produccion_v2)
│   ├── atlas_agent.py            # Clase AtlasAgent, tools, dispatcher
│   ├── atlas_chat.py             # Componente Streamlit del chat
│   ├── schema_context.py         # Schema de BD inyectado en system prompt
│   ├── kpi_auto.py               # KPIs automáticos por tipo de columna
│   ├── map_renderer.py           # 4 tipos de mapa Folium (CircleMarker, heatmap...)
│   ├── coord_cache_parquet.py    # Cache Parquet de coordenadas de clientes
│   ├── mapa_ejecutor.py          # Ejecutor de código Folium dinámico (tool)
│   └── herramientas.py           # Tools: métricas, rutas, análisis zona
├── geojson/                      # Polígonos de comunas por ciudad
├── static/
│   ├── maps/                     # Mapas HTML generados (gitignoreados)
│   └── datos/                    # Parquet de coordenadas (gitignoreados, PII)
└── .claude/
    └── CLAUDE.md                 # Este archivo
```

---

## Base de datos

- **Motor:** MySQL (solo lectura — SELECT/SHOW únicamente)
- **Schema principal:** `fullclean_contactos`
- **Schema secundario:** `fullclean_telemercadeo`
- **Conexión:** SQLAlchemy 2.x via `pre_procesamiento/db_utils.py`
- **Credenciales:** cifradas en `config/.env.enc`, nunca en texto plano

### Tablas clave

| Tabla | Descripción |
|-------|-------------|
| `contactos` | Clientes (id, nombre, barrio, estado, cant_obsequios) |
| `vwEventos` | Visitas/muestras de promotores con GPS (coordenada_latitud/lon) |
| `barrios` | Barrios con id_ciudad |
| `ciudades` | id, id_centroope (2=Cali, 3=Medellín, 4=Bogotá, 5=Pereira, 6=Manizales, 7=Bucaramanga, 8=Barranquilla) |
| `llamadas_respuestas` | Llamadas post-muestra (contestada=1 = contactabilidad real) |
| `pedidos_muestra` | Pedidos generados (es_venta=1 = conversión) |
| `rutas_cobro` | Rutas con nombre y ciudad |

### Gotchas críticos de BD

- `vwEventos.coordenada_latitud` y `coordenada_longitud` son **VARCHAR** — siempre hacer `CAST(... AS DECIMAL(10,6))`
- Coordenadas vacías vienen como `''` o `'0'`, no como NULL
- Bounds válidos Colombia: lat [-4.5, 12.5], lon [-82.0, -66.0]
- `contactos.ultima_llamada` NO mide contactabilidad — usar `llamadas_respuestas.contestada = 1`
- No-fiel: `id_categoria NOT IN (42, 55, 58, 59, 60)`

---

## Seguridad — Regla de oro

**Nunca usar `.env` plano.** El proyecto usa cifrado Fernet + PBKDF2HMAC.

```python
# CORRECTO — única forma de cargar credenciales
from config.secrets_manager import load_env_secure
load_env_secure()  # Lee config/.env.enc, usa MAPAS_SECRET_PASSPHRASE del OS

# INCORRECTO — prohibido en cualquier archivo
from dotenv import load_dotenv
load_dotenv()   # ← jamás
```

### Setup local para desarrolladores

```bash
# 1. Agregar al shell profile (~/.zshrc o ~/.bashrc) — UNA SOLA VEZ
export MAPAS_SECRET_PASSPHRASE="la_passphrase_del_equipo"

# 2. Verificar que config/.env.enc existe (viene en el repo)
# 3. Correr la app normalmente — no se necesita .env
streamlit run app.py
```

### Setup en producción (servidor)

```bash
# Variable de entorno a nivel de sistema / Docker / CI
MAPAS_SECRET_PASSPHRASE=xxxx streamlit run app.py
# o en docker-compose.yml / .env del servidor (fuera del repo)
```

---

## Ramas activas

| Rama | Estado | Propósito |
|------|--------|-----------|
| `main` | ✅ Activa | Base estable, referencia de producción |
| `produccion_v1` | ✅ Estable | Versión desplegada actual |
| `produccion_v2` | 🚧 Desarrollo | Atlas Agent + cache coords + KPIs |
| `new_main` | 🔄 Sync | Mirror de main con ajustes menores |
| `aux_prod_v2*` | ⚠️ Obsoletas | No usar — features absorbidas |
| `modo_auditoria` | ⚠️ Congelada | Feature branch antigua |
| `temporal_local` | ⚠️ Experimental | No mergear a main |

---

## Atlas Agent (produccion_v2)

### Flujo de mapas

```
Usuario: "clientes con deuda > 50K en Medellín, mora 30-180d"
  → Validación: ①ciudad ②temporal ③filtro — si falta alguno, pide ejemplos
  → consultar_clientes(sql, ciudad_id) → KPIs automáticos + muestra
  → Si n_con_coords > 0 → generar_mapa_clientes(mismo_sql, tipo, campo_valor)
  → UI renderiza KPI cards + botón "Ver Mapa"
```

### Tipos de mapa disponibles

| Tipo | Uso | Parámetro requerido |
|------|-----|---------------------|
| `clusters` | Agrupación general | ninguno |
| `circulos_proporcionales` | Valor monetario por cliente | `campo_valor` |
| `puntos_bicolor` | Visitado/no visitado | `campo_color` (0/1) |
| `heatmap` | Densidad de actividad | `campo_valor` (peso, opcional) |

### Cache de coordenadas

- Archivo: `static/datos/coords_co_{id_centroope}.parquet`
- Columnas: `id_contacto`, `lat`, `lon`, `n_eventos_con_coords`
- Centroide de todos los eventos GPS válidos por cliente
- Filtro: `estado=1`, `estado_cxc IN(0,1)`, `cant_obsequios > 0` ó `ultimo_obsequio != ''`
- Reconstruir: `python -m agente.coord_cache_parquet --ciudad 3`
- **Gitignoreado** (PII + 8MB)

### Modelo LLM

- `claude-sonnet-4-6` via API de Anthropic
- `max_tokens=2500`
- System prompt: validación + schema context + reglas de formato

---

## KPI Auto-detección (kpi_auto.py)

Orden de evaluación por columna (importa el orden para evitar falsos positivos):

1. **_EXCLUIR**: id_contacto, lat, lon, nombre, barrio, ruta... → siempre omitir
2. **_CONTEOS** (`cant_*`, `n_*`, `num_*`, `total_*`) → enteros, primero que monetarias
3. **_MONETARIAS** (`deuda`, `saldo`, `valor`, `monto`...) → solo si mean >= 100
4. **_FLAGS** (`visitado`, `activo`, `es_*`...) → 0/1 proporciones
5. **_FECHAS edad** (`ultima_*`, `dias_*`, `edad_*`) → días, solo si mean < 1e9 (guard epoch)
6. **_FECHAS datetime** (`fecha_*`) → min/max de fechas

---

## Convenciones de código

- Imports de BD siempre: `from pre_procesamiento.db_utils import sql_read`
- Carga de env siempre: `from config.secrets_manager import load_env_secure; load_env_secure()`
- Mapas: `folium.CircleMarker` (píxeles) — nunca `folium.Circle` (metros)
- Tiles: siempre Esri `World_Street_Map`
- Centro de mapa: siempre `ciudad_id` del sidebar, no centroide de datos

---

## Variables de entorno requeridas

| Variable | Dónde | Descripción |
|----------|-------|-------------|
| `MAPAS_SECRET_PASSPHRASE` | OS / servidor | Passphrase para descifrar `.env.enc` |
| `DB_HOST` | `.env.enc` | Host MySQL |
| `DB_PORT` | `.env.enc` | Puerto (3306) |
| `DB_USER` | `.env.enc` | Usuario BD |
| `DB_PASSWORD` | `.env.enc` | Contraseña BD |
| `DB_NAME` | `.env.enc` | Base de datos |
| `ANTHROPIC_API_KEY` | `.env.enc` | API key para Atlas Agent |
| `FLASK_SERVER_URL` | `.env.enc` | URL del servidor Flask (default: localhost:5000) |

---

## Próximos pasos (backlog técnico)

- [ ] Agente SQL local con Vanna AI + Groq (Text-to-SQL sin costo por token)
- [ ] Chat multi-turno con validación progresiva (preguntar lo que falta, no bloquear)
- [ ] Cache de tiempos de viaje implícitos (grafo barrio→barrio desde vwEventos)
- [ ] "Ciudad simulada" — matriz de tiempos operacionales reales
