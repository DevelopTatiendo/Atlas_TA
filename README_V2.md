# Atlas TA — produccion_v2

Nace de `produccion_v1` con un objetivo central: convertir la herramienta de mapas en un **agente conversacional** capaz de analizar la operación de campo, generar reportes automáticos y responder preguntas sobre clientes y promotores en lenguaje natural.

---

## Qué hay nuevo en v2

### 1. Atlas Agent (`agente/`)

Un agente basado en Claude (API de Anthropic) que orquesta todas las capacidades del proyecto:

```
agente/
├── atlas_agent.py    # Agente Claude con tool_use — núcleo conversacional
├── herramientas.py   # 6 herramientas invocables por el agente
├── captura.py        # Screenshot de mapas Folium con Playwright
└── cli.py            # Interfaz de línea de comandos
```

### 2. Playwright — capturas de mapas

Los mapas Folium HTML ahora pueden convertirse en imágenes PNG automáticamente para incluir en reportes Word/Excel/PDF sin intervención manual.

### 3. Análisis batch de ciudades

Una sola llamada genera las métricas de las 7 ciudades en paralelo y las devuelve ordenadas por rendimiento.

---

## Arquitectura

```
CLI / API REST
      │
      ▼
AtlasAgent (Claude API + tool_use)
      │  Orquesta según necesidad
      ├─► consultar_metricas  ─► preprocesamiento_muestras.py ─► MySQL
      ├─► generar_mapa        ─► mapa_muestras.py ─► Folium HTML
      ├─► capturar_mapa       ─► Playwright Chromium ─► PNG
      ├─► comparar_ciudades   ─► batch 7 ciudades
      ├─► consultar_cliente   ─► MySQL (historial cliente)
      └─► listar_promotores   ─► MySQL
```

---

## Requisitos adicionales vs v1

```bash
pip install anthropic playwright
playwright install chromium
```

Y en `config/.env`:
```env
ANTHROPIC_API_KEY=sk-ant-...
```

---

## Cómo usar el agente

### Modo interactivo (chat)

```bash
python -m agente.cli
```

Ejemplo de sesión:
```
[Tú] ¿Cómo está Medellín esta semana?
[Atlas Agent] → consultar_metricas({"ciudad": "Medellín", ...})
[Atlas Agent] En Medellín esta semana operaron 9 promotores con un total de 2.417 clientes visitados.
La contactabilidad promedio es del 74% y la captación de nuevos clientes del 46%...

[Tú] Dame el mapa con captura para el reporte
[Atlas Agent] → generar_mapa({"ciudad": "Medellín", ...})
[Atlas Agent] → capturar_mapa({"html_path": "static/maps/..."})
[Atlas Agent] Mapa generado en static/maps/mapa_medellin_20260427.html
Captura guardada en static/maps/mapa_medellin_20260427.png (lista para incluir en reportes)

[Tú] ¿Cuántas veces visitaron a Juan García?
[Atlas Agent] → consultar_cliente({"nombre": "Juan García", "ciudad": "Medellín"})
[Atlas Agent] Encontré 3 registros para Juan García en Medellín...
```

### Pregunta única (para automatizaciones)

```bash
python -m agente.cli --pregunta "¿Qué ciudad tiene mejor captación este mes?"
```

### Batch (para reportes programados)

Crear un archivo `preguntas_lunes.txt`:
```
Dame el resumen nacional de esta semana
¿Qué promotores de Cali tienen conversión por debajo del 40%?
¿Cuáles son las 3 ciudades con mejor captación?
```

Luego:
```bash
python -m agente.cli --batch preguntas_lunes.txt --salida reporte_lunes.txt
```

---

## Herramientas disponibles para el agente

| Herramienta | Qué hace | Fuente |
|-------------|----------|--------|
| `consultar_metricas` | Métricas por promotor (contactabilidad, captación, conversión) | BD MySQL |
| `generar_mapa` | HTML interactivo Folium para una ciudad | mapa_muestras.py |
| `capturar_mapa` | Screenshot PNG del mapa generado | Playwright |
| `comparar_ciudades` | Tabla de las 7 ciudades en un período | BD MySQL (batch) |
| `consultar_cliente` | Historial de muestras, llamadas y ventas de un cliente | BD MySQL |
| `listar_promotores_activos` | Promotores que operaron en un período | BD MySQL |

---

## Flujo de reporte automático (visión)

```
Cada lunes 7am (scheduler):
  1. comparar_ciudades(semana_pasada)
  2. generar_mapa(cada ciudad con alerta)
  3. capturar_mapa(cada HTML)
  4. Generar Word con imágenes y tabla nacional
  5. Guardar en carpeta compartida
```

---

## Tareas pendientes en v2

### Inmediatas
- [ ] Integrar captura PNG en la generación del reporte Word
- [ ] Endpoint Flask `/agente/preguntar` para consumir desde otras herramientas
- [ ] Prueba end-to-end del flujo completo (pregunta → métricas → mapa → PNG → Word)

### Siguientes
- [ ] Reporte semanal automático programado (skill de Schedule)
- [ ] Alertas: notificar si algún promotor cae por debajo de umbrales definidos
- [ ] Dashboard comparativo de ciudades como artifact persistente
- [ ] Validación de % Conversión contra sistema real de pedidos

---

## Variables de entorno (completo)

```env
# BD
DB_HOST=<host>
DB_PORT=3306
DB_USER=serpibi
DB_PASSWORD=<password>
DB_NAME=fullclean_contactos

# Servidores
FLASK_SERVER_URL=http://localhost:5000
ENVIRONMENT=development

# Agente
ANTHROPIC_API_KEY=sk-ant-...

# Producción (encriptado)
MAPAS_SECRET_PASSPHRASE=<passphrase>
```
