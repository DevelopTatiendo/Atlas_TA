 # Atlas TA — Rama de Producción


Herramienta de análisis geoespacial para operaciones de campo de T Atiendo S.A.
Visualiza en mapa interactivo todas las muestras entregadas por promotores, con métricas reales de contactabilidad y conversión post-muestra.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────┐
│  app.py  (Streamlit — interfaz de usuario)          │
│    puerto 8501 (dev) / configurado en producción    │
├─────────────────────────────────────────────────────┤
│  flask_server.py  (Flask — sirve HTML de mapas)     │
│    puerto 5000 (dev y producción)                   │
├─────────────────────────────────────────────────────┤
│  MySQL  (fullclean_contactos + fullclean_telemercadeo)│
│    usuario: serpibi  (solo SELECT)                  │
└─────────────────────────────────────────────────────┘
```

Los dos procesos deben correr simultáneamente. Streamlit genera el mapa Folium (HTML estático) y Flask lo sirve para que se abra en una nueva pestaña del navegador.

---

## Estructura de archivos

```
Atlas_TA/
├── app.py                          # Streamlit — entrada principal
├── flask_server.py                 # Flask — servidor de mapas HTML
├── mapa_muestras.py                # Lógica de generación del mapa y métricas
├── mapa_consultores.py             # Mapa de consultores (flujo legacy)
├── config/
│   ├── secrets_manager.py          # Carga segura de variables de entorno
│   ├── .env                        # Variables en texto plano (desarrollo)
│   └── .env.enc                    # Variables cifradas (producción)
├── pre_procesamiento/
│   ├── preprocesamiento_muestras.py      # Consultas SQL + normalización + contactabilidad
│   ├── metricas_areas.py           # Cálculo de áreas geoespaciales (joblib)
│   └── db_utils.py                 # Helper de lectura SQL (SQLAlchemy)
├── utils/
│   ├── gestor_mapas.py             # Guardado controlado de HTMLs
│   └── spatial_ops.py              # Operaciones geoespaciales auxiliares
├── static/
│   └── maps/                       # HTMLs de mapas generados (salida)
├── geojson/
│   └── rutas/                      # GeoJSONs de cuadrantes por ciudad
│       ├── cali/
│       ├── medellin/
│       ├── barranquilla/
│       └── ...
└── requirements.txt
```

---

## Cómo correr en desarrollo (local)

### 1. Requisitos previos

```bash
pip install -r requirements.txt
```

Python 3.10+. Las dependencias críticas son:
`streamlit`, `folium`, `pandas`, `geopandas`, `scikit-learn`, `shapely`,
`mysql-connector-python`, `SQLAlchemy`, `h3`, `joblib`, `cryptography`.

### 2. Variables de entorno

Crear `config/.env` con:

```env
DB_HOST=<host_mysql>
DB_PORT=3306
DB_USER=serpibi
DB_PASSWORD=<password>
DB_NAME=fullclean_contactos
FLASK_SERVER_URL=http://localhost:5000
ENVIRONMENT=development
```

### 3. Iniciar Flask (servidor de mapas)

```bash
python flask_server.py
```

Queda escuchando en `http://localhost:5000`. Sirve los HTMLs desde `static/maps/`.

### 4. Iniciar Streamlit (interfaz)

```bash
streamlit run app.py
```

Abre automáticamente `http://localhost:8501`.

### 5. Flujo de uso

1. Seleccionar ciudad en el sidebar
2. Elegir rango de fechas y agrupación (Promotor o Mes)
3. Clic en **Generar Mapa**
4. El mapa se abre en nueva pestaña automáticamente
5. Descargar HTML o CSV desde los botones de descarga

---

## Cómo debe funcionar en producción

### Variables de entorno requeridas

```env
ENVIRONMENT=production
FLASK_SERVER_URL=https://<dominio-o-ip-del-servidor>:5000
MAPAS_SECRET_PASSPHRASE=<passphrase-para-descifrar-.env.enc>
```

En producción se usa `config/.env.enc` (cifrado con `cryptography`). La passphrase se inyecta como variable de entorno del sistema, nunca en código.

### Procesos a mantener activos

| Proceso | Comando | Puerto |
|---------|---------|--------|
| Flask | `python flask_server.py` | 5000 |
| Streamlit | `streamlit run app.py --server.port 8501` | 8501 |

Se recomienda usar `supervisor`, `systemd` o `pm2` para mantener ambos procesos vivos.

### Consideraciones de seguridad

- El usuario `serpibi` tiene permisos **solo SELECT** — no puede modificar la BD.
- Los HTMLs generados son archivos estáticos temporales. Se recomienda limpiar `static/maps/` periódicamente.
- `flask_server.py` incluye CORS habilitado — restringir a los orígenes necesarios en producción.

---

## Qué entrega la herramienta

### 1. Mapa interactivo (HTML)

Archivo HTML auto-contenido que incluye:

- **Todos los puntos GPS** de cada muestra entregada por cada promotor en el período (incluyendo re-visitas al mismo cliente).
- **Capas activables/desactivables** por promotor o por mes desde el panel lateral.
  - Desactivar `PROMOTORES` apaga todos los sub-grupos a la vez.
  - Activar un promotor individual reactiva automáticamente el grupo padre.
- **Cuadrantes (padres e hijos)** con área, muestras y días de operación en el popup.
- **Tabla de métricas** fija en la esquina inferior izquierda, ordenable por cualquier columna.
- **Resumen flotante** con total de clientes y promedio diario.

### 2. CSV de operación

Exporta el `df_filtrado` (cliente único por promotor, última muestra) con todas las columnas incluyendo flags de contactabilidad (`es_contactable`, `es_venta`). Útil para análisis en Excel o cruzar con otras fuentes.

### 3. Tabla de métricas en pantalla

Visible directamente en Streamlit antes de abrir el mapa. Mismas columnas que la leyenda del mapa.

---

## Cómo analizar los resultados

### Panel de capas (mapa)

El mapa muestra **todas las muestras** (no solo la última por cliente). Esto permite ver:
- Zonas donde un cliente fue visitado varias veces → posible re-trabajo.
- Densidad real de cobertura por promotor.
- Solapamiento entre territorios de distintos promotores.

Para comparar promotores específicos:
1. Desmarcar `PROMOTORES` → mapa limpio.
2. Marcar solo los promotores a comparar.

### Métricas de la leyenda — columna por columna

| Columna | Pregunta que responde |
|---------|----------------------|
| **#Muestras** | ¿Cuántas visitas hizo en total (con re-visitas)? |
| **#Clientes** | ¿Cuántas personas distintas visitó? |
| **Área km²** | ¿Qué tan grande es su territorio? |
| **Clientes/km²** | ¿Qué tan eficiente es su recorrido? |
| **Clientes/día hábil** | ¿Cuántos clientes visita en un día normal? |
| **% No fieles** | ¿Qué % de su cartera son clientes nuevos/potenciales? |
| **% Contactabilidad** | De todos los visitados, ¿cuántos contestaron después de la muestra? |
| **% Contactab. No Fieles** | De los clientes nuevos, ¿cuántos contestaron? |
| **% Captación** | De toda su cartera, ¿cuántos son nuevos Y contestaron? (captación efectiva) |
| **% Conversión** | De los que contestaron, ¿cuántos compraron? |

### Regla de atribución temporal

Una llamada solo se atribuye a un promotor si:
- La llamada ocurrió **después** de la fecha en que ese promotor entregó la muestra.
- La llamada fue contestada (`contestada = 1` o `contacto_exitoso = 1`).
- Para conversión: la llamada tiene `es_venta = 1`.

Esto garantiza que el mérito de la captación y la venta se atribuye a quien entregó la muestra.

### Lectura del embudo (ejemplo)

```
Visitados (2.985)
  └── Nuevos/No fieles (51,6% → ~1.541)
  └── Contactados total (74,2% → ~2.215)
       └── Nuevos contactados — Captación (27,6% → ~824)
       └── Con venta — Conversión (51,9% → ~1.149 de los contactados)
```

---

## Ciudades disponibles

| Ciudad | id_centroope |
|--------|-------------|
| Cali | 2 |
| Medellín | 3 |
| Bogotá | 4 |
| Pereira | 5 |
| Manizales | 6 |
| Bucaramanga | 7 |
| Barranquilla | 8 |

---

## Flujo interno de datos

```
consultar_db()          → SQL sobre vwEventos + vwContactos + ciudades
      ↓
crear_df()              → normalización de tipos y columnas
      ↓
filtro ≥ 3 muestras     → excluir promotores con muy poco volumen
      ↓
dedup (df_filtrado)     → cliente único por promotor (última muestra)
      ↓
consultar_llamadas_raw()→ llamadas post-muestra desde fullclean_telemercadeo
      ↓
aplicar_contactabilidad_temporal() → filtra fecha_llamada > fecha_muestra
      ↓
_calcular_metricas_agrupadas()     → porcentajes de la leyenda
      ↓
areas_muestras_resumen()           → áreas por KMeans + casco cóncavo (joblib)
      ↓
generar_mapa_muestras_visual()     → Folium HTML con df_original (todos los puntos)
```

---

## Caché

Las consultas SQL más costosas están cacheadas con `@st.cache_data(ttl=1800)`:

| Función | TTL | Qué cachea |
|---------|-----|-----------|
| `consultar_db` | 30 min | Muestras crudas por ciudad/período |
| `consultar_llamadas_raw` | 30 min | Llamadas post-muestra por ids_contacto |
| `listar_promotores` | 30 min | Lista de promotores activos |

El caché se invalida automáticamente al cambiar ciudad (limpieza de `session_state`).

---

## Tareas pendientes por prioridad

### Alta — funcional / datos

1. **Validar % Conversión contra sistema de pedidos**
   El campo `es_venta=1` en `llamadas_respuestas` es tipificación del agente, no confirmación de pedido real. Cruzar al menos una muestra histórica contra órdenes reales para calibrar la métrica.



### Media — experiencia de uso



6. **Descarga de métricas !!!!**
   El CSV actual es separado por `;`. Agregar opción de descarga `.xlsx` con formato (colores, anchos de columna) para entrega directa a coordinadores.

7. **Persistencia del mapa entre reruns**
   El caché de Streamlit guarda los datos pero el mapa se regenera en cada submit. Evaluar guardar el HTML con hash de parámetros para evitar regenerar si los datos no cambiaron.

8. **Manejo de zonas sin GeoJSON**
   Si el archivo GeoJSON de una ciudad está vacío o malformado, el mapa carga sin cuadrantes sin aviso. Agregar validación y mensaje claro al usuario.

### Baja — mejoras futuras

9. **Modo auditoría**
   El parámetro `auditoria=True` en `generar_mapa_muestras_visual` está presente pero no implementado. Podría usarse para mostrar metadatos de calidad (% puntos fuera de cuadrante, % sin barrio asignado).

10. **Exportar mapa como imagen**
    Para incluir en reportes de PowerPoint. Requiere `selenium` o `playwright` para captura headless del HTML Folium.

11. **Dashboard comparativo entre ciudades**
    Vista agregada que muestre las métricas de las 7 ciudades en una sola tabla para benchmarking nacional.
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               