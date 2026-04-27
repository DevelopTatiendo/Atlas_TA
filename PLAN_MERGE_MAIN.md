# Plan de Merge: produccion_v1 → main

## Contexto

La rama `produccion_v1` tiene 6 commits sobre `main` con mejoras que se dividen en dos grupos:

- **Mejoras universales** — deben subir a `main` para que todos los flujos se beneficien.
- **Cambios exclusivos de producción** — configuración, UI simplificada, documentación de despliegue.

---

## Qué va a main vs qué se queda en produccion_v1

### ✅ Va a main (mejoras del proyecto)

| Archivo | Qué aporta |
|---------|-----------|
| `mapa_muestras.py` | Paralelo con `joblib`, cascade JS bidireccional, métricas completas (`% Captación`, `% Conversión` temporal), `itertuples` + tooltip (rendimiento), `CATEGORIAS_FIELES` corregido, `MIN_MUESTRAS_PROMOTOR`, todos los puntos en el mapa (`df_original`) |
| `pre_procesamiento/preprocesamiento_muestras.py` | `consultar_llamadas_raw` + `aplicar_contactabilidad_temporal` (atribución temporal correcta) |
| `pre_procesamiento/metricas_areas.py` | Paralelización de cálculo de áreas con `joblib.Parallel` |
| `utils/agentes_utils.py` | Refactor para usar `db_utils.sql_read` (menos boilerplate de conexión) |
| `requirements.txt` | Dependencias nuevas: `h3`, `joblib`, actualización de versiones |
| Renombres de archivos | `new_mapa_muestras.py` → `mapa_muestras.py`, `new_preprocesamiento_muestras.py` → `preprocesamiento_muestras.py` |

### 🔒 Se queda en produccion_v1 (exclusivo de producción)

| Archivo | Por qué no va a main |
|---------|---------------------|
| `app.py` | Versión de prod tiene la UI limpia (solo flujo Muestras). `main` tiene todos los flujos (consultores, auditoría, etc.). Hacer merge completo rompería `main`. |
| `README_PRODUCCION.md` | Documentación específica del servidor de producción. No aplica en `main`. |
| `config/.env.enc` | Credenciales cifradas de producción. Nunca va a control de versiones general. |

---

## Paso a paso del merge

### Paso 1 — Reparar el índice Git (necesario antes de hacer cualquier commit)

El índice de Git quedó corrupto. Hay que reconstruirlo desde Windows:

```bash
# En la terminal del repositorio (Windows)
git fsck --full
git gc --aggressive --prune=now
# Si persiste el error "bad signature":
rm .git/index
git reset HEAD
```

### Paso 2 — Registrar los renombres en produccion_v1

Una vez que el índice esté sano, registrar los archivos renombrados en un commit limpio:

```bash
git checkout produccion_v1
git add mapa_muestras.py
git add pre_procesamiento/preprocesamiento_muestras.py
git rm --cached new_mapa_muestras.py          # si Git todavía lo ve
git rm --cached pre_procesamiento/new_preprocesamiento_muestras.py
git commit -m "rename: new_mapa/new_preprocesamiento → mapa_muestras/preprocesamiento_muestras"
```

### Paso 3 — Crear la rama de integración desde main

No se hace merge directo de `produccion_v1` a `main` porque `app.py` tiene cambios de producción que no deben entrar. En su lugar se crea una rama limpia:

```bash
git checkout main
git checkout -b feature/metricas-rendimiento-muestras
```

### Paso 4 — Copiar los archivos que SÍ van a main

Desde la rama `feature/metricas-rendimiento-muestras`, traer los archivos seleccionados de `produccion_v1` con `checkout`:

```bash
# Archivos del módulo de muestras (renombrados)
git checkout produccion_v1 -- mapa_muestras.py
git checkout produccion_v1 -- pre_procesamiento/preprocesamiento_muestras.py
git checkout produccion_v1 -- pre_procesamiento/metricas_areas.py
git checkout produccion_v1 -- utils/agentes_utils.py
git checkout produccion_v1 -- requirements.txt
```

> **Nota**: NO hacer `git checkout produccion_v1 -- app.py`.
> El `app.py` de `main` tiene los flujos de consultores, auditoría, etc. que no existen en producción. Hay que actualizar `app.py` en main manualmente (solo el import que cambió).

### Paso 5 — Actualizar app.py en main manualmente

En `main`, el único cambio necesario en `app.py` es el rename del import:

```python
# Buscar esta línea:
from new_mapa_muestras import generar_mapa_muestras_visual

# Reemplazar por:
from mapa_muestras import generar_mapa_muestras_visual
```

Adicionalmente, si `main` todavía importa `consultar_contactabilidad` (la función vieja), hay que actualizarlo:

```python
# Viejo (en new_preprocesamiento_muestras o similar):
from pre_procesamiento.new_preprocesamiento_muestras import consultar_contactabilidad

# Nuevo:
from pre_procesamiento.preprocesamiento_muestras import (
    consultar_llamadas_raw,
    aplicar_contactabilidad_temporal,
)
```

Y en cualquier sitio dentro de `main` que llame a `consultar_contactabilidad(...)`, reemplazar por el nuevo flujo de dos pasos:

```python
df_llamadas_raw = consultar_llamadas_raw(ids_contacto=ids_contacto, ...)
df_filtrado = aplicar_contactabilidad_temporal(df_filtrado, df_llamadas_raw)
```

### Paso 6 — Borrar los archivos con nombre viejo en main

Si `main` todavía tiene los archivos `new_*`:

```bash
git rm new_mapa_muestras.py
git rm pre_procesamiento/new_preprocesamiento_muestras.py
```

### Paso 7 — Commit y PR

```bash
git add -A
git commit -m "feat: metricas temporales, paralelo joblib, cascade JS, rename archivos muestras

- mapa_muestras.py: % Captación + % Conversión con atribución temporal
- mapa_muestras.py: cascade JS bidireccional para control de capas
- mapa_muestras.py: todos los puntos en mapa (df_original), itertuples perf
- preprocesamiento_muestras.py: consultar_llamadas_raw + aplicar_contactabilidad_temporal
- metricas_areas.py: paralelización con joblib
- rename: new_mapa_muestras → mapa_muestras, new_preprocesamiento → preprocesamiento_muestras"

git push origin feature/metricas-rendimiento-muestras
# Abrir PR: feature/metricas-rendimiento-muestras → main
```

### Paso 8 — Sincronizar produccion_v1 con main post-merge

Una vez aprobado el PR y mergeado a `main`:

```bash
git checkout produccion_v1
git rebase main        # o git merge main
# Resolver conflictos solo en app.py si los hay
# produccion_v1 conserva su app.py limpio de producción
git push origin produccion_v1 --force-with-lease
```

---

## Resumen visual del flujo

```
main (estado actual)
  └── feature/metricas-rendimiento-muestras   ← rama de integración
        ├── mapa_muestras.py         (de produccion_v1)
        ├── preprocesamiento_muestras.py (de produccion_v1)
        ├── metricas_areas.py        (de produccion_v1)
        ├── agentes_utils.py         (de produccion_v1)
        ├── requirements.txt         (de produccion_v1)
        └── app.py                   (de main, solo import actualizado)
              ↓  PR aprobado
main ← merge
              ↓
produccion_v1 ← rebase sobre main (conserva app.py de producción)
```

---

## Checklist de validación post-merge

- [ ] `python -c "from mapa_muestras import generar_mapa_muestras_visual; print('OK')"` sin errores
- [ ] `python -c "from pre_procesamiento.preprocesamiento_muestras import consultar_db; print('OK')"` sin errores
- [ ] Streamlit corre en `main` sin ImportError
- [ ] Mapa genera con las 10 columnas de métricas (incluyendo % Captación y % Conversión)
- [ ] Cascade JS funciona: desactivar PROMOTORES apaga todos; activar uno enciende el padre
- [ ] `new_mapa_muestras.py` y `new_preprocesamiento_muestras.py` eliminados del repo
- [ ] `produccion_v1` sigue levantando correctamente con su `app.py` de producción

---

## Nota sobre el archivo huérfano

`pre_procesamiento/new_preprocesamiento_muestras.py` no pudo ser eliminado por permisos del SO en esta sesión. Eliminarlo manualmente desde Windows antes del commit del Paso 2:

```bash
del pre_procesamiento\new_preprocesamiento_muestras.py
git rm --cached pre_procesamiento/new_preprocesamiento_muestras.py
```
