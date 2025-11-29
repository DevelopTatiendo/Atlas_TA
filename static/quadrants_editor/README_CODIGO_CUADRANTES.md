# Sistema de Códigos de Cuadrantes: PAP vs RUTA

## 1. Modo Excluyente (Toggle PAP/RUTA)

El editor funciona en **modo excluyente**: solo puedes trabajar con **PAP** o **RUTA** a la vez, seleccionado mediante el toggle en la barra de herramientas.

- **PAP**: Cuadrantes con código `CIU_pap_NNN` (contador secuencial de 3 dígitos, ej: `BR_pap_001`)
- **RUTA**: Cuadrantes con código `CIU_ruta_ID_NN` (ID de ruta + sufijo de 2 dígitos, ej: `BR_ruta_2701_01`)

El sistema **impide crear cuadrantes del tipo no seleccionado** en el toggle.

---

## 2. Generación de Códigos

### PAP
- **Patrón**: `{CIUDAD}_pap_{NÚMERO}`
- **Contador**: Global por ciudad (3 dígitos con ceros a la izquierda: `001`, `002`, etc.)
- **Ejemplo**: `BR_pap_001`, `BR_pap_002`, `BO_pap_123`
- **Propiedades almacenadas**:
  ```json
  {
    "system": "PAP",
    "city": "BR",
    "code": "BR_pap_001"
  }
  ```

### RUTA
- **Patrón**: `{CIUDAD}_ruta_{ID_RUTA}_{SUFIJO}`
- **ID de ruta**: Número único que identifica la ruta (ej: `2701`)
- **Sufijo**: Permite múltiples cuadrantes desconectados con la misma ruta (ej: `_01`, `_02`, `_03`)
- **Ejemplo**: `BR_ruta_2701_01`, `BR_ruta_2701_02`, `BO_ruta_1500_01`
- **Propiedades almacenadas**:
  ```json
  {
    "system": "RUTA",
    "city": "BR",
    "route_id": 2701,
    "route_name": "2 Barranquilla norte",
    "base_code": "BR_ruta_2701",
    "dup_index": 1,
    "code": "BR_ruta_2701_01"
  }
  ```

**El sufijo se calcula automáticamente** al crear un cuadrante: si ya existe `BR_ruta_2701_01`, el siguiente será `BR_ruta_2701_02`.

---

## 3. Validación de Códigos

### Expresiones Regulares
```javascript
RE_PAP  = /^[A-Z]{2,3}_pap_\d{3}$/    // Ej: BR_pap_001, MAN_pap_099
RE_RUTA = /^[A-Z]{2,3}_ruta_\d+(_\d{2})?$/  // Ej: BR_ruta_2701 o BR_ruta_2701_01
```

### Unicidad
- **PAP**: Cada número debe ser único por ciudad (no puede haber dos `BR_pap_001`)
- **RUTA**: Cada combinación `base + sufijo` debe ser única (pueden existir `BR_ruta_2701_01` y `BR_ruta_2701_02`, pero no dos `BR_ruta_2701_01`)

**La exportación falla si detecta códigos duplicados**.

---

## 4. Resolución de Conflictos en Importación

Al importar un GeoJSON, el sistema **detecta códigos duplicados** con los cuadrantes ya existentes en el editor y **reasigna sufijos automáticamente**:

### Ejemplo RUTA
Si importas un archivo con:
- `BR_ruta_2701` (sin sufijo)
- `BR_ruta_2701` (duplicado)

Y ya existe `BR_ruta_2701_01` en el editor, el sistema reasigna:
- Primer feature → `BR_ruta_2701_02`
- Segundo feature → `BR_ruta_2701_03`

### Ejemplo PAP
Si importas `BR_pap_005` y ya existe ese código, el sistema asigna el siguiente número global disponible (ej: `BR_pap_006`).

**Se muestra un toast informativo** con el número de conflictos resueltos y los detalles se imprimen en consola.

---

## 5. Edición de Código

Puedes editar el código de un cuadrante padre en dos momentos:

1. **Al guardar cambios de geometría**: Se abre un modal automáticamente
2. **Desde el botón "✏️ Editar código"**: Edita sin modificar geometría

### Restricciones en edición:
- **PAP**: Solo puedes editar el número (el prefijo `CIU_pap_` es readonly)
- **RUTA**: Puedes cambiar `ID_RUTA`, `nombre de ruta` y `sufijo` (se recalcula la base automáticamente)

El modal **valida unicidad en tiempo real** y **bloquea el guardado** si el código ya existe.

---

## Resumen de Flujo

```
┌──────────────────────────────────────┐
│  Toggle: Seleccionar PAP o RUTA      │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│  Crear cuadrante padre               │
│  → Modal asigna código automático    │
│  → PAP: próximo número global        │
│  → RUTA: base + próximo sufijo      │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│  Editar geometría y guardar          │
│  → Modal de edición de código        │
│  → Validar formato + unicidad        │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│  Exportar GeoJSON                    │
│  → Verificar unicidad final          │
│  → Error si hay duplicados           │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│  Importar GeoJSON                    │
│  → Detectar conflictos de código     │
│  → Reasignar sufijos automáticamente │
│  → Notificar cambios                 │
└──────────────────────────────────────┘
```

---

**Versión del editor**: 3.2  
**Última actualización**: 2025-01-21
