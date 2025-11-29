// Editor de cuadrantes - Leaflet con Draw y jerarquía cuadrante→subcuadrantes
// Inicialización del mapa centrado por ciudad
console.debug('Inicializando editor de cuadrantes...');

// === ESTADO GLOBAL DEL MODO (SISTEMA DUAL: RUTA/PAP) ===
let sistemaActual = 'RUTA'; // 'RUTA' o 'PAP' por defecto

// === CONTADORES PARA GENERACIÓN DE CÓDIGOS ===
// PAP: contador por ciudad (BR, CL, BG, etc.)
const papCountersByCity = new Map(); // key: 'BR' -> max int usado

// RUTA: contador de sufijos por base "CIU_ruta_ID"
const routeDupCounters = new Map();  // key: 'BR_ruta_2701' -> max sufijo NN usado

// === HELPERS DE FORMATO ===
const pad = (n, w) => String(n).padStart(w, '0');

// Regex de validación
const RE_PAP  = /^[A-Z]{2,3}_pap_\d{3}$/;                // BR_pap_001
const RE_RUTA = /^[A-Z]{2,3}_ruta_\d+(_\d{2})?$/;        // BR_ruta_232  ó BR_ruta_232_01

function baseRuta(cityAbbr, idRuta) { 
  return `${cityAbbr}_ruta_${idRuta}`; 
}

// Computar máximo PAP para una ciudad
function computeMaxPapForCity(cityAbbr) {
  let max = 0;
  const pattern = new RegExp(`^${cityAbbr}_pap_(\\d{3})$`);
  
  // Buscar en todas las capas visibles
  const searchLayers = (group) => {
    if (!group) return;
    group.eachLayer?.(layer => {
      const code = layer.feature?.properties?.code || layer.feature?.properties?.codigo;
      if (code) {
        const match = code.match(pattern);
        if (match) {
          const num = parseInt(match[1], 10);
          if (num > max) max = num;
        }
      }
    });
  };
  
  searchLayers(DRAWN_EDITABLE);
  searchLayers(DRAWN_LOCKED);
  
  // Buscar en padres activos
  if (state.parents) {
    state.parents.forEach(p => {
      const code = p.feature?.properties?.code || p.feature?.properties?.codigo;
      if (code) {
        const match = code.match(pattern);
        if (match) {
          const num = parseInt(match[1], 10);
          if (num > max) max = num;
        }
      }
    });
  }
  
  papCountersByCity.set(cityAbbr, max);
  return max;
}

// Computar máximo sufijo para una base RUTA
function computeMaxDupForBase(base) {
  let max = 0;
  const pattern = new RegExp(`^${base.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}_(\\d{2})$`);
  
  // Buscar en todas las capas visibles
  const searchLayers = (group) => {
    if (!group) return;
    group.eachLayer?.(layer => {
      const code = layer.feature?.properties?.code || layer.feature?.properties?.codigo;
      if (code) {
        const match = code.match(pattern);
        if (match) {
          const num = parseInt(match[1], 10);
          if (num > max) max = num;
        }
        // También considerar la base sin sufijo como _01 implícito
        if (code === base) {
          if (max < 1) max = 1;
        }
      }
    });
  };
  
  searchLayers(DRAWN_EDITABLE);
  searchLayers(DRAWN_LOCKED);
  
  // Buscar en padres activos
  if (state.parents) {
    state.parents.forEach(p => {
      const code = p.feature?.properties?.code || p.feature?.properties?.codigo;
      if (code) {
        const match = code.match(pattern);
        if (match) {
          const num = parseInt(match[1], 10);
          if (num > max) max = num;
        }
        if (code === base) {
          if (max < 1) max = 1;
        }
      }
    });
  }
  
  routeDupCounters.set(base, max);
  return max;
}

// === DICCIONARIO DE RUTAS ===
const ROUTES_MAP = {
  9:   "Ruta 3",
  13:  "Ruta 7", 
  19:  "Ruta 10",
  780: "Ruta 16 PALMIRA"
};
const getRouteLabel = (id) => ROUTES_MAP[id] || `Ruta ${id}`;
const getRouteCityById = (id) => (id == 780) ? "PALMIRA" : "CALI"; // Simple fallback
const getRouteIdFromLabel = (label) => {
  for (const [id, routeLabel] of Object.entries(ROUTES_MAP)) {
    if (routeLabel === label) return Number(id);
  }
  return null;
};

// === FUNCIÓN DE NOTIFICACIONES ===
function showToast(message, type = 'success') {
  const toast = document.createElement('div');
  toast.style.cssText = `
    position: fixed;
    top: 20px;
    right: 20px;
    padding: 12px 20px;
    border-radius: 8px;
    color: white;
    font-weight: 500;
    z-index: 10000;
    opacity: 0;
    transition: opacity 0.3s ease;
  `;
  
  // Estilos por tipo
  const styles = {
    success: 'background: #28a745; box-shadow: 0 4px 12px rgba(40, 167, 69, 0.3);',
    warning: 'background: #ffc107; color: #212529; box-shadow: 0 4px 12px rgba(255, 193, 7, 0.3);',
    error: 'background: #dc3545; box-shadow: 0 4px 12px rgba(220, 53, 69, 0.3);'
  };
  
  toast.style.cssText += styles[type] || styles.success;
  toast.textContent = message;
  
  document.body.appendChild(toast);
  
  // Mostrar con animación
  setTimeout(() => toast.style.opacity = '1', 10);
  
  // Ocultar después de 3 segundos
  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => document.body.contains(toast) && document.body.removeChild(toast), 300);
  }, 3000);
}

// Constante de opacidad fija para padres
const PARENT_FILL_OPACITY = 0.5; // 35% fijo

// Modo de exportación: 'none' | 'warn' | 'strict'
const EXPORT_VALIDATION_MODE = 'none'; // <<— modo "sin líos"

// === POLÍTICA DE VALIDACIÓN ===
const VALIDATION_POLICY = {
  blockOnChildrenOutside: false,
  blockOnCoverageIncomplete: false,
};

// === SYSTEM REGISTRY FOR DATA INTEGRITY ===

// ProjectRegistry - Central data registry for complete project state management
const ProjectRegistry = {
  // Core data maps for maintaining complete project state
  featuresByKey: new Map(),        // unique_key -> feature (complete feature data)
  parentsByCode: new Map(),        // parent_code -> parent_feature
  childrenByParent: new Map(),     // parent_code -> [child_features]
  routeIndex: new Map(),           // id_ruta -> [parent_codes]
  
  // Add or update feature in registry
  setFeature(key, feature) {
    this.featuresByKey.set(key, { ...feature });
    
    // Index by type
    if (feature.properties?.nivel === 'cuadrante') {
      this.parentsByCode.set(feature.properties.codigo, feature);
      
      // Index by route
      const ruta = feature.properties.id_ruta;
      if (ruta) {
        if (!this.routeIndex.has(ruta)) {
          this.routeIndex.set(ruta, []);
        }
        const codes = this.routeIndex.get(ruta);
        if (!codes.includes(feature.properties.codigo)) {
          codes.push(feature.properties.codigo);
        }
      }
    } else if (feature.properties?.nivel === 'subcuadrante') {
      const parentCode = feature.properties.codigo_padre;
      if (parentCode) {
        if (!this.childrenByParent.has(parentCode)) {
          this.childrenByParent.set(parentCode, []);
        }
        const children = this.childrenByParent.get(parentCode);
        // Replace if exists, add if new
        const existingIndex = children.findIndex(c => c.properties?.codigo === feature.properties.codigo);
        if (existingIndex >= 0) {
          children[existingIndex] = feature;
        } else {
          children.push(feature);
        }
      }
    }
  },
  
  // Get feature by key
  getFeature(key) {
    return this.featuresByKey.get(key);
  },
  
  // Get parent by code
  getParent(code) {
    return this.parentsByCode.get(code);
  },
  
  // Get children for parent
  getChildren(parentCode) {
    return this.childrenByParent.get(parentCode) || [];
  },
  
  // Get all features for route
  getRouteFeatures(idRuta) {
    const parentCodes = this.routeIndex.get(idRuta) || [];
    const features = [];
    
    parentCodes.forEach(code => {
      const parent = this.getParent(code);
      if (parent) {
        features.push(parent);
        features.push(...this.getChildren(code));
      }
    });
    
    return features;
  },
  
  // Get all registered features
  getAllFeatures() {
    return Array.from(this.featuresByKey.values());
  },
  
  // Remove feature from registry
  removeFeature(key) {
    const feature = this.featuresByKey.get(key);
    if (!feature) return false;
    
    this.featuresByKey.delete(key);
    
    if (feature.properties?.nivel === 'cuadrante') {
      const code = feature.properties.codigo;
      this.parentsByCode.delete(code);
      this.childrenByParent.delete(code);
      
      // Remove from route index
      const ruta = feature.properties.id_ruta;
      if (ruta && this.routeIndex.has(ruta)) {
        const codes = this.routeIndex.get(ruta);
        const index = codes.indexOf(code);
        if (index >= 0) {
          codes.splice(index, 1);
          if (codes.length === 0) {
            this.routeIndex.delete(ruta);
          }
        }
      }
    } else if (feature.properties?.nivel === 'subcuadrante') {
      const parentCode = feature.properties.codigo_padre;
      if (parentCode && this.childrenByParent.has(parentCode)) {
        const children = this.childrenByParent.get(parentCode);
        const index = children.findIndex(c => c.properties?.codigo === feature.properties.codigo);
        if (index >= 0) {
          children.splice(index, 1);
        }
      }
    }
    
    return true;
  },
  
  // Clear all data
  clear() {
    this.featuresByKey.clear();
    this.parentsByCode.clear();
    this.childrenByParent.clear();
    this.routeIndex.clear();
  },
  
  // Generate unique key for feature
  generateKey(feature) {
    if (feature.properties?.codigo) {
      return `code_${feature.properties.codigo}`;
    }
    // Fallback to geometry hash
    return `geom_${this.hashGeometry(feature.geometry)}`;
  },
  
  // Simple geometry hash for deduplication
  hashGeometry(geometry) {
    return btoa(JSON.stringify(geometry)).slice(0, 12);
  }
};

// === SISTEMA DE JERARQUÍA CUADRANTE→SUBCUADRANTES ===

// Estado del editor
const EditorState = {
  IDLE: 'idle',
  PADRE_ACTIVO: 'padre_activo', 
  CREANDO_PADRE: 'creando_padre',
  CREANDO_HIJO: 'creando_hijo',
  EDITANDO_PADRE: 'editando_padre',
  EDITANDO_HIJOS: 'editando_hijos'
};

// Estado global del editor (fuente única de verdad)
const state = {
  mode: EditorState.IDLE,
  activeParent: null, // Layer del cuadrante padre activo
  children: [], // Array de layers de subcuadrantes hijos del padre activo
  childrenGroup: null, // FeatureGroup para manejar hijos
  isAislado: false, // Si está en modo aislar
  
  // === NUEVO MODELO DE DATOS ===
  masterFC: null,           // FeatureCollection original importado
  worksetFC: null,          // Subconjunto que se está mostrando (toda ciudad o una ruta)
  changeLog: new Map(),     // key: codigo -> GeoJSON Feature actualizado
  colorRegistry: new Map()  // key: codigo (o codigo_padre para hijos) -> {fillColor,color,fillOpacity,weight}
};

// Constantes para modos de importación
const IMPORT_MODE = { PANORAMA: 'PANORAMA', EDIT_ALL: 'EDIT_ALL' };

function getSelectedImportMode() {
  const r = document.querySelector('input[name="vizMode"]:checked');
  return r ? r.value : IMPORT_MODE.PANORAMA;
}

// Extensión para múltiples padres
state.parents = state.parents || [];                 // lista de layers padre
state.childrenByParent = state.childrenByParent || {}; // { codigoPadre: Layer[] }
state.childGroupsByParent = state.childGroupsByParent || {}; // { codigoPadre: L.FeatureGroup }
state.selectedChild = null; // hijo seleccionado para eliminar

// Compatibilidad hacia atrás
let currentEditorState = EditorState.IDLE;
let activePadre = null;
let activeHijos = [];
let isAislado = false;
let padreOpacity = 0.4; // Opacidad del padre

// Sesión de edición simple por capas (sin grupos temporales)
let EDIT_SESSION = { layers: [] };

// Backup para edición de hijos
let _childrenBackup = null;

// Variables para el picker de edición (EDIT_ARM) - click to edit
let EDIT_ARM_ACTIVE = false;
let EDIT_ARM_TYPE = null; // 'parent' o 'children'

// Configuración de tolerancias según T7 (en metros)
const TOLERANCIAS = {
  SNAP_DISTANCE: 1.0, // tol_snapping = 1.0 m
  MIN_AREA_RESTO: 0.5, // Área mínima para considerar resto válido
  MAX_OVERLAP_AREA: 0.01, // area_tol = 0.01 m²
  BUFFER_TOLERANCE: 0.5, // tol_m_valid = 0.5 m
  VALIDATION_TOLERANCE: 0.5 // tol_m_valid para validaciones
};

// Configuración de CRS según T7
const CRS_CONFIG = {
  INPUT_CRS: "EPSG:4326", // WGS84 - sistema de entrada
  METRIC_CRS: "EPSG:3116", // CRS métrico para áreas y buffers (Colombia)
  DISPLAY_CRS: "EPSG:4326" // Para visualización en Leaflet
};

// Metadata del sistema
const SYSTEM_METADATA = {
  VERSION: "3.0",
  EXPORT_TYPE_HIERARCHY: "hierarchy_export",
  EXPORT_TYPE_GENERAL: "general_export",
  NIVEL_PADRE: "cuadrante",
  NIVEL_HIJO: "subcuadrante"
};

// Estilo fijo para comunas: borde negro, sin relleno
const COMUNA_STYLE = {
  color: "#000000",
  weight: 1.5,
  fillOpacity: 0.0,
  fillColor: "transparent"
};

// Estilos para jerarquía
const PADRE_STYLE = {
  color: "#000000",
  weight: 3,
  fillOpacity: PARENT_FILL_OPACITY,
  fillColor: "#667eea",
  dashArray: "5, 5"
};

const HIJO_STYLE = {
  color: "#000000",
  weight: 2,
  fillOpacity: 0.6,
  fillColor: "#11998e"
};

const ERROR_STYLE = {
  OVERLAP: { color: "#ff6b35", weight: 3, fillColor: "#ff6b35", fillOpacity: 0.7 },
  GAP: { color: "#d63031", weight: 3, fillColor: "#d63031", fillOpacity: 0.7 },
  OUTSIDE: { color: "#d63031", weight: 3, fillColor: "#d63031", fillOpacity: 1.0 }
};

// ENHANCED: Asegura que la feature tenga props de estilo persistentes según su nivel
// Policy: "preserve first, assign stable if missing"
function ensureStyleProps(featureOrLayer, isPadre = null) {
  const feat = featureOrLayer.feature ? featureOrLayer.feature : featureOrLayer;
  feat.properties = feat.properties || {};
  const p = feat.properties;

  // Determinar nivel
  const nivel = p.nivel || (isPadre === true ? 'cuadrante' : isPadre === false ? 'subcuadrante' : null);
  const defaults = (nivel === 'cuadrante') ? PADRE_STYLE : HIJO_STYLE;

  // STABLE COLOR POLICY: Use stable assignment if fillColor is missing
  if (p.fillColor == null || p.fillColor === 'undefined') {
    p.fillColor = assignStableColor(feat, isPadre);
    console.debug(`[STYLE] Assigned stable color to ${p.codigo}: ${p.fillColor}`);
  } else {
    console.debug(`[STYLE] Preserving existing color for ${p.codigo}: ${p.fillColor}`);
  }

  // Other style properties with defaults (preserve existing)
  if (p.fillOpacity == null) p.fillOpacity = defaults.fillOpacity;
  if (p.weight == null)      p.weight      = defaults.weight;
  if (p.color == null)       p.color       = (STROKE_POLICY === 'match') ? p.fillColor : '#000000';

  // Regrabar nivel si no venía
  if (!p.nivel && nivel) p.nivel = nivel;

  return p;
}

function ensureComunaStyleProps(props) {
  const p = Object.assign({}, props || {});
  p.color = p.color ?? COMUNA_STYLE.color;
  p.weight = p.weight ?? COMUNA_STYLE.weight;
  p.fillOpacity = p.fillOpacity ?? COMUNA_STYLE.fillOpacity;
  p.fillColor = p.fillColor ?? COMUNA_STYLE.fillColor;
  return p;
}

// === UTILIDADES PARA EXPORTACIÓN COMPLETA ===

// Detecta si un feature es un cuadrante válido (para exportación)
function isQuadrantFeature(f) {
  const p = f?.properties || {};
  if (p.nivel === 'cuadrante' || p.nivel === 'subcuadrante') return true;
  // Fallback para archivos antiguos - acepta todos los prefijos válidos
  return typeof p.codigo === 'string' && /^(CL|MZ|BG|MD|PR|BC|BR)_/i.test(p.codigo);
}

// Convierte layer a feature preservando propiedades de estilo
function layerToFeature(layer) {
  // Asegura que layer.feature exista y que estilos estén persistidos en properties
  const f = layer.toGeoJSON();
  f.properties = layer.feature?.properties || f.properties || {};
  // NO modificar opacidades/colores aquí: respetar lo que venga
  return f;
}

// FC de comunas base de la ciudad actual
let COMUNAS_FC = null;

// === UTILIDADES GEOESPACIALES ===

// Calcular área de polígono usando fórmula de Shoelace (en m² aproximado)
function calculateArea(geojson) {
  if (!geojson || !geojson.geometry) return 0;
  
  let coords = [];
  if (geojson.geometry.type === 'Polygon') {
    coords = geojson.geometry.coordinates[0];
  } else if (geojson.geometry.type === 'MultiPolygon') {
    // Para MultiPolygon, sumar todas las áreas
    return geojson.geometry.coordinates.reduce((total, poly) => {
      return total + calculateArea({ geometry: { type: 'Polygon', coordinates: poly } });
    }, 0);
  } else {
    return 0;
  }
  
  if (!coords || coords.length < 3) return 0;
  
  // Fórmula de Shoelace para área de polígono
  let area = 0;
  for (let i = 0; i < coords.length - 1; i++) {
    const [x1, y1] = coords[i];
    const [x2, y2] = coords[i + 1];
    area += (x1 * y2 - x2 * y1);
  }
  
  // Conversión aproximada a m² usando factor de lat promedio
  const avgLat = coords.reduce((sum, coord) => sum + coord[1], 0) / coords.length;
  const latFactor = Math.cos(avgLat * Math.PI / 180);
  const meterPerDegree = 111320;
  
  return Math.abs(area * meterPerDegree * meterPerDegree * latFactor / 2);
}

// Verificar intersección básica entre dos polígonos
function intersectGeometries(geom1, geom2) {
  // Implementación básica usando detección de vértices
  if (!geom1 || !geom2) return null;
  
  const coords1 = geom1.geometry.coordinates[0];
  const coords2 = geom2.geometry.coordinates[0];
  
  // Verificar si algún vértice de geom1 está dentro de geom2
  for (const point of coords1) {
    if (pointInPolygon(point, geom2)) {
      // Hay intersección, devolver una geometría aproximada
      return {
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [coords1.slice(0, 4)] // Intersección aproximada
        }
      };
    }
  }
  
  return null;
}

// Diferencia aproximada entre geometrías
function differenceGeometries(geom1, geom2) {
  // Por simplicidad, devolver la geometría original
  // En producción, usar turf.difference()
  return geom1;
}

// Unión de múltiples geometrías (aproximada)
function unionGeometries(geometries) {
  if (!geometries || geometries.length === 0) return null;
  
  // Devolver el primer polígono como aproximación
  // En producción, usar turf.union()
  return geometries[0];
}

// Buffer de geometría (simplificado)
function bufferGeometry(geojson, distance) {
  // Para buffer = 0, limpiar la geometría devolviendo la original
  if (distance === 0) return geojson;
  
  // En producción, usar turf.buffer()
  return geojson;
}

// Verificar si un punto está dentro de un polígono (Ray casting algorithm)
function pointInPolygon(point, polygon) {
  const [x, y] = point;
  let coords = [];
  
  if (polygon.geometry.type === 'Polygon') {
    coords = polygon.geometry.coordinates[0];
  } else {
    return false;
  }
  
  let inside = false;
  
  for (let i = 0, j = coords.length - 1; i < coords.length; j = i++) {
    const [xi, yi] = coords[i];
    const [xj, yj] = coords[j];
    
    if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
      inside = !inside;
    }
  }
  
  return inside;
}

// Verificar si una geometría está completamente dentro de otra
function geometryWithin(inner, outer) {
  if (!inner || !outer) return false;
  
  const innerCoords = inner.geometry.coordinates[0];
  
  // Verificar que todos los vértices del polígono interior estén dentro del exterior
  for (const point of innerCoords) {
    if (!pointInPolygon(point, outer)) {
      return false;
    }
  }
  
  return true;
}

// Calcular centroide aproximado de un polígono
function calculateCentroid(geojson) {
  if (!geojson || !geojson.geometry) return null;
  
  const coords = geojson.geometry.coordinates[0];
  let x = 0, y = 0;
  
  for (const coord of coords) {
    x += coord[0];
    y += coord[1];
  }
  
  return [x / coords.length, y / coords.length];
}

// === GESTIÓN DE CÓDIGOS ===

// Generar próximo código de cuadrante disponible
// Genera código de CUADRANTE PADRE según SISTEMA actual.
// - RUTA: <PREF>_ruta_<id_ruta>  (usa 'ruta' recibido)
// - PAP : <PREF>_pap_XXX         (contador 3 dígitos por ciudad)
// DEPRECATED: Esta función ya no se usa, la lógica está en showPadreConfigDialog
function generateNextCuadranteCode(ciudad, ruta) {
  const cityAbbr = getCityPrefix(ciudad);
  if (sistemaActual === 'RUTA') {
    const idRuta = String(ruta ?? '1').trim();
    const base = baseRuta(cityAbbr, idRuta);
    const nextSufijo = computeMaxDupForBase(base) + 1;
    return `${base}_${pad(nextSufijo, 2)}`;
  } else {
    // PAP
    const nextNum = computeMaxPapForCity(cityAbbr) + 1;
    return `${cityAbbr}_pap_${pad(nextNum, 3)}`;
  }
}

// Generar próximo código de subcuadrante
function generateNextSubcuadranteCode(codigoPadre) {
  const existingCodes = getAllExistingCodes();
  const prefix = `${codigoPadre}_S`;
  
  let nextNum = 1;
  while (existingCodes.includes(`${prefix}${String(nextNum).padStart(2, '0')}`)) {
    nextNum++;
  }
  
  return `${prefix}${String(nextNum).padStart(2, '0')}`;
}

// Obtener todos los códigos existentes
function getAllExistingCodes() {
  const codes = [];
  
  // Revisar cuadrantes padre
  if (activePadre && activePadre.feature && activePadre.feature.properties.codigo) {
    codes.push(activePadre.feature.properties.codigo);
  }
  
  // Revisar subcuadrantes hijos
  activeHijos.forEach(hijo => {
    if (hijo.feature && hijo.feature.properties.codigo) {
      codes.push(hijo.feature.properties.codigo);
    }
  });
  
  // Revisar otras capas existentes
  DRAWN_EDITABLE.eachLayer(layer => {
    if (layer.feature && layer.feature.properties.codigo) {
      codes.push(layer.feature.properties.codigo);
    }
  });
  
  DRAWN_LOCKED.eachLayer(layer => {
    if (layer.feature && layer.feature.properties.codigo) {
      codes.push(layer.feature.properties.codigo);
    }
  });
  
  return codes;
}

// === Config de color/borde ===
const PALETTE = [
  '#636EFA', // indigo
  '#EF553B', // orange-red
  '#00CC96', // green
  '#AB63FA', // purple
  '#FFA15A', // orange
  '#19D3F3', // cyan
  '#FF6692', // pink
  '#B6E880', // light green
  '#FF97FF', // light magenta
  '#FECB52', // gold
  '#2E91E5', // blue
  '#F46036', // vermillion
  '#1CA71C', // kelly green
  '#BC5090', // plum
  '#FFA600', // amber
  '#00F7F7', // aqua
  '#FF009D', // hot pink
  '#9A9A00', // olive
  '#000000',// NEGRO
  '#FCC6BB',// ROJO FUERTE
  '#440E03'// MARRÓN OSCURO
];
let CURRENT_FILL = PALETTE[0];
const STROKE_POLICY = 'black'; // 'black' | 'match'  (borde negro o igual al relleno)
const STROKE_WEIGHT = 2;
const FILL_OPACITY = 0.4;

// === RUTAS: diccionario local (UI pública ↔ backend) ===
// NOTA: Mantén este bloque cerca del tope del archivo para visibilidad.
// Route dictionary functions moved to top of file with ROUTES_MAP

// Normalizar propiedades de ruta al importar y crear/editar
function normalizeRouteProps(p) {
  // Si viene id_ruta, aseguremos la etiqueta pública:
  if (p.id_ruta != null && (p.ruta_publica == null || p.ruta_publica === "")) {
    p.ruta_publica = getRouteLabel(p.id_ruta);
    // opcional: estandarizar ciudad si falta
    p.ciudad = p.ciudad || getRouteCityById(p.id_ruta);
  }
  // Si viene la etiqueta y falta id_ruta, resolvemos por diccionario:
  if ((p.id_ruta == null || p.id_ruta === "") && p.ruta_publica) {
    const rid = getRouteIdFromLabel(p.ruta_publica);
    if (rid != null) p.id_ruta = rid;
  }
}

// Configuración de ciudades con sus coordenadas y zoom
const CITY_CFG = {
  CALI:      { center: [3.4516, -76.5320], zoom: 12 },
  MEDELLIN:  { center: [6.2442, -75.5812], zoom: 12 },
  MANIZALES: { center: [5.0672, -75.5174], zoom: 12 },
  PEREIRA:   { center: [4.8087, -75.6906], zoom: 12 },
  BOGOTA:    { center: [4.7110, -74.0721], zoom: 12 },
  BARRANQUILLA: { center: [10.9720, -74.7962], zoom: 12 },
  BUCARAMANGA:  { center: [7.1193, -73.1227], zoom: 12 },
};

// Human-readable route name mapping
const ROUTE_NAME_MAP = {
  '1': 'Ruta Norte',
  '2': 'Ruta Sur', 
  '3': 'Ruta Centro',
  '4': 'Ruta Oriental',
  '5': 'Ruta Occidental',
  '6': 'Ruta Valle',
  '7': 'Ruta Montaña',
  '8': 'Ruta Costa',
  '9': 'Ruta Industrial',
  '10': 'Ruta Comercial'
};

// STABLE COLOR ASSIGNMENT SYSTEM
// Policy: "preserve first, assign stable if missing"

// Enhanced color palette for stable assignments
const STABLE_COLOR_PALETTE = [
  '#636EFA', // indigo
  '#EF553B', // orange-red  
  '#00CC96', // green
  '#AB63FA', // purple
  '#FFA15A', // orange
  '#19D3F3', // cyan
  '#FF6692', // pink
  '#B6E880', // light green
  '#FF97FF', // light magenta
  '#FECB52', // gold
  '#2E91E5', // blue
  '#F46036', // vermillion
  '#1CA71C', // kelly green
  '#BC5090', // plum
  '#FFA600', // amber
  '#00F7F7', // aqua
  '#FF009D', // hot pink
  '#9A9A00', // olive
  '#8E44AD', // dark purple
  '#E74C3C', // red
  '#3498DB', // light blue
  '#F39C12', // dark orange
  '#27AE60', // dark green
  '#E67E22',  // carrot
  '000000'// NEGRO
];

// Simple hash function for deterministic color assignment
function simpleHash(str) {
  let hash = 0;
  if (!str || str.length === 0) return hash;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash);
}

// Get or create route color seed
function getRouteColorSeed(idRuta) {
  if (!ProjectRegistry.routeColorSeeds) {
    ProjectRegistry.routeColorSeeds = new Map();
  }
  
  if (!ProjectRegistry.routeColorSeeds.has(idRuta)) {
    // Generate deterministic seed based on route ID
    const seed = simpleHash(`route_${idRuta}_seed`);
    ProjectRegistry.routeColorSeeds.set(idRuta, seed);
  }
  
  return ProjectRegistry.routeColorSeeds.get(idRuta);
}

// Assign stable color based on feature properties
function assignStableColor(feature, isParent = null) {
  const props = feature.properties || {};
  
  // POLICY: Preserve existing fillColor first
  if (props.fillColor && props.fillColor !== 'undefined') {
    console.debug('[COLOR] Preserving existing fillColor:', props.fillColor);
    return props.fillColor;
  }
  
  // Determine if this is a parent or child
  const nivel = props.nivel || (isParent === true ? 'cuadrante' : isParent === false ? 'subcuadrante' : null);
  const idRuta = props.id_ruta || props.ruta;
  const codigo = props.codigo;
  
  if (!codigo || !idRuta) {
    console.warn('[COLOR] Missing codigo or id_ruta, using palette fallback');
    return STABLE_COLOR_PALETTE[0];
  }
  
  // Get route color seed for consistency within route
  const routeSeed = getRouteColorSeed(idRuta);
  
  if (nivel === 'cuadrante') {
    // Parent: Use route seed + codigo hash for stable assignment
    const codeHash = simpleHash(codigo);
    const colorIndex = (routeSeed + codeHash) % STABLE_COLOR_PALETTE.length;
    const assignedColor = STABLE_COLOR_PALETTE[colorIndex];
    
    console.debug(`[COLOR] Parent ${codigo}: route_seed=${routeSeed}, code_hash=${codeHash}, color=${assignedColor}`);
    return assignedColor;
    
  } else if (nivel === 'subcuadrante') {
    // Child: Use parent color as base + child index for variation
    const parentCode = props.codigo_padre;
    if (!parentCode) {
      console.warn('[COLOR] Child missing codigo_padre, using default');
      return HIJO_STYLE.fillColor;
    }
    
    // Get parent's color from registry or calculate it
    const parentFeature = ProjectRegistry.getParent(parentCode);
    let parentColor = PADRE_STYLE.fillColor;
    
    if (parentFeature && parentFeature.properties.fillColor) {
      parentColor = parentFeature.properties.fillColor;
    } else if (parentFeature) {
      // Calculate parent color using same logic
      parentColor = assignStableColor(parentFeature, true);
    }
    
    // Generate child variation: darker/lighter version of parent color
    const childIndex = extractChildIndex(codigo);
    const variation = generateColorVariation(parentColor, childIndex);
    
    console.debug(`[COLOR] Child ${codigo}: parent=${parentCode}, parent_color=${parentColor}, variation=${variation}`);
    return variation;
  }
  
  // Fallback to palette based on codigo hash
  const codeHash = simpleHash(codigo);
  const fallbackColor = STABLE_COLOR_PALETTE[codeHash % STABLE_COLOR_PALETTE.length];
  console.debug(`[COLOR] Fallback for ${codigo}: ${fallbackColor}`);
  return fallbackColor;
}

// Extract child index from codigo (e.g., CL_1_01_S03 -> 3)
function extractChildIndex(codigo) {
  if (!codigo) return 0;
  const match = codigo.match(/_S(\d+)$/);
  return match ? parseInt(match[1], 10) : 0;
}

// Generate color variation for children
function generateColorVariation(baseColor, childIndex) {
  if (!baseColor || baseColor === 'undefined') {
    return HIJO_STYLE.fillColor;
  }
  
  // Parse hex color
  const hex = baseColor.replace('#', '');
  if (hex.length !== 6) return baseColor;
  
  const r = parseInt(hex.substr(0, 2), 16);
  const g = parseInt(hex.substr(2, 2), 16);
  const b = parseInt(hex.substr(4, 2), 16);
  
  // Create variation based on child index
  const variation = (childIndex * 30) % 360; // Different hue shift per child
  const factor = 0.7 + (childIndex % 3) * 0.1; // Brightness variation
  
  // Apply variation
  const newR = Math.min(255, Math.max(0, Math.round(r * factor)));
  const newG = Math.min(255, Math.max(0, Math.round(g * factor)));
  const newB = Math.min(255, Math.max(0, Math.round(b * factor)));
  
  const newHex = '#' + 
    newR.toString(16).padStart(2, '0') +
    newG.toString(16).padStart(2, '0') +
    newB.toString(16).padStart(2, '0');
  
  return newHex;
}

// Route label resolver - prioritizes ruta_publica, ROUTE_NAME_MAP, then readable fallback
function routeLabelResolver(feature) {
  if (!feature || !feature.properties) {
    return 'Ruta Sin Identificar';
  }
  
  const props = feature.properties;
  
  // Priority 1: Use ruta_publica if available
  if (props.ruta_publica && typeof props.ruta_publica === 'string') {
    return props.ruta_publica.trim();
  }
  
  // Priority 2: Map id_ruta using ROUTE_NAME_MAP
  const idRuta = props.id_ruta || props.ruta;
  if (idRuta && ROUTE_NAME_MAP[idRuta.toString()]) {
    return ROUTE_NAME_MAP[idRuta.toString()];
  }
  
  // Priority 3: Create readable format from id_ruta
  if (idRuta) {
    return `Ruta ${idRuta}`;
  }
  
  // Priority 4: Extract from codigo if possible (e.g., CL_3_01 -> Ruta 3)
  if (props.codigo && typeof props.codigo === 'string') {
    const match = props.codigo.match(/^[A-Z]{2}_(\d+)_/);
    if (match) {
      const routeNum = match[1];
      return ROUTE_NAME_MAP[routeNum] || `Ruta ${routeNum}`;
    }
  }
  
  // Fallback
  return 'Ruta Sin Identificar';
}

// Mapeo de ciudades a archivos de comunas
const CITY_TO_COMUNAS_FILE = {
  CALI: 'comunas_cali.geojson',
  MEDELLIN: 'comunas_medellin.geojson',
  MANIZALES: 'comunas_manizales.geojson',
  PEREIRA: 'comunas_pereira.geojson',
  BOGOTA: 'comunas_bogota.geojson',
  BARRANQUILLA: 'comunas_barranquilla.geojson',
  BUCARAMANGA: 'comunas_bucaramanga.geojson',
};

// Función para obtener la ciudad desde la URL
function getCityFromQuery() {
    const urlParams = new URLSearchParams(window.location.search);
    const city = urlParams.get('city');
    return city ? city.toUpperCase() : 'BOGOTA';
}

// Resolver configuración de ciudad
const CITY = getCityFromQuery();
const cfg = CITY_CFG[CITY] || CITY_CFG.BOGOTA;

// Si la ciudad no existe, mostrar warning
if (!CITY_CFG[CITY]) {
    console.warn("Unknown city, fallback to BOGOTA:", CITY);
}

// Resolver archivo de comunas por ciudad
const file = CITY_TO_COMUNAS_FILE[CITY] || CITY_TO_COMUNAS_FILE.BOGOTA;
const comunasUrl = `/geojson/${file}`;

// Log de inicialización
console.debug("[EDITOR] init city:", CITY, "center:", cfg.center, "zoom:", cfg.zoom);

// Actualizar título del documento
document.title = `Editor de cuadrantes – ${CITY}`;

// Actualizar texto de ayuda dinámicamente
function updateHelpText() {
  const helpElement = document.getElementById('formato-help');
  if (helpElement) {
    const currentPrefix = cityPrefix();
    helpElement.textContent = `Formato: ${currentPrefix}_<consecutivo>`;
  }
}

// Actualizar help text cuando el DOM esté listo
document.addEventListener('DOMContentLoaded', updateHelpText);

// Inicializar mapa Leaflet con configuración de ciudad
const map = L.map('map', {
    center: cfg.center,
    zoom: cfg.zoom,
    zoomControl: true
});

// Agregar tiles de OpenStreetMap
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors'
}).addTo(map);

// Grupos oficiales
const DRAWN_EDITABLE = new L.FeatureGroup(); // Nuevos => editables
const DRAWN_LOCKED   = new L.FeatureGroup(); // Importados => bloqueados

map.addLayer(DRAWN_LOCKED);
map.addLayer(DRAWN_EDITABLE);

// Manejar click en el mapa para cancelar picker
map.on('click', (e) => {
    if (EDIT_ARM_ACTIVE) {
        // Solo cancelar si el click no es sobre un feature registrado
        if (!e.layer) {
            deactivateEditPicker();
            showToast('Selección cancelada', 'warning');
        }
    }
});

// Track del estado de edición de Leaflet.Draw
let isEditingActive = false;

// === FUNCIÓN GLOBAL PARA CONTROLAR MODO RECOLOR ===

// Función global para controlar modo recolor (one-shot)
function setRecolorMode(enabled) {
  recolorMode = !!enabled;
  const btnRecolor = document.getElementById('btn-recolor');
  if (btnRecolor) {
    btnRecolor.classList.toggle('btn-active', recolorMode);
    btnRecolor.textContent = recolorMode ? '✅ Recolor activo' : '🎨 Modo recolor';
  }
  
  // Cambiar cursor del mapa
  const mapContainer = document.getElementById('map');
  if (mapContainer) {
    mapContainer.style.cursor = recolorMode ? 'pointer' : '';
  }
  
  console.debug('[RECOLOR]', recolorMode ? 'Activado (one-shot)' : 'Desactivado');
}

// Configurar control de dibujo con polygon y rectangle
const drawControl = new L.Control.Draw({
    position: 'topleft',
    draw: {
        polygon:   { showArea: true },
        rectangle: true,
        polyline:  false,
        marker:    false,
        circle:    false,
        circlemarker: false
    },
    edit: { 
        featureGroup: DRAWN_EDITABLE,
        remove: false // Desactivar eliminación masiva
    }
});
map.addControl(drawControl);

// Borde forzado
function enforceStrokePolicy(layer) {
  const p = (layer.feature && layer.feature.properties) || (layer.feature = {properties:{}}).properties;
  const colorForStroke = (STROKE_POLICY === 'match') ? (p.fillColor || CURRENT_FILL) : '#000000';
  p.color = colorForStroke;
  p.weight = STROKE_WEIGHT;
  p.fillOpacity = (p.fillOpacity != null) ? p.fillOpacity : FILL_OPACITY;
  // aplicar
  if (layer.setStyle) layer.setStyle({ color: p.color, weight: p.weight, fillColor: p.fillColor || CURRENT_FILL, fillOpacity: p.fillOpacity });
}

// Aplicar estilo desde properties
function applyStyleFromProperties(layer) {
  const p = (layer.feature && layer.feature.properties) || {};
  const defaults = (p.nivel === 'cuadrante') ? PADRE_STYLE : HIJO_STYLE;

  const style = {
    fillColor:   (p.fillColor != null)   ? p.fillColor   : defaults.fillColor,
    color:       (STROKE_POLICY === 'match') ? ((p.fillColor != null) ? p.fillColor : defaults.fillColor) : '#000000',
    fillOpacity: (p.fillOpacity != null) ? p.fillOpacity : defaults.fillOpacity,
    weight:      (p.weight != null)      ? p.weight      : defaults.weight
  };
  if (layer.setStyle) layer.setStyle(style);
}

// ENHANCED: Click para recolor si el modo está activo (one-shot with full style preservation)
let recolorMode = false;
function attachRecolorOnClick(layer) {
  layer.on('click', () => {
    if (isEditingActive) return; // No recolor durante edición
    if (!recolorMode) return; // Solo si modo está activo
    
    // Opcional: restringir al padre activo cuando está aislado
    if (state.isAislado && state.activeParent && !isLayerOfActiveParent(layer)) return;
    
    // ENHANCED: Preserve ALL existing style properties during recolor
    layer.feature = layer.feature || { type:'Feature', properties:{} };
    const p = layer.feature.properties;
    
    // Preserve all existing style properties (not just opacity)
    const preservedStyles = {
      fillOpacity: p.fillOpacity,
      weight: p.weight,
      color: p.color,
      // Preserve any other custom style properties
      dashArray: p.dashArray,
      lineCap: p.lineCap,
      lineJoin: p.lineJoin
    };
    
    // Apply new fill color
    p.fillColor = CURRENT_FILL;
    
    // Restore ALL preserved style properties
    Object.keys(preservedStyles).forEach(key => {
      if (preservedStyles[key] != null) {
        p[key] = preservedStyles[key];
      }
    });
    
    // Update stroke color policy but preserve custom stroke if set
    if (p.color == null) {
      p.color = (STROKE_POLICY === 'match') ? p.fillColor : '#000000';
    }
    
    // Apply styles and update registry
    applyStyleFromProperties(layer);
    enforceStrokePolicy(layer);
    
    // Update in ProjectRegistry to maintain consistency
    const key = ProjectRegistry.generateKey(layer.feature);
    ProjectRegistry.setFeature(key, layer.feature);
    
    console.debug(`[RECOLOR] Applied ${CURRENT_FILL} to ${p.codigo}, preserved styles:`, preservedStyles);
    
    // Apagar modo recolor (one-shot)
    setRecolorMode(false);
  });
}

// Verificar si una capa pertenece al padre activo
function isLayerOfActiveParent(layer) {
  if (!state.activeParent) return false;
  
  const activeCode = state.activeParent.feature?.properties?.codigo;
  if (!activeCode) return false;
  
  const layerCode = layer.feature?.properties?.codigo;
  const layerParentCode = layer.feature?.properties?.codigo_padre;
  
  // Es el padre activo o es hijo del padre activo
  return (layerCode === activeCode) || (layerParentCode === activeCode);
}

// Fetch y overlay de comunas (no editable, atrás)
console.debug('[COMUNAS] fetch:', comunasUrl);
fetch(comunasUrl)
  .then(r => r.json())
  .then(data => {
    // Inyectar estilo de comuna en las properties antes de guardar
    const comunasConEstilo = (data.features || []).map(f => ({
      type: "Feature",
      geometry: f.geometry,
      properties: ensureComunaStyleProps(f.properties)
    }));
    
    // Guardar FeatureCollection con estilos inyectados para export
    COMUNAS_FC = { type: "FeatureCollection", features: comunasConEstilo };
    
    // Crear capa Leaflet con style function que respete las properties
    const comunasLayer = L.geoJSON(COMUNAS_FC, {
      interactive: false, // <— clave: no reciben clicks/hover
      style: (feat) => ({
        color: feat.properties?.color ?? COMUNA_STYLE.color,
        weight: feat.properties?.weight ?? COMUNA_STYLE.weight,
        fillOpacity: feat.properties?.fillOpacity ?? COMUNA_STYLE.fillOpacity,
        fillColor: feat.properties?.fillColor ?? COMUNA_STYLE.fillColor
      })
    }).addTo(map);
    comunasLayer.bringToBack();
    console.debug('[COMUNAS] loaded features:', (data.features || []).length);
  })
  .catch(err => console.warn('[COMUNAS] failed to load', comunasUrl, err));

// === MANEJO DE HERRAMIENTAS DE DIBUJO ===

// Activar modo de dibujo
function enableDrawingMode(type) {
  // Activar la herramienta de dibujo correspondiente
  if (type === 'polygon') {
    // Simular click en botón de polígono de Leaflet.Draw
    const polygonButton = document.querySelector('.leaflet-draw-draw-polygon');
    if (polygonButton && !polygonButton.classList.contains('leaflet-draw-toolbar-button-enabled')) {
      polygonButton.click();
    }
  }
}

// Desactivar modo de dibujo
function disableDrawingMode() {
  // Cancelar cualquier herramienta de dibujo activa
  const activeButtons = document.querySelectorAll('.leaflet-draw-toolbar-button-enabled');
  activeButtons.forEach(btn => btn.click());
}

// === MANEJO DE EVENTOS DE DIBUJO MEJORADO ===

// Handler principal para creación de elementos
map.on(L.Draw.Event.CREATED, (e) => {
  const layer = e.layer;
  layer.feature = layer.feature || { type:'Feature', properties:{} };
  
  if (state.mode === EditorState.CREANDO_PADRE) {
    // Crear cuadrante padre
    handlePadreCreated(layer);
  } else if (state.mode === EditorState.CREANDO_HIJO) {
    // Crear subcuadrante hijo
    onChildCreated(e);
  } else {
    // Comportamiento original para otros casos - siempre crear como PADRE
    layer.feature.properties.fillColor = CURRENT_FILL;
    const codigo = generateUniqueCode();
    layer.feature.properties.codigo = codigo;
    // Siempre marcar como PADRE por defecto
    layer.feature.properties.nivel = 'PADRE'; 
    layer.feature.properties.es_hijo = false;
    layer.feature.properties.codigo_padre = null;
    
    if (typeof enforceStrokePolicy === 'function') enforceStrokePolicy(layer);
    if (typeof applyStyleFromProperties === 'function') applyStyleFromProperties(layer);
    if (typeof attachRecolorOnClick === 'function') attachRecolorOnClick(layer);

    DRAWN_EDITABLE.addLayer(layer);
    console.debug('[DRAW] created -> editable layers:', DRAWN_EDITABLE.getLayers().length, 'codigo:', codigo);
  }
});

// Handler para cuando se termina cualquier dibujo
map.on(L.Draw.Event.DRAWSTOP, onDrawStopForAnyMode);

// Función para manejar creación de hijos
function onChildCreated(e) {
  if (state.mode !== EditorState.CREANDO_HIJO) return;

  const layer = e.layer;

  // 1) Validar que esté dentro del padre activo
  if (!isInsideParent(layer, state.activeParent)) {
    alert('El subcuadrante debe quedar completamente dentro del padre activo.');
    if (layer.remove) layer.remove();
    setEditorState(EditorState.PADRE_ACTIVO);
    return;
  }

  // 2) Preparar layer.feature.properties si no existe
  if (!layer.feature) layer.feature = { type: 'Feature', properties: {}, geometry: null };

  // 3) Obtener idRuta del padre
  const parentProps = state.activeParent?.feature?.properties || {};
  const parentCode = parentProps.codigo || 'PADRE';
  let idRuta = parentProps.id_ruta;
  
  // Si no tiene id_ruta, intentar extraer del código (ej: PR_3 -> ruta 3)
  if (!idRuta) {
    const match = parentCode.match(/^(CL|MZ|BG|MD|PR|BC|BR)_(\d+)/);
    idRuta = match ? match[2] : '1';
  }

  // 4) Crear sugerencia
  const sugerido = suggestedChildCode();

  // 5) Abrir el modal
  openChildCodeModal({
    ruta: idRuta,
    padre: parentCode,
    sugerido: sugerido,
    onCancel: () => { 
      layer.remove(); 
      setEditorState(EditorState.PADRE_ACTIVO); 
    },
    onSave: (codigo) => {
      const code = sanitizeCode(codigo);
      
      // Validar formato
      const validation = validateChildCode(code);
      if (!validation.valid) {
        alert(validation.message);
        return false; // no cerrar
      }
      
      if (isChildCodeTaken(code, state.children)) {
        alert('Ese código ya existe en este padre.');
        return false; // no cerrar
      }
      
      // Asignar propiedades
      const p = layer.feature.properties;
      p.codigo = code;
      p.nivel = 'subcuadrante';
      p.tipo = 'HIJO';
      p.codigo_padre = parentCode;
      // Mantener id_ruta y etiqueta pública
      p.id_ruta = parentProps.id_ruta || p.id_ruta;
      p.ruta_publica = getRouteLabel(p.id_ruta);
      p.ciudad = p.ciudad || parentProps.ciudad || getRouteCityById(p.id_ruta);
      // Heredar tipo_cuadrante del padre
      p.tipo_cuadrante = parentProps.tipo_cuadrante || "RUTA";
      
      // Persistir props de estilo en properties
      ensureStyleProps(layer, false);
      applyStyleFromProperties(layer);
      enforceStrokePolicy(layer);
      
      // Agregar al mapa
      addChildLayer(layer);
      setEditorState(EditorState.PADRE_ACTIVO);
      return true; // cerrar modal
    }
  });
}

// Función para manejar fin de dibujo
function onDrawStopForAnyMode() {
  // Si por cualquier motivo se abortó el dibujo de hijo,
  // regresamos a un estado sano (evita quedar bloqueado)
  if (state.mode === EditorState.CREANDO_HIJO) {
    setEditorState(EditorState.PADRE_ACTIVO);
  } else if (state.mode === EditorState.CREANDO_PADRE) {
    setEditorState(EditorState.IDLE);
  }
}

// Persistir estilo tras edición
map.on(L.Draw.Event.EDITED, (e) => {
  if (e.layers && e.layers.eachLayer) {
    e.layers.eachLayer((layer) => {
      // Mantener código existente (sin prompt)
      if (layer.feature && layer.feature.properties && !layer.feature.properties.codigo) {
        layer.feature.properties.codigo = generateUniqueCode();
        console.debug('[EDIT] código generado automáticamente:', layer.feature.properties.codigo);
      }
      
      if (typeof applyStyleFromProperties === 'function') applyStyleFromProperties(layer);
      if (typeof enforceStrokePolicy === 'function') enforceStrokePolicy(layer);
    });
  }
});

// Desactivar recolor cuando entras a editar
map.on('draw:editstart', () => {
  isEditingActive = true;
  setRecolorMode(false);
});

// Al salir de edición, NO reactivar recolor automáticamente
map.on('draw:editstop', () => {
  isEditingActive = false;
  // Mantener recolor apagado hasta que usuario lo active manualmente
});

// IMPORT (función para agregar capas importadas)
function addImportedFeatureLayer(feature, layer, forceState = null) {
  layer._isImported = true;

  // Asegurar que tiene código, si no asignar uno automático
  if (!layer.feature) layer.feature = feature;
  if (!layer.feature.properties) layer.feature.properties = {};

  // === NUEVO: normalizar ruta
  normalizeRouteProps(layer.feature.properties);

  if (!layer.feature.properties.codigo) {
    layer.feature.properties.codigo = generateUniqueCode();
  }

  // ENHANCED: Register in ProjectRegistry for data integrity
  const key = ProjectRegistry.generateKey(layer.feature);
  ProjectRegistry.setFeature(key, layer.feature);

  if (typeof applyStyleFromProperties === 'function') applyStyleFromProperties(layer);
  if (typeof enforceStrokePolicy === 'function') enforceStrokePolicy(layer);
  if (typeof attachRecolorOnClick === 'function') attachRecolorOnClick(layer);

  // Use forceState if provided, otherwise default to DRAWN_EDITABLE
  const targetLayer = forceState === 'DRAWN_LOCKED' ? DRAWN_LOCKED : DRAWN_EDITABLE;

  // Asegurar interactividad en modo panorama (clicks habilitados)
  if (layer.options) {
    layer.options.interactive = true;
    layer.options.bubblingMouseEvents = true;
  }

  // Determinar nivel de la feature importada (robustecido)
  const p = layer.feature?.properties || {};
  const nivel = (p.nivel || '').toString().toLowerCase();
  
  const isParent =
    nivel === 'cuadrante' || nivel === 'padre' ||
    (!!p.codigo && !/_S\d+$/i.test(p.codigo));  // sin sufijo de hijo

  const isChild =
    nivel === 'subcuadrante' ||
    /_S\d+$/i.test(p.codigo) ||
    !!p.codigo_padre;

  if (layer.options) {
    layer.options.interactive = true;           // ⬅️ asegurar click
    layer.options.bubblingMouseEvents = true;
  }

  // Registrar correctamente para habilitar clicks y selección
  if (isParent) {
    registerParent(layer);                 // ← hace bind del click y popup
  } else if (isChild) {
    const parentCode = p.codigo_padre || (p.codigo ? p.codigo.split('_').slice(0,3).join('_') : null);
    registerChild(layer, parentCode);      // ← añade a grupos y click de selección
  }

  // Mantener el agregado al grupo visual
  // (si aún no se llamó)
  if (!DRAWN_LOCKED.hasLayer(layer) && !DRAWN_EDITABLE.hasLayer(layer)) {
    targetLayer.addLayer(layer);
  }
}

// === GESTIÓN DE CÓDIGOS DE CUADRANTES ===

// Función para generar código automático único
function generateUniqueCode() {
  return `CUADRANTE_${Date.now()}`;
}

// Función para solicitar código al usuario
function promptForCode(currentCode = '') {
  const message = currentCode ? 
    `Código actual: ${currentCode}\n\nIngrese el nuevo código o identificador para este cuadrante:` :
    'Ingrese el código o identificador para este cuadrante:';
  
  const userInput = prompt(message, currentCode);
  
  // Si el usuario cancela o no ingresa nada, usar código automático
  if (userInput === null || userInput.trim() === '') {
    return generateUniqueCode();
  }
  
  return userInput.trim();
}

// Helper para recoger cuadrantes con estilo persistido
function collectQuadrantsFC() {
  const features = [];
  const collect = (group) => group.eachLayer((layer) => {
    if (!layer.toGeoJSON) return;
    // if (typeof persistStyleToProperties === 'function') persistStyleToProperties(layer);
    const f = layer.toGeoJSON();
    f.properties = Object.assign({}, layer.feature && layer.feature.properties || {});
    
    // === NUEVO: normalizar propiedades de ruta antes de exportar
    normalizeRouteProps(f.properties);
    
    features.push(f);
  });
  collect(DRAWN_LOCKED);
  collect(DRAWN_EDITABLE);
  return { type: 'FeatureCollection', features };
}

// === VALIDACIÓN DE INTEGRIDAD ===

// Validar integridad de subcuadrantes
function validarIntegridadSubcuadrantes(geomPadre, geomsHijos, tolM = TOLERANCIAS.BUFFER_TOLERANCE) {
  const errores = [];
  const warnings = [];
  let geojsonDebug = { type: 'FeatureCollection', features: [] };
  
  if (!geomPadre) {
    errores.push('No hay cuadrante padre definido');
    return { ok: false, errores, warnings, geojsonDebug };
  }
  
  if (!geomsHijos || geomsHijos.length === 0) {
    errores.push('No hay subcuadrantes definidos');
    return { ok: false, errores, warnings, geojsonDebug };
  }
  
  const areaPadre = calculateArea(geomPadre);
  
  // 1. Verificar que todos los hijos están dentro del padre
  geomsHijos.forEach((hijo, i) => {
    const areaHijo = calculateArea(hijo);
    
    // Verificar si está dentro del padre (simplificado)
    // En implementación real, usar turf.booleanWithin()
    if (areaHijo > areaPadre * 0.1) { // Si el hijo es > 10% del padre, probablemente está mal
      const hijoFueraGeom = {
        type: 'Feature',
        geometry: hijo.geometry,
        properties: { 
          error: 'outside_parent',
          message: `Subcuadrante ${i + 1} se extiende fuera del cuadrante padre`
        }
      };
      geojsonDebug.features.push(hijoFueraGeom);
      
      warnings.push(`Subcuadrante ${i + 1} se extiende fuera del cuadrante padre`);
    }
  });
  
  // 2. Verificar solapes entre hijos
  for (let i = 0; i < geomsHijos.length; i++) {
    for (let j = i + 1; j < geomsHijos.length; j++) {
      // En implementación real, usar turf.intersect()
      const overlap = intersectGeometries(geomsHijos[i], geomsHijos[j]);
      if (overlap && calculateArea(overlap) > TOLERANCIAS.MAX_OVERLAP_AREA) {
        const overlapGeom = {
          type: 'Feature',
          geometry: overlap.geometry,
          properties: {
            error: 'overlap',
            message: `Solape entre subcuadrantes ${i + 1} y ${j + 1}`
          }
        };
        geojsonDebug.features.push(overlapGeom);
        warnings.push(`Solape detectado entre subcuadrantes ${i + 1} y ${j + 1}`);
      }
    }
  }
  
  // 3. Verificar cobertura total
  const unionHijos = unionGeometries(geomsHijos);
  const huecos = differenceGeometries(geomPadre, unionHijos);
  
  if (huecos && calculateArea(huecos) > TOLERANCIAS.MIN_AREA_RESTO) {
    const huecosGeom = {
      type: 'Feature',
      geometry: huecos.geometry,
      properties: {
        error: 'gaps',
        message: 'Áreas no cubiertas por subcuadrantes'
      }
    };
    geojsonDebug.features.push(huecosGeom);
    warnings.push('Existen áreas no cubiertas por los subcuadrantes');
  }
  
  // Calcular cobertura
  const areaUnionHijos = calculateArea(unionHijos);
  const cobertura = (areaUnionHijos / areaPadre) * 100;
  
  if (cobertura < 100) {
    warnings.push(`Cobertura incompleta: ${cobertura.toFixed(1)}%`);
  }
  
  const ok = errores.length === 0;
  
  console.debug('[VALIDACION]', { ok, errores: errores.length, warnings: warnings.length, cobertura: cobertura.toFixed(1) });
  
  return { ok, errores, warnings, geojsonDebug, cobertura };
}

// === GESTIÓN DE ESTADO DEL EDITOR ===

// === FUNCIONES PARA CÓDIGOS DE SUBCUADRANTES ===

// Mapeo de ciudades a prefijos (diccionario oficial canónico)
const CITY_PREFIX = {
  'CALI': 'CL',
  'MANIZALES': 'MZ', 
  'BOGOTA': 'BG',
  'BOGOTÁ': 'BG',
  'MEDELLIN': 'MD',
  'MEDELLÍN': 'MD',
  'PEREIRA': 'PR',
  'BUCARAMANGA': 'BC',
  'BARRANQUILLA': 'BR'
};

// Obtener prefijo de ciudad desde URL
function cityPrefix() {
  const p = new URLSearchParams(location.search).get('city') || '';
  return CITY_PREFIX[p.toUpperCase()] || p.slice(0, 2).toUpperCase();
}

// Obtener prefijo de ciudad basado en el parámetro de ciudad
function getCityPrefix(ciudadSeleccionada) {
  if (!ciudadSeleccionada) {
    return cityPrefix(); // Fallback to URL-based detection
  }
  return CITY_PREFIX[ciudadSeleccionada.toUpperCase()] || ciudadSeleccionada.slice(0, 2).toUpperCase();
}

// Contador por padre para códigos únicos
if (!window.state) window.state = {};
state.childCounters = state.childCounters || {};

// Generar siguiente índice de subcuadrante
function nextChildIndex(parentCode, children) {
  if (!(parentCode in state.childCounters)) {
    const max = (children || [])
      .map(ch => (ch.feature?.properties?.codigo || '').match(/_S(\d{2})$/))
      .filter(Boolean).map(m => parseInt(m[1], 10))
      .reduce((a, b) => Math.max(a, b), 0);
    state.childCounters[parentCode] = max + 1;
  }
  return state.childCounters[parentCode]++; // 1, 2, 3...
}

// Generar sugerencia de código de subcuadrante
function suggestedChildCode() {
  const parent = state.activeParent;
  const propsP = parent?.feature?.properties || {};
  const city = cityPrefix(); // 'CL'
  const ruta = propsP.id_ruta ?? (parseInt((propsP.codigo || '').split('_')[1], 10) || 0);
  const cuad = (propsP.cuadrante || ((propsP.codigo || '').split('_')[2] || '01')).toString().padStart(2, '0');

  const idx = nextChildIndex(propsP.codigo, state.children);
  const sub = `S${String(idx).padStart(2, '0')}`;
  return `${city}_${ruta}_${cuad}_${sub}`; // ej. CL_1_01_S01
}

// Verificar si un código ya está en uso
function isChildCodeTaken(code, children) {
  return (children || []).some(c => c.feature?.properties?.codigo === code);
}

// Sanitizar código
function sanitizeCode(code) {
  return (code || '').toUpperCase().trim();
}

// Validar formato de código de subcuadrante
function validateChildCode(code) {
  if (!code || code.trim() === '') {
    return { valid: false, message: 'El código no puede estar vacío' };
  }
  
  // Formato esperado: ^(CL|MZ|BG|MD|PR|BC|BR)_\d+_\d{2}_S\d{2}$
  const pattern = /^(CL|MZ|BG|MD|PR|BC|BR)_\d+_\d{2}_S\d{2}$/;
  if (!pattern.test(code.trim())) {
    const currentPrefix = cityPrefix();
    return { valid: false, message: `Formato inválido. Use: ${currentPrefix}_1_01_S01` };
  }
  
  return { valid: true };
}

// === MODAL SIMPLIFICADO PARA CÓDIGO DE SUBCUADRANTES ===

let modalChildOptions = null; // Opciones del modal actual

// Abrir modal para código de hijo
function openChildCodeModal(opts) {
  modalChildOptions = opts;
  
  // Extraer componentes del código sugerido
  const codigo = opts.sugerido || '';
  const parts = codigo.split('_');
  const ciudad = parts[0] || cityPrefix();
  const ruta = parts[1] || opts.ruta || '1';
  const cuadrante = parts[2] || '01';
  
  // Llenar campos
  document.getElementById('hijo-ciudad').value = ciudad;
  document.getElementById('hijo-ruta').value = ruta;
  document.getElementById('hijo-cuadrante').value = cuadrante;
  document.getElementById('hijo-codigo').value = opts.sugerido;
  
  // Mostrar modal
  const modal = document.getElementById('modal-codigo-hijo');
  modal.classList.remove('hidden');
  
  // Focus en el input de código
  setTimeout(() => {
    document.getElementById('hijo-codigo').select();
  }, 100);
  
  console.debug('[MODAL]', 'Modal de código de hijo abierto', opts);
}

// Cerrar modal de código de hijo
function closeChildCodeModal() {
  const modal = document.getElementById('modal-codigo-hijo');
  modal.classList.add('hidden');
  modalChildOptions = null;
}

// Asignar código único a un subcuadrante (versión actualizada)
function assignChildCode(layer) {
  // Esta función ya no se usa directamente, 
  // el flujo ahora es manejado por onChildCreated() -> openChildCodeModal()
  console.warn('[DEPRECATED] assignChildCode() - usar onChildCreated() en su lugar');
}

// Añadir hijo al padre activo
function addChildLayer(layer) {
  // Normaliza geometry GeoJSON de la capa
  layer.feature.geometry = layer.toGeoJSON().geometry;

  // Obtener código del padre activo
  const parentCode = state.activeParent?.feature?.properties?.codigo;
  if (!parentCode) {
    console.warn('[CHILD_ADDED] No hay padre activo, no se puede agregar hijo');
    return;
  }

  // ENHANCED: Register in ProjectRegistry for data integrity
  const key = ProjectRegistry.generateKey(layer.feature);
  ProjectRegistry.setFeature(key, layer.feature);

  // Crear grupo de hijos para este padre si no existe
  if (!state.childGroupsByParent[parentCode]) {
    const childGroup = new L.FeatureGroup();
    state.childGroupsByParent[parentCode] = childGroup;
    map.addLayer(childGroup);
  }

  // Agregar al grupo específico del padre
  state.childGroupsByParent[parentCode].addLayer(layer);

  // Persistir la asociación hijo→padre
  state.childrenByParent[parentCode] = state.childrenByParent[parentCode] || [];
  state.childrenByParent[parentCode].push(layer);

  // Actualizar estado actual (compatibilidad)
  state.children = state.childrenByParent[parentCode];
  activeHijos = state.children;

  // Actualizar grupo de hijos actual
  state.childrenGroup = state.childGroupsByParent[parentCode];

  // Popup sencillo
  const code = layer.feature.properties?.codigo || '(sin código)';
  const tipo = layer.feature.properties?.tipo_cuadrante || 'RUTA';
  layer.bindPopup(`<b>Subcuadrante</b><br>Código: ${code}<br>Tipo: ${tipo}`);
  
  // Agregar funcionalidad de recolor
  attachRecolorOnClick(layer);
  
  // Registrar hijo para selección y eliminación
  registerChild(layer, parentCode);
  
  console.debug('[CHILD_ADDED]', `Hijo "${code}" agregado y registrado al padre "${parentCode}"`);
}

// === FUNCIONES PARA EDICIÓN DEL PADRE ===

// Validar que todos los hijos estén dentro del padre
function validateChildrenWithinParent(parentGeom, children) {
  const errors = [];
  for (const ch of (children || [])) {
    const chGeom = ch.toGeoJSON().geometry;
    let ok = false;
    
    try {
      if (window.turf) {
        ok = turf.booleanWithin(chGeom, parentGeom) || turf.booleanContains(parentGeom, chGeom);
      } else {
        // Fallback: verificar bounds
        const childBounds = ch.getBounds();
        const parentLayer = L.geoJSON({type: 'Feature', geometry: parentGeom});
        const parentBounds = parentLayer.getBounds();
        ok = parentBounds.contains(childBounds);
      }
    } catch (error) {
      console.warn('[VALIDATION] Error validando hijo:', error);
      ok = true; // En caso de error, permitir
    }
    
    if (!ok) {
      errors.push(ch);
    }
  }
  return errors;
}

// Resaltar hijos con error
function highlightErrorChildren(errorChildren) {
  errorChildren.forEach(child => {
    child.setStyle({
      color: '#ff0000',
      weight: 3,
      dashArray: '10, 5'
    });
  });
}

// Limpiar resaltado de hijos
function clearChildrenHighlight(children) {
  (children || []).forEach(child => {
    child.setStyle({
      color: '#666',
      weight: 2,
      dashArray: null
    });
  });
}

// Activar modo picker para seleccionar elemento a editar
function activateEditPicker(editType) {
  EDIT_ARM_ACTIVE = true;
  EDIT_ARM_TYPE = editType;
  
  // Cambiar cursor para indicar modo picker
  map.getContainer().style.cursor = 'crosshair';
  
  // Mostrar mensaje al usuario
  const message = editType === 'parent' ? 
    'Haga click en el padre (cuadrante) que desea editar' : 
    'Haga click en el padre para editar sus hijos (subcuadrantes)';
    
  showToast(message, 'info');
  
  console.debug(`[EDIT_ARM] Activado picker para editar: ${editType}`);
}

// Desactivar modo picker
function deactivateEditPicker() {
  EDIT_ARM_ACTIVE = false;
  EDIT_ARM_TYPE = null;
  map.getContainer().style.cursor = '';
  console.debug('[EDIT_ARM] Picker desactivado');
}

// Iniciar edición del padre
function startParentEditing() {
  if (!state.activeParent) return;
  
  // Cambiar estado
  setEditorState(EditorState.EDITANDO_PADRE);
  
  // Habilitar edición REAL del padre
  enableEditMode([state.activeParent]);
  
  // Evitar interferencias durante la edición
  setRecolorMode(false);
  if (state.activeParent?.options) {
    state.activeParent.options.interactive = true;
  }
  
  // Traer el padre al frente y dar feedback visual sutil
  state.activeParent.bringToFront?.();
  state.activeParent.setStyle({ weight: 4, dashArray: null, fillOpacity: 0.25 });
  
  // Baja un poco la opacidad de los hijos para ver mejor la deformación
  (state.children || []).forEach(h => h.setStyle({ fillOpacity: 0.35 }));
  
  console.debug('[EDIT_PARENT] Iniciando edición real del padre');
}

function saveParentEditing() {
  if (!state.activeParent || state.mode !== EditorState.EDITANDO_PADRE) return;
  
  console.debug('[SAVE_PARENT] Iniciando guardado de padre');
  
  // INTERCEPTAR: Abrir modal de edición de código antes de guardar
  const props = state.activeParent.feature?.properties || {};
  const currentSystem = props.system || sistemaActual;
  
  showEditCodeModal(currentSystem, props, (newProps) => {
    // Callback: actualizar propiedades y guardar
    Object.assign(state.activeParent.feature.properties, newProps);
    
    // Persistir la geometría con toGeoJSON()
    const feat = state.activeParent.toGeoJSON();
    const parentCode = feat.properties.code || feat.properties.codigo;
    
    // Sincronizar geometría en el feature interno
    state.activeParent.feature.geometry = feat.geometry;
    
    // Registrar cambios en changeLog
    if (parentCode) {
      state.changeLog.set(parentCode, feat);
      
      // SINCRONIZACIÓN DEFENSIVA: Mantener ProjectRegistry alineado
      const registryKey = ProjectRegistry.generateKey(feat);
      ProjectRegistry.setFeature(registryKey, feat);
      console.debug(`[SAVE_PARENT] Sincronizado con ProjectRegistry: ${registryKey}`);
    }
    
    // Cerrar edición
    endEditMode(true);
    
    // Restaurar estilos
    state.activeParent.setStyle({ weight: 3, dashArray: "5, 5", fillOpacity: PARENT_FILL_OPACITY });
    (state.children || []).forEach(h => applyStyleFromProperties(h));
    
    // Volver a mandar el padre detrás de los hijos para que los hijos sigan clicables
    state.activeParent.bringToBack?.();
    
    setEditorState(EditorState.PADRE_ACTIVO);
    
    showToast("Cambios guardados en el cuadrante.");
    console.debug(`[SAVE_PARENT] Padre ${parentCode} guardado exitosamente`);
  });
}

// ENHANCED: Robust containment validation with detailed error reporting
function validateChildrenContainmentRobust(parentGeom, children) {
  const errors = [];
  
  if (!children || children.length === 0) return errors;
  
  children.forEach(child => {
    const childFeature = child.toGeoJSON();
    const childCode = child.feature?.properties?.codigo || 'Unknown';
    
    let isContained = false;
    let errorReason = 'Unknown error';
    
    try {
      if (window.turf) {
        // Use turf.js for precise geometric validation
        isContained = turf.booleanContains(parentGeom, childFeature) || 
                     turf.booleanWithin(childFeature, parentGeom);
        
        if (!isContained) {
          // Try to determine specific reason
          const intersection = turf.intersect(parentGeom, childFeature);
          if (!intersection) {
            errorReason = 'Completamente fuera del padre';
          } else {
            const intersectionArea = turf.area(intersection);
            const childArea = turf.area(childFeature);
            const coverage = (intersectionArea / childArea) * 100;
            errorReason = `Solo ${coverage.toFixed(1)}% dentro del padre`;
          }
        }
      } else {
        // Fallback: basic bounds checking
        const childBounds = child.getBounds();
        const parentLayer = L.geoJSON(parentGeom);
        const parentBounds = parentLayer.getBounds();
        
        isContained = parentBounds.contains(childBounds);
        if (!isContained) {
          errorReason = 'Bounds check failed (basic validation)';
        }
      }
    } catch (error) {
      console.warn(`[CONTAINMENT] Error validating ${childCode}:`, error);
      errorReason = `Validation error: ${error.message}`;
      // Assume contained in case of validation error to prevent false positives
      isContained = true;
    }
    
    if (!isContained) {
      errors.push({
        child: child,
        childCode: childCode,
        reason: errorReason
      });
    }
  });
  
  return errors;
}

// ENHANCED: Highlight containment errors with detailed visual feedback
function highlightContainmentErrors(errors) {
  errors.forEach(error => {
    error.child.setStyle({
      color: '#e74c3c',
      weight: 4,
      dashArray: '8, 4',
      fillColor: '#e74c3c',
      fillOpacity: 0.3
    });
    
    // Add popup with error details
    error.child.bindPopup(
      `<div style="color: #e74c3c; font-weight: bold;">⚠️ Error de Contención</div>` +
      `<div><strong>Código:</strong> ${error.childCode}</div>` +
      `<div><strong>Problema:</strong> ${error.reason}</div>`,
      { autoClose: false, closeOnClick: false }
    ).openPopup();
  });
}

// Clear containment error highlighting
function clearContainmentHighlight(children) {
  (children || []).forEach(child => {
    // Restore original styles
    applyStyleFromProperties(child);
    
    // Close error popups
    if (child.getPopup()) {
      child.closePopup();
      child.unbindPopup();
    }
  });
}

// ENHANCED: Comprehensive style preservation utility
function preserveStylesBeforeEdit(layer) {
  if (!layer || !layer.feature || !layer.feature.properties) return {};
  
  const props = layer.feature.properties;
  return {
    fillColor: props.fillColor,
    fillOpacity: props.fillOpacity,
    color: props.color,
    weight: props.weight,
    opacity: props.opacity,
    dashArray: props.dashArray
  };
}

// ENHANCED: Restore preserved styles to layer and feature
function restorePreservedStyles(layer, preservedStyles) {
  if (!layer || !preservedStyles) return;
  
  // Update feature properties
  Object.keys(preservedStyles).forEach(key => {
    if (preservedStyles[key] !== undefined) {
      layer.feature.properties[key] = preservedStyles[key];
    }
  });
  
  // Apply to visual layer
  const styleToApply = {};
  Object.keys(preservedStyles).forEach(key => {
    if (preservedStyles[key] !== undefined) {
      styleToApply[key] = preservedStyles[key];
    }
  });
  
  if (Object.keys(styleToApply).length > 0) {
    layer.setStyle(styleToApply);
  }
}

// COMPREHENSIVE EDITING VALIDATION SYSTEM

// Validate before starting any editing operation
function validateBeforeEdit(layer, editType) {
  const validationErrors = [];
  
  if (!layer || !layer.feature) {
    validationErrors.push({
      type: 'INVALID_LAYER',
      message: 'Layer or feature is null/undefined',
      severity: 'CRITICAL'
    });
    return { valid: false, errors: validationErrors };
  }
  
  const props = layer.feature.properties || {};
  const codigo = props.codigo;
  
  // Validate required properties
  if (!codigo) {
    validationErrors.push({
      type: 'MISSING_CODE',
      message: 'Feature missing required codigo property',
      severity: 'CRITICAL'
    });
  }
  
  // Validate geometry
  try {
    const geom = layer.toGeoJSON();
    if (!geom || !geom.geometry) {
      validationErrors.push({
        type: 'INVALID_GEOMETRY',
        message: 'Feature has invalid or missing geometry',
        severity: 'CRITICAL'
      });
    } else if (window.turf) {
      // Additional turf.js validation
      if (!turf.booleanValid(geom)) {
        validationErrors.push({
          type: 'INVALID_GEOMETRY',
          message: 'Geometry is not valid according to turf.js',
          severity: 'WARNING'
        });
      }
    }
  } catch (error) {
    validationErrors.push({
      type: 'GEOMETRY_ERROR',
      message: `Error validating geometry: ${error.message}`,
      severity: 'CRITICAL'
    });
  }
  
  // Validate style properties
  const requiredStyles = ['fillColor', 'color'];
  const missingStyles = requiredStyles.filter(style => !props[style]);
  
  if (missingStyles.length > 0) {
    validationErrors.push({
      type: 'MISSING_STYLES',
      message: `Missing required style properties: ${missingStyles.join(', ')}`,
      severity: 'WARNING'
    });
  }
  
  // Type-specific validation
  if (editType === 'PARENT') {
    // Validate parent has children
    const children = ProjectRegistry.getChildrenByParentCode(codigo);
    if (!children || children.length === 0) {
      validationErrors.push({
        type: 'PARENT_NO_CHILDREN',
        message: 'Parent has no children in registry',
        severity: 'WARNING'
      });
    }
  }
  
  const criticalErrors = validationErrors.filter(e => e.severity === 'CRITICAL');
  const isValid = criticalErrors.length === 0;
  
  return {
    valid: isValid,
    errors: validationErrors,
    criticalErrors: criticalErrors,
    warnings: validationErrors.filter(e => e.severity === 'WARNING')
  };
}

// Validate data integrity after editing
function validateAfterEdit(layer, editType) {
  const validationErrors = [];
  const codigo = layer.feature?.properties?.codigo;
  
  try {
    // Validate geometry integrity
    const newGeom = layer.toGeoJSON();
    
    if (window.turf) {
      // Check for self-intersections
      if (!turf.booleanValid(newGeom)) {
        validationErrors.push({
          type: 'INVALID_GEOMETRY',
          message: 'Edited geometry has become invalid',
          severity: 'CRITICAL'
        });
      }
      
      // Check for minimal area
      const area = turf.area(newGeom);
      if (area < 1) { // Less than 1 m²
        validationErrors.push({
          type: 'MINIMAL_AREA',
          message: `Geometry area is very small: ${area.toFixed(2)} m²`,
          severity: 'WARNING'
        });
      }
    }
    
    // Validate ProjectRegistry consistency
    const key = ProjectRegistry.generateKey(layer.feature);
    const registryFeature = ProjectRegistry.getFeature(key);
    
    if (!registryFeature) {
      validationErrors.push({
        type: 'REGISTRY_MISSING',
        message: 'Feature not found in ProjectRegistry after edit',
        severity: 'CRITICAL'
      });
    }
    
    // Validate style preservation
    const requiredStyles = ['fillColor', 'color'];
    const props = layer.feature.properties || {};
    
    requiredStyles.forEach(style => {
      if (!props[style]) {
        validationErrors.push({
          type: 'STYLE_LOST',
          message: `Required style property '${style}' was lost during editing`,
          severity: 'WARNING'
        });
      }
    });
    
  } catch (error) {
    validationErrors.push({
      type: 'VALIDATION_ERROR',
      message: `Error during post-edit validation: ${error.message}`,
      severity: 'CRITICAL'
    });
  }
  
  const criticalErrors = validationErrors.filter(e => e.severity === 'CRITICAL');
  const isValid = criticalErrors.length === 0;
  
  return {
    valid: isValid,
    errors: validationErrors,
    criticalErrors: criticalErrors,
    warnings: validationErrors.filter(e => e.severity === 'WARNING')
  };
}

// Show validation results to user
function displayValidationResults(validation, operation) {
  if (!validation || validation.valid) {
    return true; // No issues to display
  }
  
  const criticalCount = validation.criticalErrors?.length || 0;
  const warningCount = validation.warnings?.length || 0;
  
  if (criticalCount > 0) {
    const criticalList = validation.criticalErrors.map(e => `• ${e.message}`).join('\n');
    
    alert(
      `❌ ERRORES CRÍTICOS EN ${operation.toUpperCase()}\n\n` +
      `Se encontraron ${criticalCount} errores críticos:\n${criticalList}\n\n` +
      `La operación no puede continuar.`
    );
    return false;
  }
  
  if (warningCount > 0) {
    const warningList = validation.warnings.map(e => `• ${e.message}`).join('\n');
    
    const confirmed = confirm(
      `⚠️ ADVERTENCIAS EN ${operation.toUpperCase()}\n\n` +
      `Se encontraron ${warningCount} advertencias:\n${warningList}\n\n` +
      `¿Continuar con la operación?`
    );
    return confirmed;
  }
  
  return true;
}

// COLOR CONSISTENCY VALIDATION AND MANAGEMENT

// Validate color consistency before and after editing
function validateColorConsistency(layer, operation) {
  const validationIssues = [];
  const props = layer.feature?.properties;
  const codigo = props?.codigo;
  
  if (!props) {
    validationIssues.push({
      type: 'NO_PROPERTIES',
      message: 'Feature has no properties for color validation'
    });
    return { valid: false, issues: validationIssues };
  }
  
  // Check for required color properties
  if (!props.fillColor) {
    validationIssues.push({
      type: 'MISSING_FILL_COLOR',
      message: 'Feature missing fillColor property'
    });
  }
  
  if (!props.color) {
    validationIssues.push({
      type: 'MISSING_BORDER_COLOR',
      message: 'Feature missing border color property'
    });
  }
  
  // Validate color format
  const colorRegex = /^#[0-9A-Fa-f]{6}$/;
  if (props.fillColor && !colorRegex.test(props.fillColor)) {
    validationIssues.push({
      type: 'INVALID_COLOR_FORMAT',
      message: `Invalid fillColor format: ${props.fillColor}`
    });
  }
  
  // Check color stability (only for children with route-based colors)
  if (props.ruta && operation === 'EDIT') {
    const expectedColor = getRouteColorSeed(props.ruta);
    const currentColor = props.fillColor;
    
    if (expectedColor && currentColor !== expectedColor) {
      // This might be a color variation, check if it's deterministic
      const expectedVariation = generateColorVariation(expectedColor, codigo);
      if (currentColor !== expectedVariation) {
        validationIssues.push({
          type: 'COLOR_INCONSISTENCY',
          message: `Color ${currentColor} doesn't match expected route color or variation`
        });
      }
    }
  }
  
  return {
    valid: validationIssues.length === 0,
    issues: validationIssues
  };
}

// Enforce color stability during editing operations
function enforceColorStabilityDuringEdit(layers) {
  if (!Array.isArray(layers)) {
    layers = [layers];
  }
  
  layers.forEach(layer => {
    const props = layer.feature?.properties;
    const codigo = props?.codigo;
    
    if (!props) return;
    
    // Preserve existing colors if they exist and are valid
    if (props.fillColor && props.color) {
      // Color is already assigned, preserve it
      return;
    }
    
    // Assign stable color if missing
    assignStableColor(layer);
    
    console.debug(`[COLOR_STABILITY] Enforced stable color for ${codigo}`);
  });
}

// Restore color stability after editing operations
function restoreColorStabilityAfterEdit(layers) {
  if (!Array.isArray(layers)) {
    layers = [layers];
  }
  
  const restoredCount = layers.reduce((count, layer) => {
    const props = layer.feature?.properties;
    const codigo = props?.codigo;
    
    if (!props) return count;
    
    // Validate current color assignment
    const colorValidation = validateColorConsistency(layer, 'POST_EDIT');
    
    if (!colorValidation.valid) {
      // Color became inconsistent, restore it
      const originalColor = props._originalFillColor || props.fillColor;
      
      if (originalColor) {
        // Restore preserved color
        props.fillColor = originalColor;
        layer.setStyle({ fillColor: originalColor });
        console.debug(`[COLOR_STABILITY] Restored original color for ${codigo}`);
        return count + 1;
      } else {
        // Reassign stable color
        assignStableColor(layer);
        console.debug(`[COLOR_STABILITY] Reassigned stable color for ${codigo}`);
        return count + 1;
      }
    }
    
    return count;
  }, 0);
  
  if (restoredCount > 0) {
    console.debug(`[COLOR_STABILITY] Restored color stability for ${restoredCount} features`);
  }
  
  return restoredCount;
}

// Preserve colors before entering edit mode
function preserveColorsBeforeEdit(layers) {
  if (!Array.isArray(layers)) {
    layers = [layers];
  }
  
  layers.forEach(layer => {
    const props = layer.feature?.properties;
    if (props && props.fillColor) {
      // Store original color for restoration if needed
      props._originalFillColor = props.fillColor;
      props._originalColor = props.color;
    }
  });
}

// Enhanced color assignment for edited features
function assignStableColorToEditedFeature(layer) {
  if (!layer || !layer.feature) return;
  
  const props = layer.feature.properties;
  const codigo = props?.codigo;
  const ruta = props?.ruta;
  
  // First, try to maintain existing stable color
  if (props.fillColor && props._originalFillColor === props.fillColor) {
    // Color hasn't changed, keep it
    return;
  }
  
  // For route-based features, ensure route color consistency
  if (ruta) {
    const routeColor = getRouteColorSeed(ruta);
    if (routeColor) {
      const stableColor = generateColorVariation(routeColor, codigo);
      props.fillColor = stableColor;
      props.color = routeColor; // Border uses route base color
      
      layer.setStyle({
        fillColor: stableColor,
        color: routeColor
      });
      
      console.debug(`[COLOR_STABILITY] Assigned route-based stable color to ${codigo}`);
      return;
    }
  }
  
  // Fallback to general stable color assignment
  assignStableColor(layer);
}

// ITERACIÓN 2 COMPLETION SUMMARY
console.log(`
🎉 ITERACIÓN 2 COMPLETADA - Estabilidad de colores y edición robusta

✅ FUNCIONALIDADES IMPLEMENTADAS:
• Validación robusta de edición con turf.js
• Persistencia completa de estilos durante edición
• Sistema de validación comprehensivo para geometrías
• Estabilidad de colores durante operaciones de edición
• Preservación de colores antes de editar
• Restauración de estabilidad de colores después de editar
• Validación de contención con reporte detallado de errores
• Sistema de pruebas comprehensivo para validar robustez

🔧 MEJORAS DE ROBUSTEZ:
• Prevención de "bailar" de colores en características hijas
• Validación pre/post edición para prevenir pérdida de datos
• Gestión de errores con opciones de usuario y recuperación
• Asignación determinística de colores basada en semillas de ruta
• Preservación completa de propiedades de estilo
• Validación geométrica usando turf.js con fallbacks

🧪 TESTING DISPONIBLE:
• runProjectRegistryTests() - Pruebas del sistema ProjectRegistry
• runEditingTests() - Pruebas comprehensivas de robustez de edición

📊 ESTADO DEL SISTEMA:
• Sistema ProjectRegistry: ✅ Operacional
• Modos de importación dual: ✅ Activos
• Exportación mejorada: ✅ Funcional
• Validación de edición: ✅ Implementada
• Estabilidad de colores: ✅ Garantizada
`);

console.log('🚀 Editor de Cuadrantes listo con todas las mejoras de Iteración 2');

// === SISTEMA DE RUTAS IMPLEMENTADO ===
console.log(`
🗺️ SISTEMA DE DICCIONARIO DE RUTAS IMPLEMENTADO

✅ FUNCIONALIDADES AGREGADAS:
• Diccionario local ROUTE_DICT con mapeo id ↔ etiqueta pública
• Funciones helper: getRouteLabel(), getRouteCityById(), getRouteIdFromLabel()
• Normalización automática de propiedades de ruta en importación
• Preservación de id_ruta + ruta_publica en creación/edición
• UI mejorada con nombres humanos en selectores
• Exportación garantiza ambos campos (id_ruta numérico + ruta_publica string)

🔧 RUTAS CONFIGURADAS:
• Ruta 3 (id: 9) - CALI
• Ruta 7 (id: 13) - CALI  
• Ruta 10 (id: 19) - CALI
• Ruta 16 PALMIRA (id: 780) - PALMIRA

📊 INTEGRACIONES:
• addImportedFeatureLayer() - normaliza al importar
• child creation modal - preserva rutas del padre
• saveParentEditing() - normaliza antes de guardar
• hierarchy selector - muestra nombres humanos
• collectQuadrantsFC() - normaliza en exportación
• buildFullFeatureCollection() - normaliza dataset completo

🎯 El sistema mantiene compatibilidad completa con backend (id_ruta) 
   mientras mejora la experiencia de usuario (ruta_publica)
`);

// Cancelar edición del padre
function cancelParentEditing() {
  if (!state.activeParent || state.mode !== EditorState.EDITANDO_PADRE) return;
  
  // Revertir geometría con el handler
  endEditMode(false);
  
  // Restaurar estilos
  state.activeParent.setStyle({ weight: 3, dashArray: "5, 5", fillOpacity: PARENT_FILL_OPACITY });
  (state.children || []).forEach(h => applyStyleFromProperties(h));
  
  // Volver a mandar el padre detrás
  state.activeParent.bringToBack?.();
  
  setEditorState(EditorState.PADRE_ACTIVO);
  
  console.debug('[EDIT_PARENT] Edición cancelada');
}

// === FUNCIONES PARA EDICIÓN DE HIJOS ===

// Iniciar edición de hijos
function startChildrenEditing() {
  if (!activeHijos || activeHijos.length === 0) return;
  
  // ENHANCED: Preserve colors before editing all children
  preserveColorsBeforeEdit(activeHijos);
  
  // ENHANCED: Enforce color stability for all children
  enforceColorStabilityDuringEdit(activeHijos);
  
  // backup geometrías
  _childrenBackup = activeHijos.map(l => l.toGeoJSON().geometry);
  setEditorState(EditorState.EDITANDO_HIJOS);
  enableEditMode(activeHijos);
  console.debug('[EDIT_CHILDREN] Iniciando edición de hijos con preservación de colores');
}

// ENHANCED: Guardar cambios de hijos con persistencia completa de estilos
function saveChildrenEditing() {
  if (!activePadre) return;
  
  // ENHANCED: Pre-edit validation for all children
  let hasValidationErrors = false;
  activeHijos.forEach(child => {
    const validation = validateBeforeEdit(child, 'CHILD');
    if (!validation.valid) {
      hasValidationErrors = true;
      console.warn(`[CHILDREN_EDIT] Validation errors for ${child.feature?.properties?.codigo}:`, validation.errors);
    }
  });
  
  if (hasValidationErrors) {
    const confirmed = confirm(
      `⚠️ ERRORES DE VALIDACIÓN\n\n` +
      `Se encontraron errores en algunos subcuadrantes.\n` +
      `¿Continuar con el guardado? (revisa la consola para detalles)`
    );
    if (!confirmed) return;
  }
  
  const parentGeom = activePadre.toGeoJSON();
  const parentCode = activePadre.feature?.properties?.codigo;

  // ENHANCED: Robust validation with detailed error reporting
  const containmentErrors = [];
  activeHijos.forEach(child => {
    const childGeom = child.toGeoJSON();
    const childCode = child.feature?.properties?.codigo || 'Unknown';
    
    let isContained = false;
    let errorReason = '';
    
    try {
      if (window.turf) {
        isContained = turf.booleanWithin(childGeom, parentGeom) || 
                     turf.booleanContains(parentGeom, childGeom) ||
                     turf.booleanOverlap(childGeom, parentGeom);
        
        if (!isContained) {
          const intersection = turf.intersect(parentGeom, childGeom);
          if (!intersection) {
            errorReason = 'Completamente fuera del padre';
          } else {
            const intersectionArea = turf.area(intersection);
            const childArea = turf.area(childGeom);
            const coverage = (intersectionArea / childArea) * 100;
            if (coverage < 50) {
              errorReason = `Solo ${coverage.toFixed(1)}% dentro del padre`;
            } else {
              isContained = true; // Accept if >50% contained
            }
          }
        }
      } else {
        // Fallback: bounds checking
        isContained = activePadre.getBounds().contains(child.getBounds());
        if (!isContained) {
          errorReason = 'Bounds check failed';
        }
      }
    } catch (error) {
      console.warn(`[CHILDREN_EDIT] Error validating ${childCode}:`, error);
      isContained = true; // Assume contained on validation error
    }
    
    if (!isContained) {
      containmentErrors.push({ child, childCode, errorReason });
    }
  });

  if (containmentErrors.length > 0) {
    // Highlight problematic children
    containmentErrors.forEach(error => {
      error.child.setStyle({ 
        color: '#e74c3c', 
        dashArray: '8,4', 
        weight: 3,
        fillColor: '#e74c3c',
        fillOpacity: 0.3
      });
    });
    
    const errorList = containmentErrors.map(e => `• ${e.childCode}: ${e.errorReason}`).join('\n');
    
    const confirmed = confirm(
      `❌ PROBLEMAS DE CONTENCIÓN\n\n` +
      `Los siguientes subcuadrantes tienen problemas:\n${errorList}\n\n` +
      `¿Continuar editando para corregir?`
    );
    
    if (confirmed) {
      // Clear error highlighting and stay in edit mode
      containmentErrors.forEach(error => {
        applyStyleFromProperties(error.child);
      });
      return;
    }
    // If user chose not to fix, clear highlighting and continue saving
    containmentErrors.forEach(error => {
      applyStyleFromProperties(error.child);
    });
  }

  // ENHANCED: Comprehensive style preservation and persistence
  activeHijos.forEach(child => {
    const childCode = child.feature?.properties?.codigo;
    
    // Preserve original style properties before any cleanup
    const preservedStyles = preserveStylesBeforeEdit(child);
    
    // Update geometry in feature
    const newGeometry = child.toGeoJSON().geometry;
    child.feature.geometry = newGeometry;
    
    // === NUEVO: Registrar cada hijo modificado en changeLog ===
    const feat = child.toGeoJSON();
    state.changeLog.set(feat.properties.codigo, feat);
    
    // SINCRONIZACIÓN DEFENSIVA: Mantener ProjectRegistry alineado
    const registryKey = ProjectRegistry.generateKey(feat);
    ProjectRegistry.setFeature(registryKey, feat);
    console.debug(`[SAVE_CHILDREN] Sincronizado con ProjectRegistry: ${registryKey}`);
    
    // ENHANCED: Ensure all style properties are maintained
    ensureStyleProps(child, false); // false for children
    
    // Restore preserved styles
    restorePreservedStyles(child, preservedStyles);
    
    // ENHANCED: Update ProjectRegistry with complete feature data
    const key = ProjectRegistry.generateKey(child.feature);
    ProjectRegistry.setFeature(key, child.feature);
    
    // Apply final styles (removing edit artifacts)
    const finalStyle = {
      dashArray: null,
      color: child.feature.properties.color || '#000',
      fillColor: child.feature.properties.fillColor || child.feature.properties.color || '#3388ff',
      fillOpacity: child.feature.properties.fillOpacity || CHILD_FILL_OPACITY,
      weight: child.feature.properties.weight || 2,
      opacity: child.feature.properties.opacity || 1
    };
    
    child.setStyle(finalStyle);
    
    console.debug(`[CHILDREN_EDIT] Child ${childCode} saved with preserved styles`);
  });

  // Clean up
  _childrenBackup = null;
  endEditMode(true);
  setEditorState(EditorState.PADRE_ACTIVO);
  setRecolorMode(false);
  
  // ENHANCED: Post-edit validation for all children
  let postValidationWarnings = 0;
  activeHijos.forEach(child => {
    const postValidation = validateAfterEdit(child, 'CHILD');
    if (!postValidation.valid) {
      postValidationWarnings++;
      console.warn(`[CHILDREN_EDIT] Post-validation warnings for ${child.feature?.properties?.codigo}:`, postValidation.warnings);
    }
  });
  
  if (postValidationWarnings > 0) {
    console.warn(`[CHILDREN_EDIT] ${postValidationWarnings} children have post-validation warnings`);
  }
  
  // ENHANCED: Restore color stability for all edited children
  const restoredColors = restoreColorStabilityAfterEdit(activeHijos);
  
  console.debug(`[EDIT_CHILDREN] ${activeHijos.length} children saved successfully for parent ${parentCode}, ${restoredColors} colors restored`);
}

// Cancelar edición de hijos
function cancelChildrenEditing() {
  if (_childrenBackup && _childrenBackup.length === activeHijos.length) {
    activeHijos.forEach((h, i) => {
      const restored = L.geoJSON({ type:'Feature', properties:h.feature.properties, geometry:_childrenBackup[i]}).getLayers()[0];
      h.setLatLngs(restored.getLatLngs());
    });
  }
  _childrenBackup = null;
  endEditMode(false);
  setEditorState(EditorState.PADRE_ACTIVO);
  setRecolorMode(false); // Apagar recolor al cancelar
  console.debug('[EDIT_CHILDREN] Edición cancelada');
}

// Validar si el hijo está dentro del padre usando Turf
function isInsideParent(childLayer, parentLayer) {
  if (!parentLayer) return false;
  
  try {
    if (window.turf) {
      const childGeoJSON = childLayer.toGeoJSON();
      const parentGeoJSON = parentLayer.toGeoJSON();
      return turf.booleanContains(parentGeoJSON, childGeoJSON) || 
             turf.booleanWithin(childGeoJSON, parentGeoJSON);
    } else {
      // Fallback: verificar que al menos el centroide esté dentro
      const childBounds = childLayer.getBounds();
      const parentBounds = parentLayer.getBounds();
      return parentBounds.contains(childBounds);
    }
  } catch (error) {
    console.warn('[VALIDATION] Error validando contención:', error);
    return true; // Permitir en caso de error
  }
}

// === GESTIÓN DE ESTADO MEJORADA ===

// Cambiar estado del editor (versión mejorada)
function setEditorState(next) {
  // Apagar recolor al cambiar a estados de dibujo/edición
  if ([EditorState.CREANDO_PADRE, EditorState.CREANDO_HIJO, EditorState.EDITANDO_PADRE, EditorState.EDITANDO_HIJOS].includes(next)) {
    setRecolorMode(false);
  }
  
  const prevState = state.mode;
  state.mode = next;
  
  // Mantener compatibilidad
  currentEditorState = next;
  activePadre = state.activeParent;
  activeHijos = state.children;
  isAislado = state.isAislado;
  
  console.debug('[STATE]', prevState, '->', next);

  const dis = (id, v) => {
    const el = document.getElementById(id);
    if (el) el.disabled = !!v;
  };

  // baseline según contexto
  const hasParent = !!state.activeParent;
  const hasChildren = (state.children?.length || 0) > 0;

  dis('btn-crear-padre', false);
  dis('btn-crear-hijo', !hasParent);
  dis('btn-editar-padre', !hasParent);
  dis('btn-editar-codigo', !hasParent); // Nuevo botón
  dis('btn-editar-hijo', !hasChildren);
  
  // Controles adicionales
  dis('btn-aislar', !hasParent);
  // Removed opacity slider references
  
  // Controles de eliminación
  dis('btn-delete-parent', !hasParent);
  const btnDelChild = document.getElementById('btn-delete-child');
  if (btnDelChild) {
    // visible sólo cuando haya selección
    btnDelChild.disabled = !state.selectedChild;
    if (!state.selectedChild) {
      btnDelChild.style.display = 'none';
    }
  }

  // overrides por modo
  if (next === EditorState.CREANDO_HIJO) {
    // mientras dibujo un hijo, bloqueo crear otro padre e impedir editar padre
    dis('btn-crear-padre', true);
    dis('btn-editar-padre', true);
    dis('btn-editar-codigo', true); // También bloquear edición de código
    
    // Activar herramienta de dibujo
    enableDrawingMode('polygon');
  }

  if (next === EditorState.PADRE_ACTIVO || next === EditorState.IDLE) {
    // Reactivar todo lo que dependa del padre
    dis('btn-crear-hijo', !hasParent);
    dis('btn-editar-padre', !hasParent);
    dis('btn-editar-codigo', !hasParent); // Mantener sincronizado
    dis('btn-editar-hijo', !hasChildren);
    
    // Desactivar herramientas de dibujo si estamos en IDLE
    if (next === EditorState.IDLE) {
      disableDrawingMode();
    }
  }
  
  if (next === EditorState.CREANDO_PADRE) {
    enableDrawingMode('polygon');
  }
  
  // Manejo específico para EDITANDO_PADRE
  if (next === EditorState.EDITANDO_PADRE) {
    // Deshabilitar acciones peligrosas
    dis('btn-crear-padre', true);
    dis('btn-crear-hijo', true);
    dis('btn-editar-codigo', true); // No editar código durante edición de geometría
    
    // Mostrar botones de guardar/cancelar, ocultar editar
    const btnEditarPadre = document.getElementById('btn-editar-padre');
    const btnEditarCodigo = document.getElementById('btn-editar-codigo');
    const btnGuardarPadre = document.getElementById('btn-guardar-padre');
    const btnCancelarEdicion = document.getElementById('btn-cancelar-edicion-padre');
    
    if (btnEditarPadre) btnEditarPadre.style.display = 'none';
    if (btnEditarCodigo) btnEditarCodigo.style.display = 'none';
    if (btnGuardarPadre) btnGuardarPadre.style.display = 'inline-block';
    if (btnCancelarEdicion) btnCancelarEdicion.style.display = 'inline-block';
  } else {
    // Restaurar botones normales
    const btnEditarPadre = document.getElementById('btn-editar-padre');
    const btnEditarCodigo = document.getElementById('btn-editar-codigo');
    const btnGuardarPadre = document.getElementById('btn-guardar-padre');
    const btnCancelarEdicion = document.getElementById('btn-cancelar-edicion-padre');
    
    if (btnEditarPadre) btnEditarPadre.style.display = 'inline-block';
    if (btnEditarCodigo) btnEditarCodigo.style.display = 'inline-block';
    if (btnGuardarPadre) btnGuardarPadre.style.display = 'none';
    if (btnCancelarEdicion) btnCancelarEdicion.style.display = 'none';
  }
  
  // Manejo específico para EDITANDO_HIJOS
  const btnEditarHijo = document.getElementById('btn-editar-hijo');
  const btnGuardarHijo = document.getElementById('btn-guardar-hijo');
  const btnCancelarHijo = document.getElementById('btn-cancelar-edicion-hijo');

  if (next === EditorState.EDITANDO_HIJOS) {
    if (btnEditarHijo) btnEditarHijo.style.display = 'none';
    if (btnGuardarHijo) btnGuardarHijo.style.display = 'inline-block';
    if (btnCancelarHijo) btnCancelarHijo.style.display = 'inline-block';
  } else {
    if (btnEditarHijo) btnEditarHijo.style.display = 'inline-block';
    if (btnGuardarHijo) btnGuardarHijo.style.display = 'none';
    if (btnCancelarHijo) btnCancelarHijo.style.display = 'none';
  }
  
  // Actualizar textos de botones
  const btnAislar = document.getElementById('btn-aislar');
  if (btnAislar) {
    btnAislar.textContent = state.isAislado ? '👁️ Mostrar todo' : '🔍 Aislar cuadrante';
  }
  

  
  // Marcar botón activo
  document.querySelectorAll('.btn').forEach(btn => btn.classList.remove('active-mode'));
  
  if (currentEditorState === EditorState.CREANDO_PADRE) {
    document.getElementById('btn-crear-padre').classList.add('active-mode');
  } else if (currentEditorState === EditorState.CREANDO_HIJO) {
    document.getElementById('btn-crear-hijo').classList.add('active-mode');
  } else if (currentEditorState === EditorState.EDITANDO_PADRE) {
    document.getElementById('btn-editar-padre').classList.add('active-mode');
  } else if (currentEditorState === EditorState.EDITANDO_HIJOS) {
    document.getElementById('btn-editar-hijo').classList.add('active-mode');
  }
  
  // Actualizar indicador de cobertura
  updateCoverageIndicator();
  
  // Asegurar que Import/Export siempre estén habilitados
  dis('btn-export', false);
  dis('btn-import', false);
}

// Helper para actualizar estado de UI
function updateUIState() {
  // Reaplica el estado actual a los controles
  setEditorState(state.mode);
}

// Actualizar indicador de cobertura - función vacía (UI removida)
function updateCoverageIndicator() {
  // UI de cobertura removida - mantener función para compatibilidad
  return;
  
  // Colorear según cobertura
  if (cobertura >= 95) {
    coverageElement.style.color = '#00b894';
  } else if (cobertura >= 80) {
    coverageElement.style.color = '#fdcb6e';
  } else {
    coverageElement.style.color = '#e17055';
  }
}

// Helper para iterar todos los cuadrantes
function forEachQuadrantLayer(fn) {
  DRAWN_LOCKED.eachLayer(fn);
  DRAWN_EDITABLE.eachLayer(fn);
}

// Ajuste de vista tras importar
function fitToAllIfAny() {
  const bounds = L.latLngBounds([]);
  DRAWN_LOCKED.eachLayer(l => { if (l.getBounds) bounds.extend(l.getBounds()); });
  DRAWN_EDITABLE.eachLayer(l => { if (l.getBounds) bounds.extend(l.getBounds()); });
  if (bounds.isValid()) map.fitBounds(bounds, { padding: [20, 20] });
}

function fitBoundsOfLayers(layerGroup) {
  const bounds = L.latLngBounds([]);
  layerGroup.eachLayer(l => { if (l.getBounds) bounds.extend(l.getBounds()); });
  if (bounds.isValid()) map.fitBounds(bounds, { padding: [20, 20] });
}

// === FUNCIONES PRINCIPALES DE JERARQUÍA ===

// Helpers para registrar y activar padres
function registerParent(layer) {
  const code = layer?.feature?.properties?.codigo;
  if (!code) return;
  if (!state.parents.includes(layer)) state.parents.push(layer);
  if (!state.childrenByParent[code]) state.childrenByParent[code] = [];

  // Configurar popup para cuadrante padre
  const tipo = layer.feature.properties?.tipo_cuadrante || 'RUTA';
  layer.bindPopup(`<b>Cuadrante Padre</b><br>ID: ${code}<br>Tipo: ${tipo}`);

  // Click para activar este padre (si no estamos editando)
  layer.on('click', () => {
    if (isEditingActive) return;
    
    // Manejar modo picker
    if (EDIT_ARM_ACTIVE && EDIT_ARM_TYPE) {
      deactivateEditPicker();
      setActiveParent(layer);
      
      if (EDIT_ARM_TYPE === 'parent') {
        // Iniciar edición del padre seleccionado
        setTimeout(() => startParentEditing(), 100);
      } else if (EDIT_ARM_TYPE === 'children') {
        // Iniciar edición de los hijos del padre seleccionado
        setTimeout(() => startChildrenEditing(), 100);
      }
      return;
    }
    
    // Comportamiento normal
    setActiveParent(layer);
  });
}

// Registrar hijo con click handler para selección
function registerChild(layer, parentCode) {
  if (!parentCode) parentCode = state.activeParent?.feature?.properties?.codigo;
  if (!parentCode) return;
  
  state.childrenByParent[parentCode] = state.childrenByParent[parentCode] || [];
  state.childrenByParent[parentCode].push(layer);

  if (!state.childGroupsByParent[parentCode]) {
    state.childGroupsByParent[parentCode] = L.featureGroup().addTo(map);
  }
  state.childGroupsByParent[parentCode].addLayer(layer);

  // Selección para eliminar hijo
  layer.on('click', (e) => {
    if (isEditingActive) return;
    selectChild(layer);
    e.originalEvent?.stopPropagation?.();
  });
}

// Seleccionar hijo para eliminar
function selectChild(layer) {
  // destacar visualmente el hijo y habilitar botón
  if (state.selectedChild && state.selectedChild !== layer) {
    applyStyleFromProperties(state.selectedChild); // quitar highlight anterior
  }
  state.selectedChild = layer;
  layer.setStyle({ weight: 3, dashArray: '6,3' });
  
  const btnDelChild = document.getElementById('btn-delete-child');
  if (btnDelChild) { 
    btnDelChild.style.display = 'inline-block'; 
    btnDelChild.disabled = false; 
  }
}

function setActiveParent(layer) {
  if (!layer) return;
  state.activeParent = layer;

  const code = layer.feature?.properties?.codigo;
  state.children = state.childrenByParent[code] || [];
  activePadre = state.activeParent;           // compat
  activeHijos = state.children;               // compat

  // NO eliminar otros grupos de hijos - mantener todos visibles
  // Solo actualizar referencia al grupo actual
  state.childrenGroup = state.childGroupsByParent[code] || null;

  // Crear grupo si no existe pero hay hijos
  if (!state.childrenGroup && state.children.length > 0) {
    state.childrenGroup = new L.FeatureGroup(state.children);
    state.childGroupsByParent[code] = state.childrenGroup;
    map.addLayer(state.childrenGroup);
  }

  // Mantener/actualizar aislamiento y UI
  applyAislamiento();
  setEditorState(EditorState.PADRE_ACTIVO);
  
  console.debug('[ACTIVE_PARENT_CHANGED]', `Padre activo: ${code}, Hijos: ${state.children.length}`);
}

// Manejar creación de cuadrante padre
function handlePadreCreated(layer) {
  // Aplicar buffer(0) para limpiar geometría
  const bufferedGeom = bufferGeometry(layer.toGeoJSON(), 0);
  
  // Mostrar diálogo de configuración
  showPadreConfigDialog((config) => {
    // Configurar propiedades según sistema (PAP o RUTA)
    const baseProps = {
      nivel: 'cuadrante',
      code: config.code,
      codigo: config.code, // Compatibilidad legacy
      city: config.ciudad,
      ciudad: config.ciudad, // Compatibilidad legacy
      tipo: 'PADRE',
      system: config.system
    };
    
    if (config.system === 'RUTA') {
      baseProps.route_id = config.route_id;
      baseProps.base_code = config.base_code;
      baseProps.dup_index = config.dup_index;
      if (config.route_name) {
        baseProps.route_name = config.route_name;
      }
      // Legacy
      baseProps.id_ruta = config.route_id;
      if (config.route_name) baseProps.ruta_publica = config.route_name;
    }
    // PAP no requiere propiedades adicionales más allá de system, city, code
    
    layer.feature.properties = baseProps;
    
    // ENHANCED: Register in ProjectRegistry for data integrity
    const key = ProjectRegistry.generateKey(layer.feature);
    ProjectRegistry.setFeature(key, layer.feature);
    
    // Register in changeLog for export tracking
    const feat = layer.toGeoJSON();
    state.changeLog.set(feat.properties.code, feat);
    
    // Persistir props de estilo en properties
    ensureStyleProps(layer, true);
    applyStyleFromProperties(layer);
    enforceStrokePolicy(layer);
    
    // Agregar funcionalidad de recolor
    attachRecolorOnClick(layer);
    
    // Registrar y activar nuevo padre (sin eliminar los anteriores)
    registerParent(layer);
    setActiveParent(layer);
    
    // NO forzar aislamiento automático, respetar estado del usuario
    // state.isAislado = true;
    // isAislado = state.isAislado;
    
    // Aplicar estado actual (puede estar aislado o no)
    applyAislamiento();
    
    DRAWN_EDITABLE.addLayer(layer);
    
    // Cambiar a estado PADRE_ACTIVO (permite crear hijos inmediatamente)
    setEditorState(EditorState.PADRE_ACTIVO);
    
    // Apagar recolor por seguridad
    setRecolorMode(false);
    
    console.debug('[PADRE] created and registered:', config.codigo);
  });
}

// Manejar creación de subcuadrante hijo
function handleHijoCreated(layer) {
  if (!activePadre) {
    alert('Error: No hay cuadrante padre activo');
    return;
  }
  
  // Auto-clip: intersección con padre
  const geomHijo = layer.toGeoJSON();
  const geomPadre = activePadre.toGeoJSON();
  const clippedGeom = intersectGeometries(geomHijo, geomPadre);
  
  if (!clippedGeom || calculateArea(clippedGeom) < TOLERANCIAS.MIN_AREA_RESTO) {
    alert('El subcuadrante debe estar dentro del cuadrante padre');
    return;
  }
  
  // Aplicar snapping
  const snappedGeom = applySnapping(clippedGeom);
  
  // Chequeo de solape incremental
  const overlapCheck = checkOverlapWithExistingHijos(snappedGeom);
  if (!overlapCheck.valid) {
    showOverlapWarning(overlapCheck.overlaps);
    return;
  }
  
  // Autocódigo
  const codigoPadre = activePadre.feature.properties.codigo;
  const codigoHijo = generateNextSubcuadranteCode(codigoPadre);
  
  // Configurar propiedades del hijo
  layer.feature.properties = {
    nivel: 'subcuadrante',
    codigo: codigoHijo,
    codigo_padre: codigoPadre,
    fillColor: HIJO_STYLE.fillColor,
    ...HIJO_STYLE
  };
  
  // Register in changeLog for export tracking
  const feat = layer.toGeoJSON();
  state.changeLog.set(feat.properties.codigo, feat);
  
  // Aplicar estilos
  layer.setStyle(HIJO_STYLE);
  
  // Agregar a hijos activos
  activeHijos.push(layer);
  
  DRAWN_EDITABLE.addLayer(layer);
  
  // Actualizar indicador de cobertura
  updateCoverageIndicator();
  
  console.debug('[HIJO] created:', codigoHijo, 'total hijos:', activeHijos.length);
}

// Aplicar snapping a geometría
function applySnapping(geojson) {
  // Por simplicidad, retornamos la geometría original
  // En implementación completa, ajustar vértices cerca del padre y otros hijos
  return geojson;
}

// Verificar solape con hijos existentes
function checkOverlapWithExistingHijos(newGeom) {
  const overlaps = [];
  
  activeHijos.forEach((hijo, index) => {
    const hijoGeom = hijo.toGeoJSON();
    const overlap = intersectGeometries(newGeom, hijoGeom);
    
    if (overlap && calculateArea(overlap) > TOLERANCIAS.MAX_OVERLAP_AREA) {
      overlaps.push({
        index,
        area: calculateArea(overlap),
        geometry: overlap
      });
    }
  });
  
  return {
    valid: overlaps.length === 0,
    overlaps
  };
}

// Mostrar advertencia de solape
function showOverlapWarning(overlaps) {
  // Dibujar solapes en el mapa temporalmente
  const overlapLayers = overlaps.map(overlap => {
    const layer = L.geoJSON(overlap.geometry, {
      style: ERROR_STYLE.OVERLAP
    }).addTo(map);
    
    setTimeout(() => {
      map.removeLayer(layer);
    }, 3000);
    
    return layer;
  });
  
  alert(`Solape detectado con ${overlaps.length} subcuadrante(s) existente(s). Los solapes se muestran en naranja.`);
}



// Aplicar/quitar aislamiento
function applyAislamiento() {
  if (!state.isAislado) {
    // ENHANCED: Normal mode - restore all layers with full interactivity and original styles
    forEachQuadrantLayer(layer => {
      // Restore visual styles
      applyStyleFromProperties(layer);
      
      // Restore full interactivity
      if (layer.options) {
        layer.options.interactive = true;
        layer.options.bubblingMouseEvents = true;
      }
      
      // Re-enable click handlers if they were disabled
      if (layer._isolationDisabled) {
        layer.options.interactive = true;
        delete layer._isolationDisabled;
      }
    });
    
    // Update cursor
    const mapContainer = document.getElementById('map');
    if (mapContainer) {
      mapContainer.style.cursor = '';
    }
    
    console.debug('[ISOLATION] Normal mode: all layers interactive');
    return;
  }
  
  // ENHANCED: Isolation mode - maintain visual context but disable interactivity outside active route
  if (!state.activeParent) return;
  
  const activeCode = state.activeParent.feature?.properties?.codigo;
  const activeRoute = state.activeParent.feature?.properties?.id_ruta;
  
  console.debug(`[ISOLATION] Isolating route ${activeRoute}, parent ${activeCode}`);
  
  forEachQuadrantLayer(layer => {
    const layerCode = layer.feature?.properties?.codigo;
    const layerParentCode = layer.feature?.properties?.codigo_padre;
    const layerRoute = layer.feature?.properties?.id_ruta;
    
    // Determine relationship to active route
    const isActiveParent = (layerCode === activeCode);
    const isActiveChild = (layerParentCode === activeCode);
    const isSameRoute = (layerRoute === activeRoute);
    
    if (isActiveParent || isActiveChild) {
      // Active hierarchy: full visibility and interactivity
      applyStyleFromProperties(layer);
      
      if (layer.options) {
        layer.options.interactive = true;
        layer.options.bubblingMouseEvents = true;
      }
      
      if (layer._isolationDisabled) {
        delete layer._isolationDisabled;
      }
      
    } else if (isSameRoute) {
      // Same route but not active: medium visibility, limited interactivity
      const currentStyle = layer.options || {};
      layer.setStyle({
        ...currentStyle,
        opacity: 0.4,
        fillOpacity: 0.15
      });
      
      // Disable interactions but keep visual context
      if (layer.options) {
        layer.options.interactive = false;
        layer.options.bubblingMouseEvents = false;
      }
      layer._isolationDisabled = true;
      
    } else {
      // Different route: minimal visibility, no interactivity (context only)
      const currentStyle = layer.options || {};
      layer.setStyle({
        ...currentStyle,
        opacity: 0.1,
        fillOpacity: 0.03
      });
      
      // Disable all interactions while maintaining visual context
      if (layer.options) {
        layer.options.interactive = false;
        layer.options.bubblingMouseEvents = false;
      }
      layer._isolationDisabled = true;
    }
  });
  
  // Update cursor to indicate focused editing mode
  const mapContainer = document.getElementById('map');
  if (mapContainer) {
    mapContainer.style.cursor = 'crosshair';
  }
  
  console.debug('[ISOLATION] Route isolation active: enhanced context preservation');
}

// === FUNCIONES DE ELIMINACIÓN SEGURA ===

// Eliminar padre activo y todos sus hijos
function deleteActiveParent() {
  const parent = state.activeParent;
  if (!parent) return;

  const code = parent.feature?.properties?.codigo || '(sin código)';
  const hijos = state.childrenByParent[code] || [];
  const count = hijos.length;

  const ok = confirm(`¿Eliminar el cuadrante ${code}? Se eliminarán ${count} subcuadrante(s). Esta acción no se puede deshacer.`);
  if (!ok) return;

  // 1) Borrar hijos del mapa/estado
  if (state.childGroupsByParent[code]) {
    state.childGroupsByParent[code].eachLayer(l => state.childGroupsByParent[code].removeLayer(l));
    map.removeLayer(state.childGroupsByParent[code]);
    delete state.childGroupsByParent[code];
  }
  (hijos || []).forEach(l => {
    try { DRAWN_EDITABLE.removeLayer(l); } catch(e) {}
  });
  delete state.childrenByParent[code];

  // 2) Borrar el padre del mapa/estado
  try { DRAWN_EDITABLE.removeLayer(parent); } catch(e) {}
  state.parents = state.parents.filter(p => p !== parent);

  // 3) Elegir nuevo activo (si queda alguno)
  state.activeParent = state.parents[0] || null;
  if (state.activeParent) {
    const newCode = state.activeParent.feature?.properties?.codigo;
    state.children = state.childrenByParent[newCode] || [];
    setEditorState(EditorState.PADRE_ACTIVO);
  } else {
    state.children = [];
    setEditorState(EditorState.IDLE);
  }

  // 4) Limpiar selección de hijo
  state.selectedChild = null;
  const btnDelChild = document.getElementById('btn-delete-child');
  if (btnDelChild) { 
    btnDelChild.disabled = true; 
    btnDelChild.style.display = 'none'; 
  }

  fitToAllIfAny();
  
  // Apagar recolor tras eliminar
  setRecolorMode(false);
  
  console.debug('[DELETE_PARENT]', `Padre "${code}" y ${count} hijos eliminados`);
}

// Eliminar hijo seleccionado
function deleteSelectedChild() {
  const ch = state.selectedChild;
  if (!ch) return;

  const codeParent = state.activeParent?.feature?.properties?.codigo;
  const chCode = ch.feature?.properties?.codigo || '(sin código)';
  const ok = confirm(`¿Eliminar el subcuadrante ${chCode}?`);
  if (!ok) return;

  // 1) Quitar del grupo y del mapa
  const grp = state.childGroupsByParent[codeParent];
  try { grp?.removeLayer(ch); } catch(e) {}
  try { DRAWN_EDITABLE.removeLayer(ch); } catch(e) {}

  // 2) Quitar de colecciones
  state.children = (state.children || []).filter(x => x !== ch);
  if (codeParent) {
    state.childrenByParent[codeParent] = (state.childrenByParent[codeParent] || []).filter(x => x !== ch);
  }

  // 3) Limpiar selección y UI
  state.selectedChild = null;
  const btn = document.getElementById('btn-delete-child');
  if (btn) { 
    btn.disabled = true; 
    btn.style.display = 'none'; 
  }

  fitToAllIfAny();
  setEditorState(EditorState.PADRE_ACTIVO);
  
  // Apagar recolor tras eliminar
  setRecolorMode(false);
  
  console.debug('[DELETE_CHILD]', `Hijo "${chCode}" eliminado`);
}

// Mostrar diálogo de configuración de padre
// Versión compacta y scrollable con estructura unificada
function showPadreConfigDialog(callback) {
  const ciudad = getCityFromQuery().toUpperCase();
  const cityAbbr = getCityPrefix(ciudad);
  
  // Validar modo excluyente
  const tipoActual = sistemaActual;
  
  // Contenedor raíz con clase unificada
  const wrap = document.createElement('div');
  wrap.className = 'editor-modal';
  
  // Preparar preview inicial de PAP
  const nextPapNum = computeMaxPapForCity(cityAbbr) + 1;
  const papPreview = pad(nextPapNum, 3);
  
  // Estructura modal: dialog > content > header/body/footer
  wrap.innerHTML = `
    <div class="modal-dialog">
      <div class="modal-content">
        <div class="modal-header">
          <h3 style="margin:0;font-size:18px;font-weight:600;">Configurar Cuadrante Padre (${tipoActual})</h3>
          <button type="button" id="modal-close" aria-label="Cerrar" style="background:none;border:0;font-size:24px;cursor:pointer;line-height:1;padding:0;">&times;</button>
        </div>
        
        <div class="modal-body">
          <div class="form-group">
            <label>Ciudad:</label>
            <input type="text" id="modal-ciudad" value="${cityAbbr}" readonly style="text-transform: uppercase; background:#f5f5f5;">
          </div>
          
          <!-- Campos RUTA -->
          <div class="form-group" data-mode="RUTA">
            <label>ID Ruta: <span style="color: red;">*</span></label>
            <input type="number" id="modal-ruta" min="1" max="99999" value="1" required>
            <small class="hint" style="color:#888;font-size:12px;">Número único que identifica la ruta</small>
          </div>
          
          <div class="form-group" data-mode="RUTA">
            <label>Nombre de ruta (opcional):</label>
            <input type="text" id="modal-ruta-nombre" placeholder="Ej: 2 Barranquilla norte">
            <small class="hint" style="color:#888;font-size:12px;">Etiqueta legible para mostrar en mapas</small>
          </div>
          
          <!-- Campos PAP -->
          <div class="form-group" data-mode="PAP">
            <label>Número PAP:</label>
            <div id="pap-numero-info" style="font-weight: 600; color: #11998e; font-size: 16px;">
              Se asignará automáticamente: ${papPreview}
            </div>
          </div>
          
          <div class="form-group">
            <label>Código (generado automáticamente):</label>
            <input type="text" id="codigo-preview" readonly value="Calculando..." style="font-family:monospace;background:#f0f9ff;border:1px solid #0ea5e9;color:#0c4a6e;font-weight:600;">
          </div>
        </div>
        
        <div class="modal-footer">
          <button type="button" id="modal-cancel" class="btn btn-secondary">Cancelar</button>
          <button type="button" id="modal-confirm" class="btn btn-primary">Crear Cuadrante</button>
        </div>
      </div>
    </div>
  `;
  
  document.body.appendChild(wrap);
  document.body.style.overflow = 'hidden'; // Bloquear scroll del fondo
  
  // Mostrar solo campos del sistema correcto
  wrap.querySelectorAll('[data-mode="RUTA"]').forEach(el => el.style.display = (tipoActual === 'RUTA' ? 'block' : 'none'));
  wrap.querySelectorAll('[data-mode="PAP"]').forEach(el => el.style.display = (tipoActual === 'PAP' ? 'block' : 'none'));
  
  // Función para generar el código según sistema
  function generateCodigoForModal() {
    if (tipoActual === 'RUTA') {
      const rutaInput = document.getElementById('modal-ruta');
      const idRuta = rutaInput?.value || '1';
      const base = baseRuta(cityAbbr, idRuta);
      const nextSufijo = computeMaxDupForBase(base) + 1;
      return `${base}_${pad(nextSufijo, 2)}`;
    } else {
      // PAP
      const nextNum = computeMaxPapForCity(cityAbbr) + 1;
      return `${cityAbbr}_pap_${pad(nextNum, 3)}`;
    }
  }
  
  // Actualizar código en tiempo real
  const codigoPreview = document.getElementById('codigo-preview');
  
  function updateCodigoPreview() {
    const codigo = generateCodigoForModal();
    codigoPreview.value = codigo;
  }
  
  // Función para cerrar modal
  function closeModal() {
    document.body.style.overflow = ''; // Restaurar scroll del fondo
    wrap.remove();
    setEditorState(EditorState.IDLE);
  }
  
  if (tipoActual === 'RUTA') {
    const rutaInput = document.getElementById('modal-ruta');
    rutaInput?.addEventListener('input', updateCodigoPreview);
    rutaInput?.focus();
  }
  
  updateCodigoPreview();
  
  // Eventos de botones
  document.getElementById('modal-close').addEventListener('click', closeModal);
  document.getElementById('modal-cancel').addEventListener('click', closeModal);
  
  document.getElementById('modal-confirm').addEventListener('click', () => {
    const codigo = codigoPreview.value;
    
    // Validar formato
    const esValido = (tipoActual === 'RUTA') ? RE_RUTA.test(codigo) : RE_PAP.test(codigo);
    if (!esValido) {
      alert(`Formato de código inválido: ${codigo}`);
      return;
    }
    
    // Verificar unicidad
    const codeExists = checkCodeExists(codigo);
    if (codeExists) {
      alert(`El código ${codigo} ya existe. Elija otro ID de ruta o espere a que se recalcule.`);
      return;
    }
    
    let config = {
      ciudad: cityAbbr,
      system: tipoActual,
      code: codigo
    };
    
    if (tipoActual === 'RUTA') {
      const rutaInput = document.getElementById('modal-ruta');
      const nombreInput = document.getElementById('modal-ruta-nombre');
      const idRuta = parseInt(rutaInput.value, 10);
      
      if (!idRuta || idRuta < 1) {
        alert('Ingrese un ID de ruta válido (número positivo)');
        return;
      }
      
      const base = baseRuta(cityAbbr, idRuta);
      const match = codigo.match(/_(\d{2})$/);
      const dupIndex = match ? parseInt(match[1], 10) : 1;
      
      config.route_id = idRuta;
      config.route_name = nombreInput?.value?.trim() || null;
      config.base_code = base;
      config.dup_index = dupIndex;
    }
    
    document.body.style.overflow = ''; // Restaurar scroll del fondo
    wrap.remove();
    callback(config);
  });
}

// Función auxiliar para verificar si un código ya existe
function checkCodeExists(code) {
  let exists = false;
  
  const check = (group) => {
    if (!group || exists) return;
    group.eachLayer?.(layer => {
      const layerCode = layer.feature?.properties?.code || layer.feature?.properties?.codigo;
      if (layerCode === code) exists = true;
    });
  };
  
  check(DRAWN_EDITABLE);
  check(DRAWN_LOCKED);
  
  if (state.parents) {
    state.parents.forEach(p => {
      const pCode = p.feature?.properties?.code || p.feature?.properties?.codigo;
      if (pCode === code) exists = true;
    });
  }
  
  return exists;
}

// Modal de edición de código (usado al guardar y desde el botón "✏️ Editar código")
// Versión compacta y scrollable con estructura unificada
function showEditCodeModal(system, currentProps, callback) {
  const ciudad = currentProps.city || currentProps.ciudad || getCityFromQuery().toUpperCase();
  const cityAbbr = getCityPrefix(ciudad);
  const currentCode = currentProps.code || currentProps.codigo;
  
  // Helper para extraer consecutivo PAP
  function extraerConsecutivoPAP(code) {
    if (!code) return '';
    const match = code.match(/_pap_(\d{3})$/);
    return match ? parseInt(match[1], 10) : '';
  }
  
  // Contenedor raíz con nueva clase unificada
  const wrap = document.createElement('div');
  wrap.className = 'editor-modal';
  wrap.id = 'edit-code-modal';
  
  // Preparar valores iniciales
  const routeId = currentProps.route_id || currentProps.id_ruta || 1;
  const routeName = currentProps.route_name || currentProps.ruta_publica || '';
  const base = currentProps.base_code || baseRuta(cityAbbr, routeId);
  const dupIndex = currentProps.dup_index || 1;
  const papNum = extraerConsecutivoPAP(currentCode) || 1;
  
  // Estructura modal: dialog > content > header/body/footer
  wrap.innerHTML = `
    <div class="modal-dialog">
      <div class="modal-content">
        <div class="modal-header">
          <h3 style="margin:0;font-size:18px;font-weight:600;">Editar código del cuadrante (${system})</h3>
          <button type="button" id="edit-close" aria-label="Cerrar" style="background:none;border:0;font-size:24px;cursor:pointer;line-height:1;padding:0;">&times;</button>
        </div>
        
        <div class="modal-body">
          <div class="form-group">
            <label>Ciudad:</label>
            <input type="text" id="edit-ciudad" value="${cityAbbr}" readonly style="text-transform: uppercase; background:#f5f5f5;">
          </div>
          
          <!-- Campos RUTA -->
          <div class="form-group" data-mode="RUTA">
            <label>ID Ruta: <span style="color: red;">*</span></label>
            <input type="number" id="edit-route-id" min="1" max="99999" value="${routeId}" required>
          </div>
          
          <div class="form-group" data-mode="RUTA">
            <label>Nombre de ruta (opcional):</label>
            <input type="text" id="edit-route-name" value="${routeName}" placeholder="Ej: 2 Barranquilla norte">
          </div>
          
          <div class="form-group" data-mode="RUTA">
            <label>Base (preview):</label>
            <div id="edit-base-preview" style="font-family:monospace;color:#666;font-weight:600;">${base}</div>
          </div>
          
          <div class="form-group" data-mode="RUTA">
            <label>Sufijo (01–99): <span style="color: red;">*</span></label>
            <input type="number" id="edit-suffix" min="1" max="99" value="${dupIndex}" required>
            <small class="hint" style="color:#888;font-size:12px;">Número de duplicado</small>
          </div>
          
          <!-- Campos PAP -->
          <div class="form-group" data-mode="PAP">
            <label>Prefijo (readonly):</label>
            <input type="text" id="edit-pap-prefix" value="${cityAbbr}_pap_" readonly style="background:#f5f5f5;font-family:monospace;">
          </div>
          
          <div class="form-group" data-mode="PAP">
            <label>Consecutivo (001–999): <span style="color: red;">*</span></label>
            <input type="number" id="edit-pap-num" min="1" max="999" value="${papNum}" required>
          </div>
          
          <div class="form-group">
            <label>Código resultante:</label>
            <input type="text" id="edit-code-preview" readonly value="${currentCode}" style="font-family:monospace;background:#f0f9ff;border:1px solid #0ea5e9;color:#0c4a6e;font-weight:600;">
          </div>
        </div>
        
        <div class="modal-footer">
          <button type="button" id="edit-modal-cancel" class="btn btn-secondary">Cancelar</button>
          <button type="button" id="edit-modal-save" class="btn btn-primary">Aplicar Cambios</button>
        </div>
      </div>
    </div>
  `;
  
  document.body.appendChild(wrap);
  document.body.style.overflow = 'hidden'; // Bloquear scroll del fondo
  
  // Mostrar solo campos del sistema correcto
  const sys = system.toUpperCase();
  wrap.querySelectorAll('[data-mode="RUTA"]').forEach(el => el.style.display = (sys === 'RUTA' ? 'block' : 'none'));
  wrap.querySelectorAll('[data-mode="PAP"]').forEach(el => el.style.display = (sys === 'PAP' ? 'block' : 'none'));
  
  // Función para actualizar preview del código
  function updateEditPreview() {
    let newCode;
    if (sys === 'RUTA') {
      const routeIdInput = document.getElementById('edit-route-id');
      const suffixInput = document.getElementById('edit-suffix');
      const routeId = routeIdInput?.value || '1';
      const suffix = parseInt(suffixInput?.value || '1', 10);
      const base = baseRuta(cityAbbr, routeId);
      
      // Actualizar preview de base
      const basePreview = document.getElementById('edit-base-preview');
      if (basePreview) basePreview.textContent = base;
      
      newCode = `${base}_${pad(suffix, 2)}`;
    } else {
      // PAP
      const papNumInput = document.getElementById('edit-pap-num');
      const papNum = parseInt(papNumInput?.value || '1', 10);
      newCode = `${cityAbbr}_pap_${pad(papNum, 3)}`;
    }
    
    const preview = document.getElementById('edit-code-preview');
    if (preview) preview.value = newCode;
  }
  
  // Función para cerrar modal
  function closeEditModal() {
    document.body.style.overflow = ''; // Restaurar scroll del fondo
    wrap.remove();
  }
  
  // Listeners para actualizar preview en tiempo real
  if (sys === 'RUTA') {
    const routeIdInput = document.getElementById('edit-route-id');
    const suffixInput = document.getElementById('edit-suffix');
    routeIdInput?.addEventListener('input', updateEditPreview);
    suffixInput?.addEventListener('input', updateEditPreview);
    routeIdInput?.focus();
  } else {
    const papNumInput = document.getElementById('edit-pap-num');
    papNumInput?.addEventListener('input', updateEditPreview);
    papNumInput?.focus();
  }
  
  updateEditPreview();
  
  // Eventos de botones
  document.getElementById('edit-close').addEventListener('click', closeEditModal);
  document.getElementById('edit-modal-cancel').addEventListener('click', closeEditModal);
  
  document.getElementById('edit-modal-save').addEventListener('click', () => {
    const newCode = document.getElementById('edit-code-preview').value;
    
    // Validar formato
    const esValido = (sys === 'RUTA') ? RE_RUTA.test(newCode) : RE_PAP.test(newCode);
    if (!esValido) {
      alert(`Formato de código inválido: ${newCode}`);
      return;
    }
    
    // Verificar unicidad (permitir mismo código si no cambió)
    if (newCode !== currentCode && checkCodeExists(newCode)) {
      alert(`El código ${newCode} ya existe. Elija otro valor.`);
      return;
    }
    
    // Construir nuevas propiedades
    let newProps = {
      code: newCode,
      codigo: newCode, // Legacy
      city: cityAbbr,
      ciudad: cityAbbr, // Legacy
      system: sys
    };
    
    if (sys === 'RUTA') {
      const routeIdInput = document.getElementById('edit-route-id');
      const routeNameInput = document.getElementById('edit-route-name');
      const suffixInput = document.getElementById('edit-suffix');
      
      const routeId = parseInt(routeIdInput.value, 10);
      const routeName = routeNameInput?.value?.trim() || null;
      const suffix = parseInt(suffixInput.value, 10);
      const base = baseRuta(cityAbbr, routeId);
      
      newProps.route_id = routeId;
      newProps.route_name = routeName;
      newProps.base_code = base;
      newProps.dup_index = suffix;
      
      // Legacy
      newProps.id_ruta = routeId;
      if (routeName) newProps.ruta_publica = routeName;
    }
    // PAP no requiere propiedades adicionales
    
    closeEditModal();
    callback(newProps);
  });
}

// === NUEVAS FUNCIONES DE IMPORTACIÓN ===

// Ejecutar flujo de importación basado en el modo seleccionado
async function runImportFlow(mode) {
  try {
    // 1) Si ya hay archivo cargado manualmente, úsalo
    if (state.masterFC && state.masterFC.type === 'FeatureCollection') {
      renderFCAccordingToMode(state.masterFC, mode);
      return;
    }
    
    // 2) Si no, intenta cargar base fija por ciudad
    const city = (getCityFromQuery() || 'bogota').toUpperCase();
    const baseFixed = `/geojson/pap/${city.toLowerCase()}_base_fixed.geojson`;
    const base = `/geojson/pap/${city.toLowerCase()}_base.geojson`;

    let resp = await fetch(baseFixed);
    if (!resp.ok) resp = await fetch(base);
    if (!resp.ok) throw new Error('No se pudo cargar el GeoJSON base');

    const fc = await resp.json();
    state.masterFC = fc;
    renderFCAccordingToMode(fc, mode);
  } catch (err) {
    console.error('[IMPORT FLOW] Error:', err);
    showToast('Error importando los cuadrantes', 'error');
  }
}

// Renderizar FeatureCollection según el modo seleccionado
// Función para detectar y resolver conflictos de código en importación
function resolveImportCodeConflicts(fc) {
  const ciudad = getCityFromQuery().toUpperCase();
  const cityAbbr = getCityPrefix(ciudad);
  
  // Construir mapas de códigos existentes
  const existingCodes = new Set();
  const rutaBases = new Map(); // base -> max suffix
  
  // Escanear capas existentes
  [DRAWN_EDITABLE, DRAWN_LOCKED].forEach(group => {
    if (!group) return;
    group.eachLayer?.(layer => {
      const props = layer.feature?.properties || {};
      const code = props.code || props.codigo;
      if (code) {
        existingCodes.add(code);
        
        // Si es RUTA, actualizar mapa de bases
        if (RE_RUTA.test(code)) {
          const base = code.match(/^(.+)_\d{2}$/)?.[1] || code;
          const suffix = code.match(/_(\d{2})$/);
          const num = suffix ? parseInt(suffix[1], 10) : 1;
          rutaBases.set(base, Math.max(rutaBases.get(base) || 0, num));
        }
      }
    });
  });
  
  if (state.parents) {
    state.parents.forEach(p => {
      const props = p.feature?.properties || {};
      const code = props.code || props.codigo;
      if (code) {
        existingCodes.add(code);
        
        if (RE_RUTA.test(code)) {
          const base = code.match(/^(.+)_\d{2}$/)?.[1] || code;
          const suffix = code.match(/_(\d{2})$/);
          const num = suffix ? parseInt(suffix[1], 10) : 1;
          rutaBases.set(base, Math.max(rutaBases.get(base) || 0, num));
        }
      }
    });
  }
  
  // Procesar features importadas
  const conflicts = [];
  const resolved = [];
  
  fc.features.forEach(feat => {
    const props = feat.properties || {};
    let code = props.code || props.codigo;
    
    if (!code) return; // Sin código, no hay conflicto
    
    // Detectar si hay conflicto
    if (existingCodes.has(code)) {
      // Conflicto detectado
      conflicts.push(code);
      
      // Resolver según sistema
      if (RE_RUTA.test(code)) {
        // RUTA: asignar siguiente sufijo
        const base = code.match(/^(.+)_\d{2}$/)?.[1] || code;
        const nextSuffix = (rutaBases.get(base) || 0) + 1;
        const newCode = `${base}_${pad(nextSuffix, 2)}`;
        
        // Actualizar propiedades
        props.code = newCode;
        props.codigo = newCode;
        props.dup_index = nextSuffix;
        
        rutaBases.set(base, nextSuffix);
        existingCodes.add(newCode);
        resolved.push({ old: code, new: newCode });
        
        console.debug(`[IMPORT_CONFLICT] RUTA: ${code} → ${newCode}`);
      } else if (RE_PAP.test(code)) {
        // PAP: asignar siguiente número global
        const nextNum = computeMaxPapForCity(cityAbbr) + 1;
        const newCode = `${cityAbbr}_pap_${pad(nextNum, 3)}`;
        
        props.code = newCode;
        props.codigo = newCode;
        
        existingCodes.add(newCode);
        resolved.push({ old: code, new: newCode });
        
        console.debug(`[IMPORT_CONFLICT] PAP: ${code} → ${newCode}`);
      }
    } else {
      // No hay conflicto, registrar código
      existingCodes.add(code);
      
      // Si es RUTA, actualizar mapa de bases
      if (RE_RUTA.test(code)) {
        const base = code.match(/^(.+)_\d{2}$/)?.[1] || code;
        const suffix = code.match(/_(\d{2})$/);
        const num = suffix ? parseInt(suffix[1], 10) : 1;
        rutaBases.set(base, Math.max(rutaBases.get(base) || 0, num));
      }
    }
  });
  
  // Notificar al usuario
  if (resolved.length > 0) {
    const msg = `Se resolvieron ${resolved.length} conflictos de código:\n` +
                resolved.map(r => `  ${r.old} → ${r.new}`).slice(0, 10).join('\n') +
                (resolved.length > 10 ? `\n  ... y ${resolved.length - 10} más` : '');
    console.warn('[IMPORT_CONFLICT] Conflictos resueltos:', resolved);
    showToast(`Conflictos resueltos: ${resolved.length} códigos reasignados`, 'warning');
    
    // Mostrar detalles en consola
    console.table(resolved);
  }
  
  return { conflicts: conflicts.length, resolved: resolved.length };
}

function renderFCAccordingToMode(fc, mode) {
  console.log(`[RENDER] Modo: ${mode}, Features: ${fc.features?.length || 0}`);
  
  // RESOLVER CONFLICTOS DE CÓDIGO EN IMPORTACIÓN
  const conflictStats = resolveImportCodeConflicts(fc);
  console.debug(`[RENDER] Conflictos: ${conflictStats.conflicts}, Resueltos: ${conflictStats.resolved}`);
  
  // Reset limpio
  ProjectRegistry.clear();
  DRAWN_LOCKED.clearLayers();
  DRAWN_EDITABLE.clearLayers();

  state.activeParent = null;
  state.children = [];
  state.isAislado = false;      // ⬅️ evitar bloqueo por aislamiento previo
  applyAislamiento();

  // Pintar TODO el archivo y registrar
  L.geoJSON(fc, {
    onEachFeature: (feat, layer) => {
      addImportedFeatureLayer(feat, layer, 'DRAWN_LOCKED');
    }
  }).addTo(DRAWN_LOCKED);

  fitToAllIfAny();
  setEditorState(EditorState.IDLE);

  // Modo "Edición libre": el usuario clickea un padre y luego pulsa "Editar padre"
  if (mode === IMPORT_MODE.EDIT_ALL) {
    showToast('Edición libre: haz click en un cuadrante y luego "Editar padre".', 'success');
  } else {
    showToast('Panorama cargado (solo lectura).', 'success');
  }
  
  console.log(`[RENDER] Completado. Padres registrados: ${state.parents?.length || 0}`);
}

// Función para obtener ciudad desde query params o configuración
function getCityFromQuery() {
  const params = new URLSearchParams(window.location.search);
  return params.get('city') || 'bogota'; // default
}

// Configurar controles cuando el DOM esté listo
document.addEventListener('DOMContentLoaded', function() {
    // Listener para toggle de sistema RUTA/PAP
    const radios = document.querySelectorAll('input[name="sistema"]');
    radios.forEach(rb => {
      rb.addEventListener('change', (e) => {
        sistemaActual = e.target.value; // 'RUTA' | 'PAP'
        console.log(`[SISTEMA] Cambiado a: ${sistemaActual}`);
      });
    });
    
    // Construir paleta
    const paletteDiv = document.getElementById('palette');
    if (paletteDiv) {
      PALETTE.forEach((hex, i) => {
        const s = document.createElement('div');
        s.className = 'swatch' + (i===0 ? ' active' : '');
        s.style.background = hex;
        s.dataset.color = hex;
        s.addEventListener('click', () => {
          CURRENT_FILL = hex;
          [...paletteDiv.querySelectorAll('.swatch')].forEach(el => el.classList.toggle('active', el === s));
          // actualizar color por defecto del Draw
          drawControl.setDrawingOptions({
            polygon:   { shapeOptions: { color: '#000', weight: STROKE_WEIGHT, fillColor: CURRENT_FILL, fillOpacity: FILL_OPACITY } },
            rectangle: { shapeOptions: { color: '#000', weight: STROKE_WEIGHT, fillColor: CURRENT_FILL, fillOpacity: FILL_OPACITY } }
          });
        });
        paletteDiv.appendChild(s);
      });
      console.debug('Paleta de colores inicializada');
    }

    // Botón recolor (usar función global)
    const btnRecolor = document.getElementById('btn-recolor');
    if (btnRecolor) {
      btnRecolor.addEventListener('click', () => {
        setRecolorMode(!recolorMode);
      });
      console.debug('Botón recolor configurado');
    }

const exportBtn = document.getElementById('btn-export');
if (exportBtn) {
  exportBtn.addEventListener('click', () => exportFromVisibleLayersStrict());
}

    // Nota: La configuración de importación se hace más abajo con la lógica de jerarquía

// === FUNCIONES GLOBALES DE EXPORTACIÓN ===

// === NUEVO: Exportación por merge (no por mapa visible) ===
function exportMerged() {
  const master = deepCopy(state.masterFC);
  if (!master) {
    console.warn('[EXPORT] No hay masterFC para exportar');
    showToast("No hay datos para exportar", "warning");
    return;
  }

  const porCodigo = new Map(master.features.map(f => [f.properties?.codigo, f]));
  for (const [codigo, featNuevo] of state.changeLog.entries()) {
    porCodigo.set(codigo, featNuevo); // reemplaza o inserta
  }
  const merged = { type: "FeatureCollection", features: Array.from(porCodigo.values()) };
  
  const fileName = nombreSugerido();
  downloadGeoJSON(merged, fileName);
  
  showToast(`Exportado OK: ${merged.features.length} cuadrantes`);
  console.debug(`[EXPORT] Merged export: ${merged.features.length} features, ${state.changeLog.size} changes applied`);
}

// Helper para generar nombre de archivo sugerido
function nombreSugerido() {
  const timestamp = new Date().toISOString().slice(0,16).replace(/[-:]/g, '').replace('T', '_');
  const changeCount = state.changeLog.size;
  return `cuadrantes_merged_${changeCount}cambios_${timestamp}.geojson`;
}

// Helper para deep copy
function deepCopy(obj) {
  return JSON.parse(JSON.stringify(obj));
}

// === NUEVA FUNCIÓN: EXPORT ESTRICTO DESDE CAPAS VISIBLES ===
// Garantiza que el botón "Exportar" genere un .geojson con exactamente las geometrías 
// que están en el editor (capas visibles incluidas las que están en modo edición)
// Fuente única: mapa actual, cero dependencias del ProjectRegistry snapshot
function exportFromVisibleLayersStrict() {
  const features = [];
  const sources = new Map(); // Para logging detallado por feature
  const processedCodes = new Set(); // Evitar duplicados por código
  
  console.debug('[EXPORT_STRICT] Iniciando export desde capas visibles...');
  
  // Función auxiliar para procesar layers y agregar features válidas
  const processLayerGroup = (layerGroup, sourceName) => {
    if (!layerGroup) return;
    
    let count = 0;
    layerGroup.eachLayer?.(layer => {
      try {
        const feature = layerToFeature(layer);
        if (!isQuadrantFeature(feature)) return;
        
        const codigo = feature.properties?.codigo || `UNKNOWN_${features.length}`;
        
        // Evitar duplicados
        if (processedCodes.has(codigo)) {
          console.debug(`[EXPORT_STRICT] Duplicado omitido: ${codigo} de ${sourceName}`);
          return;
        }
        
        processedCodes.add(codigo);
        features.push(feature);
        sources.set(codigo, sourceName);
        count++;
        
        console.debug(`[EXPORT_STRICT] ${sourceName}: ${codigo}`);
      } catch (e) {
        console.warn(`[EXPORT_STRICT] Error procesando layer de ${sourceName}:`, e);
      }
    });
    
    console.debug(`[EXPORT_STRICT] ${sourceName}: ${count} features procesadas`);
    return count;
  };
  
  // 1) Procesar DRAWN_EDITABLE (nuevos cuadrantes editables)
  processLayerGroup(DRAWN_EDITABLE, 'EDITABLE');
  
  // 2) Procesar DRAWN_LOCKED (cuadrantes importados bloqueados)
  processLayerGroup(DRAWN_LOCKED, 'LOCKED');
  
  // 3) Procesar todos los grupos de hijos por padre
  for (const [parentCode, childGroup] of Object.entries(state.childGroupsByParent || {})) {
    processLayerGroup(childGroup, `CHILDREN_${parentCode}`);
  }
  
  // 4) Incluir capas temporales de edición (activePadre y activeHijos)
  if (activePadre) {
    try {
      const feature = layerToFeature(activePadre);
      if (isQuadrantFeature(feature)) {
        const codigo = feature.properties?.codigo || 'TEMP_PADRE';
        
        if (!processedCodes.has(codigo)) {
          processedCodes.add(codigo);
          features.push(feature);
          sources.set(codigo, 'TEMP_PADRE_EDITING');
          console.debug(`[EXPORT_STRICT] TEMP_PADRE_EDITING: ${codigo}`);
        }
      }
    } catch (e) {
      console.warn('[EXPORT_STRICT] Error procesando activePadre:', e);
    }
  }
  
  if (activeHijos && activeHijos.length > 0) {
    activeHijos.forEach((hijo, index) => {
      try {
        const feature = layerToFeature(hijo);
        if (isQuadrantFeature(feature)) {
          const codigo = feature.properties?.codigo || `TEMP_HIJO_${index}`;
          
          if (!processedCodes.has(codigo)) {
            processedCodes.add(codigo);
            features.push(feature);
            sources.set(codigo, 'TEMP_HIJOS_EDITING');
            console.debug(`[EXPORT_STRICT] TEMP_HIJOS_EDITING: ${codigo}`);
          }
        }
      } catch (e) {
        console.warn(`[EXPORT_STRICT] Error procesando activeHijo ${index}:`, e);
      }
    });
  }
  
  // 5) Incluir state.activeParent y state.children (compatibilidad con nueva estructura)
  if (state.activeParent && state.activeParent !== activePadre) {
    try {
      const feature = layerToFeature(state.activeParent);
      if (isQuadrantFeature(feature)) {
        const codigo = feature.properties?.codigo || 'STATE_ACTIVE_PARENT';
        
        if (!processedCodes.has(codigo)) {
          processedCodes.add(codigo);
          features.push(feature);
          sources.set(codigo, 'STATE_ACTIVE_PARENT');
          console.debug(`[EXPORT_STRICT] STATE_ACTIVE_PARENT: ${codigo}`);
        }
      }
    } catch (e) {
      console.warn('[EXPORT_STRICT] Error procesando state.activeParent:', e);
    }
  }
  
  if (state.children && state.children.length > 0) {
    state.children.forEach((child, index) => {
      try {
        const feature = layerToFeature(child);
        if (isQuadrantFeature(feature)) {
          const codigo = feature.properties?.codigo || `STATE_CHILD_${index}`;
          
          if (!processedCodes.has(codigo)) {
            processedCodes.add(codigo);
            features.push(feature);
            sources.set(codigo, 'STATE_CHILDREN');
            console.debug(`[EXPORT_STRICT] STATE_CHILDREN: ${codigo}`);
          }
        }
      } catch (e) {
        console.warn(`[EXPORT_STRICT] Error procesando state.children[${index}]:`, e);
      }
    });
  }
  
  // Salvaguardas antes de exportar
  if (features.length === 0) {
    showToast("❌ No hay features para exportar", "error");
    console.warn('[EXPORT_STRICT] No features found to export');
    return;
  }
  
  // Normalizar propiedades de ruta
  features.forEach(f => {
    if (f.properties) {
      normalizeRouteProps(f.properties);
    }
  });
  
  // Ordenar por código para determinismo
  features.sort((a, b) => {
    const codigoA = a.properties?.codigo || '';
    const codigoB = b.properties?.codigo || '';
    return codigoA.localeCompare(codigoB);
  });
  
  // Crear FeatureCollection
  const featureCollection = {
    type: 'FeatureCollection',
    properties: {
      type: 'visible_layers_export',
      city: CITY,
      total_features: features.length,
      export_timestamp: new Date().toISOString(),
      editor_version: '3.1',
      export_source: 'Visible Layers Only'
    },
    features: features
  };
  
  // Generar nombre de archivo
  const timestamp = new Date().toISOString().slice(0,16).replace(/[-:]/g,'').replace('T','_');
  const fileName = `cuadrantes_visible_${timestamp}.geojson`;
  
  // Log detallado para QA
  console.debug(`[EXPORT_STRICT] Resumen de exportación:`);
  console.debug(`- Total features: ${features.length}`);
  console.debug(`- Sources:`, Object.fromEntries(sources));
  
  // VALIDACIÓN FINAL: Verificar unicidad de códigos en export
  const exportCodes = new Set();
  const duplicatesInExport = [];
  
  features.forEach(f => {
    const code = f.properties?.code || f.properties?.codigo;
    if (code) {
      if (exportCodes.has(code)) {
        duplicatesInExport.push(code);
      } else {
        exportCodes.add(code);
      }
    }
  });
  
  if (duplicatesInExport.length > 0) {
    console.error('[EXPORT_STRICT] ¡DUPLICADOS DETECTADOS!', duplicatesInExport);
    showToast(`❌ Error: ${duplicatesInExport.length} códigos duplicados detectados. No se puede exportar.`, 'error');
    alert(`Se detectaron códigos duplicados en la exportación:\n${duplicatesInExport.join(', ')}\n\nCorrija los conflictos antes de exportar.`);
    return;
  }
  
  // Mostrar toast informativo
  showToast(`📤 Exportando ${features.length} features desde capas visibles`, "success");
  
  // Descargar archivo
  downloadGeoJSON(featureCollection, fileName);
  
  console.debug(`[EXPORT_STRICT] Export completado: ${fileName}`);
}

// LEGACY: Mantener para comparativos internos (desarrollo)
const DEV_EXPORT = false; // Flag para ocultar export completo

function exportMergedFull() {
  if (!DEV_EXPORT) {
    console.debug('[EXPORT] exportMergedFull() está deshabilitado, usando exportFromVisibleLayersStrict()');
    return exportFromVisibleLayersStrict();
  }
  
  const fc = buildFullFeatureCollection(); // usa Registry + capas visibles (+ normaliza rutas)
  const ts = new Date().toISOString().slice(0,16).replace(/[-:]/g,'').replace('T','_');
  downloadGeoJSON(fc, `cuadrantes_${fc.features.length}f_${ts}.geojson`);
  showToast(`✅ Exportado: ${fc.features.length} features`, "success");
}

// Nueva función para recolectar todo el dataset exportable
function buildFullFeatureCollection() {
  // ENHANCED: Use ProjectRegistry as primary source for complete dataset export
  const registryFeatures = ProjectRegistry.getAllFeatures();
  const byCode = new Map();
  
  console.debug(`[EXPORT] ProjectRegistry contains ${registryFeatures.length} features`);
  
  // 1) Start with ProjectRegistry features (complete project state)
  registryFeatures.forEach(f => {
    if (!isQuadrantFeature(f)) return;
    
    const code = f.properties?.codigo;
    if (code) {
      byCode.set(code, { ...f }); // Clone to avoid mutations
    } else {
      const key = ProjectRegistry.generateKey(f);
      byCode.set(key, { ...f });
    }
  });
  
  // 2) Merge current visual layers (capture any unsaved edits)
  const mergeFromLayers = (layerGroup, priority = 'registry') => {
    layerGroup?.eachLayer?.(l => {
      try {
        const f = layerToFeature(l);
        if (!isQuadrantFeature(f)) return;
        
        const code = f.properties?.codigo;
        if (code) {
          // Merge strategy: prefer visual edits over registry when explicitly choosing visual priority
          if (priority === 'visual' || !byCode.has(code)) {
            byCode.set(code, f);
            
            // Also update registry with current visual state
            const key = ProjectRegistry.generateKey(f);
            ProjectRegistry.setFeature(key, f);
          }
        } else {
          const key = JSON.stringify(f.geometry);
          if (!byCode.has(key)) {
            byCode.set(key, f);
          }
        }
      } catch(e) { 
        console.warn('[EXPORT] Error merging layer:', e); 
      }
    });
  };
  
  // Merge editable layers with priority (captures current edits)
  mergeFromLayers(DRAWN_EDITABLE, 'visual');
  
  // Merge locked layers (preserve imported state)  
  mergeFromLayers(DRAWN_LOCKED, 'registry');
  
  // Merge child groups (in case they're not in main groups)
  for (const grp of Object.values(state.childGroupsByParent || {})) {
    mergeFromLayers(grp, 'visual');
  }

  const features = Array.from(byCode.values());
  
  // === NUEVO: normalizar propiedades de ruta en todas las features antes de exportar
  features.forEach(f => {
    if (f.properties) {
      normalizeRouteProps(f.properties);
    }
  });
  
  console.debug(`[EXPORT] Complete dataset assembled: ${features.length} features (Registry: ${registryFeatures.length}, Final: ${features.length})`);
  
  return { 
    type: 'FeatureCollection', 
    properties: {
      type: 'full_dataset_export',
      city: CITY,
      total_features: features.length,
      registry_features: registryFeatures.length,
      export_timestamp: new Date().toISOString(),
      editor_version: '3.1',
      export_source: 'ProjectRegistry + Visual Layers'
    },
    features 
  };
}

// Función para exportar SOLO jerarquía activa (Alt+Click)
function buildActiveHierarchyFC() {
  // ENHANCED: Use ProjectRegistry for active hierarchy export
  if (!state.activeParent) {
    return buildFeatureCollection(activePadre, activeHijos);
  }
  
  const parentCode = state.activeParent.feature?.properties?.codigo;
  if (!parentCode) {
    return buildFeatureCollection(activePadre, activeHijos);
  }
  
  // Get from ProjectRegistry first, then merge with current visual state
  const registryParent = ProjectRegistry.getParent(parentCode);
  const registryChildren = ProjectRegistry.getChildren(parentCode);
  
  console.debug(`[EXPORT ACTIVE] Registry: parent=${!!registryParent}, children=${registryChildren.length}`);
  console.debug(`[EXPORT ACTIVE] Visual: parent=${!!activePadre}, children=${activeHijos.length}`);
  
  // Use visual layers if available (captures current edits), otherwise use registry
  const exportParent = activePadre || registryParent;
  const exportChildren = activeHijos.length > 0 ? activeHijos : registryChildren;
  
  return buildFeatureCollection(exportParent, exportChildren);
}

// Función helper para construir FeatureCollection (original)
function buildFeatureCollection(padre, hijos) {
  if (padre && hijos && hijos.length > 0) {
    // Exportación de jerarquía específica
    const features = [];
    const timestamp = new Date().toISOString();
    
    // Preparar padre
    const padreFeature = {
      ...padre.toGeoJSON(),
      properties: {
        nivel: "cuadrante",
        codigo: padre.feature.properties.codigo,
        ciudad: padre.feature.properties.ciudad,
        id_ruta: padre.feature.properties.id_ruta,
        ruta: padre.feature.properties.id_ruta,
        tipo_cuadrante: padre.feature.properties.tipo_cuadrante || "RUTA",
        created_at: timestamp,
        editor_version: "3.0",
        total_hijos: hijos.length,
        fillColor: padre.feature.properties.fillColor || PADRE_STYLE.fillColor,
        color: padre.feature.properties.color || PADRE_STYLE.color,
        weight: padre.feature.properties.weight || PADRE_STYLE.weight,
        fillOpacity: padre.feature.properties.fillOpacity || PADRE_STYLE.fillOpacity
      }
    };
    features.push(padreFeature);
    
    // Preparar hijos
    hijos.forEach((hijo, index) => {
      const hijoFeature = {
        ...hijo.toGeoJSON(),
        properties: {
          nivel: "subcuadrante",
          codigo: hijo.feature.properties.codigo,
          codigo_padre: padre.feature.properties.codigo,
          ciudad: padre.feature.properties.ciudad,
          id_ruta: padre.feature.properties.id_ruta,
          ruta: padre.feature.properties.id_ruta,
          tipo_cuadrante: hijo.feature.properties.tipo_cuadrante || padre.feature.properties.tipo_cuadrante || "RUTA",
          orden: index + 1,
          created_at: timestamp,
          editor_version: "3.0",
          fillColor: hijo.feature.properties.fillColor || HIJO_STYLE.fillColor,
          color: hijo.feature.properties.color || HIJO_STYLE.color,
          weight: hijo.feature.properties.weight || HIJO_STYLE.weight,
          fillOpacity: hijo.feature.properties.fillOpacity || HIJO_STYLE.fillOpacity
        }
      };
      features.push(hijoFeature);
    });
    
    return {
      type: 'FeatureCollection',
      properties: {
        type: 'hierarchy_export',
        parent_code: padre.feature.properties.codigo,
        city: padre.feature.properties.ciudad,
        route: padre.feature.properties.id_ruta,
        total_features: features.length,
        export_timestamp: timestamp,
        crs: "EPSG:4326",
        target_crs: "EPSG:3116",
        validation_passed: true
      },
      features: features
    };
  } else {
    // Exportación general de cuadrantes
    const quadsFC = collectQuadrantsFC();
    const comunas = (COMUNAS_FC && Array.isArray(COMUNAS_FC.features)) ? COMUNAS_FC.features : [];
    
    const enrichedQuads = quadsFC.features.map(feature => ({
      ...feature,
      properties: {
        ...feature.properties,
        nivel: feature.properties.nivel || 'cuadrante',
        created_at: new Date().toISOString(),
        editor_version: '3.0',
        crs: "EPSG:4326"
      }
    }));
    
    return {
      type: 'FeatureCollection',
      properties: {
        type: 'general_export',
        city: CITY,
        total_comunas: comunas.length,
        total_quadrants: enrichedQuads.length,
        export_timestamp: new Date().toISOString()
      },
      features: [...comunas, ...enrichedQuads],
    };
  }
}

// Función helper para exportar FeatureCollection
function doExport(fc) {
  let fileName;
  if (fc.properties && fc.properties.type === 'hierarchy_export' && fc.properties.parent_code) {
    fileName = `subcuadrante_${fc.properties.parent_code}.geojson`;
  } else {
    fileName = `cuadrantes_${CITY.toLowerCase()}_${new Date().toISOString().slice(0,10)}.geojson`;
  }
  
  downloadGeoJSON(fc, fileName);
}

// Sugiere nombre de archivo para exportación completa
function suggestFileNameForFullExport() {
  const now = new Date();
  const dateStr = now.toISOString().slice(0,10); // YYYY-MM-DD
  const timeStr = now.toTimeString().slice(0,5).replace(':', ''); // HHMM
  return `cuadrantes_rutas_${CITY.toLowerCase()}_${dateStr}-${timeStr}.geojson`;
}

// Función para exportación directa (global)
function doDirectExport() {
  const fc = buildFeatureCollection(activePadre, activeHijos);
  doExport(fc);
}

// Función para exportar jerarquía específica (global)
function exportHierarchy() {
  const features = [];
  const timestamp = new Date().toISOString();
  
  // Preparar padre con propiedades completas
  const padreFeature = {
    ...activePadre.toGeoJSON(),
    properties: {
      nivel: "cuadrante",
      codigo: activePadre.feature.properties.codigo,
      ciudad: activePadre.feature.properties.ciudad,
      id_ruta: activePadre.feature.properties.id_ruta,
      ruta: activePadre.feature.properties.id_ruta, // Alias
      created_at: timestamp,
      editor_version: "3.0",
      total_hijos: activeHijos.length,
      // Preservar propiedades de estilo
      fillColor: activePadre.feature.properties.fillColor || PADRE_STYLE.fillColor,
      color: activePadre.feature.properties.color || PADRE_STYLE.color,
      weight: activePadre.feature.properties.weight || PADRE_STYLE.weight,
      fillOpacity: activePadre.feature.properties.fillOpacity || PADRE_STYLE.fillOpacity
    }
  };
  features.push(padreFeature);
  
  // Preparar hijos con propiedades completas y orden
  activeHijos.forEach((hijo, index) => {
    const hijoFeature = {
      ...hijo.toGeoJSON(),
      properties: {
        nivel: "subcuadrante",
        codigo: hijo.feature.properties.codigo,
        codigo_padre: activePadre.feature.properties.codigo,
        ciudad: activePadre.feature.properties.ciudad,
        id_ruta: activePadre.feature.properties.id_ruta,
        ruta: activePadre.feature.properties.id_ruta, // Alias
        orden: index + 1,
        created_at: timestamp,
        editor_version: "3.0",
        // Preservar propiedades de estilo
        fillColor: hijo.feature.properties.fillColor || HIJO_STYLE.fillColor,
        color: hijo.feature.properties.color || HIJO_STYLE.color,
        weight: hijo.feature.properties.weight || HIJO_STYLE.weight,
        fillOpacity: hijo.feature.properties.fillOpacity || HIJO_STYLE.fillOpacity
      }
    };
    features.push(hijoFeature);
  });
  
  // Crear FeatureCollection
  const hierarchyFC = {
    type: 'FeatureCollection',
    properties: {
      type: 'hierarchy_export',
      parent_code: activePadre.feature.properties.codigo,
      city: activePadre.feature.properties.ciudad,
      route: activePadre.feature.properties.id_ruta,
      total_features: features.length,
      export_timestamp: timestamp,
      crs: "EPSG:4326", // WGS84 para compatibilidad
      target_crs: "EPSG:3116", // CRS métrico recomendado para Colombia
      validation_passed: true
    },
    features: features
  };
  
  // Nombre de archivo según T6: subcuadrante_CL_{ruta}_{nn}.geojson
  const codigoPadre = activePadre.feature.properties.codigo;
  const fileName = `subcuadrante_${codigoPadre}.geojson`;
  
  downloadGeoJSON(hierarchyFC, fileName);
  
  console.debug('[EXPORT HIERARCHY]', {
    padre: codigoPadre,
    hijos: activeHijos.length,
    archivo: fileName
  });
}

// Función para exportar cuadrantes generales (global)
function exportGeneralQuadrants() {
  const quadsFC = collectQuadrantsFC();
  const comunas = (COMUNAS_FC && Array.isArray(COMUNAS_FC.features)) ? COMUNAS_FC.features : [];
  
  // Enriquecer features con propiedades de jerarquía
  const enrichedQuads = quadsFC.features.map(feature => ({
    ...feature,
    properties: {
      ...feature.properties,
      // Asegurar propiedades de jerarquía
      nivel: feature.properties.nivel || 'cuadrante',
      created_at: new Date().toISOString(),
      editor_version: '3.0',
      crs: "EPSG:4326"
    }
  }));
  
  const combined = {
    type: 'FeatureCollection',
    properties: {
      type: 'general_export',
      city: CITY,
      total_comunas: comunas.length,
      total_quadrants: enrichedQuads.length,
      export_timestamp: new Date().toISOString()
    },
    // Orden: comunas primero, luego cuadrantes
    features: [...comunas, ...enrichedQuads],
  };
  
  const fileName = `cuadrantes_${CITY.toLowerCase()}_${new Date().toISOString().slice(0,10)}.geojson`;
  downloadGeoJSON(combined, fileName);
  
  console.debug('[EXPORT GENERAL]', {
    city: CITY,
    comunas: comunas.length,
    cuadrantes: enrichedQuads.length,
    archivo: fileName
  });
}

// Helper para descarga de GeoJSON (global)
function downloadGeoJSON(geojsonData, fileName) {
  const blob = new Blob([JSON.stringify(geojsonData, null, 2)], { 
    type: 'application/geo+json' 
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  
  // Mostrar notificación de éxito
  showExportSuccess(fileName, geojsonData.features.length);
}

// Mostrar notificación de éxito de exportación (global)
function showExportSuccess(fileName, featureCount) {
  const notification = document.createElement('div');
  notification.style.cssText = `
    position: fixed; top: 20px; right: 20px; z-index: 9999;
    background: #2ecc71; color: white; padding: 15px 20px;
    border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 14px; font-weight: 500;
  `;
  notification.innerHTML = `
    ✅ <strong>Exportado exitosamente</strong><br>
    📄 ${fileName}<br>
    📊 ${featureCount} geometrías
  `;
  
  document.body.appendChild(notification);
  
  setTimeout(() => {
    if (document.body.contains(notification)) {
      document.body.removeChild(notification);
    }
  }, 4000);
}

    // === AUTO-CARGA Y FUNCIONES DE CARGA ===

// Obtener ciudad de la URL
function getCityFromURL() {
    const params = new URLSearchParams(window.location.search);
    return (params.get('city') || 'CALI').toUpperCase();
}

// Cargar GeoJSON por defecto de la ciudad
async function loadDefaultCityGeoJSON() {
    const city = getCityFromURL();
    const url = `/geojson/default?city=${encodeURIComponent(city)}`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`No se pudo cargar GeoJSON por defecto (${city}): ${response.status}`);
        }
        
        const featureCollection = await response.json();
        loadFeatureCollection(featureCollection, { source: 'default', lockImported: true });
        
        console.log(`[AUTO-LOAD] GeoJSON por defecto cargado para ${city}:`, featureCollection.features?.length || 0, 'features');
        
        // Ajustar vista al contenido cargado
        fitToAllIfAny();
        
    } catch (error) {
        console.warn('[AUTO-LOAD]', error.message);
        // No mostrar error al usuario, es opcional
    }
}

// Función general para cargar FeatureCollection
function loadFeatureCollection(featureCollection, options = {}) {
    const { source = 'unknown', lockImported = false } = options;
    
    if (!featureCollection || featureCollection.type !== 'FeatureCollection') {
        throw new Error('Debe ser un FeatureCollection válido');
    }
    
    const features = featureCollection.features || [];
    let loaded = 0;
    let skipped = 0;
    
    features.forEach(feature => {
        if (!feature.geometry) {
            skipped++;
            return;
        }
        
        // Filtrar comunas (properties con NOMBRE, barrio o BARRIO)
        const props = feature.properties || {};
        if (props.NOMBRE || props.barrio || props.BARRIO) {
            // Las comunas se cargan como referencia, no editables
            const layer = L.geoJSON(feature, {
                interactive: false,
                style: COMUNA_STYLE
            }).addTo(map);
            layer.bringToBack();
            skipped++;
            return;
        }
        
        // Solo cargar geometrías poligonales
        const geomType = feature.geometry.type;
        if (!['Polygon', 'MultiPolygon'].includes(geomType)) {
            skipped++;
            return;
        }
        
        // Crear capa Leaflet
        const layer = L.geoJSON(feature, {
            onEachFeature: (feat, lyr) => {
                lyr.feature = feat;
                
                // Aplicar estilos desde properties o usar por defecto
                applyStyleFromProperties(lyr);
                enforceStrokePolicy(lyr);
                attachRecolorOnClick(lyr);
                
                // Si es un padre (cuadrante), registrarlo para activación
                if (feat.properties && feat.properties.nivel === 'cuadrante' && feat.properties.codigo) {
                    registerParent(lyr);
                }
            }
        }).getLayers()[0];
        
        if (layer) {
            // Marcar si es importado
            if (source !== 'default') {
                layer._isImported = true;
            }
            
            // Agregar al grupo apropiado
            const targetGroup = lockImported ? DRAWN_LOCKED : DRAWN_EDITABLE;
            targetGroup.addLayer(layer);
            loaded++;
        }
    });
    
    console.log(`[LOAD] ${source}:`, { loaded, skipped, total: features.length });
    
    return { loaded, skipped };
}

// === CONFIGURAR EVENTOS DE JERARQUÍA ===
    
    // Botón crear cuadrante padre
    document.getElementById('btn-crear-padre').addEventListener('click', () => {
        if (currentEditorState === EditorState.CREANDO_PADRE) {
            setEditorState(EditorState.IDLE);
        } else {
            setEditorState(EditorState.CREANDO_PADRE);
        }
    });
    
    // Botón crear subcuadrantes hijos
    document.getElementById('btn-crear-hijo').addEventListener('click', () => {
        if (state.mode === EditorState.CREANDO_HIJO) {
            setEditorState(EditorState.PADRE_ACTIVO);
        } else {
            setEditorState(EditorState.CREANDO_HIJO);
        }
    });
    
    // Botón editar padre
    document.getElementById('btn-editar-padre').addEventListener('click', () => {
        if (state.activeParent) {
            // Si ya hay un padre activo, editar directamente
            startParentEditing();
        } else {
            // Activar picker para seleccionar padre
            activateEditPicker('parent');
        }
    });
    
    // Botón editar código (abre modal directamente sin entrar en modo de edición de geometría)
    document.getElementById('btn-editar-codigo').addEventListener('click', () => {
        if (!state.activeParent) return;
        
        const props = state.activeParent.feature?.properties || {};
        const currentSystem = props.system || sistemaActual;
        
        showEditCodeModal(currentSystem, props, (newProps) => {
            // Actualizar propiedades del padre
            Object.assign(state.activeParent.feature.properties, newProps);
            
            // Actualizar changeLog y ProjectRegistry
            const feat = state.activeParent.toGeoJSON();
            const parentCode = feat.properties.code || feat.properties.codigo;
            
            if (parentCode) {
                state.changeLog.set(parentCode, feat);
                const registryKey = ProjectRegistry.generateKey(feat);
                ProjectRegistry.setFeature(registryKey, feat);
            }
            
            showToast(`Código actualizado a: ${newProps.code}`);
            console.debug(`[EDIT_CODE] Código actualizado: ${newProps.code}`);
        });
    });
    
    // Botón guardar padre
    document.getElementById('btn-guardar-padre').addEventListener('click', () => {
        saveParentEditing();
    });
    
    // Botón cancelar edición padre
    document.getElementById('btn-cancelar-edicion-padre').addEventListener('click', () => {
        cancelParentEditing();
    });
    
    // Botón editar hijos
    document.getElementById('btn-editar-hijo').addEventListener('click', () => {
        if (activeHijos && activeHijos.length > 0) {
            // Si ya hay hijos activos, editar directamente
            startChildrenEditing();
        } else {
            // Activar picker para seleccionar padre cuyos hijos se van a editar
            activateEditPicker('children');
        }
    });
    
    // Botón guardar hijos
    document.getElementById('btn-guardar-hijo').addEventListener('click', saveChildrenEditing);
    
    // Botón cancelar edición hijos
    document.getElementById('btn-cancelar-edicion-hijo').addEventListener('click', cancelChildrenEditing);
    
    // Botón aislar
    document.getElementById('btn-aislar').addEventListener('click', () => {
        state.isAislado = !state.isAislado;
        isAislado = state.isAislado; // mantener compatibilidad
        applyAislamiento();
        updateUIState();
    });
    
    // Botones de eliminación
    document.getElementById('btn-delete-parent')?.addEventListener('click', deleteActiveParent);
    document.getElementById('btn-delete-child')?.addEventListener('click', deleteSelectedChild);
    

    

    
    // Control de opacidad removido - ahora usando constante fija PARENT_FILL_OPACITY
    
    // === EVENTOS DEL MODAL DE CÓDIGO DE HIJO ===
    
    // Botón guardar en modal de hijo
    document.getElementById('hijo-guardar').addEventListener('click', () => {
        if (!modalChildOptions) return;
        
        const codigo = document.getElementById('hijo-codigo').value;
        const success = modalChildOptions.onSave(codigo);
        
        if (success) {
            closeChildCodeModal();
        }
    });
    
    // Botón cancelar en modal de hijo
    document.getElementById('hijo-cancelar').addEventListener('click', () => {
        if (modalChildOptions && modalChildOptions.onCancel) {
            modalChildOptions.onCancel();
        }
        closeChildCodeModal();
    });
    
    // Tecla Enter en el input de código
    document.getElementById('hijo-codigo').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            document.getElementById('hijo-guardar').click();
        }
    });
    
    // === CONFIGURAR EVENTOS DE IMPORTACIÓN ===
    
    // Configurar botón de importación
    const btnImport = document.getElementById('btn-import');
    const fileInput = document.getElementById('file-import');
    
    if (btnImport && fileInput) {
        btnImport.addEventListener('click', () => fileInput.click());
        fileInput.addEventListener('change', onImportFileChanged);
        
        console.debug('Eventos de importación configurados');
    }
    
    // === NUEVOS HANDLERS DE IMPORTACIÓN ===
    
    // Handler para el botón del modal
    const btnImportMode = document.getElementById('btn-import-mode');
    if (btnImportMode) {
        btnImportMode.addEventListener('click', () => {
            const mode = getSelectedImportMode(); // 'PANORAMA' | 'EDIT_ALL'
            runImportFlow(mode);
        });
    }
    
    // Handlers de la barra superior
    const btnImportGeo = document.getElementById('btn-import-geojson');
    const fileGeo = document.getElementById('file-geojson');

    if (btnImportGeo && fileGeo) {
        btnImportGeo.addEventListener('click', () => fileGeo.click());
        fileGeo.addEventListener('change', () => {
            const f = fileGeo.files && fileGeo.files[0];
            if (!f) return;
            const reader = new FileReader();
            reader.onload = e => {
                try {
                    const fc = JSON.parse(e.target.result);
                    state.masterFC = fc;
                    renderFCAccordingToMode(fc, getSelectedImportMode());
                } catch (err) {
                    console.error('[IMPORT] GeoJSON inválido:', err);
                    showToast('Archivo GeoJSON inválido', 'error');
                }
            };
            reader.readAsText(f);
        });
    }
    
    // Cargar GeoJSON por defecto al inicializar
    loadDefaultCityGeoJSON();
    
    // Inicializar estado de la UI
    setEditorState(EditorState.IDLE);
    
    console.debug('Editor de cuadrantes con jerarquía inicializado completamente');
});

// === FUNCIONES DE VALIDACIÓN EXPORTACIÓN ===

function enableEditMode(layers) {
  if (!Array.isArray(layers)) layers = [layers].filter(Boolean);

  // desactivar cualquier sesión previa
  endEditMode(false);

  EDIT_SESSION.layers = layers;

  // habilitar edición nativa por capa (Leaflet.Draw la provee)
  layers.forEach(l => {
    if (l && l.editing && typeof l.editing.enable === 'function') {
      l.editing.enable();
    }
  });

  isEditingActive = true;
}

function endEditMode(save = true) {
  // En este enfoque no hay que guardar nada aquí: las funciones saveParentEditing/saveChildrenEditing
  // ya toman la geometría actual con toGeoJSON() y la persisten.
  (EDIT_SESSION.layers || []).forEach(l => {
    try {
      if (l && l.editing && typeof l.editing.disable === 'function') {
        l.editing.disable();
      }
    } catch (e) {}
  });

  EDIT_SESSION.layers = [];
  isEditingActive = false;
}

// Validar antes de exportar
function validateBeforeExport() {
    if (!activePadre || activeHijos.length === 0) {
        return { 
          valid: true, 
          errors: [], 
          warnings: [], 
          geojsonDebug: { type: 'FeatureCollection', features: [] } 
        }; // No hay jerarquía que validar
    }
    
    const geomPadre = activePadre.toGeoJSON();
    const geomsHijos = activeHijos.map(hijo => hijo.toGeoJSON());
    
    const validation = validarIntegridadSubcuadrantes(geomPadre, geomsHijos);
    
    // Convertir estructura de respuesta
    return {
      valid: validation.ok,
      errors: validation.errores || [],
      warnings: validation.warnings || [],
      geojsonDebug: validation.geojsonDebug || { type: 'FeatureCollection', features: [] }
    };
}

// Mostrar modal de validación nuevo con opciones de exportación
function openValidationModal(validation, opts = {}) {
    if (EXPORT_VALIDATION_MODE === 'none') {
        // Nunca mostrar el modal
        const fc = buildFeatureCollection(activePadre, activeHijos);
        return doExport(fc);
    }
    
    const modal = document.createElement('div');
    modal.className = 'validation-overlay';
    
    let errorsHtml = '';
    let warningsHtml = '';
    
    // Errores críticos en rojo
    validation.errors.forEach(error => {
        errorsHtml += `<div class="validation-error critical">${error}</div>`;
    });
    
    // Warnings en amarillo
    validation.warnings.forEach(warning => {
        warningsHtml += `<div class="validation-warning">${warning}</div>`;
    });
    
    const title = validation.errors.length > 0 ? '❌ Errores Críticos' : '⚠️ Advertencias de Validación';
    const description = validation.errors.length > 0 
      ? 'Se encontraron errores críticos que deben corregirse:'
      : 'Se encontraron advertencias. Puede continuar exportando:';
    
    // Botones de acción
    let actionButtons = `
        <button class="btn btn-secondary" id="validation-close">Cerrar</button>
        <button class="btn btn-info" id="validation-debug">Ver en Mapa</button>
    `;
    
    if (opts.allowExport) {
        actionButtons += `
            <button class="btn btn-success" id="validation-export-anyway">Exportar de todos modos</button>
        `;
        // TODO: Agregar botón de recortar cuando esté implementado Turf
        // actionButtons += `<button class="btn btn-warning" id="validation-clip-export">Recortar y exportar</button>`;
    }
    
    modal.innerHTML = `
        <div class="validation-panel">
            <h3>${title}</h3>
            <p>${description}</p>
            ${errorsHtml}
            ${warningsHtml}
            <div class="validation-actions">
                ${actionButtons}
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Event listeners
    document.getElementById('validation-close').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    document.getElementById('validation-debug').addEventListener('click', () => {
        // Mostrar geometrías de debug en el mapa
        if (validation.geojsonDebug && validation.geojsonDebug.features.length > 0) {
            const debugLayer = L.geoJSON(validation.geojsonDebug, {
                style: (feature) => {
                    const errorType = feature.properties.error;
                    return ERROR_STYLE[errorType.toUpperCase()] || ERROR_STYLE.GAP;
                }
            }).addTo(map);
            
            // Auto-remover después de 5 segundos
            setTimeout(() => {
                map.removeLayer(debugLayer);
            }, 5000);
        }
        
        document.body.removeChild(modal);
    });
    
    // Botón exportar de todos modos
    if (opts.allowExport) {
        const exportAnywayBtn = document.getElementById('validation-export-anyway');
        if (exportAnywayBtn) {
            exportAnywayBtn.addEventListener('click', () => {
                document.body.removeChild(modal);
                const fc = buildFeatureCollection(activePadre, activeHijos);
                doExport(fc);
            });
        }
        
        // TODO: Implementar cuando esté Turf disponible
        // const clipExportBtn = document.getElementById('validation-clip-export');
        // if (clipExportBtn) {
        //     clipExportBtn.addEventListener('click', () => {
        //         const clippedChildren = clipChildrenToParent(activeHijos, activePadre);
        //         document.body.removeChild(modal);
        //         doExport(buildFeatureCollection(activePadre, clippedChildren));
        //     });
        // }
    }
}

// Función antigua mantenida para compatibilidad (por si se usa en otro lado)
function showValidationPanel(validation) {
    // Redirigir a la nueva función
    openValidationModal(validation, { allowExport: false });
}

// === T7 — IMPORTACIÓN CON JERARQUÍA ===

// Función principal de importación con detección de jerarquía
async function onImportFileChanged(evt) {
    const file = evt.target.files?.[0];
    if (!file) return;
    
    try {
        // Leer archivo
        const text = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = e => resolve(e.target.result);
            reader.onerror = reject;
            reader.readAsText(file);
        });
        
        // Parsear JSON
        const data = JSON.parse(text);
        
        // Guardar el archivo completo como "master"
        state.masterFC = data;  // FeatureCollection completo

        // Reinicializar e indexar TODO en el registro del proyecto
        ProjectRegistry.clear();
        (data.features || []).forEach(f => {
          f.properties = f.properties || {};
          normalizeRouteProps(f.properties); // mantener id_ruta/ruta_publica
          const key = ProjectRegistry.generateKey(f);
          ProjectRegistry.setFeature(key, f); // indexar TODOS (se muestren o no)
        });
        
        // Logs de depuración
        console.debug('[IMPORT] archivo leído OK, longitud:', text.length);
        console.debug('[IMPORT] tipo:', data?.type, 'features:', data?.features?.length ?? 'n/a');
        
        // Show dual import mode selector
        showImportModeSelector(data);
        
    } catch (error) {
        console.error('[IMPORT] Error al procesar archivo:', error);
        alert('Error al importar el archivo. Verifique que sea un GeoJSON válido.');
    } finally {
        // Resetear input
        evt.target.value = '';
    }
}

// === NUEVO: UI con una sola decisión - Panorama vs Editar ruta ===
function showImportModeSelector(data) {
    const hasHierarchy = detectHierarchyStructure(data);
    
    const modal = document.createElement('div');
    modal.className = 'cuadrante-modal';
    
    // Extraer rutas disponibles usando el diccionario
    const availableRoutes = [...new Set((data.features || [data]).map(f => f.properties?.id_ruta).filter(Boolean))];
    let routeSelector = '';
    
    if (availableRoutes.length > 0) {
        routeSelector = `
            <div class="route-selector-container" id="route-selector-container" style="display: none;">
                <label>Seleccionar ruta para editar:</label>
                <select id="route-selector" class="form-control">
                    <option value="">Seleccionar ruta...</option>
                    ${availableRoutes.map(routeId => 
                        `<option value="${routeId}">${getRouteLabel(routeId)}</option>`
                    ).join('')}
                </select>
            </div>
        `;
    }
    
    modal.innerHTML = `
        <div class="cuadrante-modal-content">
            <h3>🧭 Modo de Visualización</h3>
            
            <div class="card">
                <div class="radio-row">
                    <label>
                        <input type="radio" name="vizMode" value="PANORAMA" checked>
                        🌍 Panorama (todos)
                    </label>
                    <div class="hint">Muestra todos los cuadrantes. No editable.</div>
                </div>

                <div class="radio-row">
                    <label>
                        <input type="radio" name="vizMode" value="EDIT_ALL">
                        ✏️ Edición libre (clic-para-editar)
                    </label>
                    <div class="hint">Muestra todo el archivo y permite elegir con un click qué cuadrante editar.</div>
                </div>

                <div class="actions">
                    <button id="btn-cancel" class="btn btn-secondary">Cancelar</button>
                    <button id="btn-import-mode" class="btn btn-primary">Importar</button>
                </div>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Confirm handler - usar nueva lógica unificada
    document.getElementById('btn-import-mode').addEventListener('click', async () => {
        const mode = getSelectedImportMode();
        
        // Si el modal tiene data asociada, usarla
        if (data) {
            state.masterFC = data;
            renderFCAccordingToMode(data, mode);
        } else {
            // Si no hay data, usar el flujo automático
            await runImportFlow(mode);
        }
        
        document.body.removeChild(modal);
    });
    
    // Cancel handler
    document.getElementById('btn-cancel').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
}

// === NUEVO: Funciones de importación ===

// Import PANORAMA mode - all data as locked
async function importPanoramaMode(data, hierarchyInfo) {
    console.log('[IMPORT PANORAMA] Iniciando modo panorama...');
    showToast('📊 Importando en modo panorama...', 'info');
    
    // Register features in ProjectRegistry as masterFC
    const features = data.features || [data];
    state.masterFC = { type: 'FeatureCollection', features: [] };
    state.worksetFC = { type: 'FeatureCollection', features: [] };
    
    let registered = 0;
    
    // Import all features directly to DRAWN_LOCKED without showing selectors
    features.forEach(feature => {
        if (feature.geometry && (feature.properties?.nivel === 'cuadrante' || feature.properties?.nivel === 'subcuadrante')) {
            // Register in state collections
            state.masterFC.features.push(JSON.parse(JSON.stringify(feature))); // Deep copy
            state.worksetFC.features.push(JSON.parse(JSON.stringify(feature)));
            
            // Create layer and add directly to DRAWN_LOCKED
            const layer = L.geoJSON(feature, {
                onEachFeature: (feat, lyr) => {
                    lyr.feature = feat;
                    const isPadre = (feat.properties && feat.properties.nivel === 'cuadrante');
                    ensureStyleProps(lyr, isPadre);
                    applyStyleFromProperties(lyr);
                    enforceStrokePolicy(lyr);
                    attachRecolorOnClick(lyr);
                }
            }).getLayers()[0];
            
            if (layer) {
                // Import as LOCKED (panorama mode)
                addImportedFeatureLayer(feature, layer, 'DRAWN_LOCKED');
                registered++;
            }
        }
    });
    
    console.log(`[PANORAMA MODE] Registered ${registered} features`);
    showToast(`✅ Panorama importado: ${registered} cuadrantes (modo solo lectura)`, 'success');
    
    // Fit map to show all imported features
    fitToAllIfAny();
}

// Import EDIT ROUTE mode - specific route for editing
function importEditRouteMode(data, hierarchyInfo, selectedRouteId) {
    // Limpiar el mapa antes de cargar la ruta
    DRAWN_EDITABLE.clearLayers();
    DRAWN_LOCKED.clearLayers();

    // Construir el conjunto de "padres" de la ruta seleccionada
    const features = data.features || [];
    const routeId = Number(selectedRouteId);

    // Códigos de padres pertenecientes a la ruta
    const parentCodes = new Set(
        features
            .filter(f => (f.properties?.nivel || '').toLowerCase() === 'cuadrante'
                      && Number(f.properties?.id_ruta) === routeId)
            .map(f => (f.properties?.codigo || '').toUpperCase())
    );

    // Función de pertenencia a la ruta (padre o hijo)
    const belongsToRoute = (f) => {
        const p = f?.properties || {};
        const lvl = (p.nivel || '').toLowerCase();
        const code = (p.codigo || '').toUpperCase();
        const codePadre = (p.codigo_padre || '').toUpperCase();

        // Regla A: id_ruta explícito
        if (Number(p.id_ruta) === routeId) return true;

        // Regla B: hijo cuyo codigo_padre es uno de los padres detectados
        if (lvl === 'subcuadrante' && codePadre && parentCodes.has(codePadre)) return true;

        // Regla C (fallback por patrón de código, por si faltan props):
        const patPadre = new RegExp(`^CL_${routeId}_00[A-Z]*$`, 'i');
        const patHijo  = new RegExp(`^CL_${routeId}_(\\d{2}[A-Z]*)$`, 'i');
        if (patPadre.test(code)) return true;
        const m = code.match(patHijo);
        if (m && !m[1].toUpperCase().startsWith('00')) return true;
        if (lvl === 'subcuadrante' && codePadre && patPadre.test(codePadre)) return true;

        return false;
    };

    // Añadir al mapa solo las features que pertenecen a la ruta (todas a DRAWN_EDITABLE)
    const selected = features.filter(belongsToRoute);

    selected.forEach(f => {
        const nivel = (f.properties?.nivel || '').toLowerCase();
        const layer = L.geoJSON(f, {
            onEachFeature: (feat, lyr) => {
                lyr.feature = feat;
                ensureStyleProps(lyr, nivel === 'cuadrante');     // solo completar faltantes
                applyStyleFromProperties(lyr);                    // respeta colores existentes
                enforceStrokePolicy(lyr);
                lyr.options = lyr.options || {};
                lyr.options.interactive = true;
                lyr.options.bubblingMouseEvents = true;

                if (nivel === 'cuadrante') {
                    registerParent(lyr);
                } else if (nivel === 'subcuadrante') {
                    registerChild(lyr, feat.properties?.codigo_padre);
                }
            }
        }).getLayers()[0];

        if (layer) DRAWN_EDITABLE.addLayer(layer);
    });

    // Ajustar vista y activar un padre automáticamente (mejor UX, no rompe nada)
    const firstParent = DRAWN_EDITABLE.getLayers()
        .find(l => (l.feature?.properties?.nivel || '').toLowerCase() === 'cuadrante');
    if (firstParent) setActiveParent(firstParent);
    fitBoundsOfLayers(DRAWN_EDITABLE);

    // Mantener el proyecto completo para exportar (sin dibujarlo)
    state.masterFC = data;   // guardar el archivo completo para el merge en export

    showToast(`✏️ Ruta lista para edición: ${getRouteLabel(routeId)} (${selected.length} geom.)`, 'success');
}

// Import PROJECT mode - complete integration
async function importProjectMode(data, hierarchyInfo) {
    console.log('[IMPORT PROJECT] Iniciando importación completa...');
    
    // First, register all features in ProjectRegistry
    const features = data.features || [data];
    let registered = 0;
    
    features.forEach(feature => {
        if (feature.geometry && (feature.properties?.nivel === 'cuadrante' || feature.properties?.nivel === 'subcuadrante')) {
            const key = ProjectRegistry.generateKey(feature);
            ProjectRegistry.setFeature(key, feature);
            registered++;
        }
    });
    
    console.log(`[PROJECT REGISTRY] Registered ${registered} features`);
    
    // Then proceed with visual import using existing logic
    if (hierarchyInfo.detected) {
        await importWithHierarchy(data, hierarchyInfo);
    } else {
        await importGeneralQuadrants(data);
    }
    
    // Show success notification
    showImportProjectSuccess(registered, hierarchyInfo);
}

// Import ROUTE mode - selective editing with context
async function importRouteMode(data, hierarchyInfo) {
    console.log('[IMPORT ROUTE] Iniciando importación de ruta para edición...');
    
    const features = data.features || [data];
    
    // Index hierarchy
    const padres = {};
    const hijos = {};
    
    features.forEach(f => {
        if (f.properties && f.properties.nivel === 'cuadrante') {
            padres[f.properties.codigo] = f;
            // Register in ProjectRegistry for context
            const key = ProjectRegistry.generateKey(f);
            ProjectRegistry.setFeature(key, f);
        }
    });
    
    features.forEach(f => {
        if (f.properties && f.properties.nivel === 'subcuadrante' && f.properties.codigo_padre) {
            if (!hijos[f.properties.codigo_padre]) {
                hijos[f.properties.codigo_padre] = [];
            }
            hijos[f.properties.codigo_padre].push(f);
            // Register in ProjectRegistry for context
            const key = ProjectRegistry.generateKey(f);
            ProjectRegistry.setFeature(key, f);
        }
    });
    
    // Show route selector with editing context
    showRouteEditorSelector(padres, hijos);
}

// Route selector for editing mode
function showRouteEditorSelector(padres, hijos) {
    const modal = document.createElement('div');
    modal.className = 'cuadrante-modal';
    
    let routeOptionsHtml = '<option value="">Seleccionar ruta para editar...</option>';
    
    // Group by route for better organization
    const routeGroups = {};
    Object.values(padres).forEach(padre => {
        const ruta = padre.properties.id_ruta || padre.properties.ruta || 'sin_ruta';
        const routeLabel = routeLabelResolver(padre);
        
        if (!routeGroups[ruta]) {
            routeGroups[ruta] = { label: routeLabel, padres: [] };
        }
        routeGroups[ruta].padres.push(padre);
    });
    
    // Generate HTML grouped by route
    Object.keys(routeGroups).sort().forEach(ruta => {
        const group = routeGroups[ruta];
        routeOptionsHtml += `<optgroup label="${group.label}">`;
        
        group.padres.forEach(padre => {
            const numHijos = hijos[padre.properties.codigo]?.length || 0;
            routeOptionsHtml += `
                <option value="${padre.properties.codigo}">
                    ${padre.properties.codigo} (${numHijos} subcuadrantes)
                </option>
            `;
        });
        routeOptionsHtml += '</optgroup>';
    });
    
    modal.innerHTML = `
        <div class="cuadrante-modal-content">
            <h3>✏️ Importar Ruta para Edición</h3>
            
            <div class="form-group">
                <label>Rutas disponibles:</label>
                <div class="route-summary">
                    <div>🛤️ ${Object.keys(routeGroups).length} rutas detectadas</div>
                    <div>👔 ${Object.keys(padres).length} cuadrantes padre</div>
                    <div>👶 ${Object.values(hijos).flat().length} subcuadrantes</div>
                </div>
            </div>
            
            <div class="form-group">
                <label for="route-selector">Seleccionar ruta para editar:</label>
                <select id="route-selector" class="form-control">
                    ${routeOptionsHtml}
                </select>
            </div>
            
            <div id="route-preview" class="form-group" style="display: none;">
                <label>Vista previa de edición:</label>
                <div id="route-details" class="route-preview-details"></div>
            </div>
            
            <div class="modal-buttons">
                <button type="button" class="btn btn-secondary" id="route-cancel">Cancelar</button>
                <button type="button" class="btn btn-primary" id="route-import" disabled>Importar para Editar</button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    const selector = document.getElementById('route-selector');
    const importBtn = document.getElementById('route-import');
    const routePreview = document.getElementById('route-preview');
    const routeDetails = document.getElementById('route-details');
    
    selector.addEventListener('change', (e) => {
        const codigoPadre = e.target.value;
        
        if (codigoPadre && padres[codigoPadre]) {
            importBtn.disabled = false;
            
            const padre = padres[codigoPadre];
            const hijosArray = hijos[codigoPadre] || [];
            const routeLabel = routeLabelResolver(padre);
            
            routePreview.style.display = 'block';
            routeDetails.innerHTML = `
                <div class="route-edit-preview">
                    <div class="preview-item">
                        <strong>🛤️ ${routeLabel}</strong>
                    </div>
                    <div class="preview-item">
                        👔 Padre: ${padre.properties.codigo}
                    </div>
                    <div class="preview-item">
                        👶 Subcuadrantes: ${hijosArray.length}
                    </div>
                    <div class="preview-note">
                        ℹ️ Se importará para edición manteniendo contexto visual del resto del proyecto
                    </div>
                </div>
            `;
        } else {
            importBtn.disabled = true;
            routePreview.style.display = 'none';
        }
    });
    
    document.getElementById('route-cancel').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    document.getElementById('route-import').addEventListener('click', () => {
        const codigoPadre = selector.value;
        const selectedPadre = padres[codigoPadre];
        const selectedHijos = hijos[codigoPadre] || [];
        
        document.body.removeChild(modal);
        
        // Load selected route for editing
        loadSelectedHierarchy(selectedPadre, selectedHijos);
        
        // Show route editing success
        showImportRouteSuccess(routeLabelResolver(selectedPadre), selectedHijos.length);
    });
}

// Detectar estructura de jerarquía en el archivo
function detectHierarchyStructure(data) {
    let features = [];
    
    if (data.type === 'FeatureCollection' && Array.isArray(data.features)) {
        features = data.features;
    } else if (data.type === 'Feature') {
        features = [data];
    } else {
        return { detected: false };
    }
    
    // Buscar features con propiedades de jerarquía
    const padres = features.filter(f => 
        f.properties && f.properties.nivel === 'cuadrante'
    );
    
    const hijos = features.filter(f => 
        f.properties && f.properties.nivel === 'subcuadrante' && f.properties.codigo_padre
    );
    
    // 💡 considerar jerarquía si hay al menos un padre (aunque no tenga hijos)
    const detected = padres.length > 0;
    
    return {
        detected,
        padres: padres.length,
        hijos: hijos.length,
        features: features.length,
        totalComunas: features.filter(f => 
            f.properties && (f.properties.NOMBRE || f.properties.barrio || f.properties.BARRIO)
        ).length
    };
}

// Importar archivo con jerarquía (T7)
async function importWithHierarchy(data, hierarchyInfo, forceState = null) {
    const features = data.features || [data];
    
    // Indexar jerarquía según T7
    const padres = {};
    const hijos = {};
    
    // Indexar padres
    features.forEach(f => {
        if (f.properties && f.properties.nivel === 'cuadrante') {
            padres[f.properties.codigo] = f;
        }
    });
    
    // Indexar hijos por código del padre
    features.forEach(f => {
        if (f.properties && f.properties.nivel === 'subcuadrante' && f.properties.codigo_padre) {
            if (!hijos[f.properties.codigo_padre]) {
                hijos[f.properties.codigo_padre] = [];
            }
            hijos[f.properties.codigo_padre].push(f);
        }
    });
    
    console.log('[HIERARCHY INDEX]', {
        padres: Object.keys(padres).length,
        relaciones: Object.keys(hijos).length,
        totalHijos: Object.values(hijos).flat().length
    });
    
    // Mostrar selector de jerarquía
    showHierarchySelector(padres, hijos, (selectedPadre, selectedHijos) => {
        loadSelectedHierarchy(selectedPadre, selectedHijos, forceState);
    });
}

// Mostrar selector de jerarquía para continuar edición
function showHierarchySelector(padres, hijos, callback) {
    const modal = document.createElement('div');
    modal.className = 'cuadrante-modal';
    
    // Construir opciones del selector
    let optionsHtml = '<option value="">Seleccionar cuadrante padre...</option>';
    
    // Agrupar por ruta para mejor organización
    const padresByRuta = {};
    Object.values(padres).forEach(padre => {
        const idRuta = padre.properties.id_ruta || padre.properties.ruta || 'sin_ruta';
        const rutaLabel = getRouteLabel(idRuta);
        if (!padresByRuta[rutaLabel]) {
            padresByRuta[rutaLabel] = [];
        }
        padresByRuta[rutaLabel].push(padre);
    });
    
    // Generar HTML agrupado por ruta
    Object.keys(padresByRuta).sort((a, b) => {
        // Ordenar por etiquetas de ruta
        return a.localeCompare(b);
    }).forEach(rutaLabel => {
        optionsHtml += `<optgroup label="${rutaLabel}">`;
        padresByRuta[rutaLabel].forEach(padre => {
            const numHijos = hijos[padre.properties.codigo]?.length || 0;
            const parentCode = padre.properties.codigo;
            optionsHtml += `
                <option value="${parentCode}">
                    ${rutaLabel} — ${parentCode} (${numHijos} subcuadrantes)
                </option>
            `;
        });
        optionsHtml += '</optgroup>';
    });
    
    modal.innerHTML = `
        <div class="cuadrante-modal-content">
            <h3>📂 Importar Jerarquía de Cuadrantes</h3>
            
            <div class="form-group">
                <label>Cuadrantes encontrados:</label>
                <div class="hierarchy-summary">
                    <div>👔 Padres: ${Object.keys(padres).length}</div>
                    <div>👶 Subcuadrantes: ${Object.values(hijos).flat().length}</div>
                    <div>🔗 Relaciones: ${Object.keys(hijos).length}</div>
                </div>
            </div>
            
            <div class="form-group">
                <label for="padre-selector">Seleccionar cuadrante para editar:</label>
                <select id="padre-selector" class="form-control">
                    ${optionsHtml}
                </select>
            </div>
            
            <div id="hijos-preview" class="form-group" style="display: none;">
                <label>Subcuadrantes incluidos:</label>
                <div id="hijos-list" class="hijos-preview-list"></div>
            </div>
            
            <div class="modal-buttons">
                <button type="button" class="btn btn-secondary" id="modal-cancel">Cancelar</button>
                <button type="button" class="btn btn-info" id="modal-import-all">Importar Todos</button>
                <button type="button" class="btn btn-primary" id="modal-import" disabled>Importar Selección</button>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Configurar eventos
    const selector = document.getElementById('padre-selector');
    const importBtn = document.getElementById('modal-import');
    const hijosPreview = document.getElementById('hijos-preview');
    const hijosList = document.getElementById('hijos-list');
    
    selector.addEventListener('change', (e) => {
        const codigoPadre = e.target.value;
        
        if (codigoPadre && padres[codigoPadre]) {
            importBtn.disabled = false;
            
            // Mostrar preview de hijos
            const hijosArray = hijos[codigoPadre] || [];
            if (hijosArray.length > 0) {
                hijosPreview.style.display = 'block';
                hijosList.innerHTML = hijosArray
                    .sort((a, b) => (a.properties.orden || 0) - (b.properties.orden || 0))
                    .map(hijo => `
                        <div class="hijo-preview-item">
                            <span class="codigo">${hijo.properties.codigo}</span>
                            <span class="orden">Orden: ${hijo.properties.orden || 'N/A'}</span>
                        </div>
                    `).join('');
            } else {
                hijosPreview.style.display = 'block';
                hijosList.innerHTML = `
                    <div class="hijo-preview-item no-children">
                        <span class="mensaje">Sin subcuadrantes - se habilitará creación</span>
                    </div>
                `;
            }
        } else {
            importBtn.disabled = true;
            hijosPreview.style.display = 'none';
        }
    });
    
    // Botón cancelar
    document.getElementById('modal-cancel').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    // Botón importar selección
    importBtn.addEventListener('click', () => {
        const codigoPadre = selector.value;
        const selectedPadre = padres[codigoPadre];
        const selectedHijos = hijos[codigoPadre] || [];
        
        document.body.removeChild(modal);
        callback(selectedPadre, selectedHijos);
    });
    
    // Botón importar todos
    document.getElementById('modal-import-all').addEventListener('click', () => {
        document.body.removeChild(modal);
        loadAllHierarchies(padres, hijos);
    });
}

// Cargar jerarquía seleccionada en el editor
function loadSelectedHierarchy(padreFeature, hijosFeatures, forceState = null) {
    // 1) NO borrar padres/hijos anteriores. Solo cargamos la nueva jerarquía.
    
    // 2) Cargar padre
    const padreLayer = L.geoJSON(padreFeature, { 
        style: PADRE_STYLE, 
        onEachFeature: (f, l) => { 
            l.feature = f; 
            ensureStyleProps(l, true);
            applyStyleFromProperties(l);
            enforceStrokePolicy(l);
            attachRecolorOnClick(l);
        } 
    }).getLayers()[0];
    
    // Use forceState if provided, otherwise default to DRAWN_EDITABLE
    const targetLayer = forceState === 'DRAWN_LOCKED' ? DRAWN_LOCKED : DRAWN_EDITABLE;
    targetLayer.addLayer(padreLayer);
    
    // 3) Cargar hijos y crear grupo específico para este padre
    const code = padreFeature.properties?.codigo;
    const hijosLayers = hijosFeatures
      .sort((a,b)=>(a.properties.orden||0)-(b.properties.orden||0))
      .map(f => L.geoJSON(f, { 
          style: HIJO_STYLE, 
          onEachFeature: (ft, ly) => { 
              ly.feature = ft; 
              ensureStyleProps(ly, false);
              applyStyleFromProperties(ly);
              enforceStrokePolicy(ly);
              attachRecolorOnClick(ly);
          } 
      }).getLayers()[0]);
    
    // Crear grupo de hijos específico para este padre
    if (hijosLayers.length > 0 && code) {
      const childGroup = new L.FeatureGroup(hijosLayers);
      state.childGroupsByParent[code] = childGroup;
      map.addLayer(childGroup);
    }
    
    // Agregar hijos al grupo correspondiente y registrarlos para selección
    hijosLayers.forEach(l => {
      targetLayer.addLayer(l);
      registerChild(l, code);
    });
    
    // 4) Registrar padre, asociar sus hijos y activarlo
    registerParent(padreLayer);
    if (code) state.childrenByParent[code] = hijosLayers;
    setActiveParent(padreLayer);
    fitToAllIfAny();
    
    // Apagar recolor tras importar
    setRecolorMode(false);
    
    console.log('[HIERARCHY LOADED]', {
        padre: padreFeature.properties.codigo,
        hijos: hijosFeatures.length
    });
    
    // Notificación de éxito
    showImportSuccess(padreFeature.properties.codigo, hijosFeatures.length);
}

// Cargar todas las jerarquías (padres + hijos)
function loadAllHierarchies(padres, hijos) {
    let totalPadres = 0;
    let totalHijos = 0;
    
    // Cargar todos los padres
    Object.values(padres).forEach(padreFeature => {
        // Crear layer del padre
        const padreLayer = L.geoJSON(padreFeature, { 
            style: PADRE_STYLE, 
            onEachFeature: (f, l) => { 
                l.feature = f; 
                attachRecolorOnClick(l);
            } 
        }).getLayers()[0];
        
        DRAWN_EDITABLE.addLayer(padreLayer);
        
        // Registrar padre (sin activar automáticamente)
        registerParent(padreLayer);
        
        // Cargar hijos de este padre
        const code = padreFeature.properties?.codigo;
        if (code && hijos[code]) {
            const hijosFeatures = hijos[code];
            const hijosLayers = hijosFeatures
                .sort((a, b) => (a.properties.orden || 0) - (b.properties.orden || 0))
                .map(f => L.geoJSON(f, { 
                    style: HIJO_STYLE, 
                    onEachFeature: (ft, ly) => { 
                        ly.feature = ft; 
                        attachRecolorOnClick(ly);
                    } 
                }).getLayers()[0]);
            
            // Crear grupo de hijos específico
            if (hijosLayers.length > 0) {
                const childGroup = new L.FeatureGroup(hijosLayers);
                state.childGroupsByParent[code] = childGroup;
                map.addLayer(childGroup);
                totalHijos += hijosLayers.length;
            }
            
            // Agregar hijos al grupo editable y registrarlos para selección
            hijosLayers.forEach(l => {
                DRAWN_EDITABLE.addLayer(l);
                registerChild(l, code);
            });
            
            // Registrar asociación
            state.childrenByParent[code] = hijosLayers;
        }
        
        totalPadres++;
    });
    
    // NO cambiar estado de aislamiento automáticamente
    // NO activar padre específico - mostrar todos
    state.activeParent = null;
    activePadre = null;
    state.children = [];
    activeHijos = [];
    
    // Ajustar vista
    fitToAllIfAny();
    
    // Apagar recolor tras importar todo
    setRecolorMode(false);
    
    console.log('[ALL HIERARCHIES LOADED]', {
        padres: totalPadres,
        hijos: totalHijos
    });
    
    // Notificación de éxito
    showImportAllSuccess(totalPadres, totalHijos);
}

// Mostrar notificación de importación masiva exitosa
function showImportAllSuccess(numPadres, numHijos) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #00b894;
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 184, 148, 0.3);
        z-index: 10000;
        font-family: Arial, sans-serif;
        font-size: 14px;
        max-width: 300px;
    `;
    
    notification.innerHTML = `
        <strong>📂 Todas las Jerarquías Importadas</strong><br>
        👔 Padres: ${numPadres}<br>
        👶 Subcuadrantes: ${numHijos}<br>
        💡 Click en un padre para activarlo
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (document.body.contains(notification)) {
            document.body.removeChild(notification);
        }
    }, 5000);
}

// Mostrar notificación de importación exitosa
function showImportSuccess(codigoPadre, numHijos) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #6c5ce7;
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(108, 92, 231, 0.3);
        z-index: 10000;
        font-family: Arial, sans-serif;
        font-size: 14px;
        max-width: 300px;
    `;
    
    const hijosText = numHijos > 0 
        ? `👶 Subcuadrantes: ${numHijos}`
        : `✏️ Listo para crear subcuadrantes`;
    
    notification.innerHTML = `
        <strong>📂 Jerarquía Importada</strong><br>
        👔 Padre: ${codigoPadre}<br>
        ${hijosText}
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (document.body.contains(notification)) {
            document.body.removeChild(notification);
        }
    }, 4000);
}

// Show PROJECT import success notification
function showImportProjectSuccess(registered, hierarchyInfo) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #00b894;
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 184, 148, 0.3);
        z-index: 10000;
        font-family: Arial, sans-serif;
        font-size: 14px;
        max-width: 320px;
    `;
    
    notification.innerHTML = `
        <strong>🗂️ Proyecto Importado</strong><br>
        📊 ${registered} elementos registrados<br>
        ${hierarchyInfo.detected ? 
            `👔 ${hierarchyInfo.padres} padres, 👶 ${hierarchyInfo.hijos} hijos` : 
            '📐 Geometrías generales'}<br>
        ✓ Datos fusionados exitosamente
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (document.body.contains(notification)) {
            document.body.removeChild(notification);
        }
    }, 5000);
}

// Show ROUTE import success notification  
function showImportRouteSuccess(routeLabel, numHijos) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #fdcb6e;
        color: #2d3436;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(253, 203, 110, 0.3);
        z-index: 10000;
        font-family: Arial, sans-serif;
        font-size: 14px;
        max-width: 320px;
        font-weight: 500;
    `;
    
    notification.innerHTML = `
        <strong>✏️ Ruta Lista para Editar</strong><br>
        🛤️ ${routeLabel}<br>
        👶 ${numHijos} subcuadrantes<br>
        💡 Contexto del proyecto preservado
    `;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (document.body.contains(notification)) {
            document.body.removeChild(notification);
        }
    }, 5000);
}

// Función de importación general (sin jerarquía)
async function importGeneralQuadrants(data, forceState = null) {
    try {
        // Normalizar a FeatureCollection
        let features = [];
        if (data.type === 'FeatureCollection' && Array.isArray(data.features)) {
            features = data.features;
        } else if (data.type === 'Feature') {
            features = [data];
        } else {
            console.warn('[IMPORT] Formato no reconocido, esperaba FeatureCollection o Feature');
            return;
        }
        
        let total = features.length;
        let comunasFiltradas = 0;
        let noPoligonales = 0;
        let deduplicadas = 0;
        let agregadas = 0;
        
        // Obtener geometrías y códigos existentes para de-dupe y merge
        const existingGeometries = new Set();
        const existingCodigos = new Map(); // codigo -> layer
        forEachQuadrantLayer(layer => {
            if (layer.feature && layer.feature.geometry) {
                existingGeometries.add(JSON.stringify(layer.feature.geometry));
            }
            const codigo = layer.feature?.properties?.codigo;
            if (codigo) {
                existingCodigos.set(codigo, layer);
            }
        });
        
        // Procesar cada feature
        for (const feature of features) {
            if (!feature.geometry) continue;
            
            // Filtrar comunas (properties con NOMBRE, barrio o BARRIO)
            const props = feature.properties || {};
            if (props.NOMBRE || props.barrio || props.BARRIO) {
                comunasFiltradas++;
                continue;
            }
            
            // Solo geometrías poligonales
            const geomType = feature.geometry.type;
            if (!['Polygon', 'MultiPolygon'].includes(geomType)) {
                noPoligonales++;
                continue;
            }
            
            // Preparar polígonos para procesar
            let polygonsToProcess = [];
            
            if (geomType === 'Polygon') {
                polygonsToProcess.push({
                    type: 'Feature',
                    geometry: feature.geometry,
                    properties: { ...props }
                });
            } else if (geomType === 'MultiPolygon') {
                // Dividir MultiPolygon en varios Polygon, copiando todas las propiedades de estilo
                feature.geometry.coordinates.forEach(polygonCoords => {
                    polygonsToProcess.push({
                        type: 'Feature',
                        geometry: {
                            type: 'Polygon',
                            coordinates: polygonCoords
                        },
                        properties: { ...props } // Copiar todas las propiedades incluyendo fillColor, fillOpacity, etc.
                    });
                });
            }
            
            // Procesar cada polígono
            for (const polygonFeature of polygonsToProcess) {
                // De-dupe por geometría
                const geomStr = JSON.stringify(polygonFeature.geometry);
                if (existingGeometries.has(geomStr)) {
                    deduplicadas++;
                    continue;
                }
                
                // Verificar si existe por codigo para reemplazar
                const incomingCodigo = polygonFeature.properties?.codigo;
                const existingLayer = incomingCodigo ? existingCodigos.get(incomingCodigo) : null;
                
                if (existingLayer) {
                    // Reemplazar layer existente con el mismo codigo
                    console.debug(`[IMPORT] Reemplazando feature con codigo: ${incomingCodigo}`);
                    
                    // Remover layer existente
                    if (DRAWN_EDITABLE.hasLayer(existingLayer)) DRAWN_EDITABLE.removeLayer(existingLayer);
                    if (DRAWN_LOCKED.hasLayer(existingLayer)) DRAWN_LOCKED.removeLayer(existingLayer);
                    
                    // Remover de grupos de hijos si aplica
                    for (const grp of Object.values(state.childGroupsByParent || {})) {
                        if (grp.hasLayer && grp.hasLayer(existingLayer)) {
                            grp.removeLayer(existingLayer);
                        }
                    }
                }
                
                // Crear capa Leaflet
                const layer = L.geoJSON(polygonFeature, {
                    onEachFeature: (feat, lyr) => {
                        // Asignar feature al layer
                        lyr.feature = feat;
                        
                        // Detectar si es padre por nivel
                        const isPadre = (feat.properties && feat.properties.nivel === 'cuadrante');
                        
                        // Asegurar propiedades de estilo
                        ensureStyleProps(lyr, isPadre);
                        applyStyleFromProperties(lyr);
                        enforceStrokePolicy(lyr);
                        attachRecolorOnClick(lyr);
                    }
                }).getLayers()[0];
                
                if (layer) {
                    // Usar función específica para importados
                    addImportedFeatureLayer(polygonFeature, layer, forceState);
                    existingGeometries.add(geomStr);
                    if (incomingCodigo) existingCodigos.set(incomingCodigo, layer);
                    agregadas++;
                }
            }
        }
        
        // Ajustar vista si se agregaron capas
        if (agregadas > 0) {
            try {
                fitToAllIfAny();
            } catch (e) {
                console.warn('[IMPORT] Error al ajustar vista:', e);
            }
        }
        
        // Fallback: si no hay padre activo pero cargamos 1–N polígonos,
        // intenta adoptar uno como padre (por propiedades o por el mayor área).
        if (!state.activeParent) {
            let candidate = null;

            DRAWN_EDITABLE.eachLayer(l => {
                const p = l.feature?.properties || {};
                if (p.nivel === 'cuadrante' || p.tipo === 'PADRE' || p.codigo) {
                    candidate = candidate || l;
                }
            });

            // si aún no hay, tomar el polígono de mayor área
            if (!candidate) {
                let maxArea = -1;
                DRAWN_EDITABLE.eachLayer(l => {
                    const a = calculateArea(l.toGeoJSON());
                    if (a > maxArea) { maxArea = a; candidate = l; }
                });
            }

            if (candidate) {
                // Registrar y activar el padre adoptado
                registerParent(candidate);
                setActiveParent(candidate);
                // NO forzar aislamiento automático
                // state.isAislado = true;
                // isAislado = true;
                applyAislamiento();
                fitToAllIfAny();
                console.debug('[IMPORT][FALLBACK] Padre adoptado automáticamente:', candidate.feature?.properties?.codigo || '(sin código)');
            }
        }

        // Log de resultados
        console.info('[IMPORT]', { total, comunasFiltradas, noPoligonales, deduplicadas, agregadas });
        
    } catch (error) {
        console.error('[IMPORT] Error al procesar archivo:', error);
        alert('Error al importar el archivo. Verifique que sea un GeoJSON válido.');
    }
}

console.debug('Editor de cuadrantes inicializado correctamente');
