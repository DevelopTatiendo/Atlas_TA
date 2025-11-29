/**
 * CONFIGURACIÓN DE DEPENDENCIAS PARA PRODUCCIÓN
 * 
 * Para uso completo del sistema de jerarquía, descarga e incluye:
 */

// 1. Turf.js - Operaciones geoespaciales precisas
// URL: https://cdn.jsdelivr.net/npm/@turf/turf@6/turf.min.js
// Reemplazar: /static/vendor/turf/turf.min.js

// 2. Proj4js - Transformaciones de coordenadas (opcional)
// URL: https://cdn.jsdelivr.net/npm/proj4@2/dist/proj4.min.js
// Crear: /static/vendor/proj4/proj4.min.js

/**
 * FUNCIONES QUE MEJORARÁN CON TURF.JS:
 */

// Reemplazar en editor.js:
function intersectGeometriesWithTurf(geom1, geom2) {
    if (typeof turf !== 'undefined') {
        try {
            const intersection = turf.intersect(geom1, geom2);
            return intersection;
        } catch (e) {
            console.warn('Turf intersection failed, using fallback');
        }
    }
    // Fallback a implementación actual
    return intersectGeometries(geom1, geom2);
}

function differenceGeometriesWithTurf(geom1, geom2) {
    if (typeof turf !== 'undefined') {
        try {
            const difference = turf.difference(geom1, geom2);
            return difference;
        } catch (e) {
            console.warn('Turf difference failed, using fallback');
        }
    }
    return differenceGeometries(geom1, geom2);
}

function unionGeometriesWithTurf(geometries) {
    if (typeof turf !== 'undefined' && geometries.length > 1) {
        try {
            let union = geometries[0];
            for (let i = 1; i < geometries.length; i++) {
                union = turf.union(union, geometries[i]);
            }
            return union;
        } catch (e) {
            console.warn('Turf union failed, using fallback');
        }
    }
    return unionGeometries(geometries);
}

function bufferGeometryWithTurf(geojson, distance) {
    if (typeof turf !== 'undefined') {
        try {
            // Convertir distancia a kilometers para turf
            const distanceKm = distance / 1000;
            return turf.buffer(geojson, distanceKm, { units: 'kilometers' });
        } catch (e) {
            console.warn('Turf buffer failed, using fallback');
        }
    }
    return bufferGeometry(geojson, distance);
}

function calculateAreaWithTurf(geojson) {
    if (typeof turf !== 'undefined') {
        try {
            return turf.area(geojson); // Retorna área en m²
        } catch (e) {
            console.warn('Turf area failed, using fallback');
        }
    }
    return calculateArea(geojson);
}

/**
 * INTEGRACIÓN AUTOMÁTICA:
 * 
 * Al cargar turf.js, las funciones se actualizarán automáticamente
 * para usar las operaciones precisas sin cambiar la API.
 */

// Verificar disponibilidad
function checkDependencies() {
    const status = {
        turf: typeof turf !== 'undefined',
        proj4: typeof proj4 !== 'undefined'
    };
    
    console.log('📦 Dependencias:', status);
    
    if (status.turf) {
        console.log('✅ Turf.js disponible - Operaciones geoespaciales precisas activadas');
    } else {
        console.log('⚠️ Turf.js no disponible - Usando aproximaciones básicas');
    }
    
    return status;
}

// Auto-ejecutar al cargar
if (typeof window !== 'undefined') {
    window.addEventListener('load', checkDependencies);
}
