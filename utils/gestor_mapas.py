#gestor_mapas.py

import os
from pathlib import Path
import time
import logging

logger = logging.getLogger(__name__)

# Directorio absoluto base para guardar mapas (independiente del CWD)
BASE_DIR = Path(__file__).resolve().parent.parent  # subir a raíz del proyecto si utils/ esta dentro
DEFAULT_MAPS_DIR = BASE_DIR / 'static' / 'maps'

def guardar_mapa_controlado(mapa, tipo_mapa, permitir_multiples=False, carpeta=DEFAULT_MAPS_DIR, max_archivos=10):
    """
    Guarda el mapa HTML con control de limpieza.
    - Si permitir_multiples=False: siempre guarda en nombre fijo <tipo_mapa>.html (sobrescribe la versión anterior).
    - Si permitir_multiples=True: genera nombre con timestamp <tipo_mapa>_<ts>.html.
    - Siempre limpia para dejar como máximo `max_archivos` archivos que comiencen con el prefijo.
    """
    # Aceptar Path o str
    carpeta_path = Path(carpeta) if not isinstance(carpeta, Path) else carpeta
    os.makedirs(carpeta_path, exist_ok=True)

    if permitir_multiples:
        timestamp = int(time.time())
        filename = f"{tipo_mapa}_{timestamp}.html"
    else:
        filename = f"{tipo_mapa}.html"

    filepath = str(carpeta_path / filename)

    # Siempre guardar (sobrescribir si existe en modo único)
    try:
        mapa.save(filepath)
        logger.info(f"[MAPAS] Guardando mapa tipo={tipo_mapa} en {filepath}")
    except Exception as e:
        logger.error(f"Error guardando mapa en {filepath}: {e}")

    limpiar_mapas_antiguos(str(carpeta_path), tipo_mapa, max_archivos)
    return filename

def limpiar_mapas_antiguos(directorio, prefijo, max_archivos):
    archivos = sorted(
        [f for f in Path(directorio).glob(f"{prefijo}*.html")],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )
    for archivo in archivos[max_archivos:]:
        try:
            archivo.unlink()
            logger.info(f"Archivo eliminado: {archivo.name}")
        except Exception as e:
            logger.warning(f"Error al eliminar {archivo.name}: {e}")
