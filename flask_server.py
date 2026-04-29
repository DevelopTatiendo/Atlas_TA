from config.secrets_manager import load_env_secure
load_env_secure()  # Usa config/.env.enc + MAPAS_SECRET_PASSPHRASE del OS

from flask import Flask, send_from_directory, abort, request
from pathlib import Path
from flask_cors import CORS
import os

# Directorios base absolutos y configuración explícita de estáticos
BASE_DIR = Path(__file__).resolve().parent
MAPS_DIR = BASE_DIR / "static" / "maps"
QUADRANTS_EDITOR_DIR = BASE_DIR / "static" / "quadrants_editor"
VENDOR_DIR = BASE_DIR / "static" / "vendor"
GEOJSON_DIR = BASE_DIR / "geojson"

app = Flask(
    __name__,
    static_folder=str(BASE_DIR / "static"),
    static_url_path="/static"
)
CORS(app)  # Esto permite las solicitudes cross-origin

# Directorios definidos arriba

# Ruta para servir archivos estáticos desde la carpeta static/maps
@app.route('/maps/<path:filename>')
@app.route('/static/maps/<path:filename>')
def serve_map(filename):
    full_path = MAPS_DIR / filename
    print(f"[FLASK] [MAP] BASE_DIR={BASE_DIR} | MAPS_DIR={MAPS_DIR} | filename={filename} | exists={full_path.exists()}", flush=True)
    return send_from_directory(MAPS_DIR, filename)

# Ruta para servir el editor de cuadrantes - devuelve la página principal
@app.route('/editor/cuadrantes')
def serve_quadrants_editor():
    print("[EDITOR] Serving quadrants editor", flush=True)
    return send_from_directory(QUADRANTS_EDITOR_DIR, 'index.html')

# Ruta para servir la página de validación del sistema
@app.route('/test/jerarquia')
def serve_validation_test():
    print("[TEST] Serving hierarchy validation test", flush=True)
    return send_from_directory('static/quadrants_editor', 'validation_test.html')

# Ruta para servir assets del editor de cuadrantes (JS, CSS, etc.)
@app.route('/static/quadrants_editor/<path:filename>')
def serve_quadrants_assets(filename):
    return send_from_directory(QUADRANTS_EDITOR_DIR, filename)

# Ruta para servir librerías vendor locales
@app.route('/static/vendor/<path:filename>')
def serve_vendor_assets(filename):
    return send_from_directory(VENDOR_DIR, filename)

# Ruta para servir archivos geojson con validación de seguridad
@app.route('/geojson/<path:filename>')
def serve_geojson(filename):
    # Bloquear traversal y rutas absolutas
    if '..' in filename or filename.startswith('/'):
        abort(400, description="Archivo no permitido")
    # Aceptar solo .geojson o .json
    if not (filename.endswith('.geojson') or filename.endswith('.json')):
        abort(400, description="Extensión no permitida")
    return send_from_directory(GEOJSON_DIR, filename, mimetype='application/geo+json')

# Ruta para servir GeoJSON por defecto según ciudad
@app.route('/geojson/default')
def geojson_default():
    from unicodedata import normalize
    
    # 1) Leer ciudad de query y normalizar (espacios, acentos, mayúsculas)
    raw = (request.args.get('city') or 'CALI').strip()
    # "BOGOTÁ" -> "bogota", "Medellín" -> "medellin"
    city_slug = normalize('NFKD', raw).encode('ascii', 'ignore').decode('ascii')
    city_slug = city_slug.lower().replace(' ', '_')

    # 2) Construir nombre estándar de comunas
    filename = f'comunas_{city_slug}.geojson'

    # 3) Verificar existencia (y fallback opcional por compatibilidad)
    path = GEOJSON_DIR / filename
    if not path.exists():
        # Fallback legacy solo para CALI (por si aún no está el comunas_cali.geojson)
        legacy = GEOJSON_DIR / 'cuadrantes_cali_rutas_consultores.geojson'
        if city_slug == 'cali' and legacy.exists():
            filename = 'cuadrantes_cali_rutas_consultores.geojson'
        else:
            abort(404, description=f"No hay GeoJSON de comunas para '{raw}' (esperado: {filename})")

    print(f"[GEOJSON] default city={raw} -> {filename}", flush=True)
    return send_from_directory(GEOJSON_DIR, filename, mimetype='application/geo+json')

if __name__ == '__main__':
    # Asegurar que las carpetas necesarias existen (rutas absolutas)
    os.makedirs(MAPS_DIR, exist_ok=True)
    os.makedirs(QUADRANTS_EDITOR_DIR, exist_ok=True)
    print(f"[FLASK] Iniciando servidor. BASE_DIR={BASE_DIR} | MAPS_DIR={MAPS_DIR}", flush=True)
    app.run(port=5000)