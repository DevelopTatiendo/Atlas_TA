# Configuración de Secretos - MAPAS_TA_DEV_1

## Instrucciones de Uso

### 1. Configuración Inicial (Desarrollo Local)
1. Copia `config/.env.example` a `.env` en la raíz del proyecto
2. Completa los valores reales en `.env`

### 2. Cifrado para Producción
```bash
# Cifrar el archivo .env
python -m config.secrets_manager encrypt

# Especificar rutas personalizadas (opcional)
python -m config.secrets_manager encrypt .env config/.env.enc
```

### 3. Borrar .env Original
Después del cifrado exitoso, borra el archivo `.env` original:
```bash
rm .env  # Linux/Mac
del .env # Windows
```

### 4. Ejecución en Producción
Configura la passphrase como variable de entorno:
```bash
export MAPAS_SECRET_PASSPHRASE="tu_passphrase_segura"
python app.py
```

O permite el prompt interactivo (menos seguro para automatización):
```bash
python app.py
# Te pedirá la passphrase por consola
```

## Variables de Entorno Requeridas

- `DB_HOST`: Host de la base de datos MySQL
- `DB_PORT`: Puerto de MySQL (por defecto: 3306)
- `DB_USER`: Usuario de MySQL
- `DB_PASSWORD`: Contraseña de MySQL
- `DB_NAME`: Nombre de la base de datos
- `ENVIRONMENT`: Entorno (development/production)
- `FLASK_SERVER_URL`: URL del servidor Flask (por defecto: http://localhost:5000)

## Seguridad

- El archivo `.env.enc` usa cifrado Fernet con clave derivada por PBKDF2HMAC (SHA256, 200k iteraciones)
- Los secretos solo se mantienen en memoria durante la ejecución
- Nunca se escriben secretos descifrados a disco
- El `.env` original debe eliminarse después del cifrado