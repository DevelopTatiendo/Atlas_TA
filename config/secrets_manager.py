"""
Gestión segura de secretos para MAPAS_TA_DEV_1.
Cifrado/descifrado de archivos .env usando Fernet + PBKDF2HMAC.
"""

import os
import sys
import getpass
from io import StringIO
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64
import secrets
from dotenv import dotenv_values


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    """Deriva una clave Fernet desde una passphrase usando PBKDF2HMAC."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=200_000,  # >= 200k iteraciones
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode('utf-8')))
    return key


def encrypt_env(in_path=".env", out_path="config/.env.enc"):
    """
    Cifra un archivo .env usando Fernet con clave derivada por PBKDF2HMAC.
    
    Args:
        in_path: Ruta del archivo .env a cifrar
        out_path: Ruta donde guardar el archivo cifrado
    """
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Archivo no encontrado: {in_path}")
    
    # Leer contenido del archivo .env
    with open(in_path, 'rb') as f:
        plaintext = f.read()
    
    # Solicitar passphrase
    passphrase = getpass.getpass("Ingrese passphrase para cifrar: ")
    if not passphrase.strip():
        raise ValueError("La passphrase no puede estar vacía")
    
    # Generar salt aleatorio de 16 bytes
    salt = secrets.token_bytes(16)
    
    # Derivar clave
    key = _derive_key(passphrase, salt)
    
    # Cifrar contenido
    fernet = Fernet(key)
    ciphertext = fernet.encrypt(plaintext)
    
    # Guardar: salt (16 bytes) + ciphertext
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'wb') as f:
        f.write(salt + ciphertext)
    
    print(f"✅ Archivo cifrado guardado en: {out_path}")
    print(f"🔑 Salt: {salt.hex()[:16]}... ({len(salt)} bytes)")
    print(f"📦 Tamaño cifrado: {len(ciphertext)} bytes")


def load_env_secure(prefer_plain=True, enc_path="config/.env.enc", pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False):
    """
    Carga variables de entorno de forma segura.
    
    Args:
        prefer_plain: Si True y existe .env, lo usa. Si no, usa el archivo cifrado.
        enc_path: Ruta del archivo cifrado
        pass_env_var: Variable de entorno con la passphrase
        cache: No implementado (para compatibilidad futura)
    """
    # Opción 1: Usar .env plano si existe y se prefiere
    if prefer_plain and os.path.exists(".env"):
        print("🔓 Cargando .env plano (desarrollo)")
        from dotenv import load_dotenv
        load_dotenv()
        return
    
    # Opción 2: Usar archivo cifrado
    if not os.path.exists(enc_path):
        # Si no hay .env ni .env.enc, error claro
        if not os.path.exists(".env"):
            raise RuntimeError(f"Falta configuración: no existe .env ni {enc_path}")
        raise FileNotFoundError(f"Archivo cifrado no encontrado: {enc_path}")
    
    # Obtener passphrase
    passphrase = os.environ.get(pass_env_var)
    if not passphrase:
        # Si hay archivo cifrado pero falta passphrase, error específico
        raise RuntimeError(f"Falta {pass_env_var} para descifrar {enc_path}")
    
    # Leer archivo cifrado
    with open(enc_path, 'rb') as f:
        encrypted_data = f.read()
    
    if len(encrypted_data) < 16:
        raise ValueError("Archivo cifrado corrupto (demasiado corto)")
    
    # Extraer salt y ciphertext
    salt = encrypted_data[:16]
    ciphertext = encrypted_data[16:]
    
    # Derivar clave y descifrar
    key = _derive_key(passphrase, salt)
    fernet = Fernet(key)
    
    try:
        plaintext = fernet.decrypt(ciphertext)
    except Exception as e:
        raise ValueError(f"Error al descifrar (passphrase incorrecta?): {e}")
    
    # Parsear contenido descifrado en memoria
    env_content = plaintext.decode('utf-8')
    env_vars = dotenv_values(stream=StringIO(env_content))
    
    # Cargar en os.environ (solo si no existe)
    loaded_count = 0
    for key, value in env_vars.items():
        if key and value is not None:
            os.environ.setdefault(key, value)
            loaded_count += 1
    
    print(f"🔐 Variables cargadas desde archivo cifrado: {loaded_count}")


def _cli_main():
    """Interfaz de línea de comandos."""
    if len(sys.argv) < 2:
        print("Uso: python -m config.secrets_manager <comando> [argumentos]")
        print("Comandos:")
        print("  encrypt [input] [output]  - Cifra archivo .env")
        print("Ejemplo:")
        print("  python -m config.secrets_manager encrypt")
        print("  python -m config.secrets_manager encrypt .env config/.env.enc")
        return
    
    command = sys.argv[1]
    
    if command == "encrypt":
        in_path = sys.argv[2] if len(sys.argv) > 2 else ".env"
        out_path = sys.argv[3] if len(sys.argv) > 3 else "config/.env.enc"
        
        try:
            encrypt_env(in_path, out_path)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    else:
        print(f"❌ Comando desconocido: {command}")
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()