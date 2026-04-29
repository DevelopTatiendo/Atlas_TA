"""
Gestión segura de secretos para Atlas TA.
Cifrado/descifrado de archivos .env usando Fernet + PBKDF2HMAC.

REGLA DE ORO: nunca se usa .env plano en ningún entorno.
Las credenciales viven SIEMPRE en config/.env.enc (cifrado).
La passphrase vive en la variable de entorno del OS: MAPAS_SECRET_PASSPHRASE.

── Setup local (una sola vez) ──────────────────────────────────────────────
  # Agregar a ~/.zshrc o ~/.bashrc:
  export MAPAS_SECRET_PASSPHRASE="passphrase_del_equipo"

── Setup en producción / Docker ────────────────────────────────────────────
  MAPAS_SECRET_PASSPHRASE=xxx streamlit run app.py
  # o en variables de entorno del servidor / CI

── Cifrar un .env nuevo ────────────────────────────────────────────────────
  python -m config.secrets_manager encrypt .env config/.env.enc
  # Después de cifrar, eliminar el .env plano del disco
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


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    """Deriva una clave Fernet desde una passphrase usando PBKDF2HMAC."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=200_000,
        backend=default_backend(),
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))


def _warn_plain_env_exists() -> None:
    """Advierte si existe un .env plano en el proyecto (no debería)."""
    plain_path = os.path.join(_PROJECT_ROOT, ".env")
    if os.path.exists(plain_path):
        print(
            "⚠️  ADVERTENCIA DE SEGURIDAD: existe un archivo .env plano en el proyecto.\n"
            f"   Ruta: {plain_path}\n"
            "   Este archivo contiene credenciales en texto claro.\n"
            "   Elimínalo del disco: rm .env\n"
            "   Las credenciales se cargan desde config/.env.enc (cifrado).\n",
            file=sys.stderr,
        )


def load_env_secure(
    enc_path: str = "config/.env.enc",
    pass_env_var: str = "MAPAS_SECRET_PASSPHRASE",
    # Parámetros legacy mantenidos solo para compatibilidad de firma — ignorados
    prefer_plain: bool = False,
    cache: bool = False,
) -> None:
    """Carga variables de entorno desde el archivo cifrado .env.enc.

    SIEMPRE usa el archivo cifrado. El parámetro `prefer_plain` ya no tiene
    efecto — se mantiene en la firma solo para no romper llamadas existentes.

    Args:
        enc_path:     Ruta al archivo .env.enc (relativa a la raíz del proyecto).
        pass_env_var: Variable de entorno del OS con la passphrase.
        prefer_plain: IGNORADO. Mantenido solo para compatibilidad.
        cache:        IGNORADO. Mantenido solo para compatibilidad.

    Raises:
        FileNotFoundError: Si no existe config/.env.enc.
        RuntimeError:      Si falta MAPAS_SECRET_PASSPHRASE.
        ValueError:        Si la passphrase es incorrecta o el archivo está corrupto.
    """
    # Siempre advertir si hay un .env plano suelto
    _warn_plain_env_exists()

    # Resolver ruta absoluta del .enc
    if not os.path.isabs(enc_path):
        enc_path = os.path.join(_PROJECT_ROOT, enc_path)

    if not os.path.exists(enc_path):
        raise FileNotFoundError(
            f"Archivo de secretos no encontrado: {enc_path}\n"
            "Opciones:\n"
            "  1. Pide el archivo config/.env.enc al equipo (viene en el repo).\n"
            "  2. Si tienes un .env, cífralo: python -m config.secrets_manager encrypt\n"
        )

    # Obtener passphrase desde variable de entorno del OS
    passphrase = os.environ.get(pass_env_var)
    if not passphrase:
        raise RuntimeError(
            f"Falta la variable de entorno '{pass_env_var}'.\n"
            "Agrégala a tu shell profile:\n"
            f"  export {pass_env_var}='passphrase_del_equipo'\n"
            "En producción: configúrala en las variables de entorno del servidor."
        )

    # Leer y descifrar
    with open(enc_path, "rb") as f:
        encrypted_data = f.read()

    if len(encrypted_data) < 17:
        raise ValueError(f"Archivo cifrado corrupto o vacío: {enc_path}")

    salt       = encrypted_data[:16]
    ciphertext = encrypted_data[16:]
    key        = _derive_key(passphrase, salt)
    fernet     = Fernet(key)

    try:
        plaintext = fernet.decrypt(ciphertext)
    except Exception as e:
        raise ValueError(
            f"No se pudo descifrar {enc_path}.\n"
            f"¿Es correcta la passphrase en '{pass_env_var}'?\n"
            f"Detalle: {e}"
        ) from e

    # Parsear e inyectar en os.environ (sin sobrescribir variables ya seteadas)
    env_vars = dotenv_values(stream=StringIO(plaintext.decode("utf-8")))
    loaded = sum(
        1 for k, v in env_vars.items()
        if k and v is not None and not os.environ.get(k)
        and (os.environ.__setitem__(k, v) or True)
    )
    print(f"🔐 {loaded} variables cargadas desde {os.path.basename(enc_path)}")


def encrypt_env(in_path: str = ".env", out_path: str = "config/.env.enc") -> None:
    """Cifra un archivo .env y lo guarda en out_path.

    Tras cifrar, elimina el archivo plano del disco para no dejar credenciales expuestas.
    """
    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Archivo de entrada no encontrado: {in_path}")

    with open(in_path, "rb") as f:
        plaintext = f.read()

    passphrase = getpass.getpass("Passphrase para cifrar (mín. 16 caracteres): ")
    if len(passphrase.strip()) < 8:
        raise ValueError("La passphrase debe tener al menos 8 caracteres.")

    confirm = getpass.getpass("Confirma passphrase: ")
    if passphrase != confirm:
        raise ValueError("Las passphrases no coinciden.")

    salt       = secrets.token_bytes(16)
    key        = _derive_key(passphrase, salt)
    fernet     = Fernet(key)
    ciphertext = fernet.encrypt(plaintext)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(salt + ciphertext)

    print(f"✅ Cifrado guardado en: {out_path}")
    print(f"   Salt: {salt.hex()[:16]}... | Tamaño: {len(ciphertext)} bytes")

    # Eliminar el .env plano automáticamente
    try:
        os.remove(in_path)
        print(f"🗑️  Archivo plano eliminado: {in_path}")
    except OSError as e:
        print(f"⚠️  No se pudo eliminar {in_path}: {e}")
        print("   Elimínalo manualmente para no dejar credenciales expuestas.")


def _cli_main() -> None:
    """Interfaz de línea de comandos."""
    usage = (
        "Uso: python -m config.secrets_manager <comando>\n"
        "\n"
        "Comandos:\n"
        "  encrypt [input] [output]  Cifra un .env plano → .enc\n"
        "                            Default: .env → config/.env.enc\n"
        "\n"
        "Ejemplo:\n"
        "  python -m config.secrets_manager encrypt\n"
        "  python -m config.secrets_manager encrypt .env config/.env.enc\n"
    )

    if len(sys.argv) < 2:
        print(usage)
        return

    command = sys.argv[1]

    if command == "encrypt":
        in_path  = sys.argv[2] if len(sys.argv) > 2 else ".env"
        out_path = sys.argv[3] if len(sys.argv) > 3 else "config/.env.enc"
        try:
            encrypt_env(in_path, out_path)
        except Exception as e:
            print(f"❌ Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"❌ Comando desconocido: {command}\n{usage}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()
