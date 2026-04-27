"""Test rápido: captura screenshot del mapa existente."""
import sys
from pathlib import Path

# Activar config
from config.secrets_manager import load_env_secure
load_env_secure(prefer_plain=True, enc_path="config/.env.enc",
                pass_env_var="MAPAS_SECRET_PASSPHRASE", cache=False)

from agente.captura import capturar_mapa_html

html = "static/maps/mapa_muestras.html"
png  = "static/maps/mapa_muestras.png"

print(f"Capturando {html} ...")
resultado = capturar_mapa_html(html, png, delay_ms=3000)
size = Path(resultado).stat().st_size // 1024
print(f"OK → {resultado}  ({size} KB)")
